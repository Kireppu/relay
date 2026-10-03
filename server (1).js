// Video relay: ESP32-CAM pushes JPEG frames over WebSocket (/ingest),
// the app watches them as MJPEG (/stream). Set TOKEN as an environment variable.
const http = require('http');
const { WebSocketServer } = require('ws');

const TOKEN = process.env.TOKEN;
const PORT = process.env.PORT || 8080;
if (!TOKEN) { console.error('Set the TOKEN environment variable'); process.exit(1); }

const viewers = new Set();

const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname === '/health') { res.end('ok'); return; }
  if (u.pathname === '/stream') {
    if (u.searchParams.get('token') !== TOKEN) { res.writeHead(401).end(); return; }
    res.writeHead(200, {
      'Content-Type': 'multipart/x-mixed-replace; boundary=frame',
      'Cache-Control': 'no-store',
      'Connection': 'keep-alive',
    });
    viewers.add(res);
    req.on('close', () => viewers.delete(res));
    return;
  }
  res.writeHead(404).end();
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname !== '/ingest' || u.searchParams.get('token') !== TOKEN) { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => {
    ws.on('message', (data, isBinary) => {
      if (!isBinary) return;
      const header = `--frame\r\nContent-Type: image/jpeg\r\nContent-Length: ${data.length}\r\n\r\n`;
      for (const v of viewers) {
        if (v.writableLength > 256 * 1024) continue;   // slow viewer: drop frames, never add latency
        v.write(header);
        v.write(data);
        v.write('\r\n');
      }
    });
  });
});

server.listen(PORT, () => console.log('relay listening on ' + PORT));
