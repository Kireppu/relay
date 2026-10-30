/*
 * Robot relay for Render (Node 18+, only dependency: ws)
 *
 *   wss://HOST/robot?token=ROBOT_TOKEN        robot control socket (JSON)
 *   wss://HOST/robot-video?token=ROBOT_TOKEN  robot video socket (binary JPEG frames)
 *   wss://HOST/app?token=APP_TOKEN            app control socket (JSON)
 *   https://HOST/stream?token=APP_TOKEN       MJPEG for the app's WebView
 *   https://HOST/health                       health check (use with an uptime pinger)
 *
 * App -> robot messages (anything else is dropped):
 *   {"t":"ka"}                                           keepalive - send every ~500 ms
 *   {"t":"cmd","mode":"AUTO|MANUAL","dir":"FORWARD|BACK|LEFT|RIGHT|STOP",
 *              "obstacle":"ON|OFF","lights":"ON|OFF"}    any subset of the fields
 * Robot -> app: {"t":"state", ...} about every 300 ms, plus "online": false when the robot drops.
 *
 * Env vars: ROBOT_TOKEN, APP_TOKEN (or one TOKEN for both). PORT is set by Render.
 */

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const ROBOT_TOKEN = process.env.ROBOT_TOKEN || process.env.TOKEN;
const APP_TOKEN = process.env.APP_TOKEN || process.env.TOKEN;
if (!ROBOT_TOKEN || !APP_TOKEN) {
  console.error('Set ROBOT_TOKEN and APP_TOKEN (or TOKEN) environment variables.');
  process.exit(1);
}

function safeEq(a, b) {
  const A = Buffer.from(String(a || ''));
  const B = Buffer.from(String(b));
  return A.length === B.length && crypto.timingSafeEqual(A, B);
}

// ---------- state ----------
let robot = null;                 // robot control socket
let robotVideo = null;            // robot video socket
let lastState = { t: 'state', online: false };
let lastParams = null;
const viewers = new Set();        // open /stream responses

const robotWss = new WebSocketServer({ noServer: true, maxPayload: 4 * 1024 });
const videoWss = new WebSocketServer({ noServer: true, maxPayload: 512 * 1024 });
const appWss = new WebSocketServer({ noServer: true, maxPayload: 1024 });

function sendJson(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}
function broadcastApps(obj) {
  const s = JSON.stringify(obj);
  for (const c of appWss.clients) if (c.readyState === WebSocket.OPEN) c.send(s);
}
function updateVideoWanted() {
  sendJson(robot, { t: 'video', on: viewers.size > 0 });
}

// ---------- command whitelist (the app can never send anything else to the robot) ----------
const ENUM = {
  mode: ['AUTO', 'MANUAL'],
  dir: ['FORWARD', 'BACK', 'LEFT', 'RIGHT', 'STOP'],
  obstacle: ['ON', 'OFF'],
  lights: ['ON', 'OFF'],
};
const ALIAS = { direction: 'dir' };

function sanitize(m) {
  if (!m || typeof m !== 'object') return null;
  if (m.t === 'ka') return { t: 'ka' };
  if (m.t === 'params') return { t: 'params' };
  if (m.t === 'reset') return { t: 'reset' };
  if (m.t === 'set') {
    const id = Number(m.id), v = Number(m.v);
    if (!Number.isInteger(id) || id < 0 || id > 199) return null;
    if (!Number.isFinite(v) || Math.abs(v) > 100000) return null;
    return { t: 'set', id, v };
  }
  if (m.t !== 'cmd') return null;
}

// ---------- HTTP ----------
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');

  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end('ok');
  }

  if (url.pathname === '/stream') {
    if (!safeEq(url.searchParams.get('token'), APP_TOKEN)) {
      res.writeHead(401);
      return res.end();
    }
    res.writeHead(200, {
      'Content-Type': 'multipart/x-mixed-replace; boundary=frame',
      'Cache-Control': 'no-store',
      'Access-Control-Allow-Origin': '*',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    viewers.add(res);
    updateVideoWanted();
    req.on('close', () => {
      viewers.delete(res);
      updateVideoWanted();
    });
    return;
  }

  res.writeHead(404);
  res.end();
});

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://x');
  const routes = {
    '/robot': [robotWss, ROBOT_TOKEN],
    '/robot-video': [videoWss, ROBOT_TOKEN],
    '/app': [appWss, APP_TOKEN],
  };
  const route = routes[url.pathname];
  if (!route || !safeEq(url.searchParams.get('token'), route[1])) {
    socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
    return socket.destroy();
  }
  route[0].handleUpgrade(req, socket, head, (ws) => route[0].emit('connection', ws, req));
});

// ---------- robot control ----------
robotWss.on('connection', (ws) => {
  if (robot && robot !== ws) robot.terminate();   // newest connection wins
  robot = ws;
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  updateVideoWanted();

  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    let m;
    try { m = JSON.parse(data.toString()); } catch { return; }
    if (!m) return;
    if (m.t === 'params') { lastParams = m; broadcastApps(m); return; }
    if (m.t !== 'state') return;
    m.online = true;
    lastState = m;
    broadcastApps(m);
  });

  ws.on('close', () => {
    if (robot === ws) {
      robot = null;
      lastState = { t: 'state', online: false };
      lastParams = null;
      broadcastApps(lastState);
    }
  });
  ws.on('error', () => {});
});

// ---------- robot video ----------
videoWss.on('connection', (ws) => {
  if (robotVideo && robotVideo !== ws) robotVideo.terminate();
  robotVideo = ws;
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));

  ws.on('message', (data, isBinary) => {
    if (!isBinary || viewers.size === 0) return;
    const head = Buffer.from(
      `--frame\r\nContent-Type: image/jpeg\r\nContent-Length: ${data.length}\r\n\r\n`
    );
    for (const res of viewers) {
      if (res.writableLength > 512 * 1024) continue;  // slow viewer: drop this frame
      res.write(head);
      res.write(data);
      res.write('\r\n');
    }
  });

  ws.on('close', () => { if (robotVideo === ws) robotVideo = null; });
  ws.on('error', () => {});
});

// ---------- app control ----------
appWss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  sendJson(ws, robot ? lastState : { t: 'state', online: false });
  if (robot && lastParams) sendJson(ws, lastParams);

  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    let m;
    try { m = JSON.parse(data.toString()); } catch { return; }
    const out = sanitize(m);
    if (out && robot) sendJson(robot, out);
  });
  ws.on('error', () => {});
});

// ---------- dead-socket detection ----------
setInterval(() => {
  for (const wss of [robotWss, videoWss, appWss]) {
    for (const ws of wss.clients) {
      if (!ws.isAlive) { ws.terminate(); continue; }
      ws.isAlive = false;
      ws.ping();
    }
  }
}, 20000);

server.listen(PORT, () => console.log(`relay listening on ${PORT}`));
