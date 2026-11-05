// AI 속의 사람 · 중계 서버
// 게임 화면(public/index.html)을 내려주고, 같은 방에 있는 화면끼리 메시지를 전달만 한다.
// 판정은 각 화면이 하고, 서버는 방 관리와 전달만 한다.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, 'public');
const ROOM_RE = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const TOPIC_RE = /^[a-z][a-z0-9_.-]{0,47}$/;
const MAX_ROOM = 16;          // 한 방 최대 접속자
const MAX_MSG = 8192;         // 메시지 1개 최대 바이트
const MAX_PRESENCE = 4096;    // 상태 객체 최대 바이트
const MAX_ROOMS = 500;        // 서버 전체 방 수 상한

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon' };

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/healthz') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok'); return; }
  let file = url === '/' ? '/index.html' : url;
  file = path.normalize(file).replace(/^(\.\.[/\\])+/, '');
  const full = path.join(PUBLIC, file);
  if (!full.startsWith(PUBLIC)) { res.writeHead(403); res.end(); return; }
  fs.readFile(full, (err, buf) => {
    if (err) { res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); res.end('not found'); return; }
    res.writeHead(200, { 'content-type': TYPES[path.extname(full)] || 'application/octet-stream', 'cache-control': 'no-cache' });
    res.end(buf);
  });
});

const wss = new WebSocketServer({ server, maxPayload: MAX_MSG });
const rooms = new Map(); // name -> Map(peerId -> {ws, presence})

function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function bytes(o) { return Buffer.byteLength(JSON.stringify(o)); }
function newPeerId(room) {
  for (;;) {
    const id = crypto.randomBytes(4).toString('hex'); // 방장 순서를 정할 때 쓰는 이름
    if (!room.has(id)) return id;
  }
}
function leave(ws) {
  const info = ws.info;
  if (!info) return;
  ws.info = null;
  const room = rooms.get(info.room);
  if (!room) return;
  room.delete(info.peer);
  for (const m of room.values()) send(m.ws, { t: 'left', peer: info.peer });
  if (room.size === 0) rooms.delete(info.room);
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  let tokens = 200, last = Date.now();
  ws.on('message', (data) => {
    // 연결당 초당 100개 정도까지만 허용
    const now = Date.now();
    tokens = Math.min(200, tokens + (now - last) * 0.1); last = now;
    if (tokens < 1) return;
    tokens -= 1;
    let m;
    try { m = JSON.parse(data.toString()); } catch (_) { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'join') {
      if (ws.info) return;
      const name = String(m.room || '');
      if (!ROOM_RE.test(name)) { send(ws, { t: 'err', code: 'invalid_argument' }); return; }
      let room = rooms.get(name);
      if (!room) {
        if (rooms.size >= MAX_ROOMS) { send(ws, { t: 'err', code: 'limit_reached' }); return; }
        room = new Map(); rooms.set(name, room);
      }
      if (room.size >= MAX_ROOM) { send(ws, { t: 'err', code: 'limit_reached' }); return; }
      const peer = newPeerId(room);
      const others = [...room.entries()].map(([p, v]) => ({ peer: p, presence: v.presence }));
      room.set(peer, { ws, presence: {} });
      ws.info = { room: name, peer };
      send(ws, { t: 'joined', peer, peers: others });
      for (const [p, v] of room) if (p !== peer) send(v.ws, { t: 'join', peer });
      return;
    }
    if (!ws.info) return;
    const room = rooms.get(ws.info.room);
    if (!room) return;

    if (m.t === 'emit') {
      if (typeof m.topic !== 'string' || !TOPIC_RE.test(m.topic)) return;
      if (bytes(m.data === undefined ? null : m.data) > 4096) return;
      const out = { t: 'ev', topic: m.topic, data: m.data, peer: ws.info.peer };
      for (const v of room.values()) send(v.ws, out);
    } else if (m.t === 'pr') {
      if (!m.patch || typeof m.patch !== 'object' || Array.isArray(m.patch)) return;
      const me = room.get(ws.info.peer);
      const next = Object.assign({}, me.presence);
      for (const k of Object.keys(m.patch)) {
        if (!/^[A-Za-z_][A-Za-z0-9_-]{0,63}$/.test(k) || k === '__proto__') continue;
        if (m.patch[k] === null) delete next[k]; else next[k] = m.patch[k];
      }
      if (bytes(next) > MAX_PRESENCE) return;
      me.presence = next;
      const out = { t: 'pr', peer: ws.info.peer, presence: next };
      for (const [p, v] of room) if (p !== ws.info.peer) send(v.ws, out);
    }
  });
  ws.on('close', () => leave(ws));
  ws.on('error', () => leave(ws));
});

// 끊어진 연결 정리
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; try { ws.ping(); } catch (_) {}
  }
}, 30000);

server.listen(PORT, () => console.log('listening on ' + PORT));
module.exports = { server, wss };
