/**
 * Apex GT — servidor de salas (HTTP + relay WebSocket).
 *
 * Sin dependencias: solo Node.js 18 o superior.
 *
 *   node server.mjs
 *   → http://localhost:3000
 *
 * Sirve el juego por HTTP y hace de puente entre los navegadores de la sala.
 * No hay WebRTC, así que no hay NAT, ni ICE, ni TURN que puedan fallar:
 * todos los mensajes pasan por aquí y llegan siempre.
 *
 * Para jugar con amigos por internet, expón este puerto con un túnel o
 * publícalo en Render/Fly/Railway (ver COMO-JUGAR.md).
 */
import http from 'node:http';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 3000;
const HOST = process.env.HOST || '0.0.0.0';

const MAX_PLAYERS = 4;
const ROOM_RE = /^[A-Z0-9]{4}$/;
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// ---------------------------------------------------------------- utilidades

const now = () => Date.now();
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);
const randomId = () => crypto.randomBytes(5).toString('hex');

// ------------------------------------------------------------ tramas WebSocket
// Implementación mínima de RFC 6455: texto, ping/pong, cierre y fragmentación.

function encodeFrame(opcode, payload) {
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | opcode;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}

const frameText = (str) => encodeFrame(0x1, Buffer.from(str, 'utf8'));
const framePong = (payload) => encodeFrame(0xA, payload);
const framePing = (payload = Buffer.alloc(0)) => encodeFrame(0x9, payload);
const frameClose = () => encodeFrame(0x8, Buffer.alloc(0));

/** Extrae todas las tramas completas de un buffer acumulado. */
function decodeFrames(state) {
  const frames = [];
  let buf = state.buf;
  for (;;) {
    if (buf.length < 2) break;
    const fin = (buf[0] & 0x80) !== 0;
    const opcode = buf[0] & 0x0f;
    const masked = (buf[1] & 0x80) !== 0;
    let len = buf[1] & 0x7f;
    let off = 2;

    if (len === 126) {
      if (buf.length < 4) break;
      len = buf.readUInt16BE(2);
      off = 4;
    } else if (len === 127) {
      if (buf.length < 10) break;
      const big = buf.readBigUInt64BE(2);
      if (big > BigInt(8 * 1024 * 1024)) throw new Error('frame too large');
      len = Number(big);
      off = 10;
    }

    const maskLen = masked ? 4 : 0;
    if (buf.length < off + maskLen + len) break;

    const mask = masked ? buf.subarray(off, off + 4) : null;
    off += maskLen;
    const payload = Buffer.from(buf.subarray(off, off + len));
    if (mask) for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];

    buf = buf.subarray(off + len);
    frames.push({ fin, opcode, payload });
  }
  state.buf = buf;
  return frames;
}

// ------------------------------------------------------------------- salas

/** @type {Map<string, {code:string, hostId:string, clients:Map<string, any>, created:number}>} */
const rooms = new Map();

function roomOf(code) {
  return rooms.get(code);
}

function send(client, obj) {
  if (!client || client.closed) return;
  try {
    client.socket.write(frameText(JSON.stringify(obj)));
  } catch {
    closeClient(client, 'write-error');
  }
}

function createRoom(code) {
  const room = {
    code,
    hostId: null,
    clients: new Map(),
    created: now(),
  };
  rooms.set(code, room);
  log(`sala ${code} creada`);
  return room;
}

function joinRoom(client, code, asHost) {
  let room = roomOf(code);

  if (asHost) {
    if (room) {
      // Un anfitrión nuevo reemplaza una sala huérfana (sin clientes vivos).
      if (room.clients.size > 0) {
        send(client, { t: 'error', reason: 'taken' });
        return closeClient(client, 'room-taken');
      }
      rooms.delete(code);
    }
    room = createRoom(code);
    room.hostId = client.id;
  } else {
    if (!room) {
      send(client, { t: 'error', reason: 'notfound' });
      return closeClient(client, 'room-notfound');
    }
    if (room.clients.size >= MAX_PLAYERS) {
      send(client, { t: 'error', reason: 'full' });
      return closeClient(client, 'room-full');
    }
  }

  client.room = room;
  room.clients.set(client.id, client);

  send(client, {
    t: 'ready',
    id: client.id,
    host: room.hostId,
    peers: [...room.clients.keys()].filter((id) => id !== client.id),
  });

  for (const other of room.clients.values()) {
    if (other.id !== client.id) send(other, { t: 'join', id: client.id });
  }

  log(`sala ${code}: entra ${client.id} (${room.clients.size}/${MAX_PLAYERS})`);
}

function leaveRoom(client, reason) {
  const room = client.room;
  if (!room) return;
  client.room = null;

  room.clients.delete(client.id);
  log(`sala ${room.code}: sale ${client.id} (${reason})`);

  if (room.clients.size === 0) {
    rooms.delete(room.code);
    log(`sala ${room.code} eliminada`);
    return;
  }

  if (room.hostId === client.id) {
    // Sin anfitrión la carrera no puede continuar: se cierra para todos.
    for (const other of room.clients.values()) {
      send(other, { t: 'hostgone' });
      try { other.socket.end(frameClose()); } catch {}
      other.closed = true;
    }
    rooms.delete(room.code);
    log(`sala ${room.code} cerrada (se fue el anfitrión)`);
    return;
  }

  for (const other of room.clients.values()) send(other, { t: 'leave', id: client.id });
}

function closeClient(client, reason) {
  if (client.closed) return;
  client.closed = true;
  leaveRoom(client, reason);
  try { client.socket.destroy(); } catch {}
}

// --------------------------------------------------------------- HTTP + WS

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.woff2': 'font/woff2',
  '.glb': 'model/gltf-binary',   // modelo 3D del auto
  '.wav': 'audio/wav',            // sonidos
  '.mp3': 'audio/mpeg',
  '.ogg': 'audio/ogg',
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');

  if (url.pathname === '/health') {
    // Abierto a otros sitios: el juego en GitHub Pages lo usa para despertar el servidor
    res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*', 'cache-control': 'no-store' });
    return res.end(JSON.stringify({
      ok: true,
      rooms: rooms.size,
      players: [...rooms.values()].reduce((n, r) => n + r.clients.size, 0),
    }));
  }

  if (url.pathname === '/stats') {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify(
      [...rooms.values()].map((r) => ({ code: r.code, players: r.clients.size })),
    ));
  }

  // Archivo estático, con / → apex-gt.html
  let rel = decodeURIComponent(url.pathname);
  if (rel === '/' || rel === '') rel = fs.existsSync(path.join(__dirname, 'index.html')) ? '/index.html' : '/apex-gt.html';
  const file = path.join(__dirname, path.normalize(rel).replace(/^([/\\])+/, ''));
  if (!file.startsWith(__dirname) || !MIME[path.extname(file).toLowerCase()]) {
    // Solo se sirve lo que el juego necesita: nada de .mjs ni de código del servidor.
    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
    return res.end('No encontrado');
  }
  fs.readFile(file, (err, data) => {
    if (err) {
      res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
      return res.end('No encontrado: ' + rel);
    }
    res.writeHead(200, {
      'content-type': MIME[path.extname(file)] || 'application/octet-stream',
      'cache-control': 'no-cache',
    });
    res.end(data);
  });
});

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname !== '/ws') {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    return socket.destroy();
  }

  const key = req.headers['sec-websocket-key'];
  if (!key) {
    socket.write('HTTP/1.1 400 Bad Request\r\n\r\n');
    return socket.destroy();
  }

  const roomCode = (url.searchParams.get('room') || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const asHost = url.searchParams.get('role') === 'host';

  if (!ROOM_RE.test(roomCode)) {
    socket.write('HTTP/1.1 400 Bad Request\r\n\r\n');
    return socket.destroy();
  }

  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
  );
  socket.setNoDelay(true);

  if (head && head.length) {
    // No se espera payload antes del handshake, pero por si acaso.
  }

  const client = {
    id: randomId(),
    socket,
    room: null,
    closed: false,
    buf: Buffer.alloc(0),
    frag: [],
    lastSeen: now(),
  };

  joinRoom(client, roomCode, asHost);

  socket.on('data', (chunk) => {
    if (client.closed) return;
    client.lastSeen = now();
    client.buf = Buffer.concat([client.buf, chunk]);

    let frames;
    try {
      frames = decodeFrames(client);
    } catch {
      return closeClient(client, 'bad-frame');
    }

    for (const f of frames) {
      if (f.opcode === 0x8) {
        try { socket.write(frameClose()); } catch {}
        return closeClient(client, 'client-close');
      }
      if (f.opcode === 0x9) {
        try { socket.write(framePong(f.payload)); } catch {}
        continue;
      }
      if (f.opcode === 0xA) continue;

      if (f.opcode === 0x0) {
        client.frag.push(f.payload);
        if (!f.fin) continue;
        const full = Buffer.concat(client.frag);
        client.frag = [];
        handleText(client, full.toString('utf8'));
        continue;
      }

      if (f.opcode === 0x1 || f.opcode === 0x2) {
        if (!f.fin) { client.frag = [f.payload]; continue; }
        handleText(client, f.payload.toString('utf8'));
      }
    }
  });

  socket.on('close', () => closeClient(client, 'socket-close'));
  socket.on('error', () => closeClient(client, 'socket-error'));
});

function handleText(client, text) {
  let msg;
  try {
    msg = JSON.parse(text);
  } catch {
    return;
  }
  if (!msg || typeof msg !== 'object') return;

  const room = client.room;
  if (!room) return;

  if (msg.t === 'data') {
    const payload = { t: 'data', from: client.id, d: msg.d };
    const raw = JSON.stringify(payload);
    const frame = frameText(raw);

    if (msg.to) {
      const dest = room.clients.get(msg.to);
      if (dest) { try { dest.socket.write(frame); } catch {} }
      return;
    }

    for (const other of room.clients.values()) {
      if (other.id === client.id) continue;
      try { other.socket.write(frame); } catch {}
    }
    return;
  }

  if (msg.t === 'ping') {
    send(client, { t: 'pong' });
    return;
  }

  if (msg.t === 'bye') {
    closeClient(client, 'bye');
  }
}

// Latido: cierra conexiones muertas para no dejar salas fantasma.
const heartbeat = setInterval(() => {
  const limit = now() - 45000;
  for (const room of rooms.values()) {
    for (const c of [...room.clients.values()]) {
      if (c.lastSeen < limit) {
        closeClient(c, 'timeout');
        continue;
      }
      try { c.socket.write(framePing()); } catch {}
    }
  }
}, 15000);
heartbeat.unref?.();

server.listen(PORT, HOST, () => {
  log(`Apex GT escuchando en http://localhost:${PORT}`);
  log(`WebSocket de salas en ws://localhost:${PORT}/ws`);
});

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => {
    log('cerrando…');
    for (const room of rooms.values()) {
      for (const c of room.clients.values()) {
        try { c.socket.end(frameClose()); } catch {}
      }
    }
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 1000);
  });
}
