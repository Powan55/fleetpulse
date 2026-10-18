// Dev-only stand-in for the api (also serves dist/): node dev/mock-server.mjs [--grow]
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const PORT = Number(process.env.PORT ?? 8081);
const STATES = ['HEALTHY', 'DEGRADED', 'FAILED', 'OFFLINE', 'RECOVERING'];
const grow = process.argv.includes('--grow');
const now = () => new Date().toISOString();
const devices = new Map();
const add = (i) => {
  const deviceId = `dev-${String(i).padStart(4, '0')}`;
  const d = { deviceId, state: 'HEALTHY', stateVersion: 1, stateChangedAt: now(), lastSeenAt: now() };
  devices.set(deviceId, d);
  return d;
};
let next = grow ? 1 : 1001;
for (let i = 1; i < next; i++) add(i);

const summary = () => {
  const byState = Object.fromEntries(STATES.map((s) => [s, 0]));
  for (const d of devices.values()) byState[d.state]++;
  return { total: devices.size, byState, asOf: now() };
};
const clients = new Set();
let changed = new Map();

setInterval(() => {
  for (let k = 0; k < 20; k++) {
    if (grow && next <= 1000) {
      const d = add(next++);
      changed.set(d.deviceId, { ...d });
    } else {
      const all = [...devices.values()];
      const d = all[Math.floor(Math.random() * all.length)];
      d.state = STATES[Math.floor(Math.random() * STATES.length)];
      d.stateVersion++;
      d.stateChangedAt = d.lastSeenAt = now();
      changed.set(d.deviceId, { ...d });
    }
  }
}, 1000);

setInterval(() => {
  if (!changed.size) return;
  const items = [...changed.values()];
  changed = new Map();
  for (const res of clients) {
    res.write(`event: devices\ndata: ${JSON.stringify({ items })}\n\n`);
    res.write(`event: fleet.summary\ndata: ${JSON.stringify(summary())}\n\n`);
  }
}, 250);

http
  .createServer((req, res) => {
    const url = new URL(req.url, 'http://x');
    const json = (o) => {
      const body = JSON.stringify(o);
      res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) });
      res.end(body);
    };
    if (url.pathname === '/api/v1/fleet/summary') return json(summary());
    if (url.pathname === '/api/v1/devices') {
      const items = [...devices.values()].sort((a, b) => (a.deviceId < b.deviceId ? -1 : 1));
      return json({ items, page: 0, size: 1000, totalItems: items.length });
    }
    if (url.pathname === '/api/v1/stream') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
      res.write(`retry: 2000\nevent: hello\ndata: ${JSON.stringify({ serverInstance: 'mock', at: now() })}\n\n`);
      clients.add(res);
      req.on('close', () => clients.delete(res));
      return;
    }
    const dist = fileURLToPath(new URL('../dist', import.meta.url));
    const file = join(dist, url.pathname === '/' ? 'index.html' : url.pathname.replace(/\.\./g, ''));
    const types = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };
    readFile(file).then(
      (b) => {
        res.writeHead(200, { 'Content-Type': types[extname(file)] ?? 'application/octet-stream' });
        res.end(b);
      },
      () => {
        res.statusCode = 404;
        res.end();
      },
    );
  })
  .listen(PORT, '127.0.0.1', () => console.log(`mock api on ${PORT}${grow ? ' (growing)' : ''}`));
