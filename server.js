// Crownlands multiplayer server v2.  npm install && npm start   (Node 18+)
// - accounts (scrypt), invite-key islands (max 5), tile/combat relay
// - islands are saved to disk: players can switch their phone off and their realm stays on the map;
//   attacks against them are queued and delivered when they return
// - serves www/ (the game) and public/ (install page, optional crownlands.apk)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { WebSocketServer } = require('ws');
const remote = require('./remote');
const PORT = process.env.PORT || 8080, DATA = process.env.DATA_DIR || __dirname;
const DBF = path.join(DATA, 'accounts.json'), RF = path.join(DATA, 'rooms.json');
const ROOTS = [path.join(__dirname, 'www'), path.join(__dirname, 'public')];
const NCOL = 128, MAXP = 5, NT = 56 * 56 + NCOL * NCOL, DAY = 864e5, UNITS = ['inf', 'arc', 'cav', 'sie'];
// Conquest shield: after a realm loses land to another player, nobody may conquer from it for a while.
// Longer when the victim is offline (they cannot defend) and x3 when a city or stronghold fell.
const SHIELD_ONLINE = 8 * 60e3, SHIELD_OFFLINE = 25 * 60e3, SHIELD_BIG = 3;
const START = [null, [28, 43], [12, 15], [44, 15], [45, 33], [12, 34]];   // must match the client's start positions
fs.mkdirSync(DATA, { recursive: true });

let db = { users: {}, keys: {} };
try { db = JSON.parse(fs.readFileSync(DBF, 'utf8')); } catch (e) {}
const saveDb = () => { const j = JSON.stringify(db); fs.writeFile(DBF, j, () => {}); remote.push('accounts', j); };

/* ---------- rooms (persisted) ---------- */
const rooms = new Map();
function seed(r) { // every player's starting land belongs to them from the start, so it is protected too
  for (const p of r.pl) { const [sx, sy] = START[p.s];
    for (let y = -3; y <= 3; y++) for (let x = -3; x <= 3; x++) if (x * x + y * y <= 9) {
      const i = (sy + y) * 56 + sx + x; if (!r.tiles.has(i)) r.tiles.set(i, [p.s, x === 0 && y === 0 ? 1 : 0, 1, 0]); } }
}
try {
  for (const r of JSON.parse(fs.readFileSync(RF, 'utf8'))) {
    const room = { ...r, shield: r.shield || {}, tiles: new Map(r.tiles), pl: r.pl.map(p => ({ ...p, ws: null, tal: p.tal && 'inf' in p.tal ? { h: p.tal } : p.tal })) };
    if (room.started) seed(room); rooms.set(r.code, room);
  }
} catch (e) {}
let dirty = 0;
const persist = () => { if (dirty) return; dirty = setTimeout(() => { dirty = 0;
  const out = [...rooms.values()].map(r => ({ code: r.code, host: r.host, seed: r.seed, diff: r.diff, started: r.started, at: r.at, shield: r.shield,
    tiles: [...r.tiles], pl: r.pl.map(p => ({ s: p.s, u: p.u, last: p.last, save: p.save, tal: p.tal, pend: p.pend })) }));
  const j = JSON.stringify(out); remote.push('rooms', j);
  fs.writeFile(RF + '.tmp', j, e => { if (!e) fs.rename(RF + '.tmp', RF, () => {}); }); }, 3000); };

/* ---------- http ---------- */
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.json': 'application/json', '.svg': 'image/svg+xml',
  '.webmanifest': 'application/manifest+json', '.apk': 'application/vnd.android.package-archive' };
const srv = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  let p = decodeURIComponent(req.url.split('?')[0]); if (p.endsWith('/')) p += 'index.html';
  const tryRoot = i => {
    if (i >= ROOTS.length) { res.writeHead(404); return res.end('Not found'); }
    const f = path.join(ROOTS[i], path.normalize(p));
    if (!f.startsWith(ROOTS[i])) { res.writeHead(403); return res.end(); }
    fs.readFile(f, (e, d) => {
      if (e) return tryRoot(i + 1);
      const h = { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' };
      if (f.endsWith('.apk')) h['Content-Disposition'] = 'attachment; filename="crownlands.apk"';
      res.writeHead(200, h); res.end(d);
    });
  };
  tryRoot(0);
});
const wss = new WebSocketServer({ server: srv, maxPayload: 1048576 });

/* ---------- helpers ---------- */
const hash = (p, s) => crypto.scryptSync(p, s, 32).toString('hex');
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const dname = u => (db.users[u] || {}).name || u;
const num = (v, a, b) => Number.isInteger(v) && v >= a && v <= b;
const touch = r => { r.at = Date.now(); persist(); };

function roomMsg(r, p, init) {
  const o = { t: 'room', code: r.code, host: dname(r.host), isHost: r.host === p.u, slot: p.s, seed: r.seed, diff: r.diff, started: r.started,
    pl: r.pl.map(q => ({ s: q.s, n: dname(q.u), on: !!q.ws })) };
  if (init && r.started) o.init = { shield: r.shield || {}, save: p.save || null, snap: [...r.tiles].map(([i, v]) => [i, ...v]), pend: p.pend || [],
    tal: Object.fromEntries(r.pl.filter(q => q !== p && q.tal).map(q => [q.s, q.tal])) };
  return o;
}
const bc = r => r.pl.forEach(p => send(p.ws, roomMsg(r, p)));
const others = (r, ws, o) => r.pl.forEach(p => { if (p.ws && p.ws !== ws) send(p.ws, o); });

function attach(ws) { // connect a signed-in user to the island they used most recently
  let best = null, bp = null;
  for (const r of rooms.values()) { const p = r.pl.find(q => q.u === ws.u); if (p && (!best || (p.last || 0) > (bp.last || 0))) { best = r; bp = p; } }
  if (!best) return;
  if (bp.ws && bp.ws !== ws) try { bp.ws.close(); } catch (e) {}
  bp.ws = ws; ws.room = best; bp.last = Date.now();
  send(ws, roomMsg(best, bp, true)); bp.pend = []; touch(best); bc(best);
}
function leave(ws) {
  const r = ws.room; if (!r) return; ws.room = null;
  const i = r.pl.findIndex(p => p.u === ws.u); if (i < 0) return;
  if (r.started) { r.pl[i].ws = null; bc(r); return; }          // started: your realm stays on the island
  r.pl.splice(i, 1);
  if (!r.pl.length) { rooms.delete(r.code); persist(); return; }
  if (r.host === ws.u) r.host = r.pl[0].u;
  touch(r); bc(r);
}
const newCode = () => { const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; let c; do { c = Array.from({ length: 6 }, () => A[crypto.randomInt(A.length)]).join(''); } while (rooms.has(c)); return c; };

function auth(ws, m) {
  if (m.t === 'resume') {
    const u = db.keys[m.k]; if (!u || !db.users[u]) return send(ws, { t: 'auth', ok: 0, err: 'Session expired' });
    ws.u = u; send(ws, { t: 'auth', ok: 1, name: dname(u), key: m.k }); return attach(ws);
  }
  const name = String(m.u || '').trim(), pw = String(m.p || ''), id = name.toLowerCase();
  if (!/^[A-Za-z0-9_]{3,16}$/.test(name)) return send(ws, { t: 'auth', ok: 0, err: 'Name: 3-16 letters, digits or _' });
  if (pw.length < 6 || pw.length > 64) return send(ws, { t: 'auth', ok: 0, err: 'Password: 6-64 characters' });
  if (m.m === 'reg') {
    if (db.users[id]) return send(ws, { t: 'auth', ok: 0, err: 'Name already taken' });
    const salt = crypto.randomBytes(16).toString('hex'); db.users[id] = { name, salt, h: hash(pw, salt) };
  } else {
    const u = db.users[id];
    if (!u || !crypto.timingSafeEqual(Buffer.from(u.h, 'hex'), Buffer.from(hash(pw, u.salt), 'hex'))) return send(ws, { t: 'auth', ok: 0, err: 'Wrong name or password' });
  }
  const key = crypto.randomBytes(18).toString('hex'); db.keys[key] = id; saveDb();
  ws.u = id; send(ws, { t: 'auth', ok: 1, name: dname(id), key }); attach(ws);
}

function handle(ws, m) {
  if (m.t === 'auth' || m.t === 'resume') return auth(ws, m);
  if (!ws.u) return;
  const r = ws.room, me = r && r.pl.find(p => p.u === ws.u);
  switch (m.t) {
    case 'create': {
      leave(ws); const code = newCode();
      const nr = { code, host: ws.u, seed: crypto.randomInt(1, 2 ** 31), diff: num(m.diff, 0, 4) ? m.diff : 1, started: false, at: Date.now(), shield: {}, tiles: new Map(), pl: [{ s: 1, u: ws.u, ws, last: Date.now() }] };
      rooms.set(code, nr); ws.room = nr; persist(); return bc(nr);
    }
    case 'join': {
      const x = rooms.get(String(m.code || '').toUpperCase().trim());
      if (!x) return send(ws, { t: 'err', m: 'No island with that key' });
      if (!x.pl.some(p => p.u === ws.u)) {
        if (x.started) return send(ws, { t: 'err', m: 'That game has already started' });
        if (x.pl.length >= MAXP) return send(ws, { t: 'err', m: 'Island is full (5/5)' });
        leave(ws); const s = [1, 2, 3, 4, 5].find(n => !x.pl.some(p => p.s === n)); x.pl.push({ s, u: ws.u, ws, last: Date.now() });
      } else x.pl.find(p => p.u === ws.u).last = Date.now();
      touch(x); return attach(ws);
    }
    case 'start': if (r && !r.started && r.host === ws.u && r.pl.length >= 2) { r.started = true; seed(r); touch(r); bc(r); } else if (r && r.host === ws.u) send(ws, { t: 'err', m: 'Need at least 2 players' }); return;
    case 'leave': return leave(ws);
    case 'save': // the player's own realm (coins, army, research...) so it survives switching the phone off
      if (r && r.started && me && m.d && typeof m.d === 'object' && JSON.stringify(m.d).length <= 600000) { me.save = m.d; touch(r); } return;
    case 'tiles': { // [index, owner(global slot), building, level, walls, conquest?]  conquest = 1 + previous building + 20 * previous owner
      if (!r || !r.started || !me || !Array.isArray(m.d)) return;
      const ok = [], bad = [], conq = new Map(), now = Date.now(); r.shield = r.shield || {}; let blocked = 0;
      for (const e of m.d.slice(0, 400)) {
        if (!Array.isArray(e)) continue; const [i, o, b, l, w, cq] = e;
        if (!num(i, 0, NT - 1) || o !== me.s || !num(b, 0, 12) || !num(l, 0, 40) || !num(w || 0, 0, 3) || !num(cq || 0, 0, 200)) continue;
        const cur = r.tiles.get(i);
        let po = cur && cur[0] || 0, pb = cur ? cur[1] : 0;
        if (!po && cq) { po = Math.floor((cq - 1) / 20); pb = (cq - 1) % 20; }
        if (po && po !== me.s) {                               // this is a conquest
          if (!cq) { bad.push([i, ...cur]); continue; }        // lost a race: resync the sender
          if (po >= 1 && po <= 5 && (r.shield[po] || 0) > now) { bad.push([i, ...(cur || [po, pb, 1, 0])]); blocked = po; continue; }
          conq.set(po, Math.max(conq.get(po) || 0, pb === 1 || pb === 3 ? SHIELD_BIG : 1));
        }
        r.tiles.set(i, [o, b, l, w || 0]); ok.push([i, o, b, l, w || 0]);
      }
      for (const [po, mult] of conq) {                         // start (or extend) the victim's shield
        const v = r.pl.find(p => p.s === po); if (!v) continue; const until = now + (v && v.ws ? SHIELD_ONLINE : SHIELD_OFFLINE) * mult;
        if (until > (r.shield[po] || 0)) { r.shield[po] = until; r.pl.forEach(p => send(p.ws, { t: 'shield', s: po, until })); }
      }
      if (ok.length) { others(r, ws, { t: 'tiles', d: ok }); touch(r); }
      if (bad.length) send(ws, { t: 'tiles', d: bad });
      if (blocked) send(ws, { t: 'err', m: '\u{1F6E1} That realm is shielded for ' + Math.ceil((r.shield[blocked] - now) / 6e4) + ' more min' });
      return;
    }
    case 'tally': if (r && r.started && me && m.u && typeof m.u === 'object') {
      const u = {}; for (const k of UNITS) u[k] = Math.max(0, Math.min(9999, +m.u[k] || 0)); const rr = m.r === 'c' ? 'c' : 'h';
      me.tal = me.tal && !('inf' in me.tal) ? me.tal : {}; me.tal[rr] = u; others(r, ws, { t: 'tally', from: me.s, r: rr, u }); persist();
    } return;
    case 'hit': case 'siege': { // relay to the target; queue it if they are offline
      if (!r || !r.started || !me || !num(m.to, 1, 5) || !num(m.i, 0, NT - 1)) return;
      const to = r.pl.find(p => p.s === m.to); if (!to) return;
      const ld = {}; if (m.ld) for (const k of UNITS) ld[k] = Math.max(0, Math.min(999, Math.floor(+m.ld[k] || 0)));
      const o = { t: m.t, from: me.s, i: m.i, ld, win: !!m.win };
      if (to.ws) send(to.ws, o); else { (to.pend = to.pend || []).push(o); if (to.pend.length > 40) to.pend.shift(); touch(r); }
      return;
    }
  }
}

wss.on('connection', ws => {
  ws.alive = true; ws.on('pong', () => ws.alive = true);
  ws.on('message', raw => { let m; try { m = JSON.parse(raw); } catch (e) { return; } if (m && typeof m === 'object') try { handle(ws, m); } catch (e) { console.error(e); } });
  ws.on('close', () => { const r = ws.room; if (!r) return; const p = r.pl.find(q => q.ws === ws); if (p) { p.ws = null; bc(r); } });
});
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); }), 30000);
setInterval(() => { const n = Date.now(); for (const [c, r] of rooms) if (n - r.at > (r.started ? 30 * DAY : 2 * 36e5) && !r.pl.some(p => p.ws)) rooms.delete(c); persist(); }, 6e5);
srv.listen(PORT, () => console.log('Crownlands server on http://localhost:' + PORT + '  (data: ' + DATA + ')'));
