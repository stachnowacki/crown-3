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

/* ---------- data safety: validation, atomic writes, backups, recovery ---------- */
const zlib = require('zlib'), fsp = fs.promises;
const BK = path.join(DATA, 'backups'), SCHEMA = 1;
const log = (l, m) => (l === 'error' || l === 'warn' ? console.error : console.log)(`[${new Date().toISOString()}] [${l}] ${m}`);
const okAcc = s => { try { const o = JSON.parse(s); return !!(o && o.users && typeof o.users === 'object' && o.keys && typeof o.keys === 'object'); } catch (e) { return false; } };
const okRooms = s => { try { const o = JSON.parse(s); return Array.isArray(o) && o.every(r => r && typeof r.code === 'string' && Array.isArray(r.tiles) && Array.isArray(r.pl)); } catch (e) { return false; } };
const KINDS = { accounts: { file: DBF, ok: okAcc }, rooms: { file: RF, ok: okRooms } };
fs.mkdirSync(BK, { recursive: true });
const wq = {}; const enqueue = (k, fn) => (wq[k] = (wq[k] || Promise.resolve()).then(fn).catch(e => log('error', 'write queue: ' + e.message)));
async function atomicWrite(kind, str) { const { file, ok } = KINDS[kind];
  if (!ok(str)) { log('error', `refusing to save invalid ${kind} data (previous file kept)`); return false; }
  try { await fsp.writeFile(file + '.tmp', str);
    try { const cur = await fsp.readFile(file, 'utf8'); if (cur !== str && ok(cur)) await fsp.copyFile(file, file + '.prev'); } catch (e) {}
    await fsp.rename(file + '.tmp', file); return true; }
  catch (e) { log('error', `save failed for ${kind}: ${e.message}`); return false; } }
const writeData = (kind, str) => enqueue(kind, () => atomicWrite(kind, str));
const lastBk = {};
const bkMeta = (kind, str) => { try { const o = JSON.parse(str); return kind === 'accounts' ? { users: Object.keys(o.users).length } : { rooms: o.length, codes: o.map(r => r.code), players: o.reduce((a, r) => a + r.pl.length, 0) }; } catch (e) { return {}; } };
async function backup(kind, str, why) {
  if (!KINDS[kind].ok(str)) return log('error', `backup of ${kind} skipped: data invalid`);
  const h = crypto.createHash('sha1').update(str).digest('hex'); if (why === 'scheduled' && lastBk[kind] === h) return;
  try { const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const z = zlib.gzipSync(JSON.stringify({ schema: SCHEMA, kind, at: Date.now(), why, ...bkMeta(kind, str), data: str }));
    const f = path.join(BK, `${kind}-${stamp}.json.gz`); await fsp.writeFile(f + '.tmp', z); await fsp.rename(f + '.tmp', f);
    lastBk[kind] = h; log('info', `backup ${kind} ok (${why}, ${z.length} bytes)`);
    const names = (await fsp.readdir(BK)).filter(n => n.startsWith(kind + '-') && n.endsWith('.json.gz')).sort().reverse(), keep = new Set(names.slice(0, 12)), days = new Set();
    for (const n of names) { const d = n.slice(kind.length + 1, kind.length + 11); if (!days.has(d) && days.size < 21) { days.add(d); keep.add(n); } }
    for (const n of names) if (!keep.has(n)) await fsp.unlink(path.join(BK, n)).catch(() => {});
    remote.push(kind + '.bk' + (Math.floor(Date.now() / 36e5) % 3), str); // rolling copies outside the app disk (when the Upstash mirror is enabled)
  } catch (e) { log('error', `backup ${kind} failed: ${e.message}`); } }
async function recoverData() {
  log('info', 'data directory: ' + DATA + (remote.enabled ? ' | remote mirror: ON (Upstash)' : ' | remote mirror: OFF'));
  if (!remote.enabled) log('warn', 'No UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN set. Data survives deployments only if DATA_DIR points to a persistent disk. Without one, a Render deploy wipes accounts and islands.');
  for (const kind of Object.keys(KINDS)) { const { file, ok } = KINDS[kind];
    let cur = null; try { cur = fs.readFileSync(file, 'utf8'); } catch (e) {}
    if (cur && ok(cur)) continue;
    if (cur) { const d = `${file}.corrupt-${Date.now()}`; try { fs.writeFileSync(d, cur); log('error', `${kind}: file is damaged, copy kept as ${path.basename(d)}`); } catch (e) {} }
    const c = [];
    if (remote.enabled) { try { c.push(['remote mirror', await remote.pull(kind)]); } catch (e) { log('error', 'remote pull failed: ' + e.message); } }
    try { c.push(['previous save', fs.readFileSync(file + '.prev', 'utf8')]); } catch (e) {}
    try { for (const n of fs.readdirSync(BK).filter(n => n.startsWith(kind + '-') && n.endsWith('.json.gz')).sort().reverse()) { try { c.push(['backup ' + n, JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(BK, n))).toString()).data]); } catch (e) {} } } catch (e) {}
    if (remote.enabled) for (let i = 0; i < 3; i++) { try { c.push(['remote backup ' + i, await remote.pull(kind + '.bk' + i)]); } catch (e) {} }
    const hit = c.find(x => x[1] && ok(x[1]));
    if (hit) { fs.writeFileSync(file, hit[1]); log('warn', `${kind}: RECOVERED from ${hit[0]}`); }
    else log(cur ? 'error' : 'info', cur ? `${kind}: no valid copy found; damaged file preserved, starting empty` : `${kind}: no data yet (new server)`); } }
process.on('uncaughtException', e => log('error', 'uncaught: ' + (e && e.stack || e)));
process.on('unhandledRejection', e => log('error', 'unhandled rejection: ' + (e && e.stack || e)));
(async () => { await recoverData();

let db = { users: {}, keys: {} };
try { db = JSON.parse(fs.readFileSync(DBF, 'utf8')); } catch (e) {}
if (!db.v) db.v = SCHEMA;   // explicit data version; unknown fields are always preserved
const saveDb = () => { const j = JSON.stringify(db); if (!okAcc(j)) return log('error', 'account data invalid, not saved'); writeData('accounts', j); remote.push('accounts', j); };

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
const snapshotRooms = () => [...rooms.values()].map(r => ({ code: r.code, host: r.host, seed: r.seed, diff: r.diff, started: r.started, at: r.at, shield: r.shield,
    tiles: [...r.tiles], mkt: r.mkt, chat: (r.chat || []).slice(-80), cid: r.cid || 0,
    pl: r.pl.map(p => ({ s: p.s, u: p.u, last: p.last, save: p.save, tal: p.tal, pend: p.pend, sc: p.sc, fx: p.fx })) }));
const persist = () => { if (dirty) return; dirty = setTimeout(() => { dirty = 0; const j = JSON.stringify(snapshotRooms()); if (!okRooms(j)) return log('error', 'room snapshot invalid, not saved'); remote.push('rooms', j); writeData('rooms', j); }, 3000); };

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

/* ---------- shared market (one per island): everyone trades against the same prices ---------- */
const MRES = ['wood', 'stone', 'silver', 'gold', 'diamond'], MIC = { wood: '🪵', stone: '🪨', silver: '🥈', gold: '🥇', diamond: '💎' };
const MB = { wood: 3, stone: 5, silver: 22, gold: 65, diamond: 220 }, MV = { wood: .6, stone: .7, silver: 1, gold: 1.2, diamond: 1.5 };
const MI = { wood: .0015, stone: .002, silver: .006, gold: .015, diamond: .04 }, MM0 = { wood: 600, stone: 400, silver: 150, gold: 60, diamond: 20 };
const MD = { wood: .5, stone: .4, silver: .12, gold: .05, diamond: .015 };
const cl = (v, a, b) => Math.max(a, Math.min(b, v));
function mkNew() { const m = { price: {}, mom: {}, mk: {}, hist: {}, news: '', feed: [], tk: 0 }; for (const r of MRES) { m.price[r] = MB[r]; m.mom[r] = 0; m.mk[r] = MM0[r]; m.hist[r] = [MB[r]]; } return m; }
function mkStep(m) {
  m.tk++;
  for (const r of MRES) {
    const dem = MD[r] * Math.pow(MB[r] / m.price[r], .15); m.mk[r] = cl(m.mk[r] + MD[r] - dem, 0, MM0[r] * 3);
    const sh = Math.max(.15, m.mk[r] / MM0[r]), tg = cl(MB[r] * Math.pow(1 / sh, .6), MB[r] * .35, MB[r] * 2.8);
    m.mom[r] = m.mom[r] * .9 + (Math.random() * 2 - 1) * .3 * MV[r];
    m.price[r] = cl(m.price[r] + (tg - m.price[r]) * .06 + m.price[r] * m.mom[r] * .004, MB[r] * .35, MB[r] * 2.8);
  }
  if (Math.random() < .004) { const r = MRES[Math.floor(Math.random() * 5)], up = Math.random() < .5; m.mk[r] = cl(m.mk[r] * (up ? .7 : 1.5), 0, MM0[r] * 3);
    m.news = up ? MIC[r] + ' A shortage of ' + r + ' is driving demand up. Good time to sell!' : MIC[r] + ' ' + r[0].toUpperCase() + r.slice(1) + ' floods the market. Time to buy?'; }
  if (m.tk % 2 == 0) for (const r of MRES) { const h = m.hist[r]; h.push(+m.price[r].toFixed(3)); if (h.length > 40) h.shift(); }
}
const mkPub = m => { const o = { t: 'mkt', price: {}, mk: {}, hist: m.hist, news: m.news, feed: m.feed }; for (const r of MRES) { o.price[r] = +m.price[r].toFixed(3); o.mk[r] = Math.round(m.mk[r]); } return o; };
setInterval(() => { for (const r of rooms.values()) { if (!r.started || !r.pl.some(p => p.ws)) continue; if (!r.mkt) r.mkt = mkNew(); mkStep(r.mkt); if (r.mkt.tk % 2 == 0) { const o = mkPub(r.mkt); r.pl.forEach(p => send(p.ws, o)); } } }, 1000);

/* ---------- helpers ---------- */
const hash = (p, s) => crypto.scryptSync(p, s, 32).toString('hex');
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const dname = u => (db.users[u] || {}).name || u;
const num = (v, a, b) => Number.isInteger(v) && v >= a && v <= b;
const touch = r => { r.at = Date.now(); persist(); };

function roomMsg(r, p, init) {
  const o = { t: 'room', code: r.code, host: dname(r.host), isHost: r.host === p.u, slot: p.s, seed: r.seed, diff: r.diff, started: r.started,
    pl: r.pl.map(q => ({ s: q.s, n: dname(q.u), on: !!q.ws, sc: q.sc ? q.sc.s : 0, sp: q.sc ? q.sc.p : null, fx: q.fx || null })) };
  if (init && r.started) { if (!r.mkt) r.mkt = mkNew(); o.init = { mkt: mkPub(r.mkt), chat: (r.chat || []).slice(-50), shield: r.shield || {}, save: p.save || null, snap: [...r.tiles].map(([i, v]) => [i, ...v]), pend: p.pend || [],
    tal: Object.fromEntries(r.pl.filter(q => q !== p && q.tal).map(q => [q.s, q.tal])) }; }
  return o;
}
const bc = r => r.pl.forEach(p => send(p.ws, roomMsg(r, p)));
const others = (r, ws, o) => r.pl.forEach(p => { if (p.ws && p.ws !== ws) send(p.ws, o); });

function attach(ws) { // connect a signed-in user to the island they used most recently
  let best = null, bp = null;
  for (const r of rooms.values()) { const p = r.pl.find(q => q.u === ws.u); if (p && (!best || (p.last || 0) > (bp.last || 0))) { best = r; bp = p; } }
  if (!best) return;
  if (bp.ws && bp.ws !== ws) try { bp.ws.close(); } catch (e) {}
  bp.ws = ws; ws.room = best; bp.last = Date.now(); log('info', `player reconnected: room ${best.code} slot ${bp.s}`);
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
    ws.u = u; send(ws, { t: 'auth', ok: 1, name: dname(u), key: m.k }); if (m.lb) return; return attach(ws);
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
    case 'trade': { // a trade at the island's shared market
      if (!r || !r.started || !me) return; if (!r.mkt) r.mkt = mkNew(); const m0 = r.mkt, res = String(m.r), n = Math.floor(+m.n);
      if (!MRES.includes(res) || !(n >= 1 && n <= 20000) || (m.side !== 'buy' && m.side !== 'sell')) return;
      const mid = m0.price[res];
      if (m.side === 'sell') { m0.mk[res] = Math.min(MM0[res] * 3, m0.mk[res] + n); m0.price[res] = cl(mid * (1 - Math.min(.3, n * MI[res] * .5)), MB[res] * .35, MB[res] * 2.8); }
      else { m0.mk[res] = Math.max(0, m0.mk[res] - n); m0.price[res] = cl(mid * (1 + Math.min(.25, n * MI[res] * .4)), MB[res] * .35, MB[res] * 2.8); }
      m0.feed.push({ n: dname(me.u), s: m.side, r: res, q: n, at: Date.now() }); if (m0.feed.length > 8) m0.feed.shift();
      send(ws, { t: 'tr', id: m.id, r: res, side: m.side, n, mid: +mid.toFixed(3) }); const o = mkPub(m0); r.pl.forEach(p => send(p.ws, o)); touch(r); return;
    }
    case 'score': { // my island's progress score (shown in the island ranking and the world ranking)
      if (!r || !r.started || !me || !m.s || typeof m.s !== 'object') return; const now = Date.now(); if (me.scAt && now - me.scAt < 8000) return; me.scAt = now;
      const s = cl(Math.floor(+m.s.s) || 0, 0, 2e6), p = Array.isArray(m.s.p) ? m.s.p.slice(0, 10).map(v => cl(Math.floor(+v) || 0, 0, 100)) : null;
      me.sc = { s, p, d: num(m.s.d, 0, 4) ? m.s.d : 1, at: now }; const u = db.users[ws.u]; if (u) { u.is = { s, p, d: me.sc.d, at: now }; saveDb(); } bc(r); touch(r); return;
    }
    case 'gscore': { // an offline (solo) island of a logged-in player
      const u = db.users[ws.u]; if (!u || !m.s || typeof m.s !== 'object') return; const now = Date.now(); if (ws.gsAt && now - ws.gsAt < 8000) return; ws.gsAt = now;
      const s = cl(Math.floor(+m.s.s) || 0, 0, 2e6), p = Array.isArray(m.s.p) ? m.s.p.slice(0, 10).map(v => cl(Math.floor(+v) || 0, 0, 100)) : null;
      u.gs = { s, p, d: num(m.s.d, 0, 4) ? m.s.d : 1, at: now }; saveDb(); return;
    }
    case 'lb': { // world ranking: every account's best island
      const rows = []; for (const id in db.users) { const u = db.users[id], a = u.gs, b = u.is; const best = !a ? (b ? [b, 'online'] : null) : !b ? [a, 'solo'] : (b.s > a.s ? [b, 'online'] : [a, 'solo']); if (best && best[0].s > 0) rows.push({ id, n: u.name || id, s: best[0].s, m: best[1], p: best[0].p, d: best[0].d }); }
      rows.sort((x, y) => y.s - x.s); const mine = rows.findIndex(x => x.id === ws.u);
      send(ws, { t: 'lb', top: rows.slice(0, 50).map(({ id, ...q }) => q), total: rows.length, me: mine < 0 ? null : { rank: mine + 1, s: rows[mine].s, m: rows[mine].m } }); return;
    }
    case 'fx': { // my look (picture, nickname effect, realm colour, building aura) so everyone in the room sees it
      if (!r || !me || !m.fx || typeof m.fx !== 'object') return; const now = Date.now(); if (me.fxAt && now - me.fxAt < 1500) return; me.fxAt = now;
      const fx = {}; for (const [k, mx] of [['pf', 13], ['nf', 9], ['rc', 9], ['au', 6]]) { const q = m.fx[k]; if (Number.isInteger(q) && q >= 0 && q < mx) fx[k] = q; }
      me.fx = Object.keys(fx).length ? fx : null; bc(r); touch(r); return;
    }
    case 'chat': {
      if (!r || !r.started || !me) return; const now = Date.now(); if (me.chAt && now - me.chAt < 900) return; me.chAt = now;
      const text = String(m.m || '').replace(/[\u0000-\u001f]/g, ' ').trim().slice(0, 200); if (!text) return;
      let loc = null; if (m.loc && typeof m.loc === 'object' && (m.loc.r === 'h' || m.loc.r === 'c') && num(m.loc.x, 0, 127) && num(m.loc.y, 0, 127)) loc = { r: m.loc.r, x: m.loc.x, y: m.loc.y, t: String(m.loc.t || '').slice(0, 40) };
      const msg = { id: r.cid = (r.cid || 0) + 1, s: me.s, n: dname(me.u), m: text, at: now }; if (loc) msg.loc = loc;
      if (m.fx && typeof m.fx === 'object') { const fx = {}; for (const [k, mx] of [['pf', 13], ['nf', 9], ['mf', 8]]) { const q = m.fx[k]; if (Number.isInteger(q) && q >= 0 && q < mx) fx[k] = q; } if (Object.keys(fx).length) msg.fx = fx; }
      if (me.fx) { msg.fx = msg.fx || {}; for (const k of ['pf', 'nf']) if (me.fx[k] != null) msg.fx[k] = me.fx[k]; }
      (r.chat = r.chat || []).push(msg); if (r.chat.length > 120) r.chat.splice(0, r.chat.length - 120);
      r.pl.forEach(p => send(p.ws, { t: 'chat', msg })); touch(r); return;
    }
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
  ws.on('close', () => { const r = ws.room; if (!r) return; const p = r.pl.find(q => q.ws === ws); if (p) { p.ws = null; bc(r); log('info', `player disconnected (no penalty, island kept): room ${r.code} slot ${p.s}`); } });
});
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); }), 30000);
setInterval(() => { const n = Date.now(); for (const [c, r] of rooms) if (n - r.at > (r.started ? 30 * DAY : 2 * 36e5) && !r.pl.some(p => p.ws)) rooms.delete(c); persist(); }, 6e5);
srv.listen(PORT, () => console.log('Crownlands server on http://localhost:' + PORT + '  (data: ' + DATA + ')'));

// Backups: at start-up, every 15 minutes when something changed, and on shutdown.
const runBackups = why => { backup('accounts', JSON.stringify(db), why); backup('rooms', JSON.stringify(snapshotRooms()), why); };
setTimeout(() => runBackups('boot'), 8000); setInterval(() => runBackups('scheduled'), 15 * 6e4);
let closing = false;
async function shutdown(sig) { if (closing) return; closing = true; log('info', sig + ': saving everything before exit');
  try { const a = JSON.stringify(db), r = JSON.stringify(snapshotRooms()); await writeData('accounts', a); await writeData('rooms', r); remote.push('accounts', a); remote.push('rooms', r); await backup('accounts', a, 'shutdown'); await backup('rooms', r, 'shutdown'); await remote.flushAll(); log('info', 'shutdown save complete'); } catch (e) { log('error', 'shutdown save failed: ' + e.message); }
  process.exit(0); }
process.on('SIGTERM', () => shutdown('SIGTERM')); process.on('SIGINT', () => shutdown('SIGINT'));
})().catch(e => { console.error('FATAL start-up error', e); process.exit(1); });
