// Crownlands multiplayer server v3.  npm install && npm start   (Node 18+)
// - accounts (scrypt), invite-key islands (max 5), tile/combat relay
// - islands are saved to disk and (optionally) mirrored to Upstash Redis, so realms survive restarts,
//   redeploys and crashes; attacks against offline players are queued and delivered when they return
// - automatic, verified, compressed backups with retention; admin API to list, create and restore them
// - serves www/ (the game) and public/ (install page, optional crownlands.apk)
//
// Environment (all optional):
//   PORT                      HTTP port (Render sets this)
//   DATA_DIR                  folder for accounts/islands/backups. On Render point it at a persistent disk
//                             (e.g. /var/data). Without a disk or Upstash, a redeploy wipes the data.
//   UPSTASH_REDIS_REST_URL    \ cloud copy of all data (survives any redeploy, even without a disk)
//   UPSTASH_REDIS_REST_TOKEN  /
//   ADMIN_TOKEN               enables /admin/* (backup list, manual backup, restore, export)
//   BACKUP_EVERY_MIN          scheduled backup interval when something changed (default 15)
//   ROOM_ARCHIVE_DAYS         started islands idle this long are archived, never deleted (default 120)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), zlib = require('zlib');
const { WebSocketServer } = require('ws');
const remote = require('./remote');
const fsp = fs.promises;
const PORT = process.env.PORT || 8080, DATA = process.env.DATA_DIR || __dirname;
const DBF = path.join(DATA, 'accounts.json'), RF = path.join(DATA, 'rooms.json');
const BK = path.join(DATA, 'backups'), ARCH = path.join(DATA, 'archive');
const ROOTS = [path.join(__dirname, 'www'), path.join(__dirname, 'public')];
const NCOL = 128, MAXP = 5, NT = 56 * 56 + NCOL * NCOL, DAY = 864e5, UNITS = ['inf', 'arc', 'cav', 'sie'];
// Conquest shield: after a realm loses land to another player, nobody may conquer from it for a while.
// Longer when the victim is offline (they cannot defend) and x3 when a city or stronghold fell.
const SHIELD_ONLINE = 8 * 60e3, SHIELD_OFFLINE = 25 * 60e3, SHIELD_BIG = 3;
const START = [null, [28, 43], [12, 15], [44, 15], [45, 33], [12, 34]];   // must match the client's start positions
const SCHEMA = 3, SCORE_MAX = 1500;
const BACKUP_EVERY = Math.max(5, +process.env.BACKUP_EVERY_MIN || 15) * 60e3, ARCHIVE_AFTER = Math.max(30, +process.env.ROOM_ARCHIVE_DAYS || 120) * DAY;
for (const d of [DATA, BK, ARCH]) fs.mkdirSync(d, { recursive: true });

/* ---------- logging (never passwords, hashes or session keys) ---------- */
const log = (l, m) => (l === 'error' || l === 'warn' ? console.error : console.log)(`[${new Date().toISOString()}] [${l}] ${m}`);
const quiet = {}; const logOnce = (key, l, m, ms = 60e3) => { const n = Date.now(); if ((quiet[key] || 0) > n) return; quiet[key] = n + ms; log(l, m); };

/* ---------- data safety: validation, atomic writes, backups, recovery ---------- */
const okAcc = s => { try { const o = JSON.parse(s); return !!(o && o.users && typeof o.users === 'object' && o.keys && typeof o.keys === 'object'); } catch (e) { return false; } };
const okRooms = s => { try { const o = JSON.parse(s); return Array.isArray(o) && o.every(r => r && typeof r.code === 'string' && Array.isArray(r.tiles) && Array.isArray(r.pl)); } catch (e) { return false; } };
const KINDS = { accounts: { file: DBF, ok: okAcc }, rooms: { file: RF, ok: okRooms } };
const sha1 = s => crypto.createHash('sha1').update(s).digest('hex');

/* ---------- schema migration (persistent data is kept separate from game code) ----------
   Every migration is incremental and idempotent: running it twice is a no-op, and fields the
   current server does not recognise are copied through untouched. A copy of the file is kept
   before a migration rewrites it (".premigrate-<time>"), and a backup is made as well. */
function migrateAccounts(o) {
  const from = o.v == null ? 1 : +o.v;
  if (from < 2) {
    for (const id in o.users) { const u = o.users[id]; if (!u || typeof u !== 'object') continue;
      if (typeof u.name !== 'string' || !u.name) u.name = id;
      if (typeof u.created !== 'number') u.created = Date.now(); }
  }
  if (from < 3) {   // v3: leaderboard scores are an index (max ~1300); keep old values but clamp absurd ones
    for (const id in o.users) { const u = o.users[id]; if (!u || typeof u !== 'object') continue;
      for (const k of ['is', 'gs']) if (u[k] && typeof u[k] === 'object' && +u[k].s > SCORE_MAX) u[k].s = SCORE_MAX; }
  }
  if (from < SCHEMA) log('info', `migration: accounts schema ${from} -> ${SCHEMA} (${Object.keys(o.users).length} accounts kept)`);
  o.v = Math.max(from, SCHEMA);
  return o;
}
function migrateRooms(list) {
  for (const r of list) {
    if (!r || typeof r !== 'object') continue;
    if (!('shield' in r) || typeof r.shield !== 'object' || r.shield === null) r.shield = {};
    if (!Array.isArray(r.chat)) r.chat = [];
    if (!Array.isArray(r.pl)) r.pl = [];
    for (const p of r.pl) { if (!p || typeof p !== 'object') continue;
      if (p.last == null) p.last = 0;
      if (p.tal && typeof p.tal === 'object' && 'inf' in p.tal) p.tal = { h: p.tal };   // old single-army shape
      if (!('sc' in p)) p.sc = null;
      if (!Array.isArray(p.mids)) p.mids = [];        // v3: ids of battle messages already applied
    }
    if ((r.v || 1) < SCHEMA) r.v = SCHEMA;
  }
  return list;
}
function loadBackup(file, kind) {
  try {
    const box = JSON.parse(zlib.gunzipSync(fs.readFileSync(file)).toString());
    if (!box || box.kind !== kind || typeof box.data !== 'string') return null;
    if (!KINDS[kind].ok(box.data)) return null;
    if (box.check && box.check !== sha1(box.data)) { log('error', `backup ${path.basename(file)} failed its checksum`); return null; }
    return { data: box.data, at: box.at || 0, why: box.why || '', meta: box, file };
  } catch (e) { return null; }
}
const backupFiles = kind => { try { return fs.readdirSync(BK).filter(n => n.startsWith(kind + '-') && n.endsWith('.json.gz')).sort().reverse(); } catch (e) { return []; } };
const wq = {}; const enqueue = (k, fn) => (wq[k] = (wq[k] || Promise.resolve()).then(fn).catch(e => log('error', 'write queue: ' + e.message)));
/* write to a temp file, keep the previous good file as .prev, then rename: an interrupted write can never
   destroy the last valid copy */
async function atomicWrite(kind, str) { const { file, ok } = KINDS[kind];
  if (!ok(str)) { log('error', `refusing to save invalid ${kind} data (previous file kept)`); return false; }
  try { await fsp.writeFile(file + '.tmp', str);
    try { const cur = await fsp.readFile(file, 'utf8'); if (cur !== str && ok(cur)) await fsp.copyFile(file, file + '.prev'); } catch (e) {}
    await fsp.rename(file + '.tmp', file); return true; }
  catch (e) { log('error', `save failed for ${kind}: ${e.message}`); return false; } }
const writeData = (kind, str) => enqueue(kind, () => atomicWrite(kind, str));
const lastBk = {};
const bkMeta = (kind, str) => { try { const o = JSON.parse(str); return kind === 'accounts' ? { schema: o.v || 1, users: Object.keys(o.users).length }
  : { rooms: o.length, codes: o.map(r => r.code), players: o.reduce((a, r) => a + r.pl.length, 0), who: Object.fromEntries(o.map(r => [r.code, r.pl.map(p => p.u)])) }; } catch (e) { return {}; } };
const KEEP = 24, KEEP_DAYS = 30;   // 24 most recent copies + the newest copy of each of the last 30 days
let bkStats = { ok: 0, fail: 0, last: 0, lastErr: '' };
/* Backups are separate files (gzip + sha1 checksum + metadata) written next to - never over - the data.
   Every new backup is read back and verified before it counts. Retention never deletes the newest valid copy. */
async function backup(kind, str, why) {
  if (!KINDS[kind].ok(str)) { bkStats.fail++; return log('error', `backup of ${kind} skipped: data invalid`); }
  const h = sha1(str); if (why === 'scheduled' && lastBk[kind] === h) return;
  try { const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const z = zlib.gzipSync(JSON.stringify({ schema: SCHEMA, kind, at: Date.now(), why, check: h, ...bkMeta(kind, str), data: str }));
    const f = path.join(BK, `${kind}-${stamp}.json.gz`); await fsp.writeFile(f + '.tmp', z); await fsp.rename(f + '.tmp', f);
    const back = loadBackup(f, kind); if (!back) { await fsp.unlink(f).catch(() => {}); throw new Error('backup did not read back correctly'); }
    lastBk[kind] = h; bkStats.ok++; bkStats.last = Date.now();
    log('info', `backup ${kind} ok (${why}, ${z.length} bytes${kind === 'rooms' ? ', ' + JSON.parse(str).length + ' islands' : ''})`);
    const names = backupFiles(kind), keep = new Set(names.slice(0, KEEP)), days = new Set();
    for (const n of names) { const d = n.slice(kind.length + 1, kind.length + 11); if (!days.has(d) && days.size < KEEP_DAYS) { days.add(d); keep.add(n); } }
    keep.add(path.basename(f));   // the one we just verified
    for (const n of names) if (!keep.has(n)) await fsp.unlink(path.join(BK, n)).catch(() => {});
    // rolling copies outside the app disk
    const slot = Math.floor(Date.now() / 36e5) % 3;
    if (kind === 'accounts') remote.push('accounts.bk' + slot, str);
    else for (const r of JSON.parse(str)) remote.push('room:' + r.code + '.bk' + slot, JSON.stringify(r));
  } catch (e) { bkStats.fail++; bkStats.lastErr = e.message; log('error', `backup ${kind} failed: ${e.message}`); } }

async function recoverData() {
  log('info', 'data directory: ' + DATA + (remote.enabled ? ' | remote mirror: ON (Upstash)' : ' | remote mirror: OFF'));
  for (const kind of Object.keys(KINDS)) { const { file, ok } = KINDS[kind];
    let cur = null; try { cur = fs.readFileSync(file, 'utf8'); } catch (e) {}
    if (cur && ok(cur)) { migrateFile(kind, cur); continue; }
    if (cur) { const d = `${file}.corrupt-${Date.now()}`; try { fs.writeFileSync(d, cur); log('error', `${kind}: file is damaged, copy kept as ${path.basename(d)}`); } catch (e) {} }
    const c = [];
    if (remote.enabled) {   /* never start empty just because the cloud copy could not be read: retry, then stop */
      let got = false;
      for (let a = 1; a <= 5 && !got; a++) { try { c.push(['remote mirror', kind === 'rooms' ? await remote.pullRooms() : await remote.pull(kind)]); got = true; }
        catch (e) { log('error', `remote pull of ${kind} failed (attempt ${a}/5): ${e.message}`); await new Promise(r => setTimeout(r, 2000 * a)); } }
      if (!got && !cur && !fs.existsSync(file + '.prev') && !backupFiles(kind).length) { log('error', `FATAL: no local ${kind} and the cloud copy cannot be read. Refusing to start empty (it would overwrite the cloud copy). Render will restart the service.`); process.exit(1); }
    }
    try { c.push(['previous save', fs.readFileSync(file + '.prev', 'utf8')]); } catch (e) {}
    for (const n of backupFiles(kind)) { const b = loadBackup(path.join(BK, n), kind); if (b) c.push(['backup ' + n, b.data]); }
    if (remote.enabled && kind === 'accounts') for (let i = 0; i < 3; i++) { try { c.push(['remote backup ' + i, await remote.pull(kind + '.bk' + i)]); } catch (e) {} }
    const hit = c.find(x => x[1] && ok(x[1]));
    if (hit) { fs.writeFileSync(file, hit[1]); log('warn', `recovery: ${kind} RESTORED from ${hit[0]} (${hit[1].length} bytes)`); migrateFile(kind, hit[1]); }
    else log(cur ? 'error' : 'info', cur ? `recovery: ${kind} - no valid copy found; damaged file preserved, starting empty` : `${kind}: no data yet (new server)`); } }
/* write an already-validated file through the same migration path, keeping the old copy first */
function migrateFile(kind, raw) {
  let data; try { data = JSON.parse(raw); } catch (e) { return; }
  const before = JSON.stringify(data);
  if (kind === 'accounts') migrateAccounts(data); else migrateRooms(data);
  const after = JSON.stringify(data);
  if (after !== before) {
    const { file } = KINDS[kind];
    try { fs.copyFileSync(file, `${file}.premigrate-${Date.now()}`); } catch (e) {}
    backup(kind, raw, 'pre-migration');
    try { fs.writeFileSync(file + '.tmp', after); fs.renameSync(file + '.tmp', file); log('info', `${kind}: migrated on disk to schema ${SCHEMA}`); } catch (e) { log('error', `${kind}: migration write failed: ${e.message}`); }
  }
}
/* Is the data folder really persistent? A marker file written at every boot tells us whether the
   previous boot's files survived. This is reported honestly on /status and to players. */
const MARK = path.join(DATA, '.crownlands-disk');
let disk = { boots: 0, first: Date.now(), survived: false };
try { const o = JSON.parse(fs.readFileSync(MARK, 'utf8')); disk = { boots: (o.boots | 0), first: o.first || Date.now(), survived: true }; } catch (e) {}
disk.boots++; try { fs.writeFileSync(MARK, JSON.stringify({ boots: disk.boots, first: disk.first, last: Date.now() })); } catch (e) { log('error', 'cannot write to the data folder: ' + e.message); }
const durable = () => remote.enabled || (!!process.env.DATA_DIR && disk.survived);
const storageInfo = () => ({ durable: durable(), remote: remote.enabled, diskSet: !!process.env.DATA_DIR, diskSurvivedRestart: disk.survived, boots: disk.boots });

process.on('uncaughtException', e => log('error', 'uncaught: ' + (e && e.stack || e)));
process.on('unhandledRejection', e => log('error', 'unhandled rejection: ' + (e && e.stack || e)));
(async () => { await recoverData();
if (!remote.enabled && !process.env.DATA_DIR) log('warn', 'STORAGE IS NOT PERMANENT: set DATA_DIR to a Render persistent disk and/or UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN. Until then a redeploy wipes accounts and islands.');
else if (!remote.enabled && !disk.survived) log('warn', `DATA_DIR is set (${DATA}) but this is the first boot that sees it. If you see this after every deploy, the folder is NOT a persistent disk.`);

let db = { users: {}, keys: {} };
try { db = JSON.parse(fs.readFileSync(DBF, 'utf8')); } catch (e) {}
migrateAccounts(db);   // explicit data version; unknown fields are always preserved
const saveDb = () => { const j = JSON.stringify(db); if (!okAcc(j)) return log('error', 'account data invalid, not saved'); writeData('accounts', j); remote.push('accounts', j); };

/* ---------- rooms (persisted) ---------- */
const rooms = new Map();
function seed(r) { // every player's starting land belongs to them from the start, so it is protected too
  for (const p of r.pl) { const [sx, sy] = START[p.s];
    for (let y = -3; y <= 3; y++) for (let x = -3; x <= 3; x++) if (x * x + y * y <= 9) {
      const i = (sy + y) * 56 + sx + x; if (!r.tiles.has(i)) r.tiles.set(i, [p.s, x === 0 && y === 0 ? 1 : 0, 1, 0]); } }
}
const liveRoom = r => ({ ...r, v: r.v || SCHEMA, shield: r.shield || {}, tiles: new Map(r.tiles), pl: r.pl.map(p => ({ ...p, ws: null, mids: Array.isArray(p.mids) ? p.mids : [], tal: p.tal && 'inf' in p.tal ? { h: p.tal } : p.tal })) });
function loadRooms(list) { rooms.clear(); for (const r of migrateRooms(list)) { const room = liveRoom(r); if (room.started) seed(room); rooms.set(r.code, room); } }
try { loadRooms(JSON.parse(fs.readFileSync(RF, 'utf8'))); log('info', `loaded ${rooms.size} islands, ${Object.keys(db.users).length} accounts`); }
catch (e) { if (e.code !== 'ENOENT') log('error', 'rooms load failed: ' + e.message); }
let dirty = 0;
const snapRoom = r => ({ v: Math.max(r.v || 0, SCHEMA), code: r.code, host: r.host, seed: r.seed, diff: r.diff, started: r.started, at: r.at, shield: r.shield || {},
    tiles: [...r.tiles], mkt: r.mkt, chat: (r.chat || []).slice(-80), cid: r.cid || 0,
    pl: r.pl.map(p => ({ s: p.s, u: p.u, last: p.last || 0, save: p.save, tal: p.tal, pend: p.pend, sc: p.sc || null, fx: p.fx, mids: (p.mids || []).slice(-300) })) });
const snapshotRooms = () => [...rooms.values()].map(snapRoom);
function persistNow() { const list = snapshotRooms(), j = JSON.stringify(list); if (!okRooms(j)) return log('error', 'room snapshot invalid, not saved');
  remote.pushRooms(list); remote.push('rooms', j); return writeData('rooms', j); }
const persist = () => { if (dirty) return; dirty = setTimeout(() => { dirty = 0; persistNow(); }, 3000); };

/* ---------- http ---------- */
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.json': 'application/json', '.svg': 'image/svg+xml',
  '.webmanifest': 'application/manifest+json', '.apk': 'application/vnd.android.package-archive', '.png': 'image/png' };
const srv = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (req.url.split('?')[0] === '/status') { res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify({ ok: true, schema: SCHEMA, ...storageInfo(),
      storage: remote.enabled ? (process.env.DATA_DIR ? 'upstash+disk:' + DATA : 'upstash') : process.env.DATA_DIR ? 'disk:' + DATA : 'app-folder (NOT permanent)',
      accounts: Object.keys(db.users).length, rooms: rooms.size, online: wss ? wss.clients.size : 0,
      backups: { accounts: backupFiles('accounts').length, rooms: backupFiles('rooms').length, ok: bkStats.ok, failed: bkStats.fail, last: bkStats.last ? new Date(bkStats.last).toISOString() : null },
      remoteHealth: remote.health ? remote.health() : null })); }
  if (req.url.split('?')[0].startsWith('/admin/')) return adminApi(req, res);
  let p; try { p = decodeURIComponent(req.url.split('?')[0]); } catch (e) { res.writeHead(400); return res.end(); }
  if (p.endsWith('/')) p += 'index.html';
  const tryRoot = i => {
    if (i >= ROOTS.length) { res.writeHead(404); return res.end('Not found'); }
    const f = path.join(ROOTS[i], path.normalize(p));
    if (!f.startsWith(ROOTS[i])) { res.writeHead(403); return res.end(); }
    fs.readFile(f, (e, d) => {
      if (e) return tryRoot(i + 1);
      const h = { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' };
      if (f.endsWith('.html')) h['Cache-Control'] = 'no-cache';   // players always get the newest game after a deploy
      if (f.endsWith('.apk')) h['Content-Disposition'] = 'attachment; filename="crownlands.apk"';
      res.writeHead(200, h); res.end(d);
    });
  };
  tryRoot(0);
});

/* ---------- admin: backups, restore, export (only when ADMIN_TOKEN is set) ----------
   Every restore first makes a "pre-restore" backup of the current data, so a restore can itself be undone. */
const ADMIN = process.env.ADMIN_TOKEN || '';
function kickRoom(r, why) { for (const p of r.pl) if (p.ws) { try { send(p.ws, { t: 'err', m: why }); p.ws.room = null; p.ws.close(); } catch (e) {} p.ws = null; } }
function findBackup(kind, file) { const n = path.basename(String(file || '')); if (!n.startsWith(kind + '-')) return null; return loadBackup(path.join(BK, n), kind); }
async function adminApi(req, res) {
  const sendJ = (code, o) => { res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(o, null, 1)); };
  if (!ADMIN) return sendJ(403, { error: 'Admin API is disabled. Set ADMIN_TOKEN to use it.' });
  const tok = req.headers.authorization || '', want = 'Bearer ' + ADMIN;
  if (tok.length !== want.length || !crypto.timingSafeEqual(Buffer.from(tok), Buffer.from(want))) { log('warn', 'admin: rejected request from ' + (req.socket.remoteAddress || '?')); return sendJ(401, { error: 'Unauthorised' }); }
  const u = new URL(req.url, 'http://x'), route = u.pathname, q = k => u.searchParams.get(k) || '', post = req.method === 'POST';
  try {
    if (route === '/admin/backups') {
      return sendJ(200, { schema: SCHEMA, storage: storageInfo(), backups: ['accounts', 'rooms'].map(kind => ({ kind, copies: backupFiles(kind).map(n => {
        const b = loadBackup(path.join(BK, n), kind); return { file: n, valid: !!b, at: b ? new Date(b.at).toISOString() : null, why: b ? b.why : null,
          users: b && b.meta.users, rooms: b && b.meta.rooms, players: b && b.meta.who }; }) })) });
    }
    if (route === '/admin/backup') {   // create an on-demand backup now (GET or POST)
      const why = q('why') || 'manual';
      await backup('accounts', JSON.stringify(db), why); await backup('rooms', JSON.stringify(snapshotRooms()), why);
      log('info', 'admin: manual backup created (' + why + ')'); return sendJ(200, { ok: true, accounts: backupFiles('accounts')[0], rooms: backupFiles('rooms')[0] });
    }
    if (route === '/admin/export') {   // download the live data (keep it somewhere safe before a risky deploy)
      const kind = q('kind') === 'accounts' ? 'accounts' : 'rooms', body = kind === 'accounts' ? JSON.stringify(db) : JSON.stringify(snapshotRooms());
      res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Disposition': `attachment; filename="crownlands-${kind}-${new Date().toISOString().slice(0, 10)}.json"` }); return res.end(body);
    }
    if (!post && route.startsWith('/admin/restore')) return sendJ(405, { error: 'Use POST for restores' });
    if (route === '/admin/restore') {   // whole accounts file or whole islands file
      const kind = q('kind'); if (!KINDS[kind]) return sendJ(400, { error: 'kind must be accounts or rooms' });
      const b = findBackup(kind, q('file')); if (!b) return sendJ(404, { error: 'No valid backup with that name' });
      await backup('accounts', JSON.stringify(db), 'pre-restore'); await backup('rooms', JSON.stringify(snapshotRooms()), 'pre-restore');
      if (kind === 'accounts') { db = migrateAccounts(JSON.parse(b.data)); saveDb(); }
      else { for (const r of rooms.values()) kickRoom(r, 'The island was restored from a backup. Reconnecting…'); loadRooms(JSON.parse(b.data)); await persistNow(); }
      log('warn', `admin: RESTORED ${kind} from ${path.basename(b.file)} (taken ${new Date(b.at).toISOString()})`);
      return sendJ(200, { ok: true, restored: kind, from: path.basename(b.file) });
    }
    if (route === '/admin/restore-room' || route === '/admin/restore-player') {   // one island, or one player's realm on an island
      const code = q('code').toUpperCase(), b = findBackup('rooms', q('file')); if (!b) return sendJ(404, { error: 'No valid rooms backup with that name' });
      const old = JSON.parse(b.data).find(r => r.code === code); if (!old) return sendJ(404, { error: 'That island is not in this backup' });
      await backup('rooms', JSON.stringify(snapshotRooms()), 'pre-restore');
      if (route === '/admin/restore-room') {
        const cur = rooms.get(code); if (cur) kickRoom(cur, 'The island was restored from a backup. Reconnecting…');
        const room = liveRoom(migrateRooms([old])[0]); if (room.started) seed(room); rooms.set(code, room);
      } else {
        const user = q('user').toLowerCase(), cur = rooms.get(code); if (!cur) return sendJ(404, { error: 'That island does not exist now; restore the whole island instead' });
        const op = old.pl.find(p => p.u === user), cp = cur.pl.find(p => p.u === user); if (!op || !cp) return sendJ(404, { error: 'Player not on that island' });
        cp.save = op.save; cp.tal = op.tal;
        for (const [i, v] of old.tiles) if (v[0] === op.s) { const now = cur.tiles.get(i); if (!now || now[0] === 0 || now[0] === op.s) cur.tiles.set(i, v); }   // give back their land unless someone else holds it now
        if (cp.ws) { try { send(cp.ws, { t: 'err', m: 'Your realm was restored from a backup. Reconnecting…' }); cp.ws.room = null; cp.ws.close(); } catch (e) {} cp.ws = null; }
      }
      await persistNow(); log('warn', `admin: RESTORED ${route.endsWith('room') ? 'island' : 'player ' + q('user')} ${code} from ${path.basename(b.file)}`);
      return sendJ(200, { ok: true });
    }
  } catch (e) { log('error', 'admin api: ' + e.message); return sendJ(500, { error: 'Internal error: ' + e.message }); }
  return sendJ(404, { error: 'Unknown admin route' });
}

const wss = new WebSocketServer({ server: srv, maxPayload: 1048576 });

/* ---------- shared market (one per island): everyone trades against the same prices ---------- */
const MRES = ['wood', 'stone', 'silver', 'gold', 'diamond'], MIC = { wood: '🪵', stone: '🪨', silver: '🥈', gold: '🥇', diamond: '💎' };
const MB = { wood: 3, stone: 5, silver: 22, gold: 65, diamond: 220 }, MV = { wood: .6, stone: .7, silver: 1, gold: 1.2, diamond: 1.5 };
const MI = { wood: .0015, stone: .002, silver: .006, gold: .015, diamond: .04 }, MM0 = { wood: 600, stone: 400, silver: 150, gold: 60, diamond: 20 };
const MD = { wood: .5, stone: .4, silver: .12, gold: .05, diamond: .015 };
const cl = (v, a, b) => Math.max(a, Math.min(b, v));
function mkNew() { const m = { price: {}, mom: {}, mk: {}, hist: {}, news: '', feed: [], tk: 0 }; for (const r of MRES) { m.price[r] = MB[r]; m.mom[r] = 0; m.mk[r] = MM0[r]; m.hist[r] = [MB[r]]; } return m; }
function mkStep(m) {   // pacing matches the offline market: slower drift, calmer noise, 2-minute chart
  m.tk++;
  for (const r of MRES) {
    const dem = MD[r] * Math.pow(MB[r] / m.price[r], .15); m.mk[r] = cl(m.mk[r] + MD[r] - dem, 0, MM0[r] * 3);
    const sh = Math.max(.15, m.mk[r] / MM0[r]), tg = cl(MB[r] * Math.pow(1 / sh, .6), MB[r] * .35, MB[r] * 2.8);
    m.mom[r] = m.mom[r] * .9 + (Math.random() * 2 - 1) * .22 * MV[r];
    m.price[r] = cl(m.price[r] + (tg - m.price[r]) * .03 + m.price[r] * m.mom[r] * .004, MB[r] * .35, MB[r] * 2.8);
  }
  if (Math.random() < .004) { const r = MRES[Math.floor(Math.random() * 5)], up = Math.random() < .5; m.mk[r] = cl(m.mk[r] * (up ? .7 : 1.5), 0, MM0[r] * 3);
    m.news = up ? MIC[r] + ' A shortage of ' + r + ' is driving demand up. Good time to sell!' : MIC[r] + ' ' + r[0].toUpperCase() + r.slice(1) + ' floods the market. Time to buy?'; }
  if (m.tk % 3 == 0) for (const r of MRES) { const h = m.hist[r]; h.push(+m.price[r].toFixed(3)); if (h.length > 40) h.shift(); }
}
const mkPub = m => { const o = { t: 'mkt', price: {}, mk: {}, hist: m.hist, news: m.news, feed: m.feed }; for (const r of MRES) { o.price[r] = +m.price[r].toFixed(3); o.mk[r] = Math.round(m.mk[r]); } return o; };
setInterval(() => { for (const r of rooms.values()) { if (!r.started || !r.pl.some(p => p.ws)) continue; if (!r.mkt) r.mkt = mkNew(); mkStep(r.mkt); if (r.mkt.tk % 2 == 0) { const o = mkPub(r.mkt); r.pl.forEach(p => send(p.ws, o)); } } }, 1000);

/* ---------- helpers ---------- */
const hash = (p, s) => crypto.scryptSync(p, s, 32).toString('hex');
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const dname = u => (db.users[u] || {}).name || u;
const num = (v, a, b) => Number.isInteger(v) && v >= a && v <= b;
const fin = (v, a, b) => typeof v === 'number' && Number.isFinite(v) && v >= a && v <= b;
const touch = r => { r.at = Date.now(); persist(); };
const bad = (ws, what) => logOnce('bad:' + ws.u + what, 'warn', `invalid action rejected: ${what} (user ${ws.u || '?'})`);

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
  if (bp.ws && bp.ws !== ws) { try { send(bp.ws, { t: 'kicked', m: 'Your realm was opened on another device.' }); bp.ws.room = null; bp.ws.close(); } catch (e) {} log('info', `session replaced by a newer connection: room ${best.code} slot ${bp.s}`); }
  bp.ws = ws; ws.room = best; bp.last = Date.now(); log('info', `player (re)connected: room ${best.code} slot ${bp.s}`);
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
const CAPS = { ack: 1, px: 1, kicked: 1 };

function auth(ws, m) {
  if (m.t === 'resume') {
    const u = db.keys[m.k]; if (!u || !db.users[u]) return send(ws, { t: 'auth', ok: 0, err: 'Session expired' });
    ws.u = u; send(ws, { t: 'auth', ok: 1, name: dname(u), key: m.k, st: storageInfo(), caps: CAPS }); if (m.lb) return; return attach(ws);
  }
  const name = String(m.u || '').trim(), pw = String(m.p || ''), id = name.toLowerCase();
  if (!/^[A-Za-z0-9_]{3,16}$/.test(name)) return send(ws, { t: 'auth', ok: 0, err: 'Name: 3-16 letters, digits or _' });
  if (pw.length < 6 || pw.length > 64) return send(ws, { t: 'auth', ok: 0, err: 'Password: 6-64 characters' });
  const ip = (ws._ip || '?'), fk = 'auth:' + ip, n = Date.now(); authFails[fk] = (authFails[fk] || []).filter(t => n - t < 10 * 60e3);
  if (authFails[fk].length >= 12) return send(ws, { t: 'auth', ok: 0, err: 'Too many attempts. Try again in a few minutes.' });
  if (m.m === 'reg') {
    if (db.users[id]) return send(ws, { t: 'auth', ok: 0, err: 'Name already taken' });
    const salt = crypto.randomBytes(16).toString('hex'); db.users[id] = { name, salt, h: hash(pw, salt), created: Date.now() };
    log('info', 'account created: ' + id);
  } else {
    const u = db.users[id];
    if (!u || !crypto.timingSafeEqual(Buffer.from(u.h, 'hex'), Buffer.from(hash(pw, u.salt), 'hex'))) { authFails[fk].push(n); return send(ws, { t: 'auth', ok: 0, err: 'Wrong name or password' }); }
  }
  const key = crypto.randomBytes(18).toString('hex'); db.keys[key] = id; saveDb();
  ws.u = id; send(ws, { t: 'auth', ok: 1, name: dname(id), key, st: storageInfo(), caps: CAPS }); attach(ws);
}
const authFails = {};
setInterval(() => { const n = Date.now(); for (const k in authFails) if (!authFails[k].some(t => n - t < 10 * 60e3)) delete authFails[k]; }, 10 * 60e3);

/* a realm save sent by a client: structure and values are checked before it replaces the stored one */
function checkSave(d) {
  if (!d || typeof d !== 'object' || Array.isArray(d)) return 'not an object';
  if (d.coins != null && !fin(d.coins, 0, 1e18)) return 'impossible coins';
  if (d.res != null) { if (typeof d.res !== 'object') return 'bad resources'; for (const k in d.res) if (!fin(+d.res[k], 0, 1e16)) return 'impossible ' + k; }
  for (const key of ['army']) if (d[key] != null) { if (typeof d[key] !== 'object') return 'bad ' + key; for (const u in d[key]) if (!fin(+d[key][u], 0, 1e7)) return 'impossible army'; }
  if (d.gar != null && typeof d.gar !== 'object') return 'bad garrisons';
  if (d.time != null && !fin(d.time, 0, 1e10)) return 'bad time';
  return null;
}
/* score is a 0-1300 index; it cannot jump faster than real progress allows */
function okScore(prev, s, now) { const el = prev && prev.at ? (now - prev.at) / 60e3 : 1e9; return !prev || s <= (prev.s || 0) + 25 + 40 * el; }
const seen = (p, mid) => { if (typeof mid !== 'string' || mid.length > 40) return false; p.mids = p.mids || []; if (p.mids.includes(mid)) return true; p.mids.push(mid); if (p.mids.length > 400) p.mids.splice(0, p.mids.length - 300); return false; };
const nb = (i, f) => { if (i < 3136) { const x = i % 56, y = (i / 56) | 0; return [[1, 0], [-1, 0], [0, 1], [0, -1]].some(([a, b]) => { const X = x + a, Y = y + b; return X >= 0 && Y >= 0 && X < 56 && Y < 56 && f(Y * 56 + X); }); }
  const j = i - 3136, x = j % NCOL, y = (j / NCOL) | 0; return [[1, 0], [-1, 0], [0, 1], [0, -1]].some(([a, b]) => { const X = x + a, Y = y + b; return X >= 0 && Y >= 0 && X < NCOL && Y < NCOL && f(3136 + Y * NCOL + X); }); };

function handle(ws, m) {
  if (m.t === 'auth' || m.t === 'resume') return auth(ws, m);
  if (!ws.u) return;
  const r = ws.room, me = r && r.pl.find(p => p.u === ws.u);
  switch (m.t) {
    case 'create': {
      leave(ws); const code = newCode();
      const nr = { code, host: ws.u, seed: crypto.randomInt(1, 2 ** 31), diff: num(m.diff, 0, 4) ? m.diff : 1, started: false, at: Date.now(), shield: {}, tiles: new Map(), pl: [{ s: 1, u: ws.u, ws, last: Date.now(), mids: [] }] };
      rooms.set(code, nr); ws.room = nr; log('info', `island created: ${code}`); persist(); return bc(nr);
    }
    case 'join': {
      const x = rooms.get(String(m.code || '').toUpperCase().trim());
      if (!x) return send(ws, { t: 'err', m: 'No island with that key' });
      if (!x.pl.some(p => p.u === ws.u)) {
        if (x.started) return send(ws, { t: 'err', m: 'That game has already started' });
        if (x.pl.length >= MAXP) return send(ws, { t: 'err', m: 'Island is full (5/5)' });
        leave(ws); const s = [1, 2, 3, 4, 5].find(n => !x.pl.some(p => p.s === n)); x.pl.push({ s, u: ws.u, ws, last: Date.now(), mids: [] });
      } else x.pl.find(p => p.u === ws.u).last = Date.now();
      touch(x); return attach(ws);
    }
    case 'start': if (r && !r.started && r.host === ws.u && r.pl.length >= 2) { r.started = true; seed(r); touch(r); bc(r); log('info', `island started: ${r.code} (${r.pl.length} players)`); } else if (r && r.host === ws.u) send(ws, { t: 'err', m: 'Need at least 2 players' }); return;
    case 'leave': return leave(ws);
    case 'trade': { // a trade at the island's shared market; filled along the price curve like the offline market
      if (!r || !r.started || !me) return; if (!r.mkt) r.mkt = mkNew(); const m0 = r.mkt, res = String(m.r), n = Math.floor(+m.n);
      if (!MRES.includes(res) || !(n >= 1 && n <= 50000) || (m.side !== 'buy' && m.side !== 'sell')) return bad(ws, 'trade');
      ws.tids = ws.tids || new Set(); if (m.id != null) { if (ws.tids.has(m.id)) return; ws.tids.add(m.id); if (ws.tids.size > 200) ws.tids = new Set([...ws.tids].slice(-100)); }
      const mid = m0.price[res]; let px;
      if (m.side === 'sell') { const imp = Math.min(.3, n * MI[res] * .5); px = mid * (1 - imp / 2); m0.mk[res] = Math.min(MM0[res] * 3, m0.mk[res] + n); m0.price[res] = cl(mid * (1 - imp), MB[res] * .35, MB[res] * 2.8); }
      else { const imp = Math.min(.25, n * MI[res] * .4); px = mid * (1 + imp / 2); m0.mk[res] = Math.max(0, m0.mk[res] - n); m0.price[res] = cl(mid * (1 + imp), MB[res] * .35, MB[res] * 2.8); }
      m0.feed.push({ n: dname(me.u), s: m.side, r: res, q: n, at: Date.now() }); if (m0.feed.length > 8) m0.feed.shift();
      send(ws, { t: 'tr', id: m.id, r: res, side: m.side, n, mid: +mid.toFixed(3), px: +px.toFixed(4) }); const o = mkPub(m0); r.pl.forEach(p => send(p.ws, o)); touch(r); return;
    }
    case 'score': { // my island's progress score (shown in the island ranking and the world ranking)
      if (!r || !r.started || !me || !m.s || typeof m.s !== 'object') return; const now = Date.now(); if (me.scAt && now - me.scAt < 8000) return; me.scAt = now;
      let s = cl(Math.floor(+m.s.s) || 0, 0, SCORE_MAX); const p = Array.isArray(m.s.p) ? m.s.p.slice(0, 10).map(v => cl(Math.floor(+v) || 0, 0, 100)) : null;
      if (!okScore(me.sc, s, now)) { bad(ws, 'score jump'); s = Math.floor((me.sc.s || 0) + 25 + 40 * (now - me.sc.at) / 60e3); }
      me.sc = { s, p, d: num(m.s.d, 0, 4) ? m.s.d : 1, at: now }; const u = db.users[ws.u]; if (u) { u.is = { s, p, d: me.sc.d, at: now }; saveDb(); } bc(r); touch(r); return;
    }
    case 'gscore': { // an offline (solo) island of a logged-in player
      const u = db.users[ws.u]; if (!u || !m.s || typeof m.s !== 'object') return; const now = Date.now(); if (ws.gsAt && now - ws.gsAt < 8000) return; ws.gsAt = now;
      let s = cl(Math.floor(+m.s.s) || 0, 0, SCORE_MAX); const p = Array.isArray(m.s.p) ? m.s.p.slice(0, 10).map(v => cl(Math.floor(+v) || 0, 0, 100)) : null;
      if (u.gs && !okScore(u.gs, s, now)) { bad(ws, 'solo score jump'); s = Math.floor((u.gs.s || 0) + 25 + 40 * (now - u.gs.at) / 60e3); }
      u.gs = { s, p, d: num(m.s.d, 0, 4) ? m.s.d : 1, at: now }; saveDb(); return;
    }
    case 'lb': { // world ranking: every account's best island
      const rows = []; for (const id in db.users) { const u = db.users[id], a = u.gs, b = u.is; const best = !a ? (b ? [b, 'online'] : null) : !b ? [a, 'solo'] : (b.s > a.s ? [b, 'online'] : [a, 'solo']); if (best && best[0].s > 0) rows.push({ id, n: u.name || id, s: Math.min(SCORE_MAX, best[0].s), m: best[1], p: best[0].p, d: best[0].d }); }
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
    case 'save': { // the player's own realm (coins, army, research...) so it survives switching the phone off
      if (!r || !r.started || !me || !m.d || typeof m.d !== 'object') return;
      let size = 0; try { size = JSON.stringify(m.d).length; } catch (e) { return bad(ws, 'save (not serialisable)'); }
      if (size > 600000) return bad(ws, 'save too large (' + size + ')');
      const why = checkSave(m.d); if (why) { bad(ws, 'save: ' + why); return send(ws, { t: 'err', m: 'The server refused a damaged save; your previous one is kept.' }); }
      if (me.save && +me.save.ts && +m.d.ts && m.d.ts < me.save.ts - 10 * 60e3) return bad(ws, 'stale save from an older session');
      if (me.save && fin(+me.save.coins, 0, 1e18) && m.d.coins > me.save.coins * 20 + 5e6) logOnce('jump:' + ws.u, 'warn', `suspicious coin jump for ${ws.u} on ${r.code}: ${Math.round(me.save.coins)} -> ${Math.round(m.d.coins)} (kept; older copies are in the backups)`);
      me.save = m.d; touch(r); return;
    }
    case 'tiles': { // [index, owner(global slot), building, level, walls, conquest?]  conquest = 1 + previous building + 20 * previous owner
      if (!r || !r.started || !me || !Array.isArray(m.d)) return;
      const ok = [], badT = [], conq = new Map(), now = Date.now(); r.shield = r.shield || {}; let blocked = 0;
      const owner = i => { const c = r.tiles.get(i); return c ? c[0] : 0; }, claims = [];
      for (const e of m.d.slice(0, 400)) {
        if (!Array.isArray(e)) continue; const [i, o, b, l, w, cq] = e;
        if (!num(i, 0, NT - 1) || o !== me.s || !num(b, 0, 12) || !num(l, 0, 40) || !num(w || 0, 0, 3) || !num(cq || 0, 0, 200)) { bad(ws, 'tile values'); continue; }
        const cur = r.tiles.get(i);
        let po = cur && cur[0] || 0, pb = cur ? cur[1] : 0;
        if (!po && cq) { po = Math.floor((cq - 1) / 20); pb = (cq - 1) % 20; }
        if (po && po !== me.s) {                               // this is a conquest
          if (!cq) { badT.push([i, ...cur]); continue; }        // lost a race: resync the sender
          if (po >= 1 && po <= 5 && (r.shield[po] || 0) > now) { badT.push([i, ...(cur || [po, pb, 1, 0])]); blocked = po; continue; }
          conq.set(po, Math.max(conq.get(po) || 0, pb === 1 || pb === 3 ? SHIELD_BIG : 1));
        } else if (!po && i < 3136 && !cq) { claims.push([i, o, b, l, w]); continue; }   // a neutral claim on the home island: must touch the realm
        r.tiles.set(i, [o, b, l, w || 0]); ok.push([i, o, b, l, w || 0]);
      }
      /* claims are accepted in chains (a tile next to one claimed in the same batch is fine) */
      let left = claims, prog = true;
      while (left.length && prog) { prog = false; const next = [];
        for (const c of left) { if (nb(c[0], j => owner(j) === me.s)) { r.tiles.set(c[0], [c[1], c[2], c[3], c[4] || 0]); ok.push([c[0], c[1], c[2], c[3], c[4] || 0]); prog = true; } else next.push(c); }
        left = next; }
      if (left.length) { bad(ws, 'claim not touching the realm'); for (const c of left) badT.push([c[0], 0, 0, 0, 0]); }
      for (const [po, mult] of conq) {                         // start (or extend) the victim's shield
        const v = r.pl.find(p => p.s === po); if (!v) continue; const until = now + (v && v.ws ? SHIELD_ONLINE : SHIELD_OFFLINE) * mult;
        if (until > (r.shield[po] || 0)) { r.shield[po] = until; r.pl.forEach(p => send(p.ws, { t: 'shield', s: po, until })); }
      }
      if (ok.length) { others(r, ws, { t: 'tiles', d: ok }); touch(r); }
      if (badT.length) send(ws, { t: 'tiles', d: badT });
      if (blocked) send(ws, { t: 'err', m: '\u{1F6E1} That realm is shielded for ' + Math.ceil((r.shield[blocked] - now) / 6e4) + ' more min' });
      return;
    }
    case 'tally': if (r && r.started && me && m.u && typeof m.u === 'object') {
      const u = {}; for (const k of UNITS) u[k] = Math.max(0, Math.min(9999, Math.floor(+m.u[k]) || 0)); const rr = m.r === 'c' ? 'c' : 'h';
      me.tal = me.tal && !('inf' in me.tal) ? me.tal : {}; me.tal[rr] = u; others(r, ws, { t: 'tally', from: me.s, r: rr, u }); persist();
    } return;
    case 'hit': case 'siege': { // relay to the target; queue it if they are offline. Each id is applied once.
      if (!r || !r.started || !me || !num(m.to, 1, 5) || !num(m.i, 0, NT - 1)) return bad(ws, m.t);
      if (m.mid != null) { if (seen(me, m.mid)) return send(ws, { t: 'ack', mid: m.mid }); send(ws, { t: 'ack', mid: m.mid }); }
      const to = r.pl.find(p => p.s === m.to); if (!to || to === me) return;
      const ld = {}; if (m.ld) for (const k of UNITS) ld[k] = Math.max(0, Math.min(999, Math.floor(+m.ld[k] || 0)));
      const o = { t: m.t, from: me.s, i: m.i, ld, win: !!m.win };
      if (to.ws) send(to.ws, o); else { (to.pend = to.pend || []).push(o); if (to.pend.length > 40) to.pend.shift(); }
      touch(r); return;
    }
    default: bad(ws, 'unknown message ' + String(m.t).slice(0, 20));
  }
}

wss.on('connection', (ws, req) => {
  ws.alive = true; ws.on('pong', () => ws.alive = true); ws._ip = (req && (req.headers['x-forwarded-for'] || req.socket.remoteAddress) || '?').toString().split(',')[0].trim();
  ws.rate = { t: 0, n: 0 };
  ws.on('message', raw => { const n = Date.now(); if (n - ws.rate.t > 1000) { ws.rate.t = n; ws.rate.n = 0; } if (++ws.rate.n > 80) return logOnce('flood:' + ws.u, 'warn', 'message flood ignored from ' + (ws.u || 'anonymous'));
    let m; try { m = JSON.parse(raw); } catch (e) { return; } if (m && typeof m === 'object') try { handle(ws, m); } catch (e) { log('error', 'handler ' + String(m.t).slice(0, 20) + ': ' + (e && e.stack || e)); } });
  ws.on('error', e => logOnce('wserr:' + ws._ip, 'warn', 'connection error: ' + e.message));
  ws.on('close', () => { const r = ws.room; if (!r) return; const p = r.pl.find(q => q.ws === ws); if (p) { p.ws = null; bc(r); log('info', `player disconnected (no penalty; realm and battles kept): room ${r.code} slot ${p.s}`); } });
});
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) { log('info', 'connection timed out (no reply to ping)'); return ws.terminate(); } ws.alive = false; ws.ping(); }), 30000);
/* idle islands: lobbies that never started are removed after 2 h; started islands are ARCHIVED (file + cloud), never just deleted */
setInterval(async () => { const n = Date.now();
  for (const [c, r] of rooms) { if (r.pl.some(p => p.ws)) continue;
    if (!r.started && n - r.at > 2 * 36e5) { rooms.delete(c); continue; }
    if (r.started && n - r.at > ARCHIVE_AFTER) { const s = JSON.stringify(snapRoom(r));
      try { await fsp.writeFile(path.join(ARCH, `room-${c}-${new Date().toISOString().slice(0, 10)}.json.gz`), zlib.gzipSync(s)); await remote.archive(c, s); rooms.delete(c); log('info', `island ${c} archived after ${Math.round((n - r.at) / DAY)} idle days (restore: see README)`); }
      catch (e) { log('error', `archiving island ${c} failed, kept: ${e.message}`); } } }
  persist(); }, 6e5);
srv.listen(PORT, () => log('info', `Crownlands server v3 on http://localhost:${PORT}  (data: ${DATA}; permanent storage: ${durable() ? 'yes' : 'NO'})`));

// Backups: at start-up, on a schedule when something changed, and on shutdown.
const runBackups = why => { backup('accounts', JSON.stringify(db), why); backup('rooms', JSON.stringify(snapshotRooms()), why); };
setTimeout(() => runBackups('boot'), 8000); setInterval(() => runBackups('scheduled'), BACKUP_EVERY);
let closing = false;
async function shutdown(sig) { if (closing) return; closing = true; log('info', sig + ': saving everything before exit');
  try { const a = JSON.stringify(db), list = snapshotRooms(), r = JSON.stringify(list); await writeData('accounts', a); await writeData('rooms', r); remote.push('accounts', a); remote.pushRooms(list); remote.push('rooms', r);
    await backup('accounts', a, 'shutdown'); await backup('rooms', r, 'shutdown'); const ok = await remote.flushAll(); log('info', 'shutdown save complete' + (remote.enabled ? (ok ? ' (cloud copy up to date)' : ' (cloud copy FAILED - local copy kept)') : '')); } catch (e) { log('error', 'shutdown save failed: ' + e.message); }
  process.exit(0); }
process.on('SIGTERM', () => shutdown('SIGTERM')); process.on('SIGINT', () => shutdown('SIGINT'));
})().catch(e => { console.error('FATAL start-up error', e); process.exit(1); });
