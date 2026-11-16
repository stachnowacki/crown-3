// Optional cloud memory: keeps accounts + islands in an Upstash Redis database (REST API) so a host whose
// disk is wiped on every deploy (for example a free Render web service) never forgets anything.
//
// Keys (all prefixed "crownlands:"):
//   accounts            gzip+base64 of accounts.json
//   room:<CODE>         one key per island (so a busy server never hits Upstash's 1 MB value limit)
//   rooms.idx           JSON list of the island codes that exist
//   rooms               legacy single-blob copy of all islands (read for migration, written while small)
//   accounts.bk0..2     rolling hourly copies;  room:<CODE>.bk0..2 the same per island
//   archive:<CODE>      islands archived after long inactivity
const zlib = require('zlib');
const URL_ = process.env.UPSTASH_REDIS_REST_URL, TOK = process.env.UPSTASH_REDIS_REST_TOKEN;
const EVERY = Math.max(2, +process.env.REMOTE_EVERY_S || 15) * 1000;   // how often a changed key is uploaded
exports.enabled = !!(URL_ && TOK);
const P = 'crownlands:', MAXV = 950000;
let failures = 0, lastOk = 0, lastErr = '';
exports.health = () => ({ enabled: exports.enabled, failures, lastOk, lastErr });

async function cmd(a) {
  const r = await fetch(URL_, { method: 'POST', headers: { Authorization: 'Bearer ' + TOK, 'Content-Type': 'application/json' }, body: JSON.stringify(a) });
  const j = await r.json(); if (j.error) throw new Error(j.error); return j.result;
}
const pack = s => zlib.gzipSync(Buffer.from(s)).toString('base64');
const unpack = v => v ? zlib.gunzipSync(Buffer.from(v, 'base64')).toString('utf8') : null;
exports.pull = async name => unpack(await cmd(['GET', P + name]));

/* rooms are stored one key per island; a legacy single "rooms" blob is still read if no index exists */
exports.pullRooms = async () => {
  const idx = await exports.pull('rooms.idx');
  if (!idx) return exports.pull('rooms');
  const codes = JSON.parse(idx), out = [];
  for (const c of codes) { const s = await exports.pull('room:' + c); if (s) { try { out.push(JSON.parse(s)); } catch (e) {} } }
  return JSON.stringify(out);
};

const timers = {}, pending = {}, sentHash = {};
async function send(name) {
  const s = pending[name]; delete pending[name]; if (s == null) return true;
  try {
    const z = pack(s);
    if (z.length > MAXV) { lastErr = name + ' is too large for one key (' + z.length + ' bytes)'; console.error('[remote] ' + lastErr); return false; }
    await cmd(['SET', P + name, z]); lastOk = Date.now(); failures = 0; return true;
  } catch (e) {
    failures++; lastErr = e.message; console.error('[remote] save of ' + name + ' failed (' + failures + ' in a row): ' + e.message);
    if (pending[name] == null) { pending[name] = s; schedule(name, Math.min(60000, EVERY * failures)); }   // retry later, newest data wins
    return false;
  }
}
function schedule(name, ms) { if (timers[name]) return; timers[name] = setTimeout(() => { timers[name] = 0; send(name); }, ms); }
exports.push = (name, str) => { if (!exports.enabled) return; pending[name] = str; schedule(name, EVERY); };
/* push every island that changed, plus the index; unchanged islands cost nothing */
const crypto = require('crypto');
exports.pushRooms = (list) => {
  if (!exports.enabled) return;
  const codes = [];
  for (const r of list) { const s = JSON.stringify(r), h = crypto.createHash('sha1').update(s).digest('hex'); codes.push(r.code);
    if (sentHash['room:' + r.code] !== h) { sentHash['room:' + r.code] = h; exports.push('room:' + r.code, s); } }
  const idx = JSON.stringify(codes.sort());
  if (sentHash.idx !== idx) { sentHash.idx = idx; exports.push('rooms.idx', idx); }
};
exports.archive = async (code, str) => { if (!exports.enabled) return; try { await cmd(['SET', P + 'archive:' + code, pack(str)]); } catch (e) { console.error('[remote] archive failed: ' + e.message); } };
exports.flushAll = async () => { let ok = true; for (const n of Object.keys(pending)) { clearTimeout(timers[n]); timers[n] = 0; ok = (await send(n)) && ok; } return ok; };
