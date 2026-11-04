// Optional cloud memory: keeps accounts + islands in a free Upstash Redis database so a free web host
// (which wipes its disk on every restart) never forgets anything.
const zlib = require('zlib');
const URL_ = process.env.UPSTASH_REDIS_REST_URL, TOK = process.env.UPSTASH_REDIS_REST_TOKEN;
exports.enabled = !!(URL_ && TOK);
async function cmd(a) {
  const r = await fetch(URL_, { method: 'POST', headers: { Authorization: 'Bearer ' + TOK, 'Content-Type': 'application/json' }, body: JSON.stringify(a) });
  const j = await r.json(); if (j.error) throw new Error(j.error); return j.result;
}
exports.pull = async name => { const v = await cmd(['GET', 'crownlands:' + name]); return v ? zlib.gunzipSync(Buffer.from(v, 'base64')).toString('utf8') : null; };
const timers = {}, pending = {};
async function send(name) {
  const s = pending[name]; delete pending[name]; if (s == null) return;
  try { const z = zlib.gzipSync(Buffer.from(s)).toString('base64'); if (z.length > 950000) return console.error('[remote] ' + name + ' is too large to store'); await cmd(['SET', 'crownlands:' + name, z]); }
  catch (e) { console.error('[remote] save failed:', e.message); }
}
exports.push = (name, str) => { if (!exports.enabled) return; pending[name] = str; if (timers[name]) return; timers[name] = setTimeout(() => { timers[name] = 0; send(name); }, 5000); };
exports.flushAll = async () => { for (const n of Object.keys(pending)) { clearTimeout(timers[n]); timers[n] = 0; await send(n); } };
