// Crownlands multiplayer server v4 (v3 + seasons, world events and exactly-once reward claims).  npm install && npm start   (Node 18+)
// - accounts (scrypt), invite-key islands (max 7, joinable after the start), tile/combat relay
// - islands are saved to disk and (optionally) mirrored to Upstash Redis, so realms survive restarts,
//   redeploys and crashes; attacks against offline players are queued and delivered when they return
// - automatic, verified, compressed backups with retention; admin API to list, create and restore them
// - serves www/ (the game) and public/ (install page, optional crownlands.apk)
// - offline island snapshots that other players can visit: DATA_DIR/snaps/<account>.json.gz (+ cloud key snap:<account>)
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
// Map layout (update 18.11): the home island is 126x126 (was 56x56) and the shared New Land 404x404 (was 128x128).
// Tile index = home y*HN+x, then the New Land from HN*HN on. Islands saved with the old layout are migrated on load
// (their old map sits in the middle of the bigger one: +OFFH / +OFFC on both axes), so no realm is lost.
const HN = 126, NCOL = 404, OFFH = 35, OFFC = 138, HT = HN * HN, MAXP = 7, NT = HT + NCOL * NCOL, DAY = 864e5, UNITS = ['inf', 'arc', 'cav', 'sie'];
const PROTO = 2;   // clients older than this use the old tile layout and must reload
// Conquest shield: after a realm loses land to another player, nobody may conquer from it for a while.
// Longer when the victim is offline (they cannot defend) and x3 when a city or stronghold fell.
const SHIELD_ONLINE = 8 * 60e3, SHIELD_OFFLINE = 25 * 60e3, SHIELD_BIG = 3;
const START = [null, [28, 43], [12, 15], [44, 15], [45, 33], [12, 34]].map(p => p && [p[0] + OFFH, p[1] + OFFH]);   // must match the client's start positions (slots 1-5)
// Slots 6-7, and anyone who joins after the start, have no fixed start: their game picks a spot on the real terrain
// (free land first, then claimed land with the fewest buildings) and sends it as a 'spawn' message; it is kept as p.sp.
const SCHEMA = 4, SCORE_MAX = 1e15;   // v4: the kingdom index has no ceiling any more
/*@@CORE7@@*/
/* ===== CORE7: shared by the server and the game. Pure functions, no DOM, no Node APIs.
   The server runs this to decide the world; the offline game runs the same code locally. ===== */
const CORE7=(()=>{
const DAY=864e5,T0=Date.UTC(2026,0,1),EP=14*DAY;
const h32=s=>{s=String(s);let h=2166136261;for(let i=0;i<s.length;i++){h^=s.charCodeAt(i);h=Math.imul(h,16777619)}return h>>>0};
const cl=(v,a,b)=>Math.max(a,Math.min(b,v));

/* ---------- seasons: calendar months (UTC). Add a theme to THEMES, or an entry to OVERRIDE keyed by season index, to change what a season is. ---------- */
const THEMES=[
 {k:'conquest',n:'The Age of Conquest',ic:'⚔️',col:'#ff6b5a',w:{eco:.7,mil:1.6,exp:1,dev:.7,spec:1},intro:'Banners rise along every border. This season rewards those who march, besiege and hold what they win.',desc:'Military and conquest',em:'Laurel of Conquest',ti:'Season Victor'},
 {k:'trade',n:'The Golden Trade Era',ic:'💰',col:'#ffd34a',w:{eco:1.7,mil:.6,exp:.9,dev:.9,spec:1},intro:'Caravans fill the roads and prices sing. This season rewards merchants, builders of markets and the well-stocked.',desc:'Trade and prosperity',em:'Golden Scale',ti:'Merchant of the Era'},
 {k:'winter',n:'Winter of the North',ic:'❄️',col:'#8fe0ff',w:{eco:1,mil:.9,exp:.6,dev:1.5,spec:1},intro:'The north wind closes the passes. This season rewards kingdoms that prepare, research and build solid foundations.',desc:'Endurance and development',em:'Frost Crest',ti:'Warden of Winter'},
 {k:'expansion',n:'The Great Expansion',ic:'🗺️',col:'#7bd88f',w:{eco:.8,mil:.9,exp:1.8,dev:.8,spec:1},intro:'New horizons open for every ruler. This season rewards those who claim, settle and discover.',desc:'Territory and settlement',em:'Pathfinder Wreath',ti:'Pathfinder'},
 {k:'discovery',n:'The Age of Discovery',ic:'🧭',col:'#61d1d9',w:{eco:.9,mil:.7,exp:1.3,dev:1.2,spec:1.4},intro:'Ruins, relics and rumours draw explorers out. This season rewards curiosity, research and the world events that shape the realm.',desc:'Exploration and events',em:'Compass Rose',ti:'Discoverer'},
 {k:'dragon',n:"The Dragon's Awakening",ic:'🐉',col:'#b58cff',w:{eco:.8,mil:1.4,exp:.8,dev:1.1,spec:1.3},intro:'Something ancient stirs in the mountains. This season rewards strong armies, strong walls and steady nerves.',desc:'Defence and strength',em:'Dragon Scale',ti:'Dragonwatch'},
 {k:'renaissance',n:'The Imperial Renaissance',ic:'🏛️',col:'#f2bd54',w:{eco:1.1,mil:.7,exp:.9,dev:1.7,spec:1},intro:'Scholars, masons and artists flourish. This season rewards research, great works and well-built cities.',desc:'Learning and great works',em:'Imperial Seal',ti:'Renaissance Patron'}];
const OVERRIDE={};   // e.g. OVERRIDE[11]={n:'A special winter',...} replaces the theme of season index 11
function seasonFor(now){const d=new Date(Math.max(now,T0));let idx=(d.getUTCFullYear()-2026)*12+d.getUTCMonth();if(idx<0)idx=0;
 const th=OVERRIDE[idx]||THEMES[idx%THEMES.length],start=Date.UTC(2026,idx,1),end=Date.UTC(2026,idx+1,1);
 return Object.assign({idx,id:'s'+idx,start,end,len:end-start,left:Math.max(0,end-now)},th)}
function seasonAt(idx){const th=OVERRIDE[idx]||THEMES[idx%THEMES.length];return Object.assign({idx,id:'s'+idx,start:Date.UTC(2026,idx,1),end:Date.UTC(2026,idx+1,1)},th)}
/* the season track: points needed -> reward id. Server and game both use the ids. */
const TRACK=[{id:'m1',p:20,k:'small'},{id:'m2',p:50,k:'small',g:3},{id:'m3',p:90,k:'cos'},{id:'m4',p:130,k:'med',g:5},{id:'m5',p:170,k:'rare'},{id:'m6',p:210,k:'med',g:6},{id:'fin',p:250,k:'final',g:15},{id:'b1',p:300,k:'bonus',g:10},{id:'b2',p:340,k:'bonus',g:10}];

/* ---------- major world events: one every 14 days, known in advance from the calendar ---------- */
const WE={
 invasion:{n:'The Great Invasion',ic:'⚔️',col:'#ff5a5a',days:6,
  intro:'A vast host has crossed the edge of the known world and is marching on the border kingdoms. No single realm can turn it back. Hold the line together.',
  st:[{n:'Muster the defences',ic:'🛡️',kind:'supply',gp:50,t:'Send supplies to the border forts.'},{n:'Meet the host',ic:'⚔️',kind:'act',key:'won',per:4,gp:48,t:'Win battles against any enemy.'},{n:'Hold the passes',ic:'🧱',kind:'supply',gp:56,t:'Reinforce the mountain passes.'}],
  out:{win:'The host broke against the combined defences and fled the world. Every kingdom that stood is honoured.',partial:'The host retreated after heavy losses, leaving scars on the borderlands.',fail:'The invaders overran several frontier regions before winter halted them.'},
  mod:{win:{n:'Victory feast',ic:'🎉',inc:1.08,days:2},partial:{n:'Scarred borders',ic:'🩹',def:1.05,days:2},fail:{n:'Occupied frontier',ic:'🏴',prod:.95,days:2}},
  rw:[{s:25,r:.04,sp:10},{s:70,r:.08,g:4,sp:20},{s:140,r:.14,g:10,sp:35,ti:'defender'}]},
 trade:{n:'The Golden Trade Era',ic:'💰',col:'#ffd34a',days:7,
  intro:'A grand charter opens the trade roads across the world. Merchants will pay well for those who produce, trade and invest. The most successful will be remembered.',
  st:[{n:'Open the roads',ic:'🐪',kind:'act',key:'trades',per:.6,gp:54,t:'Make trades at the market.'},{n:'Fill the caravans',ic:'📦',kind:'supply',gp:52,t:'Send goods to the caravans.'},{n:'Build the trade houses',ic:'🏦',kind:'supply',gp:58,t:'Invest coins in trade infrastructure.'}],
  out:{win:'The roads are open. A lasting trade route now serves every kingdom.',partial:'The charter was signed, but only some of the roads opened.',fail:'The merchants moved on before the roads were ready.'},
  mod:{win:{n:'Golden trade route',ic:'🐪',inc:1.06,spr:.99,days:3},partial:{n:'Trade charter',ic:'📜',inc:1.03,days:2},fail:null},
  rw:[{s:30,r:.04,sp:10},{s:80,r:.09,g:4,sp:20},{s:160,r:.15,g:10,sp:35,ti:'merchant'}]},
 drought:{n:'The Great Drought',ic:'☀️',col:'#f0a040',days:7,
  intro:'Rivers fall and fields crack across many regions. Prepare, trade for supplies and help your neighbours. Those who share their stores will not be forgotten.',
  st:[{n:'Dig the wells',ic:'⛏️',kind:'supply',gp:50,t:'Contribute materials for wells and cisterns.'},{n:'Buy and move grain',ic:'🌾',kind:'act',key:'trades',per:.6,gp:50,t:'Trade to bring food and goods to the dry regions.'},{n:'Share the stores',ic:'🤝',kind:'supply',gp:56,t:'Donate to the neighbouring kingdoms in need.'}],
  out:{win:'Rain returned to a world that had shared its stores. A bumper harvest follows.',partial:'The worst was averted, though the recovery will be slow.',fail:'The drought ran its course. The recovery will take time.'},
  mod:{win:{n:'Bumper harvest',ic:'🌾',prod:1.08,days:2},partial:{n:'Slow recovery',ic:'🌦️',prod:1.02,days:2},fail:{n:'Lingering dry spell',ic:'☀️',prod:.95,days:2}},
  rw:[{s:25,r:.05,sp:10},{s:70,r:.1,g:4,sp:20},{s:140,r:.16,g:10,sp:35,ti:'steward'}]},
 empire:{n:'The Lost Empire',ic:'🏺',col:'#d7a6ff',days:7,
  intro:'Surveyors report the ruins of an ancient empire beneath the sands. Fund the expeditions and uncover what it left behind. Each expedition may bring home something different.',
  st:[{n:'Fund the expeditions',ic:'🧭',kind:'supply',gp:52,t:'Contribute supplies to the expeditions.'},{n:'Decipher the archive',ic:'📜',kind:'act',key:'techs',per:8,gp:44,t:'Complete research in the development tree.'},{n:'Open the vault',ic:'🗝️',kind:'supply',gp:58,t:'Pay for the final excavation.'}],
  out:{win:'The vault was opened. The empire\'s lost works now enrich every kingdom that helped.',partial:'Part of the archive was recovered before the sands shifted.',fail:'The sands buried the dig. Only a few relics were saved.'},
  mod:{win:{n:'Ancient knowledge',ic:'📚',inc:1.04,days:3},partial:{n:'Recovered scrolls',ic:'📜',inc:1.02,days:2},fail:null},
  rw:[{s:25,r:.05,sp:10},{s:75,r:.1,g:5,sp:20},{s:150,r:.16,g:12,sp:35,ti:'discoverer'}],relics:true},
 succession:{n:'The Succession War',ic:'👑',col:'#9d6cff',days:6,fac:['House Valmar','House Orrin'],
  intro:'The neighbouring throne is empty and two claimants raise banners. Back a side, trade with the factions, or keep to your own counsel. The world will live with the result.',
  st:[{n:'Raise the banners',ic:'🚩',kind:'supply',gp:50,t:'Send supplies to your chosen claimant.'},{n:'March to the claimant',ic:'⚔️',kind:'act',key:'won',per:4,gp:46,t:'Win battles in support of the war.'},{n:'Crown the victor',ic:'👑',kind:'supply',gp:56,t:'Fund the coronation and the peace.'}],
  out:{win:'The war ended with a single crown and a settled peace across the border.',partial:'A fragile truce holds, with the crown still contested.',fail:'The realm splintered. The border remains unsettled.'},
  mod:{win:{n:'Settled borders',ic:'🕊️',def:1.05,inc:1.03,days:3},partial:{n:'Fragile truce',ic:'🕯️',def:1.03,days:2},fail:{n:'Unsettled border',ic:'🔥',def:.97,days:2}},
  rw:[{s:25,r:.05,sp:10},{s:75,r:.1,g:4,sp:20},{s:150,r:.16,g:10,sp:35,ti:'kingmaker'}],fw:{0:{n:'House Valmar crowned',inc:1.03},1:{n:'House Orrin crowned',def:1.04}}}};
const KINDS=Object.keys(WE);
const NPC=.4,PCAP=1.6,MINU=8,RUNIT=12;
const pcOf=s=>Math.round(s.gp*PCAP);
const RELICS=['Bronze Astrolabe','Sun Disc','Jade Mask','Sealed Codex','Iron Crown Shard','Moonstone Idol','Cartographer\'s Slate','Emerald Signet'];
const relicsOf=(acc,w)=>{const u=sumU(w.pl[acc]),out=[];for(let i=1;i<=Math.floor(u/RUNIT);i++){const r=h32(acc+'|'+w.id+'|'+w.start+'|'+i)%RELICS.length;if(!out.includes(r))out.push(r)}return out};

function kindOf(e){let prev=-1,k=0;for(let i=0;i<=e;i++){k=h32('we'+i)%KINDS.length;if(k===prev)k=(k+1+h32('x'+i)%(KINDS.length-1))%KINDS.length;prev=k}return KINDS[k]}
function schedule(now){const e=Math.max(0,Math.floor((now-T0)/EP)),mk=ep=>{const id=kindOf(ep),d=WE[id],start=T0+ep*EP+DAY+(h32('o'+ep)%2)*DAY*.5;return{ep,id,start,end:start+d.days*DAY,announce:T0+ep*EP}};
 const a=mk(e),b=mk(e+1);let cur=null,next=null;
 if(now<a.start)next=a;else if(now<a.end)cur=a;else next=b;
 return{cur,next,ep:e}}
const sumU=p=>p&&p.s?p.s.reduce((a,b)=>a+(b|0),0):0;
const stageWin=(w,k)=>{const D=(w.end-w.start)/3;return[w.start+k*D,w.start+(k+1)*D]};
const stageAt=(w,now)=>cl(Math.floor((now-w.start)/((w.end-w.start)/3)),0,2);
function newEvent(sch){return{v:1,id:sch.id,ep:sch.ep,start:sch.start,end:sch.end,pl:{},ok:[0,0,0],res:null}}
function totals(w,k,now){const s=WE[w.id].st[k],pc=pcOf(s),acc=Object.keys(w.pl),n=Math.max(1,acc.length),G=Math.round(s.gp*Math.pow(n,.85));let mine=0;for(const a of acc)mine+=Math.min(pc,(w.pl[a].s||[])[k]|0);
 const[a,b]=stageWin(w,k),f=cl((now-a)/(b-a),0,1),npc=Math.round(G*NPC*f);return{G,mine,npc,tot:mine+npc,n,pc}}
/* idempotent: safe to call at any time, any number of times, after any gap */
function evaluate(w,now){if(!w||w.res)return w;for(let k=0;k<3;k++){if(w.ok[k])continue;const t=totals(w,k,now);if(t.tot>=t.G)w.ok[k]=1;else if(now>=stageWin(w,k)[1])w.ok[k]=2}
 if(now>=w.end){for(let k=0;k<3;k++)if(!w.ok[k])w.ok[k]=2;finalize(w)}return w}
function finalize(w){if(w.res)return;const def=WE[w.id],n=w.ok.filter(x=>x==1).length,o=n>=3?'win':n==2?'partial':'fail';
 const rows=Object.keys(w.pl).map(a=>[a,Math.min(3e3,sumU(w.pl[a]))]).sort((x,y)=>y[1]-x[1]||(x[0]<y[0]?-1:1));
 let fac=null;if(def.fac){const f=[0,0];for(const a in w.pl){const q=w.pl[a];if(q.f===0||q.f===1)f[q.f]+=sumU(q)}fac=f[0]>f[1]?0:f[1]>f[0]?1:2}
 let mod=def.mod[o]?Object.assign({},def.mod[o]):null;if(mod&&def.fw&&fac!=null&&fac<2&&o!='fail')mod=Object.assign(mod,def.fw[fac],{ic:mod.ic});
 if(mod)mod.until=w.end+(mod.days||2)*DAY;
 w.res={o,ok:w.ok.slice(),fac,top:rows.slice(0,3).map(r=>r[0]),n:rows.length,units:rows.reduce((a,r)=>a+r[1],0),mod,at:w.end}}
/* what a participant earns: tier 0 none, 1 participant, 2 contributor, 3 champion */
function rewardFor(w,acc){if(!w||!w.res)return null;const p=w.pl[acc];if(!p)return{tier:0};const def=WE[w.id];let u=0,m=0;for(let k=0;k<3;k++){const c=pcOf(def.st[k]);u+=Math.min(c,(p.s||[])[k]|0);m+=c}
 const f=u/m,tier=u<MINU?0:f>=.7?3:f>=.35?2:1,o=w.res.o,mult=o=='win'?1:o=='partial'?.7:.45,top=w.res.top.indexOf(acc);
 const wonSide=def.fac&&w.res.fac!=null&&w.res.fac<2&&p.f===w.res.fac;
 return{tier,mult:mult*(wonSide?1.2:1),o,top:top>=0&&u>=MINU?top+1:0,units:u,rel:def.relics?relicsOf(acc,w):null,side:wonSide?1:0,fac:p.f}}
/* a player's contribution message, validated: returns units actually accepted */
function contribute(w,acc,k,n,f,now){evaluate(w,now);if(w.res||now<w.start||now>=w.end)return 0;if(k!==stageAt(w,now))return 0;n=n|0;if(n<1||n>10)return 0;
 const def=WE[w.id],s=def.st[k],pc=pcOf(s);let p=w.pl[acc];if(!p){if(Object.keys(w.pl).length>=500)return 0;p=w.pl[acc]={s:[0,0,0],f:2}}
 if(def.fac&&(f===0||f===1)&&p.f===2)p.f=f;
 const room=pc-(p.s[k]|0),a=Math.min(room,n);if(a<=0)return 0;p.s[k]+=a;p.t=now;evaluate(w,now);return a}
return{DAY,T0,EP,THEMES,TRACK,WE,KINDS,RELICS,NPC,PCAP,MINU,RUNIT,pcOf,seasonFor,seasonAt,schedule,kindOf,newEvent,evaluate,totals,stageAt,stageWin,contribute,rewardFor,relicsOf,sumU,h32,cl}})();

/*@@END CORE7@@*/
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
/* v4: the bigger islands. Old home index (56x56) and New Land index (128x128) move to the middle of the new maps. */
const OLD_HT = 56 * 56;
function remapIdx(i) { if (!Number.isInteger(i) || i < 0) return i;
  if (i < OLD_HT) { const x = i % 56, y = (i / 56) | 0; return (y + OFFH) * HN + x + OFFH; }
  const j = i - OLD_HT; if (j >= 128 * 128) return -1; const x = j % 128, y = (j / 128) | 0; return HT + (y + OFFC) * NCOL + x + OFFC; }
function migrateLayout(r) {
  if (Array.isArray(r.tiles)) r.tiles = r.tiles.map(e => Array.isArray(e) ? [remapIdx(e[0]), e[1]] : e).filter(e => !Array.isArray(e) || e[0] >= 0);
  for (const p of r.pl || []) { if (!p || typeof p !== 'object') continue;
    if (Array.isArray(p.sp)) p.sp = [p.sp[0] + OFFH, p.sp[1] + OFFH];
    if (Array.isArray(p.pend)) for (const h of p.pend) if (h && Number.isInteger(h.i)) h.i = remapIdx(h.i); }
  for (const m of r.chat || []) if (m && m.loc) { const d = m.loc.r === 'c' ? OFFC : OFFH; m.loc.x += d; m.loc.y += d; }
  r.mv = 2; log('info', `island ${r.code}: map enlarged (realms kept, moved to the middle of the bigger island)`);
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
    if (!r.mv) migrateLayout(r);
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
  for (const p of r.pl) { const st = p.sp || (!p.late && START[p.s]); if (!st) continue; const [sx, sy] = st;
    for (let y = -3; y <= 3; y++) for (let x = -3; x <= 3; x++) if (x * x + y * y <= 9) {
      const i = (sy + y) * HN + sx + x; if (!r.tiles.has(i)) r.tiles.set(i, [p.s, x === 0 && y === 0 ? 1 : 0, 1, 0]); } }
}
const liveRoom = r => ({ ...r, v: r.v || SCHEMA, shield: r.shield || {}, tiles: new Map(r.tiles), pl: r.pl.map(p => ({ ...p, ws: null, mids: Array.isArray(p.mids) ? p.mids : [], tal: p.tal && 'inf' in p.tal ? { h: p.tal } : p.tal })) });
function loadRooms(list) { rooms.clear(); for (const r of migrateRooms(list)) { const room = liveRoom(r); if (room.started) seed(room); rooms.set(r.code, room); } }
try { loadRooms(JSON.parse(fs.readFileSync(RF, 'utf8'))); log('info', `loaded ${rooms.size} islands, ${Object.keys(db.users).length} accounts`); }
catch (e) { if (e.code !== 'ENOENT') log('error', 'rooms load failed: ' + e.message); }
let dirty = 0;
const snapRoom = r => ({ v: Math.max(r.v || 0, SCHEMA), mv: r.mv || 2, code: r.code, host: r.host, seed: r.seed, diff: r.diff, started: r.started, at: r.at, shield: r.shield || {},
    tiles: [...r.tiles], mkt: r.mkt, chat: (r.chat || []).slice(-80), cid: r.cid || 0,
    pl: r.pl.map(p => ({ s: p.s, u: p.u, last: p.last || 0, sp: p.sp || null, late: p.late ? 1 : 0, save: p.save, tal: p.tal, pend: p.pend, sc: p.sc || null, fx: p.fx, mids: (p.mids || []).slice(-300) })) });
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
      accounts: Object.keys(db.users).length, rooms: rooms.size, world: { season: CORE7.seasonFor(Date.now()).id, active: (() => { const s = w7Sched(Date.now()); return s.cur ? s.cur.id : null; })() }, online: wss ? wss.clients.size : 0,
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
    if (route === '/admin/world') {   // inspect, or (POST) force-start / clear a world event. Times come from this server.
      tickWorld(); if (post) { if (q('clear')) { W.force = null; } else { const id = q('kind'); if (!CORE7.WE[id]) return sendJ(400, { error: 'kind must be one of ' + CORE7.KINDS.join(', ') }); const now = Date.now(), hrs = Math.max(.002, Math.min(24 * 14, +q("hours") || CORE7.WE[id].days * 24)); W.force = { id, ep: -1, start: now, end: now + hrs * 36e5 }; }
        saveDb(); tickWorld(); w7Cast(); log('warn', 'admin: world event ' + (q('clear') ? 'override cleared' : 'forced: ' + q('kind'))); }
      return sendJ(200, { force: W.force, schedule: CORE7.schedule(Date.now()), season: CORE7.seasonFor(Date.now()), events: Object.fromEntries(Object.entries(W.ev).map(([k, e]) => [k, { ok: e.ok, players: Object.keys(e.pl).length, res: e.res && e.res.o, claims: Object.keys(e.cl || {}).length }])), hist: W.hist }); }
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

/* ---------- island snapshots (visit other players' offline islands) ----------
   One gzip file per account in DATA/snaps, mirrored to the cloud as "snap:<id>". Kept outside accounts.json so the
   account data stays small. A snapshot is display data only (terrain + owners + buildings): nothing in it is trusted. */
const RX = ['👍', '👎', '❤️', '😢', '🙏'];
const SNAPS = path.join(DATA, 'snaps'); try { fs.mkdirSync(SNAPS, { recursive: true }); } catch (e) {}
const snapMem = new Map(), snapMeta = {};
try { for (const f of fs.readdirSync(SNAPS)) if (f.endsWith('.json.gz')) snapMeta[f.slice(0, -8)] = { at: fs.statSync(path.join(SNAPS, f)).mtimeMs }; } catch (e) {}
const snapOk = d => { const ok1 = r => r && typeof r === 'object' && Number.isInteger(r.n) && r.n >= 8 && r.n <= 512 && typeof r.t === 'string' && r.t.length === r.n * r.n && (r.x == null || Array.isArray(r.x));
  return ok1(d.h) && (d.c == null || ok1(d.c)); };
const hasSnap = id => !!snapMeta[id] || !!(db.users[id] && db.users[id].snAt);
const online = id => { for (const c of wss.clients) if (c.u === id && c.readyState === 1) return true; return false; };
function putSnap(id, j) { snapMeta[id] = { at: Date.now() }; if (db.users[id] && !db.users[id].snAt) { db.users[id].snAt = Date.now(); saveDb(); } snapMem.set(id, j); if (snapMem.size > 40) snapMem.delete(snapMem.keys().next().value);
  enqueue('snap:' + id, () => fsp.writeFile(path.join(SNAPS, id + '.json.gz'), zlib.gzipSync(j))); remote.push('snap:' + id, j); }
async function getSnap(id) { if (snapMem.has(id)) return snapMem.get(id);
  try { const j = zlib.gunzipSync(await fsp.readFile(path.join(SNAPS, id + '.json.gz'))).toString(); snapMem.set(id, j); return j; } catch (e) {}
  if (remote.enabled) { try { const j = await remote.pull('snap:' + id); if (j) { snapMeta[id] = snapMeta[id] || { at: Date.now() }; snapMem.set(id, j); return j; } } catch (e) {} }
  return null; }

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
  if ((ws.pv | 0) < PROTO) return send(ws, { t: 'err', m: OLD_CLIENT, reload: 1 });
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
const CAPS = { ack: 1, px: 1, kicked: 1, world: 1, map: 2 };   // map: 2 = the bigger-island tile layout (update 18.11)

const OLD_CLIENT = 'Crownlands was updated (bigger islands). Please reload the game to continue.';
function auth(ws, m) {
  ws.pv = m.pv | 0;   // an old page is still signed in (its key is kept), but it cannot enter an island until it reloads
  if (m.t === 'resume') {
    const u = db.keys[m.k]; if (!u || !db.users[u]) return send(ws, { t: 'auth', ok: 0, err: 'Session expired' });
    ws.u = u; send(ws, { t: 'auth', ok: 1, name: dname(u), key: m.k, st: storageInfo(), caps: CAPS }); send(ws, wView(u)); if (m.lb) return; return attach(ws);
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
  ws.u = id; send(ws, { t: 'auth', ok: 1, name: dname(id), key, st: storageInfo(), caps: CAPS }); send(ws, wView(id)); attach(ws);
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
/* the index has no ceiling; it may grow by a fixed amount plus a share of itself per minute */
const scoreCap = (prev, now) => { const el = Math.max(0, (now - prev.at) / 60e3), b = prev.s || 0; return Math.floor(b + 25 + 40 * el + b * Math.min(3, .02 * el)); };
function okScore(prev, s, now) { return !prev || !prev.at || s <= scoreCap(prev, now); }
const tit = v => typeof v === 'string' && /^[A-Za-z' ]{1,28}$/.test(v) ? v : '';
const seen = (p, mid) => { if (typeof mid !== 'string' || mid.length > 40) return false; p.mids = p.mids || []; if (p.mids.includes(mid)) return true; p.mids.push(mid); if (p.mids.length > 400) p.mids.splice(0, p.mids.length - 300); return false; };
const nb = (i, f) => { if (i < HT) { const x = i % HN, y = (i / HN) | 0; return [[1, 0], [-1, 0], [0, 1], [0, -1]].some(([a, b]) => { const X = x + a, Y = y + b; return X >= 0 && Y >= 0 && X < HN && Y < HN && f(Y * HN + X); }); }
  const j = i - HT, x = j % NCOL, y = (j / NCOL) | 0; return [[1, 0], [-1, 0], [0, 1], [0, -1]].some(([a, b]) => { const X = x + a, Y = y + b; return X >= 0 && Y >= 0 && X < NCOL && Y < NCOL && f(HT + Y * NCOL + X); }); };


/* ---------- v4: world events, season claims. State lives inside the accounts data, so the existing backups,
   verification, restore API and Upstash mirror protect it too. Time always comes from this server's clock. ---------- */
db.world = db.world && typeof db.world === 'object' ? db.world : {}; const W = db.world;
W.v = 1; W.ev = W.ev && typeof W.ev === 'object' ? W.ev : {}; W.hist = Array.isArray(W.hist) ? W.hist : []; if (!('force' in W)) W.force = null;
const evKey = e => e.id + '@' + e.start;
let w7Save = 0; const w7Soon = () => { if (w7Save) return; w7Save = setTimeout(() => { w7Save = 0; saveDb(); }, 4000); };
let w7Bc = 0; const w7Cast = () => { if (w7Bc) return; w7Bc = setTimeout(() => { w7Bc = 0; for (const c of wss.clients) if (c.u && c.readyState === 1) send(c, wView(c.u)); }, 2500); };
const w7Sched = now => { const f = W.force; if (f && now < f.end + 8 * DAY && f.id && CORE7.WE[f.id]) { const cur = now >= f.start && now < f.end ? f : null; if (cur || now < f.start) return { cur, next: cur ? null : f }; } return CORE7.schedule(now); };
function tickWorld() { const now = Date.now(); let ch = false;
  const sc = w7Sched(now); if (sc.cur) { const k = evKey(sc.cur); if (!W.ev[k]) { W.ev[k] = CORE7.newEvent(sc.cur); ch = true; log('info', `world event started: ${CORE7.WE[sc.cur.id].n}`); } }
  for (const k of Object.keys(W.ev)) { const e = W.ev[k]; if (!e || !CORE7.WE[e.id]) { delete W.ev[k]; ch = true; continue; } if (e.res) continue;
    CORE7.evaluate(e, now); if (e.res) { ch = true; W.hist.unshift({ key: k, id: e.id, o: e.res.o, at: e.end, n: e.res.n, units: e.res.units }); if (W.hist.length > 30) W.hist.length = 30; log('info', `world event ended: ${CORE7.WE[e.id].n} - ${e.res.o}, ${e.res.n} participants`); } }
  for (const k of Object.keys(W.ev)) if (W.ev[k].res && now - W.ev[k].end > 75 * DAY) { delete W.ev[k]; ch = true; }   // old finished events are dropped, their summary stays in the history
  if (ch) { saveDb(); w7Cast(); } }
function wView(acc) { const now = Date.now(); let cur = null, rec = [];
  for (const k of Object.keys(W.ev)) { const e = W.ev[k], my = e.pl[acc];
    const v = { key: k, id: e.id, start: e.start, end: e.end, ok: e.ok, stage: CORE7.stageAt(e, now), tot: [0, 1, 2].map(i => CORE7.totals(e, i, Math.min(now, e.end))), n: Object.keys(e.pl).length,
      my: my ? { s: my.s, f: my.f } : null };
    if (!e.res) cur = v;
    else { v.res = e.res; const rw = my ? CORE7.rewardFor(e, acc) : null; v.my = my ? { s: my.s, f: my.f, rw, claimed: !!(e.cl && e.cl[acc]) } : null; if (now - e.end < 12 * DAY || (v.my && v.my.rw && v.my.rw.tier && !v.my.claimed)) rec.push(v); } }
  const sch = w7Sched(now), mods = []; for (const k of Object.keys(W.ev)) { const m = W.ev[k].res && W.ev[k].res.mod; if (m && m.until > now) mods.push(Object.assign({ key: k }, m)); }
  const u = db.users[acc] || {}, sc = u.x7 && u.x7.sc || {};
  return { t: 'world', now, cur, recent: rec.sort((a, b) => b.end - a.end).slice(0, 4), next: sch.cur ? CORE7.schedule(sch.cur.end + 1).next : sch.next, hist: W.hist.slice(0, 10), mods, sc, acc }; }
const w7Rate = (o, key, ms) => { const n = Date.now(); if (o[key] && n - o[key] < ms) return true; o[key] = n; return false; };
const dupAcc = (u, mid) => { if (typeof mid !== 'string' || mid.length > 40) return true; const x = u.x7 = u.x7 || {}; x.mids = x.mids || []; if (x.mids.includes(mid)) return true; x.mids.push(mid); if (x.mids.length > 120) x.mids.splice(0, x.mids.length - 80); return false; };
const w7Mins = {};
function w7Handle(ws, m) {
  const u = db.users[ws.u]; if (!u) return;
  if (m.t === 'we') { if (w7Rate(ws, 'weAt', 1500)) return; tickWorld(); return send(ws, wView(ws.u)); }
  if (m.t === 'wec') {   // a contribution to the current stage. Counted once per message id; capped per stage and per minute.
    const now = Date.now(); tickWorld(); const sc = w7Sched(now), e = sc.cur && W.ev[evKey(sc.cur)]; if (!e || e.res) return send(ws, { t: 'wecok', mid: m.mid, a: 0, why: 'no event' });
    if (dupAcc(u, m.mid)) return send(ws, { t: 'wecok', mid: m.mid, a: 0, dup: 1 });
    const mm = w7Mins[ws.u] = (w7Mins[ws.u] || []).filter(q => now - q[0] < 60e3), used = mm.reduce((a, q) => a + q[1], 0);
    if (!Number.isInteger(m.k) || m.k < 0 || m.k > 2 || !Number.isInteger(m.n) || m.n < 1 || m.n > 10 || used + m.n > 60) { bad(ws, 'event contribution'); return send(ws, { t: 'wecok', mid: m.mid, a: 0, why: 'rejected' }); }
    const a = CORE7.contribute(e, ws.u, m.k, m.n, m.f, now); if (a) mm.push([now, a]);
    if (a) { w7Soon(); w7Cast(); } const p = e.pl[ws.u]; return send(ws, { t: 'wecok', mid: m.mid, a, k: m.k, my: p ? { s: p.s, f: p.f } : null }); }
  if (m.t === 'wecl') {   // claim the reward of a finished event: granted at most once per account
    if (w7Rate(ws, 'clAt', 700)) return; const e = typeof m.key === 'string' && W.ev[m.key]; if (!e || !e.res) return send(ws, { t: 'weclok', key: m.key, no: 1 });
    const rw = CORE7.rewardFor(e, ws.u); e.cl = e.cl || {}; const had = !!e.cl[ws.u];
    if (!rw || !rw.tier) return send(ws, { t: 'weclok', key: m.key, tier: 0 });
    if (!had) { e.cl[ws.u] = Date.now(); saveDb(); log('info', `world event reward claimed: ${ws.u} ${m.key} tier ${rw.tier}`); }
    return send(ws, { t: 'weclok', key: m.key, id: e.id, rw, d: CORE7.WE[e.id].rw[rw.tier - 1], dup: had ? 1 : 0 }); }
  if (m.t === 'scl') {   // season milestone claim: once per account per season, current or previous season only
    if (w7Rate(ws, 'sclAt', 500)) return; const now = Date.now(), cur = CORE7.seasonFor(now), ok = m.sid === cur.id || m.sid === 's' + (cur.idx - 1);
    const tr = CORE7.TRACK.find(t => t.id === m.m); if (!ok || !tr) { bad(ws, 'season claim'); return send(ws, { t: 'sclok', sid: m.sid, m: m.m, no: 1 }); }
    const x = u.x7 = u.x7 || {}; x.sc = x.sc || {}; const L = x.sc[m.sid] = x.sc[m.sid] || [], had = L.includes(m.m);
    if (!had) { L.push(m.m); saveDb(); }
    return send(ws, { t: 'sclok', sid: m.sid, m: m.m, dup: had ? 1 : 0 }); }
}
setInterval(tickWorld, 30000); setTimeout(tickWorld, 1500);
function handle(ws, m) {
  if (m.t === 'auth' || m.t === 'resume') return auth(ws, m);
  if (!ws.u) return;
  if (m.t === 'we' || m.t === 'wec' || m.t === 'wecl' || m.t === 'scl') return w7Handle(ws, m);
  const r = ws.room, me = r && r.pl.find(p => p.u === ws.u);
  if ((ws.pv | 0) < PROTO && !['lb', 'gscore', 'leave'].includes(m.t)) { if (!ws.oldWarn) { ws.oldWarn = 1; send(ws, { t: 'err', m: OLD_CLIENT, reload: 1 }); } return; }
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
        if (x.pl.length >= MAXP) return send(ws, { t: 'err', m: `Island is full (${MAXP}/${MAXP})` });
        leave(ws); const s = [1, 2, 3, 4, 5, 6, 7].find(n => !x.pl.some(p => p.s === n));
        x.pl.push({ s, u: ws.u, ws, last: Date.now(), mids: [], late: x.started ? 1 : 0 });   // joining a started island: land comes from 'spawn'
        if (x.started) { log('info', `player joined a started island: ${x.code} slot ${s}`); x.pl.forEach(p => { if (p.u !== ws.u) send(p.ws, { t: 'joined', s, n: dname(ws.u) }); }); }
      } else x.pl.find(p => p.u === ws.u).last = Date.now();
      touch(x); return attach(ws);
    }
    case 'start': if (r && !r.started && r.host === ws.u && r.pl.length >= 2) { r.started = true; seed(r); touch(r); r.pl.forEach(p => send(p.ws, roomMsg(r, p, true))); log('info', `island started: ${r.code} (${r.pl.length} players)`); } else if (r && r.host === ws.u) send(ws, { t: 'err', m: 'Need at least 2 players' }); return;
    case 'spawn': { // a realm with no land on the home island gets a starting circle (radius 3) where its game chose
      if (!r || !r.started || !me || !num(m.c, 0, HT - 1) || !Array.isArray(m.d)) return bad(ws, 'spawn');
      const now = Date.now(); if (me.spAt && now - me.spAt < 3000) return; me.spAt = now;
      for (const [i, v] of r.tiles) if (i < HT && v[0] === me.s) return send(ws, { t: 'spawnok', had: 1 });   // already has land
      const cx = m.c % HN, cy = (m.c / HN) | 0, held = i => { const c = r.tiles.get(i); return c && c[0] && c[0] !== me.s && c[1]; };
      if (cx < 3 || cy < 3 || cx > HN - 4 || cy > HN - 4 || held(m.c)) return send(ws, { t: 'spawnno' });
      const set = new Set([m.c]);
      for (const i of m.d.slice(0, 40)) { if (!num(i, 0, HT - 1)) continue; const x = i % HN, y = (i / HN) | 0; if ((x - cx) ** 2 + (y - cy) ** 2 <= 9) set.add(i); }
      const ok = [];
      for (const i of set) { if (held(i)) continue;            // other players' buildings are never taken
        const v = [me.s, i === m.c ? 1 : 0, 1, 0]; r.tiles.set(i, v); ok.push([i, ...v]); }
      me.sp = [cx, cy]; me.late = 0; log('info', `spawned: room ${r.code} slot ${me.s} at ${cx},${cy} (${ok.length} tiles)`);
      r.pl.forEach(p => send(p.ws, { t: 'tiles', d: ok, sp: me.s })); send(ws, { t: 'spawnok' }); touch(r); bc(r); return;
    }
    case 'leave': return leave(ws);
    case 'trade': { // a trade at the island's shared market; filled along the price curve like the offline market
      if (!r || !r.started || !me) return; if (!r.mkt) r.mkt = mkNew(); const m0 = r.mkt, res = String(m.r), n = Math.floor(+m.n);
      if (!MRES.includes(res) || !(n >= 1 && n <= 1e12) || (m.side !== 'buy' && m.side !== 'sell')) return bad(ws, 'trade');
      ws.tids = ws.tids || new Set(); if (m.id != null) { if (ws.tids.has(m.id)) return; ws.tids.add(m.id); if (ws.tids.size > 200) ws.tids = new Set([...ws.tids].slice(-100)); }
      const mid = m0.price[res]; let px;
      if (m.side === 'sell') { const imp = Math.min(.3, n * MI[res] * .5); px = mid * (1 - imp / 2); m0.mk[res] = Math.min(MM0[res] * 3, m0.mk[res] + n); m0.price[res] = cl(mid * (1 - imp), MB[res] * .35, MB[res] * 2.8); }
      else { const imp = Math.min(.25, n * MI[res] * .4); px = mid * (1 + imp / 2); m0.mk[res] = Math.max(0, m0.mk[res] - n); m0.price[res] = cl(mid * (1 + imp), MB[res] * .35, MB[res] * 2.8); }
      m0.feed.push({ n: dname(me.u), s: m.side, r: res, q: n, at: Date.now() }); if (m0.feed.length > 8) m0.feed.shift();
      send(ws, { t: 'tr', id: m.id, r: res, side: m.side, n, mid: +mid.toFixed(3), px: +px.toFixed(4) }); const o = mkPub(m0); r.pl.forEach(p => send(p.ws, o)); touch(r); return;
    }
    case 'score': { // my island's progress score (shown in the island ranking and the world ranking)
      if (!r || !r.started || !me || !m.s || typeof m.s !== 'object') return; const now = Date.now(); if (me.scAt && now - me.scAt < 8000) return; me.scAt = now;
      let s = cl(Math.floor(+m.s.s) || 0, 0, SCORE_MAX); const p = Array.isArray(m.s.p) ? m.s.p.slice(0, 10).map(v => cl(Math.floor(+v) || 0, 0, 1e12)) : null;
      if (!okScore(me.sc, s, now)) { bad(ws, 'score jump'); s = scoreCap(me.sc, now); }
      me.sc = { s, p, d: num(m.s.d, 0, 4) ? m.s.d : 1, at: now, ti: tit(m.s.ti) }; const u = db.users[ws.u]; if (u) { u.is = { s, p, d: me.sc.d, at: now, ti: me.sc.ti }; saveDb(); } bc(r); touch(r); return;
    }
    case 'gscore': { // an offline (solo) island of a logged-in player
      const u = db.users[ws.u]; if (!u || !m.s || typeof m.s !== 'object') return; const now = Date.now(); if (ws.gsAt && now - ws.gsAt < 8000) return; ws.gsAt = now;
      let s = cl(Math.floor(+m.s.s) || 0, 0, SCORE_MAX); const p = Array.isArray(m.s.p) ? m.s.p.slice(0, 10).map(v => cl(Math.floor(+v) || 0, 0, 1e12)) : null;
      if (u.gs && !okScore(u.gs, s, now)) { bad(ws, 'solo score jump'); s = scoreCap(u.gs, now); }
      u.gs = { s, p, d: num(m.s.d, 0, 4) ? m.s.d : 1, at: now, ti: tit(m.s.ti) }; saveDb(); return;
    }
    case 'lb': { // world ranking: every account's best island
      const rows = []; for (const id in db.users) { const u = db.users[id], a = u.gs, b = u.is; const best = !a ? (b ? [b, 'online'] : null) : !b ? [a, 'solo'] : (b.s > a.s ? [b, 'online'] : [a, 'solo']); if (best && best[0].s > 0) rows.push({ id, n: u.name || id, s: Math.min(SCORE_MAX, best[0].s), m: best[1], p: best[0].p, d: best[0].d, ti: best[0].ti || '', vi: hasSnap(id) ? 1 : 0, on: online(id) ? 1 : 0 }); }
      rows.sort((x, y) => y.s - x.s); const mine = rows.findIndex(x => x.id === ws.u);
      send(ws, { t: 'lb', top: rows.slice(0, 50), total: rows.length, me: mine < 0 ? null : { rank: mine + 1, s: rows[mine].s, m: rows[mine].m } }); return;
    }
    case 'fx': { // my look (picture, nickname effect, realm colour, building aura) so everyone in the room sees it
      if (!r || !me || !m.fx || typeof m.fx !== 'object') return; const now = Date.now(); if (me.fxAt && now - me.fxAt < 1500) return; me.fxAt = now;
      const fx = {}; for (const [k, mx] of [['pf', 13], ['nf', 9], ['rc', 9], ['au', 6]]) { const q = m.fx[k]; if (Number.isInteger(q) && q >= 0 && q < mx) fx[k] = q; }
      me.fx = Object.keys(fx).length ? fx : null; bc(r); touch(r); return;
    }
    case 'chat': {
      if (!r || !r.started || !me) return; const now = Date.now(); if (me.chAt && now - me.chAt < 900) return; me.chAt = now;
      const text = String(m.m || '').replace(/[\u0000-\u001f]/g, ' ').trim().slice(0, 200); if (!text) return;
      let loc = null; if (m.loc && typeof m.loc === 'object' && (m.loc.r === 'h' || m.loc.r === 'c') && num(m.loc.x, 0, NCOL - 1) && num(m.loc.y, 0, NCOL - 1)) loc = { r: m.loc.r, x: m.loc.x, y: m.loc.y, t: String(m.loc.t || '').slice(0, 40) };
      const msg = { id: r.cid = (r.cid || 0) + 1, s: me.s, n: dname(me.u), m: text, at: now }; if (loc) msg.loc = loc;
      if (m.fx && typeof m.fx === 'object') { const fx = {}; for (const [k, mx] of [['pf', 13], ['nf', 9], ['mf', 8]]) { const q = m.fx[k]; if (Number.isInteger(q) && q >= 0 && q < mx) fx[k] = q; } if (Object.keys(fx).length) msg.fx = fx; }
      if (me.fx) { msg.fx = msg.fx || {}; for (const k of ['pf', 'nf']) if (me.fx[k] != null) msg.fx[k] = me.fx[k]; }
      (r.chat = r.chat || []).push(msg); if (r.chat.length > 120) r.chat.splice(0, r.chat.length - 120);
      r.pl.forEach(p => send(p.ws, { t: 'chat', msg })); touch(r); return;
    }
    case 'react': { // an emoji reaction on a chat message: 0 thumbs up, 1 thumbs down, 2 heart, 3 crying, 4 please. Tapping again removes it.
      if (!r || !r.started || !me || !num(m.id, 1, 1e9) || !num(m.e, 0, RX.length - 1)) return bad(ws, 'react');
      const now = Date.now(); if (me.rxAt && now - me.rxAt < 250) return; me.rxAt = now;
      const msg = (r.chat || []).find(q => q.id === m.id); if (!msg) return;
      const rx = msg.rx = msg.rx && typeof msg.rx === 'object' ? msg.rx : {}, k = String(m.e), L = rx[k] = Array.isArray(rx[k]) ? rx[k] : [], at = L.indexOf(me.s);
      if (at >= 0) L.splice(at, 1); else L.push(me.s);
      if (!L.length) delete rx[k]; if (!Object.keys(rx).length) delete msg.rx;
      r.pl.forEach(p => send(p.ws, { t: 'react', id: msg.id, rx: msg.rx || {} })); touch(r); return;
    }
    case 'gsnap': { // a compact picture of the player's offline island, so other players can visit it (read only)
      const u = db.users[ws.u]; if (!u || !m.d || typeof m.d !== 'object') return; const now = Date.now(); if (ws.snAt && now - ws.snAt < 60e3) return;
      let j = ''; try { j = JSON.stringify(m.d); } catch (e) { return bad(ws, 'snapshot'); }
      if (j.length > 900000 || !snapOk(m.d)) return bad(ws, 'snapshot'); ws.snAt = now; putSnap(ws.u, j); return;
    }
    case 'visit': { // read someone's offline island
      const id = String(m.id || '').toLowerCase(); if (!db.users[id]) return send(ws, { t: 'isle', id, no: 1 });
      const now = Date.now(); if (ws.viAt && now - ws.viAt < 1500) return; ws.viAt = now;
      getSnap(id).then(j => send(ws, j ? { t: 'isle', id, n: dname(id), d: JSON.parse(j), at: (snapMeta[id] || {}).at || 0, sc: (db.users[id].gs || {}).s || 0, p: (db.users[id].gs || {}).p || null } : { t: 'isle', id, no: 1 })).catch(() => send(ws, { t: 'isle', id, no: 1 }));
      return;
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
          if (po >= 1 && po <= MAXP && (r.shield[po] || 0) > now) { badT.push([i, ...(cur || [po, pb, 1, 0])]); blocked = po; continue; }
          conq.set(po, Math.max(conq.get(po) || 0, pb === 1 || pb === 3 ? SHIELD_BIG : 1));
        } else if (!po && i < HT && !cq) { claims.push([i, o, b, l, w]); continue; }   // a neutral claim on the home island: must touch the realm
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
      if (!r || !r.started || !me || !num(m.to, 1, MAXP) || !num(m.i, 0, NT - 1)) return bad(ws, m.t);
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
