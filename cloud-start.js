// Start script for cloud hosting: restore the saved game from the free Upstash database, then run the server.
const fs = require('fs'), path = require('path'), remote = require('./remote');
(async () => {
  if (remote.enabled) {
    const DATA = process.env.DATA_DIR || __dirname; fs.mkdirSync(DATA, { recursive: true });
    for (const [n, f] of [['accounts', 'accounts.json'], ['rooms', 'rooms.json']]) {
      try { const v = await remote.pull(n); if (v) { fs.writeFileSync(path.join(DATA, f), v); console.log('[remote] restored ' + n); } }
      catch (e) { console.error('[remote] could not restore ' + n + ': ' + e.message); }
    }
    process.on('SIGTERM', async () => { await remote.flushAll(); process.exit(0); });
  } else console.log('[remote] no Upstash settings found: data is only kept on this machine\'s disk');
  require('./server.js');
})();
