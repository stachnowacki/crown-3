const C='crownlands-v9',F=['./','index.html','manifest.webmanifest','icon.svg','icon-192.png','icon-512.png','crownlands-pwa-icon.svg'];
self.addEventListener('install',e=>e.waitUntil(caches.open(C).then(c=>c.addAll(F)).catch(()=>{})));
self.addEventListener('activate',e=>e.waitUntil(caches.keys().then(k=>Promise.all(k.filter(x=>x!=C).map(x=>caches.delete(x))))));
self.addEventListener('fetch',e=>{if(e.request.method!=='GET'||e.request.headers.get('upgrade'))return;e.respondWith(fetch(e.request).catch(()=>caches.match(e.request)))});
