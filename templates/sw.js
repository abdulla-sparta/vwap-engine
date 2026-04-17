const CACHE = 'se-v1';
const OFFLINE = ['/mobile', '/static/manifest.json'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(OFFLINE)));
  self.skipWaiting();
});

self.addEventListener('activate', e => e.waitUntil(clients.claim()));

self.addEventListener('fetch', e => {
  if (
    e.request.url.includes('/socket.io') ||
    e.request.url.includes('/live_status') ||
    e.request.url.includes('/auth')
  ) return;

  e.respondWith(
    fetch(e.request).catch(() => caches.match(e.request))
  );
});