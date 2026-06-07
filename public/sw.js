// Development service worker (no-op)
// In production, this is replaced by the build integration with a full
// precache manifest and workbox runtime caching strategies.
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));
