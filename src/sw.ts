/**
 * Service Worker - Offline support using Workbox
 *
 * Uses workbox-precaching to cache all static assets (HTML, CSS, JS, search index,
 * images, and content) at build time for full offline functionality.
 * Uses workbox-routing for runtime caching strategies.
 *
 * Capabilities:
 * - Precaches all build-time static assets for offline access
 * - Routes navigation requests to precached HTML pages
 * - Caches runtime image/font requests with cache-first strategy
 * - Enables offline navigation, search, and progress tracking
 *
 * Requirements: 10.1, 10.2, 10.5, 10.6, 10.7
 */

import { precacheAndRoute, cleanupOutdatedCaches } from 'workbox-precaching';
import { registerRoute, NavigationRoute } from 'workbox-routing';
import { CacheFirst, StaleWhileRevalidate, NetworkFirst } from 'workbox-strategies';
import { ExpirationPlugin } from 'workbox-expiration';
import { CacheableResponsePlugin } from 'workbox-cacheable-response';

declare const self: ServiceWorkerGlobalScope & {
  __WB_MANIFEST: Array<{ url: string; revision: string | null } | string>;
};

// Clean up old caches from previous versions
cleanupOutdatedCaches();

// Precache all static assets injected at build time
// The __WB_MANIFEST placeholder is replaced by the build integration with the actual file list
precacheAndRoute(self.__WB_MANIFEST || []);

// Cache-first strategy for images (long-lived assets)
registerRoute(
  ({ request }) => request.destination === 'image',
  new CacheFirst({
    cacheName: 'images-cache',
    plugins: [
      new CacheableResponsePlugin({ statuses: [0, 200] }),
      new ExpirationPlugin({
        maxEntries: 200,
        maxAgeSeconds: 30 * 24 * 60 * 60, // 30 days
      }),
    ],
  })
);

// Stale-while-revalidate for fonts
registerRoute(
  ({ request }) => request.destination === 'font',
  new StaleWhileRevalidate({
    cacheName: 'fonts-cache',
    plugins: [
      new CacheableResponsePlugin({ statuses: [0, 200] }),
      new ExpirationPlugin({
        maxEntries: 30,
        maxAgeSeconds: 365 * 24 * 60 * 60, // 1 year
      }),
    ],
  })
);

// Network-first for the search index (ensures fresh data when online)
registerRoute(
  ({ url }) => url.pathname.endsWith('/search-index.json'),
  new NetworkFirst({
    cacheName: 'search-index-cache',
    plugins: [
      new CacheableResponsePlugin({ statuses: [0, 200] }),
    ],
  })
);

// Handle navigation requests - serve precached pages offline
const navigationHandler = new NetworkFirst({
  cacheName: 'navigation-cache',
  plugins: [
    new CacheableResponsePlugin({ statuses: [0, 200] }),
  ],
});

registerRoute(
  new NavigationRoute(navigationHandler)
);

// Listen for messages from the main thread
self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});
