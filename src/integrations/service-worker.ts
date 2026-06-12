/**
 * Astro Integration: Service Worker Builder
 *
 * Bundles src/sw.ts into the output directory and injects a precache manifest
 * containing all static assets from the build. Uses esbuild (available via Astro/Vite)
 * to bundle the service worker with workbox dependencies.
 *
 * The integration:
 * 1. After the Astro build completes, scans the output directory for static files
 * 2. Generates a precache manifest (URL + revision hash pairs)
 * 3. Bundles src/sw.ts with the manifest injected in place of self.__WB_MANIFEST
 * 4. Writes the bundled sw.js to the output (dist/) directory
 *
 * Requirements: 10.1, 10.2, 10.5, 10.6
 */

import type { AstroIntegration } from 'astro';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { build as esbuildBuild } from 'esbuild';

interface ManifestEntry {
  url: string;
  revision: string;
}

/**
 * Recursively list all files in a directory.
 */
function getAllFiles(dir: string, baseDir: string): string[] {
  const results: string[] = [];
  if (!fs.existsSync(dir)) return results;

  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...getAllFiles(fullPath, baseDir));
    } else if (entry.isFile()) {
      results.push(fullPath);
    }
  }
  return results;
}

/**
 * Generate a short revision hash for a file based on its content.
 */
function getFileRevision(filePath: string): string {
  const content = fs.readFileSync(filePath);
  return crypto.createHash('md5').update(content).digest('hex').slice(0, 8);
}

/**
 * File extensions to include in the precache manifest.
 */
const CACHEABLE_EXTENSIONS = new Set([
  '.html',
  '.css',
  '.js',
  '.mjs',
  '.json',
  '.svg',
  '.png',
  '.jpg',
  '.jpeg',
  '.gif',
  '.webp',
  '.woff',
  '.woff2',
  '.ttf',
  '.ico',
]);

/**
 * Files to exclude from precaching (the SW itself, source maps, etc.)
 */
function shouldExclude(relativePath: string): boolean {
  if (relativePath === 'sw.js') return true;
  if (relativePath.endsWith('.map')) return true;
  if (relativePath.startsWith('.')) return true;
  return false;
}

/**
 * Build the precache manifest from the output directory.
 */
function buildManifest(outputDir: string): ManifestEntry[] {
  const files = getAllFiles(outputDir, outputDir);
  const manifest: ManifestEntry[] = [];

  for (const filePath of files) {
    const ext = path.extname(filePath).toLowerCase();
    if (!CACHEABLE_EXTENSIONS.has(ext)) continue;

    // Relative URLs (no leading slash) so Workbox resolves them against the
    // service worker's own scope — required when the site is served from a
    // sub-path like GitHub Pages' /<repo>/.
    const relativePath = path.relative(outputDir, filePath).replace(/\\/g, '/');
    if (shouldExclude(relativePath)) continue;

    manifest.push({
      url: relativePath,
      revision: getFileRevision(filePath),
    });
  }

  return manifest;
}

/**
 * Bundle src/sw.ts with esbuild, injecting the precache manifest in place of
 * self.__WB_MANIFEST. All workbox dependencies are bundled locally — no CDN needed.
 */
async function generateServiceWorkerBundle(
  manifest: ManifestEntry[],
  projectRoot: string
): Promise<string> {
  const swSourcePath = path.join(projectRoot, 'src', 'sw.ts');
  const manifestJson = JSON.stringify(manifest);

  const result = await esbuildBuild({
    entryPoints: [swSourcePath],
    bundle: true,
    format: 'iife',
    platform: 'browser',
    target: 'es2017',
    write: false,
    define: {
      // Replace self.__WB_MANIFEST with the actual manifest array
      'self.__WB_MANIFEST': manifestJson,
    },
    minify: false,
  });

  const code = result.outputFiles[0].text;
  return `/* Service Worker - DE Interview Prep App (auto-generated, do not edit) */\n${code}`;
}

/**
 * Astro integration that generates a service worker with precache manifest after build.
 */
export function serviceWorkerIntegration(): AstroIntegration {
  return {
    name: 'service-worker-builder',
    hooks: {
      'astro:build:done': async ({ dir, logger }) => {
        const outputDir = fileURLToPath(dir).replace(/\/$/, '');
        const normalizedDir = outputDir.startsWith('/') && process.platform === 'win32'
          ? outputDir.slice(1)
          : outputDir;
        const projectRoot = path.resolve(normalizedDir, '..');

        logger.info('Building service worker with precache manifest...');

        try {
          // Build the precache manifest from all output files
          const manifest = buildManifest(normalizedDir);
          logger.info(`Found ${manifest.length} files to precache.`);

          // Bundle src/sw.ts with all workbox deps included locally (no CDN)
          const swContent = await generateServiceWorkerBundle(manifest, projectRoot);

          // Write sw.js to the output directory root
          const swOutputPath = path.join(normalizedDir, 'sw.js');
          fs.writeFileSync(swOutputPath, swContent, 'utf-8');

          logger.info(`Service worker written to: ${swOutputPath}`);
        } catch (error) {
          logger.error(`Failed to build service worker: ${(error as Error).message}`);
          // Don't throw - allow build to complete without SW if there's an issue
          // The registration script will handle the missing SW gracefully
          logger.warn('Build will continue without service worker. Offline access will be unavailable.');
        }
      },

      'astro:server:setup': async ({ logger }) => {
        // During dev, create a no-op service worker for testing registration
        const rootDir = process.cwd();
        const publicDir = path.join(rootDir, 'public');
        const swPath = path.join(publicDir, 'sw.js');

        // Only write dev SW if it doesn't already exist
        if (!fs.existsSync(swPath)) {
          const devSw = `// Development service worker (no-op)\n// In production, this is replaced by the build integration.\nself.addEventListener('install', () => self.skipWaiting());\nself.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));\n`;
          fs.writeFileSync(swPath, devSw, 'utf-8');
          logger.info('Created development service worker at public/sw.js');
        }
      },
    },
  };
}

export default serviceWorkerIntegration;
