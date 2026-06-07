/**
 * Standalone script to generate search-index.json
 * 
 * Can be run directly via: npx tsx scripts/build-search-index.ts
 * Or via npm script: npm run build:search-index
 *
 * This is also integrated into the Astro build pipeline via the
 * searchIndexIntegration in src/integrations/search-index.ts
 */

import path from 'node:path';
import fs from 'node:fs';
import { buildSearchIndex } from '../src/integrations/search-index.ts';

const rootDir = path.resolve(import.meta.dirname, '..');
const outputPath = path.join(rootDir, 'public', 'search-index.json');

console.log('[build-search-index] Starting search index generation...');
console.log(`[build-search-index] Root directory: ${rootDir}`);

try {
  const entries = buildSearchIndex(rootDir);

  // Ensure public directory exists
  const publicDir = path.dirname(outputPath);
  if (!fs.existsSync(publicDir)) {
    fs.mkdirSync(publicDir, { recursive: true });
  }

  fs.writeFileSync(outputPath, JSON.stringify(entries, null, 2), 'utf-8');

  console.log(`[build-search-index] Success: ${entries.length} entries written to ${outputPath}`);

  // Print summary
  const topicCounts = new Map<string, number>();
  for (const entry of entries) {
    const count = topicCounts.get(entry.topicDisplayName) || 0;
    topicCounts.set(entry.topicDisplayName, count + 1);
  }

  if (topicCounts.size > 0) {
    console.log('\n[build-search-index] Index summary by topic:');
    for (const [topic, count] of [...topicCounts.entries()].sort((a, b) => b[1] - a[1])) {
      console.log(`  ${topic}: ${count} entries`);
    }
  }
} catch (error) {
  console.error(`[build-search-index] Error: ${(error as Error).message}`);
  process.exit(1);
}
