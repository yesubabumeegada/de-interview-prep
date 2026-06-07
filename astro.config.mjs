import { defineConfig } from 'astro/config';
import react from '@astrojs/react';
import tailwind from '@astrojs/tailwind';
import remarkGfm from 'remark-gfm';
import rehypePrismPlus from 'rehype-prism-plus';
import remarkMermaid from './src/plugins/remarkMermaid.ts';
import { validateContentFrontmatter } from './src/plugins/validateContent.ts';
import searchIndexIntegration from './src/integrations/search-index.ts';
import serviceWorkerIntegration from './src/integrations/service-worker.ts';
import contentValidatorIntegration from './src/integrations/content-validator.ts';
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

/**
 * Custom Astro integration that validates topics.config.json at build time.
 * Fails the build if the config is missing or contains malformed JSON.
 * Requirements: 9.5, 9.6
 */
function validateTopicsConfig() {
  return {
    name: 'validate-topics-config',
    hooks: {
      'astro:config:setup'({ logger }) {
        const configPath = resolve(process.cwd(), 'content/topics.config.json');

        if (!existsSync(configPath)) {
          throw new Error(
            `[validate-topics-config] Build failed: Configuration file not found at "${configPath}". ` +
            `The topics.config.json file is required and must be located in the content/ directory.`
          );
        }

        let configContent;
        try {
          configContent = readFileSync(configPath, 'utf-8');
        } catch (err) {
          throw new Error(
            `[validate-topics-config] Build failed: Unable to read configuration file at "${configPath}". ` +
            `Error: ${err instanceof Error ? err.message : String(err)}`
          );
        }

        let config;
        try {
          config = JSON.parse(configContent);
        } catch (err) {
          throw new Error(
            `[validate-topics-config] Build failed: Malformed JSON in configuration file "${configPath}". ` +
            `Parse error: ${err instanceof Error ? err.message : String(err)}`
          );
        }

        // Validate basic structure
        if (!config.topics || !Array.isArray(config.topics)) {
          throw new Error(
            `[validate-topics-config] Build failed: Configuration file "${configPath}" must contain a "topics" array at the root level.`
          );
        }

        if (config.topics.length === 0) {
          throw new Error(
            `[validate-topics-config] Build failed: Configuration file "${configPath}" contains an empty "topics" array. At least one topic is required.`
          );
        }

        // Validate each topic has required fields
        for (const topic of config.topics) {
          if (!topic.id || typeof topic.id !== 'string') {
            throw new Error(
              `[validate-topics-config] Build failed: Each topic in "${configPath}" must have a string "id" field. Found: ${JSON.stringify(topic)}`
            );
          }
          if (!topic.displayName || typeof topic.displayName !== 'string') {
            throw new Error(
              `[validate-topics-config] Build failed: Topic "${topic.id}" is missing a "displayName" string field.`
            );
          }
          if (typeof topic.order !== 'number') {
            throw new Error(
              `[validate-topics-config] Build failed: Topic "${topic.id}" is missing a numeric "order" field.`
            );
          }
          if (!Array.isArray(topic.subtopics)) {
            throw new Error(
              `[validate-topics-config] Build failed: Topic "${topic.id}" is missing a "subtopics" array field.`
            );
          }
        }

        logger.info(`Validated topics.config.json: ${config.topics.length} topics found.`);
      },
    },
  };
}

// https://astro.build/config
export default defineConfig({
  site: 'https://yesubabumeegada.github.io',
  base: '/de-interview-prep',
  integrations: [
    validateTopicsConfig(),
    validateContentFrontmatter(),
    contentValidatorIntegration(),
    searchIndexIntegration(),
    serviceWorkerIntegration(),
    react(),
    tailwind({
      configFile: './tailwind.config.cjs',
    }),
  ],
  markdown: {
    remarkPlugins: [remarkGfm, remarkMermaid],
    rehypePlugins: [
      [rehypePrismPlus, { ignoreMissing: true, showLineNumbers: true }],
    ],
    shikiConfig: {
      theme: 'github-dark',
    },
  },
  output: 'static',
  build: {
    format: 'directory',
    // Performance: Inline small CSS/JS assets to reduce HTTP requests
    inlineStylesheets: 'auto',
  },
  // Performance: Enable asset compression and optimization
  compressHTML: true,
  vite: {
    build: {
      // Performance: Enable CSS code splitting for better caching
      cssCodeSplit: true,
      // Performance: Set reasonable chunk size warning limit
      chunkSizeWarningLimit: 500,
      // Performance: Optimize asset handling
      assetsInlineLimit: 4096, // Inline assets smaller than 4KB as base64
      rollupOptions: {
        output: {
          // Performance: Manual chunk splitting for better caching
          manualChunks: {
            'search': ['fuse.js'],
            'react-vendor': ['react', 'react-dom'],
          },
          // Performance: Use hashed filenames for long-term caching
          assetFileNames: 'assets/[name].[hash][extname]',
          chunkFileNames: 'chunks/[name].[hash].js',
          entryFileNames: 'entries/[name].[hash].js',
        },
      },
      // Performance: Enable minification
      minify: 'esbuild',
      // Performance: Generate source maps only in development
      sourcemap: false,
    },
    // Performance: Optimize dependency pre-bundling
    optimizeDeps: {
      include: ['react', 'react-dom', 'fuse.js'],
    },
  },
});
