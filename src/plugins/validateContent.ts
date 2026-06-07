/**
 * Astro integration plugin that validates markdown front-matter at build time.
 * 
 * Scans all .md files in the content/topics/ directory, validates their
 * front-matter against the schema, and:
 * - Logs warnings for files with invalid front-matter (identifying the file and error)
 * - Skips invalid files (they won't be processed by Astro content collections)
 * - Continues the build with all valid content files
 * 
 * Requirements: 9.4 - Log warning and skip files with invalid front-matter
 */
import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { resolve, join, relative } from 'node:path';
import { validateFrontmatter } from '../utils/frontmatterValidator.ts';
import { parseFrontmatter as sharedParseFrontmatter } from '../utils/parseFrontmatter.ts';

/**
 * Recursively finds all .md files in a directory.
 */
function findMarkdownFiles(dir: string): string[] {
  const files: string[] = [];
  if (!existsSync(dir)) return files;

  const entries = readdirSync(dir);
  for (const entry of entries) {
    const fullPath = join(dir, entry);
    const stat = statSync(fullPath);
    if (stat.isDirectory()) {
      files.push(...findMarkdownFiles(fullPath));
    } else if (entry.endsWith('.md')) {
      files.push(fullPath);
    }
  }
  return files;
}

/**
 * Astro integration that validates content front-matter at build start.
 */
export function validateContentFrontmatter() {
  return {
    name: 'validate-content-frontmatter',
    hooks: {
      'astro:config:setup'({ logger }: { logger: { info: (msg: string) => void; warn: (msg: string) => void } }) {
        const contentDir = resolve(process.cwd(), 'content/topics');

        if (!existsSync(contentDir)) {
          logger.info('No content/topics directory found. Skipping front-matter validation.');
          return;
        }

        const mdFiles = findMarkdownFiles(contentDir);
        if (mdFiles.length === 0) {
          logger.info('No markdown files found in content/topics/. Skipping front-matter validation.');
          return;
        }

        let validCount = 0;
        let invalidCount = 0;

        for (const filePath of mdFiles) {
          const relativePath = relative(process.cwd(), filePath);
          let content: string;
          try {
            content = readFileSync(filePath, 'utf-8');
          } catch {
            logger.warn(`[front-matter] Unable to read file: ${relativePath}. Skipping.`);
            invalidCount++;
            continue;
          }

          const { data: frontmatter } = sharedParseFrontmatter(content);
          if (Object.keys(frontmatter).length === 0) {
            logger.warn(`[front-matter] No front-matter found in: ${relativePath}. Skipping.`);
            invalidCount++;
            continue;
          }

          const result = validateFrontmatter(frontmatter);
          if (!result.valid) {
            const errorMessages = result.errors.map((e) => `  - ${e.field}: ${e.message}`).join('\n');
            logger.warn(
              `[front-matter] Invalid front-matter in "${relativePath}":\n${errorMessages}\n  File will be skipped.`
            );
            invalidCount++;
          } else {
            validCount++;
          }
        }

        logger.info(
          `Front-matter validation complete: ${validCount} valid, ${invalidCount} invalid (skipped) out of ${mdFiles.length} files.`
        );
      },
    },
  };
}
