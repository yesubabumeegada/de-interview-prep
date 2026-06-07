/**
 * Astro Integration: Search Index Generator
 *
 * Generates `public/search-index.json` at build time by:
 * 1. Reading all markdown files from content/topics/ recursively
 * 2. Parsing front-matter to extract metadata
 * 3. Stripping markdown to plain text for the body field
 * 4. Looking up display names from content/topics.config.json
 * 5. Writing the resulting array to public/search-index.json
 *
 * Requirements: 4.1, 4.5, 11.3
 */

import type { AstroIntegration } from 'astro';
import fs from 'node:fs';
import path from 'node:path';
import { parseFrontmatter as sharedParseFrontmatter } from '../utils/parseFrontmatter.ts';

// --- Interfaces ---

interface SearchIndexEntry {
  id: string;
  title: string;
  topic: string;
  topicDisplayName: string;
  subtopic: string;
  subtopicDisplayName: string;
  contentType: string;
  difficultyLevel: string | null;
  body: string;
  url: string;
}

interface TopicConfig {
  id: string;
  displayName: string;
  order: number;
  icon: string;
  subtopics: SubtopicConfig[];
}

interface SubtopicConfig {
  id: string;
  displayName: string;
  order: number;
}

interface TopicsConfig {
  topics: TopicConfig[];
}

interface FrontMatter {
  title?: string;
  topic?: string;
  subtopic?: string;
  content_type?: string;
  difficulty_level?: string;
  layer?: string;
  tags?: string[];
  [key: string]: unknown;
}

// --- Front-Matter Parser ---

function parseFrontMatter(content: string): { data: FrontMatter; body: string } {
  const { data, body } = sharedParseFrontmatter(content);
  return { data: data as FrontMatter, body };
}

// --- Markdown to Plain Text ---

/**
 * Strip markdown formatting to produce plain text for search indexing.
 * Removes:
 * - Headers (# ## ### etc.)
 * - Bold/italic markers (* ** _ __)
 * - Links [text](url) -> text
 * - Images ![alt](src) -> alt
 * - Code blocks (``` ... ```)
 * - Inline code (` ... `)
 * - HTML tags
 * - Mermaid blocks
 * - Horizontal rules (--- ***)
 * - Block quotes (>)
 * - List markers (- * + 1.)
 */
function stripMarkdownToText(markdown: string): string {
  let text = markdown;

  // Remove code blocks (fenced with ``` or ~~~)
  text = text.replace(/```[\s\S]*?```/g, '');
  text = text.replace(/~~~[\s\S]*?~~~/g, '');

  // Remove mermaid blocks
  text = text.replace(/```mermaid[\s\S]*?```/g, '');

  // Remove HTML tags
  text = text.replace(/<[^>]+>/g, '');

  // Remove images ![alt](url)
  text = text.replace(/!\[([^\]]*)\]\([^)]*\)/g, '$1');

  // Remove links [text](url) -> text
  text = text.replace(/\[([^\]]*)\]\([^)]*\)/g, '$1');

  // Remove reference-style links [text][ref]
  text = text.replace(/\[([^\]]*)\]\[[^\]]*\]/g, '$1');

  // Remove heading markers
  text = text.replace(/^#{1,6}\s+/gm, '');

  // Remove bold/italic markers
  text = text.replace(/(\*\*\*|___)(.*?)\1/g, '$2');
  text = text.replace(/(\*\*|__)(.*?)\1/g, '$2');
  text = text.replace(/(\*|_)(.*?)\1/g, '$2');

  // Remove strikethrough
  text = text.replace(/~~(.*?)~~/g, '$1');

  // Remove inline code
  text = text.replace(/`([^`]*)`/g, '$1');

  // Remove blockquotes
  text = text.replace(/^>\s?/gm, '');

  // Remove horizontal rules
  text = text.replace(/^[-*_]{3,}\s*$/gm, '');

  // Remove list markers (unordered)
  text = text.replace(/^[\s]*[-*+]\s+/gm, '');

  // Remove list markers (ordered)
  text = text.replace(/^[\s]*\d+\.\s+/gm, '');

  // Remove table formatting
  text = text.replace(/\|/g, ' ');
  text = text.replace(/^[-:|\s]+$/gm, '');

  // Collapse multiple newlines to single space
  text = text.replace(/\n{2,}/g, '\n');

  // Collapse multiple spaces
  text = text.replace(/[ \t]+/g, ' ');

  // Trim
  text = text.trim();

  return text;
}

// --- File Discovery ---

/**
 * Recursively find all markdown files in a directory.
 */
function findMarkdownFiles(dir: string): string[] {
  const results: string[] = [];

  if (!fs.existsSync(dir)) {
    return results;
  }

  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);

    if (entry.isDirectory()) {
      results.push(...findMarkdownFiles(fullPath));
    } else if (entry.isFile() && (entry.name.endsWith('.md') || entry.name.endsWith('.mdx'))) {
      results.push(fullPath);
    }
  }

  return results;
}

// --- Display Name Lookup ---

/**
 * Build lookup maps from topics config for fast display name resolution.
 */
function buildDisplayNameMaps(config: TopicsConfig): {
  topicDisplayNames: Map<string, string>;
  subtopicDisplayNames: Map<string, string>;
} {
  const topicDisplayNames = new Map<string, string>();
  const subtopicDisplayNames = new Map<string, string>();

  for (const topic of config.topics) {
    topicDisplayNames.set(topic.id, topic.displayName);
    for (const subtopic of topic.subtopics) {
      // Key: "topicId/subtopicId" for uniqueness across topics
      subtopicDisplayNames.set(`${topic.id}/${subtopic.id}`, subtopic.displayName);
    }
  }

  return { topicDisplayNames, subtopicDisplayNames };
}

// --- URL Generation ---

/**
 * Generate the URL path for a content item.
 */
function generateUrl(topic: string, subtopic: string): string {
  return `/topic/${topic}/${subtopic}`;
}

// --- Slug Generation ---

/**
 * Generate a slug from a file path relative to the topics directory.
 * e.g., "aws-services/s3/fundamentals.md" -> "aws-services/s3/fundamentals"
 */
function generateSlug(relativePath: string): string {
  return relativePath
    .replace(/\\/g, '/')
    .replace(/\.(md|mdx)$/, '');
}

// --- Validation ---

const VALID_CONTENT_TYPES = ['study_material', 'code_snippet', 'diagram', 'scenario_question'];
const VALID_DIFFICULTY_LEVELS = ['junior', 'mid-level', 'senior'];

/**
 * Validate front-matter has required fields.
 */
function isValidFrontMatter(data: FrontMatter, filePath: string): boolean {
  if (!data.title || !data.topic || !data.subtopic || !data.content_type) {
    console.warn(
      `[search-index] Skipping ${filePath}: Missing required front-matter fields (title, topic, subtopic, content_type)`
    );
    return false;
  }

  if (typeof data.title === 'string' && data.title.length > 120) {
    console.warn(
      `[search-index] Skipping ${filePath}: Title exceeds 120 characters`
    );
    return false;
  }

  if (!VALID_CONTENT_TYPES.includes(data.content_type as string)) {
    console.warn(
      `[search-index] Skipping ${filePath}: Invalid content_type "${data.content_type}". Must be one of: ${VALID_CONTENT_TYPES.join(', ')}`
    );
    return false;
  }

  if (data.difficulty_level && !VALID_DIFFICULTY_LEVELS.includes(data.difficulty_level as string)) {
    console.warn(
      `[search-index] Skipping ${filePath}: Invalid difficulty_level "${data.difficulty_level}". Must be one of: ${VALID_DIFFICULTY_LEVELS.join(', ')}`
    );
    return false;
  }

  return true;
}

// --- Main Build Function ---

/**
 * Build the search index from content files.
 */
export function buildSearchIndex(rootDir: string): SearchIndexEntry[] {
  const contentDir = path.join(rootDir, 'content', 'topics');
  const configPath = path.join(rootDir, 'content', 'topics.config.json');

  // Load topics config
  if (!fs.existsSync(configPath)) {
    throw new Error(
      `[search-index] topics.config.json not found at: ${configPath}`
    );
  }

  const configContent = fs.readFileSync(configPath, 'utf-8');
  let config: TopicsConfig;
  try {
    config = JSON.parse(configContent);
  } catch (e) {
    throw new Error(
      `[search-index] Failed to parse topics.config.json: ${(e as Error).message}`
    );
  }

  const { topicDisplayNames, subtopicDisplayNames } = buildDisplayNameMaps(config);

  // Find all markdown files
  const markdownFiles = findMarkdownFiles(contentDir);
  const entries: SearchIndexEntry[] = [];

  for (const filePath of markdownFiles) {
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const { data, body } = parseFrontMatter(fileContent);

    // Validate front-matter
    if (!isValidFrontMatter(data, filePath)) {
      continue;
    }

    const topic = data.topic as string;
    const subtopic = data.subtopic as string;
    const title = data.title as string;
    const contentType = data.content_type as string;
    const difficultyLevel = data.difficulty_level
      ? (data.difficulty_level as string)
      : null;

    // Generate relative path for slug
    const relativePath = path.relative(contentDir, filePath);
    const slug = generateSlug(relativePath);

    // Build the search index entry
    const entry: SearchIndexEntry = {
      id: slug,
      title,
      topic,
      topicDisplayName: topicDisplayNames.get(topic) || topic,
      subtopic,
      subtopicDisplayName: subtopicDisplayNames.get(`${topic}/${subtopic}`) || subtopic,
      contentType,
      difficultyLevel,
      body: stripMarkdownToText(body),
      url: generateUrl(topic, subtopic),
    };

    entries.push(entry);
  }

  return entries;
}

// --- Astro Integration ---

/**
 * Astro integration that generates search-index.json during the build process.
 * The index is written to public/search-index.json so it's available as a static asset.
 */
export function searchIndexIntegration(): AstroIntegration {
  return {
    name: 'search-index-generator',
    hooks: {
      'astro:build:start': async ({ logger }) => {
        const rootDir = process.cwd();
        const outputPath = path.join(rootDir, 'public', 'search-index.json');

        logger.info('Generating search index...');

        try {
          const entries = buildSearchIndex(rootDir);

          // Ensure public directory exists
          const publicDir = path.dirname(outputPath);
          if (!fs.existsSync(publicDir)) {
            fs.mkdirSync(publicDir, { recursive: true });
          }

          // Write the search index
          fs.writeFileSync(outputPath, JSON.stringify(entries, null, 2), 'utf-8');

          logger.info(`Search index generated: ${entries.length} entries written to ${outputPath}`);
        } catch (error) {
          logger.error(`Failed to generate search index: ${(error as Error).message}`);
          throw error;
        }
      },

      'astro:server:setup': async ({ logger }) => {
        // Also generate during dev server startup for development convenience
        const rootDir = process.cwd();
        const outputPath = path.join(rootDir, 'public', 'search-index.json');

        logger.info('Generating search index for dev server...');

        try {
          const entries = buildSearchIndex(rootDir);

          const publicDir = path.dirname(outputPath);
          if (!fs.existsSync(publicDir)) {
            fs.mkdirSync(publicDir, { recursive: true });
          }

          fs.writeFileSync(outputPath, JSON.stringify(entries, null, 2), 'utf-8');

          logger.info(`Search index generated: ${entries.length} entries`);
        } catch (error) {
          logger.warn(`Failed to generate search index for dev: ${(error as Error).message}`);
          // Don't throw in dev - write empty index as fallback
          fs.writeFileSync(outputPath, '[]', 'utf-8');
        }
      },
    },
  };
}

export default searchIndexIntegration;
