/**
 * Astro Integration: Build-Time Content Validator
 *
 * Validates content completeness and cross-reference integrity at build time.
 * Hooks into `astro:build:start` to run before final output generation.
 *
 * Validation rules:
 * 1. Each topic has ≥ 10 scenario questions
 * 2. Each subtopic has ≥ 1 study material + ≥ 1 code snippet
 * 3. Each scenario question has ≥ 2 follow-up probes
 * 4. Each subtopic has content for all 4 layers (fundamentals, intermediate, senior-deep-dive, real-world)
 * 5. All cross-reference links resolve to valid topic/subtopic combinations
 * 6. topics.config.json contains all 18 topics with expected subtopics
 *
 * Behavior:
 * - Logs WARNINGS for missing content (since only one topic is authored so far)
 * - FAILS the build only if topics.config.json is structurally invalid
 * - Reports a summary of which topics/subtopics pass and which are missing content
 *
 * Requirements: 3.1, 3.5, 7.1, 12.14, 12.15, 13.2, 13.3, 13.4, 13.5
 */

import type { AstroIntegration } from 'astro';
import fs from 'node:fs';
import path from 'node:path';
import { parseFrontmatter as sharedParseFrontmatter } from '../utils/parseFrontmatter.ts';

// --- Interfaces ---

interface SubtopicConfig {
  id: string;
  displayName: string;
  order: number;
}

interface TopicConfig {
  id: string;
  displayName: string;
  order: number;
  icon: string;
  subtopics: SubtopicConfig[];
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

interface ValidationResult {
  passed: number;
  warned: number;
  failed: number;
  messages: { level: 'pass' | 'warn' | 'error'; message: string }[];
}

// --- Constants ---

const EXPECTED_TOPIC_COUNT = 26;
const MIN_SCENARIO_QUESTIONS_PER_TOPIC = 10;
const MIN_FOLLOW_UP_PROBES = 2;
const REQUIRED_LAYERS = ['fundamentals', 'intermediate', 'senior-deep-dive', 'real-world'];
const VALID_CONTENT_TYPES = ['study_material', 'code_snippet', 'diagram', 'scenario_question'];
const VALID_DIFFICULTY_LEVELS = ['junior', 'mid-level', 'senior'];

// --- Front-Matter Parser ---

function parseFrontMatter(content: string): { data: FrontMatter; body: string } {
  const { data, body } = sharedParseFrontmatter(content);
  return { data: data as FrontMatter, body };
}

// --- File Discovery ---

function findMarkdownFiles(dir: string): string[] {
  const results: string[] = [];
  if (!fs.existsSync(dir)) return results;

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

// --- Follow-Up Probe Counter ---

/**
 * Count follow-up probes in a scenario question file.
 * Probes are identified by markdown patterns like:
 * - **Probe N: ...** (bold heading pattern)
 * - ### Probe N or ## Probe N
 * - **Follow-up ...** patterns
 *
 * For scenario files with multiple scenarios, count per-scenario.
 */
function countFollowUpProbes(body: string): Map<number, number> {
  const probesByScenario = new Map<number, number>();

  // Split the body into scenarios using ## Scenario N pattern
  const scenarioSections = body.split(/^## Scenario \d+/m);

  // First section is before any scenario header (skip it)
  for (let i = 1; i < scenarioSections.length; i++) {
    const section = scenarioSections[i];
    // Count probes in this scenario section
    // Match patterns like: **Probe 1:, **Probe 2:, etc.
    const probeMatches = section.match(/\*\*Probe \d+[:\s]/g);
    probesByScenario.set(i, probeMatches ? probeMatches.length : 0);
  }

  // If no scenario sections found, try single-scenario format
  if (probesByScenario.size === 0) {
    const probeMatches = body.match(/\*\*Probe \d+[:\s]/g);
    if (probeMatches) {
      probesByScenario.set(1, probeMatches.length);
    }
  }

  return probesByScenario;
}

/**
 * Count scenario questions in a file's body.
 * Scenarios are identified by ## Scenario N headings.
 */
function countScenarios(body: string): number {
  const matches = body.match(/^## Scenario \d+/gm);
  return matches ? matches.length : 0;
}

// --- Cross-Reference Link Extraction ---

/**
 * Extract internal cross-reference links from markdown body.
 * Matches patterns like [text](/topic/xxx/yyy) or [text](/topic/xxx)
 */
function extractCrossReferenceLinks(body: string): string[] {
  const linkRegex = /\[([^\]]*)\]\(\/topic\/([^)]+)\)/g;
  const links: string[] = [];
  let match;

  while ((match = linkRegex.exec(body)) !== null) {
    links.push(match[2]); // Capture the path after /topic/
  }

  return links;
}

// --- Validation Logic ---

export function validateContent(rootDir: string): ValidationResult {
  const result: ValidationResult = { passed: 0, warned: 0, failed: 0, messages: [] };

  const configPath = path.join(rootDir, 'content', 'topics.config.json');
  const contentDir = path.join(rootDir, 'content', 'topics');

  // ============================================================
  // VALIDATION 6: Validate topics.config.json structure
  // This is the only validation that FAILS the build
  // ============================================================

  if (!fs.existsSync(configPath)) {
    result.failed++;
    result.messages.push({
      level: 'error',
      message: `topics.config.json not found at: ${configPath}`
    });
    return result;
  }

  let configContent: string;
  try {
    configContent = fs.readFileSync(configPath, 'utf-8');
  } catch (err) {
    result.failed++;
    result.messages.push({
      level: 'error',
      message: `Unable to read topics.config.json: ${err instanceof Error ? err.message : String(err)}`
    });
    return result;
  }

  let config: TopicsConfig;
  try {
    config = JSON.parse(configContent);
  } catch (err) {
    result.failed++;
    result.messages.push({
      level: 'error',
      message: `Malformed JSON in topics.config.json: ${err instanceof Error ? err.message : String(err)}`
    });
    return result;
  }

  if (!config.topics || !Array.isArray(config.topics)) {
    result.failed++;
    result.messages.push({
      level: 'error',
      message: `topics.config.json must contain a "topics" array at root level`
    });
    return result;
  }

  // Validate 18 topics are present
  if (config.topics.length !== EXPECTED_TOPIC_COUNT) {
    result.failed++;
    result.messages.push({
      level: 'error',
      message: `topics.config.json must contain exactly ${EXPECTED_TOPIC_COUNT} topics, found ${config.topics.length}`
    });
    return result;
  }

  // Validate each topic has required fields
  for (const topic of config.topics) {
    if (!topic.id || typeof topic.id !== 'string') {
      result.failed++;
      result.messages.push({
        level: 'error',
        message: `Topic missing required "id" string field: ${JSON.stringify(topic)}`
      });
      return result;
    }
    if (!topic.displayName || typeof topic.displayName !== 'string') {
      result.failed++;
      result.messages.push({
        level: 'error',
        message: `Topic "${topic.id}" missing required "displayName" field`
      });
      return result;
    }
    if (typeof topic.order !== 'number') {
      result.failed++;
      result.messages.push({
        level: 'error',
        message: `Topic "${topic.id}" missing required numeric "order" field`
      });
      return result;
    }
    if (!Array.isArray(topic.subtopics) || topic.subtopics.length === 0) {
      result.failed++;
      result.messages.push({
        level: 'error',
        message: `Topic "${topic.id}" must have a non-empty "subtopics" array`
      });
      return result;
    }
  }

  result.passed++;
  result.messages.push({
    level: 'pass',
    message: `topics.config.json is valid: ${config.topics.length} topics with subtopics configured`
  });

  // Build valid topic/subtopic lookup for cross-reference validation
  const validPaths = new Set<string>();
  for (const topic of config.topics) {
    validPaths.add(topic.id); // /topic/{topicId} is valid
    for (const subtopic of topic.subtopics) {
      validPaths.add(`${topic.id}/${subtopic.id}`); // /topic/{topicId}/{subtopicId} is valid
    }
  }

  // ============================================================
  // Read all content files and organize by topic/subtopic
  // ============================================================

  const markdownFiles = findMarkdownFiles(contentDir);

  // Maps for accumulating content data
  const scenarioCountByTopic = new Map<string, number>();
  const studyMaterialsBySubtopic = new Map<string, number>();
  const codeSnippetsBySubtopic = new Map<string, number>();
  const layersBySubtopic = new Map<string, Set<string>>();
  const allCrossRefLinks: { file: string; link: string }[] = [];
  const probeWarnings: string[] = [];

  for (const filePath of markdownFiles) {
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const { data, body } = parseFrontMatter(fileContent);

    // Skip files without valid front-matter
    if (!data.topic || !data.subtopic || !data.content_type) continue;
    if (!VALID_CONTENT_TYPES.includes(data.content_type as string)) continue;

    const topic = data.topic as string;
    const subtopic = data.subtopic as string;
    const contentType = data.content_type as string;
    const layer = data.layer as string | undefined;
    const subtopicKey = `${topic}/${subtopic}`;

    // Count study materials per subtopic
    if (contentType === 'study_material') {
      studyMaterialsBySubtopic.set(subtopicKey, (studyMaterialsBySubtopic.get(subtopicKey) || 0) + 1);
    }

    // Count code snippets per subtopic
    if (contentType === 'code_snippet') {
      codeSnippetsBySubtopic.set(subtopicKey, (codeSnippetsBySubtopic.get(subtopicKey) || 0) + 1);
    }

    // Track layers per subtopic
    if (layer && REQUIRED_LAYERS.includes(layer)) {
      if (!layersBySubtopic.has(subtopicKey)) {
        layersBySubtopic.set(subtopicKey, new Set());
      }
      layersBySubtopic.get(subtopicKey)!.add(layer);
    }

    // Count scenarios and validate follow-up probes
    if (contentType === 'scenario_question') {
      const scenarioCount = countScenarios(body);
      scenarioCountByTopic.set(topic, (scenarioCountByTopic.get(topic) || 0) + scenarioCount);

      // Validate follow-up probes per scenario
      const probesByScenario = countFollowUpProbes(body);
      for (const [scenarioNum, probeCount] of probesByScenario) {
        if (probeCount < MIN_FOLLOW_UP_PROBES) {
          probeWarnings.push(
            `${path.relative(rootDir, filePath)} - Scenario ${scenarioNum} has ${probeCount} follow-up probes (requires ≥ ${MIN_FOLLOW_UP_PROBES})`
          );
        }
      }
    }

    // Extract cross-reference links
    const links = extractCrossReferenceLinks(body);
    for (const link of links) {
      allCrossRefLinks.push({ file: path.relative(rootDir, filePath), link });
    }
  }

  // ============================================================
  // VALIDATION 1: Each topic has ≥ 10 scenario questions
  // ============================================================

  for (const topic of config.topics) {
    const count = scenarioCountByTopic.get(topic.id) || 0;
    if (count >= MIN_SCENARIO_QUESTIONS_PER_TOPIC) {
      result.passed++;
      result.messages.push({
        level: 'pass',
        message: `Topic "${topic.displayName}" has ${count} scenario questions (≥ ${MIN_SCENARIO_QUESTIONS_PER_TOPIC})`
      });
    } else {
      result.warned++;
      result.messages.push({
        level: 'warn',
        message: `Topic "${topic.displayName}" has ${count} scenario questions (requires ≥ ${MIN_SCENARIO_QUESTIONS_PER_TOPIC})`
      });
    }
  }

  // ============================================================
  // VALIDATION 2: Each subtopic has ≥ 1 study material + ≥ 1 code snippet
  // ============================================================

  for (const topic of config.topics) {
    for (const subtopic of topic.subtopics) {
      const key = `${topic.id}/${subtopic.id}`;
      const materialCount = studyMaterialsBySubtopic.get(key) || 0;
      const snippetCount = codeSnippetsBySubtopic.get(key) || 0;

      if (materialCount >= 1 && snippetCount >= 1) {
        result.passed++;
        result.messages.push({
          level: 'pass',
          message: `Subtopic "${topic.displayName} > ${subtopic.displayName}" has ${materialCount} study material(s) and ${snippetCount} code snippet(s)`
        });
      } else {
        const missing: string[] = [];
        if (materialCount < 1) missing.push('study material');
        if (snippetCount < 1) missing.push('code snippet');
        result.warned++;
        result.messages.push({
          level: 'warn',
          message: `Subtopic "${topic.displayName} > ${subtopic.displayName}" missing: ${missing.join(', ')} (has ${materialCount} study material(s), ${snippetCount} code snippet(s))`
        });
      }
    }
  }

  // ============================================================
  // VALIDATION 3: Each scenario question has ≥ 2 follow-up probes
  // ============================================================

  if (probeWarnings.length === 0 && scenarioCountByTopic.size > 0) {
    result.passed++;
    result.messages.push({
      level: 'pass',
      message: `All scenario questions have ≥ ${MIN_FOLLOW_UP_PROBES} follow-up probes`
    });
  } else if (probeWarnings.length > 0) {
    for (const warning of probeWarnings) {
      result.warned++;
      result.messages.push({
        level: 'warn',
        message: `Insufficient follow-up probes: ${warning}`
      });
    }
  }

  // ============================================================
  // VALIDATION 4: Each subtopic has content for all 4 layers
  // ============================================================

  for (const topic of config.topics) {
    for (const subtopic of topic.subtopics) {
      const key = `${topic.id}/${subtopic.id}`;
      const layers = layersBySubtopic.get(key) || new Set();
      const missingLayers = REQUIRED_LAYERS.filter(l => !layers.has(l));

      if (missingLayers.length === 0) {
        result.passed++;
        result.messages.push({
          level: 'pass',
          message: `Subtopic "${topic.displayName} > ${subtopic.displayName}" has all 4 content layers`
        });
      } else {
        result.warned++;
        result.messages.push({
          level: 'warn',
          message: `Subtopic "${topic.displayName} > ${subtopic.displayName}" missing layers: ${missingLayers.join(', ')}`
        });
      }
    }
  }

  // ============================================================
  // VALIDATION 5: All cross-reference links resolve
  // ============================================================

  let brokenLinks = 0;
  for (const { file, link } of allCrossRefLinks) {
    if (!validPaths.has(link)) {
      brokenLinks++;
      result.warned++;
      result.messages.push({
        level: 'warn',
        message: `Broken cross-reference link in ${file}: /topic/${link} does not resolve to a valid topic/subtopic`
      });
    }
  }

  if (brokenLinks === 0 && allCrossRefLinks.length > 0) {
    result.passed++;
    result.messages.push({
      level: 'pass',
      message: `All ${allCrossRefLinks.length} cross-reference links resolve correctly`
    });
  } else if (allCrossRefLinks.length === 0) {
    result.messages.push({
      level: 'pass',
      message: `No cross-reference links found to validate`
    });
    result.passed++;
  }

  return result;
}

// --- Astro Integration ---

export function contentValidatorIntegration(): AstroIntegration {
  return {
    name: 'content-validator',
    hooks: {
      'astro:build:start': async ({ logger }) => {
        const rootDir = process.cwd();

        logger.info('Running content validation...');
        logger.info('─'.repeat(60));

        const result = validateContent(rootDir);

        // Print summary grouped by level
        const errors = result.messages.filter(m => m.level === 'error');
        const warnings = result.messages.filter(m => m.level === 'warn');
        const passes = result.messages.filter(m => m.level === 'pass');

        // Print passes (summarized)
        if (passes.length > 0) {
          logger.info(`\n✓ PASSED (${passes.length}):`);
          for (const msg of passes) {
            logger.info(`  ✓ ${msg.message}`);
          }
        }

        // Print warnings
        if (warnings.length > 0) {
          logger.warn(`\n⚠ WARNINGS (${warnings.length}):`);
          for (const msg of warnings) {
            logger.warn(`  ⚠ ${msg.message}`);
          }
        }

        // Print errors
        if (errors.length > 0) {
          logger.error(`\n✗ ERRORS (${errors.length}):`);
          for (const msg of errors) {
            logger.error(`  ✗ ${msg.message}`);
          }
        }

        // Print overall summary
        logger.info('─'.repeat(60));
        logger.info(
          `Content validation complete: ${result.passed} passed, ${result.warned} warnings, ${result.failed} errors`
        );

        // Only fail the build for structural config errors
        if (result.failed > 0) {
          throw new Error(
            `[content-validator] Build failed due to ${result.failed} structural error(s) in topics.config.json. See above for details.`
          );
        }

        if (warnings.length > 0) {
          logger.warn(
            `\nNote: ${warnings.length} content warnings found. These will become errors once all topics are fully authored.`
          );
        }
      },
    },
  };
}

export default contentValidatorIntegration;
