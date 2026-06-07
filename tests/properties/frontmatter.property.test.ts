import { describe, it, expect } from 'vitest';
import * as fc from 'fast-check';
import {
  validateFrontmatter,
  VALID_CONTENT_TYPES,
  VALID_DIFFICULTY_LEVELS,
  VALID_LAYERS,
  type ValidFrontmatter,
} from '../../src/utils/frontmatterValidator';

// --- Helpers ---

/**
 * Simple YAML serializer for flat front-matter objects.
 * Mirrors the format expected by the parseFrontmatter function in validateContent.ts.
 */
function serializeToYaml(fm: Record<string, unknown>): string {
  const lines: string[] = [];
  for (const [key, value] of Object.entries(fm)) {
    if (value === undefined || value === null) continue;
    if (Array.isArray(value)) {
      const items = value.map((item) => `"${String(item)}"`).join(', ');
      lines.push(`${key}: [${items}]`);
    } else {
      lines.push(`${key}: "${String(value)}"`);
    }
  }
  return lines.join('\n');
}

/**
 * Simple front-matter parser matching the one used at build time.
 * Extracts YAML front-matter from markdown and parses key-value pairs.
 */
function parseFrontmatter(content: string): Record<string, unknown> | null {
  const fmRegex = /^---\r?\n([\s\S]*?)\r?\n---/;
  const match = content.match(fmRegex);
  if (!match) return null;

  const yamlContent = match[1];
  const result: Record<string, unknown> = {};

  for (const line of yamlContent.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;

    const colonIndex = trimmed.indexOf(':');
    if (colonIndex === -1) continue;

    const key = trimmed.substring(0, colonIndex).trim();
    let value: string | string[] | undefined = trimmed.substring(colonIndex + 1).trim();

    // Remove surrounding quotes
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }

    // Handle inline arrays like ["tag1", "tag2"]
    if (value.startsWith('[') && value.endsWith(']')) {
      const arrayContent = value.slice(1, -1);
      value = arrayContent
        .split(',')
        .map((item) => item.trim().replace(/^["']|["']$/g, ''))
        .filter((item) => item.length > 0);
    }

    if (value !== undefined) {
      result[key] = value;
    }
  }

  return result;
}

// --- Generators ---

/** Generate a valid title (1-120 chars, no colons, quotes, newlines, or brackets to avoid YAML ambiguity) */
const arbTitle = fc
  .string({ minLength: 1, maxLength: 120 })
  .map((s) => s.replace(/[\n\r:"'\[\]\\]/g, 'x'))
  .filter((s) => s.trim().length > 0 && s.length <= 120);

/** Generate a valid topic slug (non-empty, no special YAML chars) */
const arbSlug = fc
  .string({ minLength: 1, maxLength: 40 })
  .map((s) => s.replace(/[\n\r:"'\[\]\\,\s]/g, '-').replace(/^-+|-+$/g, ''))
  .filter((s) => s.length > 0);

const arbContentType = fc.constantFrom(...VALID_CONTENT_TYPES);
const arbDifficultyLevel = fc.constantFrom(...VALID_DIFFICULTY_LEVELS);
const arbLayer = fc.constantFrom(...VALID_LAYERS);

/** Generate a valid tag (non-empty string without YAML-problematic chars) */
const arbTag = fc
  .string({ minLength: 1, maxLength: 30 })
  .map((s) => s.replace(/[\n\r:"'\[\]\\,]/g, 'x'))
  .filter((s) => s.trim().length > 0);

/** Generator for a valid front-matter object with all required fields and optional fields */
const arbValidFrontmatter: fc.Arbitrary<ValidFrontmatter> = fc.record({
  title: arbTitle,
  topic: arbSlug,
  subtopic: arbSlug,
  content_type: arbContentType,
  difficulty_level: fc.option(arbDifficultyLevel, { nil: undefined }),
  layer: fc.option(arbLayer, { nil: undefined }),
  tags: fc.option(fc.array(arbTag, { minLength: 1, maxLength: 5 }), { nil: undefined }),
});

/** Generator for front-matter missing at least one required field */
const arbMissingRequiredField: fc.Arbitrary<Record<string, unknown>> = fc
  .record({
    title: fc.option(arbTitle, { nil: undefined }),
    topic: fc.option(arbSlug, { nil: undefined }),
    subtopic: fc.option(arbSlug, { nil: undefined }),
    content_type: fc.option(arbContentType, { nil: undefined }),
    difficulty_level: fc.option(arbDifficultyLevel, { nil: undefined }),
  })
  .filter((fm) => {
    // At least one required field must be missing
    return (
      fm.title === undefined ||
      fm.topic === undefined ||
      fm.subtopic === undefined ||
      fm.content_type === undefined
    );
  })
  .map((fm) => {
    // Remove undefined keys to simulate missing fields
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(fm)) {
      if (value !== undefined) {
        result[key] = value;
      }
    }
    return result;
  });

/** Generator for unrecognized content_type values */
const arbInvalidContentType = fc
  .string({ minLength: 1, maxLength: 30 })
  .filter((s) => !(VALID_CONTENT_TYPES as readonly string[]).includes(s) && s.trim().length > 0);

/** Generator for unrecognized difficulty_level values */
const arbInvalidDifficultyLevel = fc
  .string({ minLength: 1, maxLength: 30 })
  .filter(
    (s) => !(VALID_DIFFICULTY_LEVELS as readonly string[]).includes(s) && s.trim().length > 0
  );

/** Generator for front-matter with an invalid content_type */
const arbInvalidContentTypeFm: fc.Arbitrary<Record<string, unknown>> = fc.record({
  title: arbTitle,
  topic: arbSlug,
  subtopic: arbSlug,
  content_type: arbInvalidContentType,
});

/** Generator for front-matter with an invalid difficulty_level */
const arbInvalidDifficultyFm: fc.Arbitrary<Record<string, unknown>> = fc.record({
  title: arbTitle,
  topic: arbSlug,
  subtopic: arbSlug,
  content_type: arbContentType,
  difficulty_level: arbInvalidDifficultyLevel,
});

/** Combined generator for any invalid front-matter (missing fields OR bad values) */
const arbInvalidFrontmatter: fc.Arbitrary<Record<string, unknown>> = fc.oneof(
  arbMissingRequiredField,
  arbInvalidContentTypeFm,
  arbInvalidDifficultyFm
);

// --- Property Tests ---

describe('Feature: de-interview-prep-app, Property 17: Front-matter parsing round-trip', () => {
  /**
   * **Validates: Requirements 9.3**
   *
   * For any valid front-matter object containing all required fields
   * (title ≤ 120 chars, topic, subtopic, content_type ∈ {study_material, code_snippet, diagram, scenario_question})
   * and optional fields (difficulty_level ∈ {junior, mid-level, senior}),
   * serializing to YAML and parsing back SHALL produce an equivalent object.
   */
  it('serializing valid front-matter to YAML and parsing back produces an equivalent validated object', () => {
    fc.assert(
      fc.property(arbValidFrontmatter, (fm) => {
        // Serialize to YAML-like front-matter block
        const fmObject: Record<string, unknown> = {
          title: fm.title,
          topic: fm.topic,
          subtopic: fm.subtopic,
          content_type: fm.content_type,
        };
        if (fm.difficulty_level !== undefined) {
          fmObject.difficulty_level = fm.difficulty_level;
        }
        if (fm.layer !== undefined) {
          fmObject.layer = fm.layer;
        }
        if (fm.tags !== undefined) {
          fmObject.tags = fm.tags;
        }

        const yaml = serializeToYaml(fmObject);
        const markdown = `---\n${yaml}\n---\n\n# Content`;

        // Parse back
        const parsed = parseFrontmatter(markdown);
        expect(parsed).not.toBeNull();

        // Validate the parsed result
        const result = validateFrontmatter(parsed);
        expect(result.valid).toBe(true);

        if (result.valid) {
          // Verify equivalence of required fields
          expect(result.data.title).toBe(fm.title);
          expect(result.data.topic).toBe(fm.topic);
          expect(result.data.subtopic).toBe(fm.subtopic);
          expect(result.data.content_type).toBe(fm.content_type);

          // Verify optional fields round-trip correctly
          if (fm.difficulty_level !== undefined) {
            expect(result.data.difficulty_level).toBe(fm.difficulty_level);
          }
          if (fm.layer !== undefined) {
            expect(result.data.layer).toBe(fm.layer);
          }
          if (fm.tags !== undefined) {
            expect(result.data.tags).toEqual(fm.tags);
          }
        }
      }),
      { numRuns: 100 }
    );
  });
});

describe('Feature: de-interview-prep-app, Property 18: Front-matter validation rejects invalid entries', () => {
  /**
   * **Validates: Requirements 9.4**
   *
   * For any front-matter object that is missing at least one required field
   * (title, topic, subtopic, content_type) OR has an unrecognized content_type
   * or difficulty_level value, the validator SHALL return a rejection result
   * identifying the specific validation error(s).
   */
  it('front-matter with missing required fields or unrecognized enum values is rejected with specific errors', () => {
    fc.assert(
      fc.property(arbInvalidFrontmatter, (fm) => {
        const result = validateFrontmatter(fm);

        // Must be rejected
        expect(result.valid).toBe(false);

        if (!result.valid) {
          // Must have at least one error
          expect(result.errors.length).toBeGreaterThan(0);

          // Each error must identify the specific field
          for (const error of result.errors) {
            expect(error.field).toBeTruthy();
            expect(error.message).toBeTruthy();
          }

          // Verify the errors correspond to the actual issues
          const errorFields = result.errors.map((e) => e.field);

          // Check missing required fields are reported
          if (fm.title === undefined || fm.title === null) {
            expect(errorFields).toContain('title');
          }
          if (fm.topic === undefined || fm.topic === null) {
            expect(errorFields).toContain('topic');
          }
          if (fm.subtopic === undefined || fm.subtopic === null) {
            expect(errorFields).toContain('subtopic');
          }
          if (fm.content_type === undefined || fm.content_type === null) {
            expect(errorFields).toContain('content_type');
          }

          // Check invalid content_type is reported
          if (
            fm.content_type !== undefined &&
            fm.content_type !== null &&
            typeof fm.content_type === 'string' &&
            !(VALID_CONTENT_TYPES as readonly string[]).includes(fm.content_type)
          ) {
            expect(errorFields).toContain('content_type');
          }

          // Check invalid difficulty_level is reported
          if (
            fm.difficulty_level !== undefined &&
            fm.difficulty_level !== null &&
            typeof fm.difficulty_level === 'string' &&
            !(VALID_DIFFICULTY_LEVELS as readonly string[]).includes(fm.difficulty_level)
          ) {
            expect(errorFields).toContain('difficulty_level');
          }
        }
      }),
      { numRuns: 100 }
    );
  });
});
