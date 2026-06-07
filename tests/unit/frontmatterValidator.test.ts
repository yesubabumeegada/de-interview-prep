import { describe, it, expect } from 'vitest';
import {
  validateFrontmatter,
  VALID_CONTENT_TYPES,
  VALID_DIFFICULTY_LEVELS,
  VALID_LAYERS,
  type ValidFrontmatter,
} from '../../src/utils/frontmatterValidator';

describe('frontmatterValidator', () => {
  const validFrontmatter: Record<string, unknown> = {
    title: 'S3 Bucket Lifecycle Policies',
    topic: 'aws-services',
    subtopic: 's3',
    content_type: 'study_material',
  };

  describe('valid front-matter', () => {
    it('accepts minimal valid front-matter with only required fields', () => {
      const result = validateFrontmatter(validFrontmatter);
      expect(result.valid).toBe(true);
      if (result.valid) {
        expect(result.data.title).toBe('S3 Bucket Lifecycle Policies');
        expect(result.data.topic).toBe('aws-services');
        expect(result.data.subtopic).toBe('s3');
        expect(result.data.content_type).toBe('study_material');
        expect(result.data.difficulty_level).toBeUndefined();
        expect(result.data.layer).toBeUndefined();
        expect(result.data.tags).toBeUndefined();
      }
    });

    it('accepts front-matter with all optional fields', () => {
      const full = {
        ...validFrontmatter,
        difficulty_level: 'senior',
        layer: 'senior-deep-dive',
        tags: ['storage', 'lifecycle', 'cost'],
      };
      const result = validateFrontmatter(full);
      expect(result.valid).toBe(true);
      if (result.valid) {
        expect(result.data.difficulty_level).toBe('senior');
        expect(result.data.layer).toBe('senior-deep-dive');
        expect(result.data.tags).toEqual(['storage', 'lifecycle', 'cost']);
      }
    });

    it('accepts all valid content_type values', () => {
      for (const contentType of VALID_CONTENT_TYPES) {
        const fm = { ...validFrontmatter, content_type: contentType };
        const result = validateFrontmatter(fm);
        expect(result.valid).toBe(true);
      }
    });

    it('accepts all valid difficulty_level values', () => {
      for (const level of VALID_DIFFICULTY_LEVELS) {
        const fm = { ...validFrontmatter, difficulty_level: level };
        const result = validateFrontmatter(fm);
        expect(result.valid).toBe(true);
      }
    });

    it('accepts all valid layer values', () => {
      for (const layer of VALID_LAYERS) {
        const fm = { ...validFrontmatter, layer };
        const result = validateFrontmatter(fm);
        expect(result.valid).toBe(true);
      }
    });

    it('accepts title at exactly 120 characters', () => {
      const fm = { ...validFrontmatter, title: 'a'.repeat(120) };
      const result = validateFrontmatter(fm);
      expect(result.valid).toBe(true);
    });
  });

  describe('invalid front-matter - missing required fields', () => {
    it('rejects null input', () => {
      const result = validateFrontmatter(null);
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors[0].field).toBe('frontmatter');
      }
    });

    it('rejects undefined input', () => {
      const result = validateFrontmatter(undefined);
      expect(result.valid).toBe(false);
    });

    it('rejects missing title', () => {
      const { title, ...rest } = validFrontmatter;
      const result = validateFrontmatter(rest);
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors.some((e) => e.field === 'title')).toBe(true);
      }
    });

    it('rejects missing topic', () => {
      const { topic, ...rest } = validFrontmatter;
      const result = validateFrontmatter(rest);
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors.some((e) => e.field === 'topic')).toBe(true);
      }
    });

    it('rejects missing subtopic', () => {
      const { subtopic, ...rest } = validFrontmatter;
      const result = validateFrontmatter(rest);
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors.some((e) => e.field === 'subtopic')).toBe(true);
      }
    });

    it('rejects missing content_type', () => {
      const { content_type, ...rest } = validFrontmatter;
      const result = validateFrontmatter(rest);
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors.some((e) => e.field === 'content_type')).toBe(true);
      }
    });

    it('reports all missing fields at once', () => {
      const result = validateFrontmatter({});
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors.length).toBe(4); // title, topic, subtopic, content_type
      }
    });
  });

  describe('invalid front-matter - bad values', () => {
    it('rejects title exceeding 120 characters', () => {
      const fm = { ...validFrontmatter, title: 'a'.repeat(121) };
      const result = validateFrontmatter(fm);
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors[0].field).toBe('title');
        expect(result.errors[0].message).toContain('120');
      }
    });

    it('rejects empty title', () => {
      const fm = { ...validFrontmatter, title: '' };
      const result = validateFrontmatter(fm);
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors[0].field).toBe('title');
      }
    });

    it('rejects unrecognized content_type', () => {
      const fm = { ...validFrontmatter, content_type: 'blog_post' };
      const result = validateFrontmatter(fm);
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors[0].field).toBe('content_type');
        expect(result.errors[0].message).toContain('unrecognized');
      }
    });

    it('rejects unrecognized difficulty_level', () => {
      const fm = { ...validFrontmatter, difficulty_level: 'expert' };
      const result = validateFrontmatter(fm);
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors[0].field).toBe('difficulty_level');
        expect(result.errors[0].message).toContain('unrecognized');
      }
    });

    it('rejects unrecognized layer', () => {
      const fm = { ...validFrontmatter, layer: 'advanced' };
      const result = validateFrontmatter(fm);
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors[0].field).toBe('layer');
        expect(result.errors[0].message).toContain('unrecognized');
      }
    });

    it('rejects non-array tags', () => {
      const fm = { ...validFrontmatter, tags: 'not-an-array' };
      const result = validateFrontmatter(fm);
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors[0].field).toBe('tags');
      }
    });

    it('rejects tags with non-string items', () => {
      const fm = { ...validFrontmatter, tags: ['valid', 123, true] };
      const result = validateFrontmatter(fm);
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors[0].field).toBe('tags');
      }
    });

    it('rejects non-string title', () => {
      const fm = { ...validFrontmatter, title: 42 };
      const result = validateFrontmatter(fm);
      expect(result.valid).toBe(false);
      if (!result.valid) {
        expect(result.errors[0].field).toBe('title');
      }
    });
  });
});
