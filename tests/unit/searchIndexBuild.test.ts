import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { buildSearchIndex } from '../../src/integrations/search-index';

describe('Search Index Build Script', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-index-test-'));
    // Create content directory structure
    fs.mkdirSync(path.join(tmpDir, 'content', 'topics', 'aws-services', 's3'), { recursive: true });
    fs.mkdirSync(path.join(tmpDir, 'content', 'topics', 'databricks', 'delta-lake'), { recursive: true });
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  function writeConfig(config: object) {
    fs.writeFileSync(
      path.join(tmpDir, 'content', 'topics.config.json'),
      JSON.stringify(config, null, 2),
      'utf-8'
    );
  }

  function writeContent(relativePath: string, content: string) {
    const fullPath = path.join(tmpDir, 'content', 'topics', relativePath);
    const dir = path.dirname(fullPath);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(fullPath, content, 'utf-8');
  }

  const validConfig = {
    topics: [
      {
        id: 'aws-services',
        displayName: 'AWS Services',
        order: 1,
        icon: 'aws',
        subtopics: [
          { id: 's3', displayName: 'S3', order: 1 },
          { id: 'glue', displayName: 'Glue', order: 2 },
        ],
      },
      {
        id: 'databricks',
        displayName: 'Databricks',
        order: 2,
        icon: 'databricks',
        subtopics: [
          { id: 'delta-lake', displayName: 'Delta Lake', order: 1 },
        ],
      },
    ],
  };

  describe('buildSearchIndex', () => {
    it('generates entries from valid markdown files with front-matter', () => {
      writeConfig(validConfig);
      writeContent('aws-services/s3/fundamentals.md', `---
title: "S3 Fundamentals"
topic: "aws-services"
subtopic: "s3"
content_type: "study_material"
difficulty_level: "junior"
---

# Amazon S3 Fundamentals

Amazon S3 (Simple Storage Service) is an object storage service.

## Key Concepts

- Buckets are containers for objects
- Objects are files stored in buckets
- Keys are unique identifiers for objects
`);

      const entries = buildSearchIndex(tmpDir);

      expect(entries).toHaveLength(1);
      expect(entries[0]).toMatchObject({
        id: 'aws-services/s3/fundamentals',
        title: 'S3 Fundamentals',
        topic: 'aws-services',
        topicDisplayName: 'AWS Services',
        subtopic: 's3',
        subtopicDisplayName: 'S3',
        contentType: 'study_material',
        difficultyLevel: 'junior',
        url: '/topic/aws-services/s3',
      });
      // Body should be plain text without markdown formatting
      expect(entries[0].body).toContain('Amazon S3');
      expect(entries[0].body).toContain('Simple Storage Service');
      expect(entries[0].body).not.toContain('#');
      expect(entries[0].body).not.toContain('- ');
    });

    it('handles multiple files across different topics', () => {
      writeConfig(validConfig);
      writeContent('aws-services/s3/fundamentals.md', `---
title: "S3 Basics"
topic: "aws-services"
subtopic: "s3"
content_type: "study_material"
---

S3 is object storage.
`);
      writeContent('databricks/delta-lake/fundamentals.md', `---
title: "Delta Lake Basics"
topic: "databricks"
subtopic: "delta-lake"
content_type: "study_material"
---

Delta Lake is an open-source storage layer.
`);

      const entries = buildSearchIndex(tmpDir);

      expect(entries).toHaveLength(2);
      expect(entries.map(e => e.topic)).toContain('aws-services');
      expect(entries.map(e => e.topic)).toContain('databricks');
    });

    it('sets difficultyLevel to null when not provided', () => {
      writeConfig(validConfig);
      writeContent('aws-services/s3/fundamentals.md', `---
title: "S3 Basics"
topic: "aws-services"
subtopic: "s3"
content_type: "study_material"
---

Content here.
`);

      const entries = buildSearchIndex(tmpDir);

      expect(entries).toHaveLength(1);
      expect(entries[0].difficultyLevel).toBeNull();
    });

    it('skips files with missing required front-matter fields', () => {
      writeConfig(validConfig);
      // Missing content_type
      writeContent('aws-services/s3/incomplete.md', `---
title: "S3 Incomplete"
topic: "aws-services"
subtopic: "s3"
---

Some content.
`);

      const entries = buildSearchIndex(tmpDir);

      expect(entries).toHaveLength(0);
    });

    it('skips files with invalid content_type', () => {
      writeConfig(validConfig);
      writeContent('aws-services/s3/invalid.md', `---
title: "S3 Invalid"
topic: "aws-services"
subtopic: "s3"
content_type: "invalid_type"
---

Some content.
`);

      const entries = buildSearchIndex(tmpDir);

      expect(entries).toHaveLength(0);
    });

    it('skips files with invalid difficulty_level', () => {
      writeConfig(validConfig);
      writeContent('aws-services/s3/invalid-difficulty.md', `---
title: "S3 Invalid Difficulty"
topic: "aws-services"
subtopic: "s3"
content_type: "study_material"
difficulty_level: "expert"
---

Some content.
`);

      const entries = buildSearchIndex(tmpDir);

      expect(entries).toHaveLength(0);
    });

    it('strips markdown formatting from body', () => {
      writeConfig(validConfig);
      writeContent('aws-services/s3/formatted.md', `---
title: "S3 Formatted"
topic: "aws-services"
subtopic: "s3"
content_type: "study_material"
---

# Main Header

This is **bold** and *italic* text.

\`\`\`python
def hello():
    print("hello")
\`\`\`

- List item 1
- List item 2

[Click here](https://example.com) for more info.
`);

      const entries = buildSearchIndex(tmpDir);

      expect(entries).toHaveLength(1);
      const body = entries[0].body;
      // Should not contain markdown syntax
      expect(body).not.toContain('# ');
      expect(body).not.toContain('**');
      expect(body).not.toContain('*italic*');
      expect(body).not.toContain('```');
      expect(body).not.toContain('[Click here]');
      expect(body).not.toContain('(https://example.com)');
      // Should contain the text content
      expect(body).toContain('Main Header');
      expect(body).toContain('bold');
      expect(body).toContain('italic');
      expect(body).toContain('Click here');
      expect(body).toContain('List item 1');
    });

    it('throws error when topics.config.json is missing', () => {
      // Don't write config
      expect(() => buildSearchIndex(tmpDir)).toThrow('topics.config.json not found');
    });

    it('throws error when topics.config.json has invalid JSON', () => {
      fs.writeFileSync(
        path.join(tmpDir, 'content', 'topics.config.json'),
        '{ invalid json }',
        'utf-8'
      );

      expect(() => buildSearchIndex(tmpDir)).toThrow('Failed to parse topics.config.json');
    });

    it('returns empty array when no markdown files exist', () => {
      writeConfig(validConfig);

      const entries = buildSearchIndex(tmpDir);

      expect(entries).toEqual([]);
    });

    it('generates correct URL path from topic and subtopic', () => {
      writeConfig(validConfig);
      writeContent('databricks/delta-lake/fundamentals.md', `---
title: "Delta Lake Fundamentals"
topic: "databricks"
subtopic: "delta-lake"
content_type: "study_material"
---

Delta Lake content.
`);

      const entries = buildSearchIndex(tmpDir);

      expect(entries[0].url).toBe('/topic/databricks/delta-lake');
    });

    it('handles all valid content types', () => {
      writeConfig(validConfig);
      const contentTypes = ['study_material', 'code_snippet', 'diagram', 'scenario_question'];

      for (const ct of contentTypes) {
        writeContent(`aws-services/s3/${ct}.md`, `---
title: "S3 ${ct}"
topic: "aws-services"
subtopic: "s3"
content_type: "${ct}"
---

Content for ${ct}.
`);
      }

      const entries = buildSearchIndex(tmpDir);

      expect(entries).toHaveLength(4);
      const types = entries.map(e => e.contentType);
      for (const ct of contentTypes) {
        expect(types).toContain(ct);
      }
    });

    it('uses topic ID as display name when topic is not in config', () => {
      writeConfig(validConfig);
      writeContent('unknown-topic/sub/file.md', `---
title: "Unknown Topic File"
topic: "unknown-topic"
subtopic: "sub"
content_type: "study_material"
---

Content.
`);

      const entries = buildSearchIndex(tmpDir);

      expect(entries).toHaveLength(1);
      expect(entries[0].topicDisplayName).toBe('unknown-topic');
      expect(entries[0].subtopicDisplayName).toBe('sub');
    });

    it('skips files without front-matter entirely', () => {
      writeConfig(validConfig);
      writeContent('aws-services/s3/no-frontmatter.md', `# Just a heading

Some content without front-matter.
`);

      const entries = buildSearchIndex(tmpDir);

      expect(entries).toHaveLength(0);
    });
  });
});
