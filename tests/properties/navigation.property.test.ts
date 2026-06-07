import { describe, it, expect } from 'vitest';
import * as fc from 'fast-check';
import {
  sortSubtopics,
  aggregateTopicStats,
  buildBreadcrumbs,
} from '../../src/services/navigationService';
import type {
  SubtopicConfig,
  TopicConfig,
  ContentItem,
  BreadcrumbItem,
} from '../../src/services/navigationService';

// --- Generators ---

const VALID_CONTENT_TYPES = ['study_material', 'code_snippet', 'diagram', 'scenario_question'] as const;

const arbSubtopicConfig: fc.Arbitrary<SubtopicConfig> = fc.record({
  id: fc.string({ minLength: 1, maxLength: 30 }).map((s) => s.replace(/\s+/g, '-').replace(/[^a-z0-9-]/gi, '') || 'subtopic-1'),
  displayName: fc.string({ minLength: 1, maxLength: 50 }).filter((s) => s.trim().length >= 1),
  order: fc.integer({ min: 1, max: 1000 }),
});

const arbTopicConfig: fc.Arbitrary<TopicConfig> = fc.record({
  id: fc.string({ minLength: 1, maxLength: 30 }).map((s) => s.replace(/\s+/g, '-').replace(/[^a-z0-9-]/gi, '') || 'topic-1'),
  displayName: fc.string({ minLength: 1, maxLength: 50 }).filter((s) => s.trim().length >= 1),
  order: fc.integer({ min: 1, max: 100 }),
  subtopics: fc.array(arbSubtopicConfig, { minLength: 0, maxLength: 20 }),
});

const arbTopicId = fc.constantFrom(
  'aws-services', 'databricks', 'snowflake', 'hadoop', 'oracle',
  'teradata', 'python', 'pyspark', 'sql', 'airflow',
  'bash-scripting', 'kafka', 'nifi', 'etl-concepts', 'power-bi',
  'rag-llm', 'ai', 'data-modeling'
);

const arbContentType = fc.constantFrom(...VALID_CONTENT_TYPES);

const arbContentItem: fc.Arbitrary<ContentItem> = fc.record({
  topicId: arbTopicId,
  contentType: arbContentType,
});

const arbDisplayName = fc.string({ minLength: 1, maxLength: 60 }).filter((s) => s.trim().length >= 1);

// --- Property Tests ---

describe('Feature: de-interview-prep-app, Property 1: Subtopic ordering preserves config order', () => {
  /**
   * **Validates: Requirements 1.2**
   *
   * For any topic configuration with subtopics having arbitrary order values,
   * the navigation system SHALL render subtopics in ascending order of their
   * `order` field, such that for any two adjacent subtopics in the rendered list,
   * the first has a lower or equal order value than the second.
   */
  it('subtopics are sorted in ascending order of their order field', () => {
    fc.assert(
      fc.property(
        fc.array(arbSubtopicConfig, { minLength: 1, maxLength: 30 }),
        (subtopics) => {
          const sorted = sortSubtopics(subtopics);

          // Result has the same length as input
          expect(sorted.length).toBe(subtopics.length);

          // For any two adjacent subtopics, the first has lower or equal order
          for (let i = 1; i < sorted.length; i++) {
            expect(sorted[i].order).toBeGreaterThanOrEqual(sorted[i - 1].order);
          }

          // All original items are preserved (no items lost or added)
          const originalIds = new Set(subtopics.map((s) => s.id));
          const sortedIds = new Set(sorted.map((s) => s.id));
          expect(sortedIds.size).toBe(originalIds.size);
          for (const id of originalIds) {
            expect(sortedIds.has(id)).toBe(true);
          }
        }
      ),
      { numRuns: 100 }
    );
  });

  it('sorting does not mutate the original array', () => {
    fc.assert(
      fc.property(
        fc.array(arbSubtopicConfig, { minLength: 1, maxLength: 20 }),
        (subtopics) => {
          const original = [...subtopics];
          sortSubtopics(subtopics);

          // Original array should be unchanged
          expect(subtopics).toEqual(original);
        }
      ),
      { numRuns: 100 }
    );
  });
});

describe('Feature: de-interview-prep-app, Property 2: Content count aggregation accuracy', () => {
  /**
   * **Validates: Requirements 1.3**
   *
   * For any collection of content items distributed across topics, the dashboard
   * SHALL display counts per topic where `studyMaterialCount` equals the number
   * of items with `contentType === 'study_material'` for that topic, and similarly
   * for all other content types, with zero displayed for topics with no items
   * of a given type.
   */
  it('count aggregation accurately reflects content items per topic and type', () => {
    // Use a fixed set of topic configs so we can predictably generate content items
    const topicConfigs: TopicConfig[] = [
      { id: 'aws-services', displayName: 'AWS Services', order: 1, subtopics: [] },
      { id: 'databricks', displayName: 'Databricks', order: 2, subtopics: [] },
      { id: 'snowflake', displayName: 'Snowflake', order: 3, subtopics: [] },
      { id: 'python', displayName: 'Python', order: 4, subtopics: [] },
    ];

    const arbContentItemForTopics: fc.Arbitrary<ContentItem> = fc.record({
      topicId: fc.constantFrom('aws-services', 'databricks', 'snowflake', 'python'),
      contentType: arbContentType,
    });

    fc.assert(
      fc.property(
        fc.array(arbContentItemForTopics, { minLength: 0, maxLength: 50 }),
        (contentItems) => {
          const stats = aggregateTopicStats(topicConfigs, contentItems);

          // Should return stats for all topics
          expect(stats.length).toBe(topicConfigs.length);

          for (const stat of stats) {
            const topicItems = contentItems.filter((i) => i.topicId === stat.topicId);

            // Verify each count matches the filtered count
            const expectedStudyMaterial = topicItems.filter((i) => i.contentType === 'study_material').length;
            const expectedCodeSnippet = topicItems.filter((i) => i.contentType === 'code_snippet').length;
            const expectedDiagram = topicItems.filter((i) => i.contentType === 'diagram').length;
            const expectedScenarioQuestion = topicItems.filter((i) => i.contentType === 'scenario_question').length;

            expect(stat.studyMaterialCount).toBe(expectedStudyMaterial);
            expect(stat.codeSnippetCount).toBe(expectedCodeSnippet);
            expect(stat.diagramCount).toBe(expectedDiagram);
            expect(stat.scenarioQuestionCount).toBe(expectedScenarioQuestion);

            // All counts are non-negative
            expect(stat.studyMaterialCount).toBeGreaterThanOrEqual(0);
            expect(stat.codeSnippetCount).toBeGreaterThanOrEqual(0);
            expect(stat.diagramCount).toBeGreaterThanOrEqual(0);
            expect(stat.scenarioQuestionCount).toBeGreaterThanOrEqual(0);
          }
        }
      ),
      { numRuns: 100 }
    );
  });

  it('topics with no content items have zero counts for all types', () => {
    fc.assert(
      fc.property(
        fc.array(arbTopicConfig, { minLength: 1, maxLength: 10 }),
        (topics) => {
          // No content items at all
          const stats = aggregateTopicStats(topics, []);

          for (const stat of stats) {
            expect(stat.studyMaterialCount).toBe(0);
            expect(stat.codeSnippetCount).toBe(0);
            expect(stat.diagramCount).toBe(0);
            expect(stat.scenarioQuestionCount).toBe(0);
          }
        }
      ),
      { numRuns: 100 }
    );
  });

  it('total counts across all types equals total items for that topic', () => {
    const topicConfigs: TopicConfig[] = [
      { id: 'aws-services', displayName: 'AWS Services', order: 1, subtopics: [] },
      { id: 'databricks', displayName: 'Databricks', order: 2, subtopics: [] },
    ];

    const arbContentItemForTopics: fc.Arbitrary<ContentItem> = fc.record({
      topicId: fc.constantFrom('aws-services', 'databricks'),
      contentType: arbContentType,
    });

    fc.assert(
      fc.property(
        fc.array(arbContentItemForTopics, { minLength: 0, maxLength: 40 }),
        (contentItems) => {
          const stats = aggregateTopicStats(topicConfigs, contentItems);

          for (const stat of stats) {
            const topicItemCount = contentItems.filter((i) => i.topicId === stat.topicId).length;
            const totalFromStats =
              stat.studyMaterialCount +
              stat.codeSnippetCount +
              stat.diagramCount +
              stat.scenarioQuestionCount;

            expect(totalFromStats).toBe(topicItemCount);
          }
        }
      ),
      { numRuns: 100 }
    );
  });
});

describe('Feature: de-interview-prep-app, Property 3: Breadcrumb hierarchy correctness', () => {
  /**
   * **Validates: Requirements 1.4**
   *
   * For any navigation path consisting of a topic, subtopic, and content item,
   * the breadcrumb component SHALL produce an array of at most 3 items where
   * each item's label matches the display name of its corresponding level and
   * the levels appear in strict order: topic → subtopic → content.
   */
  it('breadcrumb produces at most 3 items with labels matching display names in strict level order', () => {
    fc.assert(
      fc.property(
        arbDisplayName,
        arbDisplayName,
        arbDisplayName,
        fc.string({ minLength: 1, maxLength: 20 }).map((s) => s.replace(/\s+/g, '-') || 'id'),
        fc.string({ minLength: 1, maxLength: 20 }).map((s) => s.replace(/\s+/g, '-') || 'sub-id'),
        (topicName, subtopicName, contentTitle, topicId, subtopicId) => {
          const breadcrumbs = buildBreadcrumbs({
            topicId,
            topicDisplayName: topicName,
            subtopicId,
            subtopicDisplayName: subtopicName,
            contentTitle,
          });

          // At most 3 items
          expect(breadcrumbs.length).toBeLessThanOrEqual(3);

          // For a full path, exactly 3 items
          expect(breadcrumbs.length).toBe(3);

          // Labels match display names
          expect(breadcrumbs[0].label).toBe(topicName);
          expect(breadcrumbs[1].label).toBe(subtopicName);
          expect(breadcrumbs[2].label).toBe(contentTitle);

          // Levels are in strict order: topic → subtopic → content
          expect(breadcrumbs[0].level).toBe('topic');
          expect(breadcrumbs[1].level).toBe('subtopic');
          expect(breadcrumbs[2].level).toBe('content');
        }
      ),
      { numRuns: 100 }
    );
  });

  it('breadcrumb with only topic produces exactly 1 item at topic level', () => {
    fc.assert(
      fc.property(
        arbDisplayName,
        fc.string({ minLength: 1, maxLength: 20 }).map((s) => s.replace(/\s+/g, '-') || 'id'),
        (topicName, topicId) => {
          const breadcrumbs = buildBreadcrumbs({
            topicId,
            topicDisplayName: topicName,
          });

          expect(breadcrumbs.length).toBe(1);
          expect(breadcrumbs[0].label).toBe(topicName);
          expect(breadcrumbs[0].level).toBe('topic');
          expect(breadcrumbs[0].url).toBe(`/topic/${topicId}/`);
        }
      ),
      { numRuns: 100 }
    );
  });

  it('breadcrumb with topic and subtopic produces exactly 2 items in order', () => {
    fc.assert(
      fc.property(
        arbDisplayName,
        arbDisplayName,
        fc.string({ minLength: 1, maxLength: 20 }).map((s) => s.replace(/\s+/g, '-') || 'id'),
        fc.string({ minLength: 1, maxLength: 20 }).map((s) => s.replace(/\s+/g, '-') || 'sub-id'),
        (topicName, subtopicName, topicId, subtopicId) => {
          const breadcrumbs = buildBreadcrumbs({
            topicId,
            topicDisplayName: topicName,
            subtopicId,
            subtopicDisplayName: subtopicName,
          });

          expect(breadcrumbs.length).toBe(2);
          expect(breadcrumbs[0].label).toBe(topicName);
          expect(breadcrumbs[0].level).toBe('topic');
          expect(breadcrumbs[1].label).toBe(subtopicName);
          expect(breadcrumbs[1].level).toBe('subtopic');
          expect(breadcrumbs[1].url).toBe(`/topic/${topicId}/${subtopicId}`);
        }
      ),
      { numRuns: 100 }
    );
  });

  it('breadcrumb with no navigation context produces empty array', () => {
    const breadcrumbs = buildBreadcrumbs({});
    expect(breadcrumbs.length).toBe(0);
  });

  it('breadcrumb levels never appear out of order', () => {
    const LEVEL_ORDER = { topic: 0, subtopic: 1, content: 2 };

    fc.assert(
      fc.property(
        fc.option(arbDisplayName, { nil: undefined }),
        fc.option(arbDisplayName, { nil: undefined }),
        fc.option(arbDisplayName, { nil: undefined }),
        fc.option(
          fc.string({ minLength: 1, maxLength: 20 }).map((s) => s.replace(/\s+/g, '-') || 'id'),
          { nil: undefined }
        ),
        fc.option(
          fc.string({ minLength: 1, maxLength: 20 }).map((s) => s.replace(/\s+/g, '-') || 'sub-id'),
          { nil: undefined }
        ),
        (topicName, subtopicName, contentTitle, topicId, subtopicId) => {
          const breadcrumbs = buildBreadcrumbs({
            topicId,
            topicDisplayName: topicName,
            subtopicId,
            subtopicDisplayName: subtopicName,
            contentTitle,
          });

          // At most 3 items
          expect(breadcrumbs.length).toBeLessThanOrEqual(3);

          // Levels are in strictly increasing order
          for (let i = 1; i < breadcrumbs.length; i++) {
            expect(LEVEL_ORDER[breadcrumbs[i].level]).toBeGreaterThan(
              LEVEL_ORDER[breadcrumbs[i - 1].level]
            );
          }
        }
      ),
      { numRuns: 100 }
    );
  });
});
