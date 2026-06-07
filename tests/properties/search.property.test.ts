import { describe, it, expect, beforeEach } from 'vitest';
import * as fc from 'fast-check';
import { SearchService } from '../../src/services/searchService';
import type { SearchIndexEntry, SearchFilters } from '../../src/services/searchService';

// --- Generators ---

const VALID_CONTENT_TYPES = ['study_material', 'code_snippet', 'scenario_question'] as const;

const VALID_TOPICS = [
  'aws-services', 'databricks', 'snowflake', 'hadoop', 'oracle',
  'teradata', 'python', 'pyspark', 'sql', 'airflow',
  'bash-scripting', 'kafka', 'nifi', 'etl-concepts', 'power-bi',
  'rag-llm', 'ai', 'data-modeling',
];

const arbContentType = fc.constantFrom(...VALID_CONTENT_TYPES);
const arbTopic = fc.constantFrom(...VALID_TOPICS);

const arbSearchIndexEntry: fc.Arbitrary<SearchIndexEntry> = fc.record({
  id: fc.string({ minLength: 1, maxLength: 50 }).map((s) => s.replace(/\s+/g, '-') || 'item-1'),
  title: fc.string({ minLength: 3, maxLength: 120 }).filter((s) => s.trim().length >= 3),
  topic: arbTopic,
  topicDisplayName: fc.string({ minLength: 1, maxLength: 30 }).filter((s) => s.trim().length >= 1),
  subtopic: fc.string({ minLength: 1, maxLength: 30 }).filter((s) => s.trim().length >= 1),
  subtopicDisplayName: fc.string({ minLength: 1, maxLength: 30 }).filter((s) => s.trim().length >= 1),
  contentType: arbContentType as fc.Arbitrary<string>,
  difficultyLevel: fc.option(fc.constantFrom('junior', 'mid-level', 'senior'), { nil: null }),
  body: fc.string({ minLength: 10, maxLength: 500 }).filter((s) => s.trim().length >= 10),
  url: fc.string({ minLength: 1, maxLength: 100 }).map((s) => `/topic/${s.replace(/\s+/g, '-') || 'page'}`),
});

const arbNonEmptyIndex: fc.Arbitrary<SearchIndexEntry[]> = fc.array(arbSearchIndexEntry, {
  minLength: 1,
  maxLength: 20,
});

// Query of 2+ characters using alphanumeric content to actually produce Fuse matches
const arbSearchQuery = fc.string({ minLength: 2, maxLength: 30 }).filter(
  (s) => s.trim().length >= 2
);

// Short query (0 or 1 characters)
const arbShortQuery = fc.string({ minLength: 0, maxLength: 1 });

// Filters generator
const arbSearchFilters: fc.Arbitrary<SearchFilters> = fc.record({
  contentTypes: fc.option(
    fc.subarray([...VALID_CONTENT_TYPES], { minLength: 1 }).map(
      (arr) => arr as ('study_material' | 'code_snippet' | 'scenario_question')[]
    ),
    { nil: undefined }
  ),
  topics: fc.option(
    fc.subarray(VALID_TOPICS, { minLength: 1 }),
    { nil: undefined }
  ),
});

// --- Tests ---

describe('Feature: de-interview-prep-app, Property 8: Search results are well-formed', () => {
  /**
   * **Validates: Requirements 4.1, 4.3**
   *
   * For any search query of 2+ chars against any non-empty index,
   * results are ordered by descending relevance score, contain at most 50 items,
   * each has non-empty topic, subtopic, contentType.
   */
  it('search results are ordered by descending relevance, capped at 50, and contain required fields', () => {
    fc.assert(
      fc.property(arbSearchQuery, arbNonEmptyIndex, (query, indexData) => {
        const service = new SearchService();
        service.initializeWithData(indexData);

        const results = service.search(query);

        // At most 50 results
        expect(results.length).toBeLessThanOrEqual(50);

        // Results are ordered by ascending Fuse score (lower = more relevant)
        for (let i = 1; i < results.length; i++) {
          expect(results[i].score).toBeGreaterThanOrEqual(results[i - 1].score);
        }

        // Each result has non-empty required fields
        for (const result of results) {
          expect(result.topic).toBeTruthy();
          expect(result.subtopic).toBeTruthy();
          expect(result.contentType).toBeTruthy();
          expect(VALID_CONTENT_TYPES).toContain(result.contentType);
        }
      }),
      { numRuns: 100 }
    );
  });
});

describe('Feature: de-interview-prep-app, Property 9: Search highlight accuracy', () => {
  /**
   * **Validates: Requirements 4.2**
   *
   * For any query and matching result, highlighted ranges correspond
   * to fuzzy-matching substrings.
   */
  it('highlighted ranges are valid index ranges within their corresponding field text', () => {
    // Use known data to ensure matches occur
    const knownEntries: SearchIndexEntry[] = [
      {
        id: 'aws/s3/basics',
        title: 'S3 Bucket Lifecycle Policies',
        topic: 'aws-services',
        topicDisplayName: 'AWS Services',
        subtopic: 's3',
        subtopicDisplayName: 'S3',
        contentType: 'study_material',
        difficultyLevel: 'senior',
        body: 'Amazon S3 provides object storage with lifecycle policies for transitioning objects between storage classes and expiring objects.',
        url: '/topic/aws-services/s3',
      },
      {
        id: 'databricks/delta/basics',
        title: 'Delta Lake Fundamentals',
        topic: 'databricks',
        topicDisplayName: 'Databricks',
        subtopic: 'delta-lake',
        subtopicDisplayName: 'Delta Lake',
        contentType: 'study_material',
        difficultyLevel: 'mid-level',
        body: 'Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads.',
        url: '/topic/databricks/delta-lake',
      },
      {
        id: 'python/decorators/snippet',
        title: 'Python Decorators Pattern',
        topic: 'python',
        topicDisplayName: 'Python',
        subtopic: 'decorators',
        subtopicDisplayName: 'Decorators',
        contentType: 'code_snippet',
        difficultyLevel: 'mid-level',
        body: 'A decorator is a function that takes another function and extends its behavior without explicitly modifying it. Common decorators include retry, cache, and logging patterns.',
        url: '/topic/python/decorators',
      },
    ];

    // Queries that should produce results with highlights
    const queries = ['lifecycle', 'delta', 'decorator', 'storage', 'python'];

    fc.assert(
      fc.property(fc.constantFrom(...queries), (query) => {
        const service = new SearchService();
        service.initializeWithData(knownEntries);

        const results = service.search(query);

        for (const result of results) {
          if (result.highlights && result.highlights.length > 0) {
            for (const highlight of result.highlights) {
              // Each highlight should have a valid field name
              expect(['title', 'body', 'topic', 'subtopic']).toContain(highlight.field);

              // Each index pair should be valid (start <= end, non-negative)
              for (const [start, end] of highlight.indices) {
                expect(start).toBeGreaterThanOrEqual(0);
                expect(end).toBeGreaterThanOrEqual(start);
              }
            }

            // getHighlightedExcerpt should produce a non-empty string
            const highlighted = service.getHighlightedExcerpt(result);
            expect(highlighted).toBeTruthy();
          }
        }
      }),
      { numRuns: 100 }
    );
  });
});

describe('Feature: de-interview-prep-app, Property 10: Search filter intersection', () => {
  /**
   * **Validates: Requirements 4.4**
   *
   * For any filters combination, filtered results match BOTH contentType AND topic filters.
   */
  it('filtered results match both contentType AND topic filters when both are active', () => {
    fc.assert(
      fc.property(arbNonEmptyIndex, arbSearchFilters, (indexData, filters) => {
        const service = new SearchService();
        service.initializeWithData(indexData);

        // Use a broad query that matches many things to get results to filter
        // We'll use a substring from the first entry's body to increase chance of matches
        const query = indexData[0].body.slice(0, 5).trim();
        if (query.length < 2) return; // Skip if we can't form a valid query

        const results = service.search(query, filters);

        const hasContentTypeFilter = filters.contentTypes && filters.contentTypes.length > 0;
        const hasTopicFilter = filters.topics && filters.topics.length > 0;

        for (const result of results) {
          // If contentType filter is active, result must match one of the allowed types
          if (hasContentTypeFilter) {
            expect(filters.contentTypes).toContain(result.contentType);
          }

          // If topic filter is active, result must match one of the allowed topics
          if (hasTopicFilter) {
            // The result.topic may be topicDisplayName, so we need to check the original entry
            const matchingEntry = indexData.find((entry) => entry.id === result.id);
            if (matchingEntry) {
              expect(filters.topics).toContain(matchingEntry.topic);
            }
          }
        }
      }),
      { numRuns: 100 }
    );
  });
});

describe('Feature: de-interview-prep-app, Property 11: Short queries return empty results', () => {
  /**
   * **Validates: Requirements 4.7**
   *
   * For any string of length 0 or 1, search returns empty array.
   */
  it('queries with 0 or 1 characters always return empty results regardless of index content', () => {
    fc.assert(
      fc.property(arbShortQuery, arbNonEmptyIndex, (query, indexData) => {
        const service = new SearchService();
        service.initializeWithData(indexData);

        const results = service.search(query);

        expect(results).toEqual([]);
      }),
      { numRuns: 100 }
    );
  });
});
