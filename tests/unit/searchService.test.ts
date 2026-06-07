import { describe, it, expect, beforeEach } from 'vitest';
import { SearchService, type SearchIndexEntry, type SearchFilters } from '../../src/services/searchService';

function createMockEntry(overrides: Partial<SearchIndexEntry> = {}): SearchIndexEntry {
  return {
    id: 'aws-services/s3/fundamentals',
    title: 'S3 Bucket Fundamentals',
    topic: 'aws-services',
    topicDisplayName: 'AWS Services',
    subtopic: 's3',
    subtopicDisplayName: 'S3',
    contentType: 'study_material',
    difficultyLevel: null,
    body: 'Amazon S3 is an object storage service offering industry-leading scalability, data availability, security, and performance.',
    url: '/topic/aws-services/s3',
    ...overrides,
  };
}

function createTestIndex(): SearchIndexEntry[] {
  return [
    createMockEntry(),
    createMockEntry({
      id: 'aws-services/glue/fundamentals',
      title: 'AWS Glue ETL Basics',
      topic: 'aws-services',
      topicDisplayName: 'AWS Services',
      subtopic: 'glue',
      subtopicDisplayName: 'Glue',
      contentType: 'code_snippet',
      body: 'AWS Glue is a serverless data integration service that makes it easy to discover, prepare, and combine data for analytics.',
      url: '/topic/aws-services/glue',
    }),
    createMockEntry({
      id: 'databricks/delta-lake/fundamentals',
      title: 'Delta Lake Fundamentals',
      topic: 'databricks',
      topicDisplayName: 'Databricks',
      subtopic: 'delta-lake',
      subtopicDisplayName: 'Delta Lake',
      contentType: 'study_material',
      body: 'Delta Lake is an open-source storage framework that enables building a Lakehouse architecture with ACID transactions.',
      url: '/topic/databricks/delta-lake',
    }),
    createMockEntry({
      id: 'databricks/delta-lake/scenarios',
      title: 'Delta Lake Interview Scenarios',
      topic: 'databricks',
      topicDisplayName: 'Databricks',
      subtopic: 'delta-lake',
      subtopicDisplayName: 'Delta Lake',
      contentType: 'scenario_question',
      difficultyLevel: 'senior',
      body: 'Scenario: You need to handle concurrent writes to a Delta table in a high-throughput streaming pipeline.',
      url: '/topic/databricks/delta-lake/scenarios',
    }),
    createMockEntry({
      id: 'pyspark/dataframe-api/fundamentals',
      title: 'PySpark DataFrame API',
      topic: 'pyspark',
      topicDisplayName: 'PySpark',
      subtopic: 'dataframe-api',
      subtopicDisplayName: 'DataFrame API',
      contentType: 'study_material',
      body: 'PySpark DataFrame is a distributed collection of data organized into named columns similar to a table in relational database.',
      url: '/topic/pyspark/dataframe-api',
    }),
  ];
}

describe('SearchService', () => {
  let service: SearchService;

  beforeEach(() => {
    service = new SearchService();
    service.initializeWithData(createTestIndex());
  });

  describe('search() - basic behavior', () => {
    it('returns empty array for query with fewer than 2 characters', () => {
      expect(service.search('')).toEqual([]);
      expect(service.search('a')).toEqual([]);
    });

    it('returns results for queries with 2 or more characters', () => {
      const results = service.search('S3');
      expect(results.length).toBeGreaterThan(0);
    });

    it('returns results matching title', () => {
      const results = service.search('Delta Lake');
      expect(results.length).toBeGreaterThan(0);
      expect(results.some((r) => r.title.includes('Delta Lake'))).toBe(true);
    });

    it('returns results matching body content', () => {
      const results = service.search('serverless data integration');
      expect(results.length).toBeGreaterThan(0);
      expect(results.some((r) => r.id.includes('glue'))).toBe(true);
    });

    it('returns max 50 results by default', () => {
      const results = service.search('data');
      expect(results.length).toBeLessThanOrEqual(50);
    });

    it('respects custom limit parameter', () => {
      const results = service.search('data', undefined, 2);
      expect(results.length).toBeLessThanOrEqual(2);
    });

    it('results are sorted by score (ascending Fuse score = best match first)', () => {
      const results = service.search('Delta Lake');
      for (let i = 1; i < results.length; i++) {
        expect(results[i].score).toBeGreaterThanOrEqual(results[i - 1].score);
      }
    });

    it('each result includes topic, subtopic, and contentType', () => {
      const results = service.search('data');
      for (const result of results) {
        expect(result.topic).toBeTruthy();
        expect(result.subtopic).toBeTruthy();
        expect(result.contentType).toBeTruthy();
        expect(['study_material', 'code_snippet', 'scenario_question']).toContain(result.contentType);
      }
    });

    it('each result includes highlights array', () => {
      const results = service.search('Delta');
      for (const result of results) {
        expect(Array.isArray(result.highlights)).toBe(true);
      }
    });
  });

  describe('search() - filtering', () => {
    it('filters by content type', () => {
      const filters: SearchFilters = { contentTypes: ['code_snippet'] };
      const results = service.search('AWS', filters);
      for (const result of results) {
        expect(result.contentType).toBe('code_snippet');
      }
    });

    it('filters by topic', () => {
      const filters: SearchFilters = { topics: ['databricks'] };
      const results = service.search('data', filters);
      for (const result of results) {
        expect(result.topic).toBe('Databricks');
      }
    });

    it('applies intersection logic when both filters active', () => {
      const filters: SearchFilters = {
        contentTypes: ['study_material'],
        topics: ['databricks'],
      };
      const results = service.search('Delta', filters);
      for (const result of results) {
        expect(result.contentType).toBe('study_material');
        expect(result.topic).toBe('Databricks');
      }
    });

    it('returns empty when filters exclude all matches', () => {
      const filters: SearchFilters = {
        contentTypes: ['scenario_question'],
        topics: ['pyspark'],
      };
      const results = service.search('S3 bucket', filters);
      expect(results.length).toBe(0);
    });

    it('returns all results when filters are empty arrays', () => {
      const filters: SearchFilters = { contentTypes: [], topics: [] };
      const allResults = service.search('data');
      const filteredResults = service.search('data', filters);
      expect(filteredResults.length).toBe(allResults.length);
    });
  });

  describe('search() - error handling', () => {
    it('throws when service is not initialized', () => {
      const uninitializedService = new SearchService();
      expect(() => uninitializedService.search('test query')).toThrow('SearchService not initialized');
    });
  });

  describe('getHighlightedExcerpt()', () => {
    it('returns excerpt unchanged when no highlights exist', () => {
      const result = {
        id: 'test',
        title: 'Test',
        topic: 'aws-services',
        subtopic: 's3',
        contentType: 'study_material' as const,
        excerpt: 'This is a test excerpt',
        score: 0.5,
        url: '/test',
        highlights: [],
      };
      expect(service.getHighlightedExcerpt(result)).toBe('This is a test excerpt');
    });

    it('wraps highlighted text in <mark> tags for body matches', () => {
      const result = {
        id: 'test',
        title: 'Test Title',
        topic: 'aws-services',
        subtopic: 's3',
        contentType: 'study_material' as const,
        excerpt: 'Amazon S3 is great',
        score: 0.2,
        url: '/test',
        highlights: [
          { field: 'body', indices: [[7, 8] as [number, number]] },
        ],
      };
      const highlighted = service.getHighlightedExcerpt(result);
      expect(highlighted).toContain('<mark>');
      expect(highlighted).toContain('</mark>');
    });

    it('falls back to title highlights when no body highlights exist', () => {
      const result = {
        id: 'test',
        title: 'Delta Lake',
        topic: 'databricks',
        subtopic: 'delta-lake',
        contentType: 'study_material' as const,
        excerpt: 'Some excerpt',
        score: 0.2,
        url: '/test',
        highlights: [
          { field: 'title', indices: [[0, 4] as [number, number]] },
        ],
      };
      const highlighted = service.getHighlightedExcerpt(result);
      expect(highlighted).toContain('<mark>Delta</mark>');
    });
  });

  describe('initializeWithData()', () => {
    it('allows direct initialization without fetch', () => {
      const newService = new SearchService();
      newService.initializeWithData([createMockEntry()]);
      const results = newService.search('S3');
      expect(results.length).toBeGreaterThan(0);
    });
  });
});
