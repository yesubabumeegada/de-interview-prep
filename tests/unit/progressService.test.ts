import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createProgressService, type ProgressService, type ProgressData } from '../../src/services/progressService';

describe('ProgressService', () => {
  let service: ProgressService;
  let localStorageMock: Record<string, string>;
  let storageMock: Storage;

  beforeEach(() => {
    localStorageMock = {};
    storageMock = {
      getItem: vi.fn((key: string) => localStorageMock[key] ?? null),
      setItem: vi.fn((key: string, value: string) => {
        localStorageMock[key] = value;
      }),
      removeItem: vi.fn((key: string) => {
        delete localStorageMock[key];
      }),
      clear: vi.fn(() => {
        localStorageMock = {};
      }),
      get length() {
        return Object.keys(localStorageMock).length;
      },
      key: vi.fn((index: number) => Object.keys(localStorageMock)[index] ?? null),
    } as unknown as Storage;

    Object.defineProperty(globalThis, 'localStorage', {
      value: storageMock,
      writable: true,
      configurable: true,
    });

    // Mock window for isBrowser check
    Object.defineProperty(globalThis, 'window', {
      value: {},
      writable: true,
      configurable: true,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('initialization', () => {
    it('starts with empty progress when no stored data', () => {
      service = createProgressService();
      expect(service.isReviewed('aws-services/s3/fundamentals')).toBe(false);
      expect(service.isAttempted('aws-services/s3/scenario-1')).toBe(false);
    });

    it('loads existing progress data from localStorage', () => {
      const stored: ProgressData = {
        version: 1,
        lastUpdated: '2024-01-01T00:00:00.000Z',
        reviewed: ['aws-services/s3/fundamentals'],
        attempted: ['aws-services/s3/scenario-1'],
      };
      localStorageMock['de-prep-progress'] = JSON.stringify(stored);

      service = createProgressService();
      expect(service.isReviewed('aws-services/s3/fundamentals')).toBe(true);
      expect(service.isAttempted('aws-services/s3/scenario-1')).toBe(true);
    });

    it('starts fresh when stored data has wrong version', () => {
      const stored = {
        version: 99,
        lastUpdated: '2024-01-01T00:00:00.000Z',
        reviewed: ['aws-services/s3/fundamentals'],
        attempted: [],
      };
      localStorageMock['de-prep-progress'] = JSON.stringify(stored);

      service = createProgressService();
      expect(service.isReviewed('aws-services/s3/fundamentals')).toBe(false);
    });

    it('starts fresh when stored data is malformed JSON', () => {
      localStorageMock['de-prep-progress'] = 'not valid json{{{';
      service = createProgressService();
      expect(service.isReviewed('anything')).toBe(false);
    });

    it('starts fresh when stored data is missing required fields', () => {
      localStorageMock['de-prep-progress'] = JSON.stringify({ version: 1 });
      service = createProgressService();
      expect(service.isReviewed('anything')).toBe(false);
    });
  });

  describe('markReviewed / isReviewed', () => {
    beforeEach(() => {
      service = createProgressService();
    });

    it('marks a content ID as reviewed', () => {
      service.markReviewed('aws-services/s3/fundamentals');
      expect(service.isReviewed('aws-services/s3/fundamentals')).toBe(true);
    });

    it('returns false for content IDs that have not been reviewed', () => {
      expect(service.isReviewed('databricks/delta-lake/fundamentals')).toBe(false);
    });

    it('persists reviewed state to localStorage', () => {
      service.markReviewed('aws-services/s3/fundamentals');
      const stored = JSON.parse(localStorageMock['de-prep-progress']) as ProgressData;
      expect(stored.reviewed).toContain('aws-services/s3/fundamentals');
    });

    it('does not duplicate entries when marking same ID twice', () => {
      service.markReviewed('aws-services/s3/fundamentals');
      service.markReviewed('aws-services/s3/fundamentals');
      const stored = JSON.parse(localStorageMock['de-prep-progress']) as ProgressData;
      const count = stored.reviewed.filter((id) => id === 'aws-services/s3/fundamentals').length;
      expect(count).toBe(1);
    });
  });

  describe('markAttempted / isAttempted', () => {
    beforeEach(() => {
      service = createProgressService();
    });

    it('marks a question ID as attempted', () => {
      service.markAttempted('aws-services/s3/scenario-1');
      expect(service.isAttempted('aws-services/s3/scenario-1')).toBe(true);
    });

    it('returns false for questions that have not been attempted', () => {
      expect(service.isAttempted('pyspark/rdd-operations/scenario-3')).toBe(false);
    });

    it('persists attempted state to localStorage', () => {
      service.markAttempted('aws-services/s3/scenario-1');
      const stored = JSON.parse(localStorageMock['de-prep-progress']) as ProgressData;
      expect(stored.attempted).toContain('aws-services/s3/scenario-1');
    });

    it('does not duplicate entries when marking same ID twice', () => {
      service.markAttempted('aws-services/s3/scenario-1');
      service.markAttempted('aws-services/s3/scenario-1');
      const stored = JSON.parse(localStorageMock['de-prep-progress']) as ProgressData;
      const count = stored.attempted.filter((id) => id === 'aws-services/s3/scenario-1').length;
      expect(count).toBe(1);
    });
  });

  describe('getTopicProgress', () => {
    beforeEach(() => {
      service = createProgressService();
    });

    it('returns 0% for a topic with no reviewed/attempted items', () => {
      const progress = service.getTopicProgress('aws-services', 10, 5);
      expect(progress.percentage).toBe(0);
      expect(progress.reviewedMaterials.size).toBe(0);
      expect(progress.attemptedQuestions.size).toBe(0);
    });

    it('calculates percentage correctly with reviewed and attempted items', () => {
      service.markReviewed('aws-services/s3/fundamentals');
      service.markReviewed('aws-services/s3/intermediate');
      service.markAttempted('aws-services/s3/scenario-1');

      // 3 items out of (10 materials + 5 questions) = 3/15 * 100 = 20%
      const progress = service.getTopicProgress('aws-services', 10, 5);
      expect(progress.percentage).toBe(20);
      expect(progress.reviewedMaterials.size).toBe(2);
      expect(progress.attemptedQuestions.size).toBe(1);
    });

    it('returns 100% when all items are reviewed/attempted', () => {
      service.markReviewed('aws-services/s3/fundamentals');
      service.markReviewed('aws-services/s3/intermediate');
      service.markAttempted('aws-services/s3/scenario-1');

      // 3 items out of (2 materials + 1 question) = 3/3 * 100 = 100%
      const progress = service.getTopicProgress('aws-services', 2, 1);
      expect(progress.percentage).toBe(100);
    });

    it('clamps percentage to 100 if more items reviewed than total', () => {
      service.markReviewed('aws-services/s3/fundamentals');
      service.markReviewed('aws-services/s3/intermediate');
      service.markReviewed('aws-services/s3/senior');
      service.markAttempted('aws-services/s3/scenario-1');

      // 4 items out of (1 material + 1 question) = 4/2 * 100 = 200% → clamped to 100%
      const progress = service.getTopicProgress('aws-services', 1, 1);
      expect(progress.percentage).toBe(100);
    });

    it('returns 0% when total is 0 (avoids division by zero)', () => {
      const progress = service.getTopicProgress('aws-services', 0, 0);
      expect(progress.percentage).toBe(0);
    });

    it('only counts items belonging to the specified topic', () => {
      service.markReviewed('aws-services/s3/fundamentals');
      service.markReviewed('databricks/delta-lake/fundamentals');
      service.markAttempted('aws-services/s3/scenario-1');
      service.markAttempted('pyspark/rdd-operations/scenario-1');

      const awsProgress = service.getTopicProgress('aws-services', 5, 5);
      expect(awsProgress.reviewedMaterials.size).toBe(1);
      expect(awsProgress.attemptedQuestions.size).toBe(1);
      // 2 out of 10 = 20%
      expect(awsProgress.percentage).toBe(20);
    });

    it('includes topicId and totals in the returned object', () => {
      const progress = service.getTopicProgress('databricks', 8, 3);
      expect(progress.topicId).toBe('databricks');
      expect(progress.totalMaterials).toBe(8);
      expect(progress.totalQuestions).toBe(3);
    });
  });

  describe('resetTopic', () => {
    beforeEach(() => {
      service = createProgressService();
    });

    it('removes all progress for a specific topic', () => {
      service.markReviewed('aws-services/s3/fundamentals');
      service.markReviewed('aws-services/glue/fundamentals');
      service.markAttempted('aws-services/s3/scenario-1');
      service.markReviewed('databricks/delta-lake/fundamentals');

      service.resetTopic('aws-services');

      expect(service.isReviewed('aws-services/s3/fundamentals')).toBe(false);
      expect(service.isReviewed('aws-services/glue/fundamentals')).toBe(false);
      expect(service.isAttempted('aws-services/s3/scenario-1')).toBe(false);
      // Other topic data should remain
      expect(service.isReviewed('databricks/delta-lake/fundamentals')).toBe(true);
    });

    it('persists the reset to localStorage', () => {
      service.markReviewed('aws-services/s3/fundamentals');
      service.resetTopic('aws-services');

      const stored = JSON.parse(localStorageMock['de-prep-progress']) as ProgressData;
      expect(stored.reviewed).not.toContain('aws-services/s3/fundamentals');
    });
  });

  describe('resetAll', () => {
    beforeEach(() => {
      service = createProgressService();
    });

    it('removes all progress for all topics', () => {
      service.markReviewed('aws-services/s3/fundamentals');
      service.markReviewed('databricks/delta-lake/fundamentals');
      service.markAttempted('pyspark/rdd-operations/scenario-1');

      service.resetAll();

      expect(service.isReviewed('aws-services/s3/fundamentals')).toBe(false);
      expect(service.isReviewed('databricks/delta-lake/fundamentals')).toBe(false);
      expect(service.isAttempted('pyspark/rdd-operations/scenario-1')).toBe(false);
    });

    it('removes the storage key from localStorage', () => {
      service.markReviewed('aws-services/s3/fundamentals');
      service.resetAll();
      expect(storageMock.removeItem).toHaveBeenCalledWith('de-prep-progress');
    });
  });

  describe('isStorageAvailable', () => {
    it('returns true when localStorage is functional', () => {
      service = createProgressService();
      expect(service.isStorageAvailable()).toBe(true);
    });

    it('returns false when localStorage throws on setItem', () => {
      Object.defineProperty(globalThis, 'localStorage', {
        value: {
          getItem: () => null,
          setItem: () => { throw new Error('SecurityError'); },
          removeItem: () => {},
        },
        writable: true,
        configurable: true,
      });
      service = createProgressService();
      expect(service.isStorageAvailable()).toBe(false);
    });
  });

  describe('quota exceeded handling', () => {
    it('sets quota exceeded flag when storage write fails with QuotaExceededError', () => {
      service = createProgressService();
      expect(service.isQuotaExceeded()).toBe(false);

      // Simulate quota exceeded on next write
      (storageMock.setItem as ReturnType<typeof vi.fn>).mockImplementation(() => {
        const error = new DOMException('Quota exceeded', 'QuotaExceededError');
        throw error;
      });

      service.markReviewed('aws-services/s3/fundamentals');
      expect(service.isQuotaExceeded()).toBe(true);
    });

    it('stops further writes after quota exceeded', () => {
      service = createProgressService();

      // Simulate quota exceeded on next write
      (storageMock.setItem as ReturnType<typeof vi.fn>).mockImplementation(() => {
        const error = new DOMException('Quota exceeded', 'QuotaExceededError');
        throw error;
      });

      service.markReviewed('aws-services/s3/fundamentals');

      // Clear mock to verify no more calls
      (storageMock.setItem as ReturnType<typeof vi.fn>).mockClear();

      service.markReviewed('aws-services/s3/intermediate');
      expect(storageMock.setItem).not.toHaveBeenCalled();
    });

    it('still tracks items in memory even after quota exceeded', () => {
      service = createProgressService();

      // First mark succeeds
      service.markReviewed('aws-services/s3/fundamentals');

      // Now simulate quota exceeded
      (storageMock.setItem as ReturnType<typeof vi.fn>).mockImplementation(() => {
        const error = new DOMException('Quota exceeded', 'QuotaExceededError');
        throw error;
      });

      service.markReviewed('aws-services/s3/intermediate');

      // In-memory state should still reflect the mark
      expect(service.isReviewed('aws-services/s3/fundamentals')).toBe(true);
      expect(service.isReviewed('aws-services/s3/intermediate')).toBe(true);
    });

    it('resets quota exceeded flag after resetAll', () => {
      service = createProgressService();

      (storageMock.setItem as ReturnType<typeof vi.fn>).mockImplementation(() => {
        const error = new DOMException('Quota exceeded', 'QuotaExceededError');
        throw error;
      });

      service.markReviewed('aws-services/s3/fundamentals');
      expect(service.isQuotaExceeded()).toBe(true);

      // resetAll should clear the quota exceeded flag
      service.resetAll();
      expect(service.isQuotaExceeded()).toBe(false);
    });
  });

  describe('persistence across re-initialization', () => {
    it('data persists when a new service instance is created (simulates page reload)', () => {
      service = createProgressService();
      service.markReviewed('aws-services/s3/fundamentals');
      service.markAttempted('aws-services/s3/scenario-1');

      // Create a new instance (simulates page reload)
      const service2 = createProgressService();
      expect(service2.isReviewed('aws-services/s3/fundamentals')).toBe(true);
      expect(service2.isAttempted('aws-services/s3/scenario-1')).toBe(true);
    });
  });

  describe('getAllProgress', () => {
    it('returns a map of all topics with progress', () => {
      service = createProgressService();
      service.markReviewed('aws-services/s3/fundamentals');
      service.markReviewed('databricks/delta-lake/fundamentals');
      service.markAttempted('aws-services/s3/scenario-1');

      const all = service.getAllProgress();
      expect(all.has('aws-services')).toBe(true);
      expect(all.has('databricks')).toBe(true);
      expect(all.get('aws-services')!.reviewedMaterials.size).toBe(1);
      expect(all.get('aws-services')!.attemptedQuestions.size).toBe(1);
      expect(all.get('databricks')!.reviewedMaterials.size).toBe(1);
    });

    it('returns empty map when no progress exists', () => {
      service = createProgressService();
      const all = service.getAllProgress();
      expect(all.size).toBe(0);
    });
  });
});
