import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import * as fc from 'fast-check';
import { createProgressService, type ProgressService } from '../../src/services/progressService';

/**
 * Property-based tests for ProgressService
 *
 * Feature: de-interview-prep-app
 * Validates: Requirements 6.1, 6.2, 6.3
 */

// --- Generators ---

/**
 * Generates a segment suitable for topic/subtopic/slug (lowercase alphanumeric + hyphens).
 * Ensures non-empty and starts/ends with alphanumeric.
 */
const segmentArb = fc
  .stringOf(fc.constantFrom(...'abcdefghijklmnopqrstuvwxyz0123456789-'.split('')), {
    minLength: 1,
    maxLength: 20,
  })
  .filter((s) => /^[a-z0-9]/.test(s) && /[a-z0-9]$/.test(s) && !s.includes('--'));

/**
 * Generates a content ID in format: topic/subtopic/slug
 */
const contentIdArb = fc.tuple(segmentArb, segmentArb, segmentArb).map(([topic, subtopic, slug]) => `${topic}/${subtopic}/${slug}`);

/**
 * Generates a topic ID (first segment of a content ID).
 */
const topicIdArb = segmentArb;

/**
 * Generates a positive integer for material/question counts.
 */
const positiveIntArb = fc.integer({ min: 1, max: 100 });

/**
 * Generates a set of content IDs all belonging to the same topic.
 */
function contentIdsForTopicArb(topicId: string) {
  return fc
    .array(
      fc.tuple(segmentArb, segmentArb).map(([subtopic, slug]) => `${topicId}/${subtopic}/${slug}`),
      { minLength: 0, maxLength: 20 }
    )
    .map((ids) => [...new Set(ids)]); // deduplicate
}

// --- LocalStorage Mock Setup ---

let localStorageMock: Record<string, string>;
let storageMock: Storage;

function setupLocalStorageMock() {
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

  Object.defineProperty(globalThis, 'window', {
    value: {},
    writable: true,
    configurable: true,
  });
}

// --- Property Tests ---

describe('Feature: de-interview-prep-app, Property 12: Progress mark and retrieve round-trip', () => {
  beforeEach(() => {
    setupLocalStorageMock();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  /**
   * **Validates: Requirements 6.1, 6.2**
   *
   * For any content ID string, after markReviewed(id) then isReviewed(id) returns true;
   * after markAttempted(id) then isAttempted(id) returns true.
   * Also persists across re-initialization.
   */
  it('markReviewed(id) then isReviewed(id) returns true for any content ID', () => {
    fc.assert(
      fc.property(contentIdArb, (contentId) => {
        // Reset storage for each iteration
        localStorageMock = {};

        const service = createProgressService();
        service.markReviewed(contentId);
        expect(service.isReviewed(contentId)).toBe(true);
      }),
      { numRuns: 100 }
    );
  });

  it('markAttempted(id) then isAttempted(id) returns true for any content ID', () => {
    fc.assert(
      fc.property(contentIdArb, (contentId) => {
        // Reset storage for each iteration
        localStorageMock = {};

        const service = createProgressService();
        service.markAttempted(contentId);
        expect(service.isAttempted(contentId)).toBe(true);
      }),
      { numRuns: 100 }
    );
  });

  it('reviewed state persists across re-initialization (simulates page reload)', () => {
    fc.assert(
      fc.property(contentIdArb, (contentId) => {
        // Reset storage for each iteration
        localStorageMock = {};

        const service1 = createProgressService();
        service1.markReviewed(contentId);

        // Create a new service instance (simulates page reload)
        const service2 = createProgressService();
        expect(service2.isReviewed(contentId)).toBe(true);
      }),
      { numRuns: 100 }
    );
  });

  it('attempted state persists across re-initialization (simulates page reload)', () => {
    fc.assert(
      fc.property(contentIdArb, (contentId) => {
        // Reset storage for each iteration
        localStorageMock = {};

        const service1 = createProgressService();
        service1.markAttempted(contentId);

        // Create a new service instance (simulates page reload)
        const service2 = createProgressService();
        expect(service2.isAttempted(contentId)).toBe(true);
      }),
      { numRuns: 100 }
    );
  });

  it('marking multiple IDs preserves all of them across re-initialization', () => {
    fc.assert(
      fc.property(
        fc.array(contentIdArb, { minLength: 1, maxLength: 10 }),
        fc.array(contentIdArb, { minLength: 1, maxLength: 10 }),
        (reviewedIds, attemptedIds) => {
          // Reset storage for each iteration
          localStorageMock = {};

          const service1 = createProgressService();

          // Mark all reviewed
          for (const id of reviewedIds) {
            service1.markReviewed(id);
          }
          // Mark all attempted
          for (const id of attemptedIds) {
            service1.markAttempted(id);
          }

          // Re-initialize
          const service2 = createProgressService();

          // All reviewed IDs should persist
          for (const id of reviewedIds) {
            expect(service2.isReviewed(id)).toBe(true);
          }
          // All attempted IDs should persist
          for (const id of attemptedIds) {
            expect(service2.isAttempted(id)).toBe(true);
          }
        }
      ),
      { numRuns: 100 }
    );
  });
});

describe('Feature: de-interview-prep-app, Property 13: Progress percentage calculation', () => {
  beforeEach(() => {
    setupLocalStorageMock();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  /**
   * **Validates: Requirements 6.3**
   *
   * For any topic with N materials and M questions, where R reviewed and A attempted,
   * percentage equals (R + A) / (N + M) * 100 clamped to [0, 100].
   */
  it('percentage equals (R + A) / (N + M) * 100 clamped to [0, 100]', () => {
    fc.assert(
      fc.property(
        topicIdArb,
        positiveIntArb,
        positiveIntArb,
        fc.integer({ min: 0, max: 20 }),
        fc.integer({ min: 0, max: 20 }),
        (topicId, totalMaterials, totalQuestions, reviewedCount, attemptedCount) => {
          // Reset storage for each iteration
          localStorageMock = {};

          const service = createProgressService();

          // Generate unique reviewed IDs for this topic
          for (let i = 0; i < reviewedCount; i++) {
            service.markReviewed(`${topicId}/sub${i}/material-${i}`);
          }

          // Generate unique attempted IDs for this topic
          for (let i = 0; i < attemptedCount; i++) {
            service.markAttempted(`${topicId}/sub${i}/question-${i}`);
          }

          const progress = service.getTopicProgress(topicId, totalMaterials, totalQuestions);

          // Calculate expected percentage
          const total = totalMaterials + totalQuestions;
          const expectedRaw = ((reviewedCount + attemptedCount) / total) * 100;
          const expected = Math.min(Math.max(expectedRaw, 0), 100);

          expect(progress.percentage).toBeCloseTo(expected, 10);
          expect(progress.percentage).toBeGreaterThanOrEqual(0);
          expect(progress.percentage).toBeLessThanOrEqual(100);
        }
      ),
      { numRuns: 100 }
    );
  });

  it('percentage is 0 when no items are reviewed or attempted', () => {
    fc.assert(
      fc.property(
        topicIdArb,
        positiveIntArb,
        positiveIntArb,
        (topicId, totalMaterials, totalQuestions) => {
          // Reset storage for each iteration
          localStorageMock = {};

          const service = createProgressService();
          const progress = service.getTopicProgress(topicId, totalMaterials, totalQuestions);

          expect(progress.percentage).toBe(0);
        }
      ),
      { numRuns: 100 }
    );
  });

  it('percentage is 0 when total materials and questions are both 0', () => {
    fc.assert(
      fc.property(topicIdArb, (topicId) => {
        // Reset storage for each iteration
        localStorageMock = {};

        const service = createProgressService();
        // Even if we mark some items, with total 0 the percentage should be 0
        service.markReviewed(`${topicId}/sub1/material-1`);
        service.markAttempted(`${topicId}/sub1/question-1`);

        const progress = service.getTopicProgress(topicId, 0, 0);
        expect(progress.percentage).toBe(0);
      }),
      { numRuns: 100 }
    );
  });

  it('percentage is clamped to 100 when reviewed + attempted exceeds total', () => {
    fc.assert(
      fc.property(
        topicIdArb,
        fc.integer({ min: 1, max: 5 }),
        fc.integer({ min: 1, max: 5 }),
        (topicId, totalMaterials, totalQuestions) => {
          // Reset storage for each iteration
          localStorageMock = {};

          const service = createProgressService();
          const total = totalMaterials + totalQuestions;

          // Mark more items than total
          const itemCount = total + 5;
          for (let i = 0; i < itemCount; i++) {
            service.markReviewed(`${topicId}/sub${i}/material-${i}`);
          }

          const progress = service.getTopicProgress(topicId, totalMaterials, totalQuestions);
          expect(progress.percentage).toBe(100);
        }
      ),
      { numRuns: 100 }
    );
  });

  it('only counts items belonging to the specified topic', () => {
    fc.assert(
      fc.property(
        topicIdArb,
        topicIdArb,
        positiveIntArb,
        positiveIntArb,
        (topicA, topicB, totalMaterials, totalQuestions) => {
          // Ensure topics are different
          fc.pre(topicA !== topicB);

          // Reset storage for each iteration
          localStorageMock = {};

          const service = createProgressService();

          // Mark items for topicA
          service.markReviewed(`${topicA}/sub1/material-1`);
          service.markAttempted(`${topicA}/sub1/question-1`);

          // Mark items for topicB
          service.markReviewed(`${topicB}/sub1/material-1`);

          // topicA should only count its own items
          const progressA = service.getTopicProgress(topicA, totalMaterials, totalQuestions);
          expect(progressA.reviewedMaterials.size).toBe(1);
          expect(progressA.attemptedQuestions.size).toBe(1);

          // topicB should only count its own items
          const progressB = service.getTopicProgress(topicB, totalMaterials, totalQuestions);
          expect(progressB.reviewedMaterials.size).toBe(1);
          expect(progressB.attemptedQuestions.size).toBe(0);
        }
      ),
      { numRuns: 100 }
    );
  });
});
