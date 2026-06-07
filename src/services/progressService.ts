/**
 * ProgressService - Tracks user progress through study materials and scenario questions.
 * Persists data in localStorage under key "de-prep-progress" with a versioned JSON schema.
 * Handles SSR/build contexts where window/localStorage may be unavailable.
 * On quota exceeded, stops writes and flags a notification.
 *
 * Storage key: "de-prep-progress"
 * Schema version: 1
 *
 * Content IDs follow format: {topic}/{subtopic}/{slug}
 * Topic progress percentage: (reviewed + attempted) / (totalMaterials + totalQuestions) * 100, clamped [0, 100]
 */

const STORAGE_KEY = 'de-prep-progress';
const SCHEMA_VERSION = 1;

/**
 * Persisted data shape in localStorage.
 */
export interface ProgressData {
  version: 1;
  lastUpdated: string; // ISO 8601 timestamp
  reviewed: string[];  // Array of content IDs marked as reviewed
  attempted: string[]; // Array of scenario question IDs attempted
}

/**
 * Represents progress for a single topic.
 */
export interface TopicProgress {
  topicId: string;
  reviewedMaterials: Set<string>;
  attemptedQuestions: Set<string>;
  totalMaterials: number;
  totalQuestions: number;
  percentage: number;
}

/**
 * Public interface for the ProgressService.
 */
export interface ProgressService {
  markReviewed(contentId: string): void;
  markAttempted(questionId: string): void;
  isReviewed(contentId: string): boolean;
  isAttempted(questionId: string): boolean;
  getTopicProgress(topicId: string, totalMaterials: number, totalQuestions: number): TopicProgress;
  getAllProgress(): Map<string, TopicProgress>;
  resetTopic(topicId: string): void;
  resetAll(): void;
  isStorageAvailable(): boolean;
  /** Returns true if a quota exceeded error has occurred and writes are disabled. */
  isQuotaExceeded(): boolean;
}

/**
 * Checks if we're in a browser environment with DOM/storage APIs available.
 */
function isBrowser(): boolean {
  return typeof window !== 'undefined' && typeof localStorage !== 'undefined';
}

/**
 * Creates an empty ProgressData object.
 */
function createEmptyData(): ProgressData {
  return {
    version: SCHEMA_VERSION,
    lastUpdated: new Date().toISOString(),
    reviewed: [],
    attempted: [],
  };
}

/**
 * Safely reads and parses the progress data from localStorage.
 * Returns null if storage is unavailable, data is missing, or data is malformed.
 */
function readProgressData(): ProgressData | null {
  if (!isBrowser()) return null;
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;

    const parsed = JSON.parse(raw);

    // Validate schema version and structure
    if (
      parsed &&
      typeof parsed === 'object' &&
      parsed.version === SCHEMA_VERSION &&
      Array.isArray(parsed.reviewed) &&
      Array.isArray(parsed.attempted) &&
      typeof parsed.lastUpdated === 'string'
    ) {
      return parsed as ProgressData;
    }

    // Invalid or outdated schema — return null so caller initializes fresh data
    return null;
  } catch {
    // JSON parse error or localStorage access error
    return null;
  }
}

/**
 * Safely writes progress data to localStorage.
 * Returns false if a quota exceeded error occurred.
 */
function writeProgressData(data: ProgressData): boolean {
  if (!isBrowser()) return false;
  try {
    data.lastUpdated = new Date().toISOString();
    localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
    return true;
  } catch (error: unknown) {
    // Check for quota exceeded (DOMException with name "QuotaExceededError")
    if (
      error instanceof DOMException &&
      (error.name === 'QuotaExceededError' || error.name === 'NS_ERROR_DOM_QUOTA_REACHED')
    ) {
      return false;
    }
    // Other storage errors — also treat as write failure
    return false;
  }
}

/**
 * Extracts the topic ID from a content ID.
 * Content IDs follow format: {topic}/{subtopic}/{slug}
 * Returns the first segment as the topic ID.
 */
function extractTopicId(contentId: string): string {
  const slashIndex = contentId.indexOf('/');
  if (slashIndex === -1) return contentId;
  return contentId.substring(0, slashIndex);
}

/**
 * Clamps a number to the range [min, max].
 */
function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

/**
 * Calculates progress percentage: (R + A) / (N + M) * 100, clamped to [0, 100].
 * If total is 0, returns 0 (avoid division by zero).
 */
function calculatePercentage(reviewed: number, attempted: number, totalMaterials: number, totalQuestions: number): number {
  const total = totalMaterials + totalQuestions;
  if (total === 0) return 0;
  const raw = ((reviewed + attempted) / total) * 100;
  return clamp(raw, 0, 100);
}

/**
 * Creates and returns a ProgressService instance.
 */
export function createProgressService(): ProgressService {
  // In-memory cache of progress data
  let data: ProgressData = readProgressData() ?? createEmptyData();

  // Sets for O(1) lookups
  let reviewedSet: Set<string> = new Set(data.reviewed);
  let attemptedSet: Set<string> = new Set(data.attempted);

  // Tracks whether a quota exceeded error has occurred
  let quotaExceeded = false;

  /**
   * Persists current in-memory state to localStorage.
   * If quota is exceeded, sets the quotaExceeded flag and stops future writes.
   */
  function persist(): void {
    if (quotaExceeded) return;

    data.reviewed = Array.from(reviewedSet);
    data.attempted = Array.from(attemptedSet);

    const success = writeProgressData(data);
    if (!success) {
      quotaExceeded = true;
    }
  }

  /**
   * Reloads data from localStorage into the in-memory cache.
   * Useful after external modifications or for testing re-initialization scenarios.
   */
  function reload(): void {
    data = readProgressData() ?? createEmptyData();
    reviewedSet = new Set(data.reviewed);
    attemptedSet = new Set(data.attempted);
  }

  return {
    markReviewed(contentId: string): void {
      if (!reviewedSet.has(contentId)) {
        reviewedSet.add(contentId);
        persist();
      }
    },

    markAttempted(questionId: string): void {
      if (!attemptedSet.has(questionId)) {
        attemptedSet.add(questionId);
        persist();
      }
    },

    isReviewed(contentId: string): boolean {
      return reviewedSet.has(contentId);
    },

    isAttempted(questionId: string): boolean {
      return attemptedSet.has(questionId);
    },

    getTopicProgress(topicId: string, totalMaterials: number, totalQuestions: number): TopicProgress {
      // Filter reviewed/attempted items that belong to this topic
      const topicReviewed = new Set<string>();
      const topicAttempted = new Set<string>();

      for (const id of reviewedSet) {
        if (extractTopicId(id) === topicId) {
          topicReviewed.add(id);
        }
      }

      for (const id of attemptedSet) {
        if (extractTopicId(id) === topicId) {
          topicAttempted.add(id);
        }
      }

      const percentage = calculatePercentage(
        topicReviewed.size,
        topicAttempted.size,
        totalMaterials,
        totalQuestions
      );

      return {
        topicId,
        reviewedMaterials: topicReviewed,
        attemptedQuestions: topicAttempted,
        totalMaterials,
        totalQuestions,
        percentage,
      };
    },

    getAllProgress(): Map<string, TopicProgress> {
      // Group all content IDs by topic
      const topicIds = new Set<string>();

      for (const id of reviewedSet) {
        topicIds.add(extractTopicId(id));
      }
      for (const id of attemptedSet) {
        topicIds.add(extractTopicId(id));
      }

      const progressMap = new Map<string, TopicProgress>();

      for (const topicId of topicIds) {
        // Without external totals, we report what we know
        // Totals default to 0 — callers should use getTopicProgress with real totals
        progressMap.set(topicId, this.getTopicProgress(topicId, 0, 0));
      }

      return progressMap;
    },

    resetTopic(topicId: string): void {
      // Remove all entries belonging to this topic
      for (const id of [...reviewedSet]) {
        if (extractTopicId(id) === topicId) {
          reviewedSet.delete(id);
        }
      }
      for (const id of [...attemptedSet]) {
        if (extractTopicId(id) === topicId) {
          attemptedSet.delete(id);
        }
      }
      persist();
    },

    resetAll(): void {
      reviewedSet.clear();
      attemptedSet.clear();
      data = createEmptyData();

      if (!isBrowser()) return;
      try {
        localStorage.removeItem(STORAGE_KEY);
      } catch {
        // Silently fail if storage is unavailable
      }
      // Reset quota exceeded flag since we cleared data
      quotaExceeded = false;
    },

    isStorageAvailable(): boolean {
      if (!isBrowser()) return false;
      try {
        const testKey = '__de-prep-storage-test__';
        localStorage.setItem(testKey, 'test');
        const retrieved = localStorage.getItem(testKey);
        localStorage.removeItem(testKey);
        return retrieved === 'test';
      } catch {
        return false;
      }
    },

    isQuotaExceeded(): boolean {
      return quotaExceeded;
    },
  };
}

// Export a singleton instance for use across the application
export const progressService: ProgressService = createProgressService();
