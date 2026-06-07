import { useState, useEffect, useCallback } from 'react';
import { createProgressService, type ProgressService, type TopicProgress } from '../../services/progressService';

/**
 * Topic metadata for display in the dashboard.
 * Order matches the fixed topic order from requirements.
 */
const TOPICS: { id: string; displayName: string }[] = [
  { id: 'aws-services', displayName: 'AWS Services' },
  { id: 'databricks', displayName: 'Databricks' },
  { id: 'snowflake', displayName: 'Snowflake' },
  { id: 'hadoop', displayName: 'Hadoop' },
  { id: 'oracle', displayName: 'Oracle' },
  { id: 'teradata', displayName: 'Teradata' },
  { id: 'python', displayName: 'Python' },
  { id: 'pyspark', displayName: 'PySpark' },
  { id: 'sql', displayName: 'SQL' },
  { id: 'airflow', displayName: 'Airflow' },
  { id: 'bash-scripting', displayName: 'Bash Scripting' },
  { id: 'kafka', displayName: 'Kafka' },
  { id: 'nifi', displayName: 'NiFi' },
  { id: 'etl-concepts', displayName: 'ETL Concepts' },
  { id: 'power-bi', displayName: 'Power BI' },
  { id: 'rag-llm', displayName: 'RAG/LLM' },
  { id: 'ai', displayName: 'AI' },
  { id: 'data-modeling', displayName: 'Data Modeling' },
];

/**
 * Default content totals per topic (study materials + scenario questions).
 * These would ideally come from build-time generated data;
 * for now each topic defaults to estimated totals that can be overridden via props.
 */
const DEFAULT_TOTALS: Record<string, { materials: number; questions: number }> = Object.fromEntries(
  TOPICS.map((t) => [t.id, { materials: 10, questions: 10 }])
);

export interface TopicTotals {
  [topicId: string]: { materials: number; questions: number };
}

export interface ProgressDashboardProps {
  /** Optional content totals per topic. Falls back to defaults if not provided. */
  topicTotals?: TopicTotals;
  /** Optional content ID to mark as reviewed (passed from parent page context). */
  contentIdToMark?: string;
  /** When true, only shows the Mark as Reviewed button without the full topic progress list. */
  compact?: boolean;
}

/**
 * ProgressDashboard - Displays per-topic progress percentages and provides
 * "Mark as Reviewed" and reset functionality. Rendered as a client:idle island.
 *
 * Features:
 * - Per-topic progress bar with percentage
 * - "Mark as Reviewed" button integration (marks the current content as reviewed)
 * - Reset progress per-topic or all with confirmation prompt
 * - Notification if localStorage is unavailable
 *
 * Requirements: 6.1, 6.3, 6.5, 6.6
 */
export default function ProgressDashboard({
  topicTotals,
  contentIdToMark,
  compact = false,
}: ProgressDashboardProps) {
  const [service] = useState<ProgressService>(() => createProgressService());
  const [progressData, setProgressData] = useState<Map<string, TopicProgress>>(new Map());
  const [storageAvailable, setStorageAvailable] = useState(true);
  const [quotaExceeded, setQuotaExceeded] = useState(false);
  const [resetConfirm, setResetConfirm] = useState<string | null>(null); // topicId or 'all'
  const [markedContent, setMarkedContent] = useState<string | null>(null);

  const totals = topicTotals ?? DEFAULT_TOTALS;

  /**
   * Refreshes progress data from the service for all topics.
   */
  const refreshProgress = useCallback(() => {
    const map = new Map<string, TopicProgress>();
    for (const topic of TOPICS) {
      const t = totals[topic.id] ?? { materials: 0, questions: 0 };
      const progress = service.getTopicProgress(topic.id, t.materials, t.questions);
      map.set(topic.id, progress);
    }
    setProgressData(map);
    setQuotaExceeded(service.isQuotaExceeded());
  }, [service, totals]);

  // Initialize on mount
  useEffect(() => {
    const available = service.isStorageAvailable();
    setStorageAvailable(available);
    refreshProgress();
  }, [service, refreshProgress]);

  /**
   * Handle "Mark as Reviewed" for the current content.
   */
  const handleMarkReviewed = useCallback(() => {
    if (!contentIdToMark) return;
    service.markReviewed(contentIdToMark);
    setMarkedContent(contentIdToMark);
    refreshProgress();
  }, [service, contentIdToMark, refreshProgress]);

  /**
   * Initiate reset confirmation (per-topic or all).
   */
  const handleResetRequest = useCallback((target: string) => {
    setResetConfirm(target);
  }, []);

  /**
   * Confirm and execute the reset.
   */
  const handleResetConfirm = useCallback(() => {
    if (!resetConfirm) return;

    if (resetConfirm === 'all') {
      service.resetAll();
    } else {
      service.resetTopic(resetConfirm);
    }

    setResetConfirm(null);
    setMarkedContent(null);
    refreshProgress();
  }, [service, resetConfirm, refreshProgress]);

  /**
   * Cancel the reset.
   */
  const handleResetCancel = useCallback(() => {
    setResetConfirm(null);
  }, []);

  /**
   * Get overall progress across all topics.
   */
  const getOverallProgress = useCallback((): number => {
    let totalReviewed = 0;
    let totalAttempted = 0;
    let totalItems = 0;

    for (const topic of TOPICS) {
      const t = totals[topic.id] ?? { materials: 0, questions: 0 };
      const progress = progressData.get(topic.id);
      totalReviewed += progress?.reviewedMaterials.size ?? 0;
      totalAttempted += progress?.attemptedQuestions.size ?? 0;
      totalItems += t.materials + t.questions;
    }

    if (totalItems === 0) return 0;
    return Math.min(100, Math.max(0, ((totalReviewed + totalAttempted) / totalItems) * 100));
  }, [progressData, totals]);

  const overallProgress = getOverallProgress();
  const isCurrentContentReviewed = contentIdToMark ? service.isReviewed(contentIdToMark) : false;

  return (
    <div
      className="w-full rounded-lg bg-[var(--color-surface)] border border-[var(--color-surface-tertiary)] shadow-card p-[16px]"
      data-testid="progress-dashboard"
    >
      {/* Storage unavailable notification */}
      {!storageAvailable && (
        <div
          className="mb-[16px] p-[12px] rounded-md bg-[var(--color-warning)]/10 border border-[var(--color-warning)]/30 flex items-start gap-[8px]"
          role="alert"
          aria-live="polite"
          data-testid="storage-unavailable-notice"
        >
          <svg
            className="w-[20px] h-[20px] text-[var(--color-warning)] shrink-0 mt-[2px]"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
            aria-hidden="true"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L3.732 16.5c-.77.833.192 2.5 1.732 2.5z"
            />
          </svg>
          <p className="text-body-sm text-[var(--color-content)]">
            Local storage is unavailable. Your progress cannot be saved and will be lost when you close this page.
          </p>
        </div>
      )}

      {/* Quota exceeded notification */}
      {quotaExceeded && storageAvailable && (
        <div
          className="mb-[16px] p-[12px] rounded-md bg-[var(--color-danger)]/10 border border-[var(--color-danger)]/30 flex items-start gap-[8px]"
          role="alert"
          aria-live="polite"
          data-testid="quota-exceeded-notice"
        >
          <svg
            className="w-[20px] h-[20px] text-[var(--color-danger)] shrink-0 mt-[2px]"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
            aria-hidden="true"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
          <p className="text-body-sm text-[var(--color-content)]">
            Storage quota exceeded. New progress cannot be saved. Consider resetting some topics to free space.
          </p>
        </div>
      )}

      {/* Header with overall progress and reset all */}
      {!compact && (
      <div className="flex items-center justify-between mb-[16px]">
        <div>
          <h3 className="text-heading-4 font-semibold text-[var(--color-content)] m-0">
            Progress
          </h3>
          <p className="text-body-sm text-[var(--color-content-secondary)] m-0 mt-[4px]">
            {overallProgress.toFixed(0)}% overall completed
          </p>
        </div>
        <button
          type="button"
          onClick={() => handleResetRequest('all')}
          className="min-h-[44px] px-[12px] py-[8px] rounded-md text-body-sm font-medium
            text-[var(--color-danger)] bg-transparent border border-[var(--color-danger)]/30
            hover:bg-[var(--color-danger)]/10 focus:outline-none focus:ring-2 focus:ring-[var(--color-danger)]
            transition-colors duration-150"
          aria-label="Reset all progress"
          data-testid="reset-all-btn"
        >
          Reset All
        </button>
      </div>
      )}

      {/* Overall progress bar */}
      {!compact && (
      <div className="mb-[24px]">
        <div
          className="w-full h-[8px] rounded-full bg-[var(--color-surface-tertiary)] overflow-hidden"
          role="progressbar"
          aria-valuenow={Math.round(overallProgress)}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-label="Overall progress"
        >
          <div
            className="h-full rounded-full bg-[var(--color-primary)] transition-all duration-300 ease-in-out"
            style={{ width: `${overallProgress}%` }}
          />
        </div>
      </div>
      )}

      {/* Mark as Reviewed button (when contentIdToMark is provided) */}
      {contentIdToMark && (
        <div className="mb-[24px] p-[12px] rounded-md bg-[var(--color-surface-secondary)] border border-[var(--color-surface-tertiary)]">
          {isCurrentContentReviewed || markedContent === contentIdToMark ? (
            <div className="flex items-center gap-[8px]" data-testid="marked-reviewed-indicator">
              <svg
                className="w-[20px] h-[20px] text-[var(--color-success)]"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
                aria-hidden="true"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M5 13l4 4L19 7"
                />
              </svg>
              <span className="text-body-sm font-medium text-[var(--color-success)]">
                Marked as reviewed
              </span>
            </div>
          ) : (
            <button
              type="button"
              onClick={handleMarkReviewed}
              className="min-h-[44px] w-full px-[16px] py-[8px] rounded-md text-body-sm font-medium
                text-white bg-[var(--color-primary)] hover:bg-[var(--color-primary-dark)]
                focus:outline-none focus:ring-2 focus:ring-[var(--color-primary)]
                transition-colors duration-150"
              data-testid="mark-reviewed-btn"
            >
              Mark as Reviewed
            </button>
          )}
        </div>
      )}

      {/* Per-topic progress list */}
      {!compact && (
      <div className="space-y-[12px]" data-testid="topic-progress-list">
        {TOPICS.map((topic) => {
          const progress = progressData.get(topic.id);
          const percentage = progress?.percentage ?? 0;
          const reviewed = progress?.reviewedMaterials.size ?? 0;
          const attempted = progress?.attemptedQuestions.size ?? 0;
          const t = totals[topic.id] ?? { materials: 0, questions: 0 };
          const total = t.materials + t.questions;

          return (
            <div
              key={topic.id}
              className="flex items-center gap-[12px] group"
              data-testid={`topic-progress-${topic.id}`}
            >
              {/* Topic name */}
              <span className="text-body-sm text-[var(--color-content)] font-medium min-w-[120px] shrink-0">
                {topic.displayName}
              </span>

              {/* Progress bar */}
              <div className="flex-1 min-w-0">
                <div
                  className="w-full h-[6px] rounded-full bg-[var(--color-surface-tertiary)] overflow-hidden"
                  role="progressbar"
                  aria-valuenow={Math.round(percentage)}
                  aria-valuemin={0}
                  aria-valuemax={100}
                  aria-label={`${topic.displayName} progress`}
                >
                  <div
                    className={`h-full rounded-full transition-all duration-300 ease-in-out ${
                      percentage >= 100
                        ? 'bg-[var(--color-success)]'
                        : percentage >= 50
                        ? 'bg-[var(--color-primary)]'
                        : 'bg-[var(--color-accent)]'
                    }`}
                    style={{ width: `${percentage}%` }}
                  />
                </div>
              </div>

              {/* Percentage and count */}
              <span className="text-caption text-[var(--color-content-secondary)] min-w-[72px] text-right shrink-0">
                {percentage.toFixed(0)}% ({reviewed + attempted}/{total})
              </span>

              {/* Reset topic button */}
              <button
                type="button"
                onClick={() => handleResetRequest(topic.id)}
                className="min-w-[44px] min-h-[44px] flex items-center justify-center rounded-md
                  text-[var(--color-content-tertiary)] hover:text-[var(--color-danger)]
                  hover:bg-[var(--color-danger)]/10
                  opacity-0 group-hover:opacity-100 focus:opacity-100
                  focus:outline-none focus:ring-2 focus:ring-[var(--color-danger)]
                  transition-all duration-150"
                aria-label={`Reset progress for ${topic.displayName}`}
                data-testid={`reset-topic-${topic.id}`}
              >
                <svg
                  className="w-[16px] h-[16px]"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                  aria-hidden="true"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
                  />
                </svg>
              </button>
            </div>
          );
        })}
      </div>
      )}

      {/* Reset confirmation modal */}
      {resetConfirm && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
          role="dialog"
          aria-modal="true"
          aria-labelledby="reset-confirm-title"
          data-testid="reset-confirm-dialog"
          onClick={(e) => {
            // Close on backdrop click
            if (e.target === e.currentTarget) handleResetCancel();
          }}
          onKeyDown={(e) => {
            // Close on Escape key (Requirement 5.4)
            if (e.key === 'Escape') {
              e.preventDefault();
              handleResetCancel();
            }
          }}
        >
          <div className="bg-[var(--color-surface)] rounded-lg shadow-modal p-[24px] max-w-[400px] w-[90%] mx-[16px]">
            <h4
              id="reset-confirm-title"
              className="text-heading-4 font-semibold text-[var(--color-content)] m-0 mb-[12px]"
            >
              Reset Progress
            </h4>
            <p className="text-body text-[var(--color-content-secondary)] m-0 mb-[24px]">
              {resetConfirm === 'all'
                ? 'Are you sure you want to reset progress for all topics? This action cannot be undone.'
                : `Are you sure you want to reset progress for "${TOPICS.find((t) => t.id === resetConfirm)?.displayName ?? resetConfirm}"? This action cannot be undone.`}
            </p>
            <div className="flex items-center justify-end gap-[12px]">
              <button
                type="button"
                onClick={handleResetCancel}
                className="min-h-[44px] px-[16px] py-[8px] rounded-md text-body-sm font-medium
                  text-[var(--color-content)] bg-[var(--color-surface-secondary)]
                  hover:bg-[var(--color-surface-tertiary)]
                  focus:outline-none focus:ring-2 focus:ring-[var(--color-primary)]
                  transition-colors duration-150"
                data-testid="reset-cancel-btn"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleResetConfirm}
                className="min-h-[44px] px-[16px] py-[8px] rounded-md text-body-sm font-medium
                  text-white bg-[var(--color-danger)] hover:bg-[var(--color-danger-dark)]
                  focus:outline-none focus:ring-2 focus:ring-[var(--color-danger)]
                  transition-colors duration-150"
                data-testid="reset-confirm-btn"
              >
                Reset
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
