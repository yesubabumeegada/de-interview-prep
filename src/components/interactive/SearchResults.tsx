import { useMemo } from 'react';
import DOMPurify from 'isomorphic-dompurify';
import { type SearchResult } from '../../services/searchService';
import { searchService } from '../../services/searchService';

const sanitize = (html: string) => DOMPurify.sanitize(html, { ALLOWED_TAGS: ['mark', 'span', 'b', 'em'], ALLOWED_ATTR: ['class'] });

/**
 * Content type display configuration.
 */
const CONTENT_TYPE_CONFIG: Record<string, { label: string; color: string; bgColor: string }> = {
  study_material: {
    label: 'Study Material',
    color: 'text-primary',
    bgColor: 'bg-primary-light',
  },
  code_snippet: {
    label: 'Code Snippet',
    color: 'text-accent',
    bgColor: 'bg-accent-light',
  },
  scenario_question: {
    label: 'Scenario Question',
    color: 'text-success',
    bgColor: 'bg-success-light',
  },
};

export interface SearchResultsProps {
  /** The search results to display */
  results: SearchResult[];
  /** The current search query */
  query: string;
  /** Whether search is in progress */
  isSearching: boolean;
  /** Whether any filters are currently active */
  hasActiveFilters: boolean;
}

/**
 * SearchResults - Displays search results with highlighted matching terms,
 * topic/subtopic labels, and content type badges.
 *
 * Shows "no results" message with suggestion when empty.
 * Results display matching terms highlighted using <mark> tags from SearchService.
 *
 * Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7
 */
export default function SearchResults({
  results,
  query,
  isSearching,
  hasActiveFilters,
}: SearchResultsProps) {
  /**
   * Generate highlighted excerpts for all results.
   */
  const highlightedResults = useMemo(() => {
    return results.map((result) => ({
      ...result,
      highlightedExcerpt: searchService.getHighlightedExcerpt(result),
      highlightedTitle: getHighlightedTitle(result, query),
    }));
  }, [results, query]);

  // Loading state
  if (isSearching) {
    return (
      <div
        className="absolute top-full left-0 right-0 mt-[8px] p-[16px] rounded-lg
          bg-surface border border-surface-secondary shadow-elevated z-40"
        role="status"
        aria-live="polite"
        aria-label="Searching"
      >
        <div className="flex items-center gap-[12px]">
          {/* Spinner */}
          <svg
            className="w-[20px] h-[20px] animate-spin text-primary"
            fill="none"
            viewBox="0 0 24 24"
            aria-hidden="true"
          >
            <circle
              className="opacity-25"
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
            />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
            />
          </svg>
          <span className="text-body-sm text-content-secondary">Searching...</span>
        </div>
      </div>
    );
  }

  // No results state
  if (results.length === 0 && query.length >= 2) {
    return (
      <div
        className="absolute top-full left-0 right-0 mt-[8px] p-[24px] rounded-lg
          bg-surface border border-surface-secondary shadow-elevated z-40"
        role="status"
        aria-live="polite"
      >
        <div className="text-center">
          {/* No results icon */}
          <svg
            className="w-[40px] h-[40px] mx-auto mb-[12px] text-content-tertiary"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
            aria-hidden="true"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={1.5}
              d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
          <p className="text-body font-medium text-content mb-[8px]">
            No results found for &ldquo;{query}&rdquo;
          </p>
          <p className="text-body-sm text-content-secondary">
            {hasActiveFilters
              ? 'Try removing some filters or modifying your search query.'
              : 'Try a different search term or check the spelling.'}
          </p>
        </div>
      </div>
    );
  }

  // Results list
  if (results.length === 0) {
    return null;
  }

  return (
    <div
      className="absolute top-full left-0 right-0 mt-[8px] rounded-lg
        bg-surface border border-surface-secondary shadow-elevated z-40
        max-h-[480px] overflow-y-auto custom-scrollbar"
      role="listbox"
      aria-label={`Search results: ${results.length} ${results.length === 1 ? 'result' : 'results'} found`}
    >
      {/* Results count header */}
      <div className="sticky top-0 px-[16px] py-[8px] bg-surface-secondary border-b border-surface-tertiary">
        <span className="text-caption text-content-tertiary">
          {results.length} {results.length === 1 ? 'result' : 'results'} found
        </span>
      </div>

      {/* Result items */}
      <ul className="py-[4px]">
        {highlightedResults.map((result) => (
          <li key={result.id} role="option" aria-selected={false}>
            <a
              href={result.url}
              className="block px-[16px] py-[12px] min-h-touch hover:bg-surface-secondary
                focus:bg-surface-secondary focus:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-primary
                transition-colors duration-150 border-b border-surface-secondary last:border-b-0"
            >
              {/* Top row: title + content type badge */}
              <div className="flex items-start justify-between gap-[12px] mb-[4px]">
                <h4
                  className="text-body font-medium text-content flex-1 line-clamp-1"
                  dangerouslySetInnerHTML={{ __html: sanitize(result.highlightedTitle) }}
                />
                <ContentTypeBadge contentType={result.contentType} />
              </div>

              {/* Topic / Subtopic labels */}
              <div className="flex items-center gap-[8px] mb-[4px]">
                <span className="text-caption text-content-secondary font-medium">
                  {result.topic}
                </span>
                <span className="text-caption text-content-tertiary" aria-hidden="true">
                  ›
                </span>
                <span className="text-caption text-content-tertiary">
                  {result.subtopic}
                </span>
              </div>

              {/* Excerpt with highlights */}
              <p
                className="text-body-sm text-content-secondary line-clamp-2
                  [&_mark]:bg-warning-light [&_mark]:text-content [&_mark]:rounded-sm [&_mark]:px-[2px]"
                dangerouslySetInnerHTML={{ __html: sanitize(result.highlightedExcerpt) }}
              />
            </a>
          </li>
        ))}
      </ul>
    </div>
  );
}

/**
 * ContentTypeBadge - Displays a colored badge for the content type.
 */
function ContentTypeBadge({
  contentType,
}: {
  contentType: 'study_material' | 'code_snippet' | 'scenario_question';
}) {
  const config = CONTENT_TYPE_CONFIG[contentType] || CONTENT_TYPE_CONFIG.study_material;

  return (
    <span
      className={`inline-flex items-center px-[8px] py-[2px] rounded-full text-caption font-medium
        whitespace-nowrap ${config.color} ${config.bgColor}`}
    >
      {config.label}
    </span>
  );
}

/**
 * Generate highlighted title by wrapping query matches in <mark> tags.
 * Uses case-insensitive matching on the query terms within the title.
 */
function getHighlightedTitle(result: SearchResult, query: string): string {
  // Check if we have title highlights from Fuse.js
  const titleHighlight = result.highlights.find((h) => h.field === 'title');

  if (titleHighlight && titleHighlight.indices.length > 0) {
    return applyMarks(result.title, titleHighlight.indices);
  }

  // Fallback: simple text matching for highlighting
  return highlightTextMatch(result.title, query);
}

/**
 * Apply <mark> tags at given indices (from Fuse.js matches).
 * Indices are Fuse.js-style inclusive [start, end] pairs.
 */
function applyMarks(text: string, indices: [number, number][]): string {
  if (!indices.length || !text) {
    return escapeHtml(text);
  }

  // Sort indices ascending by start position
  const sortedIndices = [...indices].sort((a, b) => a[0] - b[0]);

  let result = '';
  let lastIndex = 0;

  for (const [start, end] of sortedIndices) {
    const clampedStart = Math.max(0, Math.min(start, text.length));
    const clampedEnd = Math.max(0, Math.min(end + 1, text.length)); // Fuse indices are inclusive

    if (clampedStart >= clampedEnd || clampedStart < lastIndex) continue;

    // Add text before this match (escaped)
    result += escapeHtml(text.slice(lastIndex, clampedStart));
    // Add matched text in <mark> tags (escaped)
    result += `<mark>${escapeHtml(text.slice(clampedStart, clampedEnd))}</mark>`;
    lastIndex = clampedEnd;
  }

  // Add remaining text after last match
  result += escapeHtml(text.slice(lastIndex));

  return result;
}

/**
 * Simple text match highlighting as a fallback.
 */
function highlightTextMatch(text: string, query: string): string {
  if (!query || query.length < 2) {
    return escapeHtml(text);
  }

  const escapedQuery = query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const splitRegex = new RegExp(`(${escapedQuery})`, 'gi');
  const parts = text.split(splitRegex);

  return parts
    .map((part) =>
      part.toLowerCase() === query.toLowerCase()
        ? `<mark>${escapeHtml(part)}</mark>`
        : escapeHtml(part)
    )
    .join('');
}

/**
 * Escape HTML special characters to prevent XSS.
 */
function escapeHtml(text: string): string {
  if (!text) return '';
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}
