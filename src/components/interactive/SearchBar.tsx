import { useState, useCallback, useRef, useEffect } from 'react';
import { searchService, type SearchResult, type SearchFilters } from '../../services/searchService';
import SearchResults from './SearchResults';

/**
 * Available content types for filtering.
 */
const CONTENT_TYPES = [
  { id: 'study_material', label: 'Study Material' },
  { id: 'code_snippet', label: 'Code Snippet' },
  { id: 'scenario_question', label: 'Scenario Question' },
] as const;

/**
 * Available topics for filtering (loaded from config or index).
 */
const TOPICS = [
  { id: 'aws-services', label: 'AWS Services' },
  { id: 'databricks', label: 'Databricks' },
  { id: 'snowflake', label: 'Snowflake' },
  { id: 'hadoop', label: 'Hadoop' },
  { id: 'oracle', label: 'Oracle' },
  { id: 'teradata', label: 'Teradata' },
  { id: 'python', label: 'Python' },
  { id: 'pyspark', label: 'PySpark' },
  { id: 'sql', label: 'SQL' },
  { id: 'airflow', label: 'Airflow' },
  { id: 'bash-scripting', label: 'Bash Scripting' },
  { id: 'kafka', label: 'Kafka' },
  { id: 'nifi', label: 'NiFi' },
  { id: 'etl-concepts', label: 'ETL Concepts' },
  { id: 'power-bi', label: 'Power BI' },
  { id: 'rag-llm', label: 'RAG/LLM' },
  { id: 'ai', label: 'AI' },
  { id: 'data-modeling', label: 'Data Modeling' },
];

export interface SearchBarProps {
  /** URL to the search index JSON file */
  indexUrl?: string;
}

/**
 * SearchBar - Interactive search component with filtering.
 *
 * Triggers search on input ≥ 2 characters; clears results when query < 2 chars.
 * Includes filter controls for content type and topic (multiple selectable).
 * Results must return within 500ms.
 *
 * Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7
 */
export default function SearchBar({ indexUrl = '/search-index.json' }: SearchBarProps) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [initialized, setInitialized] = useState(false);
  const [initError, setInitError] = useState(false);
  const [showFilters, setShowFilters] = useState(false);
  const [selectedContentTypes, setSelectedContentTypes] = useState<string[]>([]);
  const [selectedTopics, setSelectedTopics] = useState<string[]>([]);

  const debounceTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const searchStartTime = useRef<number>(0);

  // Initialize search service on mount
  useEffect(() => {
    let cancelled = false;

    async function init() {
      try {
        await searchService.initialize(indexUrl);
        if (!cancelled) {
          setInitialized(true);
        }
      } catch {
        if (!cancelled) {
          setInitError(true);
        }
      }
    }

    init();
    return () => { cancelled = true; };
  }, [indexUrl]);

  /**
   * Execute search with current filters.
   */
  const executeSearch = useCallback(
    (searchQuery: string) => {
      if (searchQuery.length < 2) {
        setResults([]);
        setIsSearching(false);
        return;
      }

      if (!initialized) {
        return;
      }

      setIsSearching(true);
      searchStartTime.current = performance.now();

      const filters: SearchFilters = {};
      if (selectedContentTypes.length > 0) {
        filters.contentTypes = selectedContentTypes as SearchFilters['contentTypes'];
      }
      if (selectedTopics.length > 0) {
        filters.topics = selectedTopics;
      }

      const searchResults = searchService.search(searchQuery, filters, 50);
      setResults(searchResults);
      setIsSearching(false);
    },
    [initialized, selectedContentTypes, selectedTopics]
  );

  /**
   * Handle input change with debounce for performance.
   */
  const handleInputChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const value = e.target.value;
      setQuery(value);

      // Clear results immediately if query is too short
      if (value.length < 2) {
        setResults([]);
        setIsSearching(false);
        if (debounceTimer.current) {
          clearTimeout(debounceTimer.current);
          debounceTimer.current = null;
        }
        return;
      }

      // Debounce search execution for performance
      if (debounceTimer.current) {
        clearTimeout(debounceTimer.current);
      }

      setIsSearching(true);
      debounceTimer.current = setTimeout(() => {
        executeSearch(value);
      }, 150);
    },
    [executeSearch]
  );

  /**
   * Handle clearing the search input.
   */
  const handleClear = useCallback(() => {
    setQuery('');
    setResults([]);
    setIsSearching(false);
    if (debounceTimer.current) {
      clearTimeout(debounceTimer.current);
      debounceTimer.current = null;
    }
  }, []);

  /**
   * Toggle a content type filter.
   */
  const toggleContentType = useCallback(
    (typeId: string) => {
      setSelectedContentTypes((prev) => {
        const next = prev.includes(typeId)
          ? prev.filter((t) => t !== typeId)
          : [...prev, typeId];
        return next;
      });
    },
    []
  );

  /**
   * Toggle a topic filter.
   */
  const toggleTopic = useCallback(
    (topicId: string) => {
      setSelectedTopics((prev) => {
        const next = prev.includes(topicId)
          ? prev.filter((t) => t !== topicId)
          : [...prev, topicId];
        return next;
      });
    },
    []
  );

  // Re-execute search when filters change (if there's an active query)
  useEffect(() => {
    if (query.length >= 2) {
      executeSearch(query);
    }
  }, [selectedContentTypes, selectedTopics, executeSearch, query]);

  // Clean up debounce timer on unmount
  useEffect(() => {
    return () => {
      if (debounceTimer.current) {
        clearTimeout(debounceTimer.current);
      }
    };
  }, []);

  if (initError) {
    return (
      <div className="text-body-sm text-danger" role="alert">
        Search is currently unavailable. Please try again later.
      </div>
    );
  }

  return (
    <div className="relative w-full max-w-[640px]">
      {/* Search input */}
      <div className="relative flex items-center">
        {/* Search icon */}
        <svg
          className="absolute left-[12px] w-[20px] h-[20px] text-content-tertiary pointer-events-none"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
          />
        </svg>

        <input
          type="search"
          value={query}
          onChange={handleInputChange}
          placeholder="Search topics, code snippets, questions..."
          aria-label="Search content"
          aria-describedby="search-hint"
          className="w-full min-h-touch pl-[44px] pr-[80px] py-[8px] rounded-lg
            bg-surface border border-surface-secondary
            text-body text-content placeholder:text-content-tertiary
            focus:outline-none focus:ring-2 focus:ring-primary focus:border-primary
            transition-colors duration-200"
          autoComplete="off"
        />

        {/* Clear button */}
        {query.length > 0 && (
          <button
            type="button"
            onClick={handleClear}
            className="absolute right-[44px] min-w-touch min-h-touch flex items-center justify-center
              text-content-tertiary hover:text-content transition-colors duration-150"
            aria-label="Clear search"
          >
            <svg
              className="w-[18px] h-[18px]"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M6 18L18 6M6 6l12 12"
              />
            </svg>
          </button>
        )}

        {/* Filter toggle button */}
        <button
          type="button"
          onClick={() => setShowFilters((prev) => !prev)}
          className={`absolute right-[8px] min-w-touch min-h-touch flex items-center justify-center
            rounded-md transition-colors duration-150
            ${showFilters || selectedContentTypes.length > 0 || selectedTopics.length > 0
              ? 'text-primary bg-primary-light'
              : 'text-content-tertiary hover:text-content'}`}
          aria-label="Toggle search filters"
          aria-expanded={showFilters}
        >
          <svg
            className="w-[18px] h-[18px]"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z"
            />
          </svg>
          {/* Active filters indicator */}
          {(selectedContentTypes.length > 0 || selectedTopics.length > 0) && (
            <span className="absolute top-[6px] right-[6px] w-[8px] h-[8px] rounded-full bg-primary" />
          )}
        </button>
      </div>

      {/* Accessibility hint */}
      <span id="search-hint" className="sr-only">
        Type at least 2 characters to search. Use filter button to narrow results.
      </span>

      {/* Filter panel */}
      {showFilters && (
        <div
          className="absolute top-full left-0 right-0 mt-[8px] p-[16px] rounded-lg
            bg-surface border border-surface-secondary shadow-elevated z-50"
          role="group"
          aria-label="Search filters"
        >
          {/* Content type filters */}
          <div className="mb-[16px]">
            <h4 className="text-body-sm font-medium text-content mb-[8px]">Content Type</h4>
            <div className="flex flex-wrap gap-[8px]">
              {CONTENT_TYPES.map((type) => (
                <button
                  key={type.id}
                  type="button"
                  onClick={() => toggleContentType(type.id)}
                  className={`px-[12px] py-[4px] rounded-full text-body-sm font-medium
                    min-h-touch flex items-center transition-colors duration-150
                    ${selectedContentTypes.includes(type.id)
                      ? 'bg-primary text-white'
                      : 'bg-surface-secondary text-content-secondary hover:bg-surface-tertiary'}`}
                  aria-pressed={selectedContentTypes.includes(type.id)}
                >
                  {type.label}
                </button>
              ))}
            </div>
          </div>

          {/* Topic filters */}
          <div>
            <h4 className="text-body-sm font-medium text-content mb-[8px]">Topic</h4>
            <div className="flex flex-wrap gap-[8px] max-h-[200px] overflow-y-auto">
              {TOPICS.map((topic) => (
                <button
                  key={topic.id}
                  type="button"
                  onClick={() => toggleTopic(topic.id)}
                  className={`px-[12px] py-[4px] rounded-full text-body-sm font-medium
                    min-h-touch flex items-center transition-colors duration-150
                    ${selectedTopics.includes(topic.id)
                      ? 'bg-primary text-white'
                      : 'bg-surface-secondary text-content-secondary hover:bg-surface-tertiary'}`}
                  aria-pressed={selectedTopics.includes(topic.id)}
                >
                  {topic.label}
                </button>
              ))}
            </div>
          </div>

          {/* Clear all filters */}
          {(selectedContentTypes.length > 0 || selectedTopics.length > 0) && (
            <button
              type="button"
              onClick={() => {
                setSelectedContentTypes([]);
                setSelectedTopics([]);
              }}
              className="mt-[12px] text-body-sm text-primary hover:text-primary-dark
                font-medium transition-colors duration-150 min-h-touch flex items-center"
            >
              Clear all filters
            </button>
          )}
        </div>
      )}

      {/* Search results dropdown */}
      {query.length >= 2 && (
        <SearchResults
          results={results}
          query={query}
          isSearching={isSearching}
          hasActiveFilters={selectedContentTypes.length > 0 || selectedTopics.length > 0}
        />
      )}
    </div>
  );
}
