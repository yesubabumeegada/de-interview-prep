import Fuse from 'fuse.js';

// --- Interfaces ---

export interface SearchResult {
  id: string;
  title: string;
  topic: string;
  subtopic: string;
  contentType: 'study_material' | 'code_snippet' | 'scenario_question';
  excerpt: string;
  score: number;
  url: string;
  highlights: { field: string; indices: [number, number][] }[];
}

export interface SearchFilters {
  contentTypes?: ('study_material' | 'code_snippet' | 'scenario_question')[];
  topics?: string[];
}

export interface SearchIndexEntry {
  id: string;
  title: string;
  topic: string;
  topicDisplayName: string;
  subtopic: string;
  subtopicDisplayName: string;
  contentType: string;
  difficultyLevel: string | null;
  body: string;
  url: string;
}

export interface ISearchService {
  initialize(indexUrl: string): Promise<void>;
  search(query: string, filters?: SearchFilters, limit?: number): SearchResult[];
  getHighlightedExcerpt(result: SearchResult): string;
}

// --- Fuse.js Configuration ---

const fuseOptions: Fuse.IFuseOptions<SearchIndexEntry> = {
  keys: [
    { name: 'title', weight: 0.4 },
    { name: 'body', weight: 0.3 },
    { name: 'topic', weight: 0.15 },
    { name: 'subtopic', weight: 0.15 },
  ],
  threshold: 0.3,
  includeScore: true,
  includeMatches: true,
  minMatchCharLength: 2,
};

// --- SearchService Implementation ---

export class SearchService implements ISearchService {
  private fuse: Fuse<SearchIndexEntry> | null = null;
  private indexData: SearchIndexEntry[] = [];

  /**
   * Initialize the search service by fetching the search index and creating the Fuse instance.
   */
  async initialize(indexUrl: string): Promise<void> {
    const response = await fetch(indexUrl);
    if (!response.ok) {
      throw new Error(`Failed to load search index: ${response.status} ${response.statusText}`);
    }
    const raw: unknown = await response.json();
    if (!Array.isArray(raw) || raw.length === 0) {
      throw new Error('Search index is empty or malformed');
    }
    this.indexData = raw as SearchIndexEntry[];
    this.fuse = new Fuse(this.indexData, fuseOptions);
  }

  /**
   * Initialize directly from data (useful for testing or SSR scenarios).
   */
  initializeWithData(data: SearchIndexEntry[]): void {
    this.indexData = data;
    this.fuse = new Fuse(this.indexData, fuseOptions);
  }

  /**
   * Search the index with optional filters and limit.
   * Returns empty array for queries with fewer than 2 characters.
   * Results are sorted by descending relevance (lowest Fuse score = best match = first).
   */
  search(query: string, filters?: SearchFilters, limit?: number): SearchResult[] {
    if (query.length < 2) {
      return [];
    }

    if (!this.fuse) {
      throw new Error('SearchService not initialized. Call initialize() first.');
    }

    const maxResults = limit ?? 50;

    // Perform Fuse.js search
    const fuseResults = this.fuse.search(query);

    // Apply filters
    let filtered = fuseResults;

    if (filters) {
      const hasContentTypeFilter = filters.contentTypes && filters.contentTypes.length > 0;
      const hasTopicFilter = filters.topics && filters.topics.length > 0;

      if (hasContentTypeFilter || hasTopicFilter) {
        filtered = fuseResults.filter((result) => {
          const matchesContentType = hasContentTypeFilter
            ? filters.contentTypes!.includes(
                result.item.contentType as 'study_material' | 'code_snippet' | 'scenario_question'
              )
            : true;

          const matchesTopic = hasTopicFilter
            ? filters.topics!.includes(result.item.topic)
            : true;

          // Intersection logic: when both filters are active, result must match BOTH
          return matchesContentType && matchesTopic;
        });
      }
    }

    // Limit results and sort by score (ascending Fuse score = descending relevance)
    const limitedResults = filtered.slice(0, maxResults);

    // Map Fuse results to SearchResult interface
    return limitedResults.map((fuseResult) => {
      const item = fuseResult.item;
      const score = fuseResult.score ?? 0;

      // Extract highlights from Fuse matches
      const highlights: { field: string; indices: [number, number][] }[] = [];
      if (fuseResult.matches) {
        for (const match of fuseResult.matches) {
          if (match.key && match.indices) {
            highlights.push({
              field: match.key,
              indices: match.indices.map(([start, end]) => [start, end] as [number, number]),
            });
          }
        }
      }

      // Generate excerpt from body (first 200 characters or highlighted portion)
      const excerpt = this.generateExcerpt(item.body, fuseResult.matches);

      return {
        id: item.id,
        title: item.title,
        topic: item.topicDisplayName || item.topic,
        subtopic: item.subtopicDisplayName || item.subtopic,
        contentType: item.contentType as 'study_material' | 'code_snippet' | 'scenario_question',
        excerpt,
        score,
        url: item.url,
        highlights,
      };
    });
  }

  /**
   * Generate a highlighted excerpt string from a search result.
   * Wraps matching text segments in <mark> tags based on Fuse.js match indices.
   */
  getHighlightedExcerpt(result: SearchResult): string {
    if (!result.highlights || result.highlights.length === 0) {
      return result.excerpt;
    }

    // Find highlights for the excerpt field (body matches)
    const bodyHighlight = result.highlights.find((h) => h.field === 'body');
    if (!bodyHighlight || bodyHighlight.indices.length === 0) {
      // Try title highlights as fallback
      const titleHighlight = result.highlights.find((h) => h.field === 'title');
      if (!titleHighlight || titleHighlight.indices.length === 0) {
        return result.excerpt;
      }
      return this.applyHighlightMarks(result.title, titleHighlight.indices);
    }

    return this.applyHighlightMarks(result.excerpt, bodyHighlight.indices);
  }

  /**
   * Apply <mark> tags to text at the given indices.
   */
  private applyHighlightMarks(text: string, indices: [number, number][]): string {
    if (!indices.length || !text) {
      return text;
    }

    // Sort indices by start position (descending) to avoid offset issues when inserting marks
    const sortedIndices = [...indices].sort((a, b) => b[0] - a[0]);

    let result = text;
    for (const [start, end] of sortedIndices) {
      // Clamp indices to text bounds
      const clampedStart = Math.max(0, Math.min(start, text.length));
      const clampedEnd = Math.max(0, Math.min(end + 1, text.length)); // Fuse indices are inclusive

      if (clampedStart < clampedEnd && clampedStart < result.length) {
        const before = result.slice(0, clampedStart);
        const match = result.slice(clampedStart, clampedEnd);
        const after = result.slice(clampedEnd);
        result = `${before}<mark>${match}</mark>${after}`;
      }
    }

    return result;
  }

  /**
   * Generate an excerpt from the body text, preferring the area around first match.
   */
  private generateExcerpt(
    body: string,
    matches: readonly Fuse.FuseResultMatch[] | undefined
  ): string {
    const maxLength = 200;

    if (!body) {
      return '';
    }

    // Find first body match to center the excerpt around it
    const bodyMatch = matches?.find((m) => m.key === 'body');
    if (bodyMatch && bodyMatch.indices.length > 0) {
      const firstMatchStart = bodyMatch.indices[0][0];
      // Start excerpt a bit before the match for context
      const excerptStart = Math.max(0, firstMatchStart - 40);
      const excerptEnd = Math.min(body.length, excerptStart + maxLength);
      let excerpt = body.slice(excerptStart, excerptEnd);

      if (excerptStart > 0) {
        excerpt = '...' + excerpt;
      }
      if (excerptEnd < body.length) {
        excerpt = excerpt + '...';
      }

      return excerpt;
    }

    // No body match; return first 200 characters
    if (body.length <= maxLength) {
      return body;
    }
    return body.slice(0, maxLength) + '...';
  }
}

// --- Singleton Export ---

export const searchService = new SearchService();
