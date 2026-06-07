/**
 * NavigationService
 *
 * Pure utility functions for navigation logic:
 * - Sorting subtopics by order field
 * - Aggregating content counts per topic for the dashboard
 * - Building breadcrumb hierarchy
 *
 * Requirements: 1.2, 1.3, 1.4
 */

// --- Interfaces ---

export interface SubtopicConfig {
  id: string;
  displayName: string;
  order: number;
}

export interface TopicConfig {
  id: string;
  displayName: string;
  order: number;
  subtopics: SubtopicConfig[];
}

export interface BreadcrumbItem {
  label: string;
  url: string;
  level: 'topic' | 'subtopic' | 'content';
}

export interface ContentItem {
  topicId: string;
  contentType: 'study_material' | 'code_snippet' | 'diagram' | 'scenario_question';
}

export interface TopicStats {
  topicId: string;
  displayName: string;
  studyMaterialCount: number;
  codeSnippetCount: number;
  diagramCount: number;
  scenarioQuestionCount: number;
}

// --- Functions ---

/**
 * Sorts subtopics by their `order` field in ascending order.
 * Returns a new array (does not mutate input).
 *
 * Requirement 1.2: Navigation_System SHALL display subtopics ordered
 * according to the sequence defined in the content configuration file.
 */
export function sortSubtopics(subtopics: SubtopicConfig[]): SubtopicConfig[] {
  return [...subtopics].sort((a, b) => a.order - b.order);
}

/**
 * Aggregates content items into per-topic statistics.
 * For each topic in the provided config, counts items by contentType.
 * Topics with no matching items get zero counts.
 *
 * Requirement 1.3: Dashboard SHALL display counts of Study_Materials,
 * Code_Snippets, Diagrams, and Scenario_Questions per Topic,
 * displaying zero for any content type with no entries.
 */
export function aggregateTopicStats(
  topics: TopicConfig[],
  contentItems: ContentItem[]
): TopicStats[] {
  return topics.map((topic) => {
    const topicItems = contentItems.filter((item) => item.topicId === topic.id);

    return {
      topicId: topic.id,
      displayName: topic.displayName,
      studyMaterialCount: topicItems.filter((i) => i.contentType === 'study_material').length,
      codeSnippetCount: topicItems.filter((i) => i.contentType === 'code_snippet').length,
      diagramCount: topicItems.filter((i) => i.contentType === 'diagram').length,
      scenarioQuestionCount: topicItems.filter((i) => i.contentType === 'scenario_question').length,
    };
  });
}

/**
 * Builds a breadcrumb trail for a navigation path.
 * Produces at most 3 items in strict order: topic → subtopic → content.
 * Each item's label matches its display name.
 *
 * Requirement 1.4: Breadcrumb navigation showing current location as
 * a hierarchy of up to 3 levels: Topic, Subtopic, and Content item title.
 */
export function buildBreadcrumbs(params: {
  topicId?: string;
  topicDisplayName?: string;
  subtopicId?: string;
  subtopicDisplayName?: string;
  contentTitle?: string;
}): BreadcrumbItem[] {
  const items: BreadcrumbItem[] = [];

  if (params.topicId && params.topicDisplayName) {
    items.push({
      label: params.topicDisplayName,
      url: `/topic/${params.topicId}/`,
      level: 'topic',
    });
  }

  if (params.subtopicId && params.subtopicDisplayName && params.topicId) {
    items.push({
      label: params.subtopicDisplayName,
      url: `/topic/${params.topicId}/${params.subtopicId}`,
      level: 'subtopic',
    });
  }

  if (params.contentTitle && params.topicId) {
    items.push({
      label: params.contentTitle,
      url: '#',
      level: 'content',
    });
  }

  return items;
}
