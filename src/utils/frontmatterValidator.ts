/**
 * Front-matter validation utility for content files.
 * 
 * Validates markdown front-matter against the schema defined in the design:
 * - Required: title (max 120 chars), topic, subtopic, content_type
 * - Optional: difficulty_level, layer, tags
 * 
 * Used both at build time (to log warnings and skip invalid files)
 * and in property tests (to verify validation behavior).
 * 
 * Requirements: 9.3, 9.4
 */

/** Valid content type values */
export const VALID_CONTENT_TYPES = [
  'study_material',
  'code_snippet',
  'diagram',
  'scenario_question',
] as const;

/** Valid difficulty level values */
export const VALID_DIFFICULTY_LEVELS = [
  'junior',
  'mid-level',
  'senior',
] as const;

/** Valid layer values */
export const VALID_LAYERS = [
  'fundamentals',
  'intermediate',
  'senior-deep-dive',
  'real-world',
] as const;

export type ContentType = (typeof VALID_CONTENT_TYPES)[number];
export type DifficultyLevel = (typeof VALID_DIFFICULTY_LEVELS)[number];
export type Layer = (typeof VALID_LAYERS)[number];

/** Represents a valid, fully-parsed front-matter object */
export interface ValidFrontmatter {
  title: string;
  topic: string;
  subtopic: string;
  content_type: ContentType;
  difficulty_level?: DifficultyLevel;
  layer?: Layer;
  tags?: string[];
}

/** Represents a validation error for a specific field */
export interface ValidationError {
  field: string;
  message: string;
}

/** Result of front-matter validation */
export type ValidationResult =
  | { valid: true; data: ValidFrontmatter }
  | { valid: false; errors: ValidationError[] };

/**
 * Validates a front-matter object against the schema.
 * 
 * @param frontmatter - The parsed front-matter object to validate
 * @returns A ValidationResult indicating success with parsed data or failure with errors
 */
export function validateFrontmatter(frontmatter: unknown): ValidationResult {
  const errors: ValidationError[] = [];

  if (!frontmatter || typeof frontmatter !== 'object') {
    return {
      valid: false,
      errors: [{ field: 'frontmatter', message: 'Front-matter must be a non-null object' }],
    };
  }

  const fm = frontmatter as Record<string, unknown>;

  // Validate title (required, string, max 120 chars)
  if (fm.title === undefined || fm.title === null) {
    errors.push({ field: 'title', message: 'Required field "title" is missing' });
  } else if (typeof fm.title !== 'string') {
    errors.push({ field: 'title', message: 'Field "title" must be a string' });
  } else if (fm.title.length === 0) {
    errors.push({ field: 'title', message: 'Field "title" must not be empty' });
  } else if (fm.title.length > 120) {
    errors.push({
      field: 'title',
      message: `Field "title" exceeds maximum length of 120 characters (got ${fm.title.length})`,
    });
  }

  // Validate topic (required, string)
  if (fm.topic === undefined || fm.topic === null) {
    errors.push({ field: 'topic', message: 'Required field "topic" is missing' });
  } else if (typeof fm.topic !== 'string') {
    errors.push({ field: 'topic', message: 'Field "topic" must be a string' });
  } else if (fm.topic.length === 0) {
    errors.push({ field: 'topic', message: 'Field "topic" must not be empty' });
  }

  // Validate subtopic (required, string)
  if (fm.subtopic === undefined || fm.subtopic === null) {
    errors.push({ field: 'subtopic', message: 'Required field "subtopic" is missing' });
  } else if (typeof fm.subtopic !== 'string') {
    errors.push({ field: 'subtopic', message: 'Field "subtopic" must be a string' });
  } else if (fm.subtopic.length === 0) {
    errors.push({ field: 'subtopic', message: 'Field "subtopic" must not be empty' });
  }

  // Validate content_type (required, enum)
  if (fm.content_type === undefined || fm.content_type === null) {
    errors.push({ field: 'content_type', message: 'Required field "content_type" is missing' });
  } else if (typeof fm.content_type !== 'string') {
    errors.push({ field: 'content_type', message: 'Field "content_type" must be a string' });
  } else if (!(VALID_CONTENT_TYPES as readonly string[]).includes(fm.content_type)) {
    errors.push({
      field: 'content_type',
      message: `Field "content_type" has unrecognized value "${fm.content_type}". Must be one of: ${VALID_CONTENT_TYPES.join(', ')}`,
    });
  }

  // Validate difficulty_level (optional, enum)
  if (fm.difficulty_level !== undefined && fm.difficulty_level !== null) {
    if (typeof fm.difficulty_level !== 'string') {
      errors.push({ field: 'difficulty_level', message: 'Field "difficulty_level" must be a string' });
    } else if (!(VALID_DIFFICULTY_LEVELS as readonly string[]).includes(fm.difficulty_level)) {
      errors.push({
        field: 'difficulty_level',
        message: `Field "difficulty_level" has unrecognized value "${fm.difficulty_level}". Must be one of: ${VALID_DIFFICULTY_LEVELS.join(', ')}`,
      });
    }
  }

  // Validate layer (optional, enum)
  if (fm.layer !== undefined && fm.layer !== null) {
    if (typeof fm.layer !== 'string') {
      errors.push({ field: 'layer', message: 'Field "layer" must be a string' });
    } else if (!(VALID_LAYERS as readonly string[]).includes(fm.layer)) {
      errors.push({
        field: 'layer',
        message: `Field "layer" has unrecognized value "${fm.layer}". Must be one of: ${VALID_LAYERS.join(', ')}`,
      });
    }
  }

  // Validate tags (optional, array of strings)
  if (fm.tags !== undefined && fm.tags !== null) {
    if (!Array.isArray(fm.tags)) {
      errors.push({ field: 'tags', message: 'Field "tags" must be an array' });
    } else {
      const nonStringTags = fm.tags.filter((tag: unknown) => typeof tag !== 'string');
      if (nonStringTags.length > 0) {
        errors.push({ field: 'tags', message: 'All items in "tags" array must be strings' });
      }
    }
  }

  if (errors.length > 0) {
    return { valid: false, errors };
  }

  return {
    valid: true,
    data: {
      title: fm.title as string,
      topic: fm.topic as string,
      subtopic: fm.subtopic as string,
      content_type: fm.content_type as ContentType,
      difficulty_level: fm.difficulty_level as DifficultyLevel | undefined,
      layer: fm.layer as Layer | undefined,
      tags: fm.tags as string[] | undefined,
    },
  };
}
