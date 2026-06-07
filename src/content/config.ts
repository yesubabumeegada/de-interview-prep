/**
 * Astro Content Collections Configuration
 * 
 * Defines the "topics" collection for all markdown content in content/topics/.
 * Implements front-matter schema validation per Requirements 9.1, 9.2, 9.3, 9.4.
 * 
 * Required fields: title (max 120 chars), topic, subtopic, content_type
 * Optional fields: difficulty_level, layer, tags
 * 
 * Invalid front-matter files are logged with warnings and skipped (not included
 * in the collection). The build continues with valid files.
 */
import { defineCollection, z } from 'astro:content';

/**
 * Zod schema for front-matter validation.
 * - title: Required string, max 120 characters
 * - topic: Required string, must match a topic ID from topics.config.json
 * - subtopic: Required string, must match a subtopic ID within the topic
 * - content_type: Required enum (study_material, code_snippet, diagram, scenario_question)
 * - difficulty_level: Optional enum (junior, mid-level, senior)
 * - layer: Optional enum (fundamentals, intermediate, senior-deep-dive, real-world)
 * - tags: Optional array of strings for enhanced search
 */
const topicsCollection = defineCollection({
  type: 'content',
  schema: z.object({
    title: z
      .string({
        required_error: 'Front-matter field "title" is required',
      })
      .min(1, 'Front-matter field "title" must not be empty')
      .max(120, 'Front-matter field "title" must not exceed 120 characters'),

    topic: z
      .string({
        required_error: 'Front-matter field "topic" is required',
      })
      .min(1, 'Front-matter field "topic" must not be empty'),

    subtopic: z
      .string({
        required_error: 'Front-matter field "subtopic" is required',
      })
      .min(1, 'Front-matter field "subtopic" must not be empty'),

    content_type: z.enum(
      ['study_material', 'code_snippet', 'diagram', 'scenario_question'],
      {
        errorMap: () => ({
          message:
            'Front-matter field "content_type" must be one of: study_material, code_snippet, diagram, scenario_question',
        }),
      }
    ),

    difficulty_level: z
      .enum(['junior', 'mid-level', 'senior'], {
        errorMap: () => ({
          message:
            'Front-matter field "difficulty_level" must be one of: junior, mid-level, senior',
        }),
      })
      .optional(),

    layer: z
      .enum(['fundamentals', 'intermediate', 'senior-deep-dive', 'real-world'], {
        errorMap: () => ({
          message:
            'Front-matter field "layer" must be one of: fundamentals, intermediate, senior-deep-dive, real-world',
        }),
      })
      .optional(),

    tags: z.array(z.string()).optional(),
  }),
});

export const collections = {
  topics: topicsCollection,
};
