/**
 * Filter Service - Pure utility functions for filtering content.
 *
 * Provides difficulty-level filtering for scenario questions.
 * Requirements: 3.4
 */

export type DifficultyLevel = 'junior' | 'mid-level' | 'senior';

export interface ScenarioQuestion {
  id: string;
  difficultyLevel: DifficultyLevel;
  [key: string]: unknown;
}

/**
 * Filters scenario questions by difficulty level.
 *
 * Returns only questions whose difficultyLevel matches the selected filter,
 * and includes ALL questions of that difficulty level from the input set.
 *
 * @param questions - Array of scenario questions with mixed difficulty levels
 * @param level - The difficulty level to filter by
 * @returns Filtered array containing only matching questions
 */
export function filterByDifficulty<T extends ScenarioQuestion>(
  questions: T[],
  level: DifficultyLevel
): T[] {
  return questions.filter((q) => q.difficultyLevel === level);
}
