/**
 * CodeBlock component type definitions.
 *
 * Requirements: 2.2, 2.3, 2.6, 7.2, 7.4, 7.5
 */

export interface CodeBlockProps {
  /** The raw code string to display */
  code: string;
  /** Programming language for syntax highlighting (e.g., 'python', 'sql', 'bash') */
  language: string;
  /** Whether to display line numbers (default: true) */
  showLineNumbers: boolean;
  /** Maximum height in pixels for the code container (default: 500px for snippets > 30 lines) */
  maxHeight?: number;
}

/**
 * Computes line numbers for a code snippet.
 * Returns an array of line numbers starting at 1.
 */
export function computeLineNumbers(code: string): number[] {
  const lines = code.split('\n');
  return lines.map((_, index) => index + 1);
}

/**
 * Determines if a code block should have scroll constraint.
 * If line count > 30, maxHeight = 500px with overflow scrolling.
 */
export function getCodeBlockScrollProps(code: string): {
  shouldScroll: boolean;
  maxHeight: number | undefined;
} {
  const lineCount = code.split('\n').length;
  if (lineCount > 30) {
    return { shouldScroll: true, maxHeight: 500 };
  }
  return { shouldScroll: false, maxHeight: undefined };
}
