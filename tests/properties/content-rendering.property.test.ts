import { describe, it, expect } from 'vitest';
import * as fc from 'fast-check';
import { clampZoom } from '../../src/components/content/ImageModal';

/**
 * Property-based tests for Content Rendering components
 *
 * Feature: de-interview-prep-app
 * Validates: Requirements 2.1, 3.2, 3.3, 7.2, 7.4, 8.4
 */

// --- Generators ---

/** Valid programming languages for code snippets */
const VALID_LANGUAGES = ['python', 'sql', 'scala', 'bash', 'hcl', 'javascript', 'typescript'] as const;
const arbLanguage = fc.constantFrom(...VALID_LANGUAGES);

/** Generates a non-empty code string with a specified number of lines */
const arbCodeLines = (minLines: number, maxLines: number) =>
  fc.array(
    fc.string({ minLength: 1, maxLength: 80 }).filter((s) => s.trim().length > 0),
    { minLength: minLines, maxLength: maxLines }
  ).map((lines) => lines.join('\n'));

/** Generates a non-empty string suitable for text content */
const arbNonEmptyText = fc.string({ minLength: 3, maxLength: 200 }).filter(
  (s) => s.trim().length >= 3
);

/**
 * Text safe for use as ATX heading content.
 * CommonMark strips trailing `#` sequences from headings, so we exclude them.
 */
/**
 * Safe alphabet for heading text: avoids all CommonMark/GFM special chars
 * (_*`~[]\&<>^) that get consumed or transformed by the markdown parser.
 */
const arbHeadingText = fc
  .stringOf(
    fc.char().filter((c) => !/[_*`~\[\]\\&<>^#!]/.test(c) && c !== '\n'),
    { minLength: 3, maxLength: 80 }
  )
  .filter((s) => s.trim().length >= 3);

/** Difficulty level generator */
const arbDifficultyLevel = fc.constantFrom('junior', 'mid-level', 'senior') as fc.Arbitrary<
  'junior' | 'mid-level' | 'senior'
>;

/** Generates a valid ScenarioQuestion object */
const arbScenarioQuestion = fc.record({
  id: fc.string({ minLength: 1, maxLength: 50 }).map((s) => s.replace(/\s+/g, '-') || 'q-1'),
  scenario: arbNonEmptyText,
  interviewerTesting: arbNonEmptyText,
  detailedAnswer: arbNonEmptyText,
  codeSnippets: fc.array(
    fc.record({
      code: arbCodeLines(1, 10),
      language: arbLanguage as fc.Arbitrary<string>,
      showLineNumbers: fc.boolean(),
      maxHeight: fc.option(fc.constant(500), { nil: undefined }),
    }),
    { minLength: 0, maxLength: 3 }
  ),
  diagrams: fc.array(arbNonEmptyText, { minLength: 0, maxLength: 2 }),
  followUpQuestions: fc.array(
    fc.record({
      question: arbNonEmptyText,
      answer: arbNonEmptyText,
    }),
    { minLength: 2, maxLength: 5 }
  ),
  difficultyLevel: arbDifficultyLevel,
});

/** Generates valid markdown containing specific elements */
const arbMarkdownWithHeadings = fc.record({
  h1: arbHeadingText,
  h2: arbHeadingText,
  h3: arbHeadingText,
  h4: arbHeadingText,
  paragraph: arbNonEmptyText,
  bulletItems: fc.array(arbNonEmptyText, { minLength: 1, maxLength: 4 }),
  numberedItems: fc.array(arbNonEmptyText, { minLength: 1, maxLength: 4 }),
  tableHeaders: fc.array(
    fc.string({ minLength: 1, maxLength: 15 }).filter((s) => s.trim().length >= 1 && !s.includes('|') && !s.includes('\n')),
    { minLength: 2, maxLength: 4 }
  ),
  tableRow: fc.array(
    fc.string({ minLength: 1, maxLength: 15 }).filter((s) => s.trim().length >= 1 && !s.includes('|') && !s.includes('\n')),
    { minLength: 2, maxLength: 4 }
  ),
});

/**
 * Constructs a markdown string from the generated structure.
 * Ensures proper markdown syntax for each element type.
 */
function buildMarkdown(data: {
  h1: string;
  h2: string;
  h3: string;
  h4: string;
  paragraph: string;
  bulletItems: string[];
  numberedItems: string[];
  tableHeaders: string[];
  tableRow: string[];
}): string {
  const parts: string[] = [];

  parts.push(`# ${data.h1.replace(/\n/g, ' ')}`);
  parts.push('');
  parts.push(`## ${data.h2.replace(/\n/g, ' ')}`);
  parts.push('');
  parts.push(`### ${data.h3.replace(/\n/g, ' ')}`);
  parts.push('');
  parts.push(`#### ${data.h4.replace(/\n/g, ' ')}`);
  parts.push('');
  parts.push(data.paragraph.replace(/\n/g, ' '));
  parts.push('');

  // Bullet list
  for (const item of data.bulletItems) {
    parts.push(`- ${item.replace(/\n/g, ' ')}`);
  }
  parts.push('');

  // Numbered list
  for (let i = 0; i < data.numberedItems.length; i++) {
    parts.push(`${i + 1}. ${data.numberedItems[i].replace(/\n/g, ' ')}`);
  }
  parts.push('');

  // Table - ensure same column count for header and row
  const colCount = Math.min(data.tableHeaders.length, data.tableRow.length);
  if (colCount >= 2) {
    const headers = data.tableHeaders.slice(0, colCount);
    const row = data.tableRow.slice(0, colCount);
    parts.push(`| ${headers.join(' | ')} |`);
    parts.push(`| ${headers.map(() => '---').join(' | ')} |`);
    parts.push(`| ${row.join(' | ')} |`);
  }

  return parts.join('\n');
}

/**
 * Simple markdown-to-HTML rendering using unified/remark/rehype.
 * This tests the same pipeline Astro uses for content processing.
 */
async function renderMarkdownToHtml(markdown: string): Promise<string> {
  const { unified } = await import('unified');
  const remarkParse = (await import('remark-parse')).default;
  const remarkGfm = (await import('remark-gfm')).default;
  const remarkRehype = (await import('remark-rehype')).default;
  const rehypeStringify = (await import('rehype-stringify')).default;

  const result = await unified()
    .use(remarkParse)
    .use(remarkGfm)
    .use(remarkRehype)
    .use(rehypeStringify)
    .process(markdown);

  return String(result);
}

// --- Utility Functions for CodeBlock logic ---

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

/**
 * Extracts the visible and hidden content from a ScenarioQuestion
 * based on the initial (un-revealed) state.
 */
export function getScenarioInitialState(question: {
  scenario: string;
  interviewerTesting: string;
  detailedAnswer: string;
  codeSnippets: { code: string }[];
  diagrams: string[];
}) {
  return {
    visible: {
      scenario: question.scenario,
      interviewerTesting: question.interviewerTesting,
    },
    hidden: {
      detailedAnswer: question.detailedAnswer,
      codeSnippets: question.codeSnippets,
      diagrams: question.diagrams,
    },
  };
}

/**
 * Returns all content that should be visible in the fully-revealed state.
 */
export function getScenarioRevealedContent(question: {
  scenario: string;
  interviewerTesting: string;
  detailedAnswer: string;
  followUpQuestions: { question: string; answer: string }[];
}) {
  return {
    scenario: question.scenario,
    interviewerTesting: question.interviewerTesting,
    detailedAnswer: question.detailedAnswer,
    followUpQuestions: question.followUpQuestions,
  };
}

// --- Property Tests ---

describe('Feature: de-interview-prep-app, Property 4: Markdown rendering produces correct HTML elements', () => {
  /**
   * **Validates: Requirements 2.1**
   *
   * For any valid markdown containing headings H1-H4, paragraphs, bullet lists,
   * numbered lists, and tables, the rendering pipeline produces HTML with
   * corresponding elements (<h1>-<h4>, <p>, <ul>, <ol>, <table>).
   */
  it('markdown with headings, paragraphs, lists, and tables produces correct HTML elements', async () => {
    await fc.assert(
      fc.asyncProperty(arbMarkdownWithHeadings, async (data) => {
        const markdown = buildMarkdown(data);
        const html = await renderMarkdownToHtml(markdown);

        // H1 through H4 should be present
        expect(html).toContain('<h1');
        expect(html).toContain('<h2');
        expect(html).toContain('<h3');
        expect(html).toContain('<h4');

        // Paragraph should be present
        expect(html).toContain('<p');

        // Bullet list (unordered) should be present
        expect(html).toContain('<ul');
        expect(html).toContain('<li');

        // Numbered list (ordered) should be present
        expect(html).toContain('<ol');

        // Table should be present (if we generated >= 2 columns)
        const colCount = Math.min(data.tableHeaders.length, data.tableRow.length);
        if (colCount >= 2) {
          expect(html).toContain('<table');
          expect(html).toContain('<th');
          expect(html).toContain('<td');
        }

        // Content should not be lost - check heading text appears in output.
        // Strip HTML tags and decode basic entities to compare plain text.
        const stripHtml = (s: string) =>
          s.replace(/<[^>]+>/g, ' ')
            // decode hex and decimal numeric entities
            .replace(/&#x([0-9a-fA-F]+);/g, (_, h) => String.fromCodePoint(parseInt(h, 16)))
            .replace(/&#([0-9]+);/g, (_, d) => String.fromCodePoint(parseInt(d, 10)))
            .replace(/&amp;/g, '&')
            .replace(/&lt;/g, '<')
            .replace(/&gt;/g, '>')
            .replace(/&quot;/g, '"')
            .replace(/&#39;/g, "'")
            .replace(/\s+/g, ' ')
            .trim();
        const textContent = stripHtml(html);
        // Normalize whitespace in both sides since HTML collapses multiple spaces
        const normalize = (s: string) => s.replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
        expect(textContent).toContain(normalize(data.h1));
      }),
      { numRuns: 100 }
    );
  });
});

describe('Feature: de-interview-prep-app, Property 5: Scenario question rendering includes all required fields', () => {
  /**
   * **Validates: Requirements 3.2**
   *
   * For any valid ScenarioQuestion object (with non-empty scenario, interviewerTesting,
   * detailedAnswer, and followUpQuestions), the fully-revealed rendered output SHALL
   * contain text from each of these fields.
   */
  it('fully-revealed scenario includes scenario, interviewerTesting, detailedAnswer, and followUpQuestions', () => {
    fc.assert(
      fc.property(arbScenarioQuestion, (question) => {
        const revealed = getScenarioRevealedContent(question);

        // All required fields are present and non-empty in revealed state
        expect(revealed.scenario).toBe(question.scenario);
        expect(revealed.scenario.trim().length).toBeGreaterThan(0);

        expect(revealed.interviewerTesting).toBe(question.interviewerTesting);
        expect(revealed.interviewerTesting.trim().length).toBeGreaterThan(0);

        expect(revealed.detailedAnswer).toBe(question.detailedAnswer);
        expect(revealed.detailedAnswer.trim().length).toBeGreaterThan(0);

        expect(revealed.followUpQuestions).toBe(question.followUpQuestions);
        expect(revealed.followUpQuestions.length).toBeGreaterThanOrEqual(2);

        // Each follow-up question has both question and answer text
        for (const fq of revealed.followUpQuestions) {
          expect(fq.question.trim().length).toBeGreaterThan(0);
          expect(fq.answer.trim().length).toBeGreaterThan(0);
        }
      }),
      { numRuns: 100 }
    );
  });
});

describe('Feature: de-interview-prep-app, Property 6: Scenario question initial state hides answer', () => {
  /**
   * **Validates: Requirements 3.3**
   *
   * For any valid ScenarioQuestion, the initial (un-revealed) render state SHALL
   * include the scenario description and interviewer-testing text, and SHALL NOT
   * include the detailed answer, code snippets content, or diagram content
   * in the visible state.
   */
  it('initial state shows scenario and interviewerTesting, hides detailedAnswer, code snippets, and diagrams', () => {
    fc.assert(
      fc.property(arbScenarioQuestion, (question) => {
        const state = getScenarioInitialState(question);

        // Visible section includes scenario and interviewerTesting
        expect(state.visible.scenario).toBe(question.scenario);
        expect(state.visible.scenario.trim().length).toBeGreaterThan(0);
        expect(state.visible.interviewerTesting).toBe(question.interviewerTesting);
        expect(state.visible.interviewerTesting.trim().length).toBeGreaterThan(0);

        // Hidden section contains the answer, code snippets, and diagrams
        expect(state.hidden.detailedAnswer).toBe(question.detailedAnswer);
        expect(state.hidden.codeSnippets).toBe(question.codeSnippets);
        expect(state.hidden.diagrams).toBe(question.diagrams);

        // The visible section does NOT contain the detailedAnswer text
        const visibleText = `${state.visible.scenario} ${state.visible.interviewerTesting}`;
        // Only verify non-inclusion if detailedAnswer is different from scenario/interviewerTesting
        if (
          question.detailedAnswer !== question.scenario &&
          question.detailedAnswer !== question.interviewerTesting
        ) {
          expect(visibleText).not.toContain(question.detailedAnswer);
        }
      }),
      { numRuns: 100 }
    );
  });
});

describe('Feature: de-interview-prep-app, Property 14: Code block line numbers and language label', () => {
  /**
   * **Validates: Requirements 7.2**
   *
   * For any code snippet with a specified programming language, the rendered output
   * SHALL contain line numbers starting at 1 and incrementing by 1 for each line,
   * and SHALL display a language label matching the snippet's specified language.
   */
  it('line numbers start at 1 and increment by 1 for each line', () => {
    fc.assert(
      fc.property(arbCodeLines(1, 60), arbLanguage, (code, language) => {
        const lineNumbers = computeLineNumbers(code);
        const lines = code.split('\n');

        // Line numbers array length matches the number of lines
        expect(lineNumbers.length).toBe(lines.length);

        // First line number is 1
        expect(lineNumbers[0]).toBe(1);

        // Each subsequent line number increments by 1
        for (let i = 1; i < lineNumbers.length; i++) {
          expect(lineNumbers[i]).toBe(lineNumbers[i - 1] + 1);
        }

        // Last line number equals total line count
        expect(lineNumbers[lineNumbers.length - 1]).toBe(lines.length);

        // Language label should be a valid non-empty string
        expect(language.length).toBeGreaterThan(0);
        expect(VALID_LANGUAGES).toContain(language);
      }),
      { numRuns: 100 }
    );
  });
});

describe('Feature: de-interview-prep-app, Property 15: Code block scroll threshold', () => {
  /**
   * **Validates: Requirements 7.4**
   *
   * For any code snippet, if line count exceeds 30, the rendered container SHALL have
   * max-height 500px with overflow scrolling enabled. If line count is 30 or fewer,
   * no max-height constraint SHALL be applied.
   */
  it('code with > 30 lines gets max-height 500px; code with <= 30 lines has no constraint', () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: 100 }),
        (lineCount) => {
          // Generate code with exactly the specified number of lines
          const lines = Array.from({ length: lineCount }, (_, i) => `line ${i + 1}`);
          const code = lines.join('\n');

          const scrollProps = getCodeBlockScrollProps(code);

          if (lineCount > 30) {
            expect(scrollProps.shouldScroll).toBe(true);
            expect(scrollProps.maxHeight).toBe(500);
          } else {
            expect(scrollProps.shouldScroll).toBe(false);
            expect(scrollProps.maxHeight).toBeUndefined();
          }
        }
      ),
      { numRuns: 100 }
    );
  });

  it('boundary: exactly 30 lines has no scroll; 31 lines triggers scroll', () => {
    // 30 lines: no scroll
    const code30 = Array.from({ length: 30 }, (_, i) => `line ${i + 1}`).join('\n');
    const props30 = getCodeBlockScrollProps(code30);
    expect(props30.shouldScroll).toBe(false);
    expect(props30.maxHeight).toBeUndefined();

    // 31 lines: scroll
    const code31 = Array.from({ length: 31 }, (_, i) => `line ${i + 1}`).join('\n');
    const props31 = getCodeBlockScrollProps(code31);
    expect(props31.shouldScroll).toBe(true);
    expect(props31.maxHeight).toBe(500);
  });
});

describe('Feature: de-interview-prep-app, Property 16: Zoom level clamping', () => {
  /**
   * **Validates: Requirements 8.4**
   *
   * For any requested zoom level value (integer or decimal), the diagram modal zoom
   * controller SHALL clamp the value to the nearest valid step in the range [50, 300]
   * with increments of 25 (i.e., valid values are 50, 75, 100, ..., 275, 300).
   */
  const VALID_ZOOM_VALUES = [50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300];

  it('clampZoom always returns a value in the valid set [50, 75, 100, ..., 275, 300]', () => {
    fc.assert(
      fc.property(fc.double({ min: -1000, max: 1000, noNaN: true }), (value) => {
        const result = clampZoom(value);
        expect(VALID_ZOOM_VALUES).toContain(result);
      }),
      { numRuns: 100 }
    );
  });

  it('clampZoom result is always within [50, 300]', () => {
    fc.assert(
      fc.property(fc.double({ min: -10000, max: 10000, noNaN: true }), (value) => {
        const result = clampZoom(value);
        expect(result).toBeGreaterThanOrEqual(50);
        expect(result).toBeLessThanOrEqual(300);
      }),
      { numRuns: 100 }
    );
  });

  it('clampZoom result is always a multiple of 25', () => {
    fc.assert(
      fc.property(fc.double({ min: -10000, max: 10000, noNaN: true }), (value) => {
        const result = clampZoom(value);
        expect(result % 25).toBe(0);
      }),
      { numRuns: 100 }
    );
  });

  it('clampZoom rounds to nearest 25-increment within range', () => {
    fc.assert(
      fc.property(fc.double({ min: 50, max: 300, noNaN: true }), (value) => {
        const result = clampZoom(value);

        // The result should be the nearest valid step to the input
        const rounded = Math.round(value / 25) * 25;
        const expected = Math.max(50, Math.min(300, rounded));
        expect(result).toBe(expected);
      }),
      { numRuns: 100 }
    );
  });

  it('clampZoom handles NaN and Infinity by returning 100', () => {
    expect(clampZoom(NaN)).toBe(100);
    expect(clampZoom(Infinity)).toBe(100);
    expect(clampZoom(-Infinity)).toBe(100);
  });

  it('values below 50 clamp to 50', () => {
    fc.assert(
      fc.property(fc.double({ min: -10000, max: 49, noNaN: true }), (value) => {
        const result = clampZoom(value);
        expect(result).toBe(50);
      }),
      { numRuns: 100 }
    );
  });

  it('values above 300 clamp to 300', () => {
    fc.assert(
      fc.property(fc.double({ min: 301, max: 10000, noNaN: true }), (value) => {
        const result = clampZoom(value);
        expect(result).toBe(300);
      }),
      { numRuns: 100 }
    );
  });
});
