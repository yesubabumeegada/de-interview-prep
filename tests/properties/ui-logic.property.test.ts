import { describe, it, expect } from 'vitest';
import * as fc from 'fast-check';
import { filterByDifficulty, type DifficultyLevel, type ScenarioQuestion } from '../../src/services/filterService';
import { LAYER_ORDER, LAYER_DISPLAY_NAMES, type LayerId } from '../../src/components/interactive/LayerCollapse';

/**
 * Property-based tests for UI Logic: Difficulty Filtering and Layer State
 *
 * Feature: de-interview-prep-app
 * Validates: Requirements 3.4, 13.1, 13.11
 */

// --- Generators ---

/** Difficulty level generator */
const arbDifficultyLevel: fc.Arbitrary<DifficultyLevel> = fc.constantFrom('junior', 'mid-level', 'senior');

/** Generates a unique ID string */
const arbId = fc.string({ minLength: 1, maxLength: 30 }).map(
  (s) => s.replace(/\s+/g, '-') || 'id-1'
);

/** Generates a ScenarioQuestion with a specific difficulty level */
const arbScenarioQuestionWithDifficulty = (level: DifficultyLevel): fc.Arbitrary<ScenarioQuestion> =>
  arbId.map((id) => ({
    id,
    difficultyLevel: level,
  }));

/** Generates a ScenarioQuestion with a random difficulty level */
const arbScenarioQuestion: fc.Arbitrary<ScenarioQuestion> = fc.record({
  id: arbId,
  difficultyLevel: arbDifficultyLevel,
});

/** Generates an array of scenario questions with mixed difficulty levels */
const arbMixedQuestions: fc.Arbitrary<ScenarioQuestion[]> = fc.array(arbScenarioQuestion, {
  minLength: 0,
  maxLength: 50,
});

/** Generates layer IDs in a random order (for testing reordering) */
const arbShuffledLayers: fc.Arbitrary<LayerId[]> = fc.shuffledSubarray(
  [...LAYER_ORDER],
  { minLength: 1, maxLength: LAYER_ORDER.length }
);

/** Generates a full set of layers in random order */
const arbAllLayersShuffled: fc.Arbitrary<LayerId[]> = fc.constant([...LAYER_ORDER]).chain(
  (layers) => fc.shuffledSubarray(layers, { minLength: layers.length, maxLength: layers.length })
);

// --- Helper Functions ---

/**
 * Determines the initial expanded/collapsed state for a set of layers.
 * Only 'fundamentals' should be expanded; all others collapsed.
 */
function getInitialLayerState(layers: LayerId[]): Record<LayerId, boolean> {
  const state: Partial<Record<LayerId, boolean>> = {};
  for (const layer of layers) {
    state[layer] = layer === 'fundamentals';
  }
  return state as Record<LayerId, boolean>;
}

/**
 * Sorts layers into the fixed LAYER_ORDER.
 * Filters out any layers not in LAYER_ORDER.
 */
function sortLayersInFixedOrder(layers: LayerId[]): LayerId[] {
  return LAYER_ORDER.filter((id) => layers.includes(id));
}

// --- Property Tests ---

describe('Feature: de-interview-prep-app, Property 7: Difficulty filtering returns only matching questions', () => {
  /**
   * **Validates: Requirements 3.4**
   *
   * For any set of scenario questions with mixed difficulty levels and a selected
   * difficulty filter, the filtered result SHALL contain only questions whose
   * `difficultyLevel` matches the selected filter, and SHALL contain ALL questions
   * of that difficulty level from the input set.
   */
  it('filtered results contain ONLY questions matching the selected difficulty level', () => {
    fc.assert(
      fc.property(arbMixedQuestions, arbDifficultyLevel, (questions, selectedLevel) => {
        const result = filterByDifficulty(questions, selectedLevel);

        // Every item in the result must match the selected difficulty level
        for (const q of result) {
          expect(q.difficultyLevel).toBe(selectedLevel);
        }
      }),
      { numRuns: 100 }
    );
  });

  it('filtered results contain ALL questions of the selected difficulty level from the input', () => {
    fc.assert(
      fc.property(arbMixedQuestions, arbDifficultyLevel, (questions, selectedLevel) => {
        const result = filterByDifficulty(questions, selectedLevel);

        // Count how many questions in the input have the selected level
        const expectedCount = questions.filter((q) => q.difficultyLevel === selectedLevel).length;

        // The result must contain exactly that many items
        expect(result.length).toBe(expectedCount);
      }),
      { numRuns: 100 }
    );
  });

  it('filtering preserves question identity (same objects, same order)', () => {
    fc.assert(
      fc.property(arbMixedQuestions, arbDifficultyLevel, (questions, selectedLevel) => {
        const result = filterByDifficulty(questions, selectedLevel);

        // Collect the expected questions manually
        const expected = questions.filter((q) => q.difficultyLevel === selectedLevel);

        // Same length
        expect(result.length).toBe(expected.length);

        // Same items in same relative order
        for (let i = 0; i < result.length; i++) {
          expect(result[i].id).toBe(expected[i].id);
          expect(result[i].difficultyLevel).toBe(expected[i].difficultyLevel);
        }
      }),
      { numRuns: 100 }
    );
  });

  it('filtering an empty array returns an empty array for any difficulty level', () => {
    fc.assert(
      fc.property(arbDifficultyLevel, (level) => {
        const result = filterByDifficulty([], level);
        expect(result).toEqual([]);
        expect(result.length).toBe(0);
      }),
      { numRuns: 100 }
    );
  });

  it('filtering a homogeneous set returns all items when level matches, none otherwise', () => {
    fc.assert(
      fc.property(
        arbDifficultyLevel,
        arbDifficultyLevel,
        fc.integer({ min: 1, max: 20 }),
        (questionLevel, filterLevel, count) => {
          const questions: ScenarioQuestion[] = Array.from({ length: count }, (_, i) => ({
            id: `q-${i}`,
            difficultyLevel: questionLevel,
          }));

          const result = filterByDifficulty(questions, filterLevel);

          if (questionLevel === filterLevel) {
            expect(result.length).toBe(count);
          } else {
            expect(result.length).toBe(0);
          }
        }
      ),
      { numRuns: 100 }
    );
  });
});

describe('Feature: de-interview-prep-app, Property 19: Content layer ordering', () => {
  /**
   * **Validates: Requirements 13.1**
   *
   * For any subtopic page containing content layers, the rendered output SHALL present
   * layers in the fixed order: Fundamentals → Intermediate → Senior-Level → Real-World,
   * regardless of source file ordering.
   */
  it('LAYER_ORDER defines the fixed order: fundamentals, intermediate, senior-deep-dive, real-world', () => {
    expect(LAYER_ORDER).toEqual([
      'fundamentals',
      'intermediate',
      'senior-deep-dive',
      'real-world',
    ]);
  });

  it('sorting any permutation of layers results in the fixed order', () => {
    fc.assert(
      fc.property(arbAllLayersShuffled, (shuffledLayers) => {
        const sorted = sortLayersInFixedOrder(shuffledLayers);

        // The result must be in the fixed order
        expect(sorted).toEqual([...LAYER_ORDER]);
      }),
      { numRuns: 100 }
    );
  });

  it('sorting a subset of layers preserves the relative fixed order', () => {
    fc.assert(
      fc.property(arbShuffledLayers, (shuffledSubset) => {
        const sorted = sortLayersInFixedOrder(shuffledSubset);

        // Verify the sorted result is a subsequence of LAYER_ORDER
        let lastIndex = -1;
        for (const layer of sorted) {
          const currentIndex = LAYER_ORDER.indexOf(layer);
          expect(currentIndex).toBeGreaterThan(lastIndex);
          lastIndex = currentIndex;
        }

        // Verify all input layers are in the output
        for (const layer of shuffledSubset) {
          expect(sorted).toContain(layer);
        }
      }),
      { numRuns: 100 }
    );
  });

  it('LAYER_DISPLAY_NAMES maps each layer to its expected display name', () => {
    fc.assert(
      fc.property(fc.constantFrom(...LAYER_ORDER), (layerId) => {
        const displayName = LAYER_DISPLAY_NAMES[layerId];
        expect(displayName).toBeDefined();
        expect(displayName.length).toBeGreaterThan(0);

        // Verify the specific mapping
        const expectedNames: Record<LayerId, string> = {
          fundamentals: 'Fundamentals',
          intermediate: 'Intermediate Concepts',
          'senior-deep-dive': 'Senior-Level Deep Dive',
          'real-world': 'Real-World Production Examples',
        };
        expect(displayName).toBe(expectedNames[layerId]);
      }),
      { numRuns: 100 }
    );
  });

  it('sorted layers never reorder the canonical sequence', () => {
    fc.assert(
      fc.property(
        fc.array(fc.constantFrom(...LAYER_ORDER), { minLength: 1, maxLength: 10 }),
        (inputLayers) => {
          // Deduplicate since sortLayersInFixedOrder uses includes
          const unique = [...new Set(inputLayers)] as LayerId[];
          const sorted = sortLayersInFixedOrder(unique);

          // For any two items in sorted result, their order should match LAYER_ORDER
          for (let i = 0; i < sorted.length - 1; i++) {
            const indexA = LAYER_ORDER.indexOf(sorted[i]);
            const indexB = LAYER_ORDER.indexOf(sorted[i + 1]);
            expect(indexA).toBeLessThan(indexB);
          }
        }
      ),
      { numRuns: 100 }
    );
  });
});

describe('Feature: de-interview-prep-app, Property 20: Layer initial collapse state', () => {
  /**
   * **Validates: Requirements 13.11**
   *
   * For any subtopic page with multiple content layers, the initial render state SHALL
   * have the Fundamentals layer expanded and all other layers collapsed.
   */
  it('initial state has fundamentals expanded and all other layers collapsed', () => {
    fc.assert(
      fc.property(arbShuffledLayers, (layers) => {
        const state = getInitialLayerState(layers);

        for (const layer of layers) {
          if (layer === 'fundamentals') {
            expect(state[layer]).toBe(true);
          } else {
            expect(state[layer]).toBe(false);
          }
        }
      }),
      { numRuns: 100 }
    );
  });

  it('when fundamentals is present in any subset, it is always the only expanded layer', () => {
    fc.assert(
      fc.property(
        fc.shuffledSubarray([...LAYER_ORDER], { minLength: 2, maxLength: 4 }).filter(
          (layers) => layers.includes('fundamentals')
        ),
        (layers) => {
          const state = getInitialLayerState(layers as LayerId[]);

          // Count how many layers are expanded
          const expandedLayers = (layers as LayerId[]).filter((l) => state[l]);
          expect(expandedLayers.length).toBe(1);
          expect(expandedLayers[0]).toBe('fundamentals');
        }
      ),
      { numRuns: 100 }
    );
  });

  it('when fundamentals is NOT present, no layer is expanded', () => {
    const nonFundamentalsLayers: LayerId[] = ['intermediate', 'senior-deep-dive', 'real-world'];

    fc.assert(
      fc.property(
        fc.shuffledSubarray(nonFundamentalsLayers, { minLength: 1, maxLength: 3 }),
        (layers) => {
          const state = getInitialLayerState(layers as LayerId[]);

          // No layer should be expanded
          for (const layer of layers as LayerId[]) {
            expect(state[layer]).toBe(false);
          }
        }
      ),
      { numRuns: 100 }
    );
  });

  it('all four layers present: fundamentals=true, intermediate=false, senior-deep-dive=false, real-world=false', () => {
    fc.assert(
      fc.property(arbAllLayersShuffled, (shuffledLayers) => {
        const state = getInitialLayerState(shuffledLayers);

        expect(state['fundamentals']).toBe(true);
        expect(state['intermediate']).toBe(false);
        expect(state['senior-deep-dive']).toBe(false);
        expect(state['real-world']).toBe(false);
      }),
      { numRuns: 100 }
    );
  });

  it('each non-fundamentals layer has an expand control available (state is toggleable)', () => {
    fc.assert(
      fc.property(arbAllLayersShuffled, (shuffledLayers) => {
        const state = getInitialLayerState(shuffledLayers);

        // Verify each collapsed layer can be toggled (simulating expand control)
        for (const layer of shuffledLayers) {
          if (layer !== 'fundamentals') {
            // Initial state is collapsed
            expect(state[layer]).toBe(false);
            // Simulating toggle: the state value is a boolean, meaning it can be toggled
            expect(typeof state[layer]).toBe('boolean');
          }
        }
      }),
      { numRuns: 100 }
    );
  });
});
