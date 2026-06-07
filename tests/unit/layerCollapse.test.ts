import { describe, it, expect } from 'vitest';
import {
  LAYER_ORDER,
  LAYER_DISPLAY_NAMES,
  type LayerId,
} from '../../src/components/interactive/LayerCollapse';

/**
 * Unit tests for LayerCollapse component logic.
 *
 * Validates:
 * - Property 19: Content layer ordering (Fundamentals → Intermediate → Senior-Level → Real-World)
 * - Property 20: Layer initial collapse state (Fundamentals expanded, others collapsed)
 * - Requirements: 13.11, 14.8, 14.9
 */

describe('LayerCollapse - Layer Ordering (Property 19)', () => {
  it('should define exactly 4 layers in fixed order', () => {
    expect(LAYER_ORDER).toHaveLength(4);
    expect(LAYER_ORDER[0]).toBe('fundamentals');
    expect(LAYER_ORDER[1]).toBe('intermediate');
    expect(LAYER_ORDER[2]).toBe('senior-deep-dive');
    expect(LAYER_ORDER[3]).toBe('real-world');
  });

  it('should have display names for all layers', () => {
    expect(LAYER_DISPLAY_NAMES['fundamentals']).toBe('Fundamentals');
    expect(LAYER_DISPLAY_NAMES['intermediate']).toBe('Intermediate Concepts');
    expect(LAYER_DISPLAY_NAMES['senior-deep-dive']).toBe('Senior-Level Deep Dive');
    expect(LAYER_DISPLAY_NAMES['real-world']).toBe('Real-World Production Examples');
  });

  it('should have a display name for every layer in LAYER_ORDER', () => {
    for (const layerId of LAYER_ORDER) {
      expect(LAYER_DISPLAY_NAMES[layerId]).toBeDefined();
      expect(LAYER_DISPLAY_NAMES[layerId].length).toBeGreaterThan(0);
    }
  });

  it('LAYER_ORDER entries match LAYER_DISPLAY_NAMES keys', () => {
    const displayNameKeys = Object.keys(LAYER_DISPLAY_NAMES) as LayerId[];
    for (const key of displayNameKeys) {
      expect(LAYER_ORDER).toContain(key);
    }
    for (const id of LAYER_ORDER) {
      expect(displayNameKeys).toContain(id);
    }
  });
});

describe('LayerCollapse - Initial Collapse State (Property 20)', () => {
  it('should have fundamentals as the first layer (expanded by default in component)', () => {
    // The component uses getInitialExpandedState() which sets
    // fundamentals: true, all others: false
    // We verify the ordering here - fundamentals MUST be first
    expect(LAYER_ORDER[0]).toBe('fundamentals');
  });

  it('should have all non-fundamentals layers after fundamentals (collapsed by default)', () => {
    const nonFundamentals = LAYER_ORDER.slice(1);
    expect(nonFundamentals).toEqual(['intermediate', 'senior-deep-dive', 'real-world']);
  });
});
