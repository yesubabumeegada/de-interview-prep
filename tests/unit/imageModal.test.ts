import { describe, it, expect } from 'vitest';
import { clampZoom } from '../../src/components/content/ImageModal';

/**
 * Unit tests for ImageModal zoom clamping logic.
 * Validates Property 16: Zoom level clamping to [50, 300] in 25% increments.
 * Requirements: 8.4
 */
describe('ImageModal - clampZoom', () => {
  it('should return 100 for input 100 (valid step)', () => {
    expect(clampZoom(100)).toBe(100);
  });

  it('should return 50 for the minimum valid value', () => {
    expect(clampZoom(50)).toBe(50);
  });

  it('should return 300 for the maximum valid value', () => {
    expect(clampZoom(300)).toBe(300);
  });

  it('should clamp values below 50 to 50', () => {
    expect(clampZoom(0)).toBe(50);
    expect(clampZoom(-100)).toBe(50);
    expect(clampZoom(25)).toBe(50);
    expect(clampZoom(30)).toBe(50);
  });

  it('should clamp values above 300 to 300', () => {
    expect(clampZoom(301)).toBe(300);
    expect(clampZoom(500)).toBe(300);
    expect(clampZoom(1000)).toBe(300);
    expect(clampZoom(325)).toBe(300);
  });

  it('should round to nearest 25% increment', () => {
    expect(clampZoom(110)).toBe(100); // closer to 100 than 125
    expect(clampZoom(113)).toBe(125); // closer to 125 than 100
    expect(clampZoom(112)).toBe(100); // exactly at midpoint rounds to 100
    expect(clampZoom(113)).toBe(125);
    expect(clampZoom(87)).toBe(75);
    expect(clampZoom(88)).toBe(100);
    expect(clampZoom(137)).toBe(125);
    expect(clampZoom(138)).toBe(150);
  });

  it('should handle all valid steps correctly (50 to 300 in 25 increments)', () => {
    const validSteps = [50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300];
    for (const step of validSteps) {
      expect(clampZoom(step)).toBe(step);
    }
  });

  it('should handle decimal inputs by rounding', () => {
    expect(clampZoom(99.5)).toBe(100);
    expect(clampZoom(100.5)).toBe(100);
    expect(clampZoom(112.4)).toBe(100);
    expect(clampZoom(112.6)).toBe(125);
  });

  it('should handle NaN by defaulting to 100%', () => {
    expect(clampZoom(NaN)).toBe(100);
  });

  it('should handle Infinity by defaulting to 100%', () => {
    expect(clampZoom(Infinity)).toBe(100);
    expect(clampZoom(-Infinity)).toBe(100);
  });
});
