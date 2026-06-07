import React, { useState, useCallback } from 'react';
import DOMPurify from 'isomorphic-dompurify';

/**
 * Layer configuration: defines the fixed ordering and initial state.
 * Property 19: Content layer ordering (Fundamentals → Intermediate → Senior-Level → Real-World)
 * Property 20: Layer initial collapse state (Fundamentals expanded, others collapsed)
 */
export const LAYER_ORDER = [
  'fundamentals',
  'intermediate',
  'senior-deep-dive',
  'real-world',
] as const;

export type LayerId = (typeof LAYER_ORDER)[number];

export const LAYER_DISPLAY_NAMES: Record<LayerId, string> = {
  fundamentals: 'Fundamentals',
  intermediate: 'Intermediate Concepts',
  'senior-deep-dive': 'Senior-Level Deep Dive',
  'real-world': 'Real-World Production Examples',
};

const LAYER_COLORS: Record<LayerId, string> = {
  fundamentals: '#10b981',        // Green
  intermediate: '#f59e0b',        // Amber
  'senior-deep-dive': '#ef4444',  // Red
  'real-world': '#6366f1',        // Indigo
};

/** Initial expand state: only Fundamentals is expanded */
function getInitialExpandedState(): Record<LayerId, boolean> {
  return {
    fundamentals: true,
    intermediate: false,
    'senior-deep-dive': false,
    'real-world': false,
  };
}

export interface LayerContent {
  id: LayerId;
  /** Content can be a React node or an HTML string to render via dangerouslySetInnerHTML */
  content: React.ReactNode | string;
}

export interface LayerCollapseProps {
  /**
   * Array of layer content objects. They will be rendered in the fixed
   * LAYER_ORDER regardless of the order they appear in this array.
   */
  layers: LayerContent[];
}

/**
 * LayerCollapse - Collapsible content layers for subtopic pages (React island, client:load).
 *
 * Features:
 * - Fixed layer ordering: Fundamentals → Intermediate → Senior-Level → Real-World
 * - Fundamentals expanded by default; all others collapsed
 * - Distinct section headers with layer name and expand/collapse chevron
 * - Smooth animation for expand/collapse
 * - 44x44px minimum touch targets on headers
 * - Accessible: uses aria-expanded, aria-controls, proper heading semantics
 *
 * Requirements: 13.11, 14.8, 14.9
 * Validates: Property 19 (Content layer ordering), Property 20 (Layer initial collapse state)
 */
export default function LayerCollapse({ layers }: LayerCollapseProps) {
  const [expanded, setExpanded] = useState<Record<LayerId, boolean>>(
    getInitialExpandedState
  );

  const toggleLayer = useCallback((layerId: LayerId) => {
    setExpanded((prev) => ({
      ...prev,
      [layerId]: !prev[layerId],
    }));
  }, []);

  // Sort layers into fixed order, filtering out any that aren't provided
  const sortedLayers = LAYER_ORDER.map((id) =>
    layers.find((l) => l.id === id)
  ).filter((l): l is LayerContent => l !== undefined);

  return (
    <div className="space-y-3" data-testid="layer-collapse">
      {sortedLayers.map((layer) => {
        const isExpanded = expanded[layer.id];
        const panelId = `layer-panel-${layer.id}`;
        const headerId = `layer-header-${layer.id}`;

        return (
          <section
            key={layer.id}
            className="rounded-lg border border-[var(--color-surface-tertiary)] bg-[var(--color-surface)] shadow-sm overflow-hidden"
            data-testid={`layer-section-${layer.id}`}
            data-layer-id={layer.id}
          >
            {/* Layer Header */}
            <button
              id={headerId}
              type="button"
              onClick={() => toggleLayer(layer.id)}
              className="flex items-center justify-between w-full min-h-[44px] px-4 py-3 text-left bg-[var(--color-surface-secondary)] hover:bg-[var(--color-surface-tertiary)] focus:outline-none focus:ring-2 focus:ring-inset focus:ring-[var(--color-primary)] transition-colors duration-150"
              aria-expanded={isExpanded}
              aria-controls={panelId}
              data-testid={`layer-toggle-${layer.id}`}
            >
              <div className="flex items-center gap-2">
                <span 
                  className="w-2.5 h-2.5 rounded-full shrink-0"
                  style={{ backgroundColor: LAYER_COLORS[layer.id] }}
                  aria-hidden="true"
                />
                <h3 className="text-base font-semibold text-[var(--color-content)] m-0">
                  {LAYER_DISPLAY_NAMES[layer.id]}
                </h3>
              </div>
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="20"
                height="20"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                className={`flex-shrink-0 text-[var(--color-content-secondary)] transition-transform duration-200 ease-in-out ${
                  isExpanded ? 'rotate-180' : 'rotate-0'
                }`}
                aria-hidden="true"
              >
                <polyline points="6 9 12 15 18 9" />
              </svg>
            </button>

            {/* Layer Content Panel */}
            <div
              id={panelId}
              role="region"
              aria-labelledby={headerId}
              className={`overflow-hidden transition-[grid-template-rows] duration-300 ease-in-out grid ${
                isExpanded
                  ? 'grid-rows-[1fr] opacity-100'
                  : 'grid-rows-[0fr] opacity-0'
              }`}
              style={{ transitionProperty: 'grid-template-rows, opacity' }}
              data-testid={`layer-content-${layer.id}`}
            >
              <div className="min-h-0">
                <div className="px-4 py-4">
                  {typeof layer.content === 'string' ? (
                    <div
                      className="content-renderer"
                      dangerouslySetInnerHTML={{ __html: DOMPurify.sanitize(layer.content) }}
                    />
                  ) : (
                    layer.content
                  )}
                </div>
              </div>
            </div>
          </section>
        );
      })}
    </div>
  );
}
