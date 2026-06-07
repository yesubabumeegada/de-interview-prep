import React, { useState, useEffect, useCallback, useRef } from 'react';

/**
 * ImageModal - Full-screen modal for viewing images and diagrams with zoom controls.
 * This is a React island (client:load) for the Astro-based DE Interview Prep App.
 *
 * Features:
 * - Zoom range: 50%–300% in 25% increments (Property 16)
 * - Pinch gesture support for touch devices
 * - Zoom-in/zoom-out buttons
 * - Close via button, Escape key, or click outside modal
 *
 * Requirements: 2.5, 8.3, 8.4, 8.5
 */

export interface ImageModalProps {
  /** The image source URL */
  src: string;
  /** Alt text for accessibility */
  alt: string;
  /** Whether the modal is open */
  isOpen: boolean;
  /** Callback when the modal is requested to close */
  onClose: () => void;
  /** Initial zoom level (default: 100) */
  initialZoom?: number;
}

/** Valid zoom levels from 50% to 300% in 25% increments */
const MIN_ZOOM = 50;
const MAX_ZOOM = 300;
const ZOOM_INCREMENT = 25;

/**
 * Clamps a zoom value to the nearest valid step in [50, 300] with 25% increments.
 * Valid values: 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300
 */
export function clampZoom(value: number): number {
  // Handle NaN/invalid inputs by defaulting to 100%
  if (!Number.isFinite(value)) {
    return 100;
  }
  // Round to nearest increment
  const rounded = Math.round(value / ZOOM_INCREMENT) * ZOOM_INCREMENT;
  // Clamp to [MIN_ZOOM, MAX_ZOOM]
  return Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, rounded));
}

export default function ImageModal({
  src,
  alt,
  isOpen,
  onClose,
  initialZoom = 100,
}: ImageModalProps) {
  const [zoom, setZoom] = useState<number>(clampZoom(initialZoom));
  const modalRef = useRef<HTMLDivElement>(null);
  const imageContainerRef = useRef<HTMLDivElement>(null);
  const lastPinchDistanceRef = useRef<number | null>(null);

  // Reset zoom when modal opens
  useEffect(() => {
    if (isOpen) {
      setZoom(clampZoom(initialZoom));
    }
  }, [isOpen, initialZoom]);

  // Handle Escape key to close modal
  useEffect(() => {
    if (!isOpen) return;

    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault();
        onClose();
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [isOpen, onClose]);

  // Prevent body scroll when modal is open
  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = 'hidden';
    } else {
      document.body.style.overflow = '';
    }
    return () => {
      document.body.style.overflow = '';
    };
  }, [isOpen]);

  // Zoom in by one increment
  const handleZoomIn = useCallback(() => {
    setZoom((prev) => clampZoom(prev + ZOOM_INCREMENT));
  }, []);

  // Zoom out by one increment
  const handleZoomOut = useCallback(() => {
    setZoom((prev) => clampZoom(prev - ZOOM_INCREMENT));
  }, []);

  // Set zoom to a specific value from the range input
  const handleZoomChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setZoom(clampZoom(Number(e.target.value)));
  }, []);

  // Handle click on backdrop (outside the image) to close
  const handleBackdropClick = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      // Only close if clicking the backdrop itself, not the image or controls
      if (e.target === e.currentTarget) {
        onClose();
      }
    },
    [onClose]
  );

  // Pinch gesture support for touch devices
  const handleTouchStart = useCallback((e: React.TouchEvent) => {
    if (e.touches.length === 2) {
      const dx = e.touches[0].clientX - e.touches[1].clientX;
      const dy = e.touches[0].clientY - e.touches[1].clientY;
      lastPinchDistanceRef.current = Math.sqrt(dx * dx + dy * dy);
    }
  }, []);

  const handleTouchMove = useCallback(
    (e: React.TouchEvent) => {
      if (e.touches.length === 2 && lastPinchDistanceRef.current !== null) {
        e.preventDefault();
        const dx = e.touches[0].clientX - e.touches[1].clientX;
        const dy = e.touches[0].clientY - e.touches[1].clientY;
        const currentDistance = Math.sqrt(dx * dx + dy * dy);
        const delta = currentDistance - lastPinchDistanceRef.current;

        // Threshold to avoid jittery zoom
        if (Math.abs(delta) > 20) {
          if (delta > 0) {
            setZoom((prev) => clampZoom(prev + ZOOM_INCREMENT));
          } else {
            setZoom((prev) => clampZoom(prev - ZOOM_INCREMENT));
          }
          lastPinchDistanceRef.current = currentDistance;
        }
      }
    },
    []
  );

  const handleTouchEnd = useCallback(() => {
    lastPinchDistanceRef.current = null;
  }, []);

  if (!isOpen) return null;

  return (
    <div
      ref={modalRef}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-sm"
      onClick={handleBackdropClick}
      role="dialog"
      aria-modal="true"
      aria-label={`Image viewer: ${alt}`}
      data-testid="image-modal"
    >
      {/* Close button */}
      <button
        onClick={onClose}
        className="absolute top-4 right-4 z-50 flex h-[44px] w-[44px] items-center justify-center rounded-full bg-surface/90 text-content shadow-elevated transition-all duration-150 hover:bg-surface hover:scale-110 focus:outline-none focus:ring-2 focus:ring-primary"
        aria-label="Close modal"
        data-testid="image-modal-close"
      >
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="24"
          height="24"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <line x1="18" y1="6" x2="6" y2="18" />
          <line x1="6" y1="6" x2="18" y2="18" />
        </svg>
      </button>

      {/* Image container with zoom */}
      <div
        ref={imageContainerRef}
        className="flex items-center justify-center overflow-auto max-h-[calc(100vh-120px)] max-w-[calc(100vw-48px)]"
        onTouchStart={handleTouchStart}
        onTouchMove={handleTouchMove}
        onTouchEnd={handleTouchEnd}
        data-testid="image-modal-container"
      >
        <img
          src={src}
          alt={alt}
          className="select-none transition-transform duration-200"
          style={{ transform: `scale(${zoom / 100})` }}
          draggable={false}
          data-testid="image-modal-image"
        />
      </div>

      {/* Zoom controls bar */}
      <div
        className="absolute bottom-6 left-1/2 -translate-x-1/2 flex items-center gap-3 rounded-lg bg-surface/95 px-4 py-2 shadow-elevated backdrop-blur-sm"
        onClick={(e) => e.stopPropagation()}
        data-testid="image-modal-controls"
      >
        {/* Zoom out button */}
        <button
          onClick={handleZoomOut}
          disabled={zoom <= MIN_ZOOM}
          className="flex h-[44px] w-[44px] items-center justify-center rounded-md text-content transition-colors duration-150 hover:bg-surface-secondary disabled:opacity-40 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-primary"
          aria-label="Zoom out"
          data-testid="image-modal-zoom-out"
        >
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
          >
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
            <line x1="8" y1="11" x2="14" y2="11" />
          </svg>
        </button>

        {/* Zoom range slider */}
        <input
          type="range"
          min={MIN_ZOOM}
          max={MAX_ZOOM}
          step={ZOOM_INCREMENT}
          value={zoom}
          onChange={handleZoomChange}
          className="w-32 accent-primary cursor-pointer"
          aria-label={`Zoom level: ${zoom}%`}
          data-testid="image-modal-zoom-slider"
        />

        {/* Zoom percentage label */}
        <span
          className="min-w-[48px] text-center text-body-sm font-medium text-content"
          data-testid="image-modal-zoom-label"
        >
          {zoom}%
        </span>

        {/* Zoom in button */}
        <button
          onClick={handleZoomIn}
          disabled={zoom >= MAX_ZOOM}
          className="flex h-[44px] w-[44px] items-center justify-center rounded-md text-content transition-colors duration-150 hover:bg-surface-secondary disabled:opacity-40 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-primary"
          aria-label="Zoom in"
          data-testid="image-modal-zoom-in"
        >
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
          >
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
            <line x1="11" y1="8" x2="11" y2="14" />
            <line x1="8" y1="11" x2="14" y2="11" />
          </svg>
        </button>
      </div>
    </div>
  );
}
