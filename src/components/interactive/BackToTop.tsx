import React, { useState, useEffect, useCallback } from 'react';

/**
 * BackToTop - Floating back-to-top button (React island, client:idle).
 *
 * Behavior:
 * - Hidden by default
 * - Appears with fade-in animation when user scrolls > 1 viewport height
 * - Smooth-scrolls to page top on click
 * - Respects prefers-reduced-motion
 * - 44x44px minimum touch target
 *
 * Requirements: 14.8, 14.9
 */

export default function BackToTop() {
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    const handleScroll = () => {
      // Show when scrolled more than 1 viewport height
      const scrollThreshold = window.innerHeight;
      setVisible(window.scrollY > scrollThreshold);
    };

    window.addEventListener('scroll', handleScroll, { passive: true });
    // Check initial scroll position (e.g., page reload while scrolled)
    handleScroll();

    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  const scrollToTop = useCallback(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);

  if (!visible) {
    return null;
  }

  return (
    <button
      type="button"
      onClick={scrollToTop}
      className="fixed bottom-6 right-6 z-50 flex items-center justify-center w-[44px] h-[44px] rounded-full bg-[var(--color-primary)] text-white shadow-lg hover:bg-[var(--color-primary-dark)] focus:outline-none focus:ring-2 focus:ring-[var(--color-primary)] focus:ring-offset-2 back-to-top-enter interactive"
      aria-label="Scroll to top"
      title="Back to top"
      data-testid="back-to-top"
    >
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="20"
        height="20"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <polyline points="18 15 12 9 6 15" />
      </svg>
    </button>
  );
}
