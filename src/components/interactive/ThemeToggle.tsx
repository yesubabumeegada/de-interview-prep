import React, { useState, useEffect, useCallback } from 'react';
import { themeService, type Theme } from '../../services/themeService';

/**
 * ThemeToggle - Dark/light mode toggle button (React island).
 * Rendered as client:load in the TopBar for every page.
 *
 * Features:
 * - Sun icon (light mode) / Moon icon (dark mode)
 * - 200–400ms color transition via CSS
 * - Persists preference in localStorage (key: "de-prep-theme")
 * - 44x44px minimum touch target
 * - Accessible labeling with aria-label
 * - Respects prefers-reduced-motion
 *
 * Requirements: 14.6
 */

export default function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>(() => {
    // Initialize from themeService (reads localStorage / defaults to 'light')
    return themeService.getCurrentTheme();
  });

  const reducedMotion = themeService.respectsReducedMotion();

  // Sync with the document in case the theme was set before hydration
  useEffect(() => {
    const currentTheme = themeService.getCurrentTheme();
    setTheme(currentTheme);
  }, []);

  const handleToggle = useCallback(() => {
    themeService.toggleTheme();
    const newTheme = themeService.getCurrentTheme();
    setTheme(newTheme);

    // Add theme-transition class to the document root for smooth color transitions
    if (!reducedMotion) {
      document.documentElement.classList.add('theme-transition');
      // Remove after the transition completes to avoid interfering with other transitions
      setTimeout(() => {
        document.documentElement.classList.remove('theme-transition');
      }, 400);
    }
  }, [reducedMotion]);

  const isDark = theme === 'dark';
  const label = isDark ? 'Switch to light mode' : 'Switch to dark mode';

  return (
    <button
      type="button"
      onClick={handleToggle}
      className="flex items-center justify-center w-[44px] h-[44px] rounded-lg bg-[var(--color-surface-secondary)] hover:bg-[var(--color-surface-tertiary)] focus:outline-none focus:ring-2 focus:ring-[var(--color-primary)] transition-colors duration-300 ease-in-out"
      aria-label={label}
      title={label}
      data-testid="theme-toggle"
    >
      {isDark ? (
        // Sun icon - shown in dark mode (click to go light)
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="22"
          height="22"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="text-[var(--color-warning)] transition-transform duration-300 ease-in-out"
          aria-hidden="true"
        >
          <circle cx="12" cy="12" r="5" />
          <line x1="12" y1="1" x2="12" y2="3" />
          <line x1="12" y1="21" x2="12" y2="23" />
          <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" />
          <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
          <line x1="1" y1="12" x2="3" y2="12" />
          <line x1="21" y1="12" x2="23" y2="12" />
          <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" />
          <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
        </svg>
      ) : (
        // Moon icon - shown in light mode (click to go dark)
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
          className="text-[var(--color-secondary)] transition-transform duration-300 ease-in-out"
          aria-hidden="true"
        >
          <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
        </svg>
      )}
    </button>
  );
}
