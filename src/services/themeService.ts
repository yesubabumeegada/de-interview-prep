/**
 * ThemeService - Manages dark/light mode toggle with localStorage persistence.
 * Applies Tailwind's class-based dark mode by toggling `dark` on document.documentElement.
 * Handles SSR/build contexts where window/localStorage may be unavailable.
 *
 * Storage key: "de-prep-theme"
 * Values: "light" | "dark"
 * Default: "light"
 */

export type Theme = 'light' | 'dark';

const STORAGE_KEY = 'de-prep-theme';
const DEFAULT_THEME: Theme = 'light';

export interface ThemeService {
  getCurrentTheme(): Theme;
  toggleTheme(): void;
  setTheme(theme: Theme): void;
  respectsReducedMotion(): boolean;
}

/**
 * Checks if we're in a browser environment with access to DOM and storage APIs.
 */
function isBrowser(): boolean {
  return typeof window !== 'undefined' && typeof document !== 'undefined';
}

/**
 * Safely reads the theme preference from localStorage.
 * Returns null if storage is unavailable or value is not a valid theme.
 */
function readStoredTheme(): Theme | null {
  if (!isBrowser()) return null;
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored === 'light' || stored === 'dark') {
      return stored;
    }
    return null;
  } catch {
    // localStorage may throw (e.g., SecurityError in some contexts)
    return null;
  }
}

/**
 * Safely writes the theme preference to localStorage.
 */
function writeStoredTheme(theme: Theme): void {
  if (!isBrowser()) return;
  try {
    localStorage.setItem(STORAGE_KEY, theme);
  } catch {
    // Silently fail if storage is unavailable or quota exceeded
  }
}

/**
 * Applies or removes the `dark` class on document.documentElement.
 */
function applyThemeToDocument(theme: Theme): void {
  if (!isBrowser()) return;
  if (theme === 'dark') {
    document.documentElement.classList.add('dark');
  } else {
    document.documentElement.classList.remove('dark');
  }
}

/**
 * Creates and returns a ThemeService instance.
 * On creation, reads the stored preference (defaulting to light) and applies it.
 */
function createThemeService(): ThemeService {
  // Determine initial theme from storage, defaulting to light
  let currentTheme: Theme = readStoredTheme() ?? DEFAULT_THEME;

  // Apply on initialization
  applyThemeToDocument(currentTheme);

  return {
    getCurrentTheme(): Theme {
      return currentTheme;
    },

    toggleTheme(): void {
      const newTheme: Theme = currentTheme === 'light' ? 'dark' : 'light';
      this.setTheme(newTheme);
    },

    setTheme(theme: Theme): void {
      currentTheme = theme;
      applyThemeToDocument(theme);
      writeStoredTheme(theme);
    },

    respectsReducedMotion(): boolean {
      if (!isBrowser()) return false;
      try {
        return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      } catch {
        return false;
      }
    },
  };
}

// Export a singleton instance for use across the application
export const themeService: ThemeService = createThemeService();

// Also export the factory for testing or cases where a fresh instance is needed
export { createThemeService };
