import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createThemeService, type ThemeService } from '../../src/services/themeService';

describe('ThemeService', () => {
  let service: ThemeService;
  let localStorageMock: Record<string, string>;

  beforeEach(() => {
    // Mock localStorage
    localStorageMock = {};
    const storageMock = {
      getItem: vi.fn((key: string) => localStorageMock[key] ?? null),
      setItem: vi.fn((key: string, value: string) => {
        localStorageMock[key] = value;
      }),
      removeItem: vi.fn((key: string) => {
        delete localStorageMock[key];
      }),
      clear: vi.fn(() => {
        localStorageMock = {};
      }),
      get length() {
        return Object.keys(localStorageMock).length;
      },
      key: vi.fn((index: number) => Object.keys(localStorageMock)[index] ?? null),
    };
    Object.defineProperty(globalThis, 'localStorage', { value: storageMock, writable: true, configurable: true });

    // Mock document.documentElement
    const classList = new Set<string>();
    const documentElementMock = {
      classList: {
        add: vi.fn((cls: string) => classList.add(cls)),
        remove: vi.fn((cls: string) => classList.delete(cls)),
        contains: (cls: string) => classList.has(cls),
      },
    };
    Object.defineProperty(globalThis, 'document', {
      value: { documentElement: documentElementMock },
      writable: true,
      configurable: true,
    });

    // Mock window.matchMedia
    Object.defineProperty(globalThis, 'window', {
      value: {
        matchMedia: vi.fn((query: string) => ({
          matches: false,
          media: query,
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
        })),
      },
      writable: true,
      configurable: true,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('initialization', () => {
    it('defaults to light mode when no stored preference', () => {
      service = createThemeService();
      expect(service.getCurrentTheme()).toBe('light');
    });

    it('reads stored dark theme from localStorage', () => {
      localStorageMock['de-prep-theme'] = 'dark';
      service = createThemeService();
      expect(service.getCurrentTheme()).toBe('dark');
    });

    it('reads stored light theme from localStorage', () => {
      localStorageMock['de-prep-theme'] = 'light';
      service = createThemeService();
      expect(service.getCurrentTheme()).toBe('light');
    });

    it('defaults to light when stored value is invalid', () => {
      localStorageMock['de-prep-theme'] = 'invalid-value';
      service = createThemeService();
      expect(service.getCurrentTheme()).toBe('light');
    });

    it('applies dark class on init when stored theme is dark', () => {
      localStorageMock['de-prep-theme'] = 'dark';
      service = createThemeService();
      expect(document.documentElement.classList.add).toHaveBeenCalledWith('dark');
    });

    it('removes dark class on init when stored theme is light', () => {
      localStorageMock['de-prep-theme'] = 'light';
      service = createThemeService();
      expect(document.documentElement.classList.remove).toHaveBeenCalledWith('dark');
    });
  });

  describe('setTheme', () => {
    beforeEach(() => {
      service = createThemeService();
    });

    it('sets theme to dark and applies dark class', () => {
      service.setTheme('dark');
      expect(service.getCurrentTheme()).toBe('dark');
      expect(document.documentElement.classList.add).toHaveBeenCalledWith('dark');
    });

    it('sets theme to light and removes dark class', () => {
      service.setTheme('dark');
      service.setTheme('light');
      expect(service.getCurrentTheme()).toBe('light');
      expect(document.documentElement.classList.remove).toHaveBeenCalledWith('dark');
    });

    it('persists theme preference to localStorage', () => {
      service.setTheme('dark');
      expect(localStorage.setItem).toHaveBeenCalledWith('de-prep-theme', 'dark');
    });
  });

  describe('toggleTheme', () => {
    it('toggles from light to dark', () => {
      service = createThemeService();
      expect(service.getCurrentTheme()).toBe('light');
      service.toggleTheme();
      expect(service.getCurrentTheme()).toBe('dark');
    });

    it('toggles from dark to light', () => {
      localStorageMock['de-prep-theme'] = 'dark';
      service = createThemeService();
      expect(service.getCurrentTheme()).toBe('dark');
      service.toggleTheme();
      expect(service.getCurrentTheme()).toBe('light');
    });

    it('persists toggled theme to localStorage', () => {
      service = createThemeService();
      service.toggleTheme();
      expect(localStorage.setItem).toHaveBeenCalledWith('de-prep-theme', 'dark');
    });
  });

  describe('respectsReducedMotion', () => {
    it('returns false when prefers-reduced-motion is not set', () => {
      service = createThemeService();
      expect(service.respectsReducedMotion()).toBe(false);
    });

    it('returns true when prefers-reduced-motion: reduce is active', () => {
      (window.matchMedia as ReturnType<typeof vi.fn>).mockReturnValue({
        matches: true,
        media: '(prefers-reduced-motion: reduce)',
      });
      service = createThemeService();
      expect(service.respectsReducedMotion()).toBe(true);
    });
  });

  describe('graceful handling when APIs unavailable', () => {
    it('defaults to light when localStorage throws', () => {
      Object.defineProperty(globalThis, 'localStorage', {
        value: {
          getItem: () => { throw new Error('SecurityError'); },
          setItem: () => { throw new Error('SecurityError'); },
        },
        writable: true,
        configurable: true,
      });
      service = createThemeService();
      expect(service.getCurrentTheme()).toBe('light');
    });

    it('does not throw when setTheme is called and localStorage is unavailable', () => {
      Object.defineProperty(globalThis, 'localStorage', {
        value: {
          getItem: () => null,
          setItem: () => { throw new Error('QuotaExceededError'); },
        },
        writable: true,
        configurable: true,
      });
      service = createThemeService();
      expect(() => service.setTheme('dark')).not.toThrow();
      expect(service.getCurrentTheme()).toBe('dark');
    });
  });
});
