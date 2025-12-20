/**
 * Theme System
 *
 * CSS variable-based theming with built-in dark/light themes.
 * Users can create custom themes by defining CSS variables.
 */

import type { ThemeInterface } from "./types";

// ============================================================================
// Theme Definitions
// ============================================================================

export interface ThemeDefinition {
  id: string;
  name: string;
  variables: Record<string, string>;
}

export const themes: Record<string, ThemeDefinition> = {
  dark: {
    id: "dark",
    name: "Dark",
    variables: {
      // Backgrounds
      "--bg-primary": "#0d1117",
      "--bg-secondary": "#161b22",
      "--bg-tertiary": "#21262d",
      "--bg-hover": "#30363d",
      "--bg-active": "#388bfd22",

      // Borders
      "--border-primary": "#30363d",
      "--border-secondary": "#21262d",
      "--border-focus": "#58a6ff",

      // Text
      "--text-primary": "#e6edf3",
      "--text-secondary": "#8b949e",
      "--text-muted": "#6e7681",
      "--text-link": "#58a6ff",

      // Accents
      "--accent-primary": "#58a6ff",
      "--accent-success": "#3fb950",
      "--accent-warning": "#d29922",
      "--accent-danger": "#f85149",

      // Semantic colors
      "--color-positive": "#3fb950",
      "--color-negative": "#f85149",
      "--color-income": "#3fb950",
      "--color-expense": "#f85149",

      // Sidebar
      "--sidebar-bg": "#010409",
      "--sidebar-border": "#30363d",
      "--sidebar-item-hover": "#161b22",
      "--sidebar-item-active": "#0d1117",

      // Tabs
      "--tab-bg": "#0d1117",
      "--tab-active-bg": "#161b22",
      "--tab-border": "#30363d",

      // Status bar
      "--statusbar-bg": "#010409",
      "--statusbar-border": "#30363d",

      // Command palette
      "--palette-bg": "#161b22",
      "--palette-border": "#30363d",
      "--palette-item-hover": "#21262d",

      // Input
      "--input-bg": "#0d1117",
      "--input-border": "#30363d",
      "--input-focus-border": "#58a6ff",

      // Code/monospace
      "--code-bg": "#161b22",

      // Shadows
      "--shadow-sm": "0 1px 2px rgba(0, 0, 0, 0.3)",
      "--shadow-md": "0 4px 6px rgba(0, 0, 0, 0.4)",
      "--shadow-lg": "0 10px 20px rgba(0, 0, 0, 0.5)",

      // Fonts
      "--font-mono": "'JetBrains Mono', 'Fira Code', 'SF Mono', Consolas, monospace",
      "--font-sans": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",

      // Spacing
      "--spacing-xs": "4px",
      "--spacing-sm": "8px",
      "--spacing-md": "12px",
      "--spacing-lg": "16px",
      "--spacing-xl": "24px",

      // Border radius
      "--radius-sm": "4px",
      "--radius-md": "6px",
      "--radius-lg": "8px",
    },
  },

  light: {
    id: "light",
    name: "Light",
    variables: {
      // Backgrounds
      "--bg-primary": "#ffffff",
      "--bg-secondary": "#f6f8fa",
      "--bg-tertiary": "#eaeef2",
      "--bg-hover": "#eaeef2",
      "--bg-active": "#ddf4ff",

      // Borders
      "--border-primary": "#d0d7de",
      "--border-secondary": "#eaeef2",
      "--border-focus": "#0969da",

      // Text
      "--text-primary": "#1f2328",
      "--text-secondary": "#656d76",
      "--text-muted": "#8c959f",
      "--text-link": "#0969da",

      // Accents
      "--accent-primary": "#0969da",
      "--accent-success": "#1a7f37",
      "--accent-warning": "#9a6700",
      "--accent-danger": "#cf222e",

      // Semantic colors
      "--color-positive": "#1a7f37",
      "--color-negative": "#cf222e",
      "--color-income": "#1a7f37",
      "--color-expense": "#cf222e",

      // Sidebar
      "--sidebar-bg": "#f6f8fa",
      "--sidebar-border": "#d0d7de",
      "--sidebar-item-hover": "#eaeef2",
      "--sidebar-item-active": "#ffffff",

      // Tabs
      "--tab-bg": "#f6f8fa",
      "--tab-active-bg": "#ffffff",
      "--tab-border": "#d0d7de",

      // Status bar
      "--statusbar-bg": "#f6f8fa",
      "--statusbar-border": "#d0d7de",

      // Command palette
      "--palette-bg": "#ffffff",
      "--palette-border": "#d0d7de",
      "--palette-item-hover": "#f6f8fa",

      // Input
      "--input-bg": "#ffffff",
      "--input-border": "#d0d7de",
      "--input-focus-border": "#0969da",

      // Code/monospace
      "--code-bg": "#f6f8fa",

      // Shadows
      "--shadow-sm": "0 1px 2px rgba(0, 0, 0, 0.05)",
      "--shadow-md": "0 4px 6px rgba(0, 0, 0, 0.1)",
      "--shadow-lg": "0 10px 20px rgba(0, 0, 0, 0.15)",

      // Fonts (same as dark)
      "--font-mono": "'JetBrains Mono', 'Fira Code', 'SF Mono', Consolas, monospace",
      "--font-sans": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",

      // Spacing (same as dark)
      "--spacing-xs": "4px",
      "--spacing-sm": "8px",
      "--spacing-md": "12px",
      "--spacing-lg": "16px",
      "--spacing-xl": "24px",

      // Border radius (same as dark)
      "--radius-sm": "4px",
      "--radius-md": "6px",
      "--radius-lg": "8px",
    },
  },

  // Example custom theme
  nord: {
    id: "nord",
    name: "Nord",
    variables: {
      "--bg-primary": "#2e3440",
      "--bg-secondary": "#3b4252",
      "--bg-tertiary": "#434c5e",
      "--bg-hover": "#4c566a",
      "--bg-active": "#5e81ac33",

      "--border-primary": "#4c566a",
      "--border-secondary": "#434c5e",
      "--border-focus": "#88c0d0",

      "--text-primary": "#eceff4",
      "--text-secondary": "#d8dee9",
      "--text-muted": "#a5abb6",
      "--text-link": "#88c0d0",

      "--accent-primary": "#88c0d0",
      "--accent-success": "#a3be8c",
      "--accent-warning": "#ebcb8b",
      "--accent-danger": "#bf616a",

      "--color-positive": "#a3be8c",
      "--color-negative": "#bf616a",
      "--color-income": "#a3be8c",
      "--color-expense": "#bf616a",

      "--sidebar-bg": "#2e3440",
      "--sidebar-border": "#3b4252",
      "--sidebar-item-hover": "#3b4252",
      "--sidebar-item-active": "#434c5e",

      "--tab-bg": "#2e3440",
      "--tab-active-bg": "#3b4252",
      "--tab-border": "#4c566a",

      "--statusbar-bg": "#2e3440",
      "--statusbar-border": "#3b4252",

      "--palette-bg": "#3b4252",
      "--palette-border": "#4c566a",
      "--palette-item-hover": "#434c5e",

      "--input-bg": "#2e3440",
      "--input-border": "#4c566a",
      "--input-focus-border": "#88c0d0",

      "--code-bg": "#3b4252",

      "--shadow-sm": "0 1px 2px rgba(0, 0, 0, 0.3)",
      "--shadow-md": "0 4px 6px rgba(0, 0, 0, 0.4)",
      "--shadow-lg": "0 10px 20px rgba(0, 0, 0, 0.5)",

      "--font-mono": "'JetBrains Mono', 'Fira Code', 'SF Mono', Consolas, monospace",
      "--font-sans": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",

      "--spacing-xs": "4px",
      "--spacing-sm": "8px",
      "--spacing-md": "12px",
      "--spacing-lg": "16px",
      "--spacing-xl": "24px",

      "--radius-sm": "4px",
      "--radius-md": "6px",
      "--radius-lg": "8px",
    },
  },
};

// ============================================================================
// Theme Manager
// ============================================================================

class ThemeManager implements ThemeInterface {
  private _current: string = "dark";
  private subscribers: Set<(themeId: string) => void> = new Set();

  get current(): string {
    return this._current;
  }

  subscribe(callback: (themeId: string) => void): () => void {
    this.subscribers.add(callback);
    callback(this._current); // Call immediately with current value
    return () => this.subscribers.delete(callback);
  }

  setTheme(themeId: string) {
    if (!themes[themeId]) {
      console.warn(`Theme not found: ${themeId}`);
      return;
    }

    this._current = themeId;
    this.applyTheme(themeId);
    this.subscribers.forEach((cb) => cb(themeId));
  }

  private applyTheme(themeId: string) {
    const theme = themes[themeId];
    if (!theme) return;

    const root = document.documentElement;
    for (const [key, value] of Object.entries(theme.variables)) {
      root.style.setProperty(key, value);
    }
  }

  getVar(name: string): string {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }

  // Initialize theme on load
  init() {
    // Check for saved preference
    const saved = localStorage.getItem("treeline-theme");
    if (saved && themes[saved]) {
      this.setTheme(saved);
    } else {
      // Default to dark
      this.setTheme("dark");
    }
  }

  // Get all available themes
  getAvailableThemes(): { id: string; name: string }[] {
    return Object.values(themes).map((t) => ({ id: t.id, name: t.name }));
  }
}

export const themeManager = new ThemeManager();
