/**
 * Treeline Public Plugin SDK
 *
 * This module exposes the API available to community plugins.
 * It's passed to external plugin views via props.
 */

import { executeQuery, type QueryResult } from "./api";
import { showToast, toast } from "./toast.svelte";
import { themeManager } from "./theme";
import { registry } from "./registry";
import { modKey, formatShortcut, isMac } from "./platform";
import {
  getPluginSettings,
  setPluginSettings,
  readPluginState,
  writePluginState,
} from "./settings";
import {
  SUPPORTED_CURRENCIES,
  DEFAULT_CURRENCY,
  getCurrencySymbol,
  formatCurrency,
  formatCurrencyCompact,
  formatAmount,
} from "../shared/currency";
import {
  getCurrency,
  formatUserCurrency,
  formatUserCurrencyCompact,
  getUserCurrencySymbol,
} from "../shared/currencyStore.svelte";

// Re-export types for plugin authors
export type { Plugin, PluginManifest, PluginContext, PluginPermissions } from "./types";
export type { QueryResult } from "./api";

/**
 * Full permissions object for a plugin
 */
export interface PluginTablePermissions {
  read?: string[];   // Tables allowed for SELECT (undefined = all allowed for backwards compat)
  write?: string[];  // Tables allowed for INSERT/UPDATE/DELETE
  create?: string[]; // Tables allowed for CREATE/DROP (implicitly writable)
}

/**
 * The SDK object passed to external plugin views via props.
 * Access it via: const { sdk } = $props();
 */
export interface PluginSDK {
  /**
   * Execute a read-only SQL query
   */
  query: <T = Record<string, any>>(sql: string) => Promise<T[]>;

  /**
   * Execute a write SQL query (restricted to plugin's allowed tables)
   */
  execute: (sql: string) => Promise<{ rowsAffected: number }>;

  /**
   * Show a toast notification
   */
  toast: {
    show: (message: string, description?: string) => void;
    success: (message: string, description?: string) => void;
    error: (message: string, description?: string) => void;
    warning: (message: string, description?: string) => void;
    info: (message: string, description?: string) => void;
  };

  /**
   * Navigate to another view
   */
  openView: (viewId: string, props?: Record<string, any>) => void;

  /**
   * Subscribe to data refresh events (called after sync/import)
   */
  onDataRefresh: (callback: () => void) => () => void;

  /**
   * Emit a data refresh event (call after modifying data)
   */
  emitDataRefresh: () => void;

  /**
   * Update the badge count shown on this plugin's sidebar item
   * @param count The badge count (0 or undefined to hide badge)
   */
  updateBadge: (count: number | undefined) => void;

  /**
   * Theme utilities
   */
  theme: {
    current: () => "light" | "dark";
    subscribe: (callback: (theme: string) => void) => () => void;
  };

  /**
   * Platform-aware modifier key ("Cmd" on Mac, "Ctrl" elsewhere)
   */
  modKey: "Cmd" | "Ctrl";

  /**
   * Format a keyboard shortcut for display
   */
  formatShortcut: (shortcut: string) => string;

  /**
   * Plugin settings (scoped to plugin ID)
   */
  settings: {
    get: <T>() => Promise<T>;
    set: <T>(settings: T) => Promise<void>;
  };

  /**
   * Plugin state (scoped to plugin ID, for ephemeral data)
   */
  state: {
    read: <T>() => Promise<T | null>;
    write: <T>(state: T) => Promise<void>;
  };

  /**
   * Currency formatting utilities
   */
  currency: {
    /** Format an amount with currency symbol (e.g., "$1,234.56") */
    format: (amount: number, currency?: string) => string;
    /** Format compactly for large amounts (e.g., "$1.2M", "$500K") */
    formatCompact: (amount: number, currency?: string) => string;
    /** Format just the number without currency symbol (e.g., "1,234.56") */
    formatAmount: (amount: number) => string;
    /** Get the symbol for a currency code (e.g., "USD" -> "$") */
    getSymbol: (currency: string) => string;
    /** Get the user's configured currency (currently always "USD" for MVP) */
    getUserCurrency: () => string;
    /** List of supported currency codes */
    supportedCurrencies: string[];
  };
}

/**
 * Create an SDK instance for a specific plugin.
 * This is called internally when mounting external plugin views.
 *
 * @param pluginId - The plugin's unique identifier
 * @param permissions - Table permissions (read is required)
 */
export function createPluginSDK(pluginId: string, permissions: PluginTablePermissions): PluginSDK {
  // Compute effective write tables: explicit write + create (create implies write)
  const effectiveWriteTables = [
    ...(permissions.write ?? []),
    ...(permissions.create ?? []),
  ];

  // Read permissions are required - empty array means no reads allowed
  const allowedReadTables = permissions.read ?? [];

  return {
    // Database - read-only queries (with table restriction)
    query: async <T = Record<string, any>>(sql: string): Promise<T[]> => {
      validateReadQuery(sql, pluginId, allowedReadTables);
      const result = await executeQuery(sql, { readonly: true });
      return result.rows as T[];
    },

    // Database - write queries (with table restriction)
    execute: async (sql: string): Promise<{ rowsAffected: number }> => {
      // Validate that query only targets allowed tables
      validateWriteQuery(sql, pluginId, effectiveWriteTables, permissions.create ?? []);
      const result = await executeQuery(sql, { readonly: false });
      return { rowsAffected: result.rows.length };
    },

    // Toast notifications
    toast: {
      show: (message: string, description?: string) => showToast(message, "info", description),
      success: (message: string, description?: string) => toast.success(message, description),
      error: (message: string, description?: string) => toast.error(message, description),
      warning: (message: string, description?: string) => toast.warning(message, description),
      info: (message: string, description?: string) => toast.info(message, description),
    },

    // Navigation
    openView: (viewId: string, props?: Record<string, any>) => {
      registry.openView(viewId, props);
    },

    // Events
    onDataRefresh: (callback: () => void) => {
      return registry.on("data:refresh", callback);
    },

    emitDataRefresh: () => {
      registry.emit("data:refresh");
    },

    // Badge - update sidebar badge for this plugin
    updateBadge: (count: number | undefined) => {
      registry.updateSidebarBadge(pluginId, count);
    },

    // Theme
    theme: {
      current: () => themeManager.current as "light" | "dark",
      subscribe: (callback: (theme: string) => void) => themeManager.subscribe(callback),
    },

    // Platform
    modKey: modKey(),
    formatShortcut,

    // Plugin settings (scoped)
    settings: {
      get: <T>() => getPluginSettings<T>(pluginId),
      set: <T>(settings: T) => setPluginSettings(pluginId, settings),
    },

    // Plugin state (scoped)
    state: {
      read: <T>() => readPluginState<T>(pluginId),
      write: <T>(state: T) => writePluginState(pluginId, state),
    },

    // Currency formatting (uses user's currency preference by default)
    currency: {
      format: (amount: number, currency?: string) => currency ? formatCurrency(amount, currency) : formatUserCurrency(amount),
      formatCompact: (amount: number, currency?: string) => currency ? formatCurrencyCompact(amount, currency) : formatUserCurrencyCompact(amount),
      formatAmount: (amount: number) => formatAmount(amount),
      getSymbol: (currency?: string) => currency ? getCurrencySymbol(currency) : getUserCurrencySymbol(),
      getUserCurrency: () => getCurrency(),
      supportedCurrencies: Object.keys(SUPPORTED_CURRENCIES),
    },
  };
}

/**
 * Extract table names from a SELECT query.
 * Returns all tables referenced in FROM and JOIN clauses.
 * Excludes CTE (Common Table Expression) aliases defined in WITH clauses.
 */
function extractReadTables(sql: string): string[] {
  // Remove SQL comments to simplify parsing
  const sqlNoComments = sql
    .replace(/--[^\n]*/g, " ")  // Remove single-line comments
    .replace(/\/\*[\s\S]*?\*\//g, " ");  // Remove multi-line comments

  const normalizedSql = sqlNoComments.toLowerCase();
  const tables: string[] = [];

  // Extract all CTE names from WITH clauses
  // Matches: WITH name AS, and subsequent name AS patterns
  const cteNames = new Set<string>();

  // Match all "name AS (" patterns that could be CTEs
  // This handles: WITH cte1 AS (...), cte2 AS (...), cte3 AS (...)
  const allCteMatches = normalizedSql.matchAll(/(\w+)\s+as\s*\(/gi);
  for (const match of allCteMatches) {
    cteNames.add(match[1]);
  }

  // Match FROM table_name (with optional alias)
  const fromMatches = normalizedSql.matchAll(/\bfrom\s+(\w+)(?:\s+(?:as\s+)?(\w+))?/gi);
  for (const match of fromMatches) {
    const tableName = match[1];
    // Exclude CTE aliases - they're not real tables
    if (!cteNames.has(tableName)) {
      tables.push(tableName);
    }
  }

  // Match JOIN table_name (with optional alias)
  const joinMatches = normalizedSql.matchAll(/\bjoin\s+(\w+)(?:\s+(?:as\s+)?(\w+))?/gi);
  for (const match of joinMatches) {
    const tableName = match[1];
    // Exclude CTE aliases
    if (!cteNames.has(tableName)) {
      tables.push(tableName);
    }
  }

  return [...new Set(tables)]; // Deduplicate
}

/**
 * Validate that a read query only accesses allowed tables.
 * Throws an error if the query attempts to read from unauthorized tables.
 * Use "*" in allowedTables to allow reading any table.
 */
function validateReadQuery(sql: string, pluginId: string, allowedTables: string[]): void {
  // Wildcard allows all reads
  if (allowedTables.includes("*")) {
    return;
  }

  const referencedTables = extractReadTables(sql);
  const normalizedAllowed = allowedTables.map(t => t.toLowerCase());

  for (const table of referencedTables) {
    if (!normalizedAllowed.includes(table)) {
      throw new Error(
        `Plugin "${pluginId}" does not have permission to read from table "${table}". ` +
        `Allowed tables: ${allowedTables.join(", ")}`
      );
    }
  }
}

/**
 * Validate that a write query only targets allowed tables.
 * Throws an error if the query attempts to write to unauthorized tables.
 *
 * @param sql - The SQL query to validate
 * @param pluginId - The plugin's ID for error messages
 * @param allowedWriteTables - Tables allowed for INSERT/UPDATE/DELETE
 * @param allowedCreateTables - Tables allowed for CREATE/DROP/ALTER
 */
function validateWriteQuery(
  sql: string,
  pluginId: string,
  allowedWriteTables: string[],
  allowedCreateTables: string[]
): void {
  // Normalize SQL for parsing
  const normalizedSql = sql.toLowerCase().trim();

  // Track if this is a DDL operation (CREATE/DROP/ALTER)
  let isDDL = false;
  let targetTable: string | null = null;

  // INSERT INTO table_name (check first, handles ON CONFLICT ... DO UPDATE)
  const insertMatch = normalizedSql.match(/^insert\s+into\s+(\w+)/);
  if (insertMatch) {
    targetTable = insertMatch[1];
  }

  // UPDATE table_name (only if not already matched as INSERT)
  if (!targetTable) {
    const updateMatch = normalizedSql.match(/^update\s+(\w+)/);
    if (updateMatch) {
      targetTable = updateMatch[1];
    }
  }

  // DELETE FROM table_name
  if (!targetTable) {
    const deleteMatch = normalizedSql.match(/^delete\s+from\s+(\w+)/);
    if (deleteMatch) {
      targetTable = deleteMatch[1];
    }
  }

  // CREATE TABLE table_name (DDL)
  if (!targetTable) {
    const createMatch = normalizedSql.match(/^create\s+table\s+(?:if\s+not\s+exists\s+)?(\w+)/);
    if (createMatch) {
      targetTable = createMatch[1];
      isDDL = true;
    }
  }

  // DROP TABLE table_name (DDL)
  if (!targetTable) {
    const dropMatch = normalizedSql.match(/^drop\s+table\s+(?:if\s+exists\s+)?(\w+)/);
    if (dropMatch) {
      targetTable = dropMatch[1];
      isDDL = true;
    }
  }

  // ALTER TABLE table_name (DDL)
  if (!targetTable) {
    const alterMatch = normalizedSql.match(/^alter\s+table\s+(\w+)/);
    if (alterMatch) {
      targetTable = alterMatch[1];
      isDDL = true;
    }
  }

  if (!targetTable) {
    // If we can't parse the table, reject for safety
    throw new Error(
      `Plugin "${pluginId}" attempted an unrecognized write query. ` +
      `Only INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, and ALTER TABLE are allowed.`
    );
  }

  // DDL operations check against create tables; DML against write tables
  const allowedTables = isDDL ? allowedCreateTables : allowedWriteTables;
  const normalizedAllowed = allowedTables.map(t => t.toLowerCase());

  if (!normalizedAllowed.includes(targetTable)) {
    const operationType = isDDL ? "create/drop/alter" : "write to";
    throw new Error(
      `Plugin "${pluginId}" does not have permission to ${operationType} table "${targetTable}". ` +
      `Allowed tables: ${allowedTables.join(", ") || "(none)"}`
    );
  }
}

// Also export individual functions for core plugins that import directly
export { showToast, modKey, formatShortcut, isMac };

// Currency utilities for core plugins
export {
  SUPPORTED_CURRENCIES,
  DEFAULT_CURRENCY,
  getCurrencySymbol,
  formatCurrency,
  formatCurrencyCompact,
  formatAmount,
};
