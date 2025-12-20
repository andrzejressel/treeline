/**
 * Treeline Plugin SDK
 *
 * This is the main entry point for plugin authors.
 * Import from '@treeline/sdk' (aliased in the build config)
 */

// Types
export type {
  Plugin,
  PluginManifest,
  PluginPermissions,
  PluginContext,
  SidebarSection,
  SidebarItem,
  ViewDefinition,
  Command,
  StatusBarItem,
  Tab,
  ThemeInterface,
} from "./types";

// Public SDK (for external plugins)
export { createPluginSDK } from "./public";
export type { PluginSDK, PluginTablePermissions } from "./public";

// Registry (for core use)
export { registry } from "./registry";

// API
export { getStatus, executeQuery } from "./api";
export type { StatusResponse, QueryResult, ExecuteQueryOptions } from "./api";

// Theme
export { themeManager, themes } from "./theme";
export type { ThemeDefinition } from "./theme";

// Settings
export {
  readSettings,
  writeSettings,
  getSettings,
  getAppSetting,
  setAppSetting,
  getPluginSettings,
  setPluginSettings,
  updatePluginSettings,
  subscribeToSettings,
  clearSettingsCache,
  readPluginState,
  writePluginState,
  runSync,
  isSyncNeeded,
  getDemoMode,
  setDemoMode,
  enableDemo,
  disableDemo,
  // Plugin enable/disable
  isPluginDisabled,
  getDisabledPlugins,
  enablePlugin,
  disablePlugin,
  // Backfill
  runBackfill,
  // CSV Import
  pickCsvFile,
  getCsvHeaders,
  importCsvPreview,
  importCsvExecute,
  // Integrations
  setupSimplefin,
  // Integration Account Settings
  getIntegrationSettings,
  updateIntegrationAccountSetting,
  // Community Plugins
  installPlugin,
  uninstallPlugin,
  // Encryption
  getEncryptionStatus,
  tryAutoUnlock,
  unlockDatabase,
  enableEncryption,
  disableEncryption,
} from "./settings";
export type {
  Settings,
  AppSettings,
  SyncResult,
  ImportColumnMapping,
  ImportPreviewResult,
  ImportExecuteResult,
  PluginInstallResult,
  EncryptionStatus,
} from "./settings";

// Toast notifications
export { toast, showToast, dismissToast, dismissAllToasts } from "./toast.svelte";
export type { Toast, ToastOptions, ToastType } from "./toast.svelte";

// Activity tracking (for status bar)
export { activityStore, withActivity } from "./activity.svelte";
export type { Activity } from "./activity.svelte";

// Platform utilities
export { isMac, modKey, formatShortcut } from "./platform";

// Currency utilities
export {
  SUPPORTED_CURRENCIES,
  DEFAULT_CURRENCY,
  getCurrencySymbol,
  formatCurrency,
  formatCurrencyCompact,
  formatAmount,
} from "./public";
