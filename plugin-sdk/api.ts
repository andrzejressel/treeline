/**
 * API interface to Tauri backend
 */

import { invoke } from "@tauri-apps/api/core";

export interface StatusResponse {
  total_accounts: number;
  total_transactions: number;
  total_snapshots: number;
  total_integrations: number;
  integration_names: string[];
  earliest_date: string | null;
  latest_date: string | null;
  accounts: unknown[];
  integrations: unknown[];
}

/**
 * Get financial data status from CLI
 */
export async function getStatus(): Promise<StatusResponse> {
  const jsonString = await invoke<string>("status");
  console.log(jsonString);

  // Parse JSON string from Rust backend
  const response = JSON.parse(jsonString);

  // Convert from raw response to StatusResponse
  return {
    total_accounts: Number(response.total_accounts) || 0,
    total_transactions: Number(response.total_transactions) || 0,
    total_snapshots: Number(response.total_snapshots) || 0,
    total_integrations: Number(response.total_integrations) || 0,
    integration_names: response.integration_names || [],
    earliest_date: response.earliest_date || null,
    latest_date: response.latest_date || null,
    accounts: response.accounts || [],
    integrations: response.integrations || []
  };
}

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  row_count: number;
}

export interface ExecuteQueryOptions {
  readonly?: boolean;
}

/**
 * Execute a SQL query against the DuckDB database
 * @param query SQL query string
 * @param options.readonly If true (default), opens read-only connection. Set to false for writes.
 */
export async function executeQuery(query: string, options: ExecuteQueryOptions = {}): Promise<QueryResult> {
  const { readonly = true } = options;

  try {
    const jsonString = await invoke<string>("execute_query", { query, readonly });

    // Parse JSON string from Rust backend
    const response = JSON.parse(jsonString);

    return {
      columns: response.columns || [],
      rows: response.rows || [],
      row_count: response.row_count || 0,
    };
  } catch (e) {
    // Tauri invoke errors come as strings from Rust's Result::Err
    if (typeof e === 'string') {
      throw new Error(e);
    }
    throw e;
  }
}
