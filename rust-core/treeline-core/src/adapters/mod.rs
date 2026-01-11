//! Adapter implementations
//!
//! Adapters implement the port traits with concrete technologies:
//! - DuckDB for the Repository port
//! - SimpleFIN HTTP client for DataAggregationProvider
//! - Lunchflow HTTP client for DataAggregationProvider (global banks)
//! - Demo data provider for testing
//! - Local filesystem for BackupStorageProvider

pub mod duckdb;
pub mod demo;
pub mod simplefin;
pub mod lunchflow;

#[cfg(test)]
pub mod lunchflow_mock;
