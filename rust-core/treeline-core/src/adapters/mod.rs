//! Adapter implementations
//!
//! Adapters implement the port traits with concrete technologies:
//! - DuckDB for the Repository port
//! - SimpleFIN HTTP client for DataAggregationProvider
//! - Demo data provider for testing
//! - Local filesystem for BackupStorageProvider

pub mod duckdb;
pub mod demo;
pub mod simplefin;
