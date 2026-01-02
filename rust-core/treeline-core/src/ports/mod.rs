//! Port definitions (hexagonal architecture)
//!
//! Ports define the interfaces for external dependencies. The core domain
//! depends only on these traits, not on concrete implementations.

mod data_provider;
mod repository;

pub use data_provider::{
    DataAggregationProvider, IntegrationProvider,
    FetchAccountsResult, FetchTransactionsResult,
};
pub use repository::Repository;
