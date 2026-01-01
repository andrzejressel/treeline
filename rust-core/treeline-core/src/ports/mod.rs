//! Port definitions (hexagonal architecture)
//!
//! Ports define the interfaces for external dependencies. The core domain
//! depends only on these traits, not on concrete implementations.

mod repository;

pub use repository::Repository;
