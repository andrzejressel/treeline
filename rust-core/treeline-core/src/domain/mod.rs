//! Core domain entities
//!
//! All business entities are defined here. These are pure data structures
//! with validation logic - no I/O or external dependencies.

mod account;
mod transaction;
pub mod balance;
mod user;
mod backup;
mod encryption;
pub mod result;

pub use account::Account;
pub use transaction::Transaction;
pub use balance::BalanceSnapshot;
pub use user::User;
pub use backup::BackupMetadata;
pub use encryption::{Argon2Params, EncryptionMetadata, EncryptionStatus};
