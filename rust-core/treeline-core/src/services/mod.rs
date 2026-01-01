//! Service layer - business logic orchestration
//!
//! Services coordinate domain logic and port interactions. Each service
//! focuses on a specific use case or feature area.

mod status;
mod sync;
mod query;
mod tag;
mod backup;
mod compact;
mod doctor;
pub mod encryption;
pub mod import;
mod balance;
pub mod plugin;

pub use status::{StatusService, StatusSummary, AccountSummary, DateRange};
pub use sync::SyncService;
pub use query::QueryService;
pub use tag::{TagService, TagResult, TagResultEntry};
pub use backup::BackupService;
pub use compact::CompactService;
pub use doctor::DoctorService;
pub use encryption::EncryptionService;
pub use import::ImportService;
pub use balance::BalanceService;
pub use plugin::{PluginService, PluginInfo, PluginManifest, PluginResult, UpdateInfo};
