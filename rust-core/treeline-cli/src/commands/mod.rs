//! CLI command implementations

pub mod backup;
pub mod compact;
pub mod demo;
pub mod doctor;
pub mod encrypt;
pub mod plugin;
pub mod query;
pub mod status;
pub mod sync;
pub mod tag;

use std::path::PathBuf;
use anyhow::{Result, Context};
use treeline_core::TreelineContext;
use treeline_core::services::EncryptionService;

/// Get the treeline directory from environment or default
pub fn get_treeline_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("TREELINE_DIR") {
        PathBuf::from(dir)
    } else {
        dirs::home_dir()
            .expect("Could not find home directory")
            .join(".treeline")
    }
}

/// Get or create treeline context
pub fn get_context() -> Result<TreelineContext> {
    let treeline_dir = get_treeline_dir();

    // Create directory if it doesn't exist
    std::fs::create_dir_all(&treeline_dir)
        .with_context(|| format!("Failed to create treeline directory: {:?}", treeline_dir))?;

    // Determine encryption key
    // Priority: TL_DB_KEY (pre-derived) > TL_DB_PASSWORD (needs derivation)
    let encryption_key = if let Ok(key) = std::env::var("TL_DB_KEY") {
        // Already derived key (used by Tauri app)
        Some(key)
    } else if let Ok(password) = std::env::var("TL_DB_PASSWORD") {
        // Password that needs derivation
        let config = treeline_core::config::Config::load(&treeline_dir).unwrap_or_default();
        let db_filename = if config.demo_mode { "demo.duckdb" } else { "treeline.duckdb" };
        let db_path = treeline_dir.join(db_filename);

        let encryption_service = EncryptionService::new(treeline_dir.clone(), db_path);
        if encryption_service.is_encrypted().unwrap_or(false) {
            // Derive key from password
            Some(encryption_service.derive_key_for_connection(&password)
                .context("Failed to derive encryption key from password")?)
        } else {
            // Database not encrypted, don't need a key
            None
        }
    } else {
        None
    };

    TreelineContext::new(&treeline_dir, encryption_key.as_deref())
        .context("Failed to initialize treeline context")
}

/// Check if demo mode is enabled
pub fn is_demo_mode() -> Result<bool> {
    let treeline_dir = get_treeline_dir();
    let config = treeline_core::config::Config::load(&treeline_dir).unwrap_or_default();
    Ok(config.demo_mode)
}
