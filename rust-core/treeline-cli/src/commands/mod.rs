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

    // Check for password in environment
    let password = std::env::var("TL_DB_PASSWORD").ok();

    TreelineContext::new(&treeline_dir, password.as_deref())
        .context("Failed to initialize treeline context")
}

/// Check if demo mode is enabled
pub fn is_demo_mode() -> Result<bool> {
    let treeline_dir = get_treeline_dir();
    let config = treeline_core::config::Config::load(&treeline_dir).unwrap_or_default();
    Ok(config.demo_mode)
}
