//! Demo command - manage demo mode

use anyhow::Result;
use clap::Subcommand;
use colored::Colorize;

use super::{get_treeline_dir, get_context, is_demo_mode};
use treeline_core::adapters::demo::{generate_demo_accounts, generate_demo_transactions, generate_demo_balance_snapshots};

#[derive(Subcommand)]
pub enum DemoCommands {
    /// Enable demo mode
    #[command(name = "on")]
    On,
    /// Disable demo mode
    #[command(name = "off")]
    Off,
    /// Show demo mode status
    Status,
}

pub fn run(command: Option<DemoCommands>) -> Result<()> {
    match command {
        Some(DemoCommands::On) => enable_demo(),
        Some(DemoCommands::Off) => disable_demo(),
        Some(DemoCommands::Status) | None => show_status(),
    }
}

fn enable_demo() -> Result<()> {
    let treeline_dir = get_treeline_dir();
    std::fs::create_dir_all(&treeline_dir)?;

    // Delete existing demo database for a fresh start (matches Python behavior)
    let demo_db = treeline_dir.join("demo.duckdb");
    let demo_wal = treeline_dir.join("demo.duckdb.wal");
    if demo_db.exists() {
        std::fs::remove_file(&demo_db)?;
    }
    if demo_wal.exists() {
        std::fs::remove_file(&demo_wal)?;
    }

    // Load or create config and enable demo mode
    let mut config = treeline_core::config::Config::load(&treeline_dir).unwrap_or_default();
    config.enable_demo_mode();
    config.save(&treeline_dir)?;

    // Populate demo data
    let ctx = get_context()?;

    // Add demo integration to database (so sync works)
    ctx.repository.upsert_integration("demo", &serde_json::json!({}))?;

    // Add demo accounts
    for account in generate_demo_accounts() {
        ctx.repository.upsert_account(&account)?;
    }

    // Add demo transactions
    for tx in generate_demo_transactions() {
        ctx.repository.upsert_transaction(&tx)?;
    }

    // Add demo balance snapshots
    for snapshot in generate_demo_balance_snapshots() {
        ctx.repository.add_balance_snapshot(&snapshot)?;
    }

    println!("{}", "Demo mode enabled".green());
    println!("Demo data has been populated. Run 'tl status' to see your demo accounts.");

    Ok(())
}

fn disable_demo() -> Result<()> {
    let treeline_dir = get_treeline_dir();

    // Load config and disable demo mode
    let mut config = treeline_core::config::Config::load(&treeline_dir).unwrap_or_default();
    config.disable_demo_mode();
    config.save(&treeline_dir)?;

    println!("{}", "Demo mode disabled".yellow());

    Ok(())
}

fn show_status() -> Result<()> {
    let is_demo = is_demo_mode()?;

    if is_demo {
        println!("Demo mode is {}", "ON".green());
    } else {
        println!("Demo mode is {}", "OFF".yellow());
    }

    Ok(())
}
