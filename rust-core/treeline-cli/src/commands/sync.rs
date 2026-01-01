//! Sync command - sync accounts and transactions from integrations

use anyhow::Result;
use colored::Colorize;

use super::get_context;

pub fn run(integration: Option<String>, dry_run: bool, json: bool) -> Result<()> {
    let ctx = get_context()?;
    let result = ctx.sync_service.sync(integration.as_deref(), dry_run)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }

    if dry_run {
        println!("{}", "DRY RUN - No changes applied".yellow());
        println!();
    }

    for sync_result in &result.results {
        if let Some(error) = &sync_result.error {
            println!("{} {} - {}", "Error:".red(), sync_result.integration, error);
        } else {
            println!("{} {}", "Synced:".green(), sync_result.integration);
            println!("  Accounts synced: {}", sync_result.accounts_synced);
            if sync_result.sync_type == "incremental" {
                println!("  Syncing transactions since {} (with 7-day overlap)", sync_result.start_date);
            } else {
                println!("  Date range: {} to {}", sync_result.start_date, sync_result.end_date);
            }
            println!("  Transaction breakdown:");
            println!("    Discovered: {}", sync_result.transaction_stats.discovered);
            println!("    New: {}", sync_result.transaction_stats.new);
            println!("    Skipped: {} (already exists)", sync_result.transaction_stats.skipped);
        }
        println!();
    }

    if result.results.is_empty() {
        println!("{}", "No integrations configured. Use 'tl setup' to add one.".yellow());
    }

    Ok(())
}
