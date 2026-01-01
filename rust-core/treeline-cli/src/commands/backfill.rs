//! Backfill command - backfill historical data

use anyhow::Result;
use clap::Subcommand;
use colored::Colorize;

use super::get_context;

#[derive(Subcommand)]
pub enum BackfillCommands {
    /// Backfill balance snapshots
    Balances {
        /// Account ID to backfill (can specify multiple)
        #[arg(long)]
        account_id: Vec<String>,
        /// Limit to last N days of history
        #[arg(long)]
        days: Option<i64>,
        /// Preview changes without saving
        #[arg(long)]
        dry_run: bool,
        /// Show detailed output
        #[arg(long, short)]
        verbose: bool,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
}

pub fn run(command: BackfillCommands) -> Result<()> {
    match command {
        BackfillCommands::Balances { account_id, days, dry_run, verbose, json } => {
            run_backfill_balances(account_id, days, dry_run, verbose, json)
        }
    }
}

fn run_backfill_balances(
    account_ids: Vec<String>,
    days: Option<i64>,
    dry_run: bool,
    verbose: bool,
    json: bool,
) -> Result<()> {
    let ctx = get_context()?;

    // Show dry-run indicator
    if dry_run && !json {
        println!("{}\n", "DRY RUN - No changes will be saved".yellow());
    }

    let result = ctx.balance_service.backfill(
        if account_ids.is_empty() { None } else { Some(account_ids) },
        days,
        dry_run,
        verbose,
    )?;

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }

    // Display warnings
    if !result.warnings.is_empty() {
        println!("\n{}", "Warnings".yellow());
        for warning in &result.warnings {
            println!("  {}", warning);
        }
    }

    // Display verbose logs
    if verbose && !result.verbose_logs.is_empty() {
        println!("\n{}", "Detailed Logs".bold());
        for log in &result.verbose_logs {
            println!("{}", log.dimmed());
        }
    }

    // Display summary
    println!("\n{} Backfill complete", "✓".green());
    println!("  Accounts processed: {}", result.accounts_processed);
    println!("  Snapshots created: {}", result.snapshots_created);
    println!("  Snapshots skipped: {}", result.snapshots_skipped);

    if dry_run {
        println!("\n{}", "DRY RUN - No changes were saved".yellow());
    }

    Ok(())
}
