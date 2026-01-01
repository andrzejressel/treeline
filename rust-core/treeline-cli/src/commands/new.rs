//! New command - create new records

use anyhow::Result;
use clap::Subcommand;
use colored::Colorize;
use dialoguer::Input;
use rust_decimal::Decimal;
use chrono::NaiveDate;

use super::get_context;

#[derive(Subcommand)]
pub enum NewCommands {
    /// Add a manual balance snapshot
    Balance {
        /// Account ID
        #[arg(long)]
        account_id: Option<String>,
        /// Balance amount
        #[arg(long)]
        balance: Option<String>,
        /// Date (YYYY-MM-DD)
        #[arg(long)]
        date: Option<String>,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
}

pub fn run(command: NewCommands) -> Result<()> {
    match command {
        NewCommands::Balance { account_id, balance, date, json } => {
            run_balance(account_id, balance, date, json)
        }
    }
}

fn run_balance(
    account_id: Option<String>,
    balance: Option<String>,
    date: Option<String>,
    json: bool,
) -> Result<()> {
    let ctx = get_context()?;

    // Get account ID interactively if not provided
    let account = match account_id {
        Some(id) => id,
        None => Input::new()
            .with_prompt("Account ID")
            .interact_text()?,
    };

    // Get balance interactively if not provided
    let balance_str = match balance {
        Some(b) => b,
        None => Input::new()
            .with_prompt("Balance")
            .interact_text()?,
    };

    let balance_decimal: Decimal = balance_str.parse()
        .map_err(|_| anyhow::anyhow!("Invalid balance amount"))?;

    // Parse date if provided
    let date_parsed = if let Some(d) = date {
        Some(NaiveDate::parse_from_str(&d, "%Y-%m-%d")
            .map_err(|_| anyhow::anyhow!("Invalid date format. Use YYYY-MM-DD"))?)
    } else {
        None
    };

    let result = ctx.balance_service.add_balance(&account, balance_decimal, date_parsed)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("{}", "Balance snapshot created".green());
        println!("  Snapshot ID: {}", result.snapshot_id);
        println!("  Account: {}", result.account_id);
        println!("  Balance: ${}", result.balance);
        println!("  Time: {}", result.snapshot_time);
    }

    Ok(())
}
