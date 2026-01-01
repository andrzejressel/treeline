//! Balance service - balance snapshot management

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use uuid::Uuid;

use crate::adapters::duckdb::DuckDbRepository;
use crate::domain::BalanceSnapshot;

/// Balance service for balance snapshot management
pub struct BalanceService {
    repository: Arc<DuckDbRepository>,
}

impl BalanceService {
    pub fn new(repository: Arc<DuckDbRepository>) -> Self {
        Self { repository }
    }

    /// Add a manual balance snapshot
    pub fn add_balance(
        &self,
        account_id: &str,
        balance: Decimal,
        date: Option<NaiveDate>,
    ) -> Result<BalanceResult> {
        // Verify account exists
        if self.repository.get_account_by_id(account_id)?.is_none() {
            anyhow::bail!("Account not found: {}", account_id);
        }

        let account_uuid = Uuid::parse_str(account_id)?;

        // Use midnight for the snapshot time (matches Python behavior)
        let snapshot_time = date
            .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
            .unwrap_or_else(|| Utc::now().naive_utc());

        // Check for duplicate balance snapshot (same account + date + balance)
        // Python allows multiple snapshots per day if the balance is different
        let snapshot_date = snapshot_time.date();
        let existing_snapshots = self.repository.get_balance_snapshots(Some(account_id))?;
        let has_same_balance = existing_snapshots.iter().any(|s| {
            s.snapshot_time.date() == snapshot_date &&
            (s.balance - balance).abs() < Decimal::new(1, 2) // Within 0.01
        });
        if has_same_balance {
            anyhow::bail!(
                "Balance snapshot already exists for {} with same balance",
                snapshot_date
            );
        }

        let snapshot = BalanceSnapshot {
            id: Uuid::new_v4(),
            account_id: account_uuid,
            balance,
            snapshot_time,
            source: Some("manual".to_string()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        self.repository.add_balance_snapshot(&snapshot)?;

        Ok(BalanceResult {
            snapshot_id: snapshot.id.to_string(),
            account_id: account_id.to_string(),
            balance: balance.to_string(),
            snapshot_time: snapshot_time.to_string(),
        })
    }

    /// Backfill balance snapshots from transaction history
    ///
    /// Walks backward from the latest balance snapshot using transaction history.
    /// Only creates snapshots for dates that don't already have one.
    pub fn backfill(
        &self,
        account_ids: Option<Vec<String>>,
        days: Option<i64>,
        dry_run: bool,
        verbose: bool,
    ) -> Result<BackfillResult> {
        let mut result = BackfillResult {
            accounts_processed: 0,
            snapshots_created: 0,
            snapshots_skipped: 0,
            warnings: Vec::new(),
            verbose_logs: Vec::new(),
            dry_run,
        };

        // Get accounts to process
        let all_accounts = self.repository.get_accounts()?;
        let accounts: Vec<_> = if let Some(ref ids) = account_ids {
            all_accounts
                .into_iter()
                .filter(|a| ids.contains(&a.id.to_string()))
                .collect()
        } else {
            all_accounts
        };

        if accounts.is_empty() {
            result.warnings.push("No accounts found to process".to_string());
            return Ok(result);
        }

        for account in accounts {
            result.accounts_processed += 1;
            let account_id_str = account.id.to_string();

            if verbose {
                result.verbose_logs.push(format!(
                    "Processing account: {} ({})",
                    account.name, account.id
                ));
            }

            // Get existing balance snapshots for this account
            let existing_snapshots = self.repository.get_balance_snapshots(Some(&account_id_str))?;

            if existing_snapshots.is_empty() {
                result.warnings.push(format!(
                    "Account {}: No balance snapshots found - cannot backfill without starting point",
                    account.name
                ));
                continue;
            }

            // Find latest snapshot as starting point
            let latest_snapshot = existing_snapshots
                .iter()
                .max_by_key(|s| s.snapshot_time)
                .unwrap();
            let starting_balance = latest_snapshot.balance;
            let starting_date = latest_snapshot.snapshot_time.date();

            if verbose {
                result.verbose_logs.push(format!(
                    "  Starting from {} on {}",
                    starting_balance, starting_date
                ));
            }

            // Build set of dates that already have snapshots
            let mut existing_dates: HashSet<NaiveDate> = existing_snapshots
                .iter()
                .map(|s| s.snapshot_time.date())
                .collect();

            // Get transactions for this account ordered by date DESC
            let transactions = self.repository.get_transactions_by_account(&account_id_str)?;

            // Walk backward through transactions
            let mut current_balance = starting_balance;
            let mut snapshots_to_create: Vec<BalanceSnapshot> = Vec::new();

            for transaction in transactions {
                let tx_date = transaction.transaction_date;

                // Skip if beyond days limit
                if let Some(limit_days) = days {
                    let days_ago = (starting_date - tx_date).num_days();
                    if days_ago > limit_days {
                        break;
                    }
                }

                // Skip if this date already has a snapshot (preserve real data)
                if existing_dates.contains(&tx_date) {
                    result.snapshots_skipped += 1;
                    if verbose {
                        result.verbose_logs.push(format!(
                            "  Skipped {} (already has snapshot)",
                            tx_date
                        ));
                    }
                    continue;
                }

                // Calculate balance before this transaction
                // If debit (negative), balance was higher before
                // If credit (positive), balance was lower before
                let balance_before = current_balance - transaction.amount;

                // Create snapshot for this date (end of day with microsecond precision like Python)
                let end_of_day = NaiveDateTime::new(
                    tx_date,
                    NaiveTime::from_hms_micro_opt(23, 59, 59, 999999).unwrap(),
                );

                let snapshot = BalanceSnapshot {
                    id: Uuid::new_v4(),
                    account_id: account.id,
                    balance: balance_before,
                    snapshot_time: end_of_day,
                    source: Some("backfill".to_string()),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                };

                snapshots_to_create.push(snapshot);
                existing_dates.insert(tx_date); // Mark as processed

                if verbose {
                    result.verbose_logs.push(format!(
                        "  {} = {} (tx: {})",
                        tx_date, balance_before, transaction.amount
                    ));
                }

                current_balance = balance_before;
            }

            // Insert snapshots (unless dry-run)
            if !snapshots_to_create.is_empty() {
                if !dry_run {
                    for snapshot in &snapshots_to_create {
                        self.repository.add_balance_snapshot(snapshot)?;
                    }
                }

                result.snapshots_created += snapshots_to_create.len() as i64;

                if verbose {
                    result.verbose_logs.push(format!(
                        "  Account {}: Created {} snapshots",
                        account.name,
                        snapshots_to_create.len()
                    ));
                }
            }
        }

        // Add summary warning
        if !result.warnings.is_empty() {
            result.warnings.insert(
                0,
                "WARNING: Balance backfill produces estimates. If transactions are missing, balances may be inaccurate.".to_string(),
            );
        }

        Ok(result)
    }
}

#[derive(Debug, Serialize)]
pub struct BalanceResult {
    pub snapshot_id: String,
    pub account_id: String,
    pub balance: String,
    pub snapshot_time: String,
}

#[derive(Debug, Serialize)]
pub struct BackfillResult {
    pub accounts_processed: i64,
    pub snapshots_created: i64,
    pub snapshots_skipped: i64,
    pub warnings: Vec<String>,
    pub verbose_logs: Vec<String>,
    pub dry_run: bool,
}
