//! Sync service - synchronize accounts and transactions from integrations

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use base64::Engine;
use chrono::{Duration, NaiveDate, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::adapters::duckdb::DuckDbRepository;
use crate::adapters::demo;
use crate::adapters::simplefin::SimpleFINClient;

/// Sync service for account and transaction synchronization
pub struct SyncService {
    repository: Arc<DuckDbRepository>,
    treeline_dir: PathBuf,
}

impl SyncService {
    pub fn new(repository: Arc<DuckDbRepository>, treeline_dir: PathBuf) -> Self {
        Self { repository, treeline_dir }
    }

    /// Sync from all integrations or a specific one
    pub fn sync(&self, integration: Option<&str>, dry_run: bool) -> Result<SyncResult> {
        // Get integrations from database
        let integrations = self.repository.get_integrations()?;
        let mut results = Vec::new();

        let integrations_to_sync: Vec<_> = if let Some(name) = integration {
            integrations.iter()
                .filter(|i| i.name == name)
                .collect()
        } else {
            integrations.iter().collect()
        };

        if integrations_to_sync.is_empty() {
            anyhow::bail!("No integrations configured");
        }

        for int in integrations_to_sync {
            // Determine provider from integration name or settings
            let provider = int.name.as_str();
            let result = self.sync_integration(&int.name, provider, &int.settings, dry_run)?;
            results.push(result);
        }

        Ok(SyncResult {
            results,
            new_accounts_without_type: Vec::new(), // TODO: Track accounts without type
        })
    }

    fn sync_integration(&self, name: &str, provider: &str, settings: &serde_json::Value, dry_run: bool) -> Result<IntegrationSyncResult> {
        let now = Utc::now();
        let end_date_naive = now.naive_utc().date();

        // Calculate start date based on sync type
        // Incremental: last transaction date - 7 days overlap
        // Initial: 90 days back
        let max_tx_date = self.repository.get_max_transaction_date()?;
        let (start_date_naive, is_incremental) = match max_tx_date {
            Some(max_date) => {
                // Incremental sync: start from max transaction date minus 7 days overlap
                (max_date - Duration::days(7), true)
            }
            None => {
                // Initial sync: 90 days back
                ((now - Duration::days(90)).naive_utc().date(), false)
            }
        };

        let start_date = start_date_naive.format("%Y-%m-%d").to_string();
        let end_date = end_date_naive.format("%Y-%m-%d").to_string();

        match provider {
            "demo" => self.sync_demo(name, dry_run, &start_date, &end_date),
            "simplefin" => self.sync_simplefin(name, settings, dry_run, start_date_naive, end_date_naive, is_incremental),
            _ => Ok(IntegrationSyncResult {
                integration: name.to_string(),
                accounts_synced: 0,
                transactions_synced: 0,
                transaction_stats: TransactionStats {
                    discovered: 0,
                    new: 0,
                    skipped: 0,
                },
                sync_type: "initial".to_string(),
                start_date,
                end_date,
                provider_warnings: Vec::new(),
                error: Some(format!("Unknown provider: {}", provider)),
            }),
        }
    }

    fn sync_demo(&self, name: &str, dry_run: bool, start_date: &str, end_date: &str) -> Result<IntegrationSyncResult> {
        let accounts = demo::generate_demo_accounts();
        let transactions = demo::generate_demo_transactions();
        let snapshots = demo::generate_demo_balance_snapshots();

        let discovered = transactions.len() as i64;
        let mut new_count = 0i64;
        let mut skipped_count = 0i64;

        // Check if this is initial or incremental sync
        let existing_count = self.repository.get_transaction_count()?;
        let sync_type = if existing_count == 0 { "initial" } else { "incremental" };

        if !dry_run {
            for account in &accounts {
                self.repository.upsert_account(account)?;
            }

            // Check for existing transactions by external ID before inserting
            // This preserves user edits (tags, descriptions, etc.)
            // Use integration name as the external ID key (matches Python behavior)
            let integration_name_lower = name.to_lowercase();
            for tx in &transactions {
                // Check if this transaction already exists by external ID
                let external_id = tx.external_ids.get(&integration_name_lower);
                let exists = if let Some(ext_id) = external_id {
                    self.repository.transaction_exists_by_external_id(&integration_name_lower, ext_id)?
                } else {
                    false
                };

                if exists {
                    skipped_count += 1;
                } else {
                    self.repository.upsert_transaction(tx)?;
                    new_count += 1;
                }
            }

            for snapshot in &snapshots {
                // Ignore snapshot insert errors (duplicates)
                let _ = self.repository.add_balance_snapshot(snapshot);
            }
        } else {
            new_count = discovered;
        }

        Ok(IntegrationSyncResult {
            integration: name.to_string(),
            accounts_synced: accounts.len() as i64,
            transactions_synced: new_count,
            transaction_stats: TransactionStats {
                discovered,
                new: new_count,
                skipped: skipped_count,
            },
            sync_type: sync_type.to_string(),
            start_date: start_date.to_string(),
            end_date: end_date.to_string(),
            provider_warnings: Vec::new(),
            error: None,
        })
    }

    fn sync_simplefin(
        &self,
        name: &str,
        settings: &serde_json::Value,
        dry_run: bool,
        start_date: NaiveDate,
        end_date: NaiveDate,
        is_incremental: bool,
    ) -> Result<IntegrationSyncResult> {
        let sync_type = if is_incremental { "incremental" } else { "initial" };
        let start_date_str = start_date.format("%Y-%m-%d").to_string();
        let end_date_str = end_date.format("%Y-%m-%d").to_string();

        // Get access URL from settings
        let access_url = settings.get("accessUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("SimpleFIN accessUrl not found in settings"))?;

        // Create SimpleFIN client
        let client = SimpleFINClient::new(access_url)?;

        // Sync accounts first
        let synced_accounts = client.get_accounts()?;
        let mut provider_warnings = synced_accounts.warnings;

        // Build map of SimpleFIN ID to internal account
        let mut sf_to_internal: HashMap<String, Uuid> = HashMap::new();
        let existing_accounts = self.repository.get_accounts()?;

        for existing in &existing_accounts {
            if let Some(sf_id) = existing.external_ids.get("simplefin") {
                sf_to_internal.insert(sf_id.clone(), existing.id);
            }
        }

        let mut accounts_synced = 0i64;

        // Build a map of original account IDs to SimpleFIN IDs for balance snapshot mapping
        let mut account_id_to_sf: HashMap<Uuid, String> = HashMap::new();
        for account in &synced_accounts.accounts {
            if let Some(sf_id) = account.external_ids.get("simplefin") {
                account_id_to_sf.insert(account.id, sf_id.clone());
            }
        }

        // Process accounts - track new vs existing even in dry-run
        for mut account in synced_accounts.accounts {
            let sf_id = account.external_ids.get("simplefin").cloned().unwrap_or_default();
            if let Some(&existing_id) = sf_to_internal.get(&sf_id) {
                // Existing account - update ID but don't count as "synced"
                account.id = existing_id;
                if !dry_run {
                    self.repository.upsert_account(&account)?;
                }
            } else {
                // New account - count it and add to map
                sf_to_internal.insert(sf_id, account.id);
                accounts_synced += 1;
                if !dry_run {
                    self.repository.upsert_account(&account)?;
                }
            }
        }

        // Save balance snapshots (only if not dry-run)
        if !dry_run {
            for snapshot in synced_accounts.balance_snapshots {
                let sf_id = account_id_to_sf.get(&snapshot.account_id).cloned();

                if let Some(sf_id) = sf_id {
                    if let Some(&internal_id) = sf_to_internal.get(&sf_id) {
                        let mut updated_snapshot = snapshot;
                        updated_snapshot.account_id = internal_id;
                        // Ignore errors (duplicates)
                        let _ = self.repository.add_balance_snapshot(&updated_snapshot);
                    }
                }
            }
        }

        // Get SimpleFIN account IDs to sync transactions for
        let sf_account_ids: Vec<String> = sf_to_internal.keys().cloned().collect();

        // Sync transactions
        let synced_txs = client.get_transactions(start_date, end_date, Some(&sf_account_ids))?;
        provider_warnings.extend(synced_txs.warnings);

        let discovered = synced_txs.transactions.len() as i64;
        let mut new_count = 0i64;
        let mut skipped_count = 0i64;

        // Check for duplicates even in dry-run to give accurate counts
        for (sf_account_id, mut tx) in synced_txs.transactions {
            // Map to internal account ID
            let internal_account_id = match sf_to_internal.get(&sf_account_id) {
                Some(&id) => id,
                None => continue, // Skip transactions for unknown accounts
            };
            tx.account_id = internal_account_id;

            // Generate fingerprint for deduplication (uses Transaction's method)
            let fingerprint = tx.calculate_fingerprint();
            tx.external_ids.insert("fingerprint".to_string(), fingerprint.clone());

            // Check if transaction exists by SimpleFIN ID
            let sf_tx_id = tx.external_ids.get("simplefin").cloned();
            let exists_by_sf_id = if let Some(ref ext_id) = sf_tx_id {
                self.repository.transaction_exists_by_external_id("simplefin", ext_id)?
            } else {
                false
            };

            // Also check by fingerprint
            let exists_by_fingerprint = self.repository.transaction_exists_by_external_id("fingerprint", &fingerprint)?;

            if exists_by_sf_id || exists_by_fingerprint {
                skipped_count += 1;
            } else {
                new_count += 1;
                if !dry_run {
                    self.repository.upsert_transaction(&tx)?;
                }
            }
        }

        Ok(IntegrationSyncResult {
            integration: name.to_string(),
            accounts_synced,
            transactions_synced: new_count,
            transaction_stats: TransactionStats {
                discovered,
                new: new_count,
                skipped: skipped_count,
            },
            sync_type: sync_type.to_string(),
            start_date: start_date_str,
            end_date: end_date_str,
            provider_warnings,
            error: None,
        })
    }

    /// List configured integrations from database
    pub fn list_integrations(&self) -> Result<Vec<IntegrationInfo>> {
        let integrations = self.repository.get_integrations()?;
        let infos = integrations.iter()
            .map(|i| IntegrationInfo {
                name: i.name.clone(),
                provider: i.name.clone(), // Provider is same as name for now
            })
            .collect();
        Ok(infos)
    }

    /// Remove an integration from database
    pub fn remove_integration(&self, name: &str) -> Result<()> {
        if !self.repository.delete_integration(name)? {
            anyhow::bail!("Integration not found: {}", name);
        }
        Ok(())
    }

    /// Set up demo integration
    pub fn setup_demo(&self) -> Result<()> {
        // Add demo integration to database
        self.repository.upsert_integration("demo", &serde_json::json!({}))?;
        Ok(())
    }

    /// Set up SimpleFIN integration
    pub fn setup_simplefin(&self, setup_token: &str) -> Result<()> {
        // Claim the setup token to get access URL
        // The token is base64-encoded claim URL
        // When POSTed, SimpleFIN returns the access URL as plain text

        let decoded = base64::engine::general_purpose::STANDARD
            .decode(setup_token)
            .map_err(|_| anyhow::anyhow!("Invalid setup token format"))?;

        let claim_url = String::from_utf8(decoded)
            .map_err(|_| anyhow::anyhow!("Invalid setup token encoding"))?;

        // Make claim request
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        let response = client.post(&claim_url).send()?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to verify SimpleFIN token");
        }

        // SimpleFIN returns the access URL as plain text
        let access_url = response.text()?;

        if access_url.is_empty() {
            anyhow::bail!("No access URL received from SimpleFIN");
        }

        // Store in database (compatible with Python CLI format)
        self.repository.upsert_integration("simplefin", &serde_json::json!({
            "accessUrl": access_url
        }))?;

        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct SyncResult {
    pub results: Vec<IntegrationSyncResult>,
    /// Accounts discovered without a type (user needs to set type)
    pub new_accounts_without_type: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct IntegrationSyncResult {
    pub integration: String,
    pub accounts_synced: i64,
    /// Total transactions synced (same as new for compatibility)
    pub transactions_synced: i64,
    pub transaction_stats: TransactionStats,
    /// "initial" or "incremental"
    pub sync_type: String,
    pub start_date: String,
    pub end_date: String,
    pub provider_warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TransactionStats {
    pub discovered: i64,
    pub new: i64,
    pub skipped: i64,
}

#[derive(Debug, Serialize)]
pub struct IntegrationInfo {
    pub name: String,
    pub provider: String,
}
