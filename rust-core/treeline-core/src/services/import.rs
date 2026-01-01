//! Import service - CSV transaction import

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::NaiveDate;
use regex::Regex;
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Sha256, Digest};
use uuid::Uuid;

use crate::adapters::duckdb::DuckDbRepository;
use crate::config::{ColumnMappings, Config, ImportProfile, ImportOptions as ConfigImportOptions};
use crate::domain::Transaction;

/// Import options for CSV processing
#[derive(Debug, Default)]
pub struct ImportOptions {
    /// Negate debit values when debits are positive in CSV
    pub debit_negative: bool,
    /// Flip signs on all amounts (for credit card statements)
    pub flip_signs: bool,
}

/// Import service for CSV imports
pub struct ImportService {
    repository: Arc<DuckDbRepository>,
    treeline_dir: PathBuf,
}

impl ImportService {
    pub fn new(repository: Arc<DuckDbRepository>, treeline_dir: PathBuf) -> Self {
        Self { repository, treeline_dir }
    }

    /// List saved import profiles
    pub fn list_profiles(&self) -> Result<std::collections::HashMap<String, ImportProfile>> {
        let config = Config::load(&self.treeline_dir)?;
        Ok(config.import_profiles)
    }

    /// Import transactions from CSV
    pub fn import(
        &self,
        file_path: &Path,
        account_id: &str,
        mappings: &ColumnMappings,
        options: &ImportOptions,
        preview_only: bool,
    ) -> Result<ImportResult> {
        // Verify account exists
        if self.repository.get_account_by_id(account_id)?.is_none() {
            anyhow::bail!("Account not found: {}", account_id);
        }

        let account_uuid = Uuid::parse_str(account_id)
            .context("Invalid account ID")?;

        // Read CSV
        let mut reader = csv::Reader::from_path(file_path)
            .context("Failed to read CSV file")?;

        let headers = reader.headers()?.clone();

        // Find column indices
        let date_idx = headers.iter().position(|h| h == mappings.date)
            .context(format!("Date column '{}' not found", mappings.date))?;

        // Check for debit/credit columns first, fall back to amount
        let debit_idx = mappings.debit.as_ref()
            .and_then(|d| headers.iter().position(|h| h == d));
        let credit_idx = mappings.credit.as_ref()
            .and_then(|c| headers.iter().position(|h| h == c));

        let amount_idx = if debit_idx.is_some() || credit_idx.is_some() {
            None
        } else {
            Some(headers.iter().position(|h| h == mappings.amount)
                .context(format!("Amount column '{}' not found", mappings.amount))?)
        };

        let desc_idx = mappings.description.as_ref()
            .and_then(|d| headers.iter().position(|h| h == d));

        let mut transactions = Vec::new();
        let mut skipped = 0;

        for result in reader.records() {
            let record = result?;

            // Parse date
            let date_str = record.get(date_idx).unwrap_or("");
            let date = parse_date(date_str);
            if date.is_none() {
                skipped += 1;
                continue;
            }
            let date = date.unwrap();

            // Parse amount from either amount column or debit/credit columns
            let amount = if let Some(amt_idx) = amount_idx {
                let amount_str = record.get(amt_idx).unwrap_or("");
                parse_amount(amount_str)
            } else {
                // Handle debit/credit columns
                // Preserve sign from CSV, only negate if debit_negative option is set
                let debit = debit_idx
                    .and_then(|i| record.get(i))
                    .and_then(|s| if s.is_empty() { None } else { parse_amount(s) });
                let credit = credit_idx
                    .and_then(|i| record.get(i))
                    .and_then(|s| if s.is_empty() { None } else { parse_amount(s) });

                match (debit, credit) {
                    (Some(d), None) => {
                        // Debit: preserve sign from CSV by default
                        // If debit_negative is true, negate positive values (for unsigned CSVs)
                        let d = if options.debit_negative && d > Decimal::ZERO { -d } else { d };
                        Some(d)
                    }
                    (None, Some(c)) => {
                        // Credit: incoming money (preserve sign)
                        Some(c)
                    }
                    (Some(d), Some(c)) => {
                        // Both present: use the one with larger absolute value
                        if d.abs() >= c.abs() {
                            let d = if options.debit_negative && d > Decimal::ZERO { -d } else { d };
                            Some(d)
                        } else {
                            Some(c)
                        }
                    }
                    (None, None) => None,
                }
            };

            if amount.is_none() {
                skipped += 1;
                continue;
            }

            let mut amount = amount.unwrap();

            // Apply flip_signs if requested (for credit card statements)
            if options.flip_signs {
                amount = -amount;
            }

            // Get description
            let description = desc_idx
                .and_then(|i| record.get(i))
                .map(|s| s.to_string());

            // Generate fingerprint for deduplication
            let fingerprint = generate_fingerprint(account_id, &date, &amount, description.as_deref());

            let mut tx = Transaction::new(Uuid::new_v4(), account_uuid, amount, date);
            tx.description = description;
            tx.external_ids.insert("fingerprint".to_string(), fingerprint);

            transactions.push(tx);
        }

        // Track discovered count (valid transactions before deduplication)
        let discovered = transactions.len() as i64;
        let fingerprints_checked = discovered;

        // Generate batch ID for this import
        let batch_id = format!("import_{}", chrono::Utc::now().format("%Y%m%d_%H%M%S"));

        // Deduplicate: check which fingerprints already exist
        let mut new_transactions = Vec::new();
        let mut duplicate_count = 0i64;

        for tx in transactions {
            if let Some(fingerprint) = tx.external_ids.get("fingerprint") {
                if self.repository.transaction_exists_by_external_id("fingerprint", fingerprint)? {
                    duplicate_count += 1;
                    continue;
                }
            }
            new_transactions.push(tx);
        }

        let imported = new_transactions.len() as i64;

        if !preview_only {
            for tx in &new_transactions {
                self.repository.upsert_transaction(tx)?;
            }
        }

        Ok(ImportResult {
            batch_id,
            discovered,
            imported,
            skipped: skipped + duplicate_count,
            fingerprints_checked,
            preview: preview_only,
            transactions: if preview_only {
                Some(new_transactions.iter().map(|t| TransactionPreview {
                    date: t.transaction_date.to_string(),
                    amount: t.amount.to_string(),
                    description: t.description.clone(),
                }).collect())
            } else {
                None
            },
        })
    }

    /// Save an import profile
    pub fn save_profile(&self, name: &str, mappings: &ColumnMappings, options: &ImportOptions) -> Result<()> {
        let mut config = Config::load(&self.treeline_dir)?;
        config.import_profiles.insert(name.to_string(), ImportProfile {
            column_mappings: mappings.clone(),
            date_format: None,
            skip_rows: 0,
            options: ConfigImportOptions {
                flip_signs: options.flip_signs,
                debit_negative: options.debit_negative,
            },
        });
        config.save(&self.treeline_dir)?;
        Ok(())
    }

    /// Get a saved profile
    pub fn get_profile(&self, name: &str) -> Result<Option<ImportProfile>> {
        let config = Config::load(&self.treeline_dir)?;
        Ok(config.import_profiles.get(name).cloned())
    }

    /// Auto-detect column mapping from CSV headers
    ///
    /// Returns best-guess mapping for date, amount, description, and optionally debit/credit columns.
    /// Matches Python CLI behavior with same pattern matching.
    pub fn detect_columns(&self, file_path: &Path) -> Result<DetectedColumns> {
        let mut reader = csv::Reader::from_path(file_path)
            .context("Failed to read CSV file")?;

        let headers: Vec<String> = reader.headers()?
            .iter()
            .map(|h| h.to_string())
            .collect();

        let date_patterns = ["date", "transaction date", "trans date", "txn date", "txndate", "posted", "post date", "dt"];
        let desc_patterns = ["description", "desc", "memo", "payee", "merchant", "details", "narration"];
        let amount_patterns = ["amount", "amt", "total", "transaction amount"];
        let debit_patterns = ["debit", "dr", "withdrawal", "debit amount"];
        let credit_patterns = ["credit", "cr", "deposit", "credit amount"];

        let mut detected = DetectedColumns::default();

        // Find date column
        for header in &headers {
            let header_lower = header.to_lowercase();
            if date_patterns.iter().any(|p| header_lower.contains(p)) {
                detected.date = Some(header.clone());
                break;
            }
        }

        // Find amount column (prefer single amount column)
        for header in &headers {
            let header_lower = header.to_lowercase();
            if amount_patterns.iter().any(|p| header_lower.contains(p)) {
                detected.amount = Some(header.clone());
                break;
            }
        }

        // If no 'amount' found, check for debit/credit
        if detected.amount.is_none() {
            for header in &headers {
                let header_lower = header.to_lowercase();
                if debit_patterns.iter().any(|p| header_lower.contains(p)) {
                    detected.debit = Some(header.clone());
                }
                if credit_patterns.iter().any(|p| header_lower.contains(p)) {
                    detected.credit = Some(header.clone());
                }
            }
        }

        // Find description column
        for header in &headers {
            let header_lower = header.to_lowercase();
            // Skip if this is the date column
            if detected.date.as_ref() == Some(header) {
                continue;
            }
            if desc_patterns.iter().any(|p| header_lower.contains(p)) {
                detected.description = Some(header.clone());
                break;
            }
        }

        // Fallback for description
        if detected.description.is_none() {
            let fallback_patterns = ["name", "type", "ref", "reference", "category"];
            for header in &headers {
                let header_lower = header.to_lowercase();
                if detected.date.as_ref() == Some(header) {
                    continue;
                }
                if fallback_patterns.iter().any(|p| header_lower.contains(p)) {
                    detected.description = Some(header.clone());
                    break;
                }
            }
        }

        Ok(detected)
    }
}

fn parse_date(s: &str) -> Option<NaiveDate> {
    // Try common formats
    let formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%m-%d-%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
    ];

    for fmt in &formats {
        if let Ok(date) = NaiveDate::parse_from_str(s, fmt) {
            return Some(date);
        }
    }
    None
}

fn parse_amount(s: &str) -> Option<Decimal> {
    let s = s.trim();

    // Handle parentheses notation for negative numbers: (100.00) -> -100.00
    let (is_negative, s) = if s.starts_with('(') && s.ends_with(')') {
        (true, &s[1..s.len()-1])
    } else {
        (false, s)
    };

    // Remove currency symbols, commas, whitespace
    let cleaned: String = s.chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
        .collect();

    let mut amount: Decimal = cleaned.parse().ok()?;

    // Apply parentheses negation
    if is_negative && amount > Decimal::ZERO {
        amount = -amount;
    }

    Some(amount)
}

/// Generate a fingerprint for transaction deduplication
/// Based on account_id, date, amount, and normalized description
fn generate_fingerprint(account_id: &str, date: &NaiveDate, amount: &Decimal, description: Option<&str>) -> String {
    let normalized_desc = description
        .map(|d| normalize_description(d))
        .unwrap_or_default();

    let fingerprint_input = format!(
        "{}|{}|{:.2}|{}",
        account_id,
        date,
        amount,
        normalized_desc
    );

    let mut hasher = Sha256::new();
    hasher.update(fingerprint_input.as_bytes());
    let result = hasher.finalize();

    // Return first 16 characters of hex hash
    result[..8].iter().map(|b| format!("{:02x}", b)).collect()
}

/// Normalize description for fingerprinting
/// Matches Python implementation exactly:
/// - Lowercase
/// - Remove literal "null" strings (CSV exports)
/// - Remove card number masks (10+ X's followed by 4 digits)
/// - Normalize account/phone numbers to last 4 digits
/// - Remove whitespace and special characters
fn normalize_description(desc: &str) -> String {
    let desc = desc.to_lowercase();

    // Remove literal "null" strings (common in CSV exports)
    let null_re = Regex::new(r"\bnull\b").unwrap();
    let mut normalized = null_re.replace_all(&desc, "").to_string();

    // Remove card number masks (10+ X's followed by 4 digits)
    let card_mask_re = Regex::new(r"x{10,}\d{4}").unwrap();
    normalized = card_mask_re.replace_all(&normalized, "").to_string();

    // Normalize phone/account numbers (7-12 chars of X's and digits)
    // Keep only last 4 digits
    let account_re = Regex::new(r"[x0-9]{7,12}").unwrap();
    normalized = account_re
        .replace_all(&normalized, |caps: &regex::Captures| {
            let text = caps.get(0).unwrap().as_str();
            let digits: String = text.chars().filter(|c| c.is_ascii_digit()).collect();
            if digits.len() >= 4 {
                digits[digits.len() - 4..].to_string()
            } else {
                text.to_string()
            }
        })
        .to_string();

    // Remove whitespace
    let whitespace_re = Regex::new(r"\s+").unwrap();
    normalized = whitespace_re.replace_all(&normalized, "").to_string();

    // Remove all special characters, keep only alphanumeric
    let special_re = Regex::new(r"[^a-z0-9]").unwrap();
    special_re.replace_all(&normalized, "").to_string()
}

/// Result of column auto-detection
#[derive(Debug, Default, Serialize)]
pub struct DetectedColumns {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub debit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credit: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ImportResult {
    /// Unique batch ID for this import
    pub batch_id: String,
    /// Total transactions discovered in CSV
    pub discovered: i64,
    /// Successfully imported transactions
    pub imported: i64,
    /// Skipped transactions (invalid or duplicate)
    pub skipped: i64,
    /// Number of fingerprints checked for deduplication
    pub fingerprints_checked: i64,
    /// Whether this was a preview (no changes applied)
    pub preview: bool,
    /// Transaction previews (only in preview mode)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions: Option<Vec<TransactionPreview>>,
}

#[derive(Debug, Serialize)]
pub struct TransactionPreview {
    pub date: String,
    pub amount: String,
    pub description: Option<String>,
}
