//! Transaction domain model

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use regex::Regex;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// A single financial transaction belonging to an account
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: Uuid,
    pub account_id: Uuid,
    /// External system identifiers (includes "fingerprint" for deduplication)
    pub external_ids: HashMap<String, String>,
    /// COMMENT: since we are porting, should we reconsider amount storage
    /// in the database? Should we store as integer? Please recommend what you think.
    pub amount: Decimal,
    pub description: Option<String>,
    pub transaction_date: NaiveDate,
    pub posted_date: NaiveDate,
    /// Tags for categorization
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Soft delete timestamp
    pub deleted_at: Option<DateTime<Utc>>,
    /// Parent transaction ID for splits
    pub parent_transaction_id: Option<Uuid>,
}

impl Transaction {
    /// Create a new transaction with required fields
    pub fn new(
        id: Uuid,
        account_id: Uuid,
        amount: Decimal,
        transaction_date: NaiveDate,
    ) -> Self {
        let now = Utc::now();
        let mut tx = Self {
            id,
            account_id,
            external_ids: HashMap::new(),
            amount,
            description: None,
            transaction_date,
            posted_date: transaction_date,
            tags: Vec::new(),
            created_at: now,
            updated_at: now,
            deleted_at: None,
            parent_transaction_id: None,
        };
        // Generate fingerprint on creation
        tx.ensure_fingerprint();
        tx
    }

    /// Ensure fingerprint exists in external_ids
    pub fn ensure_fingerprint(&mut self) {
        if !self.external_ids.contains_key("fingerprint") {
            let fingerprint = self.calculate_fingerprint();
            self.external_ids.insert("fingerprint".to_string(), fingerprint);
        }
    }

    /// Get the fingerprint if present
    pub fn fingerprint(&self) -> Option<&str> {
        self.external_ids.get("fingerprint").map(|s| s.as_str())
    }

    /// Calculate fingerprint hash for deduplication
    ///
    /// Uses: account_id, transaction_date, amount (with sign), and normalized description.
    ///
    /// Description normalization handles CSV vs SimpleFIN format differences:
    /// - Removes literal "null" strings (CSV exports)
    /// - Removes card number masks (XXXXXXXXXXXX1234 - CSV only)
    /// - Normalizes account/phone numbers to last 4 digits
    /// - Removes whitespace and special characters
    pub fn calculate_fingerprint(&self) -> String {
        let tx_date = self.transaction_date.format("%Y-%m-%d").to_string();

        // Normalize amount: treat -0 as 0
        let amount = if self.amount == Decimal::ZERO {
            Decimal::ZERO.abs()
        } else {
            self.amount
        };
        let amount_normalized = format!("{:.2}", amount);

        // Normalize description
        let desc_normalized = Self::normalize_description(self.description.as_deref());

        let fingerprint_str = format!(
            "{}|{}|{}|{}",
            self.account_id, tx_date, amount_normalized, desc_normalized
        );

        // SHA256 hash, truncated to 16 chars
        let mut hasher = Sha256::new();
        hasher.update(fingerprint_str.as_bytes());
        let result = hasher.finalize();
        hex::encode(&result[..8]) // 16 hex chars
    }

    /// Normalize description for fingerprint comparison
    fn normalize_description(desc: Option<&str>) -> String {
        let desc = desc.unwrap_or("").to_lowercase();

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

    /// Normalize tags: deduplicate, trim whitespace, remove empty
    pub fn normalize_tags(tags: &[String]) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();

        for tag in tags {
            let trimmed = tag.trim().to_string();
            if !trimmed.is_empty() && seen.insert(trimmed.clone()) {
                result.push(trimmed);
            }
        }

        result
    }
}

// Need hex encoding for fingerprint
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_generation() {
        let account_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let mut tx = Transaction::new(
            Uuid::new_v4(),
            account_id,
            Decimal::new(-5000, 2), // -50.00
            NaiveDate::from_ymd_opt(2025, 1, 15).unwrap(),
        );
        tx.description = Some("ACME STORE".to_string());

        let fp = tx.calculate_fingerprint();
        assert_eq!(fp.len(), 16);
    }

    #[test]
    fn test_description_normalization() {
        // Card mask removal
        assert!(!Transaction::normalize_description(Some("PURCHASE XXXXXXXXXXXX1234 STORE"))
            .contains("xxxx"));

        // Null removal
        assert!(!Transaction::normalize_description(Some("null PAYMENT null")).contains("null"));

        // Account number normalization
        let normalized = Transaction::normalize_description(Some("PAYMENT 7208987070"));
        assert!(normalized.contains("7070"));
    }

    #[test]
    fn test_tag_normalization() {
        let tags = vec![
            "food".to_string(),
            "  groceries ".to_string(),
            "food".to_string(), // duplicate
            "".to_string(),     // empty
        ];
        let normalized = Transaction::normalize_tags(&tags);
        assert_eq!(normalized, vec!["food", "groceries"]);
    }
}
