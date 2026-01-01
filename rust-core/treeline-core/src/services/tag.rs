//! Tag service - transaction tagging

use std::sync::Arc;

use anyhow::Result;
use serde::Serialize;
use uuid::Uuid;

use crate::adapters::duckdb::DuckDbRepository;

/// Tag service for transaction tagging
pub struct TagService {
    repository: Arc<DuckDbRepository>,
}

impl TagService {
    pub fn new(repository: Arc<DuckDbRepository>) -> Self {
        Self { repository }
    }

    /// Apply tags to transactions
    pub fn apply_tags(&self, tx_ids: &[String], tags: &[String], replace: bool) -> Result<TagResult> {
        let mut results = Vec::new();
        let mut succeeded = 0i64;
        let mut failed = 0i64;

        for tx_id in tx_ids {
            match self.apply_tags_to_transaction(tx_id, tags, replace) {
                Ok(applied_tags) => {
                    succeeded += 1;
                    results.push(TagResultEntry {
                        transaction_id: tx_id.clone(),
                        tags: Some(applied_tags),
                        success: true,
                        error: None,
                    });
                }
                Err(e) => {
                    failed += 1;
                    results.push(TagResultEntry {
                        transaction_id: tx_id.clone(),
                        tags: None,
                        success: false,
                        error: Some(e.to_string()),
                    });
                }
            }
        }

        Ok(TagResult {
            succeeded,
            failed,
            results,
        })
    }

    fn apply_tags_to_transaction(&self, tx_id: &str, new_tags: &[String], replace: bool) -> Result<Vec<String>> {
        // Validate UUID format upfront (matches Python behavior)
        if Uuid::parse_str(tx_id).is_err() {
            anyhow::bail!("Invalid UUID: {}", tx_id);
        }

        let final_tags = if replace {
            new_tags.to_vec()
        } else {
            // Get existing tags and merge
            if let Some(tx) = self.repository.get_transaction_by_id(tx_id)? {
                let mut tags = tx.tags;
                for tag in new_tags {
                    if !tags.contains(tag) {
                        tags.push(tag.clone());
                    }
                }
                tags
            } else {
                anyhow::bail!("Transaction not found");
            }
        };

        self.repository.update_transaction_tags(tx_id, &final_tags)?;
        Ok(final_tags)
    }
}

/// Result structure matching Python CLI output
#[derive(Debug, Serialize)]
pub struct TagResult {
    pub succeeded: i64,
    pub failed: i64,
    pub results: Vec<TagResultEntry>,
}

/// Individual transaction result entry
#[derive(Debug, Serialize)]
pub struct TagResultEntry {
    pub transaction_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
