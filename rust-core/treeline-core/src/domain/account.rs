//! Account domain model

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A financial account owned by the user
/// Note: account_type is a freeform string to match Python CLI behavior.
/// Common values include "checking", "savings", "credit", "investment", "loan"
/// but any string is accepted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: Uuid,
    pub name: String,
    pub nickname: Option<String>,
    pub account_type: Option<String>,
    /// ISO 4217 currency code, normalized to uppercase
    pub currency: String,
    /// External system identifiers (e.g., SimpleFIN ID)
    pub external_ids: HashMap<String, String>,
    pub balance: Option<Decimal>,
    pub institution_name: Option<String>,
    pub institution_url: Option<String>,
    pub institution_domain: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Account {
    /// Create a new account with required fields
    pub fn new(id: Uuid, name: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id,
            name: name.into(),
            nickname: None,
            account_type: None,
            currency: "USD".to_string(),
            external_ids: HashMap::new(),
            balance: None,
            institution_name: None,
            institution_url: None,
            institution_domain: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Normalize currency code to uppercase
    pub fn normalize_currency(currency: &str) -> String {
        currency.trim().to_uppercase()
    }

    /// Validate account data
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.name.trim().is_empty() {
            return Err("account name cannot be empty");
        }
        if self.currency.trim().is_empty() {
            return Err("currency cannot be empty");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_currency_normalization() {
        assert_eq!(Account::normalize_currency("usd"), "USD");
        assert_eq!(Account::normalize_currency(" eur "), "EUR");
    }

    #[test]
    fn test_account_validation() {
        let mut account = Account::new(Uuid::new_v4(), "Test Account");
        assert!(account.validate().is_ok());

        account.name = "".to_string();
        assert!(account.validate().is_err());
    }
}
