//! Mock Lunchflow API server for testing
//!
//! This module provides a mock HTTP server that simulates the Lunchflow API,
//! allowing for comprehensive testing without a real Lunchflow account.
//!
//! The mock server implements the same response structure as the real Lunchflow API:
//! - GET /accounts returns { accounts: [...], total: N }
//! - GET /accounts/{id}/balance returns { balance: { amount: N, currency: "..." } }
//! - GET /accounts/{id}/transactions returns { transactions: [...], total: N }

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use chrono::{Duration, Utc};
use serde::Serialize;

use super::lunchflow::LunchflowAccount;

/// Mock Lunchflow server for testing
pub struct MockLunchflowServer {
    port: u16,
    running: Arc<AtomicBool>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

/// Configuration for mock data generation
#[derive(Debug, Clone)]
pub struct MockConfig {
    /// Number of accounts to generate
    pub num_accounts: usize,
    /// Number of transactions per account
    pub num_transactions_per_account: usize,
    /// Whether to simulate authentication failure
    pub fail_auth: bool,
    /// Whether to simulate rate limiting
    pub rate_limit: bool,
    /// Delay in milliseconds before responding
    pub delay_ms: u64,
}

impl Default for MockConfig {
    fn default() -> Self {
        Self {
            num_accounts: 3,
            num_transactions_per_account: 50,
            fail_auth: false,
            rate_limit: false,
            delay_ms: 0,
        }
    }
}

// Response structures matching the real API

#[derive(Serialize)]
struct AccountsResponse {
    accounts: Vec<MockAccount>,
    total: usize,
}

#[derive(Serialize)]
struct MockAccount {
    id: i64,
    name: String,
    institution_name: String,
    institution_logo: Option<String>,
    provider: String,
    currency: String,
    status: String,
}

#[derive(Serialize)]
struct BalanceResponse {
    balance: BalanceData,
}

#[derive(Serialize)]
struct BalanceData {
    amount: f64,
    currency: String,
}

#[derive(Serialize)]
struct TransactionsResponse {
    transactions: Vec<MockTransaction>,
    total: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MockTransaction {
    id: String,
    account_id: i64,
    amount: f64,
    currency: String,
    date: String,
    merchant: Option<String>,
    description: String,
    is_pending: bool,
}

impl MockLunchflowServer {
    /// Start a new mock server on a random available port
    pub fn start(config: MockConfig) -> std::io::Result<Self> {
        Self::start_on_port(0, config)
    }

    /// Start mock server on a specific port (0 for random)
    pub fn start_on_port(port: u16, config: MockConfig) -> std::io::Result<Self> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port))?;
        let actual_port = listener.local_addr()?.port();
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        // Set listener to non-blocking for graceful shutdown
        listener.set_nonblocking(true)?;

        let thread_handle = thread::spawn(move || {
            while running_clone.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let cfg = config.clone();
                        thread::spawn(move || {
                            handle_connection(stream, &cfg);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No connection available, sleep briefly
                        thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(Self {
            port: actual_port,
            running,
            thread_handle: Some(thread_handle),
        })
    }

    /// Get the port the server is listening on
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Get the base URL for this mock server
    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Stop the mock server
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for MockLunchflowServer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn handle_connection(mut stream: TcpStream, config: &MockConfig) {
    let mut buffer = [0; 4096];

    if let Ok(n) = stream.read(&mut buffer) {
        let request = String::from_utf8_lossy(&buffer[..n]);

        // Add configured delay
        if config.delay_ms > 0 {
            thread::sleep(std::time::Duration::from_millis(config.delay_ms));
        }

        // Parse request line
        let first_line = request.lines().next().unwrap_or("");
        let parts: Vec<&str> = first_line.split_whitespace().collect();

        if parts.len() < 2 {
            send_response(&mut stream, 400, "Bad Request", r#"{"error": "Invalid request"}"#);
            return;
        }

        let method = parts[0];
        let path = parts[1];

        // Check x-api-key header (case-insensitive)
        let request_lower = request.to_lowercase();
        let has_valid_auth = request_lower.contains("x-api-key: test_")
            || request_lower.contains("x-api-key: lf_live_")
            || request_lower.contains("x-api-key: lf_test_")
            || request_lower.contains("x-api-key: mock_")
            || request_lower.contains("x-api-key: valid_");

        // Handle different scenarios
        if config.fail_auth {
            send_response(
                &mut stream,
                401,
                "Unauthorized",
                r#"{"error": "Invalid API key"}"#,
            );
            return;
        }

        if !has_valid_auth {
            send_response(
                &mut stream,
                401,
                "Unauthorized",
                r#"{"error": "Invalid API key"}"#,
            );
            return;
        }

        if config.rate_limit {
            send_response(
                &mut stream,
                429,
                "Too Many Requests",
                r#"{"error": "Rate limit exceeded"}"#,
            );
            return;
        }

        // Route requests - handle path with or without query string
        let path_without_query = path.split('?').next().unwrap_or(path);

        match method {
            "GET" => {
                if path_without_query == "/accounts" {
                    // List accounts: GET /accounts
                    let accounts = generate_mock_accounts(config.num_accounts);
                    let response = AccountsResponse {
                        total: accounts.len(),
                        accounts,
                    };
                    let json = serde_json::to_string(&response).unwrap();
                    send_response(&mut stream, 200, "OK", &json);
                } else if path_without_query.starts_with("/accounts/")
                    && path_without_query.ends_with("/balance")
                {
                    // Get balance: GET /accounts/{id}/balance
                    let account_id = extract_account_id(path_without_query);
                    let balance = generate_mock_balance(account_id);
                    let json = serde_json::to_string(&balance).unwrap();
                    send_response(&mut stream, 200, "OK", &json);
                } else if path_without_query.starts_with("/accounts/")
                    && path_without_query.contains("/transactions")
                {
                    // Get transactions: GET /accounts/{id}/transactions
                    let account_id = extract_account_id(path_without_query);
                    let txs = generate_mock_transactions(
                        account_id,
                        config.num_transactions_per_account,
                    );
                    let response = TransactionsResponse {
                        total: txs.len(),
                        transactions: txs,
                    };
                    let json = serde_json::to_string(&response).unwrap();
                    send_response(&mut stream, 200, "OK", &json);
                } else {
                    send_response(
                        &mut stream,
                        404,
                        "Not Found",
                        r#"{"error": "Endpoint not found"}"#,
                    );
                }
            }
            _ => {
                send_response(
                    &mut stream,
                    405,
                    "Method Not Allowed",
                    r#"{"error": "Method not allowed"}"#,
                );
            }
        }
    }
}

fn extract_account_id(path: &str) -> i64 {
    // Extract account ID from paths like /accounts/123/balance or /accounts/123/transactions
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() >= 3 {
        parts[2].parse().unwrap_or(1)
    } else {
        1
    }
}

fn send_response(stream: &mut TcpStream, status: u16, status_text: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        status,
        status_text,
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn generate_mock_accounts(count: usize) -> Vec<MockAccount> {
    let institutions = vec![
        ("Barclays", "GBP", "gocardless"),
        ("HSBC", "GBP", "gocardless"),
        ("Revolut", "EUR", "gocardless"),
        ("N26", "EUR", "gocardless"),
        ("Chase", "USD", "quiltt"),
        ("Bank of America", "USD", "quiltt"),
    ];

    (0..count)
        .map(|i| {
            let (inst, currency, provider) = institutions[i % institutions.len()];

            MockAccount {
                id: (i + 1) as i64,
                name: format!("{} Checking", inst),
                institution_name: inst.to_string(),
                institution_logo: Some(format!("https://logos.lunchflow.com/{}.png", inst.to_lowercase())),
                provider: provider.to_string(),
                currency: currency.to_string(),
                status: "ACTIVE".to_string(),
            }
        })
        .collect()
}

fn generate_mock_balance(account_id: i64) -> BalanceResponse {
    // Generate deterministic balance based on account ID
    let amount = 1000.0 + (account_id as f64 * 500.0);
    let currencies = vec!["GBP", "EUR", "USD"];
    let currency = currencies[(account_id as usize) % currencies.len()];

    BalanceResponse {
        balance: BalanceData {
            amount,
            currency: currency.to_string(),
        },
    }
}

fn generate_mock_transactions(account_id: i64, count: usize) -> Vec<MockTransaction> {
    let merchants = vec![
        ("Tesco", -45.23),
        ("Amazon", -29.99),
        ("Netflix", -9.99),
        ("Shell", -52.00),
        ("Costa Coffee", -4.50),
        ("Spotify", -9.99),
        ("Uber", -12.50),
        ("Apple", -199.00),
        ("SALARY", 3500.00),
        ("Interest", 2.50),
    ];

    let today = Utc::now().naive_utc().date();
    let currencies = vec!["GBP", "EUR", "USD"];
    let currency = currencies[(account_id as usize) % currencies.len()];

    (0..count)
        .map(|i| {
            let (merchant, amount) = merchants[i % merchants.len()];
            let days_ago = (i % 90) as i64;
            let date = today - Duration::days(days_ago);

            MockTransaction {
                id: format!("tx_{}_{}", account_id, i + 1),
                account_id,
                amount,
                currency: currency.to_string(),
                date: date.format("%Y-%m-%d").to_string(),
                merchant: Some(merchant.to_string()),
                description: format!("{} - Transaction #{}", merchant, i + 1),
                is_pending: i < 3, // First 3 transactions are pending
            }
        })
        .collect()
}

// Keep LunchflowAccount available for external use but we use MockAccount internally
#[allow(dead_code)]
fn _lunchflow_account_from_mock(mock: &MockAccount) -> LunchflowAccount {
    LunchflowAccount {
        id: mock.id.to_string(),
        name: mock.name.clone(),
        institution_name: mock.institution_name.clone(),
        institution_logo: mock.institution_logo.clone(),
        provider: Some(mock.provider.clone()),
        currency: Some(mock.currency.clone()),
        status: Some(mock.status.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::lunchflow::{LunchflowClient, LunchflowProvider};
    use crate::ports::DataAggregationProvider;

    #[test]
    fn test_mock_server_starts() {
        let server = MockLunchflowServer::start(MockConfig::default()).unwrap();
        assert!(server.port() > 0);
    }

    #[test]
    fn test_mock_server_accounts() {
        let server = MockLunchflowServer::start(MockConfig {
            num_accounts: 5,
            ..Default::default()
        })
        .unwrap();

        let client = LunchflowClient::new_with_base_url("test_key", &server.base_url()).unwrap();
        let result = client.get_accounts().unwrap();

        assert_eq!(result.accounts.len(), 5);
        assert!(result.accounts[0].lf_id.is_some());
    }

    #[test]
    fn test_mock_server_transactions() {
        let server = MockLunchflowServer::start(MockConfig {
            num_accounts: 1,
            num_transactions_per_account: 20,
            ..Default::default()
        })
        .unwrap();

        let client = LunchflowClient::new_with_base_url("test_key", &server.base_url()).unwrap();
        let accounts = client.get_accounts().unwrap();

        let account_ids: Vec<String> = accounts
            .accounts
            .iter()
            .filter_map(|a| a.lf_id.clone())
            .collect();

        let today = Utc::now().naive_utc().date();
        let start = today - Duration::days(90);

        let result = client
            .get_transactions(start, today, Some(&account_ids))
            .unwrap();

        assert_eq!(result.transactions.len(), 20);
    }

    #[test]
    fn test_mock_server_auth_failure() {
        let server = MockLunchflowServer::start(MockConfig {
            fail_auth: true,
            ..Default::default()
        })
        .unwrap();

        let client = LunchflowClient::new_with_base_url("test_key", &server.base_url()).unwrap();
        let result = client.get_accounts();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("authentication"));
    }

    #[test]
    fn test_mock_server_rate_limit() {
        let server = MockLunchflowServer::start(MockConfig {
            rate_limit: true,
            fail_auth: false,
            ..Default::default()
        })
        .unwrap();

        // Use a valid auth token prefix
        let client = LunchflowClient::new_with_base_url("valid_key", &server.base_url()).unwrap();
        let result = client.get_accounts();

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string().to_lowercase();
        assert!(
            err_msg.contains("rate limit"),
            "Expected 'rate limit' in error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_provider_with_mock() {
        let server = MockLunchflowServer::start(MockConfig::default()).unwrap();

        let provider = LunchflowProvider::new();
        let settings = serde_json::json!({
            "apiKey": "mock_api_key",
            "baseUrl": server.base_url()
        });

        let result = provider.get_accounts(&settings).unwrap();
        assert_eq!(result.accounts.len(), 3);
        assert!(!result.balance_snapshots.is_empty());
    }
}
