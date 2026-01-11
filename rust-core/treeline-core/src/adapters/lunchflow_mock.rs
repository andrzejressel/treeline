//! Mock Lunchflow API server for testing
//!
//! This module provides a mock HTTP server that simulates the Lunchflow API,
//! allowing for comprehensive testing without a real Lunchflow account.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use chrono::{Duration, Utc};

use super::lunchflow::{LunchflowAccount, LunchflowTransaction};

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
            send_response(&mut stream, 400, "Bad Request", "Invalid request");
            return;
        }

        let method = parts[0];
        let path = parts[1];

        // Check authorization header (case-insensitive)
        let request_lower = request.to_lowercase();
        let has_valid_auth = request_lower.contains("authorization: bearer test_") ||
                            request_lower.contains("authorization: bearer lf_live_") ||
                            request_lower.contains("authorization: bearer mock_") ||
                            request_lower.contains("authorization: bearer valid_");

        // Handle different scenarios
        if config.fail_auth {
            send_response(&mut stream, 401, "Unauthorized",
                r#"{"error": "Invalid API key"}"#);
            return;
        }

        if !has_valid_auth {
            send_response(&mut stream, 401, "Unauthorized",
                r#"{"error": "Invalid API key"}"#);
            return;
        }

        if config.rate_limit {
            send_response(&mut stream, 429, "Too Many Requests",
                r#"{"error": "Rate limit exceeded"}"#);
            return;
        }

        // Route requests
        match (method, path) {
            ("GET", "/accounts") | ("GET", "/v1/accounts") => {
                let accounts = generate_mock_accounts(config.num_accounts);
                let json = serde_json::to_string(&accounts).unwrap();
                send_response(&mut stream, 200, "OK", &json);
            }
            ("GET", p) if p.contains("/transactions") => {
                // Extract account ID from path like /accounts/acc_1/transactions
                let txs = generate_mock_transactions(config.num_transactions_per_account);
                let json = serde_json::to_string(&txs).unwrap();
                send_response(&mut stream, 200, "OK", &json);
            }
            _ => {
                send_response(&mut stream, 404, "Not Found",
                    r#"{"error": "Endpoint not found"}"#);
            }
        }
    }
}

fn send_response(stream: &mut TcpStream, status: u16, status_text: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        status, status_text, body.len(), body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn generate_mock_accounts(count: usize) -> Vec<LunchflowAccount> {
    let institutions = vec![
        ("Barclays", "GBP"),
        ("HSBC", "GBP"),
        ("Revolut", "EUR"),
        ("N26", "EUR"),
        ("Chase", "USD"),
        ("Bank of America", "USD"),
    ];

    let account_types = vec!["checking", "savings", "credit"];

    (0..count).map(|i| {
        let (inst, currency) = &institutions[i % institutions.len()];
        let acc_type = account_types[i % account_types.len()];
        let balance = (1000.0 + (i as f64 * 500.0)) * if acc_type == "credit" { -1.0 } else { 1.0 };

        LunchflowAccount {
            id: format!("lf_acc_{}", i + 1),
            name: format!("{} {}", inst, acc_type.to_uppercase()),
            institution_name: Some(inst.to_string()),
            balance: Some(format!("{:.2}", balance)),
            currency: Some(currency.to_string()),
            account_type: Some(acc_type.to_string()),
        }
    }).collect()
}

fn generate_mock_transactions(count: usize) -> Vec<LunchflowTransaction> {
    let merchants = vec![
        ("Tesco", "Groceries", -45.23),
        ("Amazon", "Shopping", -29.99),
        ("Netflix", "Entertainment", -9.99),
        ("Shell", "Transport", -52.00),
        ("Costa Coffee", "Food & Drink", -4.50),
        ("Spotify", "Entertainment", -9.99),
        ("Uber", "Transport", -12.50),
        ("Apple", "Shopping", -199.00),
        ("SALARY", "Income", 3500.00),
        ("Interest", "Income", 2.50),
    ];

    let today = Utc::now().naive_utc().date();

    (0..count).map(|i| {
        let (merchant, category, amount) = &merchants[i % merchants.len()];
        let days_ago = (i % 90) as i64;
        let date = today - Duration::days(days_ago);

        LunchflowTransaction {
            id: format!("lf_tx_{}", i + 1),
            date: date.format("%Y-%m-%d").to_string(),
            amount: format!("{:.2}", amount),
            currency: Some("GBP".to_string()),
            merchant: Some(merchant.to_string()),
            description: Some(format!("{} - Transaction #{}", merchant, i + 1)),
            pending: i < 3, // First 3 transactions are pending
            category: Some(category.to_string()),
        }
    }).collect()
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
        }).unwrap();

        let client = LunchflowClient::new_with_base_url("test_key", &server.base_url()).unwrap();
        let result = client.get_accounts().unwrap();

        assert_eq!(result.accounts.len(), 5);
        assert!(result.accounts[0].external_ids.contains_key("lunchflow"));
    }

    #[test]
    fn test_mock_server_transactions() {
        let server = MockLunchflowServer::start(MockConfig {
            num_accounts: 1,
            num_transactions_per_account: 20,
            ..Default::default()
        }).unwrap();

        let client = LunchflowClient::new_with_base_url("test_key", &server.base_url()).unwrap();
        let accounts = client.get_accounts().unwrap();

        let account_ids: Vec<String> = accounts.accounts
            .iter()
            .filter_map(|a| a.external_ids.get("lunchflow").cloned())
            .collect();

        let today = Utc::now().naive_utc().date();
        let start = today - Duration::days(90);

        let result = client.get_transactions(start, today, Some(&account_ids)).unwrap();

        assert_eq!(result.transactions.len(), 20);
    }

    #[test]
    fn test_mock_server_auth_failure() {
        let server = MockLunchflowServer::start(MockConfig {
            fail_auth: true,
            ..Default::default()
        }).unwrap();

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
        }).unwrap();

        // Use a valid auth token prefix
        let client = LunchflowClient::new_with_base_url("valid_key", &server.base_url()).unwrap();
        let result = client.get_accounts();

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string().to_lowercase();
        assert!(err_msg.contains("rate limit"), "Expected 'rate limit' in error, got: {}", err_msg);
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
