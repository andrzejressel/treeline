//! DuckDB repository implementation

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use duckdb::{Connection, params};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::domain::{Account, BalanceSnapshot, Transaction};

/// DuckDB repository implementation
pub struct DuckDbRepository {
    conn: Mutex<Connection>,
    db_path: PathBuf,
}

impl DuckDbRepository {
    /// Create a new DuckDB repository
    pub fn new(db_path: &Path, password: Option<&str>) -> Result<Self> {
        let conn = if let Some(pwd) = password {
            // Open encrypted database
            let conn = Connection::open(db_path)?;
            conn.execute(&format!("PRAGMA key = '{}'", pwd), [])?;
            conn
        } else {
            Connection::open(db_path)?
        };

        Ok(Self {
            conn: Mutex::new(conn),
            db_path: db_path.to_path_buf(),
        })
    }

    /// Ensure database schema exists
    pub fn ensure_schema(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        // Create migrations table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sys_migrations (
                migration_name VARCHAR PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;

        // Check if we need to run migrations
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sys_migrations",
            [],
            |row| row.get(0),
        )?;

        if count == 0 {
            self.run_migrations(&conn)?;
        }

        Ok(())
    }

    fn run_migrations(&self, conn: &Connection) -> Result<()> {
        // Create core tables
        conn.execute_batch(
            r#"
            -- Accounts table (matching Python schema - no balance column)
            CREATE TABLE IF NOT EXISTS sys_accounts (
                account_id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                nickname VARCHAR,
                account_type VARCHAR,
                currency VARCHAR NOT NULL DEFAULT 'USD',
                external_ids JSON DEFAULT '{}',
                institution_name VARCHAR,
                institution_url VARCHAR,
                institution_domain VARCHAR,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            -- Transactions table with foreign key constraint
            CREATE TABLE IF NOT EXISTS sys_transactions (
                transaction_id VARCHAR PRIMARY KEY,
                account_id VARCHAR NOT NULL,
                amount DECIMAL(15,2) NOT NULL,
                description VARCHAR,
                transaction_date DATE NOT NULL,
                posted_date DATE NOT NULL,
                tags VARCHAR[],
                external_ids JSON DEFAULT '{}',
                deleted_at TIMESTAMP,
                parent_transaction_id VARCHAR,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (account_id) REFERENCES sys_accounts(account_id)
            );

            CREATE INDEX IF NOT EXISTS idx_sys_transactions_account_id ON sys_transactions(account_id);
            CREATE INDEX IF NOT EXISTS idx_sys_transactions_date ON sys_transactions(transaction_date);
            CREATE INDEX IF NOT EXISTS idx_sys_transactions_parent_id ON sys_transactions(parent_transaction_id);

            -- Balance snapshots table with foreign key constraint
            CREATE TABLE IF NOT EXISTS sys_balance_snapshots (
                snapshot_id VARCHAR PRIMARY KEY,
                account_id VARCHAR NOT NULL,
                balance DECIMAL(15,2) NOT NULL,
                snapshot_time TIMESTAMP NOT NULL,
                source VARCHAR,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (account_id) REFERENCES sys_accounts(account_id)
            );

            CREATE INDEX IF NOT EXISTS idx_sys_balance_snapshots_account_id ON sys_balance_snapshots(account_id);
            CREATE INDEX IF NOT EXISTS idx_sys_balance_snapshots_time ON sys_balance_snapshots(snapshot_time);

            -- Integrations table
            CREATE TABLE IF NOT EXISTS sys_integrations (
                integration_name VARCHAR PRIMARY KEY,
                integration_settings JSON NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            -- Views (matching Python schema)
            CREATE OR REPLACE VIEW transactions AS
            SELECT
                t.transaction_id,
                t.account_id,
                t.amount,
                t.description,
                t.transaction_date,
                t.posted_date,
                t.tags,
                t.parent_transaction_id,
                a.name AS account_name,
                a.account_type,
                a.currency,
                a.institution_name
            FROM sys_transactions t
            LEFT JOIN sys_accounts a ON t.account_id = a.account_id
            WHERE t.deleted_at IS NULL;

            CREATE OR REPLACE VIEW accounts AS
            SELECT * FROM sys_accounts;

            CREATE OR REPLACE VIEW balance_snapshots AS
            SELECT
                s.snapshot_id,
                s.account_id,
                s.balance,
                s.snapshot_time,
                s.source,
                s.created_at,
                s.updated_at,
                a.name AS account_name,
                a.institution_name
            FROM sys_balance_snapshots s
            LEFT JOIN sys_accounts a ON s.account_id = a.account_id;

            -- Mark migration as applied
            INSERT INTO sys_migrations (migration_name) VALUES ('001_initial_schema');
            "#,
        )?;

        Ok(())
    }

    // === Account operations ===

    pub fn get_accounts(&self) -> Result<Vec<Account>> {
        let conn = self.conn.lock().unwrap();
        // Join with balance_snapshots to get the latest balance for each account
        let mut stmt = conn.prepare(
            "SELECT a.account_id, a.name, a.nickname, a.account_type, a.currency,
                    a.external_ids, a.institution_name, a.institution_url, a.institution_domain,
                    a.created_at, a.updated_at,
                    (SELECT balance FROM sys_balance_snapshots bs
                     WHERE bs.account_id = a.account_id
                     ORDER BY bs.snapshot_time DESC LIMIT 1) as latest_balance
             FROM sys_accounts a"
        )?;

        let accounts = stmt.query_map([], |row| {
            Ok(self.row_to_account(row))
        })?.filter_map(|r| r.ok()).collect();

        Ok(accounts)
    }

    pub fn get_account_by_id(&self, id: &str) -> Result<Option<Account>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT a.account_id, a.name, a.nickname, a.account_type, a.currency,
                    a.external_ids, a.institution_name, a.institution_url, a.institution_domain,
                    a.created_at, a.updated_at,
                    (SELECT balance FROM sys_balance_snapshots bs
                     WHERE bs.account_id = a.account_id
                     ORDER BY bs.snapshot_time DESC LIMIT 1) as latest_balance
             FROM sys_accounts a WHERE a.account_id = ?"
        )?;

        let account = stmt.query_row([id], |row| {
            Ok(self.row_to_account(row))
        }).ok();

        Ok(account)
    }

    fn row_to_account(&self, row: &duckdb::Row) -> Account {
        let id_str: String = row.get(0).unwrap_or_default();
        let external_ids_json: String = row.get(5).unwrap_or_else(|_| "{}".to_string());
        let created_str: String = row.get(9).unwrap_or_default();
        let updated_str: String = row.get(10).unwrap_or_default();

        Account {
            id: Uuid::parse_str(&id_str).unwrap_or_else(|_| Uuid::new_v4()),
            name: row.get(1).unwrap_or_default(),
            nickname: row.get(2).ok(),
            account_type: row.get::<_, Option<String>>(3).ok().flatten(),
            currency: row.get(4).unwrap_or_else(|_| "USD".to_string()),
            external_ids: serde_json::from_str(&external_ids_json).unwrap_or_default(),
            institution_name: row.get(6).ok(),
            institution_url: row.get(7).ok(),
            institution_domain: row.get(8).ok(),
            created_at: parse_timestamp(&created_str),
            updated_at: parse_timestamp(&updated_str),
            // Balance from latest balance snapshot (column 11)
            balance: row.get::<_, Option<f64>>(11).ok().flatten().map(|f| Decimal::try_from(f).unwrap_or_default()),
        }
    }

    pub fn upsert_account(&self, account: &Account) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let external_ids = serde_json::to_string(&account.external_ids)?;

        // Use COALESCE to preserve user-edited values like Python CLI does
        // Note: balance is stored in balance_snapshots, not in accounts table (matching Python schema)
        conn.execute(
            "INSERT INTO sys_accounts (account_id, name, nickname, account_type, currency,
                                       external_ids, institution_name, institution_url, institution_domain,
                                       created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (account_id) DO UPDATE SET
                name = EXCLUDED.name,
                nickname = COALESCE(sys_accounts.nickname, EXCLUDED.nickname),
                account_type = COALESCE(sys_accounts.account_type, EXCLUDED.account_type),
                currency = EXCLUDED.currency,
                external_ids = EXCLUDED.external_ids,
                institution_name = COALESCE(EXCLUDED.institution_name, sys_accounts.institution_name),
                institution_url = COALESCE(EXCLUDED.institution_url, sys_accounts.institution_url),
                institution_domain = COALESCE(EXCLUDED.institution_domain, sys_accounts.institution_domain),
                updated_at = EXCLUDED.updated_at",
            params![
                account.id.to_string(),
                account.name,
                account.nickname,
                account.account_type.as_ref().map(|t| t.to_string()),
                account.currency,
                external_ids,
                account.institution_name,
                account.institution_url,
                account.institution_domain,
                account.created_at.to_rfc3339(),
                account.updated_at.to_rfc3339(),
            ],
        )?;

        Ok(())
    }

    // === Transaction operations ===

    pub fn get_transactions(&self) -> Result<Vec<Transaction>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT transaction_id, account_id, amount, description, transaction_date,
                    posted_date, tags, external_ids, deleted_at, parent_transaction_id,
                    created_at, updated_at
             FROM sys_transactions
             WHERE deleted_at IS NULL"
        )?;

        let transactions = stmt.query_map([], |row| {
            Ok(self.row_to_transaction(row))
        })?.filter_map(|r| r.ok()).collect();

        Ok(transactions)
    }

    /// Get transactions for a specific account, ordered by transaction_date DESC
    pub fn get_transactions_by_account(&self, account_id: &str) -> Result<Vec<Transaction>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT transaction_id, account_id, amount, description, transaction_date,
                    posted_date, tags, external_ids, deleted_at, parent_transaction_id,
                    created_at, updated_at
             FROM sys_transactions
             WHERE account_id = ? AND deleted_at IS NULL
             ORDER BY transaction_date DESC"
        )?;

        let transactions = stmt.query_map([account_id], |row| {
            Ok(self.row_to_transaction(row))
        })?.filter_map(|r| r.ok()).collect();

        Ok(transactions)
    }

    pub fn get_transaction_count(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sys_transactions WHERE deleted_at IS NULL",
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// Get the maximum transaction date from the database
    pub fn get_max_transaction_date(&self) -> Result<Option<NaiveDate>> {
        let conn = self.conn.lock().unwrap();
        let result: Option<String> = conn.query_row(
            "SELECT MAX(transaction_date)::VARCHAR FROM sys_transactions WHERE deleted_at IS NULL",
            [],
            |row| row.get(0),
        )?;
        Ok(result.map(|s| parse_date(&s)))
    }

    pub fn get_balance_snapshot_count(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sys_balance_snapshots",
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    pub fn get_transaction_date_range(&self) -> Result<crate::services::DateRange> {
        let conn = self.conn.lock().unwrap();
        let result: (Option<String>, Option<String>) = conn.query_row(
            "SELECT
                MIN(transaction_date)::VARCHAR,
                MAX(transaction_date)::VARCHAR
             FROM sys_transactions
             WHERE deleted_at IS NULL",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok(crate::services::DateRange {
            earliest: result.0,
            latest: result.1,
        })
    }

    fn row_to_transaction(&self, row: &duckdb::Row) -> Transaction {
        let id_str: String = row.get(0).unwrap_or_default();
        let account_id_str: String = row.get(1).unwrap_or_default();
        let amount: f64 = row.get(2).unwrap_or(0.0);
        let tx_date_str: String = row.get(4).unwrap_or_default();
        let posted_date_str: String = row.get(5).unwrap_or_default();

        // Tags are stored as VARCHAR[] - DuckDB Rust binding returns them as a string
        // Parse the DuckDB array format: [tag1, tag2] or ['tag1', 'tag2']
        let tags_str: String = row.get(6).unwrap_or_else(|_| "[]".to_string());
        let tags = parse_duckdb_array(&tags_str);

        let external_ids_json: String = row.get(7).unwrap_or_else(|_| "{}".to_string());
        let parent_id_str: Option<String> = row.get(9).ok();
        let created_str: String = row.get(10).unwrap_or_default();
        let updated_str: String = row.get(11).unwrap_or_default();

        Transaction {
            id: Uuid::parse_str(&id_str).unwrap_or_else(|_| Uuid::new_v4()),
            account_id: Uuid::parse_str(&account_id_str).unwrap_or_else(|_| Uuid::new_v4()),
            amount: Decimal::try_from(amount).unwrap_or_default(),
            description: row.get(3).ok(),
            transaction_date: parse_date(&tx_date_str),
            posted_date: parse_date(&posted_date_str),
            tags,
            external_ids: serde_json::from_str(&external_ids_json).unwrap_or_default(),
            deleted_at: None,
            parent_transaction_id: parent_id_str.and_then(|s| Uuid::parse_str(&s).ok()),
            created_at: parse_timestamp(&created_str),
            updated_at: parse_timestamp(&updated_str),
        }
    }

    pub fn upsert_transaction(&self, tx: &Transaction) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let external_ids = serde_json::to_string(&tx.external_ids)?;

        // Build tags array literal for DuckDB: ['tag1', 'tag2']
        let tags_literal = format_tags_array(&tx.tags);

        // Use raw SQL with array literal since DuckDB Rust binding doesn't support array params well
        let sql = format!(
            "INSERT INTO sys_transactions (transaction_id, account_id, amount, description,
                                           transaction_date, posted_date, tags, external_ids,
                                           parent_transaction_id, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, {}, ?, ?, ?, ?)
             ON CONFLICT (transaction_id) DO UPDATE SET
                account_id = EXCLUDED.account_id,
                amount = EXCLUDED.amount,
                description = EXCLUDED.description,
                transaction_date = EXCLUDED.transaction_date,
                posted_date = EXCLUDED.posted_date,
                tags = EXCLUDED.tags,
                external_ids = EXCLUDED.external_ids,
                parent_transaction_id = EXCLUDED.parent_transaction_id,
                updated_at = EXCLUDED.updated_at",
            tags_literal
        );

        conn.execute(
            &sql,
            params![
                tx.id.to_string(),
                tx.account_id.to_string(),
                tx.amount.to_string().parse::<f64>().unwrap_or(0.0),
                tx.description,
                tx.transaction_date.to_string(),
                tx.posted_date.to_string(),
                external_ids,
                tx.parent_transaction_id.map(|id| id.to_string()),
                tx.created_at.to_rfc3339(),
                tx.updated_at.to_rfc3339(),
            ],
        )?;

        Ok(())
    }

    pub fn update_transaction_tags(&self, tx_id: &str, tags: &[String]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let tags_literal = format_tags_array(tags);
        let sql = format!(
            "UPDATE sys_transactions SET tags = {}, updated_at = CURRENT_TIMESTAMP WHERE transaction_id = ?",
            tags_literal
        );
        conn.execute(&sql, params![tx_id])?;
        Ok(())
    }

    /// Insert a transaction only if it doesn't already exist (skip existing to preserve user edits)
    /// Returns true if inserted, false if skipped
    pub fn insert_transaction_if_not_exists(&self, tx: &Transaction) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let external_ids = serde_json::to_string(&tx.external_ids)?;
        let tags_literal = format_tags_array(&tx.tags);

        // Use INSERT ... ON CONFLICT DO NOTHING to skip existing transactions
        let sql = format!(
            "INSERT INTO sys_transactions (transaction_id, account_id, amount, description,
                                           transaction_date, posted_date, tags, external_ids,
                                           parent_transaction_id, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, {}, ?, ?, ?, ?)
             ON CONFLICT (transaction_id) DO NOTHING",
            tags_literal
        );

        let rows_changed = conn.execute(
            &sql,
            params![
                tx.id.to_string(),
                tx.account_id.to_string(),
                tx.amount.to_string().parse::<f64>().unwrap_or(0.0),
                tx.description,
                tx.transaction_date.to_string(),
                tx.posted_date.to_string(),
                external_ids,
                tx.parent_transaction_id.map(|id| id.to_string()),
                tx.created_at.to_rfc3339(),
                tx.updated_at.to_rfc3339(),
            ],
        )?;

        Ok(rows_changed > 0)
    }

    /// Check if a transaction exists by ID
    pub fn transaction_exists(&self, tx_id: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sys_transactions WHERE transaction_id = ?",
            params![tx_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Check if a transaction exists by external ID for a given integration
    pub fn transaction_exists_by_external_id(&self, integration: &str, external_id: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        // Use DuckDB's JSON extraction to check if the external ID matches
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sys_transactions WHERE json_extract_string(external_ids, ?) = ?",
            params![format!("$.{}", integration), external_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn get_transaction_by_id(&self, id: &str) -> Result<Option<Transaction>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT transaction_id, account_id, amount, description, transaction_date,
                    posted_date, tags, external_ids, deleted_at, parent_transaction_id,
                    created_at, updated_at
             FROM sys_transactions WHERE transaction_id = ?"
        )?;

        let tx = stmt.query_row([id], |row| {
            Ok(self.row_to_transaction(row))
        }).ok();

        Ok(tx)
    }

    // === Balance snapshot operations ===

    pub fn add_balance_snapshot(&self, snapshot: &BalanceSnapshot) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO sys_balance_snapshots (snapshot_id, account_id, balance, snapshot_time, source, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                snapshot.id.to_string(),
                snapshot.account_id.to_string(),
                snapshot.balance.to_string().parse::<f64>().unwrap_or(0.0),
                snapshot.snapshot_time.to_string(),
                snapshot.source.as_ref().map(|s| s.to_string()),
                snapshot.created_at.to_rfc3339(),
                snapshot.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_balance_snapshots(&self, account_id: Option<&str>) -> Result<Vec<BalanceSnapshot>> {
        let conn = self.conn.lock().unwrap();
        let sql = if account_id.is_some() {
            "SELECT snapshot_id, account_id, balance, snapshot_time, source, created_at, updated_at
             FROM sys_balance_snapshots WHERE account_id = ? ORDER BY snapshot_time DESC"
        } else {
            "SELECT snapshot_id, account_id, balance, snapshot_time, source, created_at, updated_at
             FROM sys_balance_snapshots ORDER BY snapshot_time DESC"
        };

        let mut stmt = conn.prepare(sql)?;

        let snapshots = if let Some(aid) = account_id {
            stmt.query_map([aid], |row| Ok(self.row_to_balance_snapshot(row)))?
                .filter_map(|r| r.ok())
                .collect()
        } else {
            stmt.query_map([], |row| Ok(self.row_to_balance_snapshot(row)))?
                .filter_map(|r| r.ok())
                .collect()
        };

        Ok(snapshots)
    }

    fn row_to_balance_snapshot(&self, row: &duckdb::Row) -> BalanceSnapshot {
        let id_str: String = row.get(0).unwrap_or_default();
        let account_id_str: String = row.get(1).unwrap_or_default();
        let balance: f64 = row.get(2).unwrap_or(0.0);
        let snapshot_time_str: String = row.get(3).unwrap_or_default();
        let source: Option<String> = row.get(4).ok();
        let created_str: String = row.get(5).unwrap_or_default();
        let updated_str: String = row.get(6).unwrap_or_default();

        BalanceSnapshot {
            id: Uuid::parse_str(&id_str).unwrap_or_else(|_| Uuid::new_v4()),
            account_id: Uuid::parse_str(&account_id_str).unwrap_or_else(|_| Uuid::new_v4()),
            balance: Decimal::try_from(balance).unwrap_or_default(),
            snapshot_time: parse_naive_datetime(&snapshot_time_str),
            source,
            created_at: parse_timestamp(&created_str),
            updated_at: parse_timestamp(&updated_str),
        }
    }

    // === Query operations ===

    pub fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        // Validate it's a read-only query by checking SQL statement type
        // Only look at the first word after stripping whitespace/comments
        let sql_trimmed = sql.trim();
        let first_word = sql_trimmed.split_whitespace().next().unwrap_or("").to_uppercase();
        if first_word != "SELECT" && first_word != "WITH" {
            anyhow::bail!("Only SELECT queries are allowed");
        }

        // Also block dangerous operations even in subqueries
        let sql_upper = sql.to_uppercase();
        // Use word boundaries to avoid false positives (deleted_at vs DELETE)
        let dangerous_patterns = [
            " INSERT ", " UPDATE ", " DROP ", " CREATE ", " ALTER ", " TRUNCATE ",
            "\nINSERT ", "\nUPDATE ", "\nDROP ", "\nCREATE ", "\nALTER ", "\nTRUNCATE ",
            "(INSERT ", "(UPDATE ", "(DROP ", "(CREATE ", "(ALTER ", "(TRUNCATE ",
        ];
        for pattern in dangerous_patterns {
            if sql_upper.contains(pattern) {
                anyhow::bail!("Only SELECT queries are allowed");
            }
        }

        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(sql)?;

        // Execute query and iterate
        let mut result_rows = stmt.query([])?;

        // Collect all rows first
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
        let mut column_count = 0;

        while let Some(row) = result_rows.next()? {
            // Get column count from the first row
            if rows.is_empty() {
                column_count = row.as_ref().column_count();
            }

            let mut row_values: Vec<serde_json::Value> = Vec::new();
            for i in 0..column_count {
                let value = self.get_column_value(row, i);
                row_values.push(value);
            }
            rows.push(row_values);
        }

        // Drop result_rows to release borrow on stmt
        drop(result_rows);

        // Now get column names
        let columns: Vec<String> = if column_count > 0 {
            (0..column_count)
                .map(|i| stmt.column_name(i).map(|s| s.to_string()).unwrap_or_else(|_| format!("col{}", i)))
                .collect()
        } else {
            // No rows, try to get column count from statement
            let count = stmt.column_count();
            (0..count)
                .map(|i| stmt.column_name(i).map(|s| s.to_string()).unwrap_or_else(|_| format!("col{}", i)))
                .collect()
        };

        let row_count = rows.len();

        Ok(QueryResult {
            columns,
            rows,
            row_count,
        })
    }

    fn get_column_value(&self, row: &duckdb::Row, idx: usize) -> serde_json::Value {
        use duckdb::types::ValueRef;

        // Use get_ref to get the raw ValueRef, which handles all types including arrays
        match row.get_ref(idx) {
            Ok(ValueRef::Null) => serde_json::Value::Null,
            Ok(ValueRef::Boolean(b)) => serde_json::Value::Bool(b),
            Ok(ValueRef::TinyInt(i)) => serde_json::json!(i),
            Ok(ValueRef::SmallInt(i)) => serde_json::json!(i),
            Ok(ValueRef::Int(i)) => serde_json::json!(i),
            Ok(ValueRef::BigInt(i)) => serde_json::json!(i),
            Ok(ValueRef::HugeInt(i)) => serde_json::json!(i.to_string()),
            Ok(ValueRef::UTinyInt(i)) => serde_json::json!(i),
            Ok(ValueRef::USmallInt(i)) => serde_json::json!(i),
            Ok(ValueRef::UInt(i)) => serde_json::json!(i),
            Ok(ValueRef::UBigInt(i)) => serde_json::json!(i),
            Ok(ValueRef::Float(f)) => serde_json::json!(f),
            Ok(ValueRef::Double(f)) => serde_json::json!(f),
            Ok(ValueRef::Decimal(d)) => serde_json::json!(d.to_string()),
            Ok(ValueRef::Text(bytes)) => {
                let s = String::from_utf8_lossy(bytes).to_string();
                serde_json::Value::String(s)
            }
            Ok(ValueRef::Blob(bytes)) => {
                serde_json::Value::String(format!("<blob {} bytes>", bytes.len()))
            }
            Ok(ValueRef::Date32(d)) => {
                // Days since epoch
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let date = epoch + chrono::Duration::days(d as i64);
                serde_json::Value::String(date.to_string())
            }
            Ok(ValueRef::Timestamp(_, ts)) => {
                // Microseconds since epoch
                let dt = chrono::DateTime::from_timestamp_micros(ts)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| ts.to_string());
                serde_json::Value::String(dt)
            }
            Ok(ValueRef::Time64(_, t)) => serde_json::json!(t),
            Ok(ValueRef::Interval { months, days, nanos }) => {
                serde_json::json!({
                    "months": months,
                    "days": days,
                    "nanos": nanos
                })
            }
            Ok(ValueRef::List(list_type, list_idx)) => {
                // Handle VARCHAR[] arrays
                self.list_to_json(&list_type, list_idx)
            }
            Ok(ValueRef::Enum(_, idx)) => serde_json::json!(idx),
            Ok(ValueRef::Struct(arr, idx)) => {
                // Convert struct to object
                self.struct_to_json(arr, idx)
            }
            _ => serde_json::Value::Null,
        }
    }

    fn list_to_json(&self, list_type: &duckdb::types::ListType, idx: usize) -> serde_json::Value {
        use duckdb::arrow::array::{Array, StringArray};

        // Get the list array and extract values at the given index
        match list_type {
            duckdb::types::ListType::Regular(arr) => {
                if arr.is_null(idx) {
                    return serde_json::Value::Null;
                }
                let values = arr.value(idx);
                // Try to convert to string array
                if let Some(str_arr) = values.as_any().downcast_ref::<StringArray>() {
                    let items: Vec<serde_json::Value> = (0..str_arr.len())
                        .map(|i| {
                            if str_arr.is_null(i) {
                                serde_json::Value::Null
                            } else {
                                serde_json::Value::String(str_arr.value(i).to_string())
                            }
                        })
                        .collect();
                    serde_json::Value::Array(items)
                } else {
                    // Fallback: convert to string representation
                    serde_json::Value::String(format!("{:?}", values))
                }
            }
            duckdb::types::ListType::Large(arr) => {
                if arr.is_null(idx) {
                    return serde_json::Value::Null;
                }
                let values = arr.value(idx);
                if let Some(str_arr) = values.as_any().downcast_ref::<StringArray>() {
                    let items: Vec<serde_json::Value> = (0..str_arr.len())
                        .map(|i| {
                            if str_arr.is_null(i) {
                                serde_json::Value::Null
                            } else {
                                serde_json::Value::String(str_arr.value(i).to_string())
                            }
                        })
                        .collect();
                    serde_json::Value::Array(items)
                } else {
                    serde_json::Value::String(format!("{:?}", values))
                }
            }
        }
    }

    fn struct_to_json(&self, arr: &duckdb::arrow::array::StructArray, idx: usize) -> serde_json::Value {
        use duckdb::arrow::array::Array;

        if arr.is_null(idx) {
            return serde_json::Value::Null;
        }

        let mut obj = serde_json::Map::new();
        for (field_idx, field) in arr.fields().iter().enumerate() {
            let col = arr.column(field_idx);
            // Simplified: just get string representation
            if col.is_null(idx) {
                obj.insert(field.name().clone(), serde_json::Value::Null);
            } else {
                obj.insert(field.name().clone(), serde_json::Value::String(format!("{:?}", col)));
            }
        }
        serde_json::Value::Object(obj)
    }

    // === Integration operations ===

    pub fn get_integrations(&self) -> Result<Vec<Integration>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT integration_name, integration_settings FROM sys_integrations"
        )?;

        let integrations = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let settings_json: String = row.get(1)?;
            let settings: serde_json::Value = serde_json::from_str(&settings_json).unwrap_or(serde_json::json!({}));
            Ok(Integration { name, settings })
        })?.filter_map(|r| r.ok()).collect();

        Ok(integrations)
    }

    pub fn upsert_integration(&self, name: &str, settings: &serde_json::Value) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let settings_json = serde_json::to_string(settings)?;
        let now = chrono::Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO sys_integrations (integration_name, integration_settings, created_at, updated_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (integration_name) DO UPDATE SET
                integration_settings = EXCLUDED.integration_settings,
                updated_at = EXCLUDED.updated_at",
            params![name, settings_json, now, now],
        )?;

        Ok(())
    }

    pub fn delete_integration(&self, name: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "DELETE FROM sys_integrations WHERE integration_name = ?",
            params![name],
        )?;
        Ok(rows > 0)
    }

    // === Maintenance operations ===

    pub fn compact(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("CHECKPOINT; VACUUM;")?;
        Ok(())
    }

    pub fn get_db_size(&self) -> Result<u64> {
        // Get actual file size from filesystem
        let metadata = std::fs::metadata(&self.db_path)?;
        Ok(metadata.len())
    }

    // === Doctor checks ===

    pub fn check_orphaned_transactions(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT t.transaction_id FROM sys_transactions t
             LEFT JOIN sys_accounts a ON t.account_id = a.account_id
             WHERE a.account_id IS NULL AND t.deleted_at IS NULL"
        )?;

        let orphans: Vec<String> = stmt.query_map([], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(orphans)
    }

    pub fn check_orphaned_snapshots(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT s.snapshot_id FROM sys_balance_snapshots s
             LEFT JOIN sys_accounts a ON s.account_id = a.account_id
             WHERE a.account_id IS NULL"
        )?;

        let orphans: Vec<String> = stmt.query_map([], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(orphans)
    }

    pub fn check_duplicate_fingerprints(&self) -> Result<Vec<(String, i64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT json_extract_string(external_ids, '$.fingerprint') as fp, COUNT(*) as cnt
             FROM sys_transactions
             WHERE deleted_at IS NULL AND json_extract_string(external_ids, '$.fingerprint') IS NOT NULL
             GROUP BY fp HAVING COUNT(*) > 1"
        )?;

        let duplicates: Vec<(String, i64)> = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?.filter_map(|r| r.ok()).collect();

        Ok(duplicates)
    }

    pub fn check_future_transactions(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sys_transactions
             WHERE transaction_date > CURRENT_DATE + INTERVAL '1 day' AND deleted_at IS NULL",
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    pub fn count_untagged_transactions(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sys_transactions
             WHERE (tags IS NULL OR len(tags) = 0)
             AND deleted_at IS NULL",
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    pub fn count_uncategorized_expenses(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sys_transactions
             WHERE amount < 0
             AND (tags IS NULL OR len(tags) = 0)
             AND deleted_at IS NULL",
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// Check for transactions with unreasonable dates (before 1970 or more than 1 year in future)
    pub fn check_date_sanity(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT transaction_id, transaction_date, description, amount
             FROM sys_transactions
             WHERE deleted_at IS NULL
               AND (transaction_date > CURRENT_DATE + INTERVAL '365 day'
                    OR transaction_date < '1970-01-01')
             LIMIT 100"
        )?;

        let results: Vec<String> = stmt.query_map([], |row| {
            let tx_id: String = row.get(0)?;
            let date: String = row.get(1)?;
            let desc: Option<String> = row.get(2)?;
            let amount: f64 = row.get(3)?;
            Ok(format!("{}|{}|{}|{}", tx_id, date, desc.unwrap_or_default(), amount))
        })?.filter_map(|r| r.ok()).collect();

        Ok(results)
    }

    /// Check if a table exists
    pub fn table_exists(&self, table_name: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        // Split schema.table if present
        let (schema, table) = if table_name.contains('.') {
            let parts: Vec<&str> = table_name.split('.').collect();
            (parts[0], parts[1])
        } else {
            ("main", table_name)
        };

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM information_schema.tables
             WHERE table_schema = ? AND table_name = ?",
            [schema, table],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }
}

/// Query result structure
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
}

/// Integration info
#[derive(Debug, Clone)]
pub struct Integration {
    pub name: String,
    pub settings: serde_json::Value,
}

// Helper functions

fn parse_timestamp(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn parse_date(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .unwrap_or_else(|_| Utc::now().date_naive())
}

fn parse_naive_datetime(s: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .unwrap_or_else(|_| Utc::now().naive_utc())
}

/// Format tags as a DuckDB array literal: ['tag1', 'tag2']
fn format_tags_array(tags: &[String]) -> String {
    if tags.is_empty() {
        return "[]".to_string();
    }

    let escaped: Vec<String> = tags
        .iter()
        .map(|t| {
            // Escape single quotes by doubling them
            let escaped = t.replace('\'', "''");
            format!("'{}'", escaped)
        })
        .collect();

    format!("[{}]", escaped.join(", "))
}

/// Parse DuckDB array string format: [tag1, tag2] or ['tag1', 'tag2']
fn parse_duckdb_array(s: &str) -> Vec<String> {
    let s = s.trim();
    if s.is_empty() || s == "[]" || s == "NULL" {
        return Vec::new();
    }

    // Remove brackets
    let inner = s.trim_start_matches('[').trim_end_matches(']');
    if inner.is_empty() {
        return Vec::new();
    }

    // Split by comma and clean up each element
    inner
        .split(',')
        .map(|item| {
            item.trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string()
        })
        .filter(|s| !s.is_empty())
        .collect()
}
