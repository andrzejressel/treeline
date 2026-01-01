//! Query service - SQL query execution

use std::sync::Arc;

use anyhow::Result;

use crate::adapters::duckdb::{DuckDbRepository, QueryResult};

/// Query service for SQL execution
pub struct QueryService {
    repository: Arc<DuckDbRepository>,
}

impl QueryService {
    pub fn new(repository: Arc<DuckDbRepository>) -> Self {
        Self { repository }
    }

    /// Execute a SQL query
    pub fn execute(&self, sql: &str) -> Result<QueryResult> {
        self.repository.execute_query(sql)
    }
}
