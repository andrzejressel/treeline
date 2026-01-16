//! Database migrations - embedded SQL files
//!
//! Migrations are compiled into the binary at build time using include_str!.
//! Each migration is a tuple of (name, sql_content).
//! Migrations are sorted by name and applied in order.

/// All migrations, embedded at compile time.
/// Format: (filename, sql_content)
///
/// IMPORTANT: When adding a new migration:
/// 1. Create the SQL file: NNN_description.sql
/// 2. Add an entry here in order
pub const MIGRATIONS: &[(&str, &str)] = &[
    ("000_migrations.sql", include_str!("000_migrations.sql")),
    ("001_initial_schema.sql", include_str!("001_initial_schema.sql")),
    ("002_transaction_editing.sql", include_str!("002_transaction_editing.sql")),
    ("003_plugin_budget.sql", include_str!("003_plugin_budget.sql")),
    ("004_budget_month_scoped.sql", include_str!("004_budget_month_scoped.sql")),
    ("005_plugin_transactions_rules.sql", include_str!("005_plugin_transactions_rules.sql")),
    ("006_plugin_accounts_query.sql", include_str!("006_plugin_accounts_query.sql")),
    ("007_balance_snapshot_source.sql", include_str!("007_balance_snapshot_source.sql")),
    ("008_simplify_tag_rules.sql", include_str!("008_simplify_tag_rules.sql")),
    ("009_plugin_schemas.sql", include_str!("009_plugin_schemas.sql")),
    ("010_plugin_accounts_schema.sql", include_str!("010_plugin_accounts_schema.sql")),
    ("011_account_classification.sql", include_str!("011_account_classification.sql")),
];
