"""DuckDB infrastructure implementation."""

import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List
from uuid import UUID

import duckdb

from treeline.abstractions import Repository
from treeline.domain import Account, BalanceSnapshot, Fail, Ok, Result, Transaction


class DuckDBRepository(Repository):
    """DuckDB implementation of Repository."""

    def __init__(self, db_file_path: str, encryption_key: str | None = None):
        """Initialize with a database file path and optional encryption key.

        Args:
            db_file_path: Full path to the DuckDB database file
            encryption_key: Hex-encoded encryption key (if database is encrypted)
        """
        self.db_path = Path(db_file_path)
        self.db_dir = self.db_path.parent
        self.db_dir.mkdir(parents=True, exist_ok=True)
        self._encryption_key = encryption_key

    def _ensure_timezone(self, dt: datetime) -> datetime:
        """Ensure datetime is timezone-aware."""
        from datetime import date

        if dt is None:
            return dt
        # Handle datetime.date objects (from DATE columns) - convert to datetime at midnight UTC
        if isinstance(dt, date) and not isinstance(dt, datetime):
            return datetime.combine(dt, datetime.min.time(), tzinfo=timezone.utc)
        # Handle datetime objects - ensure they have timezone
        if isinstance(dt, datetime) and dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    def _get_connection(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        """Get a database connection.

        For encrypted databases, uses in-memory connection with ATTACH.
        For unencrypted databases, uses direct connection.
        """
        if self._encryption_key:
            # Encrypted: connect to memory, attach encrypted DB
            conn = duckdb.connect(":memory:")
            ro_clause = ", READ_ONLY" if read_only else ""
            conn.execute(
                f"ATTACH '{self.db_path}' AS main_db (ENCRYPTION_KEY '{self._encryption_key}'{ro_clause})"
            )
            conn.execute("USE main_db")
            return conn
        else:
            # Unencrypted: direct connect
            if read_only:
                return duckdb.connect(str(self.db_path), read_only=True)
            return duckdb.connect(str(self.db_path))

    async def ensure_db_exists(self) -> Result:
        """Ensure the database directory exists."""
        try:
            self.db_dir.mkdir(parents=True, exist_ok=True)
            return Ok()
        except Exception as e:
            return Fail(f"Failed to create database directory: {str(e)}")

    async def ensure_schema_upgraded(self) -> Result:
        """Ensure database schema is initialized with all migrations."""
        try:
            # Create database if it doesn't exist - use _get_connection for encryption support
            conn = self._get_connection()

            migrations_dir = Path(__file__).parent / "migrations"

            # Check if sys_migrations table exists
            tables_result = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'sys_migrations'"
            ).fetchall()

            # Bootstrap: if sys_migrations doesn't exist, run 000_migrations.sql first
            if not tables_result:
                bootstrap_migration = migrations_dir / "000_migrations.sql"
                if bootstrap_migration.exists():
                    with open(bootstrap_migration, "r") as f:
                        migration_sql = f.read()
                    conn.execute(migration_sql)

            # Run all migrations that haven't been applied yet
            migration_files = sorted(migrations_dir.glob("*.sql"))

            for migration_file in migration_files:
                migration_name = migration_file.name

                # Check if migration has already been applied
                result = conn.execute(
                    "SELECT migration_name FROM sys_migrations WHERE migration_name = ?",
                    [migration_name],
                ).fetchall()

                if not result:
                    # Migration hasn't been applied yet, run it
                    with open(migration_file, "r") as f:
                        migration_sql = f.read()
                    conn.execute(migration_sql)

                    # Record that this migration has been applied
                    conn.execute(
                        "INSERT INTO sys_migrations (migration_name) VALUES (?)",
                        [migration_name],
                    )

            conn.close()
            return Ok()
        except Exception as e:
            return Fail(f"Failed to initialize database: {str(e)}")

    async def add_account(self, account: Account) -> Result[Account]:
        """Add a single account."""
        try:
            conn = self._get_connection()

            conn.execute(
                """
                INSERT INTO sys_accounts (
                    account_id, name, nickname, account_type, currency,
                    external_ids, institution_name, institution_url, institution_domain,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    str(account.id),
                    account.name,
                    account.nickname,
                    account.account_type,
                    account.currency,
                    json.dumps(dict(account.external_ids)),
                    account.institution_name,
                    account.institution_url,
                    account.institution_domain,
                    account.created_at,
                    account.updated_at,
                ],
            )

            conn.close()
            return Ok(account)
        except Exception as e:
            return Fail(f"Failed to add account: {str(e)}")

    async def add_transaction(self, transaction: Transaction) -> Result[Transaction]:
        """Add a single transaction."""
        try:
            conn = self._get_connection()

            conn.execute(
                """
                INSERT INTO sys_transactions (
                    transaction_id, account_id, external_ids, amount, description,
                    transaction_date, posted_date, tags, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    str(transaction.id),
                    str(transaction.account_id),
                    json.dumps(dict(transaction.external_ids)),
                    float(transaction.amount),
                    transaction.description,
                    transaction.transaction_date,
                    transaction.posted_date,
                    list(transaction.tags),
                    transaction.created_at,
                    transaction.updated_at,
                ],
            )

            conn.close()
            return Ok(transaction)
        except Exception as e:
            return Fail(f"Failed to add transaction: {str(e)}")

    async def add_balance(self, balance: BalanceSnapshot) -> Result[BalanceSnapshot]:
        """Add a balance snapshot."""
        try:
            conn = self._get_connection()

            conn.execute(
                """
                INSERT INTO sys_balance_snapshots (
                    snapshot_id, account_id, balance, snapshot_time, created_at, source
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    str(balance.id),
                    str(balance.account_id),
                    float(balance.balance),
                    balance.snapshot_time,
                    balance.created_at,
                    balance.source,
                ],
            )

            conn.close()
            return Ok(balance)
        except Exception as e:
            return Fail(f"Failed to add balance: {str(e)}")

    async def bulk_upsert_accounts(
        self, accounts: List[Account]
    ) -> Result[List[Account]]:
        """Bulk upsert accounts."""
        try:
            conn = self._get_connection()

            for account in accounts:
                conn.execute(
                    """
                    INSERT INTO sys_accounts (
                        account_id, name, nickname, account_type, currency,
                        external_ids, institution_name, institution_url, institution_domain,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (account_id) DO UPDATE SET
                        name = excluded.name,
                        nickname = COALESCE(sys_accounts.nickname, excluded.nickname),
                        account_type = COALESCE(sys_accounts.account_type, excluded.account_type),
                        currency = excluded.currency,
                        external_ids = excluded.external_ids,
                        institution_name = COALESCE(excluded.institution_name, sys_accounts.institution_name),
                        institution_url = COALESCE(excluded.institution_url, sys_accounts.institution_url),
                        institution_domain = COALESCE(excluded.institution_domain, sys_accounts.institution_domain),
                        updated_at = excluded.updated_at
                    """,
                    [
                        str(account.id),
                        account.name,
                        account.nickname,
                        account.account_type,
                        account.currency,
                        json.dumps(dict(account.external_ids)),
                        account.institution_name,
                        account.institution_url,
                        account.institution_domain,
                        account.created_at,
                        account.updated_at,
                    ],
                )

            conn.close()
            return Ok(accounts)
        except Exception as e:
            return Fail(f"Failed to bulk upsert accounts: {str(e)}")

    async def bulk_upsert_transactions(
        self, transactions: List[Transaction]
    ) -> Result[List[Transaction]]:
        """Bulk upsert transactions."""
        try:
            conn = self._get_connection()

            for transaction in transactions:
                conn.execute(
                    """
                    INSERT INTO sys_transactions (
                        transaction_id, account_id, external_ids, amount, description,
                        transaction_date, posted_date, tags, created_at, updated_at,
                        deleted_at, parent_transaction_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (transaction_id) DO UPDATE SET
                        account_id = excluded.account_id,
                        external_ids = excluded.external_ids,
                        amount = excluded.amount,
                        description = excluded.description,
                        transaction_date = excluded.transaction_date,
                        posted_date = excluded.posted_date,
                        tags = excluded.tags,
                        updated_at = excluded.updated_at
                    """,
                    [
                        str(transaction.id),
                        str(transaction.account_id),
                        json.dumps(dict(transaction.external_ids)),
                        float(transaction.amount),
                        transaction.description,
                        transaction.transaction_date,
                        transaction.posted_date,
                        list(transaction.tags),
                        transaction.created_at,
                        transaction.updated_at,
                        transaction.deleted_at,
                        str(transaction.parent_transaction_id) if transaction.parent_transaction_id else None,
                    ],
                )

            conn.close()
            return Ok(transactions)
        except Exception as e:
            return Fail(f"Failed to bulk upsert transactions: {str(e)}")

    async def bulk_add_balances(
        self, balances: List[BalanceSnapshot]
    ) -> Result[List[BalanceSnapshot]]:
        """Bulk add balance snapshots."""
        try:
            conn = self._get_connection()

            for balance in balances:
                conn.execute(
                    """
                    INSERT INTO sys_balance_snapshots (
                        snapshot_id, account_id, balance, snapshot_time, created_at, source
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        str(balance.id),
                        str(balance.account_id),
                        float(balance.balance),
                        balance.snapshot_time,
                        balance.created_at,
                        balance.source,
                    ],
                )

            conn.close()
            return Ok(balances)
        except Exception as e:
            return Fail(f"Failed to bulk add balances: {str(e)}")

    async def update_account_by_id(self, account: Account) -> Result[Account]:
        """Update an account by ID."""
        try:
            conn = self._get_connection()

            conn.execute(
                """
                UPDATE sys_accounts SET
                    name = ?, nickname = ?, account_type = ?, currency = ?,
                    external_ids = ?, institution_name = ?, institution_url = ?,
                    institution_domain = ?, updated_at = ?
                WHERE account_id = ?
                """,
                [
                    account.name,
                    account.nickname,
                    account.account_type,
                    account.currency,
                    json.dumps(dict(account.external_ids)),
                    account.institution_name,
                    account.institution_url,
                    account.institution_domain,
                    account.updated_at,
                    str(account.id),
                ],
            )

            conn.close()
            return Ok(account)
        except Exception as e:
            return Fail(f"Failed to update account: {str(e)}")

    async def get_accounts(self) -> Result[List[Account]]:
        """Get all accounts."""
        try:
            conn = self._get_connection(read_only=True)

            result = conn.execute("SELECT * FROM sys_accounts").fetchall()
            columns = [desc[0] for desc in conn.description]

            accounts = []
            for row in result:
                row_dict = dict(zip(columns, row))
                account = Account(
                    id=UUID(row_dict["account_id"]),
                    name=row_dict["name"],
                    nickname=row_dict["nickname"],
                    account_type=row_dict["account_type"],
                    currency=row_dict["currency"],
                    external_ids=MappingProxyType(
                        json.loads(row_dict["external_ids"])
                        if row_dict["external_ids"]
                        else {}
                    ),
                    institution_name=row_dict["institution_name"],
                    institution_url=row_dict["institution_url"],
                    institution_domain=row_dict["institution_domain"],
                    created_at=self._ensure_timezone(row_dict["created_at"]),
                    updated_at=self._ensure_timezone(row_dict["updated_at"]),
                )
                accounts.append(account)

            conn.close()
            return Ok(accounts)
        except Exception as e:
            return Fail(f"Failed to get accounts: {str(e)}")

    async def get_account_by_id(self, account_id: UUID) -> Result[Account]:
        """Get a single account by ID."""
        try:
            conn = self._get_connection(read_only=True)

            result = conn.execute(
                "SELECT * FROM sys_accounts WHERE account_id = ?", [str(account_id)]
            ).fetchone()

            if not result:
                conn.close()
                return Fail("Account not found")

            columns = [desc[0] for desc in conn.description]
            row_dict = dict(zip(columns, result))

            account = Account(
                id=UUID(row_dict["account_id"]),
                name=row_dict["name"],
                nickname=row_dict["nickname"],
                account_type=row_dict["account_type"],
                currency=row_dict["currency"],
                external_ids=MappingProxyType(
                    json.loads(row_dict["external_ids"])
                    if row_dict["external_ids"]
                    else {}
                ),
                institution_name=row_dict["institution_name"],
                institution_url=row_dict["institution_url"],
                institution_domain=row_dict["institution_domain"],
                created_at=self._ensure_timezone(row_dict["created_at"]),
                updated_at=self._ensure_timezone(row_dict["updated_at"]),
            )

            conn.close()
            return Ok(account)
        except Exception as e:
            return Fail(f"Failed to get account: {str(e)}")

    async def get_account_by_external_id(self, external_id: str) -> Result[Account]:
        """Get an account by external ID."""
        # This requires JSON querying which DuckDB supports
        return Fail("Not implemented")

    async def get_transactions_by_external_ids(
        self, external_ids: List[Dict[str, str]]
    ) -> Result[List[Transaction]]:
        """Get transactions by external IDs."""
        try:
            conn = self._get_connection(read_only=True)

            transactions = []
            for ext_id_obj in external_ids:
                # Query for transactions with matching external IDs
                result = conn.execute(
                    "SELECT * FROM sys_transactions WHERE external_ids::VARCHAR LIKE ?",
                    [f"%{list(ext_id_obj.values())[0]}%"],
                ).fetchall()

                columns = [desc[0] for desc in conn.description]

                for row in result:
                    row_dict = dict(zip(columns, row))
                    transaction = Transaction(
                        id=UUID(row_dict["transaction_id"]),
                        account_id=UUID(row_dict["account_id"]),
                        external_ids=MappingProxyType(
                            json.loads(row_dict["external_ids"])
                            if row_dict["external_ids"]
                            else {}
                        ),
                        amount=Decimal(str(row_dict["amount"])),
                        description=row_dict["description"],
                        transaction_date=row_dict[
                            "transaction_date"
                        ],  # Already a date object
                        posted_date=row_dict["posted_date"],  # Already a date object
                        tags=tuple(row_dict["tags"]) if row_dict["tags"] else tuple(),
                        created_at=self._ensure_timezone(row_dict["created_at"]),
                        updated_at=self._ensure_timezone(row_dict["updated_at"]),
                        deleted_at=self._ensure_timezone(row_dict["deleted_at"]) if row_dict.get("deleted_at") else None,
                        parent_transaction_id=UUID(row_dict["parent_transaction_id"]) if row_dict.get("parent_transaction_id") else None,
                    )
                    transactions.append(transaction)

            conn.close()
            return Ok(transactions)
        except Exception as e:
            return Fail(f"Failed to get transactions: {str(e)}")

    async def get_balance_snapshots(
        self, account_id: UUID | None = None, date: str | None = None
    ) -> Result[List[BalanceSnapshot]]:
        """Get balance snapshots."""
        try:
            conn = self._get_connection(read_only=True)

            query = "SELECT * FROM sys_balance_snapshots WHERE 1=1"
            params = []

            if account_id:
                query += " AND account_id = ?"
                params.append(str(account_id))

            if date:
                query += " AND DATE(snapshot_time) = ?"
                params.append(date)

            result = conn.execute(query, params).fetchall()
            columns = [desc[0] for desc in conn.description]

            balances = []
            for row in result:
                row_dict = dict(zip(columns, row))
                balance = BalanceSnapshot(
                    id=UUID(row_dict["snapshot_id"]),
                    account_id=UUID(row_dict["account_id"]),
                    balance=Decimal(str(row_dict["balance"])),
                    snapshot_time=self._ensure_timezone(row_dict["snapshot_time"]),
                    created_at=self._ensure_timezone(row_dict["created_at"]),
                    updated_at=self._ensure_timezone(row_dict["updated_at"]),
                    source=row_dict.get("source"),
                )
                balances.append(balance)

            conn.close()
            return Ok(balances)
        except Exception as e:
            return Fail(f"Failed to get balance snapshots: {str(e)}")

    async def execute_query(self, sql: str) -> Result[Dict[str, Any]]:
        """Execute a SQL query and return structured results."""
        try:
            conn = self._get_connection(read_only=True)

            result = conn.execute(sql).fetchall()
            columns = [desc[0] for desc in conn.description] if conn.description else []

            conn.close()
            return Ok(
                {
                    "columns": columns,
                    "rows": result,  # Return raw tuples, not dicts
                    "row_count": len(result),
                }
            )
        except Exception as e:
            return Fail(f"Failed to execute query: {str(e)}")

    async def execute_write_query(self, sql: str) -> Result[None]:
        """Execute SQL write query (INSERT, UPDATE, DELETE)."""
        try:
            conn = self._get_connection(read_only=False)
            conn.execute(sql)
            conn.close()
            return Ok(None)
        except Exception as e:
            return Fail(f"Failed to execute write query: {str(e)}")

    async def get_schema_info(self) -> Result[Dict[str, Any]]:
        """Get complete schema information for all tables."""
        try:
            conn = self._get_connection(read_only=True)

            # Get all tables
            tables_result = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()

            schema_info = {}

            for (table_name,) in tables_result:
                # Get column information
                columns_result = conn.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """).fetchall()

                # Get sample data
                sample_result = conn.execute(
                    f"SELECT * FROM {table_name} LIMIT 3"
                ).fetchall()
                column_names = (
                    [desc[0] for desc in conn.description] if conn.description else []
                )

                schema_info[table_name] = {
                    "columns": [
                        {"name": col[0], "type": col[1]} for col in columns_result
                    ],
                    "sample_data": {"columns": column_names, "rows": sample_result},
                }

            conn.close()
            return Ok(schema_info)
        except Exception as e:
            return Fail(f"Failed to get schema info: {str(e)}")

    async def get_date_range_info(self) -> Result[Dict[str, Any]]:
        """Get date range information for transactions."""
        try:
            conn = self._get_connection(read_only=True)

            result = conn.execute("""
                SELECT
                    MIN(transaction_date) as earliest_date,
                    MAX(transaction_date) as latest_date,
                    COUNT(*) as total_transactions
                FROM sys_transactions
            """).fetchone()

            conn.close()

            if not result or not result[0] or not result[1]:
                return Ok(
                    {
                        "earliest_date": None,
                        "latest_date": None,
                        "total_transactions": 0,
                        "days_range": None,
                    }
                )

            earliest = result[0]
            latest = result[1]
            total = result[2]

            # Calculate date range in days
            from datetime import datetime

            if isinstance(earliest, datetime) and isinstance(latest, datetime):
                days_range = (latest - earliest).days
            else:
                days_range = None

            return Ok(
                {
                    "earliest_date": earliest,
                    "latest_date": latest,
                    "total_transactions": total,
                    "days_range": days_range,
                }
            )
        except Exception as e:
            return Fail(f"Failed to get date range info: {str(e)}")

    async def get_transaction_counts_by_fingerprint(
        self, fingerprints: List[str]
    ) -> Result[Dict[str, int]]:
        """Get count of existing transactions for each fingerprint."""
        try:
            if not fingerprints:
                return Ok({})

            conn = self._get_connection(read_only=True)

            # Build query with requested fingerprints and their counts
            fingerprints_list = ", ".join(f"'{fp}'" for fp in fingerprints)

            query = f"""
                WITH requested_fingerprints AS (
                    SELECT unnest([{fingerprints_list}]) as fingerprint
                ),
                counts AS (
                    SELECT
                        json_extract_string(external_ids, '$.fingerprint') as fingerprint,
                        COUNT(*) as count
                    FROM sys_transactions
                    WHERE json_extract_string(external_ids, '$.fingerprint') IN ({fingerprints_list})
                    GROUP BY json_extract_string(external_ids, '$.fingerprint')
                )
                SELECT
                    rf.fingerprint,
                    COALESCE(c.count, 0) as count
                FROM requested_fingerprints rf
                LEFT JOIN counts c ON rf.fingerprint = c.fingerprint
            """

            results = conn.execute(query).fetchall()
            conn.close()

            # Build dictionary mapping fingerprint -> count
            counts_dict = {row[0]: int(row[1]) for row in results}

            return Ok(counts_dict)
        except Exception as e:
            return Fail(f"Failed to get transaction counts by fingerprint: {str(e)}")

    async def upsert_integration(
        self, integration_name: str, integration_options: Dict[str, Any]
    ) -> Result[None]:
        """Upsert integration settings."""
        try:
            conn = self._get_connection()

            now = datetime.now(timezone.utc)
            conn.execute(
                """
                INSERT INTO sys_integrations (integration_name, integration_settings, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (integration_name) DO UPDATE SET
                    integration_settings = excluded.integration_settings,
                    updated_at = ?
                """,
                [integration_name, json.dumps(integration_options), now, now, now],
            )

            conn.close()
            return Ok(None)
        except Exception as e:
            return Fail(f"Failed to upsert integration: {str(e)}")

    async def list_integrations(self) -> Result[List[Dict[str, Any]]]:
        """List all integrations."""
        try:
            conn = self._get_connection(read_only=True)

            result = conn.execute(
                "SELECT integration_name, integration_settings FROM sys_integrations",
            ).fetchall()

            integrations = [
                {"integrationName": row[0], "integrationOptions": json.loads(row[1])}
                for row in result
            ]

            conn.close()
            return Ok(integrations)
        except Exception as e:
            return Fail(f"Failed to list integrations: {str(e)}")

    async def delete_integration(self, integration_name: str) -> Result[None]:
        """Delete an integration by name."""
        try:
            conn = self._get_connection()

            # Check if integration exists
            result = conn.execute(
                "SELECT 1 FROM sys_integrations WHERE integration_name = ?",
                [integration_name],
            ).fetchone()

            if not result:
                conn.close()
                return Fail(f"Integration '{integration_name}' not found")

            conn.execute(
                "DELETE FROM sys_integrations WHERE integration_name = ?",
                [integration_name],
            )

            conn.close()
            return Ok(None)
        except Exception as e:
            return Fail(f"Failed to delete integration: {str(e)}")

    async def get_integration_settings(
        self, integration_name: str
    ) -> Result[Dict[str, Any]]:
        """Get settings for a specific integration."""
        try:
            conn = self._get_connection(read_only=True)

            result = conn.execute(
                "SELECT integration_settings FROM sys_integrations WHERE integration_name = ?",
                [integration_name],
            ).fetchone()

            if not result:
                conn.close()
                return Ok({})

            settings = json.loads(result[0])
            conn.close()
            return Ok(settings)
        except Exception as e:
            return Fail(f"Failed to get integration settings: {str(e)}")

    async def get_tag_statistics(self) -> Result[Dict[str, int]]:
        """Get tag usage statistics (frequency count for each tag)."""
        try:
            conn = self._get_connection(read_only=True)

            # Query to unnest tags and count occurrences
            # DuckDB requires UNNEST in FROM clause, not SELECT
            result = conn.execute("""
                SELECT
                    tag,
                    COUNT(*) as count
                FROM sys_transactions, UNNEST(tags) as t(tag)
                GROUP BY tag
                ORDER BY count DESC
            """).fetchall()

            tag_stats = {row[0]: row[1] for row in result}
            conn.close()
            return Ok(tag_stats)
        except Exception as e:
            return Fail(f"Failed to get tag statistics: {str(e)}")

    async def get_transactions_for_tagging(
        self,
        filters: Dict[str, Any] = {},
        limit: int = 100,
        offset: int = 0,
    ) -> Result[List[Transaction]]:
        """Get transactions for tagging session."""
        try:
            conn = self._get_connection(read_only=True)

            # Build WHERE clause based on filters
            where_clauses = []
            params = []

            if filters.get("has_tags") is False:
                where_clauses.append("(tags IS NULL OR len(tags) = 0)")
            elif filters.get("has_tags") is True:
                where_clauses.append("(tags IS NOT NULL AND len(tags) > 0)")

            if filters.get("search"):
                where_clauses.append("(LOWER(description) LIKE ?)")
                params.append(f"%{filters['search'].lower()}%")

            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

            result = conn.execute(
                f"""
                SELECT
                    transaction_id,
                    account_id,
                    external_ids,
                    amount,
                    description,
                    transaction_date,
                    posted_date,
                    tags,
                    created_at,
                    updated_at,
                    deleted_at,
                    parent_transaction_id
                FROM sys_transactions
                WHERE {where_sql}
                ORDER BY transaction_date DESC
                LIMIT ? OFFSET ?
            """,
                params + [limit, offset],
            ).fetchall()

            transactions = []
            for row in result:
                transactions.append(
                    Transaction(
                        id=UUID(row[0]),
                        account_id=UUID(row[1]),
                        external_ids=json.loads(row[2]) if row[2] else {},
                        amount=Decimal(str(row[3])),
                        description=row[4],
                        transaction_date=row[5],
                        posted_date=row[6],
                        tags=tuple(row[7]) if row[7] else (),
                        created_at=self._ensure_timezone(row[8]),
                        updated_at=self._ensure_timezone(row[9]),
                        deleted_at=self._ensure_timezone(row[10]) if row[10] else None,
                        parent_transaction_id=UUID(row[11]) if row[11] else None,
                    )
                )

            conn.close()
            return Ok(transactions)
        except Exception as e:
            return Fail(f"Failed to get transactions for tagging: {str(e)}")

    async def get_transactions_by_account(
        self,
        account_id: UUID,
        order_by: str = "transaction_date DESC",
    ) -> Result[List[Transaction]]:
        """Get all transactions for a specific account."""
        try:
            conn = self._get_connection(read_only=True)

            result = conn.execute(
                f"""
                SELECT
                    transaction_id,
                    account_id,
                    external_ids,
                    amount,
                    description,
                    transaction_date,
                    posted_date,
                    tags,
                    created_at,
                    updated_at,
                    deleted_at,
                    parent_transaction_id
                FROM sys_transactions
                WHERE account_id = ?
                ORDER BY {order_by}
            """,
                [str(account_id)],
            ).fetchall()

            transactions = []
            for row in result:
                transactions.append(
                    Transaction(
                        id=UUID(row[0]),
                        account_id=UUID(row[1]),
                        external_ids=json.loads(row[2]) if row[2] else {},
                        amount=Decimal(str(row[3])),
                        description=row[4],
                        transaction_date=row[5],
                        posted_date=row[6],
                        tags=tuple(row[7]) if row[7] else (),
                        created_at=self._ensure_timezone(row[8]),
                        updated_at=self._ensure_timezone(row[9]),
                        deleted_at=self._ensure_timezone(row[10]) if row[10] else None,
                        parent_transaction_id=UUID(row[11]) if row[11] else None,
                    )
                )

            conn.close()
            return Ok(transactions)
        except Exception as e:
            return Fail(f"Failed to get transactions by account: {str(e)}")

    async def update_transaction_tags(
        self, transaction_id: UUID, tags: List[str]
    ) -> Result[Transaction]:
        """Update tags for a single transaction."""
        try:
            conn = self._get_connection()

            now = datetime.now(timezone.utc)

            # Update the transaction
            conn.execute(
                """
                UPDATE sys_transactions
                SET tags = ?, updated_at = ?
                WHERE transaction_id = ?
            """,
                [tags, now, transaction_id],
            )

            # Fetch the updated transaction
            result = conn.execute(
                """
                SELECT
                    transaction_id,
                    account_id,
                    external_ids,
                    amount,
                    description,
                    transaction_date,
                    posted_date,
                    tags,
                    created_at,
                    updated_at,
                    deleted_at,
                    parent_transaction_id
                FROM sys_transactions
                WHERE transaction_id = ?
            """,
                [transaction_id],
            ).fetchone()

            if not result:
                conn.close()
                return Fail(f"Transaction {transaction_id} not found")

            transaction = Transaction(
                id=UUID(result[0]),
                account_id=UUID(result[1]),
                external_ids=MappingProxyType(
                    json.loads(result[2]) if result[2] else {}
                ),
                amount=Decimal(str(result[3])),
                description=result[4],
                transaction_date=result[5],
                posted_date=result[6],
                tags=tuple(result[7]) if result[7] else (),
                created_at=self._ensure_timezone(result[8]),
                updated_at=self._ensure_timezone(result[9]),
                deleted_at=self._ensure_timezone(result[10]) if result[10] else None,
                parent_transaction_id=UUID(result[11]) if result[11] else None,
            )

            conn.close()
            return Ok(transaction)
        except Exception as e:
            return Fail(f"Failed to update transaction tags: {str(e)}")

    async def get_enabled_tag_rules(self) -> Result[List[Dict[str, Any]]]:
        """Get all enabled auto-tagging rules."""
        try:
            conn = self._get_connection(read_only=True)

            result = conn.execute("""
                SELECT rule_id, name, sql_condition, tags
                FROM sys_plugin_transactions_rules
                WHERE enabled = true
                ORDER BY sort_order ASC
            """).fetchall()

            rules = []
            for row in result:
                rules.append({
                    "rule_id": row[0],
                    "name": row[1],
                    "sql_condition": row[2],
                    "tags": list(row[3]) if row[3] else [],
                })

            conn.close()
            return Ok(rules)
        except Exception as e:
            # Table might not exist yet - return empty list
            if "does not exist" in str(e).lower():
                return Ok([])
            return Fail(f"Failed to get enabled tag rules: {str(e)}")

    async def apply_tags_to_transactions(
        self,
        transaction_ids: List[UUID],
        sql_condition: str,
        tags: List[str],
    ) -> Result[int]:
        """Apply tags to transactions matching a condition.

        Only updates transactions in the provided list that match the condition
        and don't already have all the specified tags.
        """
        try:
            if not transaction_ids or not sql_condition or not tags:
                return Ok(0)

            conn = self._get_connection(read_only=False)

            # Build ID filter
            id_placeholders = ", ".join(f"'{tid}'" for tid in transaction_ids)
            id_filter = f"transaction_id IN ({id_placeholders})"

            # Combine rule condition with transaction ID filter
            combined_condition = f"({sql_condition}) AND {id_filter}"

            # Build tags literal for DuckDB list operations
            tags_literal = "[" + ", ".join(f"'{t}'" for t in tags) + "]"

            # Update matching transactions to merge tags
            # DuckDB: list_distinct(list_concat(existing, new))
            update_query = f"""
                UPDATE sys_transactions
                SET tags = list_distinct(list_concat(
                    COALESCE(tags, []),
                    {tags_literal}
                ))
                WHERE {combined_condition}
                AND NOT list_has_all(COALESCE(tags, []), {tags_literal})
            """

            conn.execute(update_query)
            conn.close()

            # DuckDB doesn't return affected row count easily, return -1 to indicate success
            return Ok(-1)
        except Exception as e:
            return Fail(f"Failed to apply tags to transactions: {str(e)}")

    async def compact(self) -> Result[Dict[str, Any]]:
        """Compact the database to reclaim space from deleted rows.

        Uses DuckDB's EXPORT/IMPORT DATABASE to create a fresh, optimized copy.
        This approach handles foreign key constraints properly by generating
        SQL scripts that respect table dependencies.

        For encrypted databases, exports to unencrypted parquet files (in temp dir),
        then re-imports into a new encrypted database.
        """
        import shutil
        import tempfile

        try:
            # Get original file size
            if not self.db_path.exists():
                return Fail("Database file not found")

            original_size = self.db_path.stat().st_size

            # Create temp directory for export and temp file for new database
            with tempfile.TemporaryDirectory() as export_dir:
                fd, temp_db_str = tempfile.mkstemp(suffix=".duckdb")
                os.close(fd)
                temp_db_path = Path(temp_db_str)
                temp_db_path.unlink()  # Remove so DuckDB creates fresh

                try:
                    # Export original database to parquet files (more efficient)
                    # Use _get_connection for encryption support
                    conn = self._get_connection(read_only=True)
                    conn.execute(f"EXPORT DATABASE '{export_dir}' (FORMAT PARQUET)")
                    conn.close()

                    # Import into fresh database
                    # For encrypted DBs, need to re-encrypt the new database
                    if self._encryption_key:
                        # Create new encrypted database
                        new_conn = duckdb.connect(":memory:")
                        new_conn.execute(
                            f"ATTACH '{temp_db_path}' AS new_db (ENCRYPTION_KEY '{self._encryption_key}')"
                        )
                        new_conn.execute("USE new_db")
                        new_conn.execute(f"IMPORT DATABASE '{export_dir}'")
                        new_conn.close()
                    else:
                        # Unencrypted - direct import
                        new_conn = duckdb.connect(str(temp_db_path))
                        new_conn.execute(f"IMPORT DATABASE '{export_dir}'")
                        new_conn.close()

                    # Get compacted file size
                    compacted_size = temp_db_path.stat().st_size

                    # Replace original with compacted version
                    shutil.move(str(temp_db_path), str(self.db_path))

                    return Ok({
                        "original_size": original_size,
                        "compacted_size": compacted_size,
                    })

                except Exception as e:
                    # Clean up temp db file on error
                    if temp_db_path.exists():
                        temp_db_path.unlink()
                    raise e

        except Exception as e:
            return Fail(f"Failed to compact database: {str(e)}")

