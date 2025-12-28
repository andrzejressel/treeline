"""Database repository abstraction."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List
from uuid import UUID

from treeline.domain import Account, BalanceSnapshot, Result, Transaction


class Repository(ABC):
    @abstractmethod
    async def ensure_db_exists(self) -> Result:
        """Ensure database file exists. Does not require user context."""
        pass

    @abstractmethod
    async def ensure_schema_upgraded(self) -> Result:
        """Ensure schema is up to date. Does not require user context."""
        pass

    @abstractmethod
    async def add_account(self, account: Account) -> Result[Account]:
        pass

    @abstractmethod
    async def add_transaction(self, transaction: Transaction) -> Result[Transaction]:
        pass

    @abstractmethod
    async def add_balance(self, balance: BalanceSnapshot) -> Result[BalanceSnapshot]:
        pass

    @abstractmethod
    async def bulk_upsert_accounts(
        self, accounts: List[Account]
    ) -> Result[List[Account]]:
        pass

    @abstractmethod
    async def bulk_upsert_transactions(
        self, transactions: List[Transaction]
    ) -> Result[List[Transaction]]:
        pass

    @abstractmethod
    async def bulk_add_balances(
        self, balances: List[BalanceSnapshot]
    ) -> Result[List[BalanceSnapshot]]:
        pass

    @abstractmethod
    async def update_account_by_id(self, account: Account) -> Result[Account]:
        pass

    @abstractmethod
    async def get_accounts(self) -> Result[List[Account]]:
        pass

    @abstractmethod
    async def get_account_by_id(self, account_id: UUID) -> Result[Account]:
        pass

    @abstractmethod
    async def get_account_by_external_id(self, external_id: str) -> Result[Account]:
        pass

    @abstractmethod
    async def get_transactions_by_external_ids(
        self, external_ids: List[Dict[str, str]]
    ) -> Result[List[Transaction]]:
        pass

    @abstractmethod
    async def get_balance_snapshots(
        self, account_id: UUID | None = None, date: str | None = None
    ) -> Result[List[BalanceSnapshot]]:
        pass

    @abstractmethod
    async def execute_query(self, sql: str) -> Result[Dict[str, Any]]:
        """
        Execute SQL query and return structured results.

        Args:
            sql: SQL query to execute

        Returns:
            Result containing dict with:
              - "columns": List[str] - column names
              - "rows": List[tuple] - result rows
              - "row_count": int - number of rows
        """
        pass

    @abstractmethod
    async def execute_write_query(self, sql: str) -> Result[None]:
        """
        Execute SQL write query (INSERT, UPDATE, DELETE).

        Args:
            sql: SQL query to execute (can be multiple statements separated by ;)

        Returns:
            Result indicating success or failure
        """
        pass

    @abstractmethod
    async def get_schema_info(self) -> Result[Dict[str, Any]]:
        """
        Get complete schema information for all tables.

        Returns:
            Result containing dict with table information
        """
        pass

    @abstractmethod
    async def get_tag_statistics(self) -> Result[Dict[str, int]]:
        """
        Get tag usage statistics (frequency count for each tag).

        Returns:
            Result containing dict mapping tag names to usage counts
            Example: {"groceries": 50, "dining": 30, "transport": 20}
        """
        pass

    @abstractmethod
    async def get_transactions_for_tagging(
        self,
        filters: Dict[str, Any] = {},
        limit: int = 100,
        offset: int = 0,
    ) -> Result[List[Transaction]]:
        """
        Get transactions for tagging session.

        Args:
            filters: Optional filters (e.g., {"has_tags": False} for untagged only)
            limit: Maximum number of transactions to return
            offset: Number of transactions to skip (for pagination)

        Returns:
            Result containing list of Transaction objects
        """
        pass

    @abstractmethod
    async def update_transaction_tags(
        self, transaction_id: UUID, tags: List[str]
    ) -> Result[Transaction]:
        """
        Update tags for a single transaction.

        Args:
            transaction_id: Transaction ID to update
            tags: New list of tags (replaces existing tags)

        Returns:
            Result containing updated Transaction object
        """
        pass

    @abstractmethod
    async def get_date_range_info(self) -> Result[Dict[str, Any]]:
        """
        Get date range information for transactions.

        Returns:
            Result containing dict with earliest_date, latest_date, total_transactions, days_range
        """
        pass

    @abstractmethod
    async def get_transaction_counts_by_fingerprint(
        self, fingerprints: List[str]
    ) -> Result[Dict[str, int]]:
        """
        Get count of existing transactions for each fingerprint.

        Args:
            fingerprints: List of fingerprint strings to check

        Returns:
            Result containing dict mapping fingerprint -> count
            Example: {"fingerprint:abc123": 5, "fingerprint:def456": 3}
        """
        pass

    @abstractmethod
    async def get_transactions_by_account(
        self,
        account_id: UUID,
        order_by: str = "transaction_date DESC",
    ) -> Result[List[Transaction]]:
        """
        Get all transactions for a specific account.

        Args:
            account_id: Account ID to filter by
            order_by: SQL order clause (default: transaction_date DESC)

        Returns:
            Result containing list of Transaction objects ordered as specified
        """
        pass

    @abstractmethod
    async def upsert_integration(
        self, integration_name: str, integration_options: Dict[str, Any]
    ) -> Result[None]:
        pass

    @abstractmethod
    async def list_integrations(self) -> Result[List[Dict[str, Any]]]:
        pass

    @abstractmethod
    async def delete_integration(self, integration_name: str) -> Result[None]:
        pass

    @abstractmethod
    async def get_integration_settings(
        self, integration_name: str
    ) -> Result[Dict[str, Any]]:
        pass

    @abstractmethod
    async def compact(self) -> Result[Dict[str, Any]]:
        """Compact the database to reclaim space from deleted rows.

        Creates a fresh copy of the database, eliminating fragmentation
        and unused space.

        Returns:
            Result containing dict with:
              - "original_size": int - size in bytes before compaction
              - "compacted_size": int - size in bytes after compaction
        """
        pass

    @abstractmethod
    async def get_enabled_tag_rules(self) -> Result[List[Dict[str, Any]]]:
        """Get all enabled auto-tagging rules.

        Returns:
            Result containing list of rule dicts with:
              - rule_id: str
              - name: str
              - sql_condition: str
              - tags: List[str]
        """
        pass

    @abstractmethod
    async def apply_tags_to_transactions(
        self,
        transaction_ids: List[UUID],
        sql_condition: str,
        tags: List[str],
    ) -> Result[int]:
        """Apply tags to transactions matching a condition.

        Only updates transactions in the provided list that match the condition
        and don't already have all the specified tags.

        Args:
            transaction_ids: List of transaction IDs to consider
            sql_condition: SQL WHERE clause condition
            tags: Tags to add to matching transactions

        Returns:
            Result containing count of transactions updated
        """
        pass
