"""Service for retrieving financial data status and summaries."""

from typing import Any, Dict

from treeline.abstractions import Repository
from treeline.domain import Result


class StatusService:
    """Service for retrieving financial data status and summaries."""

    def __init__(self, repository: Repository):
        self.repository = repository

    async def get_status(self) -> Result[Dict[str, Any]]:
        """Get financial data status summary."""
        # Get accounts
        accounts_result = await self.repository.get_accounts()
        if not accounts_result.success:
            return accounts_result

        accounts = accounts_result.data or []

        # Get integrations
        integrations_result = await self.repository.list_integrations()
        if not integrations_result.success:
            return integrations_result

        integrations = integrations_result.data or []

        # Query for transaction stats
        transaction_stats_query = """
            SELECT
                COUNT(*) as total_transactions,
                MIN(transaction_date) as earliest_date,
                MAX(transaction_date) as latest_date
            FROM transactions
        """
        stats_result = await self.repository.execute_query(transaction_stats_query)

        if not stats_result.success:
            return stats_result

        transaction_stats = stats_result.data
        rows = transaction_stats.get("rows", [])

        if rows and len(rows) > 0:
            # Rows are tuples, use column indices
            row = rows[0]
            total_transactions = row[0] if len(row) > 0 else 0  # COUNT(*)
            earliest_date = row[1] if len(row) > 1 else None  # MIN(transaction_date)
            latest_date = row[2] if len(row) > 2 else None  # MAX(transaction_date)
        else:
            total_transactions = 0
            earliest_date = None
            latest_date = None

        # Query for balance snapshots
        balance_query = "SELECT COUNT(*) as total_snapshots FROM balance_snapshots"
        balance_result = await self.repository.execute_query(balance_query)

        if not balance_result.success:
            total_snapshots = 0
        else:
            balance_data = balance_result.data
            balance_rows = balance_data.get("rows", [])
            # Rows are tuples, access by index
            total_snapshots = (
                balance_rows[0][0] if balance_rows and len(balance_rows) > 0 else 0
            )

        # Return both full data (for display) and summary (for JSON)
        integration_names = [i["integrationName"] for i in integrations]

        return Result(
            success=True,
            data={
                # Full data for display functions
                "accounts": accounts,
                "integrations": integrations,
                # Summary counts
                "total_accounts": len(accounts),
                "total_transactions": total_transactions,
                "total_snapshots": total_snapshots,
                "total_integrations": len(integrations),
                "integration_names": integration_names,
                # Date range
                "earliest_date": str(earliest_date) if earliest_date else None,
                "latest_date": str(latest_date) if latest_date else None,
            },
        )
