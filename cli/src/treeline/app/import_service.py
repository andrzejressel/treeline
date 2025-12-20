"""Service for one-time bulk imports from files or external sources."""

from datetime import datetime, timezone
from typing import Any, Dict, List
from uuid import UUID

from treeline.abstractions import DataAggregationProvider, Repository
from treeline.domain import Result, Transaction


class ImportService:
    """Service for one-time bulk imports from files or external sources."""

    def __init__(
        self,
        repository: Repository,
        provider_registry: Dict[str, DataAggregationProvider],
    ):
        self.repository = repository
        self.provider_registry = provider_registry

    async def import_transactions(
        self,
        source_type: str,
        account_id: UUID,
        source_options: Dict[str, Any],
    ) -> Result[Dict[str, Any]]:
        """Import transactions from a one-time source using fingerprint deduplication.

        Args:
            source_type: Type of import source ("csv", "ynab", etc.)
            account_id: Treeline account to import transactions into
            source_options: Provider-specific options (e.g., {"file_path": "/path/to/file.csv"})

        Returns:
            Result with stats: {"discovered": 150, "imported": 120, "skipped": 30}
        """
        # Get provider
        provider = self.provider_registry.get(source_type.lower())
        if not provider:
            return Result(success=False, error=f"Unknown source type: {source_type}")

        # Get discovered transactions from source
        discovered_result = await provider.get_transactions(
            start_date=datetime.min,
            end_date=datetime.now(timezone.utc),
            provider_account_ids=[],
            provider_settings=source_options,
        )
        if not discovered_result.success:
            return discovered_result

        discovered_transactions = discovered_result.data or []

        # Map all transactions to the specified account
        # Note: Reconstruct transactions to recalculate fingerprint with new account_id
        mapped_transactions = []
        for tx in discovered_transactions:
            tx_dict = tx.model_dump()
            tx_dict["account_id"] = account_id
            # Remove fingerprint from external_ids to force regeneration with new account_id
            ext_ids = dict(tx_dict.get("external_ids", {}))
            ext_ids.pop("fingerprint", None)
            tx_dict["external_ids"] = ext_ids
            mapped_transactions.append(Transaction(**tx_dict))

        # Group by fingerprint (fingerprint is auto-set in external_ids by domain model)
        discovered_by_fingerprint: Dict[str, List[Transaction]] = {}
        for tx in mapped_transactions:
            fingerprint = tx.external_ids.get("fingerprint")
            if fingerprint:
                discovered_by_fingerprint.setdefault(fingerprint, []).append(tx)

        # Query existing counts per fingerprint
        fingerprints = list(discovered_by_fingerprint.keys())
        existing_counts_result = (
            await self.repository.get_transaction_counts_by_fingerprint(fingerprints)
        )
        if not existing_counts_result.success:
            return existing_counts_result

        existing_counts = existing_counts_result.data or {}

        # Determine which transactions to import
        transactions_to_import = []
        skipped_transactions = []  # Track skipped for debugging
        skipped_count = 0

        for fingerprint, discovered_txs in discovered_by_fingerprint.items():
            existing_count = existing_counts.get(fingerprint, 0)
            discovered_count = len(discovered_txs)

            # Import the difference (if any new transactions)
            new_count = max(0, discovered_count - existing_count)
            transactions_to_import.extend(discovered_txs[:new_count])

            # Track skipped transactions with their fingerprint and existing count
            skipped_txs = discovered_txs[new_count:]
            for tx in skipped_txs:
                skipped_transactions.append(
                    {
                        "transaction": tx,
                        "fingerprint": fingerprint,
                        "existing_count": existing_count,
                    }
                )
            skipped_count += discovered_count - new_count

        # Bulk insert (not upsert, these are all new)
        if transactions_to_import:
            import_result = await self.repository.bulk_upsert_transactions(
                transactions_to_import
            )
            if not import_result.success:
                return import_result

        return Result(
            success=True,
            data={
                "discovered": len(discovered_transactions),
                "imported": len(transactions_to_import),
                "skipped": skipped_count,
                "fingerprints_checked": len(fingerprints),
                "imported_transactions": transactions_to_import,
                "skipped_transactions": skipped_transactions,
            },
        )

    async def detect_columns(
        self, source_type: str, file_path: str
    ) -> Result[Dict[str, Any]]:
        """Detect columns automatically for import.

        Args:
            source_type: Type of import source ("csv", etc.)
            file_path: Path to file

        Returns:
            Result with column mapping dict like {"date": "Date", "amount": "Amount", ...}
        """
        # Get provider
        provider = self.provider_registry.get(source_type)
        if not provider:
            return Result(success=False, error=f"{source_type} provider not available")

        # Call provider-specific detection method
        return provider.detect_columns(file_path)

    async def preview_csv_import(
        self,
        file_path: str,
        column_mapping: Dict[str, str],
        date_format: str = "auto",
        limit: int = 5,
        flip_signs: bool = False,
        debit_negative: bool = False,
    ) -> Result[List[Transaction]]:
        """Preview transactions from CSV file before importing.

        Args:
            file_path: Path to CSV file
            column_mapping: Mapping of standard fields to CSV columns
            date_format: Date format string or "auto"
            limit: Maximum number of transactions to preview
            flip_signs: Whether to flip signs (for credit card statements)
            debit_negative: Whether to negate debit amounts (for unsigned debit/credit CSVs)

        Returns:
            Result with list of preview Transaction objects
        """
        # Get CSV provider
        provider = self.provider_registry.get("csv")
        if not provider:
            return Result(success=False, error="CSV provider not available")

        # Call provider-specific preview method
        return provider.preview_transactions(
            file_path, column_mapping, date_format, limit, flip_signs, debit_negative
        )
