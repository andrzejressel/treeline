"""Service for synchronizing financial data from providers."""

from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, TYPE_CHECKING

from treeline.abstractions import DataAggregationProvider, Repository
from treeline.domain import Result, Transaction

if TYPE_CHECKING:
    from treeline.app.account_service import AccountService
    from treeline.app.integration_service import IntegrationService
    from treeline.app.preferences_service import PreferencesService


class SyncService:
    """Service for synchronizing financial data from providers."""

    def __init__(
        self,
        provider_registry: Dict[str, DataAggregationProvider],
        repository: Repository,
        account_service: "AccountService",
        integration_service: "IntegrationService",
        preferences_service: "PreferencesService",
    ):
        self.provider_registry = provider_registry
        self.repository = repository
        self.account_service = account_service
        self.integration_service = integration_service
        self.preferences_service = preferences_service

    def _get_provider(self, integration_name: str) -> DataAggregationProvider | None:
        """Get the provider for a given integration name."""
        return self.provider_registry.get(integration_name.lower())

    async def sync_accounts(
        self, integration_name: str, provider_options: Dict[str, Any]
    ) -> Result[Dict[str, Any]]:
        """Sync accounts from a data provider."""
        data_provider = self._get_provider(integration_name)
        if not data_provider:
            return Result(
                success=False, error=f"Unknown integration: {integration_name}"
            )

        if not data_provider.can_get_accounts:
            return Result(success=False, error="Provider does not support accounts")

        integration_name_lower = integration_name.lower()

        # Get existing accounts to map external IDs
        existing_accounts_result = await self.repository.get_accounts()
        if not existing_accounts_result.success:
            return existing_accounts_result

        existing_accounts = existing_accounts_result.data or []

        # Get discovered accounts from provider
        discovered_result = await data_provider.get_accounts(
            provider_account_ids=[], provider_settings=provider_options
        )
        if not discovered_result.success:
            return discovered_result

        # Handle new response format: {"accounts": [...], "errors": [...]}
        # or old format: List[Account]
        result_data = discovered_result.data or {}
        if isinstance(result_data, dict):
            discovered_accounts = result_data.get("accounts", [])
            provider_errors = result_data.get("errors", [])
        else:
            # Backwards compatibility with providers that return List[Account]
            discovered_accounts = result_data
            provider_errors = []

        # Currency validation: filter accounts by user's configured currency
        user_currency_result = self.preferences_service.get_currency()
        user_currency = user_currency_result.data if user_currency_result.success else "USD"

        currency_warnings = []
        currency_filtered_accounts = []
        for account in discovered_accounts:
            account_currency = account.currency or "USD"
            if account_currency.upper() != user_currency.upper():
                # Warn about currency mismatch, skip the account
                currency_warnings.append(
                    f"Skipped '{account.name}' ({account.institution_name or 'Unknown institution'}): "
                    f"Account currency is {account_currency}, but your configured currency is {user_currency}. "
                    f"Multi-currency is not yet supported."
                )
            else:
                currency_filtered_accounts.append(account)

        # Add currency warnings to provider errors for display
        provider_errors.extend(currency_warnings)
        discovered_accounts = currency_filtered_accounts

        # Map discovered accounts to existing accounts by external ID
        updated_accounts = []
        new_accounts = []  # Track newly discovered accounts
        for discovered_account in discovered_accounts:
            matched = False
            for existing_account in existing_accounts:
                disc_ext_id = discovered_account.external_ids.get(
                    integration_name_lower
                )
                exist_ext_id = existing_account.external_ids.get(integration_name_lower)

                if disc_ext_id and exist_ext_id and disc_ext_id == exist_ext_id:
                    # Update discovered account to use existing ID
                    updated_account = discovered_account.model_copy(
                        update={"id": existing_account.id}
                    )
                    updated_accounts.append(updated_account)
                    matched = True
                    break

            if not matched:
                # This is a new account
                updated_accounts.append(discovered_account)
                new_accounts.append(discovered_account)

        discovered_accounts = updated_accounts

        # Bulk upsert accounts
        ingested_result = await self.repository.bulk_upsert_accounts(
            discovered_accounts
        )
        if not ingested_result.success:
            return ingested_result

        # Create balance snapshots for accounts with balances
        # Use AccountService to leverage deduplication logic
        for account in discovered_accounts:
            if account.id and account.balance is not None:
                # Call AccountService to add balance snapshot (handles deduplication)
                # Continue on failure - don't halt sync for balance snapshot issues
                await self.account_service.add_balance_snapshot(
                    account_id=account.id,
                    balance=account.balance,
                    snapshot_date=None,  # Defaults to today
                    source="sync",
                )

        return Result(
            success=True,
            data={
                "discovered_accounts": discovered_accounts,
                "ingested_accounts": ingested_result.data,
                "new_accounts": new_accounts,  # Accounts that didn't exist before
                "provider_errors": provider_errors,  # Errors from SimpleFIN (e.g., "You must reauthenticate")
            },
        )

    async def sync_transactions(
        self,
        integration_name: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        provider_options: Dict[str, Any] | None = None,
        dry_run: bool = False,
    ) -> Result[Dict[str, Any]]:
        """Sync transactions from a data provider."""
        data_provider = self._get_provider(integration_name)
        if not data_provider:
            return Result(
                success=False, error=f"Unknown integration: {integration_name}"
            )

        if not data_provider.can_get_transactions:
            return Result(success=False, error="Provider does not support transactions")

        integration_name_lower = integration_name.lower()

        # Get existing accounts to map provider account IDs
        accounts_result = await self.repository.get_accounts()
        if not accounts_result.success:
            return accounts_result

        accounts = accounts_result.data or []

        # Get integration settings to check for balances-only accounts
        integration_settings = provider_options or {}
        account_settings = integration_settings.get("accountSettings", {})

        # Get provider account IDs from existing accounts
        # Exclude accounts marked as balancesOnly in integration settings
        provider_account_ids = []
        for acc in accounts:
            provider_acc_id = acc.external_ids.get(integration_name_lower)
            if provider_acc_id:
                # Check if this account is marked as balances-only
                acc_settings = account_settings.get(provider_acc_id, {})
                if not acc_settings.get("balancesOnly", False):
                    provider_account_ids.append(provider_acc_id)

        # Get discovered transactions
        discovered_result = await data_provider.get_transactions(
            start_date,
            end_date,
            provider_account_ids=provider_account_ids,
            provider_settings=provider_options or {},
        )
        if not discovered_result.success:
            return discovered_result

        # Handle new response format: {"transactions": [...], "errors": [...]}
        # or old format: List[...]
        result_data = discovered_result.data or {}
        if isinstance(result_data, dict):
            discovered_data = result_data.get("transactions", [])
            provider_errors = result_data.get("errors", [])
        else:
            # Backwards compatibility with providers that return List
            discovered_data = result_data
            provider_errors = []

        # Map provider account IDs to internal account IDs
        account_id_map = {
            acc.external_ids.get(integration_name_lower): acc.id
            for acc in accounts
            if acc.external_ids.get(integration_name_lower)
        }

        mapped_transactions = []

        # Handle provider-specific return formats
        # SimpleFIN returns: List[(provider_account_id, Transaction)]
        # CSV returns: List[Transaction] (account_id already set)
        for item in discovered_data:
            if isinstance(item, tuple):
                # Format: (provider_account_id, transaction)
                provider_acc_id, tx = item
                internal_acc_id = account_id_map.get(provider_acc_id)

                # Skip transactions with unmapped accounts
                if not internal_acc_id:
                    continue

                # Reconstruct transaction with correct account_id to force fingerprint recalculation
                # Note: model_copy doesn't trigger @model_validator, so we use model_dump + reconstruct
                tx_dict = tx.model_dump()
                tx_dict["account_id"] = internal_acc_id

                # Remove old fingerprint to force recalculation
                if "fingerprint" in tx_dict["external_ids"]:
                    cleaned_external_ids = {
                        k: v
                        for k, v in tx_dict["external_ids"].items()
                        if k != "fingerprint"
                    }
                    tx_dict["external_ids"] = cleaned_external_ids

                # Reconstruct - this triggers @model_validator which auto-generates correct fingerprint
                mapped_tx = Transaction(**tx_dict)
                mapped_transactions.append(mapped_tx)
            else:
                # Format: Transaction (account_id already set, e.g., from CSV)
                mapped_transactions.append(item)

        # Get existing transactions by external IDs to check for duplicates
        external_id_objects = [
            {integration_name_lower: tx.external_ids.get(integration_name_lower)}
            for tx in mapped_transactions
            if tx.external_ids.get(integration_name_lower)
        ]

        existing_txs: List[Transaction] = []
        if external_id_objects:
            existing_result = await self.repository.get_transactions_by_external_ids(
                external_id_objects
            )
            if existing_result.success:
                existing_txs = existing_result.data or []

        # Build map of existing transactions by external ID
        existing_by_ext_id = {
            tx.external_ids.get(integration_name_lower): tx
            for tx in existing_txs
            if tx.external_ids.get(integration_name_lower)
        }

        # Separate new vs skipped transactions
        # IMPORTANT: Skip existing transactions to preserve user-added data like tags
        transactions_to_insert = []
        new_count = 0
        skipped_count = 0

        for discovered_tx in mapped_transactions:
            ext_id = discovered_tx.external_ids.get(integration_name_lower)
            if ext_id and ext_id in existing_by_ext_id:
                # Skip: transaction already exists, preserve user data (tags, etc.)
                skipped_count += 1
            else:
                transactions_to_insert.append(discovered_tx)
                new_count += 1

        # Bulk insert only new transactions (unless dry-run)
        if dry_run:
            # In dry-run mode, don't actually insert
            ingested_transactions = transactions_to_insert
        else:
            ingested_result = await self.repository.bulk_upsert_transactions(
                transactions_to_insert
            )
            if not ingested_result.success:
                return ingested_result
            ingested_transactions = ingested_result.data

        return Result(
            success=True,
            data={
                "discovered_transactions": mapped_transactions,
                "ingested_transactions": ingested_transactions,
                "stats": {
                    "discovered": len(mapped_transactions),
                    "new": new_count,
                    "skipped": skipped_count,
                },
                "provider_errors": provider_errors,
            },
        )

    async def sync_balances(
        self, integration_name: str, provider_options: Dict[str, Any]
    ) -> Result[Dict[str, Any]]:
        """Sync balance snapshots from a data provider.

        NOTE: This method is deprecated. Balance snapshots are now created automatically
        during sync_accounts based on the balance field in the Account model.
        """
        return Result(
            success=False,
            error="sync_balances is deprecated - balances are synced automatically during sync_accounts",
        )

    async def _calculate_sync_date_range(self) -> Result[Dict[str, datetime]]:
        """Calculate the date range for syncing transactions."""
        end_date = datetime.now(timezone.utc)

        # Query for the latest transaction date
        max_date_query = """
            SELECT MAX(transaction_date) as max_date
            FROM transactions
        """
        max_date_result = await self.repository.execute_query(max_date_query)

        if not max_date_result.success:
            # Fallback to 90 days if query fails
            return Result(
                success=True,
                data={
                    "start_date": end_date - timedelta(days=90),
                    "end_date": end_date,
                    "sync_type": "initial",
                },
            )

        rows = max_date_result.data.get("rows", [])
        if rows and rows[0][0]:
            # Incremental sync: start from last transaction date minus 7 days overlap
            max_date = rows[0][0]

            # Convert date to datetime if needed, and ensure timezone-aware
            if isinstance(max_date, date) and not isinstance(max_date, datetime):
                # DATE column - convert to datetime at midnight UTC
                max_date = datetime.combine(
                    max_date, datetime.min.time(), tzinfo=timezone.utc
                )
            elif isinstance(max_date, datetime):
                # TIMESTAMP column - ensure timezone-aware
                if max_date.tzinfo is None:
                    max_date = max_date.replace(tzinfo=timezone.utc)

            start_date = max_date - timedelta(days=7)
            return Result(
                success=True,
                data={
                    "start_date": start_date,
                    "end_date": end_date,
                    "sync_type": "incremental",
                },
            )

        # Initial sync: fetch last 90 days
        return Result(
            success=True,
            data={
                "start_date": end_date - timedelta(days=90),
                "end_date": end_date,
                "sync_type": "initial",
            },
        )

    async def sync_all_integrations(
        self, dry_run: bool = False
    ) -> Result[Dict[str, Any]]:
        """Sync all configured integrations for a user."""
        # Get integrations from IntegrationService
        integrations_result = await self.integration_service.get_integrations()
        if not integrations_result.success:
            return integrations_result

        integrations = integrations_result.data or []

        if not integrations:
            return Result(success=False, error="No integrations configured")

        sync_results = []
        all_new_accounts = []  # Track all new accounts across integrations

        for integration in integrations:
            integration_name = integration["integrationName"]
            integration_options = integration["integrationOptions"]

            # Sync accounts (skip in dry-run since we don't save them anyway)
            provider_errors = []
            if not dry_run:
                accounts_result = await self.sync_accounts(
                    integration_name, integration_options
                )

                if not accounts_result.success:
                    sync_results.append(
                        {
                            "integration": integration_name,
                            "accounts_synced": 0,
                            "transactions_synced": 0,
                            "error": accounts_result.error,
                        }
                    )
                    continue

                num_accounts = len(accounts_result.data.get("ingested_accounts", []))
                new_accounts = accounts_result.data.get("new_accounts", [])
                provider_errors.extend(accounts_result.data.get("provider_errors", []))
                # Collect new accounts that don't have account_type set
                for account in new_accounts:
                    if account.account_type is None:
                        all_new_accounts.append(account)
            else:
                num_accounts = 0  # Don't sync accounts in dry-run

            # Calculate date range for transactions
            date_range_result = await self._calculate_sync_date_range()
            if not date_range_result.success:
                sync_results.append(
                    {
                        "integration": integration_name,
                        "accounts_synced": num_accounts,
                        "transactions_synced": 0,
                        "error": "Failed to calculate sync date range",
                    }
                )
                continue

            date_range = date_range_result.data

            # Sync transactions
            transactions_result = await self.sync_transactions(
                integration_name,
                start_date=date_range["start_date"],
                end_date=date_range["end_date"],
                provider_options=integration_options,
                dry_run=dry_run,
            )

            if not transactions_result.success:
                sync_results.append(
                    {
                        "integration": integration_name,
                        "accounts_synced": num_accounts,
                        "transactions_synced": 0,
                        "sync_type": date_range["sync_type"],
                        "error": transactions_result.error,
                    }
                )
                continue

            num_transactions = len(
                transactions_result.data.get("ingested_transactions", [])
            )
            tx_stats = transactions_result.data.get("stats", {})
            provider_errors.extend(transactions_result.data.get("provider_errors", []))

            sync_results.append(
                {
                    "integration": integration_name,
                    "accounts_synced": num_accounts,
                    "transactions_synced": num_transactions,
                    "transaction_stats": tx_stats,
                    "sync_type": date_range["sync_type"],
                    "start_date": date_range["start_date"],
                    "end_date": date_range["end_date"],
                    "provider_warnings": provider_errors,
                }
            )

        return Result(
            success=True,
            data={
                "results": sync_results,
                "new_accounts_without_type": all_new_accounts,
            },
        )
