"""Service for backfilling historical data."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List
from uuid import UUID, uuid4

from treeline.abstractions import Repository
from treeline.domain import BalanceSnapshot, Ok, Fail, Result


class BackfillService:
    """Service for backfilling balance snapshots."""

    def __init__(self, repository: Repository):
        self.repository = repository

    async def backfill_balances(
        self,
        account_ids: List[UUID] | None = None,
        days: int | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> Result[Dict[str, Any]]:
        """Calculate historical balance snapshots from transactions.

        Walks backward from the latest balance snapshot using transaction history.
        Only creates snapshots for dates that don't already have one.

        Args:
            account_ids: Specific accounts (None = all accounts)
            days: Limit to last N days of history
            dry_run: Preview without saving
            verbose: Detailed output

        Returns:
            Result with stats: {
                "accounts_processed": int,
                "snapshots_created": int,
                "snapshots_skipped": int,
                "warnings": List[str],
                "verbose_logs": List[str],
                "dry_run": bool
            }
        """
        try:
            # Get accounts
            accounts_result = await self.repository.get_accounts()
            if not accounts_result.success:
                return Fail(f"Failed to get accounts: {accounts_result.error}")

            all_accounts = accounts_result.data

            # Filter by account_ids if specified
            if account_ids:
                accounts = [a for a in all_accounts if a.id in account_ids]
                if not accounts:
                    return Fail(
                        f"No accounts found matching IDs: {', '.join(str(id) for id in account_ids)}"
                    )
            else:
                accounts = all_accounts

            # Process each account
            accounts_processed = 0
            total_snapshots_created = 0
            total_snapshots_skipped = 0
            warnings: List[str] = []
            verbose_logs: List[str] = []

            for account in accounts:
                accounts_processed += 1

                # Get latest balance snapshot (required)
                snapshots_result = await self.repository.get_balance_snapshots(
                    account.id
                )
                if not snapshots_result.success:
                    warnings.append(
                        f"Account {account.name}: Failed to get snapshots - {snapshots_result.error}"
                    )
                    continue

                existing_snapshots = snapshots_result.data
                if not existing_snapshots:
                    warnings.append(
                        f"Account {account.name}: No balance snapshots found - cannot backfill without starting point"
                    )
                    continue

                # Find latest snapshot
                latest_snapshot = max(existing_snapshots, key=lambda s: s.snapshot_time)
                starting_balance = latest_snapshot.balance
                starting_date = latest_snapshot.snapshot_time.date()

                if verbose:
                    from treeline.app.preferences_service import format_currency

                    verbose_logs.append(
                        f"Account {account.name}: Starting from {format_currency(starting_balance, account.currency)} on {starting_date}"
                    )

                # Get transactions for this account (ordered DESC by date)
                transactions_result = await self.repository.get_transactions_by_account(
                    account.id, order_by="transaction_date DESC"
                )
                if not transactions_result.success:
                    warnings.append(
                        f"Account {account.name}: Failed to get transactions - {transactions_result.error}"
                    )
                    continue

                transactions = transactions_result.data

                # Build set of dates that already have snapshots
                existing_dates = {s.snapshot_time.date() for s in existing_snapshots}

                # Walk backward through transactions
                current_balance = starting_balance
                snapshots_to_create: List[BalanceSnapshot] = []

                for transaction in transactions:
                    tx_date = transaction.transaction_date

                    # Skip if beyond days limit
                    if days is not None:
                        days_ago = (starting_date - tx_date).days
                        if days_ago > days:
                            break

                    # Skip if this date already has a snapshot (preserve real data)
                    if tx_date in existing_dates:
                        total_snapshots_skipped += 1
                        if verbose:
                            verbose_logs.append(
                                f"Account {account.name}: Skipped {tx_date} (already has snapshot)"
                            )
                        continue

                    # Calculate balance before this transaction
                    # If debit (negative), balance was higher before
                    # If credit (positive), balance was lower before
                    balance_before = current_balance - transaction.amount

                    # Create snapshot for this date (end of day)
                    snapshot = BalanceSnapshot(
                        id=uuid4(),
                        account_id=account.id,
                        balance=Decimal(str(balance_before)),
                        snapshot_time=datetime.combine(
                            tx_date, datetime.max.time()
                        ).replace(tzinfo=timezone.utc),
                        created_at=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc),
                        source="backfill",
                    )
                    snapshots_to_create.append(snapshot)
                    existing_dates.add(tx_date)  # Mark as processed

                    current_balance = balance_before

                    if verbose:
                        from treeline.app.preferences_service import format_currency

                        verbose_logs.append(
                            f"Account {account.name}: {tx_date} = {format_currency(balance_before, account.currency)} (tx: {format_currency(transaction.amount, account.currency)})"
                        )

                # Insert snapshots (unless dry-run)
                if snapshots_to_create:
                    if not dry_run:
                        insert_result = await self.repository.bulk_add_balances(
                            snapshots_to_create
                        )
                        if not insert_result.success:
                            warnings.append(
                                f"Account {account.name}: Failed to insert snapshots - {insert_result.error}"
                            )
                            continue

                    total_snapshots_created += len(snapshots_to_create)

                    if verbose:
                        verbose_logs.append(
                            f"Account {account.name}: Created {len(snapshots_to_create)} snapshots"
                        )

            # Add summary warning
            if warnings:
                warnings.insert(
                    0,
                    "WARNING: Balance backfill produces estimates. If transactions are missing, balances may be inaccurate.",
                )

            return Ok(
                {
                    "accounts_processed": accounts_processed,
                    "snapshots_created": total_snapshots_created,
                    "snapshots_skipped": total_snapshots_skipped,
                    "warnings": warnings,
                    "verbose_logs": verbose_logs,
                    "dry_run": dry_run,
                }
            )

        except Exception as e:
            return Fail(f"Backfill balances failed: {str(e)}")
