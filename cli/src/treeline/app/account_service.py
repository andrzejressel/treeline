"""Service for account operations."""

from datetime import datetime, timezone, date
from decimal import Decimal
from typing import List
from uuid import UUID, uuid4

from treeline.abstractions import Repository
from treeline.domain import Account, BalanceSnapshot, Result


class AccountService:
    """Service for account operations."""

    def __init__(self, repository: Repository):
        self.repository = repository

    async def get_accounts(self) -> Result[List[Account]]:
        """Get all accounts."""
        return await self.repository.get_accounts()

    async def create_account(
        self,
        name: str,
        account_type: str,
        institution: str | None = None,
        currency: str = "USD",
        balance: Decimal | None = None,
    ) -> Result[Account]:
        """Create a new account.

        Args:
            name: Account name
            account_type: Account type (no validation - accepts any string)
            institution: Optional institution name
            currency: Currency code (default: USD)
            balance: Optional account balance

        Returns:
            Result containing the created Account
        """
        # Create new account with generated UUID and timestamps
        now = datetime.now(timezone.utc)
        account = Account(
            id=uuid4(),
            name=name,
            account_type=account_type,
            institution_name=institution,
            currency=currency,
            balance=balance,
            external_ids={},
            created_at=now,
            updated_at=now,
        )

        # Add to repository
        add_result = await self.repository.add_account(account)
        if not add_result.success:
            return add_result

        # Return the created account
        return Result(success=True, data=account)

    async def update_account_type(
        self, account_id: UUID, account_type: str
    ) -> Result[Account]:
        """Update the account type for an existing account.

        Args:
            account_id: UUID of account to update
            account_type: New account type (no validation - accepts any string)

        Returns:
            Result containing the updated Account
        """
        # Get existing account
        get_result = await self.repository.get_account_by_id(account_id)
        if not get_result.success:
            return get_result

        existing_account = get_result.data

        # Create updated account with new account_type
        # Note: Account is frozen (immutable), so we use model_copy
        now = datetime.now(timezone.utc)
        updated_account = existing_account.model_copy(
            update={"account_type": account_type, "updated_at": now}
        )

        # Update in repository
        update_result = await self.repository.update_account_by_id(updated_account)
        if not update_result.success:
            return update_result

        # Return the updated account
        return Result(success=True, data=updated_account)

    async def add_balance_snapshot(
        self,
        account_id: UUID,
        balance: Decimal,
        snapshot_date: date | None = None,
        source: str | None = None,
    ) -> Result[BalanceSnapshot]:
        """Add a balance snapshot for an account.

        Args:
            account_id: UUID of account
            balance: Account balance
            snapshot_date: Date for the snapshot (defaults to today)
            source: Source of the snapshot ('sync', 'manual', 'backfill')

        Returns:
            Result containing the created BalanceSnapshot or error if duplicate
        """
        # Verify account exists
        account_result = await self.repository.get_account_by_id(account_id)
        if not account_result.success:
            return account_result

        # Default to today if no date provided
        if snapshot_date is None:
            snapshot_date = date.today()

        # Convert date to midnight in local time (naive datetime)
        # This matches SimpleFin sync behavior and ensures DATE() queries work correctly
        snapshot_time = datetime.combine(snapshot_date, datetime.min.time())

        # Check for existing snapshots on this date
        date_str = snapshot_date.isoformat()
        existing_result = await self.repository.get_balance_snapshots(
            account_id=account_id, date=date_str
        )

        if not existing_result.success:
            # If query failed, skip to avoid duplicates
            return Result(success=False, error="Failed to check for existing snapshots")

        existing_snapshots = existing_result.data or []

        # Check if a snapshot with the same balance already exists
        has_same_balance = any(
            abs(snapshot.balance - balance) < Decimal("0.01")
            for snapshot in existing_snapshots
        )

        if has_same_balance:
            return Result(
                success=False,
                error=f"Balance snapshot already exists for {snapshot_date} with same balance",
            )

        # Create the balance snapshot
        now = datetime.now(timezone.utc)
        balance_snapshot = BalanceSnapshot(
            id=uuid4(),
            account_id=account_id,
            balance=balance,
            snapshot_time=snapshot_time,
            created_at=now,
            updated_at=now,
            source=source,
        )

        # Add to repository
        add_result = await self.repository.add_balance(balance_snapshot)
        if not add_result.success:
            return add_result

        # Return the created snapshot
        return Result(success=True, data=balance_snapshot)
