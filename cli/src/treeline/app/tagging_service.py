"""Service for managing transaction tagging operations."""

from typing import Any, Dict, List
from uuid import UUID

from treeline.abstractions import Repository
from treeline.domain import Result, Transaction


class TaggingService:
    """Service for managing transaction tagging operations."""

    def __init__(self, repository: Repository):
        """Initialize TaggingService.

        Args:
            repository: Repository for data persistence
        """
        self.repository = repository

    async def update_transaction_tags(
        self, transaction_id: UUID, tags: List[str]
    ) -> Result[Transaction]:
        """Update tags for a single transaction.

        Args:
            transaction_id: Transaction ID to update
            tags: New list of tags (replaces existing tags)

        Returns:
            Result containing updated Transaction object
        """
        return await self.repository.update_transaction_tags(transaction_id, tags)

    async def apply_auto_tag_rules(
        self, transactions: List[Transaction]
    ) -> Result[Dict[str, Any]]:
        """Apply auto-tagging rules to a list of transactions.

        Queries enabled rules from repository and applies matching tags
        to the provided transactions.

        Args:
            transactions: List of Transaction objects to potentially tag

        Returns:
            Result with stats about rules applied
        """
        if not transactions:
            return Result(
                success=True, data={"rules_applied": 0, "transactions_tagged": 0}
            )

        # Get enabled rules from repository
        rules_result = await self.repository.get_enabled_tag_rules()
        if not rules_result.success:
            # Don't fail if rules query fails, just skip tagging
            return Result(
                success=True,
                data={
                    "rules_applied": 0,
                    "transactions_tagged": 0,
                    "error": rules_result.error,
                },
            )

        rules = rules_result.data or []
        if not rules:
            return Result(
                success=True, data={"rules_applied": 0, "transactions_tagged": 0}
            )

        # Build list of transaction IDs for scoping
        transaction_ids = [tx.id for tx in transactions if tx.id]
        if not transaction_ids:
            return Result(
                success=True, data={"rules_applied": 0, "transactions_tagged": 0}
            )

        rules_applied = 0

        for rule in rules:
            sql_condition = rule.get("sql_condition")
            tags = rule.get("tags", [])

            if not sql_condition or not tags:
                continue

            try:
                apply_result = await self.repository.apply_tags_to_transactions(
                    transaction_ids=transaction_ids,
                    sql_condition=sql_condition,
                    tags=tags,
                )

                if apply_result.success:
                    rules_applied += 1

            except Exception:
                # Don't fail if a single rule fails
                continue

        return Result(
            success=True,
            data={
                "rules_applied": rules_applied,
            },
        )
