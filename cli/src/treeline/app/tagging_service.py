"""Service for managing transaction tagging operations."""

from typing import List
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
