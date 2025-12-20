"""Service for managing integrations with external providers."""

from typing import Any, Dict, List

from treeline.abstractions import IntegrationProvider, Repository
from treeline.domain import Result


class IntegrationService:
    """Service for managing integrations with external providers."""

    def __init__(self, repository: Repository):
        self.repository = repository

    async def get_integrations(self) -> Result[List[Dict[str, Any]]]:
        """Get list of configured integrations."""
        return await self.repository.list_integrations()

    async def delete_integration(self, integration_name: str) -> Result[None]:
        """Delete an integration by name."""
        return await self.repository.delete_integration(integration_name)

    async def create_integration(
        self,
        integration_provider: IntegrationProvider,
        integration_name: str,
        integration_options: Dict[str, Any],
    ) -> Result:
        """Create a new integration using a specific provider.

        Args:
            integration_provider: The provider to use for setup
            integration_name: Name of the integration (e.g., 'simplefin')
            integration_options: Provider-specific options (e.g., setup token)
        """
        result = await integration_provider.create_integration(
            integration_name, integration_options
        )
        if not result.success:
            return result

        if result.data:
            await self.repository.upsert_integration(integration_name, result.data)

        return result
