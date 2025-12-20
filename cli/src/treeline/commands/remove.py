"""Remove command - remove integrations."""

import asyncio

import typer
from rich.console import Console
from rich.prompt import Confirm

from treeline.theme import get_theme

console = Console()
theme = get_theme()


def register(app: typer.Typer, get_container: callable, ensure_initialized: callable) -> None:
    """Register the remove command with the app."""

    @app.command(name="remove")
    def remove_command(
        integration: str = typer.Argument(..., help="Integration name to remove (e.g., 'simplefin')"),
        force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation prompt"),
    ) -> None:
        """Remove an integration.

        This removes the integration configuration. Your synced data
        (accounts, transactions) will remain in the database.

        Examples:
          tl remove simplefin
          tl remove simplefin --force
        """
        ensure_initialized()

        container = get_container()
        integration_service = container.integration_service()

        # Check if integration exists
        integrations_result = asyncio.run(integration_service.get_integrations())
        if not integrations_result.success:
            console.print(f"[{theme.error}]Error: {integrations_result.error}[/{theme.error}]")
            raise typer.Exit(1)

        integration_names = [
            i.get("integrationName", "").lower()
            for i in (integrations_result.data or [])
        ]

        if integration.lower() not in integration_names:
            console.print(f"[{theme.error}]Integration '{integration}' not found[/{theme.error}]")
            if integration_names:
                console.print(f"[{theme.muted}]Configured integrations: {', '.join(integration_names)}[/{theme.muted}]")
            else:
                console.print(f"[{theme.muted}]No integrations configured[/{theme.muted}]")
            raise typer.Exit(1)

        # Confirm removal
        if not force:
            console.print(f"\n[{theme.warning}]This will remove the '{integration}' integration.[/{theme.warning}]")
            console.print(f"[{theme.muted}]Your synced data will remain in the database.[/{theme.muted}]\n")

            try:
                confirmed = Confirm.ask("Are you sure?", default=False)
            except (KeyboardInterrupt, EOFError):
                console.print(f"\n[{theme.muted}]Cancelled[/{theme.muted}]\n")
                raise typer.Exit(0)

            if not confirmed:
                console.print(f"[{theme.muted}]Cancelled[/{theme.muted}]\n")
                raise typer.Exit(0)

        # Remove integration
        result = asyncio.run(integration_service.delete_integration(integration.lower()))

        if not result.success:
            console.print(f"[{theme.error}]Error: {result.error}[/{theme.error}]")
            raise typer.Exit(1)

        console.print(f"\n[{theme.success}]✓[/{theme.success}] Integration '{integration}' removed\n")
