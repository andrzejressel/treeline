"""Demo command - toggle demo mode."""

import asyncio

import typer
from rich.console import Console

from treeline.config import is_demo_mode, set_demo_mode
from treeline.theme import get_theme

console = Console()
theme = get_theme()

# Will be set by register()
_reset_container: callable = None


def register(app: typer.Typer, get_container: callable, ensure_initialized: callable) -> None:
    """Register the demo command with the app."""

    @app.command(name="demo")
    def demo_command(
        action: str = typer.Argument(
            None, help="Action: 'on', 'off', or 'status' (default: status)"
        ),
    ) -> None:
        """Toggle demo mode on/off.

        Demo mode uses a separate database with sample data,
        allowing you to explore Treeline without connecting real accounts.

        Examples:
          tl demo          # Show current status
          tl demo on       # Enable demo mode
          tl demo off      # Disable demo mode
        """
        # Default to status if no action provided
        if action is None:
            action = "status"

        action_lower = action.lower()

        if action_lower == "status":
            _show_status()
        elif action_lower == "on":
            _enable_demo(get_container, ensure_initialized)
        elif action_lower == "off":
            _disable_demo()
        else:
            console.print(f"[{theme.error}]Unknown action: {action}[/{theme.error}]")
            console.print(f"[{theme.muted}]Use 'on', 'off', or 'status'[/{theme.muted}]")
            raise typer.Exit(1)


def _show_status() -> None:
    """Show current demo mode status."""
    if is_demo_mode():
        console.print(f"\n[{theme.warning}]Demo mode is ON[/{theme.warning}]")
        console.print(f"[{theme.muted}]Using demo.duckdb with sample data[/{theme.muted}]")
        console.print(f"[{theme.muted}]Run 'tl demo off' to switch to real data[/{theme.muted}]\n")
    else:
        console.print(f"\n[{theme.success}]Demo mode is OFF[/{theme.success}]")
        console.print(f"[{theme.muted}]Using treeline.duckdb with real data[/{theme.muted}]")
        console.print(f"[{theme.muted}]Run 'tl demo on' to try demo mode[/{theme.muted}]\n")


def _enable_demo(get_container: callable, ensure_initialized: callable) -> None:
    """Enable demo mode and sync demo data."""
    if is_demo_mode():
        console.print(f"[{theme.muted}]Demo mode is already enabled[/{theme.muted}]\n")
        return

    set_demo_mode(True)

    # Reset container to pick up new database
    from treeline.cli import reset_container
    reset_container()

    console.print(f"\n[{theme.success}]Demo mode enabled[/{theme.success}]")

    # Initialize demo database and sync demo data
    ensure_initialized()
    container = get_container()

    # Check if demo integration exists, if not create it
    integration_service = container.integration_service()
    integrations_result = asyncio.run(integration_service.get_integrations())

    has_demo = False
    if integrations_result.success:
        for integration in integrations_result.data or []:
            if integration.get("integrationName") == "demo":
                has_demo = True
                break

    # Get demo provider (used for integration and budget seeding)
    demo_provider = container.get_integration_provider("demo")

    if not has_demo:
        # Create demo integration
        asyncio.run(integration_service.create_integration(demo_provider, "demo", {}))

    # Sync demo data
    sync_service = container.sync_service()
    console.print(f"[{theme.muted}]Syncing demo data...[/{theme.muted}]")
    with console.status(f"[{theme.status_loading}]Syncing demo accounts and transactions..."):
        result = asyncio.run(sync_service.sync_all_integrations())

    if result.success:
        console.print(f"[{theme.success}]Demo data synced successfully![/{theme.success}]")
    else:
        console.print(f"[{theme.warning}]Note: {result.error}[/{theme.warning}]")

    # Get db_service for balance history and budget seeding
    db_service = container.db_service()

    # Generate interesting balance history for demo accounts
    # First, get account IDs mapping (demo external ID -> actual UUID)
    account_service = container.account_service()
    accounts_result = asyncio.run(account_service.get_accounts())

    if accounts_result.success and accounts_result.data:
        # Build mapping of demo external ID to account UUID
        account_id_map = {}
        for account in accounts_result.data:
            demo_id = account.external_ids.get("demo")
            if demo_id:
                account_id_map[demo_id] = str(account.id)

        if account_id_map:
            with console.status(f"[{theme.status_loading}]Generating balance history..."):
                balance_sql = demo_provider.generate_demo_balance_history_sql(account_id_map)
                balance_result = asyncio.run(db_service.execute_write_query(balance_sql))

            if balance_result.success:
                console.print(f"[{theme.success}]Created balance history for {len(account_id_map)} accounts[/{theme.success}]")
            else:
                console.print(f"[{theme.warning}]Note: {balance_result.error}[/{theme.warning}]")

    # Seed demo budget categories
    with console.status(f"[{theme.status_loading}]Setting up demo budget..."):
        budget_sql = demo_provider.generate_demo_budget_sql()
        budget_result = asyncio.run(db_service.execute_write_query(budget_sql))

    if budget_result.success:
        console.print(f"[{theme.success}]Demo budget configured[/{theme.success}]")
    else:
        console.print(f"[{theme.warning}]Note: {budget_result.error}[/{theme.warning}]")

    console.print(f"\n[{theme.muted}]Run 'tl status' to see demo data[/{theme.muted}]")
    console.print(f"[{theme.muted}]Run 'tl demo off' to return to real data[/{theme.muted}]\n")


def _disable_demo() -> None:
    """Disable demo mode."""
    if not is_demo_mode():
        console.print(f"[{theme.muted}]Demo mode is already disabled[/{theme.muted}]\n")
        return

    set_demo_mode(False)
    console.print(f"\n[{theme.success}]Demo mode disabled[/{theme.success}]")
    console.print(f"[{theme.muted}]Now using treeline.duckdb with real data[/{theme.muted}]")
    console.print(f"[{theme.muted}]Run 'tl status' to see your data[/{theme.muted}]\n")
