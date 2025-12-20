"""Backfill command - backfill historical balance snapshots."""

import asyncio
from typing import List
from uuid import UUID

import typer
from rich.console import Console

from treeline.theme import get_theme
from treeline.utils import get_log_file_path

console = Console()
theme = get_theme()


def display_error(error: str, show_log_hint: bool = True) -> None:
    """Display error message in consistent format."""
    console.print(f"[{theme.error}]Error: {error}[/{theme.error}]")
    if show_log_hint:
        log_file = get_log_file_path()
        console.print(f"[{theme.muted}]See {log_file} for details[/{theme.muted}]")


def register(app: typer.Typer, get_container: callable, ensure_initialized: callable) -> None:
    """Register the backfill command with the app."""

    @app.command(name="backfill")
    def backfill_command(
        resource_type: str = typer.Argument(
            ...,
            help="Type of resource to backfill (balances)",
        ),
        account_id: List[str] = typer.Option(
            None,
            "--account-id",
            help="Account ID to backfill (can specify multiple)",
        ),
        days: int = typer.Option(
            None,
            "--days",
            help="Limit to last N days of history",
        ),
        dry_run: bool = typer.Option(
            False,
            "--dry-run",
            help="Preview changes without saving",
        ),
        verbose: bool = typer.Option(
            False,
            "--verbose",
            "-v",
            help="Show detailed output",
        ),
    ):
        """Backfill historical data.

        Examples:
          # Backfill balance snapshots for all accounts
          tl backfill balances

          # Backfill specific account
          tl backfill balances --account-id ACCOUNT-UUID

          # Backfill last 90 days only
          tl backfill balances --days 90 --dry-run
        """
        ensure_initialized()

        if resource_type == "balances":
            _backfill_balances(get_container, account_id, days, dry_run, verbose)
        else:
            display_error(f"Unknown resource type: {resource_type}")
            console.print(f"[{theme.muted}]Available types: balances[/{theme.muted}]")
            raise typer.Exit(1)


def _backfill_balances(
    get_container: callable,
    account_ids_str: List[str] | None,
    days: int | None,
    dry_run: bool,
    verbose: bool,
) -> None:
    """Backfill balance snapshots from transaction history."""
    container = get_container()
    backfill_service = container.backfill_service()

    # Parse account IDs
    account_ids = [UUID(id_str) for id_str in account_ids_str] if account_ids_str else None

    # Show dry-run indicator
    if dry_run:
        console.print(f"[{theme.warning}]DRY RUN - No changes will be saved[/{theme.warning}]\n")

    # Run backfill
    with console.status("[bold]Backfilling balance snapshots..."):
        result = asyncio.run(
            backfill_service.backfill_balances(account_ids, days, dry_run, verbose)
        )

    if not result.success:
        display_error(result.error)
        raise typer.Exit(1)

    data = result.data

    # Display warnings
    if data.get("warnings"):
        console.print(f"\n[{theme.warning}]Warnings[/{theme.warning}]")
        for warning in data["warnings"]:
            console.print(f"  {warning}")

    # Display verbose logs
    if verbose and data.get("verbose_logs"):
        console.print(f"\n[{theme.ui_header}]Detailed Logs[/{theme.ui_header}]")
        for log in data["verbose_logs"]:
            console.print(f"[{theme.muted}]{log}[/{theme.muted}]")

    # Display summary
    console.print(f"\n[{theme.success}]✓[/{theme.success}] Backfill complete")
    console.print(f"  Accounts processed: {data['accounts_processed']}")
    console.print(f"  Snapshots created: {data['snapshots_created']}")
    console.print(f"  Snapshots skipped: {data['snapshots_skipped']}")

    if dry_run:
        console.print(f"\n[{theme.warning}]DRY RUN - No changes were saved[/{theme.warning}]")
