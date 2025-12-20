"""Sync command - synchronize data from connected integrations."""

import asyncio
import json

import typer
from pydantic import BaseModel
from rich.console import Console

from treeline.theme import get_theme
from treeline.utils import get_log_file_path

console = Console()
theme = get_theme()


def json_serializer(obj):
    """Custom JSON serializer for Pydantic models and other objects."""
    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json")
    return str(obj)


def output_json(data: dict) -> None:
    """Output data as JSON."""
    print(json.dumps(data, indent=2, default=json_serializer))


def display_error(error: str, show_log_hint: bool = True) -> None:
    """Display error message in consistent format."""
    console.print(f"[{theme.error}]Error: {error}[/{theme.error}]")
    if show_log_hint:
        log_file = get_log_file_path()
        console.print(f"[{theme.muted}]See {log_file} for details[/{theme.muted}]")


def display_sync_result(data: dict, dry_run: bool = False) -> None:
    """Display sync results using Rich formatting."""
    header = "Synchronizing Financial Data (DRY RUN)" if dry_run else "Synchronizing Financial Data"
    console.print(f"\n[{theme.ui_header}]{header}[/{theme.ui_header}]\n")

    for sync_result in data["results"]:
        integration_name = sync_result["integration"]
        console.print(f"[{theme.emphasis}]Syncing {integration_name}...[/{theme.emphasis}]")

        if "error" in sync_result:
            console.print(f"[{theme.error}]  ✗ {sync_result['error']}[/{theme.error}]")
            log_file = get_log_file_path()
            console.print(f"[{theme.muted}]    See {log_file} for details[/{theme.muted}]")
            continue

        console.print(
            f"[{theme.success}]  ✓[/{theme.success}] Synced {sync_result['accounts_synced']} account(s)"
        )

        if sync_result["sync_type"] == "incremental":
            console.print(
                f"[{theme.muted}]  Syncing transactions since {sync_result['start_date'].date()} (with 7-day overlap)[/{theme.muted}]"
            )
        else:
            console.print(
                f"[{theme.muted}]  Initial sync: fetching last 90 days of transactions[/{theme.muted}]"
            )

        # Display transaction breakdown if stats are available
        tx_stats = sync_result.get("transaction_stats", {})
        if tx_stats:
            discovered = tx_stats.get("discovered", 0)
            new = tx_stats.get("new", 0)
            skipped = tx_stats.get("skipped", 0)

            console.print(f"[{theme.success}]  ✓[/{theme.success}] Transaction breakdown:")
            console.print(f"[{theme.muted}]    Discovered: {discovered}[/{theme.muted}]")
            console.print(f"[{theme.muted}]    New: {new}[/{theme.muted}]")
            console.print(f"[{theme.muted}]    Skipped: {skipped} (already exists)[/{theme.muted}]")
        else:
            console.print(
                f"[{theme.success}]  ✓[/{theme.success}] Synced {sync_result['transactions_synced']} transaction(s)"
            )

        console.print(
            f"[{theme.muted}]  Balance snapshots created automatically from account data[/{theme.muted}]"
        )

        # Display provider warnings
        provider_warnings = sync_result.get("provider_warnings", [])
        if provider_warnings:
            console.print(f"\n[{theme.warning}]  ⚠ SimpleFIN warnings:[/{theme.warning}]")
            for warning in provider_warnings:
                console.print(f"[{theme.warning}]    • {warning}[/{theme.warning}]")
            console.print(
                f"[{theme.muted}]    Visit https://beta-bridge.simplefin.org/ to fix connection issues[/{theme.muted}]"
            )

    if dry_run:
        console.print(
            f"\n[{theme.warning}]⚠[/{theme.warning}] Dry run completed - no changes were made\n"
        )
        console.print(f"[{theme.muted}]Run without --dry-run to apply these changes[/{theme.muted}]\n")
    else:
        console.print(f"\n[{theme.success}]✓[/{theme.success}] Sync completed!\n")
        console.print(f"[{theme.muted}]Use 'tl status' to see your updated data[/{theme.muted}]\n")


def register(app: typer.Typer, get_container: callable, ensure_initialized: callable) -> None:
    """Register the sync command with the app."""

    @app.command(name="sync")
    def sync_command(
        json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
        dry_run: bool = typer.Option(
            False, "--dry-run", help="Show what would be synced without making changes"
        ),
    ) -> None:
        """Synchronize data from connected integrations.

        Examples:
          # Normal sync
          tl sync

          # Preview without saving
          tl sync --dry-run
        """
        ensure_initialized()

        container = get_container()
        sync_service = container.sync_service()

        # Sync all integrations with visual feedback
        if not json_output:
            status_msg = "Syncing integrations (dry-run)..." if dry_run else "Syncing integrations..."
            with console.status(f"[{theme.status_loading}]{status_msg}"):
                result = asyncio.run(sync_service.sync_all_integrations(dry_run=dry_run))
        else:
            result = asyncio.run(sync_service.sync_all_integrations(dry_run=dry_run))

        if not result.success:
            display_error(result.error)
            if result.error == "No integrations configured":
                console.print(
                    f"[{theme.muted}]Use 'tl setup' to configure an integration first[/{theme.muted}]"
                )
            raise typer.Exit(1)

        if json_output:
            output_json(result.data)
        else:
            display_sync_result(result.data, dry_run=dry_run)
