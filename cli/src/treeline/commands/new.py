"""New command - create new resources (balance snapshots)."""

import asyncio
from datetime import date
from decimal import Decimal
from uuid import UUID

import typer
from rich.console import Console
from rich.prompt import Prompt

from treeline.commands.import_cmd import _prompt_account_selection
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
    """Register the new command with the app."""

    @app.command(name="new")
    def new_command(
        resource_type: str = typer.Argument(
            ..., help="Type of resource to create (e.g., 'balance')"
        ),
        # Balance-specific options
        account_id: str = typer.Option(
            None, "--account-id", help="Account ID (UUID) for balance snapshot"
        ),
        balance: str = typer.Option(None, "--balance", help="Account balance amount"),
        snapshot_date: str = typer.Option(
            None, "--date", help="Snapshot date (YYYY-MM-DD, defaults to today)"
        ),
    ) -> None:
        """Create a new resource.

        Examples:
          # Add a balance snapshot (interactive)
          tl new balance

          # Add a balance snapshot (scriptable)
          tl new balance --account-id <uuid> --balance 1234.56
          tl new balance --account-id <uuid> --balance 1234.56 --date 2025-11-15
        """
        ensure_initialized()

        if resource_type == "balance":
            _create_balance_snapshot(get_container, account_id, balance, snapshot_date)
        else:
            display_error(f"Unknown resource type: {resource_type}")
            console.print(f"[{theme.muted}]Available types: balance[/{theme.muted}]")
            raise typer.Exit(1)


def _create_balance_snapshot(
    get_container: callable,
    account_id_str: str | None,
    balance_str: str | None,
    date_str: str | None,
) -> None:
    """Create a balance snapshot for an account."""
    container = get_container()
    account_service = container.account_service()

    # Determine mode: scriptable vs interactive
    is_scriptable = account_id_str is not None and balance_str is not None

    if is_scriptable:
        # SCRIPTABLE MODE
        try:
            account_id = UUID(account_id_str)
        except ValueError:
            display_error(f"Invalid account ID: {account_id_str}")
            console.print(f"[{theme.muted}]Account ID must be a valid UUID[/{theme.muted}]\n")
            raise typer.Exit(1)

        try:
            balance = Decimal(balance_str)
        except Exception:
            display_error(f"Invalid balance amount: {balance_str}")
            console.print(f"[{theme.muted}]Balance must be a valid number[/{theme.muted}]\n")
            raise typer.Exit(1)

        snapshot_date = None
        if date_str:
            try:
                snapshot_date = date.fromisoformat(date_str)
            except ValueError:
                display_error(f"Invalid date format: {date_str}")
                console.print(f"[{theme.muted}]Date must be in YYYY-MM-DD format[/{theme.muted}]\n")
                raise typer.Exit(1)

    else:
        # INTERACTIVE MODE
        console.print(f"\n[{theme.ui_header}]Add Balance Snapshot[/{theme.ui_header}]\n")

        accounts_result = asyncio.run(account_service.get_accounts())
        if not accounts_result.success:
            display_error(f"Failed to fetch accounts: {accounts_result.error}")
            raise typer.Exit(1)

        accounts = accounts_result.data or []

        if not accounts:
            display_error("No accounts found. Please create an account first.")
            console.print(f"[{theme.muted}]Use 'tl import' or 'tl sync' to add accounts[/{theme.muted}]\n")
            raise typer.Exit(1)

        account_id = _prompt_account_selection(accounts)

        if not account_id or account_id == "CREATE_NEW":
            console.print(f"[{theme.warning}]Cancelled[/{theme.warning}]\n")
            raise typer.Exit(0)

        try:
            balance_input = Prompt.ask(f"\n[{theme.info}]Enter balance amount[/{theme.info}]")
            balance = Decimal(balance_input)
        except (KeyboardInterrupt, EOFError):
            console.print(f"\n[{theme.warning}]Cancelled[/{theme.warning}]\n")
            raise typer.Exit(0)
        except Exception:
            display_error(f"Invalid balance amount: {balance_input}")
            raise typer.Exit(1)

        date_input = Prompt.ask(
            f"\n[{theme.info}]Enter snapshot date (YYYY-MM-DD)[/{theme.info}]",
            default="",
        )

        snapshot_date = None
        if date_input:
            try:
                snapshot_date = date.fromisoformat(date_input)
            except ValueError:
                display_error(f"Invalid date format: {date_input}")
                console.print(f"[{theme.muted}]Using today's date instead[/{theme.muted}]")
                snapshot_date = None

    # Add the balance snapshot via service
    result = asyncio.run(
        account_service.add_balance_snapshot(
            account_id=account_id,
            balance=balance,
            snapshot_date=snapshot_date,
        )
    )

    if not result.success:
        display_error(f"Failed to add balance snapshot: {result.error}")
        raise typer.Exit(1)

    snapshot = result.data
    console.print(f"\n[{theme.success}]✓ Added balance snapshot[/{theme.success}]")
    console.print(f"  Account ID: {snapshot.account_id}")
    console.print(f"  Balance: {snapshot.balance}")
    console.print(f"  Date: {snapshot.snapshot_time.date()}\n")
