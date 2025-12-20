"""Backup command - backup and restore database."""

import asyncio
import json as json_module
from typing import Optional

import typer
from rich.console import Console
from rich.prompt import Confirm, Prompt
from rich.table import Table

from treeline.config import is_demo_mode
from treeline.theme import get_theme

console = Console()
theme = get_theme()


def register(
    app: typer.Typer, get_container: callable, ensure_initialized: callable
) -> None:
    """Register the backup command with the app."""

    @app.command(name="backup")
    def backup_command(
        action: str = typer.Argument(
            "create",
            help="Action: 'create', 'list', 'restore', or 'clear'",
        ),
        backup_name: Optional[str] = typer.Argument(
            None,
            help="Backup name (required for 'restore')",
        ),
        max_backups: int = typer.Option(
            7,
            "--max-backups",
            "-m",
            help="Maximum number of backups to keep",
        ),
        force: bool = typer.Option(
            False,
            "--force",
            "-f",
            help="Skip confirmation prompts",
        ),
        json_output: bool = typer.Option(
            False,
            "--json",
            help="Output as JSON",
        ),
    ) -> None:
        """Backup and restore your Treeline database.

        Backups are stored locally and automatically managed with retention policy.
        Demo mode backups are stored separately from regular backups.

        Examples:
          tl backup                    # Create a backup
          tl backup create             # Create a backup (explicit)
          tl backup list               # List all backups
          tl backup restore <name>     # Restore from a backup
          tl backup clear              # Delete all backups
          tl backup create --max-backups 10  # Keep up to 10 backups
        """
        ensure_initialized()

        container = get_container()

        # Show demo mode indicator
        if is_demo_mode() and not json_output:
            console.print(f"[{theme.muted}](demo mode)[/{theme.muted}]")

        if action == "create":
            _do_create(container, max_backups, json_output)
        elif action == "list":
            _do_list(container, json_output)
        elif action == "restore":
            _do_restore(container, backup_name, force, json_output)
        elif action == "clear":
            _do_clear(container, force, json_output)
        else:
            console.print(
                f"[{theme.error}]Unknown action: {action}[/{theme.error}]"
            )
            console.print(
                f"[{theme.muted}]Valid actions: create, list, restore, clear[/{theme.muted}]"
            )
            raise typer.Exit(1)


def _do_create(container, max_backups: int, json_output: bool) -> None:
    """Create a new backup."""
    backup_service = container.backup_service(max_backups=max_backups)

    if not json_output:
        with console.status(f"[{theme.status_loading}]Creating backup..."):
            result = asyncio.run(backup_service.backup())
    else:
        result = asyncio.run(backup_service.backup())

    if not result.success:
        if json_output:
            print(json_module.dumps({"error": result.error}))
        else:
            console.print(f"[{theme.error}]Error: {result.error}[/{theme.error}]")
        raise typer.Exit(1)

    backup = result.data
    if json_output:
        print(
            json_module.dumps(
                {
                    "name": backup.name,
                    "created_at": backup.created_at.isoformat(),
                    "size_bytes": backup.size_bytes,
                }
            )
        )
    else:
        size_kb = backup.size_bytes / 1024
        if size_kb >= 1024:
            size_str = f"{size_kb / 1024:.1f} MB"
        else:
            size_str = f"{size_kb:.1f} KB"

        console.print(f"\n[{theme.success}]Backup created[/{theme.success}]")
        console.print(f"  Name: {backup.name}")
        console.print(f"  Size: {size_str}")
        console.print(f"  Time: {backup.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}\n")


def _do_list(container, json_output: bool) -> None:
    """List all backups."""
    backup_service = container.backup_service()

    result = asyncio.run(backup_service.list_backups())

    if not result.success:
        if json_output:
            print(json_module.dumps({"error": result.error}))
        else:
            console.print(f"[{theme.error}]Error: {result.error}[/{theme.error}]")
        raise typer.Exit(1)

    backups = result.data or []

    if json_output:
        print(
            json_module.dumps(
                [
                    {
                        "name": b.name,
                        "created_at": b.created_at.isoformat(),
                        "size_bytes": b.size_bytes,
                    }
                    for b in backups
                ]
            )
        )
        return

    if not backups:
        console.print(f"\n[{theme.muted}]No backups found[/{theme.muted}]\n")
        return

    console.print(f"\n[{theme.ui_header}]Backups[/{theme.ui_header}] ({len(backups)} total)\n")

    table = Table(show_header=True, box=None, padding=(0, 2))
    table.add_column("#", width=3, justify="right")
    table.add_column("Name", width=35)
    table.add_column("Created", width=20)
    table.add_column("Size", width=10, justify="right")

    for i, backup in enumerate(backups, 1):
        size_kb = backup.size_bytes / 1024
        if size_kb >= 1024:
            size_str = f"{size_kb / 1024:.1f} MB"
        else:
            size_str = f"{size_kb:.1f} KB"

        table.add_row(
            str(i),
            backup.name,
            backup.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            size_str,
        )

    console.print(table)
    console.print()


def _do_restore(
    container, backup_name: Optional[str], force: bool, json_output: bool
) -> None:
    """Restore from a backup."""
    backup_service = container.backup_service()

    # If no backup name provided, show list and prompt
    if not backup_name:
        if json_output:
            print(json_module.dumps({"error": "Backup name required for restore"}))
            raise typer.Exit(1)

        # List backups for selection
        list_result = asyncio.run(backup_service.list_backups())
        if not list_result.success:
            console.print(f"[{theme.error}]Error: {list_result.error}[/{theme.error}]")
            raise typer.Exit(1)

        backups = list_result.data or []
        if not backups:
            console.print(f"\n[{theme.warning}]No backups available to restore[/{theme.warning}]\n")
            raise typer.Exit(1)

        console.print(f"\n[{theme.ui_header}]Select a backup to restore:[/{theme.ui_header}]\n")

        for i, backup in enumerate(backups, 1):
            size_kb = backup.size_bytes / 1024
            if size_kb >= 1024:
                size_str = f"{size_kb / 1024:.1f} MB"
            else:
                size_str = f"{size_kb:.1f} KB"
            console.print(
                f"  [{i}] {backup.name} ({backup.created_at.strftime('%Y-%m-%d %H:%M')} - {size_str})"
            )

        console.print()

        try:
            choice = Prompt.ask(
                f"[{theme.info}]Enter number (1-{len(backups)})[/{theme.info}]",
                default="1",
            )
            idx = int(choice) - 1
            if 0 <= idx < len(backups):
                backup_name = backups[idx].name
            else:
                console.print(f"[{theme.error}]Invalid selection[/{theme.error}]")
                raise typer.Exit(1)
        except (ValueError, KeyboardInterrupt, EOFError):
            console.print(f"\n[{theme.muted}]Cancelled[/{theme.muted}]\n")
            raise typer.Exit(0)

    # Confirm restore
    if not force and not json_output:
        console.print(
            f"\n[{theme.warning}]This will overwrite your current database with the backup.[/{theme.warning}]"
        )
        console.print(f"[{theme.muted}]Backup: {backup_name}[/{theme.muted}]\n")

        try:
            confirmed = Confirm.ask("Are you sure?", default=False)
        except (KeyboardInterrupt, EOFError):
            console.print(f"\n[{theme.muted}]Cancelled[/{theme.muted}]\n")
            raise typer.Exit(0)

        if not confirmed:
            console.print(f"[{theme.muted}]Cancelled[/{theme.muted}]\n")
            raise typer.Exit(0)

    # Perform restore
    if not json_output:
        with console.status(f"[{theme.status_loading}]Restoring backup..."):
            result = asyncio.run(backup_service.restore(backup_name))
    else:
        result = asyncio.run(backup_service.restore(backup_name))

    if not result.success:
        if json_output:
            print(json_module.dumps({"error": result.error}))
        else:
            console.print(f"[{theme.error}]Error: {result.error}[/{theme.error}]")
        raise typer.Exit(1)

    if json_output:
        print(json_module.dumps({"restored": backup_name}))
    else:
        console.print(f"\n[{theme.success}]Database restored from {backup_name}[/{theme.success}]\n")


def _do_clear(container, force: bool, json_output: bool) -> None:
    """Delete all backups."""
    backup_service = container.backup_service()

    # Confirm clear
    if not force and not json_output:
        console.print(
            f"\n[{theme.warning}]This will delete ALL backups.[/{theme.warning}]\n"
        )

        try:
            confirmed = Confirm.ask("Are you sure?", default=False)
        except (KeyboardInterrupt, EOFError):
            console.print(f"\n[{theme.muted}]Cancelled[/{theme.muted}]\n")
            raise typer.Exit(0)

        if not confirmed:
            console.print(f"[{theme.muted}]Cancelled[/{theme.muted}]\n")
            raise typer.Exit(0)

    # Perform clear
    if not json_output:
        with console.status(f"[{theme.status_loading}]Deleting backups..."):
            result = asyncio.run(backup_service.clear_all())
    else:
        result = asyncio.run(backup_service.clear_all())

    if not result.success:
        if json_output:
            print(json_module.dumps({"error": result.error}))
        else:
            console.print(f"[{theme.error}]Error: {result.error}[/{theme.error}]")
        raise typer.Exit(1)

    deleted_count = result.data or 0

    if json_output:
        print(json_module.dumps({"deleted": deleted_count}))
    else:
        if deleted_count == 0:
            console.print(f"\n[{theme.muted}]No backups to delete[/{theme.muted}]\n")
        else:
            console.print(
                f"\n[{theme.success}]Deleted {deleted_count} backup(s)[/{theme.success}]\n"
            )
