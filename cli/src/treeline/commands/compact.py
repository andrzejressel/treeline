"""Compact command - compact database to reclaim space."""

import asyncio
import json as json_module

import typer
from rich.console import Console

from treeline.config import is_demo_mode
from treeline.theme import get_theme

console = Console()
theme = get_theme()


def register(
    app: typer.Typer, get_container: callable, ensure_initialized: callable
) -> None:
    """Register the compact command with the app."""

    @app.command(name="compact")
    def compact_command(
        skip_backup: bool = typer.Option(
            False,
            "--skip-backup",
            help="Skip creating a safety backup before compacting",
        ),
        json_output: bool = typer.Option(
            False,
            "--json",
            help="Output as JSON",
        ),
    ) -> None:
        """Compact the database to reclaim disk space.

        Over time, deleted data can leave unused space in the database file.
        This command creates a fresh, optimized copy to reclaim that space.

        By default, a safety backup is created before compacting. Use --skip-backup
        to skip the backup if you already have one.

        Examples:
          tl compact                  # Compact with safety backup
          tl compact --skip-backup    # Compact without backup
        """
        ensure_initialized()

        container = get_container()

        # Show demo mode indicator
        if is_demo_mode() and not json_output:
            console.print(f"[{theme.muted}](demo mode)[/{theme.muted}]")

        db_service = container.db_service()

        # Get backup service if we need to create a safety backup
        backup_service = None if skip_backup else container.backup_service()

        if not json_output:
            if skip_backup:
                status_msg = "Compacting database..."
            else:
                status_msg = "Creating backup and compacting database..."

            with console.status(f"[{theme.status_loading}]{status_msg}"):
                result = asyncio.run(db_service.compact(backup_service=backup_service))
        else:
            result = asyncio.run(db_service.compact(backup_service=backup_service))

        if not result.success:
            if json_output:
                print(json_module.dumps({"error": result.error}))
            else:
                console.print(f"[{theme.error}]Error: {result.error}[/{theme.error}]")
            raise typer.Exit(1)

        data = result.data
        original_size = data["original_size"]
        compacted_size = data["compacted_size"]
        backup_name = data.get("backup_name")

        if json_output:
            print(
                json_module.dumps(
                    {
                        "original_size": original_size,
                        "compacted_size": compacted_size,
                        "backup_name": backup_name,
                    }
                )
            )
            return

        # Calculate reduction
        if original_size > 0:
            reduction_pct = ((original_size - compacted_size) / original_size) * 100
        else:
            reduction_pct = 0

        # Format sizes
        def format_size(size_bytes: int) -> str:
            if size_bytes >= 1024 * 1024 * 1024:
                return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
            elif size_bytes >= 1024 * 1024:
                return f"{size_bytes / (1024 * 1024):.1f} MB"
            elif size_bytes >= 1024:
                return f"{size_bytes / 1024:.1f} KB"
            else:
                return f"{size_bytes} bytes"

        console.print(f"\n[{theme.success}]Database compacted[/{theme.success}]")
        console.print(f"  Before: {format_size(original_size)}")
        console.print(f"  After:  {format_size(compacted_size)}")

        if reduction_pct > 0:
            console.print(
                f"  Saved:  {format_size(original_size - compacted_size)} ({reduction_pct:.1f}% reduction)"
            )
        else:
            console.print(f"  [{theme.muted}]No space reclaimed (database was already compact)[/{theme.muted}]")

        if backup_name:
            console.print(f"\n  [{theme.muted}]Safety backup: {backup_name}[/{theme.muted}]")

        console.print()
