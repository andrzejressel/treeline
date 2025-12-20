"""Tag command - apply tags to transactions."""

import asyncio
import json
import sys
from uuid import UUID

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


def register(app: typer.Typer, get_container: callable, ensure_initialized: callable) -> None:
    """Register the tag command with the app."""

    @app.command(name="tag")
    def tag_command(
        tags: str = typer.Argument(..., help="Comma-separated tags to apply"),
        ids: str = typer.Option(
            None, "--ids", help="Comma-separated transaction IDs (or read from stdin)"
        ),
        replace: bool = typer.Option(
            False, "--replace", help="Replace all tags (default: append/merge)"
        ),
        json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    ) -> None:
        """Apply tags to specific transactions (scriptable).

        By default, tags are merged with existing tags. Use --replace to replace all tags.

        Examples:
          # Append tags to transactions (default)
          tl tag groceries,food --ids abc123,def456

          # Replace all tags
          tl tag dining --ids abc123 --replace

          # Pipe IDs from query (one ID per line)
          tl query "SELECT transaction_id FROM transactions WHERE description ILIKE '%QFC%'" --json | \\
            jq -r '.rows[][] | @text' | tl tag groceries

          # Pipe comma-separated IDs
          echo "abc123,def456" | tl tag groceries
        """
        ensure_initialized()

        # Parse IDs from --ids option or stdin
        if ids:
            transaction_ids = [tid.strip() for tid in ids.split(",") if tid.strip()]
        else:
            # Read from stdin
            stdin_input = sys.stdin.read().strip()
            if not stdin_input:
                display_error("No transaction IDs provided via --ids or stdin")
                raise typer.Exit(1)

            # Support both newline-separated and comma-separated IDs
            if "\n" in stdin_input:
                transaction_ids = [tid.strip() for tid in stdin_input.split("\n") if tid.strip()]
            else:
                transaction_ids = [tid.strip() for tid in stdin_input.split(",") if tid.strip()]

        if not transaction_ids:
            display_error("No transaction IDs provided")
            raise typer.Exit(1)

        # Parse tags
        tag_list = [tag.strip() for tag in tags.split(",") if tag.strip()]
        if not tag_list:
            display_error("No tags provided")
            raise typer.Exit(1)

        container = get_container()
        tagging_service = container.tagging_service()
        db_service = container.db_service()

        # Apply tags to each transaction
        results = []
        errors = []

        for transaction_id_str in transaction_ids:
            try:
                transaction_id = UUID(transaction_id_str)
            except ValueError:
                errors.append({
                    "transaction_id": transaction_id_str,
                    "error": f"Invalid UUID: {transaction_id_str}",
                    "success": False,
                })
                continue

            # Determine final tag list (merge or replace)
            final_tags = tag_list
            if not replace:
                # Fetch current tags and merge
                query = f"SELECT tags FROM transactions WHERE transaction_id = '{transaction_id}'"
                tags_result = asyncio.run(db_service.execute_query(query))

                if (
                    tags_result.success
                    and tags_result.data
                    and tags_result.data.get("rows")
                ):
                    current_tags = tags_result.data["rows"][0][0] or []
                    merged_tags = list(set(current_tags) | set(tag_list))
                    final_tags = merged_tags

            result = asyncio.run(
                tagging_service.update_transaction_tags(transaction_id, final_tags)
            )

            if result.success:
                results.append({
                    "transaction_id": transaction_id_str,
                    "tags": final_tags,
                    "success": True,
                })
            else:
                errors.append({
                    "transaction_id": transaction_id_str,
                    "error": result.error,
                    "success": False,
                })

        if json_output:
            output_json({
                "succeeded": len(results),
                "failed": len(errors),
                "results": results + errors,
            })
        else:
            if results:
                console.print(
                    f"\n[{theme.success}]✓ Successfully tagged {len(results)} transaction(s)[/{theme.success}]"
                )
                console.print(f"[{theme.muted}]Tags applied: {', '.join(tag_list)}[/{theme.muted}]\n")

            if errors:
                console.print(
                    f"[{theme.error}]✗ Failed to tag {len(errors)} transaction(s)[/{theme.error}]"
                )
                for error in errors:
                    console.print(f"[{theme.muted}]  {error['transaction_id']}: {error['error']}[/{theme.muted}]")
                console.print()

            if errors:
                raise typer.Exit(1)
