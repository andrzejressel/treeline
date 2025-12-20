"""Query command - execute SQL queries."""

import asyncio
import csv
import json
import sys

import typer
from pydantic import BaseModel
from rich.console import Console
from rich.table import Table

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


def display_query_result(columns: list[str], rows: list[list]) -> None:
    """Display query results as a Rich table."""
    console.print()

    table = Table(
        show_header=True, header_style=theme.ui_header, border_style=theme.separator
    )

    for col in columns:
        table.add_column(col)

    for row in rows:
        str_row = [
            str(val) if val is not None else f"[{theme.muted}]NULL[/{theme.muted}]"
            for val in row
        ]
        table.add_row(*str_row)

    console.print(table)
    console.print(
        f"\n[{theme.muted}]{len(rows)} row{'s' if len(rows) != 1 else ''} returned[/{theme.muted}]\n"
    )


def register(app: typer.Typer, get_container: callable, ensure_initialized: callable) -> None:
    """Register the query command with the app."""

    @app.command(name="query")
    def query_command(
        sql: str = typer.Argument(None, help="SQL query to execute (SELECT/WITH only)"),
        file: str = typer.Option(None, "--file", "-f", help="Read SQL from file"),
        format: str = typer.Option(
            "table", "--format", help="Output format (table, json, csv)"
        ),
        json_output: bool = typer.Option(
            False, "--json", help="Output as JSON (alias for --format json)"
        ),
    ) -> None:
        """Execute a SQL query and display results.

        Examples:
          # Inline SQL
          tl query "SELECT * FROM transactions LIMIT 10"

          # From file
          tl query --file queries/monthly_spending.sql

          # From stdin (pipe)
          cat queries/analysis.sql | tl query

          # Output as JSON
          tl query "SELECT * FROM transactions LIMIT 10" --json

          # Output as CSV
          tl query "SELECT * FROM transactions LIMIT 10" --format csv
        """
        ensure_initialized()

        container = get_container()
        db_service = container.db_service()

        # Determine SQL source
        sql_content = None

        if file:
            try:
                with open(file, "r") as f:
                    sql_content = f.read()
            except FileNotFoundError:
                display_error(f"File not found: {file}")
                raise typer.Exit(1)
            except Exception as e:
                display_error(f"Error reading file: {e}")
                raise typer.Exit(1)
        elif sql:
            sql_content = sql
        elif not sys.stdin.isatty():
            sql_content = sys.stdin.read()
        else:
            display_error(
                "No SQL provided. Use inline argument, --file option, or pipe from stdin."
            )
            console.print(f"[{theme.muted}]Examples:[/{theme.muted}]")
            console.print('  tl query "SELECT * FROM transactions LIMIT 10"')
            console.print("  tl query --file query.sql")
            console.print("  cat query.sql | tl query")
            raise typer.Exit(1)

        sql_stripped = sql_content.strip()

        # Determine output format
        output_format = "json" if json_output else format.lower()
        if output_format not in ["table", "json", "csv"]:
            display_error(f"Invalid format: {format}. Choose: table, json, csv")
            raise typer.Exit(1)

        # Execute query
        if output_format == "table":
            with console.status(f"[{theme.status_loading}]Running query..."):
                result = asyncio.run(db_service.execute_query(sql_stripped))
        else:
            result = asyncio.run(db_service.execute_query(sql_stripped))

        if not result.success:
            display_error(result.error)
            raise typer.Exit(1)

        query_result = result.data
        rows = query_result.get("rows", [])
        columns = query_result.get("columns", [])

        if len(rows) == 0:
            if output_format == "table":
                console.print(f"[{theme.muted}]No results returned.[/{theme.muted}]\n")
            elif output_format == "json":
                output_json({"columns": columns, "rows": [], "row_count": 0})
            elif output_format == "csv":
                writer = csv.writer(sys.stdout)
                writer.writerow(columns)
            return

        if output_format == "json":
            output_json({"columns": columns, "rows": rows, "row_count": len(rows)})
        elif output_format == "csv":
            writer = csv.writer(sys.stdout)
            writer.writerow(columns)
            for row in rows:
                writer.writerow(row)
        else:
            display_query_result(columns, rows)
