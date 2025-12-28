"""Treeline CLI - Personal finance in your terminal."""

import asyncio
import sys

import typer
from dotenv import load_dotenv
from rich.console import Console

from treeline.app.container import Container
from treeline.commands import (
    backfill,
    backup,
    compact,
    demo,
    doctor,
    encrypt,
    import_cmd,
    new,
    plugin,
    query,
    remove,
    setup,
    status,
    sync,
    tag,
)
from treeline.config import is_demo_mode
from treeline.theme import get_theme
from treeline.utils import get_treeline_dir

# Load environment variables from .env file
load_dotenv()


def get_version() -> str:
    """Get the version from package metadata."""
    try:
        from importlib.metadata import version

        return version("treeline-money")
    except Exception:
        return "0.1.0"


def version_callback(value: bool):
    """Show version information."""
    if value:
        console.print(get_version())
        raise typer.Exit()


app = typer.Typer(
    help="Treeline - personal finance in your terminal",
    add_completion=False,
    no_args_is_help=True,
)
console = Console()
theme = get_theme()


@app.callback()
def main(
    _version: bool = typer.Option(
        None,
        "--version",
        "-v",
        help="Show version information",
        callback=version_callback,
        is_eager=True,
    ),
):
    _ = _version  # Used by callback


# Global container instance
_container: Container | None = None


def _password_callback() -> str:
    """Interactive password prompt for encrypted databases."""
    from rich.prompt import Prompt

    return Prompt.ask("Enter database password", password=True)


def get_container() -> Container:
    """Get or create the dependency injection container."""
    global _container
    if _container is None:
        treeline_dir = get_treeline_dir()
        db_filename = "demo.duckdb" if is_demo_mode() else "treeline.duckdb"
        _container = Container(
            str(treeline_dir),
            db_filename,
            password_callback=_password_callback,
        )
    return _container


def reset_container() -> None:
    """Reset the container (used when switching demo mode)."""
    global _container
    _container = None


def ensure_treeline_initialized() -> bool:
    """Ensure treeline directory and database exist."""
    treeline_dir = get_treeline_dir()
    needs_init = not treeline_dir.exists()
    treeline_dir.mkdir(exist_ok=True)

    container = get_container()
    db_service = container.db_service()

    result = asyncio.run(db_service.initialize_db())
    if not result.success:
        console.print(
            f"[{theme.error}]Error initializing database: {result.error}[/{theme.error}]"
        )
        sys.exit(1)

    return needs_init


# Register commands from modules
status.register(app, get_container)
setup.register(app, get_container, ensure_treeline_initialized)
sync.register(app, get_container, ensure_treeline_initialized)
query.register(app, get_container, ensure_treeline_initialized)
tag.register(app, get_container, ensure_treeline_initialized)
new.register(app, get_container, ensure_treeline_initialized)
backfill.register(app, get_container, ensure_treeline_initialized)
backup.register(app, get_container, ensure_treeline_initialized)
compact.register(app, get_container, ensure_treeline_initialized)
plugin.register(app, get_container)
demo.register(app, get_container, ensure_treeline_initialized)
remove.register(app, get_container, ensure_treeline_initialized)
import_cmd.register(app, get_container, ensure_treeline_initialized)
doctor.register(app, get_container, ensure_treeline_initialized)
encrypt.register(app, get_container, ensure_treeline_initialized)


if __name__ == "__main__":
    app()
