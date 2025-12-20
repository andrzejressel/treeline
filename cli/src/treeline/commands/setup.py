"""Setup command - configure integrations."""

import asyncio

import typer
from rich.console import Console
from rich.prompt import Prompt

from treeline.config import is_demo_mode
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
    """Register the setup command with the app."""

    @app.command(name="setup")
    def setup_command(
        integration: str = typer.Argument(
            None, help="Integration name (e.g., 'simplefin'). Omit for interactive wizard."
        ),
        token: str = typer.Option(
            None, "--token", help="Setup token (optional, will prompt if not provided)"
        ),
    ) -> None:
        """Set up financial data integrations.

        Examples:
          # Interactive wizard
          tl setup

          # Direct SimpleFIN setup
          tl setup simplefin

          # Non-interactive setup with token
          tl setup simplefin --token YOUR_TOKEN

        Note: For demo mode, use 'tl demo on' instead.
        """
        ensure_initialized()

        if integration is None:
            # Interactive wizard
            console.print(f"\n[{theme.ui_header}]Integration Setup[/{theme.ui_header}]\n")
            console.print(f"[{theme.info}]Available integrations:[/{theme.info}]")
            console.print(f"  [{theme.emphasis}]1[/{theme.emphasis}] - SimpleFIN")
            console.print(f"  [{theme.emphasis}]2[/{theme.emphasis}] - Cancel\n")
            console.print(f"[{theme.muted}]Tip: Use 'tl demo on' to try with sample data[/{theme.muted}]\n")

            try:
                choice = Prompt.ask("Select integration", choices=["1", "2"], default="1")
            except (KeyboardInterrupt, EOFError):
                console.print(f"\n[{theme.warning}]Setup cancelled[/{theme.warning}]\n")
                raise typer.Exit(0)

            if choice == "1":
                integration = "simplefin"
            else:
                console.print(f"[{theme.muted}]Setup cancelled[/{theme.muted}]\n")
                raise typer.Exit(0)

        # Handle specific integrations
        integration_lower = integration.lower()
        if integration_lower == "simplefin":
            # Block in demo mode
            if is_demo_mode():
                console.print(
                    f"[{theme.warning}]Cannot set up integrations in demo mode[/{theme.warning}]"
                )
                console.print(
                    f"[{theme.muted}]Use 'tl demo off' to switch to real mode first[/{theme.muted}]\n"
                )
                raise typer.Exit(1)
            _setup_simplefin(get_container, token)
        elif integration_lower == "demo":
            # Redirect to demo command
            console.print(f"[{theme.info}]Demo is now a mode, not an integration.[/{theme.info}]")
            console.print(f"[{theme.muted}]Use 'tl demo on' to enable demo mode[/{theme.muted}]\n")
            raise typer.Exit(0)
        else:
            display_error(f"Unknown integration: {integration}", show_log_hint=False)
            console.print(f"[{theme.muted}]Supported integrations: simplefin[/{theme.muted}]")
            raise typer.Exit(1)


def _setup_simplefin(get_container: callable, token: str | None = None) -> None:
    """Set up SimpleFIN integration."""
    container = get_container()
    integration_service = container.integration_service()
    simplefin_provider = container.get_integration_provider("simplefin")

    console.print(f"\n[{theme.ui_header}]SimpleFIN Setup[/{theme.ui_header}]\n")

    # Use provided token or prompt for it
    if token:
        setup_token = token.strip()
    else:
        console.print(
            f"[{theme.muted}]If you don't have a SimpleFIN account, create one at: https://beta-bridge.simplefin.org/[/{theme.muted}]\n"
        )

        console.print(f"[{theme.info}]Enter your SimpleFIN setup token[/{theme.info}]")
        console.print(f"[{theme.muted}](Press Ctrl+C to cancel)[/{theme.muted}]\n")

        try:
            setup_token = Prompt.ask("Token")
        except (KeyboardInterrupt, EOFError):
            console.print(f"\n[{theme.warning}]Setup cancelled[/{theme.warning}]\n")
            raise typer.Exit(0)

        if not setup_token or not setup_token.strip():
            console.print(f"[{theme.warning}]Setup cancelled[/{theme.warning}]\n")
            raise typer.Exit(0)

        setup_token = setup_token.strip()

    # Setup integration
    console.print()
    with console.status(f"[{theme.status_loading}]Verifying token and setting up integration..."):
        result = asyncio.run(
            integration_service.create_integration(
                simplefin_provider, "simplefin", {"setupToken": setup_token}
            )
        )

    if not result.success:
        display_error(f"Setup failed: {result.error}")
        raise typer.Exit(1)

    console.print(f"[{theme.success}]✓[/{theme.success}] SimpleFIN integration setup successfully!\n")
    console.print(f"[{theme.muted}]Use 'tl sync' to import your transactions[/{theme.muted}]\n")
