"""Encrypt and decrypt commands - manage database encryption."""

import asyncio
import json as json_module
import os

import typer
from rich.console import Console
from rich.prompt import Confirm, Prompt

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


def register(
    app: typer.Typer, get_container: callable, ensure_initialized: callable
) -> None:
    """Register the encrypt and decrypt commands with the app."""

    @app.command(name="encrypt")
    def encrypt_command(
        action: str = typer.Argument(
            None,
            help="Optional action: 'status' to check encryption status. Omit to enable encryption.",
        ),
        password: str = typer.Option(
            None,
            "--password",
            "-p",
            help="Encryption password (also reads TL_DB_PASSWORD env var)",
        ),
        json_output: bool = typer.Option(
            False,
            "--json",
            help="Output as JSON",
        ),
    ) -> None:
        """Enable database encryption or check status.

        Encrypt your database to protect financial data at rest.
        Uses AES-256-GCM encryption with Argon2id key derivation.

        IMPORTANT: If you forget your password, your data cannot be recovered!
        Always keep backups before encrypting.

        Examples:
          tl encrypt              # Enable encryption (prompts for password)
          tl encrypt status       # Check encryption status
          tl encrypt status --json  # Status as JSON
          tl encrypt -p "pass"    # Enable with password (scripted)

        Environment:
          TL_DB_PASSWORD         # Password for scripted use
        """
        # Block in demo mode
        if is_demo_mode():
            if json_output:
                print(json_module.dumps({"error": "Cannot encrypt demo database"}))
            else:
                console.print(
                    f"[{theme.warning}]Cannot encrypt demo database[/{theme.warning}]"
                )
                console.print(
                    f"[{theme.muted}]Demo mode uses a separate, unencrypted database[/{theme.muted}]"
                )
            raise typer.Exit(1)

        # If password provided via -p, set it in env so Container can use it
        # This avoids prompting when checking status on an encrypted DB
        if password and not os.environ.get("TL_DB_PASSWORD"):
            os.environ["TL_DB_PASSWORD"] = password

        if action == "status":
            ensure_initialized()
            _do_status(get_container, json_output)
        else:
            ensure_initialized()
            _do_encrypt(get_container, password, json_output)

    @app.command(name="decrypt")
    def decrypt_command(
        password: str = typer.Option(
            None,
            "--password",
            "-p",
            help="Current encryption password (also reads TL_DB_PASSWORD env var)",
        ),
        json_output: bool = typer.Option(
            False,
            "--json",
            help="Output as JSON",
        ),
    ) -> None:
        """Disable database encryption.

        Decrypt your database to remove encryption protection.
        Requires the current encryption password.

        Examples:
          tl decrypt              # Disable encryption (prompts for password)
          tl decrypt -p "pass"    # Disable with password (scripted)

        Environment:
          TL_DB_PASSWORD         # Password for scripted use
        """
        # Block in demo mode
        if is_demo_mode():
            if json_output:
                print(json_module.dumps({"error": "Demo database is not encrypted"}))
            else:
                console.print(
                    f"[{theme.warning}]Demo database is not encrypted[/{theme.warning}]"
                )
            raise typer.Exit(1)

        # If password provided via -p, set it in env so Container can use it
        # This avoids double-prompting (once for container, once for decrypt)
        if password and not os.environ.get("TL_DB_PASSWORD"):
            os.environ["TL_DB_PASSWORD"] = password

        ensure_initialized()
        _do_decrypt(get_container, password, json_output)


def _do_status(get_container: callable, json_output: bool) -> None:
    """Show encryption status."""
    container = get_container()
    encryption_service = container.encryption_service()

    result = asyncio.run(encryption_service.get_status())

    if not result.success:
        if json_output:
            print(json_module.dumps({"error": result.error}))
        else:
            display_error(result.error)
        raise typer.Exit(1)

    status = result.data

    if json_output:
        print(
            json_module.dumps(
                {
                    "encrypted": status.encrypted,
                    "algorithm": status.algorithm,
                    "version": status.version,
                }
            )
        )
        return

    if status.encrypted:
        console.print(f"\n[{theme.success}]Database is encrypted[/{theme.success}]")
        console.print(f"  Algorithm: {status.algorithm}")
        console.print(f"  Version: {status.version}\n")
    else:
        console.print(f"\n[{theme.muted}]Database is not encrypted[/{theme.muted}]")
        console.print(f"  [{theme.info}]Use 'tl encrypt' to enable encryption[/{theme.info}]\n")


def _do_encrypt(get_container: callable, password: str | None, json_output: bool) -> None:
    """Enable encryption."""
    container = get_container()
    encryption_service = container.encryption_service()

    # Check if already encrypted
    status_result = asyncio.run(encryption_service.get_status())
    if status_result.success and status_result.data.encrypted:
        if json_output:
            print(json_module.dumps({"error": "Database is already encrypted"}))
        else:
            console.print(f"[{theme.warning}]Database is already encrypted[/{theme.warning}]")
        raise typer.Exit(1)

    # Get password from option, env var, or interactive prompt
    if not password:
        password = os.environ.get("TL_DB_PASSWORD")

    if not password and not json_output:
        console.print(f"\n[{theme.ui_header}]Enable Database Encryption[/{theme.ui_header}]\n")
        console.print(
            f"[{theme.warning}]WARNING: If you forget your password, your data cannot be recovered![/{theme.warning}]"
        )
        console.print(f"[{theme.muted}]A backup will be created before encryption.[/{theme.muted}]\n")

        try:
            password = Prompt.ask("Enter encryption password", password=True)
            confirm = Prompt.ask("Confirm password", password=True)

            if password != confirm:
                console.print(f"[{theme.error}]Passwords do not match[/{theme.error}]")
                raise typer.Exit(1)

            if len(password) < 8:
                console.print(
                    f"[{theme.warning}]Password should be at least 8 characters[/{theme.warning}]"
                )
                if not Confirm.ask("Continue anyway?", default=False):
                    raise typer.Exit(0)

        except (KeyboardInterrupt, EOFError):
            console.print(f"\n[{theme.muted}]Cancelled[/{theme.muted}]\n")
            raise typer.Exit(0)

    if not password:
        if json_output:
            print(json_module.dumps({"error": "Password required"}))
        else:
            display_error("Password required", show_log_hint=False)
            console.print(
                f"[{theme.muted}]Use --password option or set TL_DB_PASSWORD env var[/{theme.muted}]"
            )
        raise typer.Exit(1)

    # Encrypt
    if not json_output:
        with console.status(f"[{theme.status_loading}]Encrypting database..."):
            result = asyncio.run(encryption_service.encrypt(password))
    else:
        result = asyncio.run(encryption_service.encrypt(password))

    if not result.success:
        if json_output:
            print(json_module.dumps({"error": result.error}))
        else:
            display_error(result.error)
        raise typer.Exit(1)

    backup_name = result.data.get("backup_name")

    if json_output:
        print(
            json_module.dumps(
                {
                    "encrypted": True,
                    "backup_name": backup_name,
                }
            )
        )
    else:
        console.print(f"\n[{theme.success}]Database encrypted successfully[/{theme.success}]")
        if backup_name:
            console.print(f"  [{theme.muted}]Safety backup: {backup_name}[/{theme.muted}]")
        console.print(
            f"\n[{theme.info}]Remember your password - it cannot be recovered![/{theme.info}]\n"
        )


def _do_decrypt(get_container: callable, password: str | None, json_output: bool) -> None:
    """Disable encryption."""
    container = get_container()
    encryption_service = container.encryption_service()

    # Check if encrypted
    status_result = asyncio.run(encryption_service.get_status())
    if not status_result.success or not status_result.data.encrypted:
        if json_output:
            print(json_module.dumps({"error": "Database is not encrypted"}))
        else:
            console.print(f"[{theme.warning}]Database is not encrypted[/{theme.warning}]")
        raise typer.Exit(1)

    # Get password from option, env var, or interactive prompt
    if not password:
        password = os.environ.get("TL_DB_PASSWORD")

    if not password and not json_output:
        console.print(f"\n[{theme.ui_header}]Disable Database Encryption[/{theme.ui_header}]\n")
        try:
            password = Prompt.ask("Enter current password", password=True)
        except (KeyboardInterrupt, EOFError):
            console.print(f"\n[{theme.muted}]Cancelled[/{theme.muted}]\n")
            raise typer.Exit(0)

    if not password:
        if json_output:
            print(json_module.dumps({"error": "Password required"}))
        else:
            display_error("Password required", show_log_hint=False)
            console.print(
                f"[{theme.muted}]Use --password option or set TL_DB_PASSWORD env var[/{theme.muted}]"
            )
        raise typer.Exit(1)

    # Decrypt
    if not json_output:
        with console.status(f"[{theme.status_loading}]Decrypting database..."):
            result = asyncio.run(encryption_service.decrypt(password))
    else:
        result = asyncio.run(encryption_service.decrypt(password))

    if not result.success:
        if json_output:
            print(json_module.dumps({"error": result.error}))
        else:
            display_error(result.error)
        raise typer.Exit(1)

    backup_name = result.data.get("backup_name")

    if json_output:
        print(
            json_module.dumps(
                {
                    "encrypted": False,
                    "backup_name": backup_name,
                }
            )
        )
    else:
        console.print(f"\n[{theme.success}]Database decrypted successfully[/{theme.success}]")
        if backup_name:
            console.print(f"  [{theme.muted}]Safety backup: {backup_name}[/{theme.muted}]")
        console.print()
