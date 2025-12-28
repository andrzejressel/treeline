"""Plugin command - manage UI plugins."""

import json
from pathlib import Path

import typer
from pydantic import BaseModel
from rich.console import Console

from treeline.theme import get_theme
from treeline.utils import get_log_file_path

console = Console()
theme = get_theme()

# Create plugin subcommand group
plugin_app = typer.Typer(help="Plugin management commands")


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


def register(app: typer.Typer, get_container: callable) -> None:
    """Register the plugin commands with the app."""
    app.add_typer(plugin_app, name="plugin")

    @plugin_app.command(name="new")
    def plugin_new_command(
        name: str = typer.Argument(..., help="Plugin name"),
        directory: str = typer.Option(
            None,
            "--directory",
            "-d",
            help="Directory to create plugin in (defaults to current directory)",
        ),
    ) -> None:
        """Create a new plugin from template.

        Examples:
          tl plugin new my-plugin
          tl plugin new my-plugin --directory ~/my-plugins
        """
        container = get_container()
        plugin_service = container.plugin_service()

        target_dir = Path(directory).expanduser() if directory else None

        result = plugin_service.create_plugin(name, target_dir)

        if not result.success:
            display_error(result.error)
            raise typer.Exit(1)

        plugin_dir = result.data["plugin_dir"]
        console.print(f"[{theme.success}]✓ Created plugin: {name}[/{theme.success}]")
        console.print(f"\nPlugin directory: {plugin_dir}")
        console.print(f"\n[{theme.info}]Next steps:[/{theme.info}]")
        console.print(f"  1. cd {plugin_dir}")
        console.print("  2. npm install")
        console.print("  3. Edit src/index.ts and src/*View.svelte")
        console.print("  4. npm run build")
        console.print(f"  5. tl plugin install {plugin_dir}\n")

    @plugin_app.command(name="install")
    def plugin_install_command(
        source: str = typer.Argument(..., help="Local directory path or GitHub URL"),
        version: str = typer.Option(
            None,
            "--version",
            "-v",
            help="Version to install (e.g., v1.0.0). Defaults to latest release.",
        ),
        json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
        force_build: bool = typer.Option(
            False,
            "--rebuild",
            help="Force rebuild even if dist/index.js exists (local installs only)",
        ),
    ) -> None:
        """Install a plugin from local directory or GitHub release.

        For GitHub URLs, downloads pre-built release assets (manifest.json and index.js).
        For local directories, builds from source if needed.

        Examples:
          tl plugin install ~/my-plugin
          tl plugin install https://github.com/user/my-plugin
          tl plugin install https://github.com/user/my-plugin --version v1.0.0
          tl plugin install ~/my-plugin --rebuild
        """
        container = get_container()
        plugin_service = container.plugin_service()

        if not json_output:
            with console.status(
                f"[{theme.status_loading}]Installing plugin from {source}..."
            ):
                result = plugin_service.install_plugin(
                    source, version=version, force_build=force_build
                )
        else:
            result = plugin_service.install_plugin(
                source, version=version, force_build=force_build
            )

        if not result.success:
            if json_output:
                output_json({"success": False, "error": result.error})
            else:
                display_error(result.error)
            raise typer.Exit(1)

        if json_output:
            output_json({"success": True, **result.data})
        else:
            console.print(
                f"\n[{theme.success}]✓ Installed plugin: {result.data['plugin_name']}[/{theme.success}]"
            )
            console.print(f"  Plugin ID: {result.data['plugin_id']}")
            console.print(f"  Version: {result.data['version']}")
            console.print(f"  Location: {result.data['install_dir']}")
            if result.data.get("source"):
                console.print(f"  Source: {result.data['source']}")
            if result.data.get("built"):
                console.print(f"  [{theme.muted}](Built from source)[/{theme.muted}]")
            console.print(
                f"\n[{theme.info}]Restart the Treeline UI to load the plugin[/{theme.info}]\n"
            )

    @plugin_app.command(name="uninstall")
    def plugin_uninstall_command(
        plugin_id: str = typer.Argument(..., help="Plugin ID to uninstall"),
        json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    ) -> None:
        """Uninstall a plugin.

        Examples:
          tl plugin uninstall my-plugin
        """
        container = get_container()
        plugin_service = container.plugin_service()

        result = plugin_service.uninstall_plugin(plugin_id)

        if not result.success:
            if json_output:
                output_json({"success": False, "error": result.error})
            else:
                display_error(result.error)
            raise typer.Exit(1)

        if json_output:
            output_json({"success": True, **result.data})
        else:
            console.print(
                f"[{theme.success}]✓ Uninstalled plugin: {result.data['plugin_name']}[/{theme.success}]\n"
            )

    @plugin_app.command(name="list")
    def plugin_list_command(
        json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    ) -> None:
        """List installed plugins.

        Examples:
          tl plugin list
          tl plugin list --json
        """
        container = get_container()
        plugin_service = container.plugin_service()

        result = plugin_service.list_plugins()

        if not result.success:
            if json_output:
                output_json({"success": False, "error": result.error})
            else:
                display_error(result.error)
            raise typer.Exit(1)

        plugins = result.data

        if json_output:
            output_json({"success": True, "plugins": plugins})
        else:
            if not plugins:
                console.print(f"\n[{theme.muted}]No plugins installed[/{theme.muted}]")
                console.print(
                    f"[{theme.muted}]Use 'tl plugin new <name>' to create a plugin[/{theme.muted}]\n"
                )
                return

            console.print(
                f"\n[{theme.ui_header}]Installed Plugins[/{theme.ui_header}]\n"
            )

            for plugin in plugins:
                console.print(
                    f"[{theme.emphasis}]{plugin['name']}[/{theme.emphasis}] ({plugin['id']})"
                )
                console.print(f"  Version: {plugin['version']}")
                if plugin.get("description"):
                    console.print(
                        f"  [{theme.muted}]{plugin['description']}[/{theme.muted}]"
                    )
                if plugin.get("author"):
                    console.print(
                        f"  [{theme.muted}]by {plugin['author']}[/{theme.muted}]"
                    )
                console.print()

    @plugin_app.command(name="manifest")
    def plugin_manifest_command(
        source: str = typer.Argument(..., help="GitHub URL of the plugin"),
        version: str = typer.Option(
            None,
            "--version",
            "-v",
            help="Version to fetch (e.g., v1.0.0). Defaults to latest release.",
        ),
    ) -> None:
        """Fetch plugin manifest from GitHub release.

        Outputs the manifest.json contents as JSON. This is used by the UI
        to preview plugin permissions before installing.

        Examples:
          tl plugin manifest https://github.com/user/my-plugin
          tl plugin manifest https://github.com/user/my-plugin --version v1.0.0
        """
        container = get_container()
        plugin_service = container.plugin_service()

        result = plugin_service.fetch_manifest(source, version=version)

        if not result.success:
            output_json({"success": False, "error": result.error})
            raise typer.Exit(1)

        output_json({"success": True, **result.data})
