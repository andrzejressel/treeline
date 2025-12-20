"""Theme management for Treeline CLI."""

import json
from pathlib import Path
from typing import Any, Dict


class Theme:
    """Manages CLI color themes."""

    def __init__(self, theme_data: Dict[str, Any]):
        """Initialize theme with theme data.

        Args:
            theme_data: Dictionary containing theme configuration
        """
        self._data = theme_data
        self._colors = theme_data.get("colors", {})
        self._status = theme_data.get("status", {})
        self._ui = theme_data.get("ui", {})

    @classmethod
    def load(cls, theme_name: str = "default") -> "Theme":
        """Load a theme by name.

        Args:
            theme_name: Name of the theme to load (without .json extension)

        Returns:
            Theme instance

        Raises:
            FileNotFoundError: If theme file doesn't exist
            json.JSONDecodeError: If theme file is invalid JSON
        """
        theme_path = Path(__file__).parent / "themes" / f"{theme_name}.json"

        if not theme_path.exists():
            raise FileNotFoundError(f"Theme '{theme_name}' not found at {theme_path}")

        with open(theme_path, "r") as f:
            theme_data = json.load(f)

        return cls(theme_data)

    @property
    def name(self) -> str:
        """Get theme name."""
        return self._data.get("name", "Unknown")

    @property
    def description(self) -> str:
        """Get theme description."""
        return self._data.get("description", "")

    # Color accessors
    @property
    def primary(self) -> str:
        """Primary brand color."""
        return self._colors.get("primary", "green")

    @property
    def success(self) -> str:
        """Success state color."""
        return self._colors.get("success", "green")

    @property
    def error(self) -> str:
        """Error state color."""
        return self._colors.get("error", "red")

    @property
    def warning(self) -> str:
        """Warning state color."""
        return self._colors.get("warning", "yellow")

    @property
    def info(self) -> str:
        """Info state color."""
        return self._colors.get("info", "cyan")

    @property
    def muted(self) -> str:
        """Muted/dim text color."""
        return self._colors.get("muted", "dim")

    @property
    def emphasis(self) -> str:
        """Emphasized/bold text color."""
        return self._colors.get("emphasis", "bold")

    @property
    def highlight(self) -> str:
        """Highlighted text color."""
        return self._colors.get("highlight", "bold cyan")

    @property
    def separator(self) -> str:
        """Separator line color."""
        return self._colors.get("separator", "dim")

    @property
    def link(self) -> str:
        """Hyperlink color."""
        return self._colors.get("link", "blue underline")

    @property
    def positive_amount(self) -> str:
        """Positive money amount color."""
        return self._colors.get("positive_amount", "green")

    @property
    def negative_amount(self) -> str:
        """Negative money amount color."""
        return self._colors.get("negative_amount", "red")

    @property
    def neutral(self) -> str:
        """Neutral text color."""
        return self._colors.get("neutral", "white")

    # Status accessors
    @property
    def status_loading(self) -> str:
        """Loading status color."""
        return self._status.get("loading", "bold green")

    @property
    def status_complete(self) -> str:
        """Complete status color."""
        return self._status.get("complete", "green")

    @property
    def status_failed(self) -> str:
        """Failed status color."""
        return self._status.get("failed", "red")

    # UI element accessors
    @property
    def ui_header(self) -> str:
        """Header text color."""
        return self._ui.get("header", "bold cyan")

    @property
    def ui_subheader(self) -> str:
        """Subheader text color."""
        return self._ui.get("subheader", "cyan")

    @property
    def ui_label(self) -> str:
        """Label text color."""
        return self._ui.get("label", "dim")

    @property
    def ui_value(self) -> str:
        """Value text color."""
        return self._ui.get("value", "white")

    @property
    def ui_selected(self) -> str:
        """Selected item color."""
        return self._ui.get("selected", "bold cyan")

    @property
    def ui_unselected(self) -> str:
        """Unselected item color."""
        return self._ui.get("unselected", "white")


# Global theme instance
_theme: Theme | None = None


def get_theme() -> Theme:
    """Get the current theme instance.

    Returns:
        Current Theme instance
    """
    global _theme
    if _theme is None:
        _theme = Theme.load("default")
    return _theme


def set_theme(theme_name: str) -> None:
    """Set the current theme.

    Args:
        theme_name: Name of the theme to load
    """
    global _theme
    _theme = Theme.load(theme_name)
