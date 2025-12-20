"""Configuration management for Treeline."""

import json
from pathlib import Path
from typing import Any, Dict

from treeline.utils import get_treeline_dir


def get_settings_path() -> Path:
    """Get path to unified settings file (shared with UI)."""
    return get_treeline_dir() / "settings.json"


def load_settings() -> Dict[str, Any]:
    """Load settings from file, returning default structure if not found."""
    settings_path = get_settings_path()
    if not settings_path.exists():
        return {"app": {}, "plugins": {}}

    try:
        with open(settings_path) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {"app": {}, "plugins": {}}


def save_settings(settings: Dict[str, Any]) -> None:
    """Save settings to file."""
    settings_path = get_settings_path()
    settings_path.parent.mkdir(parents=True, exist_ok=True)

    with open(settings_path, "w") as f:
        json.dump(settings, f, indent=2)


def is_demo_mode() -> bool:
    """Check if demo mode is enabled.

    Demo mode can be enabled via:
    1. Settings file (tl demo on)
    2. Environment variable TREELINE_DEMO_MODE (for CI/testing)
    """
    import os

    # Env var takes precedence (for CI/testing)
    env_demo = os.getenv("TREELINE_DEMO_MODE", "").lower()
    if env_demo in ("true", "1", "yes"):
        return True
    if env_demo in ("false", "0", "no"):
        return False

    # Fall back to settings file
    settings = load_settings()
    app_settings = settings.get("app", {})
    return app_settings.get("demoMode", False)


def set_demo_mode(enabled: bool) -> None:
    """Set demo mode in settings file."""
    settings = load_settings()
    if "app" not in settings:
        settings["app"] = {}
    settings["app"]["demoMode"] = enabled
    save_settings(settings)
