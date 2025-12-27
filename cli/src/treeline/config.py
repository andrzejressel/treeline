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


# =============================================================================
# Import Profiles (named, reusable across accounts)
# =============================================================================

from typing import TypedDict, Optional, List
from datetime import datetime


class ImportProfileColumnMappings(TypedDict, total=False):
    date: str
    amount: str
    description: str
    debit: str
    credit: str


class ImportProfileOptions(TypedDict, total=False):
    flipSigns: bool
    debitNegative: bool


class ImportProfile(TypedDict, total=False):
    columnMappings: ImportProfileColumnMappings
    options: ImportProfileOptions


def get_import_profile(name: str) -> Optional[ImportProfile]:
    """Get import profile by name. Returns None if not found."""
    settings = load_settings()
    profiles = settings.get("importProfiles", {})
    return profiles.get(name)


def save_import_profile(
    name: str,
    column_mappings: Dict[str, str],
    flip_signs: bool = False,
    debit_negative: bool = False,
) -> None:
    """Save or update a named import profile."""
    settings = load_settings()
    if "importProfiles" not in settings:
        settings["importProfiles"] = {}

    settings["importProfiles"][name] = {
        "columnMappings": column_mappings,
        "options": {
            "flipSigns": flip_signs,
            "debitNegative": debit_negative,
        },
    }
    save_settings(settings)


def delete_import_profile(name: str) -> bool:
    """Delete import profile by name. Returns True if deleted, False if not found."""
    settings = load_settings()
    profiles = settings.get("importProfiles", {})
    if name in profiles:
        del profiles[name]
        settings["importProfiles"] = profiles
        save_settings(settings)
        return True
    return False


def list_import_profiles() -> List[str]:
    """Get list of all profile names."""
    settings = load_settings()
    return list(settings.get("importProfiles", {}).keys())


def get_all_import_profiles() -> Dict[str, ImportProfile]:
    """Get all import profiles."""
    settings = load_settings()
    return settings.get("importProfiles", {})
