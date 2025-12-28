"""Configuration management for Treeline."""

import json
from pathlib import Path
from typing import Any, Dict
from typing import TypedDict, Optional, List
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


def _get_import_profiles_container(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Get the importProfiles container, ensuring nested structure exists."""
    if "importProfiles" not in settings:
        settings["importProfiles"] = {}
    container = settings["importProfiles"]
    if "profiles" not in container:
        container["profiles"] = {}
    if "accountMappings" not in container:
        container["accountMappings"] = {}
    return container


def get_import_profile(name: str) -> Optional[ImportProfile]:
    """Get import profile by name. Returns None if not found."""
    settings = load_settings()
    container = _get_import_profiles_container(settings)
    return container["profiles"].get(name)


def save_import_profile(
    name: str,
    column_mappings: Dict[str, str],
    flip_signs: bool = False,
    debit_negative: bool = False,
) -> None:
    """Save or update a named import profile."""
    settings = load_settings()
    container = _get_import_profiles_container(settings)

    container["profiles"][name] = {
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
    container = _get_import_profiles_container(settings)
    if name in container["profiles"]:
        del container["profiles"][name]
        save_settings(settings)
        return True
    return False


def list_import_profiles() -> List[str]:
    """Get list of all profile names."""
    settings = load_settings()
    container = _get_import_profiles_container(settings)
    return list(container["profiles"].keys())


def get_all_import_profiles() -> Dict[str, ImportProfile]:
    """Get all import profiles."""
    settings = load_settings()
    container = _get_import_profiles_container(settings)
    return container["profiles"]


# =============================================================================
# Account to Profile Mappings
# =============================================================================


def get_account_profile_mapping(account_id: str) -> Optional[str]:
    """Get the profile name mapped to an account. Returns None if not mapped."""
    settings = load_settings()
    container = _get_import_profiles_container(settings)
    return container["accountMappings"].get(account_id)


def set_account_profile_mapping(account_id: str, profile_name: str) -> None:
    """Set the profile mapping for an account."""
    settings = load_settings()
    container = _get_import_profiles_container(settings)
    container["accountMappings"][account_id] = profile_name
    save_settings(settings)


def remove_account_profile_mapping(account_id: str) -> bool:
    """Remove the profile mapping for an account. Returns True if removed."""
    settings = load_settings()
    container = _get_import_profiles_container(settings)
    if account_id in container["accountMappings"]:
        del container["accountMappings"][account_id]
        save_settings(settings)
        return True
    return False


def get_all_account_profile_mappings() -> Dict[str, str]:
    """Get all account to profile mappings."""
    settings = load_settings()
    container = _get_import_profiles_container(settings)
    return container["accountMappings"]
