"""Dependency injection container for the application."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict

if TYPE_CHECKING:
    from treeline.app.encryption_service import EncryptionService

from treeline.abstractions import (
    BackupStorageProvider,
    DataAggregationProvider,
    IntegrationProvider,
    Repository,
)
from treeline.app.account_service import AccountService
from treeline.app.backfill_service import BackfillService
from treeline.app.backup_service import BackupService
from treeline.app.db_service import DbService
from treeline.app.doctor_service import DoctorService
from treeline.app.import_service import ImportService
from treeline.app.integration_service import IntegrationService
from treeline.app.plugin_service import PluginService
from treeline.app.preferences_service import PreferencesService
from treeline.app.status_service import StatusService
from treeline.app.sync_service import SyncService
from treeline.app.tagging_service import TaggingService
from treeline.infra.csv import CSVProvider
from treeline.infra.demo import DemoDataProvider
from treeline.infra.duckdb import DuckDBRepository
from treeline.infra.local_backup import LocalBackupStorage
from treeline.infra.simplefin import SimpleFINProvider

DEFAULT_MAX_BACKUPS = 7


class Container:
    """Dependency injection container for the application."""

    def __init__(
        self,
        treeline_dir: str,
        db_filename: str = "treeline.duckdb",
        password_callback: Callable[[], str] | None = None,
    ):
        """Initialize container.

        Args:
            treeline_dir: Directory where treeline data is stored (e.g., ~/.treeline)
            db_filename: Name of the database file (default: treeline.duckdb, demo mode uses demo.duckdb)
            password_callback: Optional callback to prompt for password interactively
        """
        self.treeline_dir = treeline_dir
        self.db_file_path = str(Path(treeline_dir) / db_filename)
        self.db_filename = db_filename
        self._instances: Dict[str, Any] = {}
        self._password_callback = password_callback
        self._encryption_key: str | None = None
        self._encryption_initialized = False

    @property
    def is_demo_mode(self) -> bool:
        """Check if this container is configured for demo mode."""
        return self.db_filename == "demo.duckdb"

    def _ensure_encryption_initialized(self) -> None:
        """Lazily initialize encryption key if database is encrypted.

        This is called automatically when accessing services that need database access.
        Demo mode databases are never encrypted. For encrypted databases,
        gets password from TL_DB_PASSWORD env var or password_callback.
        """
        if self._encryption_initialized:
            return
        self._encryption_initialized = True

        # Demo mode is never encrypted
        if self.is_demo_mode:
            return

        encryption_json_path = Path(self.treeline_dir) / "encryption.json"
        if not encryption_json_path.exists():
            return

        try:
            with open(encryption_json_path) as f:
                metadata = json.load(f)

            if not metadata.get("encrypted", False):
                return

            # Database is encrypted - check for pre-computed key first (from UI)
            precomputed_key = self._get_precomputed_key()
            if precomputed_key:
                self._encryption_key = precomputed_key
                return

            # Otherwise, need password to derive key
            password = self._get_password()
            if not password:
                raise RuntimeError(
                    "Password required for encrypted database. "
                    "Set TL_DB_PASSWORD environment variable or provide password interactively."
                )

            # Derive key using encryption service
            from treeline.app.encryption_service import EncryptionService

            svc = EncryptionService(
                treeline_dir=Path(self.treeline_dir),
                db_path=Path(self.db_file_path),
            )
            result = svc.derive_key_for_connection(password)
            if not result.success:
                raise RuntimeError(f"Failed to derive encryption key: {result.error}")
            self._encryption_key = result.data

        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid encryption.json: {e}")

    def _get_password(self) -> str | None:
        """Get password from environment variable or callback."""
        # Try environment variable first
        password = os.environ.get("TL_DB_PASSWORD")
        if password:
            return password

        # Try callback
        if self._password_callback:
            try:
                return self._password_callback()
            except (KeyboardInterrupt, EOFError):
                return None

        return None

    def _get_precomputed_key(self) -> str | None:
        """Get pre-computed encryption key from environment variable.

        This is used when the UI has already derived the key and wants to
        pass it to CLI commands without re-deriving.
        """
        return os.environ.get("TL_DB_KEY")

    def repository(self) -> Repository:
        """Get the repository instance."""
        if "repository" not in self._instances:
            # Initialize encryption lazily when repository is first accessed
            self._ensure_encryption_initialized()
            self._instances["repository"] = DuckDBRepository(
                self.db_file_path,
                encryption_key=self._encryption_key,
            )
        return self._instances["repository"]

    def provider_registry(self) -> Dict[str, DataAggregationProvider]:
        """Get the provider registry.

        Providers are registered by name. The 'demo' provider returns mock data
        for testing. CSV import always uses the real CSVProvider.
        """
        if "provider_registry" not in self._instances:
            self._instances["provider_registry"] = {
                "simplefin": SimpleFINProvider(),
                "demo": DemoDataProvider(),
                "csv": CSVProvider(),
            }
        return self._instances["provider_registry"]

    def sync_service(self) -> SyncService:
        """Get the sync service instance."""
        if "sync_service" not in self._instances:
            self._instances["sync_service"] = SyncService(
                self.provider_registry(),
                self.repository(),
                self.account_service(),
                self.integration_service(),
                self.preferences_service(),
            )
        return self._instances["sync_service"]

    def integration_service(self) -> IntegrationService:
        """Get the integration service instance."""
        if "integration_service" not in self._instances:
            self._instances["integration_service"] = IntegrationService(
                self.repository()
            )
        return self._instances["integration_service"]

    def get_integration_provider(self, integration_name: str) -> IntegrationProvider:
        """Get an integration provider by name."""
        provider = self.provider_registry().get(integration_name)
        if not provider:
            raise ValueError(f"Unknown integration: {integration_name}")

        if not isinstance(provider, IntegrationProvider):
            raise ValueError(
                f"Provider {integration_name} does not support integration setup"
            )

        return provider

    def account_service(self) -> AccountService:
        """Get the account service instance."""
        if "account_service" not in self._instances:
            self._instances["account_service"] = AccountService(self.repository())
        return self._instances["account_service"]

    def status_service(self) -> StatusService:
        """Get the status service instance."""
        if "status_service" not in self._instances:
            self._instances["status_service"] = StatusService(self.repository())
        return self._instances["status_service"]

    def db_service(self) -> DbService:
        """Get the DB service instance."""
        if "db_service" not in self._instances:
            self._instances["db_service"] = DbService(self.repository())
        return self._instances["db_service"]

    def doctor_service(self) -> DoctorService:
        """Get the doctor service instance."""
        if "doctor_service" not in self._instances:
            self._instances["doctor_service"] = DoctorService(
                self.repository(),
                sync_service=self.sync_service(),
            )
        return self._instances["doctor_service"]

    def tagging_service(self) -> TaggingService:
        """Get the tagging service instance."""
        if "tagging_service" not in self._instances:
            self._instances["tagging_service"] = TaggingService(self.repository())
        return self._instances["tagging_service"]

    def import_service(self) -> ImportService:
        """Get the import service instance."""
        if "import_service" not in self._instances:
            self._instances["import_service"] = ImportService(
                self.repository(), self.provider_registry()
            )
        return self._instances["import_service"]

    def backfill_service(self) -> BackfillService:
        """Get the backfill service instance."""
        if "backfill_service" not in self._instances:
            self._instances["backfill_service"] = BackfillService(self.repository())
        return self._instances["backfill_service"]

    def plugin_service(self) -> PluginService:
        """Get the plugin service instance."""
        if "plugin_service" not in self._instances:
            plugins_dir = Path(self.treeline_dir) / "plugins"
            self._instances["plugin_service"] = PluginService(plugins_dir)
        return self._instances["plugin_service"]

    def preferences_service(self) -> PreferencesService:
        """Get the preferences service instance."""
        if "preferences_service" not in self._instances:
            self._instances["preferences_service"] = PreferencesService()
        return self._instances["preferences_service"]

    def backup_storage_provider(self) -> BackupStorageProvider:
        """Get the backup storage provider instance.

        Uses factory pattern: demo mode gets a separate backup directory.
        """
        if "backup_storage_provider" not in self._instances:
            # Demo mode uses separate backup directory
            if self.is_demo_mode:
                backup_dir = Path(self.treeline_dir) / "backups-demo"
            else:
                backup_dir = Path(self.treeline_dir) / "backups"

            self._instances["backup_storage_provider"] = LocalBackupStorage(
                backup_dir=backup_dir,
                treeline_dir=Path(self.treeline_dir),
            )
        return self._instances["backup_storage_provider"]

    def backup_service(self, max_backups: int = DEFAULT_MAX_BACKUPS) -> BackupService:
        """Get the backup service instance.

        Args:
            max_backups: Maximum number of backups to retain (default: 7)
        """
        # Don't cache - max_backups can vary per call
        return BackupService(
            storage_provider=self.backup_storage_provider(),
            db_path=Path(self.db_file_path),
            max_backups=max_backups,
        )

    def encryption_service(self) -> "EncryptionService":
        """Get the encryption service instance."""
        if "encryption_service" not in self._instances:
            from treeline.app.encryption_service import EncryptionService

            # Don't pass backup_service for demo mode
            backup_svc = None if self.is_demo_mode else self.backup_service()

            self._instances["encryption_service"] = EncryptionService(
                treeline_dir=Path(self.treeline_dir),
                db_path=Path(self.db_file_path),
                backup_service=backup_svc,
            )
        return self._instances["encryption_service"]
