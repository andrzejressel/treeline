"""Backup storage provider abstraction."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from treeline.domain import BackupMetadata, Result


class BackupStorageProvider(ABC):
    """Abstract interface for backup storage backends.

    Implementations handle the actual storage mechanics (local filesystem, S3, etc.)
    while the BackupService handles policy (retention, settings).
    """

    @abstractmethod
    async def create_backup(self, source_path: Path) -> Result[BackupMetadata]:
        """Create a backup of the source file.

        Args:
            source_path: Path to the file to backup (e.g., DuckDB database)

        Returns:
            Result containing BackupMetadata for the created backup
        """
        pass

    @abstractmethod
    async def list_backups(self) -> Result[List[BackupMetadata]]:
        """List all available backups, sorted newest first.

        Returns:
            Result containing list of BackupMetadata, ordered by created_at descending
        """
        pass

    @abstractmethod
    async def restore_backup(
        self,
        backup_name: str,
        target_path: Path,
    ) -> Result[None]:
        """Restore a backup to the target path.

        Args:
            backup_name: Name of the backup to restore
            target_path: Path where restored file should be written

        Returns:
            Result indicating success/failure
        """
        pass

    @abstractmethod
    async def delete_backup(self, backup_name: str) -> Result[None]:
        """Delete a specific backup.

        Args:
            backup_name: Name of the backup to delete

        Returns:
            Result indicating success/failure
        """
        pass

    @abstractmethod
    async def delete_all_backups(self) -> Result[int]:
        """Delete all backups.

        Returns:
            Result containing count of deleted backups
        """
        pass
