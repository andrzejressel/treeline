"""Unit tests for BackupService."""

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from treeline.app.backup_service import BackupService
from treeline.infra.local_backup import LocalBackupStorage


@pytest.fixture
def temp_backup_dir():
    """Create a temporary backup directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def temp_db_file():
    """Create a temporary database file with content."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        f.write(b"mock database content for testing")
        f.flush()  # Ensure content is written
        temp_path = Path(f.name)

    yield temp_path

    # Cleanup
    temp_path.unlink(missing_ok=True)


@pytest.fixture
def backup_service(temp_backup_dir, temp_db_file):
    """Create a BackupService with local storage."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)
    return BackupService(
        storage_provider=storage,
        db_path=temp_db_file,
        max_backups=3,
    )


@pytest.mark.asyncio
async def test_backup_creates_backup(backup_service):
    """Test that backup creates a new backup."""
    result = await backup_service.backup()

    assert result.success
    assert result.data is not None
    assert result.data.name.startswith("treeline-")


@pytest.mark.asyncio
async def test_backup_enforces_retention(backup_service, temp_db_file):
    """Test that backup enforces max_backups retention policy."""
    # Create backups up to max (3) - microsecond precision ensures unique names
    backup_names = []
    for _ in range(3):
        result = await backup_service.backup()
        assert result.success
        backup_names.append(result.data.name)

    # Verify we have 3 backups
    list_result = await backup_service.list_backups()
    assert len(list_result.data) == 3

    # Create another backup - should delete oldest
    result = await backup_service.backup()
    assert result.success

    # Should still have 3 backups
    list_result = await backup_service.list_backups()
    assert len(list_result.data) == 3

    # The oldest backup (first created) should be gone
    current_names = [b.name for b in list_result.data]
    assert backup_names[0] not in current_names


@pytest.mark.asyncio
async def test_restore_works(backup_service, temp_db_file):
    """Test that restore overwrites the database."""
    # Backup original content
    original_content = temp_db_file.read_bytes()

    result = await backup_service.backup()
    assert result.success
    backup_name = result.data.name

    # Modify the database
    temp_db_file.write_bytes(b"modified content")
    assert temp_db_file.read_bytes() != original_content

    # Restore
    restore_result = await backup_service.restore(backup_name)
    assert restore_result.success

    # Verify content is restored
    assert temp_db_file.read_bytes() == original_content


@pytest.mark.asyncio
async def test_restore_nonexistent_backup(backup_service):
    """Test restore fails for nonexistent backup."""
    result = await backup_service.restore("nonexistent-backup.duckdb")

    assert not result.success
    assert "not found" in result.error.lower()


@pytest.mark.asyncio
async def test_list_backups(backup_service):
    """Test listing backups."""
    # Initially empty
    result = await backup_service.list_backups()
    assert result.success
    assert result.data == []

    # Create some backups
    await backup_service.backup()
    await backup_service.backup()

    result = await backup_service.list_backups()
    assert result.success
    assert len(result.data) == 2


@pytest.mark.asyncio
async def test_clear_all(backup_service):
    """Test clearing all backups."""
    # Create some backups
    await backup_service.backup()
    await backup_service.backup()

    # Clear
    result = await backup_service.clear_all()
    assert result.success
    assert result.data == 2

    # Verify empty
    list_result = await backup_service.list_backups()
    assert len(list_result.data) == 0


@pytest.mark.asyncio
async def test_max_backups_of_one(temp_backup_dir, temp_db_file):
    """Test that max_backups=1 works correctly."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)
    service = BackupService(
        storage_provider=storage,
        db_path=temp_db_file,
        max_backups=1,
    )

    # Create first backup
    result1 = await service.backup()
    assert result1.success
    name1 = result1.data.name

    # Create second backup - should delete first (microsecond precision ensures unique names)
    result2 = await service.backup()
    assert result2.success

    # Should only have 1 backup
    list_result = await service.list_backups()
    assert len(list_result.data) == 1
    assert list_result.data[0].name != name1


@pytest.mark.asyncio
async def test_backup_with_large_max_backups(temp_backup_dir, temp_db_file):
    """Test that large max_backups doesn't cause issues."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)
    service = BackupService(
        storage_provider=storage,
        db_path=temp_db_file,
        max_backups=100,
    )

    # Create a few backups
    for _ in range(5):
        result = await service.backup()
        assert result.success

    # All should exist (no retention triggered)
    list_result = await service.list_backups()
    assert len(list_result.data) == 5
