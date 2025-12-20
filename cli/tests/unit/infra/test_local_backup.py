"""Unit tests for LocalBackupStorage."""

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

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


@pytest.mark.asyncio
async def test_create_backup_success(temp_backup_dir, temp_db_file):
    """Test creating a backup successfully."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)

    result = await storage.create_backup(temp_db_file)

    assert result.success
    assert result.data is not None

    backup = result.data
    assert backup.name.startswith("treeline-")
    assert backup.name.endswith(".zip")
    assert backup.size_bytes > 0
    assert backup.created_at is not None
    assert backup.created_at.tzinfo is not None  # Timezone-aware

    # Verify file was actually created
    backup_path = temp_backup_dir / backup.name
    assert backup_path.exists()


@pytest.mark.asyncio
async def test_create_backup_source_not_found(temp_backup_dir):
    """Test creating a backup when source file doesn't exist."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)

    result = await storage.create_backup(Path("/nonexistent/file.duckdb"))

    assert not result.success
    assert "not found" in result.error.lower()


@pytest.mark.asyncio
async def test_create_backup_creates_directory(temp_db_file):
    """Test that backup creates directory if it doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backup_dir = Path(tmpdir) / "nested" / "backups"
        storage = LocalBackupStorage(backup_dir=backup_dir)

        result = await storage.create_backup(temp_db_file)

        assert result.success
        assert backup_dir.exists()


@pytest.mark.asyncio
async def test_list_backups_empty(temp_backup_dir):
    """Test listing backups when none exist."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)

    result = await storage.list_backups()

    assert result.success
    assert result.data == []


@pytest.mark.asyncio
async def test_list_backups_nonexistent_dir():
    """Test listing backups when directory doesn't exist."""
    storage = LocalBackupStorage(backup_dir=Path("/nonexistent/dir"))

    result = await storage.list_backups()

    assert result.success
    assert result.data == []


@pytest.mark.asyncio
async def test_list_backups_sorted_newest_first(temp_backup_dir, temp_db_file):
    """Test that backups are listed newest first."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)

    # Create multiple backups (microsecond precision ensures unique names)
    backups_created = []
    for _ in range(3):
        result = await storage.create_backup(temp_db_file)
        assert result.success
        backups_created.append(result.data)

    result = await storage.list_backups()

    assert result.success
    assert len(result.data) == 3

    # Verify sorted newest first
    for i in range(len(result.data) - 1):
        assert result.data[i].created_at >= result.data[i + 1].created_at


@pytest.mark.asyncio
async def test_restore_backup_success(temp_backup_dir, temp_db_file):
    """Test restoring a backup successfully."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)

    # Create a backup
    create_result = await storage.create_backup(temp_db_file)
    assert create_result.success
    backup_name = create_result.data.name

    # Create a different target file
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        target_path = Path(f.name)

    try:
        # Modify the target to verify it gets overwritten
        target_path.write_text("different content")

        # Restore
        result = await storage.restore_backup(backup_name, target_path)

        assert result.success

        # Verify content matches original
        assert target_path.read_bytes() == temp_db_file.read_bytes()
    finally:
        target_path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_restore_backup_not_found(temp_backup_dir):
    """Test restoring a backup that doesn't exist."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)

    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        target_path = Path(f.name)

    try:
        result = await storage.restore_backup("nonexistent-backup.duckdb", target_path)

        assert not result.success
        assert "not found" in result.error.lower()
    finally:
        target_path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_delete_backup_success(temp_backup_dir, temp_db_file):
    """Test deleting a backup successfully."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)

    # Create a backup
    create_result = await storage.create_backup(temp_db_file)
    assert create_result.success
    backup_name = create_result.data.name

    # Verify it exists
    assert (temp_backup_dir / backup_name).exists()

    # Delete it
    result = await storage.delete_backup(backup_name)

    assert result.success
    assert not (temp_backup_dir / backup_name).exists()


@pytest.mark.asyncio
async def test_delete_backup_not_found(temp_backup_dir):
    """Test deleting a backup that doesn't exist."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)

    result = await storage.delete_backup("nonexistent-backup.duckdb")

    assert not result.success
    assert "not found" in result.error.lower()


@pytest.mark.asyncio
async def test_delete_all_backups(temp_backup_dir, temp_db_file):
    """Test deleting all backups."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)

    # Create multiple backups
    for _ in range(3):
        result = await storage.create_backup(temp_db_file)
        assert result.success

    # Verify they exist
    list_result = await storage.list_backups()
    assert len(list_result.data) == 3

    # Delete all
    result = await storage.delete_all_backups()

    assert result.success
    assert result.data == 3

    # Verify all deleted
    list_result = await storage.list_backups()
    assert len(list_result.data) == 0


@pytest.mark.asyncio
async def test_delete_all_backups_empty(temp_backup_dir):
    """Test deleting all backups when none exist."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)

    result = await storage.delete_all_backups()

    assert result.success
    assert result.data == 0


@pytest.mark.asyncio
async def test_backup_name_format(temp_backup_dir, temp_db_file):
    """Test that backup name follows expected format."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)

    result = await storage.create_backup(temp_db_file)

    assert result.success
    backup_name = result.data.name

    # Should match pattern: treeline-YYYY-MM-DDTHH-MM-SS-ffffff.zip
    assert backup_name.startswith("treeline-")
    assert backup_name.endswith(".zip")

    # Extract and validate timestamp portion
    timestamp_str = backup_name[9:-4]  # Remove "treeline-" and ".zip"
    # Should be parseable as datetime with microseconds
    parsed = datetime.strptime(timestamp_str, "%Y-%m-%dT%H-%M-%S-%f")
    assert parsed is not None


@pytest.mark.asyncio
async def test_ignores_non_backup_files(temp_backup_dir, temp_db_file):
    """Test that list_backups ignores files that don't match naming pattern."""
    storage = LocalBackupStorage(backup_dir=temp_backup_dir)

    # Create a valid backup
    result = await storage.create_backup(temp_db_file)
    assert result.success

    # Create some files that shouldn't be picked up
    (temp_backup_dir / "random-file.txt").touch()
    (temp_backup_dir / "treeline.duckdb").touch()  # Missing timestamp
    (temp_backup_dir / "backup-2024-01-01.duckdb").touch()  # Wrong prefix

    # List should only return the valid backup
    list_result = await storage.list_backups()

    assert list_result.success
    assert len(list_result.data) == 1
    assert list_result.data[0].name == result.data.name
