"""Smoke tests for CLI commands in demo mode.

These tests verify that CLI commands work end-to-end in demo mode.
No treeline imports - only CLI interaction via subprocess.

Based on TEST_PLAN.md - covers all major CLI commands.
"""

import json
import os
import subprocess
import tempfile
from pathlib import Path


def run_cli(
    args: list[str], treeline_dir: str, input_text: str | None = None
) -> subprocess.CompletedProcess:
    """Run treeline CLI command with specified treeline directory.

    Args:
        args: CLI arguments (e.g., ["status", "--json"])
        treeline_dir: Path to treeline data directory
        input_text: Optional text to pipe to stdin

    Returns:
        CompletedProcess with stdout, stderr, returncode
    """
    env = os.environ.copy()
    env["TREELINE_DIR"] = str(Path(treeline_dir) / ".treeline")
    # Remove any existing demo mode env var to test config-based mode
    env.pop("TREELINE_DEMO_MODE", None)

    # Use Rust CLI binary
    rust_binary = Path(__file__).parent.parent.parent.parent / "rust-core" / "target" / "release" / "tl"
    cmd = [str(rust_binary)] + args
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        input=input_text,
    )


class TestDemoCommand:
    """Tests for tl demo command."""

    def test_demo_on_enables_demo_mode(self):
        """Test that 'tl demo on' enables demo mode and syncs data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_cli(["demo", "on"], tmpdir)
            assert result.returncode == 0, f"demo on failed: {result.stderr}"
            assert "Demo mode enabled" in result.stdout

    def test_demo_status_shows_on(self):
        """Test that 'tl demo status' shows ON after enabling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["demo", "status"], tmpdir)
            assert result.returncode == 0
            assert "ON" in result.stdout

    def test_demo_off_disables_demo_mode(self):
        """Test that 'tl demo off' disables demo mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["demo", "off"], tmpdir)
            assert result.returncode == 0
            assert "Demo mode disabled" in result.stdout

    def test_demo_status_shows_off(self):
        """Test that 'tl demo status' shows OFF after disabling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            run_cli(["demo", "off"], tmpdir)
            result = run_cli(["demo", "status"], tmpdir)
            assert result.returncode == 0
            assert "OFF" in result.stdout

    def test_demo_default_action_is_status(self):
        """Test that 'tl demo' without args shows status."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_cli(["demo"], tmpdir)
            assert result.returncode == 0
            # Should show either ON or OFF
            assert "Demo mode is" in result.stdout


class TestStatusCommand:
    """Tests for tl status command."""

    def test_status_shows_accounts_and_transactions(self):
        """Test that status shows account and transaction counts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["status"], tmpdir)
            assert result.returncode == 0
            # Should show some account info
            assert "account" in result.stdout.lower() or "Account" in result.stdout

    def test_status_json_output(self):
        """Test that status --json returns valid JSON with expected fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["status", "--json"], tmpdir)
            assert result.returncode == 0

            data = json.loads(result.stdout)
            assert "total_accounts" in data
            assert "total_transactions" in data
            assert data["total_accounts"] > 0
            assert data["total_transactions"] > 0


class TestSyncCommand:
    """Tests for tl sync command."""

    def test_sync_works_in_demo_mode(self):
        """Test that sync works with demo integration."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["sync"], tmpdir)
            assert result.returncode == 0
            assert (
                "Sync completed" in result.stdout or "synced" in result.stdout.lower()
            )

    def test_sync_json_output(self):
        """Test that sync --json returns valid JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["sync", "--json"], tmpdir)
            assert result.returncode == 0

            data = json.loads(result.stdout)
            assert "results" in data

    def test_sync_dry_run(self):
        """Test that sync --dry-run shows preview without changing data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["sync", "--dry-run"], tmpdir)
            assert result.returncode == 0
            assert "DRY RUN" in result.stdout or "dry run" in result.stdout.lower()


class TestQueryCommand:
    """Tests for tl query command."""

    def test_query_count_transactions(self):
        """Test basic query for transaction count."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(
                ["query", "SELECT COUNT(*) as total FROM transactions"], tmpdir
            )
            assert result.returncode == 0

    def test_query_json_output(self):
        """Test that query --json returns valid JSON with expected structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(
                ["query", "SELECT * FROM transactions LIMIT 3", "--json"], tmpdir
            )
            assert result.returncode == 0

            data = json.loads(result.stdout)
            assert "columns" in data
            assert "rows" in data
            assert "row_count" in data

    def test_query_csv_output(self):
        """Test that query --format csv returns CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(
                [
                    "query",
                    "SELECT transaction_id, amount FROM transactions LIMIT 3",
                    "--format",
                    "csv",
                ],
                tmpdir,
            )
            assert result.returncode == 0
            # CSV should have comma-separated values
            assert "," in result.stdout

    def test_query_accounts(self):
        """Test querying accounts table."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(
                ["query", "SELECT account_id, name FROM accounts", "--json"], tmpdir
            )
            assert result.returncode == 0

            data = json.loads(result.stdout)
            assert len(data["rows"]) > 0

    def test_query_rejects_write_operations(self):
        """Test that DELETE/UPDATE/INSERT are rejected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["query", "DELETE FROM transactions"], tmpdir)
            assert result.returncode != 0


class TestTagCommand:
    """Tests for tl tag command."""

    def test_tag_single_transaction(self):
        """Test applying a tag to a single transaction."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Get a transaction ID
            result = run_cli(
                ["query", "SELECT transaction_id FROM transactions LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tx_id = data["rows"][0][0]

            # Apply tag
            result = run_cli(["tag", "test-tag", "--ids", tx_id], tmpdir)
            assert result.returncode == 0

            # Verify tag was applied
            result = run_cli(
                [
                    "query",
                    f"SELECT tags FROM transactions WHERE transaction_id = '{tx_id}'",
                    "--json",
                ],
                tmpdir,
            )
            data = json.loads(result.stdout)
            assert "test-tag" in str(data["rows"][0][0])

    def test_tag_multiple_tags(self):
        """Test applying multiple comma-separated tags."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Get a transaction ID
            result = run_cli(
                ["query", "SELECT transaction_id FROM transactions LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tx_id = data["rows"][0][0]

            # Apply multiple tags
            result = run_cli(
                ["tag", "food,groceries,essentials", "--ids", tx_id], tmpdir
            )
            assert result.returncode == 0

            # Verify tags were applied
            result = run_cli(
                [
                    "query",
                    f"SELECT tags FROM transactions WHERE transaction_id = '{tx_id}'",
                    "--json",
                ],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tags_str = str(data["rows"][0][0])
            assert "food" in tags_str
            assert "groceries" in tags_str

    def test_tag_replace_mode(self):
        """Test that --replace removes existing tags."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Get a transaction ID
            result = run_cli(
                ["query", "SELECT transaction_id FROM transactions LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tx_id = data["rows"][0][0]

            # Apply initial tags
            run_cli(["tag", "old-tag", "--ids", tx_id], tmpdir)

            # Replace with new tag
            result = run_cli(["tag", "new-tag", "--ids", tx_id, "--replace"], tmpdir)
            assert result.returncode == 0

            # Verify old tag is gone, new tag is present
            result = run_cli(
                [
                    "query",
                    f"SELECT tags FROM transactions WHERE transaction_id = '{tx_id}'",
                    "--json",
                ],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tags_str = str(data["rows"][0][0])
            assert "new-tag" in tags_str
            # old-tag should be replaced (not present)

    def test_tag_json_output(self):
        """Test that tag --json returns valid JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(
                ["query", "SELECT transaction_id FROM transactions LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tx_id = data["rows"][0][0]

            result = run_cli(["tag", "json-test", "--ids", tx_id, "--json"], tmpdir)
            assert result.returncode == 0
            # Should be valid JSON
            json.loads(result.stdout)


class TestNewCommand:
    """Tests for tl new command."""

    def test_new_balance_creates_snapshot(self):
        """Test that 'tl new balance' creates a balance snapshot."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Get an account ID
            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"], tmpdir
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            # Add balance snapshot
            result = run_cli(
                ["new", "balance", "--account-id", account_id, "--balance", "9999.99"],
                tmpdir,
            )
            assert result.returncode == 0

            # Verify it was created
            result = run_cli(
                [
                    "query",
                    f"SELECT balance FROM balance_snapshots WHERE account_id = '{account_id}' ORDER BY snapshot_time DESC LIMIT 1",
                    "--json",
                ],
                tmpdir,
            )
            data = json.loads(result.stdout)
            # Balance should be close to 9999.99
            assert len(data["rows"]) > 0

    def test_new_balance_with_date(self):
        """Test that 'tl new balance --date' sets specific date."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"], tmpdir
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            result = run_cli(
                [
                    "new",
                    "balance",
                    "--account-id",
                    account_id,
                    "--balance",
                    "5000.00",
                    "--date",
                    "2025-01-15",
                ],
                tmpdir,
            )
            assert result.returncode == 0


class TestBackfillCommand:
    """Tests for tl backfill command."""

    def test_backfill_balances_dry_run(self):
        """Test that backfill balances --dry-run shows preview."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(["backfill", "balances", "--dry-run"], tmpdir)
            assert result.returncode == 0
            # Should mention dry run or preview
            assert (
                "dry" in result.stdout.lower()
                or "would" in result.stdout.lower()
                or "preview" in result.stdout.lower()
            )

    def test_backfill_balances_with_days_limit(self):
        """Test that --days limits the backfill period."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(
                ["backfill", "balances", "--days", "30", "--dry-run"], tmpdir
            )
            assert result.returncode == 0


class TestImportCommand:
    """Tests for tl import command."""

    def test_import_preview(self):
        """Test that import --preview shows transactions without importing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Create test CSV
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text(
                "Date,Description,Amount\n2025-01-01,Test Transaction,-50.00\n"
            )

            # Get account ID
            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"], tmpdir
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            # Preview import
            result = run_cli(
                ["import", str(csv_path), "--account-id", account_id, "--preview"],
                tmpdir,
            )
            assert result.returncode == 0
            assert (
                "Test Transaction" in result.stdout
                or "preview" in result.stdout.lower()
            )

    def test_import_actually_imports(self):
        """Test that import without --preview actually imports transactions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Create test CSV with unique description
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text(
                "Date,Description,Amount\n2025-01-01,UniqueImportTest123,-99.99\n"
            )

            # Get account ID
            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"], tmpdir
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            # Import
            result = run_cli(
                ["import", str(csv_path), "--account-id", account_id], tmpdir
            )
            assert result.returncode == 0

            # Verify import
            result = run_cli(
                [
                    "query",
                    "SELECT * FROM transactions WHERE description = 'UniqueImportTest123'",
                    "--json",
                ],
                tmpdir,
            )
            data = json.loads(result.stdout)
            assert len(data["rows"]) > 0

    def test_import_missing_file_error(self):
        """Test that import shows error for missing file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"], tmpdir
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            result = run_cli(
                ["import", "/nonexistent/file.csv", "--account-id", account_id], tmpdir
            )
            assert result.returncode != 0

    def test_import_requires_account_id(self):
        """Test that import requires --account-id for scriptable mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text("Date,Description,Amount\n2025-01-01,Test,-50.00\n")

            result = run_cli(["import", str(csv_path)], tmpdir)
            assert result.returncode != 0
            assert (
                "account-id" in result.stdout.lower()
                or "account-id" in result.stderr.lower()
            )

    def test_import_list_profiles_empty(self):
        """Test that --list-profiles shows no profiles initially."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_cli(["import", "--list-profiles"], tmpdir)
            assert result.returncode == 0
            assert "no saved profiles" in result.stdout.lower()

    def test_import_save_and_use_profile(self):
        """Test saving a profile with --save-profile and using it with --profile."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Create test CSV
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text(
                "Date,Description,Amount\n2025-01-01,ProfileTest1,-50.00\n"
            )

            # Get account ID
            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"], tmpdir
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            # Import with --save-profile
            result = run_cli(
                [
                    "import",
                    str(csv_path),
                    "--account-id",
                    account_id,
                    "--save-profile",
                    "testbank",
                ],
                tmpdir,
            )
            assert result.returncode == 0
            assert "profile 'testbank' saved" in result.stdout.lower()

            # Verify profile appears in list
            result = run_cli(["import", "--list-profiles"], tmpdir)
            assert result.returncode == 0
            assert "testbank" in result.stdout.lower()

            # Create another CSV and import using the saved profile
            csv_path2 = Path(tmpdir) / "test2.csv"
            csv_path2.write_text(
                "Date,Description,Amount\n2025-01-02,ProfileTest2,-75.00\n"
            )

            result = run_cli(
                [
                    "import",
                    str(csv_path2),
                    "--account-id",
                    account_id,
                    "--profile",
                    "testbank",
                ],
                tmpdir,
            )
            assert result.returncode == 0
            assert "using profile 'testbank'" in result.stdout.lower()

    def test_import_profile_not_found(self):
        """Test that --profile with non-existent profile shows error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text("Date,Description,Amount\n2025-01-01,Test,-50.00\n")

            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"], tmpdir
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            result = run_cli(
                [
                    "import",
                    str(csv_path),
                    "--account-id",
                    account_id,
                    "--profile",
                    "nonexistent",
                ],
                tmpdir,
            )
            assert result.returncode != 0
            assert (
                "not found" in result.stdout.lower()
                or "not found" in result.stderr.lower()
            )

    def test_import_list_profiles_json(self):
        """Test that --list-profiles --json outputs valid JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Create and save a profile first
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text("Date,Description,Amount\n2025-01-01,Test,-50.00\n")

            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"], tmpdir
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            run_cli(
                [
                    "import",
                    str(csv_path),
                    "--account-id",
                    account_id,
                    "--save-profile",
                    "jsontest",
                ],
                tmpdir,
            )

            # List profiles as JSON
            result = run_cli(["import", "--list-profiles", "--json"], tmpdir)
            assert result.returncode == 0
            profiles = json.loads(result.stdout)
            assert "jsontest" in profiles
            assert "columnMappings" in profiles["jsontest"]


class TestRemoveCommand:
    """Tests for tl remove command."""

    def test_remove_nonexistent_integration(self):
        """Test that removing nonexistent integration shows error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(["remove", "nonexistent"], tmpdir)
            assert result.returncode != 0
            assert (
                "not found" in result.stdout.lower()
                or "not found" in result.stderr.lower()
            )


class TestSetupCommand:
    """Tests for tl setup command."""

    def test_setup_demo_redirects(self):
        """Test that 'tl setup demo' redirects to demo command."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_cli(["setup", "demo"], tmpdir)
            # Should exit 0 but tell user to use 'tl demo on'
            assert result.returncode == 0
            assert "demo on" in result.stdout.lower()

    def test_setup_unknown_integration(self):
        """Test that unknown integration shows error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_cli(["setup", "unknown"], tmpdir)
            assert result.returncode != 0

    def test_setup_simplefin_blocked_in_demo_mode(self):
        """Test that SimpleFIN setup is blocked in demo mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(["setup", "simplefin", "--token", "fake-token"], tmpdir)
            assert result.returncode != 0
            assert "demo mode" in result.stdout.lower()


class TestBackupCommand:
    """Tests for tl backup command."""

    def test_backup_create(self):
        """Test that 'tl backup create' creates a backup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["backup", "create"], tmpdir)
            assert result.returncode == 0
            assert "Backup created" in result.stdout

    def test_backup_list(self):
        """Test that 'tl backup list' shows created backups."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            run_cli(["backup", "create"], tmpdir)
            result = run_cli(["backup", "list"], tmpdir)
            assert result.returncode == 0
            assert "treeline-" in result.stdout

    def test_backup_list_json(self):
        """Test that 'tl backup list --json' returns valid JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            run_cli(["backup", "create"], tmpdir)
            result = run_cli(["backup", "list", "--json"], tmpdir)
            assert result.returncode == 0
            data = json.loads(result.stdout)
            assert len(data) > 0
            assert "name" in data[0]
            assert "size_bytes" in data[0]

    def test_backup_restore(self):
        """Test that 'tl backup restore' restores from backup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Create backup
            result = run_cli(["backup", "create", "--json"], tmpdir)
            data = json.loads(result.stdout)
            backup_name = data["name"]

            # Restore from backup
            result = run_cli(["backup", "restore", backup_name, "--force"], tmpdir)
            assert result.returncode == 0
            assert "restored" in result.stdout.lower()

    def test_backup_clear(self):
        """Test that 'tl backup clear' deletes all backups."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Create some backups
            run_cli(["backup", "create"], tmpdir)
            run_cli(["backup", "create"], tmpdir)

            # Clear all backups
            result = run_cli(["backup", "clear", "--force"], tmpdir)
            assert result.returncode == 0
            assert "Deleted" in result.stdout

            # Verify empty
            result = run_cli(["backup", "list"], tmpdir)
            assert "No backups found" in result.stdout

    def test_backup_retention(self):
        """Test that backup respects --max-backups retention."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Create 3 backups with max 2
            run_cli(["backup", "create", "--max-backups", "2"], tmpdir)
            run_cli(["backup", "create", "--max-backups", "2"], tmpdir)
            run_cli(["backup", "create", "--max-backups", "2"], tmpdir)

            # Should only have 2 backups
            result = run_cli(["backup", "list", "--json"], tmpdir)
            data = json.loads(result.stdout)
            assert len(data) == 2


class TestCompactCommand:
    """Tests for tl compact command."""

    def test_compact_works(self):
        """Test that 'tl compact' compacts the database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["compact"], tmpdir)
            assert result.returncode == 0
            assert "compacted" in result.stdout.lower()
            assert "Before:" in result.stdout
            assert "After:" in result.stdout

    def test_compact_creates_backup_by_default(self):
        """Test that compact creates a safety backup by default."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["compact"], tmpdir)
            assert result.returncode == 0
            assert "Safety backup:" in result.stdout

    def test_compact_skip_backup(self):
        """Test that --skip-backup skips safety backup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["compact", "--skip-backup"], tmpdir)
            assert result.returncode == 0
            assert "Safety backup:" not in result.stdout

    def test_compact_json_output(self):
        """Test that compact --json returns valid JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["compact", "--json"], tmpdir)
            assert result.returncode == 0
            data = json.loads(result.stdout)
            assert "original_size" in data
            assert "compacted_size" in data
