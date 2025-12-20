"""Smoke tests for database encryption commands.

These tests verify that encryption CLI commands work end-to-end.
No treeline imports - only CLI interaction via subprocess.
"""

import json
import os
import subprocess
import tempfile
from pathlib import Path


def run_cli(
    args: list[str],
    treeline_dir: str,
    input_text: str | None = None,
    env_extras: dict | None = None,
) -> subprocess.CompletedProcess:
    """Run treeline CLI command with specified treeline directory.

    Args:
        args: CLI arguments (e.g., ["encrypt", "status"])
        treeline_dir: Path to treeline data directory
        input_text: Optional text to pipe to stdin
        env_extras: Additional environment variables to set

    Returns:
        CompletedProcess with stdout, stderr, returncode
    """
    env = os.environ.copy()
    env["TREELINE_DIR"] = str(Path(treeline_dir) / ".treeline")
    # Remove any existing demo mode env var
    env.pop("TREELINE_DEMO_MODE", None)
    # Remove any existing password env var
    env.pop("TL_DB_PASSWORD", None)

    if env_extras:
        env.update(env_extras)

    cmd = ["uv", "run", "treeline"] + args
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        input=input_text,
    )


class TestEncryptStatusCommand:
    """Tests for tl encrypt status command."""

    def test_encrypt_status_shows_unencrypted_by_default(self):
        """Test that status shows unencrypted for new database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize database (any command will do)
            run_cli(["status"], tmpdir)

            result = run_cli(["encrypt", "status"], tmpdir)
            assert result.returncode == 0
            assert "not encrypted" in result.stdout.lower()

    def test_encrypt_status_json_output(self):
        """Test that encrypt status --json returns valid JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["status"], tmpdir)

            result = run_cli(["encrypt", "status", "--json"], tmpdir)
            assert result.returncode == 0

            data = json.loads(result.stdout)
            assert "encrypted" in data
            assert data["encrypted"] is False


class TestEncryptDecryptCycle:
    """Tests for full encrypt/decrypt cycle."""

    def test_encrypt_decrypt_cycle(self):
        """Test full encrypt and decrypt cycle with password via env var."""
        with tempfile.TemporaryDirectory() as tmpdir:
            password = "test_password_123"

            # Initialize database with some data
            run_cli(["status"], tmpdir)

            # Enable encryption
            result = run_cli(
                ["encrypt", "-p", password],
                tmpdir,
            )
            assert result.returncode == 0, f"encrypt failed: {result.stderr}\n{result.stdout}"
            assert "encrypted successfully" in result.stdout.lower()

            # Verify status shows encrypted
            result = run_cli(
                ["encrypt", "status", "--json"],
                tmpdir,
                env_extras={"TL_DB_PASSWORD": password},
            )
            assert result.returncode == 0, f"status failed: {result.stderr}\n{result.stdout}"
            data = json.loads(result.stdout)
            assert data["encrypted"] is True
            assert data["algorithm"] == "argon2id"

            # Verify commands work with encrypted DB
            result = run_cli(
                ["status", "--json"],
                tmpdir,
                env_extras={"TL_DB_PASSWORD": password},
            )
            assert result.returncode == 0, f"status --json failed: {result.stderr}\n{result.stdout}"

            # Disable encryption
            result = run_cli(
                ["decrypt", "-p", password],
                tmpdir,
                env_extras={"TL_DB_PASSWORD": password},
            )
            assert result.returncode == 0, f"decrypt failed: {result.stderr}\n{result.stdout}"
            assert "decrypted successfully" in result.stdout.lower()

            # Verify status shows unencrypted
            result = run_cli(["encrypt", "status", "--json"], tmpdir)
            data = json.loads(result.stdout)
            assert data["encrypted"] is False

    def test_encrypt_with_env_password(self):
        """Test encryption using TL_DB_PASSWORD environment variable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            password = "env_password_456"

            # Initialize database
            run_cli(["status"], tmpdir)

            # Enable encryption via env var
            result = run_cli(
                ["encrypt"],
                tmpdir,
                env_extras={"TL_DB_PASSWORD": password},
            )
            assert result.returncode == 0
            assert "encrypted successfully" in result.stdout.lower()


class TestEncryptErrors:
    """Tests for encryption error cases."""

    def test_wrong_password_fails(self):
        """Test that wrong password fails on encrypted database access."""
        with tempfile.TemporaryDirectory() as tmpdir:
            correct_password = "correct_password"
            wrong_password = "wrong_password"

            # Initialize and encrypt
            run_cli(["status"], tmpdir)
            run_cli(
                ["encrypt", "-p", correct_password],
                tmpdir,
            )

            # Try to access with wrong password
            result = run_cli(
                ["status"],
                tmpdir,
                env_extras={"TL_DB_PASSWORD": wrong_password},
            )
            # Should fail because container initialization fails with wrong password
            assert result.returncode != 0

    def test_encrypt_already_encrypted_error(self):
        """Test that enabling encryption when already encrypted fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            password = "test_password"

            run_cli(["status"], tmpdir)

            # First encryption
            run_cli(
                ["encrypt", "-p", password],
                tmpdir,
            )

            # Try to encrypt again
            result = run_cli(
                ["encrypt", "-p", password],
                tmpdir,
                env_extras={"TL_DB_PASSWORD": password},
            )
            assert result.returncode != 0
            assert "already encrypted" in result.stdout.lower()

    def test_decrypt_not_encrypted_error(self):
        """Test that decrypting unencrypted database fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["status"], tmpdir)

            result = run_cli(
                ["decrypt", "-p", "password"],
                tmpdir,
            )
            assert result.returncode != 0
            assert "not encrypted" in result.stdout.lower()

    def test_decrypt_wrong_password_fails(self):
        """Test that wrong password fails on decrypt."""
        with tempfile.TemporaryDirectory() as tmpdir:
            correct_password = "correct_password"
            wrong_password = "wrong_password"

            # Initialize and encrypt
            run_cli(["status"], tmpdir)
            run_cli(
                ["encrypt", "-p", correct_password],
                tmpdir,
            )

            # Try to decrypt with wrong password
            result = run_cli(
                ["decrypt", "-p", wrong_password],
                tmpdir,
                env_extras={"TL_DB_PASSWORD": correct_password},  # Container needs correct password
            )
            assert result.returncode != 0
            assert "invalid password" in result.stdout.lower()


class TestEncryptDemoMode:
    """Tests for encryption in demo mode."""

    def test_encrypt_blocked_in_demo_mode(self):
        """Test that encryption is blocked in demo mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Enable demo mode
            run_cli(["demo", "on"], tmpdir)

            # Try to encrypt
            result = run_cli(
                ["encrypt", "-p", "password"],
                tmpdir,
            )
            assert result.returncode != 0
            assert "demo" in result.stdout.lower()

    def test_decrypt_blocked_in_demo_mode(self):
        """Test that decrypt is blocked in demo mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(
                ["decrypt", "-p", "password"],
                tmpdir,
            )
            assert result.returncode != 0
            assert "demo" in result.stdout.lower()


class TestEncryptBackup:
    """Tests for backup during encryption."""

    def test_encrypt_creates_backup(self):
        """Test that encryption creates a safety backup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            password = "test_password"

            run_cli(["status"], tmpdir)

            # Enable encryption
            result = run_cli(
                ["encrypt", "-p", password, "--json"],
                tmpdir,
            )
            assert result.returncode == 0
            data = json.loads(result.stdout)
            assert "backup_name" in data
            assert data["backup_name"] is not None

            # Verify backup exists
            result = run_cli(
                ["backup", "list", "--json"],
                tmpdir,
                env_extras={"TL_DB_PASSWORD": password},
            )
            backups = json.loads(result.stdout)
            assert len(backups) > 0

    def test_decrypt_creates_backup(self):
        """Test that decryption creates a safety backup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            password = "test_password"

            run_cli(["status"], tmpdir)

            # Encrypt
            run_cli(
                ["encrypt", "-p", password],
                tmpdir,
            )

            # Clear any existing backups
            run_cli(
                ["backup", "clear", "--force"],
                tmpdir,
                env_extras={"TL_DB_PASSWORD": password},
            )

            # Decrypt (should create backup)
            result = run_cli(
                ["decrypt", "-p", password, "--json"],
                tmpdir,
                env_extras={"TL_DB_PASSWORD": password},
            )
            assert result.returncode == 0
            data = json.loads(result.stdout)
            assert "backup_name" in data
            assert data["backup_name"] is not None


class TestEncryptedDatabaseOperations:
    """Tests for normal operations on encrypted database."""

    def test_query_works_on_encrypted_db(self):
        """Test that SQL queries work on encrypted database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            password = "test_password"

            # Initialize and add some data
            run_cli(["status"], tmpdir)

            # Encrypt
            run_cli(
                ["encrypt", "-p", password],
                tmpdir,
            )

            # Query should work
            result = run_cli(
                ["query", "SELECT COUNT(*) as cnt FROM accounts", "--json"],
                tmpdir,
                env_extras={"TL_DB_PASSWORD": password},
            )
            assert result.returncode == 0, f"query failed: {result.stderr}\n{result.stdout}"
            data = json.loads(result.stdout)
            assert "columns" in data

    def test_compact_works_on_encrypted_db(self):
        """Test that compact works on encrypted database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            password = "test_password"

            run_cli(["status"], tmpdir)
            run_cli(
                ["encrypt", "-p", password],
                tmpdir,
            )

            # Compact should work
            result = run_cli(
                ["compact", "--skip-backup", "--json"],
                tmpdir,
                env_extras={"TL_DB_PASSWORD": password},
            )
            assert result.returncode == 0, f"compact failed: {result.stderr}\n{result.stdout}"
            data = json.loads(result.stdout)
            assert "original_size" in data
            assert "compacted_size" in data
