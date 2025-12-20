"""Smoke tests for tl doctor command.

Tests the database health check functionality via CLI.
"""

import json
import os
import subprocess
import tempfile
from pathlib import Path


def run_cli(args: list[str], treeline_dir: str, input_text: str | None = None) -> subprocess.CompletedProcess:
    """Run treeline CLI command with specified treeline directory.

    Args:
        args: CLI arguments (e.g., ["doctor", "--json"])
        treeline_dir: Path to treeline data directory
        input_text: Optional text to pipe to stdin

    Returns:
        CompletedProcess with stdout, stderr, returncode
    """
    env = os.environ.copy()
    env["TREELINE_DIR"] = str(Path(treeline_dir) / ".treeline")
    env.pop("TREELINE_DEMO_MODE", None)

    cmd = ["uv", "run", "treeline"] + args
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        input=input_text,
    )


class TestDoctorCommand:
    """Tests for tl doctor command."""

    def test_doctor_runs_successfully(self):
        """Test that 'tl doctor' runs and completes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["doctor"], tmpdir)
            assert result.returncode == 0, f"doctor failed: {result.stderr}"
            assert "Health Check" in result.stdout

    def test_doctor_shows_all_checks(self):
        """Test that doctor shows all expected checks."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["doctor"], tmpdir)
            assert result.returncode == 0

            # Should show check names (title-cased)
            assert "Orphaned Transactions" in result.stdout
            assert "Orphaned Snapshots" in result.stdout
            assert "Duplicate Fingerprints" in result.stdout
            assert "Date Sanity" in result.stdout
            assert "Untagged Transactions" in result.stdout
            assert "Uncategorized Expenses" in result.stdout
            assert "Integration Connectivity" in result.stdout

    def test_doctor_shows_summary(self):
        """Test that doctor shows summary line."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["doctor"], tmpdir)
            assert result.returncode == 0
            assert "Summary:" in result.stdout
            assert "passed" in result.stdout

    def test_doctor_json_output(self):
        """Test that doctor --json returns valid JSON with expected structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["doctor", "--json"], tmpdir)
            assert result.returncode == 0

            data = json.loads(result.stdout)

            # Should have summary
            assert "summary" in data
            assert "passed" in data["summary"]
            assert "warnings" in data["summary"]
            assert "errors" in data["summary"]

            # Should have checks
            assert "checks" in data
            assert "orphaned_transactions" in data["checks"]
            assert "orphaned_snapshots" in data["checks"]
            assert "duplicate_fingerprints" in data["checks"]
            assert "date_sanity" in data["checks"]
            assert "untagged_transactions" in data["checks"]
            assert "uncategorized_expenses" in data["checks"]
            assert "integration_connectivity" in data["checks"]

            # Each check should have status and message
            for check_name, check_data in data["checks"].items():
                assert "status" in check_data, f"Missing status in {check_name}"
                assert "message" in check_data, f"Missing message in {check_name}"
                assert check_data["status"] in ("pass", "warning", "error")

    def test_doctor_verbose_mode(self):
        """Test that --verbose shows more detail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["doctor", "--verbose"], tmpdir)
            assert result.returncode == 0
            # Verbose output should not show the "Run with --verbose" hint
            assert "Run with --verbose" not in result.stdout

    def test_doctor_on_empty_database(self):
        """Test doctor works on an empty database (no demo data)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Just initialize without demo data
            run_cli(["status"], tmpdir)  # This will initialize the db
            result = run_cli(["doctor"], tmpdir)
            assert result.returncode == 0
            assert "Health Check" in result.stdout

    def test_doctor_json_check_status_values(self):
        """Test that all checks have valid status values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["doctor", "--json"], tmpdir)
            assert result.returncode == 0

            data = json.loads(result.stdout)

            for check_name, check_data in data["checks"].items():
                status = check_data["status"]
                assert status in ("pass", "warning", "error"), \
                    f"Invalid status '{status}' for check {check_name}"

    def test_doctor_exit_code_on_healthy_db(self):
        """Test that doctor returns 0 when database is healthy."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)
            result = run_cli(["doctor"], tmpdir)
            # Demo data should be healthy (no orphans, no crazy dates)
            # Exit code 0 means no errors (warnings are OK)
            assert result.returncode == 0
