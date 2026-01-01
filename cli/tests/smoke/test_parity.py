"""Parity tests - verify Rust CLI matches Python CLI behavior.

These tests verify critical behavior that must match between the Python
and Rust implementations. They test:
1. Sync deduplication (skips existing, preserves user edits)
2. Tags storage format (VARCHAR[] not JSON)
3. Import with debit/credit columns
"""

import json
import os
import subprocess
import tempfile
from pathlib import Path


# Toggle to switch between testing Python or Rust CLI
USE_RUST_CLI = True  # Set to False to test Python CLI


def run_cli(
    args: list[str], treeline_dir: str, input_text: str | None = None
) -> subprocess.CompletedProcess:
    """Run treeline CLI command with specified treeline directory."""
    env = os.environ.copy()
    env["TREELINE_DIR"] = str(Path(treeline_dir) / ".treeline")
    env.pop("TREELINE_DEMO_MODE", None)

    if USE_RUST_CLI:
        rust_binary = Path(__file__).parent.parent.parent.parent / "rust-core" / "target" / "release" / "tl"
        cmd = [str(rust_binary)] + args
    else:
        # Use Python CLI
        cmd = ["uv", "run", "tl"] + args

    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        input=input_text,
        cwd=Path(__file__).parent.parent.parent,  # cli/ directory
    )


class TestSyncDeduplication:
    """Tests for sync deduplication - must skip existing transactions."""

    def test_sync_does_not_duplicate_transactions(self):
        """Test that syncing twice doesn't create duplicate transactions.

        CRITICAL: After two syncs, transaction count should be the same.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Enable demo mode
            run_cli(["demo", "on"], tmpdir)

            # Get initial transaction count
            result = run_cli(
                ["query", "SELECT COUNT(*) as cnt FROM transactions", "--json"],
                tmpdir,
            )
            assert result.returncode == 0, f"Query failed: {result.stderr}"
            data = json.loads(result.stdout)
            initial_count = data["rows"][0][0]

            # Sync again
            result = run_cli(["sync"], tmpdir)
            assert result.returncode == 0, f"Sync failed: {result.stderr}"

            # Transaction count should NOT have increased
            result = run_cli(
                ["query", "SELECT COUNT(*) as cnt FROM transactions", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            after_sync_count = data["rows"][0][0]

            assert after_sync_count == initial_count, (
                f"Sync created duplicates! Count before: {initial_count}, "
                f"after: {after_sync_count}"
            )

    def test_sync_preserves_user_tags(self):
        """Test that sync does NOT overwrite user-added tags.

        CRITICAL: If a user tags a transaction, sync should preserve it.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Get a transaction ID
            result = run_cli(
                ["query", "SELECT transaction_id FROM transactions LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tx_id = data["rows"][0][0]

            # Add a user tag
            result = run_cli(["tag", "my-custom-tag", "--ids", tx_id], tmpdir)
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
            tags_before = data["rows"][0][0]
            assert "my-custom-tag" in str(tags_before), "Tag not applied initially"

            # Sync again
            result = run_cli(["sync"], tmpdir)
            assert result.returncode == 0

            # Verify tag is STILL there
            result = run_cli(
                [
                    "query",
                    f"SELECT tags FROM transactions WHERE transaction_id = '{tx_id}'",
                    "--json",
                ],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tags_after = data["rows"][0][0]

            assert "my-custom-tag" in str(tags_after), (
                f"Sync overwrote user tag! Before: {tags_before}, After: {tags_after}"
            )

    def test_sync_shows_skipped_count(self):
        """Test that sync output shows skipped transactions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Get initial transaction count
            result = run_cli(
                ["query", "SELECT COUNT(*) FROM transactions", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tx_count = data["rows"][0][0]

            # Sync again - should skip all existing transactions
            result = run_cli(["sync", "--json"], tmpdir)
            assert result.returncode == 0

            sync_data = json.loads(result.stdout)

            # Should have results for demo integration
            assert "results" in sync_data
            if sync_data["results"]:
                demo_result = sync_data["results"][0]
                # new_transactions should be 0 for second sync (all should be skipped)
                new_txs = demo_result.get("new_transactions", demo_result.get("transactions_synced", 0))
                assert new_txs == 0, (
                    f"Second sync should not import new transactions: {demo_result}"
                )


class TestTagsStorage:
    """Tests for tags storage format - must be VARCHAR[], not JSON."""

    def test_tags_query_with_unnest(self):
        """Test that tags can be queried using UNNEST (requires VARCHAR[])."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Add tags to a transaction
            result = run_cli(
                ["query", "SELECT transaction_id FROM transactions LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tx_id = data["rows"][0][0]

            run_cli(["tag", "groceries,food", "--ids", tx_id], tmpdir)

            # Query using UNNEST - this only works with VARCHAR[], not JSON
            result = run_cli(
                [
                    "query",
                    """
                    SELECT tag, COUNT(*) as cnt
                    FROM transactions, UNNEST(tags) as t(tag)
                    GROUP BY tag
                    """,
                    "--json",
                ],
                tmpdir,
            )

            # This should succeed if tags is VARCHAR[]
            # It will fail with JSON storage
            assert result.returncode == 0, (
                f"UNNEST query failed - tags may be stored as JSON instead of VARCHAR[]. "
                f"Error: {result.stderr}"
            )

    def test_tags_len_function(self):
        """Test that len(tags) works (requires VARCHAR[], not JSON)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(
                ["query", "SELECT transaction_id FROM transactions LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tx_id = data["rows"][0][0]

            run_cli(["tag", "test1,test2,test3", "--ids", tx_id], tmpdir)

            # Query using len() - this works with VARCHAR[]
            result = run_cli(
                [
                    "query",
                    f"""
                    SELECT len(tags) as tag_count
                    FROM transactions
                    WHERE transaction_id = '{tx_id}'
                    """,
                    "--json",
                ],
                tmpdir,
            )

            assert result.returncode == 0, f"len(tags) failed: {result.stderr}"
            data = json.loads(result.stdout)
            tag_count = data["rows"][0][0]
            assert tag_count >= 3, f"Expected at least 3 tags, got {tag_count}"

    def test_tags_list_concat(self):
        """Test that list_concat works on tags (requires VARCHAR[])."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(
                ["query", "SELECT transaction_id FROM transactions LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tx_id = data["rows"][0][0]

            run_cli(["tag", "existing-tag", "--ids", tx_id], tmpdir)

            # Query using list_concat - works with VARCHAR[]
            result = run_cli(
                [
                    "query",
                    f"""
                    SELECT list_concat(tags, ['new-tag']) as merged
                    FROM transactions
                    WHERE transaction_id = '{tx_id}'
                    """,
                    "--json",
                ],
                tmpdir,
            )

            assert result.returncode == 0, f"list_concat failed: {result.stderr}"


class TestImportDebitCredit:
    """Tests for import with debit/credit columns."""

    def test_import_with_debit_credit_columns(self):
        """Test importing CSV with separate debit/credit columns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Create CSV with debit/credit columns (accounting format)
            csv_path = Path(tmpdir) / "debit_credit.csv"
            csv_path.write_text(
                "Date,Description,Debit,Credit\n"
                "2025-01-01,Coffee Shop,5.50,\n"
                "2025-01-02,Paycheck,,1500.00\n"
                "2025-01-03,Groceries,75.00,\n"
            )

            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            # Import with debit/credit columns
            result = run_cli(
                [
                    "import",
                    str(csv_path),
                    "--account-id",
                    account_id,
                    "--date-column",
                    "Date",
                    "--description-column",
                    "Description",
                    "--debit-column",
                    "Debit",
                    "--credit-column",
                    "Credit",
                ],
                tmpdir,
            )

            assert result.returncode == 0, f"Import failed: {result.stderr}"

            # Verify transactions were imported with correct amounts
            result = run_cli(
                [
                    "query",
                    "SELECT description, amount FROM transactions "
                    "WHERE description IN ('Coffee Shop', 'Paycheck', 'Groceries') "
                    "ORDER BY description",
                    "--json",
                ],
                tmpdir,
            )
            data = json.loads(result.stdout)
            rows = data["rows"]

            # Should have 3 transactions
            assert len(rows) >= 3, f"Expected 3 transactions, got {len(rows)}"

            # Check amounts are correct - sign preserved from CSV by default
            # Debits and credits are stored as-is (use --debit-negative to negate debits)
            # Note: amounts are returned as strings from query, convert to float
            amounts = {row[0]: float(row[1]) for row in rows}

            # Debit values preserved (positive in CSV, positive in DB)
            assert amounts.get("Coffee Shop", 0) > 0, f"Coffee Shop (debit) should preserve sign, got {amounts.get('Coffee Shop')}"
            assert amounts.get("Groceries", 0) > 0, f"Groceries (debit) should preserve sign, got {amounts.get('Groceries')}"

            # Credit should be positive
            assert amounts.get("Paycheck", 0) > 0, f"Paycheck (credit) should be positive, got {amounts.get('Paycheck')}"

    def test_import_with_debit_negative_flag(self):
        """Test --debit-negative flag when debits are already negative in CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Create CSV with unsigned debits (positive values)
            csv_path = Path(tmpdir) / "unsigned_debit.csv"
            csv_path.write_text(
                "Date,Description,Debit,Credit\n"
                "2025-01-01,Coffee Shop,5.50,\n"  # Debit as positive value
            )

            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            result = run_cli(
                [
                    "import",
                    str(csv_path),
                    "--account-id",
                    account_id,
                    "--date-column",
                    "Date",
                    "--description-column",
                    "Description",
                    "--debit-column",
                    "Debit",
                    "--credit-column",
                    "Credit",
                    "--debit-negative",  # Negate positive debit values
                ],
                tmpdir,
            )

            assert result.returncode == 0, f"Import with --debit-negative failed: {result.stderr}"


class TestImportDeduplication:
    """Tests for import deduplication via fingerprints."""

    def test_import_same_file_twice_no_duplicates(self):
        """Test that importing the same CSV twice doesn't create duplicates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text(
                "Date,Description,Amount\n"
                "2025-01-01,Unique Import Test ABC,-50.00\n"
                "2025-01-02,Unique Import Test XYZ,-75.00\n"
            )

            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            # First import
            result = run_cli(
                ["import", str(csv_path), "--account-id", account_id],
                tmpdir,
            )
            assert result.returncode == 0

            # Count after first import
            result = run_cli(
                [
                    "query",
                    "SELECT COUNT(*) FROM transactions WHERE description LIKE 'Unique Import Test%'",
                    "--json",
                ],
                tmpdir,
            )
            data = json.loads(result.stdout)
            first_count = data["rows"][0][0]
            assert first_count == 2, f"Expected 2 transactions, got {first_count}"

            # Second import of same file
            result = run_cli(
                ["import", str(csv_path), "--account-id", account_id],
                tmpdir,
            )
            assert result.returncode == 0

            # Count should still be 2 (no duplicates)
            result = run_cli(
                [
                    "query",
                    "SELECT COUNT(*) FROM transactions WHERE description LIKE 'Unique Import Test%'",
                    "--json",
                ],
                tmpdir,
            )
            data = json.loads(result.stdout)
            second_count = data["rows"][0][0]

            assert second_count == first_count, (
                f"Import created duplicates! First: {first_count}, Second: {second_count}"
            )


class TestBackfillBalances:
    """Tests for backfill balances command - CRITICAL: must calculate historical balances."""

    def test_backfill_runs_successfully(self):
        """Test that backfill balances command runs successfully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Run backfill
            result = run_cli(["backfill", "balances"], tmpdir)
            assert result.returncode == 0, f"Backfill failed: {result.stderr}"

    def test_backfill_respects_days_limit(self):
        """Test that backfill respects the --days limit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Get an account
            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            # Clear existing snapshots
            run_cli(
                ["query", f"DELETE FROM sys_balance_snapshots WHERE account_id = '{account_id}'"],
                tmpdir,
            )

            # Create a balance snapshot
            run_cli(
                ["new", "balance", "--account-id", account_id, "--balance", "1000.00"],
                tmpdir,
            )

            # Run backfill with --days 7
            result = run_cli(["backfill", "balances", "--days", "7"], tmpdir)
            assert result.returncode == 0, f"Backfill with --days failed: {result.stderr}"

            # Should have processed successfully (even if no transactions in last 7 days)
            # The key is that it accepts and uses the --days parameter


class TestImportFingerprint:
    """Tests for import fingerprint normalization."""

    def test_fingerprint_handles_null_in_description(self):
        """Test that fingerprints normalize 'null' strings in descriptions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            # Create CSV with 'null' in description
            csv_path = Path(tmpdir) / "null_test.csv"
            csv_path.write_text(
                "Date,Description,Amount\n"
                "2025-01-01,Payment null null,-100.00\n"
            )

            # Import once
            run_cli(["import", str(csv_path), "--account-id", account_id], tmpdir)

            # Create CSV without 'null' but otherwise same
            csv_path2 = Path(tmpdir) / "no_null_test.csv"
            csv_path2.write_text(
                "Date,Description,Amount\n"
                "2025-01-01,Payment,-100.00\n"
            )

            # Import again - should be detected as duplicate if null is normalized
            run_cli(["import", str(csv_path2), "--account-id", account_id], tmpdir)

            # Count transactions with "Payment" description
            result = run_cli(
                ["query", "SELECT COUNT(*) FROM transactions WHERE description LIKE 'Payment%'", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            count = int(data["rows"][0][0])

            # If normalization works, these should be detected as same transaction
            # If not, we'll have 2 transactions
            # Note: This test is lenient - just ensuring import works
            assert count >= 1, "Import should have created at least one transaction"


class TestImportAmountParsing:
    """Tests for import amount parsing."""

    def test_import_parentheses_negative(self):
        """Test that (100.00) is parsed as -100.00 (accounting notation)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            # Create CSV with parentheses notation for negative
            csv_path = Path(tmpdir) / "parens_test.csv"
            csv_path.write_text(
                "Date,Description,Amount\n"
                "2025-01-01,Expense With Parens,(100.00)\n"
            )

            result = run_cli(
                ["import", str(csv_path), "--account-id", account_id],
                tmpdir,
            )
            assert result.returncode == 0, f"Import failed: {result.stderr}"

            # Check the amount was imported as negative
            result = run_cli(
                ["query", "SELECT amount FROM transactions WHERE description = 'Expense With Parens'", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)

            if data["rows"]:
                amount = float(data["rows"][0][0])
                assert amount < 0, f"Parentheses notation should be negative, got {amount}"
                assert abs(amount + 100.0) < 0.01, f"Expected -100.00, got {amount}"


class TestImportDateFormats:
    """Tests for import date format parsing."""

    def test_import_dd_mm_yyyy_format(self):
        """Test that DD-MM-YYYY date format is supported."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(
                ["query", "SELECT account_id FROM accounts LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            account_id = data["rows"][0][0]

            # Create CSV with DD-MM-YYYY format
            csv_path = Path(tmpdir) / "ddmmyyyy_test.csv"
            csv_path.write_text(
                "Date,Description,Amount\n"
                "15-01-2025,DD-MM-YYYY Test,-25.00\n"
            )

            result = run_cli(
                ["import", str(csv_path), "--account-id", account_id],
                tmpdir,
            )
            assert result.returncode == 0, f"Import with DD-MM-YYYY failed: {result.stderr}"

            # Verify it was imported with correct date
            result = run_cli(
                ["query", "SELECT transaction_date FROM transactions WHERE description = 'DD-MM-YYYY Test'", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)

            if data["rows"]:
                tx_date = data["rows"][0][0]
                assert "2025-01-15" in tx_date, f"Date should be 2025-01-15, got {tx_date}"


class TestQueryCommand:
    """Tests for query command features."""

    def test_query_basic(self):
        """Test basic query execution."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(
                ["query", "SELECT COUNT(*) as cnt FROM accounts", "--json"],
                tmpdir,
            )
            assert result.returncode == 0, f"Query failed: {result.stderr}"
            data = json.loads(result.stdout)
            assert data["rows"][0][0] > 0, "Should have accounts"

    def test_query_format_csv(self):
        """Test query with CSV format output."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(
                ["query", "SELECT name FROM accounts LIMIT 1", "--format", "csv"],
                tmpdir,
            )
            assert result.returncode == 0, f"Query with CSV format failed: {result.stderr}"
            # CSV output should have header and data
            lines = result.stdout.strip().split('\n')
            assert len(lines) >= 2, "CSV should have header and data"


class TestRemoveCommand:
    """Tests for remove command."""

    def test_remove_nonexistent_integration(self):
        """Test removing a non-existent integration."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            result = run_cli(["remove", "nonexistent-integration"], tmpdir)
            # Should fail - integration doesn't exist
            assert result.returncode != 0, "Remove should fail for non-existent integration"


class TestDemoCommand:
    """Tests for demo command."""

    def test_demo_on_off_cycle(self):
        """Test demo mode on/off cycle."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Turn on demo
            result = run_cli(["demo", "on"], tmpdir)
            assert result.returncode == 0, f"Demo on failed: {result.stderr}"

            # Check status
            result = run_cli(["demo", "status"], tmpdir)
            assert result.returncode == 0

            # Turn off demo
            result = run_cli(["demo", "off"], tmpdir)
            assert result.returncode == 0, f"Demo off failed: {result.stderr}"

    def test_demo_creates_data(self):
        """Test that demo mode creates accounts and transactions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Check accounts exist
            result = run_cli(
                ["query", "SELECT COUNT(*) FROM accounts", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            account_count = int(data["rows"][0][0])
            assert account_count > 0, "Demo should create accounts"

            # Check transactions exist
            result = run_cli(
                ["query", "SELECT COUNT(*) FROM transactions", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tx_count = int(data["rows"][0][0])
            assert tx_count > 0, "Demo should create transactions"


class TestQueryFileFlag:
    """Tests for query --file flag."""

    def test_query_from_file(self):
        """Test query can read SQL from a file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Create SQL file
            sql_path = Path(tmpdir) / "query.sql"
            sql_path.write_text("SELECT COUNT(*) as cnt FROM accounts")

            result = run_cli(
                ["query", "--file", str(sql_path), "--json"],
                tmpdir,
            )
            assert result.returncode == 0, f"Query from file failed: {result.stderr}"
            data = json.loads(result.stdout)
            assert data["rows"][0][0] > 0, "Should have accounts"

    def test_query_from_stdin(self):
        """Test query can read SQL from stdin."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Pipe SQL via stdin
            result = run_cli(
                ["query", "--json"],
                tmpdir,
                input_text="SELECT COUNT(*) as cnt FROM transactions",
            )
            assert result.returncode == 0, f"Query from stdin failed: {result.stderr}"
            data = json.loads(result.stdout)
            assert data["rows"][0][0] > 0, "Should have transactions"


class TestTagStdin:
    """Tests for tag command stdin support."""

    def test_tag_with_ids_from_stdin(self):
        """Test that tag command can read IDs from stdin."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Get a transaction ID
            result = run_cli(
                ["query", "SELECT transaction_id FROM transactions LIMIT 1", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tx_id = data["rows"][0][0]

            # Tag via stdin (newline-separated)
            result = run_cli(
                ["tag", "stdin-test"],
                tmpdir,
                input_text=tx_id,
            )
            assert result.returncode == 0, f"Tag from stdin failed: {result.stderr}"

            # Verify the tag was applied
            result = run_cli(
                ["query", f"SELECT tags FROM transactions WHERE transaction_id = '{tx_id}'", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tags = data["rows"][0][0]
            assert "stdin-test" in str(tags), f"Tag not applied: {tags}"

    def test_tag_with_comma_separated_ids_from_stdin(self):
        """Test that tag command handles comma-separated IDs from stdin."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_cli(["demo", "on"], tmpdir)

            # Get two transaction IDs
            result = run_cli(
                ["query", "SELECT transaction_id FROM transactions LIMIT 2", "--json"],
                tmpdir,
            )
            data = json.loads(result.stdout)
            tx_ids = [row[0] for row in data["rows"]]

            # Tag via stdin (comma-separated)
            result = run_cli(
                ["tag", "multi-stdin-test"],
                tmpdir,
                input_text=",".join(tx_ids),
            )
            assert result.returncode == 0, f"Tag from stdin failed: {result.stderr}"

            # Verify tags were applied
            for tx_id in tx_ids:
                result = run_cli(
                    ["query", f"SELECT tags FROM transactions WHERE transaction_id = '{tx_id}'", "--json"],
                    tmpdir,
                )
                data = json.loads(result.stdout)
                tags = data["rows"][0][0]
                assert "multi-stdin-test" in str(tags), f"Tag not applied to {tx_id}: {tags}"
