"""Unit tests for CSVProvider."""

import tempfile
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from treeline.domain import Ok, Fail
from treeline.infra.csv import CSVProvider


@pytest.mark.asyncio
async def test_csv_provider_capabilities():
    """Test that CSV provider has correct capabilities."""
    provider = CSVProvider()

    assert provider.can_get_accounts is False
    assert provider.can_get_transactions is True
    assert provider.can_get_balances is False


@pytest.mark.asyncio
async def test_get_transactions_with_simple_csv():
    """Test parsing a simple CSV file."""
    provider = CSVProvider()

    # Create temporary CSV file
    csv_content = """Date,Description,Amount
2024-10-01,Coffee at Starbucks,-5.50
2024-10-02,Grocery Store,-45.00
2024-10-03,Salary Deposit,2500.00
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = await provider.get_transactions(
            start_date=datetime.min,
            end_date=datetime.max,
            provider_account_ids=[],
            provider_settings={
                "file_path": csv_path,
                "column_mapping": {
                    "date": "Date",
                    "description": "Description",
                    "amount": "Amount",
                },
            },
        )

        assert result.success
        transactions = result.data
        assert len(transactions) == 3

        # Check first transaction
        assert transactions[0].amount == Decimal("-5.50")
        assert transactions[0].description == "Coffee at Starbucks"
        assert transactions[0].transaction_date == date(2024, 10, 1)
    finally:
        Path(csv_path).unlink()


@pytest.mark.asyncio
async def test_get_transactions_with_different_date_formats():
    """Test parsing CSV with various date formats."""
    provider = CSVProvider()

    test_cases = [
        ("2024-10-01", "YYYY-MM-DD"),
        ("10/01/2024", "MM/DD/YYYY"),
        ("01/10/2024", "DD/MM/YYYY"),
    ]

    for date_str, format_name in test_cases:
        csv_content = f"""Date,Description,Amount
{date_str},Coffee,-5.50
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            csv_path = f.name

        try:
            result = await provider.get_transactions(
                start_date=datetime.min,
                end_date=datetime.max,
                provider_account_ids=[],
                provider_settings={
                    "file_path": csv_path,
                    "column_mapping": {
                        "date": "Date",
                        "description": "Description",
                        "amount": "Amount",
                    },
                    "date_format": format_name,
                },
            )

            assert result.success, f"Failed to parse {format_name}: {result.error}"
            transactions = result.data
            assert len(transactions) == 1
        finally:
            Path(csv_path).unlink()


@pytest.mark.asyncio
async def test_get_transactions_missing_file():
    """Test error handling when CSV file doesn't exist."""
    provider = CSVProvider()

    result = await provider.get_transactions(
        start_date=datetime.min,
        end_date=datetime.max,
        provider_account_ids=[],
        provider_settings={
            "file_path": "/nonexistent/file.csv",
            "column_mapping": {
                "date": "Date",
                "description": "Description",
                "amount": "Amount",
            },
        },
    )

    assert not result.success
    assert "not found" in result.error.lower() or "no such file" in result.error.lower()


@pytest.mark.asyncio
async def test_get_transactions_missing_column_mapping():
    """Test error handling when column mapping is missing."""
    provider = CSVProvider()

    csv_content = """Date,Description,Amount
2024-10-01,Coffee,-5.50
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = await provider.get_transactions(
            start_date=datetime.min,
            end_date=datetime.max,
            provider_account_ids=[],
            provider_settings={"file_path": csv_path},
        )

        assert not result.success
        assert "column_mapping" in result.error.lower()
    finally:
        Path(csv_path).unlink()


@pytest.mark.asyncio
async def test_get_transactions_with_optional_columns():
    """Test parsing CSV with optional columns like posted_date."""
    provider = CSVProvider()

    csv_content = """Date,Posted,Description,Amount
2024-10-01,2024-10-02,Coffee,-5.50
2024-10-03,,Grocery,-45.00
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = await provider.get_transactions(
            start_date=datetime.min,
            end_date=datetime.max,
            provider_account_ids=[],
            provider_settings={
                "file_path": csv_path,
                "column_mapping": {
                    "date": "Date",
                    "posted_date": "Posted",
                    "description": "Description",
                    "amount": "Amount",
                },
            },
        )

        assert result.success
        transactions = result.data
        assert len(transactions) == 2

        # First transaction has both dates
        assert transactions[0].transaction_date == date(2024, 10, 1)
        assert transactions[0].posted_date == date(2024, 10, 2)

        # Second transaction has only transaction_date, posted_date should default to transaction_date
        assert transactions[1].transaction_date == date(2024, 10, 3)
        assert transactions[1].posted_date == date(2024, 10, 3)
    finally:
        Path(csv_path).unlink()


@pytest.mark.asyncio
async def test_get_transactions_handles_amounts_with_currency_symbols():
    """Test parsing amounts with $ signs and commas."""
    provider = CSVProvider()

    csv_content = """Date,Description,Amount
2024-10-01,Expensive Thing,"$1,234.56"
2024-10-02,Cheap Thing,"-$5.50"
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = await provider.get_transactions(
            start_date=datetime.min,
            end_date=datetime.max,
            provider_account_ids=[],
            provider_settings={
                "file_path": csv_path,
                "column_mapping": {
                    "date": "Date",
                    "description": "Description",
                    "amount": "Amount",
                },
            },
        )

        assert result.success
        transactions = result.data
        assert len(transactions) == 2
        assert transactions[0].amount == Decimal("1234.56")
        assert transactions[1].amount == Decimal("-5.50")
    finally:
        Path(csv_path).unlink()


@pytest.mark.asyncio
async def test_get_accounts_not_supported():
    """Test that get_accounts returns not supported error."""
    provider = CSVProvider()

    result = await provider.get_accounts(provider_account_ids=[], provider_settings={})

    assert not result.success
    assert "not support" in result.error.lower()


@pytest.mark.asyncio
async def test_get_balances_not_supported():
    """Test that get_balances returns not supported error."""
    provider = CSVProvider()

    result = await provider.get_balances(provider_account_ids=[], provider_settings={})

    assert not result.success
    assert "not support" in result.error.lower()


def test_detect_columns():
    """Test column auto-detection."""
    provider = CSVProvider()

    csv_content = """Date,Description,Amount
2024-10-01,Coffee,-5.50
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = provider.detect_columns(csv_path)

        assert result.success
        detected = result.data
        assert detected.get("date") == "Date"
        assert detected.get("description") == "Description"
        assert detected.get("amount") == "Amount"
    finally:
        Path(csv_path).unlink()


def test_detect_columns_debit_credit():
    """Test column auto-detection with debit/credit columns."""
    provider = CSVProvider()

    csv_content = """Transaction Date,Merchant,Debit,Credit
2024-10-01,Coffee Shop,5.50,
2024-10-02,Refund,,10.00
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = provider.detect_columns(csv_path)

        assert result.success
        detected = result.data
        assert detected.get("date") == "Transaction Date"
        assert detected.get("description") == "Merchant"
        assert detected.get("debit") == "Debit"
        assert detected.get("credit") == "Credit"
    finally:
        Path(csv_path).unlink()


def test_preview_transactions():
    """Test transaction preview."""
    provider = CSVProvider()

    csv_content = """Date,Description,Amount
2024-10-01,Coffee,-5.50
2024-10-02,Grocery,-45.00
2024-10-03,Salary,2500.00
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = provider.preview_transactions(
            csv_path,
            column_mapping={
                "date": "Date",
                "description": "Description",
                "amount": "Amount",
            },
            limit=2,
        )

        assert result.success
        transactions = result.data
        assert len(transactions) == 2
        assert transactions[0].description == "Coffee"
        assert transactions[0].amount == Decimal("-5.50")
        assert transactions[1].description == "Grocery"
    finally:
        Path(csv_path).unlink()


def test_preview_transactions_with_sign_flip():
    """Test transaction preview with sign flip."""
    provider = CSVProvider()

    csv_content = """Date,Description,Amount
2024-10-01,Coffee,5.50
2024-10-02,Salary,-2500.00
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = provider.preview_transactions(
            csv_path,
            column_mapping={
                "date": "Date",
                "description": "Description",
                "amount": "Amount",
            },
            flip_signs=True,
        )

        assert result.success
        transactions = result.data
        assert len(transactions) == 2
        # Signs should be flipped
        assert transactions[0].amount == Decimal("-5.50")  # Was 5.50
        assert transactions[1].amount == Decimal("2500.00")  # Was -2500.00
    finally:
        Path(csv_path).unlink()


@pytest.mark.asyncio
async def test_get_transactions_with_debit_credit_columns():
    """Test parsing CSV with separate debit/credit columns - preserves signs from CSV."""
    provider = CSVProvider()

    # Signs are preserved from CSV exactly as they appear
    csv_content = """Date,Description,Debit,Credit
2024-10-01,Coffee Shop,5.50,
2024-10-02,Grocery Store,45.00,
2024-10-03,Refund,,10.00
2024-10-04,Payment,,100.00
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = await provider.get_transactions(
            start_date=datetime.min,
            end_date=datetime.max,
            provider_account_ids=[],
            provider_settings={
                "file_path": csv_path,
                "column_mapping": {
                    "date": "Date",
                    "description": "Description",
                    "debit": "Debit",
                    "credit": "Credit",
                },
            },
        )

        assert result.success
        transactions = result.data
        assert len(transactions) == 4

        # Debit values preserved as positive (user will flip if these should be negative)
        assert transactions[0].amount == Decimal(
            "5.50"
        )  # Coffee (debit, positive in CSV)
        assert transactions[1].amount == Decimal(
            "45.00"
        )  # Grocery (debit, positive in CSV)

        # Credit values preserved as positive
        assert transactions[2].amount == Decimal(
            "10.00"
        )  # Refund (credit, positive in CSV)
        assert transactions[3].amount == Decimal(
            "100.00"
        )  # Payment (credit, positive in CSV)
    finally:
        Path(csv_path).unlink()


@pytest.mark.asyncio
async def test_get_transactions_with_citi_credit_card_format():
    """Test parsing Citi credit card CSV format.

    Citi format has:
    - Debit: positive values (spending)
    - Credit: negative values (payments)

    User will see preview and choose to flip signs to get:
    - Debit: negative (spending)
    - Credit: positive (payments)
    """
    provider = CSVProvider()

    # Citi credit card CSV: Debit=positive spending, Credit=negative payments
    csv_content = """Date,Description,Debit,Credit
2024-10-01,Coffee Shop,5.50,
2024-10-02,Grocery Store,45.00,
2024-10-03,Payment Thank You,,-1669.25
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = await provider.get_transactions(
            start_date=datetime.min,
            end_date=datetime.max,
            provider_account_ids=[],
            provider_settings={
                "file_path": csv_path,
                "column_mapping": {
                    "date": "Date",
                    "description": "Description",
                    "debit": "Debit",
                    "credit": "Credit",
                },
            },
        )

        assert result.success
        transactions = result.data
        assert len(transactions) == 3

        # Debit values preserved as positive (from CSV)
        assert transactions[0].amount == Decimal(
            "5.50"
        )  # Coffee (debit, positive in CSV)
        assert transactions[1].amount == Decimal(
            "45.00"
        )  # Grocery (debit, positive in CSV)

        # Credit value preserved as negative (from CSV)
        assert transactions[2].amount == Decimal(
            "-1669.25"
        )  # Payment (credit, negative in CSV)
    finally:
        Path(csv_path).unlink()


# NEW TESTS FOR IMPROVED COLUMN DETECTION


def test_detect_columns_abbreviated_names():
    """Test column detection with abbreviated names like Dt, Desc, Amt."""
    provider = CSVProvider()

    csv_content = """Dt,Desc,Amt
2024-10-01,Coffee,-5.50
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = provider.detect_columns(csv_path)

        assert result.success
        detected = result.data
        assert detected.get("date") == "Dt"
        assert detected.get("description") == "Desc"
        assert detected.get("amount") == "Amt"
    finally:
        Path(csv_path).unlink()


def test_detect_columns_international_names():
    """Test column detection with international names like Dr, Cr, Narration."""
    provider = CSVProvider()

    csv_content = """TxnDate,Narration,Dr,Cr
2024-10-01,Coffee Shop,5.50,
2024-10-02,Refund,,10.00
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = provider.detect_columns(csv_path)

        assert result.success
        detected = result.data
        assert detected.get("date") == "TxnDate"
        assert detected.get("description") == "Narration"
        assert detected.get("debit") == "Dr"
        assert detected.get("credit") == "Cr"
    finally:
        Path(csv_path).unlink()


def test_detect_columns_with_currency_suffix():
    """Test column detection with currency suffix like 'Amount EUR'."""
    provider = CSVProvider()

    csv_content = """Date,Merchant,Amount EUR
15/01/2024,Starbucks,-5.67
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = provider.detect_columns(csv_path)

        assert result.success
        detected = result.data
        assert detected.get("date") == "Date"
        assert detected.get("description") == "Merchant"
        assert detected.get("amount") == "Amount EUR"
    finally:
        Path(csv_path).unlink()


def test_detect_columns_with_extra_words():
    """Test column detection with extra words like 'Transaction Amount'."""
    provider = CSVProvider()

    csv_content = """Transaction Date,Payee Name,Transaction Amount
2024-10-01,Coffee Shop,-5.50
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = provider.detect_columns(csv_path)

        assert result.success
        detected = result.data
        assert detected.get("date") == "Transaction Date"
        assert detected.get("description") == "Payee Name"
        assert detected.get("amount") == "Transaction Amount"
    finally:
        Path(csv_path).unlink()


def test_detect_columns_withdrawal_deposit():
    """Test column detection with Withdrawal/Deposit instead of Debit/Credit."""
    provider = CSVProvider()

    csv_content = """Date,Description,Withdrawal,Deposit
2024-10-01,Coffee,5.50,
2024-10-02,Paycheck,,2500.00
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = provider.detect_columns(csv_path)

        assert result.success
        detected = result.data
        assert detected.get("date") == "Date"
        assert detected.get("description") == "Description"
        assert detected.get("debit") == "Withdrawal"
        assert detected.get("credit") == "Deposit"
    finally:
        Path(csv_path).unlink()


def test_detect_columns_with_fallback_description():
    """Test description fallback when primary description column is missing."""
    provider = CSVProvider()

    csv_content = """Trans Date,Type,Ref,Amt
2024-10-01,PURCHASE,1001,-5.50
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = provider.detect_columns(csv_path)

        assert result.success
        detected = result.data
        assert detected.get("date") == "Trans Date"
        # Should find Type as description
        assert detected.get("description") in ["Type", "Ref"]
        assert detected.get("amount") == "Amt"
    finally:
        Path(csv_path).unlink()


def test_should_negate_debits():
    """Test detection of unsigned debit/credit convention."""
    provider = CSVProvider()

    # Unsigned debit/credit: both columns have positive values
    csv_content = """Date,Description,Debit,Credit
2024-10-01,Coffee,5.50,
2024-10-02,Grocery,45.00,
2024-10-03,Paycheck,,2500.00
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = provider.should_negate_debits(csv_path, "Debit", "Credit")

        assert result.success
        # Should suggest negating debits because all debits are positive
        assert result.data is True
    finally:
        Path(csv_path).unlink()


def test_should_not_negate_signed_debits():
    """Test that signed debit/credit convention is not flagged."""
    provider = CSVProvider()

    # Signed debit/credit: debits are already negative
    csv_content = """Date,Description,Debit,Credit
2024-10-01,Coffee,-5.50,
2024-10-02,Grocery,-45.00,
2024-10-03,Paycheck,,2500.00
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = provider.should_negate_debits(csv_path, "Debit", "Credit")

        assert result.success
        # Should NOT suggest negating because debits are already negative
        assert result.data is False
    finally:
        Path(csv_path).unlink()


@pytest.mark.asyncio
async def test_get_transactions_with_debit_negative_flag():
    """Test using debit_negative flag to negate unsigned debit values."""
    provider = CSVProvider()

    # Unsigned debit/credit format
    csv_content = """Date,Description,Debit,Credit
2024-10-01,Coffee,5.50,
2024-10-02,Grocery,45.00,
2024-10-03,Paycheck,,2500.00
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        csv_path = f.name

    try:
        result = await provider.get_transactions(
            start_date=datetime.min,
            end_date=datetime.max,
            provider_account_ids=[],
            provider_settings={
                "file_path": csv_path,
                "column_mapping": {
                    "date": "Date",
                    "description": "Description",
                    "debit": "Debit",
                    "credit": "Credit",
                },
                "debit_negative": True,  # New flag
            },
        )

        assert result.success
        transactions = result.data
        assert len(transactions) == 3

        # Debits should be negated
        assert transactions[0].amount == Decimal("-5.50")  # Coffee (was 5.50)
        assert transactions[1].amount == Decimal("-45.00")  # Grocery (was 45.00)

        # Credits remain positive
        assert transactions[2].amount == Decimal("2500.00")  # Paycheck
    finally:
        Path(csv_path).unlink()
