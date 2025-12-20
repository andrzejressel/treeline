from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from treeline.domain import Account, BalanceSnapshot, Transaction


def _tz_now() -> datetime:
    return datetime.now(timezone.utc)


def _today() -> date:
    return date.today()


def test_account_fields_align_with_schema() -> None:
    created_at = datetime(2024, 1, 1, 8, 30, tzinfo=timezone(timedelta(hours=-5)))
    updated_at = _tz_now()

    account = Account(
        id=uuid4(),
        name=" Checking ",
        nickname=" Main ",
        account_type="checking",
        currency="usd",
        external_ids={"simplefin": "abc"},
        institution_name=" Treeline Bank ",
        institution_url="https://treeline.money",
        institution_domain="treeline.money",
        created_at=created_at,
        updated_at=updated_at,
    )

    assert account.name == "Checking"
    assert account.nickname == "Main"
    assert account.currency == "USD"
    assert dict(account.external_ids) == {"simplefin": "abc"}
    assert account.institution_name == "Treeline Bank"
    assert account.created_at.tzinfo == timezone.utc
    assert account.created_at == created_at.astimezone(timezone.utc)

    with pytest.raises(ValidationError):
        Account(
            id=uuid4(),
            name=" ",
            currency="USD",
            created_at=_tz_now(),
            updated_at=_tz_now(),
        )

    with pytest.raises(ValidationError):
        Account(
            id=uuid4(),
            name="Checking",
            currency="USD",
            created_at=datetime.now(),
            updated_at=_tz_now(),
        )


def test_transaction_normalizes_amount_and_tags() -> None:
    account_id: UUID = uuid4()
    transaction = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount="123.45",
        description=" Grocery run ",
        external_ids={"simplefin": "txn-1"},
        transaction_date=_today(),
        posted_date=_today(),
        created_at=_tz_now(),
        updated_at=_tz_now(),
        tags=[" groceries ", "Fuel", "groceries"],
    )

    assert transaction.amount == Decimal("123.45")
    assert transaction.description == "Grocery run"
    assert transaction.tags == ("groceries", "Fuel")
    # Fingerprint is auto-added to external_ids
    ext_ids = dict(transaction.external_ids)
    assert "simplefin" in ext_ids
    assert "fingerprint" in ext_ids  # Auto-generated

    # Zero-amount transactions are valid (transfers, pending, corrections, etc.)
    zero_amount_transaction = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount="0",
        description="Transfer",
        transaction_date=_today(),
        posted_date=_today(),
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )
    assert zero_amount_transaction.amount == Decimal("0")


def test_balance_snapshot_requires_timezone_aware_datetime() -> None:
    """Test that created_at/updated_at require timezone but snapshot_time is naive (local)."""
    account_id = uuid4()

    # snapshot_time should be naive (local time), created_at/updated_at should be timezone-aware
    snapshot = BalanceSnapshot(
        id=uuid4(),
        account_id=account_id,
        snapshot_time=datetime.now(),  # Naive = local time
        balance="1000.50",
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )

    assert snapshot.balance == Decimal("1000.50")
    assert snapshot.snapshot_time.tzinfo is None  # Naive datetime
    assert snapshot.created_at.tzinfo == timezone.utc
    assert snapshot.updated_at.tzinfo == timezone.utc

    # created_at/updated_at must still be timezone-aware
    with pytest.raises(ValidationError):
        BalanceSnapshot(
            id=uuid4(),
            account_id=account_id,
            snapshot_time=datetime.now(),
            balance="1000.50",
            created_at=datetime.now(),  # Missing timezone - should fail
            updated_at=_tz_now(),
        )


def test_transaction_auto_generates_dedup_key() -> None:
    """Test that Transaction automatically generates fingerprint in external_ids."""
    account_id = uuid4()

    # Create transaction without providing fingerprint
    tx = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=Decimal("25.50"),
        description="Coffee at Starbucks",
        transaction_date=date(2025, 10, 4),
        posted_date=date(2025, 10, 4),
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )

    # fingerprint should be auto-generated in external_ids
    assert "fingerprint" in tx.external_ids
    assert (
        len(tx.external_ids["fingerprint"]) == 16
    )  # SHA256 hash truncated to 16 chars

    # Same transaction data should generate same fingerprint
    tx2 = Transaction(
        id=uuid4(),  # Different ID
        account_id=account_id,  # Same account
        amount=Decimal("25.50"),  # Same amount
        description="Coffee at Starbucks",  # Same description
        transaction_date=date(2025, 10, 4),  # Same date
        posted_date=date(2025, 10, 4),
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )

    assert tx.external_ids["fingerprint"] == tx2.external_ids["fingerprint"]

    # Different description should generate different fingerprint
    tx3 = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=Decimal("25.50"),
        description="Coffee at Peet's",  # Different description
        transaction_date=date(2025, 10, 4),
        posted_date=date(2025, 10, 4),
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )

    assert tx.external_ids["fingerprint"] != tx3.external_ids["fingerprint"]


def test_transaction_dedup_key_strips_csv_noise() -> None:
    """Test that fingerprint normalization strips common CSV noise patterns.

    Verifies that 'null' literals and card number masks (XXXXXXXXXXXX1234)
    are stripped from descriptions during fingerprinting, allowing CSV and
    SimpleFIN formats to match when they represent the same transaction.
    """
    account_id = uuid4()
    tx_date = date(2025, 7, 9)
    amount = Decimal("21.77")

    # CSV format: has "null" and card number mask
    tx_csv = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=amount,
        description="ACME STORE #123 SEATTLE WA null XXXXXXXXXXXX9876",
        transaction_date=tx_date,
        posted_date=tx_date,
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )

    # SimpleFIN format: no "null" or card number (same vendor)
    tx_simplefin = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=amount,
        description="ACME STORE #123 SEATTLE WA",
        transaction_date=tx_date,
        posted_date=tx_date,
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )

    # Should generate same fingerprint despite format differences
    # Both normalize to: "acmestore123seattlewa"
    assert (
        tx_csv.external_ids["fingerprint"] == tx_simplefin.external_ids["fingerprint"]
    )


def test_transaction_dedup_key_preserves_order_ids() -> None:
    """Test that order IDs are preserved in fingerprint (not stripped)."""
    account_id = uuid4()
    tx_date = date(2025, 7, 9)
    amount = Decimal("21.77")

    # Same vendor, same day, same amount, different order IDs
    tx1 = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=amount,
        description="OnlineStore*ORDER5432 Seattle WA null XXXXXXXXXXXX5555",
        transaction_date=tx_date,
        posted_date=tx_date,
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )

    tx2 = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=amount,
        description="OnlineStore*ORDER9876 Seattle WA null XXXXXXXXXXXX5555",
        transaction_date=tx_date,
        posted_date=tx_date,
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )

    # Should have DIFFERENT fingerprints because order IDs differ
    assert tx1.external_ids["fingerprint"] != tx2.external_ids["fingerprint"]


def test_transaction_dedup_key_normalizes_account_numbers() -> None:
    """Test that account/phone numbers are normalized to last 4 digits."""
    account_id = uuid4()
    tx_date = date(2025, 10, 1)
    amount = Decimal("-50.00")

    # Test Case 1: City of FooBar - masked vs full account number
    simplefin_1 = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=amount,
        description="CITY OF FOOBAR UTIL XXXXXX1234 CO",
        transaction_date=tx_date,
        posted_date=tx_date,
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )
    csv_1 = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=amount,
        description="CITY OF FOOBAR UTIL 4538981234 CO",
        transaction_date=tx_date,
        posted_date=tx_date,
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )
    assert simplefin_1.external_ids["fingerprint"] == csv_1.external_ids["fingerprint"]

    # Test Case 2: Puget Sound Energy - masked vs full phone number
    simplefin_2 = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=amount,
        description="PUGET SOUND ENERGY INC XXXXXX1234 WA",
        transaction_date=tx_date,
        posted_date=tx_date,
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )
    csv_2 = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=amount,
        description="PUGET SOUND ENERGY INC 5552251234 WA",
        transaction_date=tx_date,
        posted_date=tx_date,
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )
    assert simplefin_2.external_ids["fingerprint"] == csv_2.external_ids["fingerprint"]

    # Test Case 3: Target - 4 X's vs leading zeros
    simplefin_3 = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=amount,
        description="TARGET XXXX12345 SEATTLE WA",
        transaction_date=tx_date,
        posted_date=tx_date,
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )
    csv_3 = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=amount,
        description="TARGET 000012345 SEATTLE WA",
        transaction_date=tx_date,
        posted_date=tx_date,
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )
    assert simplefin_3.external_ids["fingerprint"] == csv_3.external_ids["fingerprint"]


def test_transaction_dedup_key_respects_sign() -> None:
    """Test that fingerprint includes sign (purchase vs refund are different)."""
    account_id = uuid4()
    tx_date = date(2025, 10, 4)

    # Purchase
    tx_purchase = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=Decimal("-25.50"),
        description="Coffee at Starbucks",
        transaction_date=tx_date,
        posted_date=tx_date,
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )

    # Refund
    tx_refund = Transaction(
        id=uuid4(),
        account_id=account_id,
        amount=Decimal("25.50"),  # Positive (refund)
        description="Coffee at Starbucks",
        transaction_date=tx_date,
        posted_date=tx_date,
        created_at=_tz_now(),
        updated_at=_tz_now(),
    )

    # Should generate DIFFERENT fingerprints (purchase vs refund)
    assert (
        tx_purchase.external_ids["fingerprint"] != tx_refund.external_ids["fingerprint"]
    )
