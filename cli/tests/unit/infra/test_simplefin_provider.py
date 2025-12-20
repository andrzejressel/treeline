"""Unit tests for SimpleFINProvider."""

from datetime import datetime, timezone
from decimal import Decimal
from types import MappingProxyType
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import pytest

from treeline.domain import Account, Transaction, BalanceSnapshot, Ok
from treeline.infra.simplefin import SimpleFINProvider


@pytest.mark.asyncio
async def test_get_accounts_success():
    """Test successful account fetching from SimpleFIN."""
    provider = SimpleFINProvider()

    # Mock SimpleFIN response
    mock_response = {
        "accounts": [
            {
                "id": "acc123",
                "name": "Checking Account",
                "currency": "USD",
                "balance": "1500.50",
                "available-balance": "1450.00",
                "balance-date": 1735689600,
                "org": {
                    "name": "Test Bank",
                    "url": "https://testbank.com",
                    "domain": "testbank.com",
                },
            }
        ]
    }

    with patch("httpx.AsyncClient.get") as mock_get:
        mock_get.return_value = Mock(status_code=200, json=lambda: mock_response)

        provider_options = {
            "accessUrl": "https://username:password@bridge.simplefin.org/simplefin"
        }
        result = await provider.get_accounts(
            provider_account_ids=[], provider_settings=provider_options
        )

        assert result.success is True
        # New format: {"accounts": [...], "errors": [...]}
        accounts = result.data["accounts"]
        errors = result.data["errors"]
        assert len(accounts) == 1
        assert len(errors) == 0
        assert accounts[0].name == "Checking Account"
        assert accounts[0].external_ids.get("simplefin") == "acc123"
        assert accounts[0].currency == "USD"
        assert accounts[0].institution_name == "Test Bank"


@pytest.mark.asyncio
async def test_get_accounts_filters_by_account_ids():
    """Test that account filtering works correctly."""
    provider = SimpleFINProvider()

    mock_response = {
        "accounts": [
            {
                "id": "acc1",
                "name": "Account 1",
                "currency": "USD",
                "balance": "100",
                "available-balance": "100",
                "balance-date": 1735689600,
                "org": {
                    "name": "Bank",
                    "url": "https://bank.com",
                    "domain": "bank.com",
                },
            },
            {
                "id": "acc2",
                "name": "Account 2",
                "currency": "USD",
                "balance": "200",
                "available-balance": "200",
                "balance-date": 1735689600,
                "org": {
                    "name": "Bank",
                    "url": "https://bank.com",
                    "domain": "bank.com",
                },
            },
        ]
    }

    with patch("httpx.AsyncClient.get") as mock_get:
        mock_get.return_value = Mock(status_code=200, json=lambda: mock_response)

        provider_options = {
            "accessUrl": "https://username:password@bridge.simplefin.org/simplefin"
        }
        result = await provider.get_accounts(
            provider_account_ids=["acc1"], provider_settings=provider_options
        )

        assert result.success is True
        # New format: {"accounts": [...], "errors": [...]}
        accounts = result.data["accounts"]
        assert len(accounts) == 1
        assert accounts[0].external_ids.get("simplefin") == "acc1"


@pytest.mark.asyncio
async def test_get_accounts_missing_access_url():
    """Test that missing accessUrl returns error."""
    provider = SimpleFINProvider()

    result = await provider.get_accounts(provider_account_ids=[], provider_settings={})

    assert result.success is False
    assert "accessUrl is required" in result.error


@pytest.mark.asyncio
async def test_get_transactions_success():
    """Test successful transaction fetching from SimpleFIN."""
    provider = SimpleFINProvider()

    mock_response = {
        "accounts": [
            {
                "id": "acc123",
                "name": "Checking",
                "currency": "USD",
                "balance": "1000",
                "available-balance": "1000",
                "balance-date": 1735689600,
                "org": {
                    "name": "Bank",
                    "url": "https://bank.com",
                    "domain": "bank.com",
                },
                "transactions": [
                    {
                        "id": "tx1",
                        "posted": 1735689600,
                        "amount": "-50.00",
                        "description": "Coffee Shop",
                        "pending": False,
                    },
                    {
                        "id": "tx2",
                        "posted": 1735689700,
                        "amount": "100.00",
                        "description": "Paycheck",
                        "pending": False,
                    },
                ],
            }
        ]
    }

    with patch("httpx.AsyncClient.get") as mock_get:
        mock_get.return_value = Mock(status_code=200, json=lambda: mock_response)

        provider_options = {
            "accessUrl": "https://username:password@bridge.simplefin.org/simplefin"
        }
        start_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2025, 1, 31, tzinfo=timezone.utc)

        result = await provider.get_transactions(
            start_date,
            end_date,
            provider_account_ids=["acc123"],
            provider_settings=provider_options,
        )

        assert result.success is True
        # New format: {"transactions": [...], "errors": [...]}
        transactions_with_accounts = result.data["transactions"]
        errors = result.data["errors"]
        assert len(transactions_with_accounts) == 2
        assert len(errors) == 0

        # SimpleFIN now returns tuples of (account_id, transaction)
        account_id_1, tx1 = transactions_with_accounts[0]
        assert account_id_1 == "acc123"
        assert tx1.external_ids.get("simplefin") == "tx1"
        assert tx1.amount == Decimal("-50.00")
        assert tx1.description == "Coffee Shop"


@pytest.mark.asyncio
async def test_get_accounts_with_api_errors():
    """Test that API-level errors are captured and returned."""
    provider = SimpleFINProvider()

    # Mock response with errors (e.g., "You must reauthenticate")
    mock_response = {
        "errors": ["You must reauthenticate.", "Connection to Bank XYZ failed."],
        "accounts": [
            {
                "id": "acc123",
                "name": "Working Account",
                "currency": "USD",
                "balance": "500.00",
                "available-balance": "500.00",
                "balance-date": 1735689600,
                "org": {"name": "Working Bank"},
            }
        ],
    }

    with patch("httpx.AsyncClient.get") as mock_get:
        mock_get.return_value = Mock(status_code=200, json=lambda: mock_response)

        provider_options = {
            "accessUrl": "https://username:password@bridge.simplefin.org/simplefin"
        }
        result = await provider.get_accounts(
            provider_account_ids=[], provider_settings=provider_options
        )

        assert result.success is True
        # Accounts should still be returned
        accounts = result.data["accounts"]
        assert len(accounts) == 1
        assert accounts[0].name == "Working Account"

        # Errors should be captured
        errors = result.data["errors"]
        assert len(errors) == 2
        assert "reauthenticate" in errors[0]
        assert "Bank XYZ" in errors[1]


@pytest.mark.asyncio
async def test_get_accounts_http_403_error():
    """Test that HTTP 403 returns actionable error message."""
    provider = SimpleFINProvider()

    with patch("httpx.AsyncClient.get") as mock_get:
        mock_get.return_value = Mock(status_code=403)

        provider_options = {
            "accessUrl": "https://username:password@bridge.simplefin.org/simplefin"
        }
        result = await provider.get_accounts(
            provider_account_ids=[], provider_settings=provider_options
        )

        assert result.success is False
        assert "authentication failed" in result.error.lower()
        assert "beta-bridge.simplefin.org" in result.error


@pytest.mark.asyncio
async def test_get_accounts_http_402_error():
    """Test that HTTP 402 returns payment required message."""
    provider = SimpleFINProvider()

    with patch("httpx.AsyncClient.get") as mock_get:
        mock_get.return_value = Mock(status_code=402)

        provider_options = {
            "accessUrl": "https://username:password@bridge.simplefin.org/simplefin"
        }
        result = await provider.get_accounts(
            provider_account_ids=[], provider_settings=provider_options
        )

        assert result.success is False
        assert "payment" in result.error.lower()
        assert "beta-bridge.simplefin.org" in result.error


@pytest.mark.asyncio
async def test_parse_access_url_invalid():
    """Test that invalid access URLs are rejected."""
    provider = SimpleFINProvider()

    # Test with non-HTTPS URL
    result = await provider.get_accounts(
        provider_account_ids=[],
        provider_settings={
            "accessUrl": "http://user:pass@bridge.simplefin.org/simplefin"
        },
    )
    assert result.success is False
    assert "HTTPS" in result.error

    # Test with wrong domain
    result = await provider.get_accounts(
        provider_account_ids=[],
        provider_settings={"accessUrl": "https://user:pass@evil.com/simplefin"},
    )
    assert result.success is False
    assert "simplefin.org" in result.error


@pytest.mark.asyncio
async def test_create_integration_success():
    """Test successful integration setup with SimpleFIN."""
    provider = SimpleFINProvider()

    setup_token = "dGVzdF90b2tlbg=="  # Base64 encoded test string
    access_url = "https://username:password@bridge.simplefin.org/simplefin/access"

    with patch("httpx.AsyncClient.post") as mock_post:
        mock_post.return_value = Mock(status_code=200, text=access_url)

        result = await provider.create_integration(
            "simplefin", {"setupToken": setup_token}
        )

        assert result.success is True
        assert result.data == {"accessUrl": access_url}


@pytest.mark.asyncio
async def test_create_integration_missing_setup_token():
    """Test integration setup fails without setup token."""
    provider = SimpleFINProvider()

    result = await provider.create_integration("simplefin", {})

    assert result.success is False
    assert "setupToken is required" in result.error


@pytest.mark.asyncio
async def test_create_integration_invalid_token():
    """Test integration setup fails with invalid token."""
    provider = SimpleFINProvider()

    result = await provider.create_integration(
        "simplefin", {"setupToken": "not_valid_base64!@#"}
    )

    assert result.success is False
    assert (
        "Invalid setup token" in result.error or "setup token" in result.error.lower()
    )
