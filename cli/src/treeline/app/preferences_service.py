"""Service for managing user preferences including currency settings."""

from decimal import Decimal

from treeline.config import load_settings, save_settings
from treeline.domain import Fail, Ok, Result

# Supported currencies with their symbols and locales
SUPPORTED_CURRENCIES = {
    "USD": {"symbol": "$", "locale": "en-US", "name": "US Dollar"},
    "EUR": {"symbol": "€", "locale": "de-DE", "name": "Euro"},
    "GBP": {"symbol": "£", "locale": "en-GB", "name": "British Pound"},
    "CAD": {"symbol": "$", "locale": "en-CA", "name": "Canadian Dollar"},
    "AUD": {"symbol": "$", "locale": "en-AU", "name": "Australian Dollar"},
    "JPY": {"symbol": "¥", "locale": "ja-JP", "name": "Japanese Yen"},
    "CHF": {"symbol": "CHF", "locale": "de-CH", "name": "Swiss Franc"},
    "CNY": {"symbol": "¥", "locale": "zh-CN", "name": "Chinese Yuan"},
    "INR": {"symbol": "₹", "locale": "en-IN", "name": "Indian Rupee"},
    "MXN": {"symbol": "$", "locale": "es-MX", "name": "Mexican Peso"},
    "BRL": {"symbol": "R$", "locale": "pt-BR", "name": "Brazilian Real"},
    "KRW": {"symbol": "₩", "locale": "ko-KR", "name": "South Korean Won"},
    "SGD": {"symbol": "$", "locale": "en-SG", "name": "Singapore Dollar"},
    "HKD": {"symbol": "$", "locale": "zh-HK", "name": "Hong Kong Dollar"},
    "NOK": {"symbol": "kr", "locale": "nb-NO", "name": "Norwegian Krone"},
    "SEK": {"symbol": "kr", "locale": "sv-SE", "name": "Swedish Krona"},
    "DKK": {"symbol": "kr", "locale": "da-DK", "name": "Danish Krone"},
    "NZD": {"symbol": "$", "locale": "en-NZ", "name": "New Zealand Dollar"},
    "ZAR": {"symbol": "R", "locale": "en-ZA", "name": "South African Rand"},
    "PLN": {"symbol": "zł", "locale": "pl-PL", "name": "Polish Zloty"},
}

DEFAULT_CURRENCY = "USD"


class PreferencesService:
    """Service for managing user preferences via settings.json."""

    def get_currency(self) -> Result[str]:
        """Get the user's currency preference.

        Returns:
            Result containing the currency code (e.g., "USD", "EUR")
            Returns DEFAULT_CURRENCY if not set.
        """
        settings = load_settings()
        app_settings = settings.get("app", {})
        currency = app_settings.get("currency", DEFAULT_CURRENCY)
        return Ok(currency)

    def set_currency(self, currency: str) -> Result[None]:
        """Set the user's currency preference.

        Args:
            currency: Currency code (e.g., "USD", "EUR")

        Returns:
            Result indicating success or failure
        """
        currency = currency.upper()
        if currency not in SUPPORTED_CURRENCIES:
            return Fail(
                f"Unsupported currency: {currency}. "
                f"Supported: {', '.join(sorted(SUPPORTED_CURRENCIES.keys()))}"
            )

        settings = load_settings()
        if "app" not in settings:
            settings["app"] = {}
        settings["app"]["currency"] = currency
        save_settings(settings)
        return Ok(None)


def get_currency_symbol(currency: str) -> str:
    """Get the symbol for a currency code.

    Args:
        currency: Currency code (e.g., "USD", "EUR")

    Returns:
        Currency symbol (e.g., "$", "€")
    """
    currency = currency.upper()
    if currency in SUPPORTED_CURRENCIES:
        return SUPPORTED_CURRENCIES[currency]["symbol"]
    return currency  # Fallback to code itself


def format_currency(
    amount: Decimal | float | int,
    currency: str = DEFAULT_CURRENCY,
    show_symbol: bool = True,
    decimal_places: int = 2,
) -> str:
    """Format an amount in the specified currency.

    Args:
        amount: Amount to format
        currency: Currency code (e.g., "USD", "EUR")
        show_symbol: Whether to include currency symbol
        decimal_places: Number of decimal places (default: 2)

    Returns:
        Formatted currency string (e.g., "$1,234.56", "€1.234,56")
    """
    currency = currency.upper()
    symbol = get_currency_symbol(currency)

    # Convert to float for formatting
    if isinstance(amount, Decimal):
        amount = float(amount)

    # Handle negative amounts
    is_negative = amount < 0
    abs_amount = abs(amount)

    # Format the number with thousands separators
    if decimal_places == 0:
        formatted = f"{abs_amount:,.0f}"
    else:
        formatted = f"{abs_amount:,.{decimal_places}f}"

    # Build result
    if show_symbol:
        result = f"{symbol}{formatted}"
    else:
        result = formatted

    if is_negative:
        result = f"-{result}"

    return result


def format_currency_compact(
    amount: Decimal | float | int,
    currency: str = DEFAULT_CURRENCY,
) -> str:
    """Format a large amount compactly (e.g., $1.2M, $500K).

    Args:
        amount: Amount to format
        currency: Currency code (e.g., "USD", "EUR")

    Returns:
        Compact formatted string
    """
    currency = currency.upper()
    symbol = get_currency_symbol(currency)

    if isinstance(amount, Decimal):
        amount = float(amount)

    is_negative = amount < 0
    abs_amount = abs(amount)

    if abs_amount >= 1_000_000:
        formatted = f"{symbol}{abs_amount / 1_000_000:.1f}M"
    elif abs_amount >= 1_000:
        formatted = f"{symbol}{abs_amount / 1_000:.1f}K"
    else:
        formatted = f"{symbol}{abs_amount:,.0f}"

    if is_negative:
        formatted = f"-{formatted}"

    return formatted
