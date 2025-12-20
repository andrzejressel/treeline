"""Demo data provider for testing without real API calls."""

from datetime import datetime, timezone, timedelta, date
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Dict, List
from uuid import UUID, uuid4
import hashlib

from treeline.abstractions import DataAggregationProvider, IntegrationProvider
from treeline.domain import Account, BalanceSnapshot, Fail, Ok, Result, Transaction


class DemoDataProvider(DataAggregationProvider, IntegrationProvider):
    """Demo provider that returns fake data for testing and demonstrations.

    This provider can simulate any integration (SimpleFIN, Plaid, etc.) and returns
    realistic fake financial data without making any external API calls.
    """

    @property
    def can_get_accounts(self) -> bool:
        return True

    @property
    def can_get_transactions(self) -> bool:
        return True

    @property
    def can_get_balances(self) -> bool:
        return True

    def _generate_demo_accounts(self) -> List[Account]:
        """Generate realistic demo accounts."""
        now = datetime.now(timezone.utc)

        accounts = [
            # Primary Checking - main spending account
            Account(
                id=uuid4(),
                name="Primary Checking",
                nickname="Everyday Spending",
                account_type="checking",
                currency="USD",
                external_ids=MappingProxyType({"demo": "demo-checking-001"}),
                balance=Decimal("4823.47"),
                institution_name="Chase",
                institution_url="https://chase.com",
                institution_domain="chase.com",
                created_at=now,
                updated_at=now,
            ),
            # High-Yield Savings
            Account(
                id=uuid4(),
                name="High-Yield Savings",
                nickname="Emergency Fund",
                account_type="savings",
                currency="USD",
                external_ids=MappingProxyType({"demo": "demo-savings-001"}),
                balance=Decimal("18750.00"),
                institution_name="Marcus by Goldman Sachs",
                institution_url="https://marcus.com",
                institution_domain="marcus.com",
                created_at=now,
                updated_at=now,
            ),
            # Primary Credit Card
            Account(
                id=uuid4(),
                name="Sapphire Reserve",
                nickname="Travel Card",
                account_type="credit",
                currency="USD",
                external_ids=MappingProxyType({"demo": "demo-credit-001"}),
                balance=Decimal("-2847.63"),
                institution_name="Chase",
                institution_url="https://chase.com",
                institution_domain="chase.com",
                created_at=now,
                updated_at=now,
            ),
            # Cashback Credit Card
            Account(
                id=uuid4(),
                name="Citi Double Cash",
                nickname="Cashback Card",
                account_type="credit",
                currency="USD",
                external_ids=MappingProxyType({"demo": "demo-credit-002"}),
                balance=Decimal("-1245.89"),
                institution_name="Citi",
                institution_url="https://citi.com",
                institution_domain="citi.com",
                created_at=now,
                updated_at=now,
            ),
            # Investment Account
            Account(
                id=uuid4(),
                name="Individual Brokerage",
                nickname="Investments",
                account_type="investment",
                currency="USD",
                external_ids=MappingProxyType({"demo": "demo-investment-001"}),
                balance=Decimal("47823.15"),
                institution_name="Fidelity",
                institution_url="https://fidelity.com",
                institution_domain="fidelity.com",
                created_at=now,
                updated_at=now,
            ),
            # 401k
            Account(
                id=uuid4(),
                name="401(k)",
                nickname="Retirement",
                account_type="investment",
                currency="USD",
                external_ids=MappingProxyType({"demo": "demo-401k-001"}),
                balance=Decimal("89432.67"),
                institution_name="Fidelity",
                institution_url="https://fidelity.com",
                institution_domain="fidelity.com",
                created_at=now,
                updated_at=now,
            ),
        ]

        return accounts

    def _deterministic_day(self, seed: str, max_day: int) -> int:
        """Generate a deterministic day based on seed."""
        h = int(hashlib.md5(seed.encode()).hexdigest()[:8], 16)
        return (h % max_day) + 1

    def _generate_demo_transactions(
        self, start_date: datetime, end_date: datetime, account_ids: List[str]
    ) -> List[tuple[str, Transaction]]:
        """Generate realistic demo transactions within date range."""
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=180)
        if not end_date:
            end_date = datetime.now(timezone.utc)

        transactions = []
        now = datetime.now(timezone.utc)
        tx_counter = 0

        # =========================================
        # RECURRING MONTHLY TRANSACTIONS
        # =========================================
        monthly_recurring = [
            # Income (tagged for budget tracking)
            ("demo-checking-001", "Employer Direct Deposit - Payroll", Decimal("4250.00"), ["income", "salary"], 1),
            ("demo-checking-001", "Employer Direct Deposit - Payroll", Decimal("4250.00"), ["income", "salary"], 15),

            # Rent/Mortgage
            ("demo-checking-001", "Online Payment - Rent", Decimal("-2100.00"), ["housing", "rent"], 1),

            # Utilities
            ("demo-checking-001", "PG&E - Electricity", Decimal("-142.87"), ["utilities"], 8),
            ("demo-checking-001", "Comcast Internet", Decimal("-79.99"), ["utilities", "internet"], 12),
            ("demo-checking-001", "T-Mobile", Decimal("-85.00"), ["utilities", "phone"], 18),

            # Insurance
            ("demo-checking-001", "State Farm Auto Insurance", Decimal("-156.00"), ["insurance", "auto"], 5),
            ("demo-checking-001", "Kaiser Health Insurance", Decimal("-320.00"), ["insurance", "health"], 1),

            # Subscriptions
            ("demo-credit-001", "Netflix", Decimal("-15.99"), ["subscriptions", "entertainment"], 7),
            ("demo-credit-001", "Spotify Premium", Decimal("-10.99"), ["subscriptions", "entertainment"], 7),
            ("demo-credit-001", "NYTimes Digital", Decimal("-17.00"), ["subscriptions"], 14),
            ("demo-credit-001", "iCloud Storage", Decimal("-2.99"), ["subscriptions"], 21),
            ("demo-credit-002", "Amazon Prime", Decimal("-14.99"), ["subscriptions"], 3),
            ("demo-credit-002", "YouTube Premium", Decimal("-13.99"), ["subscriptions", "entertainment"], 11),
            ("demo-checking-001", "Planet Fitness", Decimal("-24.99"), ["subscriptions", "fitness"], 17),

            # Savings & Investments
            ("demo-savings-001", "Transfer from Checking", Decimal("750.00"), ["transfer"], 16),
            ("demo-savings-001", "Interest Payment", Decimal("78.23"), [], 28),
            ("demo-investment-001", "Transfer from Checking", Decimal("500.00"), ["transfer", "investing"], 16),
            ("demo-401k-001", "Employer 401k Contribution", Decimal("850.00"), ["investing"], 1),
            ("demo-401k-001", "Employer 401k Contribution", Decimal("850.00"), ["investing"], 15),
            ("demo-401k-001", "Employer Match", Decimal("425.00"), ["investing"], 1),
            ("demo-401k-001", "Employer Match", Decimal("425.00"), ["investing"], 15),

            # Credit Card Payments
            ("demo-credit-001", "Payment Thank You - Web", Decimal("2500.00"), ["payment"], 25),
            ("demo-credit-002", "Online Payment - Thank You", Decimal("1200.00"), ["payment"], 20),
        ]

        # =========================================
        # VARIABLE SPENDING PATTERNS
        # =========================================

        # Groceries - weekly pattern
        grocery_stores = [
            ("demo-credit-002", "Whole Foods Market", Decimal("-127.43"), ["groceries", "food"]),
            ("demo-credit-002", "Trader Joe's", Decimal("-68.92"), ["groceries", "food"]),
            ("demo-credit-002", "Safeway", Decimal("-94.56"), ["groceries", "food"]),
            ("demo-credit-002", "Costco", Decimal("-215.87"), ["groceries", "food"]),
            ("demo-credit-002", "Target", Decimal("-78.34"), ["groceries", "shopping"]),
        ]

        # Coffee - frequent
        coffee_shops = [
            ("demo-credit-001", "Starbucks", Decimal("-6.45"), ["coffee", "food"]),
            ("demo-credit-001", "Starbucks", Decimal("-5.75"), ["coffee", "food"]),
            ("demo-credit-002", "Blue Bottle Coffee", Decimal("-7.50"), ["coffee", "food"]),
            ("demo-credit-001", "Philz Coffee", Decimal("-6.25"), ["coffee", "food"]),
        ]

        # Dining out - 2-3x per week
        restaurants = [
            ("demo-credit-001", "Sweetgreen", Decimal("-16.87"), ["dining", "food", "lunch"]),
            ("demo-credit-001", "Chipotle", Decimal("-14.25"), ["dining", "food", "lunch"]),
            ("demo-credit-001", "Panera Bread", Decimal("-12.48"), ["dining", "food", "lunch"]),
            ("demo-credit-001", "The Cheesecake Factory", Decimal("-78.45"), ["dining", "food"]),
            ("demo-credit-001", "Olive Garden", Decimal("-62.30"), ["dining", "food"]),
            ("demo-credit-001", "Local Thai Kitchen", Decimal("-45.00"), ["dining", "food"]),
            ("demo-credit-001", "Sushi Masa", Decimal("-89.50"), ["dining", "food"]),
            ("demo-credit-002", "McDonald's", Decimal("-12.43"), ["dining", "food", "fast-food"]),
            ("demo-credit-002", "Chick-fil-A", Decimal("-14.87"), ["dining", "food", "fast-food"]),
            ("demo-credit-001", "DoorDash", Decimal("-34.56"), ["dining", "food", "delivery"]),
            ("demo-credit-001", "Uber Eats", Decimal("-28.90"), ["dining", "food", "delivery"]),
        ]

        # Transportation
        transportation = [
            ("demo-credit-002", "Shell", Decimal("-58.43"), ["transportation", "gas"]),
            ("demo-credit-002", "Chevron", Decimal("-52.17"), ["transportation", "gas"]),
            ("demo-credit-001", "Uber", Decimal("-24.50"), ["transportation", "rideshare"]),
            ("demo-credit-001", "Lyft", Decimal("-18.75"), ["transportation", "rideshare"]),
            ("demo-checking-001", "BART", Decimal("-6.20"), ["transportation", "transit"]),
        ]

        # Shopping
        shopping = [
            ("demo-credit-001", "Amazon.com", Decimal("-47.89"), ["shopping"]),
            ("demo-credit-001", "Amazon.com", Decimal("-124.99"), ["shopping"]),
            ("demo-credit-001", "Amazon.com", Decimal("-23.45"), ["shopping"]),
            ("demo-credit-002", "Target", Decimal("-67.82"), ["shopping"]),
            ("demo-credit-002", "Walmart", Decimal("-45.23"), ["shopping"]),
            ("demo-credit-001", "Best Buy", Decimal("-199.99"), ["shopping", "electronics"]),
            ("demo-credit-001", "Apple Store", Decimal("-49.00"), ["shopping", "electronics"]),
            ("demo-credit-002", "Home Depot", Decimal("-87.43"), ["shopping", "home"]),
            ("demo-credit-002", "IKEA", Decimal("-234.56"), ["shopping", "home"]),
            ("demo-credit-001", "Nordstrom", Decimal("-156.78"), ["shopping", "clothing"]),
            ("demo-credit-001", "Uniqlo", Decimal("-89.97"), ["shopping", "clothing"]),
        ]

        # Health & Wellness
        health = [
            ("demo-credit-002", "CVS Pharmacy", Decimal("-34.56"), ["health", "pharmacy"]),
            ("demo-credit-002", "Walgreens", Decimal("-28.90"), ["health", "pharmacy"]),
            ("demo-checking-001", "Kaiser Pharmacy", Decimal("-15.00"), ["health", "pharmacy"]),
            ("demo-credit-001", "ClassPass", Decimal("-49.00"), ["fitness"]),
        ]

        # Entertainment
        entertainment = [
            ("demo-credit-001", "AMC Theatres", Decimal("-32.50"), ["entertainment"]),
            ("demo-credit-001", "Eventbrite", Decimal("-75.00"), ["entertainment", "events"]),
            ("demo-credit-001", "Steam", Decimal("-29.99"), ["entertainment", "gaming"]),
        ]

        # Travel (occasional)
        travel = [
            ("demo-credit-001", "United Airlines", Decimal("-387.00"), ["travel", "flights"]),
            ("demo-credit-001", "Delta Airlines", Decimal("-452.00"), ["travel", "flights"]),
            ("demo-credit-001", "Marriott Hotels", Decimal("-245.87"), ["travel", "hotels"]),
            ("demo-credit-001", "Airbnb", Decimal("-312.45"), ["travel", "lodging"]),
            ("demo-credit-001", "Enterprise Rent-A-Car", Decimal("-156.78"), ["travel", "car-rental"]),
        ]

        # Personal Care
        personal = [
            ("demo-credit-002", "Supercuts", Decimal("-28.00"), ["personal"]),
            ("demo-credit-001", "Sephora", Decimal("-67.45"), ["personal", "shopping"]),
        ]

        # =========================================
        # GENERATE RECURRING TRANSACTIONS
        # =========================================
        current = start_date
        while current <= end_date:
            year, month = current.year, current.month
            days_in_month = (date(year, month + 1, 1) if month < 12 else date(year + 1, 1, 1)) - date(year, month, 1)

            for account_id, description, amount, tags, day_of_month in monthly_recurring:
                if account_ids and account_id not in account_ids:
                    continue

                actual_day = min(day_of_month, days_in_month.days)
                tx_date = date(year, month, actual_day)
                tx_datetime = datetime.combine(tx_date, datetime.min.time()).replace(tzinfo=timezone.utc)

                if tx_datetime < start_date or tx_datetime > end_date:
                    continue

                transaction = Transaction(
                    id=uuid4(),
                    account_id=UUID(int=0),
                    external_ids=MappingProxyType({"demo": f"demo-tx-{tx_counter:04d}"}),
                    amount=amount,
                    description=description,
                    transaction_date=tx_date,
                    posted_date=tx_date,
                    tags=tuple(tags),
                    created_at=now,
                    updated_at=now,
                )
                transactions.append((account_id, transaction))
                tx_counter += 1

            if month == 12:
                current = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                current = datetime(year, month + 1, 1, tzinfo=timezone.utc)

        # =========================================
        # GENERATE VARIABLE TRANSACTIONS
        # =========================================
        days_in_range = max((end_date - start_date).days, 1)

        # Helper to add transactions with frequency
        def add_variable_txns(templates: list, frequency_days: int, variance: int = 2):
            nonlocal tx_counter
            for week_num in range(days_in_range // frequency_days + 1):
                for i, (account_id, desc, amount, tags) in enumerate(templates):
                    if account_ids and account_id not in account_ids:
                        continue

                    # Deterministic but varied spacing
                    seed = f"{desc}-{week_num}-{i}"
                    offset = self._deterministic_day(seed, frequency_days)
                    base_day = week_num * frequency_days + offset

                    if base_day >= days_in_range:
                        continue

                    tx_date = (start_date + timedelta(days=base_day)).date()
                    if tx_date > end_date.date():
                        continue

                    # Slight amount variance
                    amount_variance = Decimal(str(self._deterministic_day(seed + "amt", 20) - 10)) / Decimal("100")
                    final_amount = amount * (Decimal("1") + amount_variance / Decimal("10"))
                    final_amount = final_amount.quantize(Decimal("0.01"))

                    transaction = Transaction(
                        id=uuid4(),
                        account_id=UUID(int=0),
                        external_ids=MappingProxyType({"demo": f"demo-tx-{tx_counter:04d}"}),
                        amount=final_amount,
                        description=desc,
                        transaction_date=tx_date,
                        posted_date=tx_date,
                        tags=tuple(tags),
                        created_at=now,
                        updated_at=now,
                    )
                    transactions.append((account_id, transaction))
                    tx_counter += 1

        # Apply frequencies
        add_variable_txns(grocery_stores, 7)      # Weekly groceries
        add_variable_txns(coffee_shops, 3)         # Coffee every ~3 days
        add_variable_txns(restaurants[:6], 5)      # Dining out frequently
        add_variable_txns(restaurants[6:], 10)     # Less frequent dining
        add_variable_txns(transportation[:2], 14)  # Gas bi-weekly
        add_variable_txns(transportation[2:], 10)  # Rideshare/transit
        add_variable_txns(shopping[:5], 14)        # Regular shopping
        add_variable_txns(shopping[5:], 30)        # Occasional shopping
        add_variable_txns(health, 21)              # Health monthly-ish
        add_variable_txns(entertainment, 21)       # Entertainment
        add_variable_txns(personal, 28)            # Personal care monthly

        # Travel is rare - only add a couple trips
        if days_in_range > 60:
            for i, (account_id, desc, amount, tags) in enumerate(travel[:3]):
                if account_ids and account_id not in account_ids:
                    continue
                offset = self._deterministic_day(f"travel-{i}", days_in_range - 30) + 15
                tx_date = (start_date + timedelta(days=offset)).date()

                transaction = Transaction(
                    id=uuid4(),
                    account_id=UUID(int=0),
                    external_ids=MappingProxyType({"demo": f"demo-tx-{tx_counter:04d}"}),
                    amount=amount,
                    description=desc,
                    transaction_date=tx_date,
                    posted_date=tx_date,
                    tags=tuple(tags),
                    created_at=now,
                    updated_at=now,
                )
                transactions.append((account_id, transaction))
                tx_counter += 1

        return transactions

    async def get_accounts(
        self,
        provider_account_ids: List[str] = [],
        provider_settings: Dict[str, Any] = {},
    ) -> Result[List[Account]]:
        """Get demo accounts."""
        accounts = self._generate_demo_accounts()

        if provider_account_ids:
            accounts = [
                acc
                for acc in accounts
                if acc.external_ids.get("demo") in provider_account_ids
            ]

        return Ok(accounts)

    async def get_transactions(
        self,
        start_date: datetime,
        end_date: datetime,
        provider_account_ids: List[str] = [],
        provider_settings: Dict[str, Any] = {},
    ) -> Result[List[tuple[str, Transaction]]]:
        """Get demo transactions."""
        transactions = self._generate_demo_transactions(
            start_date, end_date, provider_account_ids
        )

        return Ok(transactions)

    async def get_balances(
        self,
        provider_account_ids: List[str] = [],
        provider_settings: Dict[str, Any] = {},
    ) -> Result[List[BalanceSnapshot]]:
        """Get demo balance snapshots.

        NOTE: This method is deprecated. Balances are now returned as part of the
        Account model in get_accounts() and balance snapshots are created automatically
        by the sync service.
        """
        return Fail("get_balances is deprecated - balances are synced via get_accounts")

    async def create_integration(
        self, integration_name: str, integration_options: Dict[str, Any]
    ) -> Result[Dict[str, str]]:
        """Create demo integration (no real credentials needed)."""
        return Ok(
            {
                "accessUrl": "https://demo-provider.example.com/access/demo-user",
                "demo": "true",
            }
        )

    def generate_demo_budget_sql(self) -> str:
        """Generate SQL to seed demo budget categories for multiple months.

        Creates a realistic budget that shows off the app's capabilities:
        - Mix of under/on/over budget categories
        - Income tracking
        - Various expense categories with appropriate tags
        """
        now = datetime.now(timezone.utc)

        # Generate for current month and 5 previous months
        months = []
        for i in range(6):
            dt = now - timedelta(days=i * 30)
            months.append(dt.strftime("%Y-%m"))

        sql_statements = []

        # Clear existing demo budget data
        sql_statements.append("DELETE FROM sys_plugin_budget_categories;")
        sql_statements.append("DELETE FROM sys_plugin_budget_rollovers;")

        # Budget categories - designed to create visually appealing results
        # Income categories (match payroll transactions: $4250 x 2 = $8500/month)
        # Expense categories with tags that match demo transactions
        # Budget amounts calibrated against actual demo transaction generation:
        # - Some categories under budget (green) - good financial habits
        # - Some at budget (yellow) - need to watch spending
        # - Some over budget (red) - areas to improve
        categories = [
            # Income (tags match payroll transactions)
            ("income", "Salary", 8500, ["salary"], False, "positive"),
            # Expenses - calibrated for realistic mix of green/yellow/red
            ("expense", "Housing", 2100, ["housing", "rent"], False, "negative"),  # ~100% (on budget)
            ("expense", "Groceries", 2500, ["groceries"], False, "negative"),  # ~85% (green)
            ("expense", "Dining", 1500, ["dining"], False, "negative"),  # ~120% (red - overspending)
            ("expense", "Transportation", 500, ["transportation"], False, "negative"),  # ~70% (green)
            ("expense", "Shopping", 1500, ["shopping"], False, "negative"),  # ~125% (red - overspending)
            ("expense", "Entertainment", 250, ["entertainment", "subscriptions"], False, "negative"),  # ~130% (red)
            ("expense", "Utilities", 350, ["utilities"], False, "negative"),  # ~85% (green)
            ("expense", "Health", 300, ["health", "fitness"], False, "negative"),  # ~165% (red - need insurance?)
            ("expense", "Insurance", 500, ["insurance"], False, "negative"),  # ~95% (green)
        ]

        for month in months:
            for i, (cat_type, name, expected, tags, require_all, amount_sign) in enumerate(categories):
                cat_id = str(uuid4())
                tags_sql = "[" + ", ".join(f"'{t}'" for t in tags) + "]" if tags else "[]"
                amount_sign_sql = f"'{amount_sign}'" if amount_sign else "NULL"

                sql_statements.append(f"""
                    INSERT INTO sys_plugin_budget_categories
                        (category_id, month, type, name, expected, tags, require_all, amount_sign, sort_order)
                    VALUES
                        ('{cat_id}', '{month}', '{cat_type}', '{name}', {expected}, {tags_sql}, {require_all}, {amount_sign_sql}, {i});
                """)

        # =========================================
        # ROLLOVERS - demonstrate the rollover feature
        # =========================================
        # Create realistic rollovers based on actual spending patterns:
        # Transportation budget: $500, actual spend: ~$360 → ~$140 leftover
        # Groceries budget: $2500, actual spend: ~$2200 → ~$300 leftover
        # Utilities budget: $350, actual spend: ~$310 → ~$40 leftover
        rollovers = [
            # From October: rolled over $125 leftover from Transportation to November
            ("2025-10", "Transportation", "Transportation", "2025-11", 125),
            # From October: moved $250 from underspent Groceries to Dining (holiday prep)
            ("2025-10", "Groceries", "Dining", "2025-11", 250),
            # From November: rolled over $130 from Transportation to December
            ("2025-11", "Transportation", "Transportation", "2025-12", 130),
            # From November: moved $35 from Utilities savings to Entertainment (holiday fun)
            ("2025-11", "Utilities", "Entertainment", "2025-12", 35),
        ]

        for source_month, from_cat, to_cat, to_month, amount in rollovers:
            rollover_id = str(uuid4())
            sql_statements.append(f"""
                INSERT INTO sys_plugin_budget_rollovers
                    (rollover_id, source_month, from_category, to_category, to_month, amount)
                VALUES
                    ('{rollover_id}', '{source_month}', '{from_cat}', '{to_cat}', '{to_month}', {amount});
            """)

        return "\n".join(sql_statements)

    def generate_demo_balance_history_sql(self, account_id_map: dict[str, str]) -> str:
        """Generate SQL to create interesting historical balance snapshots.

        Creates realistic balance history with:
        - Investment accounts: Market fluctuations with overall upward trend
        - Checking/Savings: Gradual savings growth with some variation
        - Credit cards: Monthly cycles (spending up, payment down)

        Args:
            account_id_map: Dict mapping demo external IDs to actual UUIDs
                           e.g. {"demo-checking-001": "uuid-string", ...}

        Returns:
            SQL statements to insert balance snapshots
        """
        import math
        import random

        # Use a fixed seed for reproducibility in demo
        random.seed(42)

        now = datetime.now(timezone.utc)
        sql_statements = []

        # First, clear existing snapshots for demo accounts
        for demo_id, account_uuid in account_id_map.items():
            sql_statements.append(
                f"DELETE FROM sys_balance_snapshots WHERE account_id = '{account_uuid}';"
            )

        # Generate 180 days of history (6 months)
        days_of_history = 180

        # Account configurations: (demo_id, current_balance, growth_type, volatility)
        # growth_type: "market" (investment), "savings" (gradual growth), "credit" (monthly cycle)
        account_configs = {
            "demo-checking-001": (Decimal("4823.47"), "checking", 0.02),
            "demo-savings-001": (Decimal("18750.00"), "savings", 0.01),
            "demo-credit-001": (Decimal("-2847.63"), "credit", 0.15),
            "demo-credit-002": (Decimal("-1245.89"), "credit", 0.12),
            "demo-investment-001": (Decimal("47823.15"), "market", 0.08),
            "demo-401k-001": (Decimal("89432.67"), "market", 0.06),
        }

        for demo_id, account_uuid in account_id_map.items():
            if demo_id not in account_configs:
                continue

            current_balance, growth_type, volatility = account_configs[demo_id]

            # Generate daily balances going backward
            balances = []
            balance = float(current_balance)

            for day in range(days_of_history):
                # Calculate date for this snapshot
                snapshot_date = (now - timedelta(days=day)).date()

                if growth_type == "market":
                    # Investment: Market fluctuations with upward trend
                    # Working backward: today's balance = yesterday's * growth
                    # So yesterday's = today's / growth
                    # We want overall ~8-12% annual growth + daily noise

                    # Daily average growth (reverse): slightly less in the past
                    daily_growth = 1.0 + (0.10 / 365)  # ~10% annual

                    # Add market volatility (sine wave for cycles + random noise)
                    cycle = math.sin(day * 2 * math.pi / 60) * 0.02  # 60-day cycles
                    noise = (random.random() - 0.5) * volatility * 0.3

                    # Apply variation (working backward)
                    daily_factor = daily_growth + cycle + noise
                    balance = balance / daily_factor

                    # Add occasional larger moves (earnings, market events)
                    if random.random() < 0.03:  # 3% chance of big move
                        balance *= 1 + (random.random() - 0.5) * 0.05

                elif growth_type == "savings":
                    # Savings: Gradual growth with monthly deposits + interest
                    # Working backward, so subtract growth

                    # Monthly contribution pattern (higher earlier in past = balance was lower)
                    day_of_month = snapshot_date.day
                    if day_of_month == 16:  # Transfer day
                        balance = balance - 750  # Remove the transfer going backward

                    # Interest (roughly 4% APY)
                    if day_of_month == 28:
                        # Remove ~1 month of interest going backward
                        monthly_interest = balance * (0.04 / 12)
                        balance = balance - monthly_interest

                    # Small random variation
                    balance *= 1 + (random.random() - 0.5) * 0.002

                elif growth_type == "credit":
                    # Credit card: Monthly cycle - builds up, then gets paid
                    day_of_month = snapshot_date.day

                    if demo_id == "demo-credit-001":
                        # Chase Sapphire: paid on 25th, builds up rest of month
                        if day_of_month == 25:
                            # After payment, balance is low
                            balance = float(current_balance) * 0.15
                        elif day_of_month < 25:
                            # Building up spending
                            days_since_payment = (day_of_month - 25) % 30
                            progress = days_since_payment / 25
                            balance = float(current_balance) * (0.15 + 0.85 * progress)
                        else:
                            # Days after payment, building back up
                            progress = (day_of_month - 25) / 5
                            balance = float(current_balance) * (0.15 + 0.3 * progress)
                    else:
                        # Citi: paid on 20th
                        if day_of_month == 20:
                            balance = float(current_balance) * 0.10
                        elif day_of_month < 20:
                            progress = day_of_month / 20
                            balance = float(current_balance) * (0.10 + 0.90 * progress)
                        else:
                            progress = (day_of_month - 20) / 10
                            balance = float(current_balance) * (0.10 + 0.35 * progress)

                    # Add random spending variation
                    balance *= 1 + (random.random() - 0.5) * volatility

                elif growth_type == "checking":
                    # Checking: Paycheck cycles + bill payments
                    day_of_month = snapshot_date.day

                    # Paycheck on 1st and 15th brings balance up
                    # Bills throughout month bring it down
                    if day_of_month in [1, 15]:
                        balance = float(current_balance) * 1.8  # After paycheck
                    elif day_of_month in [2, 16]:
                        balance = float(current_balance) * 1.6  # Day after paycheck
                    elif day_of_month == 5:
                        balance = float(current_balance) * 0.7  # After rent
                    else:
                        # Random variation based on spending
                        base_ratio = 0.8 + (random.random() * 0.6)
                        balance = float(current_balance) * base_ratio

                # Store the balance for this day
                balances.append((snapshot_date, Decimal(str(round(balance, 2)))))

            # Generate SQL for all snapshots
            for snapshot_date, bal in balances:
                snapshot_id = str(uuid4())
                snapshot_time = f"{snapshot_date}T23:59:59"
                created_at = now.isoformat()

                sql_statements.append(f"""
                    INSERT INTO sys_balance_snapshots
                        (snapshot_id, account_id, balance, snapshot_time, created_at, source)
                    VALUES
                        ('{snapshot_id}', '{account_uuid}', {bal}, '{snapshot_time}', '{created_at}', 'sync');
                """)

        return "\n".join(sql_statements)
