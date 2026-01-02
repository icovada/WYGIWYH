import logging

import requests
from decimal import Decimal
from typing import Tuple, List

from django.db.models import QuerySet

from apps.currencies.models import Currency
from apps.currencies.exchange_rates.base import ExchangeRateProvider

logger = logging.getLogger(__name__)


class FrankfurterProvider(ExchangeRateProvider):
    """Implementation for the Frankfurter API (frankfurter.dev)"""

    BASE_URL = "https://api.frankfurter.dev/v1/latest"
    rates_inverted = (
        False  # Frankfurter returns non-inverted rates (e.g., 1 EUR = 1.1 USD)
    )

    def __init__(self, api_key: str = None):
        """
        Initializes the provider. The Frankfurter API does not require an API key,
        so the api_key parameter is ignored.
        """
        super().__init__(api_key)
        self.session = requests.Session()

    @classmethod
    def requires_api_key(cls) -> bool:
        return False

    def get_rates(
        self, target_currencies: QuerySet, exchange_currencies: set
    ) -> List[Tuple[Currency, Currency, Decimal]]:
        results = []
        currency_groups = {}
        # Group target currencies by their exchange (base) currency to minimize API calls
        for currency in target_currencies:
            if currency.exchange_currency in exchange_currencies:
                group = currency_groups.setdefault(currency.exchange_currency.code, [])
                group.append(currency)

        # Make one API call for each base currency
        for base_currency, currencies in currency_groups.items():
            try:
                # Create a comma-separated list of target currency codes
                to_currencies = ",".join(
                    currency.code
                    for currency in currencies
                    if currency.code != base_currency
                )

                # If there are no target currencies other than the base, skip the API call
                if not to_currencies:
                    # Handle the case where the only request is for the base rate (e.g., USD to USD)
                    for currency in currencies:
                        if currency.code == base_currency:
                            results.append(
                                (currency.exchange_currency, currency, Decimal("1"))
                            )
                    continue

                response = self.session.get(
                    self.BASE_URL,
                    params={"base": base_currency, "symbols": to_currencies},
                )
                response.raise_for_status()
                data = response.json()
                rates = data["rates"]

                # Process the returned rates
                for currency in currencies:
                    if currency.code == base_currency:
                        # The rate for the base currency to itself is always 1
                        rate = Decimal("1")
                    else:
                        rate = Decimal(str(rates[currency.code]))

                    results.append((currency.exchange_currency, currency, rate))

            except requests.RequestException as e:
                logger.error(
                    f"Error fetching rates from Frankfurter API for base {base_currency}: {e}"
                )
            except KeyError as e:
                logger.error(
                    f"Unexpected response structure from Frankfurter API for base {base_currency}: {e}"
                )
            except Exception as e:
                logger.error(
                    f"Unexpected error processing Frankfurter data for base {base_currency}: {e}"
                )
        return results
