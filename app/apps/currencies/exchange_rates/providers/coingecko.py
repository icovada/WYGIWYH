import logging
import time

import requests
from decimal import Decimal
from typing import Tuple, List

from django.db.models import QuerySet

from apps.currencies.models import Currency
from apps.currencies.exchange_rates.base import ExchangeRateProvider

logger = logging.getLogger(__name__)


class CoinGeckoFreeProvider(ExchangeRateProvider):
    """Implementation for CoinGecko Free API"""

    BASE_URL = "https://api.coingecko.com/api/v3"
    rates_inverted = True

    def __init__(self, api_key: str):
        super().__init__(api_key)
        self.session = requests.Session()
        self.session.headers.update({"x-cg-demo-api-key": api_key})

    @classmethod
    def requires_api_key(cls) -> bool:
        return True

    def get_rates(
        self, target_currencies: QuerySet, exchange_currencies: set
    ) -> List[Tuple[Currency, Currency, Decimal]]:
        results = []
        all_currencies = set(currency.code.lower() for currency in target_currencies)
        all_currencies.update(currency.code.lower() for currency in exchange_currencies)

        try:
            response = self.session.get(
                f"{self.BASE_URL}/simple/price",
                params={
                    "ids": ",".join(all_currencies),
                    "vs_currencies": ",".join(all_currencies),
                },
            )
            response.raise_for_status()
            rates_data = response.json()

            for target_currency in target_currencies:
                if target_currency.exchange_currency in exchange_currencies:
                    try:
                        rate = Decimal(
                            str(
                                rates_data[target_currency.code.lower()][
                                    target_currency.exchange_currency.code.lower()
                                ]
                            )
                        )
                        # The rate is already inverted, so we don't need to invert it again
                        results.append(
                            (target_currency.exchange_currency, target_currency, rate)
                        )
                    except KeyError:
                        logger.error(
                            f"Rate not found for {target_currency.code} or {target_currency.exchange_currency.code}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Error calculating rate for {target_currency.code}: {e}"
                        )

            time.sleep(1)  # CoinGecko allows 10-30 calls/minute for free tier
        except requests.RequestException as e:
            logger.error(f"Error fetching rates from CoinGecko API: {e}")

        return results


class CoinGeckoProProvider(CoinGeckoFreeProvider):
    """Implementation for CoinGecko Pro API"""

    BASE_URL = "https://pro-api.coingecko.com/api/v3/simple/price"
    rates_inverted = True

    def __init__(self, api_key: str):
        super().__init__(api_key)
        self.session = requests.Session()
        self.session.headers.update({"x-cg-pro-api-key": api_key})