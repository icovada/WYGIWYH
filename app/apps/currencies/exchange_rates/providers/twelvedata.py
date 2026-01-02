import logging
import time

import requests
from decimal import Decimal
from typing import Tuple, List

from django.db.models import QuerySet

from apps.currencies.models import Currency
from apps.currencies.exchange_rates.base import ExchangeRateProvider

logger = logging.getLogger(__name__)


class TwelveDataProvider(ExchangeRateProvider):
    """Implementation for the Twelve Data API (twelvedata.com)"""

    BASE_URL = "https://api.twelvedata.com/exchange_rate"
    rates_inverted = (
        False  # The API returns direct rates, e.g., for EUR/USD it's 1 EUR = X USD
    )

    def __init__(self, api_key: str):
        """
        Initializes the provider with an API key and a requests session.
        """
        super().__init__(api_key)
        self.session = requests.Session()

    @classmethod
    def requires_api_key(cls) -> bool:
        """This provider requires an API key."""
        return True

    def get_rates(
        self, target_currencies: QuerySet, exchange_currencies: set
    ) -> List[Tuple[Currency, Currency, Decimal]]:
        """
        Fetches exchange rates from the Twelve Data API for the given currency pairs.

        This provider makes one API call for each requested currency pair.
        """
        results = []

        for target_currency in target_currencies:
            # Ensure the target currency's exchange currency is one we're interested in
            if target_currency.exchange_currency not in exchange_currencies:
                continue

            base_currency = target_currency.exchange_currency

            # The exchange rate for the same currency is always 1
            if base_currency.code == target_currency.code:
                rate = Decimal("1")
                results.append((base_currency, target_currency, rate))
                continue

            # Construct the symbol in the format "BASE/TARGET", e.g., "EUR/USD"
            symbol = f"{base_currency.code}/{target_currency.code}"

            try:
                params = {
                    "symbol": symbol,
                    "apikey": self.api_key,
                }

                response = self.session.get(self.BASE_URL, params=params)
                response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

                data = response.json()

                # The API may return an error message in a JSON object
                if "rate" not in data:
                    error_message = data.get("message", "Rate not found in response.")
                    logger.error(
                        f"Could not fetch rate for {symbol} from Twelve Data: {error_message}"
                    )
                    continue

                # Convert the rate to a Decimal for precision
                rate = Decimal(str(data["rate"]))
                results.append((base_currency, target_currency, rate))

                logger.info(f"Successfully fetched rate for {symbol} from Twelve Data.")

                time.sleep(
                    60
                )  # We sleep every pair as to not step over TwelveData's minute limit

            except requests.RequestException as e:
                logger.error(
                    f"Error fetching rate from Twelve Data API for symbol {symbol}: {e}"
                )
            except KeyError as e:
                logger.error(
                    f"Unexpected response structure from Twelve Data API for symbol {symbol}: Missing key {e}"
                )
            except Exception as e:
                logger.error(
                    f"An unexpected error occurred while processing Twelve Data for {symbol}: {e}"
                )

        return results


class TwelveDataMarketsProvider(ExchangeRateProvider):
    """
    Provides prices for market instruments (stocks, ETFs, etc.) using the Twelve Data API.

    This provider performs a multi-step process:
    1. Parses instrument codes which can be symbols, FIGI, CUSIP, or ISIN.
    2. For CUSIPs, it defaults the currency to USD. For all others, it searches
       for the instrument to determine its native trading currency.
    3. Fetches the latest price for the instrument in its native currency.
    4. Converts the price to the requested target exchange currency.
    """

    SYMBOL_SEARCH_URL = "https://api.twelvedata.com/symbol_search"
    PRICE_URL = "https://api.twelvedata.com/price"
    EXCHANGE_RATE_URL = "https://api.twelvedata.com/exchange_rate"

    rates_inverted = True

    def __init__(self, api_key: str):
        super().__init__(api_key)
        self.session = requests.Session()

    @classmethod
    def requires_api_key(cls) -> bool:
        return True

    def _parse_code(self, raw_code: str) -> Tuple[str, str]:
        """Parses the raw code to determine its type and value."""
        if raw_code.startswith("figi:"):
            return "figi", raw_code.removeprefix("figi:")
        if raw_code.startswith("cusip:"):
            return "cusip", raw_code.removeprefix("cusip:")
        if raw_code.startswith("isin:"):
            return "isin", raw_code.removeprefix("isin:")
        return "symbol", raw_code

    def get_rates(
        self, target_currencies: QuerySet, exchange_currencies: set
    ) -> List[Tuple[Currency, Currency, Decimal]]:
        results = []

        for asset in target_currencies:
            if asset.exchange_currency not in exchange_currencies:
                continue

            code_type, code_value = self._parse_code(asset.code)
            original_currency_code = None

            try:
                # Determine the instrument's native currency
                if code_type == "cusip":
                    # CUSIP codes always default to USD
                    original_currency_code = "USD"
                    logger.info(f"Defaulting CUSIP {code_value} to USD currency.")
                else:
                    # For all other types, find currency via symbol search
                    search_params = {"symbol": code_value, "apikey": "demo"}
                    search_res = self.session.get(
                        self.SYMBOL_SEARCH_URL, params=search_params
                    )
                    search_res.raise_for_status()
                    search_data = search_res.json()

                    if not search_data.get("data"):
                        logger.warning(
                            f"TwelveDataMarkets: Symbol search for '{code_value}' returned no results."
                        )
                        continue

                    instrument_data = search_data["data"][0]
                    original_currency_code = instrument_data.get("currency")

                if not original_currency_code:
                    logger.error(
                        f"TwelveDataMarkets: Could not determine original currency for '{code_value}'."
                    )
                    continue

                # Get the instrument's price in its native currency
                price_params = {code_type: code_value, "apikey": self.api_key}
                price_res = self.session.get(self.PRICE_URL, params=price_params)
                price_res.raise_for_status()
                price_data = price_res.json()

                if "price" not in price_data:
                    error_message = price_data.get(
                        "message", "Price key not found in response"
                    )
                    logger.error(
                        f"TwelveDataMarkets: Could not get price for {code_type} '{code_value}': {error_message}"
                    )
                    continue

                price_in_original_currency = Decimal(price_data["price"])

                # Convert price to the target exchange currency
                target_exchange_currency = asset.exchange_currency

                if (
                    original_currency_code.upper()
                    == target_exchange_currency.code.upper()
                ):
                    final_price = price_in_original_currency
                else:
                    rate_symbol = (
                        f"{original_currency_code}/{target_exchange_currency.code}"
                    )
                    rate_params = {"symbol": rate_symbol, "apikey": self.api_key}
                    rate_res = self.session.get(
                        self.EXCHANGE_RATE_URL, params=rate_params
                    )
                    rate_res.raise_for_status()
                    rate_data = rate_res.json()

                    if "rate" not in rate_data:
                        error_message = rate_data.get(
                            "message", "Rate key not found in response"
                        )
                        logger.error(
                            f"TwelveDataMarkets: Could not get conversion rate for '{rate_symbol}': {error_message}"
                        )
                        continue

                    conversion_rate = Decimal(str(rate_data["rate"]))
                    final_price = price_in_original_currency * conversion_rate

                results.append((target_exchange_currency, asset, final_price))
                logger.info(
                    f"Successfully processed price for {asset.code} as {final_price} {target_exchange_currency.code}"
                )

                time.sleep(
                    60
                )  # We sleep every pair as to not step over TwelveData's minute limit

            except requests.RequestException as e:
                logger.error(
                    f"TwelveDataMarkets: API request failed for {code_value}: {e}"
                )
            except (KeyError, IndexError) as e:
                logger.error(
                    f"TwelveDataMarkets: Error processing API response for {code_value}: {e}"
                )
            except Exception as e:
                logger.error(
                    f"TwelveDataMarkets: An unexpected error occurred for {code_value}: {e}"
                )

        return results
