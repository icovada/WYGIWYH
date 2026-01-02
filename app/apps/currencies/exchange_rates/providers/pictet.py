import logging
from datetime import datetime as dt

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.exceptions import TransportQueryError
from decimal import Decimal
from typing import Tuple, List

from django.db.models import QuerySet

from apps.currencies.models import Currency
from apps.currencies.exchange_rates.base import ExchangeRateProvider

logger = logging.getLogger(__name__)


class PictetProvider(ExchangeRateProvider):
    """Implementation for Pictet graphql API"""

    rates_inverted = True

    def __init__(self, api_key: str):
        super().__init__(api_key)
        transport = RequestsHTTPTransport(
            url="https://am.pictet.com/api/gateways/publiccms-gateway/graphql",
            use_json = True,
                headers={
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:146.0) Gecko/20100101 Firefox/146.0',
                'Accept': '*/*',
                'Accept-Language': 'en-US,it;q=0.8,en;q=0.5,it-IT;q=0.3',
                'Referer': 'https://am.pictet.com/',
                'Content-Type': 'application/json',
                'Origin': 'https://am.pictet.com',
                'DNT': '1',
                'Sec-GPC': '1',
            },
        )
        self.client = Client(transport=transport)


        self.query = gql("""
            query fundChart(
                $context: UserContext!
                $isin: String!
                $relatedIsins: String
                $startDate: LocalDate
                $endDate: LocalDate
                $frequency: Frequency!
                $currencyToIso: String
            ) {
                fund(context: $context, isin: $isin, relatedIsins: $relatedIsins) {
                    currency {
                        isoCode
                    }
                    chart(
                        startDate: $startDate
                        endDate: $endDate
                        frequency: $frequency
                        currencyToIso: $currencyToIso
                    ) {
                        values {
                            pricingDate
                            fundAdjustedNav
                        }
                        currencyIn
                        currencyOut
                        frequency
                        dateRange {
                            beginDate
                            endDate
                        }
                    }
                }
            }
        """)

    @classmethod
    def requires_api_key(cls) -> bool:
        return False

    def get_rates(
        self, target_currencies: QuerySet, exchange_currencies: set
    ) -> List[Tuple[Currency, Currency, Decimal]]:
        results = []

        for target_currency in target_currencies:
            if target_currency.exchange_currency in exchange_currencies:
                self.query.variable_values = {
                    "context": {
                        "country": "IT",
                        "investorType": "PRIVATE_INVESTORS",
                        "language": "it"
                    },
                    "isin": target_currency.code,
                    "startDate": dt.now().strftime("%Y-%m-%d"),
                    "endDate": dt.now().strftime("%Y-%m-%d"),
                    "frequency": "DAILY",
                    "currencyToIso": target_currency.exchange_currency.code,
                }

                try:
                    result = self.client.execute(self.query)
                except TransportQueryError:
                    continue

                data = result["fund"]["chart"]["values"]

                rate = Decimal(
                    str(
                        data[0]["fundAdjustedNav"]
                    )
                )

                # The rate is already inverted, so we don't need to invert it again
                results.append(
                    (target_currency.exchange_currency, target_currency, rate)
                )

        return results
