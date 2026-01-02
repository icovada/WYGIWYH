import logging
import time

import requests
from decimal import Decimal
from typing import Tuple, List, Optional, Dict

from django.db.models import QuerySet

from apps.currencies.models import Currency, ExchangeRate
from apps.currencies.exchange_rates.base import ExchangeRateProvider

logger = logging.getLogger(__name__)


class TransitiveRateProvider(ExchangeRateProvider):
    """Calculates exchange rates through paths of existing rates"""

    rates_inverted = True

    def __init__(self, api_key: str = None):
        super().__init__(api_key)  # API key not needed but maintaining interface

    @classmethod
    def requires_api_key(cls) -> bool:
        return False

    def get_rates(
        self, target_currencies: QuerySet, exchange_currencies: set
    ) -> List[Tuple[Currency, Currency, Decimal]]:
        results = []

        # Get recent rates for building the graph
        recent_rates = ExchangeRate.objects.all()

        # Build currency graph
        currency_graph = self._build_currency_graph(recent_rates)

        for target in target_currencies:
            if (
                not target.exchange_currency
                or target.exchange_currency not in exchange_currencies
            ):
                continue

            # Find path and calculate rate
            from_id = target.exchange_currency.id
            to_id = target.id

            path, rate = self._find_conversion_path(currency_graph, from_id, to_id)

            if path and rate:
                path_codes = [Currency.objects.get(id=cid).code for cid in path]
                logger.info(
                    f"Found conversion path: {' -> '.join(path_codes)}, rate: {rate}"
                )
                results.append((target.exchange_currency, target, rate))
            else:
                logger.debug(
                    f"No conversion path found for {target.exchange_currency.code}->{target.code}"
                )

        return results

    @staticmethod
    def _build_currency_graph(rates) -> Dict[int, Dict[int, Decimal]]:
        """Build a graph representation of currency relationships"""
        graph = {}

        for rate in rates:
            # Add both directions to make the graph bidirectional
            if rate.from_currency_id not in graph:
                graph[rate.from_currency_id] = {}
            graph[rate.from_currency_id][rate.to_currency_id] = rate.rate

            if rate.to_currency_id not in graph:
                graph[rate.to_currency_id] = {}
            graph[rate.to_currency_id][rate.from_currency_id] = Decimal("1") / rate.rate

        return graph

    @staticmethod
    def _find_conversion_path(
        graph, from_id, to_id
    ) -> Tuple[Optional[list], Optional[Decimal]]:
        """Find the shortest path between currencies using breadth-first search"""
        if from_id not in graph or to_id not in graph:
            return None, None

        queue = [(from_id, [from_id], Decimal("1"))]
        visited = {from_id}

        while queue:
            current, path, current_rate = queue.pop(0)

            if current == to_id:
                return path, current_rate

            for neighbor, rate in graph.get(current, {}).items():
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor], current_rate * rate))

        return None, None
