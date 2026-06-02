from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Optional

import requests

from simulate.sim_fetch import resilient

_SESSION = requests.Session()


@dataclass(frozen=True)
class PriceSnapshot:
    buy_price: Optional[float]
    sell_price: Optional[float]
    midpoint: Optional[float]
    fetched_at: float


class PolymarketPriceClient:
    def __init__(self, *, base_url: str, timeout_seconds: int = 15, cache_ttl_seconds: float = 15.0):
        self.base_url = str(base_url).rstrip("/")
        self.timeout_seconds = int(timeout_seconds)
        self.cache_ttl_seconds = float(cache_ttl_seconds)
        self._cache: Dict[str, PriceSnapshot] = {}

    @staticmethod
    def _as_float(value: object) -> Optional[float]:
        try:
            if value is None:
                return None
            fv = float(value)
        except (TypeError, ValueError):
            return None
        if fv <= 0 or fv >= 1:
            return None
        return fv

    def _get_json(self, path: str, *, params: Optional[dict] = None) -> object:
        return resilient.request_json(
            session=_SESSION,
            method="GET",
            url=f"{self.base_url}{path}",
            params=params,
            timeout=float(self.timeout_seconds),
            attempts=4,
            context=f"price {path}",
            on_retry=lambda exc, attempt, total, sleep: print(
                f"[simulate] price retry path={path} attempt={attempt}/{total} "
                f"sleep={sleep:.2f}s err={resilient.describe_exception(exc)}",
                flush=True,
            ),
        )

    def _extract_book_price(self, payload: object, *, want_buy: bool) -> Optional[float]:
        if not isinstance(payload, dict):
            return None
        side_key = "asks" if want_buy else "bids"
        levels = payload.get(side_key)
        if not isinstance(levels, list) or not levels:
            return None
        prices = []
        for level in levels:
            if isinstance(level, dict):
                candidate = level.get("price")
            elif isinstance(level, (list, tuple)) and level:
                candidate = level[0]
            else:
                candidate = None
            fv = self._as_float(candidate)
            if fv is not None:
                prices.append(fv)
        if not prices:
            return None
        return min(prices) if want_buy else max(prices)

    def _extract_midpoint(self, payload: object, token_id: str) -> Optional[float]:
        if isinstance(payload, dict):
            for key in ("midpoint", "mid", token_id):
                fv = self._as_float(payload.get(key))
                if fv is not None:
                    return fv
            nested = payload.get("data")
            if isinstance(nested, dict):
                return self._extract_midpoint(nested, token_id)
        if isinstance(payload, (str, int, float)):
            return self._as_float(payload)
        return None

    def get_snapshot(self, token_id: str, *, force: bool = False) -> PriceSnapshot:
        now = time.time()
        cached = self._cache.get(str(token_id))
        if cached is not None and not force and (now - cached.fetched_at) <= self.cache_ttl_seconds:
            return cached

        buy_price = None
        sell_price = None
        midpoint = None

        try:
            book_payload = self._get_json("/book", params={"token_id": str(token_id)})
            buy_price = self._extract_book_price(book_payload, want_buy=True)
            sell_price = self._extract_book_price(book_payload, want_buy=False)
        except Exception:
            pass

        try:
            midpoint_payload = self._get_json("/midpoint", params={"token_id": str(token_id)})
            midpoint = self._extract_midpoint(midpoint_payload, str(token_id))
        except Exception:
            midpoint = None

        if midpoint is None and buy_price is not None and sell_price is not None:
            midpoint = (float(buy_price) + float(sell_price)) / 2.0

        snapshot = PriceSnapshot(
            buy_price=buy_price,
            sell_price=sell_price,
            midpoint=midpoint,
            fetched_at=now,
        )
        self._cache[str(token_id)] = snapshot
        return snapshot
