from __future__ import annotations

import json
import time
from typing import Dict, List, Optional

import requests

from . import constants

_SESSION = requests.Session()


def _parse_clob_ids(market: dict) -> List[str]:
    raw = market.get("clobTokenIds") or market.get("clob_token_ids")
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(value) for value in raw]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        if isinstance(parsed, list):
            return [str(value) for value in parsed]
    return []


def gamma_markets_by_token_ids(token_ids: List[str], *, batch: int = 20) -> Dict[str, dict]:
    wanted = [str(value) for value in sorted(set(token_ids))]
    out: Dict[str, dict] = {}
    for start in range(0, len(wanted), batch):
        chunk = wanted[start : start + batch]
        params = [("clob_token_ids", token_id) for token_id in chunk]
        params.append(("limit", "200"))
        offset = 0
        while True:
            query = [(k, v) for (k, v) in params if k != "offset"]
            query.append(("offset", str(offset)))
            markets = None
            for attempt in range(4):
                response = _SESSION.get(constants.GAMMA_MARKETS, params=query, timeout=30)
                if response.status_code in (429, 500, 502, 503, 504) and attempt < 3:
                    time.sleep(0.35 * (attempt + 1))
                    continue
                if response.status_code >= 400:
                    body = (response.text or "").replace("\n", " ").strip()
                    if len(body) > 220:
                        body = body[:220] + "..."
                    raise RuntimeError(
                        f"Gamma markets HTTP {response.status_code}: chunk={len(chunk)} offset={offset} body={body}"
                    )
                try:
                    markets = response.json()
                except ValueError as exc:
                    body = (response.text or "").replace("\n", " ").strip()
                    if len(body) > 220:
                        body = body[:220] + "..."
                    raise RuntimeError(
                        f"Gamma markets non-JSON response: chunk={len(chunk)} offset={offset} body={body}"
                    ) from exc
                break

            if isinstance(markets, dict):
                markets = markets.get("data") or markets.get("markets") or []
            if not isinstance(markets, list) or not markets:
                break

            for market in markets:
                for token_id in _parse_clob_ids(market):
                    if token_id in chunk:
                        out[token_id] = market

            if len(markets) < 200:
                break
            offset += 200
            time.sleep(0.15)
        time.sleep(0.15)
    return out


def is_sports_market(market: dict) -> Optional[bool]:
    if not isinstance(market, dict):
        return None
    category = str(market.get("category") or "").strip().lower()
    if category and "sport" in category:
        return True
    for key in ("sport", "sports", "league", "teamAID", "teamBID", "gameId", "sportsMarketType"):
        if key in market and market.get(key):
            return True
    return None
