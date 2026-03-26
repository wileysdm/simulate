from __future__ import annotations

import json
import time
from typing import Dict, List, Optional, cast

import requests
from eth_abi.abi import encode
from eth_typing import HexStr
from web3 import Web3

from . import constants
from . import decoder
from . import rpc

_SESSION = requests.Session()
_GAMMA_MARKET_CACHE: Dict[str, Dict[str, List[str]]] = {}
_RESO_CACHE: Dict[str, Optional[str]] = {}


def _parse_maybe_json_array(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = None
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        if not inner:
            return []
        return [part.strip().strip('"\'') for part in inner.split(",") if part.strip()]
    return [part.strip() for part in text.split(",") if part.strip()]


def gamma_markets_by_condition_ids(cond_ids: List[str], *, batch: int = 20, timeout: int = 20) -> None:
    needed = [decoder.norm_condition_id(cid) for cid in cond_ids if cid]
    needed = [cid for cid in needed if cid not in _GAMMA_MARKET_CACHE]
    if not needed:
        return

    for start in range(0, len(needed), batch):
        chunk = needed[start : start + batch]
        params = [("condition_ids", cid) for cid in chunk]
        params.append(("limit", "200"))
        offset = 0
        while True:
            query = [(k, v) for (k, v) in params if k != "offset"]
            query.append(("offset", str(offset)))
            response = _SESSION.get(constants.GAMMA_MARKETS, params=query, timeout=timeout)
            response.raise_for_status()
            markets = response.json()
            if isinstance(markets, dict):
                markets = markets.get("data") or markets.get("markets") or []
            if not isinstance(markets, list) or not markets:
                break

            for market in markets:
                condition_id = market.get("conditionId") or market.get("condition_id")
                if not condition_id:
                    continue
                cid_norm = decoder.norm_condition_id(str(condition_id))
                if cid_norm not in chunk:
                    continue
                outcomes = _parse_maybe_json_array(market.get("outcomes"))
                clob_ids = _parse_maybe_json_array(market.get("clobTokenIds"))
                if outcomes and clob_ids and len(outcomes) == len(clob_ids):
                    _GAMMA_MARKET_CACHE[cid_norm] = {
                        "outcomes": outcomes,
                        "clobTokenIds": clob_ids,
                    }

            if len(markets) < 200:
                break
            offset += 200
            time.sleep(0.1)
        time.sleep(0.1)


def _fn_selector(signature: str) -> bytes:
    return Web3.keccak(text=signature)[:4]


def _eth_call(to_addr: str, data_hex: str, *, urls: List[str], timeout: int = 25, retries: int = 8) -> str:
    return rpc.rpc(
        "eth_call",
        [{"to": Web3.to_checksum_address(to_addr), "data": data_hex}, "latest"],
        urls=urls,
        timeout=timeout,
        retries=retries,
    )


_SEL_PAYOUT_DEN = _fn_selector("payoutDenominator(bytes32)")
_SEL_PAYOUT_NUM = _fn_selector("payoutNumerators(bytes32,uint256)")


def _payout_denominator(
    condition_id: str,
    *,
    urls: Optional[List[str]] = None,
    timeout: int = 25,
    retries: int = 10,
) -> int:
    cid_hex = cast(HexStr, decoder.ensure_0x_hex(condition_id))
    cid = Web3.to_bytes(hexstr=cid_hex)
    data = _SEL_PAYOUT_DEN + encode(["bytes32"], [cid])
    use_urls = constants.LOGS_RPC_URLS if urls is None else urls
    out = _eth_call(
        constants.CONDITIONAL_TOKENS,
        "0x" + data.hex(),
        urls=use_urls,
        timeout=int(timeout),
        retries=int(retries),
    )
    return int(out, 16) if isinstance(out, str) else int(out)


def _payout_numerator(
    condition_id: str,
    idx: int,
    *,
    urls: Optional[List[str]] = None,
    timeout: int = 25,
    retries: int = 10,
) -> int:
    cid_hex = cast(HexStr, decoder.ensure_0x_hex(condition_id))
    cid = Web3.to_bytes(hexstr=cid_hex)
    data = _SEL_PAYOUT_NUM + encode(["bytes32", "uint256"], [cid, int(idx)])
    use_urls = constants.LOGS_RPC_URLS if urls is None else urls
    out = _eth_call(
        constants.CONDITIONAL_TOKENS,
        "0x" + data.hex(),
        urls=use_urls,
        timeout=int(timeout),
        retries=int(retries),
    )
    return int(out, 16) if isinstance(out, str) else int(out)


def _resolved_outcome_for_condition_with_urls(condition_id: str, *, urls: List[str]) -> Optional[str]:
    cid_norm = decoder.norm_condition_id(condition_id)
    if cid_norm in _RESO_CACHE:
        return _RESO_CACHE[cid_norm]

    info = _GAMMA_MARKET_CACHE.get(cid_norm)
    if not info:
        _RESO_CACHE[cid_norm] = None
        return None

    outcomes = info.get("outcomes") or []
    if not outcomes:
        _RESO_CACHE[cid_norm] = None
        return None

    try:
        timeout_sec = int(getattr(constants, "ONCHAIN_RESOLVE_TIMEOUT_SEC", 12))
        retries = int(getattr(constants, "ONCHAIN_RESOLVE_RETRIES", 3))
        denominator = _payout_denominator(
            str(cid_norm),
            urls=urls,
            timeout=timeout_sec,
            retries=retries,
        )
        if int(denominator) == 0:
            _RESO_CACHE[cid_norm] = None
            return None

        best_index = None
        best_value = -1
        for idx in range(len(outcomes)):
            value = int(
                _payout_numerator(
                    str(cid_norm),
                    idx,
                    urls=urls,
                    timeout=timeout_sec,
                    retries=retries,
                )
            )
            if value > best_value:
                best_value = value
                best_index = idx

        if best_index is None or best_value <= 0:
            _RESO_CACHE[cid_norm] = None
            return None

        winner = str(outcomes[int(best_index)])
        _RESO_CACHE[cid_norm] = winner
        return winner
    except Exception:
        _RESO_CACHE[cid_norm] = None
        return None
