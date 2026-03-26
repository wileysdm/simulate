from __future__ import annotations

from typing import Optional

from web3 import Web3

from . import constants
from . import rpc


def ensure_0x_hex(s: str) -> str:
    text = str(s).strip()
    if text.startswith("0x") or text.startswith("0X"):
        return "0x" + text[2:]
    return "0x" + text


def norm_condition_id(cid: str) -> str:
    return ensure_0x_hex(str(cid)).lower()


def extract_addr_from_topic(topic_hex: str) -> str:
    topic = str(topic_hex).lower().replace("0x", "")
    return Web3.to_checksum_address("0x" + topic[-40:])


def decode_orderfilled(log: dict, *, block_timestamp: Optional[int] = None) -> Optional[dict]:
    data = log.get("data")
    topics = log.get("topics")
    if not (isinstance(data, str) and data.startswith("0x")):
        return None
    if not (isinstance(topics, list) and len(topics) >= 4):
        return None

    payload = bytes.fromhex(data[2:])
    if len(payload) < 32 * 5:
        return None

    def u256(slot_idx: int) -> int:
        start = slot_idx * 32
        return int.from_bytes(payload[start : start + 32], "big")

    maker_asset = u256(0)
    taker_asset = u256(1)
    maker_amount = u256(2)
    taker_amount = u256(3)
    fee_amount = u256(4)

    maker = extract_addr_from_topic(topics[2])
    taker = extract_addr_from_topic(topics[3])

    block_number = int(log["blockNumber"], 16)
    timestamp = (
        int(block_timestamp)
        if block_timestamp is not None
        else rpc.block_ts_multi_provider(
            block_number,
            log_every=25000,
            urls=constants.LOGS_RPC_URLS,
            timeout=int(constants.PREFETCH_RPC_TIMEOUT_SEC),
            retries=1,
            wallclock_timeout=float(constants.PREFETCH_RPC_TIMEOUT_SEC),
        )
    )
    tx = log.get("transactionHash")
    log_index = int(log.get("logIndex", "0x0"), 16) if isinstance(log.get("logIndex"), str) else 0

    if maker_asset == 0 and taker_asset != 0:
        side = "BUY"
        token_id = str(taker_asset)
        money_amount = maker_amount
        token_amount = taker_amount
    elif taker_asset == 0 and maker_asset != 0:
        side = "SELL"
        token_id = str(maker_asset)
        money_amount = taker_amount
        token_amount = maker_amount
    else:
        return None

    if token_amount == 0:
        return None

    price = (money_amount / constants.DECIMALS) / (token_amount / constants.DECIMALS)
    return {
        "event_type": "FILL",
        "timestamp": int(timestamp),
        "maker": maker,
        "taker": taker,
        "side": side,
        "token_id": token_id,
        "price": float(price),
        "size_usdc": float(money_amount / constants.DECIMALS),
        "size_token": float(token_amount / constants.DECIMALS),
        "fee_usdc": float(fee_amount / constants.DECIMALS),
        "tx": tx,
        "log_index": int(log_index),
    }
