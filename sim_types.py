from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Tuple


@dataclass(frozen=True)
class CanonicalMarket:
    event_key: str
    anchor_token_id: str
    opposite_token_id: str
    anchor_outcome: str
    opposite_outcome: str
    question: str
    slug: str


@dataclass(frozen=True)
class CanonicalTraderFill:
    raw_fill_id: str
    block_number: int
    timestamp: int
    trade_day: str
    trader: str
    event_key: str
    question: str
    slug: str
    anchor_token_id: str
    raw_token_id: str
    raw_side: str
    axis_side: str
    qty_shares: float
    raw_price: float
    axis_price: float
    axis_notional_usdc: float
    tx: str
    log_index: int


@dataclass(frozen=True)
class IngestBatch:
    source: str
    live: bool
    batch_name: str
    fills: Tuple[CanonicalTraderFill, ...]


@dataclass(frozen=True)
class WorkerEvent:
    source: str
    event_type: str
    detail: str = ""


@dataclass
class EventExposure:
    buy_qty: float = 0.0
    buy_cost_usdc: float = 0.0
    sell_qty: float = 0.0
    sell_entry_notional_usdc: float = 0.0
    signed_position_usdc: float = 0.0
    fill_count: int = 0

    def add_fill(self, fill: CanonicalTraderFill) -> None:
        self.fill_count += 1
        if fill.axis_side == "BUY":
            self.buy_qty += float(fill.qty_shares)
            self.buy_cost_usdc += float(fill.qty_shares) * float(fill.axis_price)
            self.signed_position_usdc += float(fill.axis_notional_usdc)
            return
        self.sell_qty += float(fill.qty_shares)
        self.sell_entry_notional_usdc += float(fill.qty_shares) * float(fill.axis_price)
        self.signed_position_usdc -= float(fill.axis_notional_usdc)

    def extend(self, other: "EventExposure") -> None:
        self.buy_qty += float(other.buy_qty)
        self.buy_cost_usdc += float(other.buy_cost_usdc)
        self.sell_qty += float(other.sell_qty)
        self.sell_entry_notional_usdc += float(other.sell_entry_notional_usdc)
        self.signed_position_usdc += float(other.signed_position_usdc)
        self.fill_count += int(other.fill_count)

    def mark_pnl(self, settle_price: float) -> float:
        settle = float(settle_price)
        buy_pnl = self.buy_qty * settle - self.buy_cost_usdc
        sell_pnl = self.sell_entry_notional_usdc - self.sell_qty * settle
        return float(buy_pnl + sell_pnl)


@dataclass
class DailyTraderCounters:
    fill_count: int = 0
    total_notional_usdc: float = 0.0
    buy_notional_usdc: float = 0.0
    sell_notional_usdc: float = 0.0

    def add_fill(self, fill: CanonicalTraderFill) -> None:
        notional = float(fill.axis_notional_usdc)
        self.fill_count += 1
        self.total_notional_usdc += notional
        if fill.axis_side == "BUY":
            self.buy_notional_usdc += notional
        else:
            self.sell_notional_usdc += notional

    def extend(self, other: "DailyTraderCounters") -> None:
        self.fill_count += int(other.fill_count)
        self.total_notional_usdc += float(other.total_notional_usdc)
        self.buy_notional_usdc += float(other.buy_notional_usdc)
        self.sell_notional_usdc += float(other.sell_notional_usdc)


@dataclass(frozen=True)
class TraderSelection:
    trade_day: str
    trader: str
    source_rank: int
    selected_rank: int
    rolling_pnl_usdc: float
    p50_event_net_position_usdc: float
    top1pct_event_profit_share: Optional[float]
    active_days: int
    fill_count: int
    total_notional_usdc: float


@dataclass
class LiveSignalState:
    signed_position_usdc: float = 0.0
    initial_trigger_seen: bool = False
    threshold_usdc: float = 0.0


@dataclass
class OpenPosition:
    trader: str
    event_key: str
    question: str
    anchor_token_id: str
    direction: int
    qty_shares: float
    entry_price: float
    entry_ts: int
    fees_paid_usdc: float = 0.0


@dataclass(order=True)
class PendingAction:
    execute_ts: int
    action_seq: int
    action_type: str = field(compare=False)
    trader: str = field(compare=False)
    event_key: str = field(compare=False)
    direction: int = field(compare=False)
    anchor_token_id: str = field(compare=False)
    question: str = field(compare=False, default="")
    threshold_usdc: float = field(compare=False, default=0.0)
    trigger_ts: int = field(compare=False, default=0)
    reason: str = field(compare=False, default="")


@dataclass(frozen=True)
class ExecutedTrade:
    timestamp: int
    trader: str
    event_key: str
    question: str
    action_type: str
    direction: int
    price: float
    qty_shares: float
    fee_usdc: float
    pnl_delta_usdc: float
    cumulative_pnl_usdc: float
    reason: str
