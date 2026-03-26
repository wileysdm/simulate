from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple

from simulate.sim_fetch import constants as fetch_constants


@dataclass(frozen=True)
class StrategyConfig:
    lookback_days: int = 7
    rank_pool_size: int = 10
    follow_trader_count: int = 5
    fixed_notional_usdc: float = 100.0
    fee_rate: float = 0.001
    entry_delay_seconds: int = 300
    ranking_implied_win_threshold: float = 0.95
    close_near_resolved_threshold: float = 0.99


@dataclass(frozen=True)
class RuntimeConfig:
    clob_base_url: str = "https://clob.polymarket.com"
    price_timeout_seconds: int = 15
    backfill_days: int = 7
    history_chunk_seconds: int = 3 * 3600
    live_poll_interval_seconds: int = 10
    live_fetch_block_span: int = 10
    ingest_queue_max_batches: int = 8
    recent_dedupe_max_ids: int = 500_000
    fetch_step_blocks: int = 500
    history_fetch_step_blocks: int = 100
    fetch_tail_split_interval_seconds: float = 120.0
    decode_emit_batch_size: int = 5_000
    exchange_addresses: Tuple[str, ...] = field(
        default_factory=lambda: (
            str(fetch_constants.CTF_EXCHANGE),
            str(fetch_constants.NEG_RISK_CTF_EXCHANGE),
        )
    )
    logs_rpc_urls: Tuple[str, ...] = field(default_factory=lambda: tuple(fetch_constants.LOGS_RPC_URLS))
    ts_rpc_urls: Tuple[str, ...] = field(default_factory=lambda: tuple(fetch_constants.TS_RPC_URLS))
    history_logs_rpc_urls: Optional[Tuple[str, ...]] = field(
        default_factory=lambda: tuple(fetch_constants.HISTORY_LOGS_RPC_URLS)
    )
    live_logs_rpc_urls: Optional[Tuple[str, ...]] = field(
        default_factory=lambda: tuple(fetch_constants.LIVE_LOGS_RPC_URLS)
    )
    history_ts_rpc_urls: Optional[Tuple[str, ...]] = None
    live_ts_rpc_urls: Optional[Tuple[str, ...]] = None
    output_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parent / "runtime")
    log_path: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent / "runtime" / "logs" / "simulate.log"
    )
    raw_history_dir: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent / "runtime" / "raw_history"
    )
    raw_live_dir: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent / "runtime" / "raw_live"
    )
    trades_csv: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent / "runtime" / "simulated_trades.csv"
    )
    strategy: StrategyConfig = field(default_factory=StrategyConfig)


DEFAULT_CONFIG = RuntimeConfig()
