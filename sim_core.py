from __future__ import annotations

import csv
import heapq
import math
import queue
import threading
import time
from collections import defaultdict, deque
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import DefaultDict, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from simulate.sim_fetch import constants as fetch_constants
from simulate.sim_fetch import decoder, gamma, labels, resilient, rpc

from simulate.sim_config import RuntimeConfig
from simulate.sim_price import PolymarketPriceClient
from simulate.sim_types import (
    CanonicalMarket,
    CanonicalTraderFill,
    DailyTraderCounters,
    EventExposure,
    ExecutedTrade,
    IngestBatch,
    LiveSignalState,
    OpenPosition,
    PendingAction,
    TraderSelection,
    WorkerEvent,
)

QueueItem = Union[IngestBatch, WorkerEvent]


def norm_addr(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in {"", "nan", "none", "null"}:
        return ""
    return text


def utc_day_text(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")


def utc_midnight_ts(day_text: str) -> int:
    day_value = date.fromisoformat(str(day_text))
    return int(datetime(day_value.year, day_value.month, day_value.day, tzinfo=timezone.utc).timestamp())


def sign_int(value: float) -> int:
    if float(value) > 0:
        return 1
    if float(value) < 0:
        return -1
    return 0


def opposite_side(side: str) -> str:
    side_up = str(side).upper()
    if side_up == "BUY":
        return "SELL"
    if side_up == "SELL":
        return "BUY"
    raise ValueError(f"unsupported side: {side}")


def parse_json_list_field(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            import json

            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
        except Exception:
            pass
    return [part.strip().strip('"\'') for part in text.split(",") if str(part).strip()]


class MarketResolver:
    def __init__(self) -> None:
        self._markets_by_token: Dict[str, Optional[CanonicalMarket]] = {}
        self._markets_by_event: Dict[str, CanonicalMarket] = {}
        self._official_settle_cache: Dict[str, Optional[float]] = {}

    def ensure_tokens(self, token_ids: Iterable[str]) -> None:
        wanted = sorted({str(token_id) for token_id in token_ids if str(token_id) and str(token_id) not in self._markets_by_token})
        if not wanted:
            return
        market_map = gamma.gamma_markets_by_token_ids(wanted, batch=20)
        for token_id in wanted:
            market = market_map.get(str(token_id))
            resolved = self._build_market(market)
            self._markets_by_token[str(token_id)] = resolved
            if resolved is not None:
                self._markets_by_event[resolved.event_key] = resolved
                self._markets_by_token[resolved.anchor_token_id] = resolved
                self._markets_by_token[resolved.opposite_token_id] = resolved

    def _build_market(self, market: Optional[dict]) -> Optional[CanonicalMarket]:
        if not isinstance(market, dict):
            return None
        if gamma.is_sports_market(market) is not True:
            return None
        token_ids = parse_json_list_field(market.get("clobTokenIds") or market.get("clob_token_ids"))
        outcomes = parse_json_list_field(market.get("outcomes"))
        if len(token_ids) != 2 or len(outcomes) != 2:
            return None
        condition_id = market.get("conditionId") or market.get("condition_id")
        if not condition_id:
            return None
        event_key = decoder.norm_condition_id(str(condition_id))
        return CanonicalMarket(
            event_key=event_key,
            anchor_token_id=str(token_ids[0]),
            opposite_token_id=str(token_ids[1]),
            anchor_outcome=str(outcomes[0]),
            opposite_outcome=str(outcomes[1]),
            question=str(market.get("question") or market.get("title") or "").strip(),
            slug=str(market.get("slug") or "").strip(),
        )

    def market_for_token(self, token_id: str) -> Optional[CanonicalMarket]:
        self.ensure_tokens([str(token_id)])
        return self._markets_by_token.get(str(token_id))

    def market_for_event(self, event_key: str) -> Optional[CanonicalMarket]:
        return self._markets_by_event.get(str(event_key))

    def normalize_fill(
        self,
        *,
        raw_fill_id: str,
        block_number: int,
        timestamp: int,
        trader: str,
        token_id: str,
        raw_side: str,
        qty_shares: float,
        raw_price: float,
        tx: str,
        log_index: int,
    ) -> Optional[CanonicalTraderFill]:
        market = self.market_for_token(token_id)
        if market is None:
            return None
        trader_norm = norm_addr(trader)
        side_up = str(raw_side).upper()
        token_text = str(token_id)
        if not trader_norm or side_up not in {"BUY", "SELL"}:
            return None
        price = float(raw_price)
        qty = float(qty_shares)
        if qty <= 0 or price <= 0 or price >= 1:
            return None

        if token_text == market.anchor_token_id:
            axis_side = side_up
            axis_price = price
        elif token_text == market.opposite_token_id:
            axis_side = "BUY" if side_up == "SELL" else "SELL"
            axis_price = 1.0 - price
        else:
            return None

        if axis_price <= 0 or axis_price >= 1:
            return None
        return CanonicalTraderFill(
            raw_fill_id=str(raw_fill_id),
            block_number=int(block_number),
            timestamp=int(timestamp),
            trade_day=utc_day_text(int(timestamp)),
            trader=trader_norm,
            event_key=market.event_key,
            question=market.question,
            slug=market.slug,
            anchor_token_id=market.anchor_token_id,
            raw_token_id=token_text,
            raw_side=side_up,
            axis_side=axis_side,
            qty_shares=qty,
            raw_price=price,
            axis_price=float(axis_price),
            axis_notional_usdc=qty * float(axis_price),
            tx=str(tx or ""),
            log_index=int(log_index),
        )

    def official_anchor_settle(self, event_key: str) -> Optional[float]:
        event_key = str(event_key)
        if event_key in self._official_settle_cache:
            return self._official_settle_cache[event_key]
        market = self._markets_by_event.get(event_key)
        if market is None:
            self._official_settle_cache[event_key] = None
            return None
        try:
            labels.gamma_markets_by_condition_ids([event_key], batch=20, timeout=20)
            resolved_outcome = labels._resolved_outcome_for_condition_with_urls(
                event_key,
                urls=list(self.config.logs_rpc_urls[:3]),
            )
        except Exception:
            resolved_outcome = None
        if not resolved_outcome:
            self._official_settle_cache[event_key] = None
            return None
        settle = 1.0 if str(resolved_outcome) == str(market.anchor_outcome) else 0.0
        self._official_settle_cache[event_key] = settle
        return settle


class SimulationEngine:
    def __init__(self, config: RuntimeConfig):
        self.config = config
        self.strategy = config.strategy
        self.price_client = PolymarketPriceClient(
            base_url=config.clob_base_url,
            timeout_seconds=config.price_timeout_seconds,
        )
        self.market_resolver = MarketResolver()

        self.history_events_by_day: DefaultDict[str, Dict[str, Dict[str, EventExposure]]] = defaultdict(lambda: defaultdict(dict))
        self.history_counts_by_day: DefaultDict[str, Dict[str, DailyTraderCounters]] = defaultdict(dict)

        self.selected_traders: Dict[str, TraderSelection] = {}
        self.selected_trade_day: Optional[str] = None
        self.live_signal_states: Dict[Tuple[str, str], LiveSignalState] = {}
        self.open_positions: Dict[Tuple[str, str], OpenPosition] = {}
        self.last_position_direction: Dict[Tuple[str, str], int] = {}
        self.pending_actions: List[PendingAction] = []
        self._action_seq = 0
        self.cumulative_realized_pnl_usdc = 0.0
        self.trading_enabled_ts: Optional[int] = None

        self._history_queue: "queue.Queue[QueueItem]" = queue.Queue(maxsize=max(1, int(config.ingest_queue_max_batches)))
        self._live_queue: "queue.Queue[QueueItem]" = queue.Queue(maxsize=max(4, int(config.ingest_queue_max_batches) * 4))
        self._stop_event = threading.Event()
        self._workers: List[threading.Thread] = []
        self._backfill_drained = False
        self._live_worker_done = False
        self._minimum_trading_enabled_ts: Optional[int] = None
        self._startup_block: Optional[int] = None
        self._recent_fill_ids: set[str] = set()
        self._recent_fill_order: deque[str] = deque()
        self._hour_fill_buffers: DefaultDict[int, List[CanonicalTraderFill]] = defaultdict(list)
        self._hour_fill_buffer_ids: DefaultDict[int, set[str]] = defaultdict(set)
        self._raw_header_written: Dict[Path, bool] = {}
        self._log_file_ready = False

        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.config.raw_dir.mkdir(parents=True, exist_ok=True)
        self.config.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._trades_header_written = self.config.trades_csv.exists() and self.config.trades_csv.stat().st_size > 0

    def log(self, message: str) -> None:
        line = f"[simulate] {message}"
        print(line, flush=True)
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        mode = "a" if self._log_file_ready else "w"
        with self.config.log_path.open(mode, encoding="utf-8") as fh:
            fh.write(f"{timestamp}Z {line}\n")
        self._log_file_ready = True

    def _log_network_retry(self, label: str, exc: BaseException, attempt: int, total: int, sleep_seconds: float) -> None:
        self.log(
            f"{label} retry attempt={attempt}/{total} sleep={sleep_seconds:.2f}s "
            f"err={resilient.describe_exception(exc)}"
        )

    def _retry_network_call(self, label: str, func, *, attempts: int = 4):
        return resilient.retry_call(
            func,
            attempts=int(attempts),
            on_retry=lambda exc, attempt, total, sleep: self._log_network_retry(
                label, exc, attempt, total, sleep
            ),
        )

    def run(self, *, max_polls: int = 0) -> None:
        self.bootstrap(max_polls=max_polls)
        try:
            while True:
                worked = self._drain_once(wait_timeout=0.25)
                now_ts = int(time.time())
                self._process_due_actions(now_ts)
                self._close_near_resolved_positions(now_ts)
                self._flush_closed_hour_buffers(now_ts=now_ts)
                if int(max_polls) > 0 and self._live_worker_done and self._backfill_drained and self._queues_empty():
                    break
                if not worked:
                    time.sleep(0.25)
        finally:
            self.stop()

    def stop(self) -> None:
        self._stop_event.set()
        self._flush_closed_hour_buffers(now_ts=int(time.time()))
        for worker in self._workers:
            worker.join(timeout=0.5)

    def _history_logs_urls(self) -> List[str]:
        return list(self.config.history_logs_rpc_urls or self.config.logs_rpc_urls)

    def _live_logs_urls(self) -> List[str]:
        return list(self.config.live_logs_rpc_urls or self.config.logs_rpc_urls)

    def _history_ts_urls(self) -> List[str]:
        return list(self.config.history_ts_rpc_urls or self.config.ts_rpc_urls)

    def _live_ts_urls(self) -> List[str]:
        return list(self.config.live_ts_rpc_urls or self.config.ts_rpc_urls)

    def bootstrap(self, *, max_polls: int = 0) -> None:
        now_ts = int(time.time())
        startup_day = utc_day_text(now_ts)
        startup_next_day_ts = utc_midnight_ts(startup_day) + 86400
        backfill_start_ts = utc_midnight_ts(startup_day) - int(self.config.backfill_days) * 86400
        replayed_until_ts = self._restore_persisted_window(start_ts=int(backfill_start_ts), end_ts=int(now_ts))
        startup_block = self._retry_network_call(
            "bootstrap latest block",
            lambda: self._latest_block_number(use_live=True),
            attempts=6,
        )

        self._startup_block = int(startup_block)
        self._minimum_trading_enabled_ts = int(startup_next_day_ts)
        self.log(
            f"bootstrap launch: backfill_window={utc_day_text(backfill_start_ts)}..{startup_day} startup_ts={now_ts} startup_block={startup_block} replayed_until_ts={replayed_until_ts}"
        )

        resume_start_ts = int(backfill_start_ts)
        if replayed_until_ts is not None:
            resume_start_ts = max(int(backfill_start_ts), int(replayed_until_ts) + 1)

        if resume_start_ts <= int(now_ts):
            self._start_worker(
                name="simulate-backfill",
                target=self._backfill_worker,
                start_ts=int(resume_start_ts),
                end_block=int(startup_block),
            )
        else:
            self.log("bootstrap replay already covers current history window")
            self._queue_put(
                self._history_queue,
                WorkerEvent(source="history", event_type="done", detail="history replay complete"),
            )
        self._start_worker(
            name="simulate-tail",
            target=self._tail_worker,
            start_block=int(startup_block),
            max_polls=int(max_polls),
        )

    def _start_worker(self, *, name: str, target, **kwargs) -> None:
        worker = threading.Thread(target=target, kwargs=kwargs, name=name, daemon=True)
        worker.start()
        self._workers.append(worker)

    def _queues_empty(self) -> bool:
        return self._live_queue.empty() and self._history_queue.empty()

    def _queue_put(self, target_queue: "queue.Queue[QueueItem]", item: QueueItem) -> None:
        while not self._stop_event.is_set():
            try:
                target_queue.put(item, timeout=0.5)
                return
            except queue.Full:
                continue

    def _drain_once(self, *, wait_timeout: float) -> bool:
        item: Optional[QueueItem] = None
        source_queue: Optional["queue.Queue[QueueItem]"] = None
        preferred = (self._history_queue, self._live_queue) if not self._backfill_drained else (self._live_queue, self._history_queue)

        for candidate in preferred:
            try:
                item = candidate.get_nowait()
                source_queue = candidate
                break
            except queue.Empty:
                continue

        if item is None:
            try:
                item = preferred[0].get(timeout=max(0.05, float(wait_timeout)))
                source_queue = preferred[0]
            except queue.Empty:
                try:
                    item = preferred[1].get_nowait()
                    source_queue = preferred[1]
                except queue.Empty:
                    return False

        assert item is not None
        assert source_queue is not None
        try:
            self._process_queue_item(item)
        finally:
            source_queue.task_done()
        return True

    def _process_queue_item(self, item: QueueItem) -> None:
        if isinstance(item, IngestBatch):
            self._process_ingest_batch(item)
            return
        if item.source == "history" and item.event_type == "done":
            self._on_backfill_drained()
            return
        if item.source == "live" and item.event_type == "done":
            self._live_worker_done = True
            self.log("live worker complete")

    def _on_backfill_drained(self) -> None:
        if self._backfill_drained:
            return
        self._backfill_drained = True
        completed_ts = int(time.time())
        min_trade_ts = int(self._minimum_trading_enabled_ts or completed_ts)
        self.trading_enabled_ts = max(min_trade_ts, completed_ts)
        enabled_day = utc_day_text(self.trading_enabled_ts)
        self._refresh_selection_for_day(enabled_day)
        self.log(
            f"backfill complete: trading_enabled_ts={self.trading_enabled_ts} trade_day={enabled_day}"
        )

    def _restore_persisted_window(self, *, start_ts: int, end_ts: int) -> Optional[int]:
        descriptors: List[Tuple[int, Path]] = []
        seen_paths: set[Path] = set()

        for path in sorted(self.config.raw_dir.glob("*/*.csv")):
            parsed = self._parse_raw_hour_path(path)
            if parsed is None:
                continue
            hour_start_ts, hour_end_ts = parsed
            if int(hour_end_ts) < int(start_ts) or int(hour_start_ts) > int(end_ts):
                continue
            if path not in seen_paths:
                descriptors.append((int(hour_start_ts), path))
                seen_paths.add(path)

        if not descriptors:
            return None

        latest_ts: Optional[int] = None
        replayed_rows = 0
        seen_actor_keys: set[str] = set()
        for _, path in sorted(descriptors, key=lambda item: (item[0], str(item[1]))):
            loaded = 0
            for fill in self._iter_fill_file(path):
                if int(fill.timestamp) < int(start_ts) or int(fill.timestamp) > int(end_ts):
                    continue
                actor_key = f"{fill.raw_fill_id}:{fill.trader}"
                if actor_key in seen_actor_keys:
                    continue
                seen_actor_keys.add(actor_key)
                self._ingest_fill(fill, live=False)
                latest_ts = int(fill.timestamp) if latest_ts is None else max(int(latest_ts), int(fill.timestamp))
                loaded += 1
            if loaded > 0:
                replayed_rows += int(loaded)
                self.log(f"replay file={self._display_path(path)} rows={loaded}")

        if replayed_rows == 0:
            return None
        self.log(f"replay complete rows={replayed_rows} latest_ts={latest_ts}")
        return latest_ts

    def _parse_raw_hour_path(self, path: Path) -> Optional[Tuple[int, int]]:
        try:
            trade_day = path.parent.name
            hour = int(path.stem)
            if hour < 0 or hour > 23:
                return None
            start_ts = utc_midnight_ts(trade_day) + hour * 3600
            return int(start_ts), int(start_ts + 3599)
        except Exception:
            return None

    def _display_path(self, path: Path) -> str:
        for root in (self.config.raw_dir, self.config.output_dir):
            try:
                return str(path.relative_to(root))
            except ValueError:
                continue
        return str(path)

    @staticmethod
    def _hour_start_ts(timestamp: int) -> int:
        return int(timestamp) - (int(timestamp) % 3600)

    def _current_hour_start_ts(self, now_ts: int) -> int:
        return self._hour_start_ts(int(now_ts))

    def _raw_hour_path_from_hour_start(self, hour_start_ts: int) -> Path:
        dt = datetime.fromtimestamp(int(hour_start_ts), tz=timezone.utc)
        return self.config.raw_dir / dt.strftime("%Y-%m-%d") / f"{dt.strftime('%H')}.csv"

    def _buffer_fill_for_hour(self, hour_start_ts: int, fill: CanonicalTraderFill) -> None:
        dedupe_key = f"{fill.raw_fill_id}:{fill.trader}"
        keys = self._hour_fill_buffer_ids[int(hour_start_ts)]
        if dedupe_key in keys:
            return
        keys.add(dedupe_key)
        self._hour_fill_buffers[int(hour_start_ts)].append(fill)

    def _store_fills(self, fills: Sequence[CanonicalTraderFill], *, now_ts: int) -> None:
        if not fills:
            return
        current_hour_start_ts = self._current_hour_start_ts(int(now_ts))
        closed_by_hour: DefaultDict[int, List[CanonicalTraderFill]] = defaultdict(list)
        for fill in fills:
            hour_start_ts = self._hour_start_ts(int(fill.timestamp))
            if int(hour_start_ts) < int(current_hour_start_ts):
                closed_by_hour[int(hour_start_ts)].append(fill)
            else:
                self._buffer_fill_for_hour(int(hour_start_ts), fill)
        for hour_start_ts, group in sorted(closed_by_hour.items(), key=lambda item: item[0]):
            path = self._raw_hour_path_from_hour_start(int(hour_start_ts))
            ordered = sorted(group, key=lambda item: (item.block_number, item.tx, item.log_index, item.trader))
            self._write_fill_rows(path, ordered, append=True)

    def _flush_closed_hour_buffers(self, *, now_ts: int) -> None:
        current_hour_start_ts = self._current_hour_start_ts(int(now_ts))
        ready_hours = sorted(
            [hour_start_ts for hour_start_ts in self._hour_fill_buffers.keys() if int(hour_start_ts) < int(current_hour_start_ts)]
        )
        for hour_start_ts in ready_hours:
            fills = self._hour_fill_buffers.pop(int(hour_start_ts), [])
            self._hour_fill_buffer_ids.pop(int(hour_start_ts), None)
            if not fills:
                continue
            path = self._raw_hour_path_from_hour_start(int(hour_start_ts))
            ordered = sorted(fills, key=lambda item: (item.block_number, item.tx, item.log_index, item.trader))
            self._write_fill_rows(path, ordered, append=True)

    def _iter_fill_file(self, path: Path):
        with path.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                fill = self._fill_from_row(row)
                if fill is not None:
                    yield fill

    def _fill_from_row(self, row: Dict[str, str]) -> Optional[CanonicalTraderFill]:
        try:
            return CanonicalTraderFill(
                raw_fill_id=str(row.get("raw_fill_id") or ""),
                block_number=int(row.get("block_number") or 0),
                timestamp=int(row.get("timestamp") or 0),
                trade_day=str(row.get("trade_day") or ""),
                trader=str(row.get("trader") or ""),
                event_key=str(row.get("event_key") or ""),
                question=str(row.get("question") or ""),
                slug=str(row.get("slug") or ""),
                anchor_token_id=str(row.get("anchor_token_id") or ""),
                raw_token_id=str(row.get("raw_token_id") or ""),
                raw_side=str(row.get("raw_side") or ""),
                axis_side=str(row.get("axis_side") or ""),
                qty_shares=float(row.get("qty_shares") or 0.0),
                raw_price=float(row.get("raw_price") or 0.0),
                axis_price=float(row.get("axis_price") or 0.0),
                axis_notional_usdc=float(row.get("axis_notional_usdc") or 0.0),
                tx=str(row.get("tx") or ""),
                log_index=int(row.get("log_index") or 0),
            )
        except Exception:
            return None

    def _process_ingest_batch(self, batch: IngestBatch) -> None:
        fills: Sequence[CanonicalTraderFill] = batch.fills
        fills = self._dedupe_batch_fills(fills)
        if not fills:
            return
        now_ts = int(time.time())
        for fill in fills:
            if self.trading_enabled_ts is not None:
                self._ensure_trade_day(fill.trade_day)
            self._ingest_fill(fill, live=batch.live)
        self._store_fills(fills, now_ts=now_ts)
        self._flush_closed_hour_buffers(now_ts=now_ts)

    def _dedupe_batch_fills(self, fills: Sequence[CanonicalTraderFill]) -> List[CanonicalTraderFill]:
        out: List[CanonicalTraderFill] = []
        max_ids = max(10_000, int(self.config.recent_dedupe_max_ids))
        for fill in fills:
            dedupe_key = f"{fill.raw_fill_id}:{fill.trader}"
            if dedupe_key in self._recent_fill_ids:
                continue
            self._recent_fill_ids.add(dedupe_key)
            self._recent_fill_order.append(dedupe_key)
            out.append(fill)
            while len(self._recent_fill_order) > max_ids:
                stale = self._recent_fill_order.popleft()
                self._recent_fill_ids.discard(stale)
        return out

    def _write_fill_rows(self, path: Path, fills: Sequence[CanonicalTraderFill], *, append: bool) -> None:
        if not fills:
            return
        iterator = iter(fills)
        first_fill = next(iterator, None)
        if first_fill is None:
            return
        first_row = asdict(first_fill)
        header_written = self._raw_header_written.get(path)
        if header_written is None:
            header_written = path.exists() and path.stat().st_size > 0
            self._raw_header_written[path] = bool(header_written)
        mode = "a" if append and header_written else "w"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open(mode, newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(first_row.keys()))
            if not self._raw_header_written.get(path, False):
                writer.writeheader()
                self._raw_header_written[path] = True
            writer.writerow(first_row)
            for fill in iterator:
                writer.writerow(asdict(fill))

    def _backfill_worker(self, *, start_ts: int, end_block: int) -> None:
        chunk_seconds = max(60, int(self.config.history_chunk_seconds))
        chunk_block_span = max(1, int(math.ceil(float(chunk_seconds) * float(fetch_constants.BLOCKS_PER_SEC_EST))))
        history_ts_urls = self._history_ts_urls()
        history_logs_urls = self._history_logs_urls()
        while not self._stop_event.is_set():
            try:
                start_block, _ = self._retry_network_call(
                    "history start block",
                    lambda: rpc.blocks_for_time_range(
                        int(start_ts),
                        int(start_ts),
                        ts_urls=list(history_ts_urls),
                        fallback_log_urls=list(history_logs_urls[:3]),
                    ),
                    attempts=4,
                )
                break
            except Exception as exc:
                self.log(f"history start block deferred err={resilient.describe_exception(exc)}")
                time.sleep(5.0)
        else:
            return
        chunk_lo = int(start_block)
        final_block = int(end_block)
        while chunk_lo <= final_block and not self._stop_event.is_set():
            chunk_hi = min(final_block, int(chunk_lo) + int(chunk_block_span) - 1)
            self.log(
                f"history chunk blocks={chunk_lo}->{chunk_hi} start_ts={start_ts}"
            )
            part_seq = 0

            def on_fills(decoded_fills: Sequence[CanonicalTraderFill]) -> None:
                nonlocal part_seq
                filtered = [
                    fill
                    for fill in decoded_fills
                    if int(fill.block_number) >= int(chunk_lo)
                    and int(fill.block_number) <= int(chunk_hi)
                    and int(fill.timestamp) >= int(start_ts)
                ]
                if not filtered:
                    return
                filtered.sort(key=lambda item: (item.block_number, item.tx, item.log_index, item.trader))
                part_seq += 1
                batch_name = (
                    f"history_b{int(chunk_lo)}_{int(chunk_hi)}_part{part_seq:04d}"
                )
                self._queue_put(
                    self._history_queue,
                    IngestBatch(
                        source="history",
                        live=False,
                        batch_name=batch_name,
                        fills=tuple(filtered),
                    ),
                )

            try:
                self._retry_network_call(
                    f"history chunk {chunk_lo}->{chunk_hi}",
                    lambda: self._stream_actor_batches(
                        int(chunk_lo),
                        int(chunk_hi),
                        fetch_urls=list(history_logs_urls),
                        block_ts_urls=list(dict.fromkeys(list(history_ts_urls) + list(history_logs_urls[:3]))),
                        on_fills=on_fills,
                        step_blocks=int(self.config.history_fetch_step_blocks),
                    ),
                    attempts=3,
                )
            except Exception as exc:
                self.log(
                    f"history chunk retry deferred blocks={chunk_lo}->{chunk_hi} "
                    f"err={resilient.describe_exception(exc)}"
                )
                time.sleep(5.0)
                continue
            chunk_lo = int(chunk_hi) + 1

        self._queue_put(
            self._history_queue,
            WorkerEvent(source="history", event_type="done", detail="history drained"),
        )

    def _tail_worker(self, *, start_block: int, max_polls: int) -> None:
        cursor = int(start_block)
        min_live_block = int(start_block) + 1
        span = max(1, int(self.config.live_fetch_block_span))
        polls = 0
        live_logs_urls = self._live_logs_urls()
        live_ts_urls = self._live_ts_urls()

        while not self._stop_event.is_set():
            try:
                latest_block = self._retry_network_call(
                    "live latest block",
                    lambda: self._latest_block_number(use_live=True),
                    attempts=4,
                )
            except Exception as exc:
                self.log(f"live latest block deferred err={resilient.describe_exception(exc)}")
                time.sleep(max(2.0, float(self.config.live_poll_interval_seconds)))
                continue
            if latest_block >= min_live_block and latest_block > cursor:
                if int(latest_block) - int(cursor) > int(span):
                    from_block = max(min_live_block, int(cursor) + 1)
                    to_block = min(int(latest_block), int(from_block) + int(span) - 1)
                else:
                    to_block = int(latest_block)
                    from_block = max(min_live_block, int(to_block) - int(span) + 1)
                self.log(f"live tail blocks={from_block}->{to_block}")
                part_seq = 0

                def on_fills(decoded_fills: Sequence[CanonicalTraderFill]) -> None:
                    nonlocal part_seq
                    if not decoded_fills:
                        return
                    ordered = sorted(
                        decoded_fills,
                        key=lambda item: (item.block_number, item.tx, item.log_index, item.trader),
                    )
                    part_seq += 1
                    batch_name = f"live_{from_block}_{to_block}_part{part_seq:04d}"
                    self._queue_put(
                        self._live_queue,
                        IngestBatch(
                            source="live",
                            live=True,
                            batch_name=batch_name,
                            fills=tuple(ordered),
                        ),
                    )

                try:
                    self._retry_network_call(
                        f"live tail {from_block}->{to_block}",
                        lambda: self._stream_actor_batches(
                            int(from_block),
                            int(to_block),
                            fetch_urls=list(live_logs_urls),
                            block_ts_urls=list(dict.fromkeys(list(live_ts_urls) + list(live_logs_urls[:3]))),
                            on_fills=on_fills,
                            step_blocks=int(self.config.fetch_step_blocks),
                        ),
                        attempts=3,
                    )
                except Exception as exc:
                    self.log(
                        f"live tail deferred blocks={from_block}->{to_block} "
                        f"err={resilient.describe_exception(exc)}"
                    )
                    time.sleep(max(2.0, float(self.config.live_poll_interval_seconds)))
                    continue
                cursor = int(to_block)

            polls += 1
            if int(max_polls) > 0 and polls >= int(max_polls):
                break
            time.sleep(max(1.0, float(self.config.live_poll_interval_seconds)))

        self._queue_put(
            self._live_queue,
            WorkerEvent(source="live", event_type="done", detail="live tail stopped"),
        )

    def _latest_block_number(self, *, use_live: bool) -> int:
        urls = self._live_ts_urls() if use_live else self._history_ts_urls()
        latest_hex = rpc.rpc(
            "eth_blockNumber",
            [],
            urls=list(urls),
            timeout=30,
            retries=8,
        )
        return int(str(latest_hex), 16)

    def _stream_actor_batches(
        self,
        from_block: int,
        to_block: int,
        *,
        fetch_urls: List[str],
        block_ts_urls: List[str],
        on_fills,
        step_blocks: int,
    ) -> None:
        topic0 = decoder.ensure_0x_hex(fetch_constants.ORDERFILLED_TOPIC0)

        def on_logs(logs_batch: List[dict]) -> None:
            self._decode_actor_fill_chunks(
                logs_batch,
                block_ts_urls=block_ts_urls,
                on_fills=on_fills,
            )

        rpc.fetch_logs(
            addresses=list(self.config.exchange_addresses),
            from_block=int(from_block),
            to_block=int(to_block),
            urls=list(fetch_urls),
            topics=[topic0],
            on_logs=on_logs,
            step=max(1, int(step_blocks)),
            tail_split_interval_sec=float(self.config.fetch_tail_split_interval_seconds),
        )

    def _decode_actor_fill_chunks(
        self,
        logs_batch: Sequence[dict],
        *,
        block_ts_urls: List[str],
        on_fills,
    ) -> None:
        actor_rows: List[Tuple[str, int, int, str, str, str, float, float, str, int]] = []
        token_ids: List[str] = []
        block_ts_cache: Dict[int, int] = {}
        block_ts_timeout = int(fetch_constants.PREFETCH_RPC_TIMEOUT_SEC)
        emit_batch_size = max(100, int(self.config.decode_emit_batch_size))

        def flush_actor_rows() -> None:
            nonlocal actor_rows, token_ids
            if not actor_rows:
                return
            self.market_resolver.ensure_tokens(token_ids)
            out: List[CanonicalTraderFill] = []
            for raw_fill_id, block_number, timestamp, trader, token_id, raw_side, qty, raw_price, tx, log_index in actor_rows:
                fill = self.market_resolver.normalize_fill(
                    raw_fill_id=raw_fill_id,
                    block_number=int(block_number),
                    timestamp=timestamp,
                    trader=trader,
                    token_id=token_id,
                    raw_side=raw_side,
                    qty_shares=qty,
                    raw_price=raw_price,
                    tx=tx,
                    log_index=log_index,
                )
                if fill is not None:
                    out.append(fill)
            actor_rows = []
            token_ids = []
            if out:
                on_fills(out)

        for log in logs_batch:
            if not isinstance(log, dict):
                continue
            block_hex = log.get("blockNumber")
            if not isinstance(block_hex, str):
                continue
            try:
                block_number = int(block_hex, 16)
            except ValueError:
                continue
            block_timestamp = block_ts_cache.get(block_number)
            if block_timestamp is None:
                block_timestamp = rpc.block_ts_multi_provider(
                    block_number,
                    log_every=25000,
                    urls=block_ts_urls,
                    timeout=block_ts_timeout,
                    retries=1,
                    wallclock_timeout=float(block_ts_timeout),
                )
                block_ts_cache[block_number] = int(block_timestamp)

            ev = decoder.decode_orderfilled(log, block_timestamp=int(block_timestamp))
            if not ev:
                continue
            maker_side = str(ev.get("side") or "").upper()
            if maker_side not in {"BUY", "SELL"}:
                continue
            taker_side = opposite_side(maker_side)
            qty = float(ev.get("size_token") or 0.0)
            raw_price = float(ev.get("price") or 0.0)
            timestamp = int(ev.get("timestamp") or 0)
            tx = str(ev.get("tx") or "")
            log_index = int(ev.get("log_index") or 0)
            token_id = str(ev.get("token_id") or "")
            raw_fill_id = f"{tx}:{log_index}"
            if qty <= 0 or raw_price <= 0 or raw_price >= 1 or not token_id or not tx:
                continue
            actor_rows.append(
                (
                    raw_fill_id,
                    block_number,
                    timestamp,
                    str(ev.get("maker") or ""),
                    token_id,
                    maker_side,
                    qty,
                    raw_price,
                    tx,
                    log_index,
                )
            )
            actor_rows.append(
                (
                    raw_fill_id,
                    block_number,
                    timestamp,
                    str(ev.get("taker") or ""),
                    token_id,
                    taker_side,
                    qty,
                    raw_price,
                    tx,
                    log_index,
                )
            )
            token_ids.append(token_id)
            if len(actor_rows) >= emit_batch_size:
                flush_actor_rows()

        flush_actor_rows()

    def _ingest_fill(self, fill: CanonicalTraderFill, *, live: bool) -> None:
        self._record_history(fill)
        if not live:
            return
        if self.trading_enabled_ts is None or int(fill.timestamp) < int(self.trading_enabled_ts):
            return
        if fill.trade_day != self.selected_trade_day:
            return
        key = (fill.trader, fill.event_key)
        selection = self.selected_traders.get(fill.trader)
        state = self.live_signal_states.get(key)
        threshold_usdc: Optional[float] = None
        if selection is not None and float(selection.p50_event_net_position_usdc) > 0:
            threshold_usdc = float(selection.p50_event_net_position_usdc)
        elif state is not None and float(state.threshold_usdc) > 0 and (state.initial_trigger_seen or key in self.open_positions):
            threshold_usdc = float(state.threshold_usdc)
        if threshold_usdc is None or threshold_usdc <= 0:
            return
        self._update_live_signal(fill, threshold_usdc=float(threshold_usdc))

    def _record_history(self, fill: CanonicalTraderFill) -> None:
        trader_day_events = self.history_events_by_day[fill.trade_day].setdefault(fill.trader, {})
        exposure = trader_day_events.get(fill.event_key)
        if exposure is None:
            exposure = EventExposure()
            trader_day_events[fill.event_key] = exposure
        exposure.add_fill(fill)

        trader_day_counts = self.history_counts_by_day[fill.trade_day]
        counters = trader_day_counts.get(fill.trader)
        if counters is None:
            counters = DailyTraderCounters()
            trader_day_counts[fill.trader] = counters
        counters.add_fill(fill)

    def _ensure_trade_day(self, trade_day: str) -> None:
        if self.selected_trade_day is None:
            return
        current = date.fromisoformat(self.selected_trade_day)
        target = date.fromisoformat(str(trade_day))
        while current < target:
            current = current + timedelta(days=1)
            self._refresh_selection_for_day(current.isoformat())

    def _refresh_selection_for_day(self, trade_day: str) -> None:
        self.selected_traders = self._build_selection_for_day(trade_day)
        self.selected_trade_day = str(trade_day)
        self._prune_history_for_trade_day(trade_day)
        ranked = sorted(self.selected_traders.values(), key=lambda item: item.selected_rank)
        if not ranked:
            self.log(f"selection day={trade_day} no eligible traders")
            return
        summary = ", ".join(
            [
                f"{item.selected_rank}:{item.trader[:8]} p50={item.p50_event_net_position_usdc:.2f} pnl={item.rolling_pnl_usdc:.2f}"
                for item in ranked
            ]
        )
        self.log(f"selection day={trade_day} {summary}")

    def _prune_history_for_trade_day(self, trade_day: str) -> None:
        keep_from = (date.fromisoformat(str(trade_day)) - timedelta(days=int(self.strategy.lookback_days))).isoformat()
        drop_days = [day_text for day_text in self.history_events_by_day.keys() if str(day_text) < keep_from]
        for day_text in drop_days:
            self.history_events_by_day.pop(day_text, None)
            self.history_counts_by_day.pop(day_text, None)

    def _build_selection_for_day(self, trade_day: str) -> Dict[str, TraderSelection]:
        target_day = date.fromisoformat(str(trade_day))
        window_days = [(target_day - timedelta(days=offset)).isoformat() for offset in range(self.strategy.lookback_days, 0, -1)]

        agg_counts: Dict[str, DailyTraderCounters] = {}
        agg_active_days: Dict[str, int] = defaultdict(int)
        agg_events: Dict[str, Dict[str, EventExposure]] = defaultdict(dict)

        for day_text in window_days:
            daily_counts = self.history_counts_by_day.get(day_text, {})
            for trader, counters in daily_counts.items():
                bucket = agg_counts.get(trader)
                if bucket is None:
                    bucket = DailyTraderCounters()
                    agg_counts[trader] = bucket
                bucket.extend(counters)
                agg_active_days[trader] += 1

            daily_events = self.history_events_by_day.get(day_text, {})
            for trader, event_map in daily_events.items():
                target_events = agg_events[trader]
                for event_key, exposure in event_map.items():
                    bucket = target_events.get(event_key)
                    if bucket is None:
                        bucket = EventExposure()
                        target_events[event_key] = bucket
                    bucket.extend(exposure)

        if not agg_counts:
            return {}

        event_mark_cache: Dict[str, Optional[float]] = {}
        anchor_tokens = []
        for event_map in agg_events.values():
            for event_key in event_map:
                market = self.market_resolver.market_for_event(event_key)
                if market is not None:
                    anchor_tokens.append(market.anchor_token_id)
        for token_id in sorted(set(anchor_tokens)):
            self.price_client.get_snapshot(token_id, force=False)

        candidates: List[Tuple[str, float, Optional[float], DailyTraderCounters, int]] = []
        skew_lookup: Dict[str, Optional[float]] = {}
        for trader, counters in agg_counts.items():
            event_map = agg_events.get(trader, {})
            event_sizes = []
            event_pnls = []
            rolling_pnl = 0.0
            for event_key, exposure in event_map.items():
                event_sizes.append(abs(float(exposure.signed_position_usdc)))
                settle_price = event_mark_cache.get(event_key)
                if settle_price is None and event_key not in event_mark_cache:
                    settle_price = self._event_mark_price(event_key)
                    event_mark_cache[event_key] = settle_price
                if settle_price is None:
                    continue
                pnl = exposure.mark_pnl(settle_price)
                event_pnls.append(pnl)
                rolling_pnl += float(pnl)

            p50_position = median(event_sizes) if event_sizes else 0.0
            skew_metric = self._top1pct_profit_share(event_pnls)
            skew_lookup[trader] = skew_metric
            candidates.append((trader, float(rolling_pnl), p50_position, counters, int(agg_active_days.get(trader, 0))))

        candidates.sort(key=lambda item: (-float(item[1]), item[0]))
        pool = candidates[: int(self.strategy.rank_pool_size)]
        if not pool:
            return {}

        if len(pool) > int(self.strategy.follow_trader_count):
            drop_count = len(pool) - int(self.strategy.follow_trader_count)
            ranked_pool = []
            for source_rank, item in enumerate(pool, start=1):
                trader = item[0]
                skew = skew_lookup.get(trader)
                skew_sort = float(skew) if skew is not None and math.isfinite(float(skew)) else float("inf")
                ranked_pool.append((source_rank, item, skew_sort))
            ranked_pool.sort(key=lambda row: (-row[2], -row[0], row[1][0]))
            drop_traders = {row[1][0] for row in ranked_pool[:drop_count]}
            pool = [item for item in pool if item[0] not in drop_traders]

        selected: Dict[str, TraderSelection] = {}
        source_rank_lookup = {item[0]: rank for rank, item in enumerate(candidates[: int(self.strategy.rank_pool_size)], start=1)}
        for selected_rank, item in enumerate(pool, start=1):
            trader, rolling_pnl, p50_position, counters, active_days = item
            selection = TraderSelection(
                trade_day=str(trade_day),
                trader=str(trader),
                source_rank=int(source_rank_lookup.get(trader, selected_rank)),
                selected_rank=int(selected_rank),
                rolling_pnl_usdc=float(rolling_pnl),
                p50_event_net_position_usdc=float(p50_position or 0.0),
                top1pct_event_profit_share=skew_lookup.get(trader),
                active_days=int(active_days),
                fill_count=int(counters.fill_count),
                total_notional_usdc=float(counters.total_notional_usdc),
            )
            selected[selection.trader] = selection
        return selected

    def _event_mark_price(self, event_key: str) -> Optional[float]:
        market = self.market_resolver.market_for_event(event_key)
        if market is None:
            return None
        snapshot = self.price_client.get_snapshot(market.anchor_token_id, force=False)
        midpoint = snapshot.midpoint
        if midpoint is not None:
            threshold = float(self.strategy.ranking_implied_win_threshold)
            if midpoint >= threshold:
                return 1.0
            if midpoint <= (1.0 - threshold):
                return 0.0
            return float(midpoint)
        return self.market_resolver.official_anchor_settle(event_key)

    @staticmethod
    def _top1pct_profit_share(event_pnls: Sequence[float]) -> Optional[float]:
        if not event_pnls:
            return None
        net_profit = sum(float(value) for value in event_pnls)
        if net_profit <= 0:
            return None
        top_count = max(1, int(math.ceil(len(event_pnls) * 0.01)))
        top_profit = sum(max(float(value), 0.0) for value in sorted(event_pnls, reverse=True)[:top_count])
        return float(top_profit) / float(net_profit)

    def _update_live_signal(self, fill: CanonicalTraderFill, *, threshold_usdc: float) -> None:
        key = (fill.trader, fill.event_key)
        state = self.live_signal_states.get(key)
        if state is None:
            state = LiveSignalState()
            self.live_signal_states[key] = state
        state.threshold_usdc = float(threshold_usdc)

        if fill.axis_side == "BUY":
            state.signed_position_usdc += float(fill.axis_notional_usdc)
        else:
            state.signed_position_usdc -= float(fill.axis_notional_usdc)

        abs_position = abs(float(state.signed_position_usdc))
        direction = sign_int(state.signed_position_usdc)
        threshold = float(state.threshold_usdc)
        if threshold <= 0 or direction == 0:
            return

        open_position = self.open_positions.get(key)
        pending_open = self._has_pending_action(key, {"OPEN"})
        pending_close_or_reverse = self._has_pending_action(key, {"CLOSE", "REVERSE"})

        if open_position is None and (not state.initial_trigger_seen) and (not pending_open) and abs_position >= threshold:
            self._schedule_action(
                action_type="OPEN",
                execute_ts=int(fill.timestamp) + int(self.strategy.entry_delay_seconds),
                trader=fill.trader,
                event_key=fill.event_key,
                direction=direction,
                anchor_token_id=fill.anchor_token_id,
                question=fill.question,
                threshold_usdc=threshold,
                trigger_ts=int(fill.timestamp),
                reason="initial_threshold",
            )
            state.initial_trigger_seen = True
            return

        if open_position is None:
            last_direction = self.last_position_direction.get(key)
            if state.initial_trigger_seen and last_direction is not None and direction != last_direction and abs_position >= threshold and not pending_open:
                self._schedule_action(
                    action_type="OPEN",
                    execute_ts=int(fill.timestamp) + int(self.strategy.entry_delay_seconds),
                    trader=fill.trader,
                    event_key=fill.event_key,
                    direction=direction,
                    anchor_token_id=fill.anchor_token_id,
                    question=fill.question,
                    threshold_usdc=threshold,
                    trigger_ts=int(fill.timestamp),
                    reason="reverse_reopen_threshold",
                )
            return

        if pending_close_or_reverse:
            return

        if direction != open_position.direction:
            if abs_position >= threshold:
                self._drop_pending_actions(key, {"CLOSE", "REVERSE"})
                self._schedule_action(
                    action_type="REVERSE",
                    execute_ts=int(fill.timestamp) + int(self.strategy.entry_delay_seconds),
                    trader=fill.trader,
                    event_key=fill.event_key,
                    direction=direction,
                    anchor_token_id=fill.anchor_token_id,
                    question=fill.question,
                    threshold_usdc=threshold,
                    trigger_ts=int(fill.timestamp),
                    reason="reverse_threshold",
                )
            else:
                self._schedule_action(
                    action_type="CLOSE",
                    execute_ts=int(fill.timestamp) + int(self.strategy.entry_delay_seconds),
                    trader=fill.trader,
                    event_key=fill.event_key,
                    direction=0,
                    anchor_token_id=fill.anchor_token_id,
                    question=fill.question,
                    threshold_usdc=threshold,
                    trigger_ts=int(fill.timestamp),
                    reason="reverse_below_threshold",
                )

    def _has_pending_action(self, key: Tuple[str, str], action_types: set[str]) -> bool:
        trader, event_key = key
        for action in self.pending_actions:
            if action.trader == trader and action.event_key == event_key and action.action_type in action_types:
                return True
        return False

    def _drop_pending_actions(self, key: Tuple[str, str], action_types: set[str]) -> None:
        trader, event_key = key
        self.pending_actions = [
            action
            for action in self.pending_actions
            if not (action.trader == trader and action.event_key == event_key and action.action_type in action_types)
        ]
        heapq.heapify(self.pending_actions)

    def _schedule_action(
        self,
        *,
        action_type: str,
        execute_ts: int,
        trader: str,
        event_key: str,
        direction: int,
        anchor_token_id: str,
        question: str,
        threshold_usdc: float,
        trigger_ts: int,
        reason: str,
    ) -> None:
        key = (trader, event_key)
        if action_type == "OPEN" and self._has_pending_action(key, {"OPEN"}):
            return
        if action_type == "CLOSE" and self._has_pending_action(key, {"CLOSE", "REVERSE"}):
            return
        if action_type == "REVERSE" and self._has_pending_action(key, {"REVERSE"}):
            return
        self._action_seq += 1
        heapq.heappush(
            self.pending_actions,
            PendingAction(
                execute_ts=int(execute_ts),
                action_seq=int(self._action_seq),
                action_type=str(action_type),
                trader=str(trader),
                event_key=str(event_key),
                direction=int(direction),
                anchor_token_id=str(anchor_token_id),
                question=str(question),
                threshold_usdc=float(threshold_usdc),
                trigger_ts=int(trigger_ts),
                reason=str(reason),
            ),
        )

    def _process_due_actions(self, now_ts: int) -> None:
        while self.pending_actions and int(self.pending_actions[0].execute_ts) <= int(now_ts):
            action = heapq.heappop(self.pending_actions)
            self._execute_action(action, now_ts=int(now_ts))

    def _resolve_execution_price(self, *, token_id: str, for_direction: int, opening_trade: bool) -> Optional[float]:
        snapshot = self.price_client.get_snapshot(token_id, force=True)
        if opening_trade:
            if int(for_direction) > 0:
                price = snapshot.buy_price
            else:
                price = snapshot.sell_price
        else:
            if int(for_direction) > 0:
                price = snapshot.sell_price
            else:
                price = snapshot.buy_price
        if price is not None:
            return float(price)
        return snapshot.midpoint

    def _execute_action(self, action: PendingAction, *, now_ts: int) -> None:
        key = (action.trader, action.event_key)
        if action.action_type == "OPEN":
            if key in self.open_positions:
                return
            self._open_position(action, now_ts=now_ts)
            self._reevaluate_after_execution(key, now_ts=now_ts)
            return

        if action.action_type == "CLOSE":
            if key not in self.open_positions:
                return
            self._close_position(key, now_ts=now_ts, reason=action.reason, action_type="CLOSE")
            self._reevaluate_after_execution(key, now_ts=now_ts)
            return

        if action.action_type == "REVERSE":
            if key not in self.open_positions:
                return
            closed = self._close_position(key, now_ts=now_ts, reason=action.reason, action_type="REVERSE_CLOSE")
            if closed:
                self._open_position(action, now_ts=now_ts, action_type="REVERSE_OPEN")
            self._reevaluate_after_execution(key, now_ts=now_ts)

    def _open_position(self, action: PendingAction, *, now_ts: int, action_type: str = "OPEN") -> None:
        price = self._resolve_execution_price(
            token_id=action.anchor_token_id,
            for_direction=action.direction,
            opening_trade=True,
        )
        if price is None or price <= 0 or price >= 1:
            self.log(f"skip {action_type.lower()} {action.trader[:8]} event={action.event_key[:10]} invalid_price={price}")
            return

        if int(action.direction) > 0:
            qty = float(self.strategy.fixed_notional_usdc) / float(price)
        else:
            denom = max(1e-9, 1.0 - float(price))
            qty = float(self.strategy.fixed_notional_usdc) / float(denom)
        fee = float(self.strategy.fixed_notional_usdc) * float(self.strategy.fee_rate)
        self.cumulative_realized_pnl_usdc -= fee
        self.open_positions[(action.trader, action.event_key)] = OpenPosition(
            trader=action.trader,
            event_key=action.event_key,
            question=action.question,
            anchor_token_id=action.anchor_token_id,
            direction=int(action.direction),
            qty_shares=float(qty),
            entry_price=float(price),
            entry_ts=int(now_ts),
            fees_paid_usdc=float(fee),
        )
        self.last_position_direction[(action.trader, action.event_key)] = int(action.direction)
        self._write_trade(
            ExecutedTrade(
                timestamp=int(now_ts),
                trader=action.trader,
                event_key=action.event_key,
                question=action.question,
                action_type=action_type,
                direction=int(action.direction),
                price=float(price),
                qty_shares=float(qty),
                fee_usdc=float(fee),
                pnl_delta_usdc=float(-fee),
                cumulative_pnl_usdc=float(self.cumulative_realized_pnl_usdc),
                reason=action.reason,
            )
        )

    def _close_position(self, key: Tuple[str, str], *, now_ts: int, reason: str, action_type: str) -> bool:
        position = self.open_positions.pop(key, None)
        if position is None:
            return False
        price = self._resolve_execution_price(
            token_id=position.anchor_token_id,
            for_direction=position.direction,
            opening_trade=False,
        )
        if price is None or price <= 0 or price >= 1:
            self.open_positions[key] = position
            self.log(f"skip close {position.trader[:8]} event={position.event_key[:10]} invalid_price={price}")
            return False

        gross_pnl = (
            (float(price) - float(position.entry_price)) * float(position.qty_shares)
            if int(position.direction) > 0
            else (float(position.entry_price) - float(price)) * float(position.qty_shares)
        )
        fee = float(self.strategy.fixed_notional_usdc) * float(self.strategy.fee_rate)
        pnl_delta = float(gross_pnl) - float(fee)
        self.cumulative_realized_pnl_usdc += pnl_delta
        self.last_position_direction[key] = int(position.direction)
        self._write_trade(
            ExecutedTrade(
                timestamp=int(now_ts),
                trader=position.trader,
                event_key=position.event_key,
                question=position.question,
                action_type=action_type,
                direction=int(position.direction),
                price=float(price),
                qty_shares=float(position.qty_shares),
                fee_usdc=float(fee),
                pnl_delta_usdc=float(pnl_delta),
                cumulative_pnl_usdc=float(self.cumulative_realized_pnl_usdc),
                reason=str(reason),
            )
        )
        return True

    def _close_near_resolved_positions(self, now_ts: int) -> None:
        threshold = float(self.strategy.close_near_resolved_threshold)
        for key, position in list(self.open_positions.items()):
            snapshot = self.price_client.get_snapshot(position.anchor_token_id, force=True)
            midpoint = snapshot.midpoint
            if midpoint is None:
                continue
            if midpoint >= threshold or midpoint <= (1.0 - threshold):
                self._drop_pending_actions(key, {"CLOSE", "REVERSE"})
                self._close_position(key, now_ts=now_ts, reason="near_resolved", action_type="CLOSE")

    def _reevaluate_after_execution(self, key: Tuple[str, str], *, now_ts: int) -> None:
        trader, event_key = key
        if self.selected_trade_day is None:
            return
        selection = self.selected_traders.get(trader)
        state = self.live_signal_states.get(key)
        if state is None:
            return
        threshold = (
            float(selection.p50_event_net_position_usdc)
            if selection is not None and float(selection.p50_event_net_position_usdc) > 0
            else float(state.threshold_usdc)
        )
        if threshold <= 0:
            return
        direction = sign_int(state.signed_position_usdc)
        abs_position = abs(float(state.signed_position_usdc))
        open_position = self.open_positions.get(key)
        market = self.market_resolver.market_for_event(event_key)
        if market is None:
            return

        if open_position is None:
            last_direction = self.last_position_direction.get(key)
            if state.initial_trigger_seen and last_direction is not None and direction != 0 and direction != last_direction and abs_position >= threshold and not self._has_pending_action(key, {"OPEN"}):
                self._schedule_action(
                    action_type="OPEN",
                    execute_ts=int(now_ts) + int(self.strategy.entry_delay_seconds),
                    trader=trader,
                    event_key=event_key,
                    direction=direction,
                    anchor_token_id=market.anchor_token_id,
                    question=market.question,
                    threshold_usdc=threshold,
                    trigger_ts=int(now_ts),
                    reason="post_execution_reopen",
                )
            return

        if direction == 0:
            if not self._has_pending_action(key, {"CLOSE", "REVERSE"}):
                self._schedule_action(
                    action_type="CLOSE",
                    execute_ts=int(now_ts) + int(self.strategy.entry_delay_seconds),
                    trader=trader,
                    event_key=event_key,
                    direction=0,
                    anchor_token_id=market.anchor_token_id,
                    question=market.question,
                    threshold_usdc=threshold,
                    trigger_ts=int(now_ts),
                    reason="post_execution_flat",
                )
            return

        if direction != open_position.direction and not self._has_pending_action(key, {"CLOSE", "REVERSE"}):
            action_type = "REVERSE" if abs_position >= threshold else "CLOSE"
            self._schedule_action(
                action_type=action_type,
                execute_ts=int(now_ts) + int(self.strategy.entry_delay_seconds),
                trader=trader,
                event_key=event_key,
                direction=direction if action_type == "REVERSE" else 0,
                anchor_token_id=market.anchor_token_id,
                question=market.question,
                threshold_usdc=threshold,
                trigger_ts=int(now_ts),
                reason="post_execution_flip",
            )

    def _write_trade(self, trade: ExecutedTrade) -> None:
        row = asdict(trade)
        mode = "a" if self._trades_header_written else "w"
        with self.config.trades_csv.open(mode, newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(row.keys()))
            if not self._trades_header_written:
                writer.writeheader()
                self._trades_header_written = True
            writer.writerow(row)
        self.log(
            f"trade {trade.action_type.lower()} trader={trade.trader[:8]} event={trade.event_key[:10]} dir={trade.direction} price={trade.price:.4f} pnl_delta={trade.pnl_delta_usdc:.4f} cum={trade.cumulative_pnl_usdc:.4f} reason={trade.reason}"
        )
