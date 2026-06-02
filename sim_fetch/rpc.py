from __future__ import annotations

import queue
import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

import requests
from web3 import Web3

from . import constants

_SESSION = requests.Session()

_BLOCK_TS_CACHE: Dict[int, int] = {}
_BLOCK_TS_CACHE_MAX = 200_000

USE_ESTIMATED_TS = False
_EST_TS_T0 = 0.0
_EST_TS_B0 = 0
_EST_TS_B1 = 0
_EST_TS_SECONDS_PER_BLOCK = 0.0


def log(msg: str) -> None:
    print(msg, flush=True)


def set_estimated_ts_window(*, t0: int, t1: int, b0: int, b1: int) -> None:
    global USE_ESTIMATED_TS, _EST_TS_T0, _EST_TS_B0, _EST_TS_B1, _EST_TS_SECONDS_PER_BLOCK
    span_blocks = max(1, int(b1) - int(b0))
    _EST_TS_SECONDS_PER_BLOCK = float(int(t1) - int(t0)) / float(span_blocks)
    _EST_TS_T0 = float(int(t0))
    _EST_TS_B0 = int(b0)
    _EST_TS_B1 = int(b1)
    USE_ESTIMATED_TS = True


def estimate_block_for_ts(ts: int) -> int:
    if float(_EST_TS_SECONDS_PER_BLOCK) > 0:
        return int(float(_EST_TS_B0) + (int(ts) - float(_EST_TS_T0)) / float(_EST_TS_SECONDS_PER_BLOCK))
    return int(
        constants.ANCHOR_BN
        + (int(ts) - int(constants.ANCHOR_TS)) / max(1e-9, float(constants.BLOCKS_PER_SEC_EST))
    )


def _rpc_core(method: str, params: list, *, urls: List[str], timeout: int = 30, retries: int = 8):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    last = None
    for k in range(retries):
        url = urls[k % max(1, len(urls))]
        try:
            r = _SESSION.post(url, json=payload, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            if isinstance(j, list) and len(j) == 1 and isinstance(j[0], dict):
                j = j[0]
            if not isinstance(j, dict):
                raise RuntimeError(f"RPC returned non-dict JSON: {j}")
            if "error" in j and j["error"]:
                msg = str(j["error"])
                if "Too many requests" in msg or "rate" in msg.lower() or j["error"].get("code") in (-32090,):
                    extra = float(
                        constants.RPC_SLEEP_OVERRIDES.get(
                            url, constants.NEW_RPC_SLEEP_OVERRIDES.get(url, 0.0)
                        )
                    )
                    time.sleep(2.0 + 0.25 * k + extra + constants.SLEEP_EXTRA)
                    continue
                raise RuntimeError(j["error"])
            return j["result"]
        except Exception as e:
            last = e
            extra = float(
                constants.RPC_SLEEP_OVERRIDES.get(url, constants.NEW_RPC_SLEEP_OVERRIDES.get(url, 0.0))
            )
            time.sleep(0.35 * (k + 1) + extra + constants.SLEEP_EXTRA)
    raise RuntimeError(f"RPC failed after retries: method={method} last={last}") from last


def rpc(
    method: str,
    params: list,
    *,
    urls: List[str],
    timeout: int = 30,
    retries: int = 8,
    wallclock_timeout: Optional[float] = None,
):
    if wallclock_timeout is None:
        return _rpc_core(method, params, urls=urls, timeout=timeout, retries=retries)

    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_rpc_core, method, params, urls=urls, timeout=timeout, retries=retries)
        try:
            return fut.result(timeout=float(wallclock_timeout))
        except FutureTimeout as e:
            fut.cancel()
            raise RuntimeError(
                f"RPC wallclock timeout: method={method} timeout={float(wallclock_timeout)}"
            ) from e


def block_ts_multi_provider(
    bn: int,
    *,
    log_every: int = 5000,
    urls: Optional[List[str]] = None,
    timeout: int = 30,
    retries: int = 6,
    wallclock_timeout: Optional[float] = None,
) -> int:
    if USE_ESTIMATED_TS and _EST_TS_SECONDS_PER_BLOCK > 0:
        if int(_EST_TS_B0) <= int(bn) <= int(_EST_TS_B1):
            est = _EST_TS_T0 + (int(bn) - int(_EST_TS_B0)) * float(_EST_TS_SECONDS_PER_BLOCK)
            return int(est)
    if bn in _BLOCK_TS_CACHE:
        return int(_BLOCK_TS_CACHE[bn])
    if log_every and bn % log_every == 0:
        log(f"block ts lookup: bn={int(bn)}")
    primary_urls = list(urls or constants.TS_RPC_URLS)
    fallback_urls = primary_urls
    if urls is None:
        fallback_urls = list(dict.fromkeys(primary_urls + constants.LOGS_RPC_URLS[:3]))

    attempts = [
        {
            "urls": primary_urls,
            "timeout": int(timeout),
            "retries": int(retries),
            "wallclock_timeout": wallclock_timeout,
        },
        {
            "urls": fallback_urls,
            "timeout": max(35, int(timeout) + 5),
            "retries": max(8, int(retries) + 2),
            "wallclock_timeout": None,
        },
    ]
    last_exc: Optional[BaseException] = None
    ts = None
    for idx, attempt_cfg in enumerate(attempts):
        try:
            blk = rpc(
                "eth_getBlockByNumber",
                [hex(int(bn)), False],
                urls=attempt_cfg["urls"],
                timeout=int(attempt_cfg["timeout"]),
                retries=int(attempt_cfg["retries"]),
                wallclock_timeout=attempt_cfg["wallclock_timeout"],
            )
            ts = int(blk["timestamp"], 16) if isinstance(blk, dict) and "timestamp" in blk else None
            if ts is not None:
                break
            last_exc = RuntimeError(f"missing timestamp for block {bn}")
        except Exception as e:
            last_exc = e
        if idx + 1 < len(attempts):
            time.sleep(0.25 * (idx + 1))
    if ts is None:
        raise RuntimeError(f"cannot get block timestamp: bn={bn}") from last_exc
    if len(_BLOCK_TS_CACHE) >= _BLOCK_TS_CACHE_MAX:
        _BLOCK_TS_CACHE.clear()
    _BLOCK_TS_CACHE[int(bn)] = int(ts)
    return int(ts)


def _latest_block_and_ts(
    *,
    ts_urls: Optional[List[str]] = None,
    fallback_log_urls: Optional[List[str]] = None,
) -> Tuple[int, int]:
    primary_ts_urls = list(ts_urls or constants.TS_RPC_URLS)
    secondary_log_urls = list(fallback_log_urls or constants.LOGS_RPC_URLS[:3])
    latest_urls = list(dict.fromkeys(primary_ts_urls + secondary_log_urls))
    bn_hex = rpc(
        "eth_blockNumber",
        [],
        urls=latest_urls,
        timeout=30,
        retries=8,
        wallclock_timeout=float(constants.RPC_WALLCLOCK_RETRY_SEC),
    )
    bn = int(bn_hex, 16)
    ts = block_ts_multi_provider(bn, urls=primary_ts_urls)
    return bn, ts


def _bracket_for_ts(
    target_ts: int,
    *,
    latest_bn: int,
    latest_ts: int,
    ts_urls: Optional[List[str]] = None,
    fallback_log_urls: Optional[List[str]] = None,
) -> Tuple[int, int]:
    guess = int(
        constants.ANCHOR_BN + (target_ts - constants.ANCHOR_TS) / max(1e-9, constants.BLOCKS_PER_SEC_EST)
    )
    guess = max(0, min(int(latest_bn), int(guess)))
    radius = 256

    def ts_of(bn: int) -> int:
        primary_urls = list(ts_urls or constants.TS_RPC_URLS)
        fallback_urls = list(fallback_log_urls or constants.LOGS_RPC_URLS[:3])
        return block_ts_multi_provider(bn, urls=list(dict.fromkeys(primary_urls + fallback_urls)))

    lo = max(0, guess - radius)
    hi = min(latest_bn, guess + radius)

    for _ in range(24):
        lo_ts = ts_of(lo)
        hi_ts = ts_of(hi)
        if lo_ts <= target_ts <= hi_ts:
            return lo, hi
        radius *= 2
        if target_ts < lo_ts:
            hi = lo
            lo = max(0, lo - radius)
        else:
            lo = hi
            hi = min(latest_bn, hi + radius)
        if lo == 0 and hi == latest_bn:
            return lo, hi
    return 0, latest_bn


def _first_block_ge_ts(
    target_ts: int,
    *,
    ts_urls: Optional[List[str]] = None,
    fallback_log_urls: Optional[List[str]] = None,
) -> int:
    latest_bn, latest_ts = _latest_block_and_ts(ts_urls=ts_urls, fallback_log_urls=fallback_log_urls)
    if target_ts >= latest_ts:
        return latest_bn

    lo, hi = _bracket_for_ts(
        target_ts,
        latest_bn=latest_bn,
        latest_ts=latest_ts,
        ts_urls=ts_urls,
        fallback_log_urls=fallback_log_urls,
    )

    log(f"binary search block for ts={target_ts} (latest_bn={latest_bn}, latest_ts={latest_ts})")
    while lo < hi:
        mid = (lo + hi) // 2
        primary_urls = list(ts_urls or constants.TS_RPC_URLS)
        fallback_urls = list(fallback_log_urls or constants.LOGS_RPC_URLS[:3])
        ts = block_ts_multi_provider(mid, urls=list(dict.fromkeys(primary_urls + fallback_urls)))
        if ts >= target_ts:
            hi = mid
        else:
            lo = mid + 1
    return lo


def blocks_for_time_range(
    t0: int,
    t1: int,
    *,
    ts_urls: Optional[List[str]] = None,
    fallback_log_urls: Optional[List[str]] = None,
) -> Tuple[int, int]:
    b0 = _first_block_ge_ts(int(t0), ts_urls=ts_urls, fallback_log_urls=fallback_log_urls)
    b1 = _first_block_ge_ts(int(t1), ts_urls=ts_urls, fallback_log_urls=fallback_log_urls)
    return int(b0), int(b1)


def fetch_logs(
    *,
    addresses: List[str],
    from_block: int,
    to_block: int,
    urls: List[str],
    topics: list,
    on_logs: Callable[[List[dict]], None],
    step: int = 10,
    max_task_retries: int = 10,
    tail_split_interval_sec: Optional[float] = None,
) -> None:
    addrs = [Web3.to_checksum_address(a) for a in addresses]

    LOGS_RPC_CALL_TIMEOUT_SEC = int(constants.LOGS_RPC_CALL_TIMEOUT_SEC)
    LOGS_RPC_WALLCLOCK_TIMEOUT_SEC = float(constants.RPC_WALLCLOCK_RETRY_SEC)
    LOGS_RPC_SLOW_SECONDS = 10
    LOGS_RPC_BAN_SECONDS = 10 * 60
    HEARTBEAT_SECONDS = 60.0
    rpc_ban_until: Dict[str, float] = {}

    def _params(lo: int, hi: int):
        return [
            {
                "fromBlock": hex(int(lo)),
                "toBlock": hex(int(hi)),
                "address": addrs,
                "topics": topics,
            }
        ]

    cur = int(from_block)
    to_block = int(to_block)
    s = max(1, int(step))

    default_span_cap = min(int(s), 500)
    rpc_span_cap: Dict[str, int] = {u: int(default_span_cap) for u in urls}

    q: "queue.Queue[Tuple[int, int, int]]" = queue.Queue()
    while cur <= to_block:
        end = min(cur + s - 1, to_block)
        q.put((cur, end, 0))
        cur = end + 1

    initial_tasks = q.qsize()
    lock = threading.Lock()
    stop = threading.Event()
    t_start = time.time()
    rebalance_seconds = 60.0
    prog = {
        "calls": 0,
        "logs": 0,
        "done": 0,
        "splits": 0,
        "enqueued": int(initial_tasks),
        "last_beat": t_start,
        "last_call_ts": t_start,
    }
    stall_seconds = 45.0
    per_rpc = {u: {"calls": 0, "logs": 0} for u in urls}
    per_rpc_perf = {u: {"span": 0.0, "time": 0.0, "calls": 0} for u in urls}
    issue = {
        "malformed": 0,
        "mixed_invalid_items": 0,
        "slow_ban": 0,
        "timeout_retry": 0,
        "timeout_handover": 0,
        "rate_retry": 0,
        "rate_handover": 0,
        "range_retry": 0,
        "range_handover": 0,
        "other_retry": 0,
        "other_handover": 0,
    }

    log(
        f"[logs] start blocks={int(from_block)}-{to_block} step={s} providers={len(urls)} tasks~{initial_tasks}"
    )

    def worker(url: str):
        def _after_call_sleep():
            extra = float(
                constants.RPC_SLEEP_OVERRIDES.get(url, constants.NEW_RPC_SLEEP_OVERRIDES.get(url, 0.0))
            )
            if extra + constants.SLEEP_EXTRA > 0:
                time.sleep(extra + constants.SLEEP_EXTRA)

        while not stop.is_set():
            with lock:
                if time.time() - float(prog.get("last_call_ts", 0.0)) >= float(stall_seconds):
                    if q.qsize() > 0:
                        rpc_ban_until.clear()
                        prog["last_call_ts"] = time.time()
                until = float(rpc_ban_until.get(url, 0.0))
            now_ts = time.time()
            if until > now_ts:
                time.sleep(min(5.0, max(0.25, until - now_ts)))
                continue

            try:
                lo, hi, attempts = q.get_nowait()
            except queue.Empty:
                return
            try:
                span = int(hi) - int(lo) + 1
                with lock:
                    cap = int(rpc_span_cap.get(url, default_span_cap))
                cap = max(1, int(cap))

                if span > cap and int(lo) < int(hi):
                    run_hi = min(int(hi), int(lo) + cap - 1)
                    if run_hi < int(hi):
                        q.put((int(run_hi) + 1, int(hi), int(attempts)))
                        with lock:
                            prog["enqueued"] += 1
                    hi = int(run_hi)

                t_call0 = time.time()
                logs = rpc(
                    "eth_getLogs",
                    _params(lo, hi),
                    urls=[url],
                    timeout=int(LOGS_RPC_CALL_TIMEOUT_SEC),
                    retries=1,
                    wallclock_timeout=float(LOGS_RPC_WALLCLOCK_TIMEOUT_SEC),
                )
                if not isinstance(logs, (list, dict)):
                    ban_until = time.time() + float(LOGS_RPC_BAN_SECONDS)
                    with lock:
                        issue["malformed"] += 1
                        rpc_ban_until[url] = max(float(rpc_ban_until.get(url, 0.0)), float(ban_until))
                    if attempts < max_task_retries:
                        with lock:
                            prog["enqueued"] += 1
                        q.put((int(lo), int(hi), int(attempts) + 1))
                        _after_call_sleep()
                        continue
                    with lock:
                        prog["enqueued"] += 1
                    q.put((int(lo), int(hi), 0))
                    _after_call_sleep()
                    continue

                if isinstance(logs, list):
                    normalized_logs = [x for x in logs if isinstance(x, dict)]
                    invalid_items = int(len(logs) - len(normalized_logs))
                    if invalid_items > 0:
                        with lock:
                            issue["mixed_invalid_items"] += int(invalid_items)
                else:
                    normalized_logs = [logs]
                t_call1 = time.time()
                elapsed = float(t_call1 - t_call0)

                span_ok = int(hi) - int(lo) + 1
                with lock:
                    if url in per_rpc_perf:
                        per_rpc_perf[url]["span"] += float(span_ok)
                        per_rpc_perf[url]["time"] += float(elapsed)
                        per_rpc_perf[url]["calls"] += 1

                if elapsed >= float(LOGS_RPC_SLOW_SECONDS):
                    ban_until = time.time() + float(LOGS_RPC_BAN_SECONDS)
                    with lock:
                        issue["slow_ban"] += 1
                        rpc_ban_until[url] = max(float(rpc_ban_until.get(url, 0.0)), float(ban_until))

                with lock:
                    prog["calls"] += 1
                    prog["last_call_ts"] = time.time()
                    if url in per_rpc:
                        per_rpc[url]["calls"] += 1
                        per_rpc[url]["logs"] += len(normalized_logs)

                if normalized_logs:
                    on_logs(normalized_logs)
                    with lock:
                        prog["logs"] += len(normalized_logs)
                _after_call_sleep()
            except Exception as e:
                from concurrent.futures import TimeoutError as FutureTimeout

                cause = getattr(e, "__cause__", None)
                msg_e = str(e).lower()
                msg_c = str(cause).lower() if cause is not None else ""
                ban_until = time.time() + float(LOGS_RPC_BAN_SECONDS)
                with lock:
                    rpc_ban_until[url] = max(float(rpc_ban_until.get(url, 0.0)), float(ban_until))
                is_timeout = (
                    isinstance(cause, (requests.exceptions.Timeout, FutureTimeout))
                    or ("wallclock timeout" in msg_e)
                    or ("timed out" in msg_e)
                    or ("timed out" in msg_c)
                )

                if is_timeout:
                    if attempts < max_task_retries:
                        with lock:
                            issue["timeout_retry"] += 1
                            prog["enqueued"] += 1
                        q.put((int(lo), int(hi), int(attempts) + 1))
                        _after_call_sleep()
                        continue
                    with lock:
                        issue["timeout_handover"] += 1
                        prog["enqueued"] += 1
                    q.put((int(lo), int(hi), 0))
                    _after_call_sleep()
                    continue

                msg = str(e).lower()
                if ("too many requests" in msg) or ("429" in msg) or ("rate" in msg):
                    if attempts < max_task_retries:
                        with lock:
                            issue["rate_retry"] += 1
                            prog["enqueued"] += 1
                        q.put((int(lo), int(hi), int(attempts) + 1))
                        _after_call_sleep()
                        continue
                    with lock:
                        issue["rate_handover"] += 1
                        prog["enqueued"] += 1
                    q.put((int(lo), int(hi), 0))
                    _after_call_sleep()
                    continue
                if ("query returned more than" in msg) or ("block range" in msg) or ("too many results" in msg):
                    span_bad = int(hi) - int(lo) + 1
                    if int(lo) < int(hi):
                        with lock:
                            issue["range_retry"] += 1
                            old_cap = int(rpc_span_cap.get(url, default_span_cap))
                            new_cap = max(1, min(old_cap, max(1, span_bad // 2)))
                            rpc_span_cap[url] = int(new_cap)
                            prog["enqueued"] += 1
                        q.put((int(lo), int(hi), int(attempts) + 1))
                        _after_call_sleep()
                        continue
                    with lock:
                        issue["range_handover"] += 1
                        prog["enqueued"] += 1
                    q.put((int(lo), int(hi), 0))
                    _after_call_sleep()
                    continue
                if attempts < max_task_retries:
                    q.put((int(lo), int(hi), int(attempts) + 1))
                    with lock:
                        issue["other_retry"] += 1
                        prog["enqueued"] += 1
                else:
                    with lock:
                        issue["other_handover"] += 1
                        prog["enqueued"] += 1
                    q.put((int(lo), int(hi), 0))
                    _after_call_sleep()
                    continue
                _after_call_sleep()
            finally:
                now = time.time()
                with lock:
                    prog["done"] += 1
                    done = int(prog["done"])
                    total = int(prog["enqueued"])
                    if (done == 1) or (done >= total) or (now - float(prog["last_beat"]) >= HEARTBEAT_SECONDS):
                        prog["last_beat"] = now
                        issue_parts = []
                        for k in (
                            "timeout_retry",
                            "rate_retry",
                            "range_retry",
                            "other_retry",
                            "timeout_handover",
                            "rate_handover",
                            "range_handover",
                            "other_handover",
                        ):
                            v = int(issue.get(k, 0))
                            if v > 0:
                                issue_parts.append(f"{k}={v}")
                        issue_str = " " + " ".join(issue_parts) if issue_parts else ""
                        log(
                            f"[logs] progress done={done}/{total} calls={int(prog['calls'])} "
                            f"q~{q.qsize()} logs={int(prog['logs'])} splits={int(prog['splits'])} "
                            f"elapsed={now - t_start:.1f}s{issue_str}"
                        )
                q.task_done()

    def rebalance_loop():
        while not stop.is_set():
            time.sleep(float(rebalance_seconds))
            with lock:
                speeds = []
                for u in urls:
                    t = float(per_rpc_perf.get(u, {}).get("time", 0.0))
                    sp = float(per_rpc_perf.get(u, {}).get("span", 0.0))
                    if t > 0:
                        speeds.append(sp / t)
                if speeds:
                    max_speed = max(speeds)
                    for u in urls:
                        perf = per_rpc_perf.get(u, {})
                        t = float(perf.get("time", 0.0))
                        sp = float(perf.get("span", 0.0))
                        if t > 0 and max_speed > 0:
                            ratio = max(0.01, (sp / t) / max_speed)
                            new_cap = int(max(1, min(int(s), round(int(s) * ratio))))
                            rpc_span_cap[u] = new_cap
                for u in urls:
                    per_rpc_perf[u] = {"span": 0.0, "time": 0.0, "calls": 0}

    threads = [threading.Thread(target=worker, args=(u,), daemon=True) for u in urls]
    rebalance_thread = threading.Thread(target=rebalance_loop, daemon=True)
    for t in threads:
        t.start()
    rebalance_thread.start()
    q.join()
    stop.set()
    for t in threads:
        t.join(timeout=0.1)
    rebalance_thread.join(timeout=0.1)

    issue_summary = ", ".join([f"{k}={int(v)}" for k, v in issue.items() if int(v) > 0]) or "none"
    log(
        f"[logs] done calls={int(prog['calls'])} tasks={int(prog['done'])}/{int(prog['enqueued'])} "
        f"logs={int(prog['logs'])} splits={int(prog['splits'])} elapsed={time.time() - t_start:.1f}s issues={issue_summary}"
    )

    ranked = sorted(
        [(u, int(s.get("calls", 0)), int(s.get("logs", 0))) for u, s in per_rpc.items()],
        key=lambda x: x[2],
        reverse=True,
    )
    top_ranked = [x for x in ranked if x[1] > 0][:3]
    if top_ranked:
        parts = []
        for u, c, lg in top_ranked:
            host = str(u).split("//")[-1].split("/")[0]
            parts.append(f"{host}:calls={c},logs={lg}")
        log(f"[logs] top_providers {'; '.join(parts)}")
