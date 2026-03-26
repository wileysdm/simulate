from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from pathlib import Path

ROOT_DIR = str(Path(__file__).resolve().parents[1])
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from simulate.sim_config import DEFAULT_CONFIG
from simulate.sim_core import SimulationEngine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the real-time follower simulation engine.")
    parser.add_argument("--max-polls", type=int, default=0, help="Stop after N live polling rounds; 0 means run forever.")
    parser.add_argument("--backfill-days", type=int, default=DEFAULT_CONFIG.backfill_days)
    parser.add_argument("--poll-interval-seconds", type=int, default=DEFAULT_CONFIG.live_poll_interval_seconds)
    parser.add_argument("--fixed-notional", type=float, default=DEFAULT_CONFIG.strategy.fixed_notional_usdc)
    parser.add_argument("--fee-rate", type=float, default=DEFAULT_CONFIG.strategy.fee_rate)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    strategy = replace(
        DEFAULT_CONFIG.strategy,
        fixed_notional_usdc=float(args.fixed_notional),
        fee_rate=float(args.fee_rate),
    )
    config = replace(
        DEFAULT_CONFIG,
        backfill_days=int(args.backfill_days),
        live_poll_interval_seconds=int(args.poll_interval_seconds),
        strategy=strategy,
    )
    engine = SimulationEngine(config)
    engine.run(max_polls=int(args.max_polls))


if __name__ == "__main__":
    main()
