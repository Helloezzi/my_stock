from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.published_picks import PublishedPicksConfig, publish_today_picks


def _cli() -> int:
    parser = argparse.ArgumentParser(description="Build a compact today_picks output for lightweight/mobile viewing")
    parser.add_argument("--market", default="ALL", help="Market label: KOSPI, KOSDAQ, or ALL")
    parser.add_argument("--top-n", type=int, default=None, help="Optional top-n universe filter")
    parser.add_argument("--rank-by", default="market_cap", help="Universe rank column")
    parser.add_argument("--strategy-key", default="pullback_rr", help="Strategy key to publish")
    parser.add_argument("--market-filter-mode", default="close_above_ma20", help="Market filter mode")
    parser.add_argument("--limit", type=int, default=10, help="How many picks to publish")
    parser.add_argument("--tolerance", type=float, default=0.04, help="Allowed distance from MA20")
    parser.add_argument("--stop-lookback", type=int, default=10, help="Stop lookback days")
    parser.add_argument("--stop-buffer", type=float, default=0.005, help="Stop buffer ratio")
    parser.add_argument("--target-lookback", type=int, default=20, help="Target lookback days")
    parser.add_argument("--min-rr", type=float, default=1.3, help="Minimum risk/reward ratio")
    parser.add_argument("--ma5-up-days", type=int, default=0, help="Require MA5 rising for N days")
    args = parser.parse_args()

    config = PublishedPicksConfig(
        market=args.market,
        top_n=args.top_n,
        rank_by=args.rank_by,
        strategy_key=args.strategy_key,
        market_filter_mode=args.market_filter_mode,
        output_limit=args.limit,
        tolerance=args.tolerance,
        stop_lookback=args.stop_lookback,
        stop_buffer=args.stop_buffer,
        target_lookback=args.target_lookback,
        min_rr=args.min_rr,
        ma5_up_days=args.ma5_up_days,
    )
    outputs = publish_today_picks(config=config)
    for label, path in outputs.items():
        print(f"{label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
