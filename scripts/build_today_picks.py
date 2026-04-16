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
    args = parser.parse_args()

    config = PublishedPicksConfig(
        market=args.market,
        top_n=args.top_n,
        rank_by=args.rank_by,
        strategy_key=args.strategy_key,
        market_filter_mode=args.market_filter_mode,
        output_limit=args.limit,
    )
    outputs = publish_today_picks(config=config)
    for label, path in outputs.items():
        print(f"{label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
