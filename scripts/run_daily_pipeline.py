from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.daily_pipeline import run_daily_pipeline
from core.published_picks import PublishedPicksConfig


def _cli() -> int:
    parser = argparse.ArgumentParser(description="Download daily data and publish today picks in one pipeline")
    parser.add_argument("--date", default=None, help="Target date (YYYYMMDD or YYYY-MM-DD). Default: today.")
    parser.add_argument("--market", default="ALL", help="KOSPI, KOSDAQ, or ALL for the download step")
    parser.add_argument("--force", action="store_true", help="Overwrite existing daily csv if present")
    parser.add_argument("--published-market", default="ALL", help="KOSPI, KOSDAQ, or ALL for published picks")
    parser.add_argument("--strategy-key", default="pullback_rr", help="Strategy key for published picks")
    parser.add_argument("--market-filter-mode", default="close_above_ma20", help="Market filter mode")
    parser.add_argument("--limit", type=int, default=10, help="How many picks to publish per market")
    args = parser.parse_args()

    download_markets = ("KOSPI", "KOSDAQ") if str(args.market).upper().strip() == "ALL" else (str(args.market).upper().strip(),)
    published_config = PublishedPicksConfig(
        market=args.published_market,
        strategy_key=args.strategy_key,
        market_filter_mode=args.market_filter_mode,
        output_limit=args.limit,
    )
    result = run_daily_pipeline(
        yyyymmdd=args.date,
        markets=download_markets,
        force=args.force,
        published_config=published_config,
    )

    for market, download_result in result.download_results.items():
        print(f"{market}: ok={download_result.ok} rows={download_result.rows} date={download_result.yyyymmdd} msg={download_result.message}")

    if result.published_ok:
        print(f"latest_json: {result.latest_json}")
        print(f"history_json: {result.history_json}")
        print(f"history_csv: {result.history_csv}")
        return 0

    print(result.message)
    return 2


if __name__ == "__main__":
    raise SystemExit(_cli())
