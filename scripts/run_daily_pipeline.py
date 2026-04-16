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
    parser.add_argument("--tolerance", type=float, default=0.04, help="Allowed distance from MA20")
    parser.add_argument("--stop-lookback", type=int, default=10, help="Stop lookback days")
    parser.add_argument("--stop-buffer", type=float, default=0.005, help="Stop buffer ratio")
    parser.add_argument("--target-lookback", type=int, default=20, help="Target lookback days")
    parser.add_argument("--min-rr", type=float, default=1.3, help="Minimum risk/reward ratio")
    parser.add_argument("--ma5-up-days", type=int, default=0, help="Require MA5 rising for N days")
    args = parser.parse_args()

    download_markets = ("KOSPI", "KOSDAQ") if str(args.market).upper().strip() == "ALL" else (str(args.market).upper().strip(),)
    published_config = PublishedPicksConfig(
        market=args.published_market,
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
