from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.config import DATA_DIR


def _latest_daily_file(market: str) -> Path | None:
    daily_dir = DATA_DIR / "daily" / market
    if not daily_dir.exists():
        return None
    files = list(daily_dir.glob("krx_ohlcv_*.csv")) + list(daily_dir.glob("ohlcv_*.csv"))
    if not files:
        return None

    def _extract_yyyymmdd(path: Path) -> str:
        stem = path.stem
        for token in reversed(stem.split("_")):
            if len(token) == 8 and token.isdigit():
                return token
        return ""

    files = sorted(files, key=lambda path: (_extract_yyyymmdd(path), path.name))
    return files[-1]


def _cache_max_date(market: str) -> str | None:
    path = DATA_DIR / "cache" / f"{market}_merged.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path, columns=["date"])
    except Exception:
        return None
    if df.empty:
        return None
    dt = pd.to_datetime(df["date"], errors="coerce").max()
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def _load_today_picks() -> dict | None:
    path = DATA_DIR / "picks" / "today_picks.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def main() -> int:
    print("== Daily Output Check ==")

    for market in ("kospi", "kosdaq"):
        latest_file = _latest_daily_file(market)
        cache_date = _cache_max_date(market)
        print(f"[{market}] latest_daily_file: {latest_file.name if latest_file else 'missing'}")
        print(f"[{market}] cache_max_date:   {cache_date or 'missing'}")

    payload = _load_today_picks()
    if not payload:
        print("[picks] today_picks.json: missing_or_invalid")
        return 2

    source = payload.get("source", {})
    summary = payload.get("summary", {})
    print(f"[picks] trade_date:      {payload.get('trade_date')}")
    print(f"[picks] generated_at:   {payload.get('generated_at')}")
    print(f"[picks] markets:        {source.get('markets')}")
    print(f"[picks] strategy:       {source.get('strategy_key')}")
    print(f"[picks] pick_count:     {summary.get('pick_count')}")
    print(f"[picks] per_market:     {summary.get('per_market_counts')}")

    picks = payload.get("picks", [])
    if picks:
        print("[picks] top_entries:")
        for item in picks[:5]:
            print(
                f"  - {item.get('market')} {item.get('ticker')} "
                f"{item.get('name')} score={item.get('score')}"
            )
    else:
        print("[picks] top_entries: none")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
