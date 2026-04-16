from __future__ import annotations

from pathlib import Path

from core.config import DATA_DIR
from core.market_cache import MARKETS, latest_daily_yyyymmdd, parquet_max_yyyymmdd


def daily_fingerprint(
    daily_base_dir: str | Path = DATA_DIR / "daily",
    cache_base_dir: str | Path = DATA_DIR / "cache",
) -> str:
    daily_root = Path(daily_base_dir)
    cache_root = Path(cache_base_dir)
    parts: list[str] = []

    for market in MARKETS:
        daily_dir = daily_root / market
        latest_daily = latest_daily_yyyymmdd(daily_dir)
        daily_count = len(list(daily_dir.glob("*.csv"))) if daily_dir.exists() else 0

        if latest_daily:
            parts.append(f"{market}:daily_latest={latest_daily}:daily_n={daily_count}")
        else:
            parts.append(f"{market}:daily=missing_or_empty")

        cache_path = cache_root / f"{market}_merged.parquet"
        if not cache_path.exists():
            parts.append(f"{market}:cache=missing")
            continue

        cache_mtime = int(cache_path.stat().st_mtime)
        cache_size = cache_path.stat().st_size
        cache_max = parquet_max_yyyymmdd(cache_path)
        if cache_max:
            parts.append(f"{market}:cache_max={cache_max}:cache_mtime={cache_mtime}:cache_size={cache_size}")
        else:
            parts.append(f"{market}:cache_mtime={cache_mtime}:cache_size={cache_size}")

    return "|".join(parts)
