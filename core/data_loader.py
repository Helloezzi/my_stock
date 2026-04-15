from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import streamlit as st

from core.config import DATA_DIR

DAILY_FILE_RE = re.compile(r"(?:krx_)?ohlcv_(\d{8})\.csv$", re.IGNORECASE)
ACTIVE_KEY = "selected_csv_name"
MARKETS = ("kospi", "kosdaq")


@dataclass(frozen=True)
class CacheUpdateResult:
    market: str
    cache_path: Path
    existing_rows: int
    added_files: int
    added_rows: int
    total_rows: int
    ok: bool
    message: str = ""


def get_active_csv_path() -> Path | None:
    name = st.session_state.get(ACTIVE_KEY)
    if not name:
        return None

    csv_path = DATA_DIR / name
    parquet_path = DATA_DIR / name.replace(".csv", ".parquet")

    if csv_path.exists() and parquet_path.exists():
        newest = max((csv_path, parquet_path), key=lambda path: path.stat().st_mtime)
        return newest
    if parquet_path.exists():
        return parquet_path
    if csv_path.exists():
        return csv_path
    return None


def list_dataset_files() -> list[Path]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(DATA_DIR.glob("*.parquet"), key=lambda path: path.stat().st_mtime, reverse=True)
    csv_files = sorted(DATA_DIR.glob("*.csv"), key=lambda path: path.stat().st_mtime, reverse=True)
    csv_files = [path for path in csv_files if not path.name.startswith("_tmp_")]

    parquet_stems = {path.stem for path in parquet_files}
    csv_files = [path for path in csv_files if path.stem not in parquet_stems]
    return parquet_files + csv_files


@st.cache_data(show_spinner=False)
def load_data(path: str, mtime: int | None = None) -> pd.DataFrame:
    del mtime

    data_path = Path(path)
    if data_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(data_path)
    else:
        df = pd.read_csv(data_path)
    return _normalize_loaded_frame(df)


def _normalize_loaded_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ticker" in out.columns:
        out["ticker"] = out["ticker"].astype("string").str.zfill(6)
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.dropna(subset=["date"])
    if "close" in out.columns:
        out = out[out["close"] > 0].copy()
    if {"ticker", "date"}.issubset(out.columns):
        out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    return out


def _list_daily_csvs(daily_dir: Path) -> list[Path]:
    if not daily_dir.exists():
        return []
    return sorted(path for path in daily_dir.glob("*.csv") if DAILY_FILE_RE.search(path.name))


def _extract_yyyymmdd(path: Path) -> Optional[str]:
    match = DAILY_FILE_RE.search(path.name)
    return match.group(1) if match else None


def _read_daily_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"ticker": "string"})
    if "date" not in df.columns or "ticker" not in df.columns:
        raise ValueError(f"invalid csv schema: {path}")

    df["ticker"] = df["ticker"].astype("string").str.zfill(6)
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    return df.dropna(subset=["date"])


def _existing_cache_dates(df: pd.DataFrame) -> set[str]:
    if df.empty or "date" not in df.columns:
        return set()
    return set(pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y%m%d").dropna().unique())


def _empty_result(market: str, cache_path: Path, message: str, *, ok: bool = False) -> tuple[pd.DataFrame, CacheUpdateResult]:
    empty = pd.DataFrame()
    return empty, CacheUpdateResult(
        market=market,
        cache_path=cache_path,
        existing_rows=0,
        added_files=0,
        added_rows=0,
        total_rows=0,
        ok=ok,
        message=message,
    )


def update_parquet_cache_for_market(
    market: str,
    daily_base_dir: str | Path = DATA_DIR / "daily",
    cache_base_dir: str | Path = DATA_DIR / "cache",
) -> tuple[pd.DataFrame, CacheUpdateResult]:
    market = market.lower().strip()
    if market not in MARKETS:
        raise ValueError("market must be 'kospi' or 'kosdaq'")

    daily_dir = Path(daily_base_dir) / market
    cache_dir = Path(cache_base_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    daily_dir.mkdir(parents=True, exist_ok=True)

    cache_path = cache_dir / f"{market}_merged.parquet"
    daily_files = _list_daily_csvs(daily_dir)

    cached = pd.read_parquet(cache_path) if cache_path.exists() else pd.DataFrame()
    existing_rows = int(len(cached))
    cached_dates = _existing_cache_dates(cached)

    if not daily_files and cached.empty:
        return _empty_result(market, cache_path, f"no daily files in {daily_dir} and no cache parquet")

    new_files = [path for path in daily_files if (_extract_yyyymmdd(path) not in cached_dates)]
    added_files = len(new_files)
    added_rows = 0

    if new_files:
        new_df = pd.concat([_read_daily_csv(path) for path in new_files], ignore_index=True)
        added_rows = int(len(new_df))
        merged = pd.concat([cached, new_df], ignore_index=True) if not cached.empty else new_df
    else:
        merged = cached

    merged = _normalize_loaded_frame(merged)
    if {"date", "ticker"}.issubset(merged.columns):
        merged = merged.drop_duplicates(subset=["date", "ticker"], keep="last")
        merged = merged.sort_values(["ticker", "date"]).reset_index(drop=True)

    if added_files:
        merged.to_parquet(cache_path, index=False)

    return merged, CacheUpdateResult(
        market=market,
        cache_path=cache_path,
        existing_rows=existing_rows,
        added_files=added_files,
        added_rows=added_rows,
        total_rows=int(len(merged)),
        ok=not merged.empty,
        message="ok" if not merged.empty else "merged is empty",
    )


def load_market_data(
    market: str,
    daily_base_dir: str | Path = DATA_DIR / "daily",
    cache_base_dir: str | Path = DATA_DIR / "cache",
) -> tuple[pd.DataFrame, CacheUpdateResult]:
    return update_parquet_cache_for_market(
        market=market,
        daily_base_dir=daily_base_dir,
        cache_base_dir=cache_base_dir,
    )


def load_all_markets(
    daily_base_dir: str | Path = DATA_DIR / "daily",
    cache_base_dir: str | Path = DATA_DIR / "cache",
) -> tuple[Dict[str, pd.DataFrame], Dict[str, CacheUpdateResult]]:
    dfs: Dict[str, pd.DataFrame] = {}
    infos: Dict[str, CacheUpdateResult] = {}

    for market in MARKETS:
        df, info = load_market_data(market, daily_base_dir=daily_base_dir, cache_base_dir=cache_base_dir)
        dfs[market] = df
        infos[market] = info

    return dfs, infos


def _latest_daily_yyyymmdd(daily_dir: Path) -> Optional[str]:
    dates = [_extract_yyyymmdd(path) for path in _list_daily_csvs(daily_dir)]
    dates = [date for date in dates if date]
    return max(dates) if dates else None


def _parquet_max_yyyymmdd(parquet_path: Path) -> Optional[str]:
    if not parquet_path.exists():
        return None

    df = pd.read_parquet(parquet_path, columns=["date"])
    if df.empty:
        return None

    dt = pd.to_datetime(df["date"], errors="coerce").max()
    if pd.isna(dt):
        return None
    return dt.strftime("%Y%m%d")


def daily_fingerprint(
    daily_base_dir: str | Path = DATA_DIR / "daily",
    cache_base_dir: str | Path = DATA_DIR / "cache",
) -> str:
    daily_root = Path(daily_base_dir)
    cache_root = Path(cache_base_dir)
    parts: list[str] = []

    for market in MARKETS:
        daily_dir = daily_root / market
        latest_daily = _latest_daily_yyyymmdd(daily_dir)
        daily_count = len(_list_daily_csvs(daily_dir))

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
        cache_max = _parquet_max_yyyymmdd(cache_path)
        if cache_max:
            parts.append(f"{market}:cache_max={cache_max}:cache_mtime={cache_mtime}:cache_size={cache_size}")
        else:
            parts.append(f"{market}:cache_mtime={cache_mtime}:cache_size={cache_size}")

    return "|".join(parts)
