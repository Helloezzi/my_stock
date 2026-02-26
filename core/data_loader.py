# core/data_loader.py
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from core.config import DATA_DIR
import streamlit as st

DATE_RE = re.compile(r"krx_ohlcv_(\d{8})\.csv$", re.IGNORECASE)

ACTIVE_KEY = "selected_csv_name"

def get_active_csv_path() -> Path | None:
    name = st.session_state.get(ACTIVE_KEY)
    if not name:
        return None

    # ✅ parquet 우선
    p_parquet = DATA_DIR / name.replace(".csv", ".parquet")
    if p_parquet.exists():
        return p_parquet

    # csv fallback
    p_csv = DATA_DIR / name
    return p_csv if p_csv.exists() else None


def list_dataset_files() -> List[Path]:
    """
    data 폴더 내 데이터셋 나열
    - parquet가 있으면 parquet 기준으로 보여주고
    - parquet 없는 csv도 보여줌
    - _tmp_ 제외
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(DATA_DIR.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    csv_files = sorted(DATA_DIR.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    csv_files = [p for p in csv_files if not p.name.startswith("_tmp_")]

    # parquet가 존재하는 csv는 중복 제거
    parquet_stems = {p.stem for p in parquet_files}
    csv_files = [p for p in csv_files if p.stem not in parquet_stems]

    return parquet_files + csv_files


@st.cache_data(show_spinner=False)
def load_data(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.suffix.lower() == ".parquet":
        df = pd.read_parquet(p)
    else:
        df = pd.read_csv(p)

    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


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


def _list_daily_csvs(daily_dir: Path) -> List[Path]:
    if not daily_dir.exists():
        return []
    files = sorted(daily_dir.glob("krx_ohlcv_*.csv"))
    # Filter only files matching pattern krx_ohlcv_YYYYMMDD.csv
    out: List[Path] = []
    for f in files:
        if DATE_RE.search(f.name):
            out.append(f)
    return out


def _extract_date_from_filename(path: Path) -> Optional[str]:
    m = DATE_RE.search(path.name)
    return m.group(1) if m else None


def _read_daily_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"ticker": "string"})
    if "date" not in df.columns or "ticker" not in df.columns:
        raise ValueError(f"invalid csv schema: {path}")

    df["ticker"] = df["ticker"].astype("string").str.zfill(6)
    # ✅ daily는 YYYYMMDD 확정이므로 여기서 format 지정
    df["date"] = pd.to_datetime(df["date"].astype("string"), format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["date"])
    return df


def _ensure_dirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def update_parquet_cache_for_market(
    market: str,
    daily_base_dir: str | Path = "data/daily",
    cache_base_dir: str | Path = "data/cache",
) -> Tuple[pd.DataFrame, CacheUpdateResult]:
    """
    Build/Update parquet cache for one market from daily CSV files.
    Returns: (df, CacheUpdateResult)
    """
    market = market.lower().strip()
    if market not in {"kospi", "kosdaq"}:
        raise ValueError("market must be 'kospi' or 'kosdaq'")

    daily_dir = Path(daily_base_dir) / market
    cache_dir = Path(cache_base_dir)
    _ensure_dirs(daily_dir, cache_dir)

    cache_path = cache_dir / f"{market}_merged.parquet"

    daily_files = _list_daily_csvs(daily_dir)
    if not daily_files:
        empty = pd.DataFrame()
        return empty, CacheUpdateResult(
            market=market,
            cache_path=cache_path,
            existing_rows=0,
            added_files=0,
            added_rows=0,
            total_rows=0,
            ok=False,
            message=f"no daily files in {daily_dir}",
        )

    # Load existing cache if present
    if cache_path.exists():
        cached = pd.read_parquet(cache_path)
        existing_rows = int(len(cached))

        if "date" in cached.columns:
            # ✅ cached['date']가 datetime이든 string이든 YYYYMMDD로 통일
            cached_dates = set(
                pd.to_datetime(cached["date"], errors="coerce").dt.strftime("%Y%m%d").dropna().unique()
            )
        else:
            cached_dates = set()
    else:
        cached = pd.DataFrame()
        existing_rows = 0
        cached_dates = set()

    # Find new files (by date)
    new_files: List[Path] = []
    for f in daily_files:
        d = _extract_date_from_filename(f)
        if d and d not in cached_dates:
            new_files.append(f)

    added_rows = 0
    added_files = 0
    if new_files:
        chunks: List[pd.DataFrame] = []
        for f in new_files:
            df = _read_daily_csv(f)
            chunks.append(df)
        new_df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        added_rows = int(len(new_df))
        added_files = int(len(new_files))

        merged = pd.concat([cached, new_df], ignore_index=True) if existing_rows else new_df
    else:
        merged = cached

    if merged.empty:
        return merged, CacheUpdateResult(
            market=market,
            cache_path=cache_path,
            existing_rows=existing_rows,
            added_files=added_files,
            added_rows=added_rows,
            total_rows=0,
            ok=False,
            message="merged is empty",
        )

    # Normalize types early
    if "ticker" in merged.columns:
        merged["ticker"] = merged["ticker"].astype("string").str.zfill(6)

    if "date" in merged.columns:
        # CSV는 YYYYMMDD string이므로 format 지정
        merged["date"] = pd.to_datetime(merged["date"], errors="coerce")

    # NaT 방지: date 파싱 실패 행 제거 (원인 추적 원하면 로그)
    if "date" in merged.columns:
        bad = merged["date"].isna().sum()
        if bad:
            merged = merged.dropna(subset=["date"])

    # Dedupe + sort once
    merged = merged.drop_duplicates(subset=["date", "ticker"], keep="last")
    merged = merged.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Remove non-trading/invalid rows (close==0 etc.)
    price_cols = [c for c in ["open", "high", "low", "close"] if c in merged.columns]

    # (optional) volume==0도 제거하고 싶으면 아래 추가
    # if "volume" in merged.columns:
    #     merged = merged[merged["volume"] > 0].copy()

    # Persist cache (overwrite)
    if new_files:
        merged.to_parquet(cache_path, index=False)

    # after date/ticker normalization
    if "close" in merged.columns:
        merged = merged[merged["close"] > 0].copy()

    return merged, CacheUpdateResult(
        market=market,
        cache_path=cache_path,
        existing_rows=existing_rows,
        added_files=added_files,
        added_rows=added_rows,
        total_rows=int(len(merged)),
        ok=True,
        message="ok",
    )


def load_market_data(
    market: str,
    daily_base_dir: str | Path = "data/daily",
    cache_base_dir: str | Path = "data/cache",
) -> Tuple[pd.DataFrame, CacheUpdateResult]:
    """Public entry: update cache if needed and return DF."""
    return update_parquet_cache_for_market(
        market=market,
        daily_base_dir=daily_base_dir,
        cache_base_dir=cache_base_dir,
    )


def load_all_markets(
    daily_base_dir: str | Path = "data/daily",
    cache_base_dir: str | Path = "data/cache",
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, CacheUpdateResult]]:
    dfs: Dict[str, pd.DataFrame] = {}
    infos: Dict[str, CacheUpdateResult] = {}
    for m in ["kospi", "kosdaq"]:
        df, info = load_market_data(m, daily_base_dir=daily_base_dir, cache_base_dir=cache_base_dir)
        dfs[m] = df
        infos[m] = info
    return dfs, infos


def daily_fingerprint(daily_base_dir: str | Path = "data/daily") -> str:
    """
    Return a stable fingerprint for daily CSV directories.
    If any new daily csv appears or files change, fingerprint changes.
    """
    base = Path(daily_base_dir)
    parts = []
    for market in ["kospi", "kosdaq"]:
        d = base / market
        if not d.exists():
            parts.append(f"{market}:missing")
            continue
        files = sorted(d.glob("krx_ohlcv_*.csv"))
        if not files:
            parts.append(f"{market}:empty")
            continue
        latest = max(files, key=lambda p: p.stat().st_mtime)
        parts.append(
            f"{market}:n={len(files)}:latest={latest.name}:mtime={int(latest.stat().st_mtime)}"
        )
    return "|".join(parts)