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

_DAILY_RE = re.compile(r"ohlcv_(\d{8})\.csv$")

ACTIVE_KEY = "selected_csv_name"

def get_active_csv_path() -> Path | None:
    name = st.session_state.get(ACTIVE_KEY)
    if not name:
        return None

    p_csv = DATA_DIR / name
    p_parquet = DATA_DIR / name.replace(".csv", ".parquet")

    has_csv = p_csv.exists()
    has_parq = p_parquet.exists()

    if has_csv and has_parq:
        # 최신 파일 선택 (mtime)
        return p_parquet if p_parquet.stat().st_mtime >= p_csv.stat().st_mtime else p_csv

    if has_parq:
        return p_parquet
    if has_csv:
        return p_csv
    return None

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
    p = get_active_csv_path()
    df = load_data(str(p), int(p.stat().st_mtime))
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
    daily_base_dir: str | Path = DATA_DIR / "daily",
    cache_base_dir: str | Path = DATA_DIR / "cache",
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
        # ✅ daily가 없어도 cache가 있으면 cache를 그대로 사용
        if cache_path.exists():
            cached = pd.read_parquet(cache_path)

            # 타입 정규화 (기존 로직 재사용)
            if "ticker" in cached.columns:
                cached["ticker"] = cached["ticker"].astype("string").str.zfill(6)
            if "date" in cached.columns:
                cached["date"] = pd.to_datetime(cached["date"], errors="coerce")
                cached = cached.dropna(subset=["date"])

            # close>0 필터 (있으면)
            if "close" in cached.columns:
                cached = cached[cached["close"] > 0].copy()

            cached = cached.sort_values(["ticker", "date"]).reset_index(drop=True)

            return cached, CacheUpdateResult(
                market=market,
                cache_path=cache_path,
                existing_rows=int(len(cached)),
                added_files=0,
                added_rows=0,
                total_rows=int(len(cached)),
                ok=True,
                message=f"loaded existing cache (daily empty): {cache_path.name}",
            )

        # cache도 없으면 진짜로 empty
        empty = pd.DataFrame()
        return empty, CacheUpdateResult(
            market=market,
            cache_path=cache_path,
            existing_rows=0,
            added_files=0,
            added_rows=0,
            total_rows=0,
            ok=False,
            message=f"no daily files in {daily_dir} and no cache parquet",
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
    daily_base_dir: str | Path = DATA_DIR / "daily",
    cache_base_dir: str | Path = DATA_DIR / "cache",
) -> Tuple[pd.DataFrame, CacheUpdateResult]:
    """Public entry: update cache if needed and return DF."""
    return update_parquet_cache_for_market(
        market=market,
        daily_base_dir=daily_base_dir,
        cache_base_dir=cache_base_dir,
    )
def load_all_markets(
    daily_base_dir: str | Path = DATA_DIR / "daily",
    cache_base_dir: str | Path = DATA_DIR / "cache",
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, CacheUpdateResult]]:
    dfs: Dict[str, pd.DataFrame] = {}
    infos: Dict[str, CacheUpdateResult] = {}

    # 1) parquet 자동 갱신 (daily -> merged)
    update_merged_parquet_from_daily("kospi", DATA_DIR)
    update_merged_parquet_from_daily("kosdaq", DATA_DIR)

    # 2) 로드는 load_market_data로 (단, load_market_data가 갱신하지 않게 수정해야 함)
    for m in ["kospi", "kosdaq"]:
        df, info = load_market_data(m, daily_base_dir=daily_base_dir, cache_base_dir=cache_base_dir)
        dfs[m] = df
        infos[m] = info

    return dfs, infos

def daily_fingerprint(
    daily_base_dir: str | Path = DATA_DIR / "daily",
    cache_base_dir: str | Path = DATA_DIR / "cache",
) -> str:
    """
    Fingerprint for Streamlit cache invalidation.

    목표:
    - daily 최신 날짜가 바뀌면 fingerprint가 바뀐다
    - parquet이 갱신되어도 fingerprint가 바뀐다
    - (mtime만 믿지 않고) 날짜 기반으로 안정적
    """
    base = Path(daily_base_dir)
    cache_dir = Path(cache_base_dir)

    parts: list[str] = []

    for market in ["kospi", "kosdaq"]:
        d = base / market
        latest_daily = _latest_daily_yyyymmdd(d)
        daily_count = len(list(d.glob("ohlcv_*.csv"))) if d.exists() else 0

        if latest_daily is None:
            parts.append(f"{market}:daily=missing_or_empty")
        else:
            parts.append(f"{market}:daily_latest={latest_daily}:daily_n={daily_count}")

        # cache parquet 상태
        cp = cache_dir / f"{market}_merged.parquet"
        if not cp.exists():
            parts.append(f"{market}:cache=missing")
        else:
            cache_mtime = int(cp.stat().st_mtime)
            cache_size = cp.stat().st_size
            pq_max = _parquet_max_yyyymmdd(cp)  # 가능하면 max_date까지 넣기
            if pq_max:
                parts.append(f"{market}:cache_max={pq_max}:cache_mtime={cache_mtime}:cache_size={cache_size}")
            else:
                parts.append(f"{market}:cache_mtime={cache_mtime}:cache_size={cache_size}")

    return "|".join(parts)


def update_merged_parquet_from_daily(
    market: str,  # "kospi" / "kosdaq"
    daily_dir: Path = Path("data/daily"),
    cache_dir: Path = Path("data/cache"),
) -> Path:
    daily_market = daily_dir / market
    out_pq = cache_dir / f"{market}_merged.parquet"
    cache_dir.mkdir(parents=True, exist_ok=True)

    daily_files = sorted(daily_market.glob("ohlcv_*.csv"))
    if not daily_files:
        return out_pq

    # 기존 parquet 로드 (없으면 빈 DF)
    if out_pq.exists():
        base = pd.read_parquet(out_pq)
        base["date"] = pd.to_datetime(base["date"])
        base["ticker"] = base["ticker"].astype(str).str.zfill(6)
    else:
        base = pd.DataFrame(columns=["date","ticker","open","high","low","close","volume"])

    # daily들을 합침
    parts = []
    for f in daily_files:
        df = pd.read_csv(f, dtype={"ticker": str})
        if df.empty:
            continue
        df["date"] = pd.to_datetime(df["date"])
        df["ticker"] = df["ticker"].astype(str).str.zfill(6)
        parts.append(df)

    if not parts:
        return out_pq

    inc = pd.concat(parts, ignore_index=True)

    merged = pd.concat([base, inc], ignore_index=True)
    merged = merged.drop_duplicates(subset=["date","ticker"], keep="last")
    merged = merged.sort_values(["ticker","date"]).reset_index(drop=True)
    merged.to_parquet(out_pq, index=False)
    return out_pq


def _latest_daily_yyyymmdd(daily_dir: Path) -> Optional[str]:
    if not daily_dir.exists():
        return None
    best = None
    for p in daily_dir.glob("ohlcv_*.csv"):
        m = _DAILY_RE.search(p.name)
        if not m:
            continue
        d = m.group(1)
        if best is None or d > best:
            best = d
    return best


def _parquet_max_yyyymmdd(pq_path: Path) -> Optional[str]:
    if not pq_path.exists():
        return None
    # 전체를 다 읽지 않고 date 컬럼만 읽는 게 최선인데,
    # pandas read_parquet(column=...)는 엔진에 따라 다를 수 있어.
    # 일단 단순/안정 우선으로 읽는다.
    df = pd.read_parquet(pq_path, columns=["date"])
    if df is None or df.empty:
        return None
    dt = pd.to_datetime(df["date"]).max()
    if pd.isna(dt):
        return None
    return dt.strftime("%Y%m%d")


def update_merged_parquet_from_daily(market: str, data_dir: Path) -> Path:
    """
    market: "kospi" | "kosdaq"
    data_dir: Path("data") 같은 루트 데이터 폴더
    """
    daily_market = data_dir / "daily" / market
    out_pq = data_dir / "cache" / f"{market}_merged.parquet"
    out_pq.parent.mkdir(parents=True, exist_ok=True)

    latest_daily = _latest_daily_yyyymmdd(daily_market)
    if latest_daily is None:
        return out_pq

    pq_max = _parquet_max_yyyymmdd(out_pq)

    # parquet가 더 최신/같으면 할 일 없음
    if pq_max is not None and pq_max >= latest_daily:
        return out_pq

    # parquet 로드(없으면 빈 DF)
    if out_pq.exists():
        base = pd.read_parquet(out_pq)
        if not base.empty:
            base["date"] = pd.to_datetime(base["date"])
            base["ticker"] = base["ticker"].astype(str).str.zfill(6)
    else:
        base = pd.DataFrame(columns=["date","ticker","open","high","low","close","volume"])

    # parquet_max 이후 daily만 읽어서 증분 반영
    daily_files = sorted(daily_market.glob("ohlcv_*.csv"))
    parts: List[pd.DataFrame] = []

    for f in daily_files:
        m = _DAILY_RE.search(f.name)
        if not m:
            continue
        d = m.group(1)
        if pq_max is not None and d <= pq_max:
            continue

        df = pd.read_csv(f, dtype={"ticker": str})
        if df.empty:
            continue
        df["date"] = pd.to_datetime(df["date"])
        df["ticker"] = df["ticker"].astype(str).str.zfill(6)
        parts.append(df)

    if not parts:
        return out_pq

    inc = pd.concat(parts, ignore_index=True)
    merged = pd.concat([base, inc], ignore_index=True)

    merged = merged.drop_duplicates(subset=["date","ticker"], keep="last")
    merged = merged.sort_values(["ticker","date"]).reset_index(drop=True)
    merged.to_parquet(out_pq, index=False)
    return out_pq