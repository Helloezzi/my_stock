# core/universe.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class UniverseInfo:
    market: str
    top_n: Optional[int]
    rank_by: str
    latest_date: str
    tickers: int
    rows: int

def get_universe(df: pd.DataFrame, top_n: int | None = None) -> list[str]:
    # ticker별 최신 row 기준으로 시총/거래대금 정렬 같은 걸 하려면 여기서 확장
    tickers = sorted(df["ticker"].unique())
    if top_n is None or top_n <= 0 or top_n >= len(tickers):
        return tickers
    return tickers[:top_n]


def select_market_df(dfs: Dict[str, pd.DataFrame], market: str) -> pd.DataFrame:
    """
    Pick a market dataframe from dict returned by load_all_markets().

    Args:
        dfs: {"kospi": df, "kosdaq": df}
        market: "KOSPI" or "KOSDAQ" (case-insensitive). Also accepts "kospi"/"kosdaq".

    Returns:
        pd.DataFrame for that market.

    Raises:
        KeyError if market not found in dfs
    """
    m = (market or "KOSPI").strip().lower()
    key = "kosdaq" if m in ("kosdaq", "kq") else "kospi"
    if key not in dfs:
        raise KeyError(f"dfs missing key: {key}")
    return dfs[key]


def get_latest_date(df: pd.DataFrame) -> str:
    """
    Return latest date string from df["date"].
    Assumes "date" is comparable as string (e.g., YYYYMMDD) or a datetime-like column.
    """
    if df is None or df.empty:
        return ""
    if "date" not in df.columns:
        raise ValueError("df has no 'date' column")

    s = df["date"]
    # If datetime-like, convert to string key
    if pd.api.types.is_datetime64_any_dtype(s):
        return s.max().strftime("%Y%m%d")
    # If string like YYYYMMDD/ISO, max() is safe lexicographically for YYYYMMDD
    return str(s.astype("string").max())


def _pick_rank_column(df: pd.DataFrame, preferred: str) -> str:
    """
    Choose a ranking column that exists in df.
    Priority: preferred -> market_cap -> value -> volume
    """
    preferred = (preferred or "").strip()
    candidates = [preferred, "market_cap", "value", "volume"]
    for c in candidates:
        if c and c in df.columns:
            return c
    raise ValueError("No suitable rank column found. Need one of: market_cap/value/volume.")


def apply_top_n(
    df: pd.DataFrame,
    top_n: Optional[int],
    rank_by: str = "market_cap",
    latest_date: Optional[str] = None,
) -> Tuple[pd.DataFrame, UniverseInfo]:
    """
    Filter df to only include Top N tickers, ranked by `rank_by` on the latest date.

    Args:
        df: market dataframe containing at least columns ["date","ticker"] and rank column.
        top_n: None or 0 => no filtering (use all)
        rank_by: ranking column. If missing, falls back to market_cap -> value -> volume.
        latest_date: if provided, uses this date; otherwise uses df max date.

    Returns:
        (filtered_df, UniverseInfo)

    Notes:
        - Rank is computed on `latest_date` snapshot (one row per ticker).
        - Safety: if duplicate (date,ticker) exists, it keeps last after sorting by rank column.
    """
    if df is None or df.empty:
        info = UniverseInfo(
            market="",
            top_n=top_n,
            rank_by=rank_by,
            latest_date=latest_date or "",
            tickers=0,
            rows=0,
        )
        return df, info

    if "date" not in df.columns or "ticker" not in df.columns:
        raise ValueError("df must contain 'date' and 'ticker' columns")

    # Normalize
    out = df.copy()
    out["ticker"] = out["ticker"].astype("string").str.zfill(6)
    out["date"] = out["date"].astype("string")

    ld = latest_date or get_latest_date(out)
    if not ld:
        info = UniverseInfo(market="", top_n=top_n, rank_by=rank_by, latest_date="", tickers=0, rows=0)
        return out, info

    # No top-n filtering
    if not top_n or int(top_n) <= 0:
        info = UniverseInfo(
            market="",
            top_n=None,
            rank_by=_pick_rank_column(out, rank_by),
            latest_date=ld,
            tickers=int(out["ticker"].nunique()),
            rows=int(len(out)),
        )
        return out, info

    n = int(top_n)
    rank_col = _pick_rank_column(out, rank_by)

    snap = out[out["date"] == ld].copy()
    if snap.empty:
        # If latest_date is not present (edge case), fallback to max date present
        ld2 = get_latest_date(out)
        snap = out[out["date"] == ld2].copy()
        ld = ld2

    # Make sure rank column numeric if possible (safe coercion)
    snap[rank_col] = pd.to_numeric(snap[rank_col], errors="coerce")

    # If multiple rows per ticker on that date, keep the best-ranked row
    snap = (
        snap.sort_values(rank_col, ascending=False, na_position="last")
        .drop_duplicates(subset=["ticker"], keep="first")
    )

    top_tickers = snap.head(n)["ticker"].astype("string").tolist()

    filtered = out[out["ticker"].isin(top_tickers)].copy()

    info = UniverseInfo(
        market="",
        top_n=n,
        rank_by=rank_col,
        latest_date=ld,
        tickers=int(len(top_tickers)),
        rows=int(len(filtered)),
    )
    return filtered, info


def build_universe(
    dfs: Dict[str, pd.DataFrame],
    market: str = "KOSPI",
    top_n: Optional[int] = None,
    rank_by: str = "market_cap",
) -> Tuple[pd.DataFrame, UniverseInfo]:
    """
    Convenience wrapper:
      1) select market df from dfs
      2) apply Top-N filter
    """
    df = select_market_df(dfs, market)
    filtered, info = apply_top_n(df, top_n=top_n, rank_by=rank_by)
    # Fill market in info (cosmetic)
    return filtered, UniverseInfo(
        market=market.upper(),
        top_n=info.top_n,
        rank_by=info.rank_by,
        latest_date=info.latest_date,
        tickers=info.tickers,
        rows=info.rows,
    )