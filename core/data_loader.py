import glob
import os
import pandas as pd
import streamlit as st
from pykrx import stock

from .config import CSV_FALLBACK, CSV_PATTERN


def get_latest_csv(pattern: str = CSV_PATTERN) -> str | None:
    files = sorted(glob.glob(pattern), reverse=True)
    return files[0] if files else None


def resolve_csv_path() -> str:
    latest = get_latest_csv()
    return latest if latest else CSV_FALLBACK


@st.cache_data(show_spinner=False)
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])

    # ticker는 무조건 문자열로 (앞자리 0 보존)
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)

    # 숫자 컬럼 강제 변환
    num_cols = ["open", "high", "low", "close", "volume"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["date", "ticker", "open", "high", "low", "close", "volume"])
    df = df.sort_values(["ticker", "date"])
    return df


@st.cache_data(show_spinner=False)
def get_ticker_name_map(tickers: list[str]) -> dict[str, str]:
    name_map: dict[str, str] = {}
    for t in tickers:
        try:
            name_map[t] = stock.get_market_ticker_name(t)
        except Exception:
            name_map[t] = t
    return name_map