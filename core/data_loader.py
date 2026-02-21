# core/data_loader.py
from __future__ import annotations
from pathlib import Path
import pandas as pd
import streamlit as st
from pykrx import stock

from .config import DATA_DIR, CSV_PATTERN, CSV_FALLBACK


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def list_csv_files() -> list[Path]:
    """
    data 폴더 내 CSV 목록(최신 순)
    """
    ensure_data_dir()
    files = sorted(DATA_DIR.glob(CSV_PATTERN), reverse=True)
    # (옵션) 예전 파일명도 같이 허용하고 싶다면:
    fallback = DATA_DIR / CSV_FALLBACK
    if fallback.exists() and fallback not in files:
        files.append(fallback)
    return files


def get_default_csv_path() -> Path | None:
    files = list_csv_files()
    return files[0] if files else None


@st.cache_data(show_spinner=False)
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)

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