# core/data_loader.py
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import pandas as pd
import streamlit as st
from pykrx import stock
from core.config import DATA_DIR

ACTIVE_KEY = "selected_csv_name"

def get_active_csv_path() -> Path | None:
    name = st.session_state.get(ACTIVE_KEY)
    if not name:
        return None
    p = DATA_DIR / name
    return p if p.exists() else None


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def list_csv_files() -> List[Path]:
    """
    data 폴더 내 CSV를 모두 나열 (새 파일명 규칙/구 파일명 규칙 모두 지원)
    - _tmp_*.csv 같은 임시 파일은 제외
    - 최신 파일이 위로 오도록 mtime 기준 내림차순 정렬
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(DATA_DIR.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    files = [p for p in files if not p.name.startswith("_tmp_")]
    return files


def get_default_csv_path() -> Optional[Path]:
    files = list_csv_files()
    return files[0] if files else None


@st.cache_data(show_spinner=False)
def load_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    # ticker/date 정리 (프로젝트 기존 로직 유지/보강)
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
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