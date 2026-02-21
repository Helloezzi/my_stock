
import yfinance as yf
import pandas as pd
import streamlit as st


@st.cache_data(show_spinner=False)
def load_kospi_index_1y():
    """
    KOSPI Index (^KS11) 1Y daily
    yfinance가 환경/버전에 따라 MultiIndex 컬럼을 반환하는 경우가 있어 방어적으로 처리
    """
    raw = yf.download("^KS11", period="1y", interval="1d", auto_adjust=False, progress=False)

    if raw is None or raw.empty:
        return pd.DataFrame()

    df = raw.copy()

    # 1) MultiIndex 컬럼인 경우 (예: ('Close', '^KS11')) 형태 -> Close만 뽑기
    if isinstance(df.columns, pd.MultiIndex):
        # 보통 첫 레벨에 Open/High/Low/Close/... 가 있고
        # 두 번째 레벨에 티커가 붙음
        if "Close" in df.columns.get_level_values(0):
            close_series = df["Close"]
            # close_series가 DataFrame일 수 있으니 첫 컬럼(=^KS11)을 뽑음
            if isinstance(close_series, pd.DataFrame):
                close_series = close_series.iloc[:, 0]
            df = pd.DataFrame({"close": close_series})
        else:
            # 예상 못한 형태면 평탄화 후 시도
            df.columns = ["_".join(map(str, c)).strip() for c in df.columns.to_list()]
            # Close 비슷한 컬럼 찾기
            close_candidates = [c for c in df.columns if c.lower().startswith("close")]
            if not close_candidates:
                return pd.DataFrame()
            df = df.rename(columns={close_candidates[0]: "close"})[["close"]]

    else:
        # 2) 일반 컬럼인 경우: Close가 있으면 close로 rename
        if "Close" in df.columns:
            df = df.rename(columns={"Close": "close"})[["close"]]
        elif "close" in df.columns:
            df = df[["close"]]
        else:
            return pd.DataFrame()

    # 인덱스(날짜)를 컬럼으로
    df = df.reset_index()
    if "Date" in df.columns:
        df = df.rename(columns={"Date": "date"})
    elif "index" in df.columns:
        df = df.rename(columns={"index": "date"})

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=["date", "close"]).sort_values("date")

    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()
    return df