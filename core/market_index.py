import pandas as pd
import streamlit as st
import yfinance as yf


def _load_index_1y(symbol: str) -> pd.DataFrame:
    raw = yf.download(symbol, period="1y", interval="1d", auto_adjust=False, progress=False)
    if raw is None or raw.empty:
        return pd.DataFrame()

    df = raw.copy()
    if isinstance(df.columns, pd.MultiIndex):
        if "Close" in df.columns.get_level_values(0):
            close_series = df["Close"]
            if isinstance(close_series, pd.DataFrame):
                close_series = close_series.iloc[:, 0]
            df = pd.DataFrame({"close": close_series})
        else:
            df.columns = ["_".join(map(str, c)).strip() for c in df.columns.to_list()]
            close_candidates = [c for c in df.columns if c.lower().startswith("close")]
            if not close_candidates:
                return pd.DataFrame()
            df = df.rename(columns={close_candidates[0]: "close"})[["close"]]
    else:
        if "Close" in df.columns:
            df = df.rename(columns={"Close": "close"})[["close"]]
        elif "close" in df.columns:
            df = df[["close"]]
        else:
            return pd.DataFrame()

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


@st.cache_data(show_spinner=False)
def load_kospi_index_1y():
    return _load_index_1y("^KS11")


@st.cache_data(show_spinner=False)
def load_kosdaq_index_1y():
    return _load_index_1y("^KQ11")


def build_market_index_snapshot() -> dict[str, dict]:
    snapshots: dict[str, dict] = {}
    loaders = {
        "KOSPI": load_kospi_index_1y,
        "KOSDAQ": load_kosdaq_index_1y,
    }
    for market_name, loader in loaders.items():
        df = loader()
        if df is None or df.empty:
            continue
        latest = df.dropna(subset=["date", "close"]).sort_values("date")
        if latest.empty:
            continue

        last = latest.iloc[-1]
        prev_close = float(latest.iloc[-2]["close"]) if len(latest) >= 2 else None
        close = float(last["close"])
        change_pct = ((close - prev_close) / prev_close * 100.0) if prev_close not in (None, 0) else None

        snapshots[market_name] = {
            "date": pd.to_datetime(last["date"]).strftime("%Y-%m-%d"),
            "close": round(close, 2),
            "change_pct": round(float(change_pct), 2) if change_pct is not None else None,
        }

    extra_symbols = {
        "USD/KRW": "KRW=X",
        "S&P 500": "^GSPC",
        "NASDAQ": "^IXIC",
    }
    for label, symbol in extra_symbols.items():
        df = _load_index_1y(symbol)
        if df is None or df.empty:
            continue
        latest = df.dropna(subset=["date", "close"]).sort_values("date")
        if latest.empty:
            continue

        last = latest.iloc[-1]
        prev_close = float(latest.iloc[-2]["close"]) if len(latest) >= 2 else None
        close = float(last["close"])
        change_pct = ((close - prev_close) / prev_close * 100.0) if prev_close not in (None, 0) else None

        snapshots[label] = {
            "date": pd.to_datetime(last["date"]).strftime("%Y-%m-%d"),
            "close": round(close, 2),
            "change_pct": round(float(change_pct), 2) if change_pct is not None else None,
        }
    return snapshots
