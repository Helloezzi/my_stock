# download_daily_yf.py
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import yfinance as yf


@dataclass
class MarketSpec:
    name: str                    # "kospi" / "kosdaq"
    index_symbol: str            # "^KS11" / "^KQ11"
    suffix: str                  # ".KS" / ".KQ"
    universe_csv: Path           # data/universe_kospi.csv 등


KOSPI = MarketSpec("kospi", "^KS11", ".KS", Path("data/universe_kospi.csv"))
KOSDAQ = MarketSpec("kosdaq", "^KQ11", ".KQ", Path("data/universe_kosdaq.csv"))


def _load_universe(p: Path, n: Optional[int] = None) -> List[str]:
    df = pd.read_csv(p, dtype={"ticker": str})
    tickers = df["ticker"].astype(str).str.replace("A", "", regex=False).str.strip()
    tickers = tickers[tickers.str.fullmatch(r"\d{1,6}", na=False)].str.zfill(6)
    uniq = tickers.drop_duplicates().tolist()
    return uniq[:n] if n else uniq


def _nearest_trading_day(index_symbol: str, end_yyyymmdd: str) -> str:
    """
    휴장일 캘린더 없이도 yfinance 지수로 '마지막 거래일'을 결정.
    """
    end = datetime.strptime(end_yyyymmdd, "%Y%m%d").date()
    start = end - timedelta(days=14)
    df = yf.download(
        index_symbol,
        start=start,
        end=end + timedelta(days=1),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if df is None or df.empty:
        return end_yyyymmdd
    last_dt = pd.to_datetime(df.index.max()).date()
    return last_dt.strftime("%Y%m%d")


def _download_day_long(yf_tickers: List[str], day_yyyymmdd: str) -> pd.DataFrame:
    """
    특정 하루(day)만: start=day, end=day+1 로 다운로드
    return: date,ticker,open,high,low,close,volume
    """
    d = datetime.strptime(day_yyyymmdd, "%Y%m%d").date()
    raw = yf.download(
        yf_tickers,
        start=d,
        end=d + timedelta(days=1),
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=True,
    )

    cols_out = ["date", "ticker", "open", "high", "low", "close", "volume"]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=cols_out)

    idx = pd.to_datetime(raw.index).tz_localize(None)

    # single ticker case
    if not isinstance(raw.columns, pd.MultiIndex):
        cols = {str(c).lower(): c for c in raw.columns}
        needed = ["open", "high", "low", "close", "volume"]
        if not all(k in cols for k in needed):
            return pd.DataFrame(columns=cols_out)
        t6 = yf_tickers[0].split(".")[0]
        df1 = pd.DataFrame(
            {
                "date": idx,
                "ticker": t6,
                "open": raw[cols["open"]].values,
                "high": raw[cols["high"]].values,
                "low": raw[cols["low"]].values,
                "close": raw[cols["close"]].values,
                "volume": raw[cols["volume"]].values,
            }
        )
        return df1.dropna(subset=["close"]).reset_index(drop=True)

    out_rows = []

    lvl0 = list(raw.columns.get_level_values(0))
    if "Open" in lvl0 or "Close" in lvl0:
        # (PriceField, Ticker)
        for yt in yf_tickers:
            if ("Close", yt) not in raw.columns:
                continue
            sub = raw.xs(yt, axis=1, level=1, drop_level=False)

            def _col(field: str):
                return sub[(field, yt)] if (field, yt) in sub.columns else None

            c = _col("Close")
            if c is None:
                continue

            df1 = pd.DataFrame(
                {
                    "date": idx,
                    "ticker": yt.split(".")[0],
                    "open": _col("Open").values if _col("Open") is not None else None,
                    "high": _col("High").values if _col("High") is not None else None,
                    "low": _col("Low").values if _col("Low") is not None else None,
                    "close": c.values,
                    "volume": _col("Volume").values if _col("Volume") is not None else None,
                }
            )
            out_rows.append(df1)
    else:
        # (Ticker, PriceField)
        for yt in yf_tickers:
            if (yt, "Close") not in raw.columns:
                continue
            df1 = pd.DataFrame(
                {
                    "date": idx,
                    "ticker": yt.split(".")[0],
                    "open": raw[(yt, "Open")].values if (yt, "Open") in raw.columns else None,
                    "high": raw[(yt, "High")].values if (yt, "High") in raw.columns else None,
                    "low": raw[(yt, "Low")].values if (yt, "Low") in raw.columns else None,
                    "close": raw[(yt, "Close")].values,
                    "volume": raw[(yt, "Volume")].values if (yt, "Volume") in raw.columns else None,
                }
            )
            out_rows.append(df1)

    if not out_rows:
        return pd.DataFrame(columns=cols_out)

    out = pd.concat(out_rows, ignore_index=True)
    out = out.dropna(subset=["close"])
    return out.reset_index(drop=True)


def download_daily_snapshot(
    spec: MarketSpec,
    end_date: Optional[str] = None,
    n: Optional[int] = None,
    out_dir: Optional[Path] = None,
    max_retries: int = 3,
    sleep_base: float = 0.8,
) -> Tuple[Path, str, int]:
    """
    returns: (saved_csv_path, used_day_yyyymmdd, rows)
    """
    if end_date is None:
        end_date = date.today().strftime("%Y%m%d")

    day = _nearest_trading_day(spec.index_symbol, end_date)

    out_dir = out_dir or Path("data/daily") / spec.name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"ohlcv_{day}.csv"

    if out_csv.exists() and out_csv.stat().st_size > 50:
        df = pd.read_csv(out_csv, dtype={"ticker": str})
        return out_csv, day, len(df)

    tickers = _load_universe(spec.universe_csv, n=n)
    if not tickers:
        raise RuntimeError(f"Universe empty: {spec.universe_csv}")

    yf_tickers = [t + spec.suffix for t in tickers]

    last_err = None
    df_day = None
    for attempt in range(1, max_retries + 1):
        try:
            time.sleep(sleep_base)
            df_day = _download_day_long(yf_tickers, day)
            break
        except Exception as e:
            last_err = e
            time.sleep(min(8.0, sleep_base * (2 ** (attempt - 1))))

    if df_day is None:
        raise RuntimeError(f"Download failed: {last_err}")

    df_day["date"] = pd.to_datetime(df_day["date"])
    df_day["ticker"] = df_day["ticker"].astype(str).str.zfill(6)
    df_day = df_day.drop_duplicates(subset=["date", "ticker"], keep="last")
    df_day = df_day.sort_values(["ticker", "date"]).reset_index(drop=True)

    df_day.to_csv(out_csv, index=False, encoding="utf-8-sig")
    return out_csv, day, len(df_day)


if __name__ == "__main__":
    p1, d1, r1 = download_daily_snapshot(KOSPI, end_date=None, n=200)
    print("[KOSPI]", "day=", d1, "rows=", r1, "file=", p1)

    p2, d2, r2 = download_daily_snapshot(KOSDAQ, end_date=None, n=200)
    print("[KOSDAQ]", "day=", d2, "rows=", r2, "file=", p2)