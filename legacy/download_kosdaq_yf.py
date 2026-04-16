# download_kosdaq_yf.py
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf


def _calc_date_range(end_date: str | None, lookback: str) -> tuple[str, str]:
    if end_date is None:
        end = datetime.today().date()
    else:
        end = datetime.strptime(end_date, "%Y%m%d").date()

    if lookback == "6mo":
        start = end - timedelta(days=183)
    elif lookback == "2y":
        start = end - timedelta(days=365 * 2 + 5)
    else:
        start = end - timedelta(days=365 + 5)

    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def _to_yf_ticker_kosdaq(ticker6: str) -> str:
    # KOSDAQ 종목: 000660.KQ 형태
    return f"{str(ticker6).zfill(6)}.KQ"


def _load_universe(universe_csv: str, n: Optional[int] = None) -> List[str]:
    p = Path(universe_csv)
    if not p.exists():
        raise RuntimeError(f"Universe file not found: {p}")

    df = pd.read_csv(p, dtype={"ticker": str})
    if "ticker" not in df.columns:
        raise RuntimeError(f"Universe CSV has no 'ticker' column: {p}")

    tickers = (
        df["ticker"]
        .astype(str)
        .str.strip()
        .str.replace("A", "", regex=False)
        .str.zfill(6)
        .tolist()
    )

    # 숫자 6자리만
    tickers = [t for t in tickers if t.isdigit() and len(t) == 6]
    tickers = sorted(set(tickers))

    if n is not None:
        tickers = tickers[: int(n)]
    return tickers


def _download_chunk_yf(yf_tickers: List[str], start_date: str, end_date: str) -> pd.DataFrame:
    start = datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date()

    raw = yf.download(
        yf_tickers,
        start=start,
        end=end + timedelta(days=1),
        interval="1d",
        group_by="ticker",
        threads=True,
        auto_adjust=False,
        progress=False,
    )

    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])

    idx = pd.to_datetime(raw.index).tz_localize(None)

    # 단일 티커면 컬럼이 단층 구조일 수 있음
    if not isinstance(raw.columns, pd.MultiIndex):
        cols = {c.lower(): c for c in raw.columns}
        needed = ["open", "high", "low", "close", "volume"]
        if not all(k in cols for k in needed):
            return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])
        return (
            pd.DataFrame(
                {
                    "date": idx,
                    "ticker": yf_tickers[0].split(".")[0],  # 6-digit
                    "open": raw[cols["open"]].values,
                    "high": raw[cols["high"]].values,
                    "low": raw[cols["low"]].values,
                    "close": raw[cols["close"]].values,
                    "volume": raw[cols["volume"]].values,
                }
            )
            .dropna(subset=["close"])
            .reset_index(drop=True)
        )

    out_rows: List[pd.DataFrame] = []

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
        return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])

    out = pd.concat(out_rows, ignore_index=True)
    out = out.dropna(subset=["close"])
    return out.reset_index(drop=True)


def _save_rows_to_csv(rows: List[pd.DataFrame], out_csv_path: str) -> None:
    all_df = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if all_df.empty:
        return
    all_df["date"] = pd.to_datetime(all_df["date"])
    all_df["ticker"] = all_df["ticker"].astype(str).str.zfill(6)
    all_df = all_df.drop_duplicates(subset=["date", "ticker"], keep="last")
    all_df = all_df.sort_values(["ticker", "date"]).reset_index(drop=True)
    all_df.to_csv(out_csv_path, index=False, encoding="utf-8-sig")


def rebuild_kosdaq_csv(
    out_csv_path: str,
    universe_csv: str = "data/universe_kosdaq.csv",
    n: Optional[int] = None,              # None이면 universe 전체
    end_date: str | None = None,
    lookback: str = "1y",
    max_retries: int = 3,
    sleep_base: float = 0.6,
    checkpoint_every: int = 25,
    progress_cb: Optional[Callable[[Dict[str, object]], None]] = None,
    chunk_size: int = 25,
) -> Tuple[str, List[str], str]:
    start_date, end_date_str = _calc_date_range(end_date, lookback)

    tickers = _load_universe(universe_csv, n=n)
    used_end = end_date_str  # KOSDAQ는 지수로 영업일 보정 안 하고, end 그대로 사용

    rows: List[pd.DataFrame] = []
    failed: List[str] = []
    saved_tickers = set()

    if os.path.exists(out_csv_path):
        existing = pd.read_csv(out_csv_path, dtype={"ticker": str})
        if "ticker" in existing.columns:
            existing["ticker"] = existing["ticker"].astype(str).str.zfill(6)
            saved_tickers = set(existing["ticker"].unique())
            rows.append(existing)

    remaining = [t for t in tickers if t not in saved_tickers]
    total = len(remaining)
    done = 0

    for i in range(0, total, chunk_size):
        chunk = remaining[i : i + chunk_size]
        yf_chunk = [_to_yf_ticker_kosdaq(t) for t in chunk]

        ok_df = None
        for attempt in range(1, max_retries + 1):
            try:
                time.sleep(sleep_base)
                ok_df = _download_chunk_yf(yf_chunk, start_date, used_end)
                break
            except Exception:
                time.sleep(min(5.0, sleep_base * (2 ** (attempt - 1))))

        if ok_df is None or ok_df.empty:
            # chunk 실패 -> 개별 재시도
            for t in chunk:
                yt = _to_yf_ticker_kosdaq(t)
                got = None
                for attempt in range(1, max_retries + 1):
                    try:
                        time.sleep(sleep_base)
                        got = _download_chunk_yf([yt], start_date, used_end)
                        break
                    except Exception:
                        time.sleep(min(5.0, sleep_base * (2 ** (attempt - 1))))
                if got is None or got.empty:
                    failed.append(t)
                else:
                    rows.append(got)
                    saved_tickers.add(t)

                done += 1
                if progress_cb:
                    progress_cb({"done": done, "total": total, "ticker": t, "failed": len(failed)})
        else:
            rows.append(ok_df)
            for t in chunk:
                saved_tickers.add(t)
                done += 1
                if progress_cb:
                    progress_cb({"done": done, "total": total, "ticker": t, "failed": len(failed)})

        if done > 0 and (done % checkpoint_every == 0):
            _save_rows_to_csv(rows, out_csv_path)

    _save_rows_to_csv(rows, out_csv_path)
    return out_csv_path, failed, used_end