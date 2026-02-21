# download_kospi.py
from __future__ import annotations

import os
import time
import random
from datetime import datetime
from typing import List, Tuple

import pandas as pd
from pykrx import stock
from dateutil.relativedelta import relativedelta

from datetime import timedelta

def _ensure_trading_day(date_str: str, max_back: int = 10) -> str:
    d = datetime.strptime(date_str, "%Y%m%d").date()
    for _ in range(max_back):
        try:
            cap = stock.get_market_cap(d.strftime("%Y%m%d"), market="KOSPI")
            if cap is not None and not cap.empty:
                return d.strftime("%Y%m%d")
        except Exception:
            pass
        d -= timedelta(days=1)
    return date_str  # fallback


def _calc_date_range(end_date: str | None, lookback: str) -> tuple[str, str]:
    if end_date is None:
        end = datetime.today().date()
    else:
        end = datetime.strptime(end_date, "%Y%m%d").date()

    if lookback == "6mo":
        start = end - relativedelta(months=6)
    elif lookback == "2y":
        start = end - relativedelta(years=2)
    else:
        start = end - relativedelta(years=1)

    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

def rebuild_kospi_top200_csv(
    out_csv_path: str,
    n: int = 200,
    end_date: str | None = None,     # "YYYYMMDD"
    lookback: str = "1y",            # "6mo" | "1y" | "2y"
    max_retries: int = 3,
    sleep_base: float = 0.35,
    checkpoint_every: int = 25,
) -> Tuple[str, List[str]]:
    start_date, end_date_str = _calc_date_range(end_date, lookback)

    cap = stock.get_market_cap(end_date_str, market="KOSPI").sort_values("시가총액", ascending=False)
    tickers = [str(t).zfill(6) for t in cap.index[:n].tolist()]

    rows: List[pd.DataFrame] = []
    failed: List[str] = []

    saved_tickers = set()
    if os.path.exists(out_csv_path):
        existing = pd.read_csv(out_csv_path)
        existing["ticker"] = existing["ticker"].astype(str).str.zfill(6)
        saved_tickers = set(existing["ticker"].unique())
        rows.append(existing)

    done_count = 0

    for t in tickers:
        if t in saved_tickers:
            continue

        ok = False
        for attempt in range(1, max_retries + 1):
            try:
                time.sleep(sleep_base + random.uniform(0, 0.15))

                # ✅ 반드시 for t 루프 안에서 호출
                df = stock.get_market_ohlcv(start_date, end_date_str, t)
                if df is None or df.empty:
                    raise RuntimeError("Empty dataframe")

                df = df.reset_index().rename(columns={"날짜": "date"})
                df["ticker"] = t

                rename_map = {"시가": "open", "고가": "high", "저가": "low", "종가": "close", "거래량": "volume"}
                df = df.rename(columns=rename_map)[["date", "ticker", "open", "high", "low", "close", "volume"]]

                rows.append(df)
                saved_tickers.add(t)
                done_count += 1
                ok = True
                break

            except Exception as e:
                backoff = min(5.0, sleep_base * (2 ** (attempt - 1)))
                time.sleep(backoff)

        if not ok:
            failed.append(t)
            time.sleep(1.0)

        if done_count > 0 and (done_count % checkpoint_every == 0):
            _save_rows_to_csv(rows, out_csv_path)

    _save_rows_to_csv(rows, out_csv_path)
    return out_csv_path, failed


def _save_rows_to_csv(rows: List[pd.DataFrame], out_csv_path: str) -> None:
    all_df = pd.concat(rows, ignore_index=True)
    all_df["date"] = pd.to_datetime(all_df["date"])
    all_df["ticker"] = all_df["ticker"].astype(str).str.zfill(6)
    all_df = all_df.drop_duplicates(subset=["date", "ticker"], keep="last")
    all_df = all_df.sort_values(["ticker", "date"]).reset_index(drop=True)
    all_df.to_csv(out_csv_path, index=False, encoding="utf-8-sig")