# download_kospi.py
from __future__ import annotations

from datetime import datetime, timedelta
import os
import time
import random
from typing import List, Tuple

import pandas as pd
from pykrx import stock


def rebuild_kospi_top200_1y_csv(
    out_csv_path: str = "kospi_top200_1y_daily.csv",
    n: int = 200,
    max_retries: int = 5,
    sleep_base: float = 0.2,          # 기본 텀(너무 빠르면 멈춤/차단 가능)
    checkpoint_every: int = 20,       # 20개마다 중간 저장
) -> Tuple[str, List[str]]:
    """
    Returns:
      (saved_csv_path, failed_tickers)
    """
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

    cap = stock.get_market_cap(end_date, market="KOSPI").sort_values("시가총액", ascending=False)
    tickers = [str(t).zfill(6) for t in cap.index[:n].tolist()]

    # 기존 파일이 있으면 이어쓰기(원하면 GUI에서 삭제 후 호출)
    if os.path.exists(out_csv_path):
        existing = pd.read_csv(out_csv_path)
        existing["ticker"] = existing["ticker"].astype(str).str.zfill(6)
        saved_tickers = set(existing["ticker"].unique())
        rows = [existing]
    else:
        saved_tickers = set()
        rows = []

    failed: List[str] = []
    done_count = 0

    for t in tickers:
        if t in saved_tickers:
            continue

        ok = False
        last_err = None

        for attempt in range(1, max_retries + 1):
            try:
                # 호출 텀 + 약간의 랜덤 지터
                time.sleep(sleep_base + random.uniform(0, 0.15))

                df = stock.get_market_ohlcv(start_date, end_date, t)
                if df is None or df.empty:
                    raise RuntimeError("Empty dataframe")

                df = df.reset_index().rename(columns={"날짜": "date"})
                df["ticker"] = t

                rename_map = {"시가": "open", "고가": "high", "저가": "low", "종가": "close", "거래량": "volume"}
                df = df.rename(columns=rename_map)

                keep = ["date", "ticker", "open", "high", "low", "close", "volume"]
                df = df[keep]

                rows.append(df)
                saved_tickers.add(t)
                done_count += 1
                ok = True
                break

            except Exception as e:
                last_err = e
                # 백오프(점점 더 기다림)
                backoff = min(5.0, sleep_base * (2 ** (attempt - 1)))
                time.sleep(backoff)

        if not ok:
            failed.append(t)
            # 여기서도 너무 빠르게 실패 연속이면 잠깐 쉬는 게 도움이 됨
            time.sleep(1.0)

        # 체크포인트 저장
        if done_count > 0 and (done_count % checkpoint_every == 0):
            _save_rows_to_csv(rows, out_csv_path)

    # 마지막 저장
    _save_rows_to_csv(rows, out_csv_path)

    return out_csv_path, failed


def _save_rows_to_csv(rows: List[pd.DataFrame], out_csv_path: str) -> None:
    all_df = pd.concat(rows, ignore_index=True)
    all_df["date"] = pd.to_datetime(all_df["date"])
    all_df["ticker"] = all_df["ticker"].astype(str).str.zfill(6)
    all_df = all_df.drop_duplicates(subset=["date", "ticker"], keep="last")
    all_df = all_df.sort_values(["ticker", "date"]).reset_index(drop=True)
    all_df.to_csv(out_csv_path, index=False, encoding="utf-8-sig")