# test_date_20260227.py
from __future__ import annotations

from pykrx import stock
import pandas as pd

import downloader_daily as dd  # 같은 폴더에 downloader_daily.py가 있어야 함


DATE = "20260227"


def _probe_ohlcv(date: str, market: str) -> None:
    print(f"\n=== OHLCV probe: {market} {date} ===")
    try:
        df = stock.get_market_ohlcv_by_ticker(date, market=market)
        if df is None:
            print("ohlcv: None")
            return
        print("ohlcv empty?:", df.empty, "shape:", df.shape)
        print("ohlcv cols:", list(df.columns))
        if not df.empty:
            print("ohlcv head:\n", df.head(3))
    except Exception as e:
        print("ohlcv EXCEPTION:", repr(e))


def _probe_cap(date: str, market: str) -> None:
    print(f"\n=== CAP probe: {market} {date} ===")
    try:
        df = stock.get_market_cap_by_ticker(date, market=market)
        if df is None:
            print("cap: None")
            return
        print("cap empty?:", df.empty, "shape:", df.shape)
        print("cap cols:", list(df.columns))
        if not df.empty:
            print("cap head:\n", df.head(3))
    except Exception as e:
        # 네가 겪은 KeyError 포함해서 여기로 잡힘
        print("cap EXCEPTION:", repr(e))


def _show_downloader_fallback(date: str, market: str) -> None:
    print(f"\n=== DOWNLOADER result: {market} request={date} ===")
    # 내부 로직이 어떤 날짜로 보정하는지
    ensured = dd._ensure_business_day(date, max_back=10, market_probe=market)
    print("ensure_business_day ->", ensured)

    # 실제 다운로드 함수가 최종적으로 어떤 date로 저장하려는지
    res = dd.download_daily_one_market(
        yyyymmdd=date,
        market=market,
        out_dir="data/daily_test",
        force=True,     # 매번 다시 받게
        min_rows=10,    # 테스트라 낮춤
    )
    print("download_daily_one_market ->", res)


def main():
    # 1) pykrx 원본 응답을 20260227로만 직접 찔러본다
    for m in ("KOSPI", "KOSDAQ"):
        _probe_ohlcv(DATE, m)
        _probe_cap(DATE, m)

    # 2) 우리 다운로더가 왜 fallback 하는지 확인
    for m in ("KOSPI", "KOSDAQ"):
        _show_downloader_fallback(DATE, m)


if __name__ == "__main__":
    pd.set_option("display.width", 160)
    pd.set_option("display.max_columns", 50)
    main()