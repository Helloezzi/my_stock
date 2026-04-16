# download_kospi_yf.py
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf

from pathlib import Path
UNIVERSE_CACHE = Path("data") / "universe_top200.csv"   # 프로젝트 구조에 맞게 조정

# 선택: Top200 구성은 pykrx를 계속 사용 (최소 의존)
try:
    from pykrx import stock as krx_stock
except Exception:
    krx_stock = None


def _load_universe_cache(path: Path) -> list[str]:
    if not path.exists():
        return []
    df = pd.read_csv(path)
    if "ticker" not in df.columns:
        return []
    return sorted(df["ticker"].astype(str).str.zfill(6).unique().tolist())

def _save_universe_cache(path: Path, tickers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"ticker": [str(t).zfill(6) for t in tickers]}).to_csv(path, index=False, encoding="utf-8-sig")


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


def _nearest_trading_day_ks11(end_yyyymmdd: str) -> str:
    """
    한국 휴장일 달력 없이도 '근사적으로' 영업일을 찾는 방법:
    ^KS11 데이터를 end까지 받아서 마지막 날짜를 사용.
    """
    end = datetime.strptime(end_yyyymmdd, "%Y%m%d").date()
    start = end - timedelta(days=14)  # 2주면 휴일 포함해도 대부분 커버
    df = yf.download("^KS11", start=start, end=end + timedelta(days=1), interval="1d", progress=False, auto_adjust=False)
    if df is None or df.empty:
        return end_yyyymmdd
    last_dt = df.index.max().date()
    return last_dt.strftime("%Y%m%d")


def _to_yf_ticker_kospi(ticker6: str) -> str:
    # KOSPI 종목: 005930.KS
    return f"{str(ticker6).zfill(6)}.KS"


def _get_topn_kospi_tickers_by_mcap(end_yyyymmdd: str, n: int) -> List[str]:
    if krx_stock is None:
        raise RuntimeError("pykrx is required to build TopN universe by market cap, but not installed.")

    # ✅ pykrx 영업일 보정이 터져도 end_yyyymmdd 그대로 사용
    try:
        end_yyyymmdd = krx_stock.get_nearest_business_day_in_a_week(end_yyyymmdd)
    except Exception:
        pass

    cap = krx_stock.get_market_cap(end_yyyymmdd, market="KOSPI").sort_values("시가총액", ascending=False)
    cap = cap[cap.index.astype(str).str.fullmatch(r"\d{6}")]
    n_use = min(int(n), len(cap.index))
    return [str(t).zfill(6) for t in cap.index[:n_use].tolist()]


def _download_chunk_yf(yf_tickers: List[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    yfinance multi-ticker download 결과를 long format(date,ticker,open,high,low,close,volume)으로 변환
    """
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

    out_rows = []
    idx = pd.to_datetime(raw.index).tz_localize(None)

    # 단일 티커면 컬럼이 단층 구조일 수 있음
    if not isinstance(raw.columns, pd.MultiIndex):
        # columns: Open, High, Low, Close, Volume, Adj Close ...
        cols = {c.lower(): c for c in raw.columns}
        needed = ["open", "high", "low", "close", "volume"]
        if not all(k in cols for k in needed):
            return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])
        df1 = pd.DataFrame({
            "date": idx,
            "ticker": yf_tickers[0].split(".")[0],  # 005930
            "open": raw[cols["open"]].values,
            "high": raw[cols["high"]].values,
            "low": raw[cols["low"]].values,
            "close": raw[cols["close"]].values,
            "volume": raw[cols["volume"]].values,
        })
        return df1.dropna(subset=["close"]).reset_index(drop=True)

    # MultiIndex: (PriceField, Ticker) or (Ticker, PriceField) 환경마다 다름
    # yfinance는 보통 (PriceField, Ticker) 형태가 많음
    lvl0 = list(raw.columns.get_level_values(0))
    if "Open" in lvl0 or "Close" in lvl0:
        # (PriceField, Ticker)
        for yt in yf_tickers:
            if ("Close", yt) not in raw.columns:
                continue
            sub = raw.xs(yt, axis=1, level=1, drop_level=False)
            # sub columns: (Open,yt),(High,yt)...
            def _col(field: str):
                return sub[(field, yt)] if (field, yt) in sub.columns else None

            c = _col("Close")
            if c is None:
                continue
            df1 = pd.DataFrame({
                "date": idx,
                "ticker": yt.split(".")[0],
                "open": _col("Open").values if _col("Open") is not None else None,
                "high": _col("High").values if _col("High") is not None else None,
                "low": _col("Low").values if _col("Low") is not None else None,
                "close": c.values,
                "volume": _col("Volume").values if _col("Volume") is not None else None,
            })
            out_rows.append(df1)

    else:
        # (Ticker, PriceField)
        for yt in yf_tickers:
            if (yt, "Close") not in raw.columns:
                continue
            df1 = pd.DataFrame({
                "date": idx,
                "ticker": yt.split(".")[0],
                "open": raw[(yt, "Open")].values if (yt, "Open") in raw.columns else None,
                "high": raw[(yt, "High")].values if (yt, "High") in raw.columns else None,
                "low": raw[(yt, "Low")].values if (yt, "Low") in raw.columns else None,
                "close": raw[(yt, "Close")].values,
                "volume": raw[(yt, "Volume")].values if (yt, "Volume") in raw.columns else None,
            })
            out_rows.append(df1)

    if not out_rows:
        return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])

    out = pd.concat(out_rows, ignore_index=True)
    out = out.dropna(subset=["close"])
    return out.reset_index(drop=True)


def _save_rows_to_csv(rows: List[pd.DataFrame], out_csv_path: str) -> None:
    all_df = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if all_df.empty:
        # 빈 파일 만들지 않음
        return
    all_df["date"] = pd.to_datetime(all_df["date"])
    all_df["ticker"] = all_df["ticker"].astype(str).str.zfill(6)
    all_df = all_df.drop_duplicates(subset=["date", "ticker"], keep="last")
    all_df = all_df.sort_values(["ticker", "date"]).reset_index(drop=True)
    all_df.to_csv(out_csv_path, index=False, encoding="utf-8-sig")


def rebuild_kospi_top200_csv(
    out_csv_path: str,
    n: int = 200,
    end_date: str | None = None,
    lookback: str = "1y",
    max_retries: int = 3,
    sleep_base: float = 0.6,
    checkpoint_every: int = 25,
    progress_cb: Optional[Callable[[Dict[str, object]], None]] = None,
    chunk_size: int = 25,
) -> Tuple[str, List[str], str]:
    start_date, end_date_str = _calc_date_range(end_date, lookback)
    
    # 1) 최신 거래일은 yfinance로 (데이터가 실제 존재하는 날짜)
    used_end = _latest_trading_day_by_yf(end_date_str)

    # 2) 유니버스(Top200)는:
    #    - pykrx 성공하면 갱신 + 캐시 저장
    #    - 실패하면 캐시/기존 CSV에서 가져와 계속 진행
    tickers = []
    try:
        tickers = _get_topn_kospi_tickers_by_mcap(used_end, n=n)
        _save_universe_cache(UNIVERSE_CACHE, tickers)
    except Exception:
        # 2-1) 캐시 우선
        tickers = _load_universe_cache(UNIVERSE_CACHE)

        # 2-2) 캐시도 없으면, 기존 out_csv에서 복구
        if not tickers and os.path.exists(out_csv_path):
            existing = pd.read_csv(out_csv_path)
            if "ticker" in existing.columns:
                tickers = sorted(existing["ticker"].astype(str).str.zfill(6).unique().tolist())

        if not tickers:
            raise RuntimeError("Universe build failed (pykrx down) and no cache/previous CSV available.")

    # 기존 파일 있으면 이어받기
    rows: List[pd.DataFrame] = []
    failed: List[str] = []
    saved_tickers = set()

    if os.path.exists(out_csv_path):
        existing = pd.read_csv(out_csv_path)
        if "ticker" in existing.columns:
            existing["ticker"] = existing["ticker"].astype(str).str.zfill(6)
            saved_tickers = set(existing["ticker"].unique())
            rows.append(existing)

    # yfinance 티커 변환
    remaining = [t for t in tickers if t not in saved_tickers]
    total = len(remaining)
    done = 0

    # chunk download
    for i in range(0, total, chunk_size):
        chunk = remaining[i:i + chunk_size]
        yf_chunk = [_to_yf_ticker_kospi(t) for t in chunk]

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
                yt = _to_yf_ticker_kospi(t)
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


def _latest_trading_day_by_yf(end_yyyymmdd: str, probes: list[str] | None = None) -> str:
    """
    yfinance 기반 '실제 존재하는' 최신 거래일을 찾는다.
    - probes: 대표 티커(6자리). 기본은 삼성/하이닉스 등
    """
    if probes is None:
        probes = ["005930", "000660", "035420", "051910"]  # 삼성, 하이닉스, 네이버, LG화학 등

    end = datetime.strptime(end_yyyymmdd, "%Y%m%d").date()
    start = end - timedelta(days=14)

    latest = None
    for t6 in probes:
        yt = _to_yf_ticker_kospi(t6)
        df = yf.download(yt, start=start, end=end + timedelta(days=1), interval="1d", progress=False, auto_adjust=False)
        if df is None or df.empty:
            continue
        d = pd.to_datetime(df.index.max()).date()
        if latest is None or d > latest:
            latest = d

    return (latest.strftime("%Y%m%d") if latest else end_yyyymmdd)