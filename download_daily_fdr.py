from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import FinanceDataReader as fdr
import pandas as pd


@dataclass(frozen=True)
class MarketSpec:
    name: str
    universe_csv: Path


KOSPI = MarketSpec("kospi", Path("data/universe_kospi.csv"))
KOSDAQ = MarketSpec("kosdaq", Path("data/universe_kosdaq.csv"))


def _load_tickers_from_cache(market: str) -> list[str]:
    cache_candidates = [
        Path(f"data/cache/{market}_merged.parquet"),
        Path(f"/app/data/cache/{market}_merged.parquet"),
    ]

    for path in cache_candidates:
        if not path.exists():
            continue
        try:
            df = pd.read_parquet(path, columns=["ticker"])
            tickers = (
                df["ticker"]
                .astype(str)
                .str.replace("A", "", regex=False)
                .str.strip()
                .str.zfill(6)
            )
            tickers = tickers[tickers.str.fullmatch(r"\d{6}", na=False)].drop_duplicates().tolist()
            if tickers:
                return tickers
        except Exception:
            continue

    return []


def _load_tickers_from_listing(market: str) -> list[str]:
    try:
        df = fdr.StockListing("KRX")
    except Exception:
        return []

    if df is None or df.empty or "Code" not in df.columns:
        return []

    out = df.copy()
    if "Market" in out.columns:
        target = str(market).upper().strip()
        out = out[out["Market"].astype(str).str.upper() == target].copy()

    tickers = (
        out["Code"]
        .astype(str)
        .str.replace("A", "", regex=False)
        .str.strip()
        .str.zfill(6)
    )
    return tickers[tickers.str.fullmatch(r"\d{6}", na=False)].drop_duplicates().tolist()


def _load_universe(path: Path, limit: int | None = None) -> list[str]:
    tickers: list[str] = []

    if path.exists():
        df = pd.read_csv(path, dtype={"ticker": str})
        tickers = (
            df["ticker"]
            .astype(str)
            .str.replace("A", "", regex=False)
            .str.strip()
            .str.zfill(6)
        )
        tickers = tickers[tickers.str.fullmatch(r"\d{6}", na=False)].drop_duplicates().tolist()
    else:
        market = "kosdaq" if "kosdaq" in path.name.lower() else "kospi"
        tickers = _load_tickers_from_cache(market)
        if not tickers:
            tickers = _load_tickers_from_listing(market)

    if not tickers:
        raise FileNotFoundError(f"Universe not found and no cache fallback available: {path}")

    return tickers[:limit] if limit else tickers


def _fetch_symbol(symbol: str, start: str, end: str, sleep_sec: float = 0.0) -> pd.DataFrame:
    if sleep_sec > 0:
        time.sleep(sleep_sec)

    df = fdr.DataReader(f"NAVER:{symbol}", start, end)
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])

    out = df.reset_index().rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    out["ticker"] = symbol
    out["date"] = pd.to_datetime(out["date"])
    cols = ["date", "ticker", "open", "high", "low", "close", "volume"]
    return out[cols]


def _save_daily_files(df: pd.DataFrame, out_dir: Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    saved = 0

    for day, day_df in df.groupby(df["date"].dt.strftime("%Y%m%d")):
        out_path = out_dir / f"krx_ohlcv_{day}.csv"
        day_df = day_df.copy()
        day_df["date"] = pd.to_datetime(day_df["date"], errors="coerce").dt.strftime("%Y%m%d")
        day_df["ticker"] = day_df["ticker"].astype(str).str.zfill(6)
        day_df = day_df.dropna(subset=["date"])
        day_df = day_df.drop_duplicates(subset=["date", "ticker"], keep="last")
        day_df = day_df.sort_values(["ticker", "date"]).reset_index(drop=True)
        day_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        saved += 1

    return saved


def backfill_market(
    spec: MarketSpec,
    start: str,
    end: str,
    limit: int | None = None,
    sleep_sec: float = 0.0,
) -> tuple[pd.DataFrame, list[str], int]:
    tickers = _load_universe(spec.universe_csv, limit=limit)
    frames: list[pd.DataFrame] = []
    failed: list[str] = []

    for idx, ticker in enumerate(tickers, start=1):
        try:
            df = _fetch_symbol(ticker, start, end, sleep_sec=sleep_sec)
            if df.empty:
                failed.append(ticker)
            else:
                frames.append(df)
        except Exception:
            failed.append(ticker)

        if idx % 100 == 0:
            print(f"[{spec.name}] {idx}/{len(tickers)} done, failures={len(failed)}")

    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["date", "ticker", "open", "high", "low", "close", "volume"]
    )
    saved = _save_daily_files(merged, Path("data/daily") / spec.name) if not merged.empty else 0
    return merged, failed, saved


def backfill_all(
    start: str,
    end: str,
    markets: Iterable[MarketSpec] = (KOSPI, KOSDAQ),
    limit_by_market: dict[str, int] | None = None,
    sleep_sec: float = 0.0,
) -> None:
    limit_by_market = limit_by_market or {}
    for spec in markets:
        merged, failed, saved = backfill_market(
            spec,
            start=start,
            end=end,
            limit=limit_by_market.get(spec.name),
            sleep_sec=sleep_sec,
        )
        latest = pd.to_datetime(merged["date"]).max().date() if not merged.empty else None
        print(
            f"[{spec.name}] rows={len(merged)} saved_days={saved} latest={latest} failed={len(failed)}"
        )


def _cli() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Backfill daily KRX data via FinanceDataReader NAVER source")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--market", default="ALL", help="KOSPI, KOSDAQ, or ALL")
    parser.add_argument("--limit", type=int, default=None, help="Optional ticker limit per market")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep between requests")
    args = parser.parse_args()

    start = args.start.replace("-", "")
    end = args.end.replace("-", "")
    market = args.market.upper().strip()

    if market == "KOSPI":
        backfill_all(start, end, markets=(KOSPI,), limit_by_market={"kospi": args.limit} if args.limit else {}, sleep_sec=args.sleep)
        return 0
    if market == "KOSDAQ":
        backfill_all(start, end, markets=(KOSDAQ,), limit_by_market={"kosdaq": args.limit} if args.limit else {}, sleep_sec=args.sleep)
        return 0

    limits = {}
    if args.limit:
        limits = {"kospi": args.limit, "kosdaq": args.limit}
    backfill_all(start, end, limit_by_market=limits, sleep_sec=args.sleep)
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
