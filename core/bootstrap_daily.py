# core/bootstrap_daily.py
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from pykrx import stock

from core.downloader_daily import download_daily_one_market, DownloadResult


@dataclass(frozen=True)
class BootstrapSummary:
    market: str
    start: str
    end: str
    probed_days: int
    business_days: int
    skipped_existing: int
    downloaded: int
    failed: int


def _to_date(s: str | date | None) -> date:
    if s is None:
        return datetime.now().date()
    if isinstance(s, date):
        return s
    s = s.strip()
    # allow YYYYMMDD or YYYY-MM-DD
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%Y%m%d").date()
    return datetime.strptime(s, "%Y-%m-%d").date()


def _to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _iter_dates_inclusive(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _is_business_day(yyyymmdd: str, market: str) -> bool:
    """
    Decide if yyyymmdd is a trading day by probing market cap table.
    If empty -> not a trading day (weekend/holiday).
    """
    try:
        cap = stock.get_market_cap_by_ticker(yyyymmdd, market=market)
        return cap is not None and not cap.empty
    except Exception:
        return False


def bootstrap_market_daily(
    market: str,
    start: str | date,
    end: str | date,
    out_dir: str | Path = "data/daily",
    force: bool = False,
    sleep_sec: float = 0.05,
    min_rows: Optional[int] = None,
    verbose: bool = True,
) -> Tuple[List[DownloadResult], BootstrapSummary]:
    """
    Bootstrap daily CSV files for a given market in [start, end] date range.

    - Skips non-trading days automatically
    - Skips already existing daily files unless force=True
    - Writes one CSV per trading day into:
        data/daily/<market.lower()>/krx_ohlcv_YYYYMMDD.csv
    """
    m = market.upper().strip()
    if m not in {"KOSPI", "KOSDAQ"}:
        raise ValueError("market must be KOSPI or KOSDAQ")

    start_d = _to_date(start)
    end_d = _to_date(end)
    if start_d > end_d:
        start_d, end_d = end_d, start_d

    # per-market sensible defaults
    if min_rows is None:
        min_rows = 500 if m == "KOSPI" else 1200

    results: List[DownloadResult] = []
    probed = 0
    biz = 0
    skipped_existing = 0
    downloaded = 0
    failed = 0

    out_base = Path(out_dir) / m.lower()
    out_base.mkdir(parents=True, exist_ok=True)

    if verbose:
        print(f"[bootstrap] {m} {start_d} -> {end_d} | out={out_base}")

    for d in _iter_dates_inclusive(start_d, end_d):
        probed += 1
        yyyymmdd = _to_yyyymmdd(d)
        out_csv = out_base / f"krx_ohlcv_{yyyymmdd}.csv"

        if out_csv.exists() and not force:
            skipped_existing += 1
            continue

        if not _is_business_day(yyyymmdd, m):
            continue

        biz += 1
        r = download_daily_one_market(
            yyyymmdd=yyyymmdd,
            market=m,
            out_dir=out_dir,
            force=force,
            min_rows=min_rows,
        )
        results.append(r)

        if r.ok:
            downloaded += 1
            if verbose:
                print(f"  OK  {m} {yyyymmdd} rows={r.rows}")
        else:
            failed += 1
            if verbose:
                print(f"  FAIL {m} {yyyymmdd} msg={r.message}")

        if sleep_sec and sleep_sec > 0:
            time.sleep(sleep_sec)

    summary = BootstrapSummary(
        market=m,
        start=_to_yyyymmdd(start_d),
        end=_to_yyyymmdd(end_d),
        probed_days=probed,
        business_days=biz,
        skipped_existing=skipped_existing,
        downloaded=downloaded,
        failed=failed,
    )
    return results, summary


def bootstrap_all(
    start: str | date,
    end: str | date,
    markets: Iterable[str] = ("KOSPI", "KOSDAQ"),
    out_dir: str | Path = "data/daily",
    force: bool = False,
    sleep_sec: float = 0.05,
    verbose: bool = True,
) -> Tuple[Dict[str, List[DownloadResult]], Dict[str, BootstrapSummary]]:
    all_results: Dict[str, List[DownloadResult]] = {}
    summaries: Dict[str, BootstrapSummary] = {}

    for m in markets:
        res, summ = bootstrap_market_daily(
            market=m,
            start=start,
            end=end,
            out_dir=out_dir,
            force=force,
            sleep_sec=sleep_sec,
            verbose=verbose,
        )
        all_results[m.upper()] = res
        summaries[m.upper()] = summ

    return all_results, summaries


def _cli() -> int:
    import argparse

    p = argparse.ArgumentParser(description="Bootstrap daily KRX OHLCV CSVs for a date range")
    p.add_argument("--start", required=True, help="Start date (YYYYMMDD or YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date (YYYYMMDD or YYYY-MM-DD)")
    p.add_argument("--market", default="ALL", help="KOSPI, KOSDAQ, or ALL")
    p.add_argument("--out-dir", default="data/daily", help="Base output directory")
    p.add_argument("--force", action="store_true", help="Overwrite existing daily csv")
    p.add_argument("--sleep", type=float, default=0.05, help="Sleep seconds between requests (rate-limit safety)")
    p.add_argument("--quiet", action="store_true", help="Less output")
    args = p.parse_args()

    market = args.market.upper().strip()
    verbose = not args.quiet

    if market == "ALL":
        _, summaries = bootstrap_all(
            start=args.start,
            end=args.end,
            out_dir=args.out_dir,
            force=args.force,
            sleep_sec=args.sleep,
            verbose=verbose,
        )
        print(json.dumps({k: vars(v) for k, v in summaries.items()}, ensure_ascii=False, indent=2))
        ok = all(v.failed == 0 for v in summaries.values())
        return 0 if ok else 2

    res, summ = bootstrap_market_daily(
        market=market,
        start=args.start,
        end=args.end,
        out_dir=args.out_dir,
        force=args.force,
        sleep_sec=args.sleep,
        verbose=verbose,
    )
    print(json.dumps(vars(summ), ensure_ascii=False, indent=2))
    ok = summ.failed == 0
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(_cli())