# core/downloader_daily.py
from __future__ import annotations

import os
import json
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd
from pykrx import stock


@dataclass(frozen=True)
class DownloadResult:
    market: str
    yyyymmdd: str
    out_csv: Path
    rows: int
    ok: bool
    message: str = ""


def _to_yyyymmdd(d: date | str | None) -> str:
    if d is None:
        return datetime.now().strftime("%Y%m%d")
    if isinstance(d, date):
        return d.strftime("%Y%m%d")
    s = str(d).strip()
    if len(s) == 8 and s.isdigit():
        return s
    # Allow YYYY-MM-DD
    return datetime.strptime(s, "%Y-%m-%d").strftime("%Y%m%d")


def _ensure_business_day(yyyymmdd: str, max_back: int = 10, market_probe: str = "KOSPI") -> str:
    """Return a yyyymmdd that has non-empty data (walk backwards if needed)."""
    d = datetime.strptime(yyyymmdd, "%Y%m%d").date()
    for _ in range(max_back):
        ds = d.strftime("%Y%m%d")
        try:
            cap = stock.get_market_cap_by_ticker(ds, market=market_probe)
            if cap is not None and not cap.empty:
                return ds
        except Exception:
            pass
        d -= timedelta(days=1)
    return yyyymmdd


def _atomic_write_csv(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        delete=False,
        dir=str(out_path.parent),
        suffix=".tmp",
        encoding="utf-8-sig",
    ) as f:
        tmp_path = Path(f.name)
        df.to_csv(f, index=False)
    os.replace(tmp_path, out_path)


def _normalize_columns(df: pd.DataFrame, yyyymmdd: str) -> pd.DataFrame:
    """Normalize KRX(Korean) column names to stable English names."""
    out = df.copy()

    rename_map = {
        "시가": "open",
        "고가": "high",
        "저가": "low",
        "종가": "close",
        "거래량": "volume",
        "거래대금": "value",
        "시가총액": "market_cap",
        "상장주식수": "shares",
    }
    out = out.rename(columns={k: v for k, v in rename_map.items() if k in out.columns})

    # ticker index -> column
    out = out.reset_index()
    if out.columns[0] != "ticker":
        out = out.rename(columns={out.columns[0]: "ticker"})

    out["ticker"] = out["ticker"].astype(str).str.zfill(6)
    out.insert(0, "date", yyyymmdd)

    preferred = [
        "date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "market_cap",
        "shares",
    ]
    cols = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]
    return out[cols]


def download_daily_one_market(
    yyyymmdd: str | date | None = None,
    market: str = "KOSPI",
    out_dir: str | Path = "data/daily",
    force: bool = False,
    min_rows: int = 50,
) -> DownloadResult:
    """
    Download one-day OHLCV(+cap) for a single market and write to CSV.

    Output path:
      data/daily/<market.lower()>/krx_ohlcv_YYYYMMDD.csv

    Notes:
    - Uses pykrx: get_market_ohlcv_by_ticker + get_market_cap_by_ticker.
    - If the requested day has no data (weekend/holiday), it walks back to nearest day with data.
    """
    market = market.upper().strip()
    if market not in {"KOSPI", "KOSDAQ"}:
        raise ValueError("market must be 'KOSPI' or 'KOSDAQ'")

    target = _to_yyyymmdd(yyyymmdd)
    target = _ensure_business_day(target, max_back=10, market_probe=market)

    out_dir = Path(out_dir)
    out_path = out_dir / market.lower() / f"krx_ohlcv_{target}.csv"

    if out_path.exists() and not force:
        return DownloadResult(market=market, yyyymmdd=target, out_csv=out_path, rows=0, ok=True, message="already exists")

    try:
        ohlcv = stock.get_market_ohlcv_by_ticker(target, market=market)
        if ohlcv is None or ohlcv.empty:
            return DownloadResult(market=market, yyyymmdd=target, out_csv=out_path, rows=0, ok=False, message="ohlcv is empty")

        cap = stock.get_market_cap_by_ticker(target, market=market)  # may be empty sometimes
        df = ohlcv.copy()
        if cap is not None and not cap.empty:
            df = df.join(cap, how="left", rsuffix="_cap")

        df = _normalize_columns(df, target)

        # quick sanity check
        if len(df) < min_rows:
            _atomic_write_csv(df, out_path)
            return DownloadResult(market=market, yyyymmdd=target, out_csv=out_path, rows=int(len(df)), ok=False, message=f"too few rows (<{min_rows})")

        _atomic_write_csv(df, out_path)
        return DownloadResult(market=market, yyyymmdd=target, out_csv=out_path, rows=int(len(df)), ok=True, message="ok")

    except Exception as e:
        return DownloadResult(market=market, yyyymmdd=target, out_csv=out_path, rows=0, ok=False, message=str(e))


def download_daily_all(
    yyyymmdd: str | date | None = None,
    markets: Iterable[str] = ("KOSPI", "KOSDAQ"),
    out_dir: str | Path = "data/daily",
    force: bool = False,
    min_rows_by_market: Optional[Dict[str, int]] = None,
) -> Dict[str, DownloadResult]:
    """Download one day for multiple markets."""
    if min_rows_by_market is None:
        # 대략적인 안전장치(너무 적으면 실패로 판단)
        min_rows_by_market = {"KOSPI": 500, "KOSDAQ": 1200}

    results: Dict[str, DownloadResult] = {}
    for m in markets:
        mm = str(m).upper().strip()
        results[mm] = download_daily_one_market(
            yyyymmdd=yyyymmdd,
            market=mm,
            out_dir=out_dir,
            force=force,
            min_rows=int(min_rows_by_market.get(mm, 50)),
        )
    return results


def _cli() -> int:
    import argparse

    p = argparse.ArgumentParser(description="Download daily KRX OHLCV (KOSPI/KOSDAQ) and save CSV per market")
    p.add_argument("--date", default=None, help="Target date (YYYYMMDD or YYYY-MM-DD). Default: today.")
    p.add_argument("--market", default="ALL", help="KOSPI, KOSDAQ, or ALL")
    p.add_argument("--out-dir", default="data/daily", help="Base output directory")
    p.add_argument("--force", action="store_true", help="Overwrite existing csv")
    args = p.parse_args()

    market = args.market.upper().strip()
    if market == "ALL":
        res = download_daily_all(args.date, out_dir=args.out_dir, force=args.force)
        payload = {k: vars(v) for k, v in res.items()}
        print(json.dumps(payload, ensure_ascii=False, default=str, indent=2))
        ok = all(v.ok for v in res.values())
        return 0 if ok else 2

    r = download_daily_one_market(args.date, market=market, out_dir=args.out_dir, force=args.force)
    print(json.dumps(vars(r), ensure_ascii=False, default=str, indent=2))
    return 0 if r.ok else 2


if __name__ == "__main__":
    raise SystemExit(_cli())