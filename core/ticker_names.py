# core/ticker_names.py
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict

from pykrx import stock


CACHE_DIR = Path("data/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _today_str() -> str:
    return datetime.now().strftime("%Y%m%d")


def _cache_path(market: str) -> Path:
    market = market.lower()
    return CACHE_DIR / f"ticker_names_{market}.json"


def _download_name_map(market: str) -> Dict[str, str]:
    """
    Download ticker name map for given market using pykrx.
    """
    market = market.upper()
    today = _today_str()

    tickers = stock.get_market_ticker_list(today, market=market)

    name_map = {}
    for t in tickers:
        try:
            name = stock.get_market_ticker_name(t)
            name_map[str(t).zfill(6)] = name
        except Exception:
            name_map[str(t).zfill(6)] = str(t).zfill(6)

    return name_map


def load_ticker_name_map(market: str, force_refresh: bool = False) -> Dict[str, str]:
    """
    Load ticker name map with daily cache.
    """
    market = market.upper()
    cache_file = _cache_path(market)

    if cache_file.exists() and not force_refresh:
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if payload.get("date") == _today_str():
                return payload["data"]
        except Exception:
            pass

    # refresh
    name_map = _download_name_map(market)

    payload = {
        "date": _today_str(),
        "data": name_map,
    }

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    return name_map


def load_all_name_maps(force_refresh: bool = False) -> Dict[str, Dict[str, str]]:
    """
    Return {"KOSPI": {...}, "KOSDAQ": {...}}
    """
    return {
        "KOSPI": load_ticker_name_map("KOSPI", force_refresh=force_refresh),
        "KOSDAQ": load_ticker_name_map("KOSDAQ", force_refresh=force_refresh),
    }