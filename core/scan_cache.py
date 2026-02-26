# core/scan_cache.py
from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from core.config import DATA_DIR

SCAN_CACHE_DIR = DATA_DIR / "scan_cache"


def ensure_scan_cache_dir() -> None:
    SCAN_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def scan_signature(
    latest_date: str,
    market: str,
    top_n: Any,
    strategy_label: str,
    market_mode: str,
    params: Any,
) -> str:
    # params -> json-safe dict
    if hasattr(params, "__dict__"):
        params_obj = params.__dict__
    elif isinstance(params, dict):
        params_obj = params
    else:
        # 마지막 fallback
        params_obj = {"value": str(params)}

    payload = {
        "latest_date": str(latest_date),
        "market": str(market).upper(),
        "top_n": top_n,
        "strategy": strategy_label,
        "market_mode": market_mode,
        "params": params_obj,
    }
    s = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def cache_path(sig: str) -> Path:
    ensure_scan_cache_dir()
    return SCAN_CACHE_DIR / f"scan_{sig}.parquet"


def load_cached_scan(sig: str) -> Optional[pd.DataFrame]:
    p = cache_path(sig)
    if not p.exists():
        return None
    try:
        return pd.read_parquet(p)
    except Exception:
        try:
            p.unlink(missing_ok=True)
        except Exception:
            pass
        return None


def save_cached_scan(sig: str, df: pd.DataFrame) -> None:
    p = cache_path(sig)
    df.to_parquet(p, index=False)


def levels_path(sig: str) -> Path:
    ensure_scan_cache_dir()
    return SCAN_CACHE_DIR / f"levels_{sig}.json"


def load_cached_levels(sig: str) -> Optional[Dict[str, Any]]:
    p = levels_path(sig)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        try:
            p.unlink(missing_ok=True)
        except Exception:
            pass
        return None


def save_cached_levels(sig: str, levels: Dict[str, Any]) -> None:
    p = levels_path(sig)
    p.write_text(json.dumps(levels, ensure_ascii=False), encoding="utf-8")