# core/ticker_names.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import streamlit as st
from pykrx import stock
from core.config import DATA_DIR

NAME_CACHE_PATH = DATA_DIR / "ticker_name_map.json"


def _load_cache() -> Dict[str, str]:
    if not NAME_CACHE_PATH.exists():
        return {}
    try:
        return json.loads(NAME_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cache(cache: Dict[str, str]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    NAME_CACHE_PATH.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


@st.cache_data(show_spinner=False)
def get_ticker_name_map(tickers: list[str]) -> dict[str, str]:
    tickers = [str(t).zfill(6) for t in tickers]
    cache = _load_cache()

    # ✅ 없거나, 값이 티커 그대로면(과거 실패로 박제된 케이스) 다시 조회 대상으로 간주
    missing = [t for t in tickers if (t not in cache) or (cache.get(t) == t)]

    if missing:
        for t in missing:
            try:
                nm = stock.get_market_ticker_name(t)
                if nm:
                    cache[t] = nm
                else:
                    # nm이 비면 저장하지 말고 다음에 재시도 여지 남김
                    cache.pop(t, None)
            except Exception:
                # 실패 저장 금지(박제 방지)
                cache.pop(t, None)

        _save_cache(cache)

    return {t: cache.get(t, t) for t in tickers}

def clear_name_cache() -> None:
    """원하면 UI 버튼에 연결해서 캐시 초기화 가능."""
    try:
        NAME_CACHE_PATH.unlink(missing_ok=True)
    except Exception:
        pass
    st.cache_data.clear()