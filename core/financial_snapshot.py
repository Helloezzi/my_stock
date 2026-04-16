from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from core.config import DATA_DIR

CACHE_DIR = DATA_DIR / "cache" / "financial_snapshots"
NAVER_ITEM_URL = "https://finance.naver.com/item/main.naver?code={ticker}"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def _normalize_number(text: str) -> float | None:
    raw = str(text or "").strip()
    if not raw or raw in {"N/A", "-", "적자"}:
        return None
    cleaned = raw.replace(",", "").replace("배", "").replace("원", "").replace("%", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def _first_number_pair(text: str) -> tuple[float | None, float | None]:
    parts = [part.strip() for part in str(text or "").split("l")]
    if len(parts) < 2:
        parts = [part.strip() for part in str(text or "").split("|")]
    left = _normalize_number(parts[0]) if len(parts) >= 1 else None
    right = _normalize_number(parts[1]) if len(parts) >= 2 else None
    return left, right


def _fetch_from_naver(ticker: str) -> dict[str, Any]:
    url = NAVER_ITEM_URL.format(ticker=str(ticker).zfill(6))
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    rows = soup.select("#tab_con1 table tr")
    info: dict[str, Any] = {
        "ticker": str(ticker).zfill(6),
        "per": None,
        "eps": None,
        "estimated_per": None,
        "estimated_eps": None,
        "pbr": None,
        "bps": None,
        "dividend_yield": None,
        "industry_per": None,
        "industry_name": None,
        "industry_change": None,
    }

    for a in soup.select("a[href*='sise_group_detail.naver?type=upjong']"):
        text = a.get_text(" ", strip=True)
        href = a.get("href", "")
        if not text or text in {"더보기", "동일업종 PER", "동일업종 등락률", "업종별"}:
            continue
        info["industry_name"] = text
        m = re.search(r"no=(\d+)", href)
        if m:
            info["industry_code"] = m.group(1)
        break

    for tr in rows:
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        key = th.get_text(" ", strip=True)
        value = td.get_text(" ", strip=True)

        if key.startswith("PER l EPS"):
            info["per"], info["eps"] = _first_number_pair(value)
        elif key.startswith("추정PER l EPS"):
            info["estimated_per"], info["estimated_eps"] = _first_number_pair(value)
        elif key.startswith("PBR l BPS"):
            info["pbr"], info["bps"] = _first_number_pair(value)
        elif key.startswith("배당수익률"):
            info["dividend_yield"] = _normalize_number(value)
        elif key.startswith("동일업종 PER"):
            info["industry_per"] = _normalize_number(value)
        elif key.startswith("동일업종 등락률"):
            info["industry_change"] = _normalize_number(value)

    return info


def _cache_path(trade_date: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"financial_snapshot_{trade_date.replace('-', '')}.json"


def load_financial_cache(trade_date: str) -> dict[str, Any]:
    path = _cache_path(trade_date)
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def save_financial_cache(trade_date: str, payload: dict[str, Any]) -> None:
    path = _cache_path(trade_date)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def get_financial_snapshots(tickers: list[str], trade_date: str) -> dict[str, dict[str, Any]]:
    cache = load_financial_cache(trade_date)
    out: dict[str, dict[str, Any]] = {}

    for ticker in [str(t).zfill(6) for t in tickers if str(t).strip()]:
        cached = cache.get(ticker)
        cache_ok = (
            isinstance(cached, dict)
            and bool(cached)
            and "industry_name" in cached
            and "industry_change" in cached
        )
        if cache_ok:
            out[ticker] = cached
            continue
        try:
            snapshot = _fetch_from_naver(ticker)
        except Exception:
            snapshot = {"ticker": ticker}
        cache[ticker] = snapshot
        out[ticker] = snapshot

    save_financial_cache(trade_date, cache)
    return out
