from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from core.config import DATA_DIR
from core.data_loader import load_all_markets
from core.market_filter import kospi_market_ok
from core.market_index import load_kospi_index_1y
from core.strategies import ScanParams, get_strategies
from core.universe import build_universe

PICKS_DIR = DATA_DIR / "picks"
PICKS_HISTORY_DIR = PICKS_DIR / "history"
TODAY_PICKS_PATH = PICKS_DIR / "today_picks.json"


@dataclass(frozen=True)
class PublishedPicksConfig:
    market: str = "ALL"
    top_n: int | None = None
    rank_by: str = "market_cap"
    strategy_key: str = "pullback_rr"
    market_filter_mode: str = "close_above_ma20"
    output_limit: int = 10
    tolerance: float = 0.04
    stop_lookback: int = 10
    stop_buffer: float = 0.005
    target_lookback: int = 20
    min_rr: float = 1.3
    ma5_up_days: int = 0


def _ensure_dirs() -> None:
    PICKS_DIR.mkdir(parents=True, exist_ok=True)
    PICKS_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _normalize_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return str(value)


def _load_local_name_cache() -> dict[str, str]:
    cache: dict[str, str] = {}
    candidates = [
        DATA_DIR / "ticker_name_map.json",
        DATA_DIR / "cache" / "ticker_names_kospi.json",
        DATA_DIR / "cache" / "ticker_names_kosdaq.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(raw, dict):
            cache.update({str(k).zfill(6): str(v) for k, v in raw.items()})
    return cache


def _get_strategy_by_key(strategy_key: str):
    strategies = get_strategies()
    by_key = {strategy.key: strategy for strategy in strategies}
    if strategy_key not in by_key:
        raise KeyError(f"Unknown strategy_key: {strategy_key}")
    return by_key[strategy_key]


def _default_scan_params(cfg: PublishedPicksConfig) -> ScanParams:
    return ScanParams(
        tolerance=cfg.tolerance,
        stop_lookback=cfg.stop_lookback,
        stop_buffer=cfg.stop_buffer,
        target_lookback=cfg.target_lookback,
        min_rr=cfg.min_rr,
        ma5_up_days=cfg.ma5_up_days,
    )


def _resolve_markets(market: str) -> list[str]:
    value = str(market or "ALL").strip().upper()
    if value == "ALL":
        return ["KOSPI", "KOSDAQ"]
    if value in {"KOSPI", "KOSDAQ"}:
        return [value]
    raise ValueError("market must be KOSPI, KOSDAQ, or ALL")


def _pick_history_json_path(trade_date: str) -> Path:
    ymd = trade_date.replace("-", "")
    return PICKS_HISTORY_DIR / f"today_picks_{ymd}.json"


def _pick_history_csv_path(trade_date: str) -> Path:
    ymd = trade_date.replace("-", "")
    return PICKS_HISTORY_DIR / f"today_picks_{ymd}.csv"


def load_published_picks(path: str | Path = TODAY_PICKS_PATH) -> dict[str, Any] | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_published_picks(payload: dict[str, Any], path: str | Path = TODAY_PICKS_PATH) -> Path:
    _ensure_dirs()
    out_path = Path(path)
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return out_path


def build_published_picks(config: PublishedPicksConfig | None = None) -> tuple[dict[str, Any], pd.DataFrame]:
    cfg = config or PublishedPicksConfig()
    dfs, _ = load_all_markets()
    markets = _resolve_markets(cfg.market)
    strategy = _get_strategy_by_key(cfg.strategy_key)
    params = _default_scan_params(cfg)
    idx_df = load_kospi_index_1y()
    market_ok, market_msg = kospi_market_ok(idx_df, mode=cfg.market_filter_mode)
    name_map = _load_local_name_cache()

    frames: list[pd.DataFrame] = []
    universe_rows = 0
    universe_tickers = 0
    latest_dates: list[str] = []
    per_market_counts: dict[str, int] = {}

    if market_ok:
        for market_name in markets:
            market_df, uni = build_universe(
                dfs,
                market=market_name,
                top_n=cfg.top_n,
                rank_by=cfg.rank_by,
            )
            universe_rows += int(uni.rows)
            universe_tickers += int(uni.tickers)
            if uni.latest_date:
                latest_dates.append(str(uni.latest_date))

            if market_df is None or market_df.empty:
                per_market_counts[market_name] = 0
                continue

            scan_df = strategy.scan(market_df, params)
            if scan_df is None or scan_df.empty:
                per_market_counts[market_name] = 0
                continue

            latest_rows = (
                market_df.sort_values(["ticker", "date"])
                .drop_duplicates(subset=["ticker"], keep="last")
                .loc[:, [c for c in ["ticker", "close", "volume"] if c in market_df.columns]]
            )
            market_picks = (
                scan_df.sort_values("score", ascending=False)
                .head(int(cfg.output_limit))
                .reset_index(drop=True)
                .merge(latest_rows, on="ticker", how="left")
            )
            market_picks["market"] = market_name
            per_market_counts[market_name] = int(len(market_picks))
            frames.append(market_picks)
    else:
        for market_name in markets:
            per_market_counts[market_name] = 0

    picks_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not picks_df.empty:
        picks_df = picks_df.sort_values(["score", "market"], ascending=[False, True]).reset_index(drop=True)

    trade_date = max(latest_dates) if latest_dates else datetime.now().strftime("%Y-%m-%d")

    picks: list[dict[str, Any]] = []
    for idx, row in enumerate(picks_df.to_dict("records"), start=1):
        ticker = str(row.get("ticker", "")).zfill(6)
        picks.append(
            {
                "rank": idx,
                "ticker": ticker,
                "name": name_map.get(ticker, ticker),
                "market": str(row.get("market", "")),
                "date": _normalize_scalar(row.get("date")),
                "stage": _normalize_scalar(row.get("stage")),
                "score": _normalize_scalar(row.get("score")),
                "entry": _normalize_scalar(row.get("entry")),
                "stop": _normalize_scalar(row.get("stop")),
                "target": _normalize_scalar(row.get("target")),
                "rr": _normalize_scalar(row.get("rr")),
                "close": _normalize_scalar(row.get("close")),
                "volume": _normalize_scalar(row.get("volume")),
            }
        )

    payload = {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "trade_date": trade_date,
        "source": {
            "markets": markets,
            "latest_data_date": trade_date,
            "market_filter_mode": cfg.market_filter_mode,
            "top_n": cfg.top_n,
            "strategy_key": strategy.key,
            "strategy_label": strategy.name,
            "scan_params": asdict(params),
        },
        "summary": {
            "market_ok": market_ok,
            "market_msg": market_msg,
            "pick_count": len(picks),
            "per_market_counts": per_market_counts,
            "universe_rows": universe_rows,
            "universe_tickers": universe_tickers,
        },
        "picks": picks,
    }
    return payload, picks_df


def publish_today_picks(config: PublishedPicksConfig | None = None) -> dict[str, Path]:
    payload, picks_df = build_published_picks(config=config)
    trade_date = str(payload["trade_date"])

    latest_path = save_published_picks(payload, TODAY_PICKS_PATH)
    history_json_path = save_published_picks(payload, _pick_history_json_path(trade_date))
    history_csv_path = _pick_history_csv_path(trade_date)
    _ensure_dirs()
    picks_df.to_csv(history_csv_path, index=False, encoding="utf-8-sig")

    return {
        "latest_json": latest_path,
        "history_json": history_json_path,
        "history_csv": history_csv_path,
    }
