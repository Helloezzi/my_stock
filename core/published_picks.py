from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from core.config import DATA_DIR
from core.data_loader import load_all_markets
from core.financial_snapshot import get_financial_snapshots
from core.market_filter import kospi_market_ok
from core.market_index import build_market_index_snapshot, load_kospi_index_1y
from core.strategies import ScanParams, get_strategies
from core.ticker_names import get_ticker_name_map
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
    final_pick_limit: int = 5
    max_a_picks: int = 2
    max_b_picks: int = 3
    max_watch_picks: int = 2


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
            source = raw.get("data") if isinstance(raw.get("data"), dict) else raw
            cache.update(
                {
                    str(k).zfill(6): str(v)
                    for k, v in source.items()
                    if str(k).strip().isdigit() and len(str(k).strip()) <= 6
                }
            )
    return cache


def _enrich_name_cache_for_tickers(tickers: list[str], fallback_map: dict[str, str]) -> dict[str, str]:
    normalized = [str(t).zfill(6) for t in tickers if str(t).strip()]
    if not normalized:
        return fallback_map

    missing = [ticker for ticker in normalized if not fallback_map.get(ticker) or fallback_map.get(ticker) == ticker]
    if not missing:
        return fallback_map

    try:
        resolved = get_ticker_name_map(missing, online_lookup=True)
    except Exception:
        return fallback_map

    merged = dict(fallback_map)
    for ticker, name in resolved.items():
        ticker_key = str(ticker).zfill(6)
        text = str(name).strip()
        if text and text != ticker_key:
            merged[ticker_key] = text
    return merged


def _resolve_pick_stage(strategy_key: str, row: dict[str, Any]) -> str:
    stage = _normalize_scalar(row.get("stage"))
    if stage not in (None, "", "None"):
        return str(stage)

    defaults = {
        "pullback_rr": "PULLBACK",
        "vol_compression_breakout": "WATCH",
    }
    return defaults.get(str(strategy_key).strip(), "PICK")


def _selection_note(label: str) -> str:
    notes = {
        "A": "High-conviction actionable setup",
        "B": "Secondary actionable setup",
        "Watch": "Monitor only, not first-priority",
    }
    return notes.get(label, "")


def _confidence_from_score(score: Any) -> float | None:
    value = _normalize_scalar(score)
    if value is None:
        return None
    try:
        score_value = float(value)
    except Exception:
        return None
    confidence = max(0.0, min(1.0, (score_value - 45.0) / 35.0))
    return round(confidence, 3)


def _volume_label(volume: Any) -> str:
    value = _normalize_scalar(volume)
    if value is None:
        return "unknown"
    try:
        volume_value = float(value)
    except Exception:
        return "unknown"
    if volume_value >= 1_000_000:
        return "high"
    if volume_value >= 200_000:
        return "medium"
    return "light"


def _build_reason_lines(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    rr = pd.to_numeric(row.get("rr"), errors="coerce")
    score = pd.to_numeric(row.get("score"), errors="coerce")
    risk_pct = pd.to_numeric(row.get("risk_pct"), errors="coerce")
    reward_pct = pd.to_numeric(row.get("reward_pct"), errors="coerce")
    volume = pd.to_numeric(row.get("volume"), errors="coerce")

    if pd.notna(score):
        if float(score) >= 68:
            reasons.append("technical score is strong")
        elif float(score) >= 58:
            reasons.append("technical score is acceptable")

    if pd.notna(rr):
        if float(rr) >= 2.0:
            reasons.append("risk/reward is strong")
        elif float(rr) >= 1.6:
            reasons.append("risk/reward is workable")

    if pd.notna(risk_pct) and float(risk_pct) <= 0.06:
        reasons.append("stop distance is relatively tight")

    if pd.notna(reward_pct) and float(reward_pct) >= 0.12:
        reasons.append("target upside is meaningful")

    if pd.notna(volume):
        label = _volume_label(volume)
        if label == "high":
            reasons.append("liquidity is strong")
        elif label == "medium":
            reasons.append("liquidity is usable")

    return reasons[:3]


def _build_risk_flags(row: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    rr = pd.to_numeric(row.get("rr"), errors="coerce")
    risk_pct = pd.to_numeric(row.get("risk_pct"), errors="coerce")
    volume = pd.to_numeric(row.get("volume"), errors="coerce")
    score = pd.to_numeric(row.get("score"), errors="coerce")

    if pd.notna(risk_pct) and float(risk_pct) > 0.08:
        flags.append("wide_stop")
    if pd.notna(rr) and float(rr) < 1.8:
        flags.append("rr_not_strong")
    if pd.notna(volume) and float(volume) < 200_000:
        flags.append("lighter_volume")
    if pd.notna(score) and float(score) < 60:
        flags.append("score_borderline")

    return flags


def _build_pick_extras(item: dict[str, Any]) -> tuple[str, list[str], list[str]]:
    volume_label = _volume_label(item.get("volume"))
    reasons = _build_reason_lines(item)
    flags = _build_risk_flags(item)
    return volume_label, reasons, flags


def _build_ai_summary(item: dict[str, Any]) -> str:
    label = str(item.get("action_label", "") or "").strip()
    name = str(item.get("name", "") or item.get("ticker", "")).strip()
    rr = pd.to_numeric(item.get("rr"), errors="coerce")
    score = pd.to_numeric(item.get("score"), errors="coerce")
    fin_status = str(item.get("financial_status", "") or "").strip()
    industry_name = str(item.get("industry_name", "") or "").strip()
    sector_score = pd.to_numeric(item.get("sector_score"), errors="coerce")
    reasons = item.get("why_selected") or []
    risk_flags = item.get("risk_flags") or []
    sector_flags = item.get("sector_flags") or []

    parts: list[str] = []
    if label == "A":
        parts.append(f"{name}는 오늘 우선 검토할 후보입니다.")
    elif label == "B":
        parts.append(f"{name}는 보조 검토 후보입니다.")
    elif label == "Watch":
        parts.append(f"{name}는 관찰용 후보입니다.")

    detail_parts: list[str] = []
    if pd.notna(score):
        detail_parts.append(f"기술 점수는 {float(score):.1f}")
    if pd.notna(rr):
        detail_parts.append(f"R/R은 {float(rr):.2f}")
    if fin_status:
        detail_parts.append(f"재무 상태는 {fin_status}")
    if industry_name:
        detail_parts.append(f"업종은 {industry_name}")
    if pd.notna(sector_score):
        detail_parts.append(f"섹터 점수는 {int(float(sector_score))}")
    if detail_parts:
        parts.append(", ".join(detail_parts) + " 수준입니다.")

    if reasons:
        parts.append("선정 이유: " + ", ".join([str(x) for x in reasons[:3]]) + ".")

    all_flags = [str(x) for x in (risk_flags + sector_flags) if str(x).strip()]
    if all_flags:
        parts.append("주의 요소: " + ", ".join(all_flags[:3]) + ".")

    return " ".join(parts).strip()


def _financial_score(snapshot: dict[str, Any]) -> int:
    score = 0
    eps = pd.to_numeric(snapshot.get("eps"), errors="coerce")
    bps = pd.to_numeric(snapshot.get("bps"), errors="coerce")
    per = pd.to_numeric(snapshot.get("per"), errors="coerce")
    pbr = pd.to_numeric(snapshot.get("pbr"), errors="coerce")
    dividend = pd.to_numeric(snapshot.get("dividend_yield"), errors="coerce")

    if pd.notna(eps) and float(eps) > 0:
        score += 35
    if pd.notna(bps) and float(bps) > 0:
        score += 20
    if pd.notna(per) and 0 < float(per) <= 25:
        score += 20
    elif pd.notna(per) and 25 < float(per) <= 60:
        score += 10
    if pd.notna(pbr) and 0 < float(pbr) <= 3:
        score += 15
    elif pd.notna(pbr) and 3 < float(pbr) <= 6:
        score += 8
    if pd.notna(dividend) and float(dividend) > 0:
        score += 10
    return int(min(score, 100))


def _financial_status(snapshot: dict[str, Any], score: int) -> str:
    eps = pd.to_numeric(snapshot.get("eps"), errors="coerce")
    bps = pd.to_numeric(snapshot.get("bps"), errors="coerce")
    if (pd.notna(eps) and float(eps) <= 0) or (pd.notna(bps) and float(bps) <= 0):
        return "weak"
    if score >= 65:
        return "strong"
    if score >= 40:
        return "acceptable"
    return "weak"


def _financial_flags(snapshot: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    eps = pd.to_numeric(snapshot.get("eps"), errors="coerce")
    bps = pd.to_numeric(snapshot.get("bps"), errors="coerce")
    per = pd.to_numeric(snapshot.get("per"), errors="coerce")
    pbr = pd.to_numeric(snapshot.get("pbr"), errors="coerce")

    if pd.isna(eps):
        flags.append("eps_missing")
    elif float(eps) <= 0:
        flags.append("negative_eps")

    if pd.isna(bps):
        flags.append("bps_missing")
    elif float(bps) <= 0:
        flags.append("negative_bps")

    if pd.notna(per) and float(per) > 40:
        flags.append("high_per")
    if pd.notna(pbr) and float(pbr) > 5:
        flags.append("high_pbr")

    return flags


def _industry_profile(industry_name: Any) -> str:
    text = str(industry_name or "").strip()
    if not text:
        return "unknown"

    growth_keywords = [
        "반도체",
        "전력",
        "방산",
        "소프트웨어",
        "바이오",
        "2차전지",
        "전기장비",
        "로봇",
        "AI",
        "클라우드",
    ]
    declining_keywords = [
        "섬유",
        "종이",
        "출판",
        "전통미디어",
        "담배",
    ]

    if any(keyword in text for keyword in growth_keywords):
        return "growth"
    if any(keyword in text for keyword in declining_keywords):
        return "declining"
    return "neutral"


def _sector_score(snapshot: dict[str, Any], industry_counts: dict[str, int]) -> int:
    score = 40
    industry_name = str(snapshot.get("industry_name") or "").strip()
    profile = _industry_profile(industry_name)
    industry_change = pd.to_numeric(snapshot.get("industry_change"), errors="coerce")
    peer_count = int(industry_counts.get(industry_name, 0)) if industry_name else 0

    if peer_count >= 2:
        score += 20
    if pd.notna(industry_change):
        if float(industry_change) >= 1.5:
            score += 20
        elif float(industry_change) > 0:
            score += 10
        elif float(industry_change) < -1.5:
            score -= 10
    if profile == "growth":
        score += 15
    elif profile == "declining":
        score -= 15

    return max(0, min(100, int(score)))


def _sector_flags(snapshot: dict[str, Any], industry_counts: dict[str, int]) -> list[str]:
    flags: list[str] = []
    industry_name = str(snapshot.get("industry_name") or "").strip()
    industry_change = pd.to_numeric(snapshot.get("industry_change"), errors="coerce")
    profile = _industry_profile(industry_name)
    peer_count = int(industry_counts.get(industry_name, 0)) if industry_name else 0

    if not industry_name:
        flags.append("industry_unknown")
    elif peer_count <= 1:
        flags.append("isolated_sector")

    if pd.notna(industry_change) and float(industry_change) < 0:
        flags.append("industry_down_day")
    if profile == "declining":
        flags.append("declining_industry")

    return flags


def _apply_financial_overlay(picks: list[dict[str, Any]], trade_date: str) -> list[dict[str, Any]]:
    if not picks:
        return picks

    snapshots = get_financial_snapshots([item["ticker"] for item in picks], trade_date=trade_date)
    out: list[dict[str, Any]] = []
    for item in picks:
        ticker = str(item.get("ticker", "")).zfill(6)
        snapshot = snapshots.get(ticker, {})
        fin_score = _financial_score(snapshot)
        fin_status = _financial_status(snapshot, fin_score)
        fin_flags = _financial_flags(snapshot)

        updated = dict(item)
        updated["financial_score"] = fin_score
        updated["financial_status"] = fin_status
        updated["financial_flags"] = fin_flags
        updated["per"] = _normalize_scalar(snapshot.get("per"))
        updated["eps"] = _normalize_scalar(snapshot.get("eps"))
        updated["pbr"] = _normalize_scalar(snapshot.get("pbr"))
        updated["bps"] = _normalize_scalar(snapshot.get("bps"))
        updated["dividend_yield"] = _normalize_scalar(snapshot.get("dividend_yield"))
        updated["industry_per"] = _normalize_scalar(snapshot.get("industry_per"))

        if fin_status == "weak":
            if updated.get("action_label") == "A":
                updated["action_label"] = "B"
                updated["selection_note"] = "Financial quality reduced conviction by one level"
            elif updated.get("action_label") == "B":
                updated["action_label"] = "Watch"
                updated["selection_note"] = "Financial quality reduced conviction by one level"

        out.append(updated)

    return out


def _apply_sector_overlay(picks: list[dict[str, Any]], trade_date: str) -> list[dict[str, Any]]:
    if not picks:
        return picks

    snapshots = get_financial_snapshots([item["ticker"] for item in picks], trade_date=trade_date)
    industry_counts: dict[str, int] = {}
    for ticker in [str(item["ticker"]).zfill(6) for item in picks]:
        industry_name = str((snapshots.get(ticker) or {}).get("industry_name") or "").strip()
        if industry_name:
            industry_counts[industry_name] = industry_counts.get(industry_name, 0) + 1

    out: list[dict[str, Any]] = []
    for item in picks:
        ticker = str(item.get("ticker", "")).zfill(6)
        snapshot = snapshots.get(ticker, {})
        industry_name = snapshot.get("industry_name")
        industry_change = _normalize_scalar(snapshot.get("industry_change"))
        industry_profile = _industry_profile(industry_name)
        sector_score = _sector_score(snapshot, industry_counts)
        sector_flags = _sector_flags(snapshot, industry_counts)

        updated = dict(item)
        updated["industry_name"] = _normalize_scalar(industry_name)
        updated["industry_change"] = industry_change
        updated["industry_profile"] = industry_profile
        updated["sector_score"] = sector_score
        updated["sector_flags"] = sector_flags

        if industry_profile == "declining":
            if updated.get("action_label") == "A":
                updated["action_label"] = "B"
                updated["selection_note"] = "Industry profile reduced conviction by one level"
            elif updated.get("action_label") == "B":
                updated["action_label"] = "Watch"
                updated["selection_note"] = "Industry profile reduced conviction by one level"

        out.append(updated)
    return out


def _apply_final_selection(picks_df: pd.DataFrame, cfg: PublishedPicksConfig) -> pd.DataFrame:
    if picks_df is None or picks_df.empty:
        return pd.DataFrame()

    df = picks_df.copy()
    for col in ("entry", "stop", "target", "rr", "score", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["risk_pct"] = ((df["entry"] - df["stop"]) / df["entry"]).replace([float("inf"), float("-inf")], pd.NA)
    df["reward_pct"] = ((df["target"] - df["entry"]) / df["entry"]).replace([float("inf"), float("-inf")], pd.NA)

    base_mask = (
        df["entry"].notna()
        & df["stop"].notna()
        & df["target"].notna()
        & df["rr"].notna()
        & df["score"].notna()
        & df["volume"].notna()
        & (df["rr"] >= max(cfg.min_rr, 1.5))
        & (df["risk_pct"] > 0)
        & (df["risk_pct"] <= 0.10)
        & (df["volume"] >= 50_000)
        & (df["score"] >= 50.0)
    )
    df = df[base_mask].copy()
    if df.empty:
        return df

    df = df.sort_values(["score", "rr", "volume"], ascending=[False, False, False]).reset_index(drop=True)

    a_mask = (
        (df["score"] >= 68.0)
        & (df["rr"] >= 1.8)
        & (df["risk_pct"] <= 0.08)
        & (df["volume"] >= 100_000)
    )
    b_mask = (
        (df["score"] >= 58.0)
        & (df["rr"] >= 1.6)
        & (df["risk_pct"] <= 0.10)
        & (df["volume"] >= 50_000)
    )
    watch_mask = (
        (df["score"] >= 50.0)
        & (df["rr"] >= 1.5)
        & (df["risk_pct"] <= 0.10)
        & (df["volume"] >= 50_000)
    )

    a_df = df[a_mask].head(int(cfg.max_a_picks)).copy()
    taken = set(a_df["ticker"].astype(str).tolist())

    b_df = df[b_mask & ~df["ticker"].astype(str).isin(taken)].head(int(cfg.max_b_picks)).copy()
    taken.update(b_df["ticker"].astype(str).tolist())

    remaining_slots = max(0, int(cfg.final_pick_limit) - len(a_df) - len(b_df))
    watch_limit = min(int(cfg.max_watch_picks), remaining_slots)
    watch_df = df[watch_mask & ~df["ticker"].astype(str).isin(taken)].head(watch_limit).copy()

    if not a_df.empty:
        a_df["action_label"] = "A"
    if not b_df.empty:
        b_df["action_label"] = "B"
    if not watch_df.empty:
        watch_df["action_label"] = "Watch"

    selected = pd.concat([a_df, b_df, watch_df], ignore_index=True)
    if selected.empty:
        return selected

    selected["selection_note"] = selected["action_label"].map(_selection_note)
    selected["confidence"] = selected["score"].map(_confidence_from_score)
    selected = selected.head(int(cfg.final_pick_limit)).reset_index(drop=True)

    if "action_label" in selected.columns and not (selected["action_label"] == "A").any():
        b_idx = selected.index[selected["action_label"] == "B"].tolist()
        if b_idx:
            promote_idx = b_idx[0]
            selected.loc[promote_idx, "action_label"] = "A"
            selected.loc[promote_idx, "selection_note"] = "Top actionable setup for today"
            current_conf = pd.to_numeric(selected.loc[promote_idx, "confidence"], errors="coerce")
            if pd.notna(current_conf):
                selected.loc[promote_idx, "confidence"] = round(min(1.0, float(current_conf) + 0.08), 3)

    return selected


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
    market_index_snapshot = build_market_index_snapshot()
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
                .head(max(int(cfg.output_limit), int(cfg.final_pick_limit)))
                .reset_index(drop=True)
                .merge(latest_rows, on="ticker", how="left")
            )
            market_picks["market"] = market_name
            frames.append(market_picks)
    else:
        for market_name in markets:
            per_market_counts[market_name] = 0

    picks_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not picks_df.empty:
        picks_df = _apply_final_selection(picks_df, cfg)
        picks_df = picks_df.sort_values(["score", "market"], ascending=[False, True]).reset_index(drop=True)
        pick_tickers = picks_df["ticker"].astype(str).str.zfill(6).tolist()
        name_map = _enrich_name_cache_for_tickers(pick_tickers, name_map)

    for market_name in markets:
        market_count = 0
        if not picks_df.empty and "market" in picks_df.columns:
            market_count = int((picks_df["market"].astype(str).str.upper() == market_name.upper()).sum())
        per_market_counts[market_name] = market_count

    trade_date = max(latest_dates) if latest_dates else datetime.now().strftime("%Y-%m-%d")

    picks: list[dict[str, Any]] = []
    for idx, row in enumerate(picks_df.to_dict("records"), start=1):
        ticker = str(row.get("ticker", "")).zfill(6)
        name = str(row.get("name", "")).strip() or name_map.get(ticker, ticker)
        volume_label, reasons, risk_flags = _build_pick_extras(row)
        picks.append(
            {
                "rank": idx,
                "ticker": ticker,
                "name": name,
                "market": str(row.get("market", "")),
                "date": _normalize_scalar(row.get("date")),
                "stage": _resolve_pick_stage(strategy.key, row),
                "action_label": _normalize_scalar(row.get("action_label")),
                "selection_note": _normalize_scalar(row.get("selection_note")),
                "confidence": _normalize_scalar(row.get("confidence")),
                "score": _normalize_scalar(row.get("score")),
                "entry": _normalize_scalar(row.get("entry")),
                "stop": _normalize_scalar(row.get("stop")),
                "target": _normalize_scalar(row.get("target")),
                "rr": _normalize_scalar(row.get("rr")),
                "close": _normalize_scalar(row.get("close")),
                "volume": _normalize_scalar(row.get("volume")),
                "volume_label": volume_label,
                "risk_pct": _normalize_scalar(row.get("risk_pct")),
                "reward_pct": _normalize_scalar(row.get("reward_pct")),
                "why_selected": reasons,
                "risk_flags": risk_flags,
            }
        )

    picks = _apply_financial_overlay(picks, trade_date)
    picks = _apply_sector_overlay(picks, trade_date)
    for item in picks:
        item["ai_summary"] = _build_ai_summary(item)

    label_counts: dict[str, int] = {}
    for item in picks:
        label = str(item.get("action_label", "")).strip()
        if not label:
            continue
        label_counts[label] = label_counts.get(label, 0) + 1

    actionable_count = label_counts.get("A", 0) + label_counts.get("B", 0)

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
            "selection_policy": {
                "final_pick_limit": cfg.final_pick_limit,
                "max_a_picks": cfg.max_a_picks,
                "max_b_picks": cfg.max_b_picks,
                "max_watch_picks": cfg.max_watch_picks,
            },
        },
        "summary": {
            "market_ok": market_ok,
            "market_msg": market_msg,
            "market_index_snapshot": market_index_snapshot,
            "pick_count": len(picks),
            "per_market_counts": per_market_counts,
            "label_counts": label_counts,
            "actionable_count": actionable_count,
            "no_pick": actionable_count == 0,
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
