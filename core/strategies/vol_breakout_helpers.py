from __future__ import annotations

from typing import Any

import pandas as pd

from .base import ScanParams


RESULT_COLUMNS = [
    "ticker",
    "date",
    "stage",
    "entry",
    "stop",
    "target",
    "risk",
    "reward",
    "rr",
    "bb_width",
    "bb_q20",
    "range20",
    "ma_gap",
    "vol_ratio_5v20",
    "prev20_high",
    "high_break",
    "close_hold",
    "vol_surge_ratio",
    "ma20",
    "ma60",
    "ret20",
    "compression_score",
    "trend_score",
    "score",
]


def clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else (1.0 if x > 1.0 else float(x))


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    if b == 0 or pd.isna(b) or pd.isna(a):
        return default
    return float(a) / float(b)


def prepare_group_indicators(g: pd.DataFrame, strategy: Any) -> pd.DataFrame:
    g = g.sort_values("date").copy()
    g["value"] = g["close"] * g["volume"]
    g["ma20"] = g["close"].rolling(20).mean()
    g["ma60"] = g["close"].rolling(60).mean()
    g["std20"] = g["close"].pct_change().rolling(strategy.BB_LOOKBACK).std()
    g["bb_width"] = 4.0 * g["std20"]
    g["vol_ma20"] = g["volume"].rolling(20).mean()
    g["vol_ma20_prev"] = g["vol_ma20"].shift(1)

    den = g["close"].replace(0, pd.NA)
    g["range20"] = (
        g["high"].rolling(strategy.RANGE_LOOKBACK).max()
        - g["low"].rolling(strategy.RANGE_LOOKBACK).min()
    ) / den
    g["prev20_high"] = g["high"].rolling(strategy.BREAKOUT_LOOKBACK).max().shift(1)
    g["ret20"] = g["close"].pct_change(20)
    g["ret1"] = g["close"].pct_change()
    g["vol_5_mean"] = g["volume"].rolling(5).mean()
    return g


def passes_basic_filters(g: pd.DataFrame, strategy: Any) -> bool:
    if len(g) < strategy.MIN_HISTORY:
        return False

    if "market_cap" in g.columns:
        mcap = pd.to_numeric(g["market_cap"], errors="coerce").iloc[-1]
        if pd.notna(mcap) and float(mcap) < strategy.MIN_MARKET_CAP:
            return False

    value_ma20 = float(g["value"].rolling(20).mean().iloc[-1]) if len(g) >= 20 else 0.0
    return value_ma20 >= strategy.MIN_VALUE_MA20


def evaluate_candidate(g: pd.DataFrame, ticker: str, strategy: Any, params: ScanParams) -> dict[str, object] | None:
    last = g.iloc[-1]
    if pd.isna(last["ma20"]) or pd.isna(last["ma60"]) or pd.isna(last["bb_width"]) or pd.isna(last["prev20_high"]):
        return None

    std60 = float(g["ret1"].tail(60).std())
    if std60 > strategy.MAX_STD60:
        return None

    prev_close = float(g["close"].iloc[-2]) if len(g) >= 2 else float("nan")
    day_range = safe_div(float(last["high"]) - float(last["low"]), float(last["close"]), default=0.0)
    if day_range > strategy.MAX_DAY_RANGE:
        return None

    gap_up = safe_div(float(last["open"]) - prev_close, prev_close, default=0.0)
    if gap_up > strategy.MAX_GAP_UP:
        return None

    bb_window = g["bb_width"].tail(strategy.BB_WINDOW_FOR_PERCENTILE).dropna()
    if len(bb_window) < int(strategy.BB_WINDOW_FOR_PERCENTILE * 0.7):
        return None

    bb_q = float(bb_window.quantile(strategy.BB_WIDTH_Q))
    bb_width = float(last["bb_width"])
    bb_is_compressed = (bb_width <= bb_q) if bb_q > 0 else False

    bb_ok_5 = int((g["bb_width"].tail(5) <= bb_q).sum())
    if bb_ok_5 < strategy.MIN_BB_OK_5:
        return None

    close_value = float(last["close"])
    ma_gap = abs(float(last["ma20"]) - float(last["ma60"])) / close_value if close_value != 0 else 1.0
    ma_converged = ma_gap <= float(params.tolerance)

    range20 = float(last["range20"]) if not pd.isna(last["range20"]) else 1.0
    in_box = range20 <= strategy.RANGE_MAX

    vol_5 = float(g["vol_5_mean"].iloc[-1]) if not pd.isna(g["vol_5_mean"].iloc[-1]) else 0.0
    vol_ma20 = float(last["vol_ma20"]) if not pd.isna(last["vol_ma20"]) else 0.0
    vol_ratio_5v20 = safe_div(vol_5, vol_ma20, default=999.0)
    vol_dry = vol_ratio_5v20 <= strategy.VOL_RATIO_MAX

    if not bool(bb_is_compressed and ma_converged and in_box and vol_dry):
        return None

    prev20_high = float(last["prev20_high"])
    high_break = float(last["high"]) > prev20_high
    close_hold = close_value > prev20_high
    breakout_confirmed = bool(high_break and close_hold)

    if breakout_confirmed:
        close_margin = (close_value / prev20_high - 1.0) if prev20_high > 0 else 0.0
        if close_margin < strategy.MIN_CLOSE_MARGIN:
            return None

    vol_surge_ratio = safe_div(float(last["volume"]), float(last["vol_ma20_prev"]), default=0.0)
    vol_surge_ok = bool(vol_surge_ratio >= strategy.VOL_SURGE_MIN)
    if breakout_confirmed:
        vol_5_mean = float(g["vol_5_mean"].iloc[-1]) if not pd.isna(g["vol_5_mean"].iloc[-1]) else 0.0
        vol_vs_5 = safe_div(float(last["volume"]), vol_5_mean, default=0.0)
        if vol_surge_ratio < strategy.VOL_SURGE_MIN:
            return None
        if vol_vs_5 < strategy.VOL_SURGE_VS_VOL5_MIN:
            return None

    stage = "BREAKOUT" if breakout_confirmed and vol_surge_ok else "WATCH"

    entry = close_value
    recent_low = float(g["low"].tail(params.stop_lookback).min())
    stop = recent_low * (1.0 - float(params.stop_buffer))
    risk = entry - stop
    if risk <= 0:
        return None

    target_a = float(g["high"].tail(params.target_lookback).max())
    target_b = entry + 2.0 * risk
    target = max(target_a, target_b)
    reward = target - entry
    if reward <= 0:
        return None

    rr = reward / risk
    if stage == "BREAKOUT" and rr < float(params.min_rr):
        return None

    bb_score = 0.0
    if bb_q > 0:
        bb_score = clamp01(1.0 - (bb_width / bb_q - 1.0))

    range_score = clamp01(1.0 - (range20 / strategy.RANGE_MAX))
    ma_score = clamp01(1.0 - (ma_gap / max(float(params.tolerance), 1e-9)))
    vol_dry_score = clamp01(1.0 - (vol_ratio_5v20 / strategy.VOL_RATIO_MAX))
    compression_score = 0.35 * bb_score + 0.25 * range_score + 0.20 * ma_score + 0.20 * vol_dry_score

    trend_up = 1.0 if float(last["ma20"]) > float(last["ma60"]) else 0.0
    ret20 = float(last["ret20"]) if not pd.isna(last["ret20"]) else 0.0
    trend_score = 0.6 * trend_up + 0.4 * clamp01(ret20 / 0.10)

    breakout_score = 1.0 if breakout_confirmed else 0.0
    vol_surge_score = clamp01((vol_surge_ratio - strategy.VOL_SURGE_MIN) / 1.0) if vol_surge_ratio >= strategy.VOL_SURGE_MIN else 0.0

    if stage == "WATCH":
        total01 = 0.70 * compression_score + 0.30 * trend_score
    else:
        total01 = (
            0.45 * compression_score
            + 0.15 * trend_score
            + 0.20 * breakout_score
            + 0.20 * clamp01(0.5 + 0.5 * vol_surge_score)
        )

    score = 100.0 * clamp01(total01)

    return {
        "ticker": ticker,
        "date": pd.to_datetime(last["date"]),
        "stage": stage,
        "entry": entry,
        "stop": stop,
        "target": target,
        "risk": float(risk),
        "reward": float(reward),
        "rr": float(rr),
        "bb_width": float(bb_width),
        "bb_q20": float(bb_q),
        "range20": float(range20),
        "ma_gap": float(ma_gap),
        "vol_ratio_5v20": float(vol_ratio_5v20),
        "prev20_high": float(prev20_high),
        "high_break": bool(high_break),
        "close_hold": bool(close_hold),
        "vol_surge_ratio": float(vol_surge_ratio),
        "ma20": float(last["ma20"]),
        "ma60": float(last["ma60"]),
        "ret20": float(ret20),
        "compression_score": float(compression_score),
        "trend_score": float(trend_score),
        "score": float(score),
    }


def build_result_frame(results: list[dict[str, object]]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame(columns=RESULT_COLUMNS)

    out = pd.DataFrame(results)
    stage_rank = {"BREAKOUT": 0, "WATCH": 1}
    out["stage_rank"] = out["stage"].map(stage_rank).fillna(9).astype(int)
    return (
        out.sort_values(["stage_rank", "score"], ascending=[True, False])
        .drop(columns=["stage_rank"])
        .reset_index(drop=True)
    )
