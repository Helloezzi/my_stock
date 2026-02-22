# core/strategies/pullback_rr.py
import pandas as pd
from .base import Strategy, ScanParams


def _clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else (1.0 if x > 1.0 else float(x))


def _rr_preference_score(rr: float, center: float = 2.15, half_width: float = 1.35) -> float:
    """
    0..1 score peaking near center.
    기본값(center=2.15)은 RR 1.8~2.5 구간을 가장 선호하도록 설계.
    """
    return _clamp01(1.0 - abs(rr - center) / half_width)


class PullbackRRStrategy(Strategy):
    key = "pullback_rr"
    name = "Pullback + Risk/Reward"

    def scan(self, df: pd.DataFrame, params: ScanParams) -> pd.DataFrame:
        results = []

        for t, g in df.groupby("ticker"):
            g = g.sort_values("date").copy()
            if len(g) < 120:
                continue

            # Core indicators
            g["ma5"] = g["close"].rolling(5).mean()
            g["ma20"] = g["close"].rolling(20).mean()
            g["ma60"] = g["close"].rolling(60).mean()
            g["vol_ma20"] = g["volume"].rolling(20).mean()

            # Extra indicators for scoring
            g["std20"] = g["close"].pct_change().rolling(20).std()
            g["ret20"] = g["close"].pct_change(20)

            last = g.iloc[-1]
            if pd.isna(last["ma20"]) or pd.isna(last["ma60"]) or pd.isna(last["vol_ma20"]):
                continue

            # --- Base setup filters (기존 유지) ---
            uptrend = last["ma20"] > last["ma60"]

            high20 = g["high"].rolling(20).max().iloc[-1]
            high60 = g["high"].rolling(60).max().iloc[-1]
            had_momentum = (
                (not pd.isna(high20)) and
                (not pd.isna(high60)) and
                (high20 >= high60 * 0.98)
            )

            near_ma20 = abs(last["close"] - last["ma20"]) / last["ma20"] <= params.tolerance

            vol_5 = g["volume"].tail(5).mean()
            vol_cooling = vol_5 < last["vol_ma20"]

            if not (uptrend and had_momentum and near_ma20 and vol_cooling):
                continue

            # --- Entry/Stop/Target ---
            entry = float(last["close"])
            recent_low = float(g["low"].tail(params.stop_lookback).min())
            stop = recent_low * (1.0 - params.stop_buffer)
            target = float(g["high"].tail(params.target_lookback).max())

            risk = entry - stop
            reward = target - entry
            if risk <= 0 or reward <= 0:
                continue

            rr = reward / risk
            if rr < params.min_rr:
                continue

            # MA5 slope (short-term momentum)
            ma5_now = float(last["ma5"]) if not pd.isna(last.get("ma5")) else None
            ma5_3ago = float(g["ma5"].iloc[-4]) if len(g) >= 4 and not pd.isna(g["ma5"].iloc[-4]) else ma5_now

            ma5_slope_3d = 0.0
            if ma5_now is not None and ma5_3ago is not None and ma5_3ago != 0:
                ma5_slope_3d = (ma5_now / ma5_3ago - 1.0)

            # +1% / 3D => 1.0, 0% => 0.0, 음수 => 0.0
            ma5_slope_score = _clamp01(ma5_slope_3d / 0.01)

            # MA5 slope filter (optional)
            if params.require_ma5_positive:
                if ma5_slope_3d <= params.ma5_min_slope:
                    continue

            # =========================
            # Scoring (0..100)
            # =========================

            # 1) RR preference (peak around 1.8~2.5)
            rr_pref = _rr_preference_score(float(rr))

            # 2) Trend quality: MA20 slope + 20D return
            ma20_now = float(last["ma20"])
            ma20_5ago = float(g["ma20"].iloc[-6]) if len(g) >= 6 and not pd.isna(g["ma20"].iloc[-6]) else ma20_now
            ma20_slope_5d = 0.0 if ma20_5ago == 0 else (ma20_now / ma20_5ago - 1.0)
            # 2% 상승/5일이면 만점
            trend_slope = _clamp01(ma20_slope_5d / 0.02)

            ret20 = float(last["ret20"]) if not pd.isna(last.get("ret20")) else 0.0
            # 10% 상승/20일이면 만점
            trend_ret = _clamp01(ret20 / 0.10)

            trend_score = 0.6 * trend_slope + 0.4 * trend_ret

            # 3) Volatility compression (BB width proxy: 4*std20)
            std20 = float(last["std20"]) if not pd.isna(last.get("std20")) else 0.0
            bb_width = 4.0 * std20
            # 20% 폭이면 0점, 0%면 1점
            vol_score = _clamp01(1.0 - (bb_width / 0.20))

            # 4) Volume behavior: prefer cooling ratio near ~0.75
            vol_ma20 = float(last["vol_ma20"])
            vol_ratio_5v20 = float(vol_5 / vol_ma20) if vol_ma20 != 0 else 1.0
            vol_score2 = _clamp01(1.0 - abs(vol_ratio_5v20 - 0.75) / 0.75)

            # 5) Relative strength: 후보들 ret20 percentile로 계산(후처리)
            rs_score = 0.0

            # Weighted total (0..1)
            total01 = (
                0.35 * rr_pref
                + 0.20 * trend_score
                + 0.15 * vol_score
                + 0.10 * vol_score2
                + 0.10 * rs_score
                + 0.10 * ma5_slope_score
            )
            score = 100.0 * total01

            results.append({
                "ticker": t,
                "date": last["date"].date(),
                "entry": entry,
                "stop": stop,
                "target": target,
                "risk": risk,
                "reward": reward,
                "rr": float(rr),
                
                "ma5": float(last["ma5"]),
                "ma5_slope_3d": float(ma5_slope_3d),
                "ma5_slope_score": float(ma5_slope_score),

                # raw info (optional)
                "ma20": float(last["ma20"]),
                "ma60": float(last["ma60"]),
                "ma20_slope_5d": float(ma20_slope_5d),
                "ret20": float(ret20),
                "bb_width": float(bb_width),
                "vol_ratio_5v20": float(vol_ratio_5v20),

                # component scores
                "rr_pref": float(rr_pref),
                "trend_score": float(trend_score),
                "vol_score": float(vol_score),
                "vol_score2": float(vol_score2),
                "rs_score": float(rs_score),

                "score": float(score),
            })

        if not results:
            return pd.DataFrame(columns=[
                "ticker","date","entry","stop","target","risk","reward","rr",
                "ma5","ma5_slope_3d","ma5_slope_score",
                "ma20","ma60","ma20_slope_5d","ret20","bb_width","vol_ratio_5v20",
                "rr_pref","trend_score","vol_score","vol_score2","rs_score","score",
            ])

        out = pd.DataFrame(results)

        # Relative strength percentile among scan candidates
        out["rs_score"] = out["ret20"].rank(pct=True).fillna(0.0)

        # Recompute final score including rs_score
        total01 = (
            0.35 * out["rr_pref"]
            + 0.20 * out["trend_score"]
            + 0.15 * out["vol_score"]
            + 0.10 * out["vol_score2"]
            + 0.10 * out["rs_score"]
            + 0.10 * out["ma5_slope_score"]
        )
        out["score"] = (100.0 * total01).astype(float)

        return out.sort_values("score", ascending=False).reset_index(drop=True)