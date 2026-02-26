# core/strategies/pullback_rr.py
import pandas as pd
from .base import Strategy, ScanParams
import numpy as np

fail = {"len<120":0, "na":0, "uptrend":0, "momentum":0, "near_ma20":0, "vol":0, "risk_reward":0, "min_rr":0}

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
        if df is None or df.empty:
            return pd.DataFrame()

        # ---- 준비 ----
        g = df.sort_values(["ticker", "date"]).copy()

        # 최소 길이 필터(120)
        counts = g.groupby("ticker")["date"].size()
        ok_len = counts[counts >= 120].index
        g = g[g["ticker"].isin(ok_len)].copy()

        fail = {
            "len<120": int((counts < 120).sum()),
            "na": 0,
            "uptrend": 0,
            "momentum": 0,
            "near_ma20": 0,
            "vol": 0,
            "risk_reward": 0,
            "min_rr": 0,
            "ma5_slope": 0,
        }

        if g.empty:
            print("[PullbackRR fail stats]", fail)
            return pd.DataFrame(columns=[
                "ticker","date","entry","stop","target","risk","reward","rr",
                "ma5","ma5_slope_3d","ma5_slope_score",
                "ma20","ma60","ma20_slope_5d","ret20","bb_width","vol_ratio_5v20",
                "rr_pref","trend_score","vol_score","vol_score2","rs_score","score",
            ])

        close_g = g.groupby("ticker")["close"]
        vol_g   = g.groupby("ticker")["volume"]
        high_g  = g.groupby("ticker")["high"]
        low_g   = g.groupby("ticker")["low"]

        # ---- rolling 지표 (벡터화) ----
        g["ma5"] = close_g.transform(lambda s: s.rolling(5).mean())
        g["ma20"] = close_g.transform(lambda s: s.rolling(20).mean())
        g["ma60"] = close_g.transform(lambda s: s.rolling(60).mean())
        g["vol_ma20"] = vol_g.transform(lambda s: s.rolling(20).mean())
        g["std20"] = close_g.transform(lambda s: s.pct_change().rolling(20).std())
        g["ret20"] = close_g.transform(lambda s: s.pct_change(20))

        g["high20"] = high_g.transform(lambda s: s.rolling(20).max())
        g["high60"] = high_g.transform(lambda s: s.rolling(60).max())
        g["vol_5"] = vol_g.transform(lambda s: s.rolling(5).mean())

        g["recent_low"] = low_g.transform(lambda s: s.rolling(params.stop_lookback).min())
        g["target"] = high_g.transform(lambda s: s.rolling(params.target_lookback).max())

        # ma20 5일 전, ma5 3일 전 (원래 코드의 -6, -4 기준과 동일하게 맞춤)
        g["ma20_5ago"] = close_g.transform(lambda s: s.rolling(20).mean().shift(5))
        g["ma5_3ago"] = close_g.transform(lambda s: s.rolling(5).mean().shift(3))

        # ---- ticker별 마지막 row ----
        last = g.groupby("ticker", as_index=False).tail(1).copy()

        # ---- NA 단계 ----
        need_cols = ["ma20", "ma60", "vol_ma20", "ma5", "ma20_5ago", "ma5_3ago", "std20", "ret20", "high20", "high60", "recent_low", "target", "vol_5"]
        na_mask = last[need_cols].isna().any(axis=1)
        fail["na"] = int(na_mask.sum())
        last = last[~na_mask].copy()
        if last.empty:
            print("[PullbackRR fail stats]", fail)
            return pd.DataFrame(columns=[
                "ticker","date","entry","stop","target","risk","reward","rr",
                "ma5","ma5_slope_3d","ma5_slope_score",
                "ma20","ma60","ma20_slope_5d","ret20","bb_width","vol_ratio_5v20",
                "rr_pref","trend_score","vol_score","vol_score2","rs_score","score",
            ])

        # ---- 조건 필터 ----
        uptrend = last["ma20"] > last["ma60"]
        fail["uptrend"] = int((~uptrend).sum())
        last = last[uptrend].copy()
        if last.empty:
            print("[PullbackRR fail stats]", fail)
            return pd.DataFrame(columns=[
                "ticker","date","entry","stop","target","risk","reward","rr",
                "ma5","ma5_slope_3d","ma5_slope_score",
                "ma20","ma60","ma20_slope_5d","ret20","bb_width","vol_ratio_5v20",
                "rr_pref","trend_score","vol_score","vol_score2","rs_score","score",
            ])

        had_momentum = last["high20"] >= last["high60"] * 0.95
        fail["momentum"] = int((~had_momentum).sum())
        last = last[had_momentum].copy()
        if last.empty:
            print("[PullbackRR fail stats]", fail)
            return pd.DataFrame(columns=[
                "ticker","date","entry","stop","target","risk","reward","rr",
                "ma5","ma5_slope_3d","ma5_slope_score",
                "ma20","ma60","ma20_slope_5d","ret20","bb_width","vol_ratio_5v20",
                "rr_pref","trend_score","vol_score","vol_score2","rs_score","score",
            ])

        near_ma20 = (last["close"] - last["ma20"]).abs() / last["ma20"] <= params.tolerance
        fail["near_ma20"] = int((~near_ma20).sum())
        last = last[near_ma20].copy()
        if last.empty:
            print("[PullbackRR fail stats]", fail)
            return pd.DataFrame(columns=[
                "ticker","date","entry","stop","target","risk","reward","rr",
                "ma5","ma5_slope_3d","ma5_slope_score",
                "ma20","ma60","ma20_slope_5d","ret20","bb_width","vol_ratio_5v20",
                "rr_pref","trend_score","vol_score","vol_score2","rs_score","score",
            ])

        vol_ok = (last["vol_ma20"] > 0) & (last["vol_5"] <= last["vol_ma20"] * 1.5)
        fail["vol"] = int((~vol_ok).sum())
        last = last[vol_ok].copy()
        if last.empty:
            print("[PullbackRR fail stats]", fail)
            return pd.DataFrame(columns=[
                "ticker","date","entry","stop","target","risk","reward","rr",
                "ma5","ma5_slope_3d","ma5_slope_score",
                "ma20","ma60","ma20_slope_5d","ret20","bb_width","vol_ratio_5v20",
                "rr_pref","trend_score","vol_score","vol_score2","rs_score","score",
            ])

        # ---- entry/stop/target/risk/reward/rr ----
        last["entry"] = last["close"].astype(float)
        last["stop"] = (last["recent_low"] * (1.0 - params.stop_buffer)).astype(float)
        last["risk"] = (last["entry"] - last["stop"]).astype(float)
        last["reward"] = (last["target"] - last["entry"]).astype(float)

        rr_ok = (last["risk"] > 0) & (last["reward"] > 0)
        fail["risk_reward"] = int((~rr_ok).sum())
        last = last[rr_ok].copy()
        if last.empty:
            print("[PullbackRR fail stats]", fail)
            return pd.DataFrame(columns=[
                "ticker","date","entry","stop","target","risk","reward","rr",
                "ma5","ma5_slope_3d","ma5_slope_score",
                "ma20","ma60","ma20_slope_5d","ret20","bb_width","vol_ratio_5v20",
                "rr_pref","trend_score","vol_score","vol_score2","rs_score","score",
            ])

        last["rr"] = (last["reward"] / last["risk"]).astype(float)
        min_rr_ok = last["rr"] >= params.min_rr
        fail["min_rr"] = int((~min_rr_ok).sum())
        last = last[min_rr_ok].copy()
        if last.empty:
            print("[PullbackRR fail stats]", fail)
            return pd.DataFrame(columns=[
                "ticker","date","entry","stop","target","risk","reward","rr",
                "ma5","ma5_slope_3d","ma5_slope_score",
                "ma20","ma60","ma20_slope_5d","ret20","bb_width","vol_ratio_5v20",
                "rr_pref","trend_score","vol_score","vol_score2","rs_score","score",
            ])

        # ---- MA5 slope ----
        last["ma5_slope_3d"] = (last["ma5"] / last["ma5_3ago"] - 1.0).fillna(0.0)
        last.loc[~np.isfinite(last["ma5_slope_3d"]), "ma5_slope_3d"] = 0.0
        last["ma5_slope_score"] = (last["ma5_slope_3d"] / 0.01).clip(lower=0.0, upper=1.0)

        if params.require_ma5_positive:
            ok_ma5 = last["ma5_slope_3d"] > params.ma5_min_slope
            fail["ma5_slope"] = int((~ok_ma5).sum())
            last = last[ok_ma5].copy()
            if last.empty:
                print("[PullbackRR fail stats]", fail)
                return pd.DataFrame(columns=[
                    "ticker","date","entry","stop","target","risk","reward","rr",
                    "ma5","ma5_slope_3d","ma5_slope_score",
                    "ma20","ma60","ma20_slope_5d","ret20","bb_width","vol_ratio_5v20",
                    "rr_pref","trend_score","vol_score","vol_score2","rs_score","score",
                ])

        # ---- scoring (동일 로직) ----
        def _clamp01_series(s: pd.Series) -> pd.Series:
            return s.clip(lower=0.0, upper=1.0)

        # rr_pref (peak around center)
        center = 2.15
        half_width = 1.35
        last["rr_pref"] = _clamp01_series(1.0 - (last["rr"] - center).abs() / half_width)

        # trend_score
        last["ma20_slope_5d"] = (last["ma20"] / last["ma20_5ago"] - 1.0).fillna(0.0)
        last.loc[~np.isfinite(last["ma20_slope_5d"]), "ma20_slope_5d"] = 0.0
        trend_slope = _clamp01_series(last["ma20_slope_5d"] / 0.02)

        last["ret20"] = last["ret20"].astype(float).fillna(0.0)
        trend_ret = _clamp01_series(last["ret20"] / 0.10)

        last["trend_score"] = 0.6 * trend_slope + 0.4 * trend_ret

        # vol_score: bb_width=4*std20
        last["std20"] = last["std20"].astype(float).fillna(0.0)
        last["bb_width"] = 4.0 * last["std20"]
        last["vol_score"] = _clamp01_series(1.0 - (last["bb_width"] / 0.20))

        # vol_score2
        last["vol_ratio_5v20"] = (last["vol_5"] / last["vol_ma20"]).replace([float("inf"), float("-inf")], 1.0).fillna(1.0)
        last["vol_score2"] = _clamp01_series(1.0 - (last["vol_ratio_5v20"] - 0.75).abs() / 0.75)

        # rs_score 후처리
        last["rs_score"] = 0.0

        total01 = (
            0.35 * last["rr_pref"]
            + 0.20 * last["trend_score"]
            + 0.15 * last["vol_score"]
            + 0.10 * last["vol_score2"]
            + 0.10 * last["rs_score"]
            + 0.10 * last["ma5_slope_score"]
        )
        last["score"] = (100.0 * total01).astype(float)

        out_cols = [
            "ticker","date","entry","stop","target","risk","reward","rr",
            "ma5","ma5_slope_3d","ma5_slope_score",
            "ma20","ma60","ma20_slope_5d","ret20","bb_width","vol_ratio_5v20",
            "rr_pref","trend_score","vol_score","vol_score2","rs_score","score",
        ]
        out = last[out_cols].copy()

        # RS percentile among candidates
        out["rs_score"] = out["ret20"].rank(pct=True).fillna(0.0)

        # final score recompute (원본과 동일)
        total01 = (
            0.35 * out["rr_pref"]
            + 0.20 * out["trend_score"]
            + 0.15 * out["vol_score"]
            + 0.10 * out["vol_score2"]
            + 0.10 * out["rs_score"]
            + 0.10 * out["ma5_slope_score"]
        )
        out["score"] = (100.0 * total01).astype(float)

        print("[PullbackRR fail stats]", fail)
        return out.sort_values("score", ascending=False).reset_index(drop=True)
    