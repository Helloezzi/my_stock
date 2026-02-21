# core/strategies/pullback_rr.py
import pandas as pd
from .base import Strategy, ScanParams

class PullbackRRStrategy(Strategy):
    key = "pullback_rr"
    name = "Pullback + Risk/Reward"

    def scan(self, df: pd.DataFrame, params: ScanParams) -> pd.DataFrame:
        results = []

        for t, g in df.groupby("ticker"):
            g = g.sort_values("date").copy()
            if len(g) < 120:
                continue

            g["ma20"] = g["close"].rolling(20).mean()
            g["ma60"] = g["close"].rolling(60).mean()
            g["vol_ma20"] = g["volume"].rolling(20).mean()

            last = g.iloc[-1]
            if pd.isna(last["ma20"]) or pd.isna(last["ma60"]) or pd.isna(last["vol_ma20"]):
                continue

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

            score = rr + (1.0 - abs(entry - float(last["ma20"])) / float(last["ma20"]))

            results.append({
                "ticker": t,
                "date": last["date"].date(),
                "entry": entry,
                "stop": stop,
                "target": target,
                "risk": risk,
                "reward": reward,
                "rr": rr,
                "ma20": float(last["ma20"]),
                "ma60": float(last["ma60"]),
                "vol_ratio_5v20": float(vol_5 / float(last["vol_ma20"])),
                "score": float(score),
            })

        if not results:
            return pd.DataFrame(columns=[
                "ticker","date","entry","stop","target","risk","reward","rr",
                "ma20","ma60","vol_ratio_5v20","score"
            ])

        return pd.DataFrame(results).sort_values("score", ascending=False).reset_index(drop=True)