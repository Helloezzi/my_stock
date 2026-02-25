# core/strategies/vol_compression_breakout.py
import pandas as pd
from .base import Strategy, ScanParams

from pykrx import stock



def _clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else (1.0 if x > 1.0 else float(x))


def _safe_div(a: float, b: float, default: float = 0.0) -> float:
    if b == 0 or pd.isna(b) or pd.isna(a):
        return default
    return float(a) / float(b)


class VolCompressionBreakoutStrategy(Strategy):
    """
    Volatility Compression -> Breakout strategy

    Stages:
      - WATCH   : compression candidates (pre-breakout watchlist)
      - BREAKOUT: confirmed breakout with volume surge

    Notes:
      - Breakout confirmation: High > prev20_high AND Close > prev20_high
      - Volume surge: volume / vol_ma20_prev >= 1.5
    """

    key = "vol_compression_breakout"
    name = "Vol Compression -> Breakout (Watch + Confirm + VolSurge)"

    # --- Tunables (keep simple & stable) ---
    BB_LOOKBACK = 20
    BB_WINDOW_FOR_PERCENTILE = 120
    BB_WIDTH_Q = 0.20          # lower 20% = compression
    RANGE_LOOKBACK = 20
    RANGE_MAX = 0.10           # 20D box range <= 10%
    VOL_RATIO_MAX = 0.85       # 5D vol / 20D vol <= 0.85 (dry-up)
    BREAKOUT_LOOKBACK = 20
    # --- Tight filters (quality-first) ---
    MAX_STD60 = 0.035         # 60D daily-return std upper bound (3.5%)
    MAX_DAY_RANGE = 0.12      # (high-low)/close max (12%)
    MAX_GAP_UP = 0.06         # gap-up max (6%)
    MIN_BB_OK_5 = 4           # last 5 days, bb_width <= q20 count
    MIN_CLOSE_MARGIN = 0.01   # close must be >= prev20_high * (1 + 1%)
    VOL_SURGE_MIN = 1.8       # tighten from 1.5 -> 1.8
    VOL_SURGE_VS_VOL5_MIN = 1.3  # today vol / vol_5 mean
    # liquidity / size filters
    MIN_MARKET_CAP = 3_000_0000_0000   # 3,000억 (원)
    MIN_VALUE_MA20 = 30_0000_0000      # 30억 (원) = 20일 평균 거래대금
    MIN_HISTORY = 140

    def scan(self, df: pd.DataFrame, params: ScanParams) -> pd.DataFrame:
        results = []

        # -------------------------
        # Market cap map (once per scan)
        # -------------------------
        scan_date = pd.to_datetime(df["date"].max()).strftime("%Y%m%d")
        try:
            cap_df = stock.get_market_cap(scan_date, market="ALL")
            cap_map = cap_df["시가총액"].to_dict()  # key: ticker(str) -> market cap (KRW)
        except Exception:
            cap_map = {}

        for t, g in df.groupby("ticker"):

            g = g.sort_values("date").copy()
            if len(g) < self.MIN_HISTORY:
                continue

            # -------------------------
            # Filters: market cap / trading value
            # -------------------------
            mcap = float(cap_map.get(t, 0))
            if mcap < self.MIN_MARKET_CAP:
                continue            

            # 20D average trading value (KRW)
            g["value"] = g["close"] * g["volume"]
            value_ma20 = float(g["value"].rolling(20).mean().iloc[-1]) if len(g) >= 20 else 0.0
            if value_ma20 < self.MIN_VALUE_MA20:
                continue

            # -------------------------
            # Indicators
            # -------------------------
            g["ma20"] = g["close"].rolling(20).mean()
            g["ma60"] = g["close"].rolling(60).mean()

            # BB width proxy using return std (consistent with your PullbackRR)
            g["std20"] = g["close"].pct_change().rolling(self.BB_LOOKBACK).std()
            g["bb_width"] = 4.0 * g["std20"]  # ~ (upper-lower)/close proxy

            g["vol_ma20"] = g["volume"].rolling(20).mean()
            g["vol_ma20_prev"] = g["vol_ma20"].shift(1)

            # 20D box range (exclude today for breakout/reference)
            den = g["close"].replace(0, pd.NA)
            g["range20"] = (g["high"].rolling(self.RANGE_LOOKBACK).max()
                - g["low"].rolling(self.RANGE_LOOKBACK).min()) / den

            # breakout reference: prior 20D high (exclude today)
            g["prev20_high"] = g["high"].rolling(self.BREAKOUT_LOOKBACK).max().shift(1)

            # trend / rs helper
            g["ret20"] = g["close"].pct_change(20)

            # --- extra helpers for tight filters ---
            g["ret1"] = g["close"].pct_change()
            
            #g["value_ma20"] = g["value"].rolling(20).mean()
            g["vol_5_mean"] = g["volume"].rolling(5).mean()

            last = g.iloc[-1]
            if pd.isna(last["ma20"]) or pd.isna(last["ma60"]) or pd.isna(last["bb_width"]) or pd.isna(last["prev20_high"]):
                continue

            # -------------------------
            # Tight filter #1: Volatility upper bound (60D)
            # -------------------------
            std60 = float(g["ret1"].tail(60).std())
            if std60 > self.MAX_STD60:
                continue

            # -------------------------
            # Tight filter #2: Avoid huge day range / gap-up
            # -------------------------
            prev_close = float(g["close"].iloc[-2]) if len(g) >= 2 else float("nan")

            day_range = _safe_div(float(last["high"]) - float(last["low"]), float(last["close"]), default=0.0)
            if day_range > self.MAX_DAY_RANGE:
                continue

            gap_up = _safe_div(float(last["open"]) - prev_close, prev_close, default=0.0)
            if gap_up > self.MAX_GAP_UP:
                continue

            # -------------------------
            # Compression conditions
            # -------------------------
            # BB width percentile (last vs last 120)
            bb_window = g["bb_width"].tail(self.BB_WINDOW_FOR_PERCENTILE).dropna()
            if len(bb_window) < int(self.BB_WINDOW_FOR_PERCENTILE * 0.7):
                continue

            bb_q = float(bb_window.quantile(self.BB_WIDTH_Q))
            bb_width = float(last["bb_width"])
            bb_is_compressed = (bb_width <= bb_q) if bb_q > 0 else False

            # -------------------------
            # Tight filter #3: Compression persistence
            # -------------------------
            bb_ok_5 = int((g["bb_width"].tail(5) <= bb_q).sum())
            if bb_ok_5 < self.MIN_BB_OK_5:
                continue

            # MA convergence (reuse params.tolerance as "ma_gap_max")
            ma_gap = abs(float(last["ma20"]) - float(last["ma60"])) / float(last["close"]) if float(last["close"]) != 0 else 1.0
            ma_converged = ma_gap <= float(params.tolerance)

            range20 = float(last["range20"]) if not pd.isna(last["range20"]) else 1.0
            in_box = range20 <= self.RANGE_MAX

            vol_5 = float(g["vol_5_mean"].iloc[-1]) if not pd.isna(g["vol_5_mean"].iloc[-1]) else 0.0
            vol_ma20 = float(last["vol_ma20"]) if not pd.isna(last["vol_ma20"]) else 0.0
            vol_ratio_5v20 = _safe_div(vol_5, vol_ma20, default=999.0)
            vol_dry = vol_ratio_5v20 <= self.VOL_RATIO_MAX

            compression_ok = bool(bb_is_compressed and ma_converged and in_box and vol_dry)

            if not compression_ok:
                continue

            # -------------------------
            # Breakout conditions (strengthened)
            # -------------------------
            prev20_high = float(last["prev20_high"])
            high_break = float(last["high"]) > prev20_high
            close_hold = float(last["close"]) > prev20_high
            breakout_confirmed = bool(high_break and close_hold)

            if breakout_confirmed:
                # -------------------------
                # Tight filter #4: Close margin above breakout level
                # -------------------------
                close_margin = (float(last["close"]) / prev20_high - 1.0) if prev20_high > 0 else 0.0
                if close_margin < self.MIN_CLOSE_MARGIN:
                    # If you want WATCH list to include pre-breakout, only enforce this on BREAKOUT.
                    # For quality-first, enforce always.
                    continue

            # Volume surge on breakout day
            vol_surge_ratio = _safe_div(float(last["volume"]), float(last["vol_ma20_prev"]), default=0.0)
            vol_surge_ok = bool(vol_surge_ratio >= self.VOL_SURGE_MIN)

            if breakout_confirmed:
                # -------------------------
                # Tight filter #5: Volume surge quality (today vs vol_5_mean)
                # -------------------------
                vol_5_mean = float(g["vol_5_mean"].iloc[-1]) if not pd.isna(g["vol_5_mean"].iloc[-1]) else 0.0
                vol_vs_5 = _safe_div(float(last["volume"]), vol_5_mean, default=0.0)

            if breakout_confirmed:
                if vol_surge_ratio < self.VOL_SURGE_MIN:
                    continue
                if vol_vs_5 < self.VOL_SURGE_VS_VOL5_MIN:
                    continue

            # Stage decision:
            # - WATCH  : compression ok but not (confirmed breakout + vol surge)
            # - BREAKOUT: confirmed breakout AND vol surge
            if breakout_confirmed and vol_surge_ok:
                stage = "BREAKOUT"
            else:
                stage = "WATCH"

            # -------------------------
            # Levels (Entry/Stop/Target) only meaningful for BREAKOUT
            # For WATCH we still compute to preview, but you may ignore in UI.
            # -------------------------
            entry = float(last["close"])
            recent_low = float(g["low"].tail(params.stop_lookback).min())
            stop = recent_low * (1.0 - float(params.stop_buffer))

            risk = entry - stop
            if risk <= 0:
                continue

            # target: max(recent high, entry + 2R)
            target_a = float(g["high"].tail(params.target_lookback).max())
            target_b = entry + 2.0 * risk
            target = max(target_a, target_b)

            reward = target - entry
            if reward <= 0:
                continue

            rr = reward / risk
            # min_rr는 BREAKOUT만 적용(Watchlist는 후보라서 유연하게)
            if stage == "BREAKOUT" and rr < float(params.min_rr):
                continue

            # -------------------------
            # Scoring (0..100)
            # - WATCH: compression quality 중심
            # - BREAKOUT: compression + breakout confirmation + volume surge 가중
            # -------------------------
            # compression strength sub-scores
            bb_score = 0.0
            if bb_q > 0:
                # smaller than q20 => closer to 1.0
                bb_score = _clamp01(1.0 - (bb_width / bb_q - 1.0))  # 1 at <=q, decays above
                bb_score = _clamp01(bb_score)

            range_score = _clamp01(1.0 - (range20 / self.RANGE_MAX))
            ma_score = _clamp01(1.0 - (ma_gap / max(float(params.tolerance), 1e-9)))
            vol_dry_score = _clamp01(1.0 - (vol_ratio_5v20 / self.VOL_RATIO_MAX))

            compression_score = 0.35 * bb_score + 0.25 * range_score + 0.20 * ma_score + 0.20 * vol_dry_score

            # trend quality (light)
            trend_up = 1.0 if float(last["ma20"]) > float(last["ma60"]) else 0.0
            ret20 = float(last["ret20"]) if not pd.isna(last["ret20"]) else 0.0
            trend_score = 0.6 * trend_up + 0.4 * _clamp01(ret20 / 0.10)  # 10%/20d -> 1

            # breakout & volume scores
            breakout_score = 1.0 if breakout_confirmed else 0.0
            vol_surge_score = _clamp01((vol_surge_ratio - self.VOL_SURGE_MIN) / 1.0) if vol_surge_ratio >= self.VOL_SURGE_MIN else 0.0
            # if exactly at threshold -> 0, if 2.5x -> ~1

            if stage == "WATCH":
                total01 = 0.70 * compression_score + 0.30 * trend_score
            else:
                total01 = (
                    0.45 * compression_score
                    + 0.15 * trend_score
                    + 0.20 * breakout_score
                    + 0.20 * _clamp01(0.5 + 0.5 * vol_surge_score)  # surge threshold already met
                )

            score = 100.0 * _clamp01(total01)

            results.append({
                "ticker": t,
                "date": pd.to_datetime(last["date"]),
                "stage": stage,  # WATCH or BREAKOUT

                "entry": entry,
                "stop": stop,
                "target": target,
                "risk": float(risk),
                "reward": float(reward),
                "rr": float(rr),

                # compression raw
                "bb_width": float(bb_width),
                "bb_q20": float(bb_q),
                "range20": float(range20),
                "ma_gap": float(ma_gap),
                "vol_ratio_5v20": float(vol_ratio_5v20),

                # breakout raw
                "prev20_high": float(prev20_high),
                "high_break": bool(high_break),
                "close_hold": bool(close_hold),
                "vol_surge_ratio": float(vol_surge_ratio),

                # trend raw
                "ma20": float(last["ma20"]),
                "ma60": float(last["ma60"]),
                "ret20": float(ret20),

                # component scores
                "compression_score": float(compression_score),
                "trend_score": float(trend_score),

                "score": float(score),
            })

        if not results:
            return pd.DataFrame(columns=[
                "ticker","date","stage",
                "entry","stop","target","risk","reward","rr",
                "bb_width","bb_q20","range20","ma_gap","vol_ratio_5v20",
                "prev20_high","high_break","close_hold","vol_surge_ratio",
                "ma20","ma60","ret20",
                "compression_score","trend_score","score",
            ])

        out = pd.DataFrame(results)

        # Optional: prioritize BREAKOUT over WATCH, then score
        stage_rank = {"BREAKOUT": 0, "WATCH": 1}
        out["stage_rank"] = out["stage"].map(stage_rank).fillna(9).astype(int)

        out = out.sort_values(["stage_rank", "score"], ascending=[True, False]).drop(columns=["stage_rank"]).reset_index(drop=True)
        return out