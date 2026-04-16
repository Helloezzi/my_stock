import pandas as pd

from .base import ScanParams, Strategy
from .vol_breakout_helpers import (
    build_result_frame,
    evaluate_candidate,
    passes_basic_filters,
    prepare_group_indicators,
)


class VolCompressionBreakoutStrategy(Strategy):
    key = "vol_compression_breakout"
    name = "Vol Compression -> Breakout (Watch + Confirm + VolSurge)"

    BB_LOOKBACK = 20
    BB_WINDOW_FOR_PERCENTILE = 120
    BB_WIDTH_Q = 0.20
    RANGE_LOOKBACK = 20
    RANGE_MAX = 0.10
    VOL_RATIO_MAX = 0.85
    BREAKOUT_LOOKBACK = 20

    MAX_STD60 = 0.035
    MAX_DAY_RANGE = 0.12
    MAX_GAP_UP = 0.06
    MIN_BB_OK_5 = 4
    MIN_CLOSE_MARGIN = 0.01
    VOL_SURGE_MIN = 1.8
    VOL_SURGE_VS_VOL5_MIN = 1.3

    MIN_MARKET_CAP = 3_000_0000_0000
    MIN_VALUE_MA20 = 30_0000_0000
    MIN_HISTORY = 140

    def scan(self, df: pd.DataFrame, params: ScanParams) -> pd.DataFrame:
        results: list[dict[str, object]] = []

        for ticker, g in df.groupby("ticker"):
            prepared = prepare_group_indicators(g, self)
            if not passes_basic_filters(prepared, self):
                continue

            row = evaluate_candidate(prepared, ticker, self, params)
            if row:
                results.append(row)

        return build_result_frame(results)
