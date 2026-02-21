import pandas as pd

def kospi_market_ok(idx_df: pd.DataFrame, mode: str) -> tuple[bool, str]:
    if idx_df is None or idx_df.empty:
        return True, "Index data not available (passed)."

    last = idx_df.iloc[-1]
    if pd.isna(last["ma20"]):
        return True, "Index MA20 not ready (passed)."

    c1 = (last["close"] > last["ma20"])
    c2 = (not pd.isna(last["ma60"])) and (last["ma20"] > last["ma60"])

    if mode == "close_above_ma20":
        return bool(c1), f"KOSPI close({last['close']:.2f}) > MA20({last['ma20']:.2f})"
    if mode == "ma20_above_ma60":
        return bool(c2), f"KOSPI MA20({last['ma20']:.2f}) > MA60({last['ma60']:.2f})"
    if mode == "both":
        return bool(c1 and c2), (
            f"KOSPI close({last['close']:.2f}) > MA20({last['ma20']:.2f}) and "
            f"MA20({last['ma20']:.2f}) > MA60({last['ma60']:.2f})"
        )
    return bool(c1), "Default: close_above_ma20"