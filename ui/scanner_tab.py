from __future__ import annotations

import streamlit as st

from core.app_runtime import run_scanner_if_needed
from core.market_filter import kospi_market_ok
from core.market_index import load_kospi_index_1y
from ui.scanner_view import render_scanner_results


def _load_market_filter_state(market_mode: str) -> tuple[bool, str]:
    idx_df = load_kospi_index_1y()
    return kospi_market_ok(idx_df, mode=market_mode)


def render_scanner_tab(
    *,
    sb: dict,
    uni,
    market: str,
    top_n,
    data_df,
    name_map: dict,
    strategy_labels: list[str],
    strategy_by_label: dict,
):
    strategy_label = sb.get("selected_strategy_label", strategy_labels[0] if strategy_labels else "")
    market_mode = sb.get("market_mode", "close_above_ma20")
    params = sb.get("params", None)

    if not strategy_label:
        st.warning("No strategy available.")
        st.stop()

    st.caption(f"Strategy: **{strategy_label}** | KOSPI Filter: **{market_mode}**")

    result = run_scanner_if_needed(
        latest_date=str(uni.latest_date),
        market=market,
        top_n=top_n,
        strategy_label=strategy_label,
        market_mode=market_mode,
        params=params,
        strategy_by_label=strategy_by_label,
        market_filter_loader=lambda: _load_market_filter_state(market_mode),
        data_df=data_df,
    )

    if result.market_msg:
        st.write(f"Market filter: **{result.market_msg}**")

    if result.scan_df is not None:
        if not result.market_ok:
            st.warning("KOSPI filter blocked scan.")
        else:
            max_rows = 20 if sb.get("lightweight_mode", False) else None
            pick = render_scanner_results(result.scan_df, name_map, max_rows=max_rows)
            if pick:
                st.session_state["selected_scan_ticker"] = pick
