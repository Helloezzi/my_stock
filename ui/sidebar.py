import os
import time
import streamlit as st

from core.strategies.base import ScanParams


def render_sidebar(*, strategy_labels: list[str]) -> dict:
    """
    Returns:
      dict with keys:
        tab, rebuild_clicked, run_scan, market_mode, params, selected_strategy_label
    """
    out = {
        "tab": "Data",
        "rebuild_clicked": False,
        "run_scan": False,
        "market_mode": "close_above_ma20",
        "params": ScanParams(),
        "selected_strategy_label": strategy_labels[0] if strategy_labels else "",
    }

    with st.sidebar:
        st.header("Menu")
        tab = st.radio("Select", ["Data", "Scanner"], index=0)
        out["tab"] = tab
        st.divider()

        if tab == "Data":
            st.subheader("Data")
            out["rebuild_clicked"] = st.button("Rebuild (Last 1Y, KOSPI Top200)", type="primary")

        elif tab == "Scanner":
            st.subheader("Scanner")
            st.caption("Strategy + Market filter + Parameters")

            out["selected_strategy_label"] = st.selectbox(
                "Strategy",
                options=strategy_labels,
                index=0,
            )

            out["market_mode"] = st.selectbox(
                "KOSPI filter",
                ["close_above_ma20", "ma20_above_ma60", "both"],
                index=0
            )

            tolerance = st.slider("MA20 tolerance (%)", 1, 10, 3) / 100
            stop_lookback = st.slider("Stop lookback (days)", 5, 30, 10)
            stop_buffer = st.slider("Stop buffer (%)", 0.0, 3.0, 0.5, 0.1) / 100
            target_lookback = st.slider("Target lookback (days)", 10, 90, 20)
            min_rr = st.slider("Min R/R", 0.5, 5.0, 1.5, 0.1)

            out["params"] = ScanParams(
                tolerance=tolerance,
                stop_lookback=stop_lookback,
                stop_buffer=stop_buffer,
                target_lookback=target_lookback,
                min_rr=min_rr,
            )

            if st.button("Run Scan", type="primary"):
                out["run_scan"] = True

    return out