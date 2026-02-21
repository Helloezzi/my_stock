# ui/sidebar.py
import streamlit as st
from core.strategies.base import ScanParams


def render_sidebar(strategy_labels, csv_options, csv_default_index=0):
    out = {}

    st.sidebar.title("Menu")

    tab = st.sidebar.radio(
        "Select",
        ["Data", "Scanner", "Browse"],
        index=1,
    )
    out["tab"] = tab

    st.sidebar.divider()

    if tab == "Data":
        st.sidebar.subheader("Data")

        selected_csv = st.sidebar.selectbox(
            "CSV file",
            options=csv_options,
            index=csv_default_index if csv_options else 0,
        )
        out["selected_csv"] = selected_csv

        end_date = st.sidebar.date_input("End Date")
        lookback = st.sidebar.selectbox("Lookback", ["6mo", "1y", "2y"], index=1)

        rebuild_clicked = st.sidebar.button("Rebuild CSV")

        out["end_date"] = end_date
        out["lookback"] = lookback
        out["rebuild_clicked"] = rebuild_clicked

    elif tab == "Scanner":
        st.sidebar.subheader("Scanner")
        st.sidebar.caption("Strategy + Market filter + Parameters")

        out["selected_strategy_label"] = st.sidebar.selectbox(
            "Strategy",
            options=strategy_labels,
            index=0,
            key="strategy_select",
        )

        out["market_mode"] = st.sidebar.selectbox(
            "KOSPI filter",
            ["close_above_ma20", "ma20_above_ma60", "both"],
            index=0,
            key="market_mode_select",
        )

        tolerance = st.sidebar.slider("MA20 tolerance (%)", 1, 10, 3, key="tol") / 100
        stop_lookback = st.sidebar.slider("Stop lookback (days)", 5, 30, 10, key="slb")
        stop_buffer = st.sidebar.slider("Stop buffer (%)", 0.0, 3.0, 0.5, 0.1, key="sbuf") / 100
        target_lookback = st.sidebar.slider("Target lookback (days)", 10, 90, 20, key="tlb")
        min_rr = st.sidebar.slider("Min R/R", 0.5, 5.0, 1.5, 0.1, key="mrr")

        # ✅ dict 말고 ScanParams 객체로 반환
        out["params"] = ScanParams(
            tolerance=tolerance,
            stop_lookback=stop_lookback,
            stop_buffer=stop_buffer,
            target_lookback=target_lookback,
            min_rr=min_rr,
        )

        out["run_scan"] = st.sidebar.button("Run Scan", type="primary", key="run_scan_btn")

    elif tab == "Browse":
        st.sidebar.subheader("Browse Top200")

    return out