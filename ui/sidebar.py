# ui/sidebar.py
import streamlit as st
from core.strategies.base import ScanParams

TAB_KEY = "active_tab"

def render_sidebar(strategy_labels, csv_options, csv_default_index=0):
    out = {}

    st.sidebar.title("Menu")

    # ✅ 최초 1회만 Data 탭
    if TAB_KEY not in st.session_state:
        st.session_state[TAB_KEY] = "Data"

    tabs = ["Data", "Scanner", "Browse"]

    # ✅ sidebar에 렌더
    tab = st.sidebar.radio(
        "Select",
        tabs,
        key=TAB_KEY,
        index=tabs.index(st.session_state[TAB_KEY]) if st.session_state[TAB_KEY] in tabs else 0,
    )
    out["tab"] = tab

    st.sidebar.divider()

    if tab == "Data":
        st.sidebar.subheader("Data")
        st.sidebar.caption("Manage datasets in the main view →")

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