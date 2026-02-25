# ui/sidebar.py
import streamlit as st
from core.strategies.base import ScanParams

TAB_KEY = "active_tab"

def _market_and_topn_controls(prefix: str = ""):
    market = st.sidebar.selectbox(
        "Market",
        options=["KOSPI", "KOSDAQ"],
        index=0,
        key=f"{prefix}market_select",
    )

    use_all = st.sidebar.checkbox(
        "Use all tickers (no Top-N limit)",
        value=True,
        key=f"{prefix}use_all_tickers",
    )

    top_n = None
    if not use_all:
        top_n = int(
            st.sidebar.number_input(
                "Top N (by market cap on latest date)",
                min_value=10,
                max_value=3000,
                value=200,
                step=10,
                key=f"{prefix}top_n_input",
            )
        )
    return market, top_n


def render_sidebar(strategy_labels):
    out = {}

    st.sidebar.title("Menu")

    if TAB_KEY not in st.session_state:
        st.session_state[TAB_KEY] = "Scanner"

    tabs = ["Scanner", "Browse"]

    tab = st.sidebar.radio(
        "Select",
        tabs,
        key=TAB_KEY,
        index=tabs.index(st.session_state[TAB_KEY]),
    )
    out["tab"] = tab

    st.sidebar.divider()

    if tab == "Scanner":
        st.sidebar.subheader("Scanner")

        market, top_n = _market_and_topn_controls(prefix="scan_")
        out["market"] = market
        out["top_n"] = top_n

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

        tolerance = st.sidebar.slider("MA20 tolerance (%)", 1, 10, 3) / 100
        stop_lookback = st.sidebar.slider("Stop lookback (days)", 5, 30, 10)
        stop_buffer = st.sidebar.slider("Stop buffer (%)", 0.0, 3.0, 0.5, 0.1) / 100
        target_lookback = st.sidebar.slider("Target lookback (days)", 10, 90, 20)
        min_rr = st.sidebar.slider("Min R/R", 0.5, 5.0, 1.5, 0.1)

        require_ma5_positive = st.sidebar.checkbox("Require MA5 slope positive", value=False)
        ma5_min_slope = st.sidebar.number_input("MA5 min slope (3d)", value=0.0, step=0.001, format="%.3f")

        out["params"] = ScanParams(
            tolerance=tolerance,
            stop_lookback=stop_lookback,
            stop_buffer=stop_buffer,
            target_lookback=target_lookback,
            min_rr=min_rr,
            require_ma5_positive=require_ma5_positive,
            ma5_min_slope=ma5_min_slope,
        )

    elif tab == "Browse":
        st.sidebar.subheader("Browse")

        market, top_n = _market_and_topn_controls(prefix="browse_")
        out["market"] = market
        out["top_n"] = top_n

    return out