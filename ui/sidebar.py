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


def render_sidebar(strategy_labels, published_picks_available: bool = False):
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

    st.sidebar.subheader("View")
    out["lightweight_mode"] = st.sidebar.checkbox(
        "Lightweight mobile mode",
        value=True,
        help="Use smaller tables and avoid heavy chart rendering by default.",
        key="lightweight_mode",
    )
    out["show_chart"] = st.sidebar.checkbox(
        "Show chart",
        value=not out["lightweight_mode"],
        help="Disable this on mobile for faster response.",
        key="show_chart",
    )

    st.sidebar.divider()

    if tab == "Scanner":
        st.sidebar.subheader("Scanner")

        scanner_source_options = ["Full scanner"]
        if published_picks_available:
            scanner_source_options = ["Published daily picks", "Full scanner"]

        default_source = "Published daily picks" if (published_picks_available and out["lightweight_mode"]) else "Full scanner"
        out["scanner_source"] = st.sidebar.radio(
            "Scanner source",
            scanner_source_options,
            index=scanner_source_options.index(default_source),
            help="Use precomputed daily picks for faster mobile loading, or switch to the full scanner.",
            key="scanner_source",
        )

        if out["scanner_source"] == "Published daily picks":
            out["market"] = "KOSPI"
            out["top_n"] = None
            out["market_mode"] = "close_above_ma20"
            out["selected_strategy_label"] = strategy_labels[0] if strategy_labels else ""
            out["params"] = ScanParams()

            st.sidebar.caption(
                "Published daily picks mode hides heavy scanner controls and loads the precomputed daily result first."
            )
        else:
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

            require_ma5_up = st.sidebar.checkbox("Require MA5 rising", value=False, key="ma5_up_check")
            ma5_up_days = 0
            if require_ma5_up:
                ma5_up_days = st.sidebar.slider(
                    "MA5 rising days",
                    min_value=1,
                    max_value=5,
                    value=3,
                    step=1,
                    key="ma5_up_days",
                )

            out["params"] = ScanParams(
                tolerance=tolerance,
                stop_lookback=stop_lookback,
                stop_buffer=stop_buffer,
                target_lookback=target_lookback,
                min_rr=min_rr,
                ma5_up_days=ma5_up_days,
            )

    elif tab == "Browse":
        st.sidebar.subheader("Browse")

        market, top_n = _market_and_topn_controls(prefix="browse_")
        out["market"] = market
        out["top_n"] = top_n

    return out
