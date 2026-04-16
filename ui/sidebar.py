# ui/sidebar.py
import streamlit as st
from core.strategies.base import ScanParams
from ui.texts import t

TAB_KEY = "active_tab"


def _sync_show_chart_from_sidebar():
    value = bool(st.session_state.get("show_chart_sidebar", False))
    st.session_state["show_chart_enabled"] = value
    st.session_state["show_chart_inline_toggle"] = value


def _market_and_topn_controls(prefix: str = ""):
    market = st.sidebar.selectbox(
        t("sidebar.market"),
        options=["KOSPI", "KOSDAQ"],
        index=0,
        key=f"{prefix}market_select",
    )

    use_all = st.sidebar.checkbox(
        t("sidebar.use_all_tickers"),
        value=True,
        key=f"{prefix}use_all_tickers",
    )

    top_n = None
    if not use_all:
        top_n = int(
            st.sidebar.number_input(
                t("sidebar.top_n"),
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

    st.sidebar.title(t("sidebar.menu"))

    legacy_tab_map = {
        "Scanner": t("sidebar.tab.scanner"),
        "Browse": t("sidebar.tab.browse"),
    }
    if TAB_KEY not in st.session_state:
        st.session_state[TAB_KEY] = t("sidebar.tab.scanner")
    else:
        st.session_state[TAB_KEY] = legacy_tab_map.get(st.session_state[TAB_KEY], st.session_state[TAB_KEY])

    tabs = [t("sidebar.tab.scanner"), t("sidebar.tab.browse")]

    tab = st.sidebar.radio(
        t("sidebar.select"),
        tabs,
        key=TAB_KEY,
        index=tabs.index(st.session_state[TAB_KEY]),
    )
    out["tab"] = tab

    st.sidebar.subheader(t("sidebar.view"))
    out["lightweight_mode"] = st.sidebar.checkbox(
        t("sidebar.lightweight_mode"),
        value=True,
        help=t("sidebar.lightweight_help"),
        key="lightweight_mode",
    )

    if "show_chart_enabled" not in st.session_state:
        st.session_state["show_chart_enabled"] = not out["lightweight_mode"]
    if "show_chart_sidebar" not in st.session_state:
        st.session_state["show_chart_sidebar"] = st.session_state["show_chart_enabled"]

    st.sidebar.checkbox(
        t("sidebar.show_chart"),
        help=t("sidebar.show_chart_help"),
        key="show_chart_sidebar",
        on_change=_sync_show_chart_from_sidebar,
    )
    out["show_chart"] = bool(st.session_state.get("show_chart_enabled", False))

    st.sidebar.divider()

    if tab == t("sidebar.tab.scanner"):
        st.sidebar.subheader(t("sidebar.scanner"))

        scanner_source_options = [t("sidebar.source.full")]
        if published_picks_available:
            scanner_source_options = [t("sidebar.source.published"), t("sidebar.source.full")]

        legacy_source_map = {
            "Full scanner": t("sidebar.source.full"),
            "Published daily picks": t("sidebar.source.published"),
        }
        if "scanner_source" in st.session_state:
            st.session_state["scanner_source"] = legacy_source_map.get(
                st.session_state["scanner_source"],
                st.session_state["scanner_source"],
            )

        default_source = t("sidebar.source.published") if (published_picks_available and out["lightweight_mode"]) else t("sidebar.source.full")
        out["scanner_source"] = st.sidebar.radio(
            t("sidebar.scanner_source"),
            scanner_source_options,
            index=scanner_source_options.index(default_source),
            help=t("sidebar.scanner_source_help"),
            key="scanner_source",
        )

        if out["scanner_source"] == t("sidebar.source.published"):
            out["market"] = "KOSPI"
            out["top_n"] = None
            out["market_mode"] = "close_above_ma20"
            out["selected_strategy_label"] = strategy_labels[0] if strategy_labels else ""
            out["params"] = ScanParams()

            st.sidebar.caption(
                t("sidebar.published_caption")
            )
        else:
            market, top_n = _market_and_topn_controls(prefix="scan_")
            out["market"] = market
            out["top_n"] = top_n

            out["selected_strategy_label"] = st.sidebar.selectbox(
                t("sidebar.strategy"),
                options=strategy_labels,
                index=0,
                key="strategy_select",
            )

            out["market_mode"] = st.sidebar.selectbox(
                t("sidebar.kospi_filter"),
                ["close_above_ma20", "ma20_above_ma60", "both"],
                index=0,
                key="market_mode_select",
            )

            tolerance = st.sidebar.slider(t("sidebar.ma20_tolerance"), 1, 10, 3) / 100
            stop_lookback = st.sidebar.slider(t("sidebar.stop_lookback"), 5, 30, 10)
            stop_buffer = st.sidebar.slider(t("sidebar.stop_buffer"), 0.0, 3.0, 0.5, 0.1) / 100
            target_lookback = st.sidebar.slider(t("sidebar.target_lookback"), 10, 90, 20)
            min_rr = st.sidebar.slider(t("sidebar.min_rr"), 0.5, 5.0, 1.5, 0.1)

            require_ma5_up = st.sidebar.checkbox(t("sidebar.require_ma5_up"), value=False, key="ma5_up_check")
            ma5_up_days = 0
            if require_ma5_up:
                ma5_up_days = st.sidebar.slider(
                    t("sidebar.ma5_up_days"),
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

    elif tab == t("sidebar.tab.browse"):
        st.sidebar.subheader(t("sidebar.browse"))

        market, top_n = _market_and_topn_controls(prefix="browse_")
        out["market"] = market
        out["top_n"] = top_n

    return out
