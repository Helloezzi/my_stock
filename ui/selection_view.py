from __future__ import annotations

import pandas as pd
import streamlit as st

from ui.position_view import render_risk_settings_and_summary, render_trade_levels
from ui.search_view import render_naver_link, render_search_and_select
from ui.texts import t
from ui.chart_renderer import render_chart


def _sync_show_chart_from_inline():
    value = bool(st.session_state.get("show_chart_inline_toggle", False))
    st.session_state["show_chart_enabled"] = value
    st.session_state["show_chart_sidebar"] = value


def render_browse_tab(*, tickers, name_map, market_label: str):
    render_search_and_select(
        tickers,
        name_map,
        state_key="selected_browse_ticker",
        title=t("search.title_browse", market_label=market_label),
    )


def resolve_selected_ticker(tab: str):
    if tab == "Scanner":
        return st.session_state.get("selected_scan_ticker"), st.session_state.get("scan_levels", None)
    if tab == "Browse":
        return st.session_state.get("selected_browse_ticker"), None
    return None, None


def render_selected_panel(*, tab: str, sb: dict, selected, name_map: dict, data_df: pd.DataFrame, scan_levels):
    if not selected:
        return

    selected = str(selected).zfill(6)
    selected_name = name_map.get(selected, selected)
    show_identity_header = not (
        tab == t("sidebar.tab.scanner") and sb.get("scanner_source") == t("sidebar.source.published")
    )
    if show_identity_header:
        st.subheader(f"{selected} - {selected_name}")
        render_naver_link(selected)

    sub = data_df[data_df["ticker"].astype(str).str.zfill(6) == selected].copy()
    sub["date"] = pd.to_datetime(sub["date"], errors="coerce")
    sub = sub.dropna(subset=["date"]).sort_values("date")

    if sub.empty:
        st.warning(t("selected.no_ohlcv"))
        st.stop()

    prefix = "ps_scan" if tab == t("sidebar.tab.scanner") else "ps_browse"

    chart_df = sub.tail(160).copy() if sb.get("lightweight_mode", False) else sub
    entry, stop, target = render_trade_levels(
        selected=selected,
        sub=chart_df,
        scan_levels=scan_levels,
        key_prefix=prefix,
    )

    st.divider()

    if sb.get("lightweight_mode", False):
        if "show_chart_inline_toggle" not in st.session_state:
            st.session_state["show_chart_inline_toggle"] = bool(sb.get("show_chart", False))
        st.checkbox(
            t("today.show_chart"),
            help=t("today.show_chart_help"),
            key="show_chart_inline_toggle",
            on_change=_sync_show_chart_from_inline,
        )

    if sb.get("show_chart", True):
        render_chart(chart_df, entry, stop, target, compact=sb.get("lightweight_mode", False))

    st.divider()
    render_risk_settings_and_summary(
        selected=selected,
        entry=entry,
        stop=stop,
        target=target,
        key_prefix=prefix,
        use_expander=False,
    )
