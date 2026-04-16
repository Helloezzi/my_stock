from __future__ import annotations

import pandas as pd
import streamlit as st

from ui.chart_view import render_chart_and_sizing_two_column
from ui.search_view import render_naver_link, render_search_and_select


def render_browse_tab(*, tickers, name_map, market_label: str):
    render_search_and_select(
        tickers,
        name_map,
        state_key="selected_browse_ticker",
        title=f"Browse ({market_label})",
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
    st.subheader(f"{selected} - {selected_name}")
    render_naver_link(selected)

    sub = data_df[data_df["ticker"].astype(str).str.zfill(6) == selected].copy()
    sub["date"] = pd.to_datetime(sub["date"], errors="coerce")
    sub = sub.dropna(subset=["date"]).sort_values("date")

    if sub.empty:
        st.warning("No OHLCV rows for selected ticker (after date normalization).")
        st.stop()

    prefix = "ps_scan" if tab == "Scanner" else "ps_browse"

    if sb.get("show_chart", True):
        render_chart_and_sizing_two_column(
            selected=selected,
            sub=sub.tail(160).copy() if sb.get("lightweight_mode", False) else sub,
            scan_levels=scan_levels,
            key_prefix=prefix,
            compact=sb.get("lightweight_mode", False),
        )
    else:
        last = sub.iloc[-1]
        st.info("Chart rendering is disabled in lightweight mode. Turn on 'Show chart' in the sidebar when needed.")
        st.write(
            f"Latest close: **{float(last['close']):,.0f}** | "
            f"Latest date: **{pd.to_datetime(last['date']).date()}** | "
            f"Rows: **{len(sub):,}**"
        )
