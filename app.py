# app.py
import os
from datetime import datetime
import streamlit as st

from core.config import APP_TITLE, DATA_DIR, CSV_PREFIX
from core.data_loader import list_csv_files, load_data, get_ticker_name_map
from core.market_index import load_kospi_index_1y
from core.market_filter import kospi_market_ok
from core.strategies import get_strategies

from ui.sidebar import render_sidebar
from ui.scanner_view import render_scanner_results
from ui.chart_view import render_search_and_select

from ui.chart_view import (
    render_search_and_select,
    render_naver_link,
    render_position_sizing,
    render_chart,
    render_chart_and_sizing_two_column,
)

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

strategies = get_strategies()
strategy_by_label = {s.name: s for s in strategies}
strategy_labels = list(strategy_by_label.keys())

csv_files = list_csv_files()
csv_labels = [p.name for p in csv_files]

sb = render_sidebar(
    strategy_labels=strategy_labels,
    csv_options=csv_labels,
)

if not csv_files:
    st.warning("No CSV found.")
    st.stop()

selected_csv_path = csv_files[0]
df = load_data(str(selected_csv_path))

tickers = sorted(df["ticker"].unique())
name_map = get_ticker_name_map(tickers)

# 상태 분리
if "selected_scan_ticker" not in st.session_state:
    st.session_state["selected_scan_ticker"] = None

if "selected_browse_ticker" not in st.session_state:
    st.session_state["selected_browse_ticker"] = tickers[0]

# =========================
# SCANNER TAB
# =========================
if sb["tab"] == "Scanner":

    if sb.get("run_scan"):
        idx_df = load_kospi_index_1y()
        ok, msg = kospi_market_ok(idx_df, mode=sb["market_mode"])
        st.session_state["market_ok"] = ok
        st.session_state["market_msg"] = msg

        if ok:
            strategy = strategy_by_label[sb["selected_strategy_label"]]
            scan_df = strategy.scan(df, sb["params"])
            st.session_state["scan_df"] = scan_df
        else:
            st.session_state["scan_df"] = None

    scan_df = st.session_state.get("scan_df")

    if scan_df is None:
        st.info("Run Scan first.")
        st.stop()

    active = render_scanner_results(
        scan_df,
        name_map,
        state_key="selected_scan_ticker",
    )

# =========================
# BROWSE TAB
# =========================
elif sb["tab"] == "Browse":

    active = render_search_and_select(
        tickers,
        name_map,
        state_key="selected_browse_ticker",
        title="Browse KOSPI Top200",
    )

# =========================
# 공통 차트 영역
# =========================
if sb["tab"] == "Scanner":
    selected = st.session_state.get("selected_scan_ticker")
    scan_levels = st.session_state.get("scan_levels", None)
elif sb["tab"] == "Browse":
    selected = st.session_state.get("selected_browse_ticker")
    scan_levels = None
else:
    selected = None
    scan_levels = None

if selected:
    selected_name = name_map.get(selected, selected)
    st.subheader(f"{selected} - {selected_name}")
    render_naver_link(selected)

    sub = df[df["ticker"] == selected].sort_values("date").copy()

    prefix = "ps_scan" if sb["tab"] == "Scanner" else "ps_browse"

    render_chart_and_sizing_two_column(
        selected=selected,
        sub=sub,
        scan_levels=scan_levels,
        key_prefix=prefix,
    )