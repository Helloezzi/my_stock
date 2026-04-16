# app.py
from datetime import datetime, time as dtime

import streamlit as st

from core.app_runtime import ensure_session_defaults, load_buffers
from core.auto_daily import try_run_daily_once_async
from core.config import APP_TITLE
from core.data_loader import daily_fingerprint
from core.strategies import get_strategies
from core.ticker_names import get_ticker_name_map_local
from core.universe import build_universe
from ui.scanner_tab import render_scanner_tab
from ui.selection_view import render_browse_tab, render_selected_panel, resolve_selected_ticker
from ui.sidebar import render_sidebar

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

if datetime.now().time() >= dtime(16, 20):
    try_run_daily_once_async()

if st.sidebar.button("Refresh data", help="Clear cache and reload parquet buffers"):
    st.cache_data.clear()
    st.rerun()

fp = daily_fingerprint()
dfs, infos = load_buffers(fp)

strategies = get_strategies()
strategy_by_label = {strategy.name: strategy for strategy in strategies}
strategy_labels = list(strategy_by_label.keys())

sb = render_sidebar(strategy_labels=strategy_labels)
tab = sb.get("tab", "Scanner")

market = sb.get("market", "KOSPI")
top_n = sb.get("top_n", None)
df, uni = build_universe(dfs, market=market, top_n=top_n, rank_by="market_cap")

if df is None or df.empty:
    st.warning("No data loaded. Check daily downloader output and parquet cache.")
    st.stop()

tickers = sorted(df["ticker"].astype(str).str.zfill(6).unique())
name_map = get_ticker_name_map_local(tickers)

if not tickers:
    st.warning("No tickers found in the selected universe.")
    st.stop()

ensure_session_defaults(tickers)

st.caption(
    f"Universe: **{uni.market}** | Latest: **{uni.latest_date}** | "
    f"Tickers: **{uni.tickers:,}** | Rows: **{uni.rows:,}** | "
    f"TopN: **{uni.top_n if uni.top_n else 'ALL'}** (by {uni.rank_by})"
)

if tab == "Scanner":
    render_scanner_tab(
        sb=sb,
        uni=uni,
        market=market,
        top_n=top_n,
        data_df=df,
        name_map=name_map,
        strategy_labels=strategy_labels,
        strategy_by_label=strategy_by_label,
    )
elif tab == "Browse":
    render_browse_tab(tickers=tickers, name_map=name_map, market_label=uni.market)
else:
    st.info("Data tab is deprecated in the new pipeline. Use daily downloader + parquet buffers.")
    st.stop()

selected, scan_levels = resolve_selected_ticker(tab)
render_selected_panel(
    tab=tab,
    sb=sb,
    selected=selected,
    name_map=name_map,
    data_df=df,
    scan_levels=scan_levels,
)
