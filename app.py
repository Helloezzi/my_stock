# app.py
from datetime import datetime, time as dtime

import pandas as pd
import streamlit as st

from core.app_runtime import ensure_session_defaults, load_buffers
from core.auto_daily import try_run_daily_once_async
from core.config import APP_TITLE
from core.data_loader import daily_fingerprint
from core.published_picks import load_published_picks
from core.strategies import get_strategies
from core.ticker_names import get_ticker_name_map_local
from core.universe import build_universe
from ui.scanner_tab import render_scanner_tab
from ui.selection_view import render_browse_tab, render_selected_panel, resolve_selected_ticker
from ui.sidebar import render_sidebar
from ui.texts import t
from ui.today_picks_view import render_today_picks

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

if datetime.now().time() >= dtime(16, 20):
    try_run_daily_once_async()

if st.sidebar.button(t("app.refresh_data"), help=t("app.refresh_help")):
    st.cache_data.clear()
    st.rerun()

published = load_published_picks()

strategies = get_strategies()
strategy_by_label = {strategy.name: strategy for strategy in strategies}
strategy_labels = list(strategy_by_label.keys())

sb = render_sidebar(
    strategy_labels=strategy_labels,
    published_picks_available=bool(published),
)
tab = sb.get("tab", t("sidebar.tab.scanner"))

lightweight_today_picks = bool(
    tab == t("sidebar.tab.scanner")
    and published
    and sb.get("scanner_source") == t("sidebar.source.published")
)

if lightweight_today_picks:
    selected, scan_levels = render_today_picks(
        published,
        lightweight_mode=sb.get("lightweight_mode", False),
    )

    if selected:
        fp = daily_fingerprint()
        dfs, infos = load_buffers(fp)
        combined = pd.concat(
            [df for df in dfs.values() if df is not None and not df.empty],
            ignore_index=True,
        ) if dfs else pd.DataFrame()

        if combined.empty:
            st.warning(t("app.published_chart_missing"))
            st.stop()

        name_map = {
            str(item.get("ticker", "")).zfill(6): item.get("name", str(item.get("ticker", "")).zfill(6))
            for item in published.get("picks", [])
        }
        render_selected_panel(
            tab=t("sidebar.tab.scanner"),
            sb=sb,
            selected=selected,
            name_map=name_map,
            data_df=combined,
            scan_levels=scan_levels,
        )
    st.stop()

fp = daily_fingerprint()
dfs, infos = load_buffers(fp)

market = sb.get("market", "KOSPI")
top_n = sb.get("top_n", None)
df, uni = build_universe(dfs, market=market, top_n=top_n, rank_by="market_cap")

if df is None or df.empty:
    st.warning(t("app.no_data"))
    st.stop()

tickers = sorted(df["ticker"].astype(str).str.zfill(6).unique())
name_map = get_ticker_name_map_local(tickers)

if not tickers:
    st.warning(t("app.no_tickers"))
    st.stop()

ensure_session_defaults(tickers)

st.caption(
    t(
        "app.universe_caption",
        market=uni.market,
        latest_date=uni.latest_date,
        tickers=uni.tickers,
        rows=uni.rows,
        top_n=uni.top_n if uni.top_n else "ALL",
        rank_by=uni.rank_by,
    )
)

if tab == t("sidebar.tab.scanner"):
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
elif tab == t("sidebar.tab.browse"):
    render_browse_tab(tickers=tickers, name_map=name_map, market_label=uni.market)
else:
    st.info(t("app.data_tab_deprecated"))
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
