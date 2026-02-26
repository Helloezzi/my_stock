# app.py
import streamlit as st
import json
import hashlib
import pandas as pd
from datetime import datetime, time as dtime

from core.config import APP_TITLE
from core.data_loader import load_all_markets, daily_fingerprint
from core.universe import build_universe
from core.ticker_names import get_ticker_name_map

from core.market_index import load_kospi_index_1y
from core.market_filter import kospi_market_ok
from core.strategies import get_strategies

from ui.sidebar import render_sidebar
from ui.scanner_view import render_scanner_results
from ui.chart_view import (
    render_search_and_select,
    render_naver_link,
    render_chart_and_sizing_two_column,
)

from core.auto_daily import try_run_daily_once_async
from core.scan_cache import (
    scan_signature, load_cached_scan, save_cached_scan,
    load_cached_levels, save_cached_levels
)

# -----------------------------
# Page config MUST be first st-call
# -----------------------------
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

# 서버 뜰 때마다 시도하지만, 하루 1회만 실제 실행됨
now = datetime.now().time()
if now >= dtime(16, 20):
    try_run_daily_once_async()

# -----------------------------
# Data buffers (daily -> parquet cache)
# -----------------------------
@st.cache_data(show_spinner=True)
def load_buffers(_fingerprint: str):
    dfs, infos = load_all_markets()
    return dfs, infos


# Optional: manual refresh button
if st.sidebar.button("🔄 Refresh data", help="Clear cache and reload parquet buffers"):
    st.cache_data.clear()
    st.rerun()

fp = daily_fingerprint()
dfs, infos = load_buffers(fp)

# -----------------------------
# Strategies
# -----------------------------
strategies = get_strategies()
strategy_by_label = {s.name: s for s in strategies}
strategy_labels = list(strategy_by_label.keys())

# -----------------------------
# Sidebar
# -----------------------------
sb = render_sidebar(strategy_labels=strategy_labels)

tab = sb.get("tab", "Scanner")

# -----------------------------
# Pick market universe (KOSPI/KOSDAQ + TopN)
# -----------------------------
market = sb.get("market", "KOSPI")          # fallback if sidebar not updated yet
top_n = sb.get("top_n", None)              # None = 전체
df, uni = build_universe(dfs, market=market, top_n=top_n, rank_by="market_cap")

if df is None or df.empty:
    st.warning("No data loaded. Check daily downloader output and parquet cache.")
    st.stop()

tickers = sorted(df["ticker"].astype(str).str.zfill(6).unique())
name_map = get_ticker_name_map(tickers)

if not tickers:
    st.warning("No tickers found in the selected universe.")
    st.stop()

# -----------------------------
# Session state defaults
# -----------------------------
if "selected_scan_ticker" not in st.session_state:
    st.session_state["selected_scan_ticker"] = None
if "selected_browse_ticker" not in st.session_state:
    st.session_state["selected_browse_ticker"] = tickers[0]
if "scan_sig" not in st.session_state:
    st.session_state["scan_sig"] = None

# -----------------------------
# Top header info (lightweight)
# -----------------------------
st.caption(
    f"Universe: **{uni.market}** | Latest: **{uni.latest_date}** | "
    f"Tickers: **{uni.tickers:,}** | Rows: **{uni.rows:,}** | "
    f"TopN: **{uni.top_n if uni.top_n else 'ALL'}** (by {uni.rank_by})"
)


# -----------------------------
# Tabs behavior
# -----------------------------
if tab == "Scanner":
    strategy_label = sb.get("selected_strategy_label", strategy_labels[0] if strategy_labels else "")
    market_mode = sb.get("market_mode", "close_above_ma20")
    params = sb.get("params", None)

    if not strategy_label:
        st.warning("No strategy available.")
        st.stop()

    st.caption(f"Strategy: **{strategy_label}** | KOSPI Filter: **{market_mode}**")

    # 설정이 바뀐 경우에만 재스캔
    sig = scan_signature(
        latest_date=str(uni.latest_date),
        market=market,
        top_n=top_n,
        strategy_label=strategy_label,
        market_mode=market_mode,
        params=params,
    )
    last_sig = st.session_state.get("scan_sig")

    if sig != last_sig:
        st.session_state["scan_sig"] = sig
        st.session_state["selected_scan_ticker"] = None

        cur_set = set(tickers)
        sel = st.session_state.get("selected_scan_ticker")
        if sel and str(sel).zfill(6) not in cur_set:
            st.session_state["selected_scan_ticker"] = None

        # 시장 필터: 기존 로직 유지 (KOSPI index 기반)
        idx_df = load_kospi_index_1y()
        ok, msg = kospi_market_ok(idx_df, mode=market_mode)
        st.session_state["market_ok"] = ok
        st.session_state["market_msg"] = msg

        if ok:
            cached = load_cached_scan(sig)
            cached_levels = load_cached_levels(sig)

            if cached is not None and cached_levels is not None:
                scan_df = cached
                levels = cached_levels
            else:
                strategy = strategy_by_label[strategy_label]
                scan_df = cached if cached is not None else strategy.scan(df, params)

                # levels 생성
                if not scan_df.empty and "ticker" in scan_df.columns:
                    need = [c for c in ["entry", "stop", "target", "rr"] if c in scan_df.columns]
                    levels = scan_df.set_index("ticker")[need].to_dict("index") if need else {}
                else:
                    levels = {}

                # 저장
                save_cached_scan(sig, scan_df)
                save_cached_levels(sig, levels)

            st.session_state["scan_df"] = scan_df
            st.session_state["scan_levels"] = levels
        else:
            st.session_state["scan_df"] = None
            st.session_state["scan_levels"] = {}

    market_msg = st.session_state.get("market_msg", "")
    market_ok = st.session_state.get("market_ok", True)
    scan_df = st.session_state.get("scan_df", None)

    if market_msg:
        st.write(f"Market filter: **{market_msg}**")

    if scan_df is not None:
        if not market_ok:
            st.warning("KOSPI filter blocked scan.")
        else:
            pick = render_scanner_results(scan_df, name_map)
            if pick:
                st.session_state["selected_scan_ticker"] = pick

elif tab == "Browse":
    # Browse: 검색/선택 UI
    _ = render_search_and_select(
        tickers,
        name_map,
        state_key="selected_browse_ticker",
        title=f"Browse ({uni.market})",
    )

else:
    # Data 탭은 당장 “나중에 고민”이었으니, 일단 안내만.
    st.info("Data tab is deprecated in the new pipeline. Use daily downloader + parquet buffers.")
    st.stop()


# -----------------------------
# Common chart area
# -----------------------------
if tab == "Scanner":
    selected = st.session_state.get("selected_scan_ticker")
    scan_levels = st.session_state.get("scan_levels", None)
elif tab == "Browse":
    selected = st.session_state.get("selected_browse_ticker")
    scan_levels = None
else:
    selected = None
    scan_levels = None

if selected:
    selected = str(selected).zfill(6)
    selected_name = name_map.get(selected, selected)
    st.subheader(f"{selected} - {selected_name}")
    render_naver_link(selected)
    
    sub = df[df["ticker"].astype(str).str.zfill(6) == selected].copy()
    #st.write("rows before date parse:", len(sub))
    sub["date"] = pd.to_datetime(sub["date"], errors="coerce")
    #st.write("rows after date parse:", sub["date"].notna().sum())
    sub = sub.dropna(subset=["date"])

    sub = sub.sort_values("date")

    if sub.empty:
        st.warning("No OHLCV rows for selected ticker (after date normalization).")
        st.stop()

    prefix = "ps_scan" if tab == "Scanner" else "ps_browse"

    render_chart_and_sizing_two_column(
        selected=selected,
        sub=sub,
        scan_levels=scan_levels,
        key_prefix=prefix,
    )