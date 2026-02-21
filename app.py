import os
import time
import streamlit as st

import download_kospi as dk

from core.config import APP_TITLE
from core.data_loader import resolve_csv_path, load_data, get_ticker_name_map
from core.market_index import load_kospi_index_1y
from core.market_filter import kospi_market_ok
from core.strategies import get_strategies
from ui.sidebar import render_sidebar
from ui.scanner_view import render_scanner_results
from ui.chart_view import (
    render_search_and_select,
    render_naver_link,
    render_position_sizing,
    render_chart,
)

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)
st.caption(f"Running file: {os.path.abspath(__file__)}")

# --- Strategies ---
strategies = get_strategies()
strategy_by_label = {s.name: s for s in strategies}
strategy_labels = list(strategy_by_label.keys())

# --- Sidebar ---
sb = render_sidebar(strategy_labels=strategy_labels)

# --- CSV path ---
CSV_PATH = resolve_csv_path()

# --- Data rebuild ---
if sb["tab"] == "Data" and sb["rebuild_clicked"]:
    with st.status("Downloading... (rebuild 1Y)", expanded=True) as status:
        try:
            if os.path.exists(CSV_PATH):
                os.remove(CSV_PATH)

            out_path, failed = dk.rebuild_kospi_top200_1y_csv(out_csv_path=CSV_PATH, n=200)
            st.success(f"Saved: {out_path}")
            if failed:
                st.warning(
                    f"Failed tickers ({len(failed)}): {', '.join(failed[:20])}"
                    + (" ..." if len(failed) > 20 else "")
                )
            status.update(label=f"Done ✅ Saved: {out_path}", state="complete")
            st.toast("Data rebuilt successfully", icon="✅")

            time.sleep(0.2)
            st.cache_data.clear()
            st.rerun()

        except Exception as e:
            status.update(label="Failed ❌", state="error")
            st.exception(e)

# --- Load CSV ---
if not os.path.exists(CSV_PATH):
    st.warning(f"CSV not found: {CSV_PATH}\n\nGo to **Data** tab and press **Rebuild**.")
    st.stop()

df = load_data(CSV_PATH)
tickers = sorted(df["ticker"].unique())
name_map = get_ticker_name_map(tickers)

if "selected_ticker" not in st.session_state:
    st.session_state["selected_ticker"] = tickers[0]

# --- Scanner ---
if sb["run_scan"]:
    st.session_state["run_scan"] = True
    st.session_state["scan_params"] = sb["params"]
    st.session_state["market_mode"] = sb["market_mode"]
    st.session_state["strategy_label"] = sb["selected_strategy_label"]

if st.session_state.get("run_scan", False):
    params = st.session_state.get("scan_params", sb["params"])
    market_mode = st.session_state.get("market_mode", sb["market_mode"])
    strategy_label = st.session_state.get("strategy_label", strategy_labels[0])

    idx_df = load_kospi_index_1y()
    ok, msg = kospi_market_ok(idx_df, mode=market_mode)

    st.write(f"Market filter: **{msg}**")

    if not ok:
        st.warning("KOSPI filter blocked scan.")
    else:
        strategy = strategy_by_label[strategy_label]
        scan_df = strategy.scan(df, params)

        pick = render_scanner_results(scan_df, name_map)
        if pick:
            st.session_state["selected_ticker"] = pick
            # 차트/포지션 기본값용
            st.session_state["scan_levels"] = (
                scan_df.set_index("ticker")[["entry", "stop", "target", "rr"]]
                .to_dict(orient="index")
            )

    st.divider()

# --- Search + Select ---
selected = render_search_and_select(tickers, name_map)
selected_name = name_map.get(selected, selected)
st.subheader(f"{selected} - {selected_name}")
render_naver_link(selected)

# --- Chart ---
sub = df[df["ticker"] == selected].sort_values("date").copy()
scan_levels = st.session_state.get("scan_levels", None)

entry, stop, target = render_position_sizing(selected, sub, scan_levels)
render_chart(sub, entry, stop, target)