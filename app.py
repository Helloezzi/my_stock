import streamlit as st
import json
import hashlib

from core.config import APP_TITLE
from core.data_loader import list_csv_files, load_data, get_ticker_name_map
from core.market_index import load_kospi_index_1y
from core.market_filter import kospi_market_ok
from core.strategies import get_strategies

from ui.sidebar import render_sidebar
from ui.scanner_view import render_scanner_results
from ui.chart_view import render_search_and_select, render_naver_link, render_chart_and_sizing_two_column
from ui.data_view import render_data_tab

from core.data_loader import load_data, get_active_csv_path

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

selected_csv_path = None

# =========================
# DATA TAB (main view)
# =========================
tab = sb["tab"]  # 너의 sidebar 리턴값 기준

if tab == "Data":
    render_data_tab()  # active만 바꿈 (return 불필요)
else:
    active_path = get_active_csv_path()
    if not active_path:
        st.warning("No active CSV. Go to Data tab and select a dataset.")
        st.stop()

    df = load_data(str(active_path))
    # 이후 Scanner/Browse 로직 진행

# =========================
# Load active CSV for Scanner/Browse
# =========================
csv_files = list_csv_files()
if not csv_files:
    st.warning("No CSV found.")
    st.stop()

if selected_csv_path is None:
    active_name = st.session_state.get("selected_csv_name")
    selected_csv_path = next((p for p in csv_files if p.name == active_name), csv_files[0])

df = load_data(str(selected_csv_path))

tickers = sorted(df["ticker"].unique())
name_map = get_ticker_name_map(tickers)

# 상태 분리
if "selected_scan_ticker" not in st.session_state:
    st.session_state["selected_scan_ticker"] = None
if "selected_browse_ticker" not in st.session_state:
    st.session_state["selected_browse_ticker"] = tickers[0]

def _scan_signature(csv_name: str, strategy_label: str, market_mode: str, params) -> str:
    # params가 dataclass면 asdict, 아니면 dict/객체에 맞게 변환
    if hasattr(params, "__dict__"):
        params_obj = params.__dict__
    else:
        params_obj = dict(params)

    payload = {
        "csv": csv_name,
        "strategy": strategy_label,
        "market_mode": market_mode,
        "params": params_obj,
    }
    s = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.md5(s.encode("utf-8")).hexdigest()

# =========================
# SCANNER TAB
# =========================
if sb["tab"] == "Scanner":
    # 현재 설정
    csv_name = st.session_state.get("selected_csv_name", selected_csv_path.name)
    strategy_label = sb["selected_strategy_label"]
    market_mode = sb["market_mode"]
    params = sb["params"]

    active_name = st.session_state.get("selected_csv_name", "(none)")
    st.caption(f"Dataset: **{active_name}**")

    sig = _scan_signature(csv_name, strategy_label, market_mode, params)
    last_sig = st.session_state.get("scan_sig")

    # ✅ 바뀐 경우에만 재스캔
    if sig != last_sig:
        st.session_state["scan_sig"] = sig

        idx_df = load_kospi_index_1y()
        ok, msg = kospi_market_ok(idx_df, mode=market_mode)
        st.session_state["market_ok"] = ok
        st.session_state["market_msg"] = msg

        if ok:
            strategy = strategy_by_label[strategy_label]
            scan_df = strategy.scan(df, params)
            st.session_state["scan_df"] = scan_df
            st.session_state["scan_levels"] = (
                scan_df.set_index("ticker")[["entry", "stop", "target", "rr"]].to_dict(orient="index")
                if not scan_df.empty else {}
            )
        else:
            st.session_state["scan_df"] = None
            st.session_state["scan_levels"] = {}

    # 이후 렌더는 저장된 결과 사용
    scan_df = st.session_state.get("scan_df", None)
    market_msg = st.session_state.get("market_msg", "")
    market_ok = st.session_state.get("market_ok", True)

    if market_msg:
        st.write(f"Market filter: **{market_msg}**")

    if scan_df is not None:
        if not market_ok:
            st.warning("KOSPI filter blocked scan.")
        else:
            pick = render_scanner_results(scan_df, name_map)
            if pick:
                st.session_state["selected_ticker"] = pick

# =========================
# BROWSE TAB
# =========================
elif sb["tab"] == "Browse":
    active_name = st.session_state.get("selected_csv_name", "(none)")
    st.caption(f"Dataset: **{active_name}**")
    _ = render_search_and_select(
        tickers,
        name_map,
        state_key="selected_browse_ticker",
        title="Browse KOSPI Top200",
    )    

# =========================
# Common chart area
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