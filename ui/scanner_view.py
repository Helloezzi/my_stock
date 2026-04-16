import pandas as pd
import streamlit as st


def _fmt_int(x):
    if pd.isna(x):
        return ""
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return str(x)


def _fmt_float(x, digits=2):
    if pd.isna(x):
        return ""
    try:
        return f"{float(x):,.{digits}f}"
    except Exception:
        return str(x)


def render_scanner_results(scan_df, name_map, state_key="selected_scan_ticker", max_rows: int | None = None):
    if scan_df is None or scan_df.empty:
        st.warning("No scan results.")
        return None

    st.subheader("Scanner Results")

    df = scan_df.copy()
    df["name"] = df["ticker"].map(lambda x: name_map.get(x, x))

    if "ma5_slope_3d" in df.columns:
        df["ma5_slope_%"] = (df["ma5_slope_3d"] * 100).round(2)

    if "ma5_slope_score" in df.columns:
        df["ma5_score"] = df["ma5_slope_score"].round(2)

    keep_cols = [
        "ticker", "name", "date",
        "entry", "stop", "target",
        "rr",
        "rr_pref", "trend_score", "rs_score", "vol_score",
        "vol_ratio_5v20",
        "score",
        "ma5_slope_%",
        "ma5_score",
    ]
    df = df[[c for c in keep_cols if c in df.columns]]

    col_rename = {
        "ticker": "Code",
        "name": "Name",
        "date": "Date",
        "entry": "Entry",
        "stop": "Stop",
        "target": "Target",
        "rr": "R/R",
        "vol_ratio_5v20": "Volume",
        "rr_pref": "RR Pref",
        "trend_score": "Trend",
        "rs_score": "Relative Strength",
        "vol_score": "Volatility",
        "score": "Score",
        "ma5_slope_%": "MA5 Slope %",
        "ma5_score": "MA5 Score",
    }
    df = df.rename(columns=col_rename)

    preferred_order = [
        "Name", "Code", "Date",
        "Entry", "Stop", "Target",
        "R/R",
        "RR Pref", "Trend", "MA5 Slope %", "MA5 Score",
        "Relative Strength", "Volatility", "Volume", "Score",
    ]
    df = df[[c for c in preferred_order if c in df.columns]]

    if max_rows and max_rows > 0:
        df = df.head(int(max_rows)).copy()

    display_df = df.copy()

    for col in ["Entry", "Stop", "Target"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].map(_fmt_int)

    float_cols = ["R/R", "RR Pref", "Trend", "MA5 Slope %", "MA5 Score", "Relative Strength", "Volatility", "Volume", "Score"]
    for col in float_cols:
        if col in display_df.columns:
            display_df[col] = display_df[col].map(lambda v: _fmt_float(v, 3 if col == "R/R" else 2))

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    tickers = df["Code"].astype(str).str.zfill(6).tolist() if "Code" in df.columns else scan_df["ticker"].tolist()
    if not tickers:
        return None

    current = st.session_state.get(state_key, tickers[0])
    if current not in tickers:
        current = tickers[0]
        st.session_state[state_key] = current

    pick = st.selectbox(
        "Pick from results to view chart",
        options=tickers,
        index=tickers.index(current),
        format_func=lambda x: f"{x} - {name_map.get(x, x)}",
        key=f"{state_key}_selectbox",
    )

    st.session_state[state_key] = pick
    return pick
