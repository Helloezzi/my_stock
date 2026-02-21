# ui/scanner_view.py
import streamlit as st

import pandas as pd

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

def render_scanner_results(scan_df, name_map, state_key="selected_scan_ticker"):
    if scan_df is None or scan_df.empty:
        st.warning("No scan results.")
        return None

    st.subheader("Scanner Results")

    df = scan_df.copy()
    df["name"] = df["ticker"].map(lambda x: name_map.get(x, x))

    # ✅ 1) 먼저 "원본 컬럼명" 기준으로 보여줄 컬럼만 선택
    keep_cols = [
        "ticker", "name", "date",
        "entry", "stop", "target",
        "rr",
        "rr_pref", "trend_score", "rs_score", "vol_score",
        "vol_ratio_5v20",
        "score",
    ]
    df = df[[c for c in keep_cols if c in df.columns]]

    # ✅ 2) 그 다음에 rename
    col_rename = {
        "ticker": "코드",
        "name": "이름",
        "date": "기준",
        "entry": "진입",
        "stop": "손절",
        "target": "목표",
        "rr": "R/R",
        "vol_ratio_5v20": "거래",

        "rr_pref": "RR선호",
        "trend_score": "추세",
        "rs_score": "상대강도",
        "vol_score": "변동성",
        "score": "총점",
    }
    df = df.rename(columns=col_rename)

    # ✅ 3) 표시 순서(한글 컬럼명 기준)
    preferred_order = [
        "이름", "코드", "기준",
        "진입", "손절", "목표",
        "R/R",
        "RR선호", "추세", "상대강도", "변동성",
        "거래",
        "총점",
    ]
    df = df[[c for c in preferred_order if c in df.columns]]

    display_df_show = df.copy()

    # ✅ KRW(정수로 보이게)
    krw_cols = ["진입", "손절", "목표", "리스크", "보상"]
    for c in krw_cols:
        if c in display_df_show.columns:
            display_df_show[c] = display_df_show[c].map(_fmt_int)

    # ✅ 비율/점수 계열
    float_cols = ["R/R", "RR선호", "추세", "상대강도", "변동성", "거래", "총점"]
    for c in float_cols:
        if c in display_df_show.columns:
            # R/R 같은 건 2~3자리 취향. 우선 2자리 추천
            display_df_show[c] = display_df_show[c].map(lambda v: _fmt_float(v, 3 if c == "R/R" else 2))


    st.dataframe(display_df_show, use_container_width=True, hide_index=True)

    # ---- picker ----
    tickers = scan_df["ticker"].tolist()
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