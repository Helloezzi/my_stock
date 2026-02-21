# ui/scanner_view.py
import streamlit as st


def render_scanner_results(scan_df, name_map, state_key="selected_scan_ticker"):
    if scan_df is None or scan_df.empty:
        st.warning("No scan results.")
        return None

    st.subheader("Scanner Results")

    display_df = scan_df.copy()

    # 보기용 name 컬럼
    display_df["name"] = display_df["ticker"].map(
        lambda x: name_map.get(x, x)
    )

    col_rename = {
        "ticker": "코드",
        "name": "이름",
        "date": "기준",
        "entry": "진입",
        "stop": "손절",
        "target": "목표",
        "rr": "R/R",
        "risk": "리스크",
        "reward": "보상",
        "vol_ratio_5v20": "거래",
        "score": "점수",        
    }

    display_df = display_df.rename(columns={k: v for k, v in col_rename.items() if k in display_df.columns})

    # 3) 컬럼 순서: 이름을 맨 앞으로
    preferred_order = [
        "이름", "코드", "기준",
        "진입", "손절", "목표",
        "R/R", "리스크", "보상",
        "거래", "점수",        
    ]

    cols = list(display_df.columns)
    visible_cols = [c for c in preferred_order if c in display_df.columns]
    display_df = display_df[visible_cols]

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )

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