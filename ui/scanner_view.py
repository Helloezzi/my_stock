import streamlit as st
import pandas as pd


def render_scan_help() -> None:
    with st.expander("📘 Column Description (How to read this table)", expanded=False):
        st.markdown("""
        **Entry**: 최근 종가 기준  
        **Stop**: 최근 N일 저점 기반(버퍼 포함)  
        **Target**: 최근 N일 고점 기준  
        **R/R**: (Target-Entry) / (Entry-Stop)  
        **Vol_ratio_5v20**: 최근 5일 평균 거래량 / 20일 평균 거래량  
        **Score**: 내부 정렬 점수
        """)


def render_scanner_results(scan_df: pd.DataFrame, name_map: dict[str, str]) -> str | None:
    """
    Returns selected ticker from scan list (or None)
    """
    st.subheader("Scanner Results")
    render_scan_help()

    if scan_df is None or scan_df.empty:
        st.info("No candidates found.")
        return None

    scan_df = scan_df.copy()
    scan_df["name"] = scan_df["ticker"].map(name_map).fillna(scan_df["ticker"])
    scan_df["rr_flag"] = scan_df["rr"].apply(lambda x: "🔥" if x >= 2 else "")

    show_cols = ["ticker","name","date","entry","stop","target","rr","risk","reward","vol_ratio_5v20","score"]
    display_df = scan_df[show_cols].rename(columns={
        "ticker": "종목코드",
        "name": "종목명",
        "date": "기준일",
        "entry": "진입가",
        "stop": "손절가",
        "target": "목표가",
        "risk": "리스크",
        "reward": "보상",
        "rr": "손익비(R/R)",
        "vol_ratio_5v20": "거래량비(5/20)",
        "score": "점수",
    })

    st.dataframe(display_df, use_container_width=True, column_config={
        "손익비(R/R)": st.column_config.NumberColumn(help="(목표가 - 진입가) / (진입가 - 손절가)"),
        "리스크": st.column_config.NumberColumn(help="진입가 - 손절가"),
    })

    pick = st.selectbox(
        "Pick from results to view chart",
        options=scan_df["ticker"].tolist(),
        format_func=lambda x: f"{x} - {name_map.get(x, x)}",
        key="scan_pick_selectbox",
    )
    return pick