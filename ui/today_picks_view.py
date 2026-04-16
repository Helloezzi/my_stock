from __future__ import annotations

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


def render_today_picks(payload: dict, state_key: str = "selected_scan_ticker") -> tuple[str | None, dict]:
    picks = payload.get("picks", []) if isinstance(payload, dict) else []
    summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
    source = payload.get("source", {}) if isinstance(payload, dict) else {}

    st.subheader("Today Picks")
    st.caption(
        f"Trade date: **{payload.get('trade_date', '-') }** | "
        f"Strategy: **{source.get('strategy_label', '-') }** | "
        f"Picks: **{summary.get('pick_count', 0)}**"
    )

    market_ok = bool(summary.get("market_ok", True))
    market_msg = str(summary.get("market_msg", "")).strip()
    if market_msg:
        st.write(f"Market filter: **{market_msg}**")

    if not market_ok:
        st.warning("Market filter blocked daily picks.")

    if not picks:
        st.info("No published picks available for the current daily result.")
        return None, {}

    all_df = pd.DataFrame(picks)
    market_options = ["ALL"] + sorted([str(m) for m in all_df["market"].dropna().unique().tolist() if str(m).strip()])
    selected_market = st.selectbox(
        "Market view",
        options=market_options,
        index=0,
        help="Filter the published daily picks by market.",
        key="today_picks_market_filter",
    )

    if selected_market == "ALL":
        filtered_df = all_df.copy()
    else:
        filtered_df = all_df[all_df["market"].astype(str).str.upper() == selected_market.upper()].copy()

    if filtered_df.empty:
        st.info(f"No published picks available for {selected_market}.")
        return None, {}

    keep_cols = ["rank", "ticker", "name", "market", "date", "stage", "entry", "stop", "target", "rr", "score"]
    raw_df = filtered_df[[c for c in keep_cols if c in filtered_df.columns]].copy()
    display_df = raw_df.rename(
        columns={
            "rank": "Rank",
            "ticker": "Code",
            "name": "Name",
            "market": "Market",
            "date": "Date",
            "stage": "Stage",
            "entry": "Entry",
            "stop": "Stop",
            "target": "Target",
            "rr": "R/R",
            "score": "Score",
        }
    )

    for col in ["Entry", "Stop", "Target"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].map(_fmt_int)
    for col in ["R/R", "Score"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].map(lambda v: _fmt_float(v, 3 if col == "R/R" else 2))

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    filtered_records = raw_df.to_dict("records")
    tickers = [str(item.get("ticker", "")).zfill(6) for item in filtered_records if item.get("ticker")]
    if not tickers:
        return None, {}

    current = st.session_state.get(state_key, tickers[0])
    if current not in tickers:
        current = tickers[0]
        st.session_state[state_key] = current

    name_map = {
        str(item.get("ticker", "")).zfill(6): item.get("name", str(item.get("ticker", "")).zfill(6))
        for item in filtered_records
    }
    pick = st.selectbox(
        "Pick from daily results",
        options=tickers,
        index=tickers.index(current),
        format_func=lambda x: f"{x} - {name_map.get(x, x)}",
        key=f"{state_key}_today_picks_selectbox",
    )
    st.session_state[state_key] = pick

    levels = {}
    for item in filtered_records:
        ticker = str(item.get("ticker", "")).zfill(6)
        levels[ticker] = {
            "entry": item.get("entry"),
            "stop": item.get("stop"),
            "target": item.get("target"),
            "rr": item.get("rr"),
        }
    return pick, levels
