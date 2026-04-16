from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import streamlit as st

from core.data_loader import load_all_markets
from core.scan_cache import (
    load_cached_levels,
    load_cached_scan,
    save_cached_levels,
    save_cached_scan,
    scan_signature,
)


@st.cache_data(show_spinner=True)
def load_buffers(_fingerprint: str):
    dfs, infos = load_all_markets()
    return dfs, infos


def ensure_session_defaults(tickers: list[str]) -> None:
    if "selected_scan_ticker" not in st.session_state:
        st.session_state["selected_scan_ticker"] = None
    if "selected_browse_ticker" not in st.session_state:
        st.session_state["selected_browse_ticker"] = tickers[0] if tickers else None
    if "scan_sig" not in st.session_state:
        st.session_state["scan_sig"] = None


@dataclass(frozen=True)
class ScanRunResult:
    scan_df: pd.DataFrame | None
    levels: dict
    market_ok: bool
    market_msg: str


def run_scanner_if_needed(
    *,
    latest_date: str,
    market: str,
    top_n,
    strategy_label: str,
    market_mode: str,
    params,
    strategy_by_label: dict,
    market_filter_loader,
    data_df: pd.DataFrame,
) -> ScanRunResult:
    sig = scan_signature(
        latest_date=str(latest_date),
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

        market_ok, market_msg = market_filter_loader()
        st.session_state["market_ok"] = market_ok
        st.session_state["market_msg"] = market_msg

        if market_ok:
            cached = load_cached_scan(sig)
            cached_levels = load_cached_levels(sig)

            if cached is not None and cached_levels is not None:
                scan_df = cached
                levels = cached_levels
            else:
                strategy = strategy_by_label[strategy_label]
                scan_df = cached if cached is not None else strategy.scan(data_df, params)

                if not scan_df.empty and "ticker" in scan_df.columns:
                    need = [c for c in ["entry", "stop", "target", "rr"] if c in scan_df.columns]
                    levels = scan_df.set_index("ticker")[need].to_dict("index") if need else {}
                else:
                    levels = {}

                save_cached_scan(sig, scan_df)
                save_cached_levels(sig, levels)

            st.session_state["scan_df"] = scan_df
            st.session_state["scan_levels"] = levels
        else:
            st.session_state["scan_df"] = None
            st.session_state["scan_levels"] = {}

    return ScanRunResult(
        scan_df=st.session_state.get("scan_df", None),
        levels=st.session_state.get("scan_levels", {}),
        market_ok=st.session_state.get("market_ok", True),
        market_msg=st.session_state.get("market_msg", ""),
    )
