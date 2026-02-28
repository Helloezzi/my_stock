# ui/data_view.py
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

import download_kospi_yf as dk
from core.config import DATA_DIR
from core.data_loader import list_dataset_files  # 당장은 유지 (2-2에서 data_loader 교체 예정)
from core.ticker_names import clear_name_cache

RADIO_KEY = "dataset_radio"
ACTIVE_KEY = "selected_csv_name"
PENDING_ACTIVE_KEY = "pending_active_csv"

_END_RE = re.compile(r"end(\d{8})")

def _sort_key_csv(p: Path) -> tuple[int, float]:
    """
    1) 파일명에서 endYYYYMMDD가 있으면 그 날짜를 최우선 키로 사용
    2) 없으면 mtime으로 fallback
    (reverse=True로 정렬할 것)
    """
    m = _END_RE.search(p.name)
    if m:
        return (int(m.group(1)), p.stat().st_mtime)
    return (0, p.stat().st_mtime)



def _ensure_parquet(csv_path: Path) -> Path:
    """
    csv_path에 대응하는 parquet가 없으면 생성하고 parquet 경로를 반환.
    """
    parquet_path = csv_path.with_suffix(".parquet")
    if parquet_path.exists():
        return parquet_path

    df = pd.read_csv(csv_path)
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    df.to_parquet(parquet_path, index=False)
    return parquet_path


def render_data_tab() -> Optional[Path]:
    st.header("Data")
    col_left, col_right = st.columns([2.2, 1.0], gap="large")

    # -------------------------
    # Left: Dataset list (csv 기준으로 노출)
    # -------------------------
    with col_left:
        st.subheader("Datasets")

        files = sorted(list_dataset_files(), key=_sort_key_csv, reverse=True)
        if not files:
            st.warning("No CSV found.")
            return None

        names = [p.name for p in files]
        latest = names[0]

        # pending(다운로드 직후)은 최우선
        pending = st.session_state.get(PENDING_ACTIVE_KEY)
        if pending and pending in names:
            st.session_state[ACTIVE_KEY] = pending
            st.session_state[RADIO_KEY] = pending
            del st.session_state[PENDING_ACTIVE_KEY]

        # active가 없거나 목록에 없으면 최신으로
        active = st.session_state.get(ACTIVE_KEY)
        if (not active) or (active not in names):
            st.session_state[ACTIVE_KEY] = latest
            st.session_state[RADIO_KEY] = latest

        # radio 값이 깨졌으면 active로 복구
        if st.session_state.get(RADIO_KEY) not in names:
            st.session_state[RADIO_KEY] = st.session_state[ACTIVE_KEY]

        active = st.session_state[ACTIVE_KEY]
        st.caption(f"Active: **{active}**")

        def _on_dataset_change():
            st.session_state[ACTIVE_KEY] = st.session_state[RADIO_KEY]
            # 스캔/차트 상태 리셋
            st.session_state.pop("scan_df", None)
            st.session_state.pop("scan_levels", None)
            st.session_state.pop("selected_scan_ticker", None)
            st.cache_data.clear()

        st.radio(
            "Active CSV",
            options=names,
            key=RADIO_KEY,
            on_change=_on_dataset_change,
        )

    # -------------------------
    # Right: Download controls
    # -------------------------
    with col_right:
        st.subheader("Download")

        end_date = st.date_input("End date")
        lookback = st.selectbox("Lookback", ["1d", "6mo", "1y", "2y"], index=1)
        universe = st.selectbox("Universe", ["Top200", "Top500", "All(KOSPI)"], index=0)

        n = 200
        uni_label = "top200"
        if universe == "Top500":
            n = 500
            uni_label = "top500"
        elif universe == "All(KOSPI)":
            n = 10_000
            uni_label = "all"

        if st.button("Rebuild CSV", type="primary"):
            DATA_DIR.mkdir(parents=True, exist_ok=True)

            raw_end_str = end_date.strftime("%Y%m%d")
            tmp_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            tmp_csv = DATA_DIR / f"_tmp_{tmp_ts}.csv"

            progress = st.progress(0.0)
            status = st.empty()

            def _cb(p):
                done = int(p.get("done", 0))
                total = int(p.get("total", 1)) or 1
                progress.progress(min(1.0, done / total))
                status.text(f"{done}/{total} downloading...")

            res = dk.rebuild_kospi_top200_csv(
                out_csv_path=str(tmp_csv),
                n=n,
                end_date=raw_end_str,
                lookback=lookback,
                progress_cb=_cb,
            )

            if len(res) == 3:
                out_path, failed, used_end = res
            else:
                out_path, failed = res
                used_end = raw_end_str

            final_name = f"kospi_{uni_label}_{lookback}_end{used_end}.csv"
            final_csv = DATA_DIR / final_name

            if final_csv.exists():
                try:
                    Path(out_path).unlink(missing_ok=True)
                except Exception:
                    pass
                st.info(f"Already exists → reuse: {final_csv.name}")
            else:
                Path(out_path).rename(final_csv)
                st.success(f"Saved: {final_csv.name}")

            # ✅ Parquet 생성 (여기가 맞는 위치)
            try:
                parquet_path = _ensure_parquet(final_csv)
                st.success(f"Parquet saved: {parquet_path.name}")
            except Exception as e:
                st.warning(f"Parquet build failed: {e}")

            if failed:
                st.warning(
                    f"Failed tickers ({len(failed)}): {', '.join(failed[:20])}"
                    + (" ..." if len(failed) > 20 else "")
                )

            # pending으로 넘겨서 다음 rerun에서 active로 반영
            st.session_state[PENDING_ACTIVE_KEY] = final_csv.name

            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.subheader("Cache")

        if st.button("Clear name cache"):
            clear_name_cache()
            st.success("Name cache cleared.")

    # ---- return active CSV Path ----
    files = sorted(list_dataset_files(), key=_sort_key_csv, reverse=True)

    active = st.session_state.get(ACTIVE_KEY)
    if active:
        for p in files:
            if p.name == active:
                return p

    return files[0] if files else None