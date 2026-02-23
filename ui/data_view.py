# ui/data_view.py
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import streamlit as st

import download_kospi as dk
from core.config import DATA_DIR
from core.data_loader_old import list_csv_files


RADIO_KEY = "dataset_radio"
ACTIVE_KEY = "selected_csv_name"
PENDING_ACTIVE_KEY = "pending_active_csv"   # 다운로드 후 여기로 넣는 방식이면 유지

def _list_csv_paths() -> list[Path]:
    if not DATA_DIR.exists():
        return []
    return sorted(DATA_DIR.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)


_END_RE = re.compile(r"end(\d{8})")

def _sort_key_csv(p: Path) -> tuple[int, float]:
    """
    1) 파일명에서 endYYYYMMDD가 있으면 그 날짜를 최우선 키로 사용
    2) 없으면 mtime으로 fallback
    (reverse=True로 정렬할 것)
    """
    m = _END_RE.search(p.name)
    if m:
        # YYYYMMDD → int (큰 값이 최신)
        return (int(m.group(1)), p.stat().st_mtime)
    return (0, p.stat().st_mtime)

def render_data_tab() -> Optional[Path]:
    st.header("Data")

    col_left, col_right = st.columns([2.2, 1.0], gap="large")

    # -------------------------
    # Left: CSV list
    # -------------------------
    with col_left:
        st.subheader("Datasets")

        files = sorted(list_csv_files(), key=_sort_key_csv, reverse=True)
        if not files:
            st.warning("No CSV found.")
            #return None
        else:
            names = [p.name for p in files]

            # ✅ 최신 파일(맨 위)을 기본값으로 사용
            latest = names[0]

            # ✅ pending(다운로드 직후)은 최우선
            pending = st.session_state.get(PENDING_ACTIVE_KEY)
            if pending and pending in names:
                st.session_state[ACTIVE_KEY] = pending
                st.session_state[RADIO_KEY] = pending
                del st.session_state[PENDING_ACTIVE_KEY]

            # ✅ active가 없거나(첫 진입) 현재 목록에 없으면 → 최신으로 자동 선택
            active = st.session_state.get(ACTIVE_KEY)
            if (not active) or (active not in names):
                st.session_state[ACTIVE_KEY] = latest
                st.session_state[RADIO_KEY] = latest

            # ✅ radio 값이 깨졌으면 active로 복구 (위젯 생성 전)
            if st.session_state.get(RADIO_KEY) not in names:
                st.session_state[RADIO_KEY] = st.session_state[ACTIVE_KEY]

            active = st.session_state[ACTIVE_KEY]
            st.caption(f"Active: **{active}**")

            # ✅ radio 변경 시 즉시 active 반영
            def _on_dataset_change():
                st.session_state[ACTIVE_KEY] = st.session_state[RADIO_KEY]
                # 캐시/스캔 상태 리셋
                st.session_state.pop("scan_df", None)
                st.session_state.pop("scan_levels", None)
                st.session_state.pop("selected_scan_ticker", None)
                st.cache_data.clear()
                # on_change는 rerun을 자동으로 유발하므로 st.rerun()은 보통 불필요
                # (원하면 넣어도 되지만, 중복 rerun으로 깜빡임이 늘 수 있음)

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

        universe = st.selectbox(
            "Universe",
            ["Top200", "Top500", "All(KOSPI)"],
            index=0,
        )

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

            if failed:
                st.warning(
                    f"Failed tickers ({len(failed)}): {', '.join(failed[:20])}"
                    + (" ..." if len(failed) > 20 else "")
                )

            # ✅ 여기서 RADIO_KEY를 직접 바꾸면 (위젯이 이미 생성된 상태라) 문제가 날 수 있음
            # → pending으로 넘기고 다음 rerun에서 위젯 생성 전에 반영
            st.session_state[PENDING_ACTIVE_KEY] = final_csv.name

            st.cache_data.clear()
            st.rerun()

    # ---- return active Path ----
    files = sorted(list_csv_files(), key=_sort_key_csv, reverse=True)

    active = st.session_state.get(ACTIVE_KEY)
    if active:
        for p in files:
            if p.name == active:
                return p

    return files[0] if files else None