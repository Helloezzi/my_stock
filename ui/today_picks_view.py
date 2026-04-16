from __future__ import annotations

import pandas as pd
import streamlit as st

from core.market_index import build_market_index_snapshot
from ui.search_view import render_naver_link
from ui.texts import t

VOLUME_LABEL_MAP = {
    "high": "높음",
    "medium": "보통",
    "light": "적음",
    "unknown": "-",
}

FINANCIAL_STATUS_MAP = {
    "strong": "양호",
    "acceptable": "보통",
    "weak": "주의",
}

INDUSTRY_PROFILE_MAP = {
    "neutral": "중립",
    "growth": "성장",
    "declining": "둔화",
}

TEXT_FRAGMENT_MAP = {
    "technical score is strong": "기술 점수가 강함",
    "technical score is acceptable": "기술 점수가 무난함",
    "risk/reward is strong": "손익비가 좋음",
    "risk/reward is workable": "손익비가 무난함",
    "stop distance is relatively tight": "손절 폭이 비교적 타이트함",
    "target upside is meaningful": "목표 상승 여력이 충분함",
    "liquidity is strong": "유동성이 풍부함",
    "liquidity is usable": "유동성이 무난함",
    "wide_stop": "손절 폭이 넓음",
    "rr_not_strong": "손익비가 아주 강하진 않음",
    "lighter_volume": "거래량이 적은 편",
    "score_borderline": "점수가 경계선 수준",
    "isolated_sector": "같은 섹터 후보가 적음",
    "industry_down_day": "업종이 당일 약세",
    "eps_missing": "EPS 정보 없음",
    "negative_eps": "EPS 적자",
    "bps_missing": "BPS 정보 없음",
    "negative_bps": "BPS 음수",
    "high_per": "PER 부담 높음",
    "high_pbr": "PBR 부담 높음",
    "strong": "양호",
    "acceptable": "보통",
    "weak": "주의",
    "high": "높음",
    "medium": "보통",
    "light": "적음",
}


def _render_stat(label: str, value: str) -> None:
    st.markdown(
        f"""
        <div style="padding: 0.1rem 0 0.6rem 0;">
            <div style="font-size: 0.86rem; color: #b8c0cc; margin-bottom: 0.24rem;">{label}</div>
            <div style="font-size: 1.28rem; font-weight: 700; line-height: 1.2;">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _sync_show_chart_from_inline():
    value = bool(st.session_state.get("show_chart_inline_toggle", False))
    st.session_state["show_chart_enabled"] = value
    st.session_state["show_chart_sidebar"] = value


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


def _fmt_generated_at(value) -> str:
    if value is None:
        return "-"
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)


def _fmt_trade_date_title(value) -> str:
    if value is None:
        return "-"
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d")
    except Exception:
        return str(value)


def _fmt_index_value(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        return f"{float(value):,.2f}"
    except Exception:
        return str(value)


def _fmt_change_pct(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        number = float(value)
        prefix = "+" if number > 0 else ""
        return f"{prefix}{number:.2f}%"
    except Exception:
        return str(value)


def _market_tone(kospi_change, kosdaq_change, market_ok: bool | None) -> str:
    changes = []
    for value in [kospi_change, kosdaq_change]:
        if value is None or pd.isna(value):
            continue
        try:
            changes.append(float(value))
        except Exception:
            continue
    if changes:
        avg = sum(changes) / len(changes)
        if avg >= 1.0:
            return t("today.market_tone.positive")
        if avg <= -1.0:
            return t("today.market_tone.defensive")
        return t("today.market_tone.neutral")
    return t("today.market_tone.positive") if market_ok else t("today.market_tone.cautious")


def _translate_fragment(text):
    if text is None:
        return "-"
    value = str(text)
    translated = value
    for src, dest in TEXT_FRAGMENT_MAP.items():
        translated = translated.replace(src, dest)
    return translated


def _translate_list(values):
    if not values:
        return []
    return [_translate_fragment(v) for v in values if str(v).strip()]


def _build_levels(records: list[dict]) -> dict:
    levels = {}
    for item in records:
        ticker = str(item.get("ticker", "")).zfill(6)
        levels[ticker] = {
            "entry": item.get("entry"),
            "stop": item.get("stop"),
            "target": item.get("target"),
            "rr": item.get("rr"),
        }
    return levels


def _prepare_display_df(filtered_df: pd.DataFrame, keep_cols: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, list[dict], list[str], dict[str, str]]:
    raw_df = filtered_df[[c for c in keep_cols if c in filtered_df.columns]].copy()
    display_df = raw_df.rename(
        columns={
            "rank": "순위",
            "action_label": "라벨",
            "ticker": "코드",
            "name": "종목명",
            "market": "시장",
            "rr": "R/R",
            "risk_pct": "리스크 %",
            "reward_pct": "보상 %",
            "score": "점수",
            "confidence": "신뢰도",
            "financial_score": "재무",
            "financial_status": "재무 상태",
            "industry_name": "업종",
            "industry_profile": "업종 특성",
            "sector_score": "섹터",
            "volume_label": "거래량",
        }
    )

    for col in ["리스크 %", "보상 %"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].map(
                lambda v: (_fmt_float(float(v) * 100, 1) + "%") if pd.notna(v) else ""
            )
    for col in ["R/R", "점수", "신뢰도", "재무", "섹터"]:
        if col in display_df.columns:
            digits = 3 if col == "R/R" else 2
            display_df[col] = display_df[col].map(lambda v: _fmt_float(v, digits))

    if "거래량" in display_df.columns:
        display_df["거래량"] = display_df["거래량"].map(lambda v: VOLUME_LABEL_MAP.get(str(v), _translate_fragment(v)))
    if "재무 상태" in display_df.columns:
        display_df["재무 상태"] = display_df["재무 상태"].map(lambda v: FINANCIAL_STATUS_MAP.get(str(v), _translate_fragment(v)))
    if "업종 특성" in display_df.columns:
        display_df["업종 특성"] = display_df["업종 특성"].map(lambda v: INDUSTRY_PROFILE_MAP.get(str(v), _translate_fragment(v)))

    filtered_records = raw_df.to_dict("records")
    tickers = [str(item.get("ticker", "")).zfill(6) for item in filtered_records if item.get("ticker")]
    name_map = {
        str(item.get("ticker", "")).zfill(6): item.get("name", str(item.get("ticker", "")).zfill(6))
        for item in filtered_records
    }
    return raw_df, display_df, filtered_records, tickers, name_map


def render_today_picks(
    payload: dict,
    state_key: str = "selected_scan_ticker",
    lightweight_mode: bool = True,
) -> tuple[str | None, dict]:
    picks = payload.get("picks", []) if isinstance(payload, dict) else []
    summary = payload.get("summary", {}) if isinstance(payload, dict) else {}

    title_col, date_col = st.columns([1.8, 1.0])
    with title_col:
        st.subheader(t("today.title"))
    with date_col:
        st.markdown(f"### {_fmt_trade_date_title(payload.get('trade_date'))}")

    if not picks:
        st.info(t("today.no_picks"))
        return None, {}

    all_df = pd.DataFrame(picks)
    market_options = ["ALL"] + sorted([str(m) for m in all_df["market"].dropna().unique().tolist() if str(m).strip()])
    selected_market = st.session_state.get("today_picks_market_filter", "ALL")
    if selected_market not in market_options:
        selected_market = "ALL"
        st.session_state["today_picks_market_filter"] = selected_market

    filtered_df = all_df.copy() if selected_market == "ALL" else all_df[all_df["market"].astype(str).str.upper() == selected_market.upper()].copy()
    if filtered_df.empty:
        st.info(t("today.no_market_picks", market=selected_market))
        return None, {}

    per_market_counts = summary.get("per_market_counts", {}) if isinstance(summary, dict) else {}
    label_counts = summary.get("label_counts", {}) if isinstance(summary, dict) else {}
    market_snapshot = summary.get("market_index_snapshot", {}) if isinstance(summary, dict) else {}
    if not market_snapshot:
        market_snapshot = build_market_index_snapshot()
    kospi_snapshot = market_snapshot.get("KOSPI", {}) if isinstance(market_snapshot, dict) else {}
    kosdaq_snapshot = market_snapshot.get("KOSDAQ", {}) if isinstance(market_snapshot, dict) else {}
    usdkrw_snapshot = market_snapshot.get("USD/KRW", {}) if isinstance(market_snapshot, dict) else {}
    sp500_snapshot = market_snapshot.get("S&P 500", {}) if isinstance(market_snapshot, dict) else {}
    nasdaq_snapshot = market_snapshot.get("NASDAQ", {}) if isinstance(market_snapshot, dict) else {}

    st.markdown(f"### {t('today.overview')}")
    market_ok = bool(summary.get("market_ok", True))
    total_picks = int(summary.get("pick_count", len(picks)) or 0)
    actionable_count = int(summary.get("actionable_count", 0) or 0)
    watch_count = int(label_counts.get("Watch", 0) or 0)
    market_tone = _market_tone(
        kospi_snapshot.get("change_pct"),
        kosdaq_snapshot.get("change_pct"),
        market_ok,
    )
    updated_at = _fmt_generated_at(payload.get("generated_at"))

    st.caption(t("today.market_domestic"))
    m1, m2, m3 = st.columns(3)
    m1.metric(t("today.kospi_close"), _fmt_index_value(kospi_snapshot.get("close")), _fmt_change_pct(kospi_snapshot.get("change_pct")))
    m2.metric(t("today.kosdaq_close"), _fmt_index_value(kosdaq_snapshot.get("close")), _fmt_change_pct(kosdaq_snapshot.get("change_pct")))
    m3.metric(t("today.usdkrw"), _fmt_index_value(usdkrw_snapshot.get("close")), _fmt_change_pct(usdkrw_snapshot.get("change_pct")))

    st.caption(t("today.market_global"))
    g1, g2 = st.columns(2)
    g1.metric(t("today.sp500"), _fmt_index_value(sp500_snapshot.get("close")), _fmt_change_pct(sp500_snapshot.get("change_pct")))
    g2.metric(t("today.nasdaq"), _fmt_index_value(nasdaq_snapshot.get("close")), _fmt_change_pct(nasdaq_snapshot.get("change_pct")))

    st.caption(t("today.result"))
    r1, r2, r3, r4 = st.columns(4)
    r1.metric(t("today.picks"), total_picks)
    r2.metric(t("today.actionable"), actionable_count)
    r3.metric(t("today.watch"), watch_count)
    r4.metric(t("today.market_tone"), market_tone)

    st.caption(
        t(
            "today.updated_line",
            updated_at=updated_at,
            kospi_count=int(per_market_counts.get("KOSPI", 0)),
            kosdaq_count=int(per_market_counts.get("KOSDAQ", 0)),
            a_count=int(label_counts.get("A", 0)),
            b_count=int(label_counts.get("B", 0)),
            watch_count=watch_count,
        )
    )
    st.caption(t("today.market_tone_help"))
    st.divider()

    if bool(summary.get("no_pick", False)):
        st.warning(t("today.no_actionable"))

    keep_cols = [
        "rank",
        "action_label",
        "ticker",
        "name",
        "market",
        "rr",
        "risk_pct",
        "reward_pct",
        "score",
        "confidence",
        "financial_score",
        "financial_status",
        "industry_name",
        "industry_profile",
        "sector_score",
        "volume_label",
        "ai_summary",
        "why_selected",
        "risk_flags",
        "financial_flags",
        "sector_flags",
    ]

    raw_df, display_df, filtered_records, tickers, name_map = _prepare_display_df(filtered_df, keep_cols)
    if not tickers:
        return None, {}

    current = st.session_state.get(state_key, tickers[0])
    if current not in tickers:
        current = tickers[0]
        st.session_state[state_key] = current

    if lightweight_mode:
        if "show_chart_enabled" not in st.session_state:
            st.session_state["show_chart_enabled"] = False
        if "show_chart_inline_toggle" not in st.session_state:
            st.session_state["show_chart_inline_toggle"] = st.session_state["show_chart_enabled"]

    st.markdown(f"### {t('today.pick_list')}")
    selected_market = st.radio(
        t("today.market_radio"),
        options=market_options,
        horizontal=True,
        key="today_picks_market_filter",
    )

    filtered_df = all_df.copy() if selected_market == "ALL" else all_df[all_df["market"].astype(str).str.upper() == selected_market.upper()].copy()
    if filtered_df.empty:
        st.info(t("today.no_market_picks", market=selected_market))
        return None, {}

    raw_df, display_df, filtered_records, tickers, name_map = _prepare_display_df(filtered_df, keep_cols)
    current = st.session_state.get(state_key, tickers[0])
    if current not in tickers:
        current = tickers[0]
        st.session_state[state_key] = current

    pick = st.selectbox(
        t("today.pick_select"),
        options=tickers,
        index=tickers.index(current),
        format_func=lambda x: next(
            (
                f"#{int(item.get('rank', 0) or 0)} | {x} - {name_map.get(x, x)} | "
                f"{item.get('action_label', '-')} | R/R {_fmt_float(item.get('rr'), 2) or '-'}"
                for item in filtered_records
                if str(item.get("ticker", "")).zfill(6) == x
            ),
            f"{x} - {name_map.get(x, x)}",
        ),
        key=f"{state_key}_today_picks_selectbox",
    )
    st.session_state[state_key] = pick

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    selected_record = next(
        (item for item in filtered_records if str(item.get("ticker", "")).zfill(6) == pick),
        filtered_records[0],
    )

    st.divider()
    st.markdown(f"### {t('today.selected_pick')}")

    selected_ticker = str(selected_record.get("ticker", "")).zfill(6)
    selected_name = name_map.get(selected_ticker, "-")
    title_col, button_col = st.columns([4.5, 1.2], vertical_alignment="center")
    with title_col:
        st.markdown(
            f"""
            <div style="font-size: 1.55rem; font-weight: 800; line-height: 1.25; margin: 0.15rem 0 0.8rem 0;">
                {selected_ticker} - {selected_name}
            </div>
            """,
            unsafe_allow_html=True,
        )
    with button_col:
        render_naver_link(selected_ticker)

    ai_summary = str(selected_record.get("ai_summary", "") or "").strip()
    if ai_summary:
        st.info(_translate_fragment(ai_summary))

    top1, top2, top3 = st.columns(3)
    with top1:
        _render_stat("라벨", str(selected_record.get("action_label", "-") or "-"))
    with top2:
        _render_stat("R/R", _fmt_float(selected_record.get("rr"), 3) or "-")
    with top3:
        _render_stat("점수", _fmt_float(selected_record.get("score"), 2) or "-")

    mid1, mid2, mid3 = st.columns(3)
    with mid1:
        _render_stat(
            "리스크 %",
            _fmt_float((selected_record.get("risk_pct") or 0) * 100, 1) + "%"
            if selected_record.get("risk_pct") is not None else "-"
        )
    with mid2:
        _render_stat(
            "보상 %",
            _fmt_float((selected_record.get("reward_pct") or 0) * 100, 1) + "%"
            if selected_record.get("reward_pct") is not None else "-"
        )
    with mid3:
        _render_stat(
            "거래량",
            VOLUME_LABEL_MAP.get(str(selected_record.get("volume_label", "-") or "-"), _translate_fragment(selected_record.get("volume_label", "-"))),
        )

    low1, low2, low3, low4 = st.columns(4)
    with low1:
        _render_stat(
            "재무 상태",
            FINANCIAL_STATUS_MAP.get(str(selected_record.get("financial_status", "-") or "-"), _translate_fragment(selected_record.get("financial_status", "-"))),
        )
    with low2:
        _render_stat("재무 점수", _fmt_float(selected_record.get("financial_score"), 0) or "-")
    with low3:
        _render_stat("업종", str(selected_record.get("industry_name", "-") or "-"))
    with low4:
        _render_stat("섹터", _fmt_float(selected_record.get("sector_score"), 0) or "-")

    entry = _fmt_int(selected_record.get("entry")) or "-"
    stop = _fmt_int(selected_record.get("stop")) or "-"
    target = _fmt_int(selected_record.get("target")) or "-"
    st.caption(f"매매 계획: 진입 {entry} | 손절 {stop} | 목표 {target}")

    why_selected = selected_record.get("why_selected") or []
    risk_flags = selected_record.get("risk_flags") or []
    financial_flags = selected_record.get("financial_flags") or []
    sector_flags = selected_record.get("sector_flags") or []

    if why_selected:
        st.write(f"{t('today.why_selected')}: " + " | ".join(_translate_list(why_selected)))
    if risk_flags:
        st.write(f"{t('today.risk_flags')}: " + " | ".join(_translate_list(risk_flags)))
    if financial_flags:
        st.write(f"{t('today.financial_flags')}: " + " | ".join(_translate_list(financial_flags)))
    if sector_flags:
        st.write(f"{t('today.sector_flags')}: " + " | ".join(_translate_list(sector_flags)))

    levels = _build_levels(filtered_records)
    return pick, levels
