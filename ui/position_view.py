import streamlit as st

from ui.texts import t


def _render_metric(label: str, value: str) -> None:
    st.markdown(
        f"""
        <div style="padding: 0.05rem 0 0.55rem 0;">
            <div style="font-size: 0.82rem; color: #b8c0cc; margin-bottom: 0.18rem;">{label}</div>
            <div style="font-size: 1.02rem; font-weight: 700; line-height: 1.2;">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _safe_float(value, fallback: float) -> float:
    try:
        if value is None:
            return float(fallback)
        return float(value)
    except Exception:
        return float(fallback)


def _resolve_level_defaults(selected, sub, scan_levels):
    level = scan_levels.get(selected) if isinstance(scan_levels, dict) else None
    last_close = float(sub["close"].iloc[-1]) if len(sub) else 0.0
    if level:
        default_entry = _safe_float(level.get("entry"), last_close)
        default_stop = _safe_float(level.get("stop"), default_entry * 0.95 if default_entry > 0 else last_close * 0.95)
        default_target = _safe_float(level.get("target"), default_entry * 1.10 if default_entry > 0 else last_close * 1.10)
    else:
        default_entry = last_close
        default_stop = default_entry * 0.95
        default_target = default_entry * 1.10
    return default_entry, default_stop, default_target


def _ensure_trade_level_state(selected, key_prefix: str, default_entry: float, default_stop: float, default_target: float):
    entry_key = f"{key_prefix}_entry_{selected}"
    stop_key = f"{key_prefix}_stop_{selected}"
    target_key = f"{key_prefix}_target_{selected}"

    if entry_key not in st.session_state:
        st.session_state[entry_key] = int(default_entry)
    if stop_key not in st.session_state:
        st.session_state[stop_key] = int(default_stop)
    if target_key not in st.session_state:
        st.session_state[target_key] = int(default_target)
    return entry_key, stop_key, target_key


def _ensure_risk_state(key_prefix: str):
    capital_key = f"{key_prefix}_capital"
    risk_key = f"{key_prefix}_risk_pct"
    max_invest_key = f"{key_prefix}_max_invest_pct"

    if capital_key not in st.session_state:
        st.session_state[capital_key] = 1_000_000
    if risk_key not in st.session_state:
        st.session_state[risk_key] = 2.0
    if max_invest_key not in st.session_state:
        st.session_state[max_invest_key] = 50
    return capital_key, risk_key, max_invest_key


def _calculate_position_summary(entry: float, stop: float, target: float, capital: float, risk_pct: float, max_invest_pct: int):
    risk_budget = capital * (risk_pct / 100.0)
    per_share_risk = max(entry - stop, 0.0)
    qty_by_risk = int(risk_budget // per_share_risk) if per_share_risk > 0 else 0
    max_invest = capital * (max_invest_pct / 100.0)
    qty_by_max = int(max_invest // entry) if entry > 0 else 0

    qty = max(0, min(qty_by_risk, qty_by_max))
    invest = qty * entry
    loss_at_stop = qty * (entry - stop)
    profit_at_target = qty * (target - entry)
    rr = (target - entry) / (entry - stop) if (entry - stop) > 0 else 0.0
    return {
        "qty": qty,
        "invest": invest,
        "risk_budget": risk_budget,
        "loss_at_stop": loss_at_stop,
        "profit_at_target": profit_at_target,
        "rr": rr,
    }


def render_trade_levels(selected, sub, scan_levels, key_prefix: str = "ps"):
    default_entry, default_stop, default_target = _resolve_level_defaults(selected, sub, scan_levels)
    entry_key, stop_key, target_key = _ensure_trade_level_state(
        selected, key_prefix, default_entry, default_stop, default_target
    )

    st.markdown(f"#### {t('position.title')}")
    st.caption(t("position.caption"))

    col_e, col_s, col_t = st.columns(3)

    with col_e:
        entry = st.number_input(t("position.entry"), min_value=0, step=100, key=entry_key, format="%d")
        st.caption(f"KRW {entry:,.0f}")
    with col_s:
        stop = st.number_input(t("position.stop"), min_value=0, step=100, key=stop_key, format="%d")
        st.caption(f"KRW {stop:,.0f}")
    with col_t:
        target = st.number_input(t("position.target"), min_value=0, step=100, key=target_key, format="%d")
        st.caption(f"KRW {target:,.0f}")

    return float(entry), float(stop), float(target)


def render_risk_settings_and_summary(selected, entry: float, stop: float, target: float, key_prefix: str = "ps", use_expander: bool = False):
    capital_key, risk_key, max_invest_key = _ensure_risk_state(key_prefix)

    if use_expander:
        container = st.expander(t("position.risk_expander"), expanded=False)
    else:
        container = st.container()

    with container:
        if not use_expander:
            st.markdown(f"### {t('position.risk_expander')}")
        st.markdown(f"#### {t('position.risk_setting')}")
        col_cap, col_risk = st.columns([1.2, 1.0])

        with col_cap:
            capital = st.number_input(
                t("position.capital"),
                min_value=0,
                step=100_000,
                key=capital_key,
                format="%d",
            )
            st.caption(f"KRW {capital:,.0f}")

        with col_risk:
            risk_pct = st.slider(
                t("position.risk_per_trade"),
                0.1, 5.0, step=0.1,
                key=risk_key,
            )
            max_invest_pct = st.slider(
                t("position.max_invest"),
                1, 100,
                key=max_invest_key,
            )

        summary = _calculate_position_summary(
            float(entry), float(stop), float(target), float(capital), float(risk_pct), int(max_invest_pct)
        )

        st.markdown(f"#### {t('position.summary')}")
        c1, c2, c3 = st.columns(3)
        with c1:
            _render_metric(t("position.qty"), f"{summary['qty']:,}")
        with c2:
            _render_metric(t("position.invest"), f"{summary['invest']:,.0f}")
        with c3:
            _render_metric(t("position.risk_budget"), f"{summary['risk_budget']:,.0f}")

        c4, c5, c6 = st.columns(3)
        with c4:
            _render_metric(t("position.loss_at_stop"), f"{summary['loss_at_stop']:,.0f}")
        with c5:
            _render_metric(t("position.profit_at_target"), f"{summary['profit_at_target']:,.0f}")
        with c6:
            _render_metric(t("position.rr"), f"{summary['rr']:.2f}")
