import streamlit as st


def render_position_sizing(selected, sub, scan_levels, key_prefix: str = "ps"):
    st.subheader("Position Sizing")

    level = scan_levels.get(selected) if isinstance(scan_levels, dict) else None
    if level:
        default_entry = float(level.get("entry", 0.0))
        default_stop = float(level.get("stop", 0.0))
        default_target = float(level.get("target", 0.0))
    else:
        default_entry = float(sub["close"].iloc[-1]) if len(sub) else 0.0
        default_stop = default_entry * 0.95
        default_target = default_entry * 1.10

    entry_key = f"{key_prefix}_entry_{selected}"
    stop_key = f"{key_prefix}_stop_{selected}"
    target_key = f"{key_prefix}_target_{selected}"

    if entry_key not in st.session_state:
        st.session_state[entry_key] = int(default_entry)
    if stop_key not in st.session_state:
        st.session_state[stop_key] = int(default_stop)
    if target_key not in st.session_state:
        st.session_state[target_key] = int(default_target)

    st.markdown("#### Trade Levels")
    col_e, col_s, col_t = st.columns(3)

    with col_e:
        entry = st.number_input("Entry", min_value=0, step=100, key=entry_key, format="%d")
        st.caption(f"KRW {entry:,.0f}")
    with col_s:
        stop = st.number_input("Stop", min_value=0, step=100, key=stop_key, format="%d")
        st.caption(f"KRW {stop:,.0f}")
    with col_t:
        target = st.number_input("Target", min_value=0, step=100, key=target_key, format="%d")
        st.caption(f"KRW {target:,.0f}")

    st.divider()
    st.markdown("#### Risk Settings")
    col_cap, col_risk = st.columns([1.2, 1.0])

    with col_cap:
        capital = st.number_input(
            "Capital (KRW)",
            min_value=0,
            value=int(st.session_state.get(f"{key_prefix}_capital", 1_000_000)),
            step=100_000,
            key=f"{key_prefix}_capital",
            format="%d",
        )
        st.caption(f"KRW {capital:,.0f}")

    with col_risk:
        risk_pct = st.slider(
            "Risk per trade (%)",
            0.1, 5.0, float(st.session_state.get(f"{key_prefix}_risk_pct", 2.0)), 0.1,
            key=f"{key_prefix}_risk_pct",
        )
        max_invest_pct = st.slider(
            "Max invest per trade (%)",
            1, 100, int(st.session_state.get(f"{key_prefix}_max_invest_pct", 50)),
            key=f"{key_prefix}_max_invest_pct",
        )

    entry = float(entry)
    stop = float(stop)
    target = float(target)
    capital = float(capital)

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

    st.divider()
    st.markdown("#### Position Summary")
    c1, c2, c3 = st.columns(3)
    c1.metric("Qty (shares)", f"{qty:,}")
    c2.metric("Invest (KRW)", f"{invest:,.0f}")
    c3.metric("Risk budget (KRW)", f"{risk_budget:,.0f}")

    c4, c5, c6 = st.columns(3)
    c4.metric("Loss @ Stop", f"{loss_at_stop:,.0f}")
    c5.metric("Profit @ Target", f"{profit_at_target:,.0f}")
    c6.metric("R/R", f"{rr:.2f}")

    return entry, stop, target
