import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from core.position import calc_position
from core.links import naver_stock_url
# ui/chart_view.py

def krw(v):
    return f"{v:,.0f}"

def render_search_and_select(
    tickers,
    name_map,
    state_key="selected_browse_ticker",
    title="Search Top200",
):
    st.subheader(title)

    q = st.text_input(
        "Search (Ticker or Name)",
        value="",
        key=f"{state_key}_search",
    ).strip()

    filtered = []
    for t in tickers:
        nm = name_map.get(t, t)
        if not q or q.lower() in t.lower() or q in str(nm):
            filtered.append(t)

    if not filtered:
        st.warning("검색 결과가 없습니다.")
        return None

    current = st.session_state.get(state_key, filtered[0])
    if current not in filtered:
        current = filtered[0]
        st.session_state[state_key] = current

    selected = st.selectbox(
        "Select Ticker",
        options=filtered,
        index=filtered.index(current),
        format_func=lambda x: f"{x} - {name_map.get(x, x)}",
        key=f"{state_key}_selectbox",
    )

    st.session_state[state_key] = selected
    return selected


def render_naver_link(ticker: str) -> None:
    st.link_button(
        label="네이버 주식 페이지 열기",
        url=naver_stock_url(ticker),
        help="선택한 종목의 네이버 금융 페이지로 이동"
    )

def render_position_sizing(selected, sub, scan_levels, key_prefix: str = "ps"):
    st.subheader("Position Sizing")

    # -------------------------
    # Defaults from scan_levels / last close
    # -------------------------
    level = scan_levels.get(selected) if isinstance(scan_levels, dict) else None
    if level:
        default_entry = float(level.get("entry", 0.0))
        default_stop = float(level.get("stop", 0.0))
        default_target = float(level.get("target", 0.0))
    else:
        default_entry = float(sub["close"].iloc[-1]) if len(sub) else 0.0
        default_stop = default_entry * 0.95
        default_target = default_entry * 1.10

    # per-ticker keys
    entry_key = f"{key_prefix}_entry_{selected}"
    stop_key = f"{key_prefix}_stop_{selected}"
    target_key = f"{key_prefix}_target_{selected}"

    # initialize once per ticker
    if entry_key not in st.session_state:
        st.session_state[entry_key] = int(default_entry)
    if stop_key not in st.session_state:
        st.session_state[stop_key] = int(default_stop)
    if target_key not in st.session_state:
        st.session_state[target_key] = int(default_target)

    # -------------------------
    # Trade Levels (Entry/Stop/Target)
    # -------------------------
    st.markdown("#### Trade Levels")
    col_e, col_s, col_t = st.columns(3)

    with col_e:
        entry = st.number_input("Entry", min_value=0, step=100, key=entry_key, format="%d")
        st.caption(f"₩ {entry:,.0f}")
    with col_s:
        stop = st.number_input("Stop", min_value=0, step=100, key=stop_key, format="%d")
        st.caption(f"₩ {stop:,.0f}")
    with col_t:
        target = st.number_input("Target", min_value=0, step=100, key=target_key, format="%d")
        st.caption(f"₩ {target:,.0f}")

    st.divider()

    # -------------------------
    # Risk Settings
    # -------------------------
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
        st.caption(f"₩ {capital:,.0f}")

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

    # -------------------------
    # Compute position
    # -------------------------
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

    # -------------------------
    # Summary (metrics in grid)
    # -------------------------
    st.markdown("#### Position Summary")

    # 2 rows x 3 columns 느낌으로
    c1, c2, c3 = st.columns(3)
    c1.metric("Qty (shares)", f"{qty:,}")
    c2.metric("Invest (KRW)", f"{invest:,.0f}")
    c3.metric("Risk budget (KRW)", f"{risk_budget:,.0f}")

    c4, c5, c6 = st.columns(3)
    c4.metric("Loss @ Stop", f"{loss_at_stop:,.0f}")
    c5.metric("Profit @ Target", f"{profit_at_target:,.0f}")
    c6.metric("R/R", f"{rr:.2f}")

    return entry, stop, target


def render_chart(sub: pd.DataFrame, entry: float, stop: float, target: float):
    
    if sub is None or len(sub) == 0:
        st.warning("No data to render chart.")
        return

    if "x" not in sub.columns:
        sub = sub.copy()
        sub["x"] = range(len(sub))

    if sub["x"].isna().all() or len(sub["x"]) == 0:
        st.warning("Chart x-axis is empty.")
        return
    
    sub["date"] = pd.to_datetime(sub["date"], errors="coerce")
    sub = sub.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # ✅ trading-day index axis
    sub["x"] = sub.index.astype(int)    
    
    x0 = int(sub["x"].iloc[0])
    x1 = int(sub["x"].iloc[-1])
    x1_pad = x1 + 5  # ✅ 봉 기준 패딩(원하면 10)

    # indicators
    sub["ma5"] = sub["close"].rolling(5).mean()
    sub["ma20"] = sub["close"].rolling(20).mean()
    sub["ma60"] = sub["close"].rolling(60).mean()
    sub["ma120"] = sub["close"].rolling(120).mean()
    sub["std20"] = sub["close"].rolling(20).std()
    sub["bb_upper"] = sub["ma20"] + 2 * sub["std20"]
    sub["bb_lower"] = sub["ma20"] - 2 * sub["std20"]

    fig = go.Figure()

    # ✅ Candle (x=sub["x"])
    fig.add_trace(go.Candlestick(
        x=sub["x"],
        open=sub["open"], high=sub["high"], low=sub["low"], close=sub["close"],
        name="Price",
        increasing=dict(line=dict(color="#F04452"), fillcolor="#F04452"),
        decreasing=dict(line=dict(color="#3182F6"), fillcolor="#3182F6"),
    ))

    # ✅ MAs
    fig.add_trace(go.Scatter(x=sub["x"], y=sub["ma5"],   name="MA5",   line=dict(color="#39FF14", width=1)))
    fig.add_trace(go.Scatter(x=sub["x"], y=sub["ma20"],  name="MA20",  line=dict(color="#D32020", width=1)))
    fig.add_trace(go.Scatter(x=sub["x"], y=sub["ma60"],  name="MA60",  line=dict(color="#F57800", width=1)))
    fig.add_trace(go.Scatter(x=sub["x"], y=sub["ma120"], name="MA120", line=dict(color="#8122A1", width=1)))

    # ✅ BB
    fig.add_trace(go.Scatter(x=sub["x"], y=sub["bb_upper"], name="BB Upper",
                             line=dict(color="rgba(255,200,0,0.45)", width=1)))
    fig.add_trace(go.Scatter(x=sub["x"], y=sub["bb_lower"], name="BB Lower",
                             line=dict(color="rgba(255,200,0,0.45)", width=1),
                             fill="tonexty", fillcolor="rgba(255,200,0,0.06)"))

    # legend only traces
    fig.add_trace(go.Scatter(x=[x0], y=[None], mode="lines",
                             line=dict(color="rgba(255,255,255,0.95)", width=1, dash="dash"),
                             name="Entry", showlegend=True))
    fig.add_trace(go.Scatter(x=[x0], y=[None], mode="lines",
                             line=dict(color="rgba(255, 80, 80,0.95)", width=1, dash="dot"),
                             name="Stop", showlegend=True))
    fig.add_trace(go.Scatter(x=[x0], y=[None], mode="lines",
                             line=dict(color="rgba( 80,200,120,0.95)", width=1, dash="dot"),
                             name="Target", showlegend=True))

    # yaxis2 dummy
    fig.add_trace(go.Scatter(
        x=sub["x"], y=sub["close"], yaxis="y2",
        mode="lines", line=dict(width=0), opacity=0,
        showlegend=False, hoverinfo="skip",
    ))

    # Entry/Stop/Target lines (✅ x0/x1 are index)
    entry_y, stop_y, target_y = float(entry), float(stop), float(target)
    draw_lines = (stop_y < entry_y) and (target_y > entry_y)
    if draw_lines:
        styles = {
            "entry":  dict(color="rgba(255,255,255,0.95)", width=1, dash="dash"),
            "stop":   dict(color="rgba(255, 80, 80,0.95)", width=1, dash="dot"),
            "target": dict(color="rgba( 80,200,120,0.95)", width=1, dash="dot"),
        }
        fig.add_shape(type="line", x0=x0, x1=x1_pad, y0=entry_y,  y1=entry_y,  xref="x", yref="y", line=styles["entry"])
        fig.add_shape(type="line", x0=x0, x1=x1_pad, y0=stop_y,   y1=stop_y,   xref="x", yref="y", line=styles["stop"])
        fig.add_shape(type="line", x0=x0, x1=x1_pad, y0=target_y, y1=target_y, xref="x", yref="y", line=styles["target"])
        fig.update_xaxes(range=[x0, x1_pad])

    # ✅ tick labels: show dates on index axis
    tick_step = max(1, len(sub) // 10)
    tickvals = sub["x"][::tick_step]
    ticktext = sub["date"].dt.strftime("%Y-%m-%d")[::tick_step]
    fig.update_xaxes(tickmode="array", tickvals=tickvals, ticktext=ticktext)

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0f1116",
        plot_bgcolor="#0f1116",
        font=dict(color="#cfd3dc"),
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.05)"),
        yaxis2=dict(overlaying="y", side="right", tickformat=",", showgrid=False,
                    zeroline=False, showline=False, ticks="outside", ticklen=4, showticklabels=True),
        xaxis_rangeslider_visible=False,
        height=650,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
                    bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
        margin=dict(t=80, r=70),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ✅ volume chart also uses index axis (no rangebreaks needed)
    colors = ["#F04452" if c >= o else "#3182F6" for c, o in zip(sub["close"], sub["open"])]
    vol_fig = go.Figure()
    vol_fig.add_trace(go.Bar(x=sub["x"], y=sub["volume"], marker_color=colors))

    vol_fig.update_xaxes(tickmode="array", tickvals=tickvals, ticktext=ticktext)
    vol_fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0f1116",
        plot_bgcolor="#0f1116",
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)")
    )
    st.plotly_chart(vol_fig, use_container_width=True)


def render_chart_and_sizing_two_column(*, selected: str, sub, scan_levels, key_prefix: str):
    col_chart, col_ps = st.columns([2.2, 1.0], gap="large")

    with col_ps:
        entry, stop, target = render_position_sizing(
            selected, sub, scan_levels,
            key_prefix=key_prefix,   # ✅
        )

    with col_chart:
        render_chart(sub, entry, stop, target)

    return entry, stop, target