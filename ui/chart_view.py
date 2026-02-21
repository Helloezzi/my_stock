import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from core.position import calc_position
from core.links import naver_stock_url


def render_search_and_select(tickers: list[str], name_map: dict[str, str]) -> str:
    query = st.text_input("Search (Ticker or Name)", value="", placeholder="예: 005930 또는 삼성")

    options = []
    q = query.strip()
    for t in tickers:
        nm = name_map.get(t, t)
        label = f"{t} - {nm}"
        if not q:
            options.append(label)
        else:
            if q.lower() in t.lower() or q in str(nm):
                options.append(label)

    if not options:
        st.warning("검색 결과가 없습니다. 다른 키워드로 검색해보세요.")
        st.stop()

    current_t = st.session_state.get("selected_ticker", tickers[0])
    current_label = f"{current_t} - {name_map.get(current_t, current_t)}"
    try:
        current_idx = options.index(current_label)
    except ValueError:
        current_idx = 0

    selected_display = st.selectbox("Select Ticker", options, index=current_idx, key="main_selectbox")
    selected = selected_display.split(" - ")[0]
    st.session_state["selected_ticker"] = selected
    return selected


def render_naver_link(ticker: str) -> None:
    st.link_button(
        label="네이버 주식 페이지 열기",
        url=naver_stock_url(ticker),
        help="선택한 종목의 네이버 금융 페이지로 이동"
    )


def render_position_sizing(selected: str, sub: pd.DataFrame, scan_levels: dict | None):
    levels = (scan_levels or {}).get(selected, None)

    if levels:
        entry_default = float(levels["entry"])
        stop_default = float(levels["stop"])
        target_default = float(levels["target"])
    else:
        entry_default = float(sub["close"].iloc[-1])
        stop_default = float(sub["low"].tail(10).min())
        target_default = float(sub["high"].tail(20).max())

    st.subheader("Position Sizing (Auto)")

    capital = st.number_input("Capital (KRW)", min_value=0, value=1_000_000, step=100_000)
    risk_pct = st.slider("Risk per trade (%)", 0.5, 5.0, 2.0, 0.1) / 100.0
    max_invest_pct = st.slider("Max invest per trade (%)", 10, 100, 50, 5) / 100.0

    col1, col2, col3 = st.columns(3)
    with col1:
        entry = st.number_input("Entry", value=float(entry_default), key=f"entry_{selected}")
    with col2:
        stop = st.number_input("Stop", value=float(stop_default), key=f"stop_{selected}")
    with col3:
        target = st.number_input("Target", value=float(target_default), key=f"target_{selected}")

    res = calc_position(capital, risk_pct, entry, stop, max_invest_pct=max_invest_pct)

    if res is None:
        st.error("Stop must be lower than Entry.")
        return float(entry), float(stop), float(target)

    qty = res["qty"]
    invest = res["invest"]
    loss_at_stop = res["loss_at_stop"]
    rr = (target - entry) / (entry - stop) if (entry - stop) > 0 else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Qty (shares)", f"{qty:,}")
    c2.metric("Invest (KRW)", f"{invest:,.0f}")
    c3.metric("Loss @ Stop", f"{loss_at_stop:,.0f}")
    c4.metric("R/R", f"{rr:.2f}")

    st.caption(
        f"Risk budget: {res['risk_budget']:,.0f} KRW | "
        f"Per-share risk: {res['per_share_risk']:,.0f} KRW"
    )

    return float(entry), float(stop), float(target)


def render_chart(sub: pd.DataFrame, entry: float, stop: float, target: float):
    sub = sub.copy()
    sub["ma20"] = sub["close"].rolling(20).mean()
    sub["ma60"] = sub["close"].rolling(60).mean()
    sub["ma120"] = sub["close"].rolling(120).mean()

    if sub["close"].median() > 2_000_000:
        st.warning("가격(close) 값이 비정상적으로 큽니다. CSV 컬럼 매핑이 꼬였을 가능성이 큽니다.")
        st.write(sub[["date","open","high","low","close","volume"]].head(20))

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=sub["date"], open=sub["open"], high=sub["high"], low=sub["low"], close=sub["close"],
        name="Price"
    ))
    fig.add_trace(go.Scatter(x=sub["date"], y=sub["ma20"], name="MA20"))
    fig.add_trace(go.Scatter(x=sub["date"], y=sub["ma60"], name="MA60"))
    fig.add_trace(go.Scatter(x=sub["date"], y=sub["ma120"], name="MA120"))

    entry_y, stop_y, target_y = float(entry), float(stop), float(target)
    x0, x1 = sub["date"].min(), sub["date"].max()
    draw_lines = (stop_y < entry_y) and (target_y > entry_y)

    if draw_lines:
        x1_pad = x1 + pd.Timedelta(days=7)
        styles = {
            "entry":  dict(color="rgba(255,255,255,0.95)", width=3, dash="dash"),
            "stop":   dict(color="rgba(255, 80, 80,0.95)", width=3, dash="dot"),
            "target": dict(color="rgba( 80,200,120,0.95)", width=3, dash="dot"),
        }

        fig.add_shape(type="line", x0=x0, x1=x1_pad, y0=entry_y,  y1=entry_y,  xref="x", yref="y", line=styles["entry"])
        fig.add_shape(type="line", x0=x0, x1=x1_pad, y0=stop_y,   y1=stop_y,   xref="x", yref="y", line=styles["stop"])
        fig.add_shape(type="line", x0=x0, x1=x1_pad, y0=target_y, y1=target_y, xref="x", yref="y", line=styles["target"])

        label_box = dict(
            showarrow=False, xref="x", yref="y", x=x1_pad, xanchor="left", yanchor="middle",
            bgcolor="rgba(20,20,20,0.85)", bordercolor="rgba(255,255,255,0.35)", borderwidth=1,
            font=dict(color="white", size=12), align="left",
        )
        fig.add_annotation(y=entry_y,  text=f"Entry {entry_y:,.0f}",  **label_box)
        fig.add_annotation(y=stop_y,   text=f"Stop  {stop_y:,.0f}",   **label_box)
        fig.add_annotation(y=target_y, text=f"Target {target_y:,.0f}", **label_box)
        fig.update_xaxes(range=[x0, x1_pad])

    fig.update_layout(xaxis_rangeslider_visible=False, height=600, yaxis=dict(tickformat=","))
    st.plotly_chart(fig, use_container_width=True)

    vol_fig = go.Figure()
    vol_fig.add_trace(go.Bar(x=sub["date"], y=sub["volume"], name="Volume"))
    vol_fig.update_layout(height=250, yaxis=dict(tickformat=","))
    st.plotly_chart(vol_fig, use_container_width=True)