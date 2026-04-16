import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def render_chart(sub: pd.DataFrame, entry: float, stop: float, target: float, *, compact: bool = False):
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
    sub["x"] = sub.index.astype(int)

    x0 = int(sub["x"].iloc[0])
    x1 = int(sub["x"].iloc[-1])
    x1_pad = x1 + 5

    sub["ma20"] = sub["close"].rolling(20).mean()
    sub["ma60"] = sub["close"].rolling(60).mean()
    if not compact:
        sub["ma5"] = sub["close"].rolling(5).mean()
        sub["ma120"] = sub["close"].rolling(120).mean()
        sub["std20"] = sub["close"].rolling(20).std()
        sub["bb_upper"] = sub["ma20"] + 2 * sub["std20"]
        sub["bb_lower"] = sub["ma20"] - 2 * sub["std20"]

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=sub["x"],
        open=sub["open"], high=sub["high"], low=sub["low"], close=sub["close"],
        name="Price",
        increasing=dict(line=dict(color="#F04452"), fillcolor="#F04452"),
        decreasing=dict(line=dict(color="#3182F6"), fillcolor="#3182F6"),
    ))

    fig.add_trace(go.Scatter(x=sub["x"], y=sub["ma20"], name="MA20", line=dict(color="#D32020", width=1)))
    fig.add_trace(go.Scatter(x=sub["x"], y=sub["ma60"], name="MA60", line=dict(color="#F57800", width=1)))
    if not compact:
        fig.add_trace(go.Scatter(x=sub["x"], y=sub["ma5"], name="MA5", line=dict(color="#39FF14", width=1)))
        fig.add_trace(go.Scatter(x=sub["x"], y=sub["ma120"], name="MA120", line=dict(color="#8122A1", width=1)))
        fig.add_trace(go.Scatter(x=sub["x"], y=sub["bb_upper"], name="BB Upper",
                                 line=dict(color="rgba(255,200,0,0.45)", width=1)))
        fig.add_trace(go.Scatter(x=sub["x"], y=sub["bb_lower"], name="BB Lower",
                                 line=dict(color="rgba(255,200,0,0.45)", width=1),
                                 fill="tonexty", fillcolor="rgba(255,200,0,0.06)"))

    fig.add_trace(go.Scatter(x=[x0], y=[None], mode="lines",
                             line=dict(color="rgba(255,255,255,0.95)", width=1, dash="dash"),
                             name="Entry", showlegend=True))
    fig.add_trace(go.Scatter(x=[x0], y=[None], mode="lines",
                             line=dict(color="rgba(255, 80, 80,0.95)", width=1, dash="dot"),
                             name="Stop", showlegend=True))
    fig.add_trace(go.Scatter(x=[x0], y=[None], mode="lines",
                             line=dict(color="rgba( 80,200,120,0.95)", width=1, dash="dot"),
                             name="Target", showlegend=True))

    fig.add_trace(go.Scatter(
        x=sub["x"], y=sub["close"], yaxis="y2",
        mode="lines", line=dict(width=0), opacity=0,
        showlegend=False, hoverinfo="skip",
    ))

    entry_y, stop_y, target_y = float(entry), float(stop), float(target)
    if (stop_y < entry_y) and (target_y > entry_y):
        styles = {
            "entry": dict(color="rgba(255,255,255,0.95)", width=1, dash="dash"),
            "stop": dict(color="rgba(255, 80, 80,0.95)", width=1, dash="dot"),
            "target": dict(color="rgba( 80,200,120,0.95)", width=1, dash="dot"),
        }
        fig.add_shape(type="line", x0=x0, x1=x1_pad, y0=entry_y, y1=entry_y, xref="x", yref="y", line=styles["entry"])
        fig.add_shape(type="line", x0=x0, x1=x1_pad, y0=stop_y, y1=stop_y, xref="x", yref="y", line=styles["stop"])
        fig.add_shape(type="line", x0=x0, x1=x1_pad, y0=target_y, y1=target_y, xref="x", yref="y", line=styles["target"])
        fig.update_xaxes(range=[x0, x1_pad])

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
        height=420 if compact else 650,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
                    bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
        margin=dict(t=80, r=70),
    )
    st.plotly_chart(fig, use_container_width=True)

    if compact:
        return

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
