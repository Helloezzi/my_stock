import os
import time
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import download_kospi as dk
from pykrx import stock
import yfinance as yf

CSV_PATH = "kospi_top200_1y_daily.csv"

st.set_page_config(page_title="KOSPI Swing Viewer", layout="wide")
st.title("KOSPI Swing Viewer")
st.caption(f"Running file: {os.path.abspath(__file__)}")

import math

def calc_position(capital: float, risk_pct: float, entry: float, stop: float,
                  max_invest_pct: float = 1.0):
    """
    capital: 총자본(원)
    risk_pct: 1회 허용 손실 비율 (예: 0.02 = 2%)
    entry/stop: 원화 가격
    max_invest_pct: 한 종목에 최대 몇 %까지 투입할지 (1.0=100%)
    """
    if entry <= stop:
        return None  # 손절가가 진입가보다 높으면 계산 불가

    risk_budget = capital * risk_pct
    per_share_risk = entry - stop

    qty = math.floor(risk_budget / per_share_risk)
    if qty <= 0:
        qty = 0

    invest = qty * entry

    # 한 종목 최대 투입 비중 제한
    invest_cap = capital * max_invest_pct
    if invest > invest_cap and entry > 0:
        qty = math.floor(invest_cap / entry)
        invest = qty * entry

    loss_at_stop = qty * per_share_risk

    return {
        "risk_budget": risk_budget,
        "per_share_risk": per_share_risk,
        "qty": qty,
        "invest": invest,
        "loss_at_stop": loss_at_stop,
    }


# ---------------------------
# Utils / Data Loaders
# ---------------------------
@st.cache_data(show_spinner=False)
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])

    # ticker는 무조건 문자열로 (앞자리 0 보존)
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)

    # 숫자 컬럼 강제 변환
    num_cols = ["open", "high", "low", "close", "volume"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["date", "ticker", "open", "high", "low", "close", "volume"])
    df = df.sort_values(["ticker", "date"])
    return df


@st.cache_data(show_spinner=False)
def get_ticker_name_map(tickers):
    name_map = {}
    for t in tickers:
        try:
            name_map[t] = stock.get_market_ticker_name(t)
        except Exception:
            name_map[t] = t
    return name_map


@st.cache_data(show_spinner=False)
def load_kospi_index_1y():
    """
    KOSPI Index (^KS11) 1Y daily
    yfinance가 환경/버전에 따라 MultiIndex 컬럼을 반환하는 경우가 있어 방어적으로 처리
    """
    raw = yf.download("^KS11", period="1y", interval="1d", auto_adjust=False, progress=False)

    if raw is None or raw.empty:
        return pd.DataFrame()

    df = raw.copy()

    # 1) MultiIndex 컬럼인 경우 (예: ('Close', '^KS11')) 형태 -> Close만 뽑기
    if isinstance(df.columns, pd.MultiIndex):
        # 보통 첫 레벨에 Open/High/Low/Close/... 가 있고
        # 두 번째 레벨에 티커가 붙음
        if "Close" in df.columns.get_level_values(0):
            close_series = df["Close"]
            # close_series가 DataFrame일 수 있으니 첫 컬럼(=^KS11)을 뽑음
            if isinstance(close_series, pd.DataFrame):
                close_series = close_series.iloc[:, 0]
            df = pd.DataFrame({"close": close_series})
        else:
            # 예상 못한 형태면 평탄화 후 시도
            df.columns = ["_".join(map(str, c)).strip() for c in df.columns.to_list()]
            # Close 비슷한 컬럼 찾기
            close_candidates = [c for c in df.columns if c.lower().startswith("close")]
            if not close_candidates:
                return pd.DataFrame()
            df = df.rename(columns={close_candidates[0]: "close"})[["close"]]

    else:
        # 2) 일반 컬럼인 경우: Close가 있으면 close로 rename
        if "Close" in df.columns:
            df = df.rename(columns={"Close": "close"})[["close"]]
        elif "close" in df.columns:
            df = df[["close"]]
        else:
            return pd.DataFrame()

    # 인덱스(날짜)를 컬럼으로
    df = df.reset_index()
    if "Date" in df.columns:
        df = df.rename(columns={"Date": "date"})
    elif "index" in df.columns:
        df = df.rename(columns={"index": "date"})

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=["date", "close"]).sort_values("date")

    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()
    return df


def kospi_market_ok(idx_df: pd.DataFrame, mode: str) -> tuple[bool, str]:
    """
    mode:
      - close_above_ma20: 지수 종가 > MA20
      - ma20_above_ma60:  지수 MA20 > MA60
      - both:            둘 다 만족
    return (ok, message)
    """
    if idx_df is None or idx_df.empty:
        return True, "Index data not available (passed)."

    last = idx_df.iloc[-1]
    if pd.isna(last["ma20"]):
        return True, "Index MA20 not ready (passed)."

    c1 = (last["close"] > last["ma20"])
    c2 = (not pd.isna(last["ma60"])) and (last["ma20"] > last["ma60"])

    if mode == "close_above_ma20":
        return bool(c1), f"KOSPI close({last['close']:.2f}) > MA20({last['ma20']:.2f})"
    if mode == "ma20_above_ma60":
        return bool(c2), f"KOSPI MA20({last['ma20']:.2f}) > MA60({last['ma60']:.2f})"
    if mode == "both":
        return bool(c1 and c2), (
            f"KOSPI close({last['close']:.2f}) > MA20({last['ma20']:.2f}) and "
            f"MA20({last['ma20']:.2f}) > MA60({last['ma60']:.2f})"
        )

    return bool(c1), "Default: close_above_ma20"


@st.cache_data(show_spinner=False)
def run_pullback_scan_with_rr(
    df: pd.DataFrame,
    tolerance: float = 0.03,     # MA20 근접 허용 (±3%)
    stop_lookback: int = 10,     # stop 기준 저점 탐색 기간
    stop_buffer: float = 0.005,  # stop을 저점보다 추가로 아래로 (0.5%)
    target_lookback: int = 20,   # 목표가 기준 고점 기간
    min_rr: float = 1.5          # 최소 R/R
) -> pd.DataFrame:
    """
    눌림 매수 후보 + 리스크/보상비 계산
    entry = 마지막 종가
    stop  = 최근 stop_lookback 저점 최저값 * (1-stop_buffer)
    target= 최근 target_lookback 고점 최고값
    rr    = (target-entry)/(entry-stop)
    """

    results = []

    for t, g in df.groupby("ticker"):
        g = g.sort_values("date").copy()
        if len(g) < 120:
            continue

        g["ma20"] = g["close"].rolling(20).mean()
        g["ma60"] = g["close"].rolling(60).mean()
        g["vol_ma20"] = g["volume"].rolling(20).mean()

        last = g.iloc[-1]
        if pd.isna(last["ma20"]) or pd.isna(last["ma60"]) or pd.isna(last["vol_ma20"]):
            continue

        # 1) 상승 추세(기본)
        uptrend = last["ma20"] > last["ma60"]

        # 2) 최근 모멘텀: 최근 20일 고점이 최근 60일 고점 근처(98% 이상)
        high20 = g["high"].rolling(20).max().iloc[-1]
        high60 = g["high"].rolling(60).max().iloc[-1]
        had_momentum = (not pd.isna(high20)) and (not pd.isna(high60)) and (high20 >= high60 * 0.98)

        # 3) 20일선 근처(눌림)
        near_ma20 = abs(last["close"] - last["ma20"]) / last["ma20"] <= tolerance

        # 4) 거래량 식는 중(눌림 특성)
        vol_5 = g["volume"].tail(5).mean()
        vol_cooling = vol_5 < last["vol_ma20"]

        if not (uptrend and had_momentum and near_ma20 and vol_cooling):
            continue

        # ---- R/R 계산 ----
        entry = float(last["close"])
        recent_low = float(g["low"].tail(stop_lookback).min())
        stop = recent_low * (1.0 - stop_buffer)

        target = float(g["high"].tail(target_lookback).max())

        risk = entry - stop
        reward = target - entry

        if risk <= 0 or reward <= 0:
            continue

        rr = reward / risk
        if rr < min_rr:
            continue

        # 정렬 스코어: rr + (ma20 근접도)
        score = rr + (1.0 - abs(entry - float(last["ma20"])) / float(last["ma20"]))

        results.append({
            "ticker": t,
            "date": last["date"].date(),
            "entry": entry,
            "stop": float(stop),
            "target": float(target),
            "risk": float(risk),
            "reward": float(reward),
            "rr": float(rr),
            "ma20": float(last["ma20"]),
            "ma60": float(last["ma60"]),
            "vol_ratio_5v20": float(vol_5 / float(last["vol_ma20"])),
            "score": float(score),
        })

    if not results:
        return pd.DataFrame(columns=[
            "ticker","date","entry","stop","target","risk","reward","rr",
            "ma20","ma60","vol_ratio_5v20","score"
        ])

    return pd.DataFrame(results).sort_values("score", ascending=False).reset_index(drop=True)


# ---------------------------
# Sidebar Tabs
# ---------------------------
with st.sidebar:
    st.header("Menu")
    tab = st.radio("Select", ["Data", "Scanner"], index=0)

    st.divider()

    if tab == "Data":
        st.subheader("Data")
        if st.button("Rebuild (Last 1Y, KOSPI Top200)", type="primary"):
            with st.status("Downloading... (rebuild 1Y)", expanded=True) as status:
                try:
                    if os.path.exists(CSV_PATH):
                        os.remove(CSV_PATH)

                    out_path, failed = dk.rebuild_kospi_top200_1y_csv(out_csv_path=CSV_PATH, n=200)
                    st.success(f"Saved: {out_path}")
                    if failed:
                        st.warning(
                            f"Failed tickers ({len(failed)}): {', '.join(failed[:20])}"
                            + (" ..." if len(failed) > 20 else "")
                        )
                    status.update(label=f"Done ✅ Saved: {out_path}", state="complete")
                    st.toast("Data rebuilt successfully", icon="✅")

                    time.sleep(0.2)
                    st.cache_data.clear()
                    st.rerun()

                except Exception as e:
                    status.update(label="Failed ❌", state="error")
                    st.exception(e)

    elif tab == "Scanner":
        st.subheader("Scanner")
        st.caption("Pullback candidates + Risk/Reward + Market filter")

        market_mode = st.selectbox(
            "KOSPI filter",
            ["close_above_ma20", "ma20_above_ma60", "both"],
            index=0
        )

        tolerance = st.slider("MA20 tolerance (%)", 1, 10, 3) / 100
        stop_lookback = st.slider("Stop lookback (days)", 5, 30, 10)
        stop_buffer = st.slider("Stop buffer (%)", 0.0, 3.0, 0.5, 0.1) / 100
        target_lookback = st.slider("Target lookback (days)", 10, 90, 20)
        min_rr = st.slider("Min R/R", 0.5, 5.0, 1.5, 0.1)

        if st.button("Run Scan", type="primary"):
            st.session_state["run_scan"] = True
            st.session_state["scan_params"] = dict(
                market_mode=market_mode,
                tolerance=tolerance,
                stop_lookback=stop_lookback,
                stop_buffer=stop_buffer,
                target_lookback=target_lookback,
                min_rr=min_rr,
            )


# ---------------------------
# Main: Load CSV
# ---------------------------
if not os.path.exists(CSV_PATH):
    st.warning(f"CSV not found: {CSV_PATH}\n\nGo to **Data** tab and press **Rebuild**.")
    st.stop()

df = load_data(CSV_PATH)

tickers = sorted(df["ticker"].unique())
name_map = get_ticker_name_map(tickers)

# selected_ticker session state init
if "selected_ticker" not in st.session_state:
    st.session_state["selected_ticker"] = tickers[0]   


# ---------------------------
# Main: Scanner Results
# ---------------------------
if st.session_state.get("run_scan", False):
    params = st.session_state.get("scan_params", {})
    idx_df = load_kospi_index_1y()
    ok, msg = kospi_market_ok(idx_df, mode=params.get("market_mode", "close_above_ma20"))

    st.subheader("Scanner Results")
    with st.expander("📘 Column Description (How to read this table)", expanded=False):
        st.markdown("""
        **Ticker**  
        - 종목 코드

        **Name**  
        - 종목명

        **Date**  
        - 스캔 기준일 (마지막 데이터 날짜)

        **Entry**  
        - 진입가 (최근 종가 기준)

        **Stop**  
        - 손절 기준가  
        - 최근 N일 저점 기반으로 계산됨

        **Target**  
        - 목표가  
        - 최근 N일 고점 기준

        **Risk**  
        - Entry - Stop

        **Reward**  
        - Target - Entry

        **R/R (Risk/Reward)**  
        - (Target - Entry) / (Entry - Stop)  
        - 1.5 이상이면 구조적으로 유리한 편

        **Vol_ratio_5v20**  
        - 최근 5일 평균 거래량 / 20일 평균 거래량  
        - 1보다 작으면 눌림 특성(거래량 감소)

        **Score**  
        - 내부 정렬용 점수  
        - R/R + 20일선 근접도 기반
        """)
    st.write(f"Market filter: **{msg}**")

    if not ok:
        st.warning("KOSPI filter blocked scan.")
    else:
        scan_df = run_pullback_scan_with_rr(
            df,
            tolerance=params.get("tolerance", 0.03),
            stop_lookback=params.get("stop_lookback", 10),
            stop_buffer=params.get("stop_buffer", 0.005),
            target_lookback=params.get("target_lookback", 20),
            min_rr=params.get("min_rr", 1.5),
        )

        if scan_df.empty:
            st.info("No pullback candidates found.")
        else:
            scan_df = scan_df.copy()
            scan_df["name"] = scan_df["ticker"].map(name_map).fillna(scan_df["ticker"])
            scan_df["rr_flag"] = scan_df["rr"].apply(lambda x: "🔥" if x >= 2 else "")            

            show_cols = ["ticker","name","date","entry","stop","target","rr","risk","reward","vol_ratio_5v20","score"]

            display_df = scan_df[show_cols].rename(columns={
                "ticker": "종목코드",
                "name": "종목명",
                "date": "기준일",
                "entry": "진입가",
                "stop": "손절가",
                "target": "목표가",
                "risk": "리스크",
                "reward": "보상",
                "rr": "손익비(R/R)",
                "ma20": "20일선",
                "ma60": "60일선",
                "vol_ratio_5v20": "거래량비(5/20)",
                "score": "점수"
            })

            st.dataframe(display_df, use_container_width=True, column_config={
                "손익비(R/R)": st.column_config.NumberColumn(
                    help="(목표가 - 진입가) / (진입가 - 손절가)"
                ),
                "리스크": st.column_config.NumberColumn(
                    help="진입가 - 손절가"
                ),
            })

            st.session_state["scan_levels"] = (
                scan_df.set_index("ticker")[["entry", "stop", "target", "rr"]]
                .to_dict(orient="index")
            )

            pick = st.selectbox(
                "Pick from results to view chart",
                options=scan_df["ticker"].tolist(),
                format_func=lambda x: f"{x} - {name_map.get(x, x)}",
                key="scan_pick_selectbox",
            )
            st.session_state["selected_ticker"] = pick

    st.divider()


# ---------------------------
# Main: Search + Select
# ---------------------------
query = st.text_input("Search (Ticker or Name)", value="", placeholder="예: 005930 또는 삼성")

options = []
q = query.strip()
for t in tickers:
    nm = name_map.get(t, t)
    label = f"{t} - {nm}"
    if not q:
        options.append(label)
    else:
        # ticker는 lower 비교, name은 그대로 포함검색(한글)
        if q.lower() in t.lower() or q in str(nm):
            options.append(label)

if not options:
    st.warning("검색 결과가 없습니다. 다른 키워드로 검색해보세요.")
    st.stop()

# current selection index
current_t = st.session_state["selected_ticker"]
current_label = f"{current_t} - {name_map.get(current_t, current_t)}"
try:
    current_idx = options.index(current_label)
except ValueError:
    current_idx = 0

selected_display = st.selectbox("Select Ticker", options, index=current_idx, key="main_selectbox")
st.session_state["selected_ticker"] = selected_display.split(" - ")[0]

selected = st.session_state["selected_ticker"]
selected_name = name_map.get(selected, selected)
st.subheader(f"{selected} - {selected_name}")


# ---------------------------
# Main: Chart
# ---------------------------
sub = df[df["ticker"] == selected].sort_values("date").copy()

# 스캐너 레벨이 있으면 그 값 사용, 없으면 데이터로 자동 계산
levels = st.session_state.get("scan_levels", {}).get(selected, None)

if levels:
    entry_default = float(levels["entry"])
    stop_default = float(levels["stop"])
    target_default = float(levels["target"])
else:
    entry_default = float(sub["close"].iloc[-1])
    stop_default = float(sub["low"].tail(10).min())
    target_default = float(sub["high"].tail(20).max())

st.subheader("Position Sizing (Auto)")

# 기본값
capital = st.number_input("Capital (KRW)", min_value=0, value=1_000_000, step=100_000)
risk_pct = st.slider("Risk per trade (%)", 0.5, 5.0, 2.0, 0.1) / 100.0
max_invest_pct = st.slider("Max invest per trade (%)", 10, 100, 50, 5) / 100.0

col1, col2, col3 = st.columns(3)
with col1:
    entry = st.number_input("Entry", value=float(entry_default), key=f"entry_{selected}")
with col2:
    stop  = st.number_input("Stop", value=float(stop_default), key=f"stop_{selected}")
with col3:
    target= st.number_input("Target", value=float(target_default), key=f"target_{selected}")

res = calc_position(capital, risk_pct, entry, stop, max_invest_pct=max_invest_pct)

if res is None:
    st.error("Stop must be lower than Entry.")
else:
    qty = res["qty"]
    invest = res["invest"]
    loss_at_stop = res["loss_at_stop"]

    reward = max(0.0, (target - entry) * qty)
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

sub["ma20"] = sub["close"].rolling(20).mean()
sub["ma60"] = sub["close"].rolling(60).mean()
sub["ma120"] = sub["close"].rolling(120).mean()

if sub["close"].median() > 2_000_000:
    st.warning("가격(close) 값이 비정상적으로 큽니다. CSV 컬럼 매핑이 꼬였을 가능성이 큽니다.")
    st.write(sub[["date","open","high","low","close","volume"]].head(20))

fig = go.Figure()
fig.add_trace(go.Candlestick(
    x=sub["date"],
    open=sub["open"],
    high=sub["high"],
    low=sub["low"],
    close=sub["close"],
    name="Price"
))
fig.add_trace(go.Scatter(x=sub["date"], y=sub["ma20"], name="MA20"))
fig.add_trace(go.Scatter(x=sub["date"], y=sub["ma60"], name="MA60"))
fig.add_trace(go.Scatter(x=sub["date"], y=sub["ma120"], name="MA120"))

# --- Entry / Stop / Target 수평선 표시 (가독성 강화 버전) ---
entry_y = float(entry)
stop_y = float(stop)
target_y = float(target)

x0 = sub["date"].min()
x1 = sub["date"].max()

draw_lines = (stop_y < entry_y) and (target_y > entry_y)

if draw_lines:
    # (선택) 오른쪽 라벨이 잘리면 x1을 약간 미래로 밀기
    x1_pad = x1 + pd.Timedelta(days=7)

    # Plotly dash 스타일: "solid", "dot", "dash", "longdash", "dashdot", "longdashdot"
    styles = {
        "entry":  dict(color="rgba(255,255,255,0.95)", width=3, dash="dash"),
        "stop":   dict(color="rgba(255, 80, 80,0.95)", width=3, dash="dot"),
        "target": dict(color="rgba( 80,200,120,0.95)", width=3, dash="dot"),
    }

    # 굵은 수평선
    fig.add_shape(type="line", x0=x0, x1=x1_pad, y0=entry_y,  y1=entry_y,
                  xref="x", yref="y", line=styles["entry"])
    fig.add_shape(type="line", x0=x0, x1=x1_pad, y0=stop_y,   y1=stop_y,
                  xref="x", yref="y", line=styles["stop"])
    fig.add_shape(type="line", x0=x0, x1=x1_pad, y0=target_y, y1=target_y,
                  xref="x", yref="y", line=styles["target"])

    # 라벨(배경/테두리 강화) - x는 오른쪽 바깥쪽에
    label_box = dict(
        showarrow=False,
        xref="x", yref="y",
        x=x1_pad,
        xanchor="left",
        yanchor="middle",
        bgcolor="rgba(20,20,20,0.85)",  # 다크 배경
        bordercolor="rgba(255,255,255,0.35)",
        borderwidth=1,
        font=dict(color="white", size=12),
        align="left",
    )

    fig.add_annotation(y=entry_y,  text=f"Entry {entry_y:,.0f}",  **label_box)
    fig.add_annotation(y=stop_y,   text=f"Stop  {stop_y:,.0f}",   **label_box)
    fig.add_annotation(y=target_y, text=f"Target {target_y:,.0f}", **label_box)

    # x축 범위도 라벨 공간 포함하도록 확장
    fig.update_xaxes(range=[x0, x1_pad])

fig.update_layout(
    xaxis_rangeslider_visible=False,
    height=600,
    yaxis=dict(tickformat=",")
)
st.plotly_chart(fig, use_container_width=True)

vol_fig = go.Figure()
vol_fig.add_trace(go.Bar(x=sub["date"], y=sub["volume"], name="Volume"))
vol_fig.update_layout(height=250, yaxis=dict(tickformat=","))
st.plotly_chart(vol_fig, use_container_width=True)