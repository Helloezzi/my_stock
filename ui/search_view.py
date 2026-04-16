import streamlit as st

from core.links import naver_stock_url


def render_search_and_select(
    tickers,
    name_map,
    state_key="selected_browse_ticker",
    title="Search Top200",
):
    st.subheader(title)

    query = st.text_input(
        "Search (Ticker or Name)",
        value="",
        key=f"{state_key}_search",
    ).strip()

    filtered = []
    for ticker in tickers:
        name = name_map.get(ticker, ticker)
        if not query or query.lower() in ticker.lower() or query in str(name):
            filtered.append(ticker)

    if not filtered:
        st.warning("No matching results.")
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
        label="Open Naver Stock Page",
        url=naver_stock_url(ticker),
        help="Open the selected stock in Naver Finance",
    )
