import streamlit as st

from core.links import naver_stock_url
from ui.texts import t


def render_search_and_select(
    tickers,
    name_map,
    state_key="selected_browse_ticker",
    title=None,
):
    st.subheader(title or t("search.title_browse", market_label="ALL"))

    query = st.text_input(
        t("search.input"),
        value="",
        key=f"{state_key}_search",
    ).strip()

    filtered = []
    for ticker in tickers:
        name = name_map.get(ticker, ticker)
        if not query or query.lower() in ticker.lower() or query in str(name):
            filtered.append(ticker)

    if not filtered:
        st.warning(t("search.no_match"))
        return None

    current = st.session_state.get(state_key, filtered[0])
    if current not in filtered:
        current = filtered[0]
        st.session_state[state_key] = current

    selected = st.selectbox(
        t("search.select_ticker"),
        options=filtered,
        index=filtered.index(current),
        format_func=lambda x: f"{x} - {name_map.get(x, x)}",
        key=f"{state_key}_selectbox",
    )

    st.session_state[state_key] = selected
    return selected


def render_naver_link(ticker: str) -> None:
    st.link_button(
        label=t("search.naver_link"),
        url=naver_stock_url(ticker),
        help=t("search.naver_help"),
    )
