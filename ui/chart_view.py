from ui.chart_renderer import render_chart
from ui.position_view import render_position_sizing
from ui.search_view import render_naver_link, render_search_and_select


def render_chart_and_sizing_two_column(*, selected: str, sub, scan_levels, key_prefix: str, compact: bool = False):
    import streamlit as st

    col_chart, col_ps = st.columns([2.2, 1.0], gap="large")

    with col_ps:
        entry, stop, target = render_position_sizing(
            selected, sub, scan_levels,
            key_prefix=key_prefix,
        )

    with col_chart:
        render_chart(sub, entry, stop, target, compact=compact)

    return entry, stop, target
