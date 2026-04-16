from ui.chart_renderer import render_chart
from ui.position_view import render_risk_settings_and_summary, render_trade_levels


def render_chart_and_sizing_two_column(*, selected: str, sub, scan_levels, key_prefix: str, compact: bool = False):
    entry, stop, target = render_trade_levels(
        selected,
        sub,
        scan_levels,
        key_prefix=key_prefix,
    )
    render_chart(sub, entry, stop, target, compact=compact)
    render_risk_settings_and_summary(
        selected,
        entry,
        stop,
        target,
        key_prefix=key_prefix,
    )

    return entry, stop, target
