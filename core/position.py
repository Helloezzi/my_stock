import math


def calc_position(
    capital: float,
    risk_pct: float,
    entry: float,
    stop: float,
    max_invest_pct: float = 1.0,
):
    if entry <= stop:
        return None

    risk_budget = capital * risk_pct
    per_share_risk = entry - stop

    qty = math.floor(risk_budget / per_share_risk)
    if qty <= 0:
        qty = 0

    invest = qty * entry

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