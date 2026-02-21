# core/strategies/__init__.py
from .base import Strategy, ScanParams
from .pullback_rr import PullbackRRStrategy

def get_strategies() -> list[Strategy]:
    return [
        PullbackRRStrategy(),
        # 나중에 BreakoutStrategy(), MeanReversionStrategy() 추가
    ]