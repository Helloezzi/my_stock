# core/strategies/__init__.py
from .base import Strategy, ScanParams
from .pullback_rr import PullbackRRStrategy
from .vol_compression_breakout import VolCompressionBreakoutStrategy

def get_strategies() -> list[Strategy]:
    return [
        PullbackRRStrategy(),
        VolCompressionBreakoutStrategy(),
    ]