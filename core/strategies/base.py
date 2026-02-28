# core/strategies/base.py
from __future__ import annotations
from dataclasses import dataclass
from abc import ABC, abstractmethod
import pandas as pd

@dataclass(frozen=True)
class ScanParams:
    tolerance: float = 0.03
    stop_lookback: int = 10
    stop_buffer: float = 0.005
    target_lookback: int = 20
    min_rr: float = 1.5
    ma5_up_days: int = 0           

class Strategy(ABC):
    key: str
    name: str

    @abstractmethod
    def scan(self, df: pd.DataFrame, params: ScanParams) -> pd.DataFrame:
        """Return columns must include at least: ticker, date, score (and any strategy-specific cols)."""
        raise NotImplementedError