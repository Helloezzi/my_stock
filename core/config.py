# core/config.py
from pathlib import Path

APP_TITLE = "KOSPI Swing Viewer"

DATA_DIR = Path("data")
CSV_PREFIX = "kospi_top200_1y_daily"
CSV_PATTERN = f"{CSV_PREFIX}_*.csv"
CSV_FALLBACK = f"{CSV_PREFIX}.csv"  # (원하면 유지, 아니면 제거 가능)