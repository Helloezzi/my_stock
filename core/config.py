# core/config.py
from pathlib import Path

APP_TITLE = "KOSPI Swing Viewer"

BASE_DIR = Path(__file__).resolve().parent.parent  # 프로젝트 루트 기준
DATA_DIR = BASE_DIR / "data"

CSV_PREFIX = "kospi_top200_1y_daily"
CSV_PATTERN = f"{CSV_PREFIX}_*.csv"
CSV_FALLBACK = f"{CSV_PREFIX}.csv"  # (원하면 유지, 아니면 제거 가능)