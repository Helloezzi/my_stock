from __future__ import annotations

from pathlib import Path

import streamlit as st

from core.config import DATA_DIR

ACTIVE_KEY = "selected_csv_name"


def get_active_csv_path() -> Path | None:
    name = st.session_state.get(ACTIVE_KEY)
    if not name:
        return None

    csv_path = DATA_DIR / name
    parquet_path = DATA_DIR / name.replace(".csv", ".parquet")

    if csv_path.exists() and parquet_path.exists():
        newest = max((csv_path, parquet_path), key=lambda path: path.stat().st_mtime)
        return newest
    if parquet_path.exists():
        return parquet_path
    if csv_path.exists():
        return csv_path
    return None


def list_dataset_files() -> list[Path]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(DATA_DIR.glob("*.parquet"), key=lambda path: path.stat().st_mtime, reverse=True)
    csv_files = sorted(DATA_DIR.glob("*.csv"), key=lambda path: path.stat().st_mtime, reverse=True)
    csv_files = [path for path in csv_files if not path.name.startswith("_tmp_")]

    parquet_stems = {path.stem for path in parquet_files}
    csv_files = [path for path in csv_files if path.stem not in parquet_stems]
    return parquet_files + csv_files
