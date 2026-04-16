# PROJECT

## One-Line Summary
- Streamlit app for KOSPI/KOSDAQ daily-data scanning, chart review, and position sizing.

## Read This First
- `app.py`: app entry.
- `core/app_runtime.py`: load data and build runtime bundle.
- `core/data_loader.py`: stable import surface for data loading helpers.
- `core/strategies/`: strategy registry and implementations.
- `ui/sidebar.py`: main control surface.

## Current Data Path
- Download daily snapshots into `data/daily/<market>/`.
- Merge unseen files into `data/cache/<market>_merged.parquet`.
- Load parquet caches into memory.
- Build universe, run strategy, render results.

## Non-Goals
- No database.
- No API server.
- No multi-user coordination.
- No distributed scheduler.
