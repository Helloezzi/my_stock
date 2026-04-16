# ARCHITECTURE

## Layers
- `app.py`: page setup, tab routing, high-level orchestration.
- `core/`: data ingestion, cache update, universe build, scan execution.
- `ui/`: rendering only.

## Important Modules
- `core/app_runtime.py`: central runtime assembly.
- `core/data_files.py`: dataset file selection.
- `core/market_cache.py`: daily CSV to parquet merge/load.
- `core/data_fingerprint.py`: freshness key generation.
- `core/strategies/`: scan logic.

## UI Split
- `ui/sidebar.py`: controls.
- `ui/scanner_tab.py` and `ui/scanner_view.py`: scan results.
- `ui/chart_view.py` plus chart/search/position subviews: browse details.

## Architectural Rules
- Put business logic in `core/`.
- Keep `ui/` free of data-fetch decisions.
- Keep `app.py` thin.
- Prefer local parquet cache over raw daily CSV reads in interactive paths.
