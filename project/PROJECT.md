# PROJECT

## Purpose
- This project is a Streamlit-based KOSPI/KOSDAQ swing trading viewer and scanner.
- It downloads daily KRX OHLCV data, merges it into parquet caches, scans with strategy rules, and lets the user inspect charts plus position sizing.

## Primary Entry Points
- `app.py`: main Streamlit app.
- `core/downloader_daily.py`: one-day KRX downloader for KOSPI/KOSDAQ.
- `core/data_loader.py`: daily CSV -> merged parquet cache update and load.
- `core/strategies/`: scanner strategy implementations.

## Main User Features
- Switch market between `KOSPI` and `KOSDAQ`.
- Optionally limit universe by Top-N market cap snapshot.
- Run scanner with strategy-specific filters.
- Apply KOSPI market regime filter before scan.
- Browse tickers and inspect chart, levels, and position sizing.

## Data Layout
- `data/daily/<market>/krx_ohlcv_YYYYMMDD.csv`: raw daily snapshots.
- `data/cache/<market>_merged.parquet`: merged history cache used by app.
- `data/scan_cache/`: cached scan results and level JSON.
- `data/_locks/`: daily auto-run lock and done markers.

## Dependencies
- UI: `streamlit`, `plotly`
- Data: `pandas`, `pyarrow`
- Market sources: `pykrx`, `yfinance`, `requests`, `bs4`, `lxml`

## Current Shape
- Single-process Streamlit app.
- Local-file-based cache and lock model.
- No DB, queue, API server, or auth layer.
