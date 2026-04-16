# ARCHITECTURE

## Layering
- `app.py`
  - Orchestrates page setup, cache invalidation, tab routing, session state, and view composition.
- `core/`
  - Owns data ingestion, cache build/update, universe selection, market filter, scan cache, strategies, and auto-daily behavior.
- `ui/`
  - Pure-ish rendering layer for sidebar, scanner table, search/select, charts, and position sizing.

## Core Module Roles
- `core/config.py`
  - Defines app title and data root paths.
- `core/downloader_daily.py`
  - Pulls KRX OHLCV/cap snapshot, normalizes columns, writes atomic CSV.
- `core/data_loader.py`
  - Updates merged parquet caches, loads both markets, computes cache fingerprint.
- `core/universe.py`
  - Selects market DF and applies Top-N ranking filter.
- `core/market_filter.py`
  - Evaluates KOSPI regime conditions such as `close > MA20`.
- `core/scan_cache.py`
  - Persists scan results by signature.
- `core/auto_daily.py`
  - Runs daily download once asynchronously after market close.
- `core/strategies/`
  - Strategy registry plus implementations.

## UI Module Roles
- `ui/sidebar.py`
  - All user controls and scan params.
- `ui/scanner_view.py`
  - Scanner result table and ticker picker.
- `ui/chart_view.py`
  - Browse/search, Naver link, candlestick chart, volume chart, position sizing.

## Important Design Choices
- Data source of truth is local files under `data/`.
- App loads from merged parquet, not raw daily CSV directly.
- Streamlit cache + custom fingerprint are both used to avoid stale data.
- Scanner output is strategy-defined but expected to include `ticker`, `date`, `score`, and often `entry/stop/target/rr`.
