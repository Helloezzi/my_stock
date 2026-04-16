# PROTOCOL

## Change Protocol
- Prefer preserving the current flow: `download daily -> merge parquet -> load universe -> scan -> render`.
- Keep new business logic in `core/`, not `ui/`.
- Keep rendering logic in `ui/`, not `app.py`.
- If adding a strategy, register it in `core/strategies/__init__.py`.

## Data Protocol
- Normalize `ticker` to zero-padded 6-digit string.
- Normalize `date` to pandas datetime as early as possible.
- Daily file naming currently expects `krx_ohlcv_YYYYMMDD.csv`.
- Parquet cache naming currently expects `<market>_merged.parquet`.

## Scan Protocol
- Build scan cache key from:
  - latest date
  - market
  - top_n
  - strategy label
  - market filter mode
  - strategy params
- If any of the above changes, treat scan results as invalidated.

## UI/State Protocol
- Sidebar is the only source of user scan parameters.
- `st.session_state` owns selected ticker, active tab, scan signature, and scan results.
- Scanner and Browse tabs maintain separate selected tickers.

## Safety Protocol
- If market data is empty, stop early with warning.
- If KOSPI regime filter blocks, do not run or display fresh scanner results as valid picks.
- If scan cache or levels JSON is corrupted, delete and rebuild it.
