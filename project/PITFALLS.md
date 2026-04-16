# PITFALLS

## High-Risk Code Areas
- `core/data_loader.py` is doing too much:
  - cache update
  - loading
  - fingerprinting
  - duplicated helper names
  - duplicated `update_merged_parquet_from_daily()` definitions
- There is at least one obviously broken/recursive-looking `load_data()` implementation and it does not appear to be part of the main app path.

## Naming Mismatch Risk
- Daily merge logic often looks for `ohlcv_*.csv`.
- Downloader writes `krx_ohlcv_YYYYMMDD.csv`.
- Some code paths use regex for `krx_ohlcv_*.csv`, others glob `ohlcv_*.csv`.
- This inconsistency can silently break cache refresh or fingerprint logic.

## Data Freshness Risk
- Streamlit cache depends on `daily_fingerprint()`.
- If fingerprint logic misses a newly written file because of naming mismatch, stale data can persist.

## External Dependency Risk
- Strategy `VolCompressionBreakoutStrategy` calls `pykrx` market-cap API during scan, not just during download.
- That makes scan behavior depend on live/external availability and can hurt performance or determinism.

## Encoding / Locale Risk
- Several Korean strings appear garbled in current file output, likely encoding-related.
- Be careful when editing user-facing labels or docs; keep file encoding consistent.

## Concurrency Risk
- Locking is file-based and simple.
- Fine for local single-instance use, but not safe enough for multi-process scheduling.

## Refactor Priorities
- Unify daily filename convention everywhere.
- Split `core/data_loader.py` by responsibility.
- Remove dead or broken legacy loader paths.
- Isolate external API calls from strategy scan path when possible.
