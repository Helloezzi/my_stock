# RUNTIME_FLOW

## Main Flow
1. `app.py` initializes page and session.
2. Optional daily auto-download may start after close.
3. `daily_fingerprint()` captures freshness state.
4. Runtime loads `kospi` and `kosdaq` parquet caches.
5. Sidebar defines market, universe scope, strategy, and params.
6. App builds universe, reuses or computes scan results, then renders UI.

## Data Flow
1. Downloader writes one daily CSV per market.
2. Cache loader merges unseen daily files into parquet.
3. Interactive views read parquet-backed data.

## Scan Flow
1. Strategy receives filtered market frame plus `ScanParams`.
2. Results are cached by signature.
3. Selected ticker drives chart and position view.
