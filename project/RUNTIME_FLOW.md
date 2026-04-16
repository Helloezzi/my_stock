# RUNTIME_FLOW

## App Flow
1. `app.py` sets Streamlit page config and title.
2. After `16:20`, app may trigger one async daily download for the day.
3. `daily_fingerprint()` is computed.
4. `load_buffers(fp)` calls `load_all_markets()`.
5. `load_all_markets()` updates parquet cache if needed, then loads `kospi` and `kosdaq`.
6. Sidebar returns tab, market, top-n, strategy, and params.
7. `build_universe()` filters the selected market DF.
8. App computes or reuses scan result depending on scan signature.
9. If market filter passes, results are shown and ticker can be selected.
10. Selected ticker is rendered with chart, volume, and position sizing.

## Daily Data Flow
1. `download_daily_all()` fetches one-day market snapshot per market.
2. Each market writes `data/daily/<market>/krx_ohlcv_YYYYMMDD.csv`.
3. Loader merges unseen daily files into `data/cache/<market>_merged.parquet`.
4. App reads merged parquet for interactive use.

## Scanner Flow
1. Strategy list comes from `get_strategies()`.
2. Selected strategy receives filtered market DF plus `ScanParams`.
3. Strategy returns scored candidates.
4. App extracts levels into `scan_levels`.
5. Results and levels are cached by signature.

## Browse Flow
1. User searches ticker/name in current universe.
2. App filters locally in memory.
3. Selected ticker reuses the same chart and position sizing components.
