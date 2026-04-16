# TODAY_PICKS_DESIGN

## Purpose
- Speed up mobile usage by avoiding full-market load and full scan on first app open.
- Turn the app into a viewer-first flow for the default daily use case.
- Keep heavy computation in an automated end-of-day step.

## Why This Is Separate From Scan Cache
- `scan_cache` is parameter-dependent and created on demand inside the app.
- `today picks` is a precomputed published result for the default daily workflow.
- `scan_cache` helps repeated interactive scans.
- `today picks` should help fast mobile opening even before any interaction.

## Default User Flow We Are Designing For
1. Daily data is collected automatically after market close.
2. The app precomputes a default scan result.
3. A small `today picks` output is written to disk.
4. Mobile opens the app and reads only `today picks` first.
5. Full-market data loading becomes optional or secondary.

## Proposed Output Files
- Primary latest file:
  - `data/picks/today_picks.json`
- Historical archive file:
  - `data/picks/history/today_picks_YYYYMMDD.json`
- Optional export for inspection:
  - `data/picks/history/today_picks_YYYYMMDD.csv`

## Proposed JSON Shape
```json
{
  "schema_version": 1,
  "generated_at": "2026-04-16T18:05:00+09:00",
  "trade_date": "2026-04-16",
  "source": {
    "markets": ["KOSPI", "KOSDAQ"],
    "latest_data_date": "2026-04-16",
    "market_filter_mode": "close_above_ma20",
    "top_n": null,
    "strategy_key": "vol_compression_breakout",
    "strategy_label": "Vol Compression -> Breakout (Watch + Confirm + VolSurge)"
  },
  "summary": {
    "market_ok": true,
    "market_msg": "OK",
    "pick_count": 8
  },
  "picks": [
    {
      "rank": 1,
      "ticker": "005930",
      "name": "Samsung Electronics",
      "market": "KOSPI",
      "date": "2026-04-16",
      "stage": "BREAKOUT",
      "score": 91.2,
      "entry": 84500,
      "stop": 81200,
      "target": 91000,
      "rr": 1.97,
      "close": 84500,
      "volume": 12345678
    }
  ]
}
```

## Minimum Required Fields
- Top-level:
  - `schema_version`
  - `generated_at`
  - `trade_date`
  - `source`
  - `summary`
  - `picks`
- Per pick:
  - `rank`
  - `ticker`
  - `name`
  - `market`
  - `date`
  - `stage`
  - `score`
  - `entry`
  - `stop`
  - `target`
  - `rr`

## File Size Rule
- The file should stay small enough for fast mobile load.
- Only include fields needed for the default first screen.
- Do not embed full OHLCV history in `today_picks.json`.
- Do not store all scan variants in this file.

## Generation Rule
- Use one default strategy and one default market-filter mode for publication.
- Use the current agreed default workflow, not every possible UI option.
- The output should be deterministic for the same input data and same config.

## Recommended First Published Config
- Market scope:
  - `KOSPI` and `KOSDAQ`
- Top-N:
  - `ALL`
- Strategy:
  - start with the strategy that reliably produces daily candidates
  - current practical default: `pullback_rr`
- Market filter:
  - keep the current KOSPI market filter rule
- Output count:
  - publish only top 10 to 20 picks

## App Integration Plan
### Phase 1
- Add a loader for `today_picks.json`.
- If the file exists, mobile or lightweight mode should show it first.
- Do not load full parquet buffers on the first lightweight screen.

### Phase 2
- Keep a manual or desktop path for full scan and exploration.
- Allow the user to open a selected ticker detail view from `today picks`.

### Phase 3
- Add optional email delivery using the same published result.

## Suggested New Modules
- `core/published_picks.py`
  - read/write `today picks` files
- `scripts/build_today_picks.py` or `core/auto_daily.py` extension
  - build the published output after data update
- optional UI module:
  - `ui/today_picks_view.py`

## Failure Behavior
- If `today_picks.json` is missing:
  - fall back to the current full app path
- If the file is corrupt:
  - ignore it, warn, and fall back
- If market filter blocks:
  - write a valid file with `market_ok=false` and empty `picks`

## Non-Goals For First Version
- Real-time intraday refresh
- Supporting every strategy and every sidebar combination
- Replacing the full app immediately
- Adding a database or API layer

## Next Implementation Slice
1. Define read/write helper for published picks.
2. Define default publication config in one place.
3. Build one manual generation command first.
4. Add a lightweight first screen that reads the published file.
5. Automate generation after the manual path is stable.
