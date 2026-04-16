# DAILY_CHECKLIST

## Purpose
- Verify that the daily data pipeline actually produced fresh outputs.
- Confirm that the mobile-first published picks flow is ready before troubleshooting the UI.

## What To Check First
1. Daily market files were updated.
2. Parquet cache latest dates are fresh enough.
3. `today_picks.json` was regenerated.
4. Published picks contain expected markets and non-broken summary fields.
5. The app shows the same result on the lightweight scanner screen.

## Fast Command
```powershell
python scripts/check_daily_outputs.py
```

## Manual Checklist
### Data Files
- `data/daily/kospi/` contains a recent `krx_ohlcv_YYYYMMDD.csv`
- `data/daily/kosdaq/` contains a recent `krx_ohlcv_YYYYMMDD.csv`

### Cache Freshness
- `data/cache/kospi_merged.parquet` max date matches or closely follows the recent daily file date
- `data/cache/kosdaq_merged.parquet` max date matches or closely follows the recent daily file date

### Published Picks
- `data/picks/today_picks.json` exists
- `generated_at` looks recent
- `trade_date` is correct
- `source.markets` includes the expected market set
- `summary.pick_count` is not unexpectedly missing
- `summary.per_market_counts` is present when publishing multiple markets

### App Behavior
- Open the app in lightweight scanner mode
- `Scanner source` is `Published daily picks`
- `Market view` can switch between `ALL`, `KOSPI`, and `KOSDAQ`
- First screen loads quickly
- Selected ticker opens detail view when needed

## When Something Looks Wrong
- If daily files are old:
  - check downloader or schedule first
- If daily files are fresh but cache dates are old:
  - check cache merge logic next
- If cache dates are fresh but `today_picks.json` is old:
  - check the publish step
- If `today_picks.json` is fresh but app still shows old data:
  - clear Streamlit cache and reload

## Minimum Success Condition
- Recent daily files exist for both markets
- `today_picks.json` exists and parses cleanly
- Mobile lightweight screen shows the same picks without full-scan delay
