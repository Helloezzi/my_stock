# PITFALLS

## Watch Closely
- File naming mismatches can break freshness detection.
- Streamlit cache can hide stale data if fingerprint inputs are incomplete.
- File-based locks are not enough for multi-instance scheduling.
- Encoding issues can reintroduce broken Korean labels or docs.

## Already Improved
- Data loader responsibilities were split.
- Legacy files were moved under `legacy/`.
- Volatility breakout strategy was split into thin entry plus helper module.
- Live market-cap dependency was removed from the hot scan path.

## Still Worth Checking
- Any new downloader path should match the existing daily filename convention.
- Any new UI feature should avoid pulling heavy data or network calls during render.
