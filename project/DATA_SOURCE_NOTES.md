# DATA_SOURCE_NOTES

## Purpose
- Record practical decisions about data sources.
- Avoid retrying unstable approaches repeatedly.

## Current Decisions

### Daily Price Data
- Current practical path:
  - `download_daily_fdr.py`
- Reason:
  - worked more reliably than the old KRX-key flow for the current setup

### pykrx Usage
- `pykrx` should not be relied on for financial/fundamental data in this project.
- Reason:
  - recent behavior has been unstable in our environment
  - some functions returned unexpected errors or inconsistent results
- Current rule:
  - do not spend more implementation time trying to restore `pykrx` fundamentals
  - use other sources for financial snapshots instead

### Financial Snapshot Source
- Current practical path:
  - Naver Finance item page parsing
- Current fields:
  - `PER`
  - `EPS`
  - `PBR`
  - `BPS`
  - `dividend_yield`
  - `industry_per`
- Reason:
  - lightweight
  - works for a small number of selected candidates
  - fits the current post-scan enrichment flow

## Operational Rule
- Prefer stable and boring sources over theoretically better but unstable ones.
- If a source repeatedly fails in real operation, document it here and move on.

## Revisit Conditions
- Revisit `pykrx` only if:
  - there is a clear verified fix
  - it works consistently in our actual environment
  - it provides a real advantage over the current lightweight path
