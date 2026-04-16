# PROTOCOL

## Data Rules
- `ticker` must stay zero-padded 6-digit text.
- `date` should become pandas datetime early.
- Daily file name: `krx_ohlcv_YYYYMMDD.csv`.
- Cache file name: `<market>_merged.parquet`.

## App Rules
- Flow stays `download -> merge -> load -> filter -> scan -> render`.
- Sidebar is the source of scan parameters.
- Register new strategies in `core/strategies/__init__.py`.

## Cache Rules
- Scan cache must vary on date, market, top-n, strategy, filter mode, and params.
- If cache metadata is suspicious or corrupted, rebuild instead of trusting it.

## Safety Rules
- Stop early on empty market data.
- Do not present blocked market-filter runs as valid picks.
