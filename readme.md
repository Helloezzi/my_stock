# my_stock

Developer-focused KOSPI/KOSDAQ daily swing-scanning app built with Streamlit.

## What This Repo Does
- Downloads or backfills daily market data.
- Merges daily CSV snapshots into parquet caches.
- Builds a market universe for KOSPI or KOSDAQ.
- Runs strategy-based scans on cached data.
- Shows chart, levels, and position sizing in a Streamlit UI.

## Main Entry Points
- `app.py`
  Main Streamlit entry.
- `core/app_runtime.py`
  Loads market buffers and initializes runtime/session defaults.
- `core/data_loader.py`
  Stable import surface for data helpers.
- `download_daily_fdr.py`
  Backfill script using FinanceDataReader NAVER source.
- `deploy_nas.bat`
  Windows helper for NAS deploy, restart, logs, and backfill.
- `project/NAS_DOCKER.md`
  NAS deployment notes.

## Project Layout
```text
my_stock/
  app.py
  download_daily_fdr.py
  deploy_nas.bat
  run.bat
  core/
    app_runtime.py
    data_files.py
    data_fingerprint.py
    data_loader.py
    downloader_daily.py
    market_cache.py
    scan_cache.py
    universe.py
    strategies/
  ui/
    sidebar.py
    scanner_tab.py
    scanner_view.py
    selection_view.py
    search_view.py
    chart_view.py
    chart_renderer.py
    position_view.py
  data/
    daily/
    cache/
    scan_cache/
    _locks/
  scripts/
    data_check.py
  project/
    NAS_DOCKER.md
  legacy/
```

## Root Folder Rule
- Keep only real runtime entry points at repo root.
- Put reference docs under `project/`.
- Put one-off developer helper scripts under `scripts/`.
- Keep inactive or historical code under `legacy/`.

## Local Development
### 1. Install
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Run
```powershell
streamlit run app.py
```

App default URL:
- `http://localhost:8501`

## Docker Run
```powershell
docker compose up --build
```

Exposed port:
- `8501`

Volume mapping:
- local `./data` -> container `/app/data`

## Data Flow
1. A downloader writes daily snapshots into `data/daily/<market>/`.
2. The loader merges unseen files into `data/cache/<market>_merged.parquet`.
3. The app loads parquet caches into memory.
4. A universe is built from the selected market and optional Top-N filter.
5. A strategy scan runs and stores reusable scan cache output.
6. UI renders candidates and the selected chart view.

## Data Conventions
- Ticker format: zero-padded 6-digit string.
- Daily file format: `krx_ohlcv_YYYYMMDD.csv`.
- Cache file format: `<market>_merged.parquet`.
- Supported markets in current app flow: `kospi`, `kosdaq`.

## Key Modules
### `core/`
- `downloader_daily.py`
  One-day downloader path.
- `market_cache.py`
  Daily CSV merge/load logic.
- `data_fingerprint.py`
  Freshness key used to invalidate Streamlit cache.
- `universe.py`
  Market and Top-N universe filtering.
- `scan_cache.py`
  Persistent scan-result caching.
- `strategies/`
  Strategy registry and scan implementations.

### `ui/`
- `sidebar.py`
  User controls and scan parameters.
- `scanner_tab.py`
  Scanner tab orchestration.
- `scanner_view.py`
  Result table rendering.
- `selection_view.py`
  Browse flow and selected ticker coordination.
- `chart_renderer.py`
  Candlestick and related chart rendering.
- `position_view.py`
  Position-sizing view.

## NAS Deployment
Common commands:

```powershell
.\deploy_nas.bat deploy
.\deploy_nas.bat fast
.\deploy_nas.bat full
.\deploy_nas.bat fastfull
.\deploy_nas.bat status
.\deploy_nas.bat logs
.\deploy_nas.bat backfill
```

Current batch defaults are defined inside `deploy_nas.bat`:
- NAS host
- SSH port
- remote deploy directory
- docker binary path
- backfill date range

Detailed NAS notes:
- `project/NAS_DOCKER.md`

## Development Rules
- Keep business logic in `core/`.
- Keep rendering logic in `ui/`.
- Keep `app.py` thin and orchestration-oriented.
- Register new strategies in `core/strategies/__init__.py`.
- Prefer reading from parquet cache in interactive paths.
- Treat `legacy/` as reference-only, not active runtime code.

## Token-Saving Notes
- Codex should prefer `project/*.md` for quick repo context.
- `.codexignore` excludes generated data, logs, and legacy code.
- Large inactive scripts were moved to `legacy/`.

## Troubleshooting
- If UI shows stale data, clear Streamlit cache and confirm latest daily files exist.
- If parquet cache is stale, verify filename convention matches `krx_ohlcv_YYYYMMDD.csv`.
- If NAS deploy succeeds but data is old, run a backfill step separately or use `full` / `fastfull`.
- If mobile is slow, prefer lightweight mode and avoid unnecessary rebuilds during deploy.
