# PROJECT

## One-Line Summary
- Streamlit app for KOSPI/KOSDAQ daily-data scanning, chart review, and position sizing.

## Product Goal
- Support swing-trading stock selection and quick decision review for buy/sell reference.
- Make the same daily result easy to check from home or office, especially on mobile.

## Current Direction
- Priority 1: keep Streamlit for now, but shift the app toward a lightweight viewer.
- Priority 2: consider a lighter non-Streamlit web UI later if more speed is needed.

## Operating Principles
- Automate daily market-data collection.
- Precompute candidate picks after market close.
- Save the daily result as a very small file.
- On mobile, load only the daily result instead of the full market dataset.
- Optionally send the same daily picks by email.

## Read This First
- `project/DECISIONS.md`: agreed product and architecture direction.
- `project/DAILY_CHECKLIST.md`: daily operations verification checklist.
- `project/SSH_ACCESS.md`: NAS SSH access and common commands.
- `app.py`: app entry.
- `core/app_runtime.py`: load data and build runtime bundle.
- `core/data_loader.py`: stable import surface for data loading helpers.
- `core/strategies/`: strategy registry and implementations.
- `ui/sidebar.py`: main control surface.

## Current Data Path
- Download daily snapshots into `data/daily/<market>/`.
- Merge unseen files into `data/cache/<market>_merged.parquet`.
- Load parquet caches into memory.
- Build universe, run strategy, render results.

## Target Data Path
- Collect daily data automatically.
- Run end-of-day scan and compute daily picks in advance.
- Publish a compact daily output such as JSON/CSV for UI use.
- Let the mobile UI read the compact daily output first.
- Keep full-market loading as a secondary or desktop-oriented path.

## Non-Goals
- No database.
- No API server.
- No multi-user coordination.
- No distributed scheduler.
