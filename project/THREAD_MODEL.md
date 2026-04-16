# THREAD_MODEL

## Runtime Concurrency
- The app is effectively single-user and request-driven inside Streamlit.
- There is one explicit background thread: `core.auto_daily.try_run_daily_once_async()`.

## Background Thread Contract
- Trigger condition in `app.py`: local time is after `16:20`.
- Behavior:
  - create lock file for today
  - start daemon thread
  - run `download_daily_all()`
  - if all markets succeed, write `.done`
  - always remove `.lock`

## Coordination Mechanism
- File-based lock model under `data/_locks/`.
- This is a best-effort process-local coordination scheme, not a distributed lock.

## State Ownership
- File system owns durable state.
- `st.session_state` owns per-session UI state.
- `st.cache_data` owns memoized load results keyed by fingerprint.

## Implications
- Safe enough for one local app instance.
- Not designed for multiple writers, multiple app replicas, or concurrent scheduled jobs.
