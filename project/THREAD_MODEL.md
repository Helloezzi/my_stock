# THREAD_MODEL

## Reality
- This is effectively a single-process Streamlit app.
- One background worker may run daily download after market close.

## Coordination
- Locks live in `data/_locks/`.
- Locking is local-file based and best-effort only.

## Ownership
- File system: durable data and markers.
- `st.session_state`: per-session UI state.
- `st.cache_data`: memoized derived loads.

## Constraint
- Do not assume safe multi-instance or multi-writer behavior.
