# DECISIONS

This file records working decisions so future implementation does not need to re-ask the same direction questions.

## Status
- Active
- Last updated: 2026-04-16

## Product Goal
- The app exists to support swing-trading stock selection.
- The user should be able to review selected candidates comfortably and use them as buy/sell reference.
- The same daily result should be easy to check from home, office, and especially mobile.

## Current Product Direction
- Keep Streamlit for now.
- Improve speed by changing the app into a lightweight viewer first.
- Revisit a lighter non-Streamlit UI later if Streamlit still feels too slow on mobile.

## Agreed Priorities
1. Automate daily market-data collection.
2. Precompute candidate picks after market close.
3. Save the daily result as a very small file.
4. On mobile, load only the daily result instead of the full market dataset.
5. Optionally send the same daily picks by email later.

## Architectural Decision
- Separate heavy computation from interactive viewing.
- Daily collection and scanning should happen before the user opens the app.
- The UI should prefer precomputed daily picks over full-market recomputation.
- Full-market loading should remain available as a secondary path, not the default mobile path.
- The detailed file and flow design for this is tracked in `project/TODAY_PICKS_DESIGN.md`.

## UI Direction
- Mobile-first speed matters more than showing every control on first load.
- The initial screen should show only the compact daily picks result.
- Expensive chart or full-data views should load only when needed.

## Automation Direction
- Daily data collection should be automated.
- End-of-day scanning should also be automated.
- The first automation target is local/NAS batch execution, not a new infrastructure layer.
- Optional email delivery should reuse the same published daily picks output.

## Deferred For Later
- Replacing Streamlit with a lighter web stack.
- Building a separate API server or database-backed architecture.

## Working Rule
- If a new task conflicts with this file, update this file first or explicitly note the override in the task.
