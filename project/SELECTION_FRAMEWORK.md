# SELECTION_FRAMEWORK

## Goal
- Do not optimize for many picks.
- Optimize for a very small number of high-conviction swing candidates.
- It is acceptable to have `no-pick` days.

## Target Output Policy
- `A`: 0 to 2 names
- `B`: 0 to 3 names
- `Watch`: optional observation only
- Total should usually stay under 5

## Core Problem With Current Version
- Chart-only ranking is not enough.
- Rank 1 is not always the best trade.
- Too many candidates reduce actual usability.
- The system should help decide what to ignore, not just what to watch.

## New Direction
- Current technical scan becomes a `candidate finder`, not the final selector.
- Final selection should use multiple filters.
- The system should explain why a pick survived and why others were rejected.

## 3-Stage Model
1. Technical scan
2. Quality filters
3. Final selection and labeling

## Stage 1: Technical Scan
- Purpose:
  - find technically interesting candidates
- Current base:
  - `pullback_rr`
- Role:
  - broad first-pass filter
  - not the final buy decision

## Stage 2: Quality Filters
- Apply hard filters and soft scoring after technical scan.

### Technical Quality
- cleaner pullback structure
- acceptable volatility
- reasonable stop distance
- no unstable breakdown-like pattern

### Financial Quality
- avoid weak balance-sheet names
- avoid persistent loss / fragile business cases
- prefer stable earnings or improving fundamentals

### Sector Strength
- prefer names inside active sectors
- prefer group movement over isolated moves
- track whether the sector is currently receiving attention

### Industry Quality
- distinguish structural growth vs declining industries
- penalize likely sunset industries unless setup is exceptional

### AI Support
- use AI for summary and explanation
- do not use AI as a direct buy/sell oracle
- good uses:
  - news summary
  - sector summary
  - company issue summary
  - explanation of why the name passed

## Selection Logic
- `Hard Filter`
  - fail => immediate reject
- `Soft Score`
  - compare survivors
- `Final Label`
  - `A`, `B`, `Watch`, or reject

## Output Fields To Add Later
- `technical_score`
- `financial_score`
- `sector_score`
- `industry_score`
- `confidence`
- `risk_flags`
- `why_selected`
- `why_rejected`
- `action_label`

## Operating Principle
- Better to return zero names than weak names.
- Fewer but clearer candidates are more useful than longer ranked lists.
- Explanations matter as much as scores.

## Incremental Plan
1. Redefine current strategy as candidate finder only.
2. Add stricter final labeling and pick-count limits.
3. Add richer technical explanation fields.
4. Add financial filters.
5. Add sector and industry scoring.
6. Add AI-assisted summaries.

## Next Step
- First implementation slice:
  - keep `pullback_rr`
  - tighten final selection
  - allow `no-pick`
  - introduce `A/B/Watch`
  - reduce output count aggressively
