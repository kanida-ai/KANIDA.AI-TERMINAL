# Hand-back — Pathfinder integration polish (after S1 · S2 · S3)

**Date:** 2026-09-10 (IST) · **Branch:** `feat/product-build` (on `0e4e98b`) · **Scope:** the seams S3 flagged in
`docs/handbacks/PF-S3.md` §4 and the "For S3" additive fields of `docs/handbacks/PF-S2.md`. Small, precise, no
redesign. Nothing in `research/*.py` or `experiments/*.py` was touched, so both engine hashes are unchanged and
the served stores are still attributable to the code that built them.

## 1. What changed

### Backend (`backend/pathfinder/`)
- **Never a 500** (S3 §4.1). `/api/pathfinder/loop` and `/api/pathfinder/learnings` under
  `KANIDA_PATHFINDER_SOURCE=research` now answer a guarded **404 `not_served_by_source`** whose `error.use[]`
  names the served paths (`/feed`, `/experiments`, `/experiment/{id}`). `ErrorBody.use` is a new optional
  field. The mock app's `/` and `/healthz` — which also called `get_store()` and raised — describe the research
  source honestly instead. `store.get_store()` names the reason when it is reached with `research`.
- **Feed metadata** (S3 §4.5). `FeedResponse.engine_version` = the edition row's stamped
  `pathfinder_research@…+code.<hash>` (what computed it — a fact, not the running semver) plus
  `; pathfinder_experiments@…+code.<hash>` from the registry's edition row when cards ride on the edition;
  `FeedResponse.schema_version` = `pathfinder_feed@1.1.0+research_store.3+experiments_store.3`.
- **`due_session` on pending cards** (S3 §4.4). The engine could not stamp it because the horizon lies past the
  data seal (`MarketData.session_after` returns None — the session is not yet a fact). The API now serves a
  **labelled projection**: `pathfinder/sessions.py` projects weekdays after the edition net of the NSE 2025–2026
  closures (the OMS calendar's list, embedded — Pathfinder does not import `backend/autotrade/`), and
  `GradingState.due_session_basis` says `projected: …`; an engine-stamped date is labelled
  `session_calendar: …`. Only a **pending root** is projected — a continuation is graded through its root's
  horizon, whose base date is the root's edition — and the grade never reads the projected date
  (`research/scan.py` grades on the first session the data actually reaches). Graded cards are untouched.
- **Registry / scoreboard numbers stay `EngineFigure`s (S3 §4.2) — documented choice.** A `Fact` needs an
  n, a window and a cost convention that mean something. A scoreboard tally *is* its own n; a declined
  candidate's `best_expectancy_net_pct` does carry stats on the trial row (`pfx_trials.stats_json`) but
  serving them as Facts means widening `RejectedCandidate` with a fact list and provenance per row — a real
  contract change, not polish. Instead the declined candidate's expectancy `EngineFigure` sheet now states the population the
  number is on (book-selected, re-audit N1) and the record it came from. Left for a contract session.
- `docs/openapi.yaml` regenerated: **35 insertions, 0 deletions**; `gen_openapi.py --check` in sync.

### Docs
- `docs/DATA_MODEL.md`: the S2 registry is **schema 3** (`pfx_candidates.family_trials_all_time`,
  `pfx_meta.schema_version = 3`; schema 1/2 refused and archived); the candidate row's expectancy is the
  book-selected one.
- `docs/TEST_PLAN.md`: the integration-polish rows PL-01 … PL-15.

### App (`kanida-app/`)
- `src/api/types.ts` mirrors every additive field: `Expectation.population / signals_fired / signals_skipped /
  equal_weighted_expectancy_net_pct / equal_weighted_n / top3_days_share_pct /
  expectancy_without_best_day_net_pct / trailing_top3_days_share_pct /
  trailing_expectancy_without_best_day_net_pct / placebo_convention / placebo_se / cluster_t_kind`,
  `GateView.insufficient`, `ForwardResult.drawdown_convention`, `ExperimentCard.family_trials_all_time`,
  `RejectedCandidate.family_trials_all_time`, `FeedResponse.engine_version / schema_version`,
  `GradingState.due_session_basis`, `ApiErrorBody.error.use`.
- **`insufficient` is "not enough data"**, never FAIL: `gateCopy()` in `honesty.ts`; `GatesList` renders a
  neutral `·`, a *not enough data* pill and the sentence "not passed, not a measured failure".
- **Book-selected vs equal-weighted, honestly** on the record screen's expectation block: the frozen figure is
  labelled *Expectancy / trade · book-selected — on the trades the book would take*, the population line is
  printed, and the equal-weighted figure sits in a context row labelled **"context: all signals, untakeable by
  the book — not the expectation"** with its n and the signals fired · skipped. The concentration facts
  (top-3 days' share, without the best day, both trailing) have their own row. Placebo se / convention and
  the cluster-t kind ride as hints; the drawdown convention rides on every period.
- **`family_trials_all_time`** on the record header and on the trials ledger ("the count the family-wise bar
  divides by; never restarts"), on the experiment slide, and on every researched-and-declined candidate.
- `due_session` is consumed everywhere it was already shown, with **"(projected)"** appended when the basis
  says so (`GradingLine`, the "testing next" slide); the empty-edition story names `engine_version` as the
  source of its figures when the feed serves one.

## 2. How to run the whole stack

```bash
# API — research source (S1 feed + S2 experiments), the app's default base URL
cd backend && KANIDA_PATHFINDER_SOURCE=research python -m uvicorn pathfinder.mock_app:app --port 8010
#   PowerShell: $env:KANIDA_PATHFINDER_SOURCE="research"; C:\Users\SPS\anaconda3\python.exe -m uvicorn pathfinder.mock_app:app --port 8010
# App — web
cd kanida-app && npm install && npx expo start --web        # http://localhost:8081 → /pathfinder
# Gates
C:\Users\SPS\anaconda3\python.exe -m pytest backend/tests/test_pathfinder_s1*.py backend/tests/test_pathfinder_s2*.py backend/tests/test_pathfinder_polish.py -q
C:\Users\SPS\anaconda3\python.exe scripts/gen_openapi.py --check
cd kanida-app && npm run typecheck && npx expo lint && npm test && npm run test:contract && npx expo export --platform all
```

## 3. Results (final tree)

backend **300 passed** (294 + 6) · `gen_openapi.py --check` up to date · typecheck clean · lint 0 errors 0 warnings
· `npm test` **36 passed** · `npm run test:contract` **19 passed** against the live research API on `:8010`
(latest edition 2026-07-29: four pending cards now carry projected due sessions 30 Jul / 5 Aug 2026 labelled
`projected`; the 22 Jul edition's graded cards keep `session_calendar`) · `expo export --platform all` exit 0.

A stale `uvicorn pathfinder.mock_app:app --port 8010` from 13:09 IST (pre-polish code) was still listening on
the app's default port when this session started; it was stopped and replaced by this session's server, which
was stopped at the end. No store was written, archived or deleted.

## 4. Risks / next
- The projection covers NSE closures for 2025–2026 only; other years are weekday-only (the basis string says so).
  Once a same-day scan runs, the seal reaches the horizon before the grade and the engine's own date takes over.
- The experiment depth screen's new rows are exercised by types and unit rows; the served registry holds zero
  experiments, so — as in S3 — they have not been seen rendered on real data.
- Facts for the registry's numbers (§1, S3 §4.2) remain a contract-session item.
