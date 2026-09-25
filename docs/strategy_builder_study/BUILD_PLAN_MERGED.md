# Strategy Builder — merged build plan

This plan merges two blueprints:
- **A** = mine: `docs/strategy_builder_study/KANIDA_STRATEGY_BUILDER_BLUEPRINT.md`.
- **B** = the other agent's: `research/strategy-builder-2026-09-24/06-kanida-blueprint.md`.

**Target:** `kanida-app`, the Expo web pilot on :8082. Owner confirmation is pending.

## 1. How the two blueprints were reconciled

| Topic | A says | B says | Merged decision |
|---|---|---|---|
| Core object | One Strategy with a status | Strategy + immutable revisions; Research/Paper/Live are *derived views*; deployments are separate | **B.** A strategy can have many revisions and many paper/live deployments, so a single status field would mislead |
| Save | Autosave versions | Autosaved draft + named immutable snapshots + optimistic concurrency (409) | **B**, plus A's "snapshot reference per version" |
| Leg cap | 10 | 8 (analytics capacity) | **8** in slice 1 |
| First-release scope | Calendars and futures in the library | Same-expiry, index options only; multi-expiry blocked until the analytics are validated | **B.** Advanced templates stay visible but disabled with a reason |
| Metrics | Provenance tag on every number | Per-metric `status` (available / unavailable / stale / unsupported) + unit + basis; never a zero placeholder | **Both**: `{status, value, unit, basis, source, as_of_ist}` |
| Entry price basis | LTP / Mid / Custom | Buy at ask, sell at bid by default; mid/LTP/manual labelled | **B** default; falls back to LTP labelled "LTP (no bid/ask in snapshot)" |
| Discovery ranking | Backtested ETV 95% low + FDR; "Model only" otherwise | "Matches your constraints", 3–6 candidates, no evidence ranking until data exists | **B now, A later.** Slice 3 ranks by constraint fit with a "Model only" label; the evidence badge switches on once the Lab has OOS n ≥ 30 |
| Execution | Intents → `backend/autotrade`; paper default; cert + armed | Preview with hash and expiry; idempotent intent; outcome-unknown reconciliation; no blind resubmit | **Both.** B's preview/intent state machine runs inside A's autotrade boundary |
| Backtest | Captured + model-priced replay, labelled | Separate Replay from Rule backtest; no silent forward-fill; coverage gates | **Both.** Model-priced results are allowed only with the label; B's coverage gate applies |
| Alerts | Strategy-level types + Adjust deep-link | Notify only; crossing, debounce and re-arm semantics; automation needs separate authorisation | **Both** |
| Timezone | Render in IST | Store expiry as an exchange-local date, never derived from UTC midnight | **Both** (a hard test in 3 browser timezones) |

Extra findings from B that I didn't have, now folded in:
- A Sensibull saved definition reopened with blank expiry and zero strikes. So: keep the original terms and offer "Repair as new draft".
- Loading states in the competitors looked like empty data. So: separate skeleton and empty states.
- Rupeezy has a portfolio-level intraday P&L auto-exit. So: alert scope is explicit, and automation is separate.

## 2. Slices (each one ships working)

| Slice | Scope | Needs broker? |
|---|---|---|
| **1 · Research workspace** | My Strategies list · Start (scratch / template) · **Builder** (legs, chain drawer, adjusters, recognition) · **Analyze** (risk strip, payoff expiry + target-date, P&L table, Greeks, SD) · **Scenario** (spot / time / IV) · autosave + named snapshots + duplicate/archive | No |
| 2 · Paper | Paper runs (bid/ask fill policy, costs, MTM, exits), activity timeline | No |
| 3 · Discover | Thesis + budget/max-loss → 3–6 explained candidates, compare | No |
| 4 · Review → autotrade (paper intent first) | Preview hash/expiry, pre-trade checks, sequencing, idempotent intent | Paper only |
| 5 · Alerts + Monitor | Strategy-level rules, notify only | No |
| 6 · Lab | Replay, then rule backtest (PIT, costs, OOS) | No |
| 7 · Live | Adapter `basket_margin`, live intents | **Yes** (after cutover, human-gated) |

## 3. Slice 1 — what gets built

**Server** (FastAPI, `kanida-app/server/kanida_pilot/strategy_builder/`, a new package that is mounted guarded):
- `instruments.py`: expiries, strikes, lot size and tick size from `derivatives.db` `contracts`. Expiry is a date string exactly as stored, IST.
- `chain.py`: the latest snapshot per contract (bid/ask/LTP/OI, captured_at), with quote-quality flags (missing bid/ask, age).
- `analytics.py`: a pure module.
  - Signed units; expiry payoff (exact breakpoints, tails → Unlimited); roots.
  - BSM target-date value using the existing `implied_vol.py` solver.
  - Greeks with explicit units; model POP; SD.
  - Per-metric status.
- `store.py` + tables: `sb_strategies`, `sb_drafts` (version token), `sb_revisions` (immutable, checksum), in the pilot DB.
- Routes under `/api/pilot/sb/*`: `instruments`, `chain`, `templates`, `strategies` (CRUD), `draft` PATCH with 409, `revisions`, `analyze`.
- Tests: golden payoffs (long call, vertical, straddle, iron condor, asymmetric wings); a timezone test.

**Web** (`kanida-app/src/strategyBuilder/`, route `/strategies`):
- Reuses the workbench tokens and theme. The analytics are mirrored in TS for instant scenario feedback; the server result wins when they disagree.

**Layout (desktop):**
```
┌ NIFTY 23,063.10 · as of Tue 23 Sep 15:45 IST (stored snapshot) · Expiry 29 Sep (W) 4 DTE ▾ ┐
│ ◀ My Strategies  "Untitled NIFTY strategy" ✎  Draft v3 · saved ✓   [Save snapshot] [Duplicate]│
├────────────── Legs (45%) ─────────────────┬──────────── Analyze (55%) ─────────────────────┤
│ Recognised: Bull Call Spread               │ Max loss −5,899 · Max profit +7,101 ·           │
│ ✓ B 29-Sep 23050 CE  −1 lot+  ask 152.55 ⋯ │ Breakeven 23,141 · Funds ~₹39.9k (est.)         │
│ ✓ S 29-Sep 23250 CE  −1 lot+  bid  61.80 ⋯ │ [payoff: expiry solid · target dashed · ±σ]     │
│ [+ Add from chain] [Template ▾]            │ Scenario: NIFTY [23,300] on [Mon 28 Sep 15:30]  │
│ Shift −|+  Width −|+  Wings −|+  ×[1]      │           IV [+0]  [Reset]                      │
│ ⚠ no bid/ask in snapshot → LTP basis       │ Tabs: P&L · Greeks · Payoff table               │
└────────────────────────────────────────────┴─────────────────────────────────────────────────┘
```
**Mobile:** risk strip + payoff first; legs in a bottom sheet.

**Out of slice 1:** broker, paper, discovery, alerts, backtest, multi-expiry, futures.

**Done when:**
- A strategy can be built from the chain or a template, analysed, scenario-tested, saved as a snapshot, reopened and duplicated.
- The golden tests pass, and the timezone test passes in 3 browser timezones.
- Every screen state (loading, empty, stale, error) has been checked in the browser at desktop and mobile widths.
