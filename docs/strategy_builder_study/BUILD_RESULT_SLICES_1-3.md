# Strategy Builder — slices 1–3 build result (25 Sep 2026)

This is the build of slices 1–3 from `BUILD_PLAN_MERGED.md` (research workspace, paper trading, Discover) in the
kanida-app pilot.

**Where to see it:** the **Strategies** tab, at `http://127.0.0.1:8082/strategies` once you are signed in.

**Data:** the stored reading in `db/derivatives.db`, Tue 23 Sep 15:45 IST. It is **not live**.
- The store has **no bid/ask**, so prices are last traded (LTP), and the app says so on every screen.
- Live quotes arrive after the Windows→Mac cutover.

## What was built

**Server** — `server/kanida_pilot/strategy_builder/`, mounted guarded at the end of `create_app`, with its own store
`var/strategy_builder.db`:

| Module | Job |
|---|---|
| `analytics.py` | Pure maths. Exact piecewise expiry payoff: max profit/loss with tails, a zero-spot floor and exact breakevens. BSM scenario value, reusing `implied_vol.py`. Greeks with explicit units. Model POP and SD bands. Every metric is an envelope (`available` / `unavailable` / `unsupported`), never a zero placeholder. Multi-expiry returns `unsupported`. |
| `market.py` | Read-only reader over `derivatives.db` (`mode=ro`): index underlyings, expiries exactly as stored (no timezone conversion), and the chain at the newest reading with computed IV and quality flags (`no_bid_ask`, `stale_trade`). |
| `templates.py` | 10 defined-risk recipes plus 4 unhedged ones (labelled), resolved against the chain; a "later" list; structure recognition. |
| `charges.py` | F&O options charges estimate (versioned; the autotrade version is equity-only). |
| `store.py` | Strategies, autosaved drafts (a stale version gets a 409 with the stored copy), immutable snapshots (checksummed), paper runs with append-only fills, activity. |
| `service.py` | Body validation, leg hydration (lot size and LTP from the chain), analysis, paper fill policy (LTP ± max(0.5%, 1 tick) against you, plus charges), marks. |
| `discover.py` | View + limits → at most 6 deduplicated, explained candidates. Defined-risk only by default. The binding constraint is named when nothing matches. Every card says "Model only — not tested on history". |
| `routes.py` | `/api/sb/*`, GET/POST only, member-only and owner-scoped. |

**Web** — `src/strategyBuilder/`, route `app/strategies.tsx`, plus a "Strategies" nav item in `src/shell/routes.tsx`:
- **Home:** start with Build from scratch, Use a template, or Find a strategy. The library has filters (Active / Paper traded / Archived), search, Duplicate and Archive.
- **Builder:**
  - A leg editor: include, B/S, strike ±, CE/PE, lots ±, and entry price as LTP or manual, with undo on remove.
  - Shift/Width/Wings adjusters and live structure recognition.
  - Autosave with a version badge.
  - A risk strip: max loss/profit, breakeven, capital at risk, net credit/debit, POP (model), charges (est.), and margin shown as "Needs broker".
  - The payoff chart, with expiry and scenario curves, SD bands, spot, breakevens and a hover readout.
  - The scenario bar: spot, trading-day date and IV shift.
  - Tabs: P&L by leg, Greeks (units stated), Payoff table, Snapshots (restore / copy), Paper, Activity.
  - The chain drawer (persistent B/S, opens centred on ATM), the template sheet and the paper confirmation sheet.
- **Discover:** view + limits → cards → Compare (overlaid expiry payoff and a table) → Use as draft.
- **Paper runs:** every run, marked at the newest reading, with net, realised, unrealised, fees and a close-now estimate.

## Verification
- `tests/test_strategy_builder.py` has **27 tests**:
  - golden payoffs (long call, short put, vertical, straddle, asymmetric condor);
  - Greeks signs and parity;
  - never-zero statuses;
  - an exact scenario at expiry and a zero scenario P&L at the reading;
  - recognition of every structure;
  - every template resolving to its own structure;
  - autosave conflict, snapshot, restore and duplicate;
  - owner scoping;
  - bad bodies refused with a reason;
  - paper fills against you with no pilot order written;
  - Discover being small, explained, honest and naming its binding constraint.
- **Full pilot suite:** 572 passed, 1 skipped.
- **Real data:** all 14 templates resolve and recognise on the stored NIFTY chain (spot 23,446.8, ATM IV 8.66%, lot 65). Each analysis takes about 1 ms.
- **Browser, screen by screen:** checked at 1440 px desktop and 375 px mobile, against an isolated dev server (:8092, throwaway accounts):
  - template → builder → scenario to expiry (= max profit) → Greeks → payoff table;
  - paper trade → paper runs page;
  - Discover → compare → use as draft;
  - chain drawer adding call legs (recognised as an iron condor) → Wings +2 → snapshot;
  - mobile builder, chain and Discover.

## Deviations from the blueprints
- **One source of analytics truth:** the server computes and the client shows it. A TS mirror was not built; per-edit latency on real data is about 1 ms plus the round trip.
- **Discover ranks by return on capital-at-risk at the user's own view** (a constraint match). The backtested-ETV ranking and FDR badge (blueprint A §7.3) switch on once the Lab exists.
- **Margin** stays "Needs broker": there is no SPAN estimate, rather than an invented number. Capital at risk is labelled as the structural maximum loss.
- **Scenario date steps** skip weekends, not exchange holidays (stated in the UI).

## Next
- **Slice 4:** order review → autotrade paper intents (preview hash and expiry, pre-trade checks, sequencing).
- **Slice 5:** strategy alerts.
- **Slice 6:** the Lab (replay, then rule backtest).
- **Live quotes and broker margin:** after cutover.
