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

---

# Slice 4 + live Kite data (25 Sep 2026)

## Live market data
- `strategy_builder/kite_market.py` is a **read-only** Kite adapter. It uses three endpoints: `/instruments/NFO`, `/quote` (with depth), and `/margins/basket` (a hypothetical basket; nothing is placed).
- **Credentials:** the engine's single source of truth, read only: the newest `kite_tokens` row plus `KITE_API_KEY` from `~/Kanida/engine/config/.env`. It never mints a token.
- **Enabling it:** set `PILOT_SB_LIVE=kite`. `migration/mac/start-pilot.sh` now defaults to it; tests never touch Kite.
- **Fallback:** `MarketRouter` serves live data when the token works and the stored reading otherwise. Every response says which source it came from.
- **Prices:** the new entry basis `exec` buys at the ask and sells at the bid (the default). `mid`, `ltp` and `manual` remain available, and each leg shows which basis it used.
- **Margin:** the real exchange margin (SPAN + exposure) from Kite's basket-margin read, with the hedge benefit. It replaces "Needs broker" whenever data is live.
- **Token:** minted on the Mac on 25 Sep at 10:03 IST at the owner's instruction. The engine auth worker stamped it `2026-09-24`, the Pacific date: **a Mac timezone bug in the engine's auth code**, not fixed here.

## Slice 4: order review, intents, paper broker (`strategy_builder/execution.py`)
- **Preview** (K11): the exact orders, with contract, side, qty, limit (at the quote or at the mid, editable within 20% of the quote), product, sequence group and freeze-qty slices. It carries the quotes it was built from, 7 checks, Kite margin, charges, a **SHA-256 hash** and a **30 s expiry**.
- **Pre-trade checks:**
  - market open;
  - quotes at most 15 s old;
  - live bid and ask on every leg;
  - spread no wider than 5% (warns otherwise);
  - freeze limits (configured; slicing applied);
  - defined risk, with unlimited loss needing an acknowledgement;
  - paper capital covering the margin.
- **Confirm:** `confirm:true` plus the preview id, hash and an **idempotency key**. The system refuses a changed, expired or stale plan (draft edited since the preview). Reusing a key returns the same deployment; the same key with a different plan gets a 409.
- **Paper broker:**
  - A BUY fills at the ask once ask ≤ limit; a SELL fills at the bid once bid ≥ limit. Otherwise the order rests, and a worker re-checks quotes every 5 s during market hours.
  - **Hedges (buys) go first; sells are released only when every buy has FILLED.** A cancelled hedge never releases the short.
  - Closing buys back shorts first. A close stopped part-way leaves the deployment `attention_required`, and a new close covers only the residual.
- **Monitor** (K12): positions from fills, marked at liquidation prices (longs at the bid, shorts at the ask), realised/unrealised/fees, each order's state, Cancel resting, Close position.
- **Live routing:** a disabled capability. It will go through `backend/autotrade` (paper by default, certified per broker, operator-armed). **No broker order API is called anywhere in the pilot.**

## Verified
- **Tests:** 36 builder tests; the full suite is 581 passed, 1 skipped. Slice 4 adds a fake live market, and its tests cover:
  - the plan itself (hash, expiry, sequence, slices);
  - the confirm guards (confirm, hash, expiry, edit-after-preview);
  - idempotency;
  - buys-first fills;
  - resting limits;
  - unlimited-loss acknowledgement;
  - a margin block;
  - regressions for the cancel/sequence bug;
  - a partial close needing attention.
- **Browser against live Kite:** 25 Sep, 10:16–10:24 IST. Template → live pricing (ask/bid) and real margin → review → place:
  - The order rested when the ask moved away.
  - It filled at a better price when the market came back, and the sell was released after the buy.
  - Close: the short was bought back first, and the long's sale rested.
- **Bug found and fixed in that browser session:** cancelling a resting hedge let the worker acknowledge the short group (a race with no state guard). Group release now needs a fully filled previous group, and every state change is guarded by the state it expects.
