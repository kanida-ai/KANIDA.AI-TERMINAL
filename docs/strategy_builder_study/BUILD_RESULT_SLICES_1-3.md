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

---

# Slice 5 — strategy alerts (25 Sep 2026)

`strategy_builder/alerts.py`, with routes under `/api/sb/alerts*` and the UI in `src/strategyBuilder/Alerts.tsx`. It adds an **Alerts** tab to every strategy, an Alerts button and badge, and an alerts centre at `/strategies?view=alerts`.

- **Notify only.** Nothing in alerts places, modifies or cancels an order. Tests assert that no pilot order and no close intent is created.
- **Rules:**

  | Rule | Fires when | Scope |
  |---|---|---|
  | Underlying crosses | the spot rises above or falls below a level | draft or deployment |
  | Near a breakeven | the spot is within X points of a breakeven | draft or deployment |
  | Position P&L | net P&L (liquidation-marked) passes a loss or profit threshold | deployment only |
  | Net delta | abs(net delta) reaches a limit | draft or deployment |
  | Short in the money | a short leg goes in the money | draft or deployment |
  | Reminder | a set time in IST arrives | draft or deployment |

  Deployment-scoped rules watch the **actual filled position** at its fill prices.
- **Firing and re-arming:**
  - A rule fires once, on the transition into its condition.
  - It re-arms only after the condition clears by a hysteresis margin: 0.1% for price, 1.5× the distance for breakevens, 10% for P&L, 80% for delta.
  - Nothing fires within the cooldown (default 15 minutes).
  - A reminder fires once and then expires.
- **Missing data:** the rule goes `data_unavailable`, records that **once** and never fires, then records `recovered` when inputs return. Rules are evaluated only in market hours, except reminders.
- **Evaluation:** a worker runs every 15 s on live Kite data. "Check now" shows the current value without changing any state. An edit made during an evaluation is not overwritten, because rules carry a version.
- **Delivery:** in-app events with acknowledge and acknowledge-all, badges, and **opt-in browser notifications** while a KANIDA tab is open. Email and push are not built.
- **Suggested alerts** for a paper deployment, in one click: near breakeven (50 points), a loss of 50% of capital at risk, short leg in the money, and an expiry-day 13:30 reminder.
- **Verified:**
  - 6 new tests: fire once, hysteresis re-arm, cooldown, data unavailable → recovered, reminder expiry, validation/scope/version, no live data → unavailable, and live position alerts with acknowledgement.
  - Full suite: 587 passed, 1 skipped.
  - **Browser against live Kite** (25 Sep, 10:35–10:37 IST): a price alert and a breakeven alert each fired exactly once on the next cycle, the badges updated, and acknowledge worked in the tab and in the centre.

---

# Slice 6 — the Lab (25 Sep 2026)

`strategy_builder/lab.py`, routes `/api/sb/lab/*` and `/api/sb/strategies/{id}/replay`, UI `src/strategyBuilder/Lab.tsx`. Open it at `/strategies?view=lab`, or from any strategy with **Prove in Lab**.

## What data exists (the scope this sets)
- **NIFTY 50 and India VIX daily** from 2013-01-01 in `db/kanida.db` (read-only). It ends 2026-07-29, so the Lab tops up later days from **Kite historical** (read-only) into `lab_daily` in its own store.
- **Option prices:** only about 3 weeks of captured 15-minute candles (31 Aug to 18 Sep) plus the 17–23 Sep snapshots. Kite serves history for **listed** contracts only, not expired ones.
- **As a result:** replay uses real prices for current contracts. A multi-year rule backtest must use **modelled** option prices.

## Replay: how did these exact contracts trade?
- The strategy's own contracts, using Kite historical candles (5-minute, 15-minute, 1-hour or daily; 2–30 days back), falling back to the captured candles.
- It starts from the first bar where **every** leg traded. A bar missing for any leg is **skipped and counted, never filled in**.
- Results are gross of charges and slippage, and say so.

## Rule backtest: would this rule have made money? (model-priced)
- **Scope:** NIFTY only. The expiry calendar is derived and stated: **monthly only** (last Thursday) before weekly options began on 2019-02-11, weekly Thursday from then, weekly Tuesday from 2025-09-01, with holidays moving to the previous trading day.
- **Sessions:** special sessions (Muhurat, special Saturdays, the Feb-2021 outage day; found as days with fewer than 300 one-minute NIFTY bars) and weekend dates are excluded. A daily bar for today counts only after 15:45 IST.
- **Timing (point in time):**
  - Decide at a day's close, using that day's close and VIX.
  - Enter at the **next open**. Strikes come from the rule at the entry-open spot and the decision-day VIX.
  - Exits (take-profit % of max profit, stop % of max loss, exit at N days to expiry, or hold) are checked at closes and **filled at the next open**.
  - Expiry settles at its close, intrinsic only.
  - One position at a time.
  - **Only closed trades count**; trades still open at the end are excluded and counted.
  - A stop is only accepted on defined-risk structures, and a target only where max profit is capped. Anything else is rejected with a reason, never silently ignored.
- **Costs:** the F&O charges estimate on every fill, **slippage** of at least 0.5% of the model price against you (it can only be raised), and STT on exercise of in-the-money longs.
- **Statistics:**
  - Discovery, out-of-sample and all trades, split by entry date (default: the midpoint of the period).
  - **Expectancy per trade with a bootstrap 95% CI**, expectancy per ₹100 at risk, total, max drawdown, worst trade, average hold.
  - Win rate is shown as context only.
  - A **random-entry control**: the same rule on random days across the whole period, 40 runs, reported for all trades and out of sample only.
  - A trade open across the discovery/out-of-sample boundary is in **neither** set, and is counted.
  - An equity curve, a trade list and CSV download.
  - Reproducible (fixed seed).
- **Badge:** reads the **out-of-sample 95% lower bound** only, needs 30 or more out-of-sample trades, and never says more than *Model-tested*. Every result carries **"Model-priced — not traded prices"** and the full assumptions: one IV for all strikes (no skew), a constant rate, and today's lot size used for every year.
- **Discover:** a card shows Lab evidence only for the **same rule** it displays: NIFTY, the same template and width, held to expiry (no stop, target or time exit). The card states the schedule and how many runs of that rule were tried. Otherwise it shows "Model only".

## Quant audit (dev-quant-auditor) and fixes
| # | Finding | Fix |
|---|---|---|
| C1 | Weekly expiries were fabricated before 2019 (NIFTY weeklies began 11 Feb 2019) | Monthly-only calendar before 2019-02-11 |
| C2 | Stop/target were silently ignored for structures without a defined max loss/profit | Rejected at validation with a reason |
| C3 | The random control only sampled the early part of the period | Random subset across the whole period; out-of-sample control added |
| P1 | Discover evidence matched on template and width only (could show a stop/target run's result) | Matches the exact held-to-expiry NIFTY rule; states the schedule and the number of runs tried |
| P2 | Special sessions were treated as normal days | Excluded (fewer than 300 one-minute bars, or a weekend date) |
| P3 | A partial bar for today could enter; the clock was not IST | IST clock; today only after 15:45 IST |
| P4 | Trades straddling the split counted as discovery | In neither set, counted as `straddled_split` |

Seven regression tests were added (`test_audit_*`). The full server suite passes: 600 passed, 1 skipped.

## First real results after the fixes (kanida.db 2016-01-01 → 2026-07-29, Wednesday decisions, 1–7 days to expiry, split 2021-04-15)
**None of the tested rules is significant out of sample.** For example, an iron condor (wings 4, take-profit 50%, stop 100%):
- 313 closed trades, an **80.5% win rate**.
- Out of sample: n=200, expectancy +₹244 per trade, 95% CI −₹381 to +₹813. The badge is *not significant*.
- 105 Wednesdays before 2019 had no monthly expiry within 1–7 days, so they were skipped. One trade straddled the split.
- Against the random-entry control, the rule beat 97.5% of runs over the full period but only 70% out of sample.

Before the fixes, this run reported 403 trades (a CI of −₹265 to +₹865). The extra trades came from the fabricated pre-2019 weeklies.

---

# Slice 7 — the live bridge to AutoTrade (25 Sep 2026, locked)

Owner decisions: **"Locked bridge"** and **"defined-risk shorts only"**. The pilot never talks to a broker. It hands the exact reviewed plan to engine AutoTrade, and AutoTrade owns execution.

## Engine side (`engine/backend/autotrade/intents/`, `api/intents_routes.py`; branch `feat/autotrade-strategy-intents`)
Before this slice, AutoTrade had no intake for external baskets:
- options were gated off as "not certified";
- short options were disallowed;
- there was no multi-leg sequencing;
- there was no operator arm.

The new module is additive; no existing session, ladder or exit code changed.
- **Intake** `POST /api/autotrade/intents`, gated by the operator token.
  - **Legs:** explicit NFO NIFTY legs, LIMIT only, on the tick, at or below the freeze quantity, one expiry, one product.
  - **Idempotency:** idempotent on (source, key); the same key with a different payload returns 409.
- **Policy:** defined risk only (per option type, long qty ≥ short qty); **every BUY group must precede every SELL group**; the exact max loss is computed from the expiry payoff.
- **Gates** (all needed for live; `GET …/capability` lists them):
  - `FALCON_AUTOTRADE_ENABLED`, `FALCON_AUTOTRADE_OPTIONS_ENABLED` and the new `AUTOTRADE_STRATEGY_INTENTS_LIVE`;
  - the broker in the new `AUTOTRADE_STRATEGY_INTENTS_CERTIFIED` list and `registry.is_certified`;
  - an unexpired **operator arm** (user + account, basket allowance, max-loss cap);
  - market open;
  - basket margin (the broker's own figure) not above free margin, checked right before the first order. An unknown margin refuses.
  - A live request that fails any gate is **blocked, never downgraded** to a dry run.
- **Arm:** `POST …/arm` needs a second secret, `FALCON_OPERATOR_ARM_TOKEN`, which the pilot never holds. TTL is at most 8 h, at most 20 baskets, and armed_by is required. Disarm needs only the operator token.
- **Dispatch:** runs group by group through the existing broker adapter's `place_order`.
  - A dry run returns DRY_RUN inside the adapter, so no order call is made.
  - Live: the next group goes only after every leg of the previous group is COMPLETE at full quantity.
  - Gates and the arm are re-checked before each group.
  - Any reject, timeout, cancel or disarm cancels the current group's resting orders and sends no later group. A basket that may hold fills ends `attention_required`; nothing is auto-flattened or resubmitted.
  - Every order is written to the order ledger (ORDER_CREATED before submission).
- **Live placement is single-shot.** It never uses the adapter's retrying `place_order`. On an exception, the dispatcher queries the orderbook by our tag:
  - found: the order is adopted;
  - confirmed absent: a clean failure;
  - lookup failed: the leg is `unknown`, which counts as a possible fill.
  - This means a timed-out order is never sent twice.
- **Restarts:** a startup sweep moves any basket that a restart left `dispatching` to `attention_required`.
- **Tests:** `tests/autotrade/test_strategy_intents.py`, 34 tests using a fake broker driven through the real single-shot path. The dry run goes through the real Zerodha adapter in dry-run mode.

## Independent review (dev-reviewer) and fixes
The verdict was: *pass for dry-run, fail for live* until the first two findings below were fixed. All eight are now fixed and regression-tested.

| Severity | Finding | Fix |
|---|---|---|
| HIGH | The adapter's retry wrapper could send a timed-out order twice (a doubled short) | Single-shot live placement plus a tag lookup; an unknown outcome is `unknown` and treated as a possible fill |
| MED | A DB error or crash after an order id was minted could mark a live leg `not_sent`; a restart could leave a basket `dispatching` | Minted-but-unrecorded legs become `unknown`, ending `attention_required`; startup sweep |
| MED | The broker gate checked the claimed broker, not the account's | The basket is refused before any order on a broker mismatch |
| LOW-MED | Tenant scope on the operator path | Live needs a named user and account; arming checks that the account belongs to the user (vault) |
| LOW | Symbol checks were loose (`NIFTYNXT50…` passed) | The symbol must encode this underlying, expiry, strike and type (weekly or monthly form) |
| LOW | Idempotency races (500, a lost arm allowance) | 409 on a conflicting race; the arm allowance is refunded |
| LOW | The arm token could equal the operator token | 503 if equal |
| LOW | Sync DB calls on the event loop at intake | Intake runs in a thread |

The instrument master (lot size) is not checked yet: the broker rejects wrong multiples, and the pilot builds legs from the live instrument list.

## Pilot side
- **Bridge:** `strategy_builder/autotrade_bridge.py` is the HTTP client (`PILOT_AUTOTRADE_URL`, `PILOT_AUTOTRADE_TOKEN`, `PILOT_AUTOTRADE_ACCOUNT`; unset means "not connected", stated). The route store keeps one hand-off per idempotency key and mirrors AutoTrade's state. The engine identity is `pilot:<user id>`.
- **What it sends:** only an unexpired, hash-checked, unedited *opening* preview with no blocking check and every short covered. Live also needs `confirm_live`.
- **Order Review:** a new AutoTrade panel shows:
  - the gate checklist and the arm;
  - "Send dry run to AutoTrade";
  - "Send live", enabled only when AutoTrade reports every gate passing *and* the user ticks the real-orders confirmation;
  - the status, followed live, with a Stop button.
- **Fix:** "At the quote" limits are now always on the 0.05 tick (buys round up, sells round down, so they still cross). AutoTrade refuses off-tick limits.

## Verified
- The engine intent suite passes 34/34. The full autotrade suite has 1469 passed and 2 failed; both failures happen on pristine `main` too (a Rupeezy flag and a step-lock square-off), so they aren't from this change.
- The pilot suite has 606 passed and 1 skipped, including 6 new bridge and tick tests.
- **End to end** (the real engine router on a temp DB, the real pilot, HTTP between them, no broker):
  - the dry run walked BUY g1 then SELL g2 and ended `dry_run_complete` with no order;
  - a live request was `blocked`, naming all six closed gates;
  - arming with the service token alone returned 403, and with the arm token 200. That cleared only "armed", and live stayed blocked on the switches;
  - a naked short put was refused by the pilot (409) and directly by the engine (400).

## To actually go live (owner only, in this order)
1. Merge the engine branch after review, and deploy to the one live machine.
2. Certify Zerodha for baskets (`AUTOTRADE_STRATEGY_INTENTS_CERTIFIED=zerodha`), options (`FALCON_AUTOTRADE_OPTIONS_ENABLED=true`) and the path (`AUTOTRADE_STRATEGY_INTENTS_LIVE=true`). `FALCON_AUTOTRADE_ENABLED` is the existing master switch.
3. Set `FALCON_OPERATOR_ARM_TOKEN` on the engine. On the pilot, set `PILOT_AUTOTRADE_URL` / `PILOT_AUTOTRADE_TOKEN` / `PILOT_AUTOTRADE_ACCOUNT`.
4. Arm one account for a short window with 1 basket and a small max-loss cap, then send a 1-lot debit spread first.

---

# Slice 8 — Adjustment assistant (K12) with Lab evidence (25 Sep 2026)

Owner decision: **"Adjust + Lab evidence"**. This is blueprint A's P7: each rule has a Lab run attached or shows "Model only".

## Rules catalogue (`strategy_builder/adjust.py`, shared by the assistant and the Lab)
- **The rules:**
  - roll the tested short k strikes away (a long wing it would cross moves with it; an inner long never moves);
  - add a hedge wing k strikes beyond each uncovered short (strangle → condor, straddle → iron butterfly);
  - close the tested side;
  - halve the lots (even lots only);
  - close all.
- **Shown as unavailable, with the reason:**
  - roll out to the next expiry: multi-expiry is not analysed or executed;
  - convert to a butterfly: covered by adding a wing to a straddle.
- **"Tested"** is deterministic: the short leg closest to or furthest past the money, as a % of spot.

## Assistant (`assistant.py`, the Adjust sheet from the Builder header and from each paper deployment)
- **Inputs:** the draft, or what a paper deployment **holds**: whole lots from fills, entry prices at the fill averages.
- **Per candidate:**
  - the **delta orders only**, priced where they would execute (buy at the ask, sell at the bid; an LTP fallback is labelled), with charges;
  - an **exact expiry overlay**: current position vs after = current + the delta orders at today's prices − their charges;
  - the worst case and breakevens after;
  - the model Δ change (shown as "Unavailable" when a leg's IV can't be solved);
  - the Kite margin change.
- **Evidence:**
  - It is attached only when today matches the Lab run: the tested short is within the run's trigger, days to expiry is inside its range, and the position wasn't already adjusted.
  - The legs must *be* the template (a stale template tag doesn't count).
  - Otherwise the label reads "Model only – Lab conditions differ", with the reason.
- **Applying:** the server recomputes the candidate (the client never sends legs) and writes it as a **new draft version**, with optimistic concurrency.
- **For a deployment:** Order Review opens in **adjust mode** and shows only the delta orders.
  - Buys (buy-backs and hedges) go first, and sells only after they fill.
  - This needs an active deployment with no resting orders.
  - Its position book follows every contract across revisions.

## Lab (`lab.py`)
- **What's tested:** one adjustment per trade (`adjust = {rule, k, trigger_pct}`).
- **Timing:** the trigger is read at a close; the tested leg is **pinned** at that close; the adjustment is filled at the **next open** at model prices, with slippage and charges on every delta order.
- **Pairing:**
  - Adjustments are only allowed with hold-to-expiry or a time exit, so every trade has a **baseline twin** with the same entry and exit days.
  - The evidence is the **paired improvement per triggered trade**, with a bootstrap CI, split into discovery and OOS.
  - Untriggered trades reproduce the baseline exactly (tested).
- **Badge:**
  - It reads the OOS interval, needs ≥30 OOS triggers, and is **Bonferroni-corrected** for every adjustment run on that structure.
  - It carries the "model prices, one IV – no skew" caveat.
  - Per ₹100 at risk is suppressed for adjusted runs.

## Quant audit (dev-quant-auditor) and fixes
The audit found no look-ahead in the trigger. It confirmed that untriggered trades reproduce the baseline and that the paired, trigger-conditioned comparison is valid.

| # | Finding | Fix |
|---|---|---|
| 1 HIGH | Evidence was attached regardless of trigger, DTE or a prior adjustment | Evidence applies only when conditions match; otherwise "Model only – Lab conditions differ" with the reason |
| 2 HIGH | No multiple-testing control (newest run wins) | Bonferroni across all adjustment runs on the structure; the label says "corrected for N runs tried" |
| 3 MED | The roll also moved an inner long (a bull call spread's long) | Only a wing between the old and new short strike moves (also confirmed in the live UI) |
| 4 MED | The tested leg was re-picked at the next open | Pinned to the close's decision |
| 5 MED | No-skew bias, and an unrealistic strike grid | Caveat on the badge itself; the grid is capped at ±15% of spot |
| 6 LOW | Per ₹100 at risk used the pre-adjustment risk | Suppressed for adjusted runs |
| 7 LOW | A stale template tag could earn evidence | The legs must be exactly the template |
| 8 LOW | A breakeven exactly at the last evaluated point was missed | Fixed |

## First real results (kanida.db 2019-03-01 → 2026-07-29; Wednesday decisions, 1–7 DTE; trigger 0.3%; OOS from 2023-01-02)
All three are **not significant** out of sample:
- **Short strangle, add wing +2:** 84 of 213 trades triggered (46 OOS). OOS mean +₹441 per triggered trade, CI −₹1,858…+₹2,785; helped 13, hurt 33.
- **Short strangle, roll +2:** OOS mean +₹194, CI −₹535…+₹908 (24 helped / 22 hurt).
- **Iron condor, close the tested side:** OOS mean +₹533, CI −₹682…+₹1,837.

## Verified
- The pilot suite passes: 618 passed, 1 skipped (20 new adjustment and audit tests).
- **In the browser against live Kite (11:45 IST):**
  - tested-leg detection;
  - the overlay;
  - bid/ask delta orders with charges;
  - the Kite margin change;
  - "Model only" badges;
  - the roll fix on a bull call spread.
- The deployment adjust flow (delta orders, fills, positions, then close only what is held) is covered by API tests. I didn't click through it on your live pilot data.

---

# Slice 9 — Evidence-ranked Discover (25 Sep 2026)

Owner decision: **"Evidence-ranked Discover"** (blueprint A §7.3), plus the alert → Adjust deep link.

## How evidence is decided (`strategy_builder/evidence.py`)
- **One hypothesis per rule.** A rule is (template, width, decision weekday, DTE range, exits, slippage). Its p is the **maximum** over all its runs, so re-running or moving the split can't improve it, and duplicates can't pad the family.
- **The test:** H0 is "out-of-sample expectancy after costs ≤ 0", on each run's stored OOS trades; n_oos ≥ 30 is required to count as a test.
- **The p-value:** Johnson's skew-adjusted t. A plain t-test and even a block bootstrap over-reject on skewed option P&L: in simulation, 13–17% at a nominal 5–10%.
- **Tail stress** (defined risk only): when losses of at least 50% of the maximum loss appear fewer than 5 times, their frequency is raised to its 95% upper bound (Wilson), and the mean must stay above zero. A sample that simply never saw the rare max-loss trade can't earn a badge.
  - Simulated null pass rates with both guards stay at or below 11% at FDR 10%, for credit structures with 2–5% tails, debit 50/50 and normal P&L.
- **Undefined-risk** structures are never "Tested ✓".
- **Correction:** Benjamini–Hochberg at FDR 10% (rank-based step-up) across **every distinct rule** the user tested. Adjustment runs are their own Bonferroni family (slice 8).
- **Ranking statistic:** return per ₹100 of maximum model loss. The ranking uses its moving-block-bootstrap 95% low, which keeps the autocorrelation of sequential trades.

## Discover
- **Card evidence:** the rule Discover shows (template, width, held to expiry). Only schedules whose DTE range covers this expiry apply, with DTE counted **from the next session**, as the Lab counts it. The most conservative of them decides.
- **Labels:** the label names the decision weekday. The note gives the runs, the number of rules corrected together, and "the Lab placed strikes by India VIX; this card uses today's chain".
- **Ranking — a stated deviation from the blueprint's single sort:**
  - **Tier 1:** "Tested ✓" defined-risk rules with an OOS 95% low above zero, by that low.
  - **Tier 2:** everything else, in the model order.
  - This way, weak or negative evidence never outranks the model order.
- **Summary line:** "N rule tests in your Lab; k survive the 10% false-discovery correction". The cards carry fixed honesty copy (§7.5).
- **Lab:** the run history shows each run's board status. `GET /api/sb/lab/evidence` returns the whole board.
- **Templates:** a Discover-created strategy now keeps its template width, so its evidence can match later.

## Alert → Adjust
Each fired alert has an **Adjust** link. It opens the strategy with the adjustment assistant already open (using its active paper deployment when there is one).

## Quant audit (dev-quant-auditor) and fixes
The verdict was "not approvable as is", with two false-"Tested ✓" routes reproduced. All nine findings are fixed:

| # | Finding | Fix |
|---|---|---|
| 1 HIGH | Re-running the same rule inflated BH (9 copies turned 1 null into 12 survivors) | One hypothesis per rule; m = distinct rules |
| 2 HIGH | "Newest run" allowed p-hacking via split/period | The most conservative run of the rule decides |
| 3 HIGH | The t-test over-rejects on skewed short-premium P&L | Johnson skew-adjusted t, plus a tail stress for under-observed max-loss events; undefined risk is never badged |
| 4 MED | The badge and the ranking used different statistics | One per-trade series (return per ₹100 of max loss); tier 1 needs a positive low |
| 5 MED | DTE counted from the reading, not the Lab's next-session entry | `next_session_dte` |
| 6 MED | Undefined-risk rows sat in tier 1 with no normalised figure | Excluded from tier 1; the copy says "per ₹100 of maximum model loss" |
| 7 MED | Weekday and strike placement differences were hidden | The weekday is in the label; the strike-placement note is on every card |
| 8 LOW | `cut > 0` rejected p = 0 survivors | Rank-based BH |
| 9 LOW | The board was recomputed per request | Per-run cache (completed runs never change) |

## Verified
- The pilot suite passes: 625 passed, 1 skipped (9 new evidence tests: per-rule grouping, re-run immunity, skew false-positive guard, exact BH step-up, p = 0, next-session DTE, tiers, Discover integration).
- **Browser (live Kite, 11:59 IST):**
  - the Discover summary ("1 rule test… 0 survive") and the tier basis;
  - the honest "Model only" copy;
  - the alert "Adjust" link opens the assistant.
- **Real Lab:** the one existing run (iron condor 50%/100%, n_oos = 207) is **not significant** (p = 0.16, 95% low −2.7 per ₹100). It doesn't attach to Discover cards anyway, because its exits make it a different rule.

---

# Evidence expansion, step 1 — the pre-registered NIFTY experiment (25 Sep 2026)

- **`experiments.py`:** a grid is expanded into its exact rule list, **hashed and written before anything runs**, then run once in parallel. Every rule is reported, including failures.
  - A new grid is a new batch; nothing is re-run to replace results.
  - CLI: `python -m kanida_pilot.strategy_builder.experiments --db var/strategy_builder.db --user <id> --grid nifty_v1`.
  - The Lab has an **Experiments** tab: pre-registration details, counts, and every rule with its n, mean ₹/trade, p and 95% low.
- **Evidence families are now per underlying**, so NIFTY, BANKNIFTY and each stock are corrected separately and one can never lift another.
- **Grid `nifty_v1`:** every defined-risk template × width (32 structures) × decision weekday (5) × DTE window (1–7, 8–14, 15–35) = **480 rules**.
  - Held to expiry, from 2019-03-01 (weekly-option era) to the end of the data, out of sample from 2023-01-02, slippage 0.5%.
- **Result (the owner's pilot Lab, `kanida.db` to 2026-07-29):**
  - 480 done, 0 failed in 42 s; **0 survive** the FDR 10% correction across 480 rules.
  - The best rule, iron condor width 6 with Thursday decisions and 8–14 DTE, has n = 89, p = 0.014. Rank 1 needs p ≤ 0.1/480 ≈ 0.0002.
  - About 15 rules have p < 0.1, against about 48 expected by chance. On average these rules lose after costs; this is not a hidden edge.
- **Discover** still shows "Model only / not significant" everywhere, which is the honest outcome.
