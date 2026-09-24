# KANIDA platform plan — audit, gaps and build sequence

Prepared 23 Sep 2026, before the 09:15 IST session. Inputs: the five research documents (Quantsapp, Sensibull
and TrendSpider teardowns, plus the two GPT research reports), the 22 Build-Now capabilities, and a direct audit
of this repository. Every codebase claim below was measured or read, not assumed.

---

## 1. What the research demands

The three teardowns agree from three directions: **traders do not lack data; they lack meaning, continuity and
proof.** The specific, repeated demands:

| # | Demand | Evidence |
|---|---|---|
| D1 | A visible data-age stamp on every number, ageing amber then red | Quantsapp: 344 freshness mentions — the defining technical complaint |
| D2 | Plain-language interpretation under every metric, **with base rate and sample size** | "the oldest and most repeated request in the corpus" |
| D3 | 09:15 cold start as a monitored, first-class event; a public status page | "chart live data does not start automatically at 9:15"; "outage during market hrs with no intimation" |
| D4 | Option chain with ΔOI as bars, per-strike PCR, and **buy/sell attribution** | Their best-praised surface, plus the named open gap |
| D5 | Every computed metric alertable, one interaction from the metric | "a knife without a handle", 2019, never fixed |
| D6 | Costs in every backtest by default; guardrails that cannot be switched off | TrendSpider's own documented confession |
| D7 | Realisable P&L and fills — never LTP; margin before the trade | Sensibull: "fill at the far touch, charge full costs, and slip" |
| D8 | The account owns entitlements, independent of broker; no phone-number wall | Sensibull's structural trap; six years of Quantsapp complaints |
| D9 | Persist everything the user creates, across refresh, symbol, device and tab | Lost drawings; multi-tab blocked |
| D10 | Searchable documentation per capability, reachable from it | No help centre exists for 69+ tools |
| D11 | Public pricing, a monthly plan, no clawback of free features | No Quantsapp price publicly visible since 2021 |
| D12 | English and Hindi | 13 requests over three years |
| D13 | A published accuracy benchmark, losers included; never sell "autopilot" | Nobody trades detections directly, at any maturity |
| D14 | AI inference metered, capped or precomputed — decided before launch | At Indian price points this is cost of goods sold |
| D15 | Scanner honesty: universe, last successful run, partial vs "no matches" | A rule builder whose search cannot finish has no value |
| D16 | Scan → setup → payoff → basket as one unbroken path | Asked of Quantsapp in 2021, still open |
| D17 | A public, verifiable track record with costs and losers | "the best idea in this teardown, and free to copy" |

Two findings shape strategy more than the rest:

1. **An immutable record is the wedge.** TrendSpider's own documentation says patterns "may disappear if deemed
   no longer relevant". A detection that vanishes retroactively can never support a track record. Our immutable
   snapshots plus outcome history are what they cannot retrofit.
2. **Never benchmark on a competitor's store rating.** Quantsapp's 4.7 measures their sales process; Trustpilot's
   1.9 measures the product. Use unsolicited channels only.

Nothing in the research asks for more tools. Every item asks for trust, continuity or explanation of data that is
already on the screen.

---

## 2. Codebase architecture map (measured)

```
Kanida_Falcon/
├─ market_data/            ingestion; derivatives/{capture,backfill,instruments,cli}.py → db/derivatives.db
├─ market_scanner/         33 modules + its own stdlib HTTP service on :8765 (equity patterns, backtests)
├─ agent_builder_service/  a separate FastAPI (:8010, 6 routes, no auth) — "local testing before dropping into
│                          the main app"; nothing in kanida-app references it
├─ db/                     kanida.db 147 GB (last price 31 Jul) · market15.db 18 GB · derivatives.db 1.7 GB
├─ scripts/ agents/ arena/ kanida_engine/ pathfinder/ v3–v6/ SELVI/   prior and parallel R&D lines
└─ kanida-app/             the product
   ├─ server/kanida_pilot/ 27 modules, 10.4k lines; ~90 routes. app.py (789) wires it; derivatives.py (3,979)
   │                       is the F&O read model; snapshots.py (343) writes immutable readings; signal_noise.py
   │                       (430) grades them; screener/ and workspace/ are new
   ├─ server/engine/       run_engine.cjs — the SAME TypeScript the browser runs, under Node
   ├─ src/derivative/      logic.ts (2,862) · signal.ts (817) · summary.ts (695) — the market vocabulary
   ├─ src/{falcon,discover,workspace,screener,workbench,layout,admin,patternHistory}
   └─ var/                 intelligence.db 149 MB (immutable snapshots + verdicts) · pilot.sqlite3 (users)
```

**The most important structural fact:** the 15-minute intelligence is computed **once**, server-side, by the same
`signal.ts` the browser renders, and written to immutable rows in `var/intelligence.db`. Your principles 2, 3 and 4
already have a working implementation. It needs widening, not inventing.

**Three servers exist**: the pilot (FastAPI, ~90 routes, also serves the web bundle), the scanner (:8765, ten
read paths proxied through a whitelist), and the Agent Builder (:8010, unreferenced). Databases are shared with a
strict discipline: everything upstream is opened read-only, and the pilot writes only its own two stores.

---

## 3. What exists today

| Area | State |
|---|---|
| F&O capture | Every 15 min into `db/derivatives.db`: `contracts`, `snapshots` (raw quotes), `candles_15m`, `underlying_snapshots`, `metrics` (computed per contract and per book), `captures` (run health), `runs`, `backfill_progress`. 216 underlyings, front two expiries, 25 readings a session |
| State engine | `signal.ts`: opening · appeared · building · held · strengthened · broadened · concentrated · shifted · slowed · fading · reversing · balanced · mixed — with side, strike range, lead strike, breadth, persistence, supporting and conflicting evidence |
| Immutable history | `reading_snapshots`: 17,712 rows, 2 sessions, 216 underlyings, each with its own contracts, `engine_version`, `rules_version` |
| Signal-to-noise | `insight_records` 22,821 · `insight_outcomes` 78,303 · `insight_verdicts` 20,534, with noise reasons and a per-session report |
| Derivative tab | 9 blocks over 20 read routes; liquidity floors (₹2 cr premium, OI lots, price) and "unusual" (≥2× its own time-of-day median volume, or a volume/OI spike) |
| Options screener | Word-state conditions, natural language, match lifecycle, alerts, per-user scanners (22 Sep) |
| Workspace | Composed rows, 22 widget types, follow/pin, autosave, templates (22 Sep) |
| AI summary | Deterministic synthesis plus scanner alignment. **There is no LLM anywhere in this repository** |
| Equity research | Pattern catalogue, backtests with fee and slippage in basis points, evidence gating, the :8765 service |
| Virtual trading | `simulation.py` (145 lines): deterministic *scenario* fixtures — target / stop / gap / partial — with synthetic prices, labelled as such. Not a fill engine |
| Identity | Email + password (argon2), sessions with CSRF, invitation-gated; Razorpay **test mode**; **no price is ever displayed in-app** |
| Help | Every Derivative block has "How to read this" — ahead of the category. Nowhere else has any |
| CI | `falcon-ci.yml`: tsc, three logic checks, pytest for the pilot and for scanner/data |
| Deployment | Dockerfile + ECS template (512 CPU / 1 GB, ap-south-1), not deployed. The runtime image has no Node, so the snapshot engine would silently stop in the cloud (documented, unapplied) |
| Dead code | `src/falcon/` (9 files) calls four `/api/falcon/*` endpoints that do not exist; `/` shows a disabled "Coming soon" button. `/agent` and `/portfolios` are redirects |

---

## 4. Gap analysis — the 22 capabilities

| # | Capability | Status | Why |
|---|---|---|---|
| 1 | Market Intelligence State Engine | **PARTIALLY READY** | Vocabulary, evidence, persistence and one shared runtime exist. Scope is the front two expiries' ATM ±5 grid; no aggressor attribution; no cross-instrument state |
| 2 | Market Data Trust Layer | **NEEDS REDESIGN** | `captures` records run health and every block prints an as-of, but a reading has one timestamp (`captured_at`). No vendor vs received vs processed split, no per-field completeness, no data-age object. The freshness *judgement* is currently client-side (`layout/dataStatus.ts`) |
| 3 | AI-native screener | **READY** | Built 22 Sep. The 15-min alert job is written but not registered |
| 4 | Monitoring & alerts | **PARTIALLY READY** | Screener alerts with transition-only dedupe; in-app only; not generic |
| 5 | Connected workspace | **READY** | Built 22 Sep; hooks pending |
| 6 | Intelligent option chain | **PARTIALLY READY** | Chain, OI-by-strike bars, IV, PCR, max pain exist. Missing: ΔOI bars *inside* the chain, per-strike PCR, Greeks in the chain, attribution |
| 7 | Verified timeline + S/N | **PARTIALLY READY** | The data exists and is immutable; the report is a Markdown file per session. No user-facing timeline, nothing public |
| 8 | Honest backtesting | **PARTIALLY READY (equity) · MISSING (options)** | Equity backtests apply fees and slippage, with audit scripts. Nothing backtests an options state or a screener condition |
| 9 | Strategy & position lab | **MISSING** | No payoff, margin or what-if. Per-strike Greeks are computed |
| 10 | Adaptive AI experience | **MISSING** | One voice; no depth switch; no "why?" beyond evidence lines |
| 11 | Aggressor-side attribution | **MISSING** | `snapshots` stores bid, ask, buy and sell quantity, so the inputs exist; nothing computes it |
| 12 | Tradability filter | **PARTIALLY READY** | Liquidity floors exist in the metrics worker and the screener. No spread, impact cost, ban list, ASM/GSM |
| 13 | Realistic virtual fills | **MISSING** | Scenario fixtures by design |
| 14 | Access & identity | **PARTIALLY READY** | Good auth, CSRF, per-user stores, no phone number. Invite-gated with no explore-before-signup |
| 15 | Market-open reliability | **NEEDS REDESIGN** | The whole pipeline runs on one laptop in Pacific time. A sleep on 18 Sep cost the session from 11:30 IST. No status page, no 09:15 gate, no paging |
| 16 | Realisable P&L + margin | **MISSING** | No margin model, no realisable marks |
| 17 | Alert-anything | **MISSING** | Alerts are screener-specific |
| 18 | Multiple-testing protection | **PARTIALLY READY** | The pattern research was multiple-testing corrected (result: no edge survived). Not a platform service |
| 19 | Corrections & revision log | **MISSING** | Immutability — the hard half — exists. No revision object, no public log |
| 20 | English + Hindi | **MISSING** | Zero i18n infrastructure: no library, no catalog, inline strings everywhere, 23 hardcoded `en-IN` calls, IST hardcoded. The narrative engine assembles English sentences grammatically, so this is not a string swap |
| 21 | Transparent pricing | **MISSING** | Test mode; no public pricing page; no price shown anywhere |
| 22 | In-product documentation | **PARTIALLY READY** | The Derivative disclosures are a real asset. Not searchable, not per-capability, absent on new surfaces |

**Score: 2 ready · 9 partially ready · 3 need redesign · 8 missing.**

---

## 5. Technical debt and correctness risks

1. **The pipeline's host is the biggest risk in the product.** One laptop, Pacific time, the Indian session
   overnight. The 18 Sep sleep is the proof.
2. **98 uncommitted files** on `codex/market-intelligence-engine`, with several sessions editing one tree.
3. **The cloud image would break intelligence silently** — no Node in the runtime stage.
4. **Build-up classification lives in three places** (server `derivatives.py`, client `logic.ts`, shared engine).
   `logic.ts` documents a production bug caused by exactly that split. This is the shape of drift your principle 3
   forbids, and it survives today.
5. **Staleness is judged on the client** (`dataStatus.ts`, `decision.ts`) — the one rule that must be server-owned.
6. **`derivatives.py` is 3,979 lines** and owns the whole read model.
7. **Naming collisions:** two "screeners" and two "workspaces" now exist.
8. **Storage:** 147 GB of stale equity history on the live disk — 765 M 1-minute bars, 162 M 5-minute and 2.6 M
   daily, last written 31 Jul by a hand-run script. It is stale, it is the largest thing on the disk, **and it
   is the only place 1-minute equity history exists** (`market15.db` holds 15-minute bars only). So it is an
   archive to move, not a file to delete. Its trading half (`orders`, `positions`, `signals`, `users`, all
   `virtual_*`, `autotrade_sessions`) is empty and can go. F&O grows ~425 MB a session (~100 GB a
   year) with no retention job running.
9. **Dead code**: `src/falcon/` calls four endpoints that do not exist.
10. **New checks are not in CI** (`check-screener.cjs`, `check-workbench-layout.cjs`).

Six more found by the server audit, in severity order:

11. **A point-in-time leak inside the "immutable" snapshots.** `snapshots.py:237` stores `iv_series` in the
    reading-T snapshot, and `_upto` clips only its `points`. The ATM contract pair is chosen from the *session's
    last* reading's spot (`derivatives.py:2932` → `_atm_pair`), and the `latest_*` / `direction` /
    `previous_close_*` fields stay whole-session. So a stored reading from 10:00 contains a choice that depended
    on 15:30. Everything else in the snapshot (grid, contracts, spot, ATM) is correctly anchored. This is the one
    finding that contradicts a house rule, and it must be fixed before any backtest is built on these rows.
12. **`/api/derivatives/signal-noise` 500s when the snapshot worker is off.** `signal_noise.py:184` reads
    `reading_snapshots`, a table only the worker creates. With `PILOT_SNAPSHOTS=off`, or before the first tick,
    the route raises `no such table` and lands in the generic 500 handler.
13. **The module is Python 3.12-only by accident** — `signal_noise.py:400` uses a nested same-quote f-string
    (PEP 701). On 3.11 it is a syntax error and the module will not import; nothing pins a minimum version.
14. **Diagnosis is near-impossible in production.** No logging configuration in the served package, so every
    `LOG.info` is discarded under uvicorn; the 500 handler logs only the exception class name — no traceback, no
    path, no request id; there are no metrics of any kind; `/health` checks a DB round-trip and nothing else.
15. **Operational sharp edges:** the rate-limit table is never pruned and every check takes a SQLite write lock,
    so all auth traffic serialises; there is no migration framework (`create_all` + a pinned version); CORS
    allows only GET/POST while four admin routes are PATCH; the two `Derivatives` instances and the snapshot
    store are never closed at shutdown.
16. **`market_intelligence.py` (203 lines) is dead code**, alongside `src/falcon/`.

And six from the ingestion audit. The first two are live incidents, not debt:

17. **Equity ingestion is failing right now.** `market15.candles_15m` stops at 2026-09-21 15:15 — the whole
    2026-09-22 equity session is missing — and the last four `ingest_runs` (23 Sep, 01:18 → 03:05) each report
    **495 errors, 0 rows, status `partial`**. F&O captured the same day 26/26, so this is specific to the equity
    path. It is invisible because `scripts/install-services.ps1:37` registers that task with **no `--log-file`**.
18. **The retention clock is running against an empty survivor.** `daily_rollups` has **0 rows** and
    `candles_day` has **0 rows**, while retention deletes `snapshots` at 30 days and `candles_15m` at 180
    (`config.py:145-150`). The rollup is what is supposed to outlive the detail. As written, roughly 30 days
    after each session its raw F&O inputs are deleted and nothing takes their place. `backfill_progress` shows
    both `day`-timeframe runs ended in `error`.
19. **A second point-in-time leak, in the metrics writer.** `load_pcr_series` (`metrics.py:1871`) selects every
    `pcr_oi` for the whole calendar day with no upper bound, and `pcr_trend` compares first against last
    (`metrics.py:663`). Live sequential capture is safe because later marks don't exist yet — but
    `scripts/metrics_loop.py:60` processes pending marks **newest-first**, so on any catch-up or recompute an
    earlier mark's stored `pcr_trend` / `pcr_trend_change` can be derived from a *later* reading.
20. **Stored metrics can be rewritten in place.** `write_metric_rows` is `INSERT OR REPLACE`, and it has been
    used: `logs/recompute_seeded.log` shows 17 marks of 18 Sep rewritten on 19 Sep. A metric recomputed later
    reads today's `candles_15m`, so its baselines may differ from what was served live, with nothing recording
    the change except `fetched_at` moving.
21. **The scope in force on a past session is not reconstructable.** `sync_contracts` resets `in_scope` globally
    on every refresh (`store.py:241`), so we cannot say later which contracts we were watching that day.
22. **Two documentation contradictions:** `DERIVATIVES_STACK.md:26-28` says the three scheduled tasks were never
    registered — all three are running; and `db/kanida.db`, still referenced as the equity store in places, has
    been stale since 2026-07-31 (the live store is `db/market15.db`).

---

## 6. Data-quality assessment

- **Strong:** every reading keeps its own contract set; the ATM rule is applied per reading; `captures` records
  run status; floors are applied at read time and stated on screen; the screener refuses to compare across a gap.
  The ingestion layer is better than I expected: it never invents a value (`None` for anything the vendor didn't
  send, no ₹0 bid for an empty book, Kite's ≤1971 "no tick" sentinel dropped), it records provenance per row
  (`snapshots.source` separates 1.67 M measured quotes from 182 k candle rebuilds; `spot_source` likewise), it
  keeps a candle-derived VWAP in a separate `average_price_est` column, it returns a *status* rather than a
  number when a figure can't be computed, it says "no baseline" below three sessions, and a missing bar keeps its
  slot marked `gap: True` instead of letting neighbours touch. Baselines are explicitly `date < session`.
- **Missing:** per-row receive time. `snapshots.fetched_at` was retired in favour of a per-mark ledger, so on a
  new-schema store we know when a *mark* was captured, never when an individual contract's quote arrived; and
  `underlying_snapshots` carries no vendor timestamp at all. That is exactly the field Phase 0 needs.
- **Weak:** we cannot separate a *vendor* delay from *our* delay; per-field completeness is inferred, not stored;
  there is no automated day-over-day or surface-vs-surface reconciliation; a session rebuilt from candles carries
  no spot, which the tab works around by choosing another reading; and the IV line is re-solved from scratch on
  every request with the session's *last* spot choosing the contract pair, so the same historical reading can
  render differently at 10:00 and at 15:30.
- **Only four sessions of F&O history exist.** The moat asset is four days old, which makes retention,
  reliability and backup worth more this quarter than any new feature.

---

## 7. Target architecture

The pipeline you drew already exists in outline. What is missing is a trust layer under it and an event layer
over it. Keep the shape; add the two layers and one rule.

```
VENDOR (Kite today · GDF evaluated)
   ↓  ingest        market_data/derivatives/capture.py        + vendor/received/processed stamps
   ↓  normalise     instruments + contracts                   + completeness per field
   ↓  QUALITY GATE  (new) data_quality service                 fresh | delayed | partial | stale | absent
   ↓  IMMUTABLE SNAPSHOT   db/derivatives.db metrics + var/intelligence.db reading_snapshots
   ↓  DERIVED       metrics worker (OI, PCR, max pain, IV, Greeks)
   ↓  TRADABILITY   (new) floors + spread + impact + ban/ASM/GSM
   ↓  STATE ENGINE  server/engine/run_engine.cjs  ← the one runtime, shared with the browser
   ↓  EVENTS        (new) transitions: appeared · broadened · lead changed · faded · ended
   ↓  PERSISTENCE   reading_snapshots (immutable) + revisions (new)
   ↓  OUTCOMES      signal_noise (exists) → verified timeline (new surface)
   └→ SCREENER · ALERTS · WORKSPACE · AI · BACKTEST · STRATEGY LAB
```

**Two rules.**

1. *No product surface computes a market fact.* The browser renders what the engine wrote. Three things break it
   today — build-up classification living in three places, the staleness verdict on the client, and the signal
   pane recomputing the live tail.
2. *A stored reading contains only what was knowable at that reading.* One thing breaks it today: the IV series
   written into each snapshot picks its ATM contract from the session's closing spot (debt item 11). Nothing may
   be backtested on these rows until that is closed.

Those are the first repairs, not a rewrite.

### Canonical snapshot (extend what exists, do not replace)

`reading_snapshots` already holds `session · reading_at · underlying · expiry · spot · atm_strike · contracts[] ·
reading · chained · engine_version · rules_version · previous_snapshot_id`. Add:

```
data_quality {state, data_age_s, vendor_ts, received_ts, processed_ts, completeness, missing_fields[]}
tradability  {liquid, spread_bps, impact_cost_bps, ban, asm_gsm, lot_size}
attribution  {side, method, confidence, why_not}        # capability 11, honest by default
revision     {revision_of, reason, issued_at}            # capability 19; never an in-place edit
```

Everything else in your canonical model (price/oi/iv/volume/bid/ask/greeks/pcr/max_pain/leading_strike/breadth/
market_state/state_change/supporting/conflicting/persistence) is already present in the metrics row or the stored
reading. The work is to serve one object rather than eleven routes assembling their own.

---

## 8. Dependency map (what must not be reordered)

```
Data trust (2) ──┬─► State engine (1) ──► Aggressor (11) ──► Screener (3) ──► Alerts (4,17) ──► Timeline (7)
                 ├─► Tradability (12) ──► Virtual fills (13) ──► Realisable P&L (16) ──► Strategy lab (9)
                 └─► Reliability (15) ──► everything user-facing
Immutable snapshots ──► Options backtest (8) ──► Experiment governance (18) ──► Public record (19)
Identity (14) ──► saved scanners/workspaces (3,5) ──► pricing (21)
i18n (20) touches every surface, so it lands before the surface count grows, not after
```

---

## 9. The build sequence

Two of your phases are already done. That changes the order.

| Phase | Name | Capabilities | Why here |
|---|---|---|---|
| **0** | **Keep the session, prove the data** | 15, 2 | Four days of history exist and the host sleeps. Nothing above this is worth building until a session cannot be lost and every number can state its age |
| 1 | The market truth, widened | 1, 11, 12 | Attribution and tradability raise the quality of the screener and alerts already shipped |
| 2 | The chain people understand | 6, 22 | The most-praised surface in the category, plus the docs that make it readable |
| 3 | Alert anything | 17, 4 | One contract, any metric; this is what lets a trader stop watching |
| 4 | The record | 7, 19 | The timeline and the correction log — the moat, made visible |
| 5 | Honest testing on our own history | 8, 18 | Backtest a *state* or a *screener condition* over immutable snapshots |
| 6 | Strategy and realism | 9, 13, 16 | Payoff, margin, realistic fills, realisable P&L |
| 7 | Reach and commerce | 10, 20, 21, 14 | Adaptive depth, Hindi, public pricing, explore-before-signup |

Already shipped: **3 (screener)** and **5 (workspace)**; **22** partially, through the Derivative disclosures.

---

## 10. Phase 0 — the design, in full

**Problem.** The product's only irreplaceable asset is the session history, and today it depends on a laptop in
Pacific time staying awake overnight. On 18 Sep it slept at 11:30 IST and the session was lost. Equity ingestion
has been failing silently since the 22nd (debt 17). Retention is set to delete raw F&O inputs into a rollup
table that has never been written (debt 18). And no number on screen can say how old it is in a way the server
owns.

**Before the phase: two things to stop today.** Turn retention off until `daily_rollups` is actually being
written — one config change buys back the deadline. Give the equity task a log file and find the 495 errors.
Neither is a build; both are losses in progress.

**Existing implementation.** `market_data/derivatives/capture.py` + the metrics worker write every 15 minutes;
`captures` records run status; `snapshots.py` writes immutable readings through the shared engine; the top bar has
a data-age pill; `CLOUD_MOVE_CHECKLIST.md` already documents what the container needs.

**Proposed architecture.**
1. **Move ingestion off the desk.** One always-on `ap-south-1` instance runs capture, metrics and the snapshot
   engine on a schedule, writing to its own disk with nightly snapshots to S3. The laptop becomes a development
   machine. Until the move completes (days, not weeks), the laptop is hardened: no sleep, a watchdog that restarts
   a missed capture, and a failure alert to the owner.
2. **A quality gate between ingest and snapshot.** Each reading is stamped with vendor, received and processed
   times, a completeness score, and the missing fields. The state becomes one of fresh, delayed, partial, stale or
   absent — computed **once, on the server**.
3. **A 09:15 gate.** A first-class check: vendor reachable, instruments refreshed, first capture written, ATM
   resolved, engine answered, API serving. It runs at 09:12 and again at 09:17, and pages on failure.
4. **A public status page** fed by the same checks, with incident history.
5. **Close both point-in-time leaks.** (a) Debt 11: the IV object written into a snapshot picks its ATM pair
   from the spot at that reading and carries only that reading's derived fields. New `engine_version`, so
   corrected rows sit beside the old ones rather than over them — which the store already supports. (b) Debt 19:
   bound `load_pcr_series` at the mark being computed, so `pcr_trend` cannot see a later reading on a catch-up
   run; then recompute the affected stored rows *as a revision*, not in place.
6. **Make a recompute honest** (debt 20): `INSERT OR REPLACE` on `metrics` becomes a revision — the new row
   carries what changed and why, and the old one survives. Same rule as `reading_snapshots`, which already has
   no UPDATE path.
7. **Retention, made safe** (debt 18). Write `daily_rollups` and `candles_day` first, verify a rollup can
   reproduce the figures we served, and only then let the prune run. F&O grows ~425 MB a session. Keep raw
   snapshots 90 days, metrics 400 days, readings forever; drop the 36.79 GB of duplicated indexes and the stale
   147 GB store to cold storage rather than deleting it — it is stale, but it is our only 1-minute equity
   history. Record the scope in force per session (debt 21) so a past day can be read
   back with the contracts we were actually watching.
8. **Make failures visible** (debt 12–14, 17): logging configuration in the served package, tracebacks and
   request paths on 500s, a `/health` that actually checks the scanner, the derivatives store, worker liveness
   and index freshness, a log file on every scheduled task, and a minimum Python version pinned in CI. A job
   that writes 0 rows and 495 errors four times running must page someone.

**Data model.** `data_quality` on the reading; `capture_health` per run per instrument; `incidents` for the
status page.

**Backend work.** Stamps in the capture worker; the quality service; `/api/data-status` serving the server's own
verdict; the 09:15 gate job; the status endpoint; the retention job; S3 backup; the IV anchoring fix; the
logging and health repairs above.

**Frontend work.** The data-age pill reads the server verdict instead of computing it; every widget header shows
age and turns amber then red; a "why is this stale?" line that names vendor delay vs our delay.

**Migration.** Existing readings keep their single timestamp and are marked `quality: legacy` rather than
back-filled with invented stamps.

**Scale.** At 1,000 users this is one writer and a read replica. At 100,000, capture is unchanged (it is
per-instrument, not per-user) and the read path moves behind a cache keyed by reading. Ingestion cost is flat in
users, which is the reason to separate it now.

**Failure modes.** Vendor silent; vendor wrong; host down; disk full; clock skew; a capture that half-succeeds.
Each must produce a *service state*, never a market statement — a rule the screener already follows.

**Testing.** Unit tests for the quality states; a replay test that feeds a delayed and a partial session; a
chaos test that kills the capture mid-run and asserts the UI says "not captured" rather than showing a stale
number; the 09:15 gate tested against a simulated late vendor; and a **no-look-ahead test** that rebuilds a
stored session reading by reading and asserts each snapshot is byte-identical whether the day ended or not.

**Definition of done.** A session cannot be lost by the host sleeping; every number on screen states its age from
a server verdict; no stored reading depends on a later one; 09:15 is verified daily and visible publicly; a week
of backups can be restored; the retention job holds the disk flat.

---

## 11. Later phases, in brief

Each gets its own full design before code, in the same shape as Phase 0.

- **Phase 1** — widen the state engine beyond ATM ±5; add aggressor attribution from bid/ask/volume/OI with an
  explicit "cannot be determined" state; add spread, impact cost and ban/ASM/GSM to tradability so the screener
  stops surfacing untradeable contracts.
- **Phase 2** — the chain: ΔOI encoded as bars in the row, per-strike PCR, Greeks in the chain, attribution
  colouring, and a docs page per capability reachable from it.
- **Phase 3** — the generic alert contract: any structured metric or state transition becomes an alert in one
  interaction, with email and web push, deduplicated on transition, and the 15-minute job registered.
- **Phase 4** — the verified timeline as a product surface, plus revisions: a corrected reading is a new row that
  points at the old one, and both stay visible.
- **Phase 5** — backtest a state or a screener condition over our own immutable snapshots, with costs and a
  variant counter that records how many things were tried.
- **Phase 6** — payoff, margin, what-if against the chain; a fill engine that uses the far touch, size and
  spread; realisable P&L everywhere a number is shown.
- **Phase 7** — depth switching (beginner / experienced / raw), Hindi, a public pricing page, and analytics
  without an account.

---

## 12. Testing and live-market validation

- **Every phase:** unit + integration, then a live session traced end to end — vendor → ingestion → snapshot →
  calculation → intelligence → API → screen — with one number chosen at random and traced back to its contract.
- **Replay:** the immutable snapshots make this cheap. Re-run a stored session and assert the engine's output is
  byte-identical for the same `engine_version`.
- **Reconciliation:** a daily job compares what the API serves against what the store holds, and mobile against
  web, and files a discrepancy rather than waiting for a user.
- **Regression:** the three existing CI checks plus the two new ones (screener parity, layout) run on every push.

---

## 13. Scaling, infrastructure and cost

- **Ingestion is flat in users; the read path is not.** Cache by reading id; one request per reading per
  instrument serves everyone.
- **Infrastructure:** one always-on ingestion host, one app host, S3 for backups, all in `ap-south-1`. The ECS
  template exists; the image needs Node added.
- **AI cost:** there is no LLM today, which is why capability 10 must arrive metered. The deterministic summary
  stays the default; a model, if added, explains a **precomputed** state and is capped per user per day.
- **Data cost and rights:** the biggest commercial risk. The pipeline runs on a broker connection today and a
  vendor trial expires 23 Sep. Redistribution, historical rights and API resale must be settled in writing before
  any data or API product is designed. PHASE0_TRACKER already records "options chain out of scope (NSE data
  licence)" — that constraint changed when the Derivative tab shipped, and the licence position must be confirmed.

---

## 14. What we should NOT build now

Volatility surface · portfolio intelligence · historical Greeks products · public API or webhooks · scripting ·
multi-account · advanced order flow · multi-expiry analysis · creator publishing · marketplace · execution,
baskets, rolling, partial fills, advanced autotrading. Also: no second screener, no second workspace, no LLM in
the critical path, and nothing that requires a broker connection before the intelligence is trusted.

---

## 15. The recommendation

**Stop the two losses today. Then start Phase 0, and build nothing else until it is done.**

The two losses, both found in the audit and both in progress right now: equity ingestion has written nothing
since the 21st and says so only inside its own database, and the F&O retention job is configured to delete raw
sessions into a rollup table that has never received a row. Neither needs a plan approved — they need a config
change and a log file.

The reasoning is not architectural, it is commercial. Everything the research documents describe as the moat —
"the only product that keeps a verified record", "AI that explains what changed", "trust through proof" — rests
on a history that is four sessions old and lives on a laptop that has already slept through a market open once.
Two of the twenty-two capabilities are about that foundation (2 and 15) and they are the two that make the other
twenty worth anything. A backtest over data we cannot vouch for is worse than no backtest.

Phase 0 is also the cheapest phase we will ever run: no new product surface, no new vocabulary for users to
learn, no design round. It is stamps, a gate, a host, a retention job and one point-in-time fix.

**Three decisions I need from you before I write a line of it:**

1. **Host.** Move ingestion to an always-on `ap-south-1` instance now, or harden the laptop first and move next
   week? I recommend hardening today (an hour) and moving within the phase, so tomorrow's session is safe either
   way.
2. **Vendor.** The GDF trial key expires tomorrow, 23 Sep 23:59 IST, and the trial never had live quotes
   enabled — only bars, 100 NFO instruments, one session per key. Phase 0's timestamps differ depending on
   whether we stay on the broker connection or move to a vendor feed, so this is a decision for this week, not
   next. Nothing GDF has touched is in the production store; the shadow database stays separate either way.
3. **Rights.** Before any of this is sold, I need the licence position in writing — redistribution, history,
   and API resale. `PHASE0_TRACKER.md` still says the options chain is out of scope for exactly this reason, and
   the Derivative tab shipped anyway.

**Still open from the last round, unchanged:** `scripts/apply-workspace-hooks.py --apply` after 15:30 IST — it
is also what mounts the screener and workspace routers into `create_app`, which is why both are currently
reachable only in the dev server and in tests. Then `start-pilot.ps1 -Build`, and optionally
`register-screener-task.ps1`.

## 16. What I did not do

I audited; I changed nothing. No file under `src/derivative/` or the named server modules was touched, no commit
was made, no running service was restarted, and the vendor connection was left alone. The one exception is this
document.
