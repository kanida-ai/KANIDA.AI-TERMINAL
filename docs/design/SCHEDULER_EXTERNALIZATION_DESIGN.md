# Scheduler Externalization — Multi-Replica Design (Fargate `app_desired_count` > 1)

Status: DESIGN ONLY (read-only investigation). No backend code changed.
Author: AutoTrade Sessions engineer. Date: 2026-07-17 IST.
Prod tree analyzed at HEAD `27709ec` (branch `prod`). Every claim below is
backed by a file:line read during this investigation.

> Read alongside: `autotrade_system_map` + `autotrade_portfolio_build` memories,
> `cloud_migration_handoff` (scheduler externalization is the cited blocker to
> `>1` replica), and `docs/design/AUTOTRADE_SCALABLE_ARCHITECTURE.md`.

---

## 0. TL;DR (the headline the operator asked for)

- **The ladder scheduler is ALREADY multi-replica-safe** at the money level. The
  atomic per-ladder+day claim (`LadderCampaign._claim_day`, `ladder.py:430`) means
  N replicas can each tick and **exactly one basket per ladder per day** opens. The
  60s wake-loop itself needs **no** single-fire coordination for correctness — only
  a redundant-compute optimization is optional. This is the proven seed.

- **Kill / entry / per-symbol-exit ACTIONS are ALREADY cross-process-guarded** by a
  compare-and-set + lease authority (`durable_claims.py`, backing `fire_guard`,
  `exit_gate`). Two drivers on one session cannot double-kill, double-enter, or
  double-exit a leg — the durable claim single-fires the action. This is a much
  larger head-start than the task assumed.

- **What genuinely fails at 2 replicas TODAY, unguarded:**
  1. **The 16:05 EOD pipeline scheduler** — guarded ONLY by an in-process
     `threading.Lock` (`main.py:47`). Two replicas = two pipeline runs = **two
     pre-market stagings = potential DOUBLE REAL ORDERS at 09:15**. This is the
     sharpest edge and it is unguarded.
  2. **Magnifier / BTST second-leg completion on resume** — guarded by a
     read-then-act DB flag (`session.py:4808`), NOT an atomic claim → two replicas
     resuming can both place the 09:16 leg.
  3. **Redundant per-session drivers + un-claimed write paths** — two `tick_driver`
     / `ws_driver` loops per session (per-process `_DRIVERS` dicts), doubled
     mark-to-market/trail persistence, doubled reconcile/GTT backfill. Not
     double-*money* on the claim-guarded paths, but wasted compute, racy persisted
     state, and any un-audited write path is a latent double-action.

- **The hardest genuinely-unsolved piece** is exactly what the task predicted:
  **which replica owns a given session's in-memory drivers.** A DB row cannot be
  "claimed" into a thread — you need **session-ownership sharding** (a durable
  owner-claim + heartbeat lease that gates `start_for_session`).

- **THE GATING CAVEAT (say it plainly):** every atomic-claim guarantee above —
  `_claim_day`, `durable_claims`, and any new claim — depends on **one shared
  transactional database**. Today AutoTrade persists to **SQLite via
  `falcon.db.falcon_conn`** (`durable_claims.py:27`, `db_migrations.py:47`), which
  is a **local file**. Two Fargate tasks have **separate filesystems** → no shared
  SQLite → **none of the claims coordinate across replicas.** SQLite over EFS/NFS
  is unsafe for concurrent writers. Therefore **true multi-replica exactly-once is
  gated on the Postgres cutover** — and note that AutoTrade's DB layer
  (`falcon/db.py`) is **not yet Postgres-aware** (the `IS_POSTGRES` scaffolding
  lives in a *different* module, `backend/db.py`). `app_desired_count` **must stay
  1** until (a) AutoTrade is on shared Postgres and (b) session-ownership sharding
  ships. What CAN be built and tested on the laptop today is listed in Phase A.

---

## 1. The three mechanisms — current single-process assumptions (file:line evidence)

### Mechanism 1 — the 16:05 IST EOD pipeline scheduler

| Concern | Evidence |
|---|---|
| Per-process daemon thread | `main.py:383` `threading.Thread(target=_schedule_daily_pipeline, daemon=True)` started inside the FastAPI `lifespan` (`main.py:341`) — one per process. |
| The timing loop | `main.py:202-319` `_schedule_daily_pipeline`: sleeps to 16:05 IST weekday (`main.py:290`), retries every `_PIPELINE_RETRY_INTERVAL_SEC` (15 min) until the 19:00 cutoff (`main.py:252-255`), boot-catch-up fires ~10s after a late boot (`main.py:268-278`). |
| The ONLY concurrency guard | `main.py:47` `_pipeline_lock = threading.Lock()`; `main.py:74` `_run_pipeline_sync` does `_pipeline_lock.acquire(blocking=False)`. **Process-local** — invisible to a second replica. |
| Why double-run costs money | `main.py:114-125` the pipeline calls `eod_orchestrator.run_eod(...)` → pre-market **staging** (QUEUED items) which `premarket_deployer` (`main.py:389-391`) fires at 09:15 as **real orders**. Two stagings → duplicate deploy candidates. |
| No durable guard exists | Verified: `grep durable_claims` / `pipeline:` in `main.py` + `falcon/jobs/_pipeline.py` → **none**. The pipeline path has zero cross-process idempotency. |

### Mechanism 2 — the ladder scheduler (`monitoring/ladder_scheduler.py`)

| Concern | Evidence |
|---|---|
| Per-process daemon thread | `ladder_scheduler.py:156-169` `start()` (module `_thread` singleton); armed per-process at `main.py:566`. |
| The wake loop | `ladder_scheduler.py:101-153` `_run`: aligned wake to 09:15 + 60s backstop poll; fires `ladder.tick_all_running(ref_now=now)` when `is_trading_day and now >= target` (`:138-139`). |
| **The atomic claim that already makes the WORK safe** | `ladder.py:430-455` `_claim_day`: `UPDATE autotrade_ladders SET last_tick_date=? WHERE ladder_id=? AND (last_tick_date IS NULL OR last_tick_date<>?)` + `rowcount == 1`. `daily_tick` calls it at `ladder.py:848-849`; only the winner runs `_spawn_child`. |
| Concurrency is a designed property, not luck | `ladder.py:1368-1387` `tick_all_running` fires ladders in **parallel** and its docstring states parallelism is sound *only* because `_claim_day` is atomic. |
| Already proven under test | `backend/tests/autotrade/test_ladder_concurrency.py`: `test_concurrent_daily_tick_same_ladder_opens_exactly_one_basket`, `test_claim_day_is_atomic_under_many_threads` (8 racers → 1 winner), plus a **mutation control** proving the pre-fix read-then-write double-opens. |

**Assessment:** the ladder scheduler's *money action* is already single-fire under
any number of concurrent ticks. The wake-loop being per-replica only causes each
replica to *attempt* the claim; the loser's UPDATE matches 0 rows and no basket
opens. **No additional coordination is required for correctness.** (One optional
optimization: leader-gate the loop so N replicas don't each do the redundant
query+claim work — cosmetic, not safety.)

### Mechanism 3 — boot-resume + per-process in-memory drivers

| Concern | Evidence |
|---|---|
| Resume runs on EVERY process start | `main.py:548` `resume_active_sessions()` + `main.py:565` `resume_active_ladders()` inside `lifespan`. |
| Resume re-arms per-session threads for ALL live sessions | `recovery.py:199-252` scans `RUNNING/SCHEDULED/KILLING/KILLING_INCOMPLETE` and re-arms `tick_driver` + `ws_driver` + square-off + magnifier/btst completion. |
| Driver registries are **per-process** | `tick_driver.py:53-54` `_DRIVERS: Dict + _LOCK = threading.Lock()`; `ws_driver.py:63-64` same. A `threading.Lock` coordinates threads in **one** process only. |
| In-memory action guards are per-process… | `fire_guard.py:34-37` `_FIRED` / `_ENTRY_CLAIMED` dicts; `exit_gate.py:57,66` `_INFLIGHT` set + lock. |
| …but they are **backed by a cross-process durable authority** | `fire_guard.py:59,84` → `durable_claims.claim("entry:{sid}"/"fire:{sid}", 6h)`. `exit_gate.py:95-104` `begin_exit_flight` → `durable_claims.claim(_flight_key, 300s)`. `durable_claims.py:53-79` is a SQLite CAS (INSERT-wins + expired-lease-takeover UPDATE) — table migrated at `db_migrations.py:892`. |
| Action paths that ARE durably guarded (verified callers) | Kill: `session.py:3971/4035/4118/4182/4569/5250`, `ws_driver.py:354/400/424`, `square_off_scheduler.py:160` all `with fire_guard.claim_fire(...)`. Entry: `session.py:2260` `fire_guard.claim_entry`. Per-symbol exit: `session.py:1207` `begin_exit_flight` inside `_exit_single_position`. |
| Action path that is **NOT** atomically guarded (gap) | `session.py:4800-4810` `complete_magnifier_entry` (and `complete_btst_entry`) guards only with `_magnifier_entry_complete()` (`session.py:4808`) — a **read-then-act** on a persisted flag, not a claim. Called from resume at `recovery.py:468/488`. |
| WS connection topology | `ws_driver.py:79` uses the **shared** `kite_ticker` (one WS per *process*, not per session). So 2 replicas = 2 Kite WS connections total (within Kite's ~3/api_key ceiling — *assumption, not verified against Kite docs here*), each independently evaluating kill on every session. |

---

## 2. What breaks at 2 replicas — per mechanism, concretely

1. **EOD pipeline:** both replicas hit 16:05 (or both do boot-catch-up) → both run
   `_run_pipeline_sync` (the `threading.Lock` is per-process, so it does not
   exclude the other replica) → **two pre-market stagings**. Downstream
   `premarket_deployer` can then deploy duplicated candidates at 09:15. **Double
   real orders. This is the worst failure in the set and it is currently
   unguarded.** (Some steps are internally idempotent — `kick_off_v7_pipeline_if_stale`,
   `_signals_fresh_for_now` — so signals may converge, but staging/deploy
   idempotency is **unverified** and must not be assumed on the money path.)

2. **Ladder scheduler:** both replicas tick → `_claim_day` lets exactly one win →
   **one basket. No money failure.** Cost: redundant queries/claim attempts on the
   loser. Safe as-is (on a shared DB — see §0 caveat).

3. **Boot-resume + drivers:**
   - Two `tick_driver` loops per session → doubled mark-to-market / `save_trail_state`
     (`session.py:4179`) writes (last-writer-wins; racy, can oscillate persisted
     trail/peak state) and doubled broker reconcile / GTT backfill on resume.
   - Two `ws_driver` loops → both evaluate the sub-second kill; a threshold breach
     makes both call `kill_switch.fire` → `claim_fire` → **exactly one flattens**
     (durable claim holds). **No double-kill.**
   - Two `_resume_scheduled` → both call `_fire_entries` → `claim_entry` →
     **exactly one entry basket.** **No double-entry.**
   - Two square-off schedulers → both fire at square-off → per-leg
     `begin_exit_flight` → **exactly one exit per leg.** **No double-exit.**
   - **Magnifier/BTST second leg (the real gap):** two `complete_magnifier_entry`
     races on the read-then-act flag → **both can place the 09:16 leg** (a
     duplicate real averaging order). This is the one resume path not behind an
     atomic claim.
   - Net: on the *claim-guarded* paths, 2 drivers ≠ double money — but you pay
     doubled compute, doubled broker API calls (rate-limit risk), racy persisted
     state, and you carry the magnifier/btst gap plus any un-audited write path.

---

## 3. Recommended coordination per mechanism (ranked simplicity + safety)

### Mechanism 1 — EOD pipeline

- **Rank 1 (best end-state, needs cloud infra): external trigger.**
  An EventBridge Scheduler / ECS-scheduled task hits an internal, auth-gated
  endpoint (e.g. `POST /internal/pipeline/run`) at 16:05 IST + retry cadence.
  Remove the in-process timing loop from replicas (gate it behind
  `SCHEDULER_IN_PROCESS`, default `true` for the laptop). The endpoint body still
  wraps the run in a durable day-claim so even a double-trigger single-fires. This
  takes timing **out of every replica** — the cleanest answer.
- **Rank 2 (buildable + testable on SQLite TODAY): durable day-claim.**
  At the top of `_run_pipeline_sync`, `durable_claims.claim(f"pipeline:{date}",
  lease≈4h)`; if lost, no-op and return. Keep the in-process loop. Additive,
  mirrors `_claim_day`, and **also hardens the existing single-process
  boot-catch-up double-fire window**. This is the same proven CAS pattern.
- **Recommendation:** build Rank 2 now (cheap, additive, laptop-testable), adopt
  Rank 1 at cutover. Note: Rank 2's cross-*replica* guarantee only materializes on
  a shared DB (§0); on the laptop it is validated with the shared-file harness and
  is immediately valuable as a single-process idempotency hardening.

### Mechanism 2 — ladder scheduler

- **No correctness work required.** Keep `_claim_day` as the authority.
- **Optional (post-cutover):** leader-gate or `SCHEDULER_IN_PROCESS`-gate the
  `_run` loop so only one replica does the redundant tick sweep. Cosmetic.
- Honest caveat: the safety is real **only on a shared DB**; on SQLite-multi-host
  it is not — but nothing is, and prod is single-process today.

### Mechanism 3 — boot-resume + drivers (the hard one)

- **Rank 1 (the real unlock — needs shared DB): session-ownership sharding.**
  Add a durable **owner-claim with a heartbeat lease**: each replica, on resume and
  on a periodic sweep, tries `durable_claims.claim(f"session_owner:{sid}",
  lease≈90s, holder=REPLICA_ID)`. **Only the owner** arms `tick_driver` /
  `ws_driver` / square-off / magnifier-btst completion for that session. A
  heartbeat thread renews the owner's lease (~every 30s). If an owner dies, the
  lease expires and another replica claims + re-arms — **this generalizes the
  existing boot-resume path into continuous failover.** Result: exactly one driver
  per session, automatic recovery. Requires: `REPLICA_ID` plumbing, a lease-renewer
  thread, and gating `start_for_session` behind ownership. Sound only on shared
  Postgres. `durable_claims` already provides the exact CAS+lease primitive — this
  is "the atomic DB-claim pattern generalized," with the necessary addition that
  **the claim gates a thread**, which the ladder pattern does not need.
- **Rank 2 (defense-in-depth, keep regardless of replica count): close the gaps.**
  The durable action-claims are the real-money floor even during a lease handover
  when two drivers transiently coexist. **Keep them.** Then:
  - Put `complete_magnifier_entry` / `complete_btst_entry` behind a durable claim
    (e.g. `entry2:{sid}`), replacing the read-then-act flag as the *authority*.
  - Audit every tick-loop write (mark-to-market, `save_trail_state`, GTT backfill,
    `reconcile_broker_positions`) and make each idempotent or owner-gated. **This
    audit is UNVERIFIED-safe today and is a Phase-A task.**
- **Recommendation:** ship Rank 2 gap-closure now (laptop-buildable+testable);
  design Rank 1 sharding for the Postgres phase — **it is the actual precondition
  for `app_desired_count > 1`.**

### On the candidate primitives (honest evaluation)

| Primitive | Verdict |
|---|---|
| **SQLite single-writer CAS** (`durable_claims`, `_claim_day`) | Proven, in use, correct — **but only within one shared DB file.** Useless across Fargate tasks with separate filesystems. Good for laptop + single-process hardening. |
| **Postgres advisory lock** | Ideal for the *loop-level singleton* (pipeline/ladder leader) post-cutover: `pg_try_advisory_lock`. Requires AutoTrade to be on PG first (not yet — `falcon/db.py` is SQLite-only). |
| **Postgres row CAS / `INSERT … ON CONFLICT`** | The natural port of `durable_claims` + owner-claim to PG. Note `durable_claims.py:72` catches `sqlite3.IntegrityError` specifically — the port is real work, not free. |
| **Redis lock** | In Phase-0 compose/terraform but **not wired**. Viable for leader election / owner leases, but adds a dependency PG already covers. Prefer PG unless Redis is needed elsewhere. |
| **Leader election** | Fine for the two *timed loops* (pipeline, ladder). Does **not** solve per-session driver ownership (that needs per-session sharding, not one leader). |
| **External trigger (EventBridge/ECS-scheduled)** | Best for the pipeline: removes in-process timing entirely. Does not help the drivers (they must run continuously in-process). |
| **Session-ownership sharding** | The **only** thing that solves in-memory driver ownership. A DB row alone can't be "claimed into a thread" — you claim ownership, then the winning process starts the thread and heartbeats the lease. |

---

## 4. Phased build plan

### Phase A — buildable + testable on the laptop NOW (SQLite, additive, default-off for new behavior)

- **A1. Pipeline durable day-claim.** Wrap `_run_pipeline_sync` in
  `durable_claims.claim(f"pipeline:{date}", ~4h)`; loser no-ops. + concurrency
  tests (2 concurrent → 1 run; wrong-day / already-claimed no-fire). Also hardens
  the current single-process boot-catch-up double-fire window.
- **A2. Close the magnifier/BTST second-leg gap.** Make `complete_magnifier_entry`
  / `complete_btst_entry` claim-gated (durable), keeping the flag as the fast path.
  + tests (2 concurrent completes → 1 leg-2 placement).
- **A3. Audit + harden un-claimed tick-loop writes** (mark-to-market,
  `save_trail_state`, GTT backfill, reconcile) — idempotent or owner-gated.
  + focused tests. (Scope: prove no double-*action*; racy last-writer-wins on pure
  display fields may be acceptable but must be documented.)
- **A4. Plumb `REPLICA_ID` + a `SCHEDULER_IN_PROCESS` env** (default `true`, no
  behavior change) so cloud can later flip per-replica timers off and stamp claim
  holders. Additive only.
- **A5. Shared-DB test harness:** point two in-test "replicas" at ONE temp SQLite
  file (WAL + 60s busy timeout, exactly `falcon_conn`'s config at `falcon/db.py:22-30`)
  to simulate multi-replica on the laptop — the closest available proxy for the
  Fargate shared-PG topology.

### Phase B — Postgres cutover (shared transactional DB)

- **B0. Make AutoTrade Postgres-aware.** Today `falcon/db.py` `connect_falcon`
  (`:15-31`) is pure `sqlite3`; the `IS_POSTGRES` scaffolding is in
  `backend/db.py` (`:45`), a different layer. AutoTrade (`durable_claims`,
  `ladder`, `session`, migrations) all import `falcon.db.falcon_conn`. **This
  routing gap must be closed before any AutoTrade claim coordinates across
  replicas.** (FLAG: this is prerequisite work not yet scoped in this doc.)
- **B1. Port `durable_claims` + `_claim_day` to Postgres** (`INSERT … ON CONFLICT`
  / row CAS, or `pg_try_advisory_lock` for the loop singletons). Re-validate CAS
  semantics are identical (the SQLite `IntegrityError` branch must map to psycopg).
- **B2. Session-ownership sharding** (owner-claim + heartbeat lease + owner-gated
  `start_for_session` + failover). The precondition for `app_desired_count > 1`.
- **B3. External trigger** for the pipeline (EventBridge → internal endpoint),
  `SCHEDULER_IN_PROCESS=false` on replicas; leader-gate (or leave, it's claim-safe)
  the ladder loop.

### Phase C — cloud validation

- Bump `desired_count` to 2 in **staging** against shared PG; run the exactly-once
  suite; chaos-kill the driver owner and prove failover re-arms with no double-fire;
  only then consider prod `app_desired_count` > 1. Mid-market-hours only on explicit
  operator instruction with the prior task-def as rollback.

---

## 5. Exactly-once test strategy (mirror the ladder atomic-claim tests)

Reuse the proven shape in `test_ladder_concurrency.py`: N threads through a
`threading.Barrier`, each independently loading state, all racing one guarded
action against **one shared DB**, then assert exactly one side-effect — **plus a
mutation control** that restores the unsafe (read-then-write) shape and proves it
double-fires (so the safety test fails for a real reason).

- **Pipeline (A1):** 2 concurrent `_run_pipeline_sync` (or 2 `claim("pipeline:date")`)
  → assert exactly one wins; assert staging ran once (count QUEUED rows). No-fire:
  wrong day / already-claimed → 0 runs. Mutation control: strip the claim → 2 runs.
- **Magnifier/BTST (A2):** 2 concurrent `complete_magnifier_entry` → assert
  leg-2 placed once (order count / blended qty). Mutation control: flag-only guard
  → 2 placements.
- **Session ownership (B2):** 2 resume passes with 2 `REPLICA_ID`s → exactly one
  arms the driver (assert via the **ownership predicate / claim holder**, not the
  per-process `_DRIVERS` dict). Failover: expire the owner lease → the second
  replica claims + arms; assert no double-fire during handover.
- **Regression floor:** re-run existing `fire_guard` / `exit_gate` /
  `test_durable_claims_cluster8` fire/no-fire tests to prove the kill/entry/exit
  floor still holds after the additions.

**Honest limitation of the laptop suite:** a single shared SQLite file is an
*approximation* of the Fargate shared-PG topology. The CAS **predicate semantics**
transfer, but SQLite's single-writer serialization ≠ Postgres MVCC. Every
exactly-once guarantee **must be re-validated on Postgres in Phase C** before
`app_desired_count` > 1. Do not treat green laptop tests as proof of multi-host
safety.

---

## 6. Assumptions & unknowns (flagged)

- **[GATING]** All claim coordination assumes ONE shared transactional DB. On
  SQLite-per-task it does not hold. Multi-replica exactly-once is gated on the
  Postgres cutover. `app_desired_count` stays 1 until B0+B2 ship and pass Phase C.
- **[FLAG]** AutoTrade's `falcon.db.falcon_conn` is **SQLite-only** today; the
  `IS_POSTGRES` path is in a separate module (`backend/db.py`). Routing AutoTrade
  onto shared Postgres (B0) is unscoped prerequisite work.
- **[UNVERIFIED]** Pre-market **staging/deploy idempotency** across a double
  pipeline run — I did not trace `eod_orchestrator.run_eod` / `premarket_deployer`
  to confirm whether a second staging produces duplicate deploy candidates. Treat
  as unsafe until proven; A1 makes it moot.
- **[UNVERIFIED]** Full idempotency of every tick-loop write path under two drivers
  (mark-to-market, trail persist, GTT backfill, reconcile). A3 is the audit.
- **[ASSUMPTION]** Kite allows ≥2 WS connections per api_key (2 replicas = 2 shared
  tickers). Believed ~3/api_key; not verified against Kite docs here — a ceiling on
  replica count regardless.
- **[SCOPE]** `durable_claims.py:72` catches `sqlite3.IntegrityError` specifically;
  the Postgres port (B1) is real work, not a config flip.
