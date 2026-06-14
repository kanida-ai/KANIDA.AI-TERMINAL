# Kanida.AI Launch — STATUS BOARD

Updated: 2026-06-14 IST · Branch: `feat/power-user-portal`
**Source of truth for the deploy/cloud plan: [CLOUD_ARCHITECTURE.md](CLOUD_ARCHITECTURE.md).** (`DB_DEPENDENCY_MAP.md` is a supporting, code-traced reference — not the plan.)

Legend: ⬜ queued · 🔨 building · 🔎 auditing · 🟥 RED (fix needed) · 🟩 GREEN (done)

| Module | What | State | Audit |
|---|---|---|---|
| M1 | Infra — Supabase + porting script + deploy config + runbook | 🟩 | GREEN |
| M2 | DB — billing migration (Postgres) + rollback | 🟩 | GREEN |
| M3 | Billing — Razorpay service + router | 🟩 | GREEN |
| M4 | Paywall — `current_paid_user_required` + wire 5 routers | 🟩 | GREEN |
| M5 | Signup — open email signup (code→comp / none→blocked) | 🟩 | GREEN |
| M6 | Legal — Terms/Privacy/Refund/Risk pages | 🟩 | GREEN |
| M7 | Frontend — pricing/signup/billing pages | 🟩 | GREEN |
| M8 | Email — welcome/receipt/cancel | 🟩 | GREEN |

**BUILD PHASE COMPLETE — all 8 modules built + audited GREEN. Next: deploy via the 4-phase plan below (Phase 1 = off-laptop + launch on SQLite-volume).**

## Dependency order (build phase)
M1 + M2 → (M3, M4, M5) → M7 · M6 + M8 independent leaves.

> **Note on the build-phase carry-forwards (R1, BIGINT FK typing, webhook dedupe):** these were all *Postgres-portability* concerns. Under the re-scoped plan they are **Phase 4**, NOT pre-launch — Phase 1 runs on SQLite, where they don't apply. See "Carry-forwards (re-scoped)" below. The only genuinely pre-launch code item is **C3 (tests)**.

## Round log
- 2026-06-13: spec approved; CONTRACT/ENV/STATUS written; dispatched M1 + M2 build agents.
- 2026-06-13: M1 GREEN, M2 GREEN. Audits stored in `docs/launch/audits/`. R1 logged as pre-go-live sub-task.
- 2026-06-13: M3 GREEN, M4 GREEN, M5 GREEN. Backend core complete. Remaining: M6 Legal, M7 Frontend, M8 Email.
- 2026-06-13: M6 GREEN, M7 GREEN, M8 GREEN. **ALL 8 MODULES DONE.** Build phase complete; entering deploy phase.
  - M6 added `/legal` to the middleware Basic-Auth public bypass (audited: does NOT expose operator routes). Legal pages are DRAFT — need lawyer review + placeholder fill ([LEGAL ENTITY NAME], CIN, address, support email, grievance officer).

## Deploy phase — RE-SCOPED to 4-phase SQLite-volume-first (see CLOUD_ARCHITECTURE.md)

**Decision 2026-06-13:** do NOT lead with Supabase/Postgres. The codebase uses direct `sqlite3`
everywhere, so Postgres is a code refactor, not a data move. Lift-and-shift on SQLite-on-a-volume
first → gets off the laptop + launches billing with minimal risk. Postgres = Phase 4 (scale trigger).
This DEFERS C1/C2/C6 (the Postgres-portability snags) to Phase 4 — they don't block launch.

### PHASE 1 — off the laptop + LAUNCH (SQLite on cloud volume)
1. Deploy backend + **whole 573M production DB (as-is, no pruning)** + daily jobs to a cloud host with a **persistent volume**. 14G research DB stays on the laptop.
2. Copy `falcon_outcomes` (827k) into the cloud DB + repoint the evidence read → no request hits the 14G research DB.
3. Daily jobs (OHLC fetch → features → signals → portfolio EOD) run in cloud; verify Playwright headless + Kite token (stays in the SQLite volume file — NO C2 fix needed on SQLite).
4. ✅ **DONE (P1PUB, audited GREEN 2026-06-14).** Laptop→cloud publish transport built: `POST /api/falcon/publish/intelligence` (self-contained auth via `X-Publish-Secret`, fail-closed; atomic single-txn full-replace; allowlisted tables; empty-guard) + `scripts/publish_to_cloud.py` (mirrors publish_patterns cutoff/selection, `--dry-run`). Build log + audit in `docs/launch/`. Pre-go-live: run `backend/falcon/tests/test_publish_router.py`; set `FALCON_PUBLISH_SECRET` on the cloud host (else endpoint 503s by design).
5. Operator: stand up host + volume, set `POWER_JWT_SECRET` + Razorpay keys + Resend key, point DNS.
6. Lawyer review of legal pages + fill placeholders. Razorpay ₹999/mo plan created.
**→ Billing/paywall/signup (already built, SQLite-ready) go live here. Laptop loss ≠ product down.**

### PHASE 2 — materialize request-time reads (post-launch optimization)
7. Build summary tables (`pattern_stock_evidence`, `stock_signal_evidence`, `persona_backtest_*`); switch `/power/today` evidence (L1+L2) and `/api/power/personas/*` (L3) to read them.

### PHASE 3 — prune production DB (cost optimization)
8. After Phase 2 proves no request needs deep history: prune cloud `ohlc_daily`/`falcon_features` to ~300 days.

### PHASE 4 — optional Postgres/Supabase (scale trigger only)
9. Trigger = need multiple backend instances OR managed PITR backups. Then refactor DB-access layer to Postgres; use the already-built `scripts/migrate_to_supabase.py` + `migrations/0001_billing.sql`. Close C1 (webhook dedupe PG error type), C2 (kite_tokens → PG), C6 (publish → PG) HERE.

## Carry-forwards (re-scoped)
- **C1 / C2 / C6** → moved to **Phase 4** (Postgres-only concerns; irrelevant on SQLite-volume). Kept on record in CLOUD_ARCHITECTURE.
- **C3 (tests):** still pre-launch — add acceptance tests (webhook forged-sig→400 / dup→single-apply / charged→paid / cancelled→blocked; paywall 8 allow/deny; signup comp/blocked/409).
- **C4 (cosmetic):** stale docstrings.
- **Built Postgres artifacts** (`migrate_to_supabase.py`, `0001_billing.sql`) = Phase-4 shelf items, not wasted. Billing runs on SQLite today via `db_init.py`.
