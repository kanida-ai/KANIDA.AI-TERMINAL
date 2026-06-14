# Kanida.AI Launch — STATUS BOARD

Updated: 2026-06-13 IST · Branch: `feat/power-user-portal`

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

**BUILD PHASE COMPLETE — all 8 modules built + audited GREEN. Next phase: deploy (operator credentials + punch list).**

## Dependency order
M1 + M2 → (M3, M4, M5) → M7 · M6 + M8 independent leaves.

## Carry-forward items (tracked, not blocking the GREEN above)
- **R1 (HIGH):** `kite_tokens` is read/written via raw `sqlite3` in `backend/services/kite_auth.py` (bypasses `DATABASE_URL`). On a Postgres-only host the Kite token never reaches Supabase → live tier breaks. Gated by RUNBOOK §7.4 (a broken deploy cannot pass), but the real fix (route `kite_auth` through `db.py` Postgres path) must be done before go-live. **Touches the token path SHARED with Auto-Trade (7 files) — must preserve `kite_auth` public API (INV2).** Own carefully-audited sub-task before deploy.
- **Handoff:** M1 porting must land `power_user_users.id` as BIGINT in Supabase so M2's `BIGINT user_id` FKs type-match.
- Doc hygiene: stale docstrings calling `falcon_auth_log` a legacy-DB table (it's in the App DB).

## Round log
- 2026-06-13: spec approved; CONTRACT/ENV/STATUS written; dispatched M1 + M2 build agents.
- 2026-06-13: M1 GREEN, M2 GREEN. Audits stored in `docs/launch/audits/`. R1 logged as pre-go-live sub-task.
- 2026-06-13: M3 GREEN, M4 GREEN, M5 GREEN. Backend core complete. Remaining: M6 Legal, M7 Frontend, M8 Email.
- 2026-06-13: M6 GREEN, M7 GREEN, M8 GREEN. **ALL 8 MODULES DONE.** Build phase complete; entering deploy phase.
  - M6 added `/legal` to the middleware Basic-Auth public bypass (audited: does NOT expose operator routes). Legal pages are DRAFT — need lawyer review + placeholder fill ([LEGAL ENTITY NAME], CIN, address, support email, grievance officer).

## Deploy phase (operator-gated — nothing here is auto-done)
1. Close C2 (R1 kite_tokens → Postgres path) as its own audited sub-task — INV2-preserving.
2. Close C1 (webhook dedupe Postgres error type) at the cutover.
3. Add C3 acceptance tests; run them.
4. Operator credential steps (RUNBOOK_deploy.md): provision Supabase, set POWER_JWT_SECRET + Razorpay keys + Resend key, run porting script, deploy off-laptop, verify checklist.
5. Lawyer review of legal pages + fill placeholders.
6. Razorpay ₹999/mo plan created in dashboard.

## Pre-go-live punch list (close before deploy, NOT blocking GREEN)
- **C1 (from M3+M1):** webhook dedupe catches `sqlite3.IntegrityError` only — broaden to Postgres `IntegrityError` (or pre-check) at the M1/M2 Postgres runtime cutover. Latent until cutover; worst case is a Razorpay retry, no data corruption / no false grant.
- **C2 (R1 from M1):** route `kite_auth` (`kite_tokens`) through the Postgres `db.py` path — touches shared Auto-Trade token path, own audited sub-task, INV2-preserving.
- **C3 (tests):** M3/M4/M5 shipped without unit tests (no Python in build env). Add acceptance tests before go-live: webhook forged-sig→400 / dup→single-apply / charged→paid / cancelled→blocked; paywall 8 allow/deny cases; signup comp/blocked/duplicate-409.
- **C4 (cosmetic):** stale docstrings — `falcon_auth_log` "legacy DB" (it's App DB); `persona_backtest_router:14` "all public except /refresh".
