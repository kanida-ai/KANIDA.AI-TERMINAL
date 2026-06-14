# M3 — Billing (Razorpay) — Audit

**Agent:** AuditAgent-M3
**Date:** 2026-06-13 (IST)
**Scope:** READ-ONLY audit of BuildAgent-M3 artifacts against CONTRACT §1/§2/§3.2/§3.3/§5/§7 and MASTER_SPEC "M3 — Billing".
**Artifacts audited:**
- `backend/power_user/services/billing_service.py`
- `backend/power_user/routers/billing_router.py`
- `backend/main.py` (registration, lines 503–504, 535)
- `requirements.txt` (line 19)

---

## 1. Endpoint contract fidelity (CONTRACT §3.2) — PASS

| Endpoint | Contract shape | Built | Verdict |
|---|---|---|---|
| `POST /create-subscription` | auth → `{razorpay_subscription_id, short_url}` | `billing_router.py:89–115`, `CreateSubscriptionResponse` (router:43–45) returns exactly those two fields | ✅ |
| `GET /status` | auth → `{billing_plan, subscription_status, current_end}` | `billing_router.py:122–160`, `BillingStatusResponse` (router:48–51) | ✅ |
| `POST /webhook` | `X-Razorpay-Signature`, no JWT, 200, HMAC vs `RAZORPAY_WEBHOOK_SECRET`, dedupe via `razorpay_event_id` | `billing_router.py:167–234` — no auth dep, reads signature header, returns 200 on success/dedupe | ✅ |
| `POST /cancel` | auth → `{status:"cancelled"}` | `billing_router.py:241–256`, `CancelResponse` (router:54–55); service returns `status:"cancelled"` (`billing_service.py:321`) | ✅ |

Self-prefix `/api/power/billing` (router:36) matches §3.2 paths. Auth split correct: create/status/cancel gated by `current_user_required`; webhook public. Response models are strict (no contract-foreign fields leak to create-subscription/status/cancel; the webhook's `{status,deduped,applied}` body is a free JSONResponse, acceptable since §3.2 only specifies `200`).

## 2. Webhook security (critical) — PASS

- `verify_webhook_signature` (`billing_service.py:328–355`) recomputes `HMAC-SHA256(raw_body, RAZORPAY_WEBHOOK_SECRET)` over the **raw request bytes** and compares with `hmac.compare_digest` (constant-time). ✅
- Router reads `raw_body = await request.body()` (`billing_router.py:190`) and verifies **before** JSON parse and before `apply_webhook_event` — no DB write precedes verification. ✅
- **Fail-closed on all three holes:** missing signature → `False` (service:342–343); missing/unset secret → `BillingConfigError` caught → logged + `False` (service:345–349); mismatch → `compare_digest` `False`. All three → router raises **400** with no state mutation (`billing_router.py:193–196`). ✅
- Stdlib-only implementation (no SDK dependency for the security boundary) — verification still works when `razorpay` is absent. ✅
- **No bypass found.** Access to `paid` is granted ONLY inside `apply_webhook_event` (service:511–520), which is unreachable unless signature verification passed. `create_subscription` deliberately does not set `paid` (service:211–213). ✅

## 3. Idempotency + Postgres portability — PASS (with carry-forward flag)

- Dedupe via UNIQUE `razorpay_event_id`: INSERT-first into `power_user_billing_events`, catch on conflict, `rollback()`, return `{deduped:True, applied:False}` without re-applying state (`billing_service.py:446–461`). Router still answers 200 (`billing_router.py:230–234`). Idempotency key = `X-Razorpay-Event-Id` header with deterministic `body:<sha256>` fallback (`billing_router.py:211–214`). Logic is correct. ✅
- **Postgres concern (build risk #4) — VERIFIED and DOWNGRADED to GREEN-with-flag.** The dedupe catches `sqlite3.IntegrityError` only (`billing_service.py:456`). On Postgres the driver raises `psycopg2.IntegrityError`, which this `except` would NOT catch; it would bubble to the router's `except Exception` (`billing_router.py:221–227`) and surface as **500** → Razorpay retries → eventually consistent but not clean.
- **Severity assessment — NOT a current-prod-path break.** I verified the runtime DB layer: `get_db` (`dependencies.py:36,48`) unconditionally calls `sqlite3.connect(config.POWER_DB_PATH)`. `POWER_DB_PATH` is a filesystem path (`config.py:32,44`). There are **zero** `psycopg2` / `DATABASE_URL` / SQLAlchemy references anywhere under `backend/power_user/`. The only `psycopg2` usage in the repo is the offline one-time data-copy script `scripts/migrate_to_supabase.py:188` — never the request-serving connection factory. **The application has no live Postgres serving path today.** Supabase/Postgres is the stated prod *target* (CONTRACT header) but the runtime cutover has not landed in this module set.
- Therefore the bug is **latent, not active**: it cannot fire until M1/M2 replace `get_db` with a Postgres connection. It is a real correctness gap that will break the *future* Postgres path, but it does not break the path that exists now. This is a **tracked-before-go-live carry-forward**, consistent with the build's own explicit flag (build-log risk #4). The trivial fix when cutover lands: broaden the `except` to a dialect-agnostic `IntegrityError` (e.g. catch `sqlite3.IntegrityError` and, when available, `psycopg2.IntegrityError` / SQLAlchemy `IntegrityError`), or pre-check existence by `razorpay_event_id` before INSERT. **Not a RED must-fix-now** because (a) no Postgres runtime path exists yet, (b) the failure mode is a 500 that triggers Razorpay's own retry (no data corruption, no false access grant), (c) it is already tracked.

## 4. State mapping (CONTRACT §2) — PASS

`_EVENT_PLAN_MAP` (`billing_service.py:368–378`):
- `activated`/`charged` → `(active, paid)` — grants. ✅
- `halted`/`cancelled`/`completed`/`expired` → `blocked` (status mirrored) — ends access. ✅
- `authenticated` → `(authenticated, None)` and `pending` → `(halted, None)` — status-only refinements, **never touch `billing_plan`**, so never grant access. These exceed the contract's headline list but are non-grant status mirrors; acceptable per build judgment call (build-log deviation #2). ✅
- Unmapped events → recorded for audit, no-op (service:464–471). ✅
- **No path grants `paid` without a verified webhook.** `paid` is set in exactly one place (service:513–520), gated behind signature verification. `create_subscription` and `cancel` never set `paid`. ✅
- Allow-predicate alignment (CONTRACT §2): only `(paid, active)` yields access; every terminal event drives `blocked`, which M4 will 402. ✅

## 5. Invariants — PASS

- **INV2 (no falcon/* touched):** No `backend/falcon/*` import or write in either artifact. Registration block touches only the power_user import/include section (`main.py:503–504,535`). ✅
- **INV5 (no secrets in code):** All Razorpay creds read via `_env()` from `os.environ` (`billing_service.py:80–94,111–112`). No hard-coded keys/secrets/plan ids. Env names match CONTRACT §5 exactly (`RAZORPAY_KEY_ID/KEY_SECRET/WEBHOOK_SECRET/PLAN_MONTHLY`). ✅
- **INV6 (power_user/ + main.py registration + requirements only):** Both code files under `power_user/`; `main.py` change is registration-only; `requirements.txt` adds `razorpay>=1.4.1` (line 19). No out-of-scope files. ✅
- INV4 (IST): all timestamps via `_now_ist_iso()` / `_epoch_to_ist_iso()` with `IST = +05:30` (service:44,119–137). ✅
- INV3 (no mock data): no fixtures/mocks in artifacts. ✅

## 6. Registration — PASS

Import `power_billing_router` (`main.py:504`) and `include_router(power_billing_router, tags=["Power-User"])` (`main.py:535`) — identical pattern to the other 9 power_user routers (no `prefix=`, since the router self-prefixes `/api/power/billing`). Mounted in the public section (no paywall), correct per CONTRACT §4 (billing must stay public — it is the path to obtain access). ✅

---

## Non-blocking observations (not must-fix)

- **No tests shipped** (build-log risk #2). MASTER_SPEC M3 acceptance lists behavioral criteria (forged sig → reject; idempotent on dup; status updates). None are codified as automated tests, and Python could not be executed in the build env. Recommend the orchestrator require a minimal suite before go-live: (a) forged/missing sig → 400 no-write, (b) valid sig + duplicate event_id → single apply + 200, (c) `charged` → user `paid`/`active`, (d) `cancelled` → `blocked`. Audit-blocking? No (code is correct by inspection). Go-live-blocking? Recommended yes.
- **Pre-link webhook** with unknown sub + no `notes.power_user_id` records the audit row with `user_id=NULL` and applies no user state (service:497, 511 guards). Matches CONTRACT §1.3 (nullable `user_id`). Acceptable.
- **`cancel` immediate DB flip** to `blocked` (service:312–319) closes the gate before the confirming webhook; the later `subscription.cancelled` webhook re-affirms idempotently. Sound.

---

## VERDICT: GREEN

No contract violation, no webhook security hole, no invariant breach, and no break of the **currently-existing** (SQLite) runtime path.

**Postgres dedupe issue — my call: GREEN-with-flag (tracked carry-forward), NOT RED.**
Justification: the `except sqlite3.IntegrityError` will not catch `psycopg2.IntegrityError`, but I verified there is **no live Postgres serving path in `backend/power_user/`** — `get_db` is unconditionally SQLite, and `psycopg2` exists only in the offline migration script. The bug is latent until the M1/M2 runtime cutover; its worst case is a 500 that triggers Razorpay's own retry (no corruption, no false grant); and it is already explicitly tracked in the build log. It does not gate this module.

### Carry-forward (must close before the Postgres/Supabase runtime cutover, owned by M1/M2 + an M3 follow-up)
1. **Broaden the dedupe `except` to be dialect-agnostic** (`billing_service.py:456`): catch SQLite *and* Postgres integrity errors (or pre-check existence of `razorpay_event_id` before INSERT) so duplicate webhook delivery dedupes cleanly on Postgres instead of surfacing a 500. Trigger to action this: the moment any `power_user` request path gains a Postgres connection.

### Recommended before go-live (non-blocking for this audit)
2. Add the 4 behavioral tests listed under "Non-blocking observations" to satisfy MASTER_SPEC M3 acceptance criteria, and run a smoke import once `razorpay` is installed.
