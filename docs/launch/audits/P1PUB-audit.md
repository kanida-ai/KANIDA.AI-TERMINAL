# P1PUB Audit — Laptop → Cloud "Publish Intelligence" Transport

**Auditor:** AuditAgent-P1PUB · **Date:** 2026-06-14 (IST) · **Phase:** 1
**Scope:** READ-ONLY on all code. Only this file written.
**Artifacts reviewed:** `backend/falcon/routers/publish_router.py`, `backend/main.py` (registration),
`scripts/publish_to_cloud.py`, `backend/falcon/tests/test_publish_router.py`,
`scripts/publish_patterns.py`, `backend/falcon/db.py`, `backend/falcon/config.py`,
`docs/launch/ENV.md`, `docs/launch/CONTRACT.md`, `requirements.txt`.

---

## VERDICT: GREEN

No auth bypass, no partial-write path, no SQL-injection surface, no empty-bundle wipe, no invariant breach. Must-fix list is empty. Non-blocking improvements noted at the end.

---

## The three criticals — explicit confirmation

### 1. Auth — fails CLOSED, self-contained, constant-time — CONFIRMED
- `_require_secret()` (`publish_router.py:81-96`) reads `FALCON_PUBLISH_SECRET` from env.
- **Fail-closed:** env unset/empty → `503 PUBLISH_NOT_CONFIGURED` (`:84-90`). An unconfigured server cannot accept a publish. `not expected` also catches empty-string.
- **Mismatch / missing header:** `403 FORBIDDEN` (`:91-96`).
- **Constant-time:** `hmac.compare_digest(str(x_publish_secret), str(expected))` (`:91`).
- **Self-contained:** auth is checked as the FIRST action inside the handler (`:131`), before any body processing. It relies on NO upstream Basic-Auth/JWT gate — correct, since the cloud API is reachable server-to-server (confirmed against ENV.md:38 and the module docstring `:17-22`). There is NO unauthenticated path to a DB write: every branch after `_require_secret` is gated by it.

### 2. Atomicity — single transaction, full rollback, no partial commit — CONFIRMED
- All reads/validation (PRAGMA introspection, COUNT, allowlist, empty-guard, column-subset check) run BEFORE the write transaction (`:142-194`) and perform NO writes.
- `BEGIN IMMEDIATE` (`:200`) opens ONE transaction wrapping every `DELETE` + `INSERT` for all tables AND the success audit row (`:202-232`).
- `COMMIT` (`:233`) is reached ONLY after every table write and the audit row succeed.
- `except Exception` → `ROLLBACK` (`:238`) → cloud DB unchanged → returns `500 PUBLISH_FAILED` (`:250-254`). The best-effort failure-audit (`:240-249`) runs AFTER the rollback and `con.commit()` (`:247`) commits ONLY the freshly-inserted audit row (the data writes were already discarded by ROLLBACK; pysqlite auto-opens a fresh implicit txn for that lone INSERT). No path commits a partial bundle.
- `except HTTPException` → `ROLLBACK` + re-raise (`:234-236`) — defensive; no HTTPException is actually raised inside the try block, but the rollback is correct if one ever is.
- `falcon_conn()`/`connect_falcon()` (`db.py:15-44`) returns a default-isolation sqlite3 connection (no autocommit), so the explicit `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK` is the sole transaction control — same proven pattern as `publish_patterns.py:204-259`. The pre-BEGIN statements are SELECT/PRAGMA only, so no stray write transaction is open when `BEGIN IMMEDIATE` runs.
- Test `test_atomic_rollback_on_bad_row` (`test_publish_router.py:223-239`) asserts a mid-INSERT PK collision rolls back and the stale `OLD_P` row survives.

### 3. No SQL-injection surface — CONFIRMED
- **Table names:** every table written is gated by the hardcoded `ALLOWED_TABLES` allowlist (`:52-59`); any request key not in it → `400 TABLE_NOT_ALLOWED` (`:143-150`) BEFORE the DB is opened. Table names interpolated into SQL (`PRAGMA table_info({table})` `:101`, `DELETE FROM {tname}` `:203`, `INSERT INTO {tname}` `:213`, `COUNT(*) FROM {table}` `:105`) are therefore ONLY ever members of the constant allowlist — never a raw request string.
- **Column names:** every row's keys are validated as a SUBSET of the LIVE table columns introspected via `PRAGMA table_info` (`:99-101`, `:170-194`) BEFORE any SQL is built; unknown column → `400 UNKNOWN_COLUMN` (`:184-194`). The `col_list` interpolated at `:209/:213` is thus drawn only from keys already proven to be live schema columns. (Defense-in-depth note below: this is a whitelist check, not quoting — see non-blocking #1.)
- **Values:** all row values are bound parameters (`placeholders = ", ".join("?" * ...)`, `con.execute(sql, values)` `:210-215`). No value reaches SQL unparameterized.
- Net: no request-supplied string reaches SQL as raw, unvalidated SQL text.

---

## Checklist results

| # | Item | Result | Evidence |
|---|---|---|---|
| 1 | Auth fail-closed / 403 / constant-time / self-contained | ✅ | `publish_router.py:81-96,131` |
| 2 | Atomic single-txn, rollback leaves DB unchanged | ✅ | `:200-254` |
| 3 | Idempotency (full-replace DELETE+INSERT) | ✅ | `:203-215`; test `test_idempotent_repost:212-219` |
| 4 | Empty-guard on `falcon_promoted_patterns` → 400, never wipes | ✅ | `:64,152-160`; test `:152-157` (mirrors publish_patterns:160-164) |
| 5 | Injection surface (allowlist tables, PRAGMA-validated columns, parameterized values) | ✅ | `:52-59,143-150,170-194,207-215` |
| 6 | Allowlist correctness — exactly the 3 Phase-1 tables, unknown → 400, extensible | ✅ | `:52-59` (Phase-2 names commented out, ready); `:143-150` |
| 7 | Audit row in `falcon_signal_runs`, job_name=`publish_intelligence`, counts/cutoff/source, IST | ✅ | `:108-117,162,220-232`; `_now_ist` uses IST tz `:43,67-68` (INV4); test `:199-209` |
| 8 | Publisher mirrors cutoff + selection + empty-guard; env-driven; `--dry-run` no POST; doesn't break publish_patterns | ✅ | see "Publisher correctness" below |
| 9 | INV2 (no Auto-Trade touched), INV5 (no secrets), INV6 | ✅ | only new router + main.py registration; no `backend/falcon/trade/` edits; secret from env only |
| 10 | Registration / path = `/api/falcon/publish/intelligence` | ✅ | router self-prefixes `/falcon/publish/intelligence` (`:120`), mounted `prefix="/api"` (`main.py:528`) |

### Detail — Publisher correctness (item 8)
- **Same cutoff:** `_read_mining_window_years` (`publish_to_cloud.py:86-96`) is a faithful mirror of `publish_patterns.py:106-117` — reads `mining_window_years FROM falcon_engine_config WHERE id=1`, default 4. `cutoff_year = current_IST_year - window` (`:118-119`), IST tz (`:60`).
- **Same selection:** candidates `WHERE mined_year >= cutoff` (`:125-129`), promoted `WHERE pattern_id IN (eligible candidates)` (`:135-142`), taxonomy filtered to eligible `pattern_id`s with graceful full-ship fallback if no `pattern_id` column (`:156-171`). Matches `publish_patterns.py:167-178,211-224`.
- **Client-side empty guard:** 0 eligible promoted → raises before POST, same intent/message as publish_patterns (`:146-151`).
- **`--dry-run`:** builds + prints summary, returns before resolving URL/secret, never POSTs (`:241-243`).
- **Env-driven:** URL from `FALCON_PUBLISH_URL` / `--cloud-url`, secret from `FALCON_PUBLISH_SECRET`; both required before POST (`:245-253`); secret sent as `X-Publish-Secret` header (`:206-210`).
- **Local-only meta stripped:** `_mining_window_years` (prefixed `_`) is removed before POST (`:205`) and the server's pydantic model ignores unknown top-level keys regardless.
- **Does not break publish_patterns:** that file received a top-of-file NOTE comment only (`publish_patterns.py:1-10`); its logic is byte-for-byte unchanged and still functional for the local dev path.

### Detail — Registration & invariants
- Final route: `/api/falcon/publish/intelligence` (matches contract). The doc-§6 example name `/api/admin/publish-intelligence` was a sketch; placing it in the falcon namespace to reuse `falcon_conn` is a documented, sound deviation (build-log "Deviations").
- INV2: diff is a NEW router file + two lines in `main.py` (import `:486-488`, mount `:528`). Nothing under `backend/falcon/trade/`, monitor, or deployer is touched.
- INV5: no literal secrets; `FALCON_PUBLISH_SECRET` read from env on both sides. `requirements.txt:48` adds `requests>=2.31.0` (laptop-side only; server uses stdlib `hmac`/`sqlite3`/`json`).
- INV4: all timestamps via `datetime.now(IST)` with `IST = timezone(timedelta(hours=5, minutes=30))`.

---

## Non-blocking improvements (NOT required for GREEN)

1. **Column-name defense-in-depth (low risk).** Column identifiers are validated against the live PRAGMA schema (a strict whitelist) but then string-interpolated unquoted into the INSERT. This is safe today because a value that isn't an exact live-column name is rejected at `:184-194`, so no attacker-chosen string can reach the SQL. Still, wrapping identifiers in double-quotes (`'"' + c + '"'`) when building `col_list` would make the safety local to the INSERT rather than dependent on the upstream check — cheap hardening.

2. **`PRAGMA foreign_keys = OFF` is set per-connection outside the txn (`:165`)** — intentional for full-replace (matches publish_patterns) and fine. No action; noting for clarity.

3. **Failure-audit on a fresh implicit transaction (`:240-247`)** is correct but relies on pysqlite default-isolation behavior; a short inline comment that the data writes are already discarded by the preceding ROLLBACK would aid future readers. Cosmetic.

4. **Schema-drift risk (from build-log Risks).** The server validates columns are a subset of the live schema but does not require the bundle to cover ALL live columns; a row missing a NOT-NULL-without-default column would fail the INSERT and (correctly) roll back the whole bundle — safe, but surfaces only at publish time. Recommend the deploy checklist confirm cloud `falcon_pattern_taxonomy`/`*_candidates`/`*_promoted` schemas match R&D before first publish (already flagged in the build-log). No code change needed.

5. **Tests written but not executed** (no Python in build env). They are well-targeted (auth 503/403, empty-guard, unknown table/column, happy-path full-replace, idempotency, atomic rollback). Recommend running `pytest backend/falcon/tests/test_publish_router.py` on a Python box before go-live to convert "written" into "passing."

---

## Phase-1 exit-criterion mapping
The CLOUD_ARCHITECTURE §5 Phase-1 gate "Laptop→cloud publish is authenticated + atomic (endpoint requires a secret; a partial/failed bundle never leaves the cloud DB half-updated)" is **satisfied** by this transport, subject to running the test suite (non-blocking #5) and setting `FALCON_PUBLISH_SECRET` on the cloud host (intentional fail-closed; flag in deploy checklist).
