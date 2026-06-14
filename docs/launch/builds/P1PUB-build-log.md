# P1PUB — Laptop → Cloud "Publish Intelligence" Transport — Build Log

**Agent:** BuildAgent-P1PUB · **Date:** 2026-06-14 (IST) · **Phase:** 1
**Source of truth:** `docs/launch/CLOUD_ARCHITECTURE.md` §6 · `docs/launch/CONTRACT.md` §5/§7

## Why
Phase 1 moves the app + PROD DB onto a cloud SQLite volume. The laptop keeps the
14 GB research DB and mines weekly, then must push published patterns UP to the
cloud. The laptop cannot `sqlite3.connect` a remote file, so this needs an HTTP
transport. The cloud backend is reached server-to-server (api host), bypassing
the Next.js Basic-Auth middleware — so the endpoint must protect ITSELF.

## Files added / changed
| File | Change |
|---|---|
| `backend/falcon/routers/publish_router.py` | NEW — cloud ingest endpoint `POST /api/falcon/publish/intelligence` |
| `backend/main.py` | import + `include_router(falcon_publish_router, prefix="/api")` |
| `scripts/publish_to_cloud.py` | NEW — laptop publisher (build bundle, POST) |
| `scripts/publish_patterns.py` | top-of-file note: cloud path is `publish_to_cloud.py` (no logic change) |
| `requirements.txt` | add `requests>=2.31.0` (laptop publisher; server uses stdlib) |
| `backend/falcon/tests/__init__.py` | NEW (package marker) |
| `backend/falcon/tests/test_publish_router.py` | NEW — test stub (not executed; no Python here) |
| `docs/launch/ENV.md` | `FALCON_PUBLISH_SECRET`, `FALCON_PUBLISH_URL` |
| `docs/launch/CONTRACT.md` §5 | same two env vars |

## Endpoint contract — `POST /api/falcon/publish/intelligence`
**Auth:** header `X-Publish-Secret` == env `FALCON_PUBLISH_SECRET`, `hmac.compare_digest`.
- env unset → **503 `PUBLISH_NOT_CONFIGURED`** (fail closed) — `publish_router.py:_require_secret`
- missing/mismatch → **403 `FORBIDDEN`**

**Body:** `{ bundle_version:int, generated_at:str, source:str, cutoff_year:int|null, tables:{ <t>:[{col:val}...] } }`

**Allowlist (constant `ALLOWED_TABLES`, extensible for Phase 2):**
`falcon_promoted_patterns`, `falcon_pattern_candidates`, `falcon_pattern_taxonomy`.
Any other → **400 `TABLE_NOT_ALLOWED`**.

**Validation:**
- `bundle_version != 1` → 400 `UNSUPPORTED_BUNDLE_VERSION`
- table not in cloud DB → 400 `TABLE_MISSING`
- row column not a SUBSET of the LIVE table columns (introspected via PRAGMA) → 400 `UNKNOWN_COLUMN`
- **empty-guard:** `falcon_promoted_patterns` present but empty → 400 `REFUSING_EMPTY_PUBLISH`

**Response 200:** `{ status:"published", source, cutoff_year, tables:{ <t>:{before,after} }, audit_run_id }`

## Mechanism map (file:line)
- **Auth (self-contained, fail-closed, constant-time):** `publish_router.py` `_require_secret()` — 503 if env unset, `hmac.compare_digest` otherwise.
- **Empty-guard (mirror publish_patterns):** `publish_router.py` step 4 — `EMPTY_GUARD_TABLE` check before any write.
- **Atomicity:** `publish_router.py` step 6 — `BEGIN IMMEDIATE` … per-table `DELETE` + parameterized `INSERT` … `COMMIT`; any `Exception`/`HTTPException` → `ROLLBACK` (cloud DB unchanged), then best-effort `failed` audit row OUTSIDE the rolled-back txn.
- **Idempotency:** full-replace (`DELETE` then `INSERT`) → re-POST yields identical state. Test `test_idempotent_repost`.
- **Audit (IST):** `_audit_row()` writes `falcon_signal_runs` `job_name='publish_intelligence'`, notes = per-table before→after + cutoff_year + source + bundle generated_at; inside the same txn on success.

## Publisher flow — `scripts/publish_to_cloud.py`
1. `build_bundle()` opens R&D DB (`POWER_RND_DB_PATH` or default `universe_engine/data/db/kanida_universe.db`).
2. Reads `mining_window_years` from `falcon_engine_config` (default 4) → `cutoff_year = current_IST_year - window`. **Selection logic REPLICATED from `publish_patterns.py`** (candidates `WHERE mined_year >= cutoff`; promoted linked to eligible candidate ids; taxonomy filtered to those pattern_ids). Replicated, not imported, because publish_patterns' public fn is file-to-file, not row-returning. Kept identical — if one changes, mirror the other.
3. Client-side empty guard: 0 eligible promoted → fail fast (same message as publish_patterns), before POST.
4. Builds `bundle_version=1`, `generated_at` IST, `source="research-laptop"`, `cutoff_year`.
5. `post_bundle()` → POST `<FALCON_PUBLISH_URL>/api/falcon/publish/intelligence` with `X-Publish-Secret`. URL from env or `--cloud-url`.
6. CLI: `--dry-run` (build + print summary, no POST), `--cloud-url`, `--mining-window-years`. Prints the server summary.

## Env vars added
- `FALCON_PUBLISH_SECRET` (secret) — set the SAME value on cloud host AND laptop.
- `FALCON_PUBLISH_URL` — laptop-side cloud base URL.

## Constraints honored
- INV2: only ADDED a router + registration; nothing under `backend/falcon/trade/` touched.
- INV5: no secrets in code (env only). INV4: IST timestamps. INV6: changes limited to new files + main.py registration + scripts + docs + requirements.
- `publish_patterns.py` logic unchanged (note only). No git commit.

## Deviations / notes for the audit agent
- **Path mounting:** falcon routers self-prefix `/falcon/...` and are mounted with `prefix="/api"` in main.py, so the final path is `/api/falcon/publish/intelligence` (the architecture doc §6 example name was `/api/admin/publish-intelligence`; we placed it under the falcon namespace to match the existing falcon router style + `falcon_conn` DB helper). Documented here as a deliberate choice.
- **Per-row INSERT (not executemany):** rows may carry different column subsets, so each row is inserted with its own column list. Bundle sizes are KB-MB (a few k patterns), so this is fine; if bundles ever grow, group rows by identical column-set and `executemany`.
- **`falcon_conn()` does NOT wrap in an outer transaction by default** — the endpoint issues explicit `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK`, matching publish_patterns. Tests assert rollback leaves the stale row intact.
- **Server uses stdlib only** (`hmac`, `sqlite3`, `json`) — no new server dependency. `requests` is laptop-side only.
- **Tests not executed** (no Python in this environment). They monkeypatch `publish_router.falcon_conn` at a tmp SQLite DB seeded with the 3 tables + `falcon_signal_runs`. Run on a box with Python: `cd backend && pytest falcon/tests/test_publish_router.py`.

## Risks
- The cloud DB must already CONTAIN the 3 allowlisted tables (Phase-1 ships the DB as-is, so they exist). A fresh/empty cloud DB → `TABLE_MISSING` 400, which is the correct fail.
- Taxonomy filtering assumes a `pattern_id` column; if absent it ships full (handled). Confirm the cloud `falcon_pattern_taxonomy` schema matches R&D before first publish.
- `FALCON_PUBLISH_SECRET` must be set on the cloud host or every publish 503s — this is intentional fail-closed; flag in the deploy checklist.
