# Codebase Audit (2026-06)

Full audit performed before the docs/cleanup pass. The live maps have since been
split into dedicated docs; this file preserves the analysis, findings, and the
migration plan.

> Maps: see `architecture.md` (system + DBs), `api-map.md` (routers/endpoints),
> `folder-structure.md` (layout + conventions).

## Executive summary

The codebase is **not as messy as it feels**. One repo, one backend process, one
frontend deploy serving two logical products with clean routing separation
(`/power/*` vs `/falcon/*`). The "mess" was mostly top-level junk (audit memos,
logs, CSVs, stale worktrees) and legacy frontend mocks — **noise, not code**.

**Recommendation:** don't rebuild into `/products/X/...`. Archive the junk,
document the boundaries, formalize what already exists. A heavy restructure would
force days of import/CI rewiring for marginal gain.

## Product separation

| | Power User | Falcon Auto-Trade |
|---|---|---|
| Frontend | `/power/*` | `/falcon/*` (+ legacy operator routes) |
| Auth | invite → `power_jwt` JWT | HTTP Basic Auth (`middleware.ts`) |
| Backend | `power_user/routers` (8) | `falcon/routers` (5) + legacy `routers/` (11) |
| Schedulers | none (consumes V7) | V7 pipeline + live execution |
| Audience | invite-only beta | operator only |

Shared: `services/kite_auth.py`, `zerodha_auto_auth.py`, `config/.env`, the PROD DB.

## Duplicate / legacy / broken analysis

**Not actually duplicates** (distinct per product): the three `admin_router.py`
files (legacy / falcon / power_user).

**Cleaned up in the 2026-06 pass:** root audit memos → `docs/audit-history/`;
research CSVs + logs → `archive/`; 0-byte stray `kanida_quant.db` removed; runtime
logs deleted; `archive/`, `logs/`, `.claude/` gitignored.

**Still suspect (verify before action):**
- `backend/scheduler.py`, `backend/run_snapshot.py` — likely superseded.
- `engine/` (top-level) — OG research code; confirm no imports before archiving.
- Legacy frontend routes `/{terminal,dashboard,engine,welcome,login}` — pre-Power-User.
- Several `backend/routers/` (`quant`, `backtest`, `live`, `execution`, `strategy`,
  `orders`) — confirm no live consumer before unmounting.
- Empty `backend/agents/`, `backend/signals/` dirs (just `__init__.py`).
- Two `requirements.txt` (root + backend) — likely consolidatable.

## Risk list (severity-ordered)

1. **MEDIUM** — `KANIDA_DB_PATH` points at the legacy DB; don't mistake it for PROD.
2. **MEDIUM** — 11 legacy routers mounted unconditionally; audit consumers, drop unused.
3. **MEDIUM** — no tests for the V7 pipeline chain (`falcon/jobs/`).
4. **LOW** — `engine/` relationship to `falcon/` unclear; audit imports first.
5. **LOW** — `BACKEND_PORT=8000` in `.env` but uvicorn runs `:8001` (stale, harmless).
6. **LOW** — two `requirements.txt`.

## Migration plan (phased, gated)

- **Phase 0** — backup DBs, branch, green test baseline. *(Skipped for the
  docs-only pass since it touches zero code/DBs; required before the code phases.)*
- **Phase 1 — lossless (DONE 2026-06-02)** — docs/, archive/, move root junk,
  gitignore, READMEs. Zero behavior change.
- **Phase 2 — renames (DEFERRED)** — `backend/routers/` → `backend/legacy/routers/`
  (one import in `main.py`; needs backend restart). Reorganize `scripts/`.
- **Phase 3 — verified deletions (DEFERRED)** — audit each legacy router/engine
  dir for consumers, then unmount/archive. Delete empty dirs.
- **Phase 4 — nav toggle (DONE)** — admin-only "Full Kanida.AI mode" reveals
  operator nav; access still enforced by the two auth gates.
- **Phase 5 — docs (DONE)**.

## Strategic guidance: merge / toggle

Keep **one codebase, one deploy**. Gate the UI by role:
- Power User invitees see only `/power/*` nav.
- Admin gets a "Full Kanida.AI mode" toggle (UserMenu) that reveals operator nav.
- **Zero backend changes, zero security degradation** — `middleware.ts` Basic Auth
  still blocks `/falcon/*` for non-admins regardless of the cosmetic toggle.

**Do not** fork into separate Next.js/Python projects — you'd duplicate CI,
deploy, env, and auth for no gain; the products already coexist cleanly at the
routing layer.

## Open questions (carried forward)

1. Full restructure vs docs-only? → *docs-only done; restructure deferred & gated.*
2. Are `/terminal`, `/dashboard`, `/engine`, `/welcome`, `/login` still alive? → **needs usage audit.**
3. Audit each legacy router before unmounting? → **yes.**
4. Is top-level `engine/` still used? → **audit imports.**
5. Consolidate the two `requirements.txt`? → low priority.
6. Toggle placement → UserMenu dropdown (done).
