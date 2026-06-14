# P1BUNDLE — Cloud DB bundle builder — AUDIT

**Agent:** AuditAgent-P1BUNDLE
**Date:** 2026-06-14 (IST)
**Artifact audited:** `scripts/build_cloud_bundle.py`
**Method:** READ-ONLY. Table requirements re-verified independently against source resolvers/queries — NOT trusted from the build log's citations.

---

## VERDICT: GREEN

The script ships exactly the tables each request path needs, never mutates a source DB, builds SQL only from schema introspection (no unsanitized input), is stdlib-only, idempotent, and the host-placement instructions match the actual resolvers. No RED-class defect found (no wrong/missing table, no source mutation, no SQL-construction bug, no invariant breach).

---

## The two criticals — explicit confirmation

### CRITICAL 1 — Table list correctness (re-verified from source, NOT from the build log)

**`falcon_outcomes` → FULL, merged into the app DB copy.** CONFIRMED correct.
- Evidence read opens its connection at `POWER_RND_DB_PATH`:
  `backend/power_user/routers/falcon_top20_router.py:30` (`from ..config import POWER_RND_DB_PATH`) and `:94-96` (`sqlite3.connect(f"file:{POWER_RND_DB_PATH}?mode=ro", uri=True ...)`), passed in as `rnd_con` at `:101-102`.
- The explainer reads `falcon_outcomes` from that `rnd_con` at `falcon_top20_explainer.py:1103-1108`, `:1223-1226` (`_stock_lifetime_baseline`), and `:1241-1247` (`_load_outcomes`). Columns spanned: `symbol, trade_date, ret_5d, ret_10d, ret_20d, ret_30d, hit_10pc_20d, hit_15pc_20d, mae_20d, mfe_20d, failure_10d`.
- On host `POWER_RND_DB_PATH` = the app DB path (RUNBOOK §0 line 23, §2 line 52, appendix line 179; `config.py:50-53` is env-overridable). So `falcon_outcomes` MUST live inside `kanida_universe.db`. The script does exactly this (`build_app_db`, `OUTCOMES_TABLE`).
- Verbatim copy (replays source `CREATE`) preserves ALL columns → no risk of dropping a column any of the three queries name. Verified the script does not hand-pick columns.

**Persona sidecar = `falcon_promoted_patterns` + `falcon_pattern_candidates` (those two ONLY).** CONFIRMED correct.
- `persona_engine_core.py:69-74` `load_full_patterns()` reads exactly: `SELECT c.mined_year, c.rule_json, p.avg_oos_year_lift_pp FROM falcon_promoted_patterns p JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id WHERE p.classification IN ('universal','regime_dependent')`. No other table touched in that function.
- `persona_simulator.py:62-71` `_resolve_rnd_db_path()` builds `…/universe_engine/data/db/kanida_universe.db` (absent on host) then falls back to `Path(config.POWER_DB_PATH).parent / "kanida_universe_rnd.db"` (`:67-69`). It NEVER reads `POWER_RND_DB_PATH` — confirmed by repo-wide grep: `POWER_RND_DB_PATH` appears only in `config.py` and `falcon_top20_router.py`, never in any persona file.
- Call site `persona_simulator.py:408-409` (`rnd_db = _resolve_rnd_db_path(); all_pats = load_full_patterns(rnd_db)`) is NOT wrapped in try/except → a missing sidecar raises `FileNotFoundError` from `:70` and fails EVERY persona endpoint. The script's `PERSONA_PATTERN_TABLES` list = exactly these two, shipped as `kanida_universe_rnd.db`. Correct, and it correctly does NOT try to satisfy personas by putting the tables in the app DB (the resolver wouldn't look there).
- Path arithmetic check: host `POWER_DB_PATH=/data/kanida_universe.db` → primary resolver path climbs above `/data` to a non-existent `universe_engine/...` → falls to `/data/kanida_universe_rnd.db`. The "sidecar lands NEXT TO the app DB" instruction is therefore correct.

**Intraday `intraday_dataset_v2` → OPTIONAL.** CONFIRMED correct (degrades, not fatal).
- `_resolve_intraday_db_path()` `persona_simulator.py:74-79` is a hardcoded relative path, no env, no fallback, raises `FileNotFoundError` if absent.
- Call site `persona_simulator.py:428-434` IS caught (`except FileNotFoundError ... log.warning("P2 intraday filter NOT wired ...")`) → P2 runs degraded, does NOT crash. Script correctly gates this file behind `--with-intraday` and documents the §9 code change needed before the host reads it.

**No wrong-table assumption found.** The three mechanisms (evidence-via-POWER_RND_DB_PATH, personas-via-sidecar-fallback, intraday-caught) are each modeled correctly in the script.

### CRITICAL 2 — No source-DB mutation

CONFIRMED — no path writes the original app DB or the 14 GB R&D DB.
- R&D DB and intraday DB are opened ONLY via `_connect_ro()` → `sqlite3.connect("file:...?mode=ro", uri=True)` (`:166-168`, used at `:292`, `:329`, `:376`, `:458`, `:507`). Read-only URI; any write would error.
- App DB is `shutil.copy2`'d to the output FIRST (`build_app_db:288`), and only the COPY (`out_app` under `out_dir`) is opened writable (`:293`) for the DROP/replace. The original `data/db/kanida_universe.db` is never opened writable.
- The sidecar (`build_rnd_sidecar`) and intraday paths write only into files under `out_dir`. Defaults put `out_dir` at `deploy/cloud-bundle/` (not over any source).
- `--dry-run` opens only the R&D source read-only and writes nothing (`do_dry_run:458-482`). `--verify` opens produced bundle read-only.

---

## Checklist results

1. **Table-list correctness (critical):** PASS — see Critical 1. Exactly `falcon_outcomes` (full, merged into app DB) + sidecar `falcon_promoted_patterns` + `falcon_pattern_candidates`; intraday optional/caught.
2. **No source mutation (critical):** PASS — see Critical 2.
3. **Schema-preserving copy:** PASS — `_copy_table_verbatim` replays the source `CREATE TABLE` from `sqlite_master` (`:217-222`); INSERT placeholder count derived from `PRAGMA table_info` via `_column_count` (`:199-200`, `:224-226`) — no hand-written column list; values passed through `executemany` (parameterized, `:234`). Table/index names that reach raw SQL come only from the script's own constants or from `sqlite_master`/`PRAGMA` (the DB's own schema), never from user/CLI input → no injection, no column drift.
4. **falcon_outcomes replace (not append):** PASS — `_copy_table_verbatim` issues `DROP TABLE IF EXISTS` before `CREATE` (`:221`), so the sparse app-DB copy is dropped and replaced by the full R&D copy. Post-merge assertion `final_rows == src_rows` (`:306-311`) guarantees no duplication/append.
5. **Idempotent + modes:** PASS — output overwritten; merged table DROPped before recreate; sidecar `unlink`'d and rebuilt fresh (`:326-327`) then `VACUUM`'d (`:351-355`). `--dry-run` writes nothing. `--verify` asserts each required table exists AND has >0 rows (`do_verify:489-528`). Manifest prints per-file/per-table row counts + sizes (`_print_manifest:393-403`). `--dry-run`/`--verify` are mutually exclusive (`:575-577`).
6. **stdlib only:** PASS — imports `argparse, shutil, sqlite3, sys, time, pathlib, typing` only. No external deps.
7. **Host placement instructions:** PASS — `_print_host_placement` (`:406-439`) + RUNBOOK §3b state: app DB → `POWER_DB_PATH` with `FALCON_DB_PATH = POWER_RND_DB_PATH = same path`; sidecar → SAME directory as app DB; personas ignore `POWER_RND_DB_PATH`; intraday optional pending §9. Matches the verified resolvers.
8. **Invariants:** PASS — INV2: no `backend/falcon/*` execution touched (read-only verification only). INV5: no secrets in script or audit. INV6: script lives in `scripts/`.

---

## Non-blocking observations (NOT must-fix — informational)

- **(I1) `--verify` does not assert column presence.** It checks table-exists + row-count>0, not that `falcon_outcomes` carries `ret_20d/hit_10pc_20d/mae_20d/mfe_20d`. Because the copy is verbatim, the produced table mirrors whatever R&D has; if R&D's `falcon_outcomes` were ever missing a queried column, the break would surface only at request time, not at `--verify`. R&D is the canonical source so this is low-risk; an optional `PRAGMA table_info` assertion in `--verify` would close it. (Build-log Risk #2 already flags this for the operator.)
- **(I2) `--dry-run` reports persona-table counts but does NOT pre-flag a missing `falcon_outcomes` in R&D** (it prints 0 rows silently via the `_table_exists` guard at `:462`). The real build does raise on a missing R&D `falcon_outcomes` (`:295-298`), so this is a dry-run cosmetic only.
- **(I3) `--verify` is dependent on `--with-intraday` being passed at verify time** to check the intraday file (`:498-499`). If the operator builds with `--with-intraday` but verifies without it, the intraday file is silently not verified. Cosmetic; intraday is optional anyway.
- **(I4) Index replay for `falcon_outcomes` runs after a full insert** — correct for build speed; just noting the merged table's indexes come from R&D's `sqlite_master` (`:185-192`, `sql IS NOT NULL` filters auto-indexes correctly).

None of I1–I4 breaks a request path on the host, mutates a source, or is a SQL-construction/invariant defect, so none is RED.

---

## Summary for the orchestrator

- **VERDICT: GREEN.** No must-fix items.
- **Critical 1 (table list, re-verified from source):** correct — `falcon_outcomes` full into app DB; sidecar = `falcon_promoted_patterns` + `falcon_pattern_candidates` only; intraday optional & caught. No wrong/missing table.
- **Critical 2 (no source mutation):** confirmed — R&D + intraday opened `mode=ro`; app DB copied first, only the copy written; all writes land under `out_dir`.
- Schema-preserving (replayed `CREATE` + `PRAGMA`-sized parameterized inserts, no injection), `falcon_outcomes` DROP-replaced (not appended, row-count asserted), idempotent, `--dry-run`/`--verify` correct, stdlib-only, host placement matches resolvers, INV2/INV5/INV6 honored.
- Optional hardening (not required): add a `PRAGMA table_info` column-presence assertion to `--verify` for `falcon_outcomes` (I1).
