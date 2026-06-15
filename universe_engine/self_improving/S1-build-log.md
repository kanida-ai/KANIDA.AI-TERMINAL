# Step 1 Build Log — Self-Improving Engine Schema

**Date:** 2026-06-14 · **Agent:** BuildAgent-S1 · **Worktree:** `C:\Users\SPS\Desktop\kanida-dev` (branch `feat/self-improving-engine`)
**Scope:** Create `universe_engine/self_improving/` — additive SQLite DDL + idempotent applier. No commits, no DDL applied to any live DB, no existing code touched (INV2).

## Files created
| File | Purpose |
|---|---|
| `schema_self_improving.sql` | `CREATE TABLE IF NOT EXISTS` for all 10 new tables (SQLite). |
| `taxonomy_columns.sql` | 37 one-per-line `ALTER TABLE falcon_pattern_taxonomy ADD COLUMN ...`. |
| `apply_schema.py` | Idempotent applier — `--db` (required), `--dry-run`, `--verify`. Stdlib only. |
| `S1-build-log.md` | This log. |

## Context (from STEP0_AUDIT.md §E)
- New tables live in the **RND research DB** (SQLite). Postgres is Phase 4.
- Additive only; every table must have a `persona` column.
- `apply_schema.py` takes no default DB path — the caller passes the RND path so a live DB is never auto-targeted.
- `--dry-run` / `--verify` are the validation path: **Python is NOT installed in this environment**, so the applier was validated by careful reading, not execution. SQL files were grep-validated (counts below).

## Table + column inventory (10 tables)
| # | Table | Column count | persona col | PK | UNIQUE / FK preserved |
|---|---|---|---|---|---|
| 1 | `falcon_baseline_trades` | 100 | yes (`NOT NULL DEFAULT 'falcon_top10'`) | `id` AUTOINC | — |
| 2 | `falcon_pattern_contributions` | 18 | yes (`NOT NULL`) | `id` AUTOINC | FK `trade_id`→`falcon_baseline_trades(id)` |
| 3 | `falcon_post_exit_tracking` | 42 | yes (nullable, per spec) | `id` AUTOINC | FK `trade_id`→`falcon_baseline_trades(id)` |
| 4 | `falcon_near_miss_tracking` | 24 | yes (`NOT NULL`) | `id` AUTOINC | — |
| 5 | `falcon_signal_validity` | 22 | yes (`NOT NULL`) | `id` AUTOINC | — |
| 6 | `falcon_big_winner_loser_study` | 28 | yes (`NOT NULL`) | `id` AUTOINC | UNIQUE(pattern_id, persona, week_ending) |
| 7 | `falcon_pattern_weekly_state` | 24 | yes (`NOT NULL`) | `id` AUTOINC | UNIQUE(week_ending, pattern_id, persona) |
| 8 | `falcon_weekly_review_log` | 24 | yes (nullable, per spec) | `id` AUTOINC | — |
| 9 | `index_expiry_calendar` | 7 | yes — **ADDED** (not in spec DDL) | `id` AUTOINC | UNIQUE(index_name, expiry_date) |
| 10 | `fo_stock_master` | 11 | yes — **ADDED** (not in spec DDL) | `symbol` TEXT (per spec, no AUTOINC) | — |

Column counts include `id` (or `symbol` PK) and `created_at`/`updated_at`, and the added `persona` for tables 9 and 10. Verified via grep: 10 `CREATE TABLE IF NOT EXISTS`, 10 real `persona ... TEXT` column declarations.

### falcon_pattern_taxonomy additions: **37 new columns** (verified: 37 `ALTER ... ADD COLUMN`)
Groups (per spec): suitability (8), journey intelligence (5), sector/regime attribution (6), signal timing (6), big winner/loser (3), maturity (1), self-improvement (8).
**Spec-count note:** the prompt anticipated "38"; the spec's own ALTER list contains **37** distinct columns (the self-improvement group has 8, not 9). The applier and SQL both reflect the true spec list of 37 — none dropped, none invented.

## Postgres → SQLite translation decisions
| Postgres | SQLite | Applied to |
|---|---|---|
| `BIGSERIAL PRIMARY KEY` | `INTEGER PRIMARY KEY AUTOINCREMENT` | every `id` |
| `BIGINT` (FK ref cols) | `INTEGER` | `trade_id` (matches AUTOINC PK type) |
| `BOOLEAN` | `INTEGER` (0/1); `DEFAULT FALSE`→`DEFAULT 0`, `DEFAULT TRUE`→`DEFAULT 1` | all flag cols |
| `FLOAT` | `REAL` | all |
| `VARCHAR(n)` / `DATE` / `TIME` | `TEXT` | all (enum comments retained inline) |
| `INTEGER[]` / `TEXT[]` / `JSONB` | `TEXT` (store JSON) | `top_3_pattern_ids`, `pattern_mined_years_used`, all `*_by_persona`, all `falcon_weekly_review_log` array cols |
| `TIMESTAMP DEFAULT NOW()` | `TEXT DEFAULT (datetime('now'))` | `created_at` |
| `TIMESTAMP` (no default) | `TEXT` | `approved_at`, `reviewed_at` |
| `INTEGER DEFAULT n` / `FLOAT DEFAULT n` | preserved (`INTEGER DEFAULT n` / `REAL DEFAULT n`) | `weight_multiplier`, `classification_version`, `n_resolved_trades_*`, `human_approved`, `fo_eligible` |

- All `UNIQUE(...)` constraints preserved verbatim (3 tables).
- Both `FOREIGN KEY ... REFERENCES` preserved as inline `REFERENCES` (SQLite supports the syntax; FK enforcement is off by default unless `PRAGMA foreign_keys=ON`).
- Enum-explaining SQL comments from the spec retained (e.g. `exit_reason`, `move_type`, `repeater_type`, `quality_flag`, `pattern_maturity`, `rank_bucket`, `index_name`).
- `fo_stock_master.symbol` kept as `TEXT PRIMARY KEY` (no AUTOINCREMENT) — faithful to the spec's `VARCHAR(20) PRIMARY KEY`.

## Persona-column additions (spec rule: every table has a persona column)
- 8 spec tables already declared `persona`.
- **`index_expiry_calendar`** and **`fo_stock_master`** had NO `persona` in the spec DDL → `persona TEXT` (nullable) ADDED to each, commented `-- ADDED (not in spec DDL) — persona rule`. These are reference tables, so nullable is appropriate.

## Ambiguities / decisions for the audit agent
1. **Taxonomy count 37 vs 38** — resolved to 37 (true spec list). If the orchestrator expected 38, the discrepancy is in the prompt's anticipated number, not the spec.
2. **Nullable persona on tables 3 & 8** — spec declared `persona VARCHAR(50)` (nullable, no NOT NULL) for `falcon_post_exit_tracking` and `falcon_weekly_review_log`; preserved as nullable rather than forcing NOT NULL.
3. **FK enforcement** — SQLite needs `PRAGMA foreign_keys=ON` per-connection for the two FKs to enforce. The applier does NOT set it (additive/structural only); the populating code (Step 2) should enable it if enforcement is desired.
4. **`apply_schema.py` and the taxonomy table** — this build never CREATEs `falcon_pattern_taxonomy` (it's "extend, do not replace"). If the target RND DB lacks that table, the applier SKIPS all 37 ALTERs and prints a WARNING rather than erroring. The audit agent should confirm the RND DB contains `falcon_pattern_taxonomy` (STEP0 audit reports 1,943 rows in RND via `falcon_promoted_patterns`; confirm the taxonomy table name/presence in the chosen RND DB before applying).
5. **Comment stripping in the parser** — `_strip_line_comments` is line-wise and assumes no `--` inside string literals and no `/* */` blocks (true for these files). If future taxonomy edits add such constructs, the parser must be hardened.

## Risks for the audit agent
- **Not executed.** No Python in this environment → SQL + applier validated by reading + grep only. First real run should be `python apply_schema.py --db <RND.db> --dry-run`, then `--verify`.
- **Idempotency** rests on `CREATE TABLE IF NOT EXISTS` + `PRAGMA table_info` guarded ALTERs (mirrors `backend/power_user/db_init.py`). Re-running is a no-op by design.
- **No data writes, no DROP/rename/retype, no existing-row mutation** — additive only (INV7). Existing 865/1943 taxonomy rows untouched.
- **Boolean storage** is 0/1 INTEGER; downstream readers must not assume native bool.
- **Array/JSON columns** are TEXT holding JSON; downstream code owns serialization/deserialization.
