# S-TIER-DISCOVERY — Self-Learning Walk-Forward Tier Engine (build log)

**File:** `universe_engine/self_improving/falcon_tier_discovery.py`
**Branch / tree:** `feat/self-improving-engine` in `C:\Users\SPS\Desktop\kanida-dev` (dev worktree). PROD tree never touched.
**Status:** WRITTEN. Not run (no Python on this machine — orchestrator runs `--dry-run`, audits, applies). Not committed.

---

## What it does (one line)
Assigns every Falcon top-10 signal a **TIER at signal-generation time** (GOLD / ENTERPRISE / STANDARD / AVOID) and **discovers the ENTERPRISE rulebook** by beam search over signal-time-only features, **promoting rules on pooled walk-forward OOS only**.

---

## Pipeline (the chain the orchestrator will see in stdout)

1. **Load** (`load_rows`): `falcon_signal_day_context ⋈ falcon_signal_day_study ON id` in the RND DB.
   - TRAIN = `net_ret_pct IS NOT NULL AND is_extension=0` (~10,063 rows). LABEL `win = net_ret_pct > 0`; `year = signal_date[:4]`.
   - TODAY = `is_extension=1`, latest signal_date only (the live top-10), sorted by `engine_rank`.
   - `_merge_signal_time` copies ONLY signal-time features + identity into each row. Forbidden columns are never even carried.

2. **GOLD baseline** (`gold_baseline`): `signal_day_ret_pct <= 0`. IS + pooled walk-forward OOS WR/avg_ret/N. Also the `<= +2` "more volume" variant. GOLD's **OOS avg_ret** is the bar ENTERPRISE must beat.

3. **ENTERPRISE discovery** (`beam_search`): greedy "law of scaling" — start from atomic predicates, greedily add ONE condition at a time that lifts WR by ≥ `MIN_WR_IMPROVE_PP`, beam width 5, depth ≤ 5. Numeric features → decile threshold splits (`<=` and `>`); categorical (`signal_day_circuit`, `sector`) → equality with ≥ `MIN_LEAF_N` support. Each kept conjunction stores its **full scaling path** `[(condition added, cumulative WR, cumulative N, avg_ret), …]`.

4. **Walk-forward** (`walk_forward`): for each fixed conjunction, evaluate per test-year {2022..2026}, then POOL. Returns per-year stats, pooled stats, and consistency counters.

5. **Promotion** (`classify_candidate`): **OOS-only** gate
   - pooled OOS WR ≥ **80%** AND
   - pooled OOS N ≥ **100** AND
   - pooled OOS avg_ret > GOLD's OOS avg_ret AND
   - per-year: OOS WR ≥ **75%** in ≥ **4 of 5** years, each counted only if that year N ≥ **18**.
   - High WR but thin/inconsistent → `INSUFFICIENT_DATA` (reported, NOT promoted). Else just under gate → `CANDIDATE` (near-miss list).

6. **Self-learning** (`falcon_tier_rules`, dated snapshots): `previously_promoted()` reads the most recent snapshot strictly before `as_of`; any prior-PROMOTED rule_id not re-promoted this run is marked **DEMOTED** (with a stub row if it no longer surfaces). `write_rulebook` is **idempotent per as_of** (delete that as_of's rows, re-insert) and **keeps history** for other as_of values. `rule_id` = stable `ENT_<sha1(conditions_json)[:10]>`.

7. **Apply** (`assign_tier`): ENTERPRISE if matches ANY promoted enterprise rule; else GOLD if `signal_day_ret<=0`; else AVOID if `signal_day_ret>15`; else STANDARD. Applied to every resolved TRAIN row (Tier Performance) and to TODAY's top-10.

8. **Frontier** (`build_frontier`): non-dominated (OOS WR ↓, OOS N ↑) points — the operator prices off this ("at 85% WR we have N rows; drop to 80% and N triples").

9. **Excel** `out/v9/falcon_tier_discovery.xlsx`: (1) Rulebook (IS+OOS, status, overfit flag), (2) Scaling Path (best rule's WR-climb), (3) OOS WR-vs-Volume Frontier, (4) Tier Performance (per-tier N/WR/avg_ret + per-year OOS), (5) Today's Top-10 Tiered (symbol, rank, signal_day_ret, vol_ratio, two_day_ret, tier, why). openpyxl with CSV fallback.

---

## Integrity guards
- **Signal-time-only features** enforced three ways: (a) `_ALLOWED_FEATURES` whitelist, (b) `_is_forbidden()` rejects `entry_gap_pct`, `net_ret_pct`, `exit_reason`, any `d<N>_*_ret`, any `post_hold_*`, (c) `Condition.__init__` asserts, (d) `assert_no_forbidden()` re-checks every promoted conjunction, (e) a final parity print confirms no rule used a forbidden column.
- **Promotion on OOS only.** Discovery may PROPOSE on the full set; promotion reads pooled OOS + per-year consistency. Overfit flag printed when IS−OOS WR gap > 10pp.
- **NULL discipline:** a predicate on a NULL feature returns False (never imputed).
- **RND-only writes** (only `falcon_tier_rules`). PROD opened read-only/unused. Never modifies `falcon_signal_day_study` / `falcon_signal_day_context` / `out/v3..v8`.
- **`--dry-run`** prints rulebook + frontier + today's tiers and writes nothing.
- **`--as-of`** controls the snapshot date (default = max signal_date). Never calls wall-clock now.

---

## CLI
```
python falcon_tier_discovery.py --rnd-db <RND.db> --prod-db <PROD.db> [--out out/v9] [--as-of YYYY-MM-DD]
python falcon_tier_discovery.py --dry-run
```
Defaults: `--rnd-db` from persona resolver, `--prod-db` = config.POWER_DB_PATH, `--out` = `./out/v9`.

---

## Tunable constants (top of file)
`TEST_YEARS`, `ENT_OOS_WR_GATE=80`, `ENT_OOS_N_GATE=100`, `ENT_PER_YEAR_WR=75`, `ENT_PER_YEAR_MIN_YEARS=4`, `ENT_PER_YEAR_MIN_N=18`, `OVERFIT_GAP_PP=10`, `BEAM_WIDTH=5`, `MAX_DEPTH=5`, `QUANTILE_DECILES`, `MIN_LEAF_N=50`, `MIN_WR_IMPROVE_PP=0.5`, `GOLD_MAX_SIGNAL_RET=0`, `GOLD_RELAXED_MAX=2`, `AVOID_SIGNAL_RET=15`.

---

## Honesty contract built into the run
If NO conjunction clears the 80% / N≥100 / consistency gate, the script says so explicitly and reports the **BEST ROBUST rule** (highest OOS WR at N≥100, else N≥18) plus the **full WR-vs-volume frontier** — so the operator can price tiers off the achievable frontier rather than a non-existent 80% rule.

---

## Open items for orchestrator
- Run `--dry-run` first; confirm training N ≈ 10,063 and GOLD OOS WR ≈ 70% in the parity print.
- Audit the promoted rule(s) / best-robust rule and IS-OOS gaps before `--apply`.
- If gate proves too strict in this regime, `ENT_*` constants are single-point knobs.
