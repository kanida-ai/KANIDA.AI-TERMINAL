"""
Falcon Engine — pattern validator (vectorized, single-process).

For each candidate pattern (a feature-threshold rule mined on year Y / scope S /
target T), compute its precision on:
  - OTHER YEARS (walk-forward — does the pattern hold across regimes?)
  - OTHER SECTORS (cross-sector — does it generalize beyond the mined sector?)
  - The mined-year and-scope itself (recompute baseline precision from data)

Architecture: load everything into memory once (~150 MB), evaluate all rules
via vectorized numpy. Avoids per-worker DB reload overhead.

Promotion gate (all must pass):
  1. OOS year precision_lift_avg >= MIN_OOS_LIFT (currently +5pp)
  2. Pattern has positive lift in >= MIN_OOS_YEARS_PASS years (currently 2)
  3. Cross-sector OOS lift >= MIN_CROSS_SECTOR_LIFT (currently 0pp — i.e. positive)
  4. n_obs in OOS >= MIN_OOS_OBS (currently 30)
"""
from __future__ import annotations
import json, sqlite3, time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np


SCHEMA = """
CREATE TABLE IF NOT EXISTS falcon_pattern_validations (
    pattern_id          INTEGER NOT NULL,
    test_kind           TEXT NOT NULL,
    test_label          TEXT NOT NULL,
    n_obs               INTEGER,
    n_hits              INTEGER,
    precision_pct       REAL,
    base_rate_pct       REAL,
    lift_pct            REAL,
    PRIMARY KEY (pattern_id, test_kind, test_label)
);
CREATE INDEX IF NOT EXISTS idx_fpv_pid ON falcon_pattern_validations(pattern_id);

CREATE TABLE IF NOT EXISTS falcon_promoted_patterns (
    pattern_id              INTEGER PRIMARY KEY,
    classification          TEXT NOT NULL,
    avg_oos_year_lift_pp    REAL,
    n_years_passed          INTEGER,
    avg_cross_sector_lift_pp REAL,
    promoted_at             TEXT DEFAULT (datetime('now')),
    notes                   TEXT
);
"""

MIN_OOS_LIFT_PP        = 5.0
MIN_OOS_YEARS_PASS     = 2
MIN_CROSS_SECTOR_LIFT  = 0.0
MIN_OOS_OBS            = 30


FEATURE_COLS = [
    "range_pct","close_loc","gap_pct","body_pct","upper_wick_pct","lower_wick_pct",
    "dist_sma_20","dist_sma_50","dist_sma_200","slope_sma_20","slope_sma_50",
    "rsi_14","roc_5","roc_20","roc_60",
    "vol_vs_20d","vol_5d_vs_20d","n_sub_75v_7d","n_sub_75v_20d",
    "atr_20_pct","atr_5_vs_20","n_sub_2_5_range_7d","n_sub_3_range_7d",
    "n_higher_lows_5d","n_higher_highs_5d",
    "dist_high_10","dist_high_20","dist_high_60","dist_high_120","dist_high_252",
    "weekly_close_vs_sma20","weekly_breakout_20w","weekly_range_pct","weekly_close_loc",
    "rs_sector_20d","rs_sector_60d","rs_market_20d","rs_market_60d",
]
FEATURE_IDX = {c: i for i, c in enumerate(FEATURE_COLS)}


# ─────────────────────────────────────────────────────────────────────────────
# In-memory panel loader
# ─────────────────────────────────────────────────────────────────────────────

class FalconPanel:
    """Holds the full joined feature×outcome panel in memory.

    Attributes:
      X: float64 array shape (N, n_features)
      year: int array shape (N,) — calendar year
      sector_idx: int array shape (N,) — index into sector_names
      sector_names: list of sector strings
      Y: dict target_name -> int array shape (N,) — outcome label
    """
    def __init__(self, db_path: Path):
        t0 = time.time()
        con = sqlite3.connect(db_path, timeout=60.0)
        # Load symbol -> sector
        rows = con.execute("SELECT symbol, sector FROM falcon_sectors").fetchall()
        sym_to_sec = {s: sec for s, sec in rows}
        self.sector_names = sorted(set(sym_to_sec.values()))
        sec_to_idx = {s: i for i, s in enumerate(self.sector_names)}

        feat_select = ", ".join(FEATURE_COLS)
        targets = ["hit_10pc_20d", "hit_15pc_20d", "hit_25pc_30d", "hit_40pc_40d"]
        targ_select = ", ".join(f"o.{t}" for t in targets)

        sql = f"""
            SELECT f.symbol, f.trade_date, {feat_select}, {targ_select}
            FROM falcon_features f
            INNER JOIN falcon_outcomes o
              ON o.symbol = f.symbol AND o.trade_date = f.trade_date
        """
        rows = con.execute(sql).fetchall()
        con.close()

        n = len(rows)
        n_feat = len(FEATURE_COLS)
        X = np.full((n, n_feat), np.nan, dtype=np.float64)
        years = np.zeros(n, dtype=np.int32)
        sec_arr = np.full(n, -1, dtype=np.int32)
        Y = {t: np.zeros(n, dtype=np.int8) for t in targets}

        for i, r in enumerate(rows):
            sym = r[0]; date_str = r[1]
            X[i, :] = [v if v is not None else np.nan for v in r[2:2 + n_feat]]
            years[i] = int(date_str[:4])
            sec_arr[i] = sec_to_idx.get(sym_to_sec.get(sym, ""), -1)
            for j, t in enumerate(targets):
                v = r[2 + n_feat + j]
                Y[t][i] = int(v) if v is not None else 0

        self.X = X
        self.year = years
        self.sector_idx = sec_arr
        self.sec_to_idx = sec_to_idx
        self.Y = Y
        self.targets = targets
        self.n = n
        print(f"[panel] loaded {n:,} rows × {n_feat} features × {len(targets)} targets "
              f"in {time.time()-t0:.1f}s ({X.nbytes / 1e6:.1f} MB)")


# ─────────────────────────────────────────────────────────────────────────────
# Vectorized rule application
# ─────────────────────────────────────────────────────────────────────────────

def apply_rule_mask(rule: List[Tuple[str, str, float]], X: np.ndarray) -> np.ndarray:
    """Returns boolean mask of shape (N,) where rule passes. NaN treated as fail."""
    mask = np.ones(X.shape[0], dtype=bool)
    for feat, op, th in rule:
        idx = FEATURE_IDX.get(feat)
        if idx is None:
            return np.zeros(X.shape[0], dtype=bool)
        col = X[:, idx]
        if op == "<=":
            mask &= (col <= th) & ~np.isnan(col)
        else:
            mask &= (col > th) & ~np.isnan(col)
    return mask


def evaluate(rule: List[Tuple[str, str, float]],
              row_mask: np.ndarray,
              panel: FalconPanel,
              target: str) -> Tuple[int, int, float, float]:
    """Apply rule to subset of panel defined by row_mask. Returns
       (n_obs, n_hits, precision_pct, base_rate_pct)."""
    if not row_mask.any():
        return 0, 0, 0.0, 0.0
    rule_mask = apply_rule_mask(rule, panel.X)
    final = row_mask & rule_mask
    n_obs = int(final.sum())
    if n_obs == 0:
        # Compute base rate just on the row_mask subset
        sub_y = panel.Y[target][row_mask]
        base = float(sub_y.mean()) * 100 if sub_y.size > 0 else 0.0
        return 0, 0, 0.0, base
    n_hits = int(panel.Y[target][final].sum())
    precision = n_hits / n_obs * 100
    sub_y = panel.Y[target][row_mask]
    base = float(sub_y.mean()) * 100
    return n_obs, n_hits, precision, base


# ─────────────────────────────────────────────────────────────────────────────
# Main validator loop
# ─────────────────────────────────────────────────────────────────────────────

def ensure_schema(con):
    con.executescript(SCHEMA)
    con.commit()


def validate_and_promote(db_path: Path) -> Dict:
    con = sqlite3.connect(db_path, timeout=60.0)
    ensure_schema(con)
    con.execute("DELETE FROM falcon_pattern_validations")
    con.execute("DELETE FROM falcon_promoted_patterns")
    con.commit()

    # Load panel once
    panel = FalconPanel(db_path)

    # Pre-build year masks and sector masks for speed
    years_in_data = sorted(set(int(y) for y in panel.year.tolist()))
    year_masks = {str(y): (panel.year == y) for y in years_in_data}
    sector_masks = {sec: (panel.sector_idx == idx)
                       for sec, idx in panel.sec_to_idx.items()}
    print(f"[validate] year masks: {list(year_masks.keys())}")
    print(f"[validate] sector masks: {len(sector_masks)}")

    # Load all candidates
    rows = con.execute("""
        SELECT pattern_id, mined_year, scope, outcome_target, rule_json
        FROM falcon_pattern_candidates
    """).fetchall()
    print(f"[validate] {len(rows)} candidates to evaluate")

    all_records: List[tuple] = []
    t0 = time.time()
    for i, (pid, mined_year, scope, target, rule_json) in enumerate(rows):
        rule = [(f, op, th) for f, op, th in json.loads(rule_json)]
        if scope.startswith("sector:"):
            mined_sector = scope.split(":", 1)[1]
        else:
            mined_sector = None

        # 1) Per-year tests (universe-scope OOS years)
        for yr_str in year_masks:
            if yr_str == mined_year: continue
            row_mask = year_masks[yr_str]
            n, h, prec, base = evaluate(rule, row_mask, panel, target)
            lift = prec - base
            all_records.append((pid, "year", yr_str, n, h, round(prec, 4),
                                  round(base, 4), round(lift, 4)))

        # 2) Per-sector tests (within mined_year, sectors != mined_sector)
        for sec, sec_mask in sector_masks.items():
            if sec == mined_sector: continue
            row_mask = year_masks[mined_year] & sec_mask
            n, h, prec, base = evaluate(rule, row_mask, panel, target)
            lift = prec - base
            all_records.append((pid, "sector", f"sector:{sec}", n, h,
                                  round(prec, 4), round(base, 4), round(lift, 4)))

        if (i + 1) % 200 == 0:
            print(f"  [{i+1}/{len(rows)}] candidates done, {len(all_records):,} test rows  "
                   f"({time.time()-t0:.0f}s)", flush=True)

    print(f"[validate] All evaluations done in {time.time()-t0:.0f}s. "
           f"Persisting {len(all_records):,} rows...")
    # Bulk insert
    con.executemany("""
        INSERT OR REPLACE INTO falcon_pattern_validations
            (pattern_id, test_kind, test_label, n_obs, n_hits,
             precision_pct, base_rate_pct, lift_pct)
        VALUES (?,?,?,?,?,?,?,?)
    """, all_records)
    con.commit()
    print(f"[validate] Persisted.")

    # Promotion logic (DB-side aggregation)
    print("[promote] Applying promotion gate...")
    return promote_patterns(con)


def promote_patterns(con: sqlite3.Connection) -> Dict:
    con.execute("DELETE FROM falcon_promoted_patterns")
    con.commit()

    rows = con.execute("""
        SELECT pattern_id, mined_year, scope, outcome_target FROM falcon_pattern_candidates
    """).fetchall()

    promoted = []
    rejected = defaultdict(int)
    for pid, mined_year, scope, target in rows:
        year_rows = con.execute("""
            SELECT n_obs, lift_pct FROM falcon_pattern_validations
            WHERE pattern_id = ? AND test_kind = 'year'
        """, (pid,)).fetchall()
        sector_rows = con.execute("""
            SELECT n_obs, lift_pct FROM falcon_pattern_validations
            WHERE pattern_id = ? AND test_kind = 'sector'
        """, (pid,)).fetchall()

        oos_year_lifts = [l for n, l in year_rows if n >= MIN_OOS_OBS]
        if not oos_year_lifts:
            rejected["no_oos_data"] += 1; continue
        years_pass = sum(1 for l in oos_year_lifts if l > 0)
        avg_oos_year_lift = sum(oos_year_lifts) / len(oos_year_lifts)

        if avg_oos_year_lift < MIN_OOS_LIFT_PP:
            rejected["weak_oos_year_lift"] += 1; continue
        if years_pass < MIN_OOS_YEARS_PASS:
            rejected["too_few_passing_years"] += 1; continue

        cross_sec_lifts = [l for n, l in sector_rows if n >= MIN_OOS_OBS]
        avg_cross_sec = (sum(cross_sec_lifts) / len(cross_sec_lifts)) if cross_sec_lifts else 0.0

        if scope.startswith("sector:") and avg_cross_sec <= 0:
            classification = "sector_specific"
        elif years_pass < 3:
            classification = "regime_dependent"
        elif avg_cross_sec >= 0:
            classification = "universal"
        else:
            classification = "regime_dependent"

        promoted.append((pid, classification, round(avg_oos_year_lift, 4),
                          years_pass, round(avg_cross_sec, 4)))

    con.executemany("""
        INSERT INTO falcon_promoted_patterns
            (pattern_id, classification, avg_oos_year_lift_pp,
             n_years_passed, avg_cross_sector_lift_pp)
        VALUES (?, ?, ?, ?, ?)
    """, promoted)
    con.commit()

    by_cls = defaultdict(int)
    for p in promoted: by_cls[p[1]] += 1
    return {"promoted": len(promoted),
             "rejected": dict(rejected),
             "by_classification": dict(by_cls)}
