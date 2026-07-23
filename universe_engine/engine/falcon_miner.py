"""
Falcon Engine — pattern miner.

Strategy: fit shallow decision trees per (year × outcome target × scope) and
extract leaf paths (= feature-threshold rules) with statistically-significant
precision lift over the year-specific base rate.

Why decision trees:
  - Naturally discover feature COMBINATIONS, not single features
  - Output is human-readable: each leaf path = a deterministic rule
  - Shallow depth (3-5) keeps rules interpretable and reduces overfitting
  - Each leaf has clear support (n samples), precision (hit rate), lift (vs base)

Scopes:
  - 'universe' — mine across all stocks for that year
  - 'sector:XYZ' — mine only within sector XYZ for that year

(Stock-specific mining deferred to Phase 5; per-stock samples are too thin for
 reliable tree fits at year granularity. Promoted patterns will be tested per-
 stock in validation.)

Outputs:
  falcon_pattern_candidates table with one row per discovered rule.
"""
from __future__ import annotations
import json, sqlite3, statistics
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
from sklearn.tree import DecisionTreeClassifier, _tree


SCHEMA = """
CREATE TABLE IF NOT EXISTS falcon_pattern_candidates (
    pattern_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    mined_year     TEXT NOT NULL,
    scope          TEXT NOT NULL,            -- 'universe' or 'sector:XYZ'
    outcome_target TEXT NOT NULL,            -- e.g. 'hit_10pc_20d'
    n_obs          INTEGER NOT NULL,
    n_hits         INTEGER NOT NULL,
    precision_pct  REAL NOT NULL,
    base_rate_pct  REAL NOT NULL,
    lift_pct       REAL NOT NULL,            -- (precision - base_rate) in pp
    depth          INTEGER NOT NULL,
    rule_json      TEXT NOT NULL,            -- list of (feature, op, threshold) tuples
    rule_text      TEXT NOT NULL,            -- human-readable
    UNIQUE (mined_year, scope, outcome_target, rule_text)
);
CREATE INDEX IF NOT EXISTS idx_fpc_year_target  ON falcon_pattern_candidates(mined_year, outcome_target);
CREATE INDEX IF NOT EXISTS idx_fpc_lift          ON falcon_pattern_candidates(lift_pct DESC);
"""


def ensure_schema(con: sqlite3.Connection):
    con.executescript(SCHEMA)
    con.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Tree-rule extraction
# ─────────────────────────────────────────────────────────────────────────────

def _extract_leaf_rules(tree: DecisionTreeClassifier,
                          feature_names: List[str]) -> List[Dict]:
    """Walk a fitted DecisionTreeClassifier, return one dict per leaf with:
       - rule (list of (feature, op, threshold))
       - n_samples, n_class1 (positives at leaf), precision
    """
    t = tree.tree_
    out = []

    def recurse(node_id: int, path: List[Tuple[str, str, float]]):
        if t.children_left[node_id] == _tree.TREE_LEAF:
            # Leaf reached. sklearn 1.7+ stores class PROBABILITIES in tree_.value,
            # not counts. Total sample count is n_node_samples; pos count = prob × n.
            n_total = int(t.n_node_samples[node_id])
            value = t.value[node_id][0]    # shape (n_classes,) — probabilities
            prob_pos = float(value[1]) if len(value) > 1 else 0.0
            n_pos = int(round(prob_pos * n_total))
            out.append({
                "rule": path[:],
                "n_total": n_total,
                "n_pos":   n_pos,
                "precision": prob_pos,
            })
            return
        feat_idx = t.feature[node_id]
        threshold = float(t.threshold[node_id])
        feat_name = feature_names[feat_idx]
        # Left = "<=", Right = ">"
        recurse(t.children_left[node_id],  path + [(feat_name, "<=", threshold)])
        recurse(t.children_right[node_id], path + [(feat_name, ">",  threshold)])

    recurse(0, [])
    return out


def _rule_to_text(rule: List[Tuple[str, str, float]]) -> str:
    return " AND ".join(f"{f} {op} {th:.4f}" for f, op, th in rule)


# ─────────────────────────────────────────────────────────────────────────────
# Mining for one slice (year × target × scope)
# ─────────────────────────────────────────────────────────────────────────────

def _mine_slice(args) -> List[Dict]:
    db_path, year, scope, target, max_depth, min_samples_leaf, min_lift_pct = args

    con = sqlite3.connect(db_path, timeout=60.0)

    if scope == "universe":
        scope_join = ""
        scope_args = []
    elif scope.startswith("sector:"):
        sec = scope.split(":", 1)[1]
        scope_join = "INNER JOIN falcon_sectors s ON s.symbol = f.symbol AND s.sector = ?"
        scope_args = [sec]
    else:
        con.close(); return []

    # Pull joined feature × outcome rows for the year + scope
    feat_cols = [
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
    select_feats = ", ".join(f"f.{c}" for c in feat_cols)

    sql = f"""
        SELECT {select_feats}, o.{target}
        FROM falcon_features f
        INNER JOIN falcon_outcomes o ON o.symbol = f.symbol AND o.trade_date = f.trade_date
        {scope_join}
        WHERE substr(f.trade_date, 1, 4) = ?
          AND o.{target} IS NOT NULL
    """
    args_q = scope_args + [year]
    rows = con.execute(sql, args_q).fetchall()
    con.close()

    if len(rows) < 200:   # not enough data
        return []

    X = np.array([row[:-1] for row in rows], dtype=float)
    y = np.array([row[-1]  for row in rows], dtype=int)

    # Replace NaNs with column medians (sklearn DT doesn't handle NaN)
    col_medians = np.nanmedian(X, axis=0)
    X = np.where(np.isnan(X), col_medians, X)

    n_total = len(y)
    n_pos = int(y.sum())
    base_rate = n_pos / n_total
    if base_rate == 0 or base_rate == 1:
        return []

    # Fit a shallow tree
    clf = DecisionTreeClassifier(max_depth=max_depth,
                                    min_samples_leaf=min_samples_leaf,
                                    class_weight="balanced",
                                    random_state=42)
    clf.fit(X, y)

    leaves = _extract_leaf_rules(clf, feat_cols)
    candidates = []
    for leaf in leaves:
        if leaf["n_total"] < min_samples_leaf: continue
        prec = leaf["precision"]
        lift = (prec - base_rate) * 100   # in pp
        if lift < min_lift_pct: continue
        # Compress rule (multiple thresholds on same feature → keep tightest)
        rule = _compress_rule(leaf["rule"])
        candidates.append({
            "mined_year":     year,
            "scope":          scope,
            "outcome_target": target,
            "n_obs":          leaf["n_total"],
            "n_hits":         leaf["n_pos"],
            "precision_pct":  round(prec * 100, 4),
            "base_rate_pct":  round(base_rate * 100, 4),
            "lift_pct":       round(lift, 4),
            "depth":          len(rule),
            "rule_json":      json.dumps(rule),
            "rule_text":      _rule_to_text(rule),
        })
    return candidates


def _compress_rule(rule: List[Tuple[str, str, float]]) -> List[Tuple[str, str, float]]:
    """Multiple constraints on same feature → keep tightest. e.g.
       'rsi_14 > 50' AND 'rsi_14 > 55' → 'rsi_14 > 55'."""
    by_feat: Dict[str, Dict[str, float]] = defaultdict(dict)
    for f, op, th in rule:
        if op == "<=":
            cur = by_feat[f].get("<=")
            by_feat[f]["<="] = th if cur is None else min(cur, th)
        elif op == ">":
            cur = by_feat[f].get(">")
            by_feat[f][">"] = th if cur is None else max(cur, th)
    out = []
    for f, ops in sorted(by_feat.items()):
        if "<=" in ops: out.append((f, "<=", round(ops["<="], 4)))
        if ">"  in ops: out.append((f, ">",  round(ops[">"], 4)))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────────────

def mine_candidates(db_path: Path,
                     years: List[str],
                     targets: List[str],
                     scopes: List[str],
                     max_depth: int = 4,
                     min_samples_leaf: int = 50,
                     min_lift_pct: float = 10.0,
                     n_workers: int = 16) -> int:
    """Returns number of candidate patterns persisted."""
    n_workers = max(10, min(48, n_workers))
    con = sqlite3.connect(db_path, timeout=60.0)
    ensure_schema(con)
    # Clear previous candidates (idempotent re-run)
    con.execute("DELETE FROM falcon_pattern_candidates")
    con.commit()
    con.close()

    args_list = [(str(db_path), yr, sc, tg, max_depth, min_samples_leaf, min_lift_pct)
                  for yr in years for sc in scopes for tg in targets]
    print(f"[mine] {len(args_list)} mining slices  ·  {n_workers} workers  ·  "
          f"depth={max_depth}, min_leaf={min_samples_leaf}, min_lift={min_lift_pct}pp")

    all_candidates: List[Dict] = []
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_mine_slice, a): (a[1], a[2], a[3]) for a in args_list}
        done = 0
        for f in as_completed(futs):
            yr, sc, tg = futs[f]
            try:
                c = f.result()
            except Exception as e:
                print(f"  [{yr}/{sc}/{tg}] ERROR: {e}", flush=True); c = []
            all_candidates.extend(c)
            done += 1
            if done % 50 == 0:
                print(f"  [{done}/{len(args_list)}] candidates so far={len(all_candidates):,}",
                       flush=True)

    print(f"\n[mine] Total candidates discovered: {len(all_candidates):,}")

    # Persist
    con = sqlite3.connect(db_path, timeout=60.0)
    payload = [(c["mined_year"], c["scope"], c["outcome_target"],
                 c["n_obs"], c["n_hits"], c["precision_pct"],
                 c["base_rate_pct"], c["lift_pct"], c["depth"],
                 c["rule_json"], c["rule_text"])
                for c in all_candidates]
    con.executemany("""
        INSERT OR IGNORE INTO falcon_pattern_candidates
            (mined_year, scope, outcome_target, n_obs, n_hits,
             precision_pct, base_rate_pct, lift_pct, depth,
             rule_json, rule_text)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, payload)
    con.commit()
    n_persisted = con.execute("SELECT COUNT(*) FROM falcon_pattern_candidates").fetchone()[0]
    con.close()
    return n_persisted
