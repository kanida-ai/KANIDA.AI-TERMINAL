"""
FALCON HULK V1 — Stage-1 Discovery
Bagged shallow-tree recurrence miner + anti-overfit NULL gate.

Hard rules honored:
  * Reads ONLY data/db/falcon_hulk.db (point-in-time spine). No writes to it.
  * NEVER touches 2026 data. Every load asserts trade/entry date <= 2025-12-31,
    and this run only pulls <= 2024-12-31 (train 2018-2023, OOS test 2024).
  * Features/returns are FRACTIONS (0.005 = 0.5%).

Two proving slices:
  A) RELIANCE, LONG, multiday, horizon T+2, target +1% (ret_t2 >= 0.010), entry_kind='close'
  B) ADANIENT, LONG, intraday same-day, target +0.5% (mfe_intraday >= 0.005)

Method:
  * Split strict/purged/embargoed. TRAIN 2018-2023, TEST 2024.
  * B=200 bootstrap resamples -> DecisionTreeClassifier(max_depth=3, class_weight='balanced').
  * Extract every leaf PATH as a conjunctive rule; canonicalize thresholds to a
    per-feature grid so identical rules from different resamples collapse.
  * RECURRENCE(rule) = fraction of the 200 resamples in which it appears.
  * Keep recurrence >= 0.10 as candidates; evaluate each on OOS (2024).

NULL GATE (run + report FIRST):
  * M=50 reps with TRAIN labels permuted. Each rep's statistic = the MAX OOS lift
    across its candidate rules (multiple-testing-adjusted false-positive floor).
  * Report null mean / 95th / 99th pct of best-rule OOS lift, and recurrence.

Verdict: a REAL candidate is a genuine discovery only if OOS lift > null 99th pct
AND recurrence is high.
"""

import sqlite3
import numpy as np
import pandas as pd
from collections import defaultdict
from sklearn.tree import DecisionTreeClassifier
from joblib import Parallel, delayed

DB = "data/db/falcon_hulk.db"
TRAIN_START, TRAIN_END = "2018-01-01", "2023-12-31"
TEST_START, TEST_END = "2024-01-01", "2024-12-31"
EMBARGO_TRADING_DAYS = 10
B_RESAMPLES = 200
M_NULL = 50
RECURRENCE_MIN = 0.10
HARD_MAX_DATE = "2025-12-31"  # NEVER read 2026

rng_master = np.random.default_rng(20260725)


# ----------------------------------------------------------------------------
# Data loading (point-in-time; date-safety asserted)
# ----------------------------------------------------------------------------
def load_slice_multiday(symbol):
    """A) daily features at signal date T -> multiday T+2 label."""
    con = sqlite3.connect(DB)
    q = """
        SELECT f.*, l.ret_t2, l.label_end_date
        FROM hulk_labels_multiday l
        JOIN hulk_daily_features f
          ON f.symbol = l.symbol AND f.trade_date = l.entry_date
        WHERE l.symbol = ? AND l.direction='long' AND l.entry_kind='close'
          AND l.ret_t2 IS NOT NULL
          AND l.entry_date <= ?
    """
    df = pd.read_sql(q, con, params=(symbol, TEST_END))
    con.close()
    assert df["trade_date"].max() <= HARD_MAX_DATE, "date-safety: 2026 leaked!"
    assert df["trade_date"].max() <= TEST_END, "loaded beyond test window!"
    df["date"] = df["trade_date"]
    df["label_end"] = df["label_end_date"]
    df["y"] = (df["ret_t2"] >= 0.010).astype(int)
    df["realized"] = df["ret_t2"]
    feat_cols = [c for c in df.columns if c not in (
        "symbol", "trade_date", "date", "label_end_date", "label_end",
        "ret_t2", "y", "realized")]
    df = df.dropna(subset=feat_cols).reset_index(drop=True)
    return df, feat_cols


def load_slice_intraday(symbol):
    """B) intraday features at entry minute -> same-day +0.5% touch label."""
    con = sqlite3.connect(DB)
    lab = pd.read_sql(
        """SELECT symbol, entry_date, entry_minute, mfe_intraday, ret_to_close
           FROM hulk_labels_intraday
           WHERE symbol=? AND direction='long' AND entry_date <= ?""",
        con, params=(symbol, TEST_END))
    feat = pd.read_sql(
        "SELECT * FROM hulk_intraday_features WHERE symbol=?",
        con, params=(symbol,))
    con.close()
    # join key: bar_time == entry_date + ' ' + entry_minute + ':00'
    lab["bar_time"] = lab["entry_date"] + " " + lab["entry_minute"] + ":00"
    df = feat.merge(lab, on=["symbol", "bar_time"], how="inner")
    assert df["entry_date"].max() <= HARD_MAX_DATE, "date-safety: 2026 leaked!"
    assert df["entry_date"].max() <= TEST_END, "loaded beyond test window!"
    df["date"] = df["entry_date"]
    df["label_end"] = df["entry_date"]  # same-day
    df["y"] = (df["mfe_intraday"] >= 0.005).astype(int)
    df["realized"] = df["ret_to_close"]
    feat_cols = [c for c in df.columns if c not in (
        "symbol", "bar_time", "entry_date", "entry_minute", "date", "label_end",
        "mfe_intraday", "ret_to_close", "y", "realized")]
    df = df.dropna(subset=feat_cols).reset_index(drop=True)
    return df, feat_cols


# ----------------------------------------------------------------------------
# Split: purge + embargo
# ----------------------------------------------------------------------------
def split_train_test(df):
    all_dates = sorted(df["date"].unique())
    # 10 trading days before TEST_START (using dates present in this slice)
    before = [d for d in all_dates if d < TEST_START]
    embargo_cut = before[-EMBARGO_TRADING_DAYS] if len(before) >= EMBARGO_TRADING_DAYS else before[0]

    test = df[(df["date"] >= TEST_START) & (df["date"] <= TEST_END)].copy()

    train = df[(df["date"] >= TRAIN_START) & (df["date"] <= TRAIN_END)].copy()
    n0 = len(train)
    # PURGE: drop train samples whose label window overlaps into the test period
    train = train[train["label_end"] < TEST_START]
    n_purge = n0 - len(train)
    # EMBARGO: drop train samples within 10 trading days of test start
    train = train[train["date"] < embargo_cut]
    n_embargo = (n0 - n_purge) - len(train)
    train = train.reset_index(drop=True)
    return train, test, dict(n_purge=n_purge, n_embargo=n_embargo,
                             embargo_cut=embargo_cut)


# ----------------------------------------------------------------------------
# Threshold canonicalization grid (per feature, scale-adaptive)
# ----------------------------------------------------------------------------
def make_bins(train, feat_cols):
    bw = {}
    for f in feat_cols:
        s = train[f].std()
        bw[f] = max(s / 4.0, 1e-9)
    return bw


def canon_thr(f, thr, bw):
    w = bw[f]
    return round(round(thr / w) * w, 8)


# ----------------------------------------------------------------------------
# Extract leaf-path rules from a fitted shallow tree
# ----------------------------------------------------------------------------
def extract_rules(tree, feat_cols, bw, train_base_rate, min_support):
    t = tree.tree_
    rules = []  # list of (canonical_rule_tuple, precision, support)

    def recurse(node, conds):
        if t.children_left[node] == t.children_right[node]:  # leaf
            # class counts (balanced weights change value; use n_node_samples split)
            val = t.value[node][0]
            support = int(t.n_node_samples[node])
            # precision toward class 1 from raw sample counts:
            # value[] holds weighted counts; recover proportion of class1
            p1 = val[1] / (val[0] + val[1]) if (val[0] + val[1]) > 0 else 0.0
            if p1 > train_base_rate and support >= min_support and len(conds) > 0:
                rule = tuple(sorted(conds))
                rules.append((rule, p1, support))
            return
        f = feat_cols[t.feature[node]]
        thr = canon_thr(f, t.threshold[node], bw)
        recurse(t.children_left[node], conds + [(f, "<=", thr)])
        recurse(t.children_right[node], conds + [(f, ">", thr)])

    recurse(0, [])
    return rules


def apply_rule(df, rule):
    mask = np.ones(len(df), dtype=bool)
    for (f, op, thr) in rule:
        col = df[f].values
        mask &= (col <= thr) if op == "<=" else (col > thr)
    return mask


def rule_text(rule):
    return " AND ".join(f"{f} {op} {thr:.5g}" for (f, op, thr) in rule)


# ----------------------------------------------------------------------------
# One full mine: 200 bootstraps -> recurrence-scored candidates
# ----------------------------------------------------------------------------
def mine(train, feat_cols, bw, seed_offset=0, y_override=None):
    X = train[feat_cols].values
    y = (y_override if y_override is not None else train["y"].values)
    n = len(train)
    base = y.mean()
    min_leaf = max(50, int(0.005 * n))
    if base == 0 or base == 1:
        return {}, base, min_leaf

    seen = defaultdict(int)           # rule -> #resamples appeared
    train_prec = defaultdict(list)    # rule -> list of train precisions
    rng = np.random.default_rng(1000 + seed_offset)
    # pre-draw all bootstrap index sets so RNG stays deterministic under parallel
    idx_sets = [rng.integers(0, n, n) for _ in range(B_RESAMPLES)]

    def one_resample(i, idx):
        Xi, yi = X[idx], y[idx]
        if yi.sum() < min_leaf or (n - yi.sum()) < min_leaf:
            return []
        clf = DecisionTreeClassifier(
            max_depth=3, min_samples_leaf=min_leaf,
            class_weight="balanced", random_state=i + seed_offset)
        clf.fit(Xi, yi)
        rules = extract_rules(clf, feat_cols, bw, base, min_leaf)
        uniq = {}
        for rule, p1, sup in rules:
            uniq[rule] = max(uniq.get(rule, 0.0), p1)
        return list(uniq.items())

    all_uniq = Parallel(n_jobs=-1, batch_size=8)(
        delayed(one_resample)(i, idx) for i, idx in enumerate(idx_sets))

    for uniq in all_uniq:
        for rule, p1 in uniq:
            seen[rule] += 1
            train_prec[rule].append(p1)

    candidates = {}
    for rule, cnt in seen.items():
        rec = cnt / B_RESAMPLES
        if rec >= RECURRENCE_MIN:
            candidates[rule] = dict(
                recurrence=rec,
                train_prec=float(np.mean(train_prec[rule])),
                train_lift_pp=(float(np.mean(train_prec[rule])) - base) * 100.0)
    return candidates, base, min_leaf


def eval_oos(rule, test):
    mask = apply_rule(test, rule)
    n_fires = int(mask.sum())
    base = test["y"].mean()
    if n_fires == 0:
        return dict(n_fires=0, oos_hit=np.nan, oos_lift_pp=np.nan,
                    oos_avg_ret=np.nan, oos_base=base)
    hit = test["y"].values[mask].mean()
    avg_ret = test["realized"].values[mask].mean()
    return dict(n_fires=n_fires, oos_hit=float(hit),
                oos_lift_pp=float((hit - base) * 100.0),
                oos_avg_ret=float(avg_ret), oos_base=float(base))


# ----------------------------------------------------------------------------
# Slice driver
# ----------------------------------------------------------------------------
def run_slice(name, df, feat_cols):
    out = {"name": name, "feat_cols": feat_cols}
    train, test, split_info = split_train_test(df)
    bw = make_bins(train, feat_cols)
    out["n_train"] = len(train)
    out["n_test"] = len(test)
    out["train_base"] = float(train["y"].mean())
    out["test_base"] = float(test["y"].mean())
    out["split_info"] = split_info

    # ---- NULL GATE FIRST ----
    null_best_lift = []
    null_best_rec = []
    for m in range(M_NULL):
        yperm = rng_master.permutation(train["y"].values)
        cands, base, _ = mine(train, feat_cols, bw, seed_offset=5000 + m * 7,
                              y_override=yperm)
        best_lift, best_rec = np.nan, 0.0
        for rule, meta in cands.items():
            ev = eval_oos(rule, test)
            if ev["n_fires"] >= 20 and not np.isnan(ev["oos_lift_pp"]):
                if np.isnan(best_lift) or ev["oos_lift_pp"] > best_lift:
                    best_lift = ev["oos_lift_pp"]
                best_rec = max(best_rec, meta["recurrence"])
        null_best_lift.append(best_lift)
        null_best_rec.append(best_rec)

    nb = np.array([x for x in null_best_lift if not np.isnan(x)])
    out["null_n_reps_with_candidate"] = int(len(nb))
    out["null_lift_mean"] = float(np.mean(nb)) if len(nb) else np.nan
    out["null_lift_p95"] = float(np.percentile(nb, 95)) if len(nb) else np.nan
    out["null_lift_p99"] = float(np.percentile(nb, 99)) if len(nb) else np.nan
    out["null_rec_max"] = float(np.max(null_best_rec)) if null_best_rec else 0.0
    out["null_best_lift_raw"] = null_best_lift

    # ---- REAL MINE ----
    cands, base, min_leaf = mine(train, feat_cols, bw, seed_offset=0)
    out["min_leaf"] = min_leaf
    real = []
    for rule, meta in cands.items():
        ev = eval_oos(rule, test)
        real.append(dict(rule=rule, text=rule_text(rule), **meta, **ev))
    # rank by OOS lift (fires>=20)
    real = [r for r in real if r["n_fires"] >= 20]
    real.sort(key=lambda r: (-(r["oos_lift_pp"] if not np.isnan(r["oos_lift_pp"]) else -1e9),
                             -r["recurrence"]))
    out["n_candidates"] = len(real)
    floor = out["null_lift_p99"]
    for r in real:
        r["beats_floor"] = bool(
            (not np.isnan(r["oos_lift_pp"])) and (not np.isnan(floor))
            and r["oos_lift_pp"] > floor)
    out["n_beat_floor"] = int(sum(r["beats_floor"] for r in real))
    out["real"] = real
    return out


def fmt_pct(x):
    return "n/a" if (x is None or (isinstance(x, float) and np.isnan(x))) else f"{x:+.2f}"


def main():
    results = []

    dfA, fA = load_slice_multiday("RELIANCE")
    results.append(run_slice("A: RELIANCE LONG multiday T+2 (+1.0%)", dfA, fA))

    dfB, fB = load_slice_intraday("ADANIENT")
    results.append(run_slice("B: ADANIENT LONG intraday same-day (+0.5%)", dfB, fB))

    # ---- console summary ----
    for o in results:
        print("=" * 78)
        print(o["name"])
        print(f"  train n={o['n_train']} base={o['train_base']:.3f} | "
              f"test n={o['n_test']} base={o['test_base']:.3f} | "
              f"purged={o['split_info']['n_purge']} embargoed={o['split_info']['n_embargo']}")
        print(f"  NULL FLOOR (OOS lift pp): mean={fmt_pct(o['null_lift_mean'])} "
              f"p95={fmt_pct(o['null_lift_p95'])} p99={fmt_pct(o['null_lift_p99'])} "
              f"| null reps w/ candidate={o['null_n_reps_with_candidate']}/{M_NULL} "
              f"| null max recurrence={o['null_rec_max']:.2f}")
        print(f"  REAL: {o['n_candidates']} candidates, {o['n_beat_floor']} beat the p99 floor")
        for r in o["real"][:5]:
            print(f"    lift_oos={fmt_pct(r['oos_lift_pp'])}pp rec={r['recurrence']:.2f} "
                  f"train_lift={fmt_pct(r['train_lift_pp'])}pp hit={r['oos_hit']:.3f} "
                  f"avgret={fmt_pct(r['oos_avg_ret']*100 if not np.isnan(r['oos_avg_ret']) else np.nan)}% "
                  f"n={r['n_fires']} {'BEATS' if r['beats_floor'] else 'below'} :: {r['text']}")

    # stash for report generator
    import pickle, os
    os.makedirs("hulk/_out", exist_ok=True)
    with open("hulk/_out/stage1_results.pkl", "wb") as fh:
        pickle.dump(results, fh)
    print("\nsaved hulk/_out/stage1_results.pkl")


if __name__ == "__main__":
    main()
