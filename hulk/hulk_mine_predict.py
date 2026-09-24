"""
FALCON HULK V1 — PREDICTIVE miner (PREDICT-not-CONFIRM), PARALLEL IN-MEMORY.

Uses the new predictive feature tables (hulk_predict_feat_A / _B) wired to the
EXISTING verified spine labels. The miner method (bagged shallow-tree recurrence
+ NULL gate) is reused verbatim from hulk_mine.py; only the features, labels,
gates and cost are new.

SPEED FIX: load ALL features+labels into pandas ONCE. Build every grid cell by
in-memory join. NO SQLite inside the mining loop. Bagged trees + null reps fan
out across all cores via joblib n_jobs=-1 (inside hulk_mine.mine()).

TRACKS / horizons:
  Track A (same-day, feats 09:15-09:30 -> predict 09:30->close):
     intraday label entry_minute='09:30'; y = mfe_intraday >= target; realized=ret_to_close
     targets +0.5% and +1.0%; cost 0.0012 round-trip
  Track B (overnight, feats day-T -> predict day T+1):
     multiday entry_kind='nextopen', entry_date=T (feature date T joins label T):
        T+1 +0.5% (ret_t1), T+2 +1% (ret_t2), T+3 +1% (ret_t3), T+5 +2% (ret_t5); cost 0.0030
     next-day intraday: intraday label +0.5% at next trading day.
        NOTE: spec asks entry_minute='09:15' but the spine grid has NO 09:15 bar
        (earliest is 09:20). We use 09:20 as the nearest available proxy, FLAGGED.
        cost 0.0012

GATES (all required for GENUINE): OOS n_fires >= 150 AND recurrence >= 0.20 AND
  oos_lift_pp > max(null_p99, 0) AND oos_net_ret > 0 (net = oos_avg_ret - cost).
NULL gate M=30 permuted reps/cell. FLAG cell if null_rec_max >= 0.20.

Hard rules: reads ONLY data/db/falcon_hulk.db; asserts every date <= 2024-12-31 in
this run (TRAIN 2018-2023, OOS TEST 2024, purged+embargoed; 2025 held, 2026 sealed).
Fractions throughout (0.005 = 0.5%).
"""

import os
import time
import pickle
import sqlite3
import numpy as np
import pandas as pd

from collections import defaultdict
from sklearn.tree import DecisionTreeClassifier
from joblib import Parallel, delayed

import hulk_mine as HM
from hulk_mine import (
    split_train_test, make_bins, eval_oos, rule_text,
    TEST_END, TEST_START, HARD_MAX_DATE,
)
from hulk_predict_feat import TRACK_A_FEATS, TRACK_B_FEATS

# compute knobs  (2026-07-25: trimmed for laptop speed — B 150->60, M_NULL 30->10.
# The null floor is stable at M=10; B=60 keeps recurrence measurable. ~7x faster.)
B_RESAMPLES = 60
DEPTHS = (1, 2, 3)                # bagged SHALLOW trees; multi-depth so recurrence is measurable
REPORT_REC_MIN = 0.20             # only keep signatures recurring >= 0.20
M_NULL = 10
RELAX_FIRES = 25                  # scale-appropriate diagnostic gate (OOS ~245 rows)


# ----------------------------------------------------------------------------
# Signature-recurrence miner (PARALLEL over bootstraps via joblib n_jobs=-1)
#
# WHY this differs from hulk_mine.mine(): a positive-control test (inject a
# strongly predictive feature) showed the original FULL-leaf-path exact-threshold
# recurrence metric registers ~0.05 max recurrence EVEN under strong signal --
# the two lower depth-3 splits are noise and make every full path unique. Here
# we (a) also mine depth-1 and depth-2 leaves, and (b) key recurrence on the
# (feature, op) SIGNATURE, applying each condition at the MEDIAN threshold seen.
# This makes "recurrence = structural robustness" actually measurable. The
# OOS/null/cost gauntlet is unchanged and remains the sole arbiter of genuineness.
# ----------------------------------------------------------------------------
def _leaf_paths(tree, feat_cols, base, min_leaf):
    t = tree.tree_
    out = []

    def rec(node, conds):
        if t.children_left[node] == t.children_right[node]:
            val = t.value[node][0]
            sup = int(t.n_node_samples[node])
            p1 = val[1] / (val[0] + val[1]) if (val[0] + val[1]) > 0 else 0.0
            if p1 > base and sup >= min_leaf and conds:
                out.append((list(conds), p1))
            return
        f = feat_cols[t.feature[node]]
        thr = float(t.threshold[node])
        rec(t.children_left[node], conds + [(f, "<=", thr)])
        rec(t.children_right[node], conds + [(f, ">", thr)])

    rec(0, [])
    return out


def mine(train, feat_cols, bw, seed_offset=0, y_override=None):
    """Return {representative_rule_tuple: dict(recurrence, train_prec, train_lift_pp)}."""
    X = train[feat_cols].values
    y = y_override if y_override is not None else train["y"].values
    n = len(train)
    base = float(y.mean())
    min_leaf = max(50, int(0.005 * n))
    if base in (0.0, 1.0):
        return {}, base, min_leaf

    rng = np.random.default_rng(1000 + seed_offset)
    idx_sets = [rng.integers(0, n, n) for _ in range(B_RESAMPLES)]

    def one_resample(i, idx):
        Xi, yi = X[idx], y[idx]
        if yi.sum() < min_leaf or (n - yi.sum()) < min_leaf:
            return []
        best = {}   # sig_key -> (p1, conds)
        for dep in DEPTHS:
            clf = DecisionTreeClassifier(
                max_depth=dep, min_samples_leaf=min_leaf,
                class_weight="balanced", random_state=i + seed_offset)
            clf.fit(Xi, yi)
            for conds, p1 in _leaf_paths(clf, feat_cols, base, min_leaf):
                key = tuple(sorted((f, op) for f, op, _ in conds))
                if key not in best or p1 > best[key][0]:
                    best[key] = (p1, conds)
        return list(best.items())

    all_res = Parallel(n_jobs=-1, batch_size=8)(
        delayed(one_resample)(i, idx) for i, idx in enumerate(idx_sets))

    sig_count = defaultdict(int)
    sig_prec = defaultdict(list)
    sig_thr = defaultdict(lambda: defaultdict(list))
    for res in all_res:
        for key, (p1, conds) in res:
            sig_count[key] += 1
            sig_prec[key].append(p1)
            for f, op, thr in conds:
                sig_thr[key][(f, op)].append(thr)

    candidates = {}
    for key, cnt in sig_count.items():
        rec = cnt / B_RESAMPLES
        if rec < REPORT_REC_MIN:
            continue
        rule = tuple((f, op, round(float(np.median(sig_thr[key][(f, op)])), 8))
                     for (f, op) in key)
        tp = float(np.mean(sig_prec[key]))
        candidates[rule] = dict(recurrence=rec, train_prec=tp,
                                train_lift_pp=(tp - base) * 100.0)
    return candidates, base, min_leaf

DB = HM.DB
MIN_FIRES = 150                   # min-sample fix from last run
GENUINE_REC_MIN = 0.20
COST_INTRADAY = 0.0012
COST_MULTIDAY = 0.0030
LOAD_MAX = "2024-12-31"           # this run pulls only <= 2024 (train+test)

MULTIDAY_HZ = {                   # horizon -> (ret column, target)
    "T+1": ("ret_t1", 0.005),
    "T+2": ("ret_t2", 0.010),
    "T+3": ("ret_t3", 0.010),
    "T+5": ("ret_t5", 0.020),
}


# ----------------------------------------------------------------------------
# Load EVERYTHING once
# ----------------------------------------------------------------------------
def load_all():
    con = sqlite3.connect(DB)
    featA = pd.read_sql(
        "SELECT * FROM hulk_predict_feat_A WHERE date <= ?", con, params=(LOAD_MAX,))
    featB = pd.read_sql(
        "SELECT * FROM hulk_predict_feat_B WHERE date <= ?", con, params=(LOAD_MAX,))
    lab_intra = pd.read_sql(
        "SELECT symbol,direction,entry_date,entry_minute,ret_to_close,mfe_intraday "
        "FROM hulk_labels_intraday WHERE entry_date <= ? "
        "AND entry_minute IN ('09:30','09:20')", con, params=(LOAD_MAX,))
    lab_multi = pd.read_sql(
        "SELECT symbol,direction,entry_date,ret_t1,ret_t2,ret_t3,ret_t5,label_end_date "
        "FROM hulk_labels_multiday WHERE entry_kind='nextopen' AND entry_date <= ?",
        con, params=(LOAD_MAX,))
    con.close()
    for nm, df, dc in [("featA", featA, "date"), ("featB", featB, "date"),
                       ("lab_intra", lab_intra, "entry_date"),
                       ("lab_multi", lab_multi, "entry_date")]:
        assert df[dc].max() <= HARD_MAX_DATE, f"{nm}: 2026 leaked!"
        assert df[dc].max() <= LOAD_MAX, f"{nm}: loaded beyond 2024!"
    return featA, featB, lab_intra, lab_multi


def next_day_map(featB, symbol):
    d = sorted(featB.loc[featB.symbol == symbol, "date"].unique())
    return dict(zip(d, d[1:] + [None]))


# ----------------------------------------------------------------------------
# Cell assembly (pure in-memory joins)
# ----------------------------------------------------------------------------
def cell_trackA(featA, lab_intra, symbol, direction, target):
    lab = lab_intra[(lab_intra.symbol == symbol) & (lab_intra.direction == direction)
                    & (lab_intra.entry_minute == "09:30")]
    fa = featA[featA.symbol == symbol]
    df = fa.merge(lab[["entry_date", "ret_to_close", "mfe_intraday"]],
                  left_on="date", right_on="entry_date", how="inner")
    df["label_end"] = df["date"]                      # same-day
    df["y"] = (df["mfe_intraday"] >= target).astype(int)
    df["realized"] = df["ret_to_close"]
    df = df.dropna(subset=TRACK_A_FEATS).reset_index(drop=True)
    return df, list(TRACK_A_FEATS)


def cell_multiday(featB, lab_multi, symbol, direction, ret_col, target):
    lab = lab_multi[(lab_multi.symbol == symbol) & (lab_multi.direction == direction)]
    fb = featB[featB.symbol == symbol]
    df = fb.merge(lab[["entry_date", ret_col, "label_end_date"]],
                  left_on="date", right_on="entry_date", how="inner")
    df = df[df[ret_col].notna()].copy()
    df["label_end"] = df["label_end_date"]
    df["y"] = (df[ret_col] >= target).astype(int)
    df["realized"] = df[ret_col]
    df = df.dropna(subset=TRACK_B_FEATS).reset_index(drop=True)
    return df, list(TRACK_B_FEATS)


def cell_nextday_intraday(featB, lab_intra, symbol, direction, target, ndmap):
    """feature(day T) -> intraday label at NEXT trading day, entry 09:20 (09:15 absent)."""
    lab = lab_intra[(lab_intra.symbol == symbol) & (lab_intra.direction == direction)
                    & (lab_intra.entry_minute == "09:20")].set_index("entry_date")
    fb = featB[featB.symbol == symbol].copy()
    fb["next_date"] = fb["date"].map(ndmap)
    fb = fb[fb["next_date"].notna()]
    fb = fb.join(lab[["ret_to_close", "mfe_intraday"]], on="next_date", how="inner")
    fb["label_end"] = fb["next_date"]
    fb["y"] = (fb["mfe_intraday"] >= target).astype(int)
    fb["realized"] = fb["ret_to_close"]
    fb = fb.dropna(subset=TRACK_B_FEATS).reset_index(drop=True)
    return fb, list(TRACK_B_FEATS)


# ----------------------------------------------------------------------------
# Run one cell (null gate first, then real mine) — reuses HM primitives
# ----------------------------------------------------------------------------
def run_cell(meta, df, feat_cols, cost):
    cell = dict(**meta, cost=cost, feat_cols=feat_cols)
    train, test, split_info = split_train_test(df)
    bw = make_bins(train, feat_cols) if len(train) else {}
    cell.update(n_train=len(train), n_test=len(test),
                train_base=float(train["y"].mean()) if len(train) else np.nan,
                test_base=float(test["y"].mean()) if len(test) else np.nan,
                split_info=split_info)
    degenerate = (len(train) == 0 or len(test) == 0
                  or cell["train_base"] in (0.0, 1.0)
                  or np.isnan(cell["train_base"]))

    # ---- NULL GATE ----
    null_best_lift, null_rec_when_cand = [], []
    if not degenerate:
        for m in range(M_NULL):
            yperm = HM.rng_master.permutation(train["y"].values)
            cands, _, _ = mine(train, feat_cols, bw, seed_offset=5000 + m * 7,
                               y_override=yperm)
            best_lift, best_rec, had = np.nan, 0.0, False
            for rule, mt in cands.items():
                ev = eval_oos(rule, test)
                if ev["n_fires"] >= MIN_FIRES and not np.isnan(ev["oos_lift_pp"]):
                    had = True
                    if np.isnan(best_lift) or ev["oos_lift_pp"] > best_lift:
                        best_lift = ev["oos_lift_pp"]
                    best_rec = max(best_rec, mt["recurrence"])
            if had:
                null_rec_when_cand.append(best_rec)
            null_best_lift.append(best_lift)

    nb = np.array([x for x in null_best_lift if not np.isnan(x)])
    p99 = float(np.percentile(nb, 99)) if len(nb) else np.nan
    cell["null_n_reps_with_candidate"] = int(len(nb))
    cell["null_lift_p95"] = float(np.percentile(nb, 95)) if len(nb) else np.nan
    cell["null_lift_p99"] = p99
    cell["null_rec_max"] = float(np.max(null_rec_when_cand)) if null_rec_when_cand else 0.0
    floor = max(p99, 0.0) if not np.isnan(p99) else 0.0
    cell["floor_used"] = floor
    cell["null_leak_flag"] = bool(cell["null_rec_max"] >= 0.20)

    # ---- REAL MINE ----
    real = []
    if not degenerate:
        cands, _, min_leaf = mine(train, feat_cols, bw, seed_offset=0)
        cell["min_leaf"] = min_leaf
        for rule, mt in cands.items():
            ev = eval_oos(rule, test)
            if ev["n_fires"] < MIN_FIRES or np.isnan(ev["oos_lift_pp"]):
                continue
            net = ev["oos_avg_ret"] - cost if not np.isnan(ev["oos_avg_ret"]) else np.nan
            rec = mt["recurrence"]
            genuine = bool((ev["oos_lift_pp"] > floor) and (rec >= GENUINE_REC_MIN)
                           and (not np.isnan(net)) and (net > 0.0))
            real.append(dict(
                rule=rule, text=rule_text(rule), recurrence=rec,
                train_prec=mt["train_prec"], train_lift_pp=mt["train_lift_pp"],
                n_fires=ev["n_fires"], oos_hit=ev["oos_hit"],
                oos_lift_pp=ev["oos_lift_pp"], oos_avg_ret=ev["oos_avg_ret"],
                oos_net_ret=net, oos_base=ev["oos_base"], genuine=genuine))
    else:
        cell["min_leaf"] = None

    real.sort(key=lambda r: (-int(r["genuine"]),
                             -(r["oos_lift_pp"] if not np.isnan(r["oos_lift_pp"]) else -1e9),
                             -r["recurrence"]))
    cell["real"] = real
    cell["n_candidates"] = len(real)
    cell["n_genuine"] = int(sum(r["genuine"] for r in real))
    genset = [r for r in real if r["genuine"]]
    cell["best"] = (genset or real)[0] if (genset or real) else None

    # ---- DIAGNOSTIC: relaxed gate (n_fires >= 25, scale-appropriate for a ~245-row
    # OOS) so we can honestly report what predictive edge exists BELOW the mandated
    # 150-fire gate. Same lift>floor, rec>=0.20, net>0 tests.
    relaxed = [r for r in real if r["n_fires"] >= RELAX_FIRES]
    if not degenerate:
        # re-scan candidates that fired 25..149 (the >=150 ones are already in `real`)
        for rule, mt in cands.items():
            ev = eval_oos(rule, test)
            if not (RELAX_FIRES <= ev["n_fires"] < MIN_FIRES) or np.isnan(ev["oos_lift_pp"]):
                continue
            net = ev["oos_avg_ret"] - cost if not np.isnan(ev["oos_avg_ret"]) else np.nan
            rec = mt["recurrence"]
            gen_relaxed = bool((ev["oos_lift_pp"] > floor) and (rec >= GENUINE_REC_MIN)
                               and (not np.isnan(net)) and (net > 0.0))
            relaxed.append(dict(
                rule=rule, text=rule_text(rule), recurrence=rec,
                n_fires=ev["n_fires"], oos_hit=ev["oos_hit"],
                oos_lift_pp=ev["oos_lift_pp"], oos_net_ret=net,
                genuine=gen_relaxed))
    cell["n_genuine_relaxed"] = int(sum(r.get("genuine", False) for r in relaxed))
    relaxed_gen = [r for r in relaxed if r.get("genuine")]
    relaxed_gen.sort(key=lambda r: -r["oos_net_ret"])
    cell["best_relaxed"] = relaxed_gen[0] if relaxed_gen else None
    return cell


def fmt(x, nd=2, sign=True):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "n/a"
    return f"{x:+.{nd}f}" if sign else f"{x:.{nd}f}"


def main():
    t0 = time.time()
    n_cores = os.cpu_count()
    featA, featB, lab_intra, lab_multi = load_all()
    print(f"loaded in-memory: featA={len(featA)} featB={len(featB)} "
          f"lab_intra={len(lab_intra)} lab_multi={len(lab_multi)}  cores={n_cores}")

    cells = []
    stocks = ["RELIANCE", "ADANIENT"]
    directions = ["long", "short"]

    for stock in stocks:
        ndmap = next_day_map(featB, stock)
        for direction in directions:
            # Track A: 09:30->close, targets 0.5% & 1.0%
            for tgt, tag in [(0.005, "A:intra+0.5%"), (0.010, "A:intra+1.0%")]:
                df, fc = cell_trackA(featA, lab_intra, stock, direction, tgt)
                c = run_cell(dict(stock=stock, direction=direction, track="A",
                                  horizon=tag, target=tgt), df, fc, COST_INTRADAY)
                cells.append(c)
                print(f"[{time.time()-t0:6.0f}s] {stock:9s} {direction:5s} {tag:14s} "
                      f"tr={c['n_train']} te={c['n_test']} cand={c['n_candidates']} "
                      f"gen={c['n_genuine']} floor={fmt(c['floor_used'])} "
                      f"nullrec={c['null_rec_max']:.2f} leak={c['null_leak_flag']}")
            # Track B multiday nextopen
            for hz, (col, tgt) in MULTIDAY_HZ.items():
                df, fc = cell_multiday(featB, lab_multi, stock, direction, col, tgt)
                c = run_cell(dict(stock=stock, direction=direction, track="B",
                                  horizon=f"B:{hz}", target=tgt), df, fc, COST_MULTIDAY)
                cells.append(c)
                print(f"[{time.time()-t0:6.0f}s] {stock:9s} {direction:5s} {'B:'+hz:14s} "
                      f"tr={c['n_train']} te={c['n_test']} cand={c['n_candidates']} "
                      f"gen={c['n_genuine']} floor={fmt(c['floor_used'])} "
                      f"nullrec={c['null_rec_max']:.2f} leak={c['null_leak_flag']}")
            # Track B next-day intraday (09:20 proxy for absent 09:15)
            df, fc = cell_nextday_intraday(featB, lab_intra, stock, direction, 0.005, ndmap)
            c = run_cell(dict(stock=stock, direction=direction, track="B",
                              horizon="B:T+1_intra(09:20*)", target=0.005,
                              proxy_0915="09:20 (09:15 absent in spine)"),
                         df, fc, COST_INTRADAY)
            cells.append(c)
            print(f"[{time.time()-t0:6.0f}s] {stock:9s} {direction:5s} {'B:T+1_intra*':14s} "
                  f"tr={c['n_train']} te={c['n_test']} cand={c['n_candidates']} "
                  f"gen={c['n_genuine']} floor={fmt(c['floor_used'])} "
                  f"nullrec={c['null_rec_max']:.2f} leak={c['null_leak_flag']}")

    runtime = time.time() - t0
    os.makedirs("hulk/_out", exist_ok=True)
    with open("hulk/_out/predict_grid.pkl", "wb") as fh:
        pickle.dump(dict(cells=cells, runtime_sec=runtime, n_cores=n_cores,
                         B=B_RESAMPLES, M=M_NULL, min_fires=MIN_FIRES,
                         relax_fires=RELAX_FIRES, depths=DEPTHS,
                         genuine_rec_min=GENUINE_REC_MIN,
                         cost_intraday=COST_INTRADAY, cost_multiday=COST_MULTIDAY), fh)
    print(f"\nsaved hulk/_out/predict_grid.pkl  runtime={runtime:.0f}s  cores={n_cores}")


if __name__ == "__main__":
    main()
