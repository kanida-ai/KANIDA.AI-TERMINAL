"""
FALCON HULK V1 — STATE ENGINE miner (Phase 1), PREDICT-not-CONFIRM.

Mines the OPEN-ENDED generated STATE library (hulk_state_feat_A / _B, built by
hulk_state_gen.py) against the EXISTING verified spine labels. The miner method
(bagged shallow-tree signature-recurrence + NULL surrogate gate) is reused
VERBATIM from hulk_mine_predict.mine(); only the feature library (hundreds of
generated states, not the ~22 hand features), the CALIBRATION-FIXED gate, and
the output are new.

CALIBRATION FIX (this was mis-set last run at a flat n_fires>=150, impossible for
~245-row daily OOS cells): the min OOS fire floor is now SCALED per cell:
    floor = max(30, ceil(0.01 * n_test))
So a ~15k-entry intraday cell keeps a real ~150 floor; a ~245-entry daily cell
gets ~30. Everything else in the gauntlet is unchanged.

GENUINE (all required):
    n_fires   >= floor(cell)
    recurrence>= 0.20                     (signature recurrence at median threshold)
    oos_lift  >  null_p99                 (M=30 permuted surrogate over SAME library)
    oos_net   >  0   (net = oos_avg_ret - cost; intraday 0.0012 / multiday 0.0030)
FLAG null_leak when null_rec_max >= 0.20 (search out-running the gate).

TRACKS / horizons (identical framing to the hand-feature predict run):
  Track A (feats 09:15-09:30 -> predict 09:30->close): intraday label 09:30,
     y=mfe>=target, realized=ret_to_close; targets +0.5% & +1.0%; cost 0.0012.
  Track B (feats day-T -> predict day T+1):
     multiday nextopen: T+1 +0.5%(ret_t1), T+2 +1%(ret_t2), T+3 +1%(ret_t3),
       T+5 +2%(ret_t5); cost 0.0030.
     next-day intraday: 09:20 proxy for absent 09:15; +0.5%; cost 0.0012 (FLAGGED).

Hard rules: reads ONLY data/db/falcon_hulk.db; asserts every date <= 2024-12-31
(TRAIN 2018-2023, OOS TEST 2024 purged+embargoed; 2025 held, 2026 SEALED).
RUN SYNCHRONOUSLY. Fractions throughout.
"""

import os
import math
import time
import pickle
import sqlite3
import numpy as np
import pandas as pd

import hulk_mine as HM
from hulk_mine import split_train_test, make_bins, eval_oos, rule_text, HARD_MAX_DATE
# reuse the EXACT signature-recurrence miner (bagged depth-1/2/3, B=150, joblib -1)
from hulk_mine_predict import mine, B_RESAMPLES, DEPTHS, M_NULL, GENUINE_REC_MIN

DB = HM.DB
LOAD_MAX = "2024-12-31"
COST_INTRADAY = 0.0012
COST_MULTIDAY = 0.0030
NULL_LEAK_REC = 0.20

MULTIDAY_HZ = {
    "T+1": ("ret_t1", 0.005),
    "T+2": ("ret_t2", 0.010),
    "T+3": ("ret_t3", 0.010),
    "T+5": ("ret_t5", 0.020),
}


def scaled_floor(n_test):
    return max(30, int(math.ceil(0.01 * n_test)))


# ---------------------------------------------------------------------------
# Load EVERYTHING once (2026 sealed by assert; this run pulls only <= 2024)
# ---------------------------------------------------------------------------
def load_all():
    con = sqlite3.connect(DB)
    featA = pd.read_sql("SELECT * FROM hulk_state_feat_A WHERE date <= ?", con, params=(LOAD_MAX,))
    featB = pd.read_sql("SELECT * FROM hulk_state_feat_B WHERE date <= ?", con, params=(LOAD_MAX,))
    lab_intra = pd.read_sql(
        "SELECT symbol,direction,entry_date,entry_minute,ret_to_close,mfe_intraday "
        "FROM hulk_labels_intraday WHERE entry_date <= ? AND entry_minute IN ('09:30','09:20')",
        con, params=(LOAD_MAX,))
    lab_multi = pd.read_sql(
        "SELECT symbol,direction,entry_date,ret_t1,ret_t2,ret_t3,ret_t5,label_end_date "
        "FROM hulk_labels_multiday WHERE entry_kind='nextopen' AND entry_date <= ?",
        con, params=(LOAD_MAX,))
    con.close()
    for nm, df, dc in [("featA", featA, "date"), ("featB", featB, "date"),
                       ("lab_intra", lab_intra, "entry_date"), ("lab_multi", lab_multi, "entry_date")]:
        assert df[dc].max() <= HARD_MAX_DATE, f"{nm}: 2026 leaked!"
        assert df[dc].max() <= LOAD_MAX, f"{nm}: loaded beyond 2024!"
    fcA = [c for c in featA.columns if c not in ("symbol", "date")]
    fcB = [c for c in featB.columns if c not in ("symbol", "date")]
    return featA, featB, lab_intra, lab_multi, fcA, fcB


def next_day_map(featB, symbol):
    d = sorted(featB.loc[featB.symbol == symbol, "date"].unique())
    return dict(zip(d, d[1:] + [None]))


# ---------------------------------------------------------------------------
# Cell assembly (pure in-memory joins)
# ---------------------------------------------------------------------------
def cell_trackA(featA, lab_intra, symbol, direction, target, fcA):
    lab = lab_intra[(lab_intra.symbol == symbol) & (lab_intra.direction == direction)
                    & (lab_intra.entry_minute == "09:30")]
    fa = featA[featA.symbol == symbol]
    df = fa.merge(lab[["entry_date", "ret_to_close", "mfe_intraday"]],
                  left_on="date", right_on="entry_date", how="inner")
    df = df.dropna(subset=["mfe_intraday", "ret_to_close"]).copy()
    df["label_end"] = df["date"]
    df["y"] = (df["mfe_intraday"] >= target).astype(int)
    df["realized"] = df["ret_to_close"]
    return df.reset_index(drop=True)


def cell_multiday(featB, lab_multi, symbol, direction, ret_col, target, fcB):
    lab = lab_multi[(lab_multi.symbol == symbol) & (lab_multi.direction == direction)]
    fb = featB[featB.symbol == symbol]
    df = fb.merge(lab[["entry_date", ret_col, "label_end_date"]],
                  left_on="date", right_on="entry_date", how="inner")
    df = df[df[ret_col].notna()].copy()
    df["label_end"] = df["label_end_date"]
    df["y"] = (df[ret_col] >= target).astype(int)
    df["realized"] = df[ret_col]
    return df.reset_index(drop=True)


def cell_nextday_intraday(featB, lab_intra, symbol, direction, target, ndmap, fcB):
    lab = lab_intra[(lab_intra.symbol == symbol) & (lab_intra.direction == direction)
                    & (lab_intra.entry_minute == "09:20")].set_index("entry_date")
    fb = featB[featB.symbol == symbol].copy()
    fb["next_date"] = fb["date"].map(ndmap)
    fb = fb[fb["next_date"].notna()]
    fb = fb.join(lab[["ret_to_close", "mfe_intraday"]], on="next_date", how="inner")
    fb = fb.dropna(subset=["mfe_intraday", "ret_to_close"]).copy()
    fb["label_end"] = fb["next_date"]
    fb["y"] = (fb["mfe_intraday"] >= target).astype(int)
    fb["realized"] = fb["ret_to_close"]
    return fb.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Run one cell: NULL surrogate gate (M=30) over the SAME library, then real mine
# ---------------------------------------------------------------------------
def run_cell(meta, df, feat_cols, cost):
    cell = dict(**meta, cost=cost, n_states_lib=len(feat_cols))
    train, test, split_info = split_train_test(df)

    # ---- leakage-safe handling of the large generated library ----
    # A per-row "all-582-non-null" filter is impossible (0.987^582 ~= 0); instead
    # keep states covered in >=90% of TRAIN, drop TRAIN warm-up rows still sparse
    # in those states, then impute residual NaNs with the TRAIN median (test uses
    # the SAME train medians -> no test leakage).
    if len(train) and len(test):
        cov = train[feat_cols].notna().mean()
        usable = [c for c in feat_cols if cov[c] >= 0.90]
        med = train[usable].median()
        usable = [c for c in usable if pd.notna(med[c])]
        keep = train[usable].notna().mean(axis=1) >= 0.90
        train = train[keep].copy()
        train[usable] = train[usable].fillna(med)
        test = test.copy()
        test[usable] = test[usable].fillna(med)
        feat_cols = usable
    cell["n_states"] = len(feat_cols)
    bw = make_bins(train, feat_cols) if len(train) else {}
    floor = scaled_floor(len(test))
    cell.update(n_train=len(train), n_test=len(test), floor_fires=floor,
                train_base=float(train["y"].mean()) if len(train) else np.nan,
                test_base=float(test["y"].mean()) if len(test) else np.nan,
                split_info=split_info)
    degenerate = (len(train) == 0 or len(test) == 0
                  or cell["train_base"] in (0.0, 1.0) or np.isnan(cell["train_base"]))

    # ---- NULL SURROGATE (M=30 permuted reps over the SAME generated library) ----
    null_best_lift, null_rec_when_cand = [], []
    if not degenerate:
        for m in range(M_NULL):
            yperm = HM.rng_master.permutation(train["y"].values)
            cands, _, _ = mine(train, feat_cols, bw, seed_offset=5000 + m * 7, y_override=yperm)
            best_lift, best_rec, had = np.nan, 0.0, False
            for rule, mt in cands.items():
                ev = eval_oos(rule, test)
                if ev["n_fires"] >= floor and not np.isnan(ev["oos_lift_pp"]):
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
    lift_floor = max(p99, 0.0) if not np.isnan(p99) else 0.0
    cell["lift_floor_used"] = lift_floor
    cell["null_leak_flag"] = bool(cell["null_rec_max"] >= NULL_LEAK_REC)

    # ---- REAL MINE ----
    real = []
    if not degenerate:
        cands, _, min_leaf = mine(train, feat_cols, bw, seed_offset=0)
        cell["min_leaf"] = min_leaf
        cell["n_raw_candidates"] = len(cands)
        for rule, mt in cands.items():
            ev = eval_oos(rule, test)
            if ev["n_fires"] < floor or np.isnan(ev["oos_lift_pp"]):
                continue
            net = ev["oos_avg_ret"] - cost if not np.isnan(ev["oos_avg_ret"]) else np.nan
            rec = mt["recurrence"]
            genuine = bool((ev["oos_lift_pp"] > lift_floor) and (rec >= GENUINE_REC_MIN)
                           and (not np.isnan(net)) and (net > 0.0))
            real.append(dict(
                rule=rule, text=rule_text(rule), recurrence=rec,
                train_prec=mt["train_prec"], train_lift_pp=mt["train_lift_pp"],
                n_fires=ev["n_fires"], oos_hit=ev["oos_hit"], oos_lift_pp=ev["oos_lift_pp"],
                oos_avg_ret=ev["oos_avg_ret"], oos_net_ret=net, oos_base=ev["oos_base"],
                genuine=genuine))
    else:
        cell["min_leaf"] = None
        cell["n_raw_candidates"] = 0

    real.sort(key=lambda r: (-int(r["genuine"]),
                             -(r["oos_lift_pp"] if not np.isnan(r["oos_lift_pp"]) else -1e9),
                             -r["recurrence"]))
    cell["real"] = real
    cell["n_candidates"] = len(real)
    cell["n_genuine"] = int(sum(r["genuine"] for r in real))
    genset = [r for r in real if r["genuine"]]
    cell["best"] = (genset or real)[0] if (genset or real) else None
    return cell


def fmt(x, nd=2, sign=True):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "n/a"
    return f"{x:+.{nd}f}" if sign else f"{x:.{nd}f}"


def main():
    t0 = time.time()
    n_cores = os.cpu_count()
    featA, featB, lab_intra, lab_multi, fcA, fcB = load_all()
    print(f"loaded in-memory: featA={len(featA)}({len(fcA)} states) "
          f"featB={len(featB)}({len(fcB)} states) lab_intra={len(lab_intra)} "
          f"lab_multi={len(lab_multi)}  cores={n_cores}")
    print(f"miner: B={B_RESAMPLES} depths={DEPTHS} M_null={M_NULL} rec_min={GENUINE_REC_MIN} "
          f"floor=max(30,ceil(1%*n_test))")

    cells = []
    stocks = ["RELIANCE", "ADANIENT"]
    directions = ["long", "short"]

    for stock in stocks:
        ndmap = next_day_map(featB, stock)
        for direction in directions:
            for tgt, tag in [(0.005, "A:intra+0.5%"), (0.010, "A:intra+1.0%")]:
                df = cell_trackA(featA, lab_intra, stock, direction, tgt, fcA)
                c = run_cell(dict(stock=stock, direction=direction, track="A", horizon=tag,
                                  target=tgt), df, fcA, COST_INTRADAY)
                cells.append(c); _log(c, t0)
            for hz, (col, tgt) in MULTIDAY_HZ.items():
                df = cell_multiday(featB, lab_multi, stock, direction, col, tgt, fcB)
                c = run_cell(dict(stock=stock, direction=direction, track="B", horizon=f"B:{hz}",
                                  target=tgt), df, fcB, COST_MULTIDAY)
                cells.append(c); _log(c, t0)
            df = cell_nextday_intraday(featB, lab_intra, stock, direction, 0.005, ndmap, fcB)
            c = run_cell(dict(stock=stock, direction=direction, track="B",
                              horizon="B:T+1_intra(09:20*)", target=0.005,
                              proxy_0915="09:20 (09:15 absent in spine)"), df, fcB, COST_INTRADAY)
            cells.append(c); _log(c, t0)

    runtime = time.time() - t0
    os.makedirs("hulk/_out", exist_ok=True)
    with open("hulk/_out/state_grid.pkl", "wb") as fh:
        pickle.dump(dict(cells=cells, runtime_sec=runtime, n_cores=n_cores,
                         B=B_RESAMPLES, M=M_NULL, depths=DEPTHS,
                         genuine_rec_min=GENUINE_REC_MIN, n_states_A=len(fcA),
                         n_states_B=len(fcB), cost_intraday=COST_INTRADAY,
                         cost_multiday=COST_MULTIDAY,
                         floor_rule="max(30, ceil(0.01*n_test))"), fh)
    print(f"\nsaved hulk/_out/state_grid.pkl  runtime={runtime:.0f}s  cores={n_cores}")


def _log(c, t0):
    print(f"[{time.time()-t0:6.0f}s] {c['stock']:9s} {c['direction']:5s} {c['horizon']:20s} "
          f"tr={c['n_train']} te={c['n_test']} floor={c['floor_fires']} "
          f"states={c['n_states']} rawcand={c.get('n_raw_candidates',0)} "
          f"gen={c['n_genuine']} liftfloor={fmt(c['lift_floor_used'])} "
          f"nullrec={c['null_rec_max']:.2f} leak={c['null_leak_flag']}")


if __name__ == "__main__":
    main()
