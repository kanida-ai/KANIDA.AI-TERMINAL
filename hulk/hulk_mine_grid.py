"""
FALCON HULK V1 — Stage-1 FULL GRID
Reuses the validated miner from hulk_mine.py (bagged shallow-tree recurrence
miner + NULL gate). This script only WIDENS it to the full grid and applies the
one bug fix from the 2-slice run. The method is NOT re-derived.

GRID: 2 stocks (RELIANCE, ADANIENT) x 2 directions (long, short) x 6 horizons = 24 cells
Horizons / targets (label y = did it clear target at that horizon):
  intraday : y = mfe_intraday >= 0.005   (features = hulk_intraday_features @ entry minute)
  T+1      : y = ret_t1 >= 0.005         (features = hulk_daily_features @ T, entry_kind='close')
  T+2      : y = ret_t2 >= 0.010
  T+3      : y = ret_t3 >= 0.010
  T+5      : y = ret_t5 >= 0.020
  T+10     : y = ret_t10 >= 0.030

Confirmed against the spine before running:
  * multiday ret_tN(short) == -ret_tN(long) exactly (corr -1.0, sum==0) -> `>= target`
    test is sign-correct for shorts.
  * intraday mfe_intraday is stored PER-DIRECTION (short mfe = favorable DOWN move),
    so `mfe_intraday >= 0.005` is the correct short test.
  * label_end_date is always T+10 (widest window) -> conservative purge for every horizon.

BUG FIX (from 2-slice run): when the null produces NO candidates, p99 is NaN.
Treat the floor as 0.0, not NaN. A candidate is a GENUINE DISCOVERY iff:
    oos_lift_pp > max(null_p99, 0)  AND  recurrence >= 0.15  AND  oos_net_ret > 0
where net = oos_avg_realized_ret - 0.0030 (round-trip cost).

Compute budget: B=150 bootstraps, M=30 null reps (gate already validated at 50).

Hard rules honored: reads ONLY data/db/falcon_hulk.db; asserts every load <= 2025-12-31
and this run only pulls <= 2024-12-31 (TRAIN 2018-2023, OOS TEST 2024, purged+embargoed).
Fractions throughout (0.005 = 0.5%).
"""

import os
import sys
import time
import pickle
import sqlite3
import numpy as np
import pandas as pd

# Reuse the validated primitives verbatim -----------------------------------
import hulk_mine as HM
from hulk_mine import (
    split_train_test, make_bins, mine, eval_oos, rule_text,
    TEST_END, HARD_MAX_DATE,
)

# Widen compute knobs on the imported module so mine() picks them up ----------
HM.B_RESAMPLES = 150
HM.RECURRENCE_MIN = 0.10   # report candidates from 0.10; genuine gate is 0.15
M_NULL = 30

DB = HM.DB
ROUND_TRIP = 0.0030
GENUINE_REC_MIN = 0.15
MIN_FIRES = 20

DAILY_FEATS = [
    "roc_1", "roc_5", "roc_10", "roc_20", "roc_60",
    "dist_sma_20", "dist_sma_50", "dist_sma_200", "slope_sma_20",
    "rsi_14", "atr_20_pct", "range_pct", "gap_pct", "close_loc",
    "dist_high_20", "dist_high_60", "dist_high_120", "dist_high_252",
    "vol_vs_20d", "n_higher_highs_5", "n_higher_lows_5",
]
INTRADAY_FEATS = [
    "min_since_open", "ret_since_open", "opening_range_pos", "vwap_dev",
    "cum_vol_vs_20d_avg", "rolling_5min_ret", "rolling_15min_ret",
]

# horizon -> (ret column, target fraction)
MULTIDAY_HORIZONS = {
    "T+1": ("ret_t1", 0.005),
    "T+2": ("ret_t2", 0.010),
    "T+3": ("ret_t3", 0.010),
    "T+5": ("ret_t5", 0.020),
    "T+10": ("ret_t10", 0.030),
}


# ----------------------------------------------------------------------------
# Loaders (point-in-time; date-safety asserted)
# ----------------------------------------------------------------------------
def load_multiday(symbol, direction, ret_col, target):
    con = sqlite3.connect(DB)
    q = f"""
        SELECT f.*, l.{ret_col} AS retN, l.label_end_date
        FROM hulk_labels_multiday l
        JOIN hulk_daily_features f
          ON f.symbol = l.symbol AND f.trade_date = l.entry_date
        WHERE l.symbol = ? AND l.direction = ? AND l.entry_kind = 'close'
          AND l.{ret_col} IS NOT NULL
          AND l.entry_date <= ?
    """
    df = pd.read_sql(q, con, params=(symbol, direction, TEST_END))
    con.close()
    assert df["trade_date"].max() <= HARD_MAX_DATE, "date-safety: 2026 leaked!"
    assert df["trade_date"].max() <= TEST_END, "loaded beyond test window!"
    df["date"] = df["trade_date"]
    df["label_end"] = df["label_end_date"]
    df["y"] = (df["retN"] >= target).astype(int)
    df["realized"] = df["retN"]
    feat_cols = list(DAILY_FEATS)
    df = df.dropna(subset=feat_cols).reset_index(drop=True)
    return df, feat_cols


def load_intraday(symbol, direction):
    con = sqlite3.connect(DB)
    lab = pd.read_sql(
        """SELECT symbol, entry_date, entry_minute, mfe_intraday, ret_to_close
           FROM hulk_labels_intraday
           WHERE symbol=? AND direction=? AND entry_date <= ?""",
        con, params=(symbol, direction, TEST_END))
    feat = pd.read_sql(
        "SELECT * FROM hulk_intraday_features WHERE symbol=?",
        con, params=(symbol,))
    con.close()
    lab["bar_time"] = lab["entry_date"] + " " + lab["entry_minute"] + ":00"
    df = feat.merge(lab, on=["symbol", "bar_time"], how="inner")
    assert df["entry_date"].max() <= HARD_MAX_DATE, "date-safety: 2026 leaked!"
    assert df["entry_date"].max() <= TEST_END, "loaded beyond test window!"
    df["date"] = df["entry_date"]
    df["label_end"] = df["entry_date"]           # same-day
    df["y"] = (df["mfe_intraday"] >= 0.005).astype(int)
    df["realized"] = df["ret_to_close"]           # direction-specific already
    feat_cols = list(INTRADAY_FEATS)
    df = df.dropna(subset=feat_cols).reset_index(drop=True)
    return df, feat_cols


# ----------------------------------------------------------------------------
# One grid cell
# ----------------------------------------------------------------------------
def run_cell(stock, direction, horizon, df, feat_cols):
    cell = dict(stock=stock, direction=direction, horizon=horizon,
                feat_cols=feat_cols)
    train, test, split_info = split_train_test(df)
    bw = make_bins(train, feat_cols)
    cell["n_train"] = len(train)
    cell["n_test"] = len(test)
    cell["train_base"] = float(train["y"].mean()) if len(train) else np.nan
    cell["test_base"] = float(test["y"].mean()) if len(test) else np.nan
    cell["split_info"] = split_info

    degenerate = (len(train) == 0 or len(test) == 0
                  or cell["train_base"] in (0.0, 1.0)
                  or np.isnan(cell["train_base"]))

    # ---- NULL GATE FIRST ----
    null_best_lift = []
    null_rec_when_candidate = []
    null_n_candidate_reps = 0
    if not degenerate:
        for m in range(M_NULL):
            yperm = HM.rng_master.permutation(train["y"].values)
            cands, base, _ = mine(train, feat_cols, bw,
                                  seed_offset=5000 + m * 7, y_override=yperm)
            best_lift = np.nan
            best_rec = 0.0
            had = False
            for rule, meta in cands.items():
                ev = eval_oos(rule, test)
                if ev["n_fires"] >= MIN_FIRES and not np.isnan(ev["oos_lift_pp"]):
                    had = True
                    if np.isnan(best_lift) or ev["oos_lift_pp"] > best_lift:
                        best_lift = ev["oos_lift_pp"]
                    best_rec = max(best_rec, meta["recurrence"])
            if had:
                null_n_candidate_reps += 1
                null_rec_when_candidate.append(best_rec)
            null_best_lift.append(best_lift)

    nb = np.array([x for x in null_best_lift if not np.isnan(x)])
    cell["null_n_reps_with_candidate"] = int(len(nb))
    cell["null_lift_mean"] = float(np.mean(nb)) if len(nb) else np.nan
    cell["null_lift_p95"] = float(np.percentile(nb, 95)) if len(nb) else np.nan
    cell["null_lift_p99"] = float(np.percentile(nb, 99)) if len(nb) else np.nan
    cell["null_rec_max"] = (float(np.max(null_rec_when_candidate))
                            if null_rec_when_candidate else 0.0)
    cell["null_best_lift_raw"] = null_best_lift

    # BUG FIX: NaN p99 (null empty) -> floor 0.0
    p99 = cell["null_lift_p99"]
    floor = max(p99, 0.0) if not np.isnan(p99) else 0.0
    cell["floor_used"] = floor

    # Null-leak flag: gate is supposed to stay near-empty on scrambled labels.
    # Flag if the null produced OOS candidates in a meaningful share of reps AND
    # its p99 floor climbed positive (i.e. the gate is leaking for this slice).
    cell["null_leak_flag"] = bool(
        (len(nb) >= max(3, int(0.20 * M_NULL)))
        and (not np.isnan(p99)) and (p99 > 0.0))

    # ---- REAL MINE ----
    real = []
    if not degenerate:
        cands, base, min_leaf = mine(train, feat_cols, bw, seed_offset=0)
        cell["min_leaf"] = min_leaf
        for rule, meta in cands.items():
            ev = eval_oos(rule, test)
            if ev["n_fires"] < MIN_FIRES or np.isnan(ev["oos_lift_pp"]):
                continue
            net = (ev["oos_avg_ret"] - ROUND_TRIP
                   if not np.isnan(ev["oos_avg_ret"]) else np.nan)
            rec = meta["recurrence"]
            genuine = bool(
                (ev["oos_lift_pp"] > floor)
                and (rec >= GENUINE_REC_MIN)
                and (not np.isnan(net)) and (net > 0.0))
            real.append(dict(
                rule=rule, text=rule_text(rule),
                recurrence=rec,
                train_prec=meta["train_prec"],
                train_lift_pp=meta["train_lift_pp"],
                n_fires=ev["n_fires"],
                oos_hit=ev["oos_hit"],
                oos_lift_pp=ev["oos_lift_pp"],
                oos_avg_ret=ev["oos_avg_ret"],
                oos_net_ret=net,
                oos_base=ev["oos_base"],
                genuine=genuine))
    else:
        cell["min_leaf"] = None

    # sort genuine first, then by OOS lift, then recurrence
    real.sort(key=lambda r: (
        -int(r["genuine"]),
        -(r["oos_lift_pp"] if not np.isnan(r["oos_lift_pp"]) else -1e9),
        -r["recurrence"]))
    cell["real"] = real
    cell["n_candidates"] = len(real)
    cell["n_genuine"] = int(sum(r["genuine"] for r in real))

    # best-of-cell (prefer genuine, else best lift) for the map row
    best = None
    genset = [r for r in real if r["genuine"]]
    pool = genset if genset else real
    if pool:
        best = pool[0]
    cell["best"] = best
    return cell


def fmt(x, sign=True, nd=2):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "n/a"
    return (f"{x:+.{nd}f}" if sign else f"{x:.{nd}f}")


def main():
    t0 = time.time()
    cells = []
    stocks = ["RELIANCE", "ADANIENT"]
    directions = ["long", "short"]

    for stock in stocks:
        for direction in directions:
            # intraday
            df, fc = load_intraday(stock, direction)
            c = run_cell(stock, direction, "intraday", df, fc)
            cells.append(c)
            print(f"[{time.time()-t0:6.0f}s] {stock:9s} {direction:5s} intraday "
                  f"tr={c['n_train']} te={c['n_test']} "
                  f"cand={c['n_candidates']} genuine={c['n_genuine']} "
                  f"floor={fmt(c['floor_used'])} leak={c['null_leak_flag']}")
            # multiday horizons
            for hz, (col, tgt) in MULTIDAY_HORIZONS.items():
                df, fc = load_multiday(stock, direction, col, tgt)
                c = run_cell(stock, direction, hz, df, fc)
                cells.append(c)
                print(f"[{time.time()-t0:6.0f}s] {stock:9s} {direction:5s} {hz:5s}    "
                      f"tr={c['n_train']} te={c['n_test']} "
                      f"cand={c['n_candidates']} genuine={c['n_genuine']} "
                      f"floor={fmt(c['floor_used'])} leak={c['null_leak_flag']}")

    runtime = time.time() - t0
    os.makedirs("hulk/_out", exist_ok=True)
    with open("hulk/_out/stage1_grid.pkl", "wb") as fh:
        pickle.dump(dict(cells=cells, runtime_sec=runtime,
                         B=HM.B_RESAMPLES, M=M_NULL), fh)
    print(f"\nsaved hulk/_out/stage1_grid.pkl  runtime={runtime:.0f}s")


if __name__ == "__main__":
    main()
