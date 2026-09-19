"""
NET-EXPECTANCY WALK-FORWARD — the honest "sellable" test, per the Constitutional principle:
judge every worker by NET EXPECTANCY = (Pwin*AvgWin) - (Ploss*AvgLoss), after costs, sustained across
multiple point-in-time years. NOT precision, NOT win-rate.

Two changes from the old pipeline:
  1) The miner PROMOTES on expectancy (mean net ROC of the actual trade outcome), not target-touch precision.
     The trade outcome captures partial wins (predict +1%, get +0.7% -> booked +0.7%) and the true MAGNITUDE
     of being wrong (-3% hurts 30x more than -0.1%).
  2) Evaluation is a clean 3-year walk-forward: for eval year Y, mine on <=Y-2, promote on Y-1, test on Y.

Per worker, per eval year: Pwin, AvgWin, Ploss, AvgLoss, NetExpectancy, occurrences. "Sellable" = expectancy
positive in ALL evaluated years (sustained), with enough occurrences (confidence shown, not hard-gated).
Run: PYTHONIOENCODING=utf-8 python arena/expectancy_wf.py SYM1 SYM2 ...   (no args -> full universe)
"""
import os, sys, json, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
from sklearn.ensemble import RandomForestClassifier
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "kanida_engine"))
from mine_phase1 import leaf_rules, apply_rule
import features as FE

KDB = str(ROOT / "db" / "kanida.db"); SNR = str(ROOT / "db" / "KANIDA_SNR.db"); REP = ROOT / "reports"
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
LEV = {"MIS": 5.0, "CNC": 1.0, "NRML": 5.0}; COST = {"MIS": 0.08, "CNC": 0.15, "NRML": 0.05}
FOLDS = [2024, 2025, 2026]                       # eval years; each: train<=Y-2, promote Y-1, test Y
MIN_OCC = 10                                     # min occurrences for a fold's expectancy to count (confidence)
EXP_BAR = 0.0                                    # promote a rule only if its promote-year net expectancy > this


def route(d, w, is_fno):
    if w == 1: return "MIS", ("LONG" if d == "up" else "SHORT")
    if d == "up": return "CNC", "LONG"           # canonical: long multiday = CNC 1x (blanket 5x-long hurt)
    return "NRML", "SHORT"


def trade_net_roc(frame, d, pct, w, order):
    """Per-day NET ROC (%) of taking this trade: enter next open, exit on target-touch within w days
    (daily H/L) else time-exit at the w-th close, minus costs, times leverage. This IS the expectancy
    outcome — it books partial wins and the true magnitude of losses."""
    O, H, L, C = frame["_o"].values, frame["_h"].values, frame["_l"].values, frame["_c"].values
    n = len(O); entry = np.roll(O, -1).astype(float); entry[-1] = np.nan
    long = (d == "up")
    tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
    gross = np.full(n, np.nan)
    for i in range(n - 1):
        e = entry[i]
        if not np.isfinite(e) or e <= 0: continue
        hi_lo_end = min(i + w, n - 1); ex = None
        for x in range(i + 1, min(i + 1 + w, n)):
            if long and H[x] >= tgt[i]: ex = tgt[i]; break
            if (not long) and L[x] <= tgt[i]: ex = tgt[i]; break
            hi_lo_end = x
        if ex is None: ex = C[hi_lo_end]
        gross[i] = (ex / e - 1) * 100 if long else (1 - ex / e) * 100
    return (gross - COST[order]) * LEV[order]     # net ROC per day (NaN where no forward window)


def expectancy(net_roc_slice):
    r = net_roc_slice[np.isfinite(net_roc_slice)]
    if len(r) == 0: return dict(n=0, netexp=np.nan, pwin=np.nan, avgwin=np.nan, ploss=np.nan, avgloss=np.nan)
    win = r[r > 0]; loss = r[r <= 0]
    return dict(n=len(r), netexp=float(r.mean()),
                pwin=len(win) / len(r), avgwin=float(win.mean()) if len(win) else 0.0,
                ploss=len(loss) / len(r), avgloss=float(loss.mean()) if len(loss) else 0.0)


def mine_worker(symbol, is_fno):
    frame = FE.build_from_db(symbol, lookback_N=5)
    if frame.empty or len(frame) < 400: return None
    feats = [c for c in frame.columns if c not in ("_o", "_h", "_l", "_c", "year")]
    X = frame[feats].replace([np.inf, -np.inf], np.nan); yr = frame["year"].values
    # precompute per-target trade net ROC once
    net = {}
    for t, (d, pct, w) in TGT.items():
        order, _ = route(d, w, is_fno)
        net[t] = trade_net_roc(frame, d, pct, w, order)
    per_year = {}                                  # eval_year -> worker book expectancy dict
    for Y in FOLDS:
        tr_m = yr <= (Y - 2); va_m = yr == (Y - 1); te_m = yr == Y
        if tr_m.sum() < 200 or va_m.sum() < 20 or te_m.sum() < 20: continue
        promoted = []                              # (target, conds) promoted this fold (val expectancy>bar)
        for t, (d, pct, w) in TGT.items():
            nr = net[t]; lab = (nr > 0).astype(float)
            valid = np.isfinite(nr) & X.notna().all(axis=1).values
            trm = tr_m & valid
            if trm.sum() < 150 or np.unique(lab[trm]).size < 2: continue
            rf = RandomForestClassifier(n_estimators=50, max_depth=3, min_samples_leaf=25,
                                        max_features=0.5, random_state=7, n_jobs=1)
            rf.fit(X[trm].fillna(0).values, lab[trm]); seen = set()
            for conds in leaf_rules(rf, feats):
                key = tuple(sorted(conds))
                if key in seen: continue
                seen.add(key)
                mask = apply_rule(frame, conds).values
                e_tr = expectancy(nr[trm & mask]); e_va = expectancy(nr[va_m & valid & mask])
                if e_tr["n"] < MIN_OCC or e_va["n"] < MIN_OCC: continue
                if e_tr["netexp"] > EXP_BAR and e_va["netexp"] > EXP_BAR:   # positive expectancy train AND promote
                    promoted.append((t, d, pct, w, conds))
        # eval-year book: precompute promoted-rule fire masks once, then walk a non-overlapping book
        prom = [(t, w, apply_rule(frame, conds).values) for (t, d, pct, w, conds) in promoted]
        te_idx = np.where(te_m)[0]; taken = []; day = 0
        while day < len(te_idx):
            gi = te_idx[day]
            cands = [(w, net[t][gi]) for (t, w, mask) in prom if mask[gi] and np.isfinite(net[t][gi])]
            if cands:
                w, nr = cands[0]; taken.append(nr); day += w         # take trade, skip its hold window
            else:
                day += 1
        per_year[Y] = expectancy(np.array(taken)) if taken else dict(n=0, netexp=np.nan, pwin=np.nan,
                                                                      avgwin=np.nan, ploss=np.nan, avgloss=np.nan)
    if not per_year: return None
    row = {"symbol": symbol}
    yrs_pos = 0; exps = []
    for Y in FOLDS:
        e = per_year.get(Y, dict(n=0, netexp=np.nan, pwin=np.nan, avgwin=np.nan, ploss=np.nan, avgloss=np.nan))
        row[f"netexp_{Y}"] = round(e["netexp"], 3) if e["n"] else None
        row[f"n_{Y}"] = e["n"]; row[f"pwin_{Y}"] = round(e["pwin"], 2) if e["n"] else None
        row[f"avgwin_{Y}"] = round(e["avgwin"], 2) if e["n"] else None
        row[f"avgloss_{Y}"] = round(e["avgloss"], 2) if e["n"] else None
        if e["n"] >= MIN_OCC and np.isfinite(e["netexp"]):
            exps.append(e["netexp"]); yrs_pos += (e["netexp"] > 0)
    row["years_evaluated"] = len(exps); row["years_positive"] = yrs_pos
    row["min_netexp"] = round(min(exps), 3) if exps else None
    row["mean_netexp"] = round(float(np.mean(exps)), 3) if exps else None
    row["sustained"] = bool(exps) and yrs_pos == len(exps) and len(exps) == len(FOLDS)
    return row


def main():
    syms = sys.argv[1:]
    con = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in con.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}
    if not syms:
        syms = sorted(set(r[0] for r in con.execute(
            "SELECT symbol FROM instrument_labels WHERE in_nifty500=1 OR is_fno=1").fetchall()))
    con.close()
    rows = []
    for n, s in enumerate(syms, 1):
        try:
            r = mine_worker(s, int(fno.get(s, 0)))
            if r: rows.append(r)
        except Exception as e:
            print(f"  {s}: ERR {str(e)[:80]}")
        if len(syms) > 10 and n % 25 == 0: print(f"  ...{n}/{len(syms)}", flush=True)
    df = pd.DataFrame(rows)
    if df.empty: print("no results"); return
    df = df.sort_values(["sustained", "min_netexp"], ascending=[False, False])
    REP.mkdir(exist_ok=True); df.to_csv(REP / "expectancy_scorecard.csv", index=False)
    sus = df[df.sustained]
    print(f"\n================ NET-EXPECTANCY SCORECARD (walk-forward {FOLDS}) ================")
    print(f"  workers evaluated: {len(df)} | SUSTAINED (net expectancy > 0 every year): {len(sus)}")
    cols = ["symbol", "netexp_2024", "netexp_2025", "netexp_2026", "n_2024", "n_2025", "n_2026",
            "years_positive", "min_netexp"]
    show = [c for c in cols if c in df.columns]
    print("\n  top sustained workers (by worst-year expectancy):")
    print(sus.head(15)[show].to_string(index=False) if len(sus) else "  (none sustained)")
    print("\nwritten: reports/expectancy_scorecard.csv")


if __name__ == "__main__":
    main()
