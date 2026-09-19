"""
NET-EXPECTANCY SCORECARD — reuses the ALREADY-MINED micro+macro patterns (no re-mine). We take each
discovered rule and RE-JUDGE it on NET EXPECTANCY = (Pwin*AvgWin) - (Ploss*AvgLoss) after costs, instead
of precision. Leak-free: rules were discovered on <=2024, so 2025 and 2026 are clean out-of-sample.

Per worker: select rules whose <=2024 net expectancy > 0 (a-priori), then measure the traded book's net
expectancy PER YEAR for 2025 and 2026, decomposed into Pwin/AvgWin/Ploss/AvgLoss and occurrences.
"Sellable" = net expectancy positive in every evaluated year (sustained), with enough occurrences.
Run: PYTHONIOENCODING=utf-8 python arena/expectancy_scorecard.py [SYM ...]   (no args -> full universe)
"""
import sys, json, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "kanida_engine"))
from mine_phase1 import apply_rule
import features as FE

SNR = str(ROOT / "db" / "KANIDA_SNR.db"); KDB = str(ROOT / "db" / "kanida.db"); REP = ROOT / "reports"
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
LEV = {"MIS": 5.0, "CNC": 1.0, "NRML": 5.0}; COST = {"MIS": 0.08, "CNC": 0.15, "NRML": 0.05}
EVAL_YEARS = [2025, 2026]; SELECT_MAX_YEAR = 2024; MIN_OCC_PRE = 15; MIN_OCC_EVAL = 8


import routing

def route(d, w, is_fno):
    return routing.route(d, w, is_fno)[0]        # legal, F&O-aware product (compat shim)


def trade_net_roc(frame, d, pct, w, order):
    """Per-day NET ROC (%) of the trade: enter next open, exit on target touch within w (daily H/L) else
    time-exit at w-th close, net of costs x leverage. Books partial wins and true loss magnitude."""
    O, H, L, C = frame["_o"].values, frame["_h"].values, frame["_l"].values, frame["_c"].values
    n = len(O); gross = np.full(n, np.nan); long = (d == "up")
    for i in range(n - 1):
        e = O[i + 1]
        if not np.isfinite(e) or e <= 0: continue
        tgt = e * (1 + pct / 100) if long else e * (1 - pct / 100); ex = None; end = min(i + w, n - 1)
        for x in range(i + 1, min(i + 1 + w, n)):
            if long and H[x] >= tgt: ex = tgt; break
            if (not long) and L[x] <= tgt: ex = tgt; break
            end = x
        if ex is None: ex = C[end]
        gross[i] = (ex / e - 1) * 100 if long else (1 - ex / e) * 100
    return (gross - COST[order]) * LEV[order]


def expectancy(r):
    r = r[np.isfinite(r)]
    if len(r) == 0: return dict(n=0, netexp=np.nan, pwin=np.nan, avgwin=np.nan, ploss=np.nan, avgloss=np.nan)
    win = r[r > 0]; loss = r[r <= 0]
    return dict(n=len(r), netexp=float(r.mean()), pwin=len(win) / len(r),
                avgwin=float(win.mean()) if len(win) else 0.0,
                ploss=len(loss) / len(r), avgloss=float(loss.mean()) if len(loss) else 0.0)


def run_stock(symbol, con, is_fno):
    rows = con.execute("SELECT target,rule_json FROM unified_patterns WHERE symbol=?", (symbol,)).fetchall()
    rows = [(t, rj) for t, rj in rows if t in TGT]
    if not rows: return None
    frame = FE.load_frame(symbol, lookback_N=5)
    if frame.empty: return None
    yr = frame["year"].values
    net = {}
    for t in set(t for t, _ in rows):
        d, pct, w = TGT[t]; net[t] = routing.net_roc_series(frame, d, pct, w, is_fno)[0]   # legal F&O-aware routing
    pre_m = yr <= SELECT_MAX_YEAR
    kept = []                                                   # (pre_exp, target, w, fire_mask)
    for t, rj in rows:
        conds = [tuple(c) for c in json.loads(rj)]
        mask = apply_rule(frame, conds).values
        e = expectancy(net[t][pre_m & mask])
        if e["n"] >= MIN_OCC_PRE and e["netexp"] > 0:          # a-priori: positive <=2024 expectancy
            kept.append((e["netexp"], t, TGT[t][2], mask))
    if not kept: return None
    kept.sort(reverse=True, key=lambda x: x[0])                 # prefer highest pre-period expectancy
    row = {"symbol": symbol, "kept_rules": len(kept)}
    exps = []; yrs_pos = 0
    for Y in EVAL_YEARS:
        te = np.where(yr == Y)[0]; taken = []; day = 0
        while day < len(te):
            gi = te[day]; hit = None
            for pe, t, w, mask in kept:
                if mask[gi] and np.isfinite(net[t][gi]): hit = (w, net[t][gi]); break
            if hit: taken.append(hit[1]); day += hit[0]
            else: day += 1
        e = expectancy(np.array(taken))
        row[f"netexp_{Y}"] = round(e["netexp"], 3) if e["n"] else None
        row[f"n_{Y}"] = e["n"]; row[f"pwin_{Y}"] = round(e["pwin"], 2) if e["n"] else None
        row[f"avgwin_{Y}"] = round(e["avgwin"], 2) if e["n"] else None
        row[f"avgloss_{Y}"] = round(e["avgloss"], 2) if e["n"] else None
        if e["n"] >= MIN_OCC_EVAL and np.isfinite(e["netexp"]):
            exps.append(e["netexp"]); yrs_pos += (e["netexp"] > 0)
    row["years_evaluated"] = len(exps); row["years_positive"] = yrs_pos
    row["min_netexp"] = round(min(exps), 3) if exps else None
    row["mean_netexp"] = round(float(np.mean(exps)), 3) if exps else None
    row["sustained"] = bool(exps) and yrs_pos == len(EVAL_YEARS) and len(exps) == len(EVAL_YEARS)
    return row


def main():
    args = sys.argv[1:]
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    syms = args or [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]
    rows = []
    for n, s in enumerate(syms, 1):
        try:
            r = run_stock(s, con, int(fno.get(s, 0)))
            if r: rows.append(r)
        except Exception as e:
            print(f"  {s}: ERR {str(e)[:70]}")
        if len(syms) > 10 and n % 50 == 0: print(f"  ...{n}/{len(syms)}", flush=True)
    con.close()
    df = pd.DataFrame(rows)
    if df.empty: print("no results"); return
    df = df.sort_values(["sustained", "min_netexp"], ascending=[False, False])
    REP.mkdir(exist_ok=True)
    out = REP / "expectancy_scorecard.csv"
    try:
        df.to_csv(out, index=False)
    except PermissionError:
        out = REP / "expectancy_scorecard_NEW.csv"; df.to_csv(out, index=False)
        print(f"WARNING: expectancy_scorecard.csv is LOCKED (open in Excel?). Wrote {out.name} instead — "
              f"close the original and rename, or re-run.")
    sus = df[df.sustained]
    print(f"\n============ NET-EXPECTANCY SCORECARD (reused patterns, clean OOS {EVAL_YEARS}) ============")
    print(f"  workers evaluated: {len(df)} | SUSTAINED (net expectancy > 0 BOTH years): {len(sus)}")
    show = ["symbol", "netexp_2025", "netexp_2026", "pwin_2025", "avgwin_2025", "avgloss_2025",
            "n_2025", "n_2026", "min_netexp"]
    show = [c for c in show if c in df.columns]
    print("\n  top sustained workers (by worst-year net expectancy %/trade):")
    print(sus.head(15)[show].to_string(index=False) if len(sus) else "  (none)")
    print("\nwritten: reports/expectancy_scorecard.csv")


if __name__ == "__main__":
    main()
