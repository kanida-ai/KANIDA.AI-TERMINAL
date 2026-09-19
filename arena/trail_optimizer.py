"""
PER-STOCK TRAILING OPTIMIZER with WALK-FORWARD validation. For each worker's intraday MIS 5x-short days,
try a small economically-motivated menu of trails, TUNE the choice on 2025 (pick best risk-adjusted),
then CONFIRM it out-of-sample on sealed 2026. Leverage is fixed 5x — the trail is the only lever.
Overfit guard: the trail is SELECTED on 2025 only; 2026 is reported, never used to choose. A worker's
trail is 'validated' only if it stays positive AND beats no-trail on 2026.

Parallel + vectorized to hold the sub-minute milestone. Output: reports/trail_optimizer.csv.
Run: PYTHONIOENCODING=utf-8 python arena/trail_optimizer.py
"""
import os, sys, time, csv
os.environ.setdefault("OMP_NUM_THREADS", "1")
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "arena")); sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "kanida_engine"))
import intraday_trail as IT
import backtest_1min as B
REP = ROOT / "reports"

MENU = [("no-trail", "baseline", None),
        ("cap 6/2/5/3", "capital", (6, 2, 5, 3)),
        ("cap 8/3/4/2", "capital", (8, 3, 4, 2)),
        ("cap 10/4/6/3", "capital", (10, 4, 6, 3)),
        ("cap 5/2/3/1.5", "capital", (5, 2, 3, 1.5)),
        ("atr K1.2 w14", "atr", (1.2, 14)),
        ("atr K2.0 w14", "atr", (2.0, 14)),
        ("donch N20", "donchian", (20,)),
        ("donch N40", "donchian", (40,))]


def _stats(data, days, m, p):
    return IT.stats([IT.sim_day(data[d], m, p) for d in days])


def opt_worker(sym):
    try:
        days = [d for d in IT.short_days(sym)]
    except Exception:
        return None
    data = B.load_1min(sym)                                    # {date: {hm,o,h,l,c}} — fast vectorized load
    days = [d for d in days if d in data and len(data[d]["o"]) >= 5]
    d25 = [d for d in days if d[:4] == "2025"]; d26 = [d for d in days if d[:4] == "2026"]
    if len(d25) < 25 or len(d26) < 12:
        return None
    base26 = _stats(data, d26, "baseline", None)
    best = None
    for name, m, p in MENU:                                   # TUNE on 2025 only
        s25 = _stats(data, d25, m, p)
        rdd = s25["total"] / -s25["maxdd"] if s25["maxdd"] < 0 else s25["total"]
        if s25["total"] > 0 and (best is None or rdd > best[0]):
            best = (rdd, name, m, p, s25)
    if best is None:
        return None
    _, name, m, p, s25 = best
    s26 = _stats(data, d26, m, p)                             # CONFIRM out-of-sample on sealed 2026
    o_rdd = s26["total"] / -s26["maxdd"] if s26["maxdd"] < 0 else 0.0
    b_rdd = base26["total"] / -base26["maxdd"] if base26["maxdd"] < 0 else 0.0
    validated = bool(s26["total"] > 0 and o_rdd >= b_rdd)     # positive OOS AND beats no-trail on ret/DD
    return {"symbol": sym, "n25": len(d25), "n26": len(d26), "chosen_trail": name,
            "tune2025_ret": round(s25["total"]), "tune2025_dd": round(s25["maxdd"]), "tune2025_retdd": round(best[0], 2),
            "oos2026_ret": round(s26["total"]), "oos2026_dd": round(s26["maxdd"]), "oos2026_retdd": round(o_rdd, 2),
            "notrail2026_ret": round(base26["total"]), "notrail2026_dd": round(base26["maxdd"]), "notrail2026_retdd": round(b_rdd, 2),
            "validated": int(validated)}


def main():
    import multiprocessing as mp
    T0 = time.time()
    syms = IT.__dict__.get("_", None)
    d = pd.read_csv(REP / "expectancy_scorecard_NEW.csv" if (REP / "expectancy_scorecard_NEW.csv").exists()
                    else REP / "expectancy_scorecard.csv")
    syms = sorted(d[(d.sustained == True) & (d.min_netexp >= 1.0)].symbol)
    rows = []
    with mp.get_context("spawn").Pool(8) as pool:
        for r in pool.imap_unordered(opt_worker, syms):
            if r: rows.append(r)
    df = pd.DataFrame(rows)
    out = REP / "trail_optimizer.csv"
    try: df.to_csv(out, index=False)
    except PermissionError: out = REP / "trail_optimizer_fixed.csv"; df.to_csv(out, index=False)
    dt = time.time() - T0
    val = df[df.validated == 1]
    print(f"=== PER-STOCK TRAIL OPTIMIZER · walk-forward (tune 2025 / confirm 2026) · {dt:.1f}s "
          f"[{'PASS' if dt < 60 else 'over'} sub-min] ===")
    print(f"workers optimized: {len(df)} | VALIDATED (positive OOS 2026 AND beats no-trail): {len(val)}")
    print(f"chosen trails: {dict(df.chosen_trail.value_counts())}")
    print(f"\nOOS 2026 (validated cohort): sum ret {val.oos2026_ret.sum():,.0f}% | "
          f"median ret/DD trailed {val.oos2026_retdd.median():.1f} vs no-trail {val.notrail2026_retdd.median():.1f}")
    print("\ntop 12 by OOS 2026 ret/DD:")
    show = val.sort_values("oos2026_retdd", ascending=False).head(12)
    for _, r in show.iterrows():
        print(f"  {r.symbol:<11} {r.chosen_trail:<14} OOS26 ret {r.oos2026_ret:>+5}% DD {r.oos2026_dd:>+5}% "
              f"(ret/DD {r.oos2026_retdd:>5.1f})  vs no-trail ret/DD {r.notrail2026_retdd:>5.1f}")
    print(f"\nwritten: {out.name}")


if __name__ == "__main__":
    main()
