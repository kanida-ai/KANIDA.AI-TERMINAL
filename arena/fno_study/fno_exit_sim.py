"""STAGE 4 — EXIT SIMULATOR. Replays the 1-min path (09:16->15:29) per stock-day and applies exit rules,
vs hold-to-close, to see how much of the ~1.3% MFE is actually capturable. Long & Short books.
Configs: HOLD, profit-target only, target+stop bracket (OCO), trailing stop. Intrabar tie -> STOP wins (conservative).
Reuses blended avg-entry + tier from Stage-2 records. Costs included. Accumulates across all months.
Read-only sources; Falcon untouched.  Usage: python fno_exit_sim.py [YYYY-MM ...]"""
import os, sys, sqlite3, glob
import numpy as np, pandas as pd
from collections import defaultdict
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UEDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
RECDIR = os.path.join(ROOT, "arena", "fno_study", "records")

# (name, target%, stop%, trail%)  None = not used
CFG = [("HOLD", None, None, None),
       ("TGT0.5", 0.5, None, None), ("TGT1.0", 1.0, None, None), ("TGT1.5", 1.5, None, None),
       ("BR0.5/0.5", 0.5, 0.5, None), ("BR1.0/1.0", 1.0, 1.0, None), ("BR1.0/0.5", 1.0, 0.5, None),
       ("BR1.5/1.0", 1.5, 1.0, None), ("TRAIL0.3", None, None, 0.3), ("TRAIL0.5", None, None, 0.5)]

def cost_pct(exit_ratio):                         # round-trip cost as % of Rs5L (entry+exit ~Rs5L legs)
    CAP = 5e5; bv = CAP; sv = CAP*exit_ratio
    brok = min(20, 0.0003*bv)+min(20, 0.0003*sv); stt = 0.00025*max(bv, sv)
    txn = 0.0000297*(bv+sv); stamp = 0.00003*bv; sebi = 0.000001*(bv+sv); gst = 0.18*(brok+txn+sebi)
    return (brok+stt+txn+stamp+sebi+gst)/CAP*100

def first_true(mask):
    return int(np.argmax(mask)) if mask.any() else len(mask)

def exit_price(hi, lo, cl, avg, side, T, S, trail):
    n = len(cl); cands = []
    if side == "S":                               # short: profit down, loss up
        if T: i = first_true(lo <= avg*(1-T/100));  cands.append((i, "t", avg*(1-T/100))) if i < n else None
        if S: i = first_true(hi >= avg*(1+S/100));  cands.append((i, "x", avg*(1+S/100))) if i < n else None
        if trail:
            rl = np.empty(n); rl[0] = avg; rl[1:] = np.minimum.accumulate(lo)[:-1]
            lvl = rl*(1+trail/100); i = first_true(hi >= lvl)
            if i < n: cands.append((i, "x", lvl[i]))
    else:                                         # long: profit up, loss down
        if T: i = first_true(hi >= avg*(1+T/100));  cands.append((i, "t", avg*(1+T/100))) if i < n else None
        if S: i = first_true(lo <= avg*(1-S/100));  cands.append((i, "x", avg*(1-S/100))) if i < n else None
        if trail:
            rh = np.empty(n); rh[0] = avg; rh[1:] = np.maximum.accumulate(hi)[:-1]
            lvl = rh*(1-trail/100); i = first_true(lo <= lvl)
            if i < n: cands.append((i, "x", lvl[i]))
    if not cands: return cl[-1]
    mi = min(c[0] for c in cands); tied = [c for c in cands if c[0] == mi]
    pick = next((c for c in tied if c[1] == "x"), tied[0])   # tie -> stop/trail (adverse) wins
    return pick[2]

# accumulators: sums[(side,cfg)] = [ret_sum, wins, n]; by_tier[(side,cfg,tier)] = same
SUM = defaultdict(lambda: [0.0, 0, 0]); TIER = defaultdict(lambda: [0.0, 0, 0])

def process_month(ym):
    rp = os.path.join(RECDIR, f"{ym}.csv")
    if not os.path.exists(rp): return
    rec = pd.read_csv(rp)[["symbol", "date", "avg", "tier"]]
    meta = {(r.symbol, r.date): (r.avg, r.tier) for r in rec.itertuples()}
    y, m = map(int, ym.split("-")); nxt = f"{y+(m//12):04d}-{(m%12)+1:02d}-01"
    con = sqlite3.connect("file:" + UEDB.replace("\\", "/") + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT symbol,bar_time,high,low,close FROM ohlc_1min WHERE bar_time>=? AND bar_time<?",
                           con, params=(f"{ym}-01 00:00:00", f"{nxt} 00:00:00")); con.close()
    df["date"] = df.bar_time.str[:10]; df["hhmm"] = df.bar_time.str[11:16]
    df = df[df.hhmm >= "09:16"].sort_values(["symbol", "date", "bar_time"])
    for (sym, dt), g in df.groupby(["symbol", "date"]):
        meta_k = meta.get((sym, dt))
        if meta_k is None: continue
        avg, tier = meta_k
        hi = g.high.values; lo = g.low.values; cl = g.close.values
        if len(cl) < 2 or not np.isfinite(avg): continue
        for side in ("S", "L"):
            for name, T, S, tr in CFG:
                ep = cl[-1] if name == "HOLD" else exit_price(hi, lo, cl, avg, side, T, S, tr)
                ret = ((avg-ep) if side == "S" else (ep-avg))/avg*100 - cost_pct(ep/avg)
                for acc, kk in ((SUM, (side, name)), (TIER, (side, name, tier))):
                    a = acc[kk]; a[0] += ret; a[1] += (ret > 0); a[2] += 1

if __name__ == "__main__":
    months = sys.argv[1:] or [f"{y}-{m:02d}" for y in (2024, 2025, 2026) for m in range(1, 13) if (2024, 5) <= (y, m) <= (2026, 7)]
    for ym in months:
        process_month(ym); print(f"  done {ym}", flush=True)
    def line(side):
        print(f"\n{'='*70}\n{side} book — exit rule vs HOLD-to-close  (net %/day, all F&O days)\n{'='*70}")
        print(f"{'config':<12}{'avg%/day':>10}{'win%':>8}{'vs HOLD':>10}")
        hold = SUM[(side, 'HOLD')]; hbase = hold[0]/hold[2]
        for name, *_ in CFG:
            a = SUM[(side, name)]; avg = a[0]/a[2]
            print(f"{name:<12}{avg:>+10.4f}{a[1]/a[2]*100:>7.1f}%{avg-hbase:>+10.4f}")
    line("S"); line("L")
    # per-tier for the short book, best configs
    print(f"\n{'='*70}\nSHORT book by tier — HOLD vs TGT1.0 vs BR1.0/0.5 vs TRAIL0.5 (net %/day)\n{'='*70}")
    print(f"{'tier':<20}{'HOLD':>9}{'TGT1.0':>9}{'BR1.0/.5':>10}{'TRAIL0.5':>10}")
    tiers = sorted(set(k[2] for k in TIER if k[0] == 'S' and isinstance(k[2], str)))
    for t in tiers:
        vals = []
        for cfg in ("HOLD", "TGT1.0", "BR1.0/0.5", "TRAIL0.5"):
            a = TIER[('S', cfg, t)]; vals.append(a[0]/a[2] if a[2] else float('nan'))
        print(f"{t:<20}{vals[0]:>+9.3f}{vals[1]:>+9.3f}{vals[2]:>+10.3f}{vals[3]:>+10.3f}")
    pd.DataFrame([{"side": k[0], "config": k[1], "avg": v[0]/v[2], "win%": v[1]/v[2]*100, "n": v[2]}
                  for k, v in SUM.items()]).round(4).to_csv(os.path.join(ROOT, "arena", "fno_study", "FNO_EXIT_SIM.csv"), index=False)
