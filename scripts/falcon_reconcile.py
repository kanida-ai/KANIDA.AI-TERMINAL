"""Reconcile the attached 'Trailing Agent' (arm+1%) vs current live (arm+2%),
and compute the HARD CEILING for a +1%-lock strategy: % of days the basket even
TOUCHES +1% intraday. Same data/universe (Top-5, 09:15, RND DB). Next-open fills.
"""
import sqlite3, math
from pathlib import Path
from collections import defaultdict
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
PERSONA = "falcon_top10_daily"; ALIASES = {"ZOMATO": "ETERNAL"}
OPEN, CLOSE = "09:15:00", "15:29:00"; TOPN = 5; CAP = 100000.0


def load_signals(con):
    out = {}
    for ed, rk, sym in con.execute(
        "SELECT entry_date,engine_rank,symbol FROM falcon_signal_day_study "
        "WHERE persona=? AND engine_rank BETWEEN 1 AND ? ORDER BY entry_date,engine_rank",
        (PERSONA, TOPN)):
        out.setdefault(ed, []).append(sym)
    return out


def load_day(con, day, syms):
    fetch = {ALIASES.get(s, s): s for s in syms}; ph = ",".join("?" * len(fetch))
    rows = con.execute(
        f"SELECT symbol,substr(bar_time,12,5) hm,open,high,low,close FROM ohlc_1min "
        f"WHERE bar_time BETWEEN ? AND ? AND symbol IN ({ph})",
        [f"{day} {OPEN}", f"{day} {CLOSE}", *fetch.keys()]).fetchall()
    per = {}
    for osym, hm, o, h, l, c in rows:
        per.setdefault(fetch[osym], {})[hm] = (o, h, l, c)
    return per


def build(per, order):
    present = [(s, per[s]["09:15"][0]) for s in order
               if s in per and "09:15" in per[s] and per[s]["09:15"][0] and per[s]["09:15"][0] > 0]
    if not present:
        return None
    alloc = CAP / len(present)
    legs = [(s, e, math.floor(alloc / e)) for s, e in present if math.floor(alloc / e) >= 1]
    if not legs:
        return None
    syms = [s for s, _, _ in legs]
    entry = np.array([e for _, e, _ in legs], float); qty = np.array([q for _, _, q in legs], float)
    grid = sorted({m for s in syms for m in per[s]} | {"09:15"}); grid = [m for m in grid if m >= OPEN[:5]]

    def series(s, idx, ff=True):
        vals, last = [], None
        for m in grid:
            v = per[s].get(m)
            if v and v[idx] and np.isfinite(v[idx]):
                last = v[idx]
            vals.append(last)
        e = per[s]["09:15"][0]
        return [x if x is not None else e for x in vals]

    close = np.column_stack([series(s, 3) for s in syms])
    high = np.column_stack([series(s, 1) for s in syms])
    low = np.column_stack([series(s, 2) for s in syms])
    opn = np.column_stack([series(s, 0) for s in syms])
    dep = float((qty * entry).sum())
    return dict(entry=entry, qty=qty, close=close, high=high, low=low, opn=opn, dep=dep, n=len(grid))


def sim_trail(M, arm, floor, give, stop):
    """Portfolio trail with next-open fills (faithful to research_trailing_top5)."""
    qty, close, opn, dep, n = M["qty"], M["close"], M["opn"], M["dep"], M["n"]
    port = close @ qty; ret = (port - dep) / dep * 100.0
    openval = opn @ qty; nxt = np.empty_like(openval); nxt[:-1] = openval[1:]; nxt[-1] = port[-1]
    def realized(i): return (nxt[i] - dep) / dep * 100.0
    armed = False; peak = None
    for i in range(n - 1):
        r = ret[i]
        if r <= -abs(stop):
            return realized(i)
        if not armed:
            if r >= arm:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        if r <= max(floor, peak - give):
            return realized(i)
    return (port[-1] - dep) / dep * 100.0


def basket_mfe(M):
    return float((((M["high"] - M["entry"]) * M["qty"]).sum(axis=1) / M["dep"] * 100).max())


def main():
    con = sqlite3.connect(str(RND)); sig = load_signals(con); days = sorted(sig)
    CFG = {"THEIRS arm1/floor1/give0.75/stop1.5": (1.0, 1.0, 0.75, 1.5),
           "CURRENT arm2/floor1/give0.5/stop1.5": (2.0, 1.0, 0.50, 1.5)}
    res = {k: [] for k in CFG}; mfe = []; dates = []
    for d in days:
        M = build(load_day(con, d, sig[d]), sig[d])
        if not M:
            continue
        dates.append(d); mfe.append(basket_mfe(M))
        for k, (a, f, g, s) in CFG.items():
            res[k].append(sim_trail(M, a, f, g, s))
    con.close()
    mfe = np.array(mfe); n = len(dates)
    print(f"=== {n} days (Top-5, 09:15, same RND DB) ===\n")
    print("HARD CEILING — does the basket even TOUCH the level intraday?")
    for lv in (0.5, 1.0, 1.5, 2.0):
        print(f"  days basket MFE >= +{lv:.1f}%: {(mfe>=lv).mean()*100:5.1f}%")
    print(f"  -> a +1%-lock can turn AT MOST {(mfe>=1.0).mean()*100:.1f}% of days into >=+1% (minus fill lag).\n")
    print(f"{'config':<40}{'mean%':>8}{'%positive':>11}{'%days>=1%':>11}{'worst%':>8}{'sum%':>9}")
    for k in CFG:
        a = np.array(res[k])
        print(f"{k:<40}{a.mean():>8.3f}{(a>0).mean()*100:>11.1f}{(a>=1.0).mean()*100:>11.1f}{a.min():>8.2f}{a.sum():>9.1f}")
    # monthly %days>=1% for THEIRS, and how many months would pass 90%
    theirs = np.array(res["THEIRS arm1/floor1/give0.75/stop1.5"])
    md = defaultdict(list)
    for dt, v in zip(dates, theirs):
        md[dt[:7]].append(v)
    passes = sum(1 for m in md if (np.array(md[m]) >= 1.0).mean() >= 0.9)
    pos90 = sum(1 for m in md if (np.array(md[m]) > 0).mean() >= 0.9)
    print(f"\nTHEIRS config monthly: months with >=90% of days >=+1%: {passes}/{len(md)}")
    print(f"THEIRS config monthly: months with >=90% of days POSITIVE(>0): {pos90}/{len(md)}")


if __name__ == "__main__":
    main()
