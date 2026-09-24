"""Extend the intraday _opt_dataset.pkl through 2026-07-10 by appending day-matrices for
2026-07-06..10. Signals from falcon_top10_audit (durable archive; the live table rotated out
these days) — verified bit-exact vs the research study on overlap. 1-min from ohlc_1min
(refreshed to 07-10). Matrices match the stored format exactly (Rs1L sizing; the report
recomputes qty at Rs5L). Entry = 09:15 open, grid 09:15..15:29 (intraday square-off)."""
import pickle, sqlite3, sys
from pathlib import Path
import numpy as np
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
DS = ROOT / "docs" / "ops" / "_opt_dataset.pkl"
sys.path.insert(0, str(ROOT / "backend"))
from falcon.db import falcon_conn
ALIAS = {"ZOMATO": "ETERNAL"}
ASSIGNED = 100000.0     # matches the stored dataset convention (report recomputes at Rs5L)
NEW_DAYS = ["2026-07-06", "2026-07-07", "2026-07-08", "2026-07-09", "2026-07-10"]


def top5(D):
    with falcon_conn() as pc:
        pc.row_factory = None
        return pc.execute("SELECT rank,symbol FROM falcon_top10_audit WHERE entry_date=? AND rank<=5 ORDER BY rank", (D,)).fetchall()


def day_bars(con, osym, D):
    rows = con.execute(
        "SELECT substr(bar_time,12,5) hm, open, high, low, close FROM ohlc_1min "
        "WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",
        (osym, D + " 09:15:00", D + " 15:29:00")).fetchall()
    return {hm: (o, h, l, c) for hm, o, h, l, c in rows}


def build_day(con, D):
    seen = set(); syms = []; osyms = []; rank = []
    for rk, s in top5(D):
        o = ALIAS.get(s, s)
        if o in seen:
            continue
        seen.add(o); syms.append(s); osyms.append(o); rank.append(rk)
    grids = [day_bars(con, o, D) for o in osyms]
    miss = [syms[j] for j, g in enumerate(grids) if not g]
    if miss:
        return None, miss
    nstk = len(syms)
    mins = sorted(set().union(*[set(g) for g in grids]))
    n = len(mins)
    close = np.zeros((n, nstk)); opn = np.zeros((n, nstk)); high = np.zeros((n, nstk)); low = np.zeros((n, nstk))
    entry = np.zeros(nstk)
    for j in range(nstk):
        g = grids[j]
        entry[j] = g["09:15"][0] if "09:15" in g else g[sorted(g)[0]][0]
        lc = entry[j]
        for ti, hm in enumerate(mins):
            if hm in g:
                o, h, l, c = g[hm]
                o = o if o > 0 else lc; c = c if c > 0 else lc
                opn[ti, j] = o; close[ti, j] = c
                high[ti, j] = h if h > 0 else max(o, c); low[ti, j] = l if l > 0 else min(o, c)
                lc = c
            else:
                opn[ti, j] = lc; close[ti, j] = lc; high[ti, j] = lc; low[ti, j] = lc
    qty = np.floor((ASSIGNED / nstk) / entry)
    dep = float((entry * qty).sum())
    return dict(entry=entry, qty=qty, close=close, high=high, low=low, opn=opn,
               dep=dep, n=n, nstocks=nstk, syms=syms, rank=rank, grid=mins), None


def main():
    mats = pickle.load(open(DS, "rb"))
    have = {d for d, _ in mats}
    con = sqlite3.connect(str(RND))
    added = []
    for D in NEW_DAYS:
        if D in have:
            print(f"  {D} already present, skip"); continue
        M, miss = build_day(con, D)
        if M is None:
            print(f"  {D} SKIPPED — no 1-min for {miss}"); continue
        mats.append((D, M)); added.append(D)
        print(f"  {D}: {M['nstocks']} stocks {M['syms']} | {M['n']} mins | entry={np.round(M['entry'],2)}")
    con.close()
    mats.sort(key=lambda x: x[0])
    out = DS
    try:
        pickle.dump(mats, open(out, "wb"), protocol=4)
    except PermissionError:
        out = DS.with_name(DS.stem + "_v2.pkl"); pickle.dump(mats, open(out, "wb"), protocol=4)
    print(f"[*] added {added} -> dataset now {len(mats)} days, {mats[0][0]}..{mats[-1][0]}  ({out.name})")


if __name__ == "__main__":
    main()
