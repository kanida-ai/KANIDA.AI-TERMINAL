# -*- coding: utf-8 -*-
"""Auto-config the gap-fade basket: search N / gap-threshold / side to maximize return-per-drawdown.
OPTIMIZE on 2022-2025 (train), APPLY out-of-sample to 2026 (honest). Also report the in-sample 2026 'cheat'
config to expose the overfitting gap. 1X, enter open/exit close, Rs1L/stock, net 0.12%/stock."""
import os, sqlite3, itertools
import numpy as np, pandas as pd
DDB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "db", "kanida_universe.db")
COST = 0.12


def load():
    con = sqlite3.connect("file:" + DDB.replace("\\", "/") + "?mode=ro", uri=True)
    syms = pd.read_sql_query("SELECT DISTINCT symbol FROM universe_master WHERE in_nifty50=1 AND is_active=1", con).symbol.tolist()
    d = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE symbol IN (%s) AND trade_date>='2021-12-01' ORDER BY symbol,trade_date" % ",".join("?"*len(syms)), con, params=syms); con.close()
    d["pc"] = d.groupby("symbol").close.shift(1); d = d.dropna(subset=["pc"])
    d["gap"] = (d.open/d.pc-1)*100; d["oc"] = (d.close/d.open-1)*100
    return d[d.trade_date >= "2022-01-01"]


def daily_returns(d, N, gap_min, side):
    rows = []
    for dt, g in d.groupby("trade_date"):
        longs = g[g.gap <= -gap_min].sort_values("gap").head(N)
        shorts = g[g.gap >= gap_min].sort_values("gap", ascending=False).head(N)
        pos = []
        if side in ("both", "long"): pos += list(longs.oc - COST)          # long profit = oc - cost
        if side in ("both", "short"): pos += list(-shorts.oc - COST)       # short profit = -oc - cost
        if pos: rows.append((dt, np.mean(pos)))                             # equal-weight portfolio daily return %
    return pd.DataFrame(rows, columns=["date", "ret"])


def stats(R):
    if R.empty or len(R) < 20: return None
    cum = R.ret.cumsum(); dd = (cum.cummax()-cum).max()
    tot = cum.iloc[-1]; avg = R.ret.mean()
    return dict(days=len(R), total=tot, avg=avg, maxdd=dd, ratio=tot/dd if dd > 0.1 else tot,
                win=(R.ret > 0).mean()*100)


def main():
    d = load()
    tr = d[d.trade_date < "2026-01-01"]; te = d[d.trade_date >= "2026-01-01"]
    grid = list(itertools.product([2, 3, 5, 8, 10], [0.0, 0.3, 0.5, 0.8, 1.2], ["both", "long", "short"]))
    res = []
    for N, gm, sd in grid:
        st = stats(daily_returns(tr, N, gm, sd))
        if st and st["days"] >= 200: res.append((N, gm, sd, st))
    res.sort(key=lambda x: -x[3]["ratio"])
    print(f"AUTO-CONFIG gap-fade  ·  optimize on TRAIN 2022-2025 by return/DD  ·  1X  ·  Rs1L/stock\n")
    print("  TOP 6 TRAIN configs (2022-2025):")
    print(f"    {'N':>3}{'gap_min':>9}{'side':>7}{'train_tot%':>12}{'maxDD%':>9}{'ret/DD':>8}{'win%':>7}")
    for N, gm, sd, st in res[:6]:
        print(f"    {N:>3}{gm:>9}{sd:>7}{st['total']:>+12.1f}{st['maxdd']:>9.1f}{st['ratio']:>8.2f}{st['win']:>6.0f}%")
    bN, bgm, bsd, bst = res[0]
    # apply best-train config OOS to 2026
    oos = stats(daily_returns(te, bN, bgm, bsd))
    print(f"\n  >>> BEST TRAIN CONFIG: N={bN}, gap_min={bgm}%, side={bsd}")
    print(f"      TRAIN 2022-25 : total {bst['total']:+.1f}%  maxDD {bst['maxdd']:.1f}%  ret/DD {bst['ratio']:.2f}  win {bst['win']:.0f}%")
    print(f"      OOS 2026      : total {oos['total']:+.1f}%  maxDD {oos['maxdd']:.1f}%  ret/DD {oos['ratio']:.2f}  win {oos['win']:.0f}%  (avg {oos['avg']:+.3f}%/day)")
    # baseline (N=5, no filter, both) 2026 for comparison
    base26 = stats(daily_returns(te, 5, 0.0, "both"))
    print(f"      (baseline N=5/nofilter/both 2026: total {base26['total']:+.1f}%  avg {base26['avg']:+.3f}%/day)")
    # in-sample 2026 'cheat' — the config that WOULD have been best on 2026 (hindsight, overfit)
    cheat = []
    for N, gm, sd in grid:
        st = stats(daily_returns(te, N, gm, sd))
        if st: cheat.append((N, gm, sd, st))
    cheat.sort(key=lambda x: -x[3]["ratio"])
    cN, cgm, csd, cst = cheat[0]
    print(f"\n  IN-SAMPLE 2026 'cheat' best (hindsight, OVERFIT): N={cN}, gap_min={cgm}, side={csd}  ->  total {cst['total']:+.1f}%  ret/DD {cst['ratio']:.2f}  avg {cst['avg']:+.3f}%/day")
    print(f"  (the honest deployable number is the OOS 2026 line above, NOT the cheat line)")


if __name__ == "__main__":
    main()
