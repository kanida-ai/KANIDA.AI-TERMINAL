"""Daily P&L 2026-05-01 .. 2026-07-17 (fresh ohlc_daily from data/db, to 07-17; open->close = 09:15->EOD).
Falcon(top-10 L 5x), Tail-short(201+ S 5x), Combo 70/30 AND 80/20. Capital = Rs1cr, 5x MIS, equal-weight per leg.
NET P&L uses the REAL itemised cost stack (brokerage, STT, exchange txn, stamp, SEBI, GST) per name per round-trip.
Read-only; Falcon untouched."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")           # fresh ohlc_daily to 2026-07-17
RDB = os.path.join(ROOT, "data", "db", "falcon_research.db")
CAP, LEV = 1e7, 5                                                       # Rs1cr, 5x MIS

def cost_components(tb, ts):                                            # tb=buy value, ts=sell value (Rs)
    brok = min(20, 0.0003 * tb) + min(20, 0.0003 * ts)
    stt = 0.00025 * ts; txn = 0.0000297 * (tb + ts); stamp = 0.00003 * tb
    sebi = 0.000001 * (tb + ts); gst = 0.18 * (brok + txn + sebi)
    return dict(brokerage=brok, stt=stt, exchange=txn, stamp=stamp, sebi=sebi, gst=gst)

uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
od = pd.read_sql_query("SELECT symbol, trade_date, open, close FROM ohlc_daily WHERE trade_date BETWEEN '2026-05-01' AND '2026-07-17'", uc)
uc.close()
op = od.pivot_table(index="trade_date", columns="symbol", values="open"); cp = od.pivot_table(index="trade_date", columns="symbol", values="close")
cal = sorted(od.trade_date.unique()); nextd = {cal[i]: cal[i + 1] for i in range(len(cal) - 1)}
print(f"ohlc_daily coverage: {cal[0]} .. {cal[-1]} ({len(cal)} trading days)")
rc = sqlite3.connect("file:" + RDB.replace("\\", "/") + "?mode=ro", uri=True)
rk = pd.read_sql_query("SELECT signal_date, rank, symbol FROM falcon_full_ranking WHERE signal_date BETWEEN '2026-04-30' AND '2026-07-16'", rc)
rc.close()

def leg_pnl(names, ed, sgn):
    """Return (gross_pnl_Rs, cost_Rs, cost_dict) for an equal-weight 5x leg over `names` on entry day ed."""
    names = [s for s in names if ed in op.index and s in op.columns and op.at[ed, s] == op.at[ed, s] and cp.at[ed, s] == cp.at[ed, s]]
    if not names: return None
    per = LEV * CAP / len(names); gross = 0.0; cd = dict(brokerage=0, stt=0, exchange=0, stamp=0, sebi=0, gst=0)
    for s in names:
        o = op.at[ed, s]; c = cp.at[ed, s]; qty = per / o
        gross += sgn * (c - o) * qty
        comp = cost_components(o * qty, c * qty)
        for k in cd: cd[k] += comp[k]
    return gross, sum(cd.values()), cd

rows = []; tot_cost = dict(brokerage=0, stt=0, exchange=0, stamp=0, sebi=0, gst=0)
for sd, g in rk.groupby("signal_date"):
    ed = nextd.get(sd)
    if not ed: continue
    fal = leg_pnl(list(g[g["rank"] <= 10].symbol), ed, 1)
    tail = leg_pnl(list(g[g["rank"] >= 201].symbol), ed, -1)
    if not fal or not tail: continue
    fal_net = (fal[0] - fal[1]) / CAP * 100; tail_net = (tail[0] - tail[1]) / CAP * 100
    for k in tot_cost: tot_cost[k] += 0.7 * fal[2][k] + 0.3 * tail[2][k]     # attribute cost to the 70/30 book
    rows.append((ed, round(fal_net, 2), round(tail_net, 2),
                 round(0.7 * fal_net + 0.3 * tail_net, 2), round(0.8 * fal_net + 0.2 * tail_net, 2)))
P = pd.DataFrame(rows, columns=["trade_date", "Falcon", "TailShort", "Combo70_30", "Combo80_20"]).sort_values("trade_date")
for c in ["Falcon", "TailShort", "Combo70_30", "Combo80_20"]: P[c + "_cum"] = P[c].cumsum().round(1)
csv = os.path.join(ROOT, "docs", "reports", "daily_pnl_NET_2026-05-01_to_07-17.csv"); P.to_csv(csv, index=False)
print(f"\nDAILY NET P&L (Rs1cr, 5x MIS, REAL cost stack)  {P.trade_date.iloc[0]} .. {P.trade_date.iloc[-1]}\n")
print(f"{'date':<12}{'Falcon':>8}{'TailSht':>8}{'70/30':>8}{'80/20':>8}{'  |':>3}{'Fal_cum':>9}{'70/30cum':>9}{'80/20cum':>9}")
for _, r in P.iterrows():
    print(f"{r.trade_date:<12}{r.Falcon:>+8.2f}{r.TailShort:>+8.2f}{r.Combo70_30:>+8.2f}{r.Combo80_20:>+8.2f}{'  |':>3}{r.Falcon_cum:>+9.1f}{r.Combo70_30_cum:>+9.1f}{r.Combo80_20_cum:>+9.1f}")
def mdd(s): cc = s.cumsum(); return (cc.cummax() - cc).max()
print("\n" + "-" * 74)
for c in ["Falcon", "TailShort", "Combo70_30", "Combo80_20"]:
    s = P[c]; print(f"{c:<12} NET total {s.sum():>+8.1f}%  best {s.max():>+6.2f}  worst {s.min():>+6.2f}  maxDD {mdd(s):>5.1f}%  win-days {int((s>0).mean()*100)}%")
print(f"\nCOST STACK for the 70/30 book over the window (Rs, on Rs1cr, 5x, {len(P)} days):")
tc = sum(tot_cost.values())
for k, v in tot_cost.items(): print(f"   {k:<10} Rs{v:>12,.0f}  ({v/tc*100:>4.1f}% of cost)")
print(f"   {'TOTAL':<10} Rs{tc:>12,.0f}  = {tc/CAP*100:.1f}% of capital over the window (~{tc/CAP*100/len(P):.2f}%/day)")
print(f"\ndays: {len(P)}   |  CSV -> {csv}")
