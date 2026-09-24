"""Daily P&L 2026-05-01 .. 2026-07-10 from 1-MIN (extends past the stale ohlc_daily which ends 06-23).
Next-day return = entry-day 09:15 open -> EOD close, per ranked name. Falcon(top-10 L 5x), Tail-short(201-500 S 5x),
Combo(70/30). Net = (per-name ret - 0.15%) x 5x. Copy table + CSV. Read-only; Falcon untouched."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
RDB = os.path.join(ROOT, "data", "db", "falcon_research.db")
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
LEV, FRIC = 5, 0.15
# 1-min intraday return (09:15 open -> EOD close) per symbol/day
oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
m = pd.read_sql_query("SELECT symbol, substr(bar_time,1,10) d, substr(bar_time,12,5) hm, open, close FROM ohlc_1min "
                      "WHERE substr(bar_time,1,10) BETWEEN '2026-05-01' AND '2026-07-10' ORDER BY symbol, bar_time", oc)
oc.close()
agg = m.groupby(["symbol", "d"]).agg(o=("open", "first"), c=("close", "last")).reset_index()
agg["ret"] = (agg.c / agg.o - 1) * 100
retmap = {(r.symbol, r.d): r.ret for r in agg.itertuples()}
cal = sorted(agg.d.unique()); nextday = {cal[i]: cal[i + 1] for i in range(len(cal) - 1)}
# ranking (signal_date -> entry next 1-min day)
rc = sqlite3.connect("file:" + RDB.replace("\\", "/") + "?mode=ro", uri=True)
rk = pd.read_sql_query("SELECT signal_date, rank, symbol FROM falcon_full_ranking WHERE signal_date BETWEEN '2026-04-30' AND '2026-07-09'", rc)
rc.close()
rows = []
for sd, g in rk.groupby("signal_date"):
    ed = nextday.get(sd)
    if not ed: continue
    g = g.assign(ret=[retmap.get((s, ed), np.nan) for s in g.symbol]).dropna(subset=["ret"])
    if g.empty: continue
    fal = (g[g["rank"] <= 10].ret.mean() - FRIC) * LEV
    tail = (-g[g["rank"] >= 201].ret.mean() - FRIC) * LEV
    if not (fal == fal and tail == tail): continue
    rows.append((ed, round(fal, 2), round(tail, 2), round(0.7 * fal + 0.3 * tail, 2)))
P = pd.DataFrame(rows, columns=["trade_date", "Falcon%", "TailShort%", "Combo70_30%"]).sort_values("trade_date")
for c in ["Falcon%", "TailShort%", "Combo70_30%"]: P[c.replace("%", "_cum")] = P[c].cumsum().round(1)
csv = os.path.join(ROOT, "docs", "reports", "daily_pnl_2026-05_to_07-10.csv"); P.to_csv(csv, index=False)
print(f"DAILY P&L (5x MIS, net 0.15% round-trip; % of capital; from 1-min 09:15->EOD)   {P.trade_date.iloc[0]} .. {P.trade_date.iloc[-1]}\n")
print(f"{'trade_date':<12}{'Falcon':>8}{'TailShort':>10}{'Combo':>8}{'  |':>3}{'Fal_cum':>9}{'Tail_cum':>9}{'Combo_cum':>10}")
for _, r in P.iterrows():
    print(f"{r.trade_date:<12}{r['Falcon%']:>+8.2f}{r['TailShort%']:>+10.2f}{r['Combo70_30%']:>+8.2f}{'  |':>3}{r['Falcon_cum']:>+9.1f}{r['TailShort_cum']:>+9.1f}{r['Combo70_30_cum']:>+10.1f}")
def mdd(s): cc = s.cumsum(); return (cc.cummax() - cc).max()
print("\n" + "-" * 68)
for c in ["Falcon%", "TailShort%", "Combo70_30%"]:
    s = P[c]; print(f"{c:<13} total {s.sum():>+8.1f}%  best {s.max():>+6.2f}  worst {s.min():>+6.2f}  maxDD {mdd(s):>5.1f}%  win-days {int((s>0).mean()*100)}%")
print(f"\ndays: {len(P)}   |  CSV -> {csv}")
print("NOTE: ends 2026-07-10 — ohlc_1min's last day; 07-11..07-17 have NO price data yet (nothing to compute).")
