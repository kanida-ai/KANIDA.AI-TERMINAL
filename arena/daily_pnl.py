"""Daily P&L 2026-05-01 .. 2026-07-17: Falcon (Top-10 long 5x), Tail-short (201-500 short 5x), Combo (70/30).
Net = (mean per-name return - 0.15% round-trip) x 5x leverage. Sum-of-daily. Copy-friendly table + CSV."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
RDB = os.path.join(ROOT, "data", "db", "falcon_research.db")
LEV, FRIC = 5, 0.15
rc = sqlite3.connect("file:" + RDB.replace("\\", "/") + "?mode=ro", uri=True)
df = pd.read_sql_query("SELECT signal_date, entry_date, rank, nd_intraday_ret FROM falcon_full_ranking "
                       "WHERE signal_date BETWEEN '2026-05-01' AND '2026-07-17' AND nd_intraday_ret IS NOT NULL", rc)
rc.close()
rows = []
for d, g in df.groupby("signal_date"):
    fal = (g[g["rank"] <= 10].nd_intraday_ret.mean() - FRIC) * LEV
    tail = (-g[g["rank"] >= 201].nd_intraday_ret.mean() - FRIC) * LEV
    if not (fal == fal and tail == tail): continue
    combo = 0.7 * fal + 0.3 * tail
    rows.append((d, g.entry_date.iloc[0], round(fal, 2), round(tail, 2), round(combo, 2)))
P = pd.DataFrame(rows, columns=["signal_date", "trade_date", "Falcon%", "TailShort%", "Combo70_30%"])
for c in ["Falcon%", "TailShort%", "Combo70_30%"]:
    P[c.replace("%", "_cum")] = P[c].cumsum().round(1)
csv = os.path.join(ROOT, "docs", "reports", "tailshort_daily_pnl_2026-05-01_to_07-17.csv")
P.to_csv(csv, index=False)
print(f"DAILY P&L  2026-05-01 -> 2026-07-17  (5x MIS, net of 0.15% round-trip; % of capital, sum-of-daily)\n")
print(f"{'trade_date':<12}{'Falcon':>8}{'TailShort':>10}{'Combo':>8}{'|':>3}{'Fal_cum':>9}{'Tail_cum':>9}{'Combo_cum':>10}")
for _, r in P.iterrows():
    print(f"{r.trade_date:<12}{r['Falcon%']:>+8.2f}{r['TailShort%']:>+10.2f}{r['Combo70_30%']:>+8.2f}{'|':>3}{r['Falcon_cum']:>+9.1f}{r['TailShort_cum']:>+9.1f}{r['Combo70_30_cum']:>+10.1f}")
def mdd(s): c = s.cumsum(); return (c.cummax() - c).max()
print("\n" + "-" * 66)
for c in ["Falcon%", "TailShort%", "Combo70_30%"]:
    s = P[c]; print(f"{c:<14} total {s.sum():>+8.1f}%  |  best day {s.max():>+6.2f}  worst {s.min():>+6.2f}  |  maxDD {mdd(s):>5.1f}%  |  win-days {int((s>0).mean()*100)}%")
print(f"\ndays: {len(P)}  ({P.trade_date.iloc[0]} .. {P.trade_date.iloc[-1]})   |  CSV -> {csv}")
