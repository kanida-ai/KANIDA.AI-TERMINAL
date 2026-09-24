"""Step 0 audit for the market-impact/capacity simulation. Read-only."""
import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
START, END = "2024-05-14", "2026-06-15"
con = sqlite3.connect(str(DB))

# 1) volume present in ohlc_1min?
ntot, nnull, nzero = con.execute(
    "SELECT COUNT(*), SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END), "
    "SUM(CASE WHEN volume=0 THEN 1 ELSE 0 END) FROM ohlc_1min "
    "WHERE bar_time BETWEEN ? AND ?", (START, END + " 23:59:59")).fetchone()
print(f"ohlc_1min rows {ntot:,}  null-vol {nnull}  zero-vol {nzero} "
      f"({(nzero or 0)/ntot*100:.2f}% zero)")

# 2) Top-5 symbols + appearance counts (persona falcon_top10_daily, rank<=5, window)
app = pd.read_sql_query(
    """SELECT symbol, COUNT(*) appearances FROM falcon_signal_day_study
       WHERE persona='falcon_top10_daily' AND engine_rank<=5
         AND entry_date BETWEEN ? AND ? GROUP BY symbol""",
    con, params=(START, END))
syms = tuple(app["symbol"].tolist())
print(f"\nTop-5 distinct symbols across window: {len(syms)}  "
      f"(total appearances {app['appearances'].sum()})")

# alias ZOMATO->ETERNAL for volume lookups
def alias(s): return "ETERNAL" if s == "ZOMATO" else s
ohlc_syms = tuple(sorted(set(alias(s) for s in syms)))

# 3) ADV (₹) from ohlc_daily over window
ph = ",".join("?" * len(ohlc_syms))
adv = pd.read_sql_query(
    f"SELECT symbol, AVG(close*volume) adv_rs FROM ohlc_daily "
    f"WHERE trade_date BETWEEN ? AND ? AND symbol IN ({ph}) GROUP BY symbol",
    con, params=(START, END, *ohlc_syms))
adv_map = dict(zip(adv["symbol"], adv["adv_rs"]))

# 4) 9:15 candle avg ₹volume from ohlc_1min
c915 = pd.read_sql_query(
    f"SELECT symbol, AVG(close*volume) v915 FROM ohlc_1min "
    f"WHERE substr(bar_time,12,8)='09:15:00' AND bar_time BETWEEN ? AND ? "
    f"AND symbol IN ({ph}) GROUP BY symbol",
    con, params=(START, END + " 23:59:59", *ohlc_syms))
v915_map = dict(zip(c915["symbol"], c915["v915"]))
con.close()


def bucket(adv_cr):
    if adv_cr >= 500: return "Large-cap"
    if adv_cr >= 100: return "Mid-large"
    if adv_cr >= 50:  return "Mid-cap"
    return "Small-cap"


rows = []
for r in app.itertuples(index=False):
    a = adv_map.get(alias(r.symbol))
    v = v915_map.get(alias(r.symbol))
    adv_cr = (a or 0) / 1e7
    rows.append({"symbol": r.symbol, "appearances": r.appearances,
                 "adv_cr": round(adv_cr, 1),
                 "v915_cr": round((v or 0) / 1e7, 3),
                 "bucket": bucket(adv_cr) if a else "NO_DATA"})
df = pd.DataFrame(rows).sort_values("appearances", ascending=False)
df.to_csv(ROOT / "outputs" / "_top5_liquidity_audit.csv", index=False)

print("\n=== Liquidity bucket distribution (by # distinct stocks) ===")
print(df["bucket"].value_counts().to_string())
print("\n=== Weighted by Top-5 APPEARANCES (what the signal actually trades) ===")
w = df.groupby("bucket")["appearances"].sum().sort_values(ascending=False)
tot = w.sum()
for b, n in w.items():
    print(f"  {b:12} {n:6}  ({n/tot*100:.1f}%)")
print(f"\nADV ₹Cr across picks: median {df['adv_cr'].median():.0f}, "
      f"min {df['adv_cr'].min():.0f}, max {df['adv_cr'].max():.0f}")
print(f"9:15 candle ₹Cr: median {df['v915_cr'].median():.2f} "
      f"(= median {df['v915_cr'].median()/df['adv_cr'].median()*100:.2f}% of ADV)")
print("\nTop 15 most-traded picks:")
print(df.head(15).to_string(index=False))
print("\nLeast liquid 8 picks:")
print(df.sort_values("adv_cr").head(8).to_string(index=False))
print(f"\nsymbols with NO volume data: {(df['bucket']=='NO_DATA').sum()}")
