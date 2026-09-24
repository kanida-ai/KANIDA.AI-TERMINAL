"""Concrete rupee P&L. Tiered pattern-stock prior (built <=2023), score 2024 (OOS). Each day: take up to top-15
stocks that HAVE a prior boost, ranked by prior score. Fix Rs 25,000 per position. Show real monthly rupee P&L,
trade count, and return on the working capital actually needed. D1 (intraday, capital reused daily) AND D6
(positional, capital held ~6 days). One position per stock per day. No percentage-averaging games.
"""
import os, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
CAP = 25000; TOPN = 15
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date", "next_oc"]].rename(columns={"next_oc": "d1"})
L = L[L.signal_date >= "2022-01-01"].dropna(subset=["d1"]); L["yr"] = L.signal_date.str[:4]
# 6-day return for the positional view
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE trade_date>='2021-12-01' AND trade_date<='2025-02-28' ORDER BY symbol,trade_date", con); con.close()
fw = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); c = g.close.values.astype(float); td = g.trade_date.values
    entry = np.roll(o, -1); entry[-1] = np.nan; ex = np.roll(c, -6); ex[-6:] = np.nan
    fw.append(pd.DataFrame({"symbol": s, "signal_date": td, "r6": (ex - entry) / entry * 100}))
FW = pd.concat(fw, ignore_index=True)
L = L.merge(FW, on=["symbol", "signal_date"], how="left")

# prior <=2023, tiered
P = L[L.yr <= "2023"].groupby(["pattern_id", "symbol"]).agg(n=("d1", "size"), avg=("d1", "mean")).reset_index()
def tm(r):
    if r.avg >= 10: return 0.0
    if r.avg >= 2 and r.n >= 10: return 1.0
    if 1 <= r.avg < 2 and r.n >= 20: return 0.65
    if r.avg >= 5 and r.n >= 5: return 0.35
    return 0.0
P["mult"] = P.apply(tm, axis=1); P["cs"] = np.log1p(P.n) * P.avg * P.mult
PRI = P[P.mult > 0][["pattern_id", "symbol", "cs"]]

# score 2024, one row per stock-day
T = L[L.yr == "2024"].merge(PRI, on=["pattern_id", "symbol"], how="inner")   # only boosted pattern-fires
G = T.groupby(["symbol", "signal_date"]).agg(prior=("cs", "sum"), d1=("d1", "first"), r6=("r6", "first")).reset_index()
G["mo"] = G.signal_date.str[:7]
print(f"2024 boosted stock-days: {len(G):,}  ·  distinct stocks {G.symbol.nunique()}\n")

def run(retcol, label, hold_days):
    rows = []
    for mo, gm in G.groupby("mo"):
        picks = pd.concat([d.sort_values("prior", ascending=False).head(TOPN) for _, d in gm.groupby("signal_date")])
        picks = picks.dropna(subset=[retcol])
        pnl = (picks[retcol] / 100 * CAP)
        ndays = picks.signal_date.nunique()
        rows.append(dict(month=mo, trades=len(picks), pos_per_day=round(len(picks) / max(ndays, 1), 1),
                         pnl=pnl.sum(), avg_per_trade=pnl.mean()))
    R = pd.DataFrame(rows)
    wk = TOPN * CAP * hold_days   # working capital: 15 names x 25k x (hold days concurrent)
    R["ret_on_cap%"] = R.pnl / wk * 100
    tot = R.pnl.sum()
    print(f"===== {label} — Rs {CAP:,}/stock, top-{TOPN}/day, working capital Rs {wk:,.0f} =====")
    print(f"  {'month':<9}{'trades':>7}{'pos/day':>8}{'P&L Rs':>12}{'ret on cap':>12}")
    for _, r in R.iterrows():
        print(f"  {r.month:<9}{int(r.trades):>7}{r.pos_per_day:>8}{r.pnl:>+12,.0f}{r['ret_on_cap%']:>+11.1f}%")
    print(f"  {'YEAR':<9}{int(R.trades.sum()):>7}{'':>8}{tot:>+12,.0f}{tot/wk*100:>+11.1f}%   (avg Rs/trade {R.pnl.sum()/R.trades.sum():+,.0f})\n")

run("d1", "D1 INTRADAY (enter 09:15, exit EOD; 5X-able)", 1)
run("r6", "D6 POSITIONAL (hold 6 days, 1X)", 6)
print("Note: D1 is 1X here; at 5X MIS multiply D1 P&L by 5 (and the working capital is your own 1X margin).")
