"""ADDENDUM F follow-up — ITEM 1 (market-neutral combo, gated on PORTFOLIO CONTRIBUTION vs Falcon) +
ITEM 2 (tail-short capacity reality check). OOS 2026, read-only, Falcon untouched.
ITEM 1: Falcon-alone (Top-10 long, 5x) vs Falcon+combo, where combo = long(1-100 weighted to top) + short(201-500),
dollar-neutral 5x gross. Report combined net, combined maxDD, and the MEASURED daily-P&L correlation.
ITEM 2: ladder the tail-short at 25L/50L/1cr/5cr; report edge decay from participation, and how many of the
201-500 names are liquid enough to short at size (ADV-based)."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
RDB = os.path.join(ROOT, "data", "db", "falcon_research.db")
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
LEV, FRIC = 5, 0.15
rc = sqlite3.connect("file:" + RDB.replace("\\", "/") + "?mode=ro", uri=True)
df = pd.read_sql_query("SELECT signal_date, rank, symbol, nd_intraday_ret FROM falcon_full_ranking WHERE signal_date>='2026-01-01' AND nd_intraday_ret IS NOT NULL", rc)
rc.close()

def dd_of(daily):
    cum = daily.cumsum(); return (cum.cummax() - cum).max()

# tier weight for the long book (weighted toward the top)
def w(r): return 3.0 if r <= 10 else (2.0 if r <= 50 else 1.0)
df["lw"] = df["rank"].map(w)

# daily legs (as fraction, 1x)
long10 = df[df["rank"] <= 10].groupby("signal_date").nd_intraday_ret.mean() / 100
longwt = df[df["rank"] <= 100].groupby("signal_date").apply(lambda g: np.average(g.nd_intraday_ret, weights=g.lw)) / 100
shorttail = (-df[df["rank"] >= 201].groupby("signal_date").nd_intraday_ret.mean()) / 100
idx = sorted(set(long10.index) & set(longwt.index) & set(shorttail.index))
long10, longwt, shorttail = long10[idx], longwt[idx], shorttail[idx]
cost_d = FRIC / 100 * LEV / 20                        # amortized daily cost

falcon = long10 * LEV - cost_d                        # Falcon-alone: 5x top-10 long
combo = (longwt * (LEV / 2) + shorttail * (LEV / 2)) - cost_d   # dollar-neutral, 5x gross
corr = np.corrcoef(falcon, combo)[0, 1]
corr_short = np.corrcoef(long10, shorttail)[0, 1]     # does the SHORT leg hedge the long?

print("=" * 78 + "\nITEM 1 — MARKET-NEUTRAL COMBO vs FALCON (portfolio contribution, OOS 2026, 5x)\n" + "=" * 78)
print(f"daily-P&L CORRELATION  combo vs Falcon: {corr:+.2f}   |   tail-SHORT vs Falcon-long: {corr_short:+.2f}")
print(f"  -> {'combo DIVERSIFIES Falcon' if corr < 0.3 else 'combo is CORRELATED with Falcon (long leg overlaps) — limited hedge'}\n")
print(f"{'book':<26}{'OOS total%':>11}{'maxDD%':>9}{'return/DD':>10}")
for nm, series in [("Falcon-alone (top-10 L)", falcon), ("Combo-alone (mkt-neutral)", combo)]:
    tot = series.sum() * 100; dd = dd_of(series) * 100
    print(f"{nm:<26}{tot:>+11.0f}{dd:>9.1f}{(tot/dd if dd else 0):>10.2f}")
print(f"\n{'Falcon + combo blend':<26}{'OOS total%':>11}{'maxDD%':>9}{'return/DD':>10}{'vs Falcon-alone':>16}")
base_rdd = (falcon.sum() * 100) / (dd_of(falcon) * 100)
for wf in [1.0, 0.7, 0.5, 0.3]:
    b = wf * falcon + (1 - wf) * combo; tot = b.sum() * 100; dd = dd_of(b) * 100; rdd = tot / dd if dd else 0
    tag = "  <- Falcon-alone" if wf == 1.0 else (f"  {'BETTER' if rdd > base_rdd else 'worse'} r/DD")
    print(f"{f'{wf:.0%} Falcon / {1-wf:.0%} combo':<26}{tot:>+11.0f}{dd:>9.1f}{rdd:>10.2f}{tag:>16}")
# monthly of combo vs falcon
print("\nmonthly (5x %):  ", "  ".join(f"{m[2:]}" for m in sorted({d[:7] for d in idx})))
for nm, s in [("Falcon", falcon), ("Combo", combo)]:
    mm = (pd.Series(s.values, index=[d[:7] for d in idx]).groupby(level=0).sum() * 100)
    print(f"  {nm:<7}", "  ".join(f"{mm.get(m,0):+5.0f}" for m in sorted(mm.index)))

# ---------------- ITEM 2: tail-short capacity ----------------
print("\n" + "=" * 78 + "\nITEM 2 — TAIL-SHORT (rank 201-500) CAPACITY REALITY CHECK\n" + "=" * 78)
oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
adv = pd.read_sql_query("SELECT symbol, AVG(close*volume) adv FROM ohlc_daily WHERE trade_date>='2026-01-01' GROUP BY symbol", oc)
oc.close()
advm = dict(zip(adv.symbol, adv.adv))
tail = df[df["rank"] >= 201].copy(); tail["adv"] = tail.symbol.map(advm)
tail = tail[tail.adv.notna()]
n_per_day = tail.groupby("signal_date").size().mean()
print(f"tail names/day (avg): {n_per_day:.0f} | with ADV data: {tail.symbol.nunique()} distinct names")
print(f"tail-name liquidity (ADV = avg daily traded value): median Rs{tail.groupby('symbol').adv.first().median()/1e7:.1f}cr/day, "
      f"25th pct Rs{tail.groupby('symbol').adv.first().quantile(.25)/1e7:.1f}cr, 10th pct Rs{tail.groupby('symbol').adv.first().quantile(.10)/1e7:.2f}cr")
gross_edge = (-tail.nd_intraday_ret).mean()           # 1x gross edge/trade
print(f"\ntail-short gross edge/trade (1x): {gross_edge:+.3f}%")
print(f"{'capital':>9}{'per-name_Rs':>13}{'%names fillable':>16}{'slippage_bps':>13}{'net_edge/trade':>15}")
for cap, tag in [(2.5e6, "25L"), (5e6, "50L"), (1e7, "1cr"), (5e7, "5cr")]:
    posval = cap / n_per_day                          # equal-weight across the daily tail book
    part = posval / (0.10 * tail.adv)                 # daily participation (vs 10% of ADV cap for a patient intraday short)
    fillable = (part <= 1).mean() * 100               # names where you can build the position within 10% of ADV
    slip_bps = np.clip(part, 0, 1).mean() * 30        # ~30bps impact at full participation, linear
    net = gross_edge - FRIC - slip_bps / 100          # net after cost + slippage
    print(f"{tag:>9}{posval:>13,.0f}{fillable:>15.0f}%{slip_bps:>12.1f}{net:>+14.3f}%")
print("\n(participation modelled vs 10% of each name's ADV; 'fillable' = names where the per-name position stays under that cap)")
