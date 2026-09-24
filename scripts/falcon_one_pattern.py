"""Look at ONE Falcon pattern carefully (leak-free ledger, next-day 1-day hold):
  1. occurrences across all stocks   2. win rate + performance   3. # stocks where it CONSISTENTLY worked.
Pick the pattern via PID env var, else auto-pick a good illustrative one (high occurrences + positive edge).
"""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))
PP = pd.read_csv(os.path.join(OUT, "pattern_performance.csv"))

# pick pattern: env PID, else best avg_ret among frequently-firing ones
PID = int(os.environ.get("PID", "0"))
if PID == 0:
    cand = PP[(PP.n >= 2000)].sort_values("avg_ret", ascending=False)
    PID = int(cand.iloc[0].pattern_id)

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
row = con.execute("""SELECT c.rule_json, c.mined_year, c.outcome_target, p.classification, p.avg_oos_year_lift_pp
                     FROM falcon_pattern_candidates c JOIN falcon_promoted_patterns p ON c.pattern_id=p.pattern_id
                     WHERE c.pattern_id=?""", (PID,)).fetchone()
tax = con.execute("SELECT * FROM falcon_pattern_taxonomy WHERE pattern_id=?", (PID,)).fetchone()
taxcols = [d[1] for d in con.execute("PRAGMA table_info(falcon_pattern_taxonomy)")]
con.close()
rule = json.loads(row[0]); mined_year, target, cls, lift = row[1], row[2], row[3], row[4]
def plain(rule):
    nice = {"atr_20_pct": "volatility(ATR%)", "weekly_close_loc": "close-in-weekly-range",
            "weekly_range_pct": "weekly-range%", "weekly_close_vs_sma20": "wk-close-vs-20wk-avg",
            "roc_20": "20d-momentum", "roc_60": "60d-momentum", "roc_5": "5d-momentum", "rsi_14": "RSI",
            "dist_high_20": "%-below-20d-high", "dist_high_60": "%-below-60d-high", "dist_high_252": "%-below-52w-high",
            "dist_sma_50": "%-vs-50d-avg", "dist_sma_200": "%-vs-200d-avg", "slope_sma_50": "50d-avg-slope",
            "rs_sector_60d": "rel-strength-vs-sector", "rs_market_60d": "rel-strength-vs-market",
            "close_loc": "close-in-day-range", "vol_5d_vs_20d": "5d-vs-20d-volume"}
    return "  AND  ".join(f"{nice.get(f,f)} {op} {round(th,2)}" for f, op, th in rule)

P = L[L.pattern_id == PID]
tot = len(P); nsym = P.symbol.nunique()
d0, d1 = P.signal_date.min(), P.signal_date.max()
win = (P.next_oc > 0).mean() * 100; avg = P.next_oc.mean()
w = P.next_oc[P.next_oc > 0]; l = P.next_oc[P.next_oc <= 0]
# per-stock
ps = P.groupby("symbol").agg(n=("next_oc", "size"), win=("next_oc", lambda x: round((x > 0).mean() * 100, 0)),
                             avg=("next_oc", lambda x: round(x.mean(), 2))).reset_index().sort_values("n", ascending=False)
consistent = ps[(ps.n >= 5) & (ps.win >= 55) & (ps.avg > 0)]
strong = ps[(ps.n >= 8) & (ps.win >= 60) & (ps.avg > 0.3)]

print("=" * 72)
print(f"PATTERN FALCPAT_{PID}   (classification: {cls},  mined {mined_year},  built for target: {target})")
print("=" * 72)
print(f"\nWHAT IT IS (the rule, plain):\n    {plain(rule)}\n")
print("1) OCCURRENCES across all stocks (2019-2024, leak-free):")
print(f"    total times it fired : {tot:,}")
print(f"    on how many stocks   : {nsym}")
print(f"    date span            : {d0}  ->  {d1}\n")
print("2) WIN RATE & PERFORMANCE (next trading day, open->close, 1-day hold):")
print(f"    win rate (next day up): {win:.1f}%")
print(f"    average return        : {avg:+.2f}%")
print(f"    average WIN            : {w.mean():+.2f}%   average LOSS: {l.mean():+.2f}%")
print(f"    worst single day       : {P.next_oc.min():.1f}%   median: {P.next_oc.median():+.2f}%\n")
print("3) STOCKS where it CONSISTENTLY worked:")
print(f"    fired >=5 times AND won >55% AND positive avg : {len(consistent)} stocks")
print(f"    STRONG (>=8 times, >60% win, >+0.3% avg)      : {len(strong)} stocks")
print(f"\n    top 15 stocks for this pattern (by #fires):")
print(f"    {'stock':<14}{'#fires':>7}{'win%':>7}{'avg%':>8}")
for _, r in ps.head(15).iterrows():
    flag = "  <= consistent" if ((r.n >= 5) and (r.win >= 55) and (r.avg > 0)) else ""
    print(f"    {r.symbol:<14}{int(r.n):>7}{int(r.win):>6}%{r.avg:>+7.2f}%{flag}")
print(f"\n    the {len(strong)} STRONGEST stocks for this pattern:")
print("    " + ", ".join(strong.sort_values("avg", ascending=False).symbol.head(20).tolist()))
