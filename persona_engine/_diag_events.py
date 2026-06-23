"""Does 'results scheduled in the next 1-3 trading days' predict being a next-day
TOP-10 mover (gainer or loser) across the F&O universe? And is there a direction tilt?
This is the test of whether the earnings event class cracks the news-driven movers."""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from persona_engine import db, universe

con = db.connect()
fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
fo_set = set(fo)

# trading calendar
cal = [r[0] for r in con.execute(
    "SELECT DISTINCT trade_date FROM ohlc_daily WHERE trade_date>='2022-01-01' ORDER BY trade_date")]
cal_idx = {d: i for i, d in enumerate(cal)}

# next-day open->close per symbol/day (the move)
op = pd.read_sql_query(
    "SELECT symbol, trade_date, oc_full, gap_pct FROM persona_open_features "
    "WHERE symbol IN (%s) AND trade_date>='2022-01-01'" % ",".join("?"*len(fo)), con, params=fo)

# earnings -> map each result_date to the trading day >= it
earn = pd.read_sql_query("SELECT symbol, result_date FROM corp_earnings_dates", con)
earn = earn[earn["symbol"].isin(fo_set)]
def to_trading(d):
    i = np.searchsorted(cal, d)
    return cal[i] if i < len(cal) else None
earn["tday"] = earn["result_date"].map(to_trading)
earn = earn.dropna(subset=["tday"])
# set of (symbol, trading-result-day)
earn_set = set(zip(earn["symbol"], earn["tday"]))
print("earnings rows (FO, mapped):", len(earn_set), "| distinct symbols:", earn["symbol"].nunique())

# For each (symbol, prediction_date T): is there an earnings on T+1 (the outcome day)?
# outcome day = next trading day after T.
op = op.sort_values(["symbol","trade_date"])
op["is_mover_gain"] = False; op["is_mover_lose"] = False
# rank within each OUTCOME day (op.trade_date IS the morning/outcome day here)
for dt, g in op.groupby("trade_date"):
    s = g.dropna(subset=["oc_full"])
    if len(s) < 20: continue
    gain = set(s.sort_values("oc_full",ascending=False).head(10).index)
    lose = set(s.sort_values("oc_full",ascending=True).head(10).index)
    op.loc[g.index, "is_mover_gain"] = g.index.isin(gain)
    op.loc[g.index, "is_mover_lose"] = g.index.isin(lose)
op["is_mover"] = op["is_mover_gain"] | op["is_mover_lose"]
# earnings ON this outcome day (op.trade_date) for this symbol
op["earn_today"] = [ (sym,dt) in earn_set for sym,dt in zip(op["symbol"], op["trade_date"]) ]

base_mover = op["is_mover"].mean()
n_earn = op["earn_today"].sum()
print(f"\nbase P(top-10 mover any side) = {base_mover*100:.1f}%  (random ~10%)")
print(f"stock-days with earnings that morning: {n_earn}")
sub = op[op["earn_today"]]
if len(sub):
    print(f"P(top-10 mover | earnings that day)      = {sub['is_mover'].mean()*100:.1f}%  "
          f"(lift {sub['is_mover'].mean()/base_mover:.1f}x)")
    print(f"  of which gainers: {sub['is_mover_gain'].mean()*100:.1f}%  losers: {sub['is_mover_lose'].mean()*100:.1f}%")
    print(f"  mean oc_full on earnings days: {sub['oc_full'].mean():+.2f}%  abs: {sub['oc_full'].abs().mean():.2f}%")
    print(f"  vs non-earnings abs move: {op[~op['earn_today']]['oc_full'].abs().mean():.2f}%")
# How much of the actual daily top-10 movers had earnings? (recall potential)
mov = op[op["is_mover"]]
print(f"\nshare of actual top-10 movers that had earnings that day: {mov['earn_today'].mean()*100:.1f}%")
con.close()
