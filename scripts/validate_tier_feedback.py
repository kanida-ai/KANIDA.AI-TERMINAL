"""Validate the 4 review flags against the DEPLOYED classifier on all trades.

Imports the live classify_signal_tier (source of truth), classifies every
resolved historical trade, and prints the numbers needed to confirm/refute:
  1. PREMIUM per-year stability
  2. AVOID boundary: 5<sret<=7 & high-turnover  (reviewer: too broad)
  3. ENTERPRISE-Dryup real WR (reviewer: 67.8%; my export: 72.3%)
  4. GOLD population (reviewer: 0 rows; my export: 1504)
Read-only.
"""
import os, sqlite3, importlib.util
import numpy as np, pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RND  = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
OHLC = os.path.join(ROOT, "data", "db", "kanida_universe.db")

# import the LIVE deployed classifier by file path
spec = importlib.util.spec_from_file_location(
    "signal_tier", os.path.join(ROOT, "backend", "power_user", "services", "signal_tier.py"))
st = importlib.util.module_from_spec(spec); spec.loader.exec_module(st)
classify = st.classify_signal_tier

def load():
    con = sqlite3.connect(RND)
    df = pd.read_sql_query("""
        SELECT s.signal_date, s.symbol, s.avg_lift, s.net_ret_pct AS net_ret, s.exit_reason,
               x.signal_day_ret_pct AS sret, x.two_day_ret_pct AS twoday,
               x.signal_day_range_pct AS rng
        FROM falcon_signal_day_study s
        JOIN falcon_signal_day_context x
          ON s.signal_date=x.signal_date AND s.symbol=x.symbol
        WHERE s.net_ret_pct IS NOT NULL AND x.is_extension=0""", con)
    con.close()
    df["win"]=(df.net_ret>0).astype(int); df["stop"]=(df.exit_reason=="INIT_STOP").astype(int)
    df["yr"]=df.signal_date.str[:4]
    return df

def add_vol(df):
    syms=tuple(sorted(df.symbol.unique()))
    con=sqlite3.connect(OHLC)
    oh=pd.read_sql_query("SELECT symbol,trade_date,close,volume FROM ohlc_daily WHERE symbol IN (%s)"
                         % ",".join("?"*len(syms)), con, params=syms); con.close()
    oh=oh.sort_values(["symbol","trade_date"]).reset_index(drop=True)
    g=oh.groupby("symbol",group_keys=False)
    oh["avg20"]=g["volume"].transform(lambda s:s.rolling(20,min_periods=10).mean())
    oh["avg3"]=g["volume"].transform(lambda s:s.rolling(3,min_periods=2).mean())
    oh["turn"]=oh.close*oh.volume
    oh["turn_pct"]=g["turn"].transform(lambda s:s.rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True))
    oh["trend3_20"]=oh.avg3/oh.avg20
    f=oh[["symbol","trade_date","trend3_20","turn_pct"]]
    return df.merge(f,left_on=["symbol","signal_date"],right_on=["symbol","trade_date"],how="left").drop(columns="trade_date")

def wr(d):
    n=len(d); return (n, round(100*d.win.mean(),1) if n else float('nan'),
                      round(d.net_ret.mean(),2) if n else float('nan'),
                      round(100*d.stop.mean(),1) if n else float('nan'))

df=add_vol(load())
df["TIER"]=df.apply(lambda r: classify(r.sret,r.twoday,r.rng,r.avg_lift,r.trend3_20,r.turn_pct),axis=1)

print("cohort:",len(df))
print("\n=== per-tier (as the DEPLOYED classifier assigns — mutually exclusive) ===")
print(f"{'TIER':<20}{'N':>6}{'WR%':>7}{'avg%':>7}{'stop%':>7}")
order=["PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline","STANDARD","STANDARD-weak","AVOID"]
for t in order:
    n,w,a,s=wr(df[df.TIER==t]); print(f"{t:<20}{n:>6}{w:>7}{a:>7}{s:>7}")

print("\n=== FLAG 4: GOLD population (reviewer said 0) ===")
n,w,a,s=wr(df[df.TIER=="GOLD"]); print(f"  GOLD assigned: N={n} WR={w}% (turn_pct present on {df.turn_pct.notna().sum()} rows)")

print("\n=== FLAG 3: ENTERPRISE-Dryup ===")
n,w,a,s=wr(df[df.TIER=="ENTERPRISE-Dryup"]); print(f"  as-assigned (residual): N={n} WR={w}% avg={a}% stop={s}%")
stand=df[(df.sret<=2)&(df.trend3_20<0.9)]; n,w,a,s=wr(stand); print(f"  standalone sret<=2 & trend3_20<0.9: N={n} WR={w}% avg={a}%")

print("\n=== FLAG 2: AVOID boundary (5<sret<=7 with high turnover) ===")
for lo,hi in [(5,7),(7,10),(10,99)]:
    m=(df.sret>lo)&(df.sret<=hi)&(df.turn_pct>=0.75); n,w,a,s=wr(df[m])
    print(f"  {lo}<sret<={hi} & turn_pct>=0.75 : N={n} WR={w}% avg={a}% stop={s}%")
m57=(df.sret>5)&(df.sret<=7); n,w,a,s=wr(df[m57]); print(f"  (ref) all 5<sret<=7 any vol: N={n} WR={w}% avg={a}%")

print("\n=== effect of the proposed fix (sret>5 -> sret>7 in 2nd AVOID rule) ===")
moved=df[(df.sret>5)&(df.sret<=7)&(df.turn_pct>=0.75)]; n,w,a,s=wr(moved)
print(f"  trades that would move OUT of AVOID: N={n} WR={w}% avg={a}% stop={s}%  -> if WR>55, they belong in STANDARD")

print("\n=== FLAG 1: PREMIUM per-year ===")
for t in ["PREMIUM-Pullback","PREMIUM-Compression"]:
    sub=df[df.TIER==t]; yrs=" ".join(f"{y}:{wr(g)[0]}@{wr(g)[1]}%" for y,g in sub.groupby("yr"))
    print(f"  {t}: {yrs}")
