"""Overlay a NIFTY first-15-min proxy (mean first-15 return of large-cap heavyweights) on the Jan-2025 picks.
Separates real strength (stock beats market in first 15) from market beta. Cross-tabs full-day outcome by
stock-green x market-green, and by relative strength. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP)
from weekly_dayofweek_fix import OPJAN
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); MDB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
HEAVY=["RELIANCE","HDFCBANK","ICICIBANK","INFY","TCS","ITC","LT","SBIN","AXISBANK","KOTAKBANK",
       "BHARTIARTL","HINDUNILVR","BAJFINANCE","HCLTECH","MARUTI","SUNPHARMA","TITAN","NTPC","POWERGRID","TATAMOTORS"]
mcon=sqlite3.connect("file:"+MDB.replace("\\","/")+"?mode=ro",uri=True)
present=[s for s in HEAVY if mcon.execute("SELECT 1 FROM ohlc_1min WHERE symbol=? LIMIT 1",(s,)).fetchone()]
print(f"  NIFTY proxy basket ({len(present)}/{len(HEAVY)} heavyweights in 1-min): {', '.join(present[:12])}...\n")
def first15_eod(sym,date):
    df=pd.read_sql_query("SELECT open,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",mcon,params=(sym,f"{date} 09:15:00",f"{date} 15:29:00"))
    if len(df)<16: return None
    o=float(df.open.iloc[0]); c15=float(df.close.iloc[14]); eod=float(df.close.iloc[-1])
    if o<=0: return None
    return (c15-o)/o*100,(eod-o)/o*100   # first15%, full-day%
udb=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
cal=sorted(r[0] for r in udb.execute("SELECT DISTINCT trade_date FROM ohlc_daily WHERE trade_date>='2024-12-01' AND trade_date<='2025-01-31'")); udb.close()
mkt={}
for td in [d for d in cal if d>="2025-01-01"]:
    vals=[first15_eod(s,td) for s in present]; vals=[v[0] for v in vals if v is not None]
    if vals: mkt[td]=float(np.mean(vals))
rows=[]
for td in sorted(OPJAN):
    if td not in mkt: continue
    m15=mkt[td]
    for sym in OPJAN[td]:
        r=first15_eod(sym,td)
        if r is None: continue
        s15,day=r
        rows.append(dict(trade_date=td,symbol=sym,stock_first15=round(s15,2),nifty_first15=round(m15,2),
            rel_strength=round(s15-m15,2),stock_green=s15>0,nifty_green=m15>0,beats_nifty=(s15-m15)>0,
            day_ret=round(day,2),outcome=("WIN" if day>0 else "LOSS")))
mcon.close()
R=pd.DataFrame(rows)
def wr(df): return f"{(df.day_ret>0).mean()*100:.0f}% win, {df.day_ret.mean():+.2f}% avg (n={len(df)})"
print(f"  Jan-2025 picks with NIFTY-proxy overlay ({len(R)} picks):\n")
print(f"  {'stock 1st15':<14}{'nifty 1st15':<14}{'win%':>7}{'avg day%':>10}{'n':>5}")
for sg in [True,False]:
    for ng in [True,False]:
        g=R[(R.stock_green==sg)&(R.nifty_green==ng)]
        if len(g): print(f"  {'GREEN' if sg else 'red':<14}{'GREEN' if ng else 'red':<14}{(g.day_ret>0).mean()*100:>6.0f}%{g.day_ret.mean():>9.2f}%{len(g):>5}")
print(f"\n  by RELATIVE strength (stock vs nifty in first 15 min):")
print(f"    beats nifty : {wr(R[R.beats_nifty])}")
print(f"    lags nifty  : {wr(R[~R.beats_nifty])}")
print(f"\n  vs raw stock-green alone:")
print(f"    stock GREEN : {wr(R[R.stock_green])}")
print(f"    stock red   : {wr(R[~R.stock_green])}")
out=os.path.join(os.path.expanduser("~"),"Downloads","JAN2025_PICKS_NIFTY_OVERLAY.xlsx")
R.sort_values(["trade_date","symbol"]).to_excel(out,index=False)
print(f"\n  detail -> {out}")
