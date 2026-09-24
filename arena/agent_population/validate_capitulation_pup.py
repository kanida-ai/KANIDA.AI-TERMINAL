"""Validate the operator's capitulation P(up) table on the FULL Nifty-500 universe (not high-tier).
Conditions (daily + 1-min): down>=2d/3d, vol>1.5x 20d-avg, red_1min>=10, red_vol_share>0.6.
P(up) = next-day open->close positive (the intraday trade). Compare to operator: 53.9% / 52.4% / 55.7% / 58.2%.
Period 2024-10..2025-05. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
from collections import defaultdict
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); ODB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
n500=set(pd.read_sql_query("SELECT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1",con).symbol)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-01-01' AND trade_date<='2025-05-31' ORDER BY symbol,trade_date",con); con.close()
oh=oh[oh.symbol.isin(n500)]
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
DAILY={}
for s,g in oh.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True)
    c=g.close.values.astype(float); o=g.open.values.astype(float); v=g.volume.values.astype(float)
    pc=np.roll(c,1);pc[0]=np.nan; down=(c<pc).astype(int); ds=np.zeros(len(c))
    for i in range(1,len(c)): ds[i]=ds[i-1]+1 if down[i] else 0
    a20=pd.Series(v).rolling(20).mean().values; volr=v/np.where(a20==0,1e-9,a20)
    DAILY[s]=dict(ds=ds,volr=volr,o=o,c=c,idx={d:i for i,d in enumerate(g.trade_date)},n=len(c),dates=list(g.trade_date))
# next-day open->close return for a signal-day index
def nextday_ret(D,k):
    if k+1>=D["n"]: return None
    e=D["o"][k+1]
    if e<=0: return None
    return (D["c"][k+1]/e-1)*100
lo,hi="2024-10-01","2025-05-31"
sigidx=[(s,k) for s in DAILY for k,d in enumerate(DAILY[s]["dates"]) if lo<=d<=hi]
print(f"universe {len(DAILY)} stocks · {len(sigidx)} stock-days in window\n")
def pup(cond):
    rets=[]
    for s,k in sigidx:
        D=DAILY[s]
        if not cond(D,k): continue
        r=nextday_ret(D,k)
        if r is not None: rets.append(r)
    rets=np.array(rets); return len(rets), (rets>0).mean()*100 if len(rets) else 0, rets.mean() if len(rets) else 0
print("  DAILY conditions (validate vs operator):")
print(f"  {'condition':<32}{'samples':>9}{'P(up)':>8}{'avgRet%':>9}   operator")
for lab,cond,opv in [
    ("down>=3d & vol>1.5x", lambda D,k: D["ds"][k]>=3 and D["volr"][k]>1.5, 53.9),
    ("down>=2d & vol>1.5x", lambda D,k: D["ds"][k]>=2 and D["volr"][k]>1.5, 52.4),
    ("down>=2d (any vol)",  lambda D,k: D["ds"][k]>=2, None),
    ("down>=3d (any vol)",  lambda D,k: D["ds"][k]>=3, None),
    ("vol>1.5x (any)",      lambda D,k: D["volr"][k]>1.5, None),
]:
    n,p,ar=pup(cond); print(f"  {lab:<32}{n:>9}{p:>7.1f}%{ar:>+9.2f}   {('op '+str(opv)+'%') if opv else ''}")
# add 1-min variants for the down>=3d & vol>1.5x subset (red_1min, red_vol_share)
oc=sqlite3.connect("file:"+ODB.replace("\\","/")+"?mode=ro",uri=True)
subset=[(s,k) for s,k in sigidx if DAILY[s]["ds"][k]>=2 and DAILY[s]["volr"][k]>1.5 and DAILY[s]["dates"][k]<="2025-05-20"]
print(f"\n  1-min red-bar conditions on down>=2d&vol>1.5x subset ({len(subset)} candidates, sampling):")
res=defaultdict(list)
import random
for s,k in subset:
    D=DAILY[s]; day=D["dates"][k]
    b=oc.execute("SELECT open,close,volume FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",(s,day+" 09:15",day+" 15:35")).fetchall()
    if len(b)<30: continue
    o=np.array([x[0] for x in b],float); c=np.array([x[1] for x in b],float); v=np.array([x[2] for x in b],float)
    red=c<o; tv=v.sum() if v.sum()>0 else 1e-9; r1m=int(red.sum()); rvs=v[red].sum()/tv
    r=nextday_ret(D,k)
    if r is None: continue
    res["all"].append((r>0))
    if r1m>=10: res["r1m>=10"].append((r>0))
    if r1m>=10 and rvs>0.6: res["r1m>=10 & rvs>0.6"].append((r>0))
oc.close()
for lab,opv in [("all",None),("r1m>=10",55.7),("r1m>=10 & rvs>0.6",58.2)]:
    a=res[lab]; print(f"  {lab:<22}{len(a):>7} samples   P(up) {np.mean(a)*100:.1f}%   {('op '+str(opv)+'%') if opv else ''}" if a else f"  {lab}: no samples")
