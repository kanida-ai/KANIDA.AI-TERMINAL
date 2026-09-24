"""MONEY test on the FALCON-FREE PREMIUM tier. Tier the whole universe each signal day (pure price/volume,
no Falcon), take PREMIUM (Pullback / Compression), trade the basket intraday next day 09:15->EOD. Compare to
operator picks + broader high-tier buckets. Does the PREMIUM filter alone carry edge? Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP)
from tier_no_falcon import classify_tier_no_falcon
from weekly_dayofweek_fix import OPJAN
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); MDB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-01-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date",con); con.close()
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
# tier every (symbol, signal_date) with the Falcon-free classifier
rows=[]
for s,g in oh.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True)
    c=g.close.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); v=g.volume.values.astype(float)
    pc=np.roll(c,1); pc[0]=np.nan; c2=np.roll(c,2); c2[:2]=np.nan
    sret=(c/pc-1)*100; twoday=(c/c2-1)*100; rng=(h-l)/pc*100
    v20=pd.Series(v).rolling(20).mean().values; v3=pd.Series(v).rolling(3).mean().values; trend=v3/v20
    turn=c*v; ts=pd.Series(turn); turnpct=ts.rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    for i in range(len(g)):
        rows.append((s,g.trade_date.values[i],sret[i],twoday[i],rng[i],trend[i],turnpct[i]))
F=pd.DataFrame(rows,columns=["symbol","trade_date","sret","twoday","rng","trend3_20","turn_pct"])
F["tier"]=[classify_tier_no_falcon(a,b,c,d,e) for a,b,c,d,e in F[["sret","twoday","rng","trend3_20","turn_pct"]].itertuples(index=False)]
tiermap={}   # signal_date -> {tier: [symbols]}
for sd,g in F.groupby("trade_date"):
    d={}
    for t,gg in g.groupby("tier"): d[t]=list(gg.symbol.values)
    tiermap[sd]=d
mcon=sqlite3.connect("file:"+MDB.replace("\\","/")+"?mode=ro",uri=True)
def simret(sym,date):
    df=pd.read_sql_query("SELECT open,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",mcon,params=(sym,f"{date} 09:15:00",f"{date} 15:29:00"))
    if len(df)<5: return None
    e=float(df.open.iloc[0]); return (float(df.close.iloc[-1])-e)/e if e>0 else None
def basket_pnl(daymap):
    daily=[]; sizes=[]
    for td in sorted(daymap):
        syms=daymap[td];
        if not syms: continue
        rs=[r for r in (simret(s,td) for s in syms) if r is not None]
        if rs: daily.append(np.mean(rs)); sizes.append(len(rs))
    r=np.array(daily)
    if not len(r): return None
    return dict(mean=r.mean()*100,win=(r>0).mean()*100,m1=(np.prod(1+r)-1)*100,m5=(np.prod(1+5*r)-1)*100,sz=np.mean(sizes))
trade_days=[td for td in sorted(OPJAN) if cidx.get(td) and cidx[td]-1>=0]
def pool(tiers,cap=None):
    dm={}
    for td in trade_days:
        sd=cal[cidx[td]-1]; syms=[]
        for t in tiers: syms+=tiermap.get(sd,{}).get(t,[])
        dm[td]=syms[:cap] if cap else syms
    return dm
print(f"  Jan-2025 · {len(trade_days)} operator days · Falcon-FREE tier baskets · intraday 09:15->EOD\n")
print(f"  {'basket':<38}{'1x mo':>9}{'5x mo':>9}{'mean/day':>10}{'win%':>7}{'avg size':>10}")
r=basket_pnl({td:OPJAN[td] for td in trade_days}); print(f"  {'YOUR actual picks (reference)':<38}{r['m1']:>8.1f}%{r['m5']:>8.1f}%{r['mean']:>9.2f}%{r['win']:>6.0f}%{r['sz']:>9.0f}")
for lab,tiers in [("PREMIUM-Pullback only",["PREMIUM-Pullback"]),("PREMIUM-Compression only",["PREMIUM-Compression"]),
                  ("PREMIUM (both)",["PREMIUM-Pullback","PREMIUM-Compression"]),
                  ("ENTERPRISE-Dryup only",["ENTERPRISE-Dryup"]),
                  ("ALL high-tier",["PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline"])]:
    r=basket_pnl(pool(tiers))
    if r: print(f"  {lab:<38}{r['m1']:>8.1f}%{r['m5']:>8.1f}%{r['mean']:>9.2f}%{r['win']:>6.0f}%{r['sz']:>9.0f}")
mcon.close()
