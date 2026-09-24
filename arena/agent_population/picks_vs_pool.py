"""SELECTION-EDGE test: within the good-tier candidate pool (PREMIUM-Pullback/GOLD/PREMIUM-Compression,
Falcon-ranked top-100) each day, split into the stocks the operator PICKED vs the ones they SKIPPED, and
compare actual intraday returns (09:15->EOD). Isolates selection skill from the tier edge. Leak-free, read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from tier_no_falcon import classify_tier_no_falcon
from weekly_dayofweek_fix import OPJAN
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); MDB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
WIN_TIERS={"PREMIUM-Pullback","GOLD","PREMIUM-Compression"}
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-01-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date",con); con.close()
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
trows=[]
for s,g in oh.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True)
    c=g.close.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); v=g.volume.values.astype(float)
    pc=np.roll(c,1); pc[0]=np.nan; c2=np.roll(c,2); c2[:2]=np.nan
    sret=(c/pc-1)*100; twoday=(c/c2-1)*100; rng=(h-l)/pc*100
    trend=pd.Series(v).rolling(3).mean().values/pd.Series(v).rolling(20).mean().values
    turn=c*v; turnpct=pd.Series(turn).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    for i in range(len(g)): trows.append((s,g.trade_date.values[i],sret[i],twoday[i],rng[i],trend[i],turnpct[i]))
TF=pd.DataFrame(trows,columns=["symbol","trade_date","sret","twoday","rng","trend3_20","turn_pct"]).set_index(["symbol","trade_date"])
o2=oh[["symbol","trade_date","high","low","close"]].copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
parts=[]
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); c=g.close.values.astype(float); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    parts.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
BASE=feat.drop(columns=WEEKLY).merge(pd.concat(parts,ignore_index=True),on=["symbol","trade_date"],how="left")
def ranked(sd):
    fd=BASE[BASE.trade_date==sd]
    if fd.empty: return []
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms))
    for p in pats:
        if int(p["mined_year"])>=yr: continue
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*(p["oos_lift"] or 0)
    cands=[{"symbol":syms[i],"nf":int(fire[i]),"score":float(score[i])} for i in range(len(syms)) if fire[i]>=10]
    cands.sort(key=lambda c:-c["score"]); rk=sorted(cands[:100],key=lambda c:-(c["score"]/max(c["nf"],1)))
    return [(r,c["symbol"]) for r,c in enumerate(rk,1)]
mcon=sqlite3.connect("file:"+MDB.replace("\\","/")+"?mode=ro",uri=True)
def simret(sym,date):
    df=pd.read_sql_query("SELECT open,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",mcon,params=(sym,f"{date} 09:15:00",f"{date} 15:29:00"))
    if len(df)<5: return None
    e=float(df.open.iloc[0]); return (float(df.close.iloc[-1])-e)/e*100 if e>0 else None
trade_days=[td for td in sorted(OPJAN) if cidx.get(td) and cidx[td]-1>=0]
rows=[]
for td in trade_days:
    sd=cal[cidx[td]-1]; picks=set(OPJAN[td])
    for rank,sym in ranked(sd):
        tf=TF.loc[(sym,sd)] if (sym,sd) in TF.index else None
        tier=classify_tier_no_falcon(tf["sret"],tf["twoday"],tf["rng"],tf["trend3_20"],tf["turn_pct"]) if tf is not None else None
        if tier not in WIN_TIERS: continue
        ret=simret(sym,td)
        if ret is None: continue
        rows.append(dict(trade_date=td,symbol=sym,falcon_rank=rank,tier=tier,
            picked=("PICKED" if sym in picks else "skipped"),intraday_ret_pct=round(ret,2),ret_5x_pct=round(ret*5,2)))
mcon.close()
R=pd.DataFrame(rows)
def stats(g):
    r=g["intraday_ret_pct"]; return pd.Series(dict(n=len(r),win_pct=round((r>0).mean()*100,1),
        avg_ret_pct=round(r.mean(),3),avg_5x_pct=round(r.mean()*5,2),median_pct=round(r.median(),2)))
S=R.groupby("picked").apply(stats).reset_index()
print(f"  Jan-2025 · within the good-tier pool (PREMIUM-Pullback/GOLD/PREMIUM-Compression, Falcon top-100)\n")
print(f"  {'group':<10}{'n':>6}{'win%':>8}{'avg ret%':>10}{'avg 5x%':>9}{'median%':>9}")
for _,x in S.iterrows():
    print(f"  {x['picked']:<10}{int(x['n']):>6}{x['win_pct']:>8}{x['avg_ret_pct']:>10}{x['avg_5x_pct']:>9}{x['median_pct']:>9}")
p=R[R.picked=="PICKED"]["intraday_ret_pct"]; s=R[R.picked=="skipped"]["intraday_ret_pct"]
edge=p.mean()-s.mean()
print(f"\n  SELECTION EDGE (picked - skipped): {edge:+.3f}%/day per stock  ({edge*5:+.2f}% at 5x)")
print(f"  picked win {(p>0).mean()*100:.0f}%  vs  skipped win {(s>0).mean()*100:.0f}%   |  picked {len(p)} stocks, skipped {len(s)}")
# daily basket compare
D=R.groupby(["trade_date","picked"])["intraday_ret_pct"].mean().unstack()
if "PICKED" in D and "skipped" in D:
    days=D.dropna()
    print(f"\n  daily basket mean — picked beat skipped on {int((days['PICKED']>days['skipped']).sum())}/{len(days)} days")
out=os.path.join(os.path.expanduser("~"),"Downloads","JAN2025_PICKS_VS_POOL.xlsx")
with pd.ExcelWriter(out) as xw:
    R.sort_values(["trade_date","picked","falcon_rank"]).to_excel(xw,sheet_name="all_candidates",index=False)
    S.to_excel(xw,sheet_name="summary",index=False)
    D.round(2).to_excel(xw,sheet_name="daily_basket")
print(f"\n  detail -> {out}")
