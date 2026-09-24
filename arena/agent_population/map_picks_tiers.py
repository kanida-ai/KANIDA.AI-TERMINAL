"""Map ALL 213 Jan-2025 operator picks -> Falcon-FREE tier + tier features + Falcon rank + actual intraday P&L.
Writes an Excel: sheet 'picks' (one row per pick) + 'by_tier' (tier distribution, win-rate, avg return) +
'rank_x_tier' (manual rank band x tier). Leak-free. Read-only DB. Production untouched."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from tier_no_falcon import classify_tier_no_falcon, HIGH_TIERS
from weekly_dayofweek_fix import OPJAN
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); MDB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-01-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date",con); con.close()
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
# ---- Falcon-free tier features per (symbol, signal_date) ----
trows=[]
for s,g in oh.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True)
    c=g.close.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); v=g.volume.values.astype(float)
    pc=np.roll(c,1); pc[0]=np.nan; c2=np.roll(c,2); c2[:2]=np.nan
    sret=(c/pc-1)*100; twoday=(c/c2-1)*100; rng=(h-l)/pc*100
    v20=pd.Series(v).rolling(20).mean().values; trend=pd.Series(v).rolling(3).mean().values/v20
    turn=c*v; turnpct=pd.Series(turn).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    for i in range(len(g)):
        trows.append((s,g.trade_date.values[i],sret[i],twoday[i],rng[i],trend[i],turnpct[i]))
TF=pd.DataFrame(trows,columns=["symbol","trade_date","sret","twoday","rng","trend3_20","turn_pct"]).set_index(["symbol","trade_date"])
# ---- leak-free weekly + Falcon rank machinery ----
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
def rank_all(sd):
    fd=BASE[BASE.trade_date==sd]
    if fd.empty: return {}
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
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:100],key=lambda c:-(c["score"]/max(c["nf"],1)))
    return {c["symbol"]:(r,round(c["score"]/max(c["nf"],1),2),c["nf"]) for r,c in enumerate(ranked,1)}
mcon=sqlite3.connect("file:"+MDB.replace("\\","/")+"?mode=ro",uri=True)
def simret(sym,date):
    df=pd.read_sql_query("SELECT open,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",mcon,params=(sym,f"{date} 09:15:00",f"{date} 15:29:00"))
    if len(df)<5: return None
    e=float(df.open.iloc[0]); return (float(df.close.iloc[-1])-e)/e*100 if e>0 else None
DOW={0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri"}
rk_cache={}
rows=[]
for td in sorted(OPJAN):
    if not (cidx.get(td) and cidx[td]-1>=0): continue
    sd=cal[cidx[td]-1]
    if sd not in rk_cache: rk_cache[sd]=rank_all(sd)
    rk=rk_cache[sd]
    for sym,mrank in OPJAN[td].items():
        tf=TF.loc[(sym,sd)] if (sym,sd) in TF.index else None
        tier=classify_tier_no_falcon(tf["sret"],tf["twoday"],tf["rng"],tf["trend3_20"],tf["turn_pct"]) if tf is not None else None
        fr=rk.get(sym); ret=simret(sym,td)
        rows.append(dict(trade_date=td,signal_date=sd,signal_dow=DOW[pd.to_datetime(sd).dayofweek],symbol=sym,
            manual_rank=mrank,tier_no_falcon=tier,high_tier=(tier in HIGH_TIERS) if tier else None,
            falcon_rank=fr[0] if fr else None,avg_lift=fr[1] if fr else None,n_fires=fr[2] if fr else None,
            sret=round(tf["sret"],2) if tf is not None else None,twoday=round(tf["twoday"],2) if tf is not None else None,
            rng=round(tf["rng"],2) if tf is not None else None,trend3_20=round(tf["trend3_20"],2) if tf is not None else None,
            turn_pct=round(tf["turn_pct"],2) if tf is not None else None,
            intraday_ret_pct=round(ret,2) if ret is not None else None,ret_5x_pct=round(ret*5,2) if ret is not None else None,
            outcome=("WIN" if ret>0 else "LOSS") if ret is not None else None))
mcon.close()
P=pd.DataFrame(rows)
# summary by tier
def summ(g):
    r=g["intraday_ret_pct"].dropna()
    return pd.Series(dict(n_picks=len(g),win_rate_pct=round((r>0).mean()*100,1) if len(r) else None,
        avg_intraday_pct=round(r.mean(),2) if len(r) else None,avg_5x_pct=round(r.mean()*5,2) if len(r) else None,
        in_falcon_top15=int((g["falcon_rank"]<=15).sum()),in_falcon_top30=int((g["falcon_rank"]<=30).sum())))
BY=P.groupby("tier_no_falcon",dropna=False).apply(summ).reset_index().sort_values("avg_intraday_pct",ascending=False)
# manual rank band x tier
P["rank_band"]=pd.cut(P["manual_rank"],[0,5,10,15,100],labels=["1-5","6-10","11-15","16+"])
RX=pd.crosstab(P["rank_band"],P["tier_no_falcon"])
out=os.path.join(os.path.expanduser("~"),"Downloads","JAN2025_PICKS_TIER_MAP.xlsx")
with pd.ExcelWriter(out) as xw:
    P.sort_values(["trade_date","manual_rank"]).to_excel(xw,sheet_name="picks",index=False)
    BY.to_excel(xw,sheet_name="by_tier",index=False)
    RX.to_excel(xw,sheet_name="rank_x_tier")
print(f"  {len(P)} picks mapped -> {out}\n")
print("  ---- your picks by Falcon-free tier (win% + avg intraday return) ----")
print(f"  {'tier':<22}{'#picks':>7}{'win%':>7}{'avg intra%':>12}{'avg 5x%':>9}{'in F.top15':>11}")
for _,r in BY.iterrows():
    print(f"  {str(r['tier_no_falcon']):<22}{int(r['n_picks']):>7}{str(r['win_rate_pct']):>7}{str(r['avg_intraday_pct']):>12}{str(r['avg_5x_pct']):>9}{int(r['in_falcon_top15']):>11}")
print(f"\n  HIGH-tier picks: {int(P['high_tier'].sum())}/{len(P)}  ({round(P['high_tier'].mean()*100)}%)   overall win%: {round((P['intraday_ret_pct']>0).mean()*100)}")
