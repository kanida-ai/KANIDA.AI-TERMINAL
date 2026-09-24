"""Recalibrate weekly_range_pct on a day-of-week basis (strength alpha) and find the setting that best
CONSOLIDATES the operator's top-30 picks into the top-15. Bucket focus. Leak-free. Read-only.
range_adj = range * (1 + alpha*(min(5/dow,cap)-1)) ; alpha 0=baseline .. 1=full projection. Also early-week-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from weekly_dayofweek_fix import OPJAN
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int); o2["dow"]=dt.dt.dayofweek+1
# precompute calendar week-to-date weekly features + dow, once
parts=[]
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); c=g.close.values.astype(float); h=g.high.values; l=g.low.values
    hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    parts.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,dow=g.dow.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),wrp_base=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
WK=pd.concat(parts,ignore_index=True)
BASE=feat.drop(columns=WEEKLY).merge(WK,on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}; FI={c:i for i,c in enumerate(FR.FEATURE_COLS)}
def ranks(sd, alpha, cap, earlyonly):
    fd=BASE[BASE.trade_date==sd]
    if fd.empty: return {}
    dow=pd.to_numeric(fd["dow"],errors="coerce").values; dpos=np.clip(dow,1,5)
    fac=1+alpha*(np.minimum(5.0/dpos,cap)-1)
    if earlyonly: fac=np.where(dpos<=3,fac,1.0)
    wrp=fd["wrp_base"].values*fac
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col=="weekly_range_pct": X[:,j]=wrp
        elif col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms))
    for p in pats:
        if int(p["mined_year"])>=yr: continue
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*(p["oos_lift"] or 0)
    cands=[{"symbol":syms[i],"nf":int(fire[i]),"score":float(score[i])} for i in range(len(syms)) if fire[i]>=10]
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:100],key=lambda c:-(c["score"]/max(c["nf"],1)))
    return {c["symbol"]:r for r,c in enumerate(ranked,1)}
sdmap={td:(cal[cidx[td]-1] if cidx.get(td) and cidx[td]-1>=0 else None) for td in OPJAN}
allpick=[(td,s) for td in OPJAN for s in OPJAN[td]]
base={td:(ranks(sdmap[td],0,5,False) if sdmap[td] else {}) for td in OPJAN}
top30=[(td,s) for td,s in allpick if sdmap[td] and base[td].get(s) and base[td][s]<=30]
bmatched=[(td,s) for td,s in allpick if sdmap[td] and base[td].get(s) and base[td][s]<=15]
bbucket2=[(td,s) for td,s in allpick if sdmap[td] and 16<=(base[td].get(s) or 99)<=30]
print(f"  baseline: top-30 picks {len(top30)} (top-15 {len(bmatched)} + bucket2 {len(bbucket2)})")
print(f"\n  goal: consolidate the {len(top30)} top-30 picks into top-15 (baseline in-top15 = {len(bmatched)})\n")
print(f"  {'config':<28}{'net top-15':>11}{'of top30':>10}{'bkt2->15':>10}{'kept57':>8}")
best=None
for lab,alpha,cap,eo in [("baseline (alpha=0)",0,5,False),("alpha=0.2",0.2,5,False),("alpha=0.35",0.35,5,False),
                          ("alpha=0.5",0.5,5,False),("alpha=0.5 cap2.5",0.5,2.5,False),("alpha=0.35 early-only",0.35,5,True),
                          ("alpha=0.5 early-only",0.5,5,True),("alpha=0.25 cap2 early",0.25,2.0,True)]:
    rk={td:(ranks(sdmap[td],alpha,cap,eo) if sdmap[td] else {}) for td in OPJAN}
    net15=sum(1 for td,s in allpick if sdmap[td] and rk[td].get(s) and rk[td][s]<=15)
    of30=sum(1 for td,s in top30 if rk[td].get(s) and rk[td][s]<=15)
    prom=sum(1 for td,s in bbucket2 if rk[td].get(s) and rk[td][s]<=15)
    kept=sum(1 for td,s in bmatched if rk[td].get(s) and rk[td][s]<=15)
    print(f"  {lab:<28}{net15:>11}{of30:>7}/{len(top30):<2}{prom:>6}/{len(bbucket2):<2}{kept:>5}/{len(bmatched)}")
    if best is None or of30>best[1]: best=(lab,of30,net15)
print(f"\n  best consolidation: {best[0]} -> {best[1]}/{len(top30)} of your top-30 picks in top-15 (baseline {len(bmatched)})")

# detail: under alpha=0.5, per bucket-2 pick, baseline rank -> new rank, and dow
rk50={td:(ranks(sdmap[td],0.5,5,False) if sdmap[td] else {}) for td in OPJAN}
dowmap={}
for td in OPJAN:
    sd=sdmap[td]
    if not sd: continue
    r=BASE[(BASE.trade_date==sd)]
    dowmap[td]=int(pd.to_datetime(sd).dayofweek)+1
DOWN={1:"Mon",2:"Tue",3:"Wed",4:"Thu",5:"Fri"}
print(f"\n  --- Bucket-2 picks under alpha=0.5 (signal-day = trade_date - 1) ---")
print(f"  {'trade_date':<12}{'symbol':<12}{'sig dow':<8}{'base rank':>10}{'new rank':>10}   into top15?")
prom=[]; notp=[]
for td,s in sorted(bbucket2):
    br=base[td].get(s); nr=rk50[td].get(s); dw=DOWN.get(dowmap.get(td),"?")
    flag="YES <<<" if (nr and nr<=15) else ""
    print(f"  {td:<12}{s:<12}{dw:<8}{br:>10}{(nr if nr else '--'):>10}   {flag}")
    (prom if (nr and nr<=15) else notp).append((td,s))
print(f"\n  promoted into top-15: {len(prom)}  | still 16-30 or worse: {len(notp)}")
