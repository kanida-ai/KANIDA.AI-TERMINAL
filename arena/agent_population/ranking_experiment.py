"""RANKING-FORMULA experiment. Patterns UNCHANGED, features UNCHANGED (validated leak-free baseline).
Only change how we rank within the candidate pool: avg_lift (prod) vs sum-of-best-K vs high-lift-count vs max.
Q: does rewarding a stock's BEST patterns (not its diluted average) consolidate the top-30 picks into top-15?
Read-only. Production untouched."""
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
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
parts=[]
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); c=g.close.values.astype(float); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    parts.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
BASE=feat.drop(columns=WEEKLY).merge(pd.concat(parts,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
def fired_lifts(sd):
    fd=BASE[BASE.trade_date==sd]
    if fd.empty: return None
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); lifts=[[] for _ in syms]
    for p in pats:
        if int(p["mined_year"])>=yr: continue
        m=FR.rule_mask(p["rule"],X); ol=p["oos_lift"] or 0
        for i in np.nonzero(m)[0]: lifts[i].append(ol)
    return syms,lifts
METRICS=["avg (prod)","sum","top3","top5","top10","count>25","count>30","max"]
def score(l,metric):
    a=sorted(l,reverse=True)
    if metric=="avg (prod)": return sum(l)/len(l)
    if metric=="sum": return sum(l)
    if metric=="top3": return sum(a[:3])
    if metric=="top5": return sum(a[:5])
    if metric=="top10": return sum(a[:10])
    if metric=="count>25": return sum(1 for x in l if x>25)
    if metric=="count>30": return sum(1 for x in l if x>30)
    if metric=="max": return max(l)
def rank_metric(sd, metric):
    fl=fired_lifts(sd)
    if fl is None: return {}
    syms,lifts=fl
    cands=[(syms[i],lifts[i]) for i in range(len(syms)) if len(lifts[i])>=10]
    pool=sorted(cands,key=lambda c:-sum(c[1]))[:100]      # prod candidate pool = top-100 by sum-lift
    ranked=sorted(pool,key=lambda c:-score(c[1],metric))  # re-rank pool by chosen metric
    return {s:r for r,(s,l) in enumerate(ranked,1)}
sdmap={td:(cal[cidx[td]-1] if cidx.get(td) and cidx[td]-1>=0 else None) for td in OPJAN}
allpick=[(td,s) for td in OPJAN for s in OPJAN[td]]
base={td:(rank_metric(sdmap[td],"avg (prod)") if sdmap[td] else {}) for td in OPJAN}
top30=[(td,s) for td,s in allpick if sdmap[td] and base[td].get(s) and base[td][s]<=30]
matched=[(td,s) for td,s in allpick if sdmap[td] and base[td].get(s) and base[td][s]<=15]
bucket2=[(td,s) for td,s in allpick if sdmap[td] and 16<=(base[td].get(s) or 99)<=30]
print(f"  pool & features unchanged; only the RANK metric within the top-100 pool changes")
print(f"  baseline: {len(top30)} picks in top-30 ({len(matched)} top-15 + {len(bucket2)} bucket-2)\n")
print(f"  {'rank metric':<14}{'net top15':>10}{'of top30':>10}{'bkt2->15':>10}{'kept57':>9}")
for m in METRICS:
    rk={td:(rank_metric(sdmap[td],m) if sdmap[td] else {}) for td in OPJAN}
    net=sum(1 for td,s in allpick if sdmap[td] and rk[td].get(s) and rk[td][s]<=15)
    of30=sum(1 for td,s in top30 if rk[td].get(s) and rk[td][s]<=15)
    prom=sum(1 for td,s in bucket2 if rk[td].get(s) and rk[td][s]<=15)
    kept=sum(1 for td,s in matched if rk[td].get(s) and rk[td][s]<=15)
    tag=" <= prod" if m=="avg (prod)" else (" <<< better" if net>len(matched) else "")
    print(f"  {m:<14}{net:>10}{of30:>7}/{len(top30):<2}{prom:>6}/{len(bucket2):<2}{kept:>6}/{len(matched)}{tag}")
