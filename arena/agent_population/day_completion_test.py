"""THURSDAY/FRIDAY 'signal completes' test. Patterns were MINED on the FULL completed weekly bar (Mon-Fri).
At live signal time (mid-week) only a PARTIAL week-to-date value exists -> weekly features read low -> picks
underscore. Test: rank each operator pick with PARTIAL (week-to-date, live) vs COMPLETE (full-week) weekly
features. Does completing the week promote the winners into top-15? Tabulate by signal weekday. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from weekly_dayofweek_fix import OPJAN
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
WINNERS={("2025-01-24","FIVESTAR"),("2025-01-31","PNBHOUSING"),("2025-01-23","POLICYBZR")}
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
partial=[]; full=[]
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); c=g.close.values.astype(float)
    # PARTIAL: week-to-date (cumulative within week) — live/clean
    phi=g.groupby("wk").high.cummax().values; plo=g.groupby("wk").low.cummin().values
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    partial.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(phi>plo,(c-plo)/(phi-plo),np.nan),weekly_range_pct=np.where(c>0,(phi-plo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
    # FULL: whole calendar week (Mon-Fri) high/low + Friday close — the COMPLETED bar patterns were mined on
    fhi=g.groupby("wk").high.transform("max").values; flo=g.groupby("wk").low.transform("min").values
    fcl=g.groupby("wk").close.transform("last").values  # Friday close
    full.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(fhi>flo,(fcl-flo)/(fhi-flo),np.nan),weekly_range_pct=np.where(fcl>0,(fhi-flo)/fcl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(fcl/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(fcl>ph).astype(float),np.nan))))
BP=feat.drop(columns=WEEKLY).merge(pd.concat(partial,ignore_index=True),on=["symbol","trade_date"],how="left")
BF=feat.drop(columns=WEEKLY).merge(pd.concat(full,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
def rank_all(sd, SRC):
    fd=SRC[SRC.trade_date==sd]
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
    return {c["symbol"]:r for r,c in enumerate(ranked,1)}
sdmap={td:(cal[cidx[td]-1] if cidx.get(td) and cidx[td]-1>=0 else None) for td in OPJAN}
allpick=[(td,s) for td in OPJAN for s in OPJAN[td] if sdmap[td]]
DOW={0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri"}
rp={td:rank_all(sdmap[td],BP) for td in OPJAN if sdmap[td]}
rf={td:rank_all(sdmap[td],BF) for td in OPJAN if sdmap[td]}
# baseline buckets by PARTIAL (live) ranking
matched=[(td,s) for td,s in allpick if rp[td].get(s) and rp[td][s]<=15]
bucket2=[(td,s) for td,s in allpick if 16<=(rp[td].get(s) or 99)<=30]
def n15(rk): return sum(1 for td,s in allpick if rk[td].get(s) and rk[td][s]<=15)
print(f"  net top-15 (all {len(allpick)} picks):  PARTIAL(live)={n15(rp)}   COMPLETE(full-week)={n15(rf)}")
prom=sum(1 for td,s in bucket2 if rf[td].get(s) and rf[td][s]<=15); kept=sum(1 for td,s in matched if rf[td].get(s) and rf[td][s]<=15)
print(f"  bucket-2 (16-30 under live) promoted to top-15 when week COMPLETES: {prom}/{len(bucket2)}   | matched kept {kept}/{len(matched)}\n")
print(f"  --- the 3 WINNERS: rank partial(live) -> complete(full-week) ---")
for td,s in sorted(WINNERS):
    sd=sdmap[td]; wd=DOW[pd.to_datetime(sd).dayofweek]
    print(f"    {s:<11} signal {sd} ({wd}):  partial rank {rp[td].get(s)}  ->  complete rank {rf[td].get(s)}")
print(f"\n  --- promotion by SIGNAL WEEKDAY (bucket-2 picks) ---")
print(f"  {'weekday':<8}{'#bucket2':>9}{'promoted by completion':>24}")
byd={}
for td,s in bucket2:
    wd=DOW[pd.to_datetime(sdmap[td]).dayofweek]; byd.setdefault(wd,[0,0]); byd[wd][0]+=1
    if rf[td].get(s) and rf[td][s]<=15: byd[wd][1]+=1
for wd in ["Mon","Tue","Wed","Thu","Fri"]:
    if wd in byd: print(f"  {wd:<8}{byd[wd][0]:>9}{byd[wd][1]:>16}/{byd[wd][0]}")
