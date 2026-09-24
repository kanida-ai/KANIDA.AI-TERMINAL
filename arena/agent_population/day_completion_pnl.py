"""Does COMPLETING the week (full-week weekly features) make MONEY, not just rank? Generate top-15/20 basket
under PARTIAL (live) vs COMPLETE (full-week, look-ahead ceiling) weekly features, trade intraday on operator's
Jan-2025 days, compare P&L to operator's actual picks. Establishes whether estimating completion is worth it."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from weekly_dayofweek_fix import OPJAN
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); MDB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
partial=[]; full=[]
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); c=g.close.values.astype(float)
    phi=g.groupby("wk").high.cummax().values; plo=g.groupby("wk").low.cummin().values
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    partial.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(phi>plo,(c-plo)/(phi-plo),np.nan),weekly_range_pct=np.where(c>0,(phi-plo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
    fhi=g.groupby("wk").high.transform("max").values; flo=g.groupby("wk").low.transform("min").values; fcl=g.groupby("wk").close.transform("last").values
    full.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(fhi>flo,(fcl-flo)/(fhi-flo),np.nan),weekly_range_pct=np.where(fcl>0,(fhi-flo)/fcl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(fcl/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(fcl>ph).astype(float),np.nan))))
BP=feat.drop(columns=WEEKLY).merge(pd.concat(partial,ignore_index=True),on=["symbol","trade_date"],how="left")
BF=feat.drop(columns=WEEKLY).merge(pd.concat(full,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
def topN(sd,N,SRC):
    fd=SRC[SRC.trade_date==sd]
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
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:100],key=lambda c:-(c["score"]/max(c["nf"],1)))
    return [c["symbol"] for c in ranked[:N]]
mcon=sqlite3.connect("file:"+MDB.replace("\\","/")+"?mode=ro",uri=True)
def simret(sym,date):
    df=pd.read_sql_query("SELECT open,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",mcon,params=(sym,f"{date} 09:15:00",f"{date} 15:29:00"))
    if len(df)<5: return None
    e=float(df.open.iloc[0]); return (float(df.close.iloc[-1])-e)/e if e>0 else None
def pnl(daymap):
    daily=[np.mean([r for r in (simret(s,td) for s in daymap[td]) if r is not None]) for td in sorted(daymap) if daymap[td]]
    r=np.array([x for x in daily if x==x]); return dict(mean=r.mean()*100,win=(r>0).mean()*100,m1=(np.prod(1+r)-1)*100,m5=(np.prod(1+5*r)-1)*100)
trade_days=[td for td in sorted(OPJAN) if cidx.get(td) and cidx[td]-1>=0]
print(f"  Jan-2025 · {len(trade_days)} operator days · EOD intraday · does COMPLETING the week make money?\n")
print(f"  {'basket':<36}{'1x mo':>9}{'5x mo':>9}{'mean/day':>10}{'win%':>7}")
r=pnl({td:OPJAN[td] for td in trade_days}); print(f"  {'YOUR actual picks (reference)':<36}{r['m1']:>8.1f}%{r['m5']:>8.1f}%{r['mean']:>9.2f}%{r['win']:>6.0f}%")
for lab,N,SRC in [("top-15 PARTIAL (live baseline)",15,BP),("top-15 COMPLETE (full-week ceiling)",15,BF),("top-20 COMPLETE (full-week ceiling)",20,BF)]:
    dm={td:topN(cal[cidx[td]-1],N,SRC) for td in trade_days}
    r=pnl(dm); print(f"  {lab:<36}{r['m1']:>8.1f}%{r['m5']:>8.1f}%{r['mean']:>9.2f}%{r['win']:>6.0f}%")
mcon.close()
