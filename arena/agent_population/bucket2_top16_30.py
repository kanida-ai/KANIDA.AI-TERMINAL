"""BUCKET 2: operator Jan-2025 picks that sat at Falcon rank 16-30 (just missed top-15).
For each: fired patterns (rule, target, oos_lift) + the stock's key feature values + the avg_lift GAP to the
#15 stock that day (what it must close to enter top-15). This tells us what to change. Leak-free. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0,os.path.join(ROOT,"scripts")); import falcon_signal_replay as FR
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
OPJAN={
"2025-01-01":{"CHOLAHLDNG":2,"SHYAMMETL":3,"DMART":5,"SAREGAMA":6,"SAMMAANCAP":7,"AWL":9,"SUNDARMFIN":10,"POLYMED":11,"PTCIL":12,"VIJAYA":14},
"2025-01-02":{"FORCEMOT":2,"OIL":3,"DMART":4,"CHOLAHLDNG":5,"SUNDARMFIN":6,"HUDCO":7,"EICHERMOT":10,"CARTRADE":12,"IGL":14,"MAHABANK":15},
"2025-01-03":{"DMART":1,"SAREGAMA":2,"SHYAMMETL":6,"IREDA":7,"RITES":9,"ITI":11,"MAHABANK":12,"HUDCO":14},
"2025-01-06":{"TARIL":1,"FORCEMOT":2,"SUNDARMFIN":7,"AWL":10,"RITES":12,"CREDITACC":14,"HUDCO":15},
"2025-01-07":{"SRF":1,"SHYAMMETL":2,"JSWDULUX":3,"ANANDRATHI":4,"LLOYDSME":6,"VIJAYA":7,"BDL":8,"AARTIIND":9,"RKFORGE":10,"NAVINFLUOR":12,"UBL":14,"MARICO":15},
"2025-01-08":{"SRF":1,"ANANDRATHI":4,"AARTIIND":7,"CONCORDBIO":9,"RKFORGE":11,"UBL":13},
"2025-01-09":{"SHYAMMETL":1,"SRF":2,"JSWDULUX":3,"ANANDRATHI":4,"AARTIIND":5,"PTCIL":6,"VIJAYA":7,"LLOYDSME":8,"UBL":9,"RKFORGE":10,"BDL":11,"NAVINFLUOR":12,"MARICO":13},
"2025-01-10":{"SHYAMMETL":1,"LLOYDSME":5,"PTCIL":6,"VIJAYA":7,"UBL":9,"RKFORGE":10,"BDL":11,"INDIAMART":12},
"2025-01-13":{"SHYAMMETL":1,"SRF":2,"LLOYDSME":3,"JSWDULUX":5,"PTCIL":6,"VIJAYA":7,"UBL":9,"BDL":11,"CONCORDBIO":13,"INDIAMART":14},
"2025-01-14":{"IDEA":1,"RAILTEL":2,"RPOWER":3,"NBCC":4,"RVNL":5,"ADANIGREEN":6,"BDL":7,"GMDCLTD":8,"IRCON":9,"BLS":10,"INOXWIND":11,"MINDACORP":12,"SWANCORP":13,"ADANIENSOL":14,"HUDCO":15},
"2025-01-16":{"RAILTEL":1,"GMDCLTD":2,"ADANIENSOL":3,"RPOWER":5,"JWL":6,"RVNL":7,"HUDCO":8,"BDL":9,"IDBI":11,"SWANCORP":12,"SAMMAANCAP":14},
"2025-01-17":{"INOXWIND":1,"NBCC":3},
"2025-01-20":{"HFCL":1,"INOXWIND":2,"SWANCORP":3,"PAYTM":6,"NIACL":9,"JWL":10,"MINDACORP":11,"ADANIGREEN":12,"RAILTEL":13,"BSE":15},
"2025-01-21":{"CGCL":2,"ZENSARTECH":3,"COFORGE":4,"FIVESTAR":6,"ULTRACEMCO":7,"MPHASIS":8,"WOCKPHARMA":9,"POLICYBZR":10,"MCX":12,"KFINTECH":13,"KEI":14,"NAVA":15},
"2025-01-22":{"PERSISTENT":1,"CGCL":2,"COFORGE":3,"ZENSARTECH":4,"AMBER":5,"FIVESTAR":6,"WOCKPHARMA":7,"MPHASIS":8,"POLICYBZR":9,"ULTRACEMCO":10,"LAURUSLABS":11,"KEI":12,"NAVA":13,"KFINTECH":14,"MCX":15},
"2025-01-23":{"CGCL":1,"COFORGE":2,"PERSISTENT":3,"ZENSARTECH":4,"AMBER":5,"WOCKPHARMA":6,"MPHASIS":8,"POLICYBZR":9,"ULTRACEMCO":10,"LAURUSLABS":11,"KFINTECH":12,"NAVA":15},
"2025-01-24":{"CGCL":1,"FIVESTAR":4},
"2025-01-27":{"PERSISTENT":2,"ZENSARTECH":4,"COFORGE":5,"WOCKPHARMA":6,"POLICYBZR":7,"ULTRACEMCO":9,"MCX":12,"NAVA":13,"BBTC":15},
"2025-01-28":{"POLYMED":1,"ZENTEC":2,"GRAVITA":3,"KPITTECH":4,"PCBL":5,"JMFINANCIL":6,"JUBLFOOD":7,"PGEL":8,"TRENT":9,"KALYANKJIL":10,"BRIGADE":11,"OLECTRA":12,"NETWEB":13,"GODFRYPHLP":14,"SYRMA":15},
"2025-01-29":{"ZENTEC":1,"PGEL":2,"JMFINANCIL":3,"KPITTECH":5,"SYRMA":6,"GODFRYPHLP":7,"GRAVITA":9,"NETWEB":10,"NEWGEN":12,"TRENT":13,"KALYANKJIL":14,"JUBLPHARMA":15},
"2025-01-30":{"POLYMED":6,"JMFINANCIL":9},
"2025-01-31":{"PCBL":2,"GRAVITA":3,"ZENTEC":4,"PGEL":5,"POLYMED":7,"TRENT":8,"JUBLFOOD":9,"NEWGEN":10,"JMFINANCIL":11,"KALYANKJIL":12,"PNBHOUSING":14,"GODFRYPHLP":15},
}
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values; cl=g.close.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(cl-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(cl>0,(hi-lo)/cl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(cl/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(cl>ph).astype(float),np.nan))))
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
def rulestr(rule): return " & ".join(f"{f}{op}{round(th,2)}" for f,op,th in rule)
def rank_and_fire(sd):
    fd=FCpit[FCpit.trade_date==sd]
    if fd.empty: return {},{},None
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
    fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms)); fired=[[] for _ in range(len(syms))]
    for p in elig:
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*p["oos_lift"]
        for i in np.where(m)[0]: fired[i].append(p)
    cands=[{"symbol":syms[i],"nf":int(fire[i]),"score":float(score[i]),"i":i} for i in range(len(syms)) if fire[i]>=10]
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:100],key=lambda c:-(c["score"]/max(c["nf"],1)))
    rk={};
    for r,c in enumerate(ranked,1): rk[c["symbol"]]=(r,c["nf"],round(c["score"]/max(c["nf"],1),2),c["i"])
    cut15=round(ranked[14]["score"]/max(ranked[14]["nf"],1),2) if len(ranked)>=15 else None
    return rk, fired, cut15, fd
KEYF=["atr_20_pct","weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","roc_20","roc_60","rsi_14","dist_high_20","dist_sma_200","slope_sma_50"]
rows=[]; detail=[]
for td in sorted(OPJAN):
    ci=cidx.get(td); sd=cal[ci-1] if ci and ci-1>=0 else None
    if not sd: continue
    rk,fired,cut15,fd=rank_and_fire(sd)
    fdi=fd.set_index("symbol")
    for sym,mr in OPJAN[td].items():
        if sym not in rk: continue
        r,nf,al,i=rk[sym]
        if not (16<=r<=30): continue
        gap=round(cut15-al,2) if cut15 else None
        rows.append(dict(trade_date=td,signal_date=sd,symbol=sym,my_rank=mr,falcon_rank=r,avg_lift=al,n_fires=nf,
                         cut15_avglift=cut15,gap_to_top15=gap,
                         **{f: (round(float(fdi.at[sym,f]),2) if f in fdi.columns and pd.notna(fdi.at[sym,f]) else None) for f in KEYF}))
        top=sorted(fired[i],key=lambda p:-(p["oos_lift"] or 0))[:8]
        for p in top:
            detail.append(dict(signal_date=sd,symbol=sym,pattern=f"FALCPAT_{p['pattern_id']}",target=p["target"],oos_lift=round(p["oos_lift"] or 0,1),rule=rulestr(p["rule"])))
B=pd.DataFrame(rows); DET=pd.DataFrame(detail)
print(f"  BUCKET 2 (Falcon rank 16-30): {len(B)} operator picks\n")
print(B[["signal_date","symbol","my_rank","falcon_rank","avg_lift","cut15_avglift","gap_to_top15","n_fires","atr_20_pct","weekly_close_loc","weekly_range_pct","roc_20"]].to_string(index=False))
out=os.path.join(os.path.expanduser("~"),"Downloads","FALCON_JAN2025_BUCKET2_TOP16_30.xlsx")
with pd.ExcelWriter(out,engine="openpyxl") as w:
    B.to_excel(w,"features_and_gap",index=False); DET.to_excel(w,"fired_patterns",index=False)
print(f"\n  avg gap_to_top15 (avg_lift shortfall): {B.gap_to_top15.mean():.2f}  ·  median {B.gap_to_top15.median():.2f}")
print(f"  saved (features+gap sheet, fired-patterns sheet) -> {out}")
