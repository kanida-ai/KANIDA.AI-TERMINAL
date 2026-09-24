"""Pattern ATTRIBUTION for the matched Jan-2025 signals (where operator rank == Falcon rank, top-15).
For each (signal_date, symbol): rebuild, list which patterns fired (rule, target, oos_lift) = the patterns that
CONTRIBUTE to that signal. Then aggregate: which patterns drive these signals most often. Leak-free. Read-only."""
import os, sys, sqlite3, warnings
from collections import defaultdict
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0,os.path.join(ROOT,"scripts")); import falcon_signal_replay as FR
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
MATCHED={
"2025-01-02":["IREDA","RITES"],
"2025-01-03":["TARIL","FORCEMOT","SUNDARMFIN","AWL","RITES","CREDITACC","HUDCO"],
"2025-01-06":["LLOYDSME"],
"2025-01-07":["CONCORDBIO"],
"2025-01-08":["JSWDULUX","PTCIL","VIJAYA","LLOYDSME"],
"2025-01-09":["LLOYDSME","PTCIL","VIJAYA","BDL","INDIAMART"],
"2025-01-10":["SHYAMMETL","SRF","LLOYDSME","JSWDULUX","PTCIL","VIJAYA","UBL","BDL","CONCORDBIO","INDIAMART"],
"2025-01-15":["ADANIENSOL"],
"2025-01-16":["INOXWIND"],
"2025-01-17":["HFCL","INOXWIND","SWANCORP","PAYTM","NIACL","JWL","MINDACORP","ADANIGREEN","RAILTEL","BSE"],
"2025-01-21":["MCX"],
"2025-01-24":["PERSISTENT","ZENSARTECH","COFORGE","WOCKPHARMA","POLICYBZR","ULTRACEMCO","MCX","NAVA","BBTC"],
"2025-01-27":["PGEL","GODFRYPHLP"],
"2025-01-29":["JMFINANCIL"],
"2025-01-30":["PGEL","JMFINANCIL"],
}
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
PM={p["pattern_id"]:p for p in pats}
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
def rulestr(rule): return " & ".join(f"{f}{op}{round(th,2)}" for f,op,th in rule)
def fired_for(sd, want):
    fd=FCpit[FCpit.trade_date==sd]
    row={}
    for sym in want:
        r=fd[fd.symbol==sym]
        if r.empty: continue
        X=np.full((1,len(FR.FEATURE_COLS)),np.nan)
        for j,col in enumerate(FR.FEATURE_COLS):
            if col in r.columns: X[0,j]=pd.to_numeric(r[col].iloc[0],errors="coerce")
        yr=int(sd[:4]); hits=[]
        for p in pats:
            if int(p["mined_year"])>=yr: continue
            if FR.rule_mask(p["rule"],X)[0]: hits.append(p)
        row[sym]=sorted(hits,key=lambda p:-(p["oos_lift"] or 0))
    return row
rows=[]; freq=defaultdict(int); freqlift=defaultdict(float)
for sd in sorted(MATCHED):
    ff=fired_for(sd, MATCHED[sd])
    for sym,hits in ff.items():
        for p in hits: freq[p["pattern_id"]]+=1; freqlift[p["pattern_id"]]=p["oos_lift"]
        for p in hits[:6]:
            rows.append(dict(signal_date=sd,symbol=sym,n_fired=len(hits),pattern=f"FALCPAT_{p['pattern_id']}",
                             target=p["target"],oos_lift=round(p["oos_lift"] or 0,1),rule=rulestr(p["rule"])))
ATTR=pd.DataFrame(rows)
out=os.path.join(os.path.expanduser("~"),"Downloads","FALCON_JAN2025_PATTERN_ATTRIBUTION.xlsx")
ATTR.to_excel(out,index=False)
npicks=sum(len(v) for v in MATCHED.values())
print(f"  attributed {npicks} matched signals · {len(freq)} distinct patterns contribute\n")
print("  TOP 20 PATTERNS driving your matched signals (how many of the matched picks each fired on):")
print(f"  {'pattern':<13}{'#signals':>9}{'oos_lift':>9}   rule")
for pid,cnt in sorted(freq.items(),key=lambda kv:-kv[1])[:20]:
    print(f"  FALCPAT_{pid:<5}{cnt:>9}{freqlift[pid]:>9.1f}   {rulestr(PM[pid]['rule'])[:62]}")
print(f"\n  per-signal attribution -> {out}")
print(f"\n  sample — TARIL (2025-01-03, your #1 = Falcon #1) top fired patterns:")
t=ATTR[(ATTR.signal_date=='2025-01-03')&(ATTR.symbol=='TARIL')]
for _,x in t.iterrows(): print(f"    {x.pattern}  {x.target}  lift={x.oos_lift}  | {x.rule[:60]}")
