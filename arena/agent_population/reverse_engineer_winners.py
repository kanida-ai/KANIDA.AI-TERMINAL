"""Reverse-engineer the operator's winning intraday picks. Leak-free.
(A) Across ALL Falcon picks on all 21 Jan days: which SIGNAL-DAY features predict next-day intraday
    (09:15->15:29) return? (corr + top/bottom quintile spread). Includes entry_gap (known at 9:15).
(B) Operator's actual picks: their signal-day feature PROFILE vs the pool average (what did they select for?).
(C) Within operator picks: what separates the BIG movers (>+5%) from the rest.
Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
COST=0.10

OPS_PICKS={
"2026-01-01":["TARIL","OLAELEC","ANANTRAJ","HINDCOPPER","NLCINDIA","ABSLAMC","BOSCHLTD","IDBI","HONASA"],
"2026-01-02":["SJVN","HINDCOPPER","IDBI","FORCEMOT","CRAFTSMAN","BOSCHLTD","ANANTRAJ","JBMA","HONASA","NSLNISP","GRAPHITE"],
"2026-01-05":["ZYDUSWELL","GRAPHITE","FORCEMOT","MSUMI","ABSLAMC","MAHABANK"],
"2026-01-06":["IPCALAB","TATAELXSI","SIGNATURE","BHEL","IEX","PERSISTENT","GALLANTT","DMART","HINDCOPPER","CRISIL","MANAPPURAM"],
"2026-01-07":["SIGNATURE","TATAELXSI","SOLARINDS","BHEL","PERSISTENT","MANAPPURAM","INDIACEM","GALLANTT","POWERINDIA"],
"2026-01-14":["CHENNPETRO","JWL","FEDERALBNK","UNIONBANK","FORCEMOT","HFCL","MMTC","SBFC","BANKINDIA","360ONE","RBLBANK"],
"2026-01-16":["FEDERALBNK","MCX","HDFCAMC","VEDL","MANAPPURAM","APOLLOTYRE","CANFINHOME","TECHM","CANBK","NMDC","LAURUSLABS","AUBANK","ABCAPITAL","INDUSINDBK"],
"2026-01-22":["SUNTV","MINDACORP","APLAPOLLO","EMCURE","AAVAS","BANDHANBNK","RKFORGE","HOMEFIRST","ASHOKLEY"],
"2026-01-28":["SYRMA","ABDL","ABB","SPLPETRO","GVT&D","GESHIP","VTL","IDEA","CGPOWER","SIGNATURE"],
"2026-01-30":["SPLPETRO","ABDL","IDEA","VTL","DELHIVERY","GESHIP","CGPOWER","ZFCVINDIA","INTELLECT","ACMESOLAR","SYRMA"],
}
opset={(td,s) for td,ss in OPS_PICKS.items() for s in ss}

con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-02-15'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-02-15' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]; B={}
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values; cl=g.close.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(cl-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(cl>0,(hi-lo)/cl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(cl/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(cl>ph).astype(float),np.nan))))
    B[s]=dict(o=g.open.values.astype(float),c=cl,idx={d:i for i,d in enumerate(g.trade_date)})
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}

# build the analysis frame: every stock on each Jan-entry day with signal-day features + entry_gap + intraday ret
FEATS=["range_pct","close_loc","gap_pct","body_pct","dist_sma_20","dist_sma_50","dist_sma_200","slope_sma_20",
"slope_sma_50","rsi_14","roc_5","roc_20","roc_60","vol_vs_20d","vol_5d_vs_20d","atr_20_pct","atr_5_vs_20",
"dist_high_10","dist_high_20","dist_high_60","dist_high_252","rs_sector_20d","rs_sector_60d","rs_market_20d",
"rs_market_60d","weekly_close_loc","weekly_range_pct"]
rows=[]
jan=[d for d in cal if "2026-01-01"<=d<="2026-01-31"]
for td in jan:
    ci=cidx[td]
    if ci-1<0: continue
    sd=cal[ci-1]; fd=FCpit[FCpit.trade_date==sd].set_index("symbol")
    for s in fd.index:
        S=B.get(s); it=S["idx"].get(td) if S else None; isd=S["idx"].get(sd) if S else None
        if it is None or isd is None: continue
        o=S["o"][it]; sc=S["c"][isd]
        if o<=0 or sc<=0: continue
        r=(S["c"][it]/o-1)*100-COST; egap=(o/sc-1)*100
        row={"td":td,"symbol":s,"ret":r,"entry_gap":egap,"is_op":int((td,s) in opset)}
        for f in FEATS: row[f]=fd.at[s,f] if f in fd.columns else np.nan
        rows.append(row)
A=pd.DataFrame(rows)
ALLF=FEATS+["entry_gap"]

print("="*94); print(f"  REVERSE-ENGINEERING · Jan 2026 · {len(A):,} stock-days · intraday 09:15->15:29 (net {COST}%)"); print("="*94)

print("\n  (A) which SIGNAL-DAY features predict next-day intraday return? (corr, + Q5−Q1 return spread)")
res=[]
for f in ALLF:
    x=A[[f,"ret"]].dropna()
    if len(x)<200: continue
    c=x[f].corr(x.ret)
    try:
        q=pd.qcut(x[f],5,labels=False,duplicates="drop"); sp=x.ret[q==q.max()].mean()-x.ret[q==0].mean()
    except Exception: sp=np.nan
    res.append((f,c,sp))
res.sort(key=lambda z:-abs(z[1]))
print(f"  {'feature':<20}{'corr':>8}{'topQ-botQ ret%':>16}")
for f,c,sp in res[:14]:
    print(f"  {f:<20}{c:>+8.3f}{sp:>+16.2f}")

print("\n  (B) operator picks vs pool — average signal-day feature (what did you select for?):")
op=A[A.is_op==1]; pool=A[A.is_op==0]
print(f"  operator picks: n={len(op)}  avg intraday {op.ret.mean():+.2f}%   ·   pool: n={len(pool)}  avg {pool.ret.mean():+.2f}%")
print(f"  {'feature':<20}{'operator':>11}{'pool':>11}{'diff':>11}")
diffs=[]
for f in ALLF:
    o=op[f].mean(); p=pool[f].mean(); diffs.append((f,o,p,o-p))
diffs.sort(key=lambda z:-abs(z[3]) if np.isfinite(z[3]) else 0)
for f,o,p,d in diffs[:12]:
    print(f"  {f:<20}{o:>+11.2f}{p:>+11.2f}{d:>+11.2f}")

print("\n  (C) within operator picks — BIG movers (>+5%) vs the rest:")
big=op[op.ret>5]; rest=op[op.ret<=5]
print(f"  big movers n={len(big)} (avg {big.ret.mean():+.2f}%)   rest n={len(rest)} (avg {rest.ret.mean():+.2f}%)")
print(f"  {'feature':<20}{'big':>11}{'rest':>11}{'diff':>11}")
d2=[]
for f in ALLF:
    d2.append((f,big[f].mean(),rest[f].mean(),big[f].mean()-rest[f].mean()))
d2.sort(key=lambda z:-abs(z[3]) if np.isfinite(z[3]) else 0)
for f,b,r,d in d2[:10]:
    print(f"  {f:<20}{b:>+11.2f}{r:>+11.2f}{d:>+11.2f}")

# does entry_gap alone sort intraday winners? (gap-and-go test)
print("\n  (D) entry-gap test (gap known at 9:15) — intraday return by opening-gap bucket:")
A2=A.dropna(subset=["entry_gap"]).copy(); A2["gb"]=pd.cut(A2.entry_gap,[-100,-2,-0.5,0.5,2,4,100],labels=["<-2%","-2..-.5","-.5..+.5","+.5..2","+2..4",">+4%"])
print(f"  {'gap bucket':<12}{'n':>7}{'avg intraday%':>15}{'win%':>7}")
for gb,g in A2.groupby("gb"):
    print(f"  {str(gb):<12}{len(g):>7}{g.ret.mean():>+15.2f}{(g.ret>0).mean()*100:>6.0f}%")
