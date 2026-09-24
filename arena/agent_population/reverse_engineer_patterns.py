"""Reverse-engineer WHICH PATTERNS matched the operator's Jan intraday picks. Leak-free.
For every Jan trade day (buy 9:15 open, sell 15:29 close): evaluate all 684 patterns on the prior signal day,
record which fired on each stock. Then per pattern:
  op_rate   = share of operator picks where it fired
  pool_rate = share of all pool stocks where it fired
  enrichment= op_rate / pool_rate   (>1 = distinctive to operator's selection)
  intra_ret = avg NEXT-DAY intraday return of ALL stocks where it fired (does the pattern itself pay intraday?)
Surface the patterns that are BOTH in the operator's picks AND carry an intraday edge = "the working patterns".
Also tier mix. Read-only."""
import os, sys, sqlite3, warnings, json
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
COST=0.10
OPS={
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
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
PMETA={p["pattern_id"]:p for p in pats}
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-02-15'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE trade_date>='2025-06-01' AND trade_date<='2026-02-15' ORDER BY symbol,trade_date",con); con.close()
# weekly PIT
oh2=pd.read_sql_query("SELECT symbol,trade_date,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-02-15' ORDER BY symbol,trade_date","sqlite:///"+UDB.replace("\\","/")) if False else None
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
ohw=pd.read_sql_query("SELECT symbol,trade_date,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-02-15' ORDER BY symbol,trade_date",con); con.close()
o2=ohw.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values; cl=g.close.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(cl-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(cl>0,(hi-lo)/cl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(cl/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(cl>ph).astype(float),np.nan))))
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
B={}
for s,g in oh.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); B[s]=dict(o=g.open.values.astype(float),c=g.close.values.astype(float),idx={d:i for i,d in enumerate(g.trade_date)})
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}

def intraday(sym,td):
    S=B.get(sym); i=S["idx"].get(td) if S else None
    if i is None: return None
    o=S["o"][i]; return (S["c"][i]/o-1)*100-COST if o>0 else None

# accumulate per-pattern stats
from collections import defaultdict
op_fire=defaultdict(int); pool_fire=defaultdict(int); pat_ret=defaultdict(float); pat_n=defaultdict(int); pat_win=defaultdict(int)
op_pat_ret=defaultdict(float); op_pat_n=defaultdict(int)
op_n=0; pool_n=0; op_pick_patterns={}   # (td,sym)->list of fired pids
jan=[d for d in cal if "2026-01-01"<=d<="2026-01-31"]
for td in jan:
    ci=cidx[td]
    if ci-1<0: continue
    sd=cal[ci-1]; fd=FCpit[FCpit.trade_date==sd]
    if fd.empty: continue
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    intr=np.array([intraday(s,td) if intraday(s,td) is not None else np.nan for s in syms])
    yr=int(sd[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
    opset=set(OPS.get(td,[]))
    opmask=np.array([s in opset for s in syms])
    valid=~np.isnan(intr)
    pool_n+=int(valid.sum()); op_n+=int((opmask&valid).sum())
    fired_by_sym=defaultdict(list)
    for p in elig:
        m=FR.rule_mask(p["rule"],X)&valid
        if not m.any(): continue
        pid=p["pattern_id"]; idxs=np.where(m)[0]
        pool_fire[pid]+=len(idxs); pat_ret[pid]+=intr[m].sum(); pat_n[pid]+=len(idxs); pat_win[pid]+=int((intr[m]>0).sum())
        om=m&opmask
        if om.any():
            op_fire[pid]+=int(om.sum()); op_pat_ret[pid]+=intr[om].sum(); op_pat_n[pid]+=int(om.sum())
            for k in np.where(om)[0]: fired_by_sym[syms[k]].append(pid)
    for s in opset:
        if s in fired_by_sym: op_pick_patterns[(td,s)]=fired_by_sym[s]

rows=[]
for pid in pool_fire:
    orate=op_fire[pid]/op_n if op_n else 0; prate=pool_fire[pid]/pool_n if pool_n else 0
    rows.append(dict(pid=pid, target=PMETA[pid]["target"], oos_lift=PMETA[pid]["oos_lift"],
        op_hits=op_fire[pid], op_rate=orate*100, pool_rate=prate*100, enrich=(orate/prate if prate else 0),
        intra_ret=pat_ret[pid]/pat_n[pid] if pat_n[pid] else np.nan, intra_win=pat_win[pid]/pat_n[pid]*100 if pat_n[pid] else np.nan,
        op_intra=op_pat_ret[pid]/op_pat_n[pid] if op_pat_n[pid] else np.nan, n_fires=pat_n[pid]))
P=pd.DataFrame(rows)

print("="*104); print(f"  PATTERN REVERSE-ENGINEERING · Jan 2026 · operator picks n={op_n} · pool n={pool_n} · intraday net {COST}%"); print("="*104)
print(f"\n  (1) MOST COMMON patterns in your picks (op_rate = % of your picks it fired on):")
print(f"  {'pattern':<12}{'target':<16}{'inYourPicks%':>13}{'poolrate%':>10}{'enrich':>7}{'intraRet%':>10}{'intraWin%':>10}")
for _,x in P[P.op_hits>=8].sort_values("op_rate",ascending=False).head(15).iterrows():
    print(f"  FALCPAT_{x.pid:<4}{x.target:<16}{x.op_rate:>12.0f}%{x.pool_rate:>9.0f}%{x.enrich:>7.2f}{x.intra_ret:>+10.2f}{x.intra_win:>9.0f}%")

print(f"\n  (2) most DISTINCTIVE to your picks (highest enrichment, min 6 hits):")
print(f"  {'pattern':<12}{'target':<16}{'enrich':>7}{'inYourPicks%':>13}{'intraRet%':>10}{'intraWin%':>10}")
for _,x in P[P.op_hits>=6].sort_values("enrich",ascending=False).head(12).iterrows():
    print(f"  FALCPAT_{x.pid:<4}{x.target:<16}{x.enrich:>7.2f}{x.op_rate:>12.0f}%{x.intra_ret:>+10.2f}{x.intra_win:>9.0f}%")

print(f"\n  (3) THE WORKING PATTERNS — in your picks (>=6 hits) AND positive intraday edge, by intraday return:")
print(f"  {'pattern':<12}{'target':<16}{'intraRet%':>10}{'intraWin%':>10}{'inYourPicks%':>13}{'rule':<50}")
work=P[(P.op_hits>=6)&(P.intra_ret>0)].sort_values("intra_ret",ascending=False)
for _,x in work.head(15).iterrows():
    rule=" AND ".join(f"{f}{op}{th}" for f,op,th in PMETA[x.pid]["rule"])
    print(f"  FALCPAT_{x.pid:<4}{x.target:<16}{x.intra_ret:>+10.2f}{x.intra_win:>9.0f}%{x.op_rate:>12.0f}%  {rule[:48]}")

print(f"\n  (4) baseline: does the AVERAGE pattern have intraday edge?")
print(f"    all patterns avg intraday return of firings: {P.intra_ret.mean():+.2f}%  ·  pool avg intraday: {(P.intra_ret*P.n_fires).sum()/P.n_fires.sum():+.2f}%")
print(f"    patterns with POSITIVE intraday: {(P.intra_ret>0).sum()}/{len(P)}  ·  your picks tilt to enrich>1.5: {(P[P.op_hits>=6].enrich>1.5).sum()} patterns")
out=os.path.join(os.path.expanduser("~"),"Downloads","FALCON_PATTERN_REVERSE_ENGINEER_JAN.xlsx")
P.sort_values("op_rate",ascending=False).to_excel(out,index=False); print(f"\n  full pattern table -> {out}")
