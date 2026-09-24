"""Verify the operator's MANUAL Jan-2026 intraday baskets and locate each stock in the Falcon ranking.
Config: buy 09:15 (=day open), sell 15:29 (=day close), same-day, high-tier basket, equal-weight.
For each (trade_date, symbol): actual intraday ret (daily open->close), production RANK + my TIER on the
signal day (prior session). Then RECREATE the systematic high-tier basket from the ranking and compare.
Leak-free rebuild (week-to-date features). Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
HIGH={"PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline"}

# operator's baskets: trade_date -> [symbols]
BASK={
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
USER_1X={"2026-01-01":1.97,"2026-01-02":5.18,"2026-01-05":0.99,"2026-01-06":1.37,"2026-01-07":3.04,
"2026-01-14":4.19,"2026-01-16":1.25,"2026-01-22":2.83,"2026-01-28":4.22,"2026-01-30":4.60}

con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-02-15'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-02-15' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]; DO={}; DCd={}
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values; cl=g.close.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(cl-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(cl>0,(hi-lo)/cl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(cl/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(cl>ph).astype(float),np.nan))))
    DO[s]=dict(o=g.open.values.astype(float),c=cl,idx={d:i for i,d in enumerate(g.trade_date)})
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}

def classify(sret,twoday,rng,avg_lift,trend3_20,turn_pct):
    if sret is None or not np.isfinite(sret): return "UNKNOWN"
    if sret>10: return "AVOID"
    if sret>7 and np.isfinite(turn_pct or np.nan) and turn_pct>=0.75: return "AVOID"
    if sret<=2 and np.isfinite(twoday or np.nan) and twoday<-5 and avg_lift and avg_lift>15: return "PREMIUM-Pullback"
    if sret<=2 and np.isfinite(rng or np.nan) and rng<2 and avg_lift and avg_lift>15: return "PREMIUM-Compression"
    if sret<=2 and np.isfinite(trend3_20 or np.nan) and trend3_20<0.9: return "ENTERPRISE-Dryup"
    if sret<=2 and np.isfinite(turn_pct or np.nan) and turn_pct<0.75: return "GOLD"
    if sret<=2: return "GOLD-baseline"
    if sret<=5: return "STANDARD"
    return "STANDARD-weak"

def rank_tier(sigday):
    fd=FCpit[FCpit.trade_date==sigday]
    if fd.empty: return {},{}
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sigday[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
    fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms))
    for p in elig:
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*p["oos_lift"]
    # tier feats
    tf={}
    for i,sym in enumerate(syms):
        S=DO.get(sym); k=S["idx"].get(sigday) if S else None
        if k is None or k<2: continue
        e=S["c"][k]; pc=S["c"][k-1]; c2=S["c"][k-2]
        tf[sym]=((e/pc-1)*100,(e/c2-1)*100)  # sret, twoday (rng/trend/turn approx via features)
    c=[{"symbol":syms[i],"score":float(score[i]),"nf":int(fire[i])} for i in range(len(syms)) if fire[i]>=10]
    c.sort(key=lambda x:-x["score"]); c=sorted(c[:150],key=lambda x:-(x["score"]/max(x["nf"],1)))
    rk={}; ti={}
    for r,x in enumerate(c,1):
        rk[x["symbol"]]=r; al=x["score"]/max(x["nf"],1)
        row=fd[fd.symbol==x["symbol"]]
        rng=float(row.range_pct.iloc[0]) if "range_pct" in row and len(row) else np.nan
        s2=tf.get(x["symbol"],(np.nan,np.nan))
        ti[x["symbol"]]=classify(s2[0],s2[1],rng,al,np.nan,np.nan)
    return rk,ti

def intraday(sym,td):
    S=DO.get(sym); i=S["idx"].get(td) if S else None
    if i is None: return None
    o=S["o"][i]; c=S["c"][i]
    return (c/o-1)*100 if o>0 else None

print("="*104)
print("  OPERATOR MANUAL JAN INTRADAY — verify vs data + locate in ranking (rank/tier on prior-day signal)")
print("="*104)
print(f"  {'date':<11}{'stocks':>7}{'user 1x%':>9}{'ACTUAL 1x%':>11}{'in-rank':>8}{'high-tier':>10}{'sys-basket%':>12}")
rows=[]
for td in sorted(BASK):
    ci=cidx.get(td);
    if ci is None: continue
    sigday=cal[ci-1]; rk,ti=rank_tier(sigday)
    rets=[]; inrk=0; ht=0; detail=[]
    for sym in BASK[td]:
        r=intraday(sym,td)
        if r is not None: rets.append(r)
        rr=rk.get(sym); tt=ti.get(sym,"—")
        if rr: inrk+=1
        if tt in HIGH: ht+=1
        detail.append((sym,rr,tt,r))
    # systematic high-tier basket from ranking (all high-tier in top-60 that signal day)
    sysnames=[s for s in rk if ti.get(s) in HIGH]
    sysrets=[intraday(s,td) for s in sysnames]; sysrets=[x for x in sysrets if x is not None]
    actual=np.mean(rets) if rets else np.nan
    sysb=np.mean(sysrets) if sysrets else np.nan
    rows.append(dict(td=td, n=len(BASK[td]), user=USER_1X.get(td), actual=actual, inrk=inrk, ht=ht, sysn=len(sysrets), sysb=sysb, detail=detail))
    print(f"  {td:<11}{len(BASK[td]):>7}{USER_1X.get(td,np.nan):>+9.2f}{actual:>+11.2f}{inrk:>5}/{len(BASK[td])}{ht:>7}/{len(BASK[td])}{sysb:>+11.2f} (n={len(sysrets)})")

ua=np.nanmean([r["actual"] for r in rows]); sb=np.nanmean([r["sysb"] for r in rows])
print("  "+"-"*102)
print(f"  operator days avg ACTUAL intraday 1x: {ua:+.2f}%/day  ·  systematic high-tier basket avg: {sb:+.2f}%/day")
print(f"  every operator day positive (actual): {sum(1 for r in rows if r['actual']>0)}/{len(rows)}  ·  systematic positive: {sum(1 for r in rows if r['sysb']>0)}/{len(rows)}")

print("\n  sample stock-level ranking placement (Jan-02 & Jan-30):")
for td in ["2026-01-02","2026-01-30"]:
    r=[x for x in rows if x["td"]==td][0]
    print(f"   {td}:")
    for sym,rr,tt,ret in r["detail"]:
        print(f"      {sym:<12} rank {str(rr) if rr else 'not-listed':<10} tier {tt:<18} intraday {ret:+.2f}%" if ret is not None else f"      {sym:<12} rank {rr} tier {tt} (no data)")
