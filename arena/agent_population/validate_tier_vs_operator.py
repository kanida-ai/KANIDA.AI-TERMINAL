"""Reproduce the Falcon signal-tier EXACTLY (per backend signal_tier._signal_day_features + rulebook)
and validate against the operator's Jan-2026 tier labels. Features from raw OHLC on the SIGNAL day
(= prior trading session); avg_lift = score/n_fires from leak-free rebuild. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]

# operator (trade_date, symbol, tier)
OP=[("2026-01-01","TARIL","ENTERPRISE-Dryup"),("2026-01-01","OLAELEC","ENTERPRISE-Dryup"),("2026-01-01","ANANTRAJ","ENTERPRISE-Dryup"),("2026-01-01","HINDCOPPER","GOLD-baseline"),("2026-01-01","NLCINDIA","GOLD"),("2026-01-01","ABSLAMC","GOLD"),("2026-01-01","BOSCHLTD","PREMIUM-Compression"),("2026-01-01","IDBI","GOLD-baseline"),("2026-01-01","HONASA","GOLD-baseline"),
("2026-01-02","SJVN","PREMIUM-Compression"),("2026-01-02","HINDCOPPER","GOLD-baseline"),("2026-01-02","IDBI","PREMIUM-Compression"),("2026-01-02","FORCEMOT","GOLD"),("2026-01-02","CRAFTSMAN","GOLD-baseline"),("2026-01-02","BOSCHLTD","GOLD"),("2026-01-02","ANANTRAJ","ENTERPRISE-Dryup"),("2026-01-02","JBMA","GOLD"),("2026-01-02","HONASA","GOLD"),("2026-01-02","NSLNISP","GOLD"),("2026-01-02","GRAPHITE","GOLD-baseline"),
("2026-01-05","ZYDUSWELL","GOLD-baseline"),("2026-01-05","GRAPHITE","GOLD-baseline"),("2026-01-05","FORCEMOT","GOLD-baseline"),("2026-01-05","MSUMI","ENTERPRISE-Dryup"),("2026-01-05","ABSLAMC","GOLD-baseline"),("2026-01-05","MAHABANK","GOLD-baseline"),
("2026-01-06","IPCALAB","ENTERPRISE-Dryup"),("2026-01-06","TATAELXSI","ENTERPRISE-Dryup"),("2026-01-06","SIGNATURE","GOLD"),("2026-01-06","BHEL","GOLD"),("2026-01-06","IEX","ENTERPRISE-Dryup"),("2026-01-06","PERSISTENT","ENTERPRISE-Dryup"),("2026-01-06","GALLANTT","ENTERPRISE-Dryup"),("2026-01-06","DMART","GOLD"),("2026-01-06","HINDCOPPER","GOLD-baseline"),("2026-01-06","CRISIL","ENTERPRISE-Dryup"),("2026-01-06","MANAPPURAM","ENTERPRISE-Dryup"),
("2026-01-07","SIGNATURE","GOLD"),("2026-01-07","TATAELXSI","ENTERPRISE-Dryup"),("2026-01-07","SOLARINDS","GOLD"),("2026-01-07","BHEL","GOLD"),("2026-01-07","PERSISTENT","ENTERPRISE-Dryup"),("2026-01-07","MANAPPURAM","ENTERPRISE-Dryup"),("2026-01-07","INDIACEM","ENTERPRISE-Dryup"),("2026-01-07","GALLANTT","ENTERPRISE-Dryup"),("2026-01-07","POWERINDIA","ENTERPRISE-Dryup"),
("2026-01-14","CHENNPETRO","GOLD"),("2026-01-14","JWL","ENTERPRISE-Dryup"),("2026-01-14","FEDERALBNK","GOLD-baseline"),("2026-01-14","UNIONBANK","PREMIUM-Compression"),("2026-01-14","FORCEMOT","GOLD"),("2026-01-14","HFCL","ENTERPRISE-Dryup"),("2026-01-14","MMTC","ENTERPRISE-Dryup"),("2026-01-14","SBFC","ENTERPRISE-Dryup"),("2026-01-14","BANKINDIA","GOLD"),("2026-01-14","360ONE","GOLD"),("2026-01-14","RBLBANK","GOLD"),
("2026-01-16","FEDERALBNK","PREMIUM-Compression"),("2026-01-16","MCX","GOLD"),("2026-01-16","HDFCAMC","GOLD"),("2026-01-16","VEDL","GOLD"),("2026-01-16","MANAPPURAM","ENTERPRISE-Dryup"),("2026-01-16","APOLLOTYRE","GOLD"),("2026-01-16","CANFINHOME","ENTERPRISE-Dryup"),("2026-01-16","TECHM","GOLD"),("2026-01-16","CANBK","ENTERPRISE-Dryup"),("2026-01-16","NMDC","ENTERPRISE-Dryup"),("2026-01-16","LAURUSLABS","ENTERPRISE-Dryup"),("2026-01-16","AUBANK","GOLD"),("2026-01-16","ABCAPITAL","ENTERPRISE-Dryup"),("2026-01-16","INDUSINDBK","GOLD"),
("2026-01-22","SUNTV","GOLD"),("2026-01-22","MINDACORP","GOLD"),("2026-01-22","APLAPOLLO","ENTERPRISE-Dryup"),("2026-01-22","EMCURE","ENTERPRISE-Dryup"),("2026-01-22","AAVAS","ENTERPRISE-Dryup"),("2026-01-22","BANDHANBNK","GOLD-baseline"),("2026-01-22","RKFORGE","ENTERPRISE-Dryup"),("2026-01-22","HOMEFIRST","ENTERPRISE-Dryup"),("2026-01-22","ASHOKLEY","GOLD-baseline"),
("2026-01-28","SYRMA","GOLD"),("2026-01-28","ABDL","GOLD"),("2026-01-28","ABB","ENTERPRISE-Dryup"),("2026-01-28","SPLPETRO","ENTERPRISE-Dryup"),("2026-01-28","GVT&D","GOLD-baseline"),("2026-01-28","GESHIP","ENTERPRISE-Dryup"),("2026-01-28","VTL","GOLD-baseline"),("2026-01-28","IDEA","GOLD"),("2026-01-28","CGPOWER","PREMIUM-Pullback"),("2026-01-28","SIGNATURE","PREMIUM-Pullback"),
("2026-01-30","SPLPETRO","ENTERPRISE-Dryup"),("2026-01-30","ABDL","GOLD"),("2026-01-30","IDEA","GOLD-baseline"),("2026-01-30","VTL","GOLD"),("2026-01-30","DELHIVERY","GOLD"),("2026-01-30","GESHIP","GOLD"),("2026-01-30","CGPOWER","GOLD-baseline"),("2026-01-30","ZFCVINDIA","ENTERPRISE-Dryup"),("2026-01-30","INTELLECT","ENTERPRISE-Dryup"),("2026-01-30","ACMESOLAR","GOLD"),("2026-01-30","SYRMA","GOLD")]

def _ok(v): return v is not None and not (isinstance(v,float) and v!=v)
def classify(sret,twoday,rng,avg_lift,trend3_20,turn_pct):
    if _ok(sret) and sret>10: return "AVOID"
    if _ok(sret) and sret>7 and _ok(turn_pct) and turn_pct>=0.75: return "AVOID"
    if _ok(sret) and sret<=2 and _ok(twoday) and twoday<-5 and _ok(avg_lift) and avg_lift>15: return "PREMIUM-Pullback"
    if _ok(sret) and sret<=2 and _ok(rng) and rng<2 and _ok(avg_lift) and avg_lift>15: return "PREMIUM-Compression"
    if _ok(sret) and sret<=2 and _ok(trend3_20) and trend3_20<0.9: return "ENTERPRISE-Dryup"
    if _ok(sret) and sret<=2 and _ok(turn_pct) and turn_pct<0.75: return "GOLD"
    if _ok(sret) and sret<=2: return "GOLD-baseline"
    if _ok(sret) and sret<=5: return "STANDARD"
    return "STANDARD-weak"

con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-02-15'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-11-01' AND trade_date<='2026-02-15' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]; BARS={}
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values; cl=g.close.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(cl-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(cl>0,(hi-lo)/cl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(cl/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(cl>ph).astype(float),np.nan))))
    BARS[s]=dict(c=cl,h=g.high.values.astype(float),l=g.low.values.astype(float),v=g.volume.values.astype(float),idx={d:i for i,d in enumerate(g.trade_date)})
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}

def sigfeat(sym,sd):
    B=BARS.get(sym); i=B["idx"].get(sd) if B else None
    if i is None or i<1: return {}
    c=B["c"][i]; pc=B["c"][i-1]; out={}
    if pc: out["sret"]=(c/pc-1)*100; out["rng"]=(B["h"][i]-B["l"][i])/pc*100
    if i>=2 and B["c"][i-2]: out["twoday"]=(c/B["c"][i-2]-1)*100
    v=B["v"];
    if i>=10:
        w20=v[max(0,i-19):i+1]; a20=np.nanmean(w20)
        if a20>0: out["trend3_20"]=np.nanmean(v[max(0,i-2):i+1])/a20
    turns=(B["c"]*B["v"])[:i+1]
    if len(turns)>=60:
        w=turns[-252:]; out["turn_pct"]=float(np.mean(w<=turns[i]))
    return out

score_cache={}
def score_nf(sym,sd):
    if sd not in score_cache:
        fd=FCpit[FCpit.trade_date==sd]; syms=fd.symbol.values
        X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
        for j,col in enumerate(FR.FEATURE_COLS):
            if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
        yr=int(sd[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
        fire=np.zeros(len(syms),np.int32); sc=np.zeros(len(syms))
        for p in elig:
            m=FR.rule_mask(p["rule"],X)
            if not m.any(): continue
            fire+=m.astype(np.int32); sc+=m.astype(np.float64)*p["oos_lift"]
        score_cache[sd]={syms[i]:(float(sc[i]),int(fire[i])) for i in range(len(syms))}
    return score_cache[sd].get(sym,(0.0,0))

rows=[]
for td,sym,op_tier in OP:
    ci=cidx.get(td)
    if ci is None: continue
    sd=cal[ci-1]; f=sigfeat(sym,sd); sc,nf=score_nf(sym,sd)
    al=sc/nf if nf else None
    my=classify(f.get("sret"),f.get("twoday"),f.get("rng"),al,f.get("trend3_20"),f.get("turn_pct"))
    rows.append(dict(td=td,sym=sym,op=op_tier,mine=my,match=(my==op_tier),
                     sret=f.get("sret"),rng=f.get("rng"),twoday=f.get("twoday"),tr=f.get("trend3_20"),turn=f.get("turn_pct"),nf=nf,al=al))
R=pd.DataFrame(rows)
print("="*100); print(f"  TIER VALIDATION vs operator labels — {len(R)} picks"); print("="*100)
print(f"  EXACT MATCH: {R.match.sum()}/{len(R)} = {R.match.mean()*100:.0f}%")
# adjacent (same family) match
fam={"PREMIUM-Compression":"P","PREMIUM-Pullback":"P","ENTERPRISE-Dryup":"E","GOLD":"G","GOLD-baseline":"G"}
R["famok"]=R.apply(lambda x: fam.get(x.op,x.op)==fam.get(x.mine,x.mine),axis=1)
print(f"  FAMILY MATCH (GOLD~GOLD-baseline, PREMIUM~PREMIUM): {R.famok.mean()*100:.0f}%")
print("\n  mismatches:")
print(f"  {'date':<11}{'symbol':<12}{'operator':<20}{'mine':<20}{'sret':>7}{'rng':>7}{'trend3_20':>10}{'turn':>7}{'nf':>4}")
for _,x in R[~R.match].iterrows():
    print(f"  {x.td:<11}{x.sym:<12}{x.op:<20}{x.mine:<20}{(x.sret if x.sret is not None else float('nan')):>+7.2f}{(x.rng if x.rng is not None else float('nan')):>7.2f}{(x.tr if x.tr is not None else float('nan')):>10.2f}{(x.turn if x.turn is not None else float('nan')):>7.2f}{x.nf:>4}")
print("\n  confusion (operator -> mine):")
print(pd.crosstab(R.op, R.mine).to_string())
