"""ONE JOB: find a ranking that puts the operator's traded stocks in the TOP 15.
Labels = operator's actual picks (Jan + Mar-May 2025). Leak-free features (week-to-date).
Try many ranking methods; measure recall@15/@20/@30 (share of operator picks landing in top-k each day).
Methods: production score, avg_lift, n_fires, best single feature, and a TRAINED learning-to-rank model
(predict P(operator picks it) from features+pattern stats). Train on Jan+Mar+Apr, TEST on May (OOS)."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]

PICKS_RAW = """
2026-01-02|SJVN,HINDCOPPER,IDBI,FORCEMOT,CRAFTSMAN,BOSCHLTD,ANANTRAJ,JBMA,HONASA,NSLNISP,GRAPHITE
2026-01-05|ZYDUSWELL,GRAPHITE,FORCEMOT,MSUMI,ABSLAMC,MAHABANK
2026-01-06|IPCALAB,TATAELXSI,SIGNATURE,BHEL,IEX,PERSISTENT,GALLANTT,DMART,HINDCOPPER,CRISIL,MANAPPURAM
2026-01-07|SIGNATURE,TATAELXSI,SOLARINDS,BHEL,PERSISTENT,MANAPPURAM,INDIACEM,GALLANTT,POWERINDIA
2026-01-14|CHENNPETRO,JWL,FEDERALBNK,UNIONBANK,FORCEMOT,HFCL,MMTC,SBFC,BANKINDIA,360ONE,RBLBANK
2026-01-16|FEDERALBNK,MCX,HDFCAMC,VEDL,MANAPPURAM,APOLLOTYRE,CANFINHOME,TECHM,CANBK,NMDC,LAURUSLABS,AUBANK,ABCAPITAL,INDUSINDBK
2026-01-22|SUNTV,MINDACORP,APLAPOLLO,EMCURE,AAVAS,BANDHANBNK,RKFORGE,HOMEFIRST,ASHOKLEY
2026-01-28|SYRMA,ABDL,ABB,SPLPETRO,GVT&D,GESHIP,VTL,IDEA,CGPOWER,SIGNATURE
2026-01-30|SPLPETRO,ABDL,IDEA,VTL,DELHIVERY,GESHIP,CGPOWER,ZFCVINDIA,INTELLECT,ACMESOLAR,SYRMA
2025-03-04|SHYAMMETL,NAVA,DCMSHRIRAM,CAPLIPOINT,NEULANDLAB,DEEPAKFERT,VIJAYA,JYOTICNC,INDIACEM,JUBLINGREA,JINDALSAW,TARIL,WELCORP,SAREGAMA,ACUTAAS
2025-03-05|NAVA,TRITURBINE,PCBL,LLOYDSME,DATAPATTNS
2025-03-06|JYOTICNC,DATAPATTNS
2025-03-07|PARADEEP,ANANTRAJ,NEULANDLAB,DATAPATTNS,HSCL,TITAGARH,SAREGAMA,PCBL,RPOWER
2025-03-11|GODFRYPHLP,KIMS,CHENNPETRO,ADANIGREEN,JYOTICNC,GRAPHITE,TATACOMM,MRPL,KALYANKJIL,MCX,TIINDIA,ZFCVINDIA
2025-03-18|BSE,KFINTECH,MAXHEALTH,JPPOWER,TEJASNET,NIACL,RPOWER,SYRMA,RHIM,JWL,UTIAMC,DCMSHRIRAM
2025-03-21|JMFINANCIL,TEJASNET,PCBL,RAILTEL,RPOWER,BSE,ELECON,NIACL,GICRE,MANAPPURAM,JWL,NETWEB
2025-03-25|BSE,ASTERDM,RPOWER,ZFCVINDIA,CUB,NAVA,ADANIENSOL,HONAUT,JSWDULUX,HEG,ERIS
2025-03-27|BSE,ASTERDM,BEML,CUB,FORTIS,JSWDULUX,ERIS,ADANIENSOL,TARIL,GODFRYPHLP
2025-03-28|CUB,NAVA,GALLANTT,PTCIL,ZFCVINDIA,HONAUT,HEG
2025-04-02|VTL,BDL,PNBHOUSING,TATACONSUM,LINDEINDIA,SBFC,CREDITACC
2025-04-03|VTL,PARADEEP,LINDEINDIA,TRIDENT,WHIRLPOOL,GMRAIRPORT,TEJASNET,CREDITACC,CUB
2025-04-07|GPIL,VTL,PARADEEP,BDL,PNBHOUSING,GALLANTT,TRIDENT,TATACONSUM,WHIRLPOOL,TEJASNET,LINDEINDIA,GMRAIRPORT,ADANIPOWER,LEMONTREE,SBFC
2025-04-16|KFINTECH,JBMA,DOMS,NBCC
2025-04-17|DELHIVERY,FACT,BLUEJET
2025-04-22|APARINDS,GODFRYPHLP,LALPATHLAB,PERSISTENT,SPLPETRO
2025-04-23|AUBANK,ATUL,REDINGTON,GODFRYPHLP,SPLPETRO,CHOICEIN,LALPATHLAB,PERSISTENT,MPHASIS,HCLTECH,COFORGE,TECHM
2025-04-29|TIMKEN,FORCEMOT,CEATLTD,INDIACEM,MARUTI,HONASA,ATUL
2025-05-07|WELSPUNLIV,YESBANK,KPRMILL,ENDURANCE,CONCORDBIO,APARINDS,IIFL,SPLPETRO,TIMKEN,CEMPRO,TMPV,ZEEL,GVT&D,KAJARIACER,CRAFTSMAN
2025-05-09|YESBANK,SPLPETRO,ENDURANCE,KAJARIACER,IIFL,CRAFTSMAN,ELECON,RRKABEL,UNIONBANK,TMPV
2025-05-14|IRCON,PTCIL,RVNL,TITAGARH,HBLENGINE,IFCI,BAJAJHLDNG,SCI,IRFC,RITES,GESHIP
2025-05-16|IFCI,TITAGARH,JWL,GRANULES,RVNL,IRFC,SCI,GESHIP,RITES
2025-05-20|SOLARINDS,TTML,JKTYRE,JUBLPHARMA,RPOWER,GRSE,PFIZER,ERIS,CLEAN,PTCIL,IFCI,SCI
2025-05-21|TTML,RPOWER,HONASA,SOLARINDS,JKTYRE,LTFOODS,GRSE,SCI,ERIS,CLEAN,PARADEEP,PTCIL,KIMS,BDL
2025-05-23|TBOTEK,HONASA,RPOWER,ERIS,PARADEEP,LTFOODS,IFCI,JKTYRE
2025-05-27|CONCORDBIO,ITI,MMTC,POWERINDIA,WELCORP,ZENTEC,ENGINERSIN,TEGA,ELGIEQUIP,GLAXO
2025-05-30|CONCORDBIO,SUZLON,TEGA,BEML,BAYERCROP,NBCC
"""
PICKS={}
for line in PICKS_RAW.strip().splitlines():
    d,ss=line.split("|"); PICKS[d]=ss.split(",")

con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2026-01-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2026-01-31' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]; TF={}
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values
    c=g.close.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); v=g.volume.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
    pc=np.roll(c,1);pc[0]=np.nan;c2=np.roll(c,2);c2[:2]=np.nan
    sret=(c/pc-1)*100; rng=(h-l)/pc*100; twoday=(c/c2-1)*100
    a20=pd.Series(v).rolling(20).mean().values; a3=pd.Series(v).rolling(3).mean().values; tr=np.where(a20>0,a3/a20,np.nan)
    turn=pd.Series(c*v).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    for i,d in enumerate(g.trade_date.values): TF[(s,d)]=(sret[i],twoday[i],rng[i],tr[i],turn[i])
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}

FEATC=[c for c in FR.FEATURE_COLS]
def build_day(sd, td, picks):
    fd=FCpit[FCpit.trade_date==sd].copy()
    if fd.empty: return None
    syms=fd.symbol.values; X=np.full((len(syms),len(FEATC)),np.nan)
    for j,col in enumerate(FEATC):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
    fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms)); maxl=np.zeros(len(syms))
    for p in elig:
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        L=float(p["oos_lift"] or 0); fire+=m.astype(np.int32); score+=m.astype(np.float64)*L; maxl=np.where(m&(L>maxl),L,maxl)
    d=fd[["symbol"]+FEATC].copy(); d["score"]=score; d["nf"]=fire; d["maxl"]=maxl; d["avgl"]=score/np.maximum(fire,1)
    tfr=np.array([TF.get((s,sd),(np.nan,)*5) for s in syms])
    d["sret"]=tfr[:,0]; d["twoday"]=tfr[:,1]; d["rng2"]=tfr[:,2]; d["trend3_20b"]=tfr[:,3]; d["turn"]=tfr[:,4]
    d["label"]=d.symbol.isin(set(picks)).astype(int); d["td"]=td; d["sd"]=sd
    return d

rows=[]
for td in sorted(PICKS):
    ci=cidx.get(td)
    if ci is None or ci-1<0: continue
    dd=build_day(cal[ci-1], td, PICKS[td])
    if dd is not None: rows.append(dd)
D=pd.concat(rows, ignore_index=True)
XC=FEATC+["score","nf","maxl","avgl","sret","twoday","rng2","trend3_20b","turn"]
print(f"built {len(D):,} stock-days across {D.td.nunique()} operator days · {int(D.label.sum())} operator picks · {len(XC)} features")

def recall_at(df, keycol, ascending=False):
    hit={15:0,20:0,30:0}; tot=0
    for td,g in df.groupby("td"):
        g=g.sort_values(keycol, ascending=ascending); ranks={s:i+1 for i,s in enumerate(g.symbol)}
        picks=g[g.label==1].symbol
        for s in picks:
            r=ranks[s]; tot+=1
            for k in hit:
                if r<=k: hit[k]+=1
    return {k:hit[k]/tot*100 for k in hit}, tot

print("\n  BASELINE ranking methods — recall@k (% of your picks in top-k):")
print(f"  {'method':<18}{'@15':>7}{'@20':>8}{'@30':>8}")
for key in ["score","avgl","nf","maxl"]:
    r,tot=recall_at(D,key); print(f"  {key:<18}{r[15]:>6.0f}%{r[20]:>7.0f}%{r[30]:>7.0f}%")
# best single feature
bestf=None
for f in XC:
    if D[f].isna().all(): continue
    for asc in (False,True):
        r,_=recall_at(D,f,asc)
        if bestf is None or r[15]>bestf[1]: bestf=(f,r[15],asc,r)
print(f"  best single feat: {bestf[0]} ({'asc' if bestf[2] else 'desc'})  @15 {bestf[1]:.0f}%  @20 {bestf[3][20]:.0f}%  @30 {bestf[3][30]:.0f}%")

# TRAINED learning-to-rank: train Jan+Mar+Apr, test May (OOS)
D["mo"]=D.td.str[:7]
tr=D[~D.mo.isin(["2025-05"])]; te=D[D.mo=="2025-05"]
clf=HistGradientBoostingClassifier(max_iter=400,max_depth=4,learning_rate=0.06,l2_regularization=1.0,min_samples_leaf=40,
                                   class_weight="balanced",random_state=0)
clf.fit(tr[XC].values, tr.label.values)
D["pred"]=clf.predict_proba(D[XC].values)[:,1]
r_all,_=recall_at(D,"pred"); r_tr,_=recall_at(tr.assign(pred=clf.predict_proba(tr[XC].values)[:,1]),"pred"); r_te,_=recall_at(te.assign(pred=clf.predict_proba(te[XC].values)[:,1]),"pred")
print("\n  TRAINED learning-to-rank (predict YOUR picks):")
print(f"    in-sample (Jan+Mar+Apr) recall@15 {r_tr[15]:.0f}%  @20 {r_tr[20]:.0f}%  @30 {r_tr[30]:.0f}%")
print(f"    OUT-OF-SAMPLE (May)      recall@15 {r_te[15]:.0f}%  @20 {r_te[20]:.0f}%  @30 {r_te[30]:.0f}%")
print(f"    all days                 recall@15 {r_all[15]:.0f}%  @20 {r_all[20]:.0f}%  @30 {r_all[30]:.0f}%")
try:
    from sklearn.inspection import permutation_importance
    pi=permutation_importance(clf,te[XC].values,te.label.values,n_repeats=4,random_state=0,n_jobs=1)
    imp=pd.Series(pi.importances_mean,index=XC).sort_values(ascending=False)
    print("\n    what drives YOUR selection (top drivers):")
    for f,v in imp.head(12).items(): print(f"      {f:<20}{v:+.4f}")
except Exception as e: print("imp err",e)
