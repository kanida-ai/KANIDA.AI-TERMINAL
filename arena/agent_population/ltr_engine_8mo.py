"""LEARNING-TO-RANK engine v_final: reproduce operator's selection from 8 months of labeled picks.
Leak-free features (week-to-date). Train a classifier P(operator picks stock) on signal-day features;
rank each day, measure recall@15. Validate with the operator's protocol: 50:50/60:40/70:30/80:20 splits
+ swaps + chronological forward test. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP = os.path.join(ROOT, "arena", "agent_population"); sys.path.insert(0, AP); sys.path.insert(0, os.path.join(ROOT, "scripts"))
import falcon_signal_replay as FR
from operator_picks_8mo import PICKS
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]

con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-05-01' AND trade_date<='2025-05-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-05-31' ORDER BY symbol,trade_date",con); con.close()
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
    for i,d in enumerate(g.trade_date.values): TF[(s,d)]=(sret[i],twoday[i],rng[i],tr[i],turn[i],c[i],v[i])
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
FEATC=list(FR.FEATURE_COLS)

def build_day(sd, picks):
    fd=FCpit[FCpit.trade_date==sd]
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
    tfr=np.array([TF.get((s,sd),(np.nan,)*7) for s in syms])
    d["sret"]=tfr[:,0]; d["twoday"]=tfr[:,1]; d["rng2"]=tfr[:,2]; d["tr3"]=tfr[:,3]; d["turn"]=tfr[:,4]; d["px"]=tfr[:,5]; d["vol"]=tfr[:,6]
    d["label"]=d.symbol.isin(set(picks)).astype(int); d["sd"]=sd
    return d

rows=[]
tdays=sorted(PICKS)
for td in tdays:
    ci=cidx.get(td)
    if ci is None or ci-1<0: continue
    dd=build_day(cal[ci-1], PICKS[td])
    if dd is not None and dd.label.sum()>0: rows.append(dd)
D=pd.concat(rows, ignore_index=True)
XC=FEATC+["score","nf","maxl","avgl","sret","twoday","rng2","tr3","turn","px","vol"]
sdays=sorted(D.sd.unique())
print(f"built {len(D):,} rows · {len(sdays)} signal days · {int(D.label.sum())} operator picks · {len(XC)} features", flush=True)

def recall(model, test_df, ks=(10,15,20)):
    hit={k:0 for k in ks}; tot=0
    pr=model.predict_proba(test_df[XC].values)[:,1]; test_df=test_df.assign(_p=pr)
    for sd,g in test_df.groupby("sd"):
        g=g.sort_values("_p",ascending=False); ranks={s:i+1 for i,s in enumerate(g.symbol)}
        for s in g[g.label==1].symbol:
            tot+=1; r=ranks[s]
            for k in ks:
                if r<=k: hit[k]+=1
    return {k:hit[k]/tot*100 for k in ks}, tot

def fit(train_df):
    m=HistGradientBoostingClassifier(max_iter=350,max_depth=4,learning_rate=0.06,l2_regularization=1.0,
        min_samples_leaf=40,class_weight="balanced",random_state=0)
    m.fit(train_df[XC].values, train_df.label.values); return m

def split_days(frac):  # chronological first-frac
    n=int(len(sdays)*frac); return set(sdays[:n]), set(sdays[n:])

print("\n  RECALL@15 across the operator's validation protocol (train->test):")
print(f"  {'split':<16}{'train d':>8}{'test d':>7}{'R@10':>7}{'R@15':>7}{'R@20':>7}")
r15s=[]
for frac,lab in [(0.5,"50:50"),(0.6,"60:40"),(0.7,"70:30"),(0.8,"80:20")]:
    a,b=split_days(frac)
    # A->B (chronological forward)
    m=fit(D[D.sd.isin(a)]); r,_=recall(m, D[D.sd.isin(b)]); r15s.append(r[15])
    print(f"  {lab+' fwd':<16}{len(a):>8}{len(b):>7}{r[10]:>6.0f}%{r[15]:>6.0f}%{r[20]:>6.0f}%")
    # B->A (swap)
    m2=fit(D[D.sd.isin(b)]); r2,_=recall(m2, D[D.sd.isin(a)]); r15s.append(r2[15])
    print(f"  {lab+' swap':<16}{len(b):>8}{len(a):>7}{r2[10]:>6.0f}%{r2[15]:>6.0f}%{r2[20]:>6.0f}%")
print(f"\n  recall@15 across 8 configs: mean {np.mean(r15s):.0f}%  std {np.std(r15s):.0f}pp  min {min(r15s):.0f}%  max {max(r15s):.0f}%")

# CHRONOLOGICAL FORWARD TEST (train first 80% days, test last 20%) + baseline (production score rank)
a,b=split_days(0.8); m=fit(D[D.sd.isin(a)]); rf,_=recall(m, D[D.sd.isin(b)])
# baseline: rank by production score on the same test days
te=D[D.sd.isin(b)]; hit=0; tot=0
for sd,g in te.groupby("sd"):
    g=g.sort_values("score",ascending=False); ranks={s:i+1 for i,s in enumerate(g.symbol)}
    for s in g[g.label==1].symbol:
        tot+=1
        if ranks[s]<=15: hit+=1
print(f"\n  FORWARD TEST (train first 80% days -> last 20% unseen): recall@15 {rf[15]:.0f}%  (@10 {rf[10]:.0f}% @20 {rf[20]:.0f}%)")
print(f"  baseline (production score rank) recall@15 on same test days: {hit/tot*100:.0f}%")
try:
    from sklearn.inspection import permutation_importance
    pi=permutation_importance(m, te[XC].values, te.label.values, n_repeats=4, random_state=0, n_jobs=1)
    imp=pd.Series(pi.importances_mean, index=XC).sort_values(ascending=False)
    print("\n  top drivers of YOUR selection:")
    for f,v in imp.head(12).items(): print(f"    {f:<20}{v:+.4f}")
except Exception as e: print("imp err", str(e)[:60])
print(f"\n  RULE CHECK: target recall@15 >=70% consistent (std<10pp) + forward within 10pp.")
print(f"    mean {np.mean(r15s):.0f}% · std {np.std(r15s):.0f}pp · forward {rf[15]:.0f}% -> {'PASS' if np.mean(r15s)>=70 and np.std(r15s)<10 and rf[15]>=60 else 'NOT YET'}")
