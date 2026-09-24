"""NEW data: 1-min SIGNAL-DAY microstructure (price-vol absorption/confirmation, volume dry-up, VWAP, late/early
volume, up-vol fraction) + PER-STOCK percentile normalization of factors (each stock vs its own history).
Rank high-tier pool -> recall@15 vs operator, cross-validated with the protocol. Tests whether 1-min volume
dynamics + per-stock thresholds break the 28% ceiling. Leak-free. Read-only."""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
import lightgbm as lgb
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from operator_picks_8mo import PICKS
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); ODB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
HIGH={"PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline"}
OPS={"<=":np.less_equal,"<":np.less,">":np.greater,">=":np.greater_equal}
def _ok(v): return v is not None and not (isinstance(v,float) and v!=v)
def classify(sret,twoday,rng,al,tr,turn):
    if _ok(sret) and sret>10: return "AVOID"
    if _ok(sret) and sret>7 and _ok(turn) and turn>=0.75: return "AVOID"
    if _ok(sret) and sret<=2 and _ok(twoday) and twoday<-5 and _ok(al) and al>15: return "PREMIUM-Pullback"
    if _ok(sret) and sret<=2 and _ok(rng) and rng<2 and _ok(al) and al>15: return "PREMIUM-Compression"
    if _ok(sret) and sret<=2 and _ok(tr) and tr<0.9: return "ENTERPRISE-Dryup"
    if _ok(sret) and sret<=2 and _ok(turn) and turn<0.75: return "GOLD"
    if _ok(sret) and sret<=2: return "GOLD-baseline"
    if _ok(sret) and sret<=5: return "STANDARD"
    return "STANDARD-weak"
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
rows865=con.execute("""SELECT c.mined_year,c.rule_json FROM falcon_promoted_patterns p JOIN falcon_pattern_candidates c ON p.pattern_id=c.pattern_id""").fetchall()
PATS=[]
for my,rj in rows865:
    try: PATS.append((my,[(f,op,th) for f,op,th in json.loads(rj)]))
    except: pass
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
    for i,d in enumerate(g.trade_date.values): TF[(s,d)]=(sret[i],twoday[i],rng[i],tr[i],turn[i])
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
# PER-STOCK percentile normalization of key daily factors (rolling 120d rank within each stock)
PSF=["roc_5","roc_20","atr_20_pct","vol_vs_20d","rsi_14","dist_high_20","dist_high_60","dist_sma_20","dist_sma_50","range_pct"]
for c in PSF:
    if c in FCpit.columns:
        FCpit[c+"_psr"]=FCpit.groupby("symbol")[c].transform(lambda s: s.rolling(120,min_periods=30).rank(pct=True))
PSRC=[c+"_psr" for c in PSF if c in FCpit.columns]
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}; FIDX={c:i for i,c in enumerate(FR.FEATURE_COLS)}
FALCONF=list(FR.FEATURE_COLS)

def onemin_feats(oc, sym, day):
    b=oc.execute("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",
                 (sym, day+" 09:15", day+" 15:35")).fetchall()
    if len(b)<30: return None
    o=np.array([x[1] for x in b],float); h=np.array([x[2] for x in b],float); l=np.array([x[3] for x in b],float); c=np.array([x[4] for x in b],float); v=np.array([x[5] for x in b],float)
    ret=np.diff(c,prepend=c[0])/np.where(np.roll(c,1)==0,1e-9,np.roll(c,1)); ret[0]=0
    tv=v.sum() if v.sum()>0 else 1e-9; vwap=(c*v).sum()/tv
    up=v[ret>0].sum(); dn=v[ret<0].sum()
    n=len(c); last60=slice(max(0,n-60),n); first30=slice(0,30)
    # accumulation: volume-weighted sign of return; absorption: down-min volume share
    return dict(
        om_close_loc=(c[-1]-l.min())/max(h.max()-l.min(),1e-9),
        om_vwap_gap=(c[-1]/vwap-1)*100,
        om_upvol_frac=up/tv,
        om_downvol_frac=dn/tv,
        om_late_vol=v[last60].sum()/tv,
        om_early_vol=v[first30].sum()/tv,
        om_maxmin_vol_z=v.max()/max(v.mean(),1e-9),
        om_green_min=(ret>0).mean(),
        om_accum=(np.sign(ret)*v).sum()/tv,                 # net buying pressure
        om_absorb=(v[ret<0].sum()/tv) if (c[-1]>=vwap) else 0.0,  # sold into but closed above vwap = absorption
        om_ret_vol_corr=float(np.corrcoef(ret[1:],v[1:])[0,1]) if n>5 and v[1:].std()>0 else 0.0,
        om_intraday_ret=(c[-1]/o[0]-1)*100,
    )
OMC=["om_close_loc","om_vwap_gap","om_upvol_frac","om_downvol_frac","om_late_vol","om_early_vol","om_maxmin_vol_z","om_green_min","om_accum","om_absorb","om_ret_vol_corr","om_intraday_ret"]

oc=sqlite3.connect("file:"+ODB.replace("\\","/")+"?mode=ro",uri=True)
print("building rows w/ 1-min signal-day microstructure + per-stock percentiles ...", flush=True)
rows=[]
for td in sorted(PICKS):
    ci=cidx.get(td)
    if ci is None or ci-1<0: continue
    sd=cal[ci-1]; fd=FCpit[FCpit.trade_date==sd]
    if fd.empty: continue
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); fire=np.zeros(len(syms),np.int32)
    for (my,rule) in PATS:
        if int(my)>=yr: continue
        m=np.ones(len(syms),bool); ok=True
        for f,op,th in rule:
            idx=FIDX.get(f)
            if idx is None: ok=False; break
            col=X[:,idx]; m&=OPS[op](col,th)&~np.isnan(col)
        if not ok or not m.any(): continue
        fire+=m.astype(np.int32)
    picks=set(PICKS[td]); fdi=fd.set_index("symbol")
    for i in range(len(syms)):
        if fire[i]<10: continue
        tf=TF.get((syms[i],sd),(np.nan,)*5)
        if classify(tf[0],tf[1],tf[2],0,tf[3],tf[4]) not in HIGH: continue
        om=onemin_feats(oc, syms[i], sd)
        if om is None: continue
        row={"sd":sd,"symbol":syms[i],"label":1 if syms[i] in picks else 0,"nf":fire[i]}
        for c2 in FALCONF: row[c2]=fdi.at[syms[i],c2] if c2 in fdi.columns else np.nan
        for c2 in PSRC: row[c2]=fdi.at[syms[i],c2] if c2 in fdi.columns else np.nan
        row.update(om); rows.append(row)
oc.close()
D=pd.DataFrame(rows).sort_values("sd").reset_index(drop=True); XC=FALCONF+PSRC+OMC+["nf"]
sdays=sorted(D.sd.unique())
print(f"rows {len(D)} · {int(D.label.sum())} picks · {len(XC)} feats ({len(OMC)} 1-min +{len(PSRC)} per-stock) · {len(sdays)} days", flush=True)
def groups(df): return df.groupby("sd").size().values
def recall(m,df,k=15):
    df=df.copy(); df["_p"]=m.predict(df[XC].values); hit=tot=0
    for sd,g in df.groupby("sd"):
        g=g.sort_values("_p",ascending=False); rk={s:i+1 for i,s in enumerate(g.symbol)}
        for s in g[g.label==1].symbol:
            tot+=1
            if rk[s]<=k: hit+=1
    return hit/tot*100 if tot else 0
def fit(tr):
    tr=tr.sort_values("sd")
    m=lgb.LGBMRanker(objective="lambdarank",n_estimators=400,num_leaves=31,learning_rate=0.05,min_child_samples=30,
        subsample=0.8,colsample_bytree=0.8,reg_lambda=2.0,random_state=0,verbose=-1,label_gain=[0,1])
    m.fit(tr[XC].values,tr.label.values,group=groups(tr)); return m
print("\n  recall@15 · 1-min microstructure + per-stock + LambdaMART:")
r15=[]
for frac,lab in [(0.5,"50:50"),(0.6,"60:40"),(0.7,"70:30"),(0.8,"80:20")]:
    n=int(len(sdays)*frac); a=set(sdays[:n]); b=set(sdays[n:])
    m=fit(D[D.sd.isin(a)]); r=recall(m,D[D.sd.isin(b)]); r15.append(r)
    m2=fit(D[D.sd.isin(b)]); r2=recall(m2,D[D.sd.isin(a)]); r15.append(r2)
    print(f"  {lab:<7} fwd {r:>3.0f}%   swap {r2:>3.0f}%")
n=int(len(sdays)*0.8); m=fit(D[D.sd.isin(set(sdays[:n]))]); rf=recall(m,D[D.sd.isin(set(sdays[n:]))])
print(f"\n  mean {np.mean(r15):.0f}%  std {np.std(r15):.0f}pp  ·  FORWARD {rf:.0f}%  (prev ceiling 28%)")
try:
    from sklearn.inspection import permutation_importance
    te=D[D.sd.isin(set(sdays[n:]))]
    pr0=m.predict(te[XC].values)
    imps=[]
    rng=np.random.RandomState(0)
    for ci2,col in enumerate(XC):
        tt=te.copy(); tt[col]=tt[col].sample(frac=1,random_state=1).values
        # recompute recall drop
    # simple gain importance instead
    gi=pd.Series(m.booster_.feature_importance(importance_type="gain"),index=XC).sort_values(ascending=False)
    print("  top gain drivers:", ", ".join(f"{f}" for f in gi.head(12).index))
except Exception as e: print("imp err",str(e)[:60])
