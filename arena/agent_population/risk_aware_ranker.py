"""RISK-AWARE RANKER — OOS-trained model ranking the Falcon-eligible pool by predicted +10%/20d target-capture.
Leak-free: weekly features recomputed week-to-date (train+test); train 2024-06..2025-11, TEST Jan-2026 (unseen).
Target y = target-capture return = (+10 if touched within 20 sessions else day-20 close) − 0.30% cost.
X = pattern stats (score,n_fires,max_lift,avg_lift) + all 38 falcon features (weekly = PIT).
Compare NEW ranker vs OLD (production score) on the Jan test: Top-k hit%, target-ret, gradient, corr. Read-only.
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
from scipy.stats import spearmanr
from sklearn.ensemble import HistGradientBoostingRegressor
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
HZ=20; TGT=10.0; COST=0.30
TRAIN_LO,TRAIN_HI="2024-06-01","2025-11-30"; TEST_LO,TEST_HI="2026-01-01","2026-01-31"

print("loading ...", flush=True)
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-01-01' AND trade_date<='2026-02-15'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2022-06-01' AND trade_date<='2026-04-15' ORDER BY symbol,trade_date", con); con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []; SYM = {}
for s, g in o2.groupby("symbol", sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values; cl=g.close.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(cl-lo)/(hi-lo),np.nan), weekly_range_pct=np.where(cl>0,(hi-lo)/cl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(cl/sm-1)*100,np.nan), weekly_breakout_20w=np.where(ph==ph,(cl>ph).astype(float),np.nan))))
    SYM[s]=dict(o=g.open.values.astype(float), h=g.high.values.astype(float), c=cl, idx={d:i for i,d in enumerate(g.trade_date)}, n=len(g))
FCpit = feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d:i for i,d in enumerate(cal)}
FEATCOLS = FR.FEATURE_COLS[:]  # 38 (weekly are PIT in FCpit)

def score_day(day):
    fd=FCpit[FCpit.trade_date==day]
    if fd.empty: return None
    syms=fd.symbol.values; X=np.full((len(syms),len(FEATCOLS)),np.nan)
    for j,col in enumerate(FEATCOLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(day[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
    fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms)); maxl=np.zeros(len(syms))
    for p in elig:
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        L=float(p["oos_lift"] or 0.0); fire+=m.astype(np.int32); score+=m.astype(np.float64)*L; maxl=np.where(m&(L>maxl),L,maxl)
    df=fd[["symbol"]+FEATCOLS].copy(); df["score"]=score; df["nf"]=fire; df["maxl"]=maxl
    return df[df.nf>=10]

def fwd_tret(sym, d1):
    S=SYM.get(sym); i=S["idx"].get(d1) if S else None
    if i is None or i+HZ-1>=S["n"]: return None
    e=S["o"][i]
    if e<=0: return None
    hi=S["h"][i:i+HZ].max(); hit=hi>=e*(1+TGT/100); fwd20=(S["c"][i+HZ-1]/e-1)*100
    return int(hit),(TGT if hit else fwd20)-COST

def build(days):
    out=[]
    for sd in days:
        ci=cidx.get(sd)
        if ci is None or ci+1>=len(cal): continue
        d1=cal[ci+1]; S=score_day(sd)
        if S is None or S.empty: continue
        S=S.copy(); S["avgl"]=S.score/S.nf.clip(lower=1)
        ys=[]; keep=[]
        for s in S.symbol:
            f=fwd_tret(s,d1)
            if f: ys.append(f); keep.append(True)
            else: keep.append(False)
        S=S[keep].copy(); S["hit"]=[y[0] for y in ys]; S["tret"]=[y[1] for y in ys]; S["day"]=sd
        out.append(S)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()

alldays=sorted(FCpit.trade_date.unique())
train_days=[d for d in alldays if TRAIN_LO<=d<=TRAIN_HI][::2]   # every 2nd day
test_days=[d for d in alldays if TEST_LO<=d<=TEST_HI]
print(f"scoring train ({len(train_days)} days) ...", flush=True); TR=build(train_days)
print(f"scoring test ({len(test_days)} days) ...", flush=True); TE=build(test_days)
XCOLS=FEATCOLS+["score","nf","maxl","avgl"]
print(f"train {len(TR):,} instances · test {len(TE):,} instances · {len(XCOLS)} features", flush=True)

model=HistGradientBoostingRegressor(max_iter=300, max_depth=3, learning_rate=0.05, l2_regularization=1.0,
                                    min_samples_leaf=60, validation_fraction=0.15, early_stopping=True, random_state=0)
model.fit(TR[XCOLS].values, TR.tret.values)
TE=TE.copy(); TE["pred"]=model.predict(TE[XCOLS].values)
TE["rk_new"]=TE.groupby("day")["pred"].rank(ascending=False, method="first")
TE["rk_old"]=TE.groupby("day")["score"].rank(ascending=False, method="first")

base_hit=TE.hit.mean()*100; base_t=TE.tret.mean()
print("\n"+"="*96); print(f"  JAN-2026 TEST · pool base: hit {base_hit:.1f}%  target-ret {base_t:+.2f}%  ({len(TE):,} instances)"); print("="*96)
def report(rc,label):
    print(f"  {label}")
    print(f"    {'bucket':<9}{'hit%':>7}{'lift':>7}{'tRet%':>9}{'gradient(Tk vs pool)':>22}")
    for n in [5,10,20,30]:
        g=TE[TE[rc]<=n]; print(f"    Top-{n:<5}{g.hit.mean()*100:>6.1f}%{(g.hit.mean()*100)/base_hit:>7.2f}{g.tret.mean():>+9.2f}{g.tret.mean()-base_t:>+18.2f}")
    cr=spearmanr(TE[rc],TE.tret).correlation
    print(f"    corr(rank,target-ret) {cr:+.3f}   (negative = ranking works: rank 1 -> higher return)")
report("rk_old","OLD ranking (production score):")
print()
report("rk_new","NEW risk-aware ranker (OOS-trained):")

imp=pd.Series(model.feature_importances_ if hasattr(model,'feature_importances_') else np.zeros(len(XCOLS)), index=XCOLS)
try:
    from sklearn.inspection import permutation_importance
    pi=permutation_importance(model, TE[XCOLS].values, TE.tret.values, n_repeats=5, random_state=0, n_jobs=1)
    imp=pd.Series(pi.importances_mean, index=XCOLS)
except Exception: pass
print("\n  top drivers (permutation importance on test):")
for f,v in imp.sort_values(ascending=False).head(12).items(): print(f"    {f:<22}{v:+.4f}")
no=TE[TE.rk_new<=10].tret.mean(); oo=TE[TE.rk_old<=10].tret.mean()
print(f"\n  VERDICT — Top-10 target-capture: NEW {no:+.2f}%  vs  OLD {oo:+.2f}%  vs  pool {base_t:+.2f}%")
print(f"    NEW beats pool: {'YES' if no>base_t else 'NO'} · NEW beats OLD: {'YES' if no>oo else 'NO'}")
out=os.path.join(os.path.expanduser("~"),"Downloads","FALCON_RISK_AWARE_RANKER_JAN.xlsx")
TE[["day","symbol","score","nf","pred","rk_new","rk_old","hit","tret"]].to_excel(out,index=False); print(f"\n  test predictions -> {out}")
