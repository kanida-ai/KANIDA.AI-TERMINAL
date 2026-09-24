"""JAN 2026 · test RANKING KEYS at native 20d/+10% horizon. Leak-free.
Score patterns once/day -> per stock: score=Σoos_lift, n_fires, max_lift, mean_lift, and forward metrics
(hit+10%/20d, 20d MFE, 20d close ret, TARGET-capture ret = +10% if touched else day-20 close, net 0.30%).
Rank all eligible (n_fires>=10) by each KEY, pool by bucket, compare hit-rate/lift/target-ret + GRADIENT + corr.
Keys: score | avg_lift(score/nf) | n_fires | max_lift | PROD(top100-by-score then avg_lift). Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
from scipy.stats import spearmanr
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
HZ = 20; TGT = 10.0; COST = 0.30

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-02-15'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-04-15' ORDER BY symbol,trade_date", con); con.close()
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

def score_day(day):
    fd=FCpit[FCpit.trade_date==day]
    if fd.empty: return None
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(day[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
    fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms)); maxl=np.zeros(len(syms))
    for p in elig:
        m=FR.rule_mask(p["rule"],X);
        if not m.any(): continue
        L=float(p["oos_lift"] or 0.0); fire+=m.astype(np.int32); score+=m.astype(np.float64)*L
        maxl=np.where(m & (L>maxl), L, maxl)
    return pd.DataFrame(dict(symbol=syms, score=score, nf=fire, maxl=maxl))

def fwd(sym, d1):
    S=SYM.get(sym); i=S["idx"].get(d1) if S else None
    if i is None or i+HZ-1>=S["n"]: return None
    e=S["o"][i]
    if e<=0: return None
    hi=S["h"][i:i+HZ].max(); hit=hi>=e*(1+TGT/100); fwd20=(S["c"][i+HZ-1]/e-1)*100
    tret=(TGT if hit else fwd20)-COST
    return int(hit),(hi/e-1)*100,fwd20,tret

jan=[d for d in sorted(FCpit[(FCpit.trade_date>="2026-01-01")&(FCpit.trade_date<="2026-01-31")].trade_date.unique()) if cidx.get(d) is not None and cidx[d]+1<len(cal)]
rows=[]
for sd in jan:
    d1=cal[cidx[sd]+1]; S=score_day(sd)
    if S is None: continue
    S=S[S.nf>=10].copy()
    fm={}
    for s in S.symbol:
        f=fwd(s,d1)
        if f: fm[s]=f
    S=S[S.symbol.isin(fm)].copy()
    S["hit"]=[fm[s][0] for s in S.symbol]; S["mfe"]=[fm[s][1] for s in S.symbol]; S["fwd"]=[fm[s][2] for s in S.symbol]; S["tret"]=[fm[s][3] for s in S.symbol]
    S["avgl"]=S.score/S.nf.clip(lower=1); S["day"]=sd
    # PROD key: top-100 by score then avg_lift order
    S["prodkey"]=-1.0
    top=S.sort_values("score",ascending=False).head(100).index
    S.loc[top,"prodkey"]=S.loc[top,"avgl"]
    rows.append(S)
R=pd.concat(rows, ignore_index=True)
base_hit=R.hit.mean()*100; base_tret=R.tret.mean()

KEYS={"score":"score","avg_lift":"avgl","n_fires":"nf","max_lift":"maxl","PROD(2-stage)":"prodkey"}
# assign per-day rank for each key
for name,col in KEYS.items():
    R[f"rk_{name}"]=R.groupby("day")[col].rank(ascending=False, method="first")

print("="*104)
print(f"  JAN 2026 · RANKING-KEY test · native +10%/20d · base hit {base_hit:.1f}%  base target-ret {base_tret:+.2f}%  ({len(R):,} instances)")
print("="*104)
print(f"  {'key':<15}{'T5 hit':>8}{'T10 hit':>9}{'T10 lift':>9}{'T10 tRet%':>10}{'T20 tRet%':>10}{'grad(T10−r41_50)':>18}{'corr':>8}")
for name in KEYS:
    rc=f"rk_{name}"
    t5=R[R[rc]<=5]; t10=R[R[rc]<=10]; t20=R[R[rc]<=20]; mid=R[(R[rc]>=41)&(R[rc]<=50)]
    grad=t10.hit.mean()*100 - mid.hit.mean()*100
    cr=spearmanr(R[rc], R.tret).correlation
    print(f"  {name:<15}{t5.hit.mean()*100:>7.1f}%{t10.hit.mean()*100:>8.1f}%{t10.hit.mean()/ (base_hit/100)/100*100:>9.2f}{t10.tret.mean():>+10.2f}{t20.tret.mean():>+10.2f}{grad:>+17.1f}{cr:>+8.3f}")

print("\n  GRADIENT by bucket — target-capture return% (the money metric):")
print(f"  {'key':<15}" + "".join(f"{('T'+str(n)):>9}" for n in [5,10,20,30,50]) + f"{'base':>9}")
for name in KEYS:
    rc=f"rk_{name}"
    cells=[R[R[rc]<=n].tret.mean() for n in [5,10,20,30,50]]
    print(f"  {name:<15}" + "".join(f"{c:>+9.2f}" for c in cells) + f"{base_tret:>+9.2f}")

print("\n  MARGINAL bands (best keys) — hit% / target-ret%:")
for name in ["score","n_fires","avg_lift","PROD(2-stage)"]:
    rc=f"rk_{name}"; parts=[]
    for a,b in [(1,10),(11,20),(21,30),(41,50),(76,100)]:
        g=R[(R[rc]>=a)&(R[rc]<=b)]; parts.append(f"{a}-{b}:{g.hit.mean()*100:.0f}%/{g.tret.mean():+.1f}")
    print(f"  {name:<15}" + "  ".join(parts))
best=max(KEYS, key=lambda k: R[R[f'rk_{k}']<=10].tret.mean())
print(f"\n  >> best key by Top-10 target-capture return: {best} ({R[R[f'rk_{best}']<=10].tret.mean():+.2f}%)")
