"""Reverse-engineer operator trades: reconstruct leak-free market STATE at entry, rank which states separate
winners from losers, walk-forward, then TEST on 2026. Trades = 8-month operator picks (Oct24-May25); label =
next-day intraday (open->close, the tradeable outcome), win>+0.3% / loss<-0.3%. Features = primitives + derived
+ vol/liquidity regime + relative strength + market regime. GBM + univariate state-lift. Test 2026 = model
frozen on <=May25 applied to the universe (top-15/day by P(win)) -> P&L. Leak-free, read-only."""
import os, sys, warnings
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd, sqlite3
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import roc_auc_score
from operator_picks_8mo import PICKS
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2022-06-01' AND trade_date<='2026-07-27' ORDER BY symbol,trade_date",con); con.close()
adv=oh.assign(t=oh.close*oh.volume).groupby("symbol").t.mean(); keep=set(adv[adv>3e7].index); oh=oh[oh.symbol.isin(keep)]
FE=["ret1","ret2","ret3","ret5","ret10","ret20","range_pct","close_loc","atr_pct","rsi14","dist_hi20","dist_hi60","dist_hi252",
    "dist_sma20","dist_sma50","dist_sma200","slope_sma20","slope_sma50","wcl","wrp","wcvs","vol20","atr_ptile","turn_ptile"]
def rsi(c,n=14):
    d=np.diff(c,prepend=c[0]); up=np.where(d>0,d,0.0); dn=np.where(d<0,-d,0.0)
    ru=pd.Series(up).ewm(alpha=1/n,adjust=False).mean().values; rd=pd.Series(dn).ewm(alpha=1/n,adjust=False).mean().values
    return 100-100/(1+ru/np.where(rd==0,np.nan,rd))
rows=[]
for s,g in oh.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); n=len(g)
    if n<260: continue
    o=g.open.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); c=g.close.values.astype(float); v=g.volume.values.astype(float); dt=g.trade_date.values
    def rn(k): s_=np.roll(c,k); s_[:k]=np.nan; return c/s_-1
    ret1,ret2,ret3,ret5,ret10,ret20=[rn(k)*100 for k in (1,2,3,5,10,20)]
    pc=np.roll(c,1); pc[0]=np.nan
    tr=np.maximum(h-l,np.maximum(np.abs(h-pc),np.abs(l-pc))); atr=pd.Series(tr).rolling(20).mean().values
    dt2=pd.to_datetime(g.trade_date); wk=dt2.dt.isocalendar().year.astype(int)*100+dt2.dt.isocalendar().week.astype(int)
    ghi=g.assign(wk=wk.values).groupby("wk").high.cummax().values; glo=g.assign(wk=wk.values).groupby("wk").low.cummin().values
    sm20=pd.Series(c).rolling(20).mean().values; sm50=pd.Series(c).rolling(50).mean().values; sm200=pd.Series(c).rolling(200).mean().values
    d=dict(symbol=s,trade_date=dt,
        ret1=ret1,ret2=ret2,ret3=ret3,ret5=ret5,ret10=ret10,ret20=ret20,
        range_pct=(h-l)/pc*100, close_loc=np.where(h>l,(c-l)/(h-l),0.5),
        atr_pct=atr/c*100, rsi14=rsi(c),
        dist_hi20=c/pd.Series(h).rolling(20).max().values*100-100, dist_hi60=c/pd.Series(h).rolling(60).max().values*100-100, dist_hi252=c/pd.Series(h).rolling(252).max().values*100-100,
        dist_sma20=c/sm20*100-100, dist_sma50=c/sm50*100-100, dist_sma200=c/sm200*100-100,
        slope_sma20=sm20/np.roll(sm20,5)*100-100, slope_sma50=sm50/np.roll(sm50,5)*100-100,
        wcl=np.where(ghi>glo,(c-glo)/(ghi-glo),0.5), wrp=(ghi-glo)/c*100, wcvs=(c/pd.Series(c).rolling(20).mean().shift(1).values-1)*100,
        vol20=pd.Series(ret1).rolling(20).std().values,
        atr_ptile=pd.Series(atr/c).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values,
        turn_ptile=pd.Series(c*v).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values)
    df=pd.DataFrame(d)
    df["y_intra"]=np.append((c[1:]-o[1:])/o[1:]*100, np.nan)   # next-day intraday (label if this row is signal-day)
    rows.append(df)
F=pd.concat(rows,ignore_index=True)
# market regime per date
mk=F.groupby("trade_date").agg(mkt_ret20=("ret20","mean"),breadth=("ret20",lambda x:(x>0).mean()),mkt_vol=("vol20","mean")).reset_index()
F=F.merge(mk,on="trade_date",how="left"); F["rel_str20"]=F.ret20-F.mkt_ret20
FE+=["mkt_ret20","breadth","mkt_vol","rel_str20"]
# operator trades: PICKS date = trade_date; features at signal_date (prev day); label = trade_date intraday
cal=sorted(F.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
sig_rows=[]
for td,syms in PICKS.items():
    if td not in cidx or cidx[td]==0: continue
    sd=cal[cidx[td]-1]
    for s in syms: sig_rows.append((sd,s))
SD=pd.DataFrame(sig_rows,columns=["trade_date","symbol"]).drop_duplicates()
T=SD.merge(F,on=["symbol","trade_date"],how="inner").dropna(subset=["y_intra"])
T["win"]=(T.y_intra>0.3).astype(int); base=T.win.mean()
print(f"  operator trades matched: {len(T)} · win-rate(>+0.3% intraday) = {base*100:.1f}% · {T.symbol.nunique()} stocks\n")
# (1) univariate STATE LIFT: top vs bottom tercile P(win) + expectancy
print("  (1) STATE-LIFT — top-tercile vs bottom-tercile P(win) & avg intraday, ranked by |lift|:")
uni=[]
for f in FE:
    x=T[[f,"win","y_intra"]].dropna()
    if len(x)<60: continue
    q1,q2=x[f].quantile([1/3,2/3])
    hi=x[x[f]>=q2]; lo=x[x[f]<=q1]
    if len(hi)<15 or len(lo)<15: continue
    uni.append((f,hi.win.mean()*100,lo.win.mean()*100,(hi.win.mean()-lo.win.mean())*100,hi.y_intra.mean(),lo.y_intra.mean()))
U=pd.DataFrame(uni,columns=["feature","win_hi%","win_lo%","lift_pp","exp_hi%","exp_lo%"]).reindex()
U["abslift"]=U.lift_pp.abs(); U=U.sort_values("abslift",ascending=False)
print(f"  {'feature':<13}{'win_hi%':>8}{'win_lo%':>8}{'lift_pp':>8}{'exp_hi%':>8}{'exp_lo%':>8}")
for _,r in U.head(12).iterrows():
    print(f"  {r['feature']:<13}{r['win_hi%']:>7.0f}%{r['win_lo%']:>7.0f}%{r['lift_pp']:>+7.0f}{r['exp_hi%']:>+7.2f}%{r['exp_lo%']:>+7.2f}%")
# (2) GBM multivariate + walk-forward + permutation importance
T["mo"]=T.trade_date.str[:7]; months=sorted(T.mo.unique())
tr=T[T.mo<="2025-02"]; te=T[T.mo>"2025-02"]
Xtr=tr[FE].values; ytr=tr.win.values; Xte=te[FE].values; yte=te.win.values
gb=HistGradientBoostingClassifier(max_iter=250,learning_rate=0.05,max_depth=3,min_samples_leaf=40,l2_regularization=1.0).fit(Xtr,ytr)
auc=roc_auc_score(yte,gb.predict_proba(Xte)[:,1]) if len(np.unique(yte))>1 else np.nan
te=te.copy(); te["p"]=gb.predict_proba(Xte)[:,1]; topd=te[te.p>=te.p.quantile(0.7)]
print(f"\n  (2) GBM winner-classifier — train<=Feb25, test Mar-May25: AUC={auc:.3f}")
print(f"      test base win {yte.mean()*100:.0f}% · model top-30% win {topd.win.mean()*100:.0f}% · exp {topd.y_intra.mean():+.2f}% (base {te.y_intra.mean():+.2f}%)")
pi=permutation_importance(gb,Xte,yte,n_repeats=5,random_state=0)
imp=sorted(zip(FE,pi.importances_mean),key=lambda x:-x[1])[:8]
print("      top features (permutation): "+", ".join(f"{k}({v:.3f})" for k,v in imp))
# (3) 2026 TEST — model frozen on all operator trades, applied to the universe top-15/day by P(win)
gb_all=HistGradientBoostingClassifier(max_iter=250,learning_rate=0.05,max_depth=3,min_samples_leaf=40,l2_regularization=1.0).fit(T[FE].values,T.win.values)
F26=F[(F.trade_date>="2025-12-31")&(F.trade_date<="2026-07-27")].dropna(subset=FE+["y_intra"]).copy()
F26["p"]=gb_all.predict_proba(F26[FE].values)[:,1]
daily=[]
for d,g in F26.groupby("trade_date"):
    top=g.nlargest(15,"p"); daily.append((d,top.y_intra.mean()))
DD=pd.DataFrame(daily,columns=["date","ret"]); DD["mo"]=DD.date.str[:7]
print(f"\n  (3) 2026 TEST — model (<=May25) picks top-15/day by P(win) from the universe, next-day intraday:")
print(f"  {'month':<9}{'days':>6}{'avg%/day':>10}{'ret1x%':>9}{'ret5x%':>9}")
for mo,gm in DD.groupby("mo"):
    r=gm.ret.values/100; print(f"  {mo:<9}{len(gm):>6}{gm.ret.mean():>+9.2f}%{(np.prod(1+r)-1)*100:>+8.1f}%{(np.prod(1+5*r)-1)*100:>+8.1f}%")
r=DD.ret.values/100
print(f"  {'TOTAL':<9}{len(DD):>6}{DD.ret.mean():>+9.2f}%{(np.prod(1+r)-1)*100:>+8.1f}%{(np.prod(1+5*r)-1)*100:>+8.1f}%  · win-days {(DD.ret>0).mean()*100:.0f}%")
