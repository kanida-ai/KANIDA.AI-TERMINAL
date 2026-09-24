"""Reverse-engineer S05 (Trendlines) TRADES. For every S05 signal, reconstruct leak-free market STATE at entry,
find which states separate S05 winners from losers (LONG=3d, SHORT=1D), per stock, rank states, then TEST 2026:
does a state filter (learned <=2025) improve S05's 2026 out-of-sample P&L? Leak-free, read-only."""
import os, sys, sqlite3, warnings
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
LIBDB=os.path.join(os.path.dirname(os.path.abspath(__file__)),"strategy_library","library.db")
# ---- S05 signals (best config per stock, long+short) ----
cn=sqlite3.connect(LIBDB); S=pd.read_sql_query("SELECT * FROM signals WHERE strat_id='S05_trendlines'",cn); cn.close()
# ---- leak-free feature panel ----
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2022-06-01' AND trade_date<='2026-07-27' ORDER BY symbol,trade_date",con); con.close()
adv=oh.assign(t=oh.close*oh.volume).groupby("symbol").t.mean(); keep=set(adv[adv>3e7].index); oh=oh[oh.symbol.isin(keep)]
def rsi(c,n=14):
    d=np.diff(c,prepend=c[0]); up=np.where(d>0,d,0.0); dn=np.where(d<0,-d,0.0)
    ru=pd.Series(up).ewm(alpha=1/n,adjust=False).mean().values; rd=pd.Series(dn).ewm(alpha=1/n,adjust=False).mean().values
    return 100-100/(1+ru/np.where(rd==0,np.nan,rd))
rows=[]
for s,g in oh.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); n=len(g)
    if n<260: continue
    h=g.high.values.astype(float); l=g.low.values.astype(float); c=g.close.values.astype(float); v=g.volume.values.astype(float); dt=g.trade_date.values
    def rn(k): s_=np.roll(c,k); s_[:k]=np.nan; return (c/s_-1)*100
    pc=np.roll(c,1); pc[0]=np.nan; tr=np.maximum(h-l,np.maximum(np.abs(h-pc),np.abs(l-pc))); atr=pd.Series(tr).rolling(20).mean().values
    dt2=pd.to_datetime(g.trade_date); wk=dt2.dt.isocalendar().year.astype(int)*100+dt2.dt.isocalendar().week.astype(int)
    ghi=g.assign(wk=wk.values).groupby("wk").high.cummax().values; glo=g.assign(wk=wk.values).groupby("wk").low.cummin().values
    sm20=pd.Series(c).rolling(20).mean().values; sm50=pd.Series(c).rolling(50).mean().values; sm200=pd.Series(c).rolling(200).mean().values
    rows.append(pd.DataFrame(dict(symbol=s,trade_date=dt,ret3=rn(3),ret5=rn(5),ret10=rn(10),ret20=rn(20),
        range_pct=(h-l)/pc*100,close_loc=np.where(h>l,(c-l)/(h-l),.5),atr_pct=atr/c*100,rsi14=rsi(c),
        dist_hi20=c/pd.Series(h).rolling(20).max().values*100-100,dist_hi252=c/pd.Series(h).rolling(252).max().values*100-100,
        dist_sma50=c/sm50*100-100,dist_sma200=c/sm200*100-100,slope_sma20=sm20/np.roll(sm20,5)*100-100,
        wcl=np.where(ghi>glo,(c-glo)/(ghi-glo),.5),wrp=(ghi-glo)/c*100,vol20=pd.Series(rn(1)).rolling(20).std().values,
        atr_ptile=pd.Series(atr/c).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values,
        turn_ptile=pd.Series(c*v).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values)))
F=pd.concat(rows,ignore_index=True)
mk=F.groupby("trade_date").agg(mkt_ret20=("ret20","mean"),breadth=("ret20",lambda x:(x>0).mean())).reset_index()
F=F.merge(mk,on="trade_date",how="left"); F["rel_str20"]=F.ret20-F.mkt_ret20
FE=["ret3","ret5","ret10","ret20","range_pct","close_loc","atr_pct","rsi14","dist_hi20","dist_hi252","dist_sma50","dist_sma200","slope_sma20","wcl","wrp","vol20","atr_ptile","turn_ptile","mkt_ret20","breadth","rel_str20"]
# merge S05 signals with state at signal_date
M=S.merge(F.rename(columns={"trade_date":"signal_date"}),on=["symbol","signal_date"],how="inner")
M["yr"]=M.entry_date.str[:4].astype(int)
def analyze(direction, metric, lab):
    d=M[M.direction==direction].dropna(subset=[metric]).copy(); d["win"]=(d[metric]>0).astype(int); base=d.win.mean()
    print(f"\n  ===== S05 {direction} ({lab}) — {len(d)} signals · base win {base*100:.0f}% =====")
    uni=[]
    for f in FE:
        x=d[[f,"win"]].dropna()
        if len(x)<100: continue
        q1,q2=x[f].quantile([1/3,2/3]); hi=x[x[f]>=q2]; lo=x[x[f]<=q1]
        if len(hi)<30 or len(lo)<30: continue
        uni.append((f,hi.win.mean()*100,lo.win.mean()*100,(hi.win.mean()-lo.win.mean())*100))
    U=pd.DataFrame(uni,columns=["feature","win_hi%","win_lo%","lift"]); U["abs"]=U.lift.abs(); U=U.sort_values("abs",ascending=False)
    print("  state-lift (top vs bottom tercile P(win)):")
    for _,r in U.head(8).iterrows(): print(f"    {r['feature']:<12}{r['win_hi%']:>5.0f}% vs {r['win_lo%']:>3.0f}%   lift {r['lift']:+.0f}pp")
    # GBM state filter: train <=2025, test 2026
    tr=d[d.yr<=2025]; te=d[d.yr==2026]
    if len(tr)>200 and len(te)>50 and te.win.nunique()>1:
        gb=HistGradientBoostingClassifier(max_iter=200,learning_rate=0.05,max_depth=3,min_samples_leaf=40).fit(tr[FE].values,tr.win.values)
        te=te.copy(); te["p"]=gb.predict_proba(te[FE].values)[:,1]; auc=roc_auc_score(te.win,te.p)
        thr=tr[FE].pipe(lambda X:pd.Series(gb.predict_proba(X.values)[:,1])).median()
        kept=te[te.p>=thr]
        print(f"  2026 STATE-FILTER (train<=2025 GBM, AUC {auc:.3f}):")
        print(f"    all 2026 S05 {direction}: n={len(te)} win {te.win.mean()*100:.0f}% avg {te[metric].mean():+.2f}%")
        print(f"    state-filtered (p>=train median): n={len(kept)} win {kept.win.mean()*100:.0f}% avg {kept[metric].mean():+.2f}%")
analyze("LONG","ret_3d","3-day swing")
analyze("SHORT","ret_nextday","1-day")
