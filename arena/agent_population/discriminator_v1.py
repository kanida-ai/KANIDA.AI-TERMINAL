"""DISCRIMINATOR v1 — learn what separates next-day intraday WINNERS from losers, then rank a top-15 basket.
Leak-free PIT features (stored daily technicals + recomputed week-to-date weekly). Label = next-day intraday
relative return (close-open)/open, cross-sectionally demeaned (removes market beta). Train Oct2024->May2025,
FORWARD-TEST Jun->Oct 2025 month-by-month. Compare basket P&L vs market vs Falcon-style. Read-only DB."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
from sklearn.ensemble import HistGradientBoostingRegressor
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0,os.path.join(ROOT,"scripts")); import falcon_signal_replay as FR
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
LO,HI="2024-08-01","2025-10-31"
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
feat=pd.read_sql_query(f"SELECT * FROM falcon_features WHERE trade_date>='{LO}' AND trade_date<='{HI}'",con)
oh=pd.read_sql_query(f"SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-01-01' AND trade_date<='{HI}' ORDER BY symbol,trade_date",con); con.close()
# leak-free week-to-date weekly features
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
parts=[]
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); c=g.close.values.astype(float); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    parts.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
BASE=feat.drop(columns=WEEKLY).merge(pd.concat(parts,ignore_index=True),on=["symbol","trade_date"],how="left")
# next-day intraday return label
oh["intra"]=(oh.close-oh.open)/oh.open
oh["adv20"]=oh.groupby("symbol").apply(lambda g:(g.close*g.volume).rolling(20).mean().shift(1)).reset_index(level=0,drop=True)
cal=sorted(oh.trade_date.unique()); nextd={cal[i]:cal[i+1] for i in range(len(cal)-1)}
lab=oh[["symbol","trade_date","intra","adv20","open","close"]].copy()
# map: features at signal_date S -> label at next day T
BASE["trade_next"]=BASE.trade_date.map(nextd)
m=BASE.merge(lab.rename(columns={"trade_date":"trade_next","intra":"y_intra","adv20":"adv_next"}),on=["symbol","trade_next"],how="inner")
FEATS=[c for c in FR.FEATURE_COLS if c in m.columns]
# candidate pool filter: tradeable + liquid + volatile enough (operator trades high-ATR movers)
m=m[(m["open"]>40)&(m["adv_next"]>3e7)&(m["atr_20_pct"]>1.5)].copy()
# cross-sectional demean of label per signal_date (relative intraday return -> removes beta)
m["y_rel"]=m["y_intra"]-m.groupby("trade_date")["y_intra"].transform("mean")
m=m.dropna(subset=["y_rel"])
TRAIN_END="2025-05-31"; FWD=["2025-06","2025-07","2025-08","2025-09","2025-10"]
tr=m[m.trade_date<=TRAIN_END];
X=tr[FEATS].values.astype(float); y=tr["y_rel"].values.astype(float)
print(f"  train rows {len(tr):,} · feats {len(FEATS)} · forward months {FWD}")
mdl=HistGradientBoostingRegressor(max_iter=350,learning_rate=0.05,max_depth=4,min_samples_leaf=60,l2_regularization=1.0)
mdl.fit(X,y)
def basket_month(dfmo, N, by):
    daily=[]
    for td,g in dfmo.groupby("trade_date"):
        gg=g.sort_values(by,ascending=False).head(N)
        if len(gg): daily.append(gg["y_intra"].mean())
    r=np.array(daily)
    if not len(r): return None
    return dict(days=len(r),mean=r.mean()*100,win=(r>0).mean()*100,m1=(np.prod(1+r)-1)*100,m5=(np.prod(1+5*r)-1)*100)
mf=m[m.trade_date>TRAIN_END].copy(); mf["pred"]=mdl.predict(mf[FEATS].values.astype(float)); mf["mo"]=mf.trade_date.str[:7]
mf["rnd"]=np.tile(np.arange(len(mf))%997,1)[:len(mf)]  # deterministic pseudo-order for a naive baseline
print(f"\n  FORWARD TEST — top-15 basket, next-day 09:15->EOD intraday, equal-weight:")
print(f"  {'month':<9}{'DISCRIMINATOR (pred)':>26}{'':>4}{'market (all cand mean)':>24}")
print(f"  {'':<9}{'1x':>8}{'5x':>9}{'mean/d':>9}{'win':>5}   {'1x':>8}{'5x':>9}{'mean/d':>8}")
tot=[]
for mo in FWD:
    dfmo=mf[mf.mo==mo]
    if not len(dfmo): continue
    d=basket_month(dfmo,15,"pred")
    # market baseline = mean of ALL candidates each day (N=huge)
    mk=[]
    for td,g in dfmo.groupby("trade_date"): mk.append(g["y_intra"].mean())
    mk=np.array(mk); mkd=dict(m1=(np.prod(1+mk)-1)*100,m5=(np.prod(1+5*mk)-1)*100,mean=mk.mean()*100)
    print(f"  {mo:<9}{d['m1']:>7.1f}%{d['m5']:>8.1f}%{d['mean']:>8.2f}%{d['win']:>4.0f}%   {mkd['m1']:>7.1f}%{mkd['m5']:>8.1f}%{mkd['mean']:>7.2f}%")
    tot.append((mo,d,mkd))
if tot:
    import numpy as _np
    dm=_np.prod([1+t[1]['m1']/100 for t in tot])-1; d5=_np.prod([1+t[1]['m5']/100 for t in tot])-1
    km=_np.prod([1+t[2]['m1']/100 for t in tot])-1
    print(f"\n  5-month COMPOUNDED:  discriminator top-15  1x {dm*100:>7.1f}%   5x {d5*100:>8.1f}%   |  market 1x {km*100:>6.1f}%")
