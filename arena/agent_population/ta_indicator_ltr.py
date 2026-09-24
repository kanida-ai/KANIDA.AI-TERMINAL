"""Enrich the ranker with a broad TECHNICAL-INDICATOR library (mean-reversion, continuation, capitulation,
pushed-down/re-entry, momentum, volume, volatility, candles) + Falcon features + 865-pattern stats.
Predict operator picks; cross-validate recall@15 with the operator protocol. Leak-free (point-in-time). Read-only."""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from operator_picks_8mo import PICKS
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
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
def rsi(c,n):
    d=np.diff(c,prepend=c[0]); up=np.clip(d,0,None); dn=np.clip(-d,0,None)
    ru=pd.Series(up).ewm(alpha=1/n,adjust=False).mean().values; rd=pd.Series(dn).ewm(alpha=1/n,adjust=False).mean().values
    return 100-100/(1+ru/np.where(rd==0,1e-9,rd))
def ema(c,n): return pd.Series(c).ewm(span=n,adjust=False).mean().values

con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
rows865=con.execute("""SELECT c.pattern_id,c.mined_year,c.rule_json FROM falcon_promoted_patterns p
    JOIN falcon_pattern_candidates c ON p.pattern_id=c.pattern_id""").fetchall()
PATS=[]
for pid,my,rj in rows865:
    try: PATS.append((my,[(f,op,th) for f,op,th in json.loads(rj)]))
    except: pass
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-05-01' AND trade_date<='2025-05-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-01-01' AND trade_date<='2025-05-31' ORDER BY symbol,trade_date",con); con.close()
FALCONF=list(FR.FEATURE_COLS)

# ---- technical indicator library (point-in-time per symbol) ----
def indi(g):
    g=g.sort_values("trade_date").reset_index(drop=True)
    o=g.open.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); c=g.close.values.astype(float); v=g.volume.values.astype(float)
    S=lambda a,n: pd.Series(a).rolling(n)
    d=pd.DataFrame({"symbol":g.symbol.values,"trade_date":g.trade_date.values})
    sma20=S(c,20).mean().values; std20=S(c,20).std().values; sma50=S(c,50).mean().values; sma200=S(c,200).mean().values
    # mean reversion
    d["z_sma20"]=(c-sma20)/np.where(std20==0,1e-9,std20)
    d["bb_pctb"]=(c-(sma20-2*std20))/np.where(4*std20==0,1e-9,4*std20)
    d["bb_width"]=4*std20/np.where(sma20==0,1e-9,sma20)*100
    vwap20=S(c*v,20).sum().values/np.where(S(v,20).sum().values==0,1e-9,S(v,20).sum().values); d["dist_vwap20"]=(c/vwap20-1)*100
    d["rsi2"]=rsi(c,2); d["rsi7"]=rsi(c,7); d["rsi21"]=rsi(c,21)
    d["pct_from_sma50"]=(c/sma50-1)*100
    # momentum / continuation
    for n in [3,10,120]: d[f"roc{n}"]=(c/np.roll(c,n)-1)*100
    macd=ema(c,12)-ema(c,26); d["macd_hist"]=macd-ema(macd,9)
    hh=S(h,14).max().values; ll=S(l,14).min().values; d["stoch_k"]=(c-ll)/np.where(hh-ll==0,1e-9,hh-ll)*100
    d["williams_r"]=(hh-c)/np.where(hh-ll==0,1e-9,hh-ll)*-100
    tp=(h+l+c)/3; d["cci20"]=(tp-S(tp,20).mean().values)/np.where(0.015*S(tp,20).apply(lambda x:np.mean(np.abs(x-x.mean())),raw=True).values==0,1e-9,0.015*S(tp,20).apply(lambda x:np.mean(np.abs(x-x.mean())),raw=True).values)
    dc=np.sign(np.diff(c,prepend=c[0]))
    up=(dc>0).astype(int); dn=(dc<0).astype(int)
    def streak(x):
        s=np.zeros(len(x));
        for i in range(1,len(x)): s[i]=s[i-1]+1 if x[i] else 0
        return s
    d["consec_up"]=streak(up); d["consec_down"]=streak(dn)
    d["above_sma20"]=(c>sma20).astype(float); d["above_sma50"]=(c>sma50).astype(float); d["above_sma200"]=(c>sma200).astype(float)
    d["breakout_20d"]=(c>=S(h,20).max().shift(1).values).astype(float); d["breakout_55d"]=(c>=S(h,55).max().shift(1).values).astype(float)
    d["ema_align"]=((ema(c,10)>ema(c,20))&(ema(c,20)>ema(c,50))).astype(float)
    # capitulation / pushed-down
    d["dd_20"]=(c/S(h,20).max().values-1)*100; d["dd_60"]=(c/S(h,60).max().values-1)*100; d["dd_252"]=(c/S(h,252).max().values-1)*100
    lower=(np.diff(l,prepend=l[0])<0).astype(int); d["lower_lows_5"]=pd.Series(lower).rolling(5).sum().values
    ret1=np.diff(c,prepend=c[0])/np.roll(c,1); d["big_red_5"]=pd.Series((ret1<-0.03).astype(int)).rolling(5).sum().values
    av20=S(v,20).mean().values; d["down_vol_spike"]=(((dc<0)&(v>2*av20)).astype(float))
    d["days_since_20d_high"]=[ (i-np.argmax(h[max(0,i-19):i+1])-max(0,i-19)) for i in range(len(c)) ]
    # re-entry / pullback: was extended, pulled back to sma20 in uptrend
    d["pullback_up"]=(( c>sma50)&(np.abs(c/sma20-1)<0.02)&(S(c,10).max().shift(1).values/sma20-1>0.05)).astype(float)
    # volume / volatility
    d["vol_ratio_5_20"]=S(v,5).mean().values/np.where(av20==0,1e-9,av20)
    d["vol_ratio_1_20"]=v/np.where(av20==0,1e-9,av20)
    obv=np.cumsum(np.sign(np.diff(c,prepend=c[0]))*v); d["obv_slope10"]=pd.Series(obv).diff(10).values/np.where(av20==0,1e-9,av20)
    mf=tp*v; posf=np.where(np.diff(tp,prepend=tp[0])>0,mf,0); negf=np.where(np.diff(tp,prepend=tp[0])<0,mf,0)
    d["mfi14"]=100-100/(1+S(posf,14).sum().values/np.where(S(negf,14).sum().values==0,1e-9,S(negf,14).sum().values))
    d["hv20"]=pd.Series(ret1).rolling(20).std().values*100
    # candles
    rng=np.where(h-l==0,1e-9,h-l); d["upper_wick"]=(h-np.maximum(o,c))/rng; d["lower_wick"]=(np.minimum(o,c)-l)/rng; d["body"]=np.abs(c-o)/rng
    d["is_hammer"]=((d.lower_wick>0.5)&(d.body<0.3)).astype(float); d["is_doji"]=(d.body<0.1).astype(float)
    return d
print("engineering technical indicators ...", flush=True)
IND=pd.concat([indi(g) for _,g in oh.groupby("symbol",sort=False)], ignore_index=True)
NEWC=[c for c in IND.columns if c not in ("symbol","trade_date")]
# tier features
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
TF={}
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); c=g.close.values.astype(float); h=g.high.values; l=g.low.values; v=g.volume.values.astype(float)
    pc=np.roll(c,1);pc[0]=np.nan;c2=np.roll(c,2);c2[:2]=np.nan
    sret=(c/pc-1)*100; rng=(h-l)/pc*100; twoday=(c/c2-1)*100
    a20=pd.Series(v).rolling(20).mean().values; a3=pd.Series(v).rolling(3).mean().values; tr=np.where(a20>0,a3/a20,np.nan)
    turn=pd.Series(c*v).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    for i,dd in enumerate(g.trade_date.values): TF[(s,dd)]=(sret[i],twoday[i],rng[i],tr[i],turn[i])
# weekly PIT for pattern scoring
rec=[]
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values; c=g.close.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
IND=IND.set_index(["symbol","trade_date"])
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}; FIDX={c:i for i,c in enumerate(FR.FEATURE_COLS)}
print("building labeled rows + pattern stats ...", flush=True)
rows=[]
for td in sorted(PICKS):
    ci=cidx.get(td)
    if ci is None or ci-1<0: continue
    sd=cal[ci-1]; fd=FCpit[FCpit.trade_date==sd]
    if fd.empty: continue
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms)); maxl=np.zeros(len(syms))
    for (my,rule) in PATS:
        if int(my)>=yr: continue
        m=np.ones(len(syms),bool); ok=True
        for f,op,th in rule:
            idx=FIDX.get(f)
            if idx is None: ok=False; break
            col=X[:,idx]; m&=OPS[op](col,th)&~np.isnan(col)
        if not ok or not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)  # lift not needed; count confluence
    picks=set(PICKS[td]); fdi=fd.set_index("symbol")
    for i in range(len(syms)):
        if fire[i]<10: continue
        tf=TF.get((syms[i],sd),(np.nan,)*5)
        if classify(tf[0],tf[1],tf[2],0,tf[3],tf[4]) not in HIGH: continue
        try: ind=IND.loc[(syms[i],sd)]
        except: continue
        row={"sd":sd,"symbol":syms[i],"label":1 if syms[i] in picks else 0,"nf":fire[i],"score":score[i]}
        for c2 in FALCONF: row[c2]=fdi.at[syms[i],c2] if c2 in fdi.columns else np.nan
        for c2 in NEWC: row[c2]=ind[c2]
        rows.append(row)
D=pd.DataFrame(rows); XC=FALCONF+NEWC+["nf","score"]
print(f"rows {len(D)} · {int(D.label.sum())} picks · {len(XC)} features ({len(NEWC)} new TA) · {D.sd.nunique()} days", flush=True)
sdays=sorted(D.sd.unique()); idxall=np.arange(len(D))
def didx(dset): return idxall[D.sd.isin(dset).values]
def recall(m,idx,k=15):
    hit=tot=0; pr=m.predict_proba(D.iloc[idx][XC].values)[:,1]; sub=D.iloc[idx].assign(_p=pr)
    for sd,g in sub.groupby("sd"):
        g=g.sort_values("_p",ascending=False); rk={s:i+1 for i,s in enumerate(g.symbol)}
        for s in g[g.label==1].symbol:
            tot+=1
            if rk[s]<=k: hit+=1
    return hit/tot*100 if tot else 0
def fit(idx):
    m=HistGradientBoostingClassifier(max_iter=350,max_depth=4,learning_rate=0.06,l2_regularization=2.0,min_samples_leaf=40,class_weight="balanced",random_state=0)
    m.fit(D.iloc[idx][XC].values, D.iloc[idx].label.values); return m
print("\n  recall@15 with TA indicators + Falcon + confluence:")
r15=[]
for frac,lab in [(0.5,"50:50"),(0.6,"60:40"),(0.7,"70:30"),(0.8,"80:20")]:
    n=int(len(sdays)*frac); a=set(sdays[:n]); b=set(sdays[n:])
    m=fit(didx(a)); r=recall(m,didx(b)); r15.append(r)
    m2=fit(didx(b)); r2=recall(m2,didx(a)); r15.append(r2)
    print(f"  {lab:<7} fwd {r:>3.0f}%   swap {r2:>3.0f}%")
n=int(len(sdays)*0.8); m=fit(didx(set(sdays[:n]))); rf=recall(m,didx(set(sdays[n:])))
print(f"\n  mean {np.mean(r15):.0f}%  std {np.std(r15):.0f}pp  ·  FORWARD {rf:.0f}%  (prev best without TA: ~26%)")
try:
    from sklearn.inspection import permutation_importance
    te=didx(set(sdays[n:])); pi=permutation_importance(m,D.iloc[te][XC].values,D.iloc[te].label.values,n_repeats=3,random_state=0,n_jobs=1)
    imp=pd.Series(pi.importances_mean,index=XC).sort_values(ascending=False)
    print("  top drivers:", ", ".join(f"{f}({v:.3f})" for f,v in imp.head(10).items()))
except Exception as e: print("imp err",str(e)[:50])
