"""DEC-2024 minute-level MICROSTRUCTURE study. For each good-tier candidate, build first-15-min (09:15-09:29)
features that REAL intraday quant uses: relative volume (vs 20d opening-vol baseline), volume build (late vs
early), candle-size expansion, buying pressure (up-volume share), VWAP position/reclaim, and pool BREADTH.
HONEST test: correlation of each vs the TRADEABLE return 9:29->EOD (ret_0930), then score rules. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from tier_no_falcon import classify_tier_no_falcon
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); MDB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
WIN_TIERS={"PREMIUM-Pullback","GOLD","PREMIUM-Compression"}; LEV=5.0
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2024-12-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-01-01' AND trade_date<='2024-12-31' ORDER BY symbol,trade_date",con); con.close()
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
eodclose={(r.symbol,r.trade_date):r.close for r in oh.itertuples(index=False)}
trows=[]
for s,g in oh.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True)
    c=g.close.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); v=g.volume.values.astype(float)
    pc=np.roll(c,1); pc[0]=np.nan; c2=np.roll(c,2); c2[:2]=np.nan
    sret=(c/pc-1)*100; twoday=(c/c2-1)*100; rng=(h-l)/pc*100
    trend=pd.Series(v).rolling(3).mean().values/pd.Series(v).rolling(20).mean().values
    turn=c*v; tp=pd.Series(turn).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    for i in range(len(g)): trows.append((s,g.trade_date.values[i],sret[i],twoday[i],rng[i],trend[i],tp[i]))
TF=pd.DataFrame(trows,columns=["symbol","trade_date","sret","twoday","rng","trend3_20","turn_pct"]).set_index(["symbol","trade_date"])
o2=oh[["symbol","trade_date","high","low","close"]].copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
parts=[]
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); c=g.close.values.astype(float); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    parts.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
BASE=feat.drop(columns=WEEKLY).merge(pd.concat(parts,ignore_index=True),on=["symbol","trade_date"],how="left")
def ranked(sd):
    fd=BASE[BASE.trade_date==sd]
    if fd.empty: return []
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms))
    for p in pats:
        if int(p["mined_year"])>=yr: continue
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*(p["oos_lift"] or 0)
    cands=[{"symbol":syms[i],"nf":int(fire[i]),"score":float(score[i])} for i in range(len(syms)) if fire[i]>=10]
    cands.sort(key=lambda c:-c["score"]); rk=sorted(cands[:100],key=lambda c:-(c["score"]/max(c["nf"],1)))
    return [(r,c["symbol"]) for r,c in enumerate(rk,1)]
# candidate list
cand=[]
for td in [d for d in cal if "2024-12-01"<=d<="2024-12-31" and cidx[d]-1>=0]:
    sd=cal[cidx[td]-1]
    for rank,sym in ranked(sd):
        tf=TF.loc[(sym,sd)] if (sym,sd) in TF.index else None
        tier=classify_tier_no_falcon(tf["sret"],tf["twoday"],tf["rng"],tf["trend3_20"],tf["turn_pct"]) if tf is not None else None
        if tier in WIN_TIERS: cand.append((td,sym,rank,tier))
C=pd.DataFrame(cand,columns=["trade_date","symbol","falcon_rank","tier"])
# per-symbol first-15 microstructure with volume (one query per symbol, Oct-Dec for rvol baseline)
mcon=sqlite3.connect("file:"+MDB.replace("\\","/")+"?mode=ro",uri=True)
def first15_panel(sym):
    df=pd.read_sql_query("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=? AND bar_time>='2024-10-01' AND bar_time<'2025-01-01' AND substr(bar_time,12,8)>='09:15:00' AND substr(bar_time,12,8)<='09:29:00' ORDER BY bar_time",mcon,params=(sym,))
    if df.empty: return None
    df["d"]=df.bar_time.str[:10]; out=[]
    for d,g in df.groupby("d"):
        if len(g)<15: continue
        o=g.open.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); c=g.close.values.astype(float); v=g.volume.values.astype(float)
        O=o[0]; c14=c[14] if len(c)>14 else c[-1]
        if O<=0: continue
        tp=(h+l+c)/3; vwap=(tp*v).sum()/v.sum() if v.sum()>0 else np.nan
        rng=(h-l); early_rng=rng[:5].mean(); late_rng=rng[10:15].mean()
        vearly=v[:5].sum(); vlate=v[10:15].sum(); upvol=v[c>o].sum()
        out.append(dict(symbol=sym,trade_date=d,first15=(c14-O)/O*100,persist=float(np.mean(c[:15]>o[:15])),
            vol15=v[:15].sum(),vwap_pos=(c14-vwap)/vwap*100 if vwap==vwap else np.nan,
            vol_build=(vlate/vearly) if vearly>0 else np.nan,candle_expand=(late_rng/early_rng) if early_rng>0 else np.nan,
            up_vol_ratio=(upvol/v[:15].sum()) if v[:15].sum()>0 else np.nan,close_0929=c14))
    p=pd.DataFrame(out)
    if p.empty: return None
    p=p.sort_values("trade_date"); p["rvol15"]=p["vol15"]/p["vol15"].rolling(20,min_periods=5).median().shift(1)
    return p
syms=sorted(C.symbol.unique()); mp=[]
for s in syms:
    p=first15_panel(s)
    if p is not None: mp.append(p)
mcon.close()
M=pd.concat(mp,ignore_index=True)
P=C.merge(M,on=["symbol","trade_date"],how="inner")
P["eod"]=[eodclose.get((s,d)) for s,d in zip(P.symbol,P.trade_date)]
P=P.dropna(subset=["eod","close_0929"])
P["ret_0930"]=(P["eod"]-P["close_0929"])/P["close_0929"]*100
# breadth: fraction of pool green at 9:29 that day
P["breadth"]=P.groupby("trade_date")["first15"].transform(lambda s:(s>0).mean())
print(f"  panel {len(P)} candidate-days · {P.trade_date.nunique()} days · minute-level volume+candle+breadth\n")
print(f"  HONEST corr vs ret_0930 (9:29->EOD, the part you can still trade):")
for f in ["first15","persist","rvol15","vol_build","candle_expand","up_vol_ratio","vwap_pos","breadth","falcon_rank"]:
    v=P[[f,"ret_0930"]].dropna()
    print(f"    {f:<14}{v[f].corr(v['ret_0930']):+.3f}   (n={len(v)})")
# quick rule scoring @9:30 entry
def monthly(daily):
    m=np.array(daily)/100; return round((np.prod(1+m)-1)*100,1),round((np.prod(1+LEV*m)-1)*100,1),round((np.array(daily)>0).mean()*100)
def run(sel,label):
    daily=[]
    for td,g in P.groupby("trade_date"):
        pk=sel(g.copy()); r=pk["ret_0930"].dropna()
        if len(r): daily.append(r.mean())
    if not daily: return (label,0,0,0)
    m1,m5,wd=monthly(daily); return (label,m1,m5,wd)
VAR=[
 ("all good-tier @9:30",                 lambda g:g),
 ("top5 rvol (vol surge) @9:30",         lambda g:g.nlargest(5,"rvol15")),
 ("top5 up_vol_ratio (buying) @9:30",    lambda g:g.nlargest(5,"up_vol_ratio")),
 ("above VWAP + top5 rvol @9:30",        lambda g:g[g.vwap_pos>0].nlargest(5,"rvol15")),
 ("vol_build>1 & green, top5 @9:30",     lambda g:g[(g.vol_build>1)&(g.first15>0)].nlargest(5,"rvol15")),
 ("candle_expand>1 & green top5 @9:30",  lambda g:g[(g.candle_expand>1)&(g.first15>0)].nlargest(5,"rvol15")),
 ("low rvol (dry-up) green top5 @9:30",  lambda g:g[g.first15>0].nsmallest(5,"rvol15")),
 ("high breadth days, all @9:30",        lambda g:g if g.breadth.iloc[0]>0.5 else g.iloc[0:0]),
]
res=sorted([run(s,l) for l,s in VAR],key=lambda x:-x[2])
print(f"\n  === rule leaderboard (honest @9:30 entry -> EOD, 5x) ===")
print(f"  {'rule':<40}{'1x mo%':>8}{'5x mo%':>8}{'win-day%':>10}")
for lab,m1,m5,wd in res: print(f"  {lab:<40}{m1:>8}{m5:>8}{wd:>10}")
out=os.path.join(os.path.expanduser("~"),"Downloads","DEC2024_MICROSTRUCTURE_PANEL.xlsx")
P.sort_values(["trade_date","falcon_rank"]).to_excel(out,index=False)
print(f"\n  full microstructure panel -> {out}")
