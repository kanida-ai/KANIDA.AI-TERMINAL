"""DEC-2024 EXPERIMENT HARNESS. Build ONE panel: good-tier pool (PREMIUM-Pullback/GOLD/PREMIUM-Compression,
prior-day Falcon top-100) x rich PRICE-ONLY early features (9:15-9:30) + market proxy + full-day returns.
Then score many strategy variants cheaply. Goal: find a rule set that turns strongly positive on Dec-2024.
Leak-free, read-only, production untouched. Exit EOD 15:29."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from tier_no_falcon import classify_tier_no_falcon
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); MDB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
WIN_TIERS={"PREMIUM-Pullback","GOLD","PREMIUM-Compression"}; PREMIUM={"PREMIUM-Pullback","PREMIUM-Compression"}
LEV=5.0; CAPITAL=500000.0; MAXC=0.5
HEAVY=["RELIANCE","HDFCBANK","ICICIBANK","INFY","TCS","ITC","LT","SBIN","AXISBANK","KOTAKBANK","BHARTIARTL","HINDUNILVR","BAJFINANCE","HCLTECH","MARUTI","SUNPHARMA","TITAN","NTPC","POWERGRID","TATAMOTORS"]
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2024-12-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-01-01' AND trade_date<='2024-12-31' ORDER BY symbol,trade_date",con); con.close()
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
pclose={(r.symbol,r.trade_date):r.close for r in oh.itertuples(index=False)}
# tier features
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
# leak-free weekly + rank
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
mcon=sqlite3.connect("file:"+MDB.replace("\\","/")+"?mode=ro",uri=True)
def morning(sym,date):
    df=pd.read_sql_query("SELECT open,high,low,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",mcon,params=(sym,f"{date} 09:15:00",f"{date} 15:29:00"))
    if len(df)<20: return None
    o=df.open.values.astype(float); h=df.high.values.astype(float); l=df.low.values.astype(float); c=df.close.values.astype(float)
    O=o[0]; eod=c[-1]
    if O<=0: return None
    f=slice(0,15)
    first5=(c[4]-O)/O*100; first15=(c[14]-O)/O*100
    persist=float(np.mean(c[f]>o[f])); f15hi=(h[:15].max()-O)/O*100; f15lo=(l[:15].min()-O)/O*100
    # user's breakout confirmation -> entry price
    conf_px=np.nan
    for i in range(2,min(len(df)-1,45)):     # scan first ~45 min for opening confirmation
        if c[i]>O and c[i]>max(h[i-1],h[i-2]) and c[i]>c[i-1] and abs((c[i]-o[i])/o[i]*100)<=MAXC:
            conf_px=o[i+1]; break
    return dict(open0915=O,close0929=c[14],eod=eod,first5=first5,first15=first15,persist=persist,
                f15hi=f15hi,f15lo=f15lo,conf_px=conf_px)
# market proxy first15 per day
present=[s for s in HEAVY if mcon.execute("SELECT 1 FROM ohlc_1min WHERE symbol=? LIMIT 1",(s,)).fetchone()]
mkt={}
for td in [d for d in cal if "2024-12-01"<=d<="2024-12-31"]:
    vv=[]
    for s in present:
        m=morning(s,td)
        if m: vv.append(m["first15"])
    if vv: mkt[td]=float(np.mean(vv))
# build panel
rows=[]
for td in [d for d in cal if "2024-12-01"<=d<="2024-12-31" and cidx[d]-1>=0]:
    sd=cal[cidx[td]-1]; m15=mkt.get(td,np.nan)
    for rank,sym in ranked(sd):
        tf=TF.loc[(sym,sd)] if (sym,sd) in TF.index else None
        tier=classify_tier_no_falcon(tf["sret"],tf["twoday"],tf["rng"],tf["trend3_20"],tf["turn_pct"]) if tf is not None else None
        if tier not in WIN_TIERS: continue
        mo=morning(sym,td)
        if mo is None: continue
        prev=pclose.get((sym,sd)); gap=(mo["open0915"]/prev-1)*100 if prev else np.nan
        ret_open=(mo["eod"]-mo["open0915"])/mo["open0915"]*100
        ret_0930=(mo["eod"]-mo["close0929"])/mo["close0929"]*100
        ret_conf=((mo["eod"]-mo["conf_px"])/mo["conf_px"]*100) if mo["conf_px"]==mo["conf_px"] else np.nan
        rows.append(dict(trade_date=td,symbol=sym,falcon_rank=rank,tier=tier,gap=round(gap,2) if gap==gap else np.nan,
            first5=round(mo["first5"],2),first15=round(mo["first15"],2),persist=round(mo["persist"],2),
            rel15=round(mo["first15"]-m15,2) if m15==m15 else np.nan,mkt15=round(m15,2) if m15==m15 else np.nan,
            confirmed=mo["conf_px"]==mo["conf_px"],ret_open=round(ret_open,2),ret_0930=round(ret_0930,2),ret_conf=round(ret_conf,2) if ret_conf==ret_conf else np.nan))
mcon.close()
P=pd.DataFrame(rows)
# diagnostic: which early feature correlates with full-day return (from open)
print(f"  panel: {len(P)} candidate-days over {P.trade_date.nunique()} days (avg {len(P)/P.trade_date.nunique():.0f}/day)\n")
print("  DIAGNOSTIC — corr of early feature vs return.  ret_open=from 9:15 (LOOK-AHEAD: contains first15)")
print("             vs ret_0930=from 9:29 close (HONEST: the tradeable part AFTER you can see first15)")
print(f"    {'feature':<12}{'vs ret_open':>13}{'vs ret_0930':>13}")
for f in ["first5","first15","persist","rel15","gap","falcon_rank"]:
    print(f"    {f:<12}{P[f].corr(P['ret_open']):>+13.3f}{P[f].corr(P['ret_0930']):>+13.3f}")
# variant scorer
def monthly(daily):
    m=np.array(daily)/100
    return (np.prod(1+m)-1)*100,(np.prod(1+LEV*m)-1)*100,(np.array(daily)>0).mean()*100,len(daily)
def run(sel,retcol,label):
    daily=[]; ntr=[]
    for td,g in P.groupby("trade_date"):
        pick=sel(g.copy())
        r=pick[retcol].dropna()
        if len(r): daily.append(r.mean()); ntr.append(len(r))
    if not daily: return (label,0,0,0,0,0)
    m1,m5,wd,nd=monthly(daily)
    return (label,round(m1,1),round(m5,1),round(wd),round(np.mean(ntr),1),nd)
VAR=[
 # HONEST — selection uses first15 (known 9:29), entry 9:30 close (ret_0930), or 9:15-knowable only
 ("[honest] BASE all good-tier @9:15",     lambda g:g,                                                   "ret_open"),
 ("[honest] TOP5 by Falcon-rank @9:15",    lambda g:g.nsmallest(5,"falcon_rank"),                        "ret_open"),
 ("[honest] TOP5 smallest-gap @9:15",      lambda g:g.nsmallest(5,"gap"),                                "ret_open"),
 ("[honest] TOP3 early-str @9:30",         lambda g:g.nlargest(3,"first15"),                             "ret_0930"),
 ("[honest] TOP5 early-str @9:30",         lambda g:g.nlargest(5,"first15"),                             "ret_0930"),
 ("[honest] TOP5 rel-str vs mkt @9:30",    lambda g:g.nlargest(5,"rel15"),                               "ret_0930"),
 ("[honest] TOP5 persistence @9:30",       lambda g:g.nlargest(5,"persist"),                             "ret_0930"),
 ("[honest] GREEN & mkt-RED top5 @9:30",   lambda g:g[(g.first15>0)&(g.mkt15<0)].nlargest(5,"rel15"),    "ret_0930"),
 ("[honest] PREMIUM & green top5 @9:30",   lambda g:g[(g.tier.isin(PREMIUM))&(g.first15>0)].nlargest(5,"first15"),"ret_0930"),
 ("[honest] confirmed top5 rank @conf",    lambda g:g[g.confirmed].nsmallest(5,"falcon_rank"),           "ret_conf"),
 ("[honest] confirmed top5 early @conf",   lambda g:g[g.confirmed].nlargest(5,"first15"),                "ret_conf"),
 # LOOK-AHEAD (NOT tradeable) — shown only to expose the trap: select by first15 but book from 9:15 open
 ("[LOOKAHEAD] TOP3 early @9:15",          lambda g:g.nlargest(3,"first15"),                             "ret_open"),
 ("[LOOKAHEAD] TOP5 early @9:15",          lambda g:g.nlargest(5,"first15"),                             "ret_open"),
]
res=[run(sel,rc,lab) for lab,sel,rc in VAR]
res.sort(key=lambda x:-x[2])
print(f"\n  === LEADERBOARD (Dec-2024, sorted by 5x monthly) ===")
print(f"  {'strategy':<38}{'1x mo%':>8}{'5x mo%':>8}{'win-day%':>10}{'avg#/day':>9}")
for lab,m1,m5,wd,ntr,nd in res:
    print(f"  {lab:<38}{m1:>8}{m5:>8}{wd:>10}{ntr:>9}")
out=os.path.join(os.path.expanduser("~"),"Downloads","DEC2024_EXPERIMENT_PANEL.xlsx")
with pd.ExcelWriter(out) as xw:
    P.sort_values(["trade_date","falcon_rank"]).to_excel(xw,sheet_name="panel",index=False)
    pd.DataFrame(res,columns=["strategy","mo_1x","mo_5x","win_day","avg_per_day","days"]).to_excel(xw,sheet_name="leaderboard",index=False)
print(f"\n  panel + leaderboard -> {out}")
