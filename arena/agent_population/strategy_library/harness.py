"""STRATEGY LIBRARY harness — accumulate PER-STOCK hit-rates for many strategies.
Philosophy (per operator): a strategy that works for ONE stock is valuable; find which strategy fits which stock.

Contract for a strategy: define  signal_fn(g) -> np.ndarray[int]  returning the integer ROW POSITIONS in g
(the per-symbol daily frame) where the strategy fires a LONG next-bar entry. The harness handles entry
(next day's open), exits (next-day EOD / +3d / +5d close), per-stock stats, per-year persistence, and a
luck-adjusted 'confident' flag. Everything leak-safe (signal_fn must only use info up to that bar).

g columns available: trade_date, open, high, low, close, volume, atr, sma20vol, tier, sret, twoday, rng,
                     trend3_20, turn_pct   (sorted ascending by date)
Usage from a strategy file:
    from harness import evaluate
    evaluate("S01_pivot_sr", "High-Volume Pivot S/R (bull)", "desc...", signal_fn, direction="LONG")
"""
import os, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
HERE=os.path.dirname(os.path.abspath(__file__))
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
import sys; sys.path.insert(0,os.path.join(ROOT,"arena","agent_population"))
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
CACHE=os.path.join(HERE,"daily_cache.parquet"); LIBDB=os.path.join(HERE,"library.db")
START="2022-01-01"; END="2026-07-27"; TEST_START="2023-01-01"; MIN_ADV=3e7
GOOD={"PREMIUM-Pullback","GOLD","PREMIUM-Compression"}

def build_cache():
    from tier_no_falcon import classify_tier_no_falcon
    con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
    oh=pd.read_sql_query(f"SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='{START}' AND trade_date<='{END}' ORDER BY symbol,trade_date",con); con.close()
    adv=oh.assign(t=oh.close*oh.volume).groupby("symbol").t.mean(); keep=set(adv[adv>MIN_ADV].index); oh=oh[oh.symbol.isin(keep)]
    out=[]
    for s,g in oh.groupby("symbol",sort=False):
        g=g.sort_values("trade_date").reset_index(drop=True); n=len(g)
        if n<80: continue
        o=g.open.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); c=g.close.values.astype(float); v=g.volume.values.astype(float)
        tr=np.maximum(h-l,np.maximum(np.abs(h-np.roll(c,1)),np.abs(l-np.roll(c,1)))); tr[0]=h[0]-l[0]
        g["atr"]=pd.Series(tr).rolling(200,min_periods=50).mean().values
        g["sma20vol"]=pd.Series(v).rolling(20).mean().values
        pc=np.roll(c,1); pc[0]=np.nan; c2=np.roll(c,2); c2[:2]=np.nan
        g["sret"]=(c/pc-1)*100; g["twoday"]=(c/c2-1)*100; g["rng"]=(h-l)/pc*100
        g["trend3_20"]=pd.Series(v).rolling(3).mean().values/pd.Series(v).rolling(20).mean().values
        turn=c*v; g["turn_pct"]=pd.Series(turn).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
        g["tier"]=[classify_tier_no_falcon(g["sret"].iloc[i],g["twoday"].iloc[i],g["rng"].iloc[i],g["trend3_20"].iloc[i],g["turn_pct"].iloc[i]) for i in range(n)]
        out.append(g)
    D=pd.concat(out,ignore_index=True); D.to_parquet(CACHE)
    print(f"  cache built: {len(D):,} rows · {D.symbol.nunique()} symbols -> {CACHE}")
    return D

def load_cache():
    if not os.path.exists(CACHE): return build_cache()
    return pd.read_parquet(CACHE)

def _init_db():
    cn=sqlite3.connect(LIBDB); c=cn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS meta(strat_id TEXT PRIMARY KEY,name TEXT,description TEXT,direction TEXT,n_signals INT,n_stocks INT,n_confident INT)")
    c.execute("CREATE TABLE IF NOT EXISTS signals(strat_id TEXT,symbol TEXT,signal_date TEXT,entry_date TEXT,entry_px REAL,ret_nextday REAL,ret_3d REAL,ret_5d REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS stock_stats(strat_id TEXT,symbol TEXT,n INT,win_nextday REAL,avg_nextday REAL,win_3d REAL,avg_3d REAL,win_5d REAL,avg_5d REAL,years INT,worked_years INT,confident INT)")
    for col in ("config TEXT","direction TEXT"):
        try: c.execute(f"ALTER TABLE stock_stats ADD COLUMN {col}")
        except Exception: pass
    try: c.execute("ALTER TABLE signals ADD COLUMN direction TEXT")
    except Exception: pass
    try: c.execute("ALTER TABLE meta ADD COLUMN n_confident_short INT")
    except Exception: pass
    cn.commit(); return cn

def _gen_signals(D, configs, sgn):
    recs=[]
    for sym,g in D.groupby("symbol",sort=False):
        g=g.reset_index(drop=True); n=len(g); o=g.open.values.astype(float); c=g.close.values.astype(float); dts=g.trade_date.values
        for cfg,fn in configs.items():
            try: idxs=fn(g)
            except Exception: continue
            for i in np.atleast_1d(idxs):
                i=int(i)
                if i<0 or i+1>=n: continue
                ent=o[i+1]
                if ent<=0 or np.isnan(ent): continue
                recs.append((cfg,sym,dts[i],dts[i+1],round(ent,2),
                    round(sgn*(c[i+1]-ent)/ent*100,2),round(sgn*(c[min(i+3,n-1)]-ent)/ent*100,2),round(sgn*(c[min(i+5,n-1)]-ent)/ent*100,2)))
    A=pd.DataFrame(recs,columns=["config","symbol","signal_date","entry_date","entry_px","ret_nextday","ret_3d","ret_5d"])
    return A[(A.entry_date>=TEST_START)&(A.entry_date<=END)]

def _best_per_stock(A, metric):   # metric in {"ret_3d","ret_nextday"}; returns (stat_rows, kept_signal_df)
    stat=[]; sig=[]
    for sym,ds in A.groupby("symbol"):
        cand=[]
        for cfg,d in ds.groupby("config"):
            if len(d)<4: continue
            dd=d.copy(); dd["yr"]=dd.entry_date.str[:4].astype(int); wy=0
            for _,dy in dd.groupby("yr"):
                if len(dy)>=3 and (dy[metric]>0).mean()>=0.60 and dy[metric].mean()>1.0: wy+=1
            cand.append((cfg,d,len(d),wy,dd.yr.nunique(),(d[metric]>0).mean()*100,d[metric].mean()))
        if not cand: continue
        cand.sort(key=lambda x:(-x[3],-x[6])); cfg,d,nn,wy,yrs,wm,am=cand[0]
        conf=1 if (nn>=6 and wm>=60 and am>1.0 and wy>=2) else 0
        stat.append((sym,nn,round((d.ret_nextday>0).mean()*100,1),round(d.ret_nextday.mean(),2),
            round((d.ret_3d>0).mean()*100,1),round(d.ret_3d.mean(),2),round((d.ret_5d>0).mean()*100,1),round(d.ret_5d.mean(),2),yrs,wy,conf,cfg))
        sig.append(d.assign(symbol=sym))
    return stat, (pd.concat(sig,ignore_index=True) if sig else A.iloc[0:0])

def evaluate_family(strat_id, name, description, long_configs, short_configs=None):
    """Per stock: pick BEST long config (by 3-5d swing) and BEST short config (by 1D). Stores both directions."""
    D=load_cache()
    AL=_gen_signals(D,long_configs,+1.0); statL,sigL=_best_per_stock(AL,"ret_3d")
    ST=pd.DataFrame(statL,columns=["symbol","n","win_nextday","avg_nextday","win_3d","avg_3d","win_5d","avg_5d","years","worked_years","confident","config"])
    ST["strat_id"]=strat_id; ST["direction"]="LONG"
    sigL=sigL.assign(strat_id=strat_id,direction="LONG")
    n_short=0
    if short_configs:
        AS=_gen_signals(D,short_configs,-1.0); statS,sigS=_best_per_stock(AS,"ret_nextday")
        STs=pd.DataFrame(statS,columns=["symbol","n","win_nextday","avg_nextday","win_3d","avg_3d","win_5d","avg_5d","years","worked_years","confident","config"])
        STs["strat_id"]=strat_id; STs["direction"]="SHORT"; n_short=int(STs.confident.sum())
        ST=pd.concat([ST,STs],ignore_index=True)
        sigS=sigS.assign(strat_id=strat_id,direction="SHORT"); sigALL=pd.concat([sigL,sigS],ignore_index=True)
    else:
        sigALL=sigL
    cn=_init_db(); c=cn.cursor()
    for t in ("meta","signals","stock_stats"): c.execute(f"DELETE FROM {t} WHERE strat_id=?",(strat_id,))
    nconf_long=int(ST[(ST.direction=='LONG')].confident.sum())
    c.execute("INSERT INTO meta(strat_id,name,description,direction,n_signals,n_stocks,n_confident,n_confident_short) VALUES(?,?,?,?,?,?,?,?)",
              (strat_id,name,description,"LONG+SHORT" if short_configs else "LONG",len(sigALL),ST.symbol.nunique(),nconf_long,n_short))
    sigALL[["strat_id","symbol","signal_date","entry_date","entry_px","ret_nextday","ret_3d","ret_5d","direction"]].to_sql("signals",cn,if_exists="append",index=False)
    ST[["strat_id","symbol","n","win_nextday","avg_nextday","win_3d","avg_3d","win_5d","avg_5d","years","worked_years","confident","config","direction"]].to_sql("stock_stats",cn,if_exists="append",index=False)
    cn.commit(); cn.close()
    print(f"  [{strat_id}] {name}: LONG confident {nconf_long} · SHORT confident {n_short} · signals {len(sigALL)}")
    return ST

def evaluate(strat_id, name, description, signal_fn, direction="LONG"):
    D=load_cache(); sgn=1.0 if direction.upper()=="LONG" else -1.0
    rows=[]
    for sym,g in D.groupby("symbol",sort=False):
        g=g.reset_index(drop=True); n=len(g)
        try: idxs=signal_fn(g)
        except Exception: continue
        o=g.open.values.astype(float); c=g.close.values.astype(float); dts=g.trade_date.values
        for i in np.atleast_1d(idxs):
            i=int(i)
            if i<0 or i+1>=n: continue
            ent=o[i+1]
            if ent<=0 or np.isnan(ent): continue
            r1=sgn*(c[i+1]-ent)/ent*100
            r3=sgn*(c[min(i+3,n-1)]-ent)/ent*100
            r5=sgn*(c[min(i+5,n-1)]-ent)/ent*100
            rows.append((strat_id,sym,dts[i],dts[i+1],round(ent,2),round(r1,2),round(r3,2),round(r5,2)))
    S=pd.DataFrame(rows,columns=["strat_id","symbol","signal_date","entry_date","entry_px","ret_nextday","ret_3d","ret_5d"])
    S=S[(S.entry_date>=TEST_START)&(S.entry_date<=END)]
    # per-stock stats + per-year persistence
    stat_rows=[]
    for sym,d in S.groupby("symbol"):
        d=d.copy(); d["yr"]=d.entry_date.str[:4].astype(int)
        wy=0
        for y,dy in d.groupby("yr"):
            if len(dy)>=3 and (dy.ret_3d>0).mean()>=0.60 and dy.ret_3d.mean()>1.0: wy+=1
        conf=1 if (len(d)>=6 and (d.ret_3d>0).mean()>=0.60 and d.ret_3d.mean()>1.0 and wy>=2) else 0
        stat_rows.append((strat_id,sym,len(d),round((d.ret_nextday>0).mean()*100,1),round(d.ret_nextday.mean(),2),
            round((d.ret_3d>0).mean()*100,1),round(d.ret_3d.mean(),2),round((d.ret_5d>0).mean()*100,1),round(d.ret_5d.mean(),2),
            d.yr.nunique(),wy,conf))
    ST=pd.DataFrame(stat_rows,columns=["strat_id","symbol","n","win_nextday","avg_nextday","win_3d","avg_3d","win_5d","avg_5d","years","worked_years","confident"])
    cn=_init_db(); c=cn.cursor()
    for t in ("meta","signals","stock_stats"): c.execute(f"DELETE FROM {t} WHERE strat_id=?",(strat_id,))
    c.execute("INSERT INTO meta VALUES(?,?,?,?,?,?,?)",(strat_id,name,description,direction,len(S),ST.shape[0],int(ST.confident.sum())))
    S.to_sql("signals",cn,if_exists="append",index=False); ST.to_sql("stock_stats",cn,if_exists="append",index=False)
    cn.commit(); cn.close()
    print(f"  [{strat_id}] {name}: {len(S)} signals · {ST.shape[0]} stocks · CONFIDENT stocks: {int(ST.confident.sum())}")
    conf=ST[ST.confident==1].sort_values("avg_3d",ascending=False)
    if len(conf):
        print(f"  {'symbol':<12}{'#sig':>5}{'3d win%':>9}{'avg 3d%':>9}{'yrs worked':>11}")
        for _,r in conf.head(20).iterrows():
            print(f"  {r['symbol']:<12}{int(r['n']):>5}{r['win_3d']:>8.0f}%{r['avg_3d']:>8.2f}%{int(r['worked_years'])}/{int(r['years'])}".ljust(0))
    else:
        print("  (no stocks cleared the confident bar: >=6 signals, >=60% 3d win, >1% avg 3d, worked >=2 yrs)")
    return ST
