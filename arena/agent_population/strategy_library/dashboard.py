"""Per-strategy DASHBOARD from the library: by year 2023-2026, LONG (1D + 3d + 5d) & SHORT (1D):
P&L (compounded basket), hit-rate, max drawdown, +months, signal count, confident-stock count, and an
intraday-vs-positional verdict. Console scorecard + Excel. Equal-weight per-day basket of that strategy's signals."""
import os, sqlite3, warnings, sys
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
HERE=os.path.dirname(os.path.abspath(__file__)); LIBDB=os.path.join(HERE,"library.db")
ARGS=sys.argv[1:]; ONLY=[a for a in ARGS if a.startswith("S")]; WRITE_XL=("xl" in ARGS)
cn=sqlite3.connect(LIBDB)
meta=pd.read_sql_query("SELECT * FROM meta",cn)
if ONLY: meta=meta[meta.strat_id.isin(ONLY)]
sig=pd.read_sql_query("SELECT * FROM signals",cn)
ss=pd.read_sql_query("SELECT * FROM stock_stats",cn); cn.close()
sig["year"]=sig.entry_date.str[:4].astype(int); sig["ym"]=sig.entry_date.str[:7]
def basket(d, retcol):
    d=d.dropna(subset=[retcol])
    if len(d)==0: return dict(n=0,hit=np.nan,avg=np.nan,tot=np.nan,mdd=np.nan,posm=0,mo=0)
    daily=d.groupby("entry_date")[retcol].mean().sort_index()
    eq=(1+daily/100).cumprod(); dd=(eq/eq.cummax()-1).min()*100
    mcomp=daily.groupby(daily.index.str[:7]).apply(lambda s:(1+s/100).prod()-1)*100
    return dict(n=len(d),hit=(d[retcol]>0).mean()*100,avg=d[retcol].mean(),
                tot=((1+daily/100).prod()-1)*100,mdd=dd,posm=int((mcomp>0).sum()),mo=int(mcomp.shape[0]))
YEARS=[2023,2024,2025,2026]
rows=[]
for _,m in meta.iterrows():
    sid=m.strat_id
    L=sig[(sig.strat_id==sid)&(sig.direction=="LONG")]; S=sig[(sig.strat_id==sid)&(sig.direction=="SHORT")]
    confL=int(ss[(ss.strat_id==sid)&(ss.direction=="LONG")].confident.sum())
    confS=int(ss[(ss.strat_id==sid)&(ss.direction=="SHORT")].confident.sum()) if "direction" in ss else 0
    print("\n"+"="*96)
    print(f"  {sid}  —  {m['name']}")
    print(f"  confident stocks: LONG {confL} · SHORT {confS}")
    # intraday vs positional
    a1=basket(L,"ret_nextday")["avg"]; a5=basket(L,"ret_5d")["avg"]
    verdict="POSITIONAL (multi-day adds most)" if (a5==a5 and a1==a1 and a5>1.5*max(a1,0.01)) else ("INTRADAY-ok (1D captures it)" if (a1==a1 and a1>=0.8*(a5 if a5==a5 else a1)) else "MIXED")
    print(f"  intraday-vs-positional: {verdict}   (LONG avg/signal: 1D {a1:+.2f}% vs 5d {a5:+.2f}%)")
    for lab,d,rc in [("LONG 1D",L,"ret_nextday"),("LONG 3d",L,"ret_3d"),("LONG 5d",L,"ret_5d"),("SHORT 1D",S,"ret_nextday")]:
        print(f"\n  {lab:<9}{'year':<6}{'n':>6}{'hit%':>7}{'avg%':>8}{'totP&L%':>10}{'maxDD%':>9}{'+mo/mo':>9}")
        for y in YEARS+["ALL"]:
            dd=d if y=="ALL" else d[d.year==y]
            b=basket(dd,rc)
            if b["n"]==0: continue
            print(f"  {'':<9}{str(y):<6}{b['n']:>6}{b['hit']:>6.0f}%{b['avg']:>+7.2f}%{b['tot']:>+9.1f}%{b['mdd']:>+8.1f}%{b['posm']:>4}/{b['mo']:<3}")
            rows.append(dict(strategy=sid,view=lab,year=y,n=b["n"],hit_pct=round(b["hit"],1),avg_pct=round(b["avg"],2),
                total_pnl_pct=round(b["tot"],1),max_dd_pct=round(b["mdd"],1),pos_months=b["posm"],months=b["mo"],
                confident_long=confL,confident_short=confS,verdict=verdict))
if WRITE_XL:
    out=os.path.join(os.path.expanduser("~"),"Downloads","STRATEGY_DASHBOARD.xlsx")
    pd.DataFrame(rows).to_excel(out,index=False); print(f"\n  dashboard -> {out}")
else:
    print("\n  (inline only — pass 'xl' to also write Excel)")
