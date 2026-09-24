"""Drill into one strategy: confident stock lists (long & short) with per-stock best config, and config mix.
Usage: python drill.py S05_trendlines"""
import os, sqlite3, sys, warnings
warnings.filterwarnings("ignore")
import pandas as pd
HERE=os.path.dirname(os.path.abspath(__file__)); LIBDB=os.path.join(HERE,"library.db")
sid=sys.argv[1] if len(sys.argv)>1 else "S05_trendlines"
cn=sqlite3.connect(LIBDB)
ss=pd.read_sql_query("SELECT * FROM stock_stats WHERE strat_id=?",cn,params=(sid,))
me=pd.read_sql_query("SELECT * FROM meta WHERE strat_id=?",cn,params=(sid,)); cn.close()
print(f"  {sid} — {me.iloc[0]['name']}\n")
for direction,metric,mlab in [("LONG","avg_3d","3d"),("SHORT","avg_nextday","1D")]:
    d=ss[(ss.direction==direction)&(ss.confident==1)].sort_values(metric,ascending=False)
    print(f"  === {direction} confident stocks: {len(d)}  (sorted by avg {mlab}) ===")
    print(f"  {'symbol':<12}{'config':<12}{'#sig':>5}{'1D win%':>8}{'1D avg%':>8}{'3d win%':>8}{'3d avg%':>8}{'yrs':>6}")
    for _,r in d.head(30).iterrows():
        print(f"  {r['symbol']:<12}{str(r['config']):<12}{int(r['n']):>5}{r['win_nextday']:>7.0f}%{r['avg_nextday']:>+7.2f}%{r['win_3d']:>7.0f}%{r['avg_3d']:>+7.2f}%  {int(r['worked_years'])}/{int(r['years'])}")
    if len(d)>30: print(f"  ... +{len(d)-30} more")
    cc=d.config.value_counts()
    print(f"  config mix: "+", ".join(f"{k}:{v}" for k,v in cc.items())+"\n")
