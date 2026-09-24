"""Query the strategy library: per-stock BEST strategy, and the confident stock x strategy combos.
Run after registering strategies. Writes STRATEGY_LIBRARY_REPORT.xlsx."""
import os, sqlite3
import pandas as pd
HERE=os.path.dirname(os.path.abspath(__file__)); LIBDB=os.path.join(HERE,"library.db")
cn=sqlite3.connect(LIBDB)
meta=pd.read_sql_query("SELECT * FROM meta",cn)
ss=pd.read_sql_query("SELECT * FROM stock_stats",cn); cn.close()
print(f"  LIBRARY: {len(meta)} strategies registered\n")
print(f"  {'strat_id':<16}{'name':<34}{'signals':>8}{'stocks':>8}{'confident':>10}")
for _,m in meta.iterrows():
    print(f"  {m['strat_id']:<16}{m['name'][:33]:<34}{int(m['n_signals']):>8}{int(m['n_stocks']):>8}{int(m['n_confident']):>10}")
conf=ss[ss.confident==1].copy()
print(f"\n  === CONFIDENT stock x strategy combos ({len(conf)}) — ranked by avg 3d% ===")
if len(conf):
    print(f"  {'symbol':<12}{'strategy':<16}{'#sig':>5}{'3d win%':>9}{'avg 3d%':>9}{'yrs':>6}")
    for _,r in conf.sort_values("avg_3d",ascending=False).head(40).iterrows():
        print(f"  {r['symbol']:<12}{r['strat_id']:<16}{int(r['n']):>5}{r['win_3d']:>8.0f}%{r['avg_3d']:>8.2f}%{int(r['worked_years'])}/{int(r['years'])}".rstrip())
# per-stock best strategy (highest avg_3d among confident; fall back to best win_3d if none confident)
best=conf.sort_values("avg_3d",ascending=False).groupby("symbol").head(1)
out=os.path.join(os.path.expanduser("~"),"Downloads","STRATEGY_LIBRARY_REPORT.xlsx")
with pd.ExcelWriter(out) as xw:
    meta.to_excel(xw,sheet_name="strategies",index=False)
    ss.sort_values(["symbol","avg_3d"],ascending=[True,False]).to_excel(xw,sheet_name="stock_x_strategy",index=False)
    conf.sort_values("avg_3d",ascending=False).to_excel(xw,sheet_name="confident_combos",index=False)
    best.sort_values("avg_3d",ascending=False).to_excel(xw,sheet_name="best_strategy_per_stock",index=False)
print(f"\n  report -> {out}")
