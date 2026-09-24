"""HONEST reality check the user demanded: real money COMPOUNDS, and 5x leverage can RUIN you.
Take the daily net returns (fraction of equity), compound them geometrically, and ask:
  - what does a person who STARTS on the 1st of each month actually end with?
  - what is the true peak-to-trough drawdown on the real equity curve?
  - does the account get WIPED OUT (margin call: MIS is force-squared-off; equity <= ~0 => ruin)?
Compare the FIXED book (ADV-cap+VWAP, 10cr) vs the raw leveraged book (70/30 L5X+S5X, 10cr).
Read-only; Falcon untouched."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); RDB = os.path.join(ROOT, "data", "db", "falcon_research.db")
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
od = pd.read_sql_query("SELECT symbol,trade_date,open,close,volume FROM ohlc_daily WHERE trade_date BETWEEN '2026-01-01' AND '2026-07-17'", uc); uc.close()
adv = od.assign(tv=od.close * od.volume).groupby("symbol").tv.median().to_dict()
op = od.pivot_table(index="trade_date", columns="symbol", values="open"); cp = od.pivot_table(index="trade_date", columns="symbol", values="close")
cal = sorted(od.trade_date.unique()); nextd = {cal[i]: cal[i + 1] for i in range(len(cal) - 1)}
rc = sqlite3.connect("file:" + RDB.replace("\\", "/") + "?mode=ro", uri=True)
rk = pd.read_sql_query("SELECT signal_date,rank,symbol FROM falcon_full_ranking WHERE signal_date BETWEEN '2025-12-31' AND '2026-07-16'", rc); rc.close()
perday = {}
for sd, g in rk.groupby("signal_date"):
    ed = nextd.get(sd)
    if not ed or ed not in op.index: continue
    rows = []
    for _, r in g.iterrows():
        s = r.symbol
        if s in op.columns and op.at[ed, s] == op.at[ed, s] and cp.at[ed, s] == cp.at[ed, s] and adv.get(s, 0) > 0:
            rows.append((int(r["rank"]), op.at[ed, s], cp.at[ed, s], adv[s]))
    rows.sort(); perday[ed] = rows

def cost_rs(tb, ts):
    return min(20,0.0003*tb)+min(20,0.0003*ts)+0.00025*ts+0.0000297*(tb+ts)+0.00003*tb+0.000001*(tb+ts)+0.18*(min(20,0.0003*tb)+min(20,0.0003*ts)+0.0000297*(tb+ts)+0.000001*(tb+ts))

def fill(entries, notional, maxpart, sgn, eff):
    rem=notional; g=c=0.0
    for (rk_,o,cl,a) in entries:
        if rem<=0: break
        ci=min(maxpart*a,rem); qty=ci/o; tb=o*qty; ts=cl*qty
        g+=sgn*(cl-o)*qty; c+=cost_rs(tb,ts)+2*(2+800*(ci/a)*eff)/1e4*ci; rem-=ci
    return g,c

def daily_ret(cap, mode):
    """Return DataFrame of daily net return as FRACTION of capital (constant-sizing)."""
    rows=[]
    for ed,entries in perday.items():
        if mode=="fixed":
            gL,cL=fill([e for e in entries if e[0]<=100], cap*0.8*5, 0.02, 1, 0.35)
            gS,cS=fill([e for e in entries if e[0]>=201], cap*0.2*5, 0.02, -1, 0.35)
        else:  # raw: top-10 equal, tail equal, 70/30, single-shot, NO adv cap
            gL,cL=fill([e for e in entries if e[0]<=10], cap*0.7*5, 1.0, 1, 1.0)
            gS,cS=fill([e for e in entries if e[0]>=201], cap*0.3*5, 1.0, -1, 1.0)
        rows.append((ed, (gL+gS-cL-cS)/cap))
    D=pd.DataFrame(rows,columns=["d","r"]); D["ym"]=D.d.str[:7]; return D

MLBL={"2026-01":"Jan","2026-02":"Feb","2026-03":"Mar","2026-04":"Apr","2026-05":"May","2026-06":"Jun","2026-07":"Jul"}
def compound_from(D, start_ym):
    sub=D[D.ym>=start_ym].copy(); eq=1.0; peak=1.0; mdd=0.0; ruined=False; wipe_day=None
    for _,row in sub.iterrows():
        eq*=(1+row.r)
        if eq<=0.0 and not ruined: ruined=True; wipe_day=row.d; eq=0.0
        peak=max(peak,eq); mdd=max(mdd,(peak-eq)/peak)
    return eq, mdd, ruined, wipe_day, len(sub)

for cap, mode, name in [(1e8,"fixed","FIXED book (ADV-cap 2% + VWAP, 80/20)  10cr"),
                        (1e8,"raw","RAW leveraged book (top-10 / tail, 70/30, dump@open)  10cr")]:
    D=daily_ret(cap,mode)
    print("="*100 + f"\n{name}\n  worst single day: {D.r.min()*100:+.1f}%   best: {D.r.max()*100:+.1f}%   (fraction of capital, 5x)")
    # sum-of-daily (what I showed before) vs geometric (reality)
    print(f"  SUM-of-daily (what I reported): {D.r.sum()*100:+.0f}%   |  COMPOUNDED reality (start Jan): {(compound_from(D,'2026-01')[0]-1)*100:+.0f}%")
    print(f"\n  START MONTH -> real ending capital on Rs100 deployed that day, true max drawdown, ruin?")
    print(f"  {'start':<7}{'months':>7}{'end Rs':>10}{'net %':>9}{'true maxDD':>12}{'outcome':>12}")
    for ym in MLBL:
        eq,mdd,ruined,wday,n=compound_from(D,ym)
        out="WIPED OUT" if ruined else ("survived" if eq>=1 else "loss")
        print(f"  {MLBL[ym]:<7}{n:>7}{eq*100:>9.0f}{(eq-1)*100:>+9.0f}{mdd*100:>11.0f}%{out:>12}" + (f"  @ {wday}" if ruined else ""))
