"""FIRST-15-MINUTE microstructure of every Jan-2025 operator pick on the TRADE day (09:15-09:29, 1-min).
Gap vs prior-day close, first-15 trend, candle sizes, up/down candles, close position. Then compare the
signature of WINNERS vs LOSERS (full-day outcome). Writes Excel. Read-only. (Post-09:15 = exit/mgmt signal.)"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP)
from weekly_dayofweek_fix import OPJAN
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); MDB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
oh=pd.read_sql_query("SELECT symbol,trade_date,close FROM ohlc_daily WHERE trade_date>='2024-12-01' AND trade_date<='2025-01-31'",con); con.close()
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
pclose={(r.symbol,r.trade_date):r.close for r in oh.itertuples(index=False)}
mcon=sqlite3.connect("file:"+MDB.replace("\\","/")+"?mode=ro",uri=True)
def bars(sym,date):
    return pd.read_sql_query("SELECT bar_time,open,high,low,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",
                             mcon,params=(sym,f"{date} 09:15:00",f"{date} 15:29:00"))
rows=[]
for td in sorted(OPJAN):
    if not (cidx.get(td) and cidx[td]-1>=0): continue
    sd=cal[cidx[td]-1]
    for sym,mrank in OPJAN[td].items():
        df=bars(sym,td)
        if len(df)<16: continue
        f=df.iloc[:15]  # first 15 one-minute candles 09:15..09:29
        o0=float(f.open.iloc[0]); c14=float(f.close.iloc[-1])
        if o0<=0: continue
        prev=pclose.get((sym,sd))
        gap=(o0/prev-1)*100 if prev else np.nan
        f15_ret=(c14-o0)/o0*100
        hi=float(f.high.max()); lo=float(f.low.min())
        f15_high=(hi-o0)/o0*100; f15_low=(lo-o0)/o0*100
        rng=(f.high-f.low)/f.open*100
        c1_body=(f.close.iloc[0]-f.open.iloc[0])/f.open.iloc[0]*100
        c1_range=(f.high.iloc[0]-f.low.iloc[0])/f.open.iloc[0]*100
        up=int((f.close.values>f.open.values).sum())
        close_pos=(c14-lo)/(hi-lo) if hi>lo else np.nan   # where 09:29 close sits in first-15 range
        eod=float(df.close.iloc[-1]); day_ret=(eod-o0)/o0*100
        rows.append(dict(trade_date=td,symbol=sym,manual_rank=mrank,
            prev_close=round(prev,2) if prev else None,open_0915=round(o0,2),
            gap_pct=round(gap,2) if gap==gap else None,
            first15_ret_pct=round(f15_ret,2),first15_high_pct=round(f15_high,2),first15_low_pct=round(f15_low,2),
            first_candle_body_pct=round(c1_body,2),first_candle_range_pct=round(c1_range,2),
            avg_candle_range_pct=round(rng.mean(),3),up_candles=up,close_pos_in_range=round(close_pos,2) if close_pos==close_pos else None,
            day_ret_pct=round(day_ret,2),outcome=("WIN" if day_ret>0 else "LOSS")))
mcon.close()
M=pd.DataFrame(rows)
def sig(g):
    return pd.Series(dict(n=len(g),
        gap_pct=round(g.gap_pct.mean(),2),first15_ret=round(g.first15_ret_pct.mean(),2),
        first15_high=round(g.first15_high_pct.mean(),2),first15_low=round(g.first15_low_pct.mean(),2),
        first_body=round(g.first_candle_body_pct.mean(),2),avg_candle_rng=round(g.avg_candle_range_pct.mean(),3),
        up_candles=round(g.up_candles.mean(),1),close_pos=round(g.close_pos_in_range.mean(),2)))
G=M.groupby("outcome").apply(sig).reset_index()
print(f"  First-15-min signature of {len(M)} picks — WINNERS vs LOSERS (full-day outcome)\n")
cols=["n","gap_pct","first15_ret","first15_high","first15_low","first_body","avg_candle_rng","up_candles","close_pos"]
print(f"  {'group':<7}"+"".join(f"{c:>15}" for c in cols))
for _,r in G.sort_values("outcome",ascending=False).iterrows():
    print(f"  {r['outcome']:<7}"+"".join(f"{r[c]:>15}" for c in cols))
w=M[M.outcome=='WIN']; l=M[M.outcome=='LOSS']
print(f"\n  READ: winners avg gap {w.gap_pct.mean():+.2f}% / first-15 move {w.first15_ret_pct.mean():+.2f}% / {w.up_candles.mean():.1f}/15 up candles")
print(f"        losers  avg gap {l.gap_pct.mean():+.2f}% / first-15 move {l.first15_ret_pct.mean():+.2f}% / {l.up_candles.mean():.1f}/15 up candles")
# simple separator: does 'green first 15 min' (first15_ret>0) predict the day?
for thr,lab in [(0,"first-15 GREEN (ret>0)")]:
    g=M[M.first15_ret_pct>thr]; b=M[M.first15_ret_pct<=thr]
    print(f"\n  if {lab}: {len(g)} picks, day-win {round((g.day_ret_pct>0).mean()*100)}%, avg day {g.day_ret_pct.mean():+.2f}%")
    print(f"  else (first-15 red)   : {len(b)} picks, day-win {round((b.day_ret_pct>0).mean()*100)}%, avg day {b.day_ret_pct.mean():+.2f}%")
out=os.path.join(os.path.expanduser("~"),"Downloads","JAN2025_PICKS_FIRST15MIN.xlsx")
with pd.ExcelWriter(out) as xw:
    M.sort_values(["trade_date","manual_rank"]).to_excel(xw,sheet_name="first15_per_pick",index=False)
    G.to_excel(xw,sheet_name="winner_vs_loser",index=False)
print(f"\n  full per-pick first-15 data -> {out}")
