"""JAN 2026 · N=20 SPEC trade-journal DIAGNOSTIC — decompose idle% and win-rate.
Re-runs the exact N=20 spec config, then reports:
  IDLE% split: empty-slot idle (refill lag / pool exhaustion) vs whole-share rounding idle.
  WIN RATE: overall, by exit reason, avg win/loss, expectancy, profit factor.
  STOP DAMAGE: of -7% stop-outs, how many WOULD have finished green by day-7 (dip-then-rip false losses).
  REGIME: universe-wide Jan return + % stocks positive (was the market itself up or down?).
Read-only.
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
N=20; ALLOC=25000.0; STOP=-7.0; ARM=5.0; GIVE=3.0; FLOOR=0.0; MAXHOLD=7; COST=0.30; LTP_MAX=25000.0; NMAX=60

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-02-28'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-03-15' ORDER BY symbol,trade_date", con); con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []; SYM = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values; cl = g.close.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close","last"), wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(cl-lo)/(hi-lo),np.nan), weekly_range_pct=np.where(cl>0,(hi-lo)/cl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(cl/sm-1)*100,np.nan), weekly_breakout_20w=np.where(ph==ph,(cl>ph).astype(float),np.nan))))
    SYM[s] = dict(o=g.open.values.astype(float), h=g.high.values.astype(float), l=g.low.values.astype(float), c=cl, idx={d:i for i,d in enumerate(g.trade_date)}, n=len(g))
FCpit = feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d:i for i,d in enumerate(cal)}

def ranklist(day):
    fd = FCpit[FCpit.trade_date==day]
    if fd.empty: return []
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(day[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
    fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms))
    for p in elig:
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*p["oos_lift"]
    cands=[{"symbol":syms[i],"score":float(score[i]),"nf":int(fire[i])} for i in range(len(syms)) if fire[i]>=10]
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:150],key=lambda c:-(c["score"]/max(c["nf"],1)))[:NMAX]
    return [c["symbol"] for c in ranked]

jan_days = [d for d in cal if "2026-01-01"<=d<="2026-01-31"]
entry_map={D: ranklist(cal[cidx[D]-1]) for D in jan_days if cidx[D]-1>=0}
run_end = cal[min(cidx[jan_days[-1]]+MAXHOLD+1, len(cal)-1)]
dates=[d for d in cal if jan_days[0]<=d<=run_end]

base=N*ALLOC; openpos=[]; realized=0.0; blocked=set(); trades=[]; slot_empty=[]; rounding_idle=[]
for D in dates:
    free=N-len(openpos)
    if free>0 and D in entry_map:
        held={p["symbol"] for p in openpos}
        for s in entry_map[D]:
            if free<=0: break
            if s in held or s in blocked: continue
            S=SYM.get(s); i=S["idx"].get(D) if S else None
            if i is None: continue
            e=S["o"][i]
            if e<=0 or e>LTP_MAX: continue
            sh=int(ALLOC//e)
            if sh<1: continue
            openpos.append(dict(symbol=s, ei=i, entry=e, sh=sh, armed=False, tstop=None, peak=e)); held.add(s); free-=1
    stay=[]
    for p in openpos:
        S=SYM[p["symbol"]]; j=S["idx"].get(D)
        if j is None: stay.append(p); continue
        o,h,l,c=S["o"][j],S["h"][j],S["l"][j],S["c"][j]; e=p["entry"]; held=j-p["ei"]+1
        level=p["tstop"] if p["armed"] else e*(1+STOP/100.0); exit_px=None; reason=None
        if o<=level: exit_px=o; reason=("trail" if p["armed"] else "stop")
        elif l<=level: exit_px=level; reason=("trail" if p["armed"] else "stop")
        if exit_px is None:
            p["peak"]=max(p["peak"],h); pk=(p["peak"]/e-1)*100
            if not p["armed"] and pk>=ARM: p["armed"]=True
            if p["armed"]:
                tsp=max(FLOOR,pk-GIVE); ns=e*(1+tsp/100.0); p["tstop"]=ns if p["tstop"] is None else max(p["tstop"],ns)
            if held>=MAXHOLD: exit_px=c; reason="maxhold"
        if exit_px is not None:
            ret=(exit_px/e-1)*100-COST; pnl=p["sh"]*(exit_px-e)-COST/100*p["sh"]*e; realized+=pnl
            if reason=="stop": blocked.add(p["symbol"])
            # would-be day-7 outcome had we NOT stopped (dip-then-rip check)
            x7=p["ei"]+MAXHOLD-1; naked7=((S["c"][x7]/e-1)*100-COST) if x7<S["n"] else np.nan
            trades.append(dict(symbol=p["symbol"], reason=reason, held=held, ret_pct=ret, pnl=pnl, naked7=naked7))
        else: stay.append(p)
    openpos=stay
    if jan_days[0]<=D<=jan_days[-1]:
        slot_empty.append(N-len(openpos))
        dep=sum(pp["sh"]*pp["entry"] for pp in openpos); filled=len(openpos)
        rounding_idle.append((filled*ALLOC-dep) if filled else 0.0)
T=pd.DataFrame(trades)

# ---------- IDLE DECOMP ----------
mean_empty=np.mean(slot_empty); empty_idle=mean_empty/N*100
round_idle=np.mean(rounding_idle)/base*100
print("="*88); print("  IDLE% DECOMPOSITION  (N=20, base ₹500,000)"); print("="*88)
print(f"  avg filled slots     : {N-mean_empty:.1f} / {N}")
print(f"  empty-slot idle      : {empty_idle:.1f}%   (freed slots awaiting next-day refill + pool exhaustion/blocklist)")
print(f"  whole-share rounding : {round_idle:.1f}%   (₹25k / price not divisible -> leftover cash per slot)")
print(f"  TOTAL idle           : {empty_idle+round_idle:.1f}%  (≈ the 42% reported)")

# ---------- WIN RATE JOURNAL ----------
def stat(g):
    w=g[g.ret_pct>0]; l=g[g.ret_pct<=0]
    return len(g),(len(w)/len(g)*100 if len(g) else 0), w.ret_pct.mean() if len(w) else 0, l.ret_pct.mean() if len(l) else 0
n,wr,aw,al=stat(T)
print("\n"+"="*88); print(f"  WIN-RATE JOURNAL  ({n} trades, overall win {wr:.0f}%)"); print("="*88)
print(f"  {'exit reason':<12}{'n':>5}{'win%':>7}{'avg win%':>10}{'avg loss%':>11}{'P&L ₹':>12}")
for r,g in T.groupby("reason"):
    nn,w,awi,ali=stat(g); print(f"  {r:<12}{nn:>5}{w:>6.0f}%{awi:>+10.2f}{ali:>+11.2f}{g.pnl.sum():>+12,.0f}")
exp=(wr/100*aw)+((1-wr/100)*al)
pf=T[T.pnl>0].pnl.sum()/abs(T[T.pnl<=0].pnl.sum())
print(f"  {'ALL':<12}{n:>5}{wr:>6.0f}%{aw:>+10.2f}{al:>+11.2f}{T.pnl.sum():>+12,.0f}")
print(f"  expectancy/trade {exp:+.2f}%  ·  profit factor {pf:.2f}  ·  avg hold {T.held.mean():.1f}d")

# ---------- STOP DAMAGE (dip-then-rip) ----------
st=T[T.reason=="stop"]; killed=st[st.naked7>0]
print("\n"+"="*88); print("  STOP DAMAGE — dip-then-rip false losses"); print("="*88)
print(f"  −7% stop-outs: {len(st)}   avg realized {st.ret_pct.mean():+.2f}%")
print(f"  of those, WOULD have finished GREEN by day-7 if not stopped: {len(killed)} ({len(killed)/max(len(st),1)*100:.0f}%)")
print(f"  those {len(killed)} 'killed winners' would-be avg day-7: {killed.naked7.mean():+.2f}%  (locked as ~−7% instead)")

# ---------- REGIME ----------
print("\n"+"="*88); print("  REGIME — was the market itself up or down in January?"); print("="*88)
jm=[]
for s,S in SYM.items():
    i0=S["idx"].get("2026-01-01") or S["idx"].get(jan_days[0]); i1=S["idx"].get(jan_days[-1])
    if i0 and i1 and i1>i0 and S["c"][i0]>0: jm.append((S["c"][i1]/S["c"][i0]-1)*100)
jm=np.array(jm)
print(f"  universe ({len(jm)} stocks) mean Jan return: {jm.mean():+.2f}%   ·   % stocks positive: {(jm>0).mean()*100:.0f}%")
print(f"  -> a long book can't win when only {(jm>0).mean()*100:.0f}% of stocks rose.")
