"""Rolling slot-portfolio simulator for Falcon signals — PARAMETERIZED so each knob is one dial.
Leak-free (point-in-time week-to-date features), Jan-Jul 2026 entries (exits spill into Aug).

Model:
  N slots, fixed Rs `alloc` each. Every trading day at 9:15 (=open) fill EMPTY slots from that morning's
  top-N Falcon ranks (signal emitted prior session) not already held. A stopped/exited slot refills NEXT morning.
  Per open position each day (using that day's OHLC):
    - active stop level = trail_stop if armed else entry*(1+stop_pct/100)
    - gap-through: if open <= level -> exit at open ; elif low <= level -> exit at level
    - else update arm/trail for future days, then if held>=max_hold -> exit at close
  Trail models:
    donchian : arm when close>=entry*(1+arm/100); trail=max(entry, prior-10d-low), ratchet up
    arm_give : arm when peak-profit>=arm%; trail locks at max(floor%, peak%-give%) profit, ratchet up
  Cost `cost_pct` round-trip applied to each trade's return. Equity = base + realized + unrealized MTM.
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]

# ============================ CONFIG (Config #1 baseline) ============================
CFG = dict(N=10, alloc=50000.0, base=500000.0, stop_pct=-7.0, max_hold=7,
           trail="donchian", arm=12.0, don_lb=10,            # donchian trail params
           floor=0.0, give=0.0,                               # arm_give params (unused here)
           cost_pct=0.30, start="2026-01-01", end="2026-07-31")
NMAX = 25
# ====================================================================================

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-07-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-08-31' ORDER BY symbol,trade_date", con); con.close()

o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []; SYM = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close","last"), wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan), weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan), weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
    dlow = pd.Series(l).rolling(CFG["don_lb"]).min().shift(1).values   # prior N-day low (Donchian)
    SYM[s] = dict(o=g.open.values.astype(float), h=h, l=l, c=c, dlow=dlow, idx={d:i for i,d in enumerate(g.trade_date)}, dates=list(g.trade_date), n=len(g))
FCpit = feat.drop(columns=WEEKLY).merge(pd.concat(rec, ignore_index=True), on=["symbol","trade_date"], how="left")


def basket(day):
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
    cands=[{"symbol":syms[i],"n_fires":int(fire[i]),"score":float(score[i])} for i in range(len(syms)) if fire[i]>=10]
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:100],key=lambda c:-(c["score"]/max(c["n_fires"],1)))[:NMAX]
    return [c["symbol"] for c in ranked]


def run(cfg):
    N=cfg["N"]; alloc=cfg["alloc"]; base=cfg["base"]; stop=cfg["stop_pct"]; mh=cfg["max_hold"]; cost=cfg["cost_pct"]
    # entry lists: signal emitted sd -> entry next trading day
    sigdays=sorted(FCpit[(FCpit.trade_date>=cfg["start"])&(FCpit.trade_date<=cfg["end"])].trade_date.unique())
    cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
    entry_map={}
    for sd in sigdays:
        ci=cidx.get(sd)
        if ci is None or ci+1>=len(cal): continue
        entry_map[cal[ci+1]]=basket(sd)[:N] if False else basket(sd)   # full ranked; take top per free later
    run_start=cal[cidx[sigdays[0]]+1]; run_end=cal[min(cidx[sigdays[-1]]+mh+2, len(cal)-1)]
    dates=[d for d in cal if run_start<=d<=run_end]
    openpos=[]; realized=0.0; trades=[]; eqrows=[]
    for D in dates:
        # ---- 1. ENTRIES at open ----
        free=N-len(openpos)
        if free>0 and D in entry_map:
            held={p["symbol"] for p in openpos}
            for s in entry_map[D]:
                if free<=0: break
                if s in held: continue
                S=SYM.get(s); i=S["idx"].get(D) if S else None
                if i is None: continue
                e=S["o"][i]
                if e<=0: continue
                openpos.append(dict(symbol=s, ei=i, entry=e, entry_date=D, armed=False, tstop=None, peak=e))
                held.add(s); free-=1
        # ---- 2. MONITOR / EXIT during D ----
        stay=[]
        for p in openpos:
            S=SYM[p["symbol"]]; j=S["idx"].get(D)
            if j is None: stay.append(p); continue
            o,h,l,c=S["o"][j],S["h"][j],S["l"][j],S["c"][j]; e=p["entry"]; held=j-p["ei"]+1
            level = p["tstop"] if p["armed"] else e*(1+stop/100.0)
            exit_px=None; reason=None
            if o<=level: exit_px=o; reason=("trail" if p["armed"] else "stop")
            elif l<=level: exit_px=level; reason=("trail" if p["armed"] else "stop")
            if exit_px is None:
                # update arm / trail for future
                if cfg["trail"]=="donchian":
                    if not p["armed"] and c>=e*(1+cfg["arm"]/100.0): p["armed"]=True
                    if p["armed"]:
                        dl=S["dlow"][j]; newst=max(e, dl) if dl==dl else e
                        p["tstop"]=newst if p["tstop"] is None else max(p["tstop"], newst)
                else:  # arm_give
                    p["peak"]=max(p["peak"], h); pk=(p["peak"]/e-1)*100
                    if not p["armed"] and pk>=cfg["arm"]: p["armed"]=True
                    if p["armed"]:
                        tsp=max(cfg["floor"], pk-cfg["give"]); newst=e*(1+tsp/100.0)
                        p["tstop"]=newst if p["tstop"] is None else max(p["tstop"], newst)
                if held>=mh: exit_px=c; reason="maxhold"
            if exit_px is not None:
                ret=(exit_px/e-1)*100-cost; pnl=alloc*ret/100.0; realized+=pnl
                trades.append(dict(symbol=p["symbol"], entry_date=p["entry_date"], exit_date=D, held=held,
                                   entry=round(e,2), exit=round(exit_px,2), ret_pct=round(ret,2), pnl=round(pnl), reason=reason))
            else:
                stay.append(p)
        openpos=stay
        # ---- 3. equity mark ----
        unre=0.0
        for p in openpos:
            S=SYM[p["symbol"]]; j=S["idx"].get(D)
            if j is not None: unre+=alloc*(S["c"][j]/p["entry"]-1)
        eqrows.append(dict(date=D, equity=base+realized+unre, realized=realized, open_n=len(openpos)))
    return pd.DataFrame(trades), pd.DataFrame(eqrows)


T, EQ = run(CFG)
EQ["mo"]=EQ.date.str[:7]; EQ["peak"]=EQ.equity.cummax(); EQ["dd"]=(EQ.equity/EQ.peak-1)*100
base=CFG["base"]
print("="*96)
print(f"  CONFIG #1  ·  Falcon Top-{CFG['N']} rolling portfolio  ·  Rs{CFG['alloc']:,.0f}/slot on Rs{base:,.0f} base")
print(f"  stop {CFG['stop_pct']}%  ·  max-hold {CFG['max_hold']}d  ·  trail={CFG['trail']}(arm +{CFG['arm']}%, {CFG['don_lb']}d-low)  ·  cost {CFG['cost_pct']}%  ·  leak-free Jan-Jul 2026")
print("="*96)
print(f"\n  {'Month':<9}{'trades':>7}{'realized Rs':>14}{'ret% base':>11}{'end equity':>14}{'maxDD%':>9}")
for mo, g in EQ.groupby("mo"):
    tmo=T[T.exit_date.str[:7]==mo]; rp=tmo.pnl.sum()
    print(f"  {mo:<9}{len(tmo):>7}{rp:>+14,.0f}{rp/base*100:>+11.2f}{g.equity.iloc[-1]:>14,.0f}{g.dd.min():>+9.2f}")
tot=T.pnl.sum(); wr=(T.ret_pct>0).mean()*100
byr=T.groupby("reason").agg(n=("pnl","size"), avg=("ret_pct","mean"), pnl=("pnl","sum"))
print("  "+"-"*94)
print(f"  TOTAL   trades {len(T)}  ·  realized Rs{tot:+,.0f}  ·  return on Rs{base:,.0f} base {tot/base*100:+.2f}%  ·  max equity DD {EQ.dd.min():+.2f}%")
print(f"  win-rate {wr:.0f}%  ·  avg win {T[T.ret_pct>0].ret_pct.mean():+.2f}%  ·  avg loss {T[T.ret_pct<=0].ret_pct.mean():+.2f}%  ·  avg hold {T.held.mean():.1f}d")
print(f"\n  exit reasons:")
for r,x in byr.iterrows(): print(f"    {r:<9} n={int(x.n):>4}  avg {x.avg:+.2f}%  total Rs{x.pnl:+,.0f}")
out=os.path.join(os.path.expanduser("~"),"Downloads","FALCON_PORTFOLIO_CONFIG1.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    T.to_excel(w,"trades",index=False); EQ.to_excel(w,"equity_daily",index=False)
print(f"\n  trade log + daily equity -> {out}")
