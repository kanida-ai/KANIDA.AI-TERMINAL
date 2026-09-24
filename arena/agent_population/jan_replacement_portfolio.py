"""JAN 2026 · fully-invested REPLACEMENT portfolio (user's spec), leak-free, whole-share.
Seed month-start with Top-N ranked; ₹25k/slot; -7% stop; arm5/give3 trailing; max-hold 7 sessions.
Exited slot refilled NEXT session by next-ranked not-held name. Stopped-out names blocked rest of month.
Exclude LTP>₹25,000. Runs N=20 and N=50, each WITH spec (stop+trail) and NAKED (no stop/trail) for contrast.
Reports Jan return on base, max drawdown, exit-reason mix, win-rate, turnover, idle cash. Read-only.
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
ALLOC = 25000.0; STOP = -7.0; ARM = 5.0; GIVE = 3.0; FLOOR = 0.0; MAXHOLD = 7; COST = 0.30
LTP_MAX = 25000.0; NMAX = 60

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
    SYM[s] = dict(o=g.open.values.astype(float), h=g.high.values.astype(float), l=g.low.values.astype(float), c=cl,
                  idx={d:i for i,d in enumerate(g.trade_date)}, dates=list(g.trade_date), n=len(g))
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

# precompute entry ranking per trading day in Jan window (signal from prior session)
jan_entry_days = [d for d in cal if "2026-01-01" <= d <= "2026-01-31"]
entry_map = {}
for D in jan_entry_days:
    ci = cidx[D]
    if ci-1 >= 0: entry_map[D] = ranklist(cal[ci-1])
run_end = cal[min(cidx[jan_entry_days[-1]]+MAXHOLD+1, len(cal)-1)]
dates = [d for d in cal if jan_entry_days[0] <= d <= run_end]

def simulate(N, use_exits):
    base = N*ALLOC; openpos=[]; realized=0.0; blocked=set(); trades=[]; eq=[]
    for D in dates:
        # ENTRIES at open
        free = N-len(openpos)
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
                openpos.append(dict(symbol=s, ei=i, entry=e, sh=sh, entry_date=D, armed=False, tstop=None, peak=e))
                held.add(s); free-=1
        # MONITOR / EXIT
        stay=[]
        for p in openpos:
            S=SYM[p["symbol"]]; j=S["idx"].get(D)
            if j is None: stay.append(p); continue
            o,h,l,c=S["o"][j],S["h"][j],S["l"][j],S["c"][j]; e=p["entry"]; held=j-p["ei"]+1
            exit_px=None; reason=None
            if use_exits:
                level = p["tstop"] if p["armed"] else e*(1+STOP/100.0)
                if o<=level: exit_px=o; reason=("trail" if p["armed"] else "stop")
                elif l<=level: exit_px=level; reason=("trail" if p["armed"] else "stop")
                if exit_px is None:
                    p["peak"]=max(p["peak"], h); pk=(p["peak"]/e-1)*100
                    if not p["armed"] and pk>=ARM: p["armed"]=True
                    if p["armed"]:
                        tsp=max(FLOOR, pk-GIVE); ns=e*(1+tsp/100.0)
                        p["tstop"]=ns if p["tstop"] is None else max(p["tstop"], ns)
            if exit_px is None and held>=MAXHOLD: exit_px=c; reason="maxhold"
            if exit_px is not None:
                pnl=p["sh"]*(exit_px-e) - COST/100*p["sh"]*e; realized+=pnl
                if reason=="stop": blocked.add(p["symbol"])
                trades.append(dict(symbol=p["symbol"], held=held, ret_pct=(exit_px/e-1)*100-COST, pnl=pnl, reason=reason))
            else: stay.append(p)
        openpos=stay
        unre=sum(p["sh"]*(SYM[p["symbol"]]["c"][SYM[p["symbol"]]["idx"][D]]-p["entry"]) for p in openpos if D in SYM[p["symbol"]]["idx"])
        deployed=sum(p["sh"]*p["entry"] for p in openpos)
        eq.append(dict(date=D, equity=base+realized+unre, deployed=deployed, openn=len(openpos)))
    T=pd.DataFrame(trades); E=pd.DataFrame(eq); E["peak"]=E.equity.cummax(); E["dd"]=(E.equity/E.peak-1)*100
    jan=T  # (all exits; Jan-seeded)
    tot=T.pnl.sum(); wr=(T.ret_pct>0).mean()*100 if len(T) else 0
    idle=(1-E.deployed.mean()/base)*100
    return dict(N=N, exits=use_exits, base=base, trades=len(T), total=tot, ret=tot/base*100, maxdd=E.dd.min(),
                win=wr, avghold=T.held.mean() if len(T) else 0, idle=idle,
                mix=T.groupby("reason").pnl.agg(['size','sum']).to_dict('index') if len(T) else {}), T, E

print("="*100)
print("  JAN 2026 · fully-invested replacement portfolio · ₹25k/slot · leak-free · whole-share")
print("  spec = −7% stop + arm5/give3 trail + max-hold 7 ;  naked = no stop/no trail (hold to day-7)")
print("="*100)
print(f"  {'config':<26}{'slots':>6}{'base ₹':>11}{'trades':>8}{'Jan ret%':>10}{'maxDD%':>9}{'win%':>7}{'avgHold':>9}{'idle%':>7}")
allres=[]
for N in [20, 50]:
    for ux in [True, False]:
        r,T,E = simulate(N, ux); allres.append((r,T))
        lab = f"N={N} {'SPEC(stop+trail)' if ux else 'NAKED(no stop)'}"
        print(f"  {lab:<26}{N:>6}{r['base']:>11,.0f}{r['trades']:>8}{r['ret']:>+10.2f}{r['maxdd']:>+9.2f}{r['win']:>6.0f}%{r['avghold']:>9.1f}{r['idle']:>6.1f}%")
print("\n  exit-reason mix (P&L ₹) — the −7% stop's cost:")
for r,T in allres:
    if not r['exits']: continue
    m=r['mix']; parts=" · ".join(f"{k}: n={v['size']} ₹{v['sum']:+,.0f}" for k,v in m.items())
    print(f"    N={r['N']} SPEC → {parts}")
out=os.path.join(os.path.expanduser("~"),"Downloads","FALCON_JAN2026_REPLACEMENT_PORTFOLIO.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    for r,T in allres:
        T.to_excel(w, f"N{r['N']}_{'spec' if r['exits'] else 'naked'}"[:31], index=False)
print(f"\n  trade logs -> {out}")
