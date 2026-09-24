"""BEDROCK — the THREE real ways to run it, side by side. Same agent, same true-OOS 2026 window, Rs5,00,000, net 0.15%.
  1) POSITIONAL  CNC 1x  : swing hold (D15), overnight, NO leverage (CNC)
  2) INTRADAY    MIS 1x  : same-day, buy-on-dip + 1% stop, cash only
  3) INTRADAY    MIS 5x  : same-day, buy-on-dip + 1% stop, full 5x intraday margin
Config comes from the robust selection already done. Reports config + performance + monthly for each. -> Excel."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; AP = os.path.dirname(os.path.abspath(__file__)); CAP0 = 500000.0
LO, HI = "2026-01-01", "2026-07-10"
uc = sqlite3.connect("file:" + os.path.join(ROOT, "data", "db", "kanida_universe.db").replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,close FROM ohlc_daily WHERE trade_date>='2026-01-01' AND trade_date<='2026-08-15'", uc); uc.close()
CLOSE = {(r.symbol, r.trade_date): r.close for r in oh.itertuples()}; alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}

def swing(em, tg, sp, tm, cap):                          # CNC positional (1x, overnight)
    G = pd.read_pickle(os.path.join(AP, "_bedrock_grid_full.pkl")); G["uni_all"] = 1
    k = f"{em}|{tg}|{sp}|{tm}"
    sub = G[(G.entry_date >= LO) & (G.entry_date <= HI) & (G.uni_all == 1) & (G["rank"] <= cap)]; sub = sub[np.isfinite(sub[k].values)]
    byday = {d: g for d, g in sub.groupby("entry_date")}; cash = CAP0; openp = []; eqs = []; nt = 0; wins = 0; slots = cap * tm
    for d in [x for x in alldays if LO <= x <= HI]:
        eq = cash + sum(p["tk"] * (CLOSE.get((p["s"], d), p["px"]) / p["px"]) for p in openp)
        if d in byday:
            tk = max(eq / slots, 0); i0 = AIDX[d]
            for r in byday[d].itertuples():
                ep = r.ep_map.get(em)
                if ep is None: continue
                openp.append(dict(s=r.symbol, px=ep, tk=tk, ret=G.loc[r.Index, k], xd=alldays[min(i0+tm-1, len(alldays)-1)])); cash -= tk
        keep = []
        for p in openp:
            if p["xd"] <= d: cash += p["tk"]+p["tk"]*p["ret"]/100.0; nt += 1; wins += p["ret"] > 0
            else: keep.append(p)
        openp = keep
        eqs.append((d, cash + sum(p["tk"]*(CLOSE.get((p["s"], d), p["px"])/p["px"]) for p in openp)))
    for p in openp: nt += 1; wins += p["ret"] > 0
    return pd.DataFrame(eqs, columns=["date", "equity"]), nt, (wins/nt*100 if nt else 0)

def intraday(em, tg, sp, cap, lev):                      # MIS intraday (same-day close)
    G = pd.read_pickle(os.path.join(AP, "_bedrock_intraday_grid.pkl")); G["uni_all"] = 1
    k = f"{em}|{tg}|{sp}"
    sub = G[(G.entry_date >= LO) & (G.entry_date <= HI) & (G.uni_all == 1) & (G["rank"] <= cap)]; sub = sub[np.isfinite(sub[k].values)]
    byday = {d: g for d, g in sub.groupby("entry_date")}; eq = CAP0; eqs = []; nt = 0; wins = 0
    for d in sorted(byday):
        if not (LO <= d <= HI): continue
        r = byday[d].nsmallest(min(len(byday[d]), cap), "rank")[k].values
        eq *= (1 + np.mean(r)*lev/100.0); eqs.append((d, eq)); nt += len(r); wins += (r > 0).sum()
    return pd.DataFrame(eqs, columns=["date", "equity"]), nt, (wins/nt*100 if nt else 0)

def metrics(E, nt, win, name, cfg):
    E = E.copy(); E["m"] = E.date.str[:7]; fin = E.equity.iloc[-1]
    dd = ((E.equity.cummax()-E.equity)/E.equity.cummax()*100).max()
    mo = E.groupby("m").equity.last(); mo = pd.concat([pd.Series({"2026-00": CAP0}), mo]).pct_change().dropna()*100
    dret = E.equity.pct_change().dropna()*100
    return dict(name=name, cfg=cfg, ret=(fin/CAP0-1)*100, final=fin, dd=dd, win=win, trades=nt,
                worst_m=mo.min(), worst_d=dret.min()*(1), worst_d_rs=dret.min()/100*CAP0, monthly=mo)

R = [
 metrics(*swing("WAIT30", "None", "None", 15, 3), "1) POSITIONAL · CNC 1x",
         "buy near 9:45, hold ~3 weeks, top-3 stocks/day, no leverage"),
 metrics(*intraday("DIP", "None", "1.0", 3, 1), "2) INTRADAY · MIS 1x",
         "buy-on-dip + 1% stop, sell by close, top-3/day, cash only"),
 metrics(*intraday("DIP", "None", "1.0", 3, 5), "3) INTRADAY · MIS 5x",
         "same as (2) but 5x intraday margin"),
]
print("BEDROCK — THREE REAL SETTINGS, side by side (true-OOS 2026, Rs5,00,000)\n" + "="*92)
print(f"{'':<26}{'CNC 1x (positional)':>22}{'MIS 1x (intraday)':>22}{'MIS 5x (intraday)':>22}")
rowdefs = [("Return", lambda r: f"{r['ret']:+.1f}%"),
           ("Rs5,00,000 becomes", lambda r: f"Rs{r['final']:,.0f}"),
           ("Max drawdown", lambda r: f"-{r['dd']:.1f}%"),
           ("Worst month", lambda r: f"{r['worst_m']:+.1f}%"),
           ("Worst single day", lambda r: f"Rs{r['worst_d_rs']:,.0f}"),
           ("Win rate", lambda r: f"{r['win']:.0f}%"),
           ("Trades (6 months)", lambda r: f"{int(r['trades'])}"),
           ("Under your 20% DD rule?", lambda r: "YES" if r['dd'] < 20 else "NO")]
for lab, f in rowdefs:
    print(f"{lab:<26}" + "".join(f"{f(r):>22}" for r in R))
print("\nCONFIGURATION")
for r in R: print(f"  {r['name']:<24}: {r['cfg']}")
print("\nMONTHLY RETURN (2026)")
months = sorted(set().union(*[set(r["monthly"].index) for r in R]))
hdr = ["CNC 1x", "MIS 1x", "MIS 5x"]
print(f"  {'Month':<9}" + "".join(f"{h:>16}" for h in hdr))
for m in months:
    cells = []
    for r in R:
        cells.append(f"{r['monthly'][m]:+.1f}%" if m in r['monthly'] else "-")
    print(f"  {m[-2:]:<9}" + "".join(f"{c:>16}" for c in cells))
out = os.path.join(ROOT, "docs", "ops", "BEDROCK_THREE.xlsx")
tab = pd.DataFrame([{"Setting": r["name"], "Config": r["cfg"], "Return%": round(r["ret"], 1), "Final_Rs": round(r["final"]),
                     "MaxDD%": round(r["dd"], 1), "WorstMonth%": round(r["worst_m"], 1), "WorstDay_Rs": round(r["worst_d_rs"]),
                     "Win%": round(r["win"]), "Trades": r["trades"], "Under20pct": ("YES" if r["dd"] < 20 else "NO")} for r in R])
mm = pd.DataFrame({r["name"]: r["monthly"] for r in R})
with pd.ExcelWriter(out, engine="openpyxl") as w:
    tab.to_excel(w, "Comparison", index=False); mm.to_excel(w, "Monthly")
print(f"\nExcel -> {out}")
