"""BEDROCK — RISK PROJECTION for a trader. What a renter should EXPECT to endure, in rupees, from the frozen configs.
Swing (ALL top-3 WAIT30 D15) and Intraday (ALL top-20 DIP close stop1%) over true-OOS 2026, at 1x / 2x / 5x.
Computes: max drawdown + how long it lasted, worst month/day, % losing months, 1-day 95% VaR & CVaR (worst-case daily
rupee loss), and margin-call proximity at leverage. All from the realised daily equity curve. Read-only."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; AP = os.path.dirname(os.path.abspath(__file__)); CAP0 = 500000.0
LO, HI = "2026-01-01", "2026-07-10"
uc = sqlite3.connect("file:" + os.path.join(ROOT, "data", "db", "kanida_universe.db").replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,close FROM ohlc_daily WHERE trade_date>='2026-01-01' AND trade_date<='2026-08-15'", uc); uc.close()
CLOSE = {(r.symbol, r.trade_date): r.close for r in oh.itertuples()}; alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}

def swing_equity(lev=1.0):
    G = pd.read_pickle(os.path.join(AP, "_bedrock_grid_full.pkl")); G["uni_all"] = 1
    k = "WAIT30|None|None|15"; cap = 3; tm = 15
    sub = G[(G.entry_date >= LO) & (G.entry_date <= HI) & (G.uni_all == 1) & (G["rank"] <= cap)]; sub = sub[np.isfinite(sub[k].values)]
    byday = {d: g for d, g in sub.groupby("entry_date")}; cash = CAP0; openp = []; eqs = []; slots = cap * tm
    for d in [x for x in alldays if LO <= x <= HI]:
        mtm = sum(p["tk"] * (CLOSE.get((p["s"], d), p["px"]) / p["px"]) for p in openp); eq = cash + mtm
        if d in byday:
            tk = max(eq * lev / slots, 0); i0 = AIDX[d]
            for r in byday[d].itertuples():
                ep = r.ep_map.get("WAIT30")
                if ep is None: continue
                openp.append(dict(s=r.symbol, px=ep, tk=tk, ret=G.loc[r.Index, k], xd=alldays[min(i0+tm-1, len(alldays)-1)])); cash -= tk
        keep = []
        for p in openp:
            if p["xd"] <= d: cash += p["tk"] + p["tk"]*p["ret"]/100.0
            else: keep.append(p)
        openp = keep
        eqs.append((d, cash + sum(p["tk"]*(CLOSE.get((p["s"], d), p["px"])/p["px"]) for p in openp)))
    return pd.DataFrame(eqs, columns=["date", "equity"])

def intraday_equity(lev=1.0):
    G = pd.read_pickle(os.path.join(AP, "_bedrock_intraday_grid.pkl")); G["uni_all"] = 1
    k = "DIP|None|1.0"; cap = 20
    sub = G[(G.entry_date >= LO) & (G.entry_date <= HI) & (G.uni_all == 1) & (G["rank"] <= cap)]; sub = sub[np.isfinite(sub[k].values)]
    byday = {d: g for d, g in sub.groupby("entry_date")}; eq = CAP0; eqs = []
    for d in [x for x in sorted(byday) if LO <= x <= HI]:
        r = byday[d].nsmallest(min(len(byday[d]), cap), "rank")[k].values
        eq *= (1 + np.mean(r)*lev/100.0); eqs.append((d, eq))
    return pd.DataFrame(eqs, columns=["date", "equity"])

def risk(E, lev, name):
    E = E.copy(); E["ret"] = E.equity.pct_change().fillna(E.equity.iloc[0]/CAP0 - 1)*100
    peak = E.equity.cummax(); ddser = (peak - E.equity)/peak*100; maxdd = ddser.max()
    # drawdown duration (sessions from the peak preceding maxDD to recovery/end)
    imax = ddser.idxmax(); ppeak = E.loc[:imax][E.equity == peak.loc[imax]].index.min()
    rec = E.loc[imax:][E.equity >= peak.loc[imax]].index.min(); dur = (rec - ppeak) if rec == rec else (E.index[-1] - ppeak)
    E["m"] = E.date.str[:7]; mo = E.groupby("m").equity.last(); mo = pd.concat([pd.Series({LO[:7]: CAP0}), mo]).pct_change().dropna()*100
    lose_m = (mo < 0).mean()*100
    var95 = np.percentile(E.ret.dropna(), 5); cvar = E.ret[E.ret <= var95].mean()
    fin = E.equity.iloc[-1]
    print(f"\n  {name} ({lev:.0f}x)  — start Rs{CAP0:,.0f}")
    print(f"    return                : {(fin/CAP0-1)*100:+.1f}%   (Rs{fin-CAP0:+,.0f} -> Rs{fin:,.0f})")
    print(f"    MAX DRAWDOWN          : -{maxdd:.1f}%   = Rs{maxdd/100*peak.loc[imax]:,.0f} peak-to-trough, lasted ~{int(dur)} sessions")
    print(f"    worst month           : {mo.min():+.1f}%   ({mo.idxmin()})   | losing months {lose_m:.0f}%")
    print(f"    worst single day      : {E.ret.min():+.2f}%  = Rs{E.ret.min()/100*CAP0:,.0f} on Rs5L")
    print(f"    typical bad day (VaR95): {var95:+.2f}%  | if it breaches, avg loss (CVaR) {cvar:+.2f}%  = Rs{cvar/100*CAP0:,.0f}")
    if lev >= 5: print(f"    ** margin note: at {lev:.0f}x a -{100/lev:.0f}% adverse move wipes the deposit; worst day was {E.ret.min():+.1f}% **")
    return dict(name=f"{name} {lev:.0f}x", ret=(fin/CAP0-1)*100, maxdd=maxdd, worst_m=mo.min(), worst_d=E.ret.min(), var95=var95, lose_m=lose_m)

print("BEDROCK — RISK PROJECTION (true-OOS 2026, Rs5,00,000 start)")
print("="*74)
rows = []
sw = swing_equity(1.0); rows.append(risk(sw, 1, "SWING"))
idf = intraday_equity(1.0); rows.append(risk(idf, 1, "INTRADAY"))
rows.append(risk(intraday_equity(2.0), 2, "INTRADAY"))
rows.append(risk(intraday_equity(5.0), 5, "INTRADAY"))
R = pd.DataFrame(rows)
print("\n" + "="*74)
print("  CONTROLLED-RISK vs HIGH-REWARD — the trade-off, one line each:")
print(f"  {'Config':<16}{'Return':>9}{'MaxDD':>8}{'Worst mo':>10}{'Worst day':>11}{'Ret/DD':>8}")
for _, r in R.iterrows():
    print(f"  {r['name']:<16}{r.ret:>+8.1f}%{r.maxdd:>7.1f}%{r.worst_m:>+9.1f}%{r.worst_d:>+10.2f}%{r.ret/max(r.maxdd,0.1):>8.2f}")
out = os.path.join(ROOT, "docs", "ops", "BEDROCK_RISK_PROJECTION.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w: R.to_excel(w, "Risk", index=False)
print(f"\n  best return-per-unit-risk = the 'controlled risk, high reward' sweet spot.")
print(f"Excel -> {out}")
