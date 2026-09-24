"""BEDROCK — the HIGH-CONFIDENCE 2026 monthly view.
Config frozen on 2025-H1 sweep, deployed UNTOUCHED on 2026 (true OOS for BOTH config and pattern). Equal-weight, Rs5L,
net 0.15%, NO leverage, NO self-correction. Also shows the Stage-4 throttled variant side-by-side (safer, lower return)."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; AP = os.path.dirname(os.path.abspath(__file__)); CAP0 = 500000.0
G = pd.read_pickle(os.path.join(AP, "_bedrock_grid_full.pkl")); G["uni_all"] = 1
uc = sqlite3.connect("file:" + os.path.join(ROOT, "data", "db", "kanida_universe.db").replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,close FROM ohlc_daily WHERE trade_date>='2026-01-01' AND trade_date<='2026-08-15'", uc); uc.close()
CLOSE = {(r.symbol, r.trade_date): r.close for r in oh.itertuples()}; alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
KEY = "WAIT30|None|None|15"; CAP = 3; TM = 15; EM = "WAIT30"; LO, HI = "2026-01-01", "2026-07-10"

def run(throttle=False, K=20):
    sub = G[(G.entry_date >= LO) & (G.entry_date <= HI) & (G.uni_all == 1) & (G["rank"] <= CAP)]; sub = sub[np.isfinite(sub[KEY].values)]
    byday = {d: g for d, g in sub.groupby("entry_date")}
    cash = CAP0; openp = []; eqhist = []; peak = CAP0; rows = []; slots = CAP * TM
    for d in [x for x in alldays if LO <= x <= HI]:
        mtm = sum(p["tk"] * (CLOSE.get((p["s"], d), p["px"]) / p["px"]) for p in openp); eq_pre = cash + mtm
        risk_on = True
        if throttle and len(eqhist) >= K: risk_on = eqhist[-1] >= float(np.mean(eqhist[-K:]))
        ent = clo = 0; pnl = 0.0
        if d in byday and risk_on:
            tk = max(eq_pre / slots, 0); i0 = AIDX[d]
            for r in byday[d].itertuples():
                ep = r.ep_map.get(EM)
                if ep is None: continue
                openp.append(dict(s=r.symbol, px=ep, tk=tk, ret=G.loc[r.Index, KEY], xd=alldays[min(i0 + TM - 1, len(alldays) - 1)])); cash -= tk; ent += 1
        keep = []
        for p in openp:
            if p["xd"] <= d: pl = p["tk"] * p["ret"] / 100.0; cash += p["tk"] + pl; pnl += pl; clo += 1
            else: keep.append(p)
        openp = keep
        eq = cash + sum(p["tk"] * (CLOSE.get((p["s"], d), p["px"]) / p["px"]) for p in openp); peak = max(peak, eq); eqhist.append(eq)
        rows.append(dict(date=d, equity=eq, entered=ent, closed=clo, pnl=pnl, dd=(peak - eq) / peak * 100))
    for p in openp: cash += p["tk"] + p["tk"] * p["ret"] / 100.0
    D = pd.DataFrame(rows); D["m"] = D.date.str[:7]
    mo = D.groupby("m").agg(equity=("equity", "last"), entered=("entered", "sum"), closed=("closed", "sum"),
                            pnl=("pnl", "sum"), maxdd=("dd", "max")).reset_index()
    mo["start"] = [CAP0] + mo.equity.tolist()[:-1]; mo["ret"] = (mo.equity - mo.start) / mo.start * 100
    return mo, (eqhist[-1] / CAP0 - 1) * 100, pd.Series(eqhist)

FZ, fret, fe = run(throttle=False)
S4, sret, se = run(throttle=True)
fdd = ((fe.cummax() - fe) / fe.cummax() * 100).max(); sdd = ((se.cummax() - se) / se.cummax() * 100).max()
print("BEDROCK — 2026 MONTHLY (Jan 1 -> Jul 10 2026, true OOS, Rs5L)\n")
print("FROZEN CONFIG (high-confidence): ALL - top-3/day - WAIT30 entry - no target - no stop - exit D15")
print(f"{'Month':<9}{'Trades':>8}{'P&L Rs':>12}{'Return':>10}{'MaxDD':>8}{'End Equity Rs':>16}")
for _, r in FZ.iterrows():
    print(f"{r.m:<9}{int(r.closed):>8}{r.pnl:>12,.0f}{r.ret:>9.2f}%{r.maxdd:>7.1f}%{r.equity:>16,.0f}")
print(f"{'TOTAL':<9}{int(FZ.closed.sum()):>8}{(fe.iloc[-1]-CAP0):>12,.0f}{fret:>9.2f}%{fdd:>7.1f}%{fe.iloc[-1]:>16,.0f}")
print(f"\nSAFER VARIANT (Stage-4 drawdown throttle) — lower return, ~1/3 the drawdown:")
print(f"{'Month':<9}{'Trades':>8}{'P&L Rs':>12}{'Return':>10}{'MaxDD':>8}{'End Equity Rs':>16}")
for _, r in S4.iterrows():
    print(f"{r.m:<9}{int(r.closed):>8}{r.pnl:>12,.0f}{r.ret:>9.2f}%{r.maxdd:>7.1f}%{r.equity:>16,.0f}")
print(f"{'TOTAL':<9}{int(S4.closed.sum()):>8}{(se.iloc[-1]-CAP0):>12,.0f}{sret:>9.2f}%{sdd:>7.1f}%{se.iloc[-1]:>16,.0f}")
out = os.path.join(ROOT, "docs", "ops", "BEDROCK_2026_MONTHLY.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    FZ.to_excel(w, sheet_name="Frozen (high-confidence)", index=False); S4.to_excel(w, sheet_name="Throttled (safer)", index=False)
print(f"\nExcel -> {out}")
