"""BEDROCK — STAGE 4: frozen config + DISCIPLINED daily self-correction.
The Stage-2 frozen config is NEVER re-optimised (that was the failed approach — thrashing on noise). Instead ONE
robust overlay modulates RISK, using only the agent's own realised history (no lookahead):
  * EQUITY-TREND THROTTLE: each morning, if yesterday's mark-to-market equity is BELOW its own K-session moving
    average -> risk-OFF: take NO new entries that day (open positions run to their D15 exit). Above -> risk-ON.
    This is the classic 'trade your own equity curve' overlay; it directly targets the Jul2025-Mar2026 bleed.
  * EARNED LEVERAGE: only when risk-ON AND trailing-20 closed trades are net positive AND drawdown < 8%,
    new entries are sized at up to 1.5x; de-levers instantly otherwise. Borrow repaid from profits first.
Compared head-to-head with the Stage-3 FROZEN config (no overlay). Focus metric = segment-B drawdown.
Frozen config: ALL - top-3/day - WAIT30 entry - no target - no stop - exit D15.  Rs5L, net 0.15%."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP = os.path.dirname(os.path.abspath(__file__)); CAP0 = 500000.0
FROZEN = dict(uni="ALL", cap=3, entry="WAIT30", key="WAIT30|None|None|15", tm=15)
G = pd.read_pickle(os.path.join(AP, "_bedrock_grid_full.pkl")); G["uni_all"] = 1
UCOL = {"ALL": "uni_all", "FO": "uni_fo", "N50": "uni_n50"}
uc = sqlite3.connect("file:" + os.path.join(ROOT, "data", "db", "kanida_universe.db").replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,close FROM ohlc_daily WHERE trade_date>='2025-01-01' AND trade_date<='2026-08-15'", uc); uc.close()
CLOSE = {(r.symbol, r.trade_date): r.close for r in oh.itertuples()}
alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}


def run(lo, hi, throttle, use_lev, K=20, lev_max=1.5, dd_gate=8.0):
    k = FROZEN["key"]; cap = FROZEN["cap"]; tm = FROZEN["tm"]; em = FROZEN["entry"]
    sub = G[(G.entry_date >= lo) & (G.entry_date <= hi) & (G[UCOL[FROZEN["uni"]]] == 1) & (G["rank"] <= cap)]
    sub = sub[np.isfinite(sub[k].values)]
    byday = {d: g for d, g in sub.groupby("entry_date")}
    cash = CAP0; borrowed = 0.0; openp = []; rets = []; ledger = []
    eqhist = []; peak = CAP0; daily = []
    slots = cap * tm
    for d in [x for x in alldays if lo <= x <= hi]:
        mtm = sum(p["tk"] * (CLOSE.get((p["s"], d), p["px"]) / p["px"]) for p in openp)
        eq_pre = cash + mtm - borrowed
        # --- throttle from PAST equity only ---
        risk_on = True
        if throttle and len(eqhist) >= K:
            risk_on = eqhist[-1] >= float(np.mean(eqhist[-K:]))
        dd = (peak - eq_pre) / peak * 100 if peak > 0 else 0
        lev = 1.0
        if use_lev and risk_on and len(ledger) >= 20 and np.mean(ledger[-20:]) > 0 and dd < dd_gate:
            lev = lev_max
        entered = 0
        if d in byday and risk_on:
            bp = max(eq_pre, 0) * lev; tk = max(bp / slots, 0); i0 = AIDX[d]
            for r in byday[d].itertuples():
                ep = r.ep_map.get(em)
                if ep is None: continue
                xd = alldays[min(i0 + tm - 1, len(alldays) - 1)]
                openp.append(dict(s=r.symbol, px=ep, tk=tk, ret=G.loc[r.Index, k], xd=xd)); cash -= tk; entered += 1
            if cash < 0: borrowed += -cash; cash = 0.0
        closed = 0
        keep = []
        for p in openp:
            if p["xd"] <= d:
                cash += p["tk"] + p["tk"] * p["ret"] / 100.0; rets.append(p["ret"]); ledger.append(p["ret"]); closed += 1
            else: keep.append(p)
        openp = keep
        if borrowed > 0 and cash > 0: rep = min(borrowed, cash); borrowed -= rep; cash -= rep
        eq = cash + sum(p["tk"] * (CLOSE.get((p["s"], d), p["px"]) / p["px"]) for p in openp) - borrowed
        peak = max(peak, eq); eqhist.append(eq)
        daily.append(dict(date=d, equity=eq, risk=("ON" if risk_on else "OFF"), leverage=lev,
                          entered=entered, closed=closed, open_pos=len(openp), borrowed=round(borrowed, 0),
                          drawdown=round((peak - eq) / peak * 100, 2)))
    for p in openp:
        cash += p["tk"] + p["tk"] * p["ret"] / 100.0; rets.append(p["ret"])
    e = pd.Series(eqhist); mdd = ((e.cummax() - e) / e.cummax() * 100).max() if len(e) else 0
    return dict(final=cash - borrowed, ret=(cash - borrowed) / CAP0 * 100 - 100, trades=len(rets),
                win=(np.mean([x > 0 for x in rets]) * 100 if rets else 0), maxdd=mdd, daily=pd.DataFrame(daily),
                offdays=sum(1 for x in daily if x["risk"] == "OFF"))


def seg(lo, hi, lab):
    fr = run(lo, hi, throttle=False, use_lev=False)
    th = run(lo, hi, throttle=True, use_lev=False)
    s4 = run(lo, hi, throttle=True, use_lev=True)
    print(f"\n  {lab}")
    for nm, r in [("FROZEN (Stage 3)", fr), ("+ throttle only", th), ("+ throttle + earned lev (STAGE 4)", s4)]:
        print(f"    {nm:<36} {r['ret']:>+8.2f}%   maxDD {r['maxdd']:>5.1f}%   trades {r['trades']:>4}   "
              f"win {r['win']:>3.0f}%   risk-OFF days {r['offdays']:>3}")
    return fr, th, s4


print("STAGE 4 — frozen config + disciplined self-correction (equity-trend throttle + earned leverage)")
print("Frozen: ALL - top-3/day - WAIT30 - no target/stop - D15   |   throttle SMA(20)   |   lev<=1.5x when healthy")
seg("2025-01-01", "2025-06-30", "A) Jan-Jun 2025  [config IN, pattern IN]  <- not evidence")
segB = seg("2025-07-01", "2025-12-31", "B) Jul-Dec 2025  [config OUT, pattern IN]  <- THE DRAWDOWN TEST")
segC = seg("2026-01-01", "2026-07-10", "C) Jan-Jul 2026  [config OUT, pattern OUT]  <- the real edge")
cont = seg("2025-07-01", "2026-07-10", "   Jul2025 -> Jul2026 continuous (Rs5L)")

# sensitivity of the throttle window K (is the overlay robust, or K cherry-picked?)
print("\n  Throttle-window sensitivity on the continuous run (K = SMA length):")
print(f"    {'K':>4}{'throttle-only ret':>20}{'maxDD':>9}{'stage4 ret':>13}{'maxDD':>9}")
for K in [10, 15, 20, 25, 30]:
    a = run("2025-07-01", "2026-07-10", True, False, K=K)
    b = run("2025-07-01", "2026-07-10", True, True, K=K)
    print(f"    {K:>4}{a['ret']:>19.2f}%{a['maxdd']:>8.1f}%{b['ret']:>12.2f}%{b['maxdd']:>8.1f}%")

# monthly + journal for the continuous STAGE 4 run -> Excel
_, _, s4 = cont
D = s4["daily"].copy(); D["m"] = D.date.str[:7]
mo = D.groupby("m").agg(equity=("equity", "last"), risk_off=("risk", lambda x: (x == "OFF").sum()),
                        entered=("entered", "sum"), closed=("closed", "sum"), max_dd=("drawdown", "max")).reset_index()
mo["start"] = [CAP0] + mo.equity.tolist()[:-1]; mo["ret"] = (mo.equity - mo.start) / mo.start * 100
frC = run("2025-07-01", "2026-07-10", False, False)["daily"]; frC["m"] = frC.date.str[:7]
frmo = frC.groupby("m").equity.last()
print("\n  MONTHLY — continuous Jul2025->Jul2026 (frozen vs Stage 4):")
print(f"  {'Month':<9}{'FROZEN eq':>13}{'STAGE4 eq':>13}{'S4 ret':>9}{'risk-OFF days':>15}")
for _, r in mo.iterrows():
    print(f"  {r.m:<9}{frmo.get(r.m, np.nan):>13,.0f}{r.equity:>13,.0f}{r.ret:>8.2f}%{int(r.risk_off):>15}")
out = os.path.join(ROOT, "docs", "ops", "BEDROCK_STAGE4.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    mo.to_excel(w, "Monthly", index=False); s4["daily"].to_excel(w, "Daily Journal", index=False)
print(f"\nExcel -> {out}")
