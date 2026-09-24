"""BEDROCK INTRADAY — Stage 2 (config sweep) + Stage 3 (frozen true-OOS) + 2026 monthly, at 1x then 5x (MIS).
Same discipline as the Swing pipeline. INTRADAY = enter & exit same session, so capital recycles daily (no overnight):
each day deploy across up to `cap` best names, exit all by close, compound. Sweep on 2025-H1, freeze, deploy OOS 2026.
Equal-weight, Rs5L, net 0.15%. Reports the true-OOS 2026 result + monthly, at 1x and 5x."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; AP = os.path.dirname(os.path.abspath(__file__)); CAP0 = 500000.0
ENTRY_MODES = ["OPEN", "SPLIT", "WAIT30", "DIP", "BRK"]; TARGETS = [None, 1.0, 2.0, 3.0]; STOPS = [None, 1.0, 2.0]
CAPS = [3, 5, 10, 20, 999]; UNIS = ["ALL", "FO", "N50"]
SWEEP_LO, SWEEP_HI = "2025-01-01", "2025-06-30"
G = pd.read_pickle(os.path.join(AP, "_bedrock_intraday_grid.pkl")); G["uni_all"] = 1
UCOL = {"ALL": "uni_all", "FO": "uni_fo", "N50": "uni_n50"}
_n = lambda v: None if (v is None or (isinstance(v, float) and not np.isfinite(v))) else v
KEY = lambda em, tg, sp: f"{em}|{_n(tg)}|{_n(sp)}"
COMBOS = [(em, tg, sp) for em in ENTRY_MODES for tg in TARGETS for sp in STOPS]
edays = sorted(G.entry_date.unique())

def simulate(lo, hi, em, tg, sp, cap, uni, lev=1.0):
    k = KEY(em, tg, sp); cap = int(cap)
    sub = G[(G.entry_date >= lo) & (G.entry_date <= hi) & (G[UCOL[uni]] == 1) & (G["rank"] <= cap)]
    sub = sub[np.isfinite(sub[k].values)]
    if sub.empty: return CAP0, [], []
    byday = {d: g for d, g in sub.groupby("entry_date")}
    eq = CAP0; rets = []; track = []
    for d in [x for x in edays if lo <= x <= hi]:
        if d in byday:
            g = byday[d]; n = min(len(g), cap if cap != 999 else len(g))
            r = g.nsmallest(n, "rank")[k].values                 # equal weight across the day's names
            day_ret = np.mean(r) * lev                            # same-day close-out; lev scales the day's P&L
            eq *= (1 + day_ret/100.0); rets.extend(list(r))
        track.append((d, eq))
    return eq, rets, track

def line(lab, lo, hi, r, lev):
    fin, rets, tr = simulate(lo, hi, r["entry"], r["target"], r["stop"], r["cap"], r["uni"], lev)
    if not tr: print(f"    {lab:<34} (no trades)"); return
    e = pd.Series([x[1] for x in tr]); dd = ((e.cummax()-e)/e.cummax()*100).max()
    print(f"    {lab:<34} {(fin/CAP0-1)*100:>+8.2f}%   maxDD {dd:>5.1f}%   trades {len(rets):>4}   win {np.mean([x>0 for x in rets])*100:>3.0f}%")

if __name__ == "__main__":
    print(f"grid: {len(G)} signals  {G.entry_date.min()}..{G.entry_date.max()}\n")
    # STAGE 2 sweep (1x) on 2025 H1
    print(f"STAGE 2 — sweeping {len(UNIS)*len(CAPS)*len(COMBOS)} intraday configs on {SWEEP_LO}..{SWEEP_HI} (1x)...", flush=True)
    res = []
    for uni in UNIS:
        for cap in CAPS:
            for (em, tg, sp) in COMBOS:
                fin, rets, _ = simulate(SWEEP_LO, SWEEP_HI, em, tg, sp, cap, uni, 1.0)
                if len(rets) < 20: continue
                res.append(dict(uni=uni, cap=cap, entry=em, target=tg, stop=sp, ret=(fin/CAP0-1)*100,
                                trades=len(rets), win=np.mean([r > 0 for r in rets])*100))
    R = pd.DataFrame(res).sort_values("ret", ascending=False).reset_index(drop=True)
    print(f"  evaluated {len(R)} configs\n  TOP 12 (Jan-Jun 2025, 1x):")
    print(f"  {'#':<4}{'uni':<5}{'cap':>5}{'entry':>8}{'tgt':>6}{'stop':>6}{'return':>10}{'trades':>8}{'win':>6}")
    for i, r in R.head(12).iterrows():
        print(f"  {i+1:<4}{r.uni:<5}{str(r.cap):>5}{r.entry:>8}{str(_n(r.target) or '-'):>6}{str(_n(r.stop) or '-'):>6}{r.ret:>9.2f}%{r.trades:>8}{r.win:>5.0f}%")
    print("\n  TOP-20 cluster (robust vs fluke?):")
    for col in ["uni", "cap", "entry", "target", "stop"]:
        vc = R.head(20)[col].astype(str).value_counts(); print(f"    {col:<7}: " + " | ".join(f"{k}={v}" for k, v in vc.items()))
    BEST = R.iloc[0].to_dict()
    print(f"\n  >>> FROZEN INTRADAY CONFIG: {BEST['uni']} · top-{'all' if BEST['cap']==999 else BEST['cap']}/day · "
          f"{BEST['entry']} · target {_n(BEST['target']) or 'close'} · stop {_n(BEST['stop']) or '-'}")

    # STAGE 3 — frozen deployment, 1x and 5x
    for lev in [1.0, 5.0]:
        print(f"\nSTAGE 3 — FROZEN config deployed ({lev:.0f}x):")
        line("A) Jan-Jun 2025 [IN/IN] not evid", SWEEP_LO, SWEEP_HI, BEST, lev)
        line("B) Jul-Dec 2025 [OUT/IN] partial", "2025-07-01", "2025-12-31", BEST, lev)
        line("C) Jan-Jul 2026 [OUT/OUT] REAL", "2026-01-01", "2026-07-10", BEST, lev)
    print("\n  control — naive intraday (ALL, all names, OPEN, same-day close):")
    NAIVE = dict(uni="ALL", cap=999, entry="OPEN", target=None, stop=None)
    line("C) Jan-Jul 2026  1x", "2026-01-01", "2026-07-10", NAIVE, 1.0)

    # ENTRY HEAD-TO-HEAD: hold structure fixed, vary ONLY the entry, across all segments (isolate the entry effect)
    print("\n  ENTRY HEAD-TO-HEAD (fixed: ALL - top-20/day - 1% stop - same-day close; only entry varies):")
    print(f"    {'entry':<8}{'A) 2025H1':>12}{'B) 2025H2':>12}{'C) 2026 OOS':>13}{'  <- robust if all 3 hold'}")
    for em in ["OPEN", "SPLIT", "DIP", "WAIT30"]:
        r = dict(uni="ALL", cap=20, entry=em, target=None, stop=1.0)
        outs = []
        for lo, hi in [("2025-01-01", "2025-06-30"), ("2025-07-01", "2025-12-31"), ("2026-01-01", "2026-07-10")]:
            fin, rets, _ = simulate(lo, hi, em, None, 1.0, 20, "ALL", 1.0)
            outs.append(f"{(fin/CAP0-1)*100:>+10.2f}%" if rets else "    (none)")
        print(f"    {em:<8}{outs[0]:>12}{outs[1]:>12}{outs[2]:>13}")

    # 2026 monthly for frozen config at 1x and 5x
    def monthly(lev):
        _, _, tr = simulate("2026-01-01", "2026-07-10", BEST["entry"], BEST["target"], BEST["stop"], BEST["cap"], BEST["uni"], lev)
        E = pd.DataFrame(tr, columns=["date", "equity"]); E["m"] = E.date.str[:7]
        mo = E.groupby("m").equity.last().reset_index(); mo["start"] = [CAP0]+mo.equity.tolist()[:-1]
        mo["ret"] = (mo.equity-mo.start)/mo.start*100; return mo
    m1, m5 = monthly(1.0), monthly(5.0)
    print(f"\n  2026 MONTHLY — frozen intraday config (Rs5L):")
    print(f"  {'Month':<9}{'1x ret':>9}{'1x equity':>13}{'5x ret':>10}{'5x equity':>14}")
    for i in range(len(m1)):
        print(f"  {m1.m[i]:<9}{m1.ret[i]:>8.2f}%{m1.equity[i]:>13,.0f}{m5.ret[i]:>9.2f}%{m5.equity[i]:>14,.0f}")
    print(f"  {'TOTAL':<9}{(m1.equity.iloc[-1]/CAP0-1)*100:>8.2f}%{m1.equity.iloc[-1]:>13,.0f}"
          f"{(m5.equity.iloc[-1]/CAP0-1)*100:>9.2f}%{m5.equity.iloc[-1]:>14,.0f}")
    out = os.path.join(ROOT, "docs", "ops", "BEDROCK_INTRADAY_2026.xlsx")
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        R.to_excel(w, "Sweep", index=False); m1.to_excel(w, "Monthly 1x", index=False); m5.to_excel(w, "Monthly 5x", index=False)
    print(f"\nExcel -> {out}")
