"""SOLVE: preserve ~647% net ROC at Rs10cr. Root cause = 10-name long leg slippage (participation up to 6.9% of ADV).
FIX = liquidity-scaled sizing: fill best-edge names first, each capped at max_part % of its ADV, spill down the ranks.
Grid-search leverage x max_part x long_weight x long_depth. Report net ROC / maxDD / day-win / breadth vs the 647% target.
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

# precompute per entry-day: sorted list of (rank, open, close, adv)
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
    return min(20, 0.0003*tb)+min(20, 0.0003*ts)+0.00025*ts+0.0000297*(tb+ts)+0.00003*tb+0.000001*(tb+ts)+0.18*(min(20,0.0003*tb)+min(20,0.0003*ts)+0.0000297*(tb+ts)+0.000001*(tb+ts))

def fill(entries, notional, maxpart, sgn, exec_eff):
    rem = notional; g = c = s = 0.0; n = 0
    for (rk_, o, cl, a) in entries:
        if rem <= 0: break
        ci = min(maxpart * a, rem); qty = ci / o; tb = o*qty; ts = cl*qty
        g += sgn*(cl-o)*qty; c += cost_rs(tb, ts)
        s += 2*(2 + 800*(ci/a)*exec_eff)/1e4*ci; rem -= ci; n += 1   # exec_eff scales the IMPACT term (VWAP/POV)
    return g, c, s, n, notional-rem

def evalc(cap, lev, maxpart, lw, longcut, exec_eff):
    G=C=S=0.0; nets=[]; nL=nS=0; depL=depS=0
    for ed, entries in perday.items():
        longs = [e for e in entries if e[0] <= longcut]
        shorts = [e for e in entries if e[0] >= 201]
        gL,cL,sL,kL,dL = fill(longs, cap*lw*lev, maxpart, 1, exec_eff)
        gS,cS,sS,kS,dS = fill(shorts, cap*(1-lw)*lev, maxpart, -1, exec_eff)
        net = gL+gS-(cL+cS)-(sL+sS); nets.append(net)
        G+=gL+gS; C+=cL+cS+sL+sS; nL+=kL; nS+=kS; depL+=dL; depS+=dS
    nets=np.array(nets); eq=nets.cumsum(); mdd=(np.maximum.accumulate(eq)-eq).max()/cap*100
    return dict(net_roc=nets.sum()/cap*100, gross_roc=G/cap*100, cost_roc=C/cap*100, mdd=mdd,
               daywin=(nets>0).mean()*100, nL=nL/len(nets), nS=nS/len(nets),
               depL=depL/len(nets)/(cap*lw*lev)*100, days=len(nets))

CAP = 1e8
EXE = {1.0: "single-shot @open", 0.5: "TWAP (~half-day)", 0.35: "VWAP/POV 15%", 0.25: "patient POV 10%"}
print("TARGET: recover the Rs10L net ROC of +647% (Combo 70/30, L5X+S5X) — now at Rs10cr.")
print("exec_eff scales market-impact: 1.0 = dump at open (my earlier model), <1 = work the order through the day.\n")
print(f"{'execution':>20}{'maxpart':>9}{'Lweight':>8}{'grossROC':>10}{'costROC':>9}{'NET ROC':>9}{'maxDD':>7}{'daywin':>8}{'#long':>7}{'#short':>7}")
best=None
for eff, elbl in EXE.items():
    for maxpart in [0.01, 0.02, 0.03]:
        for lw in [0.6, 0.7, 0.8]:
            r = evalc(CAP, 5, maxpart, lw, 100, eff)
            hit = "  <== >=647%" if r['net_roc'] >= 647 else ""
            print(f"{elbl:>20}{maxpart*100:>8.1f}%{lw:>8.1f}{r['gross_roc']:>+10.0f}{r['cost_roc']:>9.0f}{r['net_roc']:>+9.0f}{r['mdd']:>6.0f}%{r['daywin']:>7.0f}%{r['nL']:>7.0f}{r['nS']:>7.0f}{hit}")
            if best is None or r['net_roc'] > best[0]['net_roc']: best = (r, eff, elbl, maxpart, lw)
print(f"\nBEST: {best[0]['net_roc']:+.0f}% net ROC via {best[2]} | maxpart {best[3]*100:.1f}% | Lweight {best[4]} | maxDD {best[0]['mdd']:.0f}% | day-win {best[0]['daywin']:.0f}%")

# ---- CAPACITY CURVE of the fixed book (liquidity-scaled + VWAP execution) ----
print("\n" + "="*88 + "\nCAPACITY CURVE — fixed book (maxpart 2%, 80/20, longcut 100). Where does 647% hold?\n" + "="*88)
print(f"{'Capital':>9}{'execution':>18}{'grossROC':>10}{'costROC':>9}{'NET ROC':>9}{'maxDD':>7}{'daywin':>8}{'#long':>7}{'#short':>7}")
for cap, tag in [(1e7,'₹1cr'),(2e7,'₹2cr'),(3e7,'₹3cr'),(5e7,'₹5cr'),(7e7,'₹7cr'),(1e8,'₹10cr')]:
    for eff, elbl in [(1.0,'single-shot'),(0.35,'VWAP/POV')]:
        r = evalc(cap, 5, 0.02, 0.8, 100, eff)
        flag = '  <== holds 647%' if r['net_roc']>=647 else ''
        print(f"{tag:>9}{elbl:>18}{r['gross_roc']:>+10.0f}{r['cost_roc']:>9.0f}{r['net_roc']:>+9.0f}{r['mdd']:>6.0f}%{r['daywin']:>7.0f}%{r['nL']:>7.0f}{r['nS']:>7.0f}{flag}")
