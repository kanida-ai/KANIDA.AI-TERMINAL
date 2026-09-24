"""BEDROCK — STAGE 2 (config sweep) + STAGE 3 (frozen walk-forward deployment).
STAGE 2 : sweep ALL 1,620 configs (3 universes x 5 crowding caps x 4 entry timings x 3 targets x 3 stops x 3 time-exits)
          on 2025-01..2025-06 ONLY. Pick the winner. FREEZE it. Also show how the top-20 cluster (robust vs noise).
STAGE 3 : deploy that frozen config, untouched, on 2025-07 -> 2026-07-10 and report THREE segments:
             A) Jan-Jun 2025  config IN-sample , pattern IN-sample   -> not evidence
             B) Jul-Dec 2025  config OUT-sample, pattern IN-sample   -> partial
             C) Jan-Jul 2026  config OUT-sample, pattern OUT-sample  -> THE REAL NUMBER
Equal-weight sizing, no leverage, Rs5L, net 0.15%. No self-correction yet (that is Stage 4)."""
import os, sqlite3, itertools
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP = os.path.dirname(os.path.abspath(__file__)); CAP0 = 500000.0
ENTRY_MODES = ["OPEN", "WAIT30", "DIP", "BRK"]; TARGETS = [None, 10.0, 15.0]; STOPS = [None, 6.0, 10.0]; TIMES = [5, 10, 15]
CAPS = [3, 5, 10, 20, 999]; UNIS = ["ALL", "FO", "N50"]
SWEEP_LO, SWEEP_HI = "2025-01-01", "2025-06-30"
DEPLOY_LO, DEPLOY_HI = "2025-07-01", "2026-07-10"

G = pd.read_pickle(os.path.join(AP, "_bedrock_grid_full.pkl"))
uc = sqlite3.connect("file:" + os.path.join(ROOT, "data", "db", "kanida_universe.db").replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,close FROM ohlc_daily WHERE trade_date>='2025-01-01' AND trade_date<='2026-08-15'", uc); uc.close()
CLOSE = {(r.symbol, r.trade_date): r.close for r in oh.itertuples()}
alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
def _n(v):
    return None if (v is None or (isinstance(v, float) and not np.isfinite(v))) else v
KEY = lambda em, tg, sp, tm: f"{em}|{_n(tg)}|{_n(sp)}|{int(tm)}"
COMBOS = [(em, tg, sp, tm) for em in ENTRY_MODES for tg in TARGETS for sp in STOPS for tm in TIMES]
print(f"grid: {len(G)} signals  {G.entry_date.min()}..{G.entry_date.max()}")

# pre-slice by universe for speed
G["uni_all"] = 1
UCOL = {"ALL": "uni_all", "FO": "uni_fo", "N50": "uni_n50"}


def simulate(lo, hi, em, tg, sp, tm, cap, uni, cap0=CAP0):
    tg, sp, tm, cap = _n(tg), _n(sp), int(tm), int(cap)
    k = KEY(em, tg, sp, tm)
    sub = G[(G.entry_date >= lo) & (G.entry_date <= hi) & (G[UCOL[uni]] == 1) & (G["rank"] <= cap)]
    sub = sub[np.isfinite(sub[k].values)]
    if sub.empty: return cap0, [], []
    byday = {d: g for d, g in sub.groupby("entry_date")}
    eds = sorted(byday)
    cash = cap0; openp = []; rets = []; eq_track = []
    slots = max(cap if cap != 999 else 20, 1) * tm
    for d in [x for x in alldays if lo <= x <= hi]:
        eq = cash + sum(p["tk"] * (CLOSE.get((p["s"], d), p["px"]) / p["px"]) for p in openp)
        if d in byday:
            tk = max(eq / slots, 0)
            i0 = AIDX[d]
            for r in byday[d].itertuples():
                ep = r.ep_map.get(em)
                if ep is None: continue
                xd = alldays[min(i0 + tm - 1, len(alldays)-1)]
                openp.append(dict(s=r.symbol, px=ep, tk=tk, ret=getattr(r, "_%d" % 0, None) or G.loc[r.Index, k], xd=xd))
                cash -= tk
        keep = []
        for p in openp:
            if p["xd"] <= d:
                cash += p["tk"] + p["tk"] * p["ret"] / 100.0; rets.append(p["ret"])
            else: keep.append(p)
        openp = keep
        eq_track.append((d, cash + sum(p["tk"] * (CLOSE.get((p["s"], d), p["px"]) / p["px"]) for p in openp)))
    for p in openp:
        cash += p["tk"] + p["tk"] * p["ret"] / 100.0; rets.append(p["ret"])
    return cash, rets, eq_track


# ---------------- STAGE 2: sweep ----------------
print(f"\nSTAGE 2 — sweeping {len(UNIS)*len(CAPS)*len(COMBOS)} configs on {SWEEP_LO}..{SWEEP_HI} ...", flush=True)
SWEEP_CACHE = os.path.join(AP, "_bedrock_sweep2025H1.pkl")
if os.path.exists(SWEEP_CACHE):
    R = pd.read_pickle(SWEEP_CACHE); print("  (loaded cached sweep)")
else:
    res = []
    for uni in UNIS:
        for cap in CAPS:
            for (em, tg, sp, tm) in COMBOS:
                fin, rets, _ = simulate(SWEEP_LO, SWEEP_HI, em, tg, sp, tm, cap, uni)
                if len(rets) < 20: continue
                res.append(dict(uni=uni, cap=cap, entry=em, target=tg, stop=sp, texit=tm,
                                ret=(fin/CAP0-1)*100, trades=len(rets), win=np.mean([r > 0 for r in rets])*100))
    R = pd.DataFrame(res).sort_values("ret", ascending=False).reset_index(drop=True); R.to_pickle(SWEEP_CACHE)
print(f"  evaluated {len(R)} configs with >=20 trades")
print(f"\n  TOP 15 on the sweep window (Jan-Jun 2025):")
print(f"  {'#':<4}{'uni':<5}{'cap':>5}{'entry':>8}{'tgt':>6}{'stop':>6}{'exit':>6}{'return':>10}{'trades':>8}{'win':>6}")
for i, r in R.head(15).iterrows():
    print(f"  {i+1:<4}{r.uni:<5}{str(r.cap):>5}{r.entry:>8}{str(_n(r.target) or '-'):>6}{str(_n(r.stop) or '-'):>6}{r.texit:>6}{r.ret:>9.2f}%{r.trades:>8}{r.win:>5.0f}%")
print("\n  How the TOP-20 cluster (is the winner robust or a fluke?):")
top = R.head(20)
for col in ["uni", "cap", "entry", "target", "stop", "texit"]:
    vc = top[col].astype(str).value_counts()
    print(f"    {col:<7}: " + " | ".join(f"{k}={v}/20" for k, v in vc.items()))
BEST = R.iloc[0]
print(f"\n  >>> FROZEN CONFIG: {BEST.uni} · top-{'all' if BEST.cap==999 else BEST.cap}/day · {BEST.entry} entry · "
      f"target {_n(BEST.target) or '-'} · stop {_n(BEST.stop) or '-'} · exit D{BEST.texit}")

# ---------------- STAGE 3: frozen deployment ----------------
def segment(lo, hi, label, r, cap0=CAP0):
    fin, rets, eq = simulate(lo, hi, r.entry, r.target, r.stop, r.texit, r.cap, r.uni, cap0)
    if not eq: print(f"  {label:<46} (no trades)"); return cap0
    e = pd.Series([x[1] for x in eq]); dd = ((e.cummax()-e)/e.cummax()*100).max()
    print(f"  {label:<46} Rs{fin:>10,.0f}  {(fin/cap0-1)*100:>+8.2f}%  trades {len(rets):>4}  "
          f"win {np.mean([x>0 for x in rets])*100:>3.0f}%  maxDD {dd:>5.1f}%")
    return fin

NAIVE = dict(uni="ALL", cap=999, entry="OPEN", target=None, stop=None, texit=15)
print(f"\nSTAGE 3 — deploying the FROZEN config (no re-tuning, no self-correction)\n")
print("  FROZEN CONFIG")
segment(SWEEP_LO, SWEEP_HI, "A) Jan-Jun 2025  [config IN, pattern IN]  <-not evidence", BEST)
segment("2025-07-01", "2025-12-31", "B) Jul-Dec 2025  [config OUT, pattern IN] <-partial", BEST)
segment("2026-01-01", "2026-07-10", "C) Jan-Jul 2026  [config OUT, pattern OUT] <-REAL", BEST)
fin_all = segment(DEPLOY_LO, DEPLOY_HI, "   Jul2025->Jul2026 continuous (Rs5L start)", BEST)
print("\n  NAIVE BASELINE (ALL, every name, OPEN, exit D15) — the control")
nb = pd.Series(NAIVE)
segment(SWEEP_LO, SWEEP_HI, "A) Jan-Jun 2025", nb)
segment("2025-07-01", "2025-12-31", "B) Jul-Dec 2025", nb)
segment("2026-01-01", "2026-07-10", "C) Jan-Jul 2026", nb)
segment(DEPLOY_LO, DEPLOY_HI, "   Jul2025->Jul2026 continuous", nb)

# monthly for the frozen config over the deployment
_, _, eq = simulate(DEPLOY_LO, DEPLOY_HI, BEST.entry, BEST.target, BEST.stop, BEST.texit, BEST.cap, BEST.uni)
E = pd.DataFrame(eq, columns=["date", "equity"]); E["m"] = E.date.str[:7]
mo = E.groupby("m").equity.last().reset_index()
mo["start"] = [CAP0] + mo.equity.tolist()[:-1]
mo["ret"] = (mo.equity - mo.start)/mo.start*100
print(f"\n  MONTHLY — frozen config deployed Jul2025 -> Jul2026 (Rs5L)")
print(f"  {'Month':<9}{'Return':>10}{'Equity Rs':>14}")
for _, r in mo.iterrows(): print(f"  {r.m:<9}{r.ret:>9.2f}%{r.equity:>14,.0f}")
out = os.path.join(ROOT, "docs", "ops", "BEDROCK_STAGE23.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    R.to_excel(w, sheet_name="Sweep All Configs", index=False)
    R.head(20).to_excel(w, sheet_name="Top 20", index=False)
    mo.to_excel(w, sheet_name="Monthly Frozen", index=False)
print(f"\nExcel -> {out}")
