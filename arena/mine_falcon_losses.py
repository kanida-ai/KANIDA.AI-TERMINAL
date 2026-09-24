"""FAMILY 1 — ADVERSARIAL INTEL: mine Falcon-Magnifier's OWN losing legs across Jan-Jun 2026.
Cluster losers by exit-reason, stock, sector, and day-regime (market breadth) to find where Falcon is WEAKEST —
the blueprint for adversarial challengers. Does NOT touch Falcon's logic (only reads its per-leg trade tape).
READ-ONLY on data."""
import os, sqlite3, importlib.util
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
wf = importlib.util.spec_from_file_location("wf", r"C:\Users\SPS\AppData\Local\Temp\claude\C--Users-SPS-Desktop-Kanida-ai-Terminal-Quant-Intelligence-Engine\c73fe1ef-c928-428e-a17b-d7f23047b24b\scratchpad\walk_forward.py")
WF = importlib.util.module_from_spec(wf); wf.loader.exec_module(WF)
V = WF.V; ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06"]
oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
univ = set(r[0] for r in oc.execute("SELECT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1"))
SEC = {r[0]: (r[1] or "NA") for r in oc.execute("SELECT symbol, sector FROM universe_master")}
oc.close()
fb = WF.falcon_baskets_window() if hasattr(WF, "falcon_baskets_window") else None
frank = WF.falcon_ranks_window()

legs = []; day_breadth = {}
for ym in MONTHS:
    bars = WF.load_month(oc if False else sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True), univ, ym)
    panels, days = V.build_panels(univ, bars)
    for d in days:                                                    # market breadth per day = fraction of stocks up
        M = panels[d]; c0 = M["close"].iloc[0]; c1 = M["close"].iloc[-1]
        day_breadth[d] = float((c1 > c0).sum() / max((c1 == c1).sum(), 1))
    mag = V.A1.BasketAgent("Falcon-Magnifier", fb, "MIS", 5, trail=(6, 2, 5, 3))
    mag.cash = V.CAPITAL; mag.pos = {}; mag.eq = []; mag.trades = []; mag.entry = {}; mag.peak = 0.0
    V.run(panels, days, fb, [mag], list(univ), bars, falcon_rank=frank)
    t = pd.DataFrame(mag.trades)
    if len(t): t["ym"] = ym; legs.append(t)
    del bars, panels

L = pd.concat(legs, ignore_index=True)
L["sector"] = L.sym.map(SEC); L["breadth"] = L.day.map(day_breadth); L["win"] = L.gross_pct > 0
tot = L.gross_pct.sum()
print("=" * 84)
print(f"FALCON-MAGNIFIER LOSS MINING — Jan-Jun 2026 | {len(L):,} legs | loss-rate {(~L.win).mean():.0%} | avg leg {L.gross_pct.mean():+.2f}% (1x)")
print(f"  avg WINNER {L[L.win].gross_pct.mean():+.2f}%   avg LOSER {L[~L.win].gross_pct.mean():+.2f}%   total gross {tot:+.0f}% (sum-of-legs 1x)")

print("\n[BY EXIT REASON] where the losses come from:")
print(f"{'reason':<8}{'n':>7}{'loss-rate':>11}{'avg_gross%':>11}{'tot_gross%':>12}")
for r, g in L.groupby("why"):
    print(f"{r:<8}{len(g):>7}{(~g.win).mean()*100:>10.0f}%{g.gross_pct.mean():>+11.2f}{g.gross_pct.sum():>+12.0f}")

print("\n[BY DAY-REGIME] Falcon is a LONG book — does it bleed on down-breadth days?")
L["regime"] = pd.cut(L.breadth, [0, 0.4, 0.55, 1.01], labels=["down-breadth", "neutral", "up-breadth"])
print(f"{'regime':<14}{'legs':>7}{'loss-rate':>11}{'avg_gross%':>11}{'tot_gross%':>12}")
for r, g in L.groupby("regime"):
    print(f"{str(r):<14}{len(g):>7}{(~g.win).mean()*100:>10.0f}%{g.gross_pct.mean():>+11.2f}{g.gross_pct.sum():>+12.0f}")

print("\n[BY SECTOR] worst sectors for Falcon (by total gross drag, min 30 legs):")
sec = L.groupby("sector").agg(n=("gross_pct", "size"), lossrate=("win", lambda x: (~x).mean() * 100), avg=("gross_pct", "mean"), tot=("gross_pct", "sum"))
sec = sec[sec.n >= 30].sort_values("tot")
print(f"{'sector':<22}{'legs':>7}{'loss-rate':>11}{'avg%':>8}{'tot%':>8}")
for s, r in sec.head(8).iterrows():
    print(f"{s:<22}{int(r.n):>7}{r.lossrate:>10.0f}%{r.avg:>+8.2f}{r.tot:>+8.0f}")

print("\n[WORST STOCKS] names Falcon repeatedly loses on (min 3 legs, by total drag):")
stk = L.groupby("sym").agg(n=("gross_pct", "size"), lossrate=("win", lambda x: (~x).mean() * 100), tot=("gross_pct", "sum"))
stk = stk[stk.n >= 3].sort_values("tot")
print(f"{'stock':<14}{'legs':>6}{'loss-rate':>11}{'tot_gross%':>12}")
for s, r in stk.head(12).iterrows():
    print(f"{s:<14}{int(r.n):>6}{r.lossrate:>10.0f}%{r.tot:>+12.0f}")

L.to_pickle(os.path.join(ROOT, "arena", "falcon_loss_legs.pkl"))
print(f"\nsaved per-leg tape -> arena/falcon_loss_legs.pkl  (blueprint for adversarial challengers)")
