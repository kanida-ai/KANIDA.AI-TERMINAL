"""
M12 — EVENT module. Grow the READY cohort by letting each worker trade in the calendar regime where
its edge is cleanest. Event flags are 100% leak-free (derived from the date index / known-ahead
corp-action ex-dates). Policy stays a-priori: the regime is chosen on <=2024 ONLY, then the worker is
re-graded by the SAME 6-test readiness gauntlet on sealed 2026. We measure how many NEAR -> READY.

Per worker: a-priori keep segments (<=2024 ETV>0) -> choose ONE event overlay (or none) that maximises
the worker's <=2024 ETV (min sample + margin, so it must genuinely beat "trade everything") -> run the
gauntlet on 2025 & 2026 with that overlay. Overlay menu is small & economically motivated (overfit guard).
Writes reports/m12_event_readiness.csv and prints baseline (no event) vs M12 tier counts.
"""
import sys, json, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\scripts")))
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\kanida_engine")))
from mine_phase1 import apply_rule
import features as FE

SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
KDB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
OUT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\reports")
CAP = 100_000.0
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
LEV = {"MIS": 5.0, "CNC": 1.0, "NRML": 5.0}; COST = {"MIS": 0.08, "CNC": 0.15, "NRML": 0.05}
# gauntlet thresholds (identical to worker_readiness.py)
SELECT_ETV_MIN = 0.0; MIN_TRADES = 8; RETDD_BAR = 1.0; WORST_CAP = -40.0
# event-selection guards
SEL_MIN_N = 15; SEL_MARGIN = 0.15         # overlay must have >=15 pre-trades AND beat "none" ETV by >=0.15%/trade


def route(d, w, is_fno):
    if w == 1: return "MIS", ("LONG" if d == "up" else "SHORT")
    if d == "up": return ("NRML" if is_fno else "CNC"), "LONG"
    return "NRML", "SHORT"


def sim(fnp, i, d, pct, w, order, n):
    O, H, L, C = fnp
    if i + 1 >= n: return None
    entry = O[i + 1]
    if not np.isfinite(entry) or entry <= 0: return None
    long = (d == "up"); tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
    ex = None; exi = min(i + w, n - 1)
    for x in range(i + 1, min(i + 1 + w, n)):
        hi, lo = H[x], L[x]
        if long and hi >= tgt: ex, exi = tgt, x; break
        if (not long) and lo <= tgt: ex, exi = tgt, x; break
        exi = x
    if ex is None: ex = C[exi]
    gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
    return exi, gross, order


def pnl_of(tr, cm=1.0): return [(xi, CAP * ((g - COST[o] * cm) * LEV[o]) / 100) for xi, g, o in tr]


def stats(trades):
    if not trades: return 0.0, 0.0, 0, -1e9
    ser = pd.Series([p for _, p in sorted(trades)])
    prof = float(ser.sum()); dd = float((ser.cumsum() - ser.cumsum().cummax()).min())
    worst = float(min(p for _, p in trades)) / CAP * 100
    return prof, dd, len(trades), worst


def event_flags(idx, ex_dates):
    """Leak-free calendar/known-ahead event masks aligned to the frame index."""
    n = len(idx); dow = idx.dayofweek.values
    f = {"none": np.ones(n, bool), "nmon": dow != 0, "nfri": dow != 4}
    xw = np.zeros(n, bool); me = np.zeros(n, bool)
    per = idx.to_period("M"); dfp = pd.DataFrame({"pos": np.arange(n), "d": idx, "p": per})
    for _, g in dfp.groupby("p", sort=False):
        thu = g[g.d.dt.dayofweek == 3]
        ed = thu.d.max() if len(thu) else g.d.max()          # last Thursday (or last td) = monthly expiry
        wk = g[(g.d >= ed - pd.Timedelta(days=4)) & (g.d <= ed)]
        xw[wk.pos.values] = True
        me[g.pos.values[-3:]] = True                          # last 3 trading days of month
    f["xw"] = xw; f["nxw"] = ~xw; f["me"] = me; f["nme"] = ~me
    # pre-ex-date: within 3 trading days before a corp-action ex-date (known in advance)
    pex = np.zeros(n, bool)
    if ex_dates:
        exset = pd.to_datetime(sorted(ex_dates))
        pos = {d: k for k, d in enumerate(idx)}
        order = list(idx)
        for ed in exset:
            # find trading days strictly before ed within 3 sessions
            j = np.searchsorted(order, ed)
            for k in range(max(0, j - 3), j):
                pex[k] = True
    f["npex"] = ~pex
    return f


MENU = ["none", "xw", "nxw", "me", "nme", "nmon", "nfri", "npex"]


def run_stock(symbol, con, is_fno, ex_dates):
    rows = con.execute("SELECT target,rule_json,lift_tr FROM unified_patterns WHERE symbol=? AND promoted=1",
                       (symbol,)).fetchall()
    if not rows: return None
    segs = {}
    for t, rj, lt in rows:
        if t not in TGT: continue
        d, pct, w = TGT[t]; order, dl = route(d, w, is_fno); k = f"{order} {dl} {w}D"
        s = segs.setdefault(k, {"conds": [], "lift": 0, "d": d, "pct": pct, "w": w, "order": order})
        s["conds"].append([tuple(c) for c in json.loads(rj)]); s["lift"] = max(s["lift"], lt or 0)
    frame = FE.load_frame(symbol, lookback_N=5)
    if frame.empty: return None
    yr = frame["year"].values; nfr = len(frame)
    fnp = (frame["_o"].values, frame["_h"].values, frame["_l"].values, frame["_c"].values)
    for s in segs:
        m = np.zeros(nfr, bool)
        for cds in segs[s]["conds"]: m |= apply_rule(frame, cds).values
        segs[s]["fire"] = m
    pre = np.where(yr <= 2024)[0]; keep = set()
    for s in segs:
        m = segs[s]; rocs = [r[1] for gi in pre if m["fire"][gi] for r in [sim(fnp, gi, m["d"], m["pct"], m["w"], m["order"], nfr)] if r]
        net = [(g - COST[m["order"]]) * LEV[m["order"]] for g in rocs]
        if net and float(np.mean(net)) > SELECT_ETV_MIN: keep.add(s)
    ev = event_flags(frame.index, ex_dates)

    def chosen_trade(gi):
        firing = [s for s in segs if s in keep and segs[s]["fire"][gi]]
        if not firing: return None
        seg = max(firing, key=lambda s: segs[s]["lift"]); m = segs[seg]
        r = sim(fnp, gi, m["d"], m["pct"], m["w"], m["order"], nfr)
        return None if r is None else r  # (exi, gross, order)

    # ---- choose overlay on <=2024 (approx ETV = mean net roc on eligible signal days) ----
    pre_tr = [(gi, chosen_trade(gi)) for gi in pre]
    pre_tr = [(gi, (t[1] - COST[t[2]]) * LEV[t[2]]) for gi, t in pre_tr if t]   # (gi, net_roc)
    def ov_etv(o):
        vals = [nr for gi, nr in pre_tr if ev[o][gi]]
        return (float(np.mean(vals)) if vals else -1e9, len(vals))
    base_etv, _ = ov_etv("none")
    best = "none"; best_etv = base_etv
    for o in MENU:
        if o == "none": continue
        e, c = ov_etv(o)
        if c >= SEL_MIN_N and e >= base_etv + SEL_MARGIN and e > best_etv:
            best, best_etv = o, e

    def book(eval_idx, overlay):
        mask = ev[overlay]; tr = []; i = 0
        while i < len(eval_idx) - 1:
            gi = eval_idx[i]
            if not mask[gi]: i += 1; continue
            firing = [s for s in segs if s in keep and segs[s]["fire"][gi]]
            if not firing: i += 1; continue
            seg = max(firing, key=lambda s: segs[s]["lift"]); m = segs[seg]
            r = sim(fnp, gi, m["d"], m["pct"], m["w"], m["order"], nfr)
            if r is None: i += 1; continue
            exi, g, o = r; tr.append((exi, g, o))
            while i < len(eval_idx) and eval_idx[i] <= exi: i += 1
        return tr

    def grade(overlay):
        t26 = book(list(np.where(yr == 2026)[0]), overlay); t25 = book(list(np.where(yr == 2025)[0]), overlay)
        p26, dd26, n26, worst = stats(pnl_of(t26)); p25, _, _, _ = stats(pnl_of(t25))
        p26_2x, _, _, _ = stats(pnl_of(t26, 2.0))
        rr = (p26 / -dd26) if dd26 < 0 else (float("inf") if p26 > 0 else 0.0)
        g = [p26 > 0, n26 >= MIN_TRADES, rr >= RETDD_BAR, (p25 > 0 and p26 > 0), p26_2x > 0, worst >= WORST_CAP]
        passed = sum(g); tier = "READY" if passed == 6 else ("NEAR" if passed >= 4 else "NOT READY")
        return dict(profit_2026=round(p26), retdd_2026=round(rr, 2), trades_2026=n26,
                    gates_passed=passed, tier=tier)

    per_overlay = {o: grade(o)["tier"] for o in MENU}     # global test: tier under each single rule
    # leak-free rule evidence: pooled <=2024 net-ROC per overlay (to rank rules WITHOUT touching 2026)
    pre_stats = {o: (float(np.sum([nr for gi, nr in pre_tr if ev[o][gi]])),
                     int(np.sum([1 for gi, nr in pre_tr if ev[o][gi]]))) for o in MENU}
    out = {"symbol": symbol, "base_tier": per_overlay["none"]}
    out.update({f"tier_{o}": per_overlay[o] for o in MENU})
    out.update({f"pre_sum_{o}": pre_stats[o][0] for o in MENU})
    out.update({f"pre_n_{o}": pre_stats[o][1] for o in MENU})
    return out


def main():
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}
    corp = {}
    for sym, ed in kc.execute("SELECT symbol,ex_date FROM corp_actions").fetchall():
        corp.setdefault(sym, []).append(ed)
    kc.close()
    syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]
    rows = []
    for n, s in enumerate(syms, 1):
        try:
            r = run_stock(s, con, int(fno.get(s, 0)), corp.get(s, []))
            if r: rows.append(r)
        except Exception: pass
        if n % 100 == 0: print(f"  ...{n}/{len(syms)}", flush=True)
    con.close()
    df = pd.DataFrame(rows); OUT.mkdir(exist_ok=True); df.to_csv(OUT / "m12_event_readiness.csv", index=False)
    print("\n================ M12 · EVENT MODULE — does any rule grow READY? ================")
    print(f"  baseline READY (no event): {int((df.tier_none=='READY').sum())}\n")
    print(f"  {'GLOBAL rule (all workers)':<28}{'READY':>7}{'NEAR':>7}{'delta READY':>13}")
    base_r = int((df.tier_none == "READY").sum())
    names = {"none":"trade everything","xw":"only expiry week","nxw":"skip expiry week","me":"only month-end",
             "nme":"skip month-end","nmon":"skip Mondays","nfri":"skip Fridays","npex":"skip pre-ex-date"}
    # rank rules by LEAK-FREE <=2024 evidence (pooled ETV = pooled net-ROC / pooled trades)
    pre_rank = {}
    for o in MENU:
        s, nn = df[f"pre_sum_{o}"].sum(), df[f"pre_n_{o}"].sum()
        pre_rank[o] = (s / nn) if nn else -1e9
    for o in MENU:
        r = int((df[f"tier_{o}"] == "READY").sum()); ne = int((df[f"tier_{o}"] == "NEAR").sum())
        print(f"  {names[o]:<28}{r:>7}{ne:>7}{r-base_r:>+13}   <=2024 ETV {pre_rank[o]:+.3f}")
    winner = max(MENU, key=lambda o: pre_rank[o])
    print(f"\n  LEAK-FREE pick (best <=2024 ETV): '{names[winner]}' -> sealed-2026 READY "
          f"{int((df[f'tier_{winner}']=='READY').sum())} (delta {int((df[f'tier_{winner}']=='READY').sum())-base_r:+d})")
    print("\nwritten: reports/m12_event_readiness.csv")


if __name__ == "__main__":
    main()
