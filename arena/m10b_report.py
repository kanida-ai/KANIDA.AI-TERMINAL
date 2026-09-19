"""
M10b proof report — per-stock BEFORE (raw: trade everything) vs AFTER (keep only segments whose
<=2024 ETV was positive). Leak-free: the keep decision uses only <=2024; results shown on sealed 2026
and on 2025-26. Writes reports/m10b_selection.xlsx (Summary + Per_Stock_2026 + Per_Stock_2025_26).
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
    return exi, (gross - COST[order]) * LEV[order]


def bookstats(fnp, segs, nfr, index, eval_idx, keep):
    trades = []; i = 0
    while i < len(eval_idx) - 1:
        gi = eval_idx[i]; firing = [s for s in segs if s in keep and segs[s]["fire"][gi]]
        if not firing: i += 1; continue
        seg = max(firing, key=lambda s: segs[s]["lift"]); m = segs[seg]
        r = sim(fnp, gi, m["d"], m["pct"], m["w"], m["order"], nfr)
        if r is None: i += 1; continue
        exi, net = r; trades.append((index[exi], CAP * net / 100))
        while i < len(eval_idx) and eval_idx[i] <= exi: i += 1
    prof = sum(p for _, p in trades)
    dd = 0.0
    if trades:
        s = pd.Series([p for _, p in sorted(trades)]).cumsum(); dd = float((s - s.cummax()).min())
    return prof, dd, len(trades)


def run_stock(symbol, con, is_fno):
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
    idx = frame.index
    for s in segs:
        m = np.zeros(nfr, bool)
        for cds in segs[s]["conds"]: m |= apply_rule(frame, cds).values
        segs[s]["fire"] = m
    pre = np.where(yr <= 2024)[0]
    for s in segs:
        m = segs[s]; rocs = [r[1] for gi in pre if m["fire"][gi] for r in [sim(fnp, gi, m["d"], m["pct"], m["w"], m["order"], nfr)] if r]
        m["pre_n"] = len(rocs); m["pre_etv"] = float(np.mean(rocs)) if rocs else -1e9
    keep = {s for s in segs if segs[s]["pre_etv"] > 0}
    allseg = set(segs); dropped = sorted(allseg - keep)
    res = {"symbol": symbol, "segments": len(segs), "kept": len(keep),
           "dropped_segments": ", ".join(f"{s}(etv{segs[s]['pre_etv']:+.2f})" for s in dropped) if dropped else ""}
    for tag, ev in (("2026", list(np.where(yr == 2026)[0])), ("2526", list(np.where((yr == 2025) | (yr == 2026))[0]))):
        rp, rd, rn = bookstats(fnp, segs, nfr, idx, ev, allseg)
        sp, sd, sn = bookstats(fnp, segs, nfr, idx, ev, keep)
        res[f"raw_profit_{tag}"] = round(rp); res[f"raw_dd_{tag}"] = round(rd)
        res[f"sel_profit_{tag}"] = round(sp); res[f"sel_dd_{tag}"] = round(sd)
        res[f"raw_retdd_{tag}"] = round(rp / -rd, 2) if rd < 0 else None
        res[f"sel_retdd_{tag}"] = round(sp / -sd, 2) if sd < 0 else None
    return res


def main():
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]
    rows = []
    for n, s in enumerate(syms, 1):
        try:
            r = run_stock(s, con, int(fno.get(s, 0)))
            if r: rows.append(r)
        except Exception: pass
        if n % 100 == 0: print(f"  ...{n}/{len(syms)}", flush=True)
    con.close()
    df = pd.DataFrame(rows)

    def summary(tag, label):
        rp, sp = df[f"raw_profit_{tag}"].sum(), df[f"sel_profit_{tag}"].sum()
        # portfolio DD is computed in the sweep; here we report the sum-of-stock DDs as a per-stock proxy
        return {"Window": label, "Raw profit": round(rp), "Selected profit": round(sp),
                "Profit change %": round((sp / rp - 1) * 100) if rp else None,
                "Raw workers +": int((df[f"raw_profit_{tag}"] > 0).sum()),
                "Selected workers +": int((df[f"sel_profit_{tag}"] > 0).sum())}
    summ = pd.DataFrame([summary("2026", "Sealed 2026 (honest test)"), summary("2526", "2025-26")])
    per26 = df[["symbol", "segments", "kept", "raw_profit_2026", "sel_profit_2026", "raw_retdd_2026",
                "sel_retdd_2026", "dropped_segments"]].sort_values("sel_profit_2026", ascending=False)
    per2526 = df[["symbol", "segments", "kept", "raw_profit_2526", "sel_profit_2526", "raw_retdd_2526",
                  "sel_retdd_2526", "dropped_segments"]].sort_values("sel_profit_2526", ascending=False)
    OUT.mkdir(exist_ok=True)
    with pd.ExcelWriter(OUT / "m10b_selection.xlsx", engine="openpyxl") as xl:
        summ.to_excel(xl, "Summary", index=False)
        per26.to_excel(xl, "Per_Stock_2026", index=False)
        per2526.to_excel(xl, "Per_Stock_2025_26", index=False)
    print("\n=== SUMMARY (only-trade-what-worked vs trade-everything) ===")
    print(summ.to_string(index=False))
    print(f"\nwritten: reports/m10b_selection.xlsx  ({len(df)} workers)")


if __name__ == "__main__":
    main()
