"""
KANIDA ARENA v1 — Autonomous Learning / Constitutional layer (worker model).
One worker per stock. Each worker learns, day-by-day (point-in-time), which of its stock's SIGNAL
SEGMENTS are working RECENTLY, and adapts: Keep (trade full), Watch (explore, half size), Retire
(stop) — governed by a frozen constitution. Compares the adaptive ARENA book vs the STATIC baseline
(trade everything, no learning) to prove the self-learning adds return and/or cuts drawdown.

Leak-proof: a segment's fitness at a trade's entry uses ONLY that segment's OWN prior trades, which
are all CLOSED before this entry (segment books are sequential). Hyperparameters are FROZEN a-priori.
Patterns were mined <=2024; the worker's live/learning period is 2025-2026.

Run: python arena.py SYM1 SYM2 ...   -> reports/arena_results.csv
"""
import sys, json, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\scripts")))
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\kanida_engine")))
from mine_phase1 import apply_rule
import features as FE

SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
CAP = 100_000.0
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
LEV = {"MIS": 5.0, "CNC": 1.0, "NRML": 5.0}
COST = {"MIS": 0.08, "CNC": 0.15, "NRML": 0.05}
# ---- FROZEN CONSTITUTION (a-priori; not tuned on the test period) ----
WINDOW = 10          # rolling trades used to judge a segment
MIN_N = 4            # below this -> Watch (explore, half size)
WATCH_WT = 0.5       # exploration weight for thin/uncertain segments


def route(d, w):
    if w == 1: return "MIS", ("LONG" if d == "up" else "SHORT")
    if d == "up": return "CNC", "LONG"
    return "NRML", "SHORT"


def seg_book(frame, conds_list, d, pct, w, order):
    """Sequential single-book for one segment over 2025-2026 -> stream of (entry_d, exit_d, pnl, net_roc, win)."""
    f = frame[frame["year"].isin([2025, 2026])]; days = list(f.index)
    O, H, L, C = frame["_o"], frame["_h"], frame["_l"], frame["_c"]
    lev = LEV[order]; cost = COST[order]; long = (d == "up"); out = []; i = 0
    while i < len(days) - 1:
        t = days[i]
        if not any(apply_rule(frame.loc[[t]], cds).iloc[0] for cds in conds_list):
            i += 1; continue
        ed = days[i + 1]; entry = float(O.loc[ed])
        if not np.isfinite(entry) or entry <= 0:
            i += 1; continue
        tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
        wd = days[i + 1: i + 1 + w]; ex = None; exd = wd[-1]
        for x in wd:
            if long and float(H.loc[x]) >= tgt: ex = tgt; exd = x; break
            if not long and float(L.loc[x]) <= tgt: ex = tgt; exd = x; break
        if ex is None: ex = float(C.loc[exd])
        gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
        net_roc = (gross - cost) * lev
        out.append((ed, exd, CAP * lev * (gross - cost) / 100.0, net_roc, 1 if net_roc > 0 else 0))
        i = days.index(exd) + 1
    return out


def max_dd(equity):
    if not equity: return 0.0
    s = pd.Series([e[1] for e in sorted(equity)]).cumsum()
    return float((s - s.cummax()).min())


def run_stock(symbol, con):
    rows = con.execute("SELECT target,rule_json FROM unified_patterns WHERE symbol=? AND promoted=1", (symbol,)).fetchall()
    if not rows: return None
    byseg = {}
    for t, rj in rows:
        if t in TGT: byseg.setdefault(t, []).append([tuple(c) for c in json.loads(rj)])
    frame = FE.build_from_db(symbol, lookback_N=5)
    if frame.empty: return None
    arena_pnl = static_pnl = 0.0; arena_eq = []; static_eq = []; a_trades = s_trades = 0
    seg_stat = {}
    for t, conds_list in byseg.items():
        d, pct, w = TGT[t]; order, dirlbl = route(d, w)
        book = seg_book(frame, conds_list, d, pct, w, order)
        recent = []
        ap = sp = 0.0; an = 0
        for (ed, exd, pnl, nr, win) in book:
            # constitution: judge this segment from its OWN prior (closed) trades — point-in-time
            if len(recent) < MIN_N:
                wt = WATCH_WT; status = "Watch"
            else:
                avg = np.mean(recent[-WINDOW:])
                wt = 1.0 if avg > 0 else 0.0; status = "Keep" if avg > 0 else "Retire"
            arena_pnl += pnl * wt; static_pnl += pnl
            arena_eq.append((exd, pnl * wt)); static_eq.append((exd, pnl))
            if wt > 0: a_trades += 1
            s_trades += 1; ap += pnl * wt; sp += pnl; an += (wt > 0)
            recent.append(nr)
        seg_stat[f"{order} {dirlbl} {w}D"] = (len(book), round(sp), round(ap), an)
    return {"symbol": symbol, "static_pnl": round(static_pnl), "arena_pnl": round(arena_pnl),
            "static_dd": round(max_dd(static_eq)), "arena_dd": round(max_dd(arena_eq)),
            "static_trades": s_trades, "arena_trades": a_trades, "segments": seg_stat}


def main():
    con = sqlite3.connect(SNR)
    syms = sys.argv[1:] or [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]
    res = []
    for s in syms:
        try:
            r = run_stock(s, con)
            if r: res.append(r); print(f"  {s:11} static Rs{r['static_pnl']:>9,} DD Rs{r['static_dd']:>9,} | "
                                        f"ARENA Rs{r['arena_pnl']:>9,} DD Rs{r['arena_dd']:>9,} | "
                                        f"trades {r['arena_trades']}/{r['static_trades']}", flush=True)
        except Exception as e:
            print(f"  {s}: ERR {str(e)[:80]}")
    con.close()
    df = pd.DataFrame([{k: v for k, v in r.items() if k != "segments"} for r in res])
    df.to_csv(r"C:\Users\SPS\Documents\Kanida_Falcon\reports\arena_results.csv", index=False)
    sp, ap = df.static_pnl.sum(), df.arena_pnl.sum(); sdd, add = df.static_dd.sum(), df.arena_dd.sum()
    print("\n================ ARENA vs STATIC (self-learning value) ================")
    print(f"  STATIC (trade everything) : P&L Rs{sp:>12,}  | summed maxDD Rs{sdd:>12,}")
    print(f"  ARENA  (adaptive worker)  : P&L Rs{ap:>12,}  | summed maxDD Rs{add:>12,}")
    print(f"   delta                    : P&L {(ap-sp)/abs(sp)*100:+.1f}%          | drawdown {(add-sdd)/abs(sdd)*100:+.1f}%")
    print(f"  arena took {df.arena_trades.sum()}/{df.static_trades.sum()} trades "
          f"({df.arena_trades.sum()/df.static_trades.sum()*100:.0f}% — it skipped the rest via Retire)")


if __name__ == "__main__":
    main()
