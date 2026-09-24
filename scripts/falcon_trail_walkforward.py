"""Walk-forward parameter sweep for the intraday_basket TRAILING engine.

Reuses the falcon_intraday_backtest data (RND DB: ohlc_1min + falcon_signal_day_study,
persona falcon_top10_daily). For EACH trading day it rebuilds the Top-N basket
(equal-allocate ₹CAP, entry = 09:15 OPEN, qty = floor), then simulates the ACTUAL
engine over a parameter grid:
  * per-stock STOP fires intra-bar on the bar LOW  (stock_ret <= -stop_pct)
  * basket arms at +arm_pct, tracks PEAK, exits when G <= max(peak-giveback, floor)
  * basket pre-arm hard STOP at G <= -stop_pct ; square-off at 15:29
Aggregates each combo across all days by MEDIAN / mean / worst-day / %win — robust,
out-of-sample, NOT one-day max. Output: JSON + console leaderboard.
"""
from __future__ import annotations
import sqlite3, math, itertools, json, argparse
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
PERSONA = "falcon_top10_daily"
ALIASES = {"ZOMATO": "ETERNAL"}
OPEN, CLOSE = "09:15:00", "15:29:00"

ARMS   = [0.0075, 0.01, 0.0125, 0.015, 0.02, 0.025]
GIVES  = [0.005, 0.0075, 0.01, 0.015]
FLOORS = [0.0075, 0.01]
STOPS  = [0.01, 0.015, 0.02]
CURRENT = (0.01, 0.01, 0.01, 0.015)   # arm, floor, give, stop (live default)

def load_signals(con, topn):
    out = {}
    for ed, rk, sym in con.execute(
        "SELECT entry_date, engine_rank, symbol FROM falcon_signal_day_study "
        "WHERE persona=? AND engine_rank BETWEEN 1 AND ? ORDER BY entry_date, engine_rank",
        (PERSONA, topn)):
        out.setdefault(ed, []).append(sym)
    return out

def load_day_ohlc(con, day, syms):
    fetch = {ALIASES.get(s, s): s for s in syms}
    ph = ",".join("?" * len(fetch))
    rows = con.execute(
        f"SELECT symbol, substr(bar_time,12,5) hm, open, high, low, close "
        f"FROM ohlc_1min WHERE bar_time BETWEEN ? AND ? AND symbol IN ({ph})",
        [f"{day} {OPEN}", f"{day} {CLOSE}", *fetch.keys()]).fetchall()
    per = {}
    for osym, hm, o, h, l, c in rows:
        per.setdefault(fetch[osym], {})[hm] = (o, h, l, c)
    return per

def build_matrices(per, cap):
    # keep syms with a real 09:15 open; equal-allocate; qty=floor(alloc/entry)
    present = [(s, d["09:15"][0]) for s, d in per.items()
               if "09:15" in d and d["09:15"][0] and d["09:15"][0] > 0]
    if not present:
        return None
    alloc = cap / len(present)
    legs = [(s, e, math.floor(alloc / e)) for s, e in present if math.floor(alloc / e) >= 1]
    if not legs:
        return None
    syms = [s for s, _, _ in legs]
    entry = np.array([e for _, e, _ in legs], float)
    qty = np.array([q for _, _, q in legs], float)
    # master grid: minutes >= 09:15 present anywhere
    grid = sorted({m for s in syms for m in per[s]} | {"09:15"})
    grid = [m for m in grid if m >= "09:15"]
    def series(s, idx):
        vals, last = [], None
        for m in grid:
            v = per[s].get(m)
            if v and v[idx] and np.isfinite(v[idx]): last = v[idx]
            vals.append(last)
        # bfill leading None with entry
        e = per[s]["09:15"][0]
        return [x if x is not None else e for x in vals]
    close = np.column_stack([series(s, 3) for s in syms])  # (min, stock)
    low   = np.column_stack([series(s, 2) for s in syms])
    deployed = float((qty * entry).sum())
    return dict(grid=grid, entry=entry, qty=qty, close=close, low=low, deployed=deployed)

def simulate(M, arm, floor, give, stop):
    entry, qty, close, low, dep = M["entry"], M["qty"], M["close"], M["low"], M["deployed"]
    n = len(M["grid"])
    stop_lvl = entry * (1 - stop)
    open_mask = np.ones(len(entry), bool)
    realized = 0.0; armed = False; peak = 0.0
    for i in range(1, n):
        # per-stock stops on the bar low
        for j in np.where(open_mask)[0]:
            if low[i, j] <= stop_lvl[j]:
                realized += (stop_lvl[j] - entry[j]) * qty[j]
                open_mask[j] = False
        unreal = float(((close[i] - entry) * qty * open_mask).sum())
        G = (realized + unreal) / dep
        if not armed and G <= -stop:
            return G
        if not armed and G >= arm:
            armed = True; peak = G
        if armed:
            peak = max(peak, G)
            if G <= max(peak - give, floor):
                return G
    unreal = float(((close[-1] - entry) * qty * open_mask).sum())
    return (realized + unreal) / dep

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topn", type=int, default=5)
    ap.add_argument("--cap", type=float, default=100000)
    ap.add_argument("--out", default=str(ROOT / "docs" / "ops" / "trail_walkforward.json"))
    a = ap.parse_args()
    con = sqlite3.connect(str(RND))
    sig = load_signals(con, a.topn)
    days = sorted(sig)
    combos = [(arm, fl, gv, st) for arm in ARMS for fl in FLOORS
              for gv in GIVES for st in STOPS if fl <= arm]
    if CURRENT not in combos: combos.append(CURRENT)
    print(f"[*] {len(days)} days x {len(combos)} combos  (topn={a.topn}, cap={a.cap:.0f})")
    acc = {c: [] for c in combos}
    used = 0
    for d in days:
        per = load_day_ohlc(con, d, sig[d])
        M = build_matrices(per, a.cap)
        if not M: continue
        used += 1
        for c in combos:
            acc[c].append(simulate(M, *c) * 100.0)   # % return
        if used % 100 == 0: print(f"    {used} days simulated...")
    con.close()

    # ── TRAIN/TEST split (chronological; acc lists are in day order) ──────────
    split = int(used * 0.70)
    def agg(rets):
        r = np.array(rets); import numpy as _np
        return dict(median=float(_np.median(r)), mean=float(r.mean()),
                    p10=float(_np.percentile(r,10)), worst=float(r.min()),
                    win=float((r>0).mean()), score=float(_np.median(r)+0.5*_np.percentile(r,10)))
    tt = {c: (agg(v[:split]), agg(v[split:])) for c, v in acc.items()}
    train_ranked = sorted(combos, key=lambda c: tt[c][0]["score"], reverse=True)
    best_train = train_ranked[0]
    print(f"\n=== TRAIN/TEST (train={split}d, test={used-split}d) ===")
    print(f"  Best-on-TRAIN combo: arm{best_train[0]*100:.2f} fl{best_train[1]*100:.2f} gv{best_train[2]*100:.2f} st{best_train[3]*100:.2f}")
    bt, be = tt[best_train]
    print(f"    TRAIN: median{bt['median']:.3f}% mean{bt['mean']:.3f}% win{bt['win']*100:.1f}%")
    print(f"    TEST : median{be['median']:.3f}% mean{be['mean']:.3f}% win{be['win']*100:.1f}%  <-- OOS")
    ct, ce = tt[CURRENT]
    print(f"  CURRENT on TEST: median{ce['median']:.3f}% mean{ce['mean']:.3f}% win{ce['win']*100:.1f}%")
    print(f"  OOS uplift (best-train vs current, on TEST): median {be['median']-ce['median']:+.3f}%/day")

    rows = []
    for c, rets in acc.items():
        r = np.array(rets)
        rows.append(dict(arm=c[0], floor=c[1], give=c[2], stop=c[3],
                         n=len(r), mean=float(r.mean()), median=float(np.median(r)),
                         worst=float(r.min()), p10=float(np.percentile(r, 10)),
                         win=float((r > 0).mean()), std=float(r.std())))
    # robust score: median return minus a penalty for tail risk (p10)
    for x in rows: x["score"] = x["median"] + 0.5 * x["p10"]
    rows.sort(key=lambda x: x["score"], reverse=True)
    cur = next(x for x in rows if (x["arm"],x["floor"],x["give"],x["stop"]) == CURRENT)
    out = dict(days=used, topn=a.topn, cap=a.cap, current=cur, leaderboard=rows)
    Path(a.out).write_text(json.dumps(out, indent=2))
    print(f"\n[*] used {used} days. wrote {a.out}\n")
    def line(x): return (f"  arm{x['arm']*100:>5.2f} fl{x['floor']*100:>4.2f} gv{x['give']*100:>5.2f} "
                         f"st{x['stop']*100:>4.2f} | median{x['median']:>6.3f}% mean{x['mean']:>6.3f}% "
                         f"worst{x['worst']:>7.2f}% p10{x['p10']:>6.2f}% win{x['win']*100:>4.1f}% score{x['score']:>6.3f}")
    print("=== TOP 10 by robust score (median + 0.5*p10) ===")
    for x in rows[:10]: print(line(x))
    print("=== CURRENT live params ===")
    print(line(cur), f"  (rank {rows.index(cur)+1}/{len(rows)})")
    print("=== WORST 3 ===")
    for x in rows[-3:]: print(line(x))

if __name__ == "__main__":
    main()
