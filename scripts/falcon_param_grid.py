"""Walk-forward parameter grid for the intraday_basket engine — optimize for
CONSISTENCY (not peak return), on CASH (non-MTF) daily portfolio returns.

Grid: initial stop x arm trigger x profit-lock floor x trail giveback, plus a
no-trail (stop+EOD) option (arm=999). Metrics per combo over all days AND a
chronological 70/30 train/test split (no future leakage, no cherry-picking):
  mean/median daily return, %days>=+1%, %positive, worst day, and #months
  passing the ">=+1% on 90% of days" target. Writes docs/ops/param_grid.json.
"""
import sqlite3, math, json
from pathlib import Path
from collections import defaultdict
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
PERSONA = "falcon_top10_daily"; ALIASES = {"ZOMATO": "ETERNAL"}
OPEN, CLOSE = "09:15:00", "15:29:00"; TOPN = 5; CAP = 100000.0

STOPS = [0.015, 0.02, 0.025, 0.03]
ARMS = [0.01, 0.015, 0.02, 0.025, 9.99]   # 9.99 = no trail (stop + EOD only)
GIVES = [0.005, 0.01, 0.015]
FLOORS = [0.005, 0.01]


def load_signals(con):
    out = {}
    for ed, rk, sym in con.execute(
        "SELECT entry_date,engine_rank,symbol FROM falcon_signal_day_study "
        "WHERE persona=? AND engine_rank BETWEEN 1 AND ? ORDER BY entry_date,engine_rank",
        (PERSONA, TOPN)):
        out.setdefault(ed, []).append(sym)
    return out


def load_day(con, day, syms):
    fetch = {ALIASES.get(s, s): s for s in syms}; ph = ",".join("?" * len(fetch))
    rows = con.execute(
        f"SELECT symbol,substr(bar_time,12,5) hm,open,high,low,close FROM ohlc_1min "
        f"WHERE bar_time BETWEEN ? AND ? AND symbol IN ({ph})",
        [f"{day} {OPEN}", f"{day} {CLOSE}", *fetch.keys()]).fetchall()
    per = {}
    for osym, hm, o, h, l, c in rows:
        per.setdefault(fetch[osym], {})[hm] = (o, h, l, c)
    return per


def build(per, order):
    present = [(s, per[s]["09:15"][0]) for s in order
               if s in per and "09:15" in per[s] and per[s]["09:15"][0] and per[s]["09:15"][0] > 0]
    if not present:
        return None
    alloc = CAP / len(present)
    legs = [(s, e, math.floor(alloc / e)) for s, e in present if math.floor(alloc / e) >= 1]
    if not legs:
        return None
    syms = [s for s, _, _ in legs]
    entry = np.array([e for _, e, _ in legs], float); qty = np.array([q for _, _, q in legs], float)
    grid = sorted({m for s in syms for m in per[s]} | {"09:15"}); grid = [m for m in grid if m >= OPEN[:5]]

    def series(s, idx):
        vals, last = [], None
        for m in grid:
            v = per[s].get(m)
            if v and v[idx] and np.isfinite(v[idx]):
                last = v[idx]
            vals.append(last)
        e = per[s]["09:15"][0]
        return [x if x is not None else e for x in vals]

    close = np.column_stack([series(s, 3) for s in syms]); low = np.column_stack([series(s, 2) for s in syms])
    dep = float((qty * entry).sum())
    return dict(entry=entry, qty=qty, close=close, low=low, dep=dep, n=len(grid))


def sim(M, stop, arm, give, floor):
    entry, qty, close, low, dep, n = M["entry"], M["qty"], M["close"], M["low"], M["dep"], M["n"]
    lvl = entry * (1 - stop); mask = np.ones(len(entry), bool); realized = 0.0; armed = False; peak = 0.0
    no_trail = arm >= 9
    for i in range(1, n):
        for j in np.where(mask)[0]:
            if low[i, j] <= lvl[j]:
                realized += (lvl[j] - entry[j]) * qty[j]; mask[j] = False
        if no_trail:
            continue
        G = (realized + float(((close[i] - entry) * qty * mask).sum())) / dep
        if not armed and G <= -stop:
            return G * 100
        if not armed and G >= arm:
            armed = True; peak = G
        if armed:
            peak = max(peak, G)
            if G <= max(peak - give, floor):
                return G * 100
    return (realized + float(((close[-1] - entry) * qty * mask).sum())) / dep * 100


def main():
    con = sqlite3.connect(str(RND)); sig = load_signals(con); days = sorted(sig)
    mats = []; months = []
    for d in days:
        M = build(load_day(con, d, sig[d]), sig[d])
        if M:
            mats.append(M); months.append(d[:7])
    con.close()
    n = len(mats); split = int(n * 0.70)
    print(f"[*] {n} days loaded (train={split}, test={n-split})")
    combos = []
    for st in STOPS:
        for arm in ARMS:
            if arm >= 9:
                combos.append((st, 9.99, 0.0, 0.0))
            else:
                for gv in GIVES:
                    for fl in FLOORS:
                        if fl <= arm:
                            combos.append((st, arm, gv, fl))
    combos = sorted(set(combos))
    print(f"[*] {len(combos)} combos")

    def metrics(rets, mons):
        a = np.array(rets)
        md = defaultdict(list)
        for r, m in zip(rets, mons):
            md[m].append(r)
        month_ge1 = {m: float((np.array(v) >= 1.0).mean() * 100) for m, v in md.items()}
        return dict(mean=float(a.mean()), median=float(np.median(a)),
                    ge1=float((a >= 1.0).mean() * 100), pos=float((a > 0).mean() * 100),
                    worst=float(a.min()), months_pass=sum(1 for v in month_ge1.values() if v >= 90),
                    n_months=len(month_ge1), avg_month_ge1=float(np.mean(list(month_ge1.values()))))

    rows = []
    for ci, (st, arm, gv, fl) in enumerate(combos):
        rets = [sim(M, st, arm, gv, fl) for M in mats]
        full = metrics(rets, months)
        tr = metrics(rets[:split], months[:split]); te = metrics(rets[split:], months[split:])
        rows.append(dict(stop=st, arm=arm, give=gv, floor=fl, full=full, train=tr, test=te))
        if (ci + 1) % 20 == 0:
            print(f"  {ci+1}/{len(combos)} combos...")
    # rank by consistency on FULL: primary %days>=1%, then mean, penalize worst
    rows.sort(key=lambda r: (r["full"]["ge1"], r["full"]["mean"], r["full"]["worst"]), reverse=True)
    (ROOT / "docs" / "ops" / "param_grid.json").write_text(json.dumps(rows, indent=2))
    print(f"\n=== TOP 12 by %days>=+1% (CASH, full period) ===")
    print(f"{'stop':>5}{'arm':>6}{'give':>6}{'floor':>6}{'mean':>8}{'med':>7}{'d>=1%':>7}{'pos%':>6}{'worst':>7}{'moPass':>7}{'test_d>=1%':>11}")
    for r in rows[:12]:
        f = r["full"]; a = "none" if r["arm"] >= 9 else f"{r['arm']*100:.1f}"
        print(f"{r['stop']*100:>5.1f}{a:>6}{r['give']*100:>6.2f}{r['floor']*100:>6.2f}{f['mean']:>8.3f}{f['median']:>7.3f}{f['ge1']:>7.1f}{f['pos']:>6.1f}{f['worst']:>7.2f}{f['months_pass']:>4}/{f['n_months']:<2}{r['test']['ge1']:>11.1f}")
    # current params row for reference
    cur = next((r for r in rows if abs(r['stop']-0.015)<1e-9 and abs(r['arm']-0.02)<1e-9 and abs(r['give']-0.005)<1e-9 and abs(r['floor']-0.01)<1e-9), None)
    if cur:
        f = cur['full']; print(f"\nCURRENT (1.5/2.0/0.5/1.0): mean {f['mean']:.3f}% d>=1% {f['ge1']:.1f}% pos {f['pos']:.1f}% worst {f['worst']:.2f}% monthsPass {f['months_pass']}/{f['n_months']}  (rank {rows.index(cur)+1}/{len(rows)})")
    best = rows[0]['full']
    print(f"\nBEST-consistency d>=1% = {best['ge1']:.1f}% (target was 90%). Months passing 90%: {best['months_pass']}/{best['n_months']}.")


if __name__ == "__main__":
    main()
