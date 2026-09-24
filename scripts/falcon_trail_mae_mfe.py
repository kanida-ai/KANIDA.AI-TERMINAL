"""Walk-forward MAE/MFE + trade-quality audit for the intraday_basket engine.

Reuses the RND DB (ohlc_1min x falcon_signal_day_study, persona falcon_top10_daily).
For EACH day: build the Top-N basket (entry 09:15 open, equal INR, qty=floor), then
  * compute the BASKET gross-return path (G_t = sum(close_t-entry)*qty / sum(qty*entry)),
    -> basket MFE / MAE / EOD, and whether it armed (hit +arm).
  * simulate exit VARIANTS and measure PROFIT CAPTURE (exit_G / basket_MFE), and
    per-stock STOP WHIPSAW (stopped then closed back above the stop).
All returns are % on the invested/notional basis (cap-independent).
"""
import sqlite3, math, json
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
PERSONA = "falcon_top10_daily"
ALIASES = {"ZOMATO": "ETERNAL"}
OPEN, CLOSE = "09:15:00", "15:29:00"
TOPN = 5
ARM, FLOOR, GIVE, STOP = 0.02, 0.01, 0.005, 0.015   # current live params


def load_signals(con, topn):
    out = {}
    for ed, rk, sym in con.execute(
        "SELECT entry_date,engine_rank,symbol FROM falcon_signal_day_study "
        "WHERE persona=? AND engine_rank BETWEEN 1 AND ? ORDER BY entry_date,engine_rank",
        (PERSONA, topn)):
        out.setdefault(ed, []).append(sym)
    return out


def load_day(con, day, syms):
    fetch = {ALIASES.get(s, s): s for s in syms}
    ph = ",".join("?" * len(fetch))
    rows = con.execute(
        f"SELECT symbol,substr(bar_time,12,5) hm,open,high,low,close FROM ohlc_1min "
        f"WHERE bar_time BETWEEN ? AND ? AND symbol IN ({ph})",
        [f"{day} {OPEN}", f"{day} {CLOSE}", *fetch.keys()]).fetchall()
    per = {}
    for osym, hm, o, h, l, c in rows:
        per.setdefault(fetch[osym], {})[hm] = (o, h, l, c)
    return per


def build(per, cap=100000):
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
    grid = sorted({m for s in syms for m in per[s]} | {"09:15"})
    grid = [m for m in grid if m >= OPEN[:5]]

    def series(s, idx):
        vals, last = [], None
        for m in grid:
            v = per[s].get(m)
            if v and v[idx] and np.isfinite(v[idx]):
                last = v[idx]
            vals.append(last)
        e = per[s]["09:15"][0]
        return [x if x is not None else e for x in vals]

    close = np.column_stack([series(s, 3) for s in syms])
    low = np.column_stack([series(s, 2) for s in syms])
    high = np.column_stack([series(s, 1) for s in syms])
    dep = float((qty * entry).sum())
    return dict(grid=grid, entry=entry, qty=qty, close=close, low=low, high=high, dep=dep)


def basket_path(M):
    return ((M["close"] - M["entry"]) * M["qty"]).sum(axis=1) / M["dep"]


def basket_mfe_high(M):
    return (((M["high"] - M["entry"]) * M["qty"]).sum(axis=1) / M["dep"]).max()


def sim_current(M, grace=0):
    entry, qty, close, low, dep = M["entry"], M["qty"], M["close"], M["low"], M["dep"]
    n = len(M["grid"]); stop_lvl = entry * (1 - STOP); mask = np.ones(len(entry), bool)
    realized = 0.0; armed = False; peak = 0.0
    for i in range(1, n):
        if i >= grace:
            for j in np.where(mask)[0]:
                if low[i, j] <= stop_lvl[j]:
                    realized += (stop_lvl[j] - entry[j]) * qty[j]; mask[j] = False
        G = (realized + float(((close[i] - entry) * qty * mask).sum())) / dep
        if not armed and G <= -STOP:
            return G, "BASKET_STOP"
        if not armed and G >= ARM:
            armed = True; peak = G
        if armed:
            peak = max(peak, G)
            if G <= max(peak - GIVE, FLOOR):
                return G, ("FLOOR" if max(peak - GIVE, FLOOR) == FLOOR else "TRAIL")
    return (realized + float(((close[-1] - entry) * qty * mask).sum())) / dep, "EOD"


def sim_stop_only(M, stop, grace=0):
    entry, qty, close, low, dep = M["entry"], M["qty"], M["close"], M["low"], M["dep"]
    n = len(M["grid"]); lvl = entry * (1 - stop); mask = np.ones(len(entry), bool); realized = 0.0
    for i in range(1, n):
        if i >= grace:
            for j in np.where(mask)[0]:
                if low[i, j] <= lvl[j]:
                    realized += (lvl[j] - entry[j]) * qty[j]; mask[j] = False
    return (realized + float(((close[-1] - entry) * qty * mask).sum())) / dep


def sim_per_position(M):
    entry, qty, close, low, dep = M["entry"], M["qty"], M["close"], M["low"], M["dep"]
    n = len(M["grid"]); tot = 0.0
    for j in range(len(entry)):
        e = entry[j]; armed = False; peak = 0.0; exit_ret = None
        for i in range(1, n):
            lg = (low[i, j] - e) / e
            if lg <= -STOP:
                exit_ret = -STOP; break
            g = (close[i, j] - e) / e
            if not armed and g >= ARM:
                armed = True; peak = g
            if armed:
                peak = max(peak, g)
                if g <= max(peak - GIVE, FLOOR):
                    exit_ret = g; break
        if exit_ret is None:
            exit_ret = (close[-1, j] - e) / e
        tot += exit_ret * qty[j] * e
    return tot / dep


def sim_hold(M):
    return float(((M["close"][-1] - M["entry"]) * M["qty"]).sum()) / M["dep"]


def main():
    con = sqlite3.connect(str(RND)); sig = load_signals(con, TOPN); days = sorted(sig)
    variants = {k: [] for k in ["current", "grace15", "grace30", "stop2.0", "stop2.5", "per_position", "hold"]}
    mfe = []; mae = []; eod = []; armed_days = 0; capture = []; whip_stops = 0; whip_recover = 0; used = 0
    for d in days:
        per = load_day(con, d, sig[d]); M = build(per)
        if not M:
            continue
        used += 1
        gp = basket_path(M); m_mfe = basket_mfe_high(M); m_mae = gp.min(); m_eod = gp[-1]
        mfe.append(m_mfe * 100); mae.append(m_mae * 100); eod.append(m_eod * 100)
        if m_mfe >= ARM:
            armed_days += 1
        cur, _ = sim_current(M); variants["current"].append(cur * 100)
        variants["grace15"].append(sim_current(M, grace=15)[0] * 100)
        variants["grace30"].append(sim_current(M, grace=30)[0] * 100)
        variants["stop2.0"].append(sim_stop_only(M, 0.02) * 100)
        variants["stop2.5"].append(sim_stop_only(M, 0.025) * 100)
        variants["per_position"].append(sim_per_position(M) * 100)
        variants["hold"].append(sim_hold(M) * 100)
        if m_mfe > 0.002:
            capture.append(max(0, cur) / m_mfe)
        entry, low, close = M["entry"], M["low"], M["close"]; lvl = entry * (1 - STOP)
        for j in range(len(entry)):
            if (low[1:, j] <= lvl[j]).any():
                whip_stops += 1
                if close[-1, j] > lvl[j]:
                    whip_recover += 1
        if used % 100 == 0:
            print(f"  {used} days...")
    con.close()

    def stat(a):
        a = np.array(a)
        return dict(median=float(np.median(a)), mean=float(a.mean()), worst=float(a.min()),
                    win=float((a > 0).mean() * 100), sum=float(a.sum()))

    print(f"\n=== {used} trading days | Top-{TOPN} | params arm{ARM} floor{FLOOR} give{GIVE} stop{STOP} ===")
    print(f"Basket MFE: median {np.median(mfe):+.2f}% | mean {np.mean(mfe):+.2f}%   MAE median {np.median(mae):+.2f}%   EOD median {np.median(eod):+.2f}%")
    print(f"Basket ARMED (hit +{ARM*100:.0f}%): {armed_days}/{used} = {armed_days/used*100:.0f}% of days")
    print(f"PROFIT CAPTURE (current / basket MFE): median {np.median(capture)*100:.0f}% | mean {np.mean(capture)*100:.0f}%")
    print(f"PER-STOCK STOP WHIPSAW: {whip_recover}/{whip_stops} = {whip_recover/max(whip_stops,1)*100:.0f}% of stops closed back ABOVE the stop")
    print(f"\n=== EXIT-VARIANT COMPARISON (daily basket return %, {used} days) ===")
    print(f"{'variant':<14}{'median':>9}{'mean':>9}{'worst':>9}{'win%':>8}{'sum%':>10}")
    for k in ["current", "grace15", "grace30", "stop2.0", "stop2.5", "per_position", "hold"]:
        s = stat(variants[k])
        print(f"{k:<14}{s['median']:>9.3f}{s['mean']:>9.3f}{s['worst']:>9.2f}{s['win']:>8.1f}{s['sum']:>10.1f}")
    out = {"days": used, "mfe_median": float(np.median(mfe)), "mae_median": float(np.median(mae)),
           "eod_median": float(np.median(eod)), "armed_pct": armed_days / used * 100,
           "capture_median": float(np.median(capture)) * 100, "whipsaw_pct": whip_recover / max(whip_stops, 1) * 100,
           "variants": {k: stat(v) for k, v in variants.items()}}
    (ROOT / "docs" / "ops" / "trail_mae_mfe_walkforward.json").write_text(json.dumps(out, indent=2))
    print("\nwrote docs/ops/trail_mae_mfe_walkforward.json")


if __name__ == "__main__":
    main()
