"""Full per-day + per-stock trade journal across the walk-forward period.

CASH (non-MTF) basis: equal-weight INR100k/day, entry 09:15 open, qty=floor.
Records for EVERY day, EVERY stock: rank, entry, exit (price/time/reason), return,
MAE (max adverse after entry, until exit), MFE (max favorable, entry->EOD), post-exit
high, EOD close, per-stock cash P&L, plus the day's portfolio (basket) return.

Exit engine = current live: per-stock soft stop at stop_pct; basket arm/floor/
giveback/pre-arm-stop; remaining names square off at 15:29. Writes two CSVs +
a monthly-consistency CSV to docs/ops/.
"""
import sqlite3, math, csv
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
PERSONA = "falcon_top10_daily"
ALIASES = {"ZOMATO": "ETERNAL"}
OPEN, CLOSE = "09:15:00", "15:29:00"
TOPN = 5
CAP = 100000.0
ARM, FLOOR, GIVE, STOP = 0.02, 0.01, 0.005, 0.015


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


def build(per, order, cap=CAP):
    present = [(s, per[s]["09:15"][0]) for s in order
               if s in per and "09:15" in per[s] and per[s]["09:15"][0] and per[s]["09:15"][0] > 0]
    if not present:
        return None
    alloc = cap / len(present)
    legs = [(s, e, math.floor(alloc / e)) for s, e in present if math.floor(alloc / e) >= 1]
    if not legs:
        return None
    syms = [s for s, _, _ in legs]
    entry = np.array([e for _, e, _ in legs], float)
    qty = np.array([q for _, _, q in legs], float)
    rank = {s: order.index(s) + 1 for s in syms}
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
    high = np.column_stack([series(s, 1) for s in syms])
    low = np.column_stack([series(s, 2) for s in syms])
    dep = float((qty * entry).sum())
    return dict(syms=syms, rank=rank, grid=grid, entry=entry, qty=qty,
                close=close, high=high, low=low, dep=dep)


def detailed_sim(M):
    """Run the current engine, recording per-stock exit (bar/price/reason)."""
    syms, entry, qty, close, high, low, grid, dep = (
        M["syms"], M["entry"], M["qty"], M["close"], M["high"], M["low"], M["grid"], M["dep"])
    n = len(grid); ns = len(entry)
    stop_lvl = entry * (1 - STOP)
    open_mask = np.ones(ns, bool)
    exit_bar = [None] * ns; exit_px = [None] * ns; exit_reason = [None] * ns
    realized = 0.0; armed = False; peak = 0.0
    basket_exit_bar = None; basket_reason = None
    for i in range(1, n):
        # per-stock soft stop
        for j in np.where(open_mask)[0]:
            if low[i, j] <= stop_lvl[j]:
                open_mask[j] = False
                exit_bar[j] = i; exit_px[j] = float(stop_lvl[j]); exit_reason[j] = "STOP_STOCK"
                realized += (stop_lvl[j] - entry[j]) * qty[j]
        G = (realized + float(((close[i] - entry) * qty * open_mask).sum())) / dep
        fire = None
        if not armed and G <= -STOP:
            fire = "BASKET_STOP"
        elif not armed and G >= ARM:
            armed = True; peak = G
        if armed:
            peak = max(peak, G)
            if G <= max(peak - GIVE, FLOOR):
                fire = "FLOOR" if max(peak - GIVE, FLOOR) == FLOOR else "TRAIL"
        if fire:
            basket_exit_bar = i; basket_reason = fire
            for j in np.where(open_mask)[0]:
                exit_bar[j] = i; exit_px[j] = float(close[i, j]); exit_reason[j] = fire
                open_mask[j] = False
            break
    # any still open -> EOD square-off
    for j in np.where(open_mask)[0]:
        exit_bar[j] = n - 1; exit_px[j] = float(close[n - 1, j]); exit_reason[j] = "EOD"
    # per-stock metrics
    recs = []
    for j in range(ns):
        e = entry[j]; xb = exit_bar[j]
        mae = float((low[0:xb + 1, j].min() - e) / e * 100)
        mae_bar = int(low[0:xb + 1, j].argmin())
        mfe = float((high[:, j].max() - e) / e * 100)
        mfe_bar = int(high[:, j].argmax())
        post_hi = float((high[xb + 1:, j].max() - e) / e * 100) if xb + 1 < n else float((exit_px[j] - e) / e * 100)
        eod = float((close[n - 1, j] - e) / e * 100)
        ret = (exit_px[j] - e) / e
        recs.append(dict(sym=syms[j], rank=M["rank"][syms[j]], entry=e, qty=qty[j],
                         exit_px=exit_px[j], exit_t=grid[xb], exit_reason=exit_reason[j],
                         ret_pct=ret * 100, pnl=(exit_px[j] - e) * qty[j],
                         mae_pct=mae, mae_t=grid[mae_bar], mfe_pct=mfe, mfe_t=grid[mfe_bar],
                         post_hi_pct=post_hi, eod_pct=eod))
    # basket
    day_pnl = sum(r["pnl"] for r in recs)
    day_ret = day_pnl / dep * 100
    gp = ((close - entry) * qty).sum(axis=1) / dep * 100
    mfe_b = float((((high - entry) * qty).sum(axis=1) / dep * 100).max())
    return recs, dict(day_ret=day_ret, day_pnl=day_pnl, dep=dep, n=ns,
                      basket_mfe=mfe_b, basket_mae=float(gp.min()), basket_eod=float(gp[-1]),
                      armed=(mfe_b >= ARM * 100), basket_reason=basket_reason)


def main():
    con = sqlite3.connect(str(RND)); sig = load_signals(con, TOPN); days = sorted(sig)
    stock_rows = []; day_rows = []; used = 0
    for d in days:
        per = load_day(con, d, sig[d]); M = build(per, sig[d])
        if not M:
            continue
        used += 1
        recs, b = detailed_sim(M)
        cap_pct = (max(0, b["day_ret"]) / b["basket_mfe"] * 100) if b["basket_mfe"] > 0 else 0
        month = d[:7]
        for r in recs:
            stock_rows.append([d, month, r["rank"], r["sym"], round(r["entry"], 2), int(r["qty"]),
                               round(r["exit_px"], 2), r["exit_t"], r["exit_reason"],
                               round(r["ret_pct"], 3), round(r["pnl"], 1),
                               round(r["mae_pct"], 3), r["mae_t"], round(r["mfe_pct"], 3), r["mfe_t"],
                               round(r["post_hi_pct"], 3), round(r["eod_pct"], 3)])
        day_rows.append([d, month, b["n"], round(b["day_ret"], 3), round(b["day_pnl"], 0),
                         round(b["basket_mfe"], 3), round(b["basket_mae"], 3), round(b["basket_eod"], 3),
                         "YES" if b["armed"] else "no", round(cap_pct, 0),
                         "YES" if b["day_ret"] >= 1.0 else "no", b["basket_reason"] or "mixed/stops"])
        if used % 100 == 0:
            print(f"  {used} days...")
    con.close()
    ops = ROOT / "docs" / "ops"
    with open(ops / "wf_stock_journal.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "month", "rank", "symbol", "entry", "qty", "exit_px", "exit_time",
                    "exit_reason", "ret_pct", "pnl_rs", "MAE_pct", "MAE_time", "MFE_pct", "MFE_time",
                    "post_exit_hi_pct", "EOD_close_pct"])
        w.writerows(stock_rows)
    with open(ops / "wf_day_analysis.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "month", "n_stocks", "portfolio_ret_pct_cash", "portfolio_pnl_rs",
                    "basket_MFE_pct", "basket_MAE_pct", "basket_EOD_pct", "armed", "capture_pct",
                    "hit_+1pct", "basket_exit_reason"])
        w.writerows(day_rows)
    # monthly consistency
    from collections import defaultdict
    md = defaultdict(list)
    for row in day_rows:
        md[row[1]].append(row[3])
    with open(ops / "wf_monthly_consistency.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["month", "n_days", "mean_ret_pct", "median_ret_pct", "pct_days_>=+1%",
                    "pct_positive_days", "worst_day_pct", "passes_90%_target"])
        for m in sorted(md):
            a = np.array(md[m]); ge1 = float((a >= 1.0).mean() * 100)
            w.writerow([m, len(a), round(float(a.mean()), 3), round(float(np.median(a)), 3),
                        round(ge1, 1), round(float((a > 0).mean() * 100), 1),
                        round(float(a.min()), 2), "PASS" if ge1 >= 90 else "FAIL"])
    # overall
    allr = np.array([r[3] for r in day_rows])
    print(f"\n=== {used} days · CASH (non-MTF) · current params ===")
    print(f"  mean daily portfolio return: {allr.mean():.3f}%   median: {np.median(allr):.3f}%")
    print(f"  %% days >= +1%: {(allr>=1.0).mean()*100:.1f}%   %% positive: {(allr>0).mean()*100:.1f}%   worst day: {allr.min():.2f}%")
    months = sorted(md); passes = sum(1 for m in months if (np.array(md[m])>=1.0).mean()>=0.9)
    print(f"  months passing >=+1% on 90%+ of days: {passes}/{len(months)}")
    print(f"  wrote wf_stock_journal.csv ({len(stock_rows)} rows), wf_day_analysis.csv ({len(day_rows)}), wf_monthly_consistency.csv")


if __name__ == "__main__":
    main()
