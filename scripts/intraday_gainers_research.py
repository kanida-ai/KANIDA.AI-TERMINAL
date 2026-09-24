"""
Same-Day Top-Gainers — Intraday Observation-Window Research
===========================================================

Screen ALL Nifty-500 stocks AS THEY TRADE. At observation time T, rank every
stock by its return from the 09:15 open to T, pick the top 5 (optionally
filtered), enter at the OPEN of the next 1-min candle, trail each position,
and exit same day. Find the (window x trail x filter) that most consistently
delivers a +2% portfolio day.

NOT Falcon picks — selection is 100% same-day intraday momentum, no prior-day
data for stock choice. No overnight. Fixed exit logic. Parameterised + re-runnable.

Heavy: processes the full ohlc_1min table (one day at a time).
"""
from __future__ import annotations

import argparse
import os
import sqlite3
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
OUT = ROOT / "outputs" / "Intraday_TopGainers_Research.xlsx"
DESKTOP = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"

START, END = "2024-05-14", "2026-06-15"
CAPITAL = 500_000.0
TARGET = 2.0
HARD_STOP = -1.5
BASKET = 5
WINDOWS = ["09:20", "09:25", "09:30", "09:35", "09:40", "09:45", "09:50", "09:55",
           "10:00", "10:05", "10:10", "10:15", "10:20", "10:25", "10:30", "10:45",
           "11:00", "11:15", "11:30", "11:45", "12:00"]
TRAILS = [0.30, 0.50, 0.75, 1.00, 1.50]
FILTER_SETS = ["none", "A", "B", "C", "AB", "ABC", "ABCD", "ABCDE"]
VOL_RATIO_MIN = 2.0
NEAR_HIGH_MIN = 0.70
MIN_MOVE_MIN = 2.0
SECTOR_CAP = 2
SESSION_MIN = 375


# --------------------------------------------------------------------------- #
def load_sectors(con):
    try:
        return dict(con.execute("SELECT symbol, sector FROM falcon_sectors").fetchall())
    except Exception:
        return {}


def load_avg20vol(con):
    """trailing 20d avg daily volume per (symbol, date), shifted (prior days only)."""
    df = pd.read_sql_query(
        "SELECT symbol, trade_date, volume FROM ohlc_daily "
        "WHERE trade_date BETWEEN date(?, '-40 days') AND ?", con, params=(START, END))
    df = df.sort_values(["symbol", "trade_date"])
    df["avg20"] = (df.groupby("symbol")["volume"]
                   .transform(lambda s: s.rolling(20, min_periods=5).mean().shift(1)))
    return {(r.symbol, r.trade_date): r.avg20 for r in df.itertuples(index=False)
            if pd.notna(r.avg20)}


def load_day(con, day):
    """Return dict: grid(list HH:MM), and per-symbol arrays. Vectorized via pivot."""
    rows = con.execute(
        "SELECT symbol, substr(bar_time,12,5) hm, open, high, low, close, volume "
        "FROM ohlc_1min WHERE bar_time BETWEEN ? AND ?",
        (f"{day} 09:15:00", f"{day} 15:29:59")).fetchall()
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["symbol", "hm", "open", "high", "low", "close", "volume"])
    grid = sorted(df["hm"].unique())
    gi = {m: i for i, m in enumerate(grid)}

    def piv(col, fill_ff=True):
        p = df.pivot_table(values=col, index="hm", columns="symbol", aggfunc="last").reindex(grid)
        if fill_ff:
            p = p.ffill().bfill()
        return p

    p_close = piv("close"); p_open = piv("open", fill_ff=False)
    p_high = piv("high"); p_low = piv("low")
    p_vol = piv("volume", fill_ff=False).fillna(0.0)
    cols = list(p_close.columns)
    A_close = p_close.to_numpy(float)
    A_open = p_open.to_numpy(float)
    A_high = p_high.to_numpy(float)
    A_low = p_low.to_numpy(float)
    A_vol = p_vol.to_numpy(float)
    A_oexec = np.where(np.isfinite(A_open) & (A_open > 0), A_open, A_close)
    A_runhigh = np.maximum.accumulate(A_high, axis=0)
    A_runlow = np.minimum.accumulate(A_low, axis=0)
    A_cumvol = np.cumsum(A_vol, axis=0)
    o0_row = A_open[gi["09:15"]] if "09:15" in gi else None

    syms = {}
    for j, sym in enumerate(cols):
        o0 = o0_row[j] if o0_row is not None else np.nan
        if not np.isfinite(o0) or o0 <= 0:
            continue
        syms[sym] = {"o0": float(o0), "close": A_close[:, j], "open": A_oexec[:, j],
                     "runhigh": A_runhigh[:, j], "runlow": A_runlow[:, j],
                     "cumvol": A_cumvol[:, j], "high": A_high[:, j], "low": A_low[:, j]}
    return {"grid": grid, "gi": gi, "syms": syms}


# --------------------------------------------------------------------------- #
def compute_candidates(day_data, it, avg20, date):
    """Per-symbol metrics at observation index `it` — computed ONCE per (day, window),
    reused across all filter sets. Returns list of dicts sorted by ret desc."""
    cand = []
    tfrac = (it + 1) / SESSION_MIN
    for sym, d in day_data["syms"].items():
        if not np.isfinite(d["close"][it]) or d["o0"] <= 0:
            continue
        ret = (d["close"][it] / d["o0"] - 1) * 100.0
        rh, rl = d["runhigh"][it], d["runlow"][it]
        nhigh = (d["close"][it] - rl) / (rh - rl) if rh > rl else 0.5
        a20 = avg20.get((sym, date))
        vr = d["cumvol"][it] / (a20 * tfrac) if (a20 and a20 > 0 and tfrac > 0) else 0.0
        frozen = (d["high"][:it + 1] == d["low"][:it + 1])
        near_top = d["close"][:it + 1] >= 0.999 * d["runhigh"][:it + 1]
        circuit = ret > 5.0 and int(np.sum(frozen[-10:] & near_top[-10:])) >= 3
        cand.append({"sym": sym, "ret": ret, "vr": vr, "nhigh": nhigh, "circuit": circuit})
    cand.sort(key=lambda x: -x["ret"])
    return cand


def select_basket(cands, fset, sectors):
    """Apply filter set to precomputed candidates; greedy top-BASKET with sector cap."""
    picks, sec_count = [], defaultdict(int)
    for c in cands:
        if "A" in fset and c["vr"] < VOL_RATIO_MIN:
            continue
        if "B" in fset and c["nhigh"] < NEAR_HIGH_MIN:
            continue
        if "C" in fset and c["ret"] < MIN_MOVE_MIN:
            continue
        if "E" in fset and c["circuit"]:
            continue
        if "D" in fset:
            sec = sectors.get(c["sym"], "?")
            if sec_count[sec] >= SECTOR_CAP:
                continue
            sec_count[sec] += 1
        picks.append(c["sym"])
        if len(picks) >= BASKET:
            break
    return picks


def sim_portfolio(day_data, picks, it, trail, capital):
    """Enter at open of it+1; per-stock trailing; portfolio -1.5% hard stop;
    exit all at 15:29 close. Returns dict or None."""
    grid = day_data["grid"]
    n = len(grid)
    e = it + 1
    if e >= n:
        return None
    legs = []
    for sym in picks:
        d = day_data["syms"][sym]
        ep = d["open"][e]
        if not np.isfinite(ep) or ep <= 0:
            continue
        legs.append((sym, d, ep))
    if not legs:
        return None
    alloc = capital / len(legs)
    qty = np.array([np.floor(alloc / ep) for _, _, ep in legs], dtype=float)
    keep = qty > 0
    legs = [l for l, k in zip(legs, keep) if k]
    qty = qty[keep]
    if not legs:
        return None
    entry = np.array([ep for _, _, ep in legs])
    deployed = float((qty * entry).sum())

    # per-stock natural trailing exit (index into grid, and exit price)
    exit_idx = np.full(len(legs), n - 1)
    exit_px = np.empty(len(legs))
    for k, (sym, d, ep) in enumerate(legs):
        c = d["close"]; op = d["open"]
        peak = -1e9; ex = None
        for m in range(e, n - 1):
            r = (c[m] / ep - 1) * 100.0
            peak = max(peak, r)
            if r <= peak - trail:
                ex = m; break
        if ex is None:
            exit_idx[k] = n - 1
            exit_px[k] = c[n - 1]               # 15:29 close
        else:
            exit_idx[k] = ex
            exit_px[k] = op[ex + 1]             # next-candle open

    # portfolio hard-stop walk (mark open legs at close; realized legs at exit px)
    reason = "TRAIL/CLOSE"
    stop_hit = False
    for m in range(e, n):
        val = 0.0
        for k, (sym, d, ep) in enumerate(legs):
            if m <= exit_idx[k]:
                val += qty[k] * d["close"][m]
            else:
                val += qty[k] * exit_px[k]
        pr = (val - deployed) / deployed * 100.0
        if pr <= HARD_STOP and m < n - 1:
            for k, (sym, d, ep) in enumerate(legs):
                if exit_idx[k] > m:
                    exit_idx[k] = m
                    exit_px[k] = d["open"][m + 1]
            stop_hit = True
            reason = "HARD_STOP"
            break
    final_val = float((qty * exit_px).sum())
    port_ret = (final_val - deployed) / deployed * 100.0
    last_exit_idx = int(exit_idx.max())
    return {
        "port_ret": port_ret,
        "hit2": port_ret >= TARGET,
        "hit1": port_ret >= 1.0,
        "stop": stop_hit,
        "n_legs": len(legs),
        "entry_idx": e,
        "exit_idx": last_exit_idx,
        "hold_min": last_exit_idx - e,
        "picks": [s for s, _, _ in legs],
        "leg_rets": [round((exit_px[k] / entry[k] - 1) * 100, 3) for k in range(len(legs))],
    }


# --------------------------------------------------------------------------- #
def process_chunk(payload):
    """Worker: process a contiguous chunk of days. Returns {(T,trail,fset): [recs]}."""
    db_path, days_chunk, avg20, sectors, windows, trails = payload
    con = sqlite3.connect(db_path)
    out = defaultdict(list)
    for day in days_chunk:
        dd = load_day(con, day)
        if dd is None:
            continue
        gi = dd["gi"]
        for T in windows:
            if T not in gi:
                continue
            it = gi[T]
            cands = compute_candidates(dd, it, avg20, day)
            for fset in FILTER_SETS:
                picks = select_basket(cands, fset, sectors)
                if not picks:
                    continue
                for tr in trails:
                    r = sim_portfolio(dd, picks, it, tr, CAPITAL)
                    if r is None:
                        continue
                    out[(T, tr, fset)].append({
                        "date": day, "port_ret": r["port_ret"], "hit2": r["hit2"],
                        "hit1": r["hit1"], "stop": r["stop"], "n_legs": r["n_legs"],
                        "entry_idx": r["entry_idx"], "exit_idx": r["exit_idx"],
                        "hold_min": r["hold_min"], "picks": r["picks"],
                        "leg_rets": r["leg_rets"]})
    con.close()
    return dict(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=0, help="sample first N days (0=all)")
    ap.add_argument("--windows", default="", help="comma list to restrict (validation)")
    ap.add_argument("--trails", default="", help="comma list to restrict (validation)")
    ap.add_argument("--out", default=str(OUT))
    ap.add_argument("--workers", type=int, default=10, help="parallel day-workers (1=sequential)")
    args = ap.parse_args()

    con = sqlite3.connect(str(DB))
    sectors = load_sectors(con)
    print(f"[*] sectors: {len(sectors)}", flush=True)
    avg20 = load_avg20vol(con)
    print(f"[*] avg20vol keys: {len(avg20)}", flush=True)

    days = [r[0] for r in con.execute(
        "SELECT DISTINCT substr(bar_time,1,10) d FROM ohlc_1min "
        "WHERE bar_time BETWEEN ? AND ? ORDER BY d", (START, END + " 23:59:59"))]
    if args.days:
        days = days[:args.days]
    windows = args.windows.split(",") if args.windows else WINDOWS
    trails = [float(x) for x in args.trails.split(",")] if args.trails else TRAILS
    print(f"[*] days {len(days)}  windows {len(windows)}  trails {len(trails)}  "
          f"filters {len(FILTER_SETS)}", flush=True)

    con.close()
    results = defaultdict(list)      # (T, trail, fset) -> list of per-day dicts
    nw = max(1, args.workers)
    if nw > 1:
        # split days into nw contiguous chunks, process in parallel
        chunks = [days[i::nw] for i in range(nw)]      # round-robin -> balanced load
        payloads = [(str(DB), ch, avg20, sectors, windows, trails) for ch in chunks if ch]
        print(f"[*] parallel: {len(payloads)} workers", flush=True)
        with ProcessPoolExecutor(max_workers=nw) as ex:
            for part in ex.map(process_chunk, payloads):
                for k, v in part.items():
                    results[k].extend(v)
    else:
        con = sqlite3.connect(str(DB))
        for di, day in enumerate(days):
            part = process_chunk((str(DB), [day], avg20, sectors, windows, trails))
            for k, v in part.items():
                results[k].extend(v)
            if (di + 1) % 25 == 0:
                print(f"  [{di+1}/{len(days)}] {day}", flush=True)
        con.close()
    print(f"[*] combos collected: {len(results)}", flush=True)

    # ---- aggregate metrics per combo
    def metrics(recs):
        pr = np.array([x["port_ret"] for x in recs])
        years = defaultdict(list)
        for x in recs:
            years[x["date"][:4]].append(x["hit2"])
        yr = {y: float(np.mean(v) * 100) for y, v in years.items()}
        n5 = int(np.sum([x["n_legs"] >= 5 for x in recs]))
        return {
            "N_days": len(recs),
            "N_5stocks": n5,
            "hit_2pct": round(np.mean(pr >= TARGET) * 100, 1),
            "hit_1pct": round(np.mean(pr >= 1.0) * 100, 1),
            "avg_return": round(pr.mean(), 3),
            "median_return": round(float(np.median(pr)), 3),
            "best_day": round(pr.max(), 2),
            "worst_day": round(pr.min(), 2),
            "hard_stop_rate": round(np.mean([x["stop"] for x in recs]) * 100, 1),
            "avg_hold_min": round(np.mean([x["hold_min"] for x in recs]), 1),
            "avg_n_legs": round(np.mean([x["n_legs"] for x in recs]), 2),
            "n_years": len(yr),
            "worst_year_hit": round(min(yr.values()), 1) if yr else None,
        }

    rows = []
    for (T, tr, fset), recs in results.items():
        m = metrics(recs)
        m.update({"obs_window": T, "trail_pct": tr, "filter_set": fset})
        rows.append(m)
    allres = pd.DataFrame(rows)

    # Sheet 1: heat map (no-filter, pure top5): rows=window, cols=trail, val=hit_2pct
    nf = allres[allres["filter_set"] == "none"]
    heat = nf.pivot(index="obs_window", columns="trail_pct", values="hit_2pct")

    # best window (no-filter) by max hit across trails
    best_window = heat.max(axis=1).idxmax() if not heat.empty else WINDOWS[0]

    # Sheet 2: filter comparison at best window
    s2 = allres[allres["obs_window"] == best_window].sort_values("hit_2pct", ascending=False)

    # Sheet 3: full matrix sorted, top 50 (N>=50)
    elig = allres[allres["N_days"] >= 50].copy()
    if elig.empty:                      # small-sample / validation fallback
        print("[!] no combo with N>=50 (sample run) — falling back to all combos", flush=True)
        elig = allres.copy()
    s3 = elig.sort_values("hit_2pct", ascending=False).head(50)

    # best combo at N>=50 (with 2-yr + worst-year>=65 discipline; else best raw flagged)
    disc = elig[(elig["n_years"] >= 2) & (elig["worst_year_hit"] >= 65)]
    best = (disc.sort_values("hit_2pct", ascending=False).iloc[0] if not disc.empty
            else elig.sort_values("hit_2pct", ascending=False).iloc[0])
    bkey = (best["obs_window"], best["trail_pct"], best["filter_set"])
    brecs = results[bkey]

    # Sheet 4: daily log for best combo
    s4 = pd.DataFrame([{
        "trade_date": x["date"], "obs_window": bkey[0],
        "symbols": ", ".join(x["picks"]), "leg_returns": ", ".join(map(str, x["leg_rets"])),
        "n_legs": x["n_legs"], "hold_min": x["hold_min"],
        "portfolio_return": round(x["port_ret"], 3), "hit_2pct": x["hit2"],
        "hard_stop": x["stop"]} for x in brecs])

    # Sheet 5: time-of-day of exit (hit days) — 15-min buckets of exit_idx minutes-after-entry
    hitrecs = [x for x in brecs if x["hit2"]]
    tod = pd.Series([x["hold_min"] for x in hitrecs])
    s5 = (pd.cut(tod, bins=[0, 15, 30, 45, 60, 90, 120, 180, 240, 400],
                 right=False).value_counts().sort_index().rename("hit_days_count").reset_index())

    # Sheet 6: miss analysis
    missrecs = [x for x in brecs if not x["hit2"]]
    s6 = pd.DataFrame([{
        "trade_date": x["date"], "portfolio_return": round(x["port_ret"], 3),
        "hard_stop": x["stop"], "n_legs": x["n_legs"],
        "symbols": ", ".join(x["picks"]), "leg_returns": ", ".join(map(str, x["leg_rets"]))}
        for x in missrecs])

    # Sheet 7: year-by-year best combo
    yb = defaultdict(list)
    for x in brecs:
        yb[x["date"][:4]].append(x)
    s7 = pd.DataFrame([{
        "year": y, "N": len(v),
        "hit_2pct": round(np.mean([z["hit2"] for z in v]) * 100, 1),
        "avg_return": round(np.mean([z["port_ret"] for z in v]), 3)}
        for y, v in sorted(yb.items())])

    # Sheet 8: sector breakdown (best combo)
    sec_count = defaultdict(int)
    for x in brecs:
        for s in x["picks"]:
            sec_count[sectors.get(s, "?")] += 1
    s8 = (pd.DataFrame([{"sector": k, "appearances": v} for k, v in sec_count.items()])
          .sort_values("appearances", ascending=False))

    # Sheet 9: comparison vs prior strategies
    s9 = pd.DataFrame([
        {"strategy": "Falcon Top 5 intraday (+1% target)", "hit_2pct": "n/a (was +1% target: 76.9%)",
         "avg_return": 0.607, "worst_day": -5.9, "hold": "same day"},
        {"strategy": "Same-day top gainers (best combo)",
         "hit_2pct": best["hit_2pct"], "avg_return": best["avg_return"],
         "worst_day": best["worst_day"], "hold": "same day"},
    ])

    # ---- parity checks
    checks = []
    checks.append(("Best combo N >= 50", best["N_days"] >= 50, f"N={best['N_days']}"))
    checks.append(("Best combo hit_2pct < 100% (lookahead guard)", best["hit_2pct"] < 100,
                   f"{best['hit_2pct']}%"))
    checks.append(("Best window not after 13:00", best_window <= "13:00", f"{best_window}"))
    checks.append(("All reported combos N>=50", bool((s3["N_days"] >= 50).all()), "sheet3"))

    OUTP = Path(args.out)
    OUTP.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUTP, engine="openpyxl") as xl:
        heat.to_excel(xl, sheet_name="1_HeatMap_Window_x_Trail")
        s2.to_excel(xl, sheet_name="2_Filters_at_BestWindow", index=False)
        s3.to_excel(xl, sheet_name="3_Full_Matrix_Top50", index=False)
        s4.to_excel(xl, sheet_name="4_Best_DailyLog", index=False)
        s5.to_excel(xl, sheet_name="5_TimeOfDay_Hits", index=False)
        s6.to_excel(xl, sheet_name="6_Miss_Analysis", index=False)
        s7.to_excel(xl, sheet_name="7_Year_By_Year", index=False)
        s8.to_excel(xl, sheet_name="8_Sector_Breakdown", index=False)
        s9.to_excel(xl, sheet_name="9_Vs_Prior", index=False)
        pd.DataFrame([{"check": c, "pass": "PASS" if ok else "FAIL", "detail": d}
                      for c, ok, d in checks]).to_excel(xl, sheet_name="0_Parity", index=False)
        pd.DataFrame([{"key": "best_window", "value": bkey[0]},
                      {"key": "best_trail", "value": bkey[1]},
                      {"key": "best_filter", "value": bkey[2]},
                      {"key": "best_hit_2pct", "value": best["hit_2pct"]},
                      {"key": "best_avg_return", "value": best["avg_return"]},
                      {"key": "best_N", "value": int(best["N_days"])},
                      {"key": "capital", "value": CAPITAL},
                      {"key": "target", "value": TARGET}]).to_excel(
            xl, sheet_name="0_RunInfo", index=False)

    print("\n=== HEATMAP (no-filter hit_2pct, window x trail) ===")
    print(heat.to_string())
    print(f"\n[*] best window (no-filter): {best_window}")
    print(f"[*] BEST combo: window={bkey[0]} trail={bkey[1]} filter={bkey[2]} "
          f"hit2={best['hit_2pct']}% N={best['N_days']} avg={best['avg_return']}")
    print("\n=== PARITY ===")
    for c, ok, d in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {c} :: {d}")
    print(f"\n[*] wrote {OUTP}")
    if DESKTOP.exists():
        import shutil
        shutil.copy(OUTP, DESKTOP / OUTP.name)
        print(f"[*] copied to {DESKTOP / OUTP.name}")


if __name__ == "__main__":
    main()
