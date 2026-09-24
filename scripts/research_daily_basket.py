"""
Falcon Intraday — Daily 3-5 Stock Basket Search
================================================

Goal (operator, 2026-06-25): a tradeable EVERY-DAY basket of 3-5 stocks that
targets 95% win rate and >=1% average return per day.

Unlike the 95%-filter research (which hit 96.5% by trading only ~11% of days
with 1-2 names), this run REQUIRES 3-5 names per day, so the lever shifts from
"skip bad days" to:
  - SELECTION : pick the best K of the day's Top-10 by a signal-day quality score
                (so we always have K names where data exists)
  - ENTRY TIME: 09:15 / 09:45 / 10:15 (entry = OPEN of that 1-min bar)
  - CONFIRM   : optionally only keep names already holding up by entry time
  - TARGET    : portfolio +X% -> exit next-candle OPEN, else 15:29 CLOSE (fixed logic)

Reports the full grid + the best config, honestly showing where the 95%/1%/3-5
goals can and cannot be met simultaneously.

Reuses: falcon_intraday_backtest (loaders), research_95pct_hitrate (MTF features).
"""
from __future__ import annotations

import argparse
import sqlite3
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

from falcon_intraday_backtest import Config, DEFAULT_DB, load_signals, load_ohlc_1min_day
from research_95pct_hitrate import build_mtf_features, NUMERIC_FEATURES

ROOT = Path(__file__).resolve().parent.parent
OUT_XLSX = ROOT / "outputs" / "Falcon_Daily_Basket_Search.xlsx"

# Features where a HIGHER value means MORE extended (miss-day signature). We pick
# the LEAST-extended K names each day (the rested-pullback play).
EXTENSION_FEATS = ["m_ret_pct", "three_day_ret", "w_ret_pct", "d1_ret_pct", "d0_vol_ratio"]


def build_cache(cfg: Config, signals: dict, feats: pd.DataFrame):
    """Per entry_date: minute grid + per-stock close/openexec paths from 09:15,
    the 09:15 entry open, ranks, symbols, and MTF feature rows."""
    con = sqlite3.connect(str(cfg.db_path))
    min_1m, max_1m = con.execute(
        "SELECT min(substr(bar_time,1,10)), max(substr(bar_time,1,10)) FROM ohlc_1min").fetchone()
    start = cfg.start or min_1m
    end = cfg.end or max_1m
    entry_days = sorted(d for d in signals if start <= d <= end)
    feat_lookup = {(r.entry_date, r.symbol): r for r in feats.itertuples(index=False)}

    cache = {}
    for day in entry_days:
        picks = signals[day]
        study_syms = [s for _, s in picks]
        day_ohlc = load_ohlc_1min_day(con, day, study_syms, cfg.aliases)
        grid = None
        for rk, sym in picks:
            df = day_ohlc.get(sym)
            if df is None or "09:15" not in df.index:
                continue
            g = [m for m in df.index if m >= "09:15"]
            if grid is None or len(g) > len(grid):
                grid = g
        if grid is None:
            continue
        close_cols, open_cols, e0915, ranks, syms, frows = [], [], [], [], [], []
        for rk, sym in picks:
            df = day_ohlc.get(sym)
            if df is None or "09:15" not in df.index:
                continue
            eo = df.at["09:15", "open"]
            if eo is None or not np.isfinite(eo) or eo <= 0:
                continue
            close = df["close"].reindex(grid).ffill().bfill().to_numpy(float)
            o = df["open"].reindex(grid).to_numpy(float)
            oexec = np.where(np.isfinite(o) & (o > 0), o, close)
            close_cols.append(close); open_cols.append(oexec)
            e0915.append(float(eo)); ranks.append(rk); syms.append(sym)
            frows.append(feat_lookup.get((day, sym)))
        if not syms:
            continue
        cache[day] = {
            "grid": grid,
            "close": np.column_stack(close_cols),
            "openexec": np.column_stack(open_cols),
            "e0915": np.array(e0915, float),
            "ranks": np.array(ranks),
            "symbols": syms,
            "frows": frows,
        }
    con.close()
    return cache, entry_days


def attach_scores(cache, feats: pd.DataFrame):
    """Global z-scores for the extension composite; lower composite = less extended."""
    stats = {f: (feats[f].mean(), feats[f].std(ddof=0) or 1.0) for f in EXTENSION_FEATS}
    for day, dc in cache.items():
        ext = np.zeros(len(dc["symbols"]))
        valid = np.ones(len(dc["symbols"]), dtype=bool)
        for j, fr in enumerate(dc["frows"]):
            if fr is None:
                valid[j] = False; continue
            z = 0.0
            for f in EXTENSION_FEATS:
                v = getattr(fr, f, None)
                mu, sd = stats[f]
                if v is None or not np.isfinite(v):
                    continue
                z += (v - mu) / sd
            ext[j] = z
        dc["ext_score"] = ext            # higher = more extended
        dc["score_valid"] = valid


def sim(dc, entry_idx, sel, capital, target):
    """sel: index array of chosen stocks. Entry = OPEN of bar entry_idx. Returns
    (realized_return_pct, hit, n) or None."""
    if len(sel) == 0:
        return None
    ep = dc["openexec"][entry_idx, sel]
    good = np.isfinite(ep) & (ep > 0)
    sel = sel[good]; ep = ep[good]
    if len(sel) == 0:
        return None
    alloc = capital / len(sel)
    qty = np.floor(alloc / ep)
    keep = qty > 0
    sel = sel[keep]; qty = qty[keep]; ep = ep[keep]
    if len(sel) == 0:
        return None
    deployed = float((qty * ep).sum())
    close = dc["close"][entry_idx:, sel]
    openexec = dc["openexec"][entry_idx:, sel]
    port = close @ qty
    ret = (port - deployed) / deployed * 100.0
    n = len(ret)
    for i in range(n - 1):
        if ret[i] >= target:
            ev = float(openexec[i + 1] @ qty)
            return (ev - deployed) / deployed * 100.0, True, len(sel)
    return (float(port[-1]) - deployed) / deployed * 100.0, False, len(sel)


def entry_index(grid, t):
    for i, m in enumerate(grid):
        if m >= t:
            return i
    return None


def run_config(cache, entry_days, K, entry_time, score, confirm, target, capital):
    rets, hits, nsel_list, daily = [], [], [], []
    years = {}
    for day in entry_days:
        dc = cache.get(day)
        if dc is None:
            continue
        it = entry_index(dc["grid"], entry_time)
        if it is None:
            continue
        nst = len(dc["symbols"])
        order = np.arange(nst)
        # candidate validity: has a tradeable bar at entry time
        ep = dc["openexec"][it]
        cand = np.array([np.isfinite(ep[j]) and ep[j] > 0 for j in range(nst)])
        # optional intraday confirmation, computed on info available BEFORE entry:
        # cumulative return from 09:15 open to the PRIOR bar's close (it-1) >= thresh.
        # No lookahead: we decide on the close of bar it-1 and enter at open of bar it.
        if confirm and confirm.startswith("pos"):
            thr = 0.0 if confirm == "pos" else float(confirm.split("_")[1])
            if it < 1:                      # 09:15 has no prior bar -> can't confirm
                continue
            prev_close = dc["close"][it - 1]
            conf = np.array([cand[j] and dc["e0915"][j] > 0 and
                             (prev_close[j] / dc["e0915"][j] - 1) * 100.0 >= thr
                             for j in range(nst)])
            cand = conf
        idx = order[cand]
        if len(idx) == 0:
            continue
        # rank candidates
        if score == "rank":
            idx = idx[np.argsort(dc["ranks"][idx])]            # best engine rank first
        else:  # 'nonext' — least extended first
            idx = idx[np.argsort(dc["ext_score"][idx])]
        sel = idx[:K]
        res = sim(dc, it, sel, capital, target)
        if res is None:
            continue
        r, hit, n = res
        rets.append(r); hits.append(hit); nsel_list.append(n)
        daily.append((day, r, hit, n))
        years.setdefault(day[:4], []).append(hit)
    if not rets:
        return None
    rets = np.array(rets); hits = np.array(hits); nsel = np.array(nsel_list)
    yr = {y: round(np.mean(v) * 100, 1) for y, v in years.items()}
    return {
        "K": K, "entry_time": entry_time, "score": score, "confirm": confirm or "none",
        "target": target,
        "days_traded": len(rets),
        "avg_stocks/day": round(float(nsel.mean()), 2),
        "min_stocks/day": int(nsel.min()),
        "days_with_>=3": int((nsel >= 3).sum()),
        "pct_days_>=3": round((nsel >= 3).mean() * 100, 1),
        "hit_rate %": round(hits.mean() * 100, 1),
        "avg_return %": round(rets.mean(), 3),
        "median %": round(float(np.median(rets)), 3),
        "worst_day %": round(rets.min(), 2),
        "worst_year_hit %": min(yr.values()) if yr else None,
        "year_hits": yr,
        "daily": daily,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--capital", type=float, default=500_000.0)
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--out", default=str(OUT_XLSX))
    args = ap.parse_args()
    cfg = Config(db_path=Path(args.db), capital=args.capital, start=args.start, end=args.end)

    print("[*] loading signals + MTF features ...", flush=True)
    signals = load_signals(sqlite3.connect(str(cfg.db_path)), cfg)
    feats = build_mtf_features(cfg, signals)
    print(f"    feature rows {len(feats)}", flush=True)
    print("[*] building intraday cache (1-min) ...", flush=True)
    cache, entry_days = build_cache(cfg, signals, feats)
    attach_scores(cache, feats)
    print(f"    cached days {len([d for d in entry_days if d in cache])}", flush=True)

    Ks = [3, 5]
    times = ["09:15", "09:30", "09:45", "10:15"]
    scores = ["rank", "nonext"]
    confirms = [None, "pos", "pos_0.3"]      # pos = green vs 09:15 open by prior bar
    targets = [1.0]
    results = []
    print("[*] sweeping configs ...", flush=True)
    for K, t, sc, cf, tg in product(Ks, times, scores, confirms, targets):
        m = run_config(cache, entry_days, K, t, sc, cf, tg, cfg.capital)
        if m:
            results.append(m)
    df = pd.DataFrame([{k: v for k, v in r.items() if k not in ("daily", "year_hits")}
                       for r in results])
    df = df.sort_values(["hit_rate %", "avg_return %"], ascending=False).reset_index(drop=True)

    # Configs that meet the operator goal: >=3 stocks essentially every day,
    # hit >=95, avg >=1.0
    goal = df[(df["pct_days_>=3"] >= 95) & (df["hit_rate %"] >= 95) & (df["avg_return %"] >= 1.0)]
    # Closest under the 3-5/day constraint
    feasible = df[df["pct_days_>=3"] >= 95].copy()

    print("\n=== FULL CONFIG SWEEP (sorted by hit rate) ===")
    print(df.to_string(index=False))
    print("\n=== CONFIGS MEETING GOAL (>=3 stocks 95%+ of days, hit>=95%, avg>=1%) ===")
    print("  NONE" if goal.empty else goal.to_string(index=False))
    print("\n=== BEST under '3-5 stocks nearly every day' constraint ===")
    if not feasible.empty:
        best = feasible.sort_values(["hit_rate %", "avg_return %"], ascending=False).iloc[0]
        print(best.to_string())

    # best feasible config daily log + year
    best_m = None
    if not feasible.empty:
        bk = feasible.iloc[0]
        for r in results:
            if (r["K"], r["entry_time"], r["score"], r["confirm"], r["target"]) == \
               (bk["K"], bk["entry_time"], bk["score"], bk["confirm"], bk["target"]):
                best_m = r; break

    OUT = Path(args.out); OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        df.to_excel(xl, sheet_name="Config_Sweep", index=False)
        (goal if not goal.empty else pd.DataFrame([{"result": "NO config meets >=3 stocks/day AND hit>=95% AND avg>=1%"}])
         ).to_excel(xl, sheet_name="Goal_Configs", index=False)
        if best_m:
            pd.DataFrame(best_m["daily"], columns=["entry_date", "return_pct", "hit", "n_stocks"]
                         ).to_excel(xl, sheet_name="Best_Feasible_DailyLog", index=False)
            yr = pd.DataFrame([{"year": y, "hit_rate %": v} for y, v in best_m["year_hits"].items()])
            yr.to_excel(xl, sheet_name="Best_Feasible_Year", index=False)
    print(f"\n[*] wrote {OUT}")


if __name__ == "__main__":
    main()
