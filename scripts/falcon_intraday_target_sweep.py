"""
Falcon Intraday — Target x Basket SWEEP
=======================================

Reverse-engineering question: which (basket, profit-target) combinations reach a
>=95% daily hit rate AND a 0.7-1.0% average realized return per day?

For every entry day we build the minute-by-minute portfolio return series ONCE,
then evaluate a whole grid of profit targets against it (first close-mark crossing
-> exit at next-candle OPEN, else 15:29 close). Same engine, same data, same
drop-&-reallocate and parity rules as falcon_intraday_backtest.py.

Output: console frontier + outputs/Falcon_Intraday_Target_Sweep.xlsx
"""
from __future__ import annotations

import math
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from falcon_intraday_backtest import (
    Config, DEFAULT_DB, load_signals, load_ohlc_1min_day,
)

ROOT = Path(__file__).resolve().parent.parent
OUT_XLSX = ROOT / "outputs" / "Falcon_Intraday_Target_Sweep.xlsx"

# Target grid (% portfolio gain) and baskets to sweep (unique rank-bands).
TARGETS = [0.05, 0.10, 0.15, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.25, 1.5]
SWEEP_BASKETS = {
    "Top 3":     [1, 2, 3],
    "Top 5":     [1, 2, 3, 4, 5],
    "Top 7":     [1, 2, 3, 4, 5, 6, 7],
    "Top 10":    list(range(1, 11)),
    "Ranks 3-7": [3, 4, 5, 6, 7],
    "Ranks 6-10":[6, 7, 8, 9, 10],
    "Ranks 4-10":[4, 5, 6, 7, 8, 9, 10],
}


def build_day_arrays(basket_picks, day_ohlc, capital):
    """Return (port_ret[minutes], next_open_val[minutes], close_exit_val, deployed)
    or None. port_ret marks on CLOSE; next_open_val[i] = basket value if exited at
    the OPEN of minute i+1 (executable). close_exit_val = value at last candle close."""
    present = []
    for rk, sym in basket_picks:
        df = day_ohlc.get(sym)
        if df is None or "09:15" not in df.index:
            continue
        eo = df.at["09:15", "open"]
        if eo is None or not np.isfinite(eo) or eo <= 0:
            continue
        present.append((sym, float(eo)))
    if not present:
        return None

    alloc = capital / len(present)
    syms, entry_px, qty = [], [], []
    for sym, eo in present:
        q = int(math.floor(alloc / eo))
        if q <= 0:
            continue
        syms.append(sym); entry_px.append(eo); qty.append(q)
    if not syms:
        return None

    grid = [m for m in day_ohlc[syms[0]].index if m >= "09:15"]
    qty = np.array(qty, dtype=float)
    deployed = float(np.array(entry_px) @ qty)

    close_ff = np.column_stack([
        day_ohlc[s]["close"].reindex(grid).ffill().bfill().to_numpy(float) for s in syms])
    # executable open per minute: real open where it traded, else that minute's ffill close
    open_exec = []
    for s in syms:
        o = day_ohlc[s]["open"].reindex(grid).to_numpy(float)
        c = day_ohlc[s]["close"].reindex(grid).ffill().bfill().to_numpy(float)
        o = np.where(np.isfinite(o) & (o > 0), o, c)
        open_exec.append(o)
    open_exec = np.column_stack(open_exec)

    port_val = close_ff @ qty
    port_ret = (port_val - deployed) / deployed * 100.0
    open_val = open_exec @ qty
    next_open_val = np.empty_like(open_val)
    next_open_val[:-1] = open_val[1:]
    next_open_val[-1] = port_val[-1]            # no next candle at the very end
    close_exit_val = float(port_val[-1])
    return port_ret, next_open_val, close_exit_val, deployed


def realized_for_target(port_ret, next_open_val, close_exit_val, deployed, target):
    """First minute (0..n-2) whose CLOSE-mark >= target -> exit next OPEN. Else close."""
    n = len(port_ret)
    for i in range(n - 1):
        if port_ret[i] >= target:
            return (next_open_val[i] - deployed) / deployed * 100.0, True
    return (close_exit_val - deployed) / deployed * 100.0, False


def run_sweep(cfg: Config):
    con = sqlite3.connect(str(cfg.db_path))
    signals = load_signals(con, cfg)
    min_1m, max_1m = con.execute(
        "SELECT min(substr(bar_time,1,10)), max(substr(bar_time,1,10)) FROM ohlc_1min"
    ).fetchone()
    start = cfg.start or min_1m
    end = cfg.end or max_1m
    entry_days = sorted(d for d in signals if start <= d <= end)

    # records[(basket,target)] = list of (day, realized_return, hit)
    records: dict[tuple[str, float], list[tuple[str, float, bool]]] = {
        (b, t): [] for b in SWEEP_BASKETS for t in TARGETS}
    day_present: dict[str, set] = {}     # entry_date -> set of baskets with valid sim

    for day in entry_days:
        picks_all = signals[day]
        study_syms = [s for _, s in picks_all]
        day_ohlc = load_ohlc_1min_day(con, day, study_syms, cfg.aliases)
        present_baskets = set()
        for label, ranks in SWEEP_BASKETS.items():
            rs = set(ranks)
            bp = [(rk, sym) for rk, sym in picks_all if rk in rs]
            arr = build_day_arrays(bp, day_ohlc, cfg.capital) if bp else None
            if arr is None:
                continue
            present_baskets.add(label)
            port_ret, nov, cev, dep = arr
            for t in TARGETS:
                r, hit = realized_for_target(port_ret, nov, cev, dep, t)
                records[(label, t)].append((day, r, hit))
        day_present[day] = present_baskets
    con.close()

    # Parity: restrict to days present in ALL baskets so counts match.
    common = [d for d, s in day_present.items() if len(s) == len(SWEEP_BASKETS)]
    return entry_days, common, records, day_present


def summarize(records, common, day_present):
    rows = []
    common_set = set(common)
    for (basket, target), recs in records.items():
        recs = [(d, r, h) for d, r, h in recs if d in common_set]   # parity-aligned
        arr = np.array([r for _, r, _ in recs], dtype=float)
        hits = np.array([h for _, _, h in recs], dtype=bool)
        n = len(arr)
        rows.append({
            "Basket": basket,
            "Target %": target,
            "Days": n,
            "Hit rate %": round(hits.mean() * 100, 1),
            "Avg return %": round(arr.mean(), 3),
            "Median %": round(float(np.median(arr)), 3),
            "% positive days": round((arr > 0).mean() * 100, 1),
            "Worst day %": round(arr.min(), 2),
        })
    return pd.DataFrame(rows)


def main():
    cfg = Config()
    print(f"[*] sweep: {len(SWEEP_BASKETS)} baskets x {len(TARGETS)} targets")
    print(f"[*] capital Rs {cfg.capital:,.0f}", flush=True)
    entry_days, common, records, day_present = run_sweep(cfg)
    print(f"[*] entry days {len(entry_days)}; common-to-all-baskets {len(common)}",
          flush=True)

    df = summarize(records, common, day_present)
    df = df.sort_values(["Basket", "Target %"]).reset_index(drop=True)

    # Goal screen
    goal = df[(df["Hit rate %"] >= 95.0) &
              (df["Avg return %"] >= 0.7) & (df["Avg return %"] <= 1.0)]
    near = df[(df["Hit rate %"] >= 90.0) & (df["Avg return %"] >= 0.5)]

    print("\n=== FULL SWEEP ===")
    print(df.to_string(index=False))
    print("\n=== COMBINATIONS MEETING GOAL (hit>=95% AND avg 0.7-1.0%) ===")
    print("  NONE" if goal.empty else goal.to_string(index=False))
    print("\n=== CLOSEST (hit>=90% AND avg>=0.5%) ===")
    print("  none" if near.empty else near.to_string(index=False))

    # For each basket, the target where hit-rate first reaches 95%, and its avg.
    print("\n=== WHERE EACH BASKET CROSSES 95% HIT (lowest target that does it) ===")
    cross = []
    for b in SWEEP_BASKETS:
        d = df[(df["Basket"] == b) & (df["Hit rate %"] >= 95.0)].sort_values("Target %")
        if d.empty:
            cross.append({"Basket": b, "Target @95% hit": "never (<95% at all targets)",
                          "Hit rate %": df[df["Basket"] == b]["Hit rate %"].max(),
                          "Avg return %": None})
        else:
            top = d.iloc[-1]   # highest target still >=95% hit = best avg at >=95%
            cross.append({"Basket": b, "Target @95% hit": top["Target %"],
                          "Hit rate %": top["Hit rate %"], "Avg return %": top["Avg return %"]})
    cross_df = pd.DataFrame(cross)
    print(cross_df.to_string(index=False))

    OUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as xl:
        df.to_excel(xl, sheet_name="Full_Sweep", index=False)
        (goal if not goal.empty else pd.DataFrame([{"result": "NO combination meets hit>=95% AND avg 0.7-1.0%"}])
         ).to_excel(xl, sheet_name="Goal_Hits", index=False)
        near.to_excel(xl, sheet_name="Closest_90pct", index=False)
        cross_df.to_excel(xl, sheet_name="95pct_Crossover", index=False)
        # pivot tables for readability
        df.pivot(index="Target %", columns="Basket", values="Hit rate %").to_excel(
            xl, sheet_name="Pivot_HitRate")
        df.pivot(index="Target %", columns="Basket", values="Avg return %").to_excel(
            xl, sheet_name="Pivot_AvgReturn")
    print(f"\n[*] wrote {OUT_XLSX}")


if __name__ == "__main__":
    main()
