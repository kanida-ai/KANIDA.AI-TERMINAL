"""
Top 5 @ 09:15 — Trailing-profit + (-1.5%) hard-stop variants
============================================================

Exit logic (operator spec, 2026-06-25), applied to the COMBINED Top-5 portfolio
return path (close-mark; triggered exits fill at the NEXT candle OPEN; 15:29 close
otherwise; no overnight):

  - HARD STOP (always active): if portfolio return <= -1.5% -> exit. Caps loss.
  - ARM AT +1%: once return reaches +1%, start trailing (don't exit on the arming
    candle; give it room). +1% becomes a FLOOR.
  - TRAIL: once armed, track the peak; exit when return <= max(+1.0, peak - giveback).
    -> a quiet +1% day still exits ~+1%; a runner exits `giveback` below its peak.
  - else 15:29 CLOSE.

Sweeps giveback in {0.3, 0.5, 0.75, 1.0}% and compares to the baseline hard-+1%.
Monthly / yearly / overall win-vs-loss breakdown per variant.
"""
from __future__ import annotations

import argparse
import math
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from falcon_intraday_backtest import Config, DEFAULT_DB, load_signals, load_ohlc_1min_day

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs" / "Top5_Trailing_Stop.xlsx"
DESKTOP = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
TARGET = 1.0
STOP = -1.5
GIVEBACKS = [0.30, 0.50, 0.75, 1.00]


def build_top5_cache(cfg: Config, signals: dict):
    con = sqlite3.connect(str(cfg.db_path))
    min_1m, max_1m = con.execute(
        "SELECT min(substr(bar_time,1,10)), max(substr(bar_time,1,10)) FROM ohlc_1min").fetchone()
    start = cfg.start or min_1m
    end = cfg.end or max_1m
    entry_days = sorted(d for d in signals if start <= d <= end)
    cache = {}
    for day in entry_days:
        picks = [(rk, s) for rk, s in signals[day] if rk <= 5]      # Top 5
        syms = [s for _, s in picks]
        day_ohlc = load_ohlc_1min_day(con, day, syms, cfg.aliases)
        grid = None
        for _, sym in picks:
            df = day_ohlc.get(sym)
            if df is None or "09:15" not in df.index:
                continue
            g = [m for m in df.index if m >= "09:15"]
            if grid is None or len(g) > len(grid):
                grid = g
        if grid is None:
            continue
        ccols, ocols, ep, svalid = [], [], [], []
        for _, sym in picks:
            df = day_ohlc.get(sym)
            if df is None or "09:15" not in df.index:
                continue
            eo = df.at["09:15", "open"]
            if eo is None or not np.isfinite(eo) or eo <= 0:
                continue
            close = df["close"].reindex(grid).ffill().bfill().to_numpy(float)
            o = df["open"].reindex(grid).to_numpy(float)
            oexec = np.where(np.isfinite(o) & (o > 0), o, close)
            ccols.append(close); ocols.append(oexec); ep.append(float(eo)); svalid.append(sym)
        if not ep:
            continue
        ep = np.array(ep)
        alloc = cfg.capital / len(ep)
        qty = np.floor(alloc / ep)
        keep = qty > 0
        if not keep.any():
            continue
        close = np.column_stack(ccols)[:, keep]
        openexec = np.column_stack(ocols)[:, keep]
        qty = qty[keep]; ep = ep[keep]
        symbols = [s for s, k in zip(svalid, keep) if k]
        deployed = float((qty * ep).sum())
        cache[day] = {"close": close, "openexec": openexec, "qty": qty, "deployed": deployed,
                      "symbols": symbols, "grid": grid, "ep": ep}
    con.close()
    return cache, [d for d in entry_days if d in cache]


def simulate(dc, mode, giveback=None):
    """mode: 'hard1' (baseline +1% hard) or 'trail'. Returns (return_pct, reason)."""
    qty, deployed = dc["qty"], dc["deployed"]
    port = dc["close"] @ qty
    ret = (port - deployed) / deployed * 100.0
    openval = dc["openexec"] @ qty
    nxt = np.empty_like(openval); nxt[:-1] = openval[1:]; nxt[-1] = port[-1]
    n = len(ret)

    def realized(i):
        return (nxt[i] - deployed) / deployed * 100.0

    if mode == "hard1":
        for i in range(n - 1):
            if ret[i] >= TARGET:
                return realized(i), "TARGET_HIT"
        return (port[-1] - deployed) / deployed * 100.0, "CLOSE_EXIT"

    # trailing mode
    armed = False; peak = None
    for i in range(n - 1):
        r = ret[i]
        if r <= STOP:                       # hard stop always active
            return realized(i), "STOP_1.5"
        if not armed:
            if r >= TARGET:
                armed = True; peak = r       # arm; give room (no exit this candle)
            continue
        peak = max(peak, r)
        if r <= max(TARGET, peak - giveback):
            return realized(i), "TRAIL_EXIT"
    return (port[-1] - deployed) / deployed * 100.0, "CLOSE_EXIT"


def journal_day(dc, day, giveback):
    """Per-leg trade journal for the trailing config (all legs exit together)."""
    qty, deployed, ep = dc["qty"], dc["deployed"], dc["ep"]
    grid, syms = dc["grid"], dc["symbols"]
    port = dc["close"] @ qty
    ret = (port - deployed) / deployed * 100.0
    n = len(ret); last = n - 1
    armed = False; peak = None; xi = None; reason = "CLOSE_EXIT"
    for i in range(n - 1):
        r = ret[i]
        if r <= STOP:
            xi = i; reason = "STOP_1.5"; break
        if not armed:
            if r >= TARGET:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        if r <= max(TARGET, peak - giveback):
            xi = i; reason = "TRAIL_EXIT"; break
    triggered = xi is not None
    xidx = (xi + 1) if triggered else last
    legs = []
    for k, s in enumerate(syms):
        exitpx = float(dc["openexec"][xidx, k]) if triggered else float(dc["close"][last, k])
        legs.append({"date": day, "stock": s, "entry_time": "09:15",
                     "entry_price": round(float(ep[k]), 2),
                     "exit_time": grid[min(xidx, last)], "exit_price": round(exitpx, 2),
                     "qty": int(qty[k]), "deployed_rs": round(float(qty[k] * ep[k]), 0),
                     "pnl_rs": round(float(qty[k] * (exitpx - ep[k])), 0),
                     "stock_return_pct": round((exitpx / ep[k] - 1) * 100, 2),
                     "exit_reason": reason})
    exit_val = sum(l["qty"] * l["exit_price"] for l in legs)
    port_ret = (exit_val - deployed) / deployed * 100.0
    return legs, round(port_ret, 3), reason


def run_variant(cache, days, mode, giveback=None):
    rows = []
    for day in days:
        r, reason = simulate(cache[day], mode, giveback)
        rows.append({"entry_date": day, "return_pct": round(r, 4), "reason": reason})
    df = pd.DataFrame(rows)
    df["entry_date"] = pd.to_datetime(df["entry_date"])
    df["year"] = df["entry_date"].dt.year
    df["month"] = df["entry_date"].dt.month
    df["win"] = df["return_pct"] > 0
    df["loss"] = df["return_pct"] < 0
    return df


def agg(g):
    win = g[g["win"]]; loss = g[g["loss"]]
    return pd.Series({
        "trading_days": len(g),
        "winning_days": int(g["win"].sum()),
        "losing_days": int(g["loss"].sum()),
        "win_rate_%": round(g["win"].mean() * 100, 1),
        "avg_ret_win_%": round(win["return_pct"].mean(), 3) if len(win) else None,
        "avg_ret_loss_%": round(loss["return_pct"].mean(), 3) if len(loss) else None,
        "avg_ret_all_%": round(g["return_pct"].mean(), 3),
        "best_day_%": round(g["return_pct"].max(), 2),
        "worst_day_%": round(g["return_pct"].min(), 2),
        "sum_return_%": round(g["return_pct"].sum(), 2),
    })


def summary_row(label, df):
    s = agg(df)
    reasons = df["reason"].value_counts().to_dict()
    s["label"] = label
    s["pct_stopped"] = round(reasons.get("STOP_1.5", 0) / len(df) * 100, 1)
    s["pct_trail_exit"] = round(reasons.get("TRAIL_EXIT", 0) / len(df) * 100, 1)
    s["pct_target_or_close"] = round((reasons.get("TARGET_HIT", 0) + reasons.get("CLOSE_EXIT", 0)) / len(df) * 100, 1)
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--capital", type=float, default=500_000.0)
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    args = ap.parse_args()
    cfg = Config(db_path=Path(args.db), capital=args.capital, start=args.start, end=args.end)

    print("[*] loading signals + building Top-5 1-min cache ...", flush=True)
    signals = load_signals(sqlite3.connect(str(cfg.db_path)), cfg)
    cache, days = build_top5_cache(cfg, signals)
    print(f"    cached days: {len(days)}", flush=True)

    variants = {"Baseline +1% hard": run_variant(cache, days, "hard1")}
    for g in GIVEBACKS:
        variants[f"Trail giveback {g:.2f}%"] = run_variant(cache, days, "trail", g)

    # comparison
    comp = pd.DataFrame([summary_row(k, v) for k, v in variants.items()])
    cols = ["label", "trading_days", "win_rate_%", "avg_ret_win_%", "avg_ret_loss_%",
            "avg_ret_all_%", "best_day_%", "worst_day_%", "sum_return_%",
            "pct_stopped", "pct_trail_exit", "pct_target_or_close"]
    comp = comp[cols]
    print("\n=== VARIANT COMPARISON (503 days) ===")
    print(comp.to_string(index=False))

    # trade journal for the RECOMMENDED 0.75 config (daily + exploded per-leg)
    jdr, jxr = [], []
    for day in days:
        legs, pret, reason = journal_day(cache[day], day, 0.75)
        res = "WIN" if pret > 0 else "LOSS"
        jdr.append({"entry_date": day, "n_stocks": len(legs),
                    "stocks": ", ".join(l["stock"] for l in legs),
                    "exit_time": legs[0]["exit_time"], "exit_reason": reason,
                    "portfolio_return_pct": pret, "result": res})
        for l in legs:
            jxr.append({**l, "day_portfolio_return_pct": pret, "day_result": res})
    jdaily = pd.DataFrame(jdr); jex = pd.DataFrame(jxr)
    jdaily.to_csv(OUT.parent / "Trailing_TradeLog_Daily.csv", index=False)
    jex.to_csv(OUT.parent / "Trailing_TradeLog_Exploded.csv", index=False)
    print(f"[*] journal (0.75): {len(jdaily)} days, {len(jex)} legs, "
          f"₹P&L {jex['pnl_rs'].sum():,.0f}", flush=True)

    # monthly + yearly for each variant
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        comp.to_excel(xl, sheet_name="Comparison", index=False)
        jdaily.to_excel(xl, sheet_name="Daily_TradeLog_0.75", index=False)
        jex.to_excel(xl, sheet_name="Journal_Exploded_0.75", index=False)
        for label, df in variants.items():
            safe = label.replace("%", "pc").replace(" ", "_").replace("+", "")[:28]
            monthly = df.groupby(["year", "month"]).apply(agg, include_groups=False).reset_index()
            yearly = df.groupby("year").apply(agg, include_groups=False).reset_index()
            monthly.to_excel(xl, sheet_name=f"M_{safe}"[:31], index=False)
            yearly.to_excel(xl, sheet_name=f"Y_{safe}"[:31], index=False)
        # full daily logs for the recommended giveback (0.50)
        rec = variants["Trail giveback 0.50%"][["entry_date", "return_pct", "reason"]]
        rec.to_excel(xl, sheet_name="Daily_Trail_0.50", index=False)

    # print monthly for baseline and recommended (0.50) for at-a-glance
    for label in ("Baseline +1% hard", "Trail giveback 0.50%"):
        df = variants[label]
        print(f"\n=== MONTHLY — {label} ===")
        m = df.groupby(["year", "month"]).apply(agg, include_groups=False).reset_index()
        print(m.to_string(index=False))

    print(f"\n[*] wrote {OUT}")
    if DESKTOP.exists():
        import shutil
        shutil.copy(OUT, DESKTOP / OUT.name)
        print(f"[*] copied to {DESKTOP / OUT.name}")


if __name__ == "__main__":
    main()
