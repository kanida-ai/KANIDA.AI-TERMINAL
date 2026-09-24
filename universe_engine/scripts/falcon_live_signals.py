"""
Falcon V7.1 — Live EOD Signal Generator.

Runs after market close. Emits next-day buy candidates ranked by aggregate
pattern score.

Usage:
    python scripts/falcon_live_signals.py                     # latest date in DB
    python scripts/falcon_live_signals.py --date 2026-04-30   # specific date
    python scripts/falcon_live_signals.py --top-n 25          # top-25 picks

Output:
    reports/FALCON_LIVE_signals_<date>.md     human-readable picks
    reports/_falcon_live_signals_<date>.json  machine-readable (for execution)
    reports/_falcon_live_signals_latest.csv   convenient symlink for execution

Production cron flow (tomorrow's build):
    1. Refresh OHLC bars for today                   (T+0, ~5min)
    2. Rebuild ohlc_weekly                           (T+0, ~10s)
    3. Refresh falcon_features for new dates         (T+0, ~30s)
    4. Run this script                               (T+0, ~30s)
    5. Place buy-stop orders for top picks at 9:15+  (T+1)
"""
from __future__ import annotations
import argparse, csv, json, sqlite3, sys, time
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
from engine.falcon_portfolio import (
    load_panel_with_keys, load_promoted_patterns, compute_signals,
    rule_mask, FEATURE_IDX,
)

DB = ROOT / "data" / "db" / "kanida_universe.db"
REPORTS = ROOT / "reports"


def fmt_inr(x):
    if x is None: return "—"
    s = "-" if x < 0 else ""; x = abs(x)
    if x >= 1e7: return f"{s}₹{x/1e7:.2f} Cr"
    if x >= 1e5: return f"{s}₹{x/1e5:.2f} L"
    return f"{s}₹{x:,.0f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None,
                    help="Signal date (defaults to latest available in falcon_features)")
    ap.add_argument("--top-n", type=int, default=25,
                    help="Number of top picks to emit")
    ap.add_argument("--min-fires", type=int, default=2,
                    help="Minimum patterns that must fire to qualify (default: 2)")
    ap.add_argument("--include-drawdown-bounce", action="store_true",
                    help="V7.0 mode (default is V7.1: drop drawdown_bounce)")
    args = ap.parse_args()

    print("="*78)
    print("FALCON LIVE SIGNAL GENERATOR — V7.1")
    print("="*78)
    print()

    # Load panel
    print("[load] Reading feature panel...")
    t0 = time.time()
    X, syms, dates, years = load_panel_with_keys(DB)
    print(f"  {X.shape[0]:,} feature rows  ({time.time()-t0:.1f}s)")

    # Determine signal date
    if args.date:
        signal_date = args.date
    else:
        signal_date = max(dates)
    signal_year = int(signal_date[:4])
    print(f"\nSignal date: {signal_date}  (year {signal_year})")
    print(f"Eligible patterns: those mined BEFORE {signal_year}")

    # Load patterns (V7.1 default)
    con = sqlite3.connect(DB)
    if args.include_drawdown_bounce:
        patterns = load_promoted_patterns(con)
        engine_label = "V7.0"
    else:
        patterns = load_promoted_patterns(con, exclude_families=["drawdown_bounce"])
        engine_label = "V7.1"

    # Filter to walk-forward eligible
    eligible_patterns = [p for p in patterns if int(p["mined_year"]) < signal_year]
    print(f"\nEngine:           {engine_label}")
    print(f"Total patterns:   {len(patterns)}")
    print(f"Eligible today:   {len(eligible_patterns)} (mined before {signal_year})")
    if not eligible_patterns:
        sys.exit(f"\nERROR: no patterns mined before {signal_year}")

    # Vectorized fire mask per pattern
    mask_today = (dates == signal_date)
    n_today = int(mask_today.sum())
    print(f"\nStocks with feature data on {signal_date}: {n_today}")
    if n_today == 0:
        sys.exit(f"\nERROR: no feature data for {signal_date}")

    # For each eligible pattern, check which today's stocks fire it
    today_idx = np.where(mask_today)[0]
    today_X = X[today_idx]

    # Per-stock metadata
    stock_fire_counts = np.zeros(n_today, dtype=np.int32)
    stock_score = np.zeros(n_today, dtype=np.float64)
    fired_patterns_per_stock = [[] for _ in range(n_today)]

    print(f"\n[score] Evaluating {len(eligible_patterns)} patterns against {n_today} stocks...")
    for p in eligible_patterns:
        m = rule_mask(p["rule"], today_X)
        if not m.any(): continue
        stock_fire_counts += m.astype(np.int32)
        stock_score += m.astype(np.float64) * p["oos_lift"]
        for i in np.where(m)[0]:
            if len(fired_patterns_per_stock[i]) < 5:
                fired_patterns_per_stock[i].append({
                    "pattern_id": p["pattern_id"],
                    "target":     p["target"],
                    "oos_lift":   round(p["oos_lift"], 2),
                    "rule_str":   " AND ".join(f"{f}{op}{th:.2f}" for f, op, th in p["rule"]),
                })

    # Build candidate list
    candidates = []
    for j, i in enumerate(today_idx):
        if stock_fire_counts[j] < args.min_fires: continue
        candidates.append({
            "symbol":   syms[i],
            "n_fires":  int(stock_fire_counts[j]),
            "score":    float(stock_score[j]),
            "samples":  fired_patterns_per_stock[j][:5],
        })
    candidates.sort(key=lambda c: -c["score"])

    # Get sectors + last close for each candidate
    con = sqlite3.connect(DB)
    sectors = dict(con.execute("SELECT symbol, sector FROM falcon_sectors").fetchall())
    closes = dict(con.execute(
        "SELECT symbol, close FROM ohlc_daily WHERE trade_date = ?", (signal_date,)
    ).fetchall())
    # Liquidity proxy
    liq = dict(con.execute(f"""
        SELECT symbol, AVG(close*volume) FROM ohlc_daily
        WHERE trade_date BETWEEN date(?, '-60 days') AND ?
        GROUP BY symbol
    """, (signal_date, signal_date)).fetchall())
    con.close()

    for c in candidates:
        c["sector"]    = sectors.get(c["symbol"], "—")
        c["close"]     = closes.get(c["symbol"], None)
        c["avg_value_60d"] = liq.get(c["symbol"], 0)

    print(f"[score] Qualifying stocks (≥{args.min_fires} patterns fired): {len(candidates)}")
    top = candidates[:args.top_n]

    # ── Output ────────────────────────────────────────────────────────
    REPORTS.mkdir(parents=True, exist_ok=True)
    md_path  = REPORTS / f"FALCON_LIVE_signals_{signal_date}.md"
    json_path = REPORTS / f"_falcon_live_signals_{signal_date}.json"
    latest_csv = REPORTS / "_falcon_live_signals_latest.csv"

    # JSON for execution layer
    json_path.write_text(json.dumps({
        "engine": engine_label,
        "signal_date": signal_date,
        "n_patterns_eligible": len(eligible_patterns),
        "n_stocks_evaluated": n_today,
        "n_qualifying": len(candidates),
        "top_picks": top,
    }, indent=1, default=str))

    # CSV (broker-friendly)
    with open(latest_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["rank","symbol","sector","close","n_fires","score","avg_value_60d"])
        for i, c in enumerate(top, 1):
            w.writerow([i, c["symbol"], c["sector"], c["close"],
                          c["n_fires"], round(c["score"],1), int(c.get("avg_value_60d") or 0)])

    md = build_md(signal_date, engine_label, len(eligible_patterns), n_today,
                    len(candidates), top, args)
    md_path.write_text(md, encoding="utf-8")

    print()
    print(md)
    print(f"\nReport -> {md_path}")
    print(f"JSON   -> {json_path}")
    print(f"CSV    -> {latest_csv}")


def build_md(signal_date, engine_label, n_eligible, n_stocks, n_qualifying, top, args):
    L = [f"# Falcon Live Signals — {signal_date}", ""]
    L.append(f"- Engine: **{engine_label}**")
    L.append(f"- Signal date: **{signal_date}**")
    L.append(f"- Eligible patterns (mined before {int(signal_date[:4])}): {n_eligible}")
    L.append(f"- Stocks evaluated: {n_stocks}  ·  Qualifying (≥{args.min_fires} fires): {n_qualifying}")
    L.append(f"- Showing top {len(top)} by aggregate score")
    L.append("")
    L.append("## Buy candidates for next trading day")
    L.append("")
    L.append("| Rank | Symbol | Sector | Close | n_fires | Score | Liquidity (60d avg) |")
    L.append("|---|---|---|---|---|---|---|")
    for i, c in enumerate(top, 1):
        cl = f"₹{c['close']:.1f}" if c['close'] else "—"
        L.append(f"| {i} | **{c['symbol']}** | {c['sector'][:30]} | {cl} | "
                 f"{c['n_fires']} | {c['score']:.0f} | {fmt_inr(c.get('avg_value_60d'))} |")
    L.append("")

    L.append("## Sample pattern signatures (top 5 stocks)")
    L.append("")
    for c in top[:5]:
        L.append(f"### {c['symbol']} — {c['sector']}")
        L.append("")
        L.append(f"- **n_fires:** {c['n_fires']}  ·  **score:** {c['score']:.0f}")
        L.append(f"- **Sample of patterns that fired** (showing 5 of {c['n_fires']}):")
        L.append("")
        for s in c["samples"]:
            L.append(f"  - target={s['target']}, OOS lift={s['oos_lift']}pp")
            L.append(f"    rule: `{s['rule_str']}`")
        L.append("")

    L.append("## Execution checklist")
    L.append("")
    L.append("Next trading day (T+1):")
    L.append("- Place market-on-open buy orders for the symbols above (₹1L per name)")
    L.append("- Hold for 20 trading days OR until trailing-stop triggered (10-day low after +10% HW)")
    L.append("- Initial stop: -7% from avg entry (intraday low)")
    L.append("- If you see a gap-down >7% on entry day, accept the loss (no chasing)")
    L.append("- Skip a candidate if liquidity (60d avg) < ₹5 Cr/day")
    L.append("- Skip if the stock has any corp action ±5 days from today")
    L.append("")
    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main() or 0)
