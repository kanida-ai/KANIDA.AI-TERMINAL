"""
Falcon Phase 4 — portfolio simulator using promoted patterns, walk-forward.

Generates signals via promoted patterns (only patterns mined BEFORE the trading
date are eligible — strict OOS). Runs cash-constrained portfolio sim.
"""
from __future__ import annotations
import argparse, json, sqlite3, sys, time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
from engine.falcon_portfolio import (
    load_panel_with_keys, load_promoted_patterns, compute_signals,
    simulate, START_CAP, PER_TRADE, MAX_OPEN, HOLD_DAYS,
)

DB = ROOT / "data" / "db" / "kanida_universe.db"
REPORT = ROOT / "reports" / "FALCON_V7_PORTFOLIO_REPORT.md"


def fmt_inr(x):
    if x is None: return "—"
    s = "-" if x < 0 else ""; x = abs(x)
    if x >= 1e7: return f"{s}₹{x/1e7:.2f} Cr"
    if x >= 1e5: return f"{s}₹{x/1e5:.2f} L"
    return f"{s}₹{x:,.0f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital", type=float, default=START_CAP)
    ap.add_argument("--max-open", type=int, default=MAX_OPEN)
    ap.add_argument("--hold-days", type=int, default=HOLD_DAYS)
    ap.add_argument("--top-n-per-day", type=int, default=10,
                    help="Take only top-N stocks by aggregate score per signal day")
    ap.add_argument("--min-fires", type=int, default=2,
                    help="Minimum number of patterns that must fire to qualify")
    ap.add_argument("--start-year", type=int, default=2023,
                    help="First year to trade (must be > min mined_year)")
    args = ap.parse_args()

    print(f"DB: {DB}")
    print(f"Capital: {fmt_inr(args.capital)}  ·  per-trade: {fmt_inr(PER_TRADE)}  ·  "
          f"max-open: {args.max_open}  ·  hold: {args.hold_days}d")
    print(f"Filter: min_fires≥{args.min_fires}, top-{args.top_n_per_day}/day, "
          f"trading from {args.start_year}")
    print()

    # Load panel
    print("[load] Reading feature panel...")
    t0 = time.time()
    X, syms, dates, years = load_panel_with_keys(DB)
    print(f"  {X.shape[0]:,} rows in {time.time()-t0:.1f}s")

    # Load patterns
    con = sqlite3.connect(DB)
    patterns = load_promoted_patterns(con)
    con.close()
    print(f"[load] {len(patterns)} promoted patterns (universal + regime_dependent)")

    # Compute signals
    n_fires, sum_lift = compute_signals(X, years, patterns)

    # Build signal list: rows where n_fires >= min_fires AND year >= start_year
    qualifying = (n_fires >= args.min_fires) & (years >= args.start_year)
    print(f"\n[signals] Qualifying rows: {qualifying.sum():,} "
          f"(min_fires={args.min_fires}, year>={args.start_year})")

    signals = []
    for i in np.where(qualifying)[0]:
        signals.append({
            "symbol":      syms[i],
            "signal_date": dates[i],
            "n_fires":     int(n_fires[i]),
            "score":       float(sum_lift[i]),
        })
    signals.sort(key=lambda s: (s["signal_date"], -s["score"]))
    print(f"[signals] Total: {len(signals):,}")

    # Simulate
    print(f"\n[sim] Running portfolio simulator (top-{args.top_n_per_day}/day) ...")
    t0 = time.time()
    res = simulate(DB, signals,
                    starting_capital=args.capital,
                    max_open=args.max_open,
                    hold_days=args.hold_days,
                    top_n_per_day=args.top_n_per_day)
    print(f"  Sim done in {time.time()-t0:.0f}s. {len(res['trades'])} trades.")

    md = build_report(res, args)
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(md, encoding="utf-8")
    print(md)
    print(f"\nReport -> {REPORT}")


def build_report(res, args):
    m = res["metrics"]
    L = ["# Falcon V7 — Portfolio Simulator", ""]
    L.append(f"- Starting capital: {fmt_inr(args.capital)}  ·  per-trade: {fmt_inr(PER_TRADE)}")
    L.append(f"- Max concurrent: {args.max_open}  ·  Hold: {args.hold_days}d  ·  "
             f"top-{args.top_n_per_day}/day  ·  min_fires={args.min_fires}")
    L.append(f"- Walk-forward: only patterns mined BEFORE the trade year are eligible.")
    L.append("")

    L.append("## Headline")
    L.append("")
    if not m: L.append("(No trades.)"); return "\n".join(L)
    L.append(f"- **Starting capital:** {fmt_inr(m['starting_capital'])}")
    L.append(f"- **Ending equity:** {fmt_inr(m['ending_equity'])}")
    L.append(f"- **Total P&L:** {fmt_inr(m['total_pnl'])}")
    L.append(f"- **Return on starting capital:** {m['return_pct']:+.2f}%")
    L.append(f"- **Trades taken:** {m['trades_taken']}  ·  Skipped: {m['trades_skipped']} "
             f"(cash={m['skip_breakdown']['cash']}, max_open={m['skip_breakdown']['max_open']})")
    L.append(f"- **Win rate:** {m['win_rate']:.1f}%")
    L.append(f"- **Avg win:** {fmt_inr(m['avg_win'])}  ·  Avg loss: {fmt_inr(m['avg_loss'])}  ·  "
             f"W/L: {m['wlr']:.2f}" if m.get('wlr') else "")
    L.append(f"- **Best trade:** {fmt_inr(m['best'])}  ·  Worst: {fmt_inr(m['worst'])}")
    L.append(f"- **Max DD:** {fmt_inr(m['max_dd'])} ({m['max_dd_pct']:+.2f}%)")
    L.append(f"- **Max concurrent:** {m['max_concurrent']} of {args.max_open}")
    L.append(f"- **Avg utilization:** {m['avg_util']*100:.1f}%")
    L.append("")

    L.append("## Per-year P&L (walk-forward)")
    L.append("")
    L.append("| Year | P&L |")
    L.append("|---|---|")
    for yr in sorted(m["yearly"]):
        L.append(f"| {yr} | {fmt_inr(m['yearly'][yr])} |")
    L.append("")

    L.append("## Monthly P&L")
    L.append("")
    L.append("| Month | P&L |")
    L.append("|---|---|")
    for mo in sorted(m["monthly"]):
        L.append(f"| {mo} | {fmt_inr(m['monthly'][mo])} |")
    L.append("")

    # Top winners / losers
    if res["trades"]:
        L.append("## Top 15 winners")
        L.append("")
        L.append("| Symbol | Signal | Entry | Exit (actual) | Reason | Score | n_fires | P&L | Ret % |")
        L.append("|---|---|---|---|---|---|---|---|---|")
        for t in sorted(res["trades"], key=lambda x: x["net_pnl"], reverse=True)[:15]:
            L.append(f"| {t['symbol']} | {t['signal_date']} | {t['entry_date']} | "
                     f"{t['exit_actual_date']} | {t['exit_reason']} | "
                     f"{t['score']:.1f} | {t['n_fires']} | {fmt_inr(t['net_pnl'])} | "
                     f"{t['ret_pct']:+.2f}% |")
        L.append("")
        L.append("## Bottom 15 losers")
        L.append("")
        L.append("| Symbol | Signal | Entry | Exit (actual) | Reason | Score | n_fires | P&L | Ret % |")
        L.append("|---|---|---|---|---|---|---|---|---|")
        for t in sorted(res["trades"], key=lambda x: x["net_pnl"])[:15]:
            L.append(f"| {t['symbol']} | {t['signal_date']} | {t['entry_date']} | "
                     f"{t['exit_actual_date']} | {t['exit_reason']} | "
                     f"{t['score']:.1f} | {t['n_fires']} | {fmt_inr(t['net_pnl'])} | "
                     f"{t['ret_pct']:+.2f}% |")
        L.append("")

        # Exit reason breakdown
        from collections import Counter
        reasons = Counter(t["exit_reason"] for t in res["trades"])
        L.append("## Exit reason distribution")
        L.append("")
        L.append("| Reason | Count |")
        L.append("|---|---|")
        for r, n in reasons.most_common(): L.append(f"| {r} | {n} |")
        L.append("")

    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main() or 0)
