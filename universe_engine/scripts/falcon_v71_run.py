"""Falcon V7.1 — V7 minus drawdown_bounce cluster. Re-run portfolio simulator."""
from __future__ import annotations
import argparse, json, sqlite3, statistics, sys, time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
from engine.falcon_portfolio import (
    load_panel_with_keys, load_promoted_patterns, compute_signals, simulate,
    PER_TRADE,
)

DB = ROOT / "data" / "db" / "kanida_universe.db"
REPORT = ROOT / "reports" / "FALCON_V71_REPORT.md"


def fmt_inr(x):
    if x is None: return "—"
    s = "-" if x < 0 else ""; x = abs(x)
    if x >= 1e7: return f"{s}₹{x/1e7:.2f} Cr"
    if x >= 1e5: return f"{s}₹{x/1e5:.2f} L"
    return f"{s}₹{x:,.0f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital", type=float, default=30_00_000.0)
    ap.add_argument("--max-open", type=int, default=25)
    ap.add_argument("--hold-days", type=int, default=20)
    ap.add_argument("--top-n-per-day", type=int, default=10)
    ap.add_argument("--min-fires", type=int, default=2)
    ap.add_argument("--start-year", type=int, default=2023)
    args = ap.parse_args()

    print(f"DB: {DB}")
    print(f"Falcon V7.1 — drops 'drawdown_bounce' pattern cluster")
    print(f"Capital: {fmt_inr(args.capital)}  ·  per-trade: {fmt_inr(PER_TRADE)}  ·  "
          f"max-open: {args.max_open}  ·  hold: {args.hold_days}d")
    print()

    print("[load] Reading feature panel...")
    X, syms, dates, years = load_panel_with_keys(DB)
    print(f"  {X.shape[0]:,} rows")

    con = sqlite3.connect(DB)
    patterns_v7  = load_promoted_patterns(con)
    patterns_v71 = load_promoted_patterns(con, exclude_families=["drawdown_bounce"])
    con.close()
    print(f"[load] V7.0  patterns: {len(patterns_v7)}")
    print(f"[load] V7.1  patterns: {len(patterns_v71)}  "
           f"(dropped {len(patterns_v7)-len(patterns_v71)} drawdown_bounce)")

    n_fires, sum_lift = compute_signals(X, years, patterns_v71)
    qualifying = (n_fires >= args.min_fires) & (years >= args.start_year)
    signals = []
    for i in np.where(qualifying)[0]:
        signals.append({
            "symbol": syms[i], "signal_date": dates[i],
            "n_fires": int(n_fires[i]), "score": float(sum_lift[i]),
        })
    signals.sort(key=lambda s: (s["signal_date"], -s["score"]))
    print(f"\n[signals] {len(signals):,} qualifying rows (min_fires≥{args.min_fires})")

    print(f"\n[sim] Running portfolio sim ...")
    t0 = time.time()
    res = simulate(DB, signals, starting_capital=args.capital,
                    max_open=args.max_open, hold_days=args.hold_days,
                    top_n_per_day=args.top_n_per_day)
    print(f"  Done in {time.time()-t0:.0f}s. {len(res['trades'])} trades.")

    md = build_report(res, args, n_v7=len(patterns_v7), n_v71=len(patterns_v71))
    REPORT.write_text(md, encoding="utf-8")
    print(md)
    print(f"\nReport -> {REPORT}")


def build_report(res, args, n_v7, n_v71):
    m = res["metrics"]
    L = ["# Falcon V7.1 — Drawdown-Bounce Cluster Removed", ""]
    L.append(f"- Patterns: {n_v71} (dropped {n_v7 - n_v71} drawdown_bounce vs V7.0's {n_v7})")
    L.append(f"- Capital: {fmt_inr(args.capital)}  ·  per-trade: {fmt_inr(PER_TRADE)}  "
             f"·  max-open: {args.max_open}  ·  hold: {args.hold_days}d  "
             f"·  top-{args.top_n_per_day}/day  ·  min_fires={args.min_fires}")
    L.append(f"- Walk-forward: pattern mined_year < trading_year")
    L.append(f"- Bug fixes applied: gap-through stops, no-duplicate-symbol concurrent holds")
    L.append("")

    L.append("## Headline")
    L.append("")
    L.append(f"- **Starting capital:** {fmt_inr(m['starting_capital'])}")
    L.append(f"- **Ending equity:** {fmt_inr(m['ending_equity'])}")
    L.append(f"- **Total P&L:** {fmt_inr(m['total_pnl'])}")
    L.append(f"- **Return on starting:** {m['return_pct']:+.2f}%")
    L.append(f"- **Trades taken:** {m['trades_taken']}  ·  Skipped: {m['trades_skipped']}")
    L.append(f"- **Win rate:** {m['win_rate']:.1f}%  ·  W/L ratio: {m['wlr']:.2f}" if m.get('wlr') else "")
    L.append(f"- **Avg win:** {fmt_inr(m['avg_win'])}  ·  Avg loss: {fmt_inr(m['avg_loss'])}")
    L.append(f"- **Best:** {fmt_inr(m['best'])}  ·  Worst: {fmt_inr(m['worst'])}")
    L.append(f"- **Max DD:** {fmt_inr(m['max_dd'])} ({m['max_dd_pct']:+.2f}%)")
    L.append(f"- **Max concurrent:** {m['max_concurrent']} of {args.max_open}")
    L.append(f"- **Avg utilization:** {m['avg_util']*100:.1f}%")
    L.append("")

    L.append("## Per-year P&L")
    L.append("")
    L.append("| Year | P&L |")
    L.append("|---|---|")
    for yr in sorted(m["yearly"]):
        L.append(f"| {yr} | {fmt_inr(m['yearly'][yr])} |")
    L.append("")

    # Year-by-year DD
    timeline = res["timeline"]
    by_year_tl = defaultdict(list)
    for tl in timeline: by_year_tl[tl["date"][:4]].append(tl)
    L.append("## Year-by-year drawdown")
    L.append("")
    L.append("| Year | Days | Start eq | End eq | Trough eq | Max DD ₹ | Max DD % |")
    L.append("|---|---|---|---|---|---|---|")
    for yr in sorted(by_year_tl):
        rows = by_year_tl[yr]
        peak = rows[0]["equity"]
        max_dd = 0.0
        max_dd_pct = 0.0
        for r in rows:
            if r["equity"] > peak: peak = r["equity"]
            dd = r["equity"] - peak
            dd_pct = dd / peak * 100 if peak > 0 else 0
            if dd < max_dd: max_dd = dd
            if dd_pct < max_dd_pct: max_dd_pct = dd_pct
        L.append(f"| {yr} | {len(rows)} | {fmt_inr(rows[0]['equity'])} | "
                 f"{fmt_inr(rows[-1]['equity'])} | "
                 f"{fmt_inr(min(r['equity'] for r in rows))} | "
                 f"{fmt_inr(max_dd)} | {max_dd_pct:+.2f}% |")
    L.append("")

    # Monthly stats
    monthly = m["monthly"]
    pos = sum(1 for v in monthly.values() if v > 0)
    neg = sum(1 for v in monthly.values() if v < 0)
    L.append("## Monthly P&L summary")
    L.append("")
    L.append(f"- Total months: {len(monthly)}  ·  Positive: {pos} ({pos/len(monthly)*100:.0f}%)  "
             f"·  Negative: {neg}")
    L.append(f"- Best month: {fmt_inr(max(monthly.values()))}  ·  "
             f"Worst: {fmt_inr(min(monthly.values()))}  ·  "
             f"Median: {fmt_inr(statistics.median(monthly.values()))}")
    L.append("")
    L.append("| Month | P&L |")
    L.append("|---|---|")
    for mo in sorted(monthly): L.append(f"| {mo} | {fmt_inr(monthly[mo])} |")
    L.append("")

    # Top winners/losers
    L.append("## Top 15 winners")
    L.append("")
    L.append("| Symbol | Signal | Entry | Exit | Reason | n_fires | P&L | Ret % |")
    L.append("|---|---|---|---|---|---|---|---|")
    for t in sorted(res["trades"], key=lambda x: x["net_pnl"], reverse=True)[:15]:
        L.append(f"| {t['symbol']} | {t['signal_date']} | {t['entry_date']} | "
                 f"{t['exit_actual_date']} | {t['exit_reason']} | "
                 f"{t['n_fires']} | {fmt_inr(t['net_pnl'])} | {t['ret_pct']:+.2f}% |")
    L.append("")
    L.append("## Bottom 15 losers")
    L.append("")
    L.append("| Symbol | Signal | Entry | Exit | Reason | n_fires | P&L | Ret % |")
    L.append("|---|---|---|---|---|---|---|---|")
    for t in sorted(res["trades"], key=lambda x: x["net_pnl"])[:15]:
        L.append(f"| {t['symbol']} | {t['signal_date']} | {t['entry_date']} | "
                 f"{t['exit_actual_date']} | {t['exit_reason']} | "
                 f"{t['n_fires']} | {fmt_inr(t['net_pnl'])} | {t['ret_pct']:+.2f}% |")
    L.append("")
    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main() or 0)
