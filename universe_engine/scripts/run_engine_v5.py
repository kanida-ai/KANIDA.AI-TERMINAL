"""V5 backtest: V4 signals + pyramid + trailing exit + cash constraints."""
from __future__ import annotations
import argparse, json, sqlite3, sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.engine_v5 import simulate

DB        = ROOT / "data" / "db" / "kanida_universe.db"
SIGS_JSON = ROOT / "reports" / "_engine_v3_signals.json"
REPORT    = ROOT / "reports" / "ENGINE_V5_REPORT.md"
TRADES    = ROOT / "reports" / "_v5_trades.json"
TIMELINE  = ROOT / "reports" / "_v5_timeline.json"


def fmt_inr(x):
    if x is None: return "—"
    s = "-" if x < 0 else ""; x = abs(x)
    if x >= 1e7: return f"{s}₹{x/1e7:.2f} Cr"
    if x >= 1e5: return f"{s}₹{x/1e5:.2f} L"
    return f"{s}₹{x:,.0f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital", type=float, default=30_00_000)
    ap.add_argument("--max-open", type=int, default=30)
    ap.add_argument("--filter", default="B-only",
                    choices=["B-only", "all-V3"],
                    help="B-only = Pattern B fires (V4 logic); all-V3 = all V3 signals")
    args = ap.parse_args()

    sigs = json.loads(SIGS_JSON.read_text())
    if args.filter == "B-only":
        sigs = [s for s in sigs if "B" in s["patterns"]]
    print(f"Input signals: {len(sigs)} ({args.filter})")
    print(f"Capital: {fmt_inr(args.capital)} | Max open: {args.max_open}")
    print()

    con = sqlite3.connect(DB)
    res = simulate(sigs, con,
                    starting_capital=args.capital,
                    max_concurrent=args.max_open)

    TRADES.write_text(json.dumps(res["trades"], indent=1, default=str))
    TIMELINE.write_text(json.dumps(res["timeline"], indent=1, default=str))

    md = build_report(res, args)
    REPORT.write_text(md, encoding="utf-8")
    print(md)
    print(f"\nReport -> {REPORT}")


def build_report(res, args):
    m = res["metrics"]
    L = ["# Engine V5 — V4 + Pyramid + Trailing Exit", ""]
    L.append(f"- Signal source: V4 ({args.filter}) — Pattern B firings (overlap allowed)")
    L.append(f"- Sizing: ₹1L base, +₹50k at +5%, +₹50k at +10% (cap ₹2L)")
    L.append(f"- Exit: -7% initial stop / +10% trigger trail at 10-day low / 30-day time stop")
    L.append(f"- Capital: {fmt_inr(args.capital)} starting, max {args.max_open} concurrent, no margin")
    L.append("")

    L.append("## Headline")
    L.append("")
    L.append(f"- **Starting capital:** {fmt_inr(m['starting_capital'])}")
    L.append(f"- **Ending equity:** {fmt_inr(m['ending_equity'])}")
    L.append(f"- **Total P&L:** {fmt_inr(m['total_pnl'])}")
    L.append(f"- **Return on capital:** {m['return_pct']:+.2f}%")
    L.append(f"- **Trades taken:** {m['trades_taken']}  ·  Skipped: {m['trades_skipped']}")
    L.append(f"- **Pyramided:** {m['n_pyramided']} of {m['trades_taken']}  ·  "
             f"Avg pyramid steps: {m['avg_pyramid_steps']}")
    L.append(f"- **Win rate:** {m['win_rate']:.1f}%")
    L.append(f"- **Avg win:** {fmt_inr(m['avg_win'])}  ·  **Avg loss:** {fmt_inr(m['avg_loss'])}  "
             f"·  **W/L:** {m['win_loss_ratio']:.2f}" if m['win_loss_ratio'] else "")
    L.append(f"- **Best trade:** {fmt_inr(m['best_trade'])}  ·  **Worst trade:** {fmt_inr(m['worst_trade'])}")
    L.append(f"- **Max drawdown:** {fmt_inr(m['max_drawdown_rupees'])}  "
             f"({m['max_drawdown_pct']:+.2f}%)")
    L.append(f"- **Max concurrent:** {m['max_concurrent']} of {args.max_open}")
    L.append(f"- **Avg deployed:** {fmt_inr(m['avg_deployed'])}  ·  "
             f"Avg idle: {fmt_inr(m['avg_idle_cash'])}")
    L.append(f"- **Avg utilization:** {m['avg_utilization']*100:.1f}%  ·  "
             f"Peak: {m['peak_utilization']*100:.1f}%")
    L.append(f"- **Exit reasons:** {dict(m['exit_reasons'])}")
    L.append("")

    L.append("## Monthly P&L")
    L.append("")
    L.append("| Month | P&L |")
    L.append("|---|---|")
    for mo in sorted(m["monthly_pnl"]):
        L.append(f"| {mo} | {fmt_inr(m['monthly_pnl'][mo])} |")
    L.append("")

    # V3 / V4 / V5 progression on ₹30L
    L.append("## Progression V3 → V4 → V5 (all on ₹30L)")
    L.append("")
    L.append("| Engine | Trades | WR | Avg win | Avg loss | W/L | Total P&L | ROI | Max DD% |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    L.append("| V3 (3-pat OR + Method B + T+5) | 82 | 58.5% | ₹3,681 | -₹2,078 | 1.77 | ₹1.06 L | +3.53% | -1.63% |")
    L.append("| V4 (B-only + Method A + T+5) | 79 | 54.4% | ₹3,518 | -₹2,155 | 1.53 | ₹66k | +2.20% | -1.14% |")
    wr = m['win_rate']; avgw = m['avg_win']; avgl = m['avg_loss']
    wlr = m['win_loss_ratio'] or 0
    L.append(f"| **V5 ({args.filter} + pyramid + trail)** | {m['trades_taken']} | "
             f"{wr:.1f}% | {fmt_inr(avgw)} | {fmt_inr(avgl)} | {wlr:.2f} | "
             f"{fmt_inr(m['total_pnl'])} | {m['return_pct']:+.2f}% | {m['max_drawdown_pct']:+.2f}% |")
    L.append("")

    # Top winners
    L.append("## Top 15 winners")
    L.append("")
    L.append("| Symbol | Entry | Exit | Patterns | Pyramids | HW% | Net P&L | Ret% (avg-entry) | Reason |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for t in sorted(res["trades"], key=lambda x: x["net_pnl"], reverse=True)[:15]:
        L.append(f"| {t['symbol']} | {t['entry_date']} | {t['exit_date']} | "
                 f"{t['patterns']} | {t['pyramid_steps']} | "
                 f"{t['high_water_pct']:+.1f}% | {fmt_inr(t['net_pnl'])} | "
                 f"{t['ret_pct_avg']:+.2f}% | {t['exit_reason']} |")
    L.append("")
    L.append("## Bottom 15 losers")
    L.append("")
    L.append("| Symbol | Entry | Exit | Patterns | Pyramids | HW% | Net P&L | Ret% | Reason |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for t in sorted(res["trades"], key=lambda x: x["net_pnl"])[:15]:
        L.append(f"| {t['symbol']} | {t['entry_date']} | {t['exit_date']} | "
                 f"{t['patterns']} | {t['pyramid_steps']} | "
                 f"{t['high_water_pct']:+.1f}% | {fmt_inr(t['net_pnl'])} | "
                 f"{t['ret_pct_avg']:+.2f}% | {t['exit_reason']} |")
    L.append("")

    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main() or 0)
