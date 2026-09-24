"""Run the cash-constrained P&L simulator on V3 production signals."""
from __future__ import annotations
import argparse, json, sqlite3, sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.pnl_constrained import simulate, START_CAPITAL, PER_TRADE_RUPEES, MAX_CONCURRENT

DB        = ROOT / "data" / "db" / "kanida_universe.db"
SIGS_JSON = ROOT / "reports" / "_engine_v3_signals.json"
REPORT    = ROOT / "reports" / "ENGINE_V3_PNL_CONSTRAINED_REPORT.md"


def fmt_inr(x):
    if x is None: return "—"
    sign = "-" if x < 0 else ""
    x = abs(x)
    if x >= 1e7: return f"{sign}₹{x/1e7:.2f} Cr"
    if x >= 1e5: return f"{sign}₹{x/1e5:.2f} L"
    return f"{sign}₹{x:,.0f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital",   type=float, default=START_CAPITAL)
    ap.add_argument("--per-trade", type=float, default=PER_TRADE_RUPEES)
    ap.add_argument("--max-open",  type=int,   default=MAX_CONCURRENT)
    args = ap.parse_args()

    if not SIGS_JSON.exists():
        sys.exit(f"V3 signals JSON not found: {SIGS_JSON}")
    sigs = json.loads(SIGS_JSON.read_text())
    print(f"Loaded {len(sigs)} V3 signals")
    print(f"Starting capital: {fmt_inr(args.capital)} | per-trade: {fmt_inr(args.per_trade)} | max open: {args.max_open}")
    print()

    con = sqlite3.connect(DB)
    res = simulate(sigs, con,
                    starting_capital=args.capital,
                    per_trade=args.per_trade,
                    max_concurrent=args.max_open)

    md = build_report(res, args)
    REPORT.write_text(md, encoding="utf-8")
    print(md)
    print(f"\nReport -> {REPORT}")


def build_report(res, args):
    m = res["metrics"]
    L = ["# Engine V3 P&L — Cash-Constrained Simulator", ""]
    L.append(f"- Starting capital: {fmt_inr(args.capital)}")
    L.append(f"- Per-trade slug: {fmt_inr(args.per_trade)}")
    L.append(f"- Max concurrent positions: {args.max_open}")
    L.append(f"- No margin / no borrowing — skip when cash < ₹1L OR open >= {args.max_open}")
    L.append(f"- Hold: T+1 entry to T+5 close  ·  Costs: 30bps RT + 5bps slippage each side")
    L.append("")

    # Headline
    L.append("## Headline")
    L.append("")
    L.append(f"- **Starting capital:** {fmt_inr(m['starting_capital'])}")
    L.append(f"- **Ending equity:** {fmt_inr(m['ending_equity'])}")
    L.append(f"- **Total P&L:** {fmt_inr(m['total_pnl'])}")
    L.append(f"- **Return on starting capital:** {m['return_on_starting_pct']:+.2f}%")
    L.append(f"- **Trades taken:** {m['trades_taken']}")
    L.append(f"- **Trades skipped:** {m['trades_skipped']}  "
             f"(insufficient cash: {m['skipped_breakdown']['insufficient_cash']}, "
             f"max concurrent: {m['skipped_breakdown']['max_concurrent']})")
    L.append(f"- **Win rate:** {m['win_rate']:.1f}%")
    L.append(f"- **Avg win:** {fmt_inr(m['avg_win'])}  ·  **Avg loss:** {fmt_inr(m['avg_loss'])}  ·  "
             f"**W/L ratio:** {m['win_loss_ratio']:.2f}" if m['win_loss_ratio'] else "  ·  W/L: —")
    L.append(f"- **Best trade:** {fmt_inr(m['best_trade'])}  ·  **Worst trade:** {fmt_inr(m['worst_trade'])}")
    L.append(f"- **Max drawdown:** {fmt_inr(m['max_drawdown_rupees'])}  "
             f"({m['max_drawdown_pct']:+.2f}% of peak equity)")
    L.append(f"- **Max concurrent positions used:** {m['max_concurrent']} of {args.max_open}")
    L.append(f"- **Avg deployed:** {fmt_inr(m['avg_deployed'])}  ·  **Avg idle cash:** {fmt_inr(m['avg_idle_cash'])}")
    L.append(f"- **Avg utilization:** {m['avg_utilization']*100:.1f}%  ·  "
             f"**Peak utilization:** {m['peak_utilization']*100:.1f}%")
    L.append("")

    # Monthly P&L
    L.append("## Monthly P&L (by exit month)")
    L.append("")
    L.append("| Month | P&L |")
    L.append("|---|---|")
    for mo in sorted(m["monthly_pnl"]):
        L.append(f"| {mo} | {fmt_inr(m['monthly_pnl'][mo])} |")
    L.append("")

    # Skipped breakdown by date
    sk = res["skipped"]
    if sk:
        L.append("## Skipped trades (sample of first 20)")
        L.append("")
        L.append("| Symbol | Signal date | Reason | Cash at skip | Open at skip |")
        L.append("|---|---|---|---|---|")
        for s in sk[:20]:
            L.append(f"| {s['symbol']} | {s['signal_date']} | {s['reason']} | "
                     f"{fmt_inr(s.get('cash_at_skip',0))} | {s.get('open_at_skip','?')} |")
        L.append("")
        # Skipped count by date
        by_date = Counter(s["signal_date"] for s in sk)
        L.append("**Skipped count by signal date** (only days with skips shown):")
        L.append("")
        L.append("| Date | Skipped | Reason mix |")
        L.append("|---|---|---|")
        sk_by_d_reason = Counter((s["signal_date"], s["reason"]) for s in sk)
        for d in sorted({s["signal_date"] for s in sk}):
            n_cash = sk_by_d_reason.get((d, "insufficient_cash"), 0)
            n_max  = sk_by_d_reason.get((d, "max_concurrent"), 0)
            mix = []
            if n_cash: mix.append(f"cash={n_cash}")
            if n_max:  mix.append(f"max_concurrent={n_max}")
            L.append(f"| {d} | {by_date[d]} | {', '.join(mix)} |")
        L.append("")

    # Top trades
    L.append("## Top 10 winners")
    L.append("")
    L.append("| Symbol | Entry | Exit | Patterns | P&L | Ret % |")
    L.append("|---|---|---|---|---|---|")
    for t in sorted(res["trades"], key=lambda x: x["net_pnl"], reverse=True)[:10]:
        L.append(f"| {t['symbol']} | {t['entry_date']} | {t['exit_date']} | "
                 f"{t['patterns']} | {fmt_inr(t['net_pnl'])} | {t['ret_pct']:+.2f}% |")
    L.append("")
    L.append("## Bottom 10 losers")
    L.append("")
    L.append("| Symbol | Entry | Exit | Patterns | P&L | Ret % |")
    L.append("|---|---|---|---|---|---|")
    for t in sorted(res["trades"], key=lambda x: x["net_pnl"])[:10]:
        L.append(f"| {t['symbol']} | {t['entry_date']} | {t['exit_date']} | "
                 f"{t['patterns']} | {fmt_inr(t['net_pnl'])} | {t['ret_pct']:+.2f}% |")
    L.append("")

    # Daily timeline (every 3rd day for compactness)
    L.append("## Daily equity timeline (every 3rd day shown)")
    L.append("")
    L.append("| Date | Cash | Deployed | Unrealized | Equity | Open | Util % | DD % |")
    L.append("|---|---|---|---|---|---|---|---|")
    for i, tl in enumerate(res["timeline"]):
        if i % 3 == 0 or i == len(res["timeline"]) - 1:
            L.append(f"| {tl['date']} | {fmt_inr(tl['cash'])} | "
                     f"{fmt_inr(tl['deployed'])} | {fmt_inr(tl['unrealized'])} | "
                     f"{fmt_inr(tl['equity'])} | {tl['n_open']} | "
                     f"{tl['utilization']*100:.1f}% | {tl['drawdown_pct']:+.2f}% |")
    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main() or 0)
