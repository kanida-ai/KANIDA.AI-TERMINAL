"""Run the P&L simulator on V3 production signals (Method B fills)."""
from __future__ import annotations
import argparse, json, sqlite3, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.pnl_simulator import (
    build_trades, build_daily_timeline, compute_metrics, PER_TRADE_RUPEES,
)

DB        = ROOT / "data" / "db" / "kanida_universe.db"
SIGS_JSON = ROOT / "reports" / "_engine_v3_signals.json"
REPORT    = ROOT / "reports" / "ENGINE_V3_PNL_REPORT.md"
TRADES_JSON  = ROOT / "reports" / "_v3_trades.json"
TIMELINE_JSON = ROOT / "reports" / "_v3_timeline.json"


def fmt_inr(x: float) -> str:
    """Indian formatting: lakhs and crores."""
    sign = "-" if x < 0 else ""
    x = abs(x)
    if x >= 1e7: return f"{sign}₹{x/1e7:.2f} Cr"
    if x >= 1e5: return f"{sign}₹{x/1e5:.2f} L"
    return f"{sign}₹{x:,.0f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-trade", type=float, default=PER_TRADE_RUPEES,
                    help="Rupees per trade (default 1L)")
    args = ap.parse_args()

    if not SIGS_JSON.exists():
        sys.exit(f"V3 signals JSON not found: {SIGS_JSON}\nRun scripts/run_engine_v3.py first.")

    sigs = json.loads(SIGS_JSON.read_text())
    print(f"Loaded {len(sigs)} V3 signals from {SIGS_JSON}")

    con = sqlite3.connect(DB)

    # Build trades from Method B fills only (V3 production config)
    trades = build_trades(sigs, con, per_trade=args.per_trade)
    print(f"Method B fills with full data: {len(trades)} trades\n")

    timeline = build_daily_timeline(trades, con, per_trade=args.per_trade)
    metrics  = compute_metrics(trades, timeline, per_trade=args.per_trade)

    TRADES_JSON.write_text(json.dumps(trades, indent=1, default=str))
    TIMELINE_JSON.write_text(json.dumps(timeline, indent=1, default=str))

    md = build_report(trades, timeline, metrics, args)
    REPORT.write_text(md, encoding="utf-8")
    print(md)
    print(f"\nReport       -> {REPORT}")
    print(f"Trades JSON  -> {TRADES_JSON}")
    print(f"Timeline JSON-> {TIMELINE_JSON}")


def build_report(trades, timeline, m, args) -> str:
    L = ["# Engine V3 P&L Simulator — ₹1L per trade, no capital cap", ""]
    L.append(f"- Strategy: V3 production (3 patterns OR'd + Method B 9:30 confirm)")
    L.append(f"- Per-trade slug: {fmt_inr(args.per_trade)}")
    L.append(f"- Capital: uncapped, scales with concurrent positions")
    L.append(f"- Hold rule: T+1 entry to T+5 close (5 trading days)")
    L.append(f"- Costs: 30 bps round-trip + 5 bps slippage each side")
    L.append("")

    L.append("## Headline")
    L.append("")
    L.append(f"- **Total P&L:** {fmt_inr(m['total_pnl'])}")
    L.append(f"- **N trades:** {m['n_trades']}")
    L.append(f"- **Sum capital deployed (n × ₹1L):** {fmt_inr(m['sum_deployed'])}")
    L.append(f"- **Peak capital deployed (max concurrent × ₹1L):** {fmt_inr(m['peak_deployed'])}")
    L.append(f"- **ROI on sum deployed:** {m['roc_sum_deployed']:+.2f}%")
    L.append(f"- **ROI on peak deployed:** {m['roc_peak_deployed']:+.2f}%")
    L.append(f"- **Max drawdown:** {fmt_inr(m['max_drawdown_rupees'])}  "
             f"({m['max_dd_pct_peak_deployed']:+.2f}% of peak deployed, "
             f"{m['max_dd_pct_peak_equity']:+.2f}% of peak equity)")
    L.append(f"- **Win rate:** {m['win_rate']:.1f}%")
    L.append(f"- **Avg win:** {fmt_inr(m['avg_win'])}  ·  **Avg loss:** {fmt_inr(m['avg_loss'])}  "
             f"·  **Win/Loss ratio:** "
             f"{m['win_loss_ratio']:.2f}" if m['win_loss_ratio'] else "—")
    L.append(f"- **Best trade:** {fmt_inr(m['best_trade'])}  ·  "
             f"**Worst trade:** {fmt_inr(m['worst_trade'])}")
    L.append(f"- **Max concurrent positions:** {m['max_concurrent']}")
    L.append(f"- **Capital utilization (avg/peak):** {m['capital_utilization']*100:.1f}%")
    L.append(f"- **Avg return per trade:** {m['avg_ret_pct']:+.2f}%")
    L.append("")

    # Monthly P&L
    L.append("## Monthly P&L (by exit month)")
    L.append("")
    L.append("| Month | P&L |")
    L.append("|---|---|")
    for mo in sorted(m["monthly_pnl"]):
        L.append(f"| {mo} | {fmt_inr(m['monthly_pnl'][mo])} |")
    L.append("")

    # Top trades
    L.append("## Top 10 winners")
    L.append("")
    L.append("| Symbol | Entry | Exit | Patterns | P&L | Ret % |")
    L.append("|---|---|---|---|---|---|")
    for t in sorted(trades, key=lambda x: x["net_pnl"], reverse=True)[:10]:
        L.append(f"| {t['symbol']} | {t['entry_date']} | {t['exit_date']} | "
                 f"{t['patterns']} | {fmt_inr(t['net_pnl'])} | {t['ret_pct']:+.2f}% |")
    L.append("")

    L.append("## Bottom 10 losers")
    L.append("")
    L.append("| Symbol | Entry | Exit | Patterns | P&L | Ret % |")
    L.append("|---|---|---|---|---|---|")
    for t in sorted(trades, key=lambda x: x["net_pnl"])[:10]:
        L.append(f"| {t['symbol']} | {t['entry_date']} | {t['exit_date']} | "
                 f"{t['patterns']} | {fmt_inr(t['net_pnl'])} | {t['ret_pct']:+.2f}% |")
    L.append("")

    # Drawdown sample
    L.append("## Equity curve milestones (every 5th day)")
    L.append("")
    L.append("| Date | n_open | Deployed | Cum realized | Unrealized | Equity | DD |")
    L.append("|---|---|---|---|---|---|---|")
    for i, tl in enumerate(timeline):
        if i % 5 == 0 or i == len(timeline) - 1:
            L.append(f"| {tl['date']} | {tl['n_open_after']} | "
                     f"{fmt_inr(tl['deployed_after'])} | {fmt_inr(tl['cum_realized'])} | "
                     f"{fmt_inr(tl['unrealized'])} | {fmt_inr(tl['equity'])} | "
                     f"{fmt_inr(tl['drawdown'])} |")
    L.append("")

    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main() or 0)
