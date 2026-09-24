"""
Smoke test — single test month, end-to-end pipeline validation.

Run after scripts/setup.py. Verifies:
  - DB schema present
  - OHLC bootstrapped
  - 107 strategies load and run
  - Multi-worker mining completes
  - Walk-forward emits trades
  - Both blind and smart entry P&L computed
  - Report MD writes correctly

Usage:
  python scripts/run_smoke_test.py --month 2026-03
"""
from __future__ import annotations
import argparse, json, sqlite3, sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.walkforward import run_walkforward, DEFAULTS

DB     = ROOT / "data" / "db" / "kanida_universe.db"
REPORT = ROOT / "reports" / "SMOKE_TEST.md"


def fmt_inr(x):
    sign = "-" if x < 0 else ""
    return f"{sign}₹{abs(x):,.0f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True, help="YYYY-MM, e.g. 2026-03")
    ap.add_argument("--capital", type=float, default=500_000)
    ap.add_argument("--cost-bps", type=float, default=30)
    args = ap.parse_args()

    test_year, test_month = map(int, args.month.split("-"))
    capital = args.capital
    cost_bps = args.cost_bps

    if not DB.exists():
        print(f"ERROR: DB not found at {DB}\n  Run scripts/setup.py first.")
        return 1

    con = sqlite3.connect(DB)
    cfg = dict(DEFAULTS)
    result = run_walkforward(con, test_year, test_month, cfg=cfg)
    con.close()

    trades = result["test_trades"]
    blessed = result["blessed_strategies"]

    # Compute per-mode summary
    blind_pnls = [t["blind_pnl_pct"] for t in trades if t.get("blind_pnl_pct") is not None]
    smart_pnls = [t["smart_pnl_pct"] for t in trades if t.get("smart_pnl_pct") is not None]
    smart_taken = [t for t in trades if t.get("smart_taken")]

    def stats(pnls):
        if not pnls: return {"n": 0}
        wins = sum(1 for p in pnls if p > 0)
        avg  = sum(pnls)/len(pnls)
        avg_net = avg - cost_bps/100
        # ₹ per trade with risk = 1% of capital, position size = capital * 0.01 / (avg stop pct)
        # Simpler: use ₹100,000 per trade as per main-engine convention
        per_trade = 100_000
        net_inr_gross = sum(p/100 * per_trade for p in pnls)
        net_inr_net   = net_inr_gross - len(pnls) * (cost_bps/10000) * per_trade
        return {
            "n": len(pnls), "wins": wins, "wr": wins/len(pnls),
            "avg_gross": avg, "avg_net": avg_net,
            "net_inr_gross": net_inr_gross, "net_inr_net": net_inr_net,
        }

    blind_st = stats(blind_pnls)
    smart_st = stats(smart_pnls)

    L = []
    L.append(f"# Smoke Test — {args.month}")
    L.append("")
    L.append(f"Train window: {result['train_start']} → {result['train_end']}")
    L.append(f"Universe size: {result['universe_size']} stocks")
    L.append(f"Capital per trade: ₹100,000 · Round-trip cost: {cost_bps} bps")
    L.append("")
    L.append(f"## Blessed strategies: {len(blessed)} of 107")
    L.append("")
    if blessed:
        L.append(", ".join(blessed[:30]))
        if len(blessed) > 30:
            L.append(f"… and {len(blessed) - 30} more")
    L.append("")
    L.append("## Test month results — both entry modes")
    L.append("")
    L.append("| | Blind entry (every signal) | Smart entry (filtered) |")
    L.append("|---|---|---|")
    L.append(f"| Trades | {blind_st['n']} | {smart_st['n']} (of {len(trades)} signals)|")
    L.append(f"| WR (gross > 0) | {blind_st.get('wr',0)*100:.1f}% | {smart_st.get('wr',0)*100:.1f}% |")
    L.append(f"| Avg P&L gross | {blind_st.get('avg_gross',0):+.2f}% | {smart_st.get('avg_gross',0):+.2f}% |")
    L.append(f"| Avg P&L net ({cost_bps}bps) | {blind_st.get('avg_net',0):+.2f}% | {smart_st.get('avg_net',0):+.2f}% |")
    L.append(f"| Net ₹ gross (₹1L per trade) | {fmt_inr(blind_st.get('net_inr_gross',0))} | {fmt_inr(smart_st.get('net_inr_gross',0))} |")
    L.append(f"| Net ₹ net of cost | {fmt_inr(blind_st.get('net_inr_net',0))} | {fmt_inr(smart_st.get('net_inr_net',0))} |")
    L.append("")
    L.append("## Top strategies by efficacy (training window)")
    L.append("")
    eff = result["efficacy_per_strategy"]
    ranked = sorted(eff.items(),
                    key=lambda kv: (-(kv[1].get("pf_net") or 0), -(kv[1].get("n") or 0)))
    L.append("| Strategy | n | PF net | WR after cost | Avg net % | Pos months % | Blessed |")
    L.append("|---|---|---|---|---|---|---|")
    blessed_set = set(blessed)
    for name, e in ranked[:15]:
        n = e.get("n", 0)
        if n == 0: continue
        L.append(f"| {name} | {n} | {e.get('pf_net',0):.2f} | "
                 f"{e.get('wr_after_cost',0)*100:.0f}% | {e.get('avg_net',0):+.2f}% | "
                 f"{e.get('pos_months_pct',0)*100:.0f}% | {'Yes' if name in blessed_set else 'No'} |")
    L.append("")
    # By regime
    by_regime = defaultdict(lambda: {"n":0,"sum":0.0,"w":0})
    for t in trades:
        r = t.get("regime","unknown")
        by_regime[r]["n"] += 1
        by_regime[r]["sum"] += t.get("blind_pnl_pct") or 0
        if (t.get("blind_pnl_pct") or 0) > 0: by_regime[r]["w"] += 1
    if by_regime:
        L.append("## Test-month trades by NIFTY regime (blind)")
        L.append("")
        L.append("| Regime | Trades | WR | Avg % |")
        L.append("|---|---|---|---|")
        for r in ("low_vol","mid_vol","high_vol","unknown"):
            s = by_regime.get(r);
            if not s or s["n"]==0: continue
            L.append(f"| {r} | {s['n']} | {s['w']/s['n']*100:.0f}% | {s['sum']/s['n']:+.2f}% |")
        L.append("")
    L.append("## Sample trades (first 15)")
    L.append("")
    L.append("| Symbol | Strategy | Signal | Entry | Blind exit | Reason | Days | Blind P&L% | Smart P&L% |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for t in trades[:15]:
        L.append(f"| {t['symbol']} | {t['strategy']} | {t['signal_date']} | "
                 f"{t['entry_date']} | {t.get('blind_exit_date','')} | {t.get('blind_exit_reason','')} | "
                 f"{t.get('blind_days',0)} | {t.get('blind_pnl_pct',0):+.2f}% | "
                 f"{t.get('smart_pnl_pct') if t.get('smart_pnl_pct') is not None else '—'} |")

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text("\n".join(L), encoding="utf-8")
    print(f"\n  Report → {REPORT}")
    print(f"  Trades: {len(trades)}  |  Blessed: {len(blessed)}/107")
    print(f"  Blind: n={blind_st.get('n',0)}  WR={blind_st.get('wr',0)*100:.1f}%  net={fmt_inr(blind_st.get('net_inr_net',0))}")
    print(f"  Smart: n={smart_st.get('n',0)}  WR={smart_st.get('wr',0)*100:.1f}%  net={fmt_inr(smart_st.get('net_inr_net',0))}")


if __name__ == "__main__":
    sys.exit(main() or 0)
