"""
Engine V1 — full-universe scan + entry simulation + scorecard.

Usage:
    python scripts/run_engine_v1.py
    python scripts/run_engine_v1.py --start 2026-03-05 --end 2026-04-24 --workers 16
"""
from __future__ import annotations
import argparse, json, sqlite3, statistics, sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.engine_v1 import scan_universe

DB     = ROOT / "data" / "db" / "kanida_universe.db"
RES    = ROOT / "reports" / "_engine_v1_signals.json"
REPORT = ROOT / "reports" / "ENGINE_V1_REPORT.md"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-03-05",
                    help="Need 5d OI baseline + 30d vol baseline before; default skips Feb")
    ap.add_argument("--end",   default="2026-04-24",
                    help="Need 5d forward window after; default leaves Apr 25-30 for outcomes")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--index", default="in_nifty200")
    ap.add_argument("--require-oi", action="store_true",
                    help="Only count signals on F&O symbols where OI gate could be evaluated")
    args = ap.parse_args()

    if not DB.exists(): sys.exit(f"DB not found: {DB}")

    con = sqlite3.connect(DB)
    rows = con.execute(f"""
        SELECT symbol FROM universe_master
        WHERE is_active=1 AND {args.index}=1 ORDER BY symbol
    """).fetchall()
    symbols = [r[0] for r in rows]
    con.close()

    print(f"DB:        {DB}")
    print(f"Universe:  {len(symbols)} symbols")
    print(f"Eval:      {args.start}  ->  {args.end}")
    print(f"Workers:   {args.workers}")
    print(f"Require OI gate: {args.require_oi}")
    print()

    signals = scan_universe(DB, symbols, args.start, args.end, n_workers=args.workers)
    if args.require_oi:
        signals = [s for s in signals if s["has_oi"]]
    print(f"\n{len(signals)} signals fired.")

    RES.parent.mkdir(parents=True, exist_ok=True)
    RES.write_text(json.dumps(signals, indent=1, default=str))
    print(f"Signals JSON  -> {RES}")

    md = build_report(signals, args)
    REPORT.write_text(md, encoding="utf-8")
    print(f"Report        -> {REPORT}")


# ─────────────────────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────────────────────

def _summary(signals, suffix: str):
    """Aggregate stats for entry method indicated by 'a' or 'b' in suffix."""
    filled = [s for s in signals if s.get(f"em_{suffix}_filled")]
    n = len(filled)
    if n == 0:
        return {"n_filled": 0, "fill_rate": 0}

    rets_1d = [s[f"ret_1d_{suffix}"] for s in filled if s.get(f"ret_1d_{suffix}") is not None]
    rets_5d = [s[f"ret_5d_{suffix}"] for s in filled if s.get(f"ret_5d_{suffix}") is not None]
    mfe_5d  = [s[f"mfe_5d_{suffix}"] for s in filled if s.get(f"mfe_5d_{suffix}") is not None]
    mae_5d  = [s[f"mae_5d_{suffix}"] for s in filled if s.get(f"mae_5d_{suffix}") is not None]

    def pct(ratio): return ratio * 100

    n5 = len(rets_5d)
    pos_1d = sum(1 for r in rets_1d if r > 0)
    pos_5d = sum(1 for r in rets_5d if r > 0)
    return {
        "n_filled":      n,
        "fill_rate":     n / max(len(signals), 1),
        "n_5d":          n5,
        "win_rate_1d":   pct(pos_1d / max(len(rets_1d), 1)),
        "win_rate_5d":   pct(pos_5d / n5) if n5 else 0,
        "mean_ret_1d":   pct(statistics.mean(rets_1d)) if rets_1d else 0,
        "median_ret_1d": pct(statistics.median(rets_1d)) if rets_1d else 0,
        "mean_ret_5d":   pct(statistics.mean(rets_5d)) if rets_5d else 0,
        "median_ret_5d": pct(statistics.median(rets_5d)) if rets_5d else 0,
        "mean_mfe_5d":   pct(statistics.mean(mfe_5d)) if mfe_5d else 0,
        "mean_mae_5d":   pct(statistics.mean(mae_5d)) if mae_5d else 0,
        "hit_5pc":       pct(sum(1 for s in filled if s.get(f"hit_5pc_{suffix}")) / n),
        "hit_10pc":      pct(sum(1 for s in filled if s.get(f"hit_10pc_{suffix}")) / n),
        "hit_15pc":      pct(sum(1 for s in filled if s.get(f"hit_15pc_{suffix}")) / n),
        "best_5d":       pct(max(rets_5d)) if rets_5d else 0,
        "worst_5d":      pct(min(rets_5d)) if rets_5d else 0,
    }


def build_report(signals: list, args) -> str:
    L = ["# Engine V1 — VCP Breakout Setup Detector", ""]
    L.append(f"- Universe: {args.index}, eval window {args.start} -> {args.end}")
    L.append(f"- Setup criteria: range contraction + volume dry-up + late-day vol surge + OI buildup (where data exists)")
    L.append(f"- Two entry methods compared: (A) blind MOO vs (B) two-stage buy-stop with 9:30 vol confirmation")
    L.append(f"- Outcome metrics: 1d/5d return, MFE/MAE, hit rate at +5/+10/+15%")
    L.append("")

    # Frequency
    L.append("## Frequency")
    L.append("")
    by_day = Counter(s["signal_date"] for s in signals)
    by_sym = Counter(s["symbol"]      for s in signals)
    n_days = len({s["signal_date"] for s in signals})
    L.append(f"- Total signals fired: **{len(signals)}**")
    L.append(f"- Unique signal days:  {n_days}")
    L.append(f"- Avg signals per day: {len(signals) / max(n_days, 1):.2f}")
    L.append(f"- Top signal days: " + ", ".join(f"{d}({n})" for d, n in by_day.most_common(5)))
    L.append(f"- Top firing symbols: " + ", ".join(f"{s}({n})" for s, n in by_sym.most_common(8)))
    L.append("")

    # OI subset breakdown
    n_with_oi = sum(1 for s in signals if s["has_oi"])
    L.append(f"- Of {len(signals)} signals, **{n_with_oi}** had OI gate evaluable; "
             f"{len(signals)-n_with_oi} fired on the 3-component subset (non-F&O or pre-OI window).")
    L.append("")

    # Entry-method comparison
    sa = _summary(signals, "a")
    sb = _summary(signals, "b")
    L.append("## Entry method A/B comparison")
    L.append("")
    L.append("| Metric | (A) Blind MOO | (B) Two-stage buy-stop |")
    L.append("|---|---|---|")
    rows = [
        ("Filled (of all signals)", f"{sa['n_filled']} ({sa['fill_rate']*100:.0f}%)",
                                      f"{sb['n_filled']} ({sb['fill_rate']*100:.0f}%)"),
        ("Win rate, 1d",            f"{sa['win_rate_1d']:.1f}%", f"{sb['win_rate_1d']:.1f}%"),
        ("Win rate, 5d",            f"{sa['win_rate_5d']:.1f}%", f"{sb['win_rate_5d']:.1f}%"),
        ("Mean return, 1d",         f"{sa['mean_ret_1d']:+.2f}%", f"{sb['mean_ret_1d']:+.2f}%"),
        ("Median return, 1d",       f"{sa['median_ret_1d']:+.2f}%", f"{sb['median_ret_1d']:+.2f}%"),
        ("Mean return, 5d",         f"{sa['mean_ret_5d']:+.2f}%", f"{sb['mean_ret_5d']:+.2f}%"),
        ("Median return, 5d",       f"{sa['median_ret_5d']:+.2f}%", f"{sb['median_ret_5d']:+.2f}%"),
        ("Mean MFE (5d high)",      f"{sa['mean_mfe_5d']:+.2f}%", f"{sb['mean_mfe_5d']:+.2f}%"),
        ("Mean MAE (5d low)",       f"{sa['mean_mae_5d']:+.2f}%", f"{sb['mean_mae_5d']:+.2f}%"),
        ("Hit +5% (within 5d)",     f"{sa['hit_5pc']:.1f}%", f"{sb['hit_5pc']:.1f}%"),
        ("Hit +10% (within 5d)",    f"{sa['hit_10pc']:.1f}%", f"{sb['hit_10pc']:.1f}%"),
        ("Hit +15% (within 5d)",    f"{sa['hit_15pc']:.1f}%", f"{sb['hit_15pc']:.1f}%"),
        ("Best 5d return",          f"{sa['best_5d']:+.1f}%", f"{sb['best_5d']:+.1f}%"),
        ("Worst 5d return",         f"{sa['worst_5d']:+.1f}%", f"{sb['worst_5d']:+.1f}%"),
    ]
    for r in rows:
        L.append("| " + " | ".join(r) + " |")
    L.append("")

    # Sub-cohort: with-OI signals only
    with_oi = [s for s in signals if s["has_oi"]]
    if with_oi:
        sa_oi = _summary(with_oi, "a")
        L.append(f"## With-OI subset only ({len(with_oi)} signals)")
        L.append("")
        L.append("| Metric | Blind MOO (with OI gate) |")
        L.append("|---|---|")
        L.append(f"| Win rate, 5d | {sa_oi['win_rate_5d']:.1f}% |")
        L.append(f"| Mean ret 5d  | {sa_oi['mean_ret_5d']:+.2f}% |")
        L.append(f"| Hit +5%      | {sa_oi['hit_5pc']:.1f}% |")
        L.append(f"| Hit +10%     | {sa_oi['hit_10pc']:.1f}% |")
        L.append(f"| Hit +15%     | {sa_oi['hit_15pc']:.1f}% |")
        L.append("")

    # Top-10 winners and losers
    filled_a = [s for s in signals if s.get("em_a_filled") and s.get("ret_5d_a") is not None]
    L.append("## Top 10 winners (by 5d return, method A)")
    L.append("")
    L.append("| Symbol | Signal date | Entry | 5d ret | MFE 5d | OI gate |")
    L.append("|---|---|---|---|---|---|")
    for s in sorted(filled_a, key=lambda x: x["ret_5d_a"], reverse=True)[:10]:
        L.append(f"| {s['symbol']} | {s['signal_date']} | "
                 f"{s['em_a_entry']:.2f} | {s['ret_5d_a']*100:+.2f}% | "
                 f"{(s.get('mfe_5d_a') or 0)*100:+.2f}% | "
                 f"{'OK' if s.get('s4_oi_buildup') else ('-' if s.get('s4_oi_buildup') is None else 'N/A')} |")
    L.append("")
    L.append("## Top 10 losers (by 5d return, method A)")
    L.append("")
    L.append("| Symbol | Signal date | Entry | 5d ret | MAE 5d | OI gate |")
    L.append("|---|---|---|---|---|---|")
    for s in sorted(filled_a, key=lambda x: x["ret_5d_a"])[:10]:
        L.append(f"| {s['symbol']} | {s['signal_date']} | "
                 f"{s['em_a_entry']:.2f} | {s['ret_5d_a']*100:+.2f}% | "
                 f"{(s.get('mae_5d_a') or 0)*100:+.2f}% | "
                 f"{'OK' if s.get('s4_oi_buildup') else ('-' if s.get('s4_oi_buildup') is None else 'N/A')} |")
    L.append("")

    # Verdict
    L.append("## V1 verdict")
    L.append("")
    base_5d = 50.0   # rough universe base rate
    edge_5d = sa["win_rate_5d"] - base_5d
    L.append(f"- 5d win rate (Method A): **{sa['win_rate_5d']:.1f}%** vs ~{base_5d:.0f}% naive coin-flip => edge {edge_5d:+.1f}pp")
    L.append(f"- Hit-rate at +5% in 5d: **{sa['hit_5pc']:.1f}%**")
    L.append(f"- Method B fill rate: {sb['fill_rate']*100:.0f}% (the rest were filtered out by 9:30 vol confirmation)")
    L.append(f"- Mean 5d return per signal (Method A): **{sa['mean_ret_5d']:+.2f}%**")
    L.append("")
    L.append("Read the table to decide V2 priorities: which gate to drop, which to tighten, "
             "whether OI confirmation actually helps.")

    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main() or 0)
