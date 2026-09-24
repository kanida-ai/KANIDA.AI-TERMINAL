"""Engine V2 — multi-pattern OR'd detector. Per-pattern hit-rate measurement."""
from __future__ import annotations
import argparse, json, sqlite3, statistics, sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.engine_v2 import scan_universe_v2

DB     = ROOT / "data" / "db" / "kanida_universe.db"
RES    = ROOT / "reports" / "_engine_v2_signals.json"
REPORT = ROOT / "reports" / "ENGINE_V2_REPORT.md"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-03-05")
    ap.add_argument("--end",   default="2026-04-24")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--index", default="in_nifty200")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    rows = con.execute(f"""SELECT symbol FROM universe_master
                            WHERE is_active=1 AND {args.index}=1 ORDER BY symbol""").fetchall()
    symbols = [r[0] for r in rows]
    con.close()

    print(f"DB:        {DB}")
    print(f"Universe:  {len(symbols)} symbols  (in_nifty200 = top-200 mcap proxy)")
    print(f"Eval:      {args.start}  ->  {args.end}")
    print(f"Workers:   {args.workers}")
    print()

    signals = scan_universe_v2(DB, symbols, args.start, args.end, n_workers=args.workers)
    print(f"\n{len(signals)} signals fired (any pattern).")

    RES.parent.mkdir(parents=True, exist_ok=True)
    RES.write_text(json.dumps(signals, indent=1, default=str))
    print(f"Signals JSON  -> {RES}")

    md = build_report(signals, args)
    REPORT.write_text(md, encoding="utf-8")
    print(f"Report        -> {REPORT}")


def _summary(signals, suffix: str):
    filled = [s for s in signals if s.get(f"em_{suffix}_filled")]
    n = len(filled)
    if n == 0:
        return {"n_filled": 0, "fill_rate": 0}
    rets_1d = [s[f"ret_1d_{suffix}"] for s in filled if s.get(f"ret_1d_{suffix}") is not None]
    rets_5d = [s[f"ret_5d_{suffix}"] for s in filled if s.get(f"ret_5d_{suffix}") is not None]
    mfe_5d  = [s[f"mfe_5d_{suffix}"] for s in filled if s.get(f"mfe_5d_{suffix}") is not None]
    mae_5d  = [s[f"mae_5d_{suffix}"] for s in filled if s.get(f"mae_5d_{suffix}") is not None]
    pos_1d = sum(1 for r in rets_1d if r > 0)
    pos_5d = sum(1 for r in rets_5d if r > 0)
    n5 = len(rets_5d)
    p = lambda x: x * 100
    return {
        "n_filled":      n,
        "fill_rate":     n / max(len(signals), 1),
        "n_5d":          n5,
        "win_rate_1d":   p(pos_1d / max(len(rets_1d), 1)),
        "win_rate_5d":   p(pos_5d / n5) if n5 else 0,
        "mean_ret_1d":   p(statistics.mean(rets_1d)) if rets_1d else 0,
        "median_ret_1d": p(statistics.median(rets_1d)) if rets_1d else 0,
        "mean_ret_5d":   p(statistics.mean(rets_5d)) if rets_5d else 0,
        "median_ret_5d": p(statistics.median(rets_5d)) if rets_5d else 0,
        "mean_mfe_5d":   p(statistics.mean(mfe_5d)) if mfe_5d else 0,
        "mean_mae_5d":   p(statistics.mean(mae_5d)) if mae_5d else 0,
        "hit_5pc":       p(sum(1 for s in filled if s.get(f"hit_5pc_{suffix}")) / n),
        "hit_10pc":      p(sum(1 for s in filled if s.get(f"hit_10pc_{suffix}")) / n),
        "hit_15pc":      p(sum(1 for s in filled if s.get(f"hit_15pc_{suffix}")) / n),
        "best_5d":       p(max(rets_5d)) if rets_5d else 0,
        "worst_5d":      p(min(rets_5d)) if rets_5d else 0,
    }


def build_report(signals: list, args) -> str:
    L = ["# Engine V2 — Multi-pattern Breakout Detector", ""]
    L.append(f"- Universe: {args.index}, eval window {args.start} -> {args.end}")
    L.append(f"- Universal gate: close within 3% of 20d high")
    L.append(f"- 5 OR'd patterns: A=Bandhan smart-money | B=Compression+OI | C=Heavy dry-up |")
    L.append(f"  D=Strong-close imbalance | E=RS leadership")
    L.append(f"- Per-pattern close gates: A>=60%, C>=60%, D>=70%, B and E no gate")
    L.append("")

    # Frequency
    by_pattern = Counter()
    for s in signals:
        for p in s["patterns"]:
            by_pattern[p] += 1
    by_day = Counter(s["signal_date"] for s in signals)
    by_sym = Counter(s["symbol"] for s in signals)
    L.append("## Frequency")
    L.append("")
    L.append(f"- Total signals fired: **{len(signals)}** ({by_pattern.most_common()[0][1] if by_pattern else 0} max single pattern)")
    L.append(f"- Unique signal days:  {len({s['signal_date'] for s in signals})}")
    L.append(f"- Avg signals per day: {len(signals)/max(len({s['signal_date'] for s in signals}),1):.2f}")
    L.append(f"- Pattern fires (overlap allowed): " + ", ".join(f"{p}={n}" for p, n in by_pattern.most_common()))
    L.append(f"- Top firing symbols: " + ", ".join(f"{s}({n})" for s, n in by_sym.most_common(8)))
    L.append("")

    # Aggregate A/B comparison
    sa = _summary(signals, "a")
    sb = _summary(signals, "b")
    L.append("## ALL signals — entry method A vs B")
    L.append("")
    L.append("| Metric | (A) Blind MOO | (B) Two-stage |")
    L.append("|---|---|---|")
    rows = [
        ("Filled", f"{sa['n_filled']} ({sa['fill_rate']*100:.0f}%)", f"{sb['n_filled']} ({sb['fill_rate']*100:.0f}%)"),
        ("WR 5d", f"{sa['win_rate_5d']:.1f}%", f"{sb['win_rate_5d']:.1f}%"),
        ("Mean ret 5d", f"{sa['mean_ret_5d']:+.2f}%", f"{sb['mean_ret_5d']:+.2f}%"),
        ("Hit +5%", f"{sa['hit_5pc']:.1f}%", f"{sb['hit_5pc']:.1f}%"),
        ("Hit +10%", f"{sa['hit_10pc']:.1f}%", f"{sb['hit_10pc']:.1f}%"),
        ("Hit +15%", f"{sa['hit_15pc']:.1f}%", f"{sb['hit_15pc']:.1f}%"),
        ("Best 5d", f"{sa['best_5d']:+.1f}%", f"{sb['best_5d']:+.1f}%"),
        ("Worst 5d", f"{sa['worst_5d']:+.1f}%", f"{sb['worst_5d']:+.1f}%"),
    ]
    for r in rows: L.append("| " + " | ".join(r) + " |")
    L.append("")

    # Per-pattern stats (Method A)
    L.append("## Per-pattern hit rate (Method A — blind MOO)")
    L.append("")
    L.append("Each row is the subset where THAT pattern fired (overlap with other patterns allowed).")
    L.append("")
    L.append("| Pattern | n | WR 5d | Hit +5% | Hit +10% | Hit +15% | Mean ret 5d | Best 5d | Worst 5d |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for ptn in ("A", "B", "C", "D", "E"):
        sub = [s for s in signals if ptn in s["patterns"]]
        sm = _summary(sub, "a")
        if sm["n_filled"] == 0:
            L.append(f"| {ptn} | 0 | — | — | — | — | — | — | — |")
            continue
        L.append(f"| {ptn} | {sm['n_filled']} | {sm['win_rate_5d']:.1f}% | "
                 f"{sm['hit_5pc']:.1f}% | {sm['hit_10pc']:.1f}% | {sm['hit_15pc']:.1f}% | "
                 f"{sm['mean_ret_5d']:+.2f}% | {sm['best_5d']:+.1f}% | {sm['worst_5d']:+.1f}% |")
    L.append("")

    # Single-pattern only (no overlap) — purest read of each pattern's edge
    L.append("## Per-pattern, NON-overlapping subsets (purest pattern-isolation read)")
    L.append("")
    L.append("Signals where ONLY this pattern fired (no co-firing).")
    L.append("")
    L.append("| Pattern | n | WR 5d | Hit +5% | Hit +10% | Mean ret 5d |")
    L.append("|---|---|---|---|---|---|")
    for ptn in ("A", "B", "C", "D", "E"):
        sub = [s for s in signals if s["patterns"] == [ptn]]
        sm = _summary(sub, "a")
        if sm["n_filled"] == 0:
            L.append(f"| {ptn} | 0 | — | — | — | — |")
            continue
        L.append(f"| {ptn} | {sm['n_filled']} | {sm['win_rate_5d']:.1f}% | "
                 f"{sm['hit_5pc']:.1f}% | {sm['hit_10pc']:.1f}% | {sm['mean_ret_5d']:+.2f}% |")
    L.append("")

    # Top winners and losers (Method A)
    filled_a = [s for s in signals if s.get("em_a_filled") and s.get("ret_5d_a") is not None]
    L.append("## Top 15 winners (5d return, Method A)")
    L.append("")
    L.append("| Symbol | Date | Patterns | Entry | 5d ret | MFE | Close | OI 5d |")
    L.append("|---|---|---|---|---|---|---|---|")
    for s in sorted(filled_a, key=lambda x: x["ret_5d_a"], reverse=True)[:15]:
        oi = f"{s['oi_growth_5d']*100:+.0f}%" if s.get("oi_growth_5d") is not None else "—"
        L.append(f"| {s['symbol']} | {s['signal_date']} | {''.join(s['patterns'])} | "
                 f"{s['em_a_entry']:.2f} | {s['ret_5d_a']*100:+.2f}% | "
                 f"{(s.get('mfe_5d_a') or 0)*100:+.2f}% | {s['close_loc']*100:.0f}% | {oi} |")
    L.append("")
    L.append("## Bottom 15 losers (5d return, Method A)")
    L.append("")
    L.append("| Symbol | Date | Patterns | Entry | 5d ret | MAE | Close | OI 5d |")
    L.append("|---|---|---|---|---|---|---|---|")
    for s in sorted(filled_a, key=lambda x: x["ret_5d_a"])[:15]:
        oi = f"{s['oi_growth_5d']*100:+.0f}%" if s.get("oi_growth_5d") is not None else "—"
        L.append(f"| {s['symbol']} | {s['signal_date']} | {''.join(s['patterns'])} | "
                 f"{s['em_a_entry']:.2f} | {s['ret_5d_a']*100:+.2f}% | "
                 f"{(s.get('mae_5d_a') or 0)*100:+.2f}% | {s['close_loc']*100:.0f}% | {oi} |")

    # Verdict
    L.append("")
    L.append("## V2 verdict")
    L.append("")
    if sa["n_filled"]:
        L.append(f"- 5d win rate (all signals, Method A): **{sa['win_rate_5d']:.1f}%**")
        L.append(f"- Hit +5%: **{sa['hit_5pc']:.1f}%**, Hit +10%: **{sa['hit_10pc']:.1f}%**, Hit +15%: **{sa['hit_15pc']:.1f}%**")
        L.append(f"- Best individual signal: {sa['best_5d']:+.1f}% in 5d")
    L.append("")
    L.append("**Pattern-level decision rule for V3:**")
    L.append("- Keep patterns where Hit +5% > 30% AND Mean ret 5d > +1%")
    L.append("- Drop patterns where WR 5d < 40%")
    L.append("- Investigate patterns with high WR but low hit-+10% (might be small-edge, slow-grind)")
    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main() or 0)
