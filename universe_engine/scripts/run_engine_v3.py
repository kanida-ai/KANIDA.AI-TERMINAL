"""Engine V3 — V2 + empirical fixes. Compares V3-all-signals, V3-top5-per-day, and Method B as default."""
from __future__ import annotations
import argparse, json, sqlite3, statistics, sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.engine_v3 import scan_universe_v3, TOP_N_PER_DAY

DB     = ROOT / "data" / "db" / "kanida_universe.db"
RES    = ROOT / "reports" / "_engine_v3_signals.json"
RES_T  = ROOT / "reports" / "_engine_v3_top.json"
REPORT = ROOT / "reports" / "ENGINE_V3_REPORT.md"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-03-05")
    ap.add_argument("--end",   default="2026-04-24")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--top-n", type=int, default=TOP_N_PER_DAY)
    ap.add_argument("--index", default="in_nifty200")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    rows = con.execute(f"""SELECT symbol FROM universe_master
                            WHERE is_active=1 AND {args.index}=1 ORDER BY symbol""").fetchall()
    symbols = [r[0] for r in rows]
    con.close()

    print(f"DB:        {DB}")
    print(f"Universe:  {len(symbols)} symbols")
    print(f"Eval:      {args.start}  ->  {args.end}")
    print(f"Workers:   {args.workers}")
    print(f"Top-N/day: {args.top_n}")
    print()

    all_sigs, top_sigs = scan_universe_v3(DB, symbols, args.start, args.end,
                                             n_workers=args.workers,
                                             top_n_per_day=args.top_n)
    print(f"\nAll V3 signals: {len(all_sigs)}")
    print(f"Top-{args.top_n}/day: {len(top_sigs)}")

    RES.parent.mkdir(parents=True, exist_ok=True)
    RES.write_text(json.dumps(all_sigs, indent=1, default=str))
    RES_T.write_text(json.dumps(top_sigs, indent=1, default=str))
    print(f"All JSON   -> {RES}")
    print(f"Top-N JSON -> {RES_T}")

    md = build_report(all_sigs, top_sigs, args)
    REPORT.write_text(md, encoding="utf-8")
    print(f"Report     -> {REPORT}")


def _summary(signals, suffix):
    filled = [s for s in signals if s.get(f"em_{suffix}_filled")]
    n = len(filled)
    if n == 0: return {"n_filled": 0, "fill_rate": 0}
    rets_1d = [s[f"ret_1d_{suffix}"] for s in filled if s.get(f"ret_1d_{suffix}") is not None]
    rets_5d = [s[f"ret_5d_{suffix}"] for s in filled if s.get(f"ret_5d_{suffix}") is not None]
    mfe_5d  = [s[f"mfe_5d_{suffix}"] for s in filled if s.get(f"mfe_5d_{suffix}") is not None]
    pos_5d = sum(1 for r in rets_5d if r > 0)
    n5 = len(rets_5d)
    p = lambda x: x * 100
    return {
        "n_filled":      n,
        "fill_rate":     n / max(len(signals), 1),
        "win_rate_5d":   p(pos_5d / n5) if n5 else 0,
        "mean_ret_1d":   p(statistics.mean(rets_1d)) if rets_1d else 0,
        "mean_ret_5d":   p(statistics.mean(rets_5d)) if rets_5d else 0,
        "median_ret_5d": p(statistics.median(rets_5d)) if rets_5d else 0,
        "mean_mfe_5d":   p(statistics.mean(mfe_5d)) if mfe_5d else 0,
        "hit_5pc":       p(sum(1 for s in filled if s.get(f"hit_5pc_{suffix}")) / n),
        "hit_10pc":      p(sum(1 for s in filled if s.get(f"hit_10pc_{suffix}")) / n),
        "hit_15pc":      p(sum(1 for s in filled if s.get(f"hit_15pc_{suffix}")) / n),
        "best_5d":       p(max(rets_5d)) if rets_5d else 0,
        "worst_5d":      p(min(rets_5d)) if rets_5d else 0,
    }


def _row(label, sm):
    if sm["n_filled"] == 0:
        return f"| {label} | 0 | — | — | — | — | — | — | — |"
    return (f"| {label} | {sm['n_filled']} | {sm['win_rate_5d']:.1f}% | "
             f"{sm['hit_5pc']:.1f}% | {sm['hit_10pc']:.1f}% | {sm['hit_15pc']:.1f}% | "
             f"{sm['mean_ret_5d']:+.2f}% | {sm['best_5d']:+.1f}% | {sm['worst_5d']:+.1f}% |")


def build_report(all_sigs, top_sigs, args):
    L = ["# Engine V3 — V2 + Empirical Fixes", ""]
    L.append(f"- Universe: {args.index}, eval window {args.start} -> {args.end}")
    L.append(f"- V3 changes: drop E, retire A, fakeout suppressor on D, top-{args.top_n}/day rank, default Method B")
    L.append(f"- Patterns retained: B (Compression+strict-OI), C (Heavy dry-up), D-v3 (Strong-close + fakeout suppressed)")
    L.append("")

    by_pattern = Counter(p for s in all_sigs for p in s["patterns"])
    by_pattern_top = Counter(p for s in top_sigs for p in s["patterns"])
    n_days = len({s["signal_date"] for s in all_sigs})
    L.append("## Frequency")
    L.append("")
    L.append(f"- All V3 signals: **{len(all_sigs)}** across {n_days} days  ({len(all_sigs)/max(n_days,1):.1f}/day)")
    L.append(f"- Top-{args.top_n}/day: **{len(top_sigs)}**  ({len(top_sigs)/max(n_days,1):.1f}/day average)")
    L.append(f"- Pattern fires (all): {dict(by_pattern)}")
    L.append(f"- Pattern fires (top): {dict(by_pattern_top)}")
    L.append("")

    # All-signals scorecard, A and B
    L.append("## Outcomes — V3 ALL signals (no top-N reduction)")
    L.append("")
    L.append("| Entry | n | WR 5d | Hit +5% | Hit +10% | Hit +15% | Mean ret 5d | Best | Worst |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    L.append(_row("A: Blind MOO", _summary(all_sigs, "a")))
    L.append(_row("B: Two-stage 9:30", _summary(all_sigs, "b")))
    L.append("")

    # Top-N scorecard
    L.append(f"## Outcomes — V3 TOP-{args.top_n} per day (concentrated)")
    L.append("")
    L.append("| Entry | n | WR 5d | Hit +5% | Hit +10% | Hit +15% | Mean ret 5d | Best | Worst |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    L.append(_row("A: Blind MOO", _summary(top_sigs, "a")))
    L.append(_row("B: Two-stage 9:30", _summary(top_sigs, "b")))
    L.append("")

    # Per-pattern (top-N, Method B — V3 default config)
    L.append(f"## Per-pattern (TOP-{args.top_n}/day, Method B — V3 DEFAULT CONFIG)")
    L.append("")
    L.append("| Pattern | n | WR 5d | Hit +5% | Hit +10% | Hit +15% | Mean ret 5d | Best | Worst |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for ptn in ("B", "C", "D"):
        sub = [s for s in top_sigs if ptn in s["patterns"]]
        L.append(_row(f"{ptn} (overlap)", _summary(sub, "b")))
    L.append("")

    # Compare V2 vs V3
    L.append("## V1 / V2 / V3 progression")
    L.append("")
    L.append("| Engine | n | WR 5d | Hit +5% | Hit +10% | Mean ret 5d |")
    L.append("|---|---|---|---|---|---|")
    L.append("| V1 (4-AND) | 22 | 40.9% | 18.2% | 0.0% | -1.26% |")
    L.append("| V2 (5-OR all signals, A) | 574 | 58.2% | 28.4% | 5.4% | +0.95% |")
    sa_all = _summary(all_sigs, "a"); sb_all = _summary(all_sigs, "b")
    sa_top = _summary(top_sigs, "a"); sb_top = _summary(top_sigs, "b")
    L.append(f"| V3 all (A) | {sa_all['n_filled']} | {sa_all['win_rate_5d']:.1f}% | {sa_all['hit_5pc']:.1f}% | {sa_all['hit_10pc']:.1f}% | {sa_all['mean_ret_5d']:+.2f}% |")
    L.append(f"| V3 all (B) | {sb_all['n_filled']} | {sb_all['win_rate_5d']:.1f}% | {sb_all['hit_5pc']:.1f}% | {sb_all['hit_10pc']:.1f}% | {sb_all['mean_ret_5d']:+.2f}% |")
    L.append(f"| **V3 top{args.top_n} (A)** | {sa_top['n_filled']} | {sa_top['win_rate_5d']:.1f}% | {sa_top['hit_5pc']:.1f}% | {sa_top['hit_10pc']:.1f}% | {sa_top['mean_ret_5d']:+.2f}% |")
    L.append(f"| **V3 top{args.top_n} (B)** | {sb_top['n_filled']} | {sb_top['win_rate_5d']:.1f}% | {sb_top['hit_5pc']:.1f}% | {sb_top['hit_10pc']:.1f}% | {sb_top['mean_ret_5d']:+.2f}% |")
    L.append("")

    # Top winners/losers from top-N Method B
    filled = [s for s in top_sigs if s.get("em_b_filled") and s.get("ret_5d_b") is not None]
    L.append(f"## Top 15 winners (TOP-{args.top_n}, Method B)")
    L.append("")
    L.append("| Symbol | Date | Patterns | Score | Entry | 5d ret | MFE | Close | OI 5d | Prior2d |")
    L.append("|---|---|---|---|---|---|---|---|---|---|")
    for s in sorted(filled, key=lambda x: x["ret_5d_b"], reverse=True)[:15]:
        oi = f"{s['oi_growth_5d']*100:+.0f}%" if s.get("oi_growth_5d") is not None else "—"
        p2 = f"{s['prior_2d_ret']*100:+.1f}%" if s.get("prior_2d_ret") is not None else "—"
        L.append(f"| {s['symbol']} | {s['signal_date']} | {''.join(s['patterns'])} | "
                 f"{s['score']:.2f} | {s['em_b_entry']:.2f} | {s['ret_5d_b']*100:+.2f}% | "
                 f"{(s.get('mfe_5d_b') or 0)*100:+.2f}% | {s['close_loc']*100:.0f}% | {oi} | {p2} |")
    L.append("")
    L.append(f"## Bottom 15 losers (TOP-{args.top_n}, Method B)")
    L.append("")
    L.append("| Symbol | Date | Patterns | Score | Entry | 5d ret | MAE | Close | OI 5d | Prior2d |")
    L.append("|---|---|---|---|---|---|---|---|---|---|")
    for s in sorted(filled, key=lambda x: x["ret_5d_b"])[:15]:
        oi = f"{s['oi_growth_5d']*100:+.0f}%" if s.get("oi_growth_5d") is not None else "—"
        p2 = f"{s['prior_2d_ret']*100:+.1f}%" if s.get("prior_2d_ret") is not None else "—"
        L.append(f"| {s['symbol']} | {s['signal_date']} | {''.join(s['patterns'])} | "
                 f"{s['score']:.2f} | {s['em_b_entry']:.2f} | {s['ret_5d_b']*100:+.2f}% | "
                 f"{(s.get('mae_5d_b') or 0)*100:+.2f}% | {s['close_loc']*100:.0f}% | {oi} | {p2} |")
    L.append("")
    L.append("## V3 verdict")
    L.append("")
    L.append(f"Default config (top-{args.top_n}/day + Method B):")
    L.append(f"- WR 5d: {sb_top['win_rate_5d']:.1f}%")
    L.append(f"- Hit +5%: {sb_top['hit_5pc']:.1f}%")
    L.append(f"- Hit +10%: {sb_top['hit_10pc']:.1f}%")
    L.append(f"- Hit +15%: {sb_top['hit_15pc']:.1f}%")
    L.append(f"- Mean 5d return: {sb_top['mean_ret_5d']:+.2f}%")
    L.append(f"- Signal frequency: {len(top_sigs)/max(n_days,1):.1f}/day")
    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main() or 0)
