"""V6 confluence engine — run on full universe over 2 years, multi-period exit analysis."""
from __future__ import annotations
import argparse, json, sqlite3, statistics, sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.engine_v6 import scan_universe, BASE_PATTERNS, CONFLUENCES, HOLD_PERIODS

DB     = ROOT / "data" / "db" / "kanida_universe.db"
RES    = ROOT / "reports" / "_engine_v6_signals.json"
REPORT = ROOT / "reports" / "ENGINE_V6_REPORT.md"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-04-01")
    ap.add_argument("--end",   default="2026-04-01")
    ap.add_argument("--workers", type=int, default=24)
    ap.add_argument("--index",   default="in_nifty500")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    rows = con.execute(f"""SELECT symbol FROM universe_master
                            WHERE is_active=1 AND {args.index}=1 ORDER BY symbol""").fetchall()
    symbols = [r[0] for r in rows]
    con.close()

    print(f"DB:        {DB}")
    print(f"Universe:  {len(symbols)} symbols ({args.index})")
    print(f"Eval:      {args.start} -> {args.end}")
    print(f"Workers:   {args.workers}")
    print(f"Patterns:  {list(BASE_PATTERNS.keys())}")
    print(f"Confluences: {list(CONFLUENCES.keys())}")
    print(f"Hold periods: {HOLD_PERIODS}")
    print()

    sigs = scan_universe(DB, symbols, args.start, args.end, n_workers=args.workers)
    print(f"\nTotal signals: {len(sigs)}")

    RES.parent.mkdir(parents=True, exist_ok=True)
    RES.write_text(json.dumps(sigs, indent=1, default=str))
    print(f"Signals JSON  -> {RES}")

    md = build_report(sigs, args)
    REPORT.write_text(md, encoding="utf-8")
    print(f"Report        -> {REPORT}")


def _summary(rs, ret_key):
    rets = [r[ret_key] for r in rs if ret_key in r and r[ret_key] is not None]
    if not rets: return {}
    pos = sum(1 for x in rets if x > 0)
    return {
        "n": len(rets),
        "wr": pos / len(rets) * 100,
        "mean": statistics.mean(rets),
        "median": statistics.median(rets),
        "hit_5":  sum(1 for x in rets if x >=  5) / len(rets) * 100,
        "hit_10": sum(1 for x in rets if x >= 10) / len(rets) * 100,
        "hit_15": sum(1 for x in rets if x >= 15) / len(rets) * 100,
        "best":  max(rets),
        "worst": min(rets),
    }


def _row(label, sm):
    if not sm: return f"| {label} | 0 | — | — | — | — | — | — | — | — |"
    return (f"| {label} | {sm['n']} | {sm['wr']:.1f}% | "
             f"{sm['mean']:+.2f}% | {sm['median']:+.2f}% | "
             f"{sm['hit_5']:.1f}% | {sm['hit_10']:.1f}% | {sm['hit_15']:.1f}% | "
             f"{sm['best']:+.1f}% | {sm['worst']:+.1f}% |")


def build_report(sigs, args):
    L = ["# Engine V6 — OHLC+Volume Deterministic Confluence Engine", ""]
    L.append(f"- Universe: {args.index}, eval window {args.start} -> {args.end}")
    L.append(f"- Inputs: OHLC + Volume only. No OI, no intraday.")
    L.append(f"- Trigger: ≥1 base bullish pattern AND ≥2 confluence filters fire.")
    L.append(f"- Hold periods evaluated: 5d / 10d / 20d (4 weeks) / 30d.")
    L.append(f"- Entry assumed at next-day OPEN.")
    L.append("")

    n_days = len({s["signal_date"] for s in sigs})
    L.append(f"## Frequency: **{len(sigs)} signals** across {n_days} trading days "
             f"(~{len(sigs)/max(n_days,1):.1f}/day, ~{len(sigs)/max(n_days,1)*22:.0f}/month)")
    L.append("")

    # Pattern fire counts
    base_counts = Counter(p for s in sigs for p in s["bases"])
    conf_counts = Counter(c for s in sigs for c in s["confluences"])
    L.append("**Base pattern fires (overlap allowed):** "
             + ", ".join(f"{p}={n}" for p, n in base_counts.most_common()))
    L.append("")
    L.append("**Confluence fires (overlap allowed):** "
             + ", ".join(f"{c}={n}" for c, n in conf_counts.most_common()))
    L.append("")

    # Headline aggregate (all signals)
    L.append("## Aggregate outcomes (all signals)")
    L.append("")
    L.append("| Hold | n | WR | Mean | Median | Hit +5% | Hit +10% | Hit +15% | Best | Worst |")
    L.append("|---|---|---|---|---|---|---|---|---|---|")
    for n in HOLD_PERIODS:
        L.append(_row(f"{n}d hold", _summary(sigs, f"ret_{n}d")))
    L.append("")

    # Per base-pattern × hold period
    L.append("## Per BASE PATTERN × hold (overlap allowed)")
    L.append("")
    for ptn in BASE_PATTERNS:
        L.append(f"### {ptn}")
        L.append("")
        L.append("| Hold | n | WR | Mean | Median | Hit +5% | Hit +10% | Hit +15% | Best | Worst |")
        L.append("|---|---|---|---|---|---|---|---|---|---|")
        sub = [s for s in sigs if ptn in s["bases"]]
        for n in HOLD_PERIODS:
            L.append(_row(f"{n}d", _summary(sub, f"ret_{n}d")))
        L.append("")

    # Per confluence × hold
    L.append("## Per CONFLUENCE × hold (overlap allowed)")
    L.append("")
    for cf in CONFLUENCES:
        L.append(f"### + {cf}")
        L.append("")
        L.append("| Hold | n | WR | Mean | Median | Hit +5% | Hit +10% | Hit +15% | Best | Worst |")
        L.append("|---|---|---|---|---|---|---|---|---|---|")
        sub = [s for s in sigs if cf in s["confluences"]]
        for n in HOLD_PERIODS:
            L.append(_row(f"{n}d", _summary(sub, f"ret_{n}d")))
        L.append("")

    # By number of confluences (more = stronger)
    L.append("## By CONFLUENCE COUNT (signals with N confluences fired)")
    L.append("")
    for k in sorted({s["n_confs"] for s in sigs}):
        L.append(f"### {k} confluences")
        L.append("")
        L.append("| Hold | n | WR | Mean | Median | Hit +5% | Hit +10% | Hit +15% | Best | Worst |")
        L.append("|---|---|---|---|---|---|---|---|---|---|")
        sub = [s for s in sigs if s["n_confs"] == k]
        for n in HOLD_PERIODS:
            L.append(_row(f"{n}d", _summary(sub, f"ret_{n}d")))
        L.append("")

    # Top stocks by 20d hit rate (the shortlist)
    L.append("## Stock shortlist — best 20d performers (≥3 signals, sorted by Hit +10%)")
    L.append("")
    by_sym = defaultdict(list)
    for s in sigs: by_sym[s["symbol"]].append(s)
    rows = []
    for sym, group in by_sym.items():
        sm = _summary(group, "ret_20d")
        if sm.get("n", 0) < 3: continue
        rows.append((sym, sm))
    rows.sort(key=lambda x: (x[1]["hit_10"], x[1]["mean"]), reverse=True)
    L.append("| Symbol | Signals | WR | Mean 20d | Hit +10% | Hit +15% | Best | Worst |")
    L.append("|---|---|---|---|---|---|---|---|")
    for sym, sm in rows[:30]:
        L.append(f"| {sym} | {sm['n']} | {sm['wr']:.1f}% | {sm['mean']:+.2f}% | "
                 f"{sm['hit_10']:.1f}% | {sm['hit_15']:.1f}% | {sm['best']:+.1f}% | {sm['worst']:+.1f}% |")
    L.append("")

    # Top individual signals
    L.append("## Top 20 individual signals (by 20d return)")
    L.append("")
    L.append("| Symbol | Signal date | Bases | Confluences | 20d ret | 20d MFE |")
    L.append("|---|---|---|---|---|---|")
    have_20 = [s for s in sigs if s.get("ret_20d") is not None]
    for s in sorted(have_20, key=lambda x: x["ret_20d"], reverse=True)[:20]:
        L.append(f"| {s['symbol']} | {s['signal_date']} | {'+'.join(s['bases'])} | "
                 f"{'+'.join(s['confluences'])} | {s['ret_20d']:+.1f}% | "
                 f"{s.get('mfe_20d','?')}% |")
    L.append("")

    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main() or 0)
