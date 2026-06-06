from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from engine.learning.stock_pattern_learner import FIELD_NAMES


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        _write_text(path, "")
        return
    if fields is None:
        fields = list(rows[0].keys())
    target = _safe_path(path)
    with target.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_learning_reports(
    outputs: Path, rows: list[dict[str, Any]], config: dict[str, Any]
) -> None:
    top_n = int(config["reports"]["top_n"])
    candidates = _trusted_candidates(rows, config)
    write_csv(outputs / "reports" / "learned_pattern_candidates.csv", candidates, FIELD_NAMES)
    write_csv(outputs / "reports" / "trusted_patterns_by_stock.csv", candidates, FIELD_NAMES)
    write_stock_trust_summary(outputs / "reports" / "trusted_pattern_sets.md", candidates, config)
    write_stock_trust_csv(outputs / "reports" / "trusted_pattern_sets.csv", candidates)

    for scope in config["report_scopes"]:
        scoped = _scope_rows(rows, scope)
        top = scoped[:top_n]
        write_csv(outputs / "reports" / f"top_patterns_{scope.lower()}.csv", top, FIELD_NAMES)
        write_markdown_summary(
            outputs / "reports" / f"top_patterns_{scope.lower()}.md",
            top,
            title=f"Top learned patterns - {scope}",
        )


def write_live_reports(
    outputs: Path,
    matches: list[dict[str, Any]],
    clusters: list[dict[str, Any]],
    config: dict[str, Any],
) -> None:
    top_n = int(config["reports"]["top_n"])
    write_csv(
        outputs / "reports" / "live_matches.csv",
        matches,
        FIELD_NAMES
        + [
            "match_status",
            "match_precision",
            "latest_signal_date",
            "current_trend_state",
            "current_signals",
            "current_signal_count",
            "signal_overlap_ratio",
            "current_signal_overlap_ratio",
            "decision_score",
            "conviction_tier",
            "setup_type",
            "setup_summary",
        ],
    )
    write_csv(outputs / "reports" / "portfolio_clusters.csv", clusters)
    write_markdown_summary(
        outputs / "reports" / "live_matches.md",
        matches[:top_n],
        title="Current dry-run matches",
    )
    write_cluster_markdown(outputs / "reports" / "portfolio_clusters.md", clusters)
    write_insights(outputs / "reports" / "human_readable_insights.md", matches, clusters)


def write_markdown_summary(path: Path, rows: list[dict[str, Any]], title: str) -> None:
    lines = [f"# {title}", ""]
    if not rows:
        lines.append("No qualifying rows.")
    else:
        lines.append("| Rank | Market | Ticker | TF | Bias | Tier | Match | Pattern | Occ | Win Rate | Avg Dir Ret | Decision | Evidence |")
        lines.append("|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|")
        for i, row in enumerate(rows, 1):
            lines.append(
                f"| {i} | {row['market']} | {row['ticker']} | {row['timeframe']} | {row['bias']} | "
                f"{row.get('conviction_tier', row['roster_state'])} | {row.get('match_precision', '')} | {row['pattern']} | {row['occurrences']} | "
                f"{float(row['win_rate']):.2%} | {float(row['avg_directional_return']):.2%} | "
                f"{float(row.get('decision_score', row['evidence_score'])):.3f} | {float(row['evidence_score']):.3f} |"
            )
    _write_text(path, "\n".join(lines) + "\n")


def write_cluster_markdown(path: Path, clusters: list[dict[str, Any]]) -> None:
    lines = ["# Portfolio-level clusters", ""]
    if not clusters:
        lines.append("No portfolio clusters passed the configured minimum stock count.")
    else:
        lines.append("| Rank | Market | Sector | Bias | Pattern Type | Stocks | Avg Evidence | Avg Win Rate | Tickers |")
        lines.append("|---:|---|---|---|---|---:|---:|---:|---|")
        for i, row in enumerate(clusters[:50], 1):
            lines.append(
                f"| {i} | {row['market']} | {row['sector']} | {row['bias']} | {row['pattern_type']} | "
                f"{row['stock_count']} | {float(row['avg_evidence_score']):.3f} | "
                f"{float(row['avg_win_rate']):.2%} | {row['tickers']} |"
            )
    _write_text(path, "\n".join(lines) + "\n")


def write_insights(path: Path, matches: list[dict[str, Any]], clusters: list[dict[str, Any]]) -> None:
    lines = ["# Human-readable insights", ""]
    approved = [m for m in matches if m.get("match_status") == "approved_condition"]
    lines.append(f"- Approved current conditions: {len(approved)}")
    lines.append(f"- Other surfaced conditions: {len(matches) - len(approved)}")
    lines.append(f"- Portfolio clusters detected: {len(clusters)}")
    lines.append("")
    selected = matches[:20]
    for row in selected:
        lines.append(f"- {row.get('setup_summary') or _fallback_summary(row)}")
    if clusters:
        lines.append("")
        lines.append("## Cluster read-through")
        for row in clusters[:10]:
            lines.append(
                f"- {row['market']} {row['sector']} shows a {row['bias']} {row['pattern_type']} cluster "
                f"across {row['stock_count']} stocks. Average evidence score: "
                f"{float(row['avg_evidence_score']):.3f}."
            )
    _write_text(path, "\n".join(lines) + "\n")


def _fallback_summary(row: dict[str, Any]) -> str:
    return (
        f"{row['market']} {row['ticker']} is in {row.get('current_trend_state', row['trend_state'])}. "
        f"{row['pattern']} historically produced {float(row['win_rate']):.1%} win rate and "
        f"{float(row['avg_directional_return']):.2%} average 15-day directional return."
    )


def _scope_rows(rows: list[dict[str, Any]], scope: str) -> list[dict[str, Any]]:
    if scope == "COMBINED":
        return rows
    return [r for r in rows if r["market"] == scope]


def _trusted_candidates(rows: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
    max_candidates = int(config["reports"].get("max_candidate_rows", 200000))
    max_per_bucket = int(config["reports"].get("max_patterns_per_stock_trend", 8))
    groups: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)

    eligible = [
        r
        for r in rows
        if r["roster_state"] in {"keep", "watch"}
        and float(r["evidence_score"]) >= 0.35
    ]
    for row in eligible:
        bucket = (
            row["market"],
            row["ticker"],
            row["timeframe"],
            row["bias"],
            row["trend_state"],
        )
        groups[bucket].append(row)

    candidates: list[dict[str, Any]] = []
    for bucket_rows in groups.values():
        bucket_rows.sort(
            key=lambda r: (
                r["roster_state"] != "keep",
                -float(r["evidence_score"]),
                -int(r["occurrences"]),
                -float(r["avg_directional_return"]),
                int(r["pattern_size"]),
            )
        )
        candidates.extend(bucket_rows[:max_per_bucket])

    candidates.sort(
        key=lambda r: (
            r["roster_state"] != "keep",
            -float(r["evidence_score"]),
            -int(r["occurrences"]),
            -float(r["avg_directional_return"]),
        )
    )
    return candidates[:max_candidates]


def write_stock_trust_summary(path: Path, rows: list[dict[str, Any]], config: dict[str, Any]) -> None:
    top_n = int(config["reports"]["top_n"])
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["market"], row["ticker"], row["timeframe"], row["bias"])].append(row)

    stock_groups = sorted(
        grouped.items(),
        key=lambda item: (
            -(len(item[1])),
            -max(float(r["evidence_score"]) for r in item[1]),
            item[0],
        ),
    )[:top_n]

    lines = ["# Trusted pattern sets by stock", ""]
    if not stock_groups:
        lines.append("No stock-specific trusted pattern sets qualified.")
    else:
        for (market, ticker, timeframe, bias), items in stock_groups:
            items.sort(
                key=lambda r: (
                    r["roster_state"] != "keep",
                    -float(r["evidence_score"]),
                    -int(r["occurrences"]),
                    -float(r["avg_directional_return"]),
                )
            )
            lines.append(f"## {market} {ticker} {timeframe} {bias}")
            lines.append("")
            lines.append(
                f"- Trusted patterns retained: {len(items)}"
            )
            lines.append(
                f"- Best evidence score: {max(float(r['evidence_score']) for r in items):.3f}"
            )
            lines.append("")
            lines.append("| Trend State | Pattern | State | Occ | Win Rate | Avg Dir Ret | Evidence |")
            lines.append("|---|---|---|---:|---:|---:|---:|")
            for row in items[:8]:
                lines.append(
                    f"| {row['trend_state']} | {row['pattern']} | {row['roster_state']} | "
                    f"{row['occurrences']} | {float(row['win_rate']):.2%} | "
                    f"{float(row['avg_directional_return']):.2%} | {float(row['evidence_score']):.3f} |"
                )
            lines.append("")
    _write_text(path, "\n".join(lines) + "\n")


def write_stock_trust_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["market"], row["ticker"], row["timeframe"], row["bias"])].append(row)

    out_rows: list[dict[str, Any]] = []
    for (market, ticker, timeframe, bias), items in grouped.items():
        items.sort(
            key=lambda r: (
                r["roster_state"] != "keep",
                -float(r["evidence_score"]),
                -int(r["occurrences"]),
                -float(r["avg_directional_return"]),
            )
        )
        for rank, row in enumerate(items, 1):
            out_rows.append(
                {
                    "market": market,
                    "ticker": ticker,
                    "timeframe": timeframe,
                    "bias": bias,
                    "trusted_pattern_rank": rank,
                    "trusted_pattern_count_for_stock": len(items),
                    "trend_state": row["trend_state"],
                    "pattern_type": row["pattern_type"],
                    "pattern_size": row["pattern_size"],
                    "pattern": row["pattern"],
                    "roster_state": row["roster_state"],
                    "occurrences": row["occurrences"],
                    "win_rate": row["win_rate"],
                    "avg_directional_return": row["avg_directional_return"],
                    "recent_win_rate": row["recent_win_rate"],
                    "recent_avg_directional_return": row["recent_avg_directional_return"],
                    "stability": row["stability"],
                    "decay_flag": row["decay_flag"],
                    "evidence_score": row["evidence_score"],
                }
            )
    write_csv(path, out_rows)


def _write_text(path: Path, text: str) -> None:
    target = _safe_path(path)
    target.write_text(text, encoding="utf-8")


def _safe_path(path: Path) -> Path:
    try:
        with path.open("a", encoding="utf-8"):
            pass
        return path
    except PermissionError:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return path.with_name(f"{path.stem}_{stamp}{path.suffix}")
