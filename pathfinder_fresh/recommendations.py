from __future__ import annotations

from typing import Any


def build_research_queues(latest_edition: dict[str, Any] | None, watchlist_limit: int = 20) -> dict[str, list[dict[str, Any]]]:
    """Translate published research into governed stock queues.

    LONG/SHORT require the experiment gate. WATCHLIST contains published stock or
    sector observations that did not clear that gate. A whole-market observation
    is not expanded into a stock watchlist unless it actually cleared deployment.
    """
    queues: dict[str, list[dict[str, Any]]] = {"long": [], "short": [], "watchlist": []}
    if not latest_edition:
        return queues
    assigned_directional: set[str] = set()
    seen: dict[str, set[str]] = {key: set() for key in queues}
    findings = sorted(latest_edition.get("findings", []), key=lambda finding: -float(finding["publication_score"]))
    ordered = sorted(findings, key=lambda finding: not bool(finding.get("experiment_eligible")))
    for finding in ordered:
        evaluation = finding["evaluation"]
        symbols = evaluation.get("symbols", [])
        eligible = bool(finding.get("experiment_eligible"))
        direction = evaluation.get("direction")
        if eligible and direction in ("LONG", "SHORT"):
            queue_name = direction.lower()
            status = "CLEARED_RESEARCH_GATE"
        else:
            if finding["evidence"]["provenance"] == "whole_market":
                continue
            queue_name = "watchlist"
            status = "OBSERVE_ONLY_GATE_NOT_CLEARED"
        for rank, symbol in enumerate(symbols, start=1):
            if queue_name == "watchlist" and symbol in assigned_directional:
                continue
            if symbol in seen[queue_name]:
                continue
            if queue_name == "watchlist" and len(queues[queue_name]) >= watchlist_limit:
                break
            seen[queue_name].add(symbol)
            if queue_name in ("long", "short"):
                assigned_directional.add(symbol)
            queues[queue_name].append({
                "symbol": symbol,
                "research_action": queue_name.upper(),
                "status": status,
                "research_reason": finding["title"],
                "decision": finding["decision"],
                "public_subject": finding["public_subject"],
                "horizon_sessions": evaluation["horizon_sessions"],
                "historical_metric": finding["evidence"]["observed_metric"],
                "net_edge": finding["evidence"]["net_edge"],
                "sample_size": finding["evidence"]["sample_size"],
                "null_calibrated_p": finding["evidence"]["null_calibrated_p"],
                "cost_hurdle": finding["evidence"]["cost_hurdle"],
                "provenance": finding["evidence"]["provenance"],
                "publication_score": finding["publication_score"],
                "basket_rank": rank,
                "review_status": "PENDING_RA_REVIEW",
                "finding_id": finding["finding_id"],
            })
    return queues
