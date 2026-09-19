from __future__ import annotations

import math
from typing import Any

from .questions import QUESTION_LIBRARY


REQUIRED_EVIDENCE = {
    "provenance",
    "sample_size",
    "period_start",
    "period_end",
    "regime",
    "comparison_group",
    "cost_hurdle",
    "observed_metric",
    "comparison_metric",
    "net_edge",
    "null_calibrated_p",
    "outcome_metric",
    "data_source",
}


def validate_snapshots(audit: dict[str, Any], public: dict[str, Any]) -> None:
    approved = {template.template_id for template in QUESTION_LIBRARY}
    threshold = float(audit["engine"]["policy"]["publication_threshold"])
    latest = audit.get("latest_edition") or {}
    for finding in latest.get("findings", []):
        if finding["template_id"] not in approved:
            raise AssertionError(f"Unapproved research question: {finding['template_id']}")
        if float(finding["publication_score"]) < threshold:
            raise AssertionError(f"Below-threshold finding was published: {finding['finding_id']}")
        missing = REQUIRED_EVIDENCE - set(finding["evidence"])
        if missing:
            raise AssertionError(f"Missing evidence fields for {finding['finding_id']}: {sorted(missing)}")
        if finding["grading_rule"]["horizon_sessions"] != finding["evaluation"]["horizon_sessions"]:
            raise AssertionError(f"Unfrozen horizon mismatch: {finding['finding_id']}")
        for key in ("observed_metric", "comparison_metric", "net_edge", "null_calibrated_p"):
            if not math.isfinite(float(finding["evidence"][key])):
                raise AssertionError(f"Non-finite {key}: {finding['finding_id']}")
    public_findings = (public.get("latest_edition") or {}).get("findings", [])
    for finding in public_findings:
        if finding.get("compliance_label") == "RA_REVIEW_REQUIRED" and finding["evaluation"].get("symbols"):
            raise AssertionError(f"Unreviewed stock constituent leaked: {finding['finding_id']}")
    board = audit["scoreboard"]
    if board["n"] != board["RIGHT"] + board["WRONG"] + board["INCONCLUSIVE"]:
        raise AssertionError("Scoreboard n is inconsistent")
    for hypothesis in audit.get("experiments", []):
        if hypothesis["versions_registered"] > hypothesis["max_versions"]:
            raise AssertionError(f"Version retirement rule breached: {hypothesis['hypothesis_id']}")
        gate = hypothesis["promotion_gate"]
        if gate["eligible"] and gate["oos_trials"] < gate["minimum_oos_trials"]:
            raise AssertionError(f"Premature promotion: {hypothesis['hypothesis_id']}")
