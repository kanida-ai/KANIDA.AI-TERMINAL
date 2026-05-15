"""B4 — Weekly pattern publish (R&D -> PROD).

Renamed semantically 2026-05-10. Previous behavior tried to run B1+B2+B3
(mining + outcome labeling + validation) directly in PROD — which fails
because PROD doesn't have `falcon_outcomes` (527K rows). Per the blueprint,
those steps run in R&D only against the 1.5GB historical panel.

This job's actual job: copy R&D's freshly-validated promoted_patterns +
pattern_candidates into PROD, applying the operator's `mining_window_years`
cutoff filter from `falcon_engine_config`. Triggers:
  - Manually via /falcon/admin "Weekly re-mine (heavy)" button
  - Automatically by future Sun 19:30 IST cron (after R&D side completes)
"""
from ._run_log import log_job


def run() -> dict:
    with log_job("weekly_remine") as state:
        # Defer import to avoid pulling scripts/* at job-module load time.
        # publish_patterns.py is a thin wrapper — fast, transactional, safe to
        # call repeatedly (full-replace with cutoff filter + audit row).
        import sys
        from pathlib import Path
        scripts_dir = Path(__file__).resolve().parent.parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_dir))
        try:
            from publish_patterns import publish_patterns
        finally:
            try:
                sys.path.remove(str(scripts_dir))
            except ValueError:
                pass

        result = publish_patterns()  # uses engine_config.mining_window_years

        if result.get("status") != "success":
            state["error"] = f"publish_patterns returned status={result.get('status')}"
            return {"status": "failed", **result}

        rnd_filtered = result.get("rnd_filtered", {})
        prod_after   = result.get("prod_after", {})
        n_promoted   = rnd_filtered.get("promoted", 0)
        n_candidates = rnd_filtered.get("candidates", 0)

        state["rows"]  = n_promoted + n_candidates
        state["notes"] = (f"published {n_promoted} promoted / {n_candidates} candidates "
                          f"(window={result.get('mining_window_years')}yr, "
                          f"cutoff_year>={result.get('cutoff_year')})")
        return {
            "status":        "success",
            "promoted":      n_promoted,
            "candidates":    n_candidates,
            "prod_after":    prod_after,
            "cutoff_year":   result.get("cutoff_year"),
            "mining_window_years": result.get("mining_window_years"),
        }


if __name__ == "__main__":
    print(run())
