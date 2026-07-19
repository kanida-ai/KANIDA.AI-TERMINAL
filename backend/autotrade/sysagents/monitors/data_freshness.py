"""Monitor 4 — Data-freshness: did EOD run? is signal_date current vs expected?

DATA SOURCE (read-only): the EXISTING EOD signal-freshness gate in
falcon.jobs._pipeline — expected_signal_date_iso() (what signal_date SHOULD exist
for the current IST window) vs _latest_emitted_signal_date_iso()
(MAX(signal_date) from falcon_signals_live). This is the SAME source of truth the
V7 kick-off gate uses; we only read it.

  * Weekend / pre-16:05 weekday → no expectation → OK (fresh by definition).
  * On/after 16:05 weekday with latest < expected → EOD has not produced today's
    picks → ALERT (stale signals would be traded tomorrow).

Feature gaps / artifact version are not tracked in a queryable store today →
reported UNKNOWN, not fabricated.
"""
from __future__ import annotations

from ..base import MonitorAgent
from ..signals import HealthSignal, Status


class DataFreshnessMonitor(MonitorAgent):
    subsystem = "data-freshness"

    def observe(self, context=None) -> HealthSignal:
        metrics = {
            "expected_signal_date": None,
            "latest_signal_date": None,
            "feature_gaps": None,       # no queryable gap store today
            "artifact_version": None,   # no artifact-version store today
        }
        from falcon.jobs._pipeline import (expected_signal_date_iso,
                                           _latest_emitted_signal_date_iso)
        expected = expected_signal_date_iso()
        latest = _latest_emitted_signal_date_iso()
        metrics["expected_signal_date"] = expected
        metrics["latest_signal_date"] = latest

        if expected is None:
            return self._signal(
                Status.OK,
                "no EOD expectation now (weekend / pre-16:05) — fresh by "
                f"definition; latest signal_date={latest}", metrics)
        if not latest:
            return self._signal(
                Status.ALERT,
                f"EOD expected {expected} but falcon_signals_live is EMPTY — "
                "signals did not run", metrics)
        if latest >= expected:
            return self._signal(
                Status.OK,
                f"signals fresh (latest={latest} >= expected={expected})", metrics)
        return self._signal(
            Status.ALERT,
            f"STALE signals: latest={latest} < expected={expected} — EOD has not "
            "produced today's picks", metrics)
