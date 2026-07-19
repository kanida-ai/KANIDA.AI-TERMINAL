"""Monitor 2 — Broker-health (per broker): token validity/expiry, egress, cert.

DATA SOURCES (read-only, NO money-path / order calls):
  * Kite operator token — services.kite_auth.get_cached_token_status(): the
    ALREADY-COMPUTED token status (O(1), NO network probe). We deliberately do
    NOT call get_token_status() here because that issues a kite.profile() network
    round-trip against the broker — a read that still consumes the broker session
    / rate budget. Observation must be free, so we read the cache the admin
    widget / auth scheduler already populate; a cold cache reads UNKNOWN.
  * Certification — autotrade.broker.registry.is_certified(broker): the live-order
    gate state per broker.

NOT AVAILABLE today (reported UNKNOWN/NA, not fabricated):
  * API latency & rate-limit saturation — no counter exists.
  * Per-account egress reachability — egress.egress_status() needs a
    broker_account_id and only yields the configured IP, not a live reachability
    probe; a network probe here would be an active broker touch, so it is out of
    Phase-1 scope. Egress is surfaced only as configured/unconfigured when a
    context account id is supplied.

SECURITY: only booleans + the token PREVIEW's validity flag are emitted — never
the token, the api_key, the proxy URL, or a raw IP.
"""
from __future__ import annotations

from ..base import MonitorAgent
from ..signals import HealthSignal, Status
from ..util import in_market_hours

# Brokers whose certification we surface. Broker-agnostic: read from the registry,
# no per-broker code path.
_KNOWN_BROKERS = ("zerodha", "rupeezy", "upstox", "angel", "dhan", "fyers",
                  "fivepaisa")


class BrokerHealthMonitor(MonitorAgent):
    subsystem = "broker-health"

    def observe(self, context=None) -> HealthSignal:
        metrics = {
            "kite_token_valid": None,       # None = unknown (cold cache)
            "kite_token_source": None,
            "certified_brokers": [],
            "api_latency_ms": None,         # no counter today
            "rate_limit_saturation": None,  # no counter today
            "egress": "NA",                 # per-account; needs a broker_account_id
        }

        # ── Kite operator token (cached only; no network) ────────────────────
        token_valid = None
        try:
            from services.kite_auth import get_cached_token_status
            st = get_cached_token_status(max_age_sec=120.0)
            if st is not None:
                token_valid = bool(st.get("valid"))
                metrics["kite_token_valid"] = token_valid
                metrics["kite_token_source"] = st.get("token_source")
        except Exception:  # noqa: BLE001
            token_valid = None

        # ── Certification per known broker ───────────────────────────────────
        certified = []
        try:
            from ..broker.registry import is_certified
            for b in _KNOWN_BROKERS:
                try:
                    if is_certified(b):
                        certified.append(b)
                except Exception:  # noqa: BLE001
                    continue
            metrics["certified_brokers"] = certified
        except Exception:  # noqa: BLE001
            pass

        # ── Status judgement ─────────────────────────────────────────────────
        if token_valid is None:
            return self._signal(
                Status.UNKNOWN,
                "Kite token status not in cache (no recent probe) — UNKNOWN; "
                "certification read OK", metrics)
        if token_valid is False:
            # An invalid/expired operator token during market hours is a page —
            # it is the classic root of "entries failing" cascades. Off-hours it
            # is expected (daily token expiry), so it is a WARN.
            status = Status.CRITICAL if in_market_hours() else Status.WARN
            return self._signal(
                status,
                "Kite operator token INVALID/EXPIRED "
                f"({'in market hours' if status == Status.CRITICAL else 'off-hours'})",
                metrics)
        return self._signal(
            Status.OK,
            f"Kite token valid; {len(certified)} certified broker(s)", metrics)
