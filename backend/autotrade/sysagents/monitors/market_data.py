"""Monitor 5 — Market-data: WS/ticker alive, tick freshness, bar gaps.

DATA SOURCE (read-only): the SHARED Falcon KiteTicker's diagnostic snapshot,
falcon.trade.services.kite_ticker.status() — connected flag, last_tick_at,
tick/subscription counts. This is a pure read of the ticker's in-memory state
(the same snapshot the /api/falcon/trade/ticker endpoint returns); it does NOT
touch the trading path and places no order.

SLO (from §8): tick freshness p95<1s/p99<2s; ALERT when the newest tick is >5s
old, CRITICAL when >15s old — but ONLY during market hours with the ticker
connected AND at least one symbol subscribed. Off-hours, disconnected (paper /
auth-not-ready), or nothing subscribed → NA/UNKNOWN (no ticks are EXPECTED, so a
stale age is not a fault). Per-symbol tick freshness and bar-gap detection are not
exposed by the ticker snapshot → reported UNKNOWN.
"""
from __future__ import annotations

from ..base import MonitorAgent
from ..signals import HealthSignal, Status
from ..util import age_seconds, in_market_hours

_ALERT_AGE_SEC = 5.0
_CRITICAL_AGE_SEC = 15.0


class MarketDataMonitor(MonitorAgent):
    subsystem = "market-data"

    def observe(self, context=None) -> HealthSignal:
        metrics = {
            "connected": None,
            "subscribed_count": None,
            "last_tick_age_sec": None,
            "tick_count": None,
            "per_symbol_freshness": None,   # not exposed by the snapshot
            "bar_gaps": None,               # not exposed by the snapshot
        }
        try:
            from falcon.trade.services import kite_ticker
            st = kite_ticker.status()
        except Exception:  # noqa: BLE001
            return self._signal(
                Status.UNKNOWN,
                "ticker status unavailable (module not importable)", metrics)

        connected = bool(st.get("connected"))
        subscribed = int(st.get("subscribed_count") or 0)
        age = age_seconds(st.get("last_tick_at"))
        metrics.update({
            "connected": connected,
            "subscribed_count": subscribed,
            "tick_count": st.get("tick_count"),
            "last_tick_age_sec": round(age, 2) if age is not None else None,
        })

        if not in_market_hours():
            return self._signal(
                Status.NA,
                f"off market hours — ticker connected={connected}, "
                f"no ticks expected", metrics)
        if not connected or subscribed == 0:
            # Off / not subscribed during hours: could be paper-only or auth not
            # ready. It is UNKNOWN (we cannot assert a data fault) not a page.
            return self._signal(
                Status.UNKNOWN,
                f"ticker not streaming in-hours (connected={connected}, "
                f"subscribed={subscribed}) — no live-session tick source", metrics)
        if age is None:
            return self._signal(
                Status.UNKNOWN,
                "ticker connected but no last_tick_at recorded yet", metrics)
        if age > _CRITICAL_AGE_SEC:
            return self._signal(
                Status.CRITICAL,
                f"tick freshness {age:.1f}s > {_CRITICAL_AGE_SEC:.0f}s (CRIT) — "
                "ticker stalled in market hours", metrics)
        if age > _ALERT_AGE_SEC:
            return self._signal(
                Status.ALERT,
                f"tick freshness {age:.1f}s > {_ALERT_AGE_SEC:.0f}s — ticks lagging",
                metrics)
        return self._signal(
            Status.OK,
            f"ticks fresh ({age:.1f}s), {subscribed} subscribed", metrics)
