"""Monitor 8 — Trading-stats (OBSERVATIONAL): per-strategy gross/net, WR, DD.

DATA SOURCE (read-only): today's CLOSED rows in autotrade_positions (realised_pnl)
— the same rows the P&L dashboard attributes. This is a lightweight, purely
OBSERVATIONAL summary: realised P&L, closed-trade count, and win-rate for the day.

This monitor NEVER pages a high severity from stats alone — a losing day is not a
platform fault. Drawdown-vs-baseline needs a per-strategy baseline store that does
not exist yet → reported UNKNOWN (not fabricated). Its role is to give the
orchestrator context, not to trigger action.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from falcon.db import falcon_conn

from ..base import MonitorAgent
from ..signals import HealthSignal, Status

IST = timezone(timedelta(hours=5, minutes=30))


class TradingStatsMonitor(MonitorAgent):
    subsystem = "trading-stats"

    def observe(self, context=None) -> HealthSignal:
        today = datetime.now(IST).date().isoformat()
        metrics = {
            "closed_trades_today": 0,
            "realised_pnl_today": 0.0,
            "win_rate_today": None,
            "drawdown_vs_baseline": None,   # no baseline store today
        }
        with falcon_conn() as con:
            rows = con.execute(
                "SELECT realised_pnl FROM autotrade_positions "
                "WHERE status='CLOSED' AND substr(COALESCE(closed_at,''),1,10)=?",
                (today,)).fetchall()
        pnls = [float(r["realised_pnl"]) for r in rows
                if r["realised_pnl"] is not None]
        n = len(pnls)
        metrics["closed_trades_today"] = n
        metrics["realised_pnl_today"] = round(sum(pnls), 2)
        if n > 0:
            wins = sum(1 for p in pnls if p > 0)
            metrics["win_rate_today"] = round(wins / n, 4)

        # Purely observational: OK when we have data, UNKNOWN when there is none
        # to report yet. Never escalates to a page.
        if n == 0:
            return self._signal(
                Status.UNKNOWN,
                "no closed trades today yet (observational; no baseline store)",
                metrics)
        return self._signal(
            Status.OK,
            f"{n} closed trade(s) today, realised Rs{metrics['realised_pnl_today']:,.0f}, "
            f"WR {metrics['win_rate_today']:.0%} (observational)", metrics)
