"""Kanida AutoTrade — Automated Portfolio Trading System.

ADDITIVE extension of the existing Falcon trade layer. This package adds:
  - Multi-broker routing (Zerodha live by wrapping the existing Kite stack;
    Fyers/Upstox/Angel/Dhan stubbed).
  - A capital-allocation engine (equal / pct_cap / manual sizing).
  - Limit/VWAP order modes + F&O (FUT/CE/PE) instrument construction.
  - A tick-level PortfolioMonitor computing gross_return against the original
    total_allocated_capital.
  - A portfolio Kill Switch that flattens every broker in parallel.
  - A single exit gate (exit_lock) so the kill switch and the 4 existing
    per-position / day-bound exits never double-exit.

GUARDRAILS:
  * Live execution + kill switch ship DISABLED by default. A session only
    places real orders when kill_switch_enabled / live mode is explicitly on
    AND the master env FALCON_AUTOTRADE_ENABLED == 'true'.
  * DRY-RUN / PAPER mode (the default) places NO real orders.
  * Nothing here modifies the existing Falcon engine, tier classifier, the
    frontend, or the existing exit logic beyond the minimal exit-gate wrap.
"""

__all__ = ["config", "capital", "session"]
