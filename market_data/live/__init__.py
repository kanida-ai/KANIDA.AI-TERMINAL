"""Incremental live ingest for the 15-minute base store (contract section 5).

`ingest.py` walks the NIFTY 500 every cycle, asks the configured provider for
the 15-minute bars each symbol is missing between its last stored bar and the
provider's `latest_completed_bar`, and appends them to `db/market15.db` through
`market_data.store`.  `cli.py` is the operator front end.

Nothing here writes to `db/kanida.db`; it is opened `mode=ro` for the universe
and the base session calendar only.
"""
from .calendar_ext import live_calendar, nifty500, SymbolInfo
from .ingest import (
    DAILY_TABLE,
    CycleResult,
    LiveIngest,
    SymbolPlan,
    SymbolResult,
    ensure_live_schema,
    latest_live_bar,
    plan_symbol,
    read_daily_bars,
)

__all__ = [
    "DAILY_TABLE", "CycleResult", "LiveIngest", "SymbolInfo", "SymbolPlan",
    "SymbolResult", "ensure_live_schema", "latest_live_bar", "live_calendar",
    "nifty500", "plan_symbol", "read_daily_bars",
]
