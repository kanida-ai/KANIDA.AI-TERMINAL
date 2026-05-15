"""Tick-size rounding helper. NSE has per-scrip tick sizes (0.05 / 0.10 / 1.00 / etc.).

Caller passes the right tick (from mtf_eligibility.get_tick_size); this just rounds.
Kept separate to avoid import cycles between volatility_gate and the routers/services
that need to know per-stock tick.
"""
from __future__ import annotations


def round_to_tick_size(price: float, tick: float) -> float:
    """Round price to the nearest multiple of tick. Defensive on bad inputs."""
    if price <= 0:
        return 0.0
    if tick is None or tick <= 0:
        tick = 0.05
    # Two-decimal rounding to avoid floating-point creep
    return round(round(price / tick) * tick, 2)
