"""ICEBERG order-leg planner — SPRINT CLUSTER 7.

Splits ONE logical order (entry or exit) into child legs each <= the effective
slice cap, so a single order above the per-symbol NSE FREEZE QUANTITY is never
rejected by the exchange and a large market order does not cause impact/slippage.
Required for large-capital deployment.

PURE FUNCTIONS ONLY — no DB, no broker, no side effects. INERT BY DEFAULT: the
session slices only when `config.iceberg_enabled` is True AND the total qty
exceeds the effective slice cap; otherwise plan_iceberg_legs returns a SINGLE leg
and the order path is byte-for-byte unchanged.

Effective slice = min(configured slice, exchange freeze cap) so slicing always
respects the exchange freeze quantity even when the operator did not set a slice.
"""
from __future__ import annotations

import math
from typing import List, Optional


def plan_iceberg_legs(total_qty: int, slice_qty: Optional[int]) -> List[int]:
    """Split `total_qty` into child legs each <= `slice_qty`, summing to total_qty.

    * A SINGLE leg [total_qty] when slicing is inapplicable — slice_qty is None /
      <= 0, or total_qty <= slice_qty.
    * Otherwise ceil(total_qty / slice_qty) legs: floor(total/slice) FULL legs of
      slice_qty, then the REMAINDER as the LAST leg (omitted when it is 0).
    * total_qty <= 0 → [] (nothing to place).

    Examples (the CAP 1 proof):
      plan_iceberg_legs(1000, 300) == [300, 300, 300, 100]   # ceil(1000/300)=4
      plan_iceberg_legs(250, 300)  == [250]                  # total <= slice
      plan_iceberg_legs(900, 300)  == [300, 300, 300]        # no remainder
    """
    total = int(total_qty or 0)
    if total <= 0:
        return []
    if not slice_qty or int(slice_qty) <= 0 or total <= int(slice_qty):
        return [total]
    s = int(slice_qty)
    full = total // s
    rem = total % s
    legs = [s] * full
    if rem:
        legs.append(rem)
    return legs


def freeze_cap_for(config, symbol: str) -> Optional[int]:
    """The exchange per-symbol FREEZE quantity cap for `symbol`, or None.

    Sourced from iceberg_freeze_qty_map[symbol] (per-symbol override) else the
    conservative iceberg_freeze_qty_default. This cap is applied even when the
    operator set no explicit slice, so a large order is ALWAYS sliced below the
    exchange freeze quantity (an order above the freeze qty is rejected outright)."""
    fmap = getattr(config, "iceberg_freeze_qty_map", None) or {}
    if symbol in fmap and fmap[symbol]:
        try:
            v = int(fmap[symbol])
            if v > 0:
                return v
        except (TypeError, ValueError):
            pass
    d = getattr(config, "iceberg_freeze_qty_default", None)
    if d is not None:
        try:
            v = int(d)
            if v > 0:
                return v
        except (TypeError, ValueError):
            pass
    return None


def effective_slice_qty(config, symbol: str, total_qty: int,
                        price: Optional[float] = None) -> Optional[int]:
    """The effective per-leg slice cap for `symbol` = MIN of every configured
    source, or None when NO cap applies (→ a single order).

    Sources (each optional; the SMALLEST positive value wins — the most
    conservative slice always governs):
      * iceberg_slice_qty         — explicit slice size (shares / units).
      * iceberg_slice_value/price — a ₹-notional slice → floor(value / price).
      * freeze cap                — the exchange freeze quantity (freeze_cap_for),
                                    respected even when no slice was configured.

    Returns None when iceberg is disabled OR no source yields a positive cap
    (→ plan_iceberg_legs returns a single leg = byte-for-byte unchanged)."""
    if not getattr(config, "iceberg_enabled", False):
        return None
    caps: List[int] = []
    sq = getattr(config, "iceberg_slice_qty", None)
    if sq is not None:
        try:
            if int(sq) > 0:
                caps.append(int(sq))
        except (TypeError, ValueError):
            pass
    sv = getattr(config, "iceberg_slice_value", None)
    if sv is not None and price:
        try:
            if float(sv) > 0 and float(price) > 0:
                v = int(math.floor(float(sv) / float(price)))
                if v > 0:
                    caps.append(v)
        except (TypeError, ValueError):
            pass
    fc = freeze_cap_for(config, symbol)
    if fc is not None and fc > 0:
        caps.append(int(fc))
    if not caps:
        return None
    return min(caps)


def plan_for(config, symbol: str, total_qty: int,
             price: Optional[float] = None) -> List[int]:
    """The child-leg plan for `symbol` under `config`. Returns [total_qty] (a
    single order) whenever iceberg is off / inapplicable, so a caller can branch
    on `len(plan_for(...)) > 1` to decide whether to slice."""
    eff = effective_slice_qty(config, symbol, total_qty, price)
    return plan_iceberg_legs(total_qty, eff)
