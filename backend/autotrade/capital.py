"""CapitalAllocator — three sizing modes + quantity calculation.

Sizing modes (select via TradingSessionConfig.sizing_mode):
  equal    : capital / top_n_stocks per position
  pct_cap  : min(capital / n, capital * max_pct_per_position) per position
  manual   : exact per-symbol ₹ amounts from config.manual_amounts

calculate_quantity fetches live LTP + lot size from the broker at entry time:
  EQ / MTF / CNC : floor(amount / ltp)
  FUT            : floor(amount / (ltp * lot_size)) * lot_size
  CE / PE        : floor(amount / (premium * lot_size)) * lot_size
Never returns 0 — raises InsufficientCapitalError when amount < one lot value.
Lot sizes ALWAYS come from the broker instrument master (never hardcoded).
"""
from __future__ import annotations

import math
from typing import Dict, List

from .config import TradingSessionConfig


class InsufficientCapitalError(Exception):
    """Raised when the allocated amount cannot fund even one share / lot."""


class CapitalAllocator:
    def __init__(self, config: TradingSessionConfig):
        self.config = config

    # ── Per-symbol ₹ allocation ───────────────────────────────────────────────
    def allocate(self, symbols: List[str]) -> Dict[str, float]:
        """Return {symbol: rupee_amount} per the configured sizing mode.

        `symbols` is the routed pick list for ONE broker profile. The caller is
        responsible for ensuring the total respects the profile's allocated
        capital; this method sizes against config.total_allocated_capital, which
        for a single-broker session equals the profile capital.
        """
        cfg = self.config
        n = len(symbols)
        if n == 0:
            return {}

        if cfg.sizing_mode == "equal":
            per = cfg.total_allocated_capital / n
            return {s: per for s in symbols}

        if cfg.sizing_mode == "pct_cap":
            cap = cfg.total_allocated_capital * cfg.max_pct_per_position
            per = min(cfg.total_allocated_capital / n, cap)
            return {s: per for s in symbols}

        if cfg.sizing_mode == "manual":
            # Only symbols present in manual_amounts get funded.
            out = {s: float(cfg.manual_amounts.get(s, 0.0)) for s in symbols}
            return out

        raise ValueError(f"unknown sizing_mode: {cfg.sizing_mode}")

    # ── Quantity from broker LTP + lot size ───────────────────────────────────
    def calculate_quantity(self, symbol: str, amount: float, broker) -> int:
        """Compute the integer quantity to order for `amount` rupees.

        `broker` is a BrokerClient. We call broker.get_ltp(symbol) and, for F&O,
        broker.get_lot_size(contract). Never returns 0.
        """
        cfg = self.config
        itype = cfg.instrument_type

        ltp = broker.get_ltp(symbol)
        if ltp is None or ltp <= 0:
            raise InsufficientCapitalError(
                f"{symbol}: no valid LTP from broker (got {ltp})"
            )

        if itype in ("EQ", "MTF"):
            qty = math.floor(amount / ltp)
            if qty < 1:
                raise InsufficientCapitalError(
                    f"{symbol}: amount ₹{amount:.0f} < 1 share at ₹{ltp:.2f}"
                )
            return qty

        if itype == "FUT":
            contract = broker.get_active_futures(symbol, cfg.expiry_preference)
            lot_size = broker.get_lot_size(contract)
            if lot_size <= 0:
                raise InsufficientCapitalError(f"{symbol}: invalid lot_size {lot_size}")
            lots = math.floor(amount / (ltp * lot_size))
            if lots < 1:
                raise InsufficientCapitalError(
                    f"{symbol}: amount ₹{amount:.0f} < 1 FUT lot "
                    f"(₹{ltp * lot_size:.0f}/lot)"
                )
            return lots * lot_size

        if itype in ("CE", "PE"):
            chain = broker.get_option_chain(symbol)
            strike = _select_atm_strike(chain, itype, ltp)
            contract = broker.get_option_contract(symbol, strike, cfg.expiry_preference)
            lot_size = broker.get_lot_size(contract)
            premium = broker.get_ltp(contract)
            if premium is None or premium <= 0 or lot_size <= 0:
                raise InsufficientCapitalError(
                    f"{symbol}: invalid option premium/lot ({premium}/{lot_size})"
                )
            lots = math.floor(amount / (premium * lot_size))
            if lots < 1:
                raise InsufficientCapitalError(
                    f"{symbol}: amount ₹{amount:.0f} < 1 option lot "
                    f"(₹{premium * lot_size:.0f}/lot)"
                )
            return lots * lot_size

        raise ValueError(f"unknown instrument_type: {itype}")


def _select_atm_strike(chain, option_type: str, spot: float) -> float:
    """Pick the strike nearest spot. `chain` is broker-shaped: a list of dicts
    with a 'strike' key, or a list of floats."""
    if not chain:
        raise InsufficientCapitalError("empty option chain")
    strikes = []
    for c in chain:
        if isinstance(c, dict):
            strikes.append(float(c["strike"]))
        else:
            strikes.append(float(c))
    return min(strikes, key=lambda k: abs(k - spot))
