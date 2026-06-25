"""Instrument-specific order construction (EQ / MTF / FUT / CE / PE).

Builds a normalised Order (see orders.py) for any instrument type. F&O contract
selection + lot sizing ALWAYS query the broker instrument master at runtime —
never hardcoded. Mirrors spec Section 5.2 build_order.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # avoid import cycle at module load
    from .orders import Order


def build_eq_order(symbol: str, qty: int, config) -> "Order":
    from .orders import Order
    return Order(symbol=symbol, qty=qty, exchange="NSE",
                 product=config.order_product, order_type=config.order_type,
                 instrument_type="EQ", limit_offset_pct=config.limit_offset_pct,
                 vwap_window_seconds=config.vwap_window_seconds)


def build_mtf_order(symbol: str, qty: int, config) -> "Order":
    from .orders import Order
    # MTF is a product flag; CNC-like hold.
    return Order(symbol=symbol, qty=qty, exchange="NSE",
                 product="MTF", order_type=config.order_type,
                 instrument_type="MTF", limit_offset_pct=config.limit_offset_pct,
                 vwap_window_seconds=config.vwap_window_seconds)


def build_fut_order(symbol: str, qty: int, config, broker) -> "Order":
    from .orders import Order
    contract = broker.get_active_futures(symbol, config.expiry_preference)
    lot_size = broker.get_lot_size(contract)
    lots = qty // lot_size if lot_size else 0
    return Order(symbol=contract, qty=lots * lot_size, exchange="NFO",
                 product="NRML", order_type=config.order_type,
                 instrument_type="FUT", lot_size=lot_size, lots=lots,
                 limit_offset_pct=config.limit_offset_pct,
                 vwap_window_seconds=config.vwap_window_seconds)


def build_option_order(symbol: str, qty: int, config, broker) -> "Order":
    from .orders import Order
    from ..capital import _select_atm_strike
    spot = broker.get_ltp(symbol) or 0.0
    chain = broker.get_option_chain(symbol)
    strike = _select_atm_strike(chain, config.instrument_type, spot)
    contract = broker.get_option_contract(symbol, strike, config.expiry_preference)
    lot_size = broker.get_lot_size(contract)
    lots = qty // lot_size if lot_size else 0
    return Order(symbol=contract, qty=lots * lot_size, exchange="NFO",
                 product="NRML", order_type=config.order_type,
                 instrument_type=config.instrument_type, lot_size=lot_size,
                 lots=lots, strike=strike,
                 limit_offset_pct=config.limit_offset_pct,
                 vwap_window_seconds=config.vwap_window_seconds)
