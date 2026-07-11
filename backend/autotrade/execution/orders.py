"""Normalised Order + build_order dispatcher + async place_order_with_retry.

Order is a broker-agnostic dataclass. build_order dispatches to the
instrument-specific constructors. place_order_with_retry implements the
spec Section 5.3 async retry-with-timeout around broker.place_order.

MARKET / LIMIT / VWAP semantics (spec 5.1):
  MARKET : place market at entry_time, accept open.
  LIMIT  : place limit at ltp*(1+limit_offset_pct); if unfilled within
           entry_window_seconds → cancel + MARKET fallback (driven by the
           session orchestrator; Order just carries the params + price).
  VWAP   : observe vwap_window_seconds then place MARKET (orchestrator times
           the wait; Order is tagged so the orchestrator knows to delay).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("kanida.autotrade.execution.orders")


class OrderTimeoutError(Exception):
    def __init__(self, order):
        self.order = order
        super().__init__(f"order timed out: {getattr(order, 'symbol', '?')}")


@dataclass
class Order:
    symbol: str
    qty: int
    exchange: str = "NSE"
    product: str = "CNC"               # CNC | MIS | MTF | NRML
    order_type: str = "MARKET"         # MARKET | LIMIT | VWAP
    instrument_type: str = "EQ"
    transaction_type: str = "BUY"
    price: Optional[float] = None       # limit price (computed by orchestrator)
    lot_size: Optional[int] = None
    lots: Optional[int] = None
    strike: Optional[float] = None
    limit_offset_pct: float = 0.001
    vwap_window_seconds: int = 60
    # SPRINT CLUSTER 2 (CAP 1): the durable ownership key + the compact broker tag.
    # client_order_id is minted BEFORE submission and persisted in our DB; `tag`
    # (<=20 alphanumeric chars) is compact_tag(client_order_id) and rides on the
    # broker order so we recognise OUR OWN orders in the broker orderbook. Both
    # default None → the pre-existing per-symbol tag is used (byte-for-byte
    # unchanged for any caller that does not set them).
    client_order_id: Optional[str] = None
    tag: Optional[str] = None

    def compute_limit_price(self, reference_price: float) -> float:
        """Limit = reference * (1 + offset). Used for LIMIT entries."""
        return round(reference_price * (1 + self.limit_offset_pct), 2)

    def to_kite_params(self, kite) -> dict:
        """Translate to KiteConnect.place_order kwargs. Reuses Kite constants.
        Only called on a REAL (non-dry-run) path — see ZerodhaBroker.place_order.
        """
        product_map = {
            "CNC": kite.PRODUCT_CNC, "MIS": kite.PRODUCT_MIS,
            "NRML": kite.PRODUCT_NRML, "MTF": "MTF",
        }
        # NO SILENT DEFAULT (real-money safety): an unrecognised product must
        # RAISE, never fall back to CNC — a wrong product silently placed at the
        # broker is exactly the "UI shows one product, broker gets another" bug
        # class. config.validate() already restricts order_product to the four
        # keys above, so reaching here with anything else is a code bug to surface.
        if self.product not in product_map:
            raise ValueError(
                f"unmapped order product {self.product!r} (expected one of "
                f"{sorted(product_map)}) — refusing to default to CNC")
        # VWAP resolves to a MARKET order at the end of its window.
        kite_otype = (kite.ORDER_TYPE_LIMIT if self.order_type == "LIMIT"
                      else kite.ORDER_TYPE_MARKET)
        params = dict(
            variety=kite.VARIETY_REGULAR,
            exchange=getattr(kite, f"EXCHANGE_{self.exchange}", self.exchange),
            tradingsymbol=self.symbol,
            transaction_type=(kite.TRANSACTION_TYPE_BUY
                              if self.transaction_type == "BUY"
                              else kite.TRANSACTION_TYPE_SELL),
            quantity=self.qty,
            product=product_map[self.product],
            order_type=kite_otype,
            # CAP 1: the compact client-order tag (<=20 alnum) when this leg was
            # minted a client_order_id; else the pre-existing per-symbol tag
            # (byte-for-byte unchanged). The FULL client_order_id is stored in our
            # DB (the position row + the order ledger), mapped to the broker id.
            tag=(self.tag or f"at-{self.symbol[:10].lower()}")[:20],
        )
        if self.order_type == "LIMIT" and self.price:
            params["price"] = self.price
        if kite_otype == kite.ORDER_TYPE_MARKET:
            params["market_protection"] = 1.0
        return params


def build_order(symbol: str, qty: int, config, broker) -> Order:
    """Dispatch to the right instrument constructor (spec 5.2)."""
    from . import instruments
    itype = config.instrument_type
    if itype == "EQ":
        return instruments.build_eq_order(symbol, qty, config)
    if itype == "MTF":
        return instruments.build_mtf_order(symbol, qty, config)
    if itype == "FUT":
        return instruments.build_fut_order(symbol, qty, config, broker)
    if itype in ("CE", "PE"):
        return instruments.build_option_order(symbol, qty, config, broker)
    raise ValueError(f"unknown instrument_type: {itype}")


async def place_order_with_retry(order: Order, broker, max_retries: int = 3,
                                 timeout_ms: int = 2000):
    """Async retry-with-timeout around broker.place_order (spec 5.3).

    Note: broker.place_order is itself dry-run-safe — in paper mode this returns
    a DRY_RUN result and no real order is sent.
    """
    last_exc: Optional[BaseException] = None
    for attempt in range(max_retries):
        try:
            return await asyncio.wait_for(broker.place_order(order),
                                          timeout=timeout_ms / 1000)
        except asyncio.TimeoutError as e:
            last_exc = e
            log.error("Order timeout attempt %d: %s", attempt + 1, order.symbol)
            if attempt == max_retries - 1:
                raise OrderTimeoutError(order)
    if last_exc:
        raise OrderTimeoutError(order)
