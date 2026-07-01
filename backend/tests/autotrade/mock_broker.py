"""Async mock BrokerClient for AutoTrade tests. NEVER touches real Kite."""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from autotrade.broker.base import BrokerClient, OrderResult


class MockBroker(BrokerClient):
    broker_name = "mock"

    def __init__(self, profile, dry_run: bool = False, *,
                 ltps: Optional[Dict[str, float]] = None,
                 exit_delay_sec: float = 0.0,
                 fail_symbols: Optional[set] = None,
                 lot_size: int = 50,
                 pending_orders: Optional[List[dict]] = None,
                 partial_fills: Optional[Dict[str, int]] = None):
        super().__init__(profile, dry_run=dry_run)
        self.ltps = ltps or {}
        self.exit_delay_sec = exit_delay_sec
        self.fail_symbols = fail_symbols or set()
        self._lot_size = lot_size
        self._pending = pending_orders or []
        self.partial_fills = partial_fills or {}
        self.placed: List[Any] = []
        self.exits: List[tuple] = []
        self.cancelled: List[str] = []

    # market data
    def get_ltp(self, symbol: str) -> Optional[float]:
        return self.ltps.get(symbol)

    def set_ltp(self, symbol: str, price: float) -> None:
        self.ltps[symbol] = price

    # instrument master
    def get_lot_size(self, contract: str) -> int:
        return self._lot_size

    def get_active_futures(self, symbol: str, expiry_preference: str) -> str:
        return f"{symbol}FUT"

    def get_option_chain(self, symbol: str) -> List[Any]:
        spot = self.ltps.get(symbol, 100.0)
        return [{"strike": s} for s in (spot - 10, spot, spot + 10)]

    def get_option_contract(self, symbol, strike, expiry_preference) -> str:
        return f"{symbol}{int(strike)}{expiry_preference[:2].upper()}"

    # order lifecycle
    async def place_order(self, order) -> OrderResult:
        self.placed.append(order)
        if order.symbol in self.partial_fills:
            filled = self.partial_fills[order.symbol]
            return OrderResult(status="PARTIAL", broker_order_id="ord-" + order.symbol,
                               symbol=order.symbol, qty=order.qty,
                               filled_qty=filled,
                               avg_price=self.ltps.get(order.symbol))
        return OrderResult(status="PLACED", broker_order_id="ord-" + order.symbol,
                           symbol=order.symbol, qty=order.qty,
                           filled_qty=order.qty,
                           avg_price=self.ltps.get(order.symbol))

    async def get_pending_orders(self) -> List[Any]:
        return list(self._pending)

    async def cancel_order(self, order_id: str) -> Any:
        self.cancelled.append(order_id)
        return {"status": "CANCELLED", "order_id": order_id}

    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str,
                                kite_product: str | None = None) -> OrderResult:
        # kite_product accepted (and ignored) to match the real ZerodhaBroker
        # signature after the MTF-exit-product fix; kill_switch/exit paths pass it.
        if self.exit_delay_sec:
            await asyncio.sleep(self.exit_delay_sec)
        self.exits.append((symbol, qty))
        if symbol in self.fail_symbols:
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty, error="mock failure")
        return OrderResult(status="PLACED", broker_order_id="exit-" + symbol,
                           symbol=symbol, qty=qty)
