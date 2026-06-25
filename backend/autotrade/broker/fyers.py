"""Fyers broker client — STUB. Not yet implemented.

Placeholder so the BrokerRouter can recognise the broker name. Every method
raises NotImplementedError. Implement by wrapping the Fyers SDK the same way
zerodha.py wraps the existing Kite stack.
"""
from __future__ import annotations

from typing import Any, List, Optional

from .base import BrokerClient, OrderResult


class FyersBroker(BrokerClient):
    broker_name = "fyers"

    def _ni(self, what: str):
        raise NotImplementedError(
            f"Fyers broker not implemented yet: {what}. "
            "Wrap the Fyers SDK following autotrade/broker/zerodha.py."
        )

    def get_ltp(self, symbol: str) -> Optional[float]:
        self._ni("get_ltp")

    def get_lot_size(self, contract: str) -> int:
        self._ni("get_lot_size")

    def get_active_futures(self, symbol: str, expiry_preference: str) -> str:
        self._ni("get_active_futures")

    def get_option_chain(self, symbol: str) -> List[Any]:
        self._ni("get_option_chain")

    def get_option_contract(self, symbol: str, strike: float,
                            expiry_preference: str) -> str:
        self._ni("get_option_contract")

    async def place_order(self, order) -> OrderResult:
        self._ni("place_order")

    async def get_pending_orders(self) -> List[Any]:
        self._ni("get_pending_orders")

    async def cancel_order(self, order_id: str) -> Any:
        self._ni("cancel_order")

    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str) -> OrderResult:
        self._ni("place_market_exit")
