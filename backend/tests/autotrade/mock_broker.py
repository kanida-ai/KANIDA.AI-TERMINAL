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
                 partial_fills: Optional[Dict[str, int]] = None,
                 fut_margin_per_lot: Optional[float] = None,
                 no_future_symbols: Optional[set] = None,
                 margins: Optional[Dict[str, float]] = None,
                 margins_available: bool = True,
                 net_positions: Optional[Dict[str, int]] = None,
                 reject_symbols: Optional[set] = None):
        super().__init__(profile, dry_run=dry_run)
        # POST-PLACEMENT REJECTION sim: symbols the broker ACCEPTS (issues an
        # order_id) but the EXCHANGE then REJECTS asynchronously (0 fill) — e.g. a
        # circuit-limit breach. place_order returns PLACED w/ no fill; the entry
        # reconcile's get_order_status then reports REJECTED. Default empty →
        # existing tests unaffected.
        self.reject_symbols = reject_symbols or set()
        # Pre-exit reconciliation guard: simulate the broker's live net book.
        # {symbol: signed_qty}. None (default) means the mock does NOT answer the
        # net-position probe → the base default (None) is used and the exit path
        # is unchanged. A dict entry of 0 = flat at the broker (already closed).
        self._net_positions = net_positions
        self.ltps = ltps or {}
        # FEATURE C: per-share MTF/MIS margin the mock reports (leverage). When
        # margins_available is False the margin API is simulated as DOWN → the
        # allocator must cash-fall-back (never over-deploy).
        self.margins = margins or {}
        self.margins_available = margins_available
        self.exit_delay_sec = exit_delay_sec
        self.fail_symbols = fail_symbols or set()
        self._lot_size = lot_size
        # FUTURES: per-lot margin the mock "broker" reports (None → base default
        # None → capital refuses to size, matching the real no-margin-API guard).
        self._fut_margin_per_lot = fut_margin_per_lot
        # FUTURES eligibility: symbols the mock has NO future for (filter drops).
        self._no_future_symbols = no_future_symbols or set()
        self._pending = pending_orders or []
        self.partial_fills = partial_fills or {}
        self.placed: List[Any] = []
        self.exits: List[tuple] = []          # (symbol, qty) — unchanged shape
        self.exit_calls: List[dict] = []      # full exit args incl. direction
        self.cancelled: List[str] = []

    # market data
    def get_ltp(self, symbol: str) -> Optional[float]:
        return self.ltps.get(symbol)

    def set_ltp(self, symbol: str, price: float) -> None:
        self.ltps[symbol] = price

    # margin (MTF / MIS leverage sizing)
    def get_margin_per_share(self, symbol: str, product: str = "MTF"):
        if not self.margins_available:
            return None
        return self.margins.get(symbol)

    def get_margins_batch(self, symbols, product: str = "MTF") -> dict:
        if not self.margins_available:
            return {}
        return {s: self.margins[s] for s in symbols if s in self.margins}

    # instrument master
    def get_lot_size(self, contract: str) -> int:
        return self._lot_size

    def get_active_futures(self, symbol: str, expiry_preference: str) -> str:
        if symbol in self._no_future_symbols:
            raise ValueError(f"no active futures for {symbol}")
        return f"{symbol}FUT"

    def get_active_futures_or_none(self, symbol: str, expiry_preference: str):
        if symbol in self._no_future_symbols:
            return None
        return f"{symbol}FUT"

    def get_fut_margin_per_lot(self, symbol: str,
                               expiry_preference: str = "near"):
        return self._fut_margin_per_lot

    def get_net_position_qty(self, symbol: str, instrument_type: str = "EQ"):
        # None (default) → don't reconcile (base behaviour). A configured dict
        # answers the live net book for the pre-exit reconciliation guard.
        if self._net_positions is None:
            return None
        return self._net_positions.get(symbol)

    def get_option_chain(self, symbol: str) -> List[Any]:
        spot = self.ltps.get(symbol, 100.0)
        return [{"strike": s} for s in (spot - 10, spot, spot + 10)]

    def get_option_contract(self, symbol, strike, expiry_preference) -> str:
        return f"{symbol}{int(strike)}{expiry_preference[:2].upper()}"

    # order lifecycle
    async def place_order(self, order) -> OrderResult:
        self.placed.append(order)
        if order.symbol in self.reject_symbols:
            # Accepted (order_id issued) but NO fill — the reconcile poll will see
            # REJECTED. Mirrors a real exchange circuit-limit / RMS rejection.
            return OrderResult(status="PLACED", broker_order_id="ord-" + order.symbol,
                               symbol=order.symbol, qty=order.qty,
                               filled_qty=0, avg_price=None)
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

    def get_order_status(self, order_id: str) -> dict:
        # Synthetic COMPLETE fill for the exit order the confirm poller checks.
        # order_id shape is "exit-<SYMBOL>" (see place_market_exit). Report the
        # placed exit qty as fully filled so confirm_exit resolves COMPLETE for
        # non-dry mock brokers (matches a real broker's near-instant MARKET fill).
        # ENTRY reconcile probe ("ord-<SYM>"): a rejected symbol reports REJECTED
        # (0 fill) so _fire_one drops the leg instead of registering a phantom.
        if str(order_id).startswith("ord-"):
            esym = order_id[len("ord-"):]
            if esym in self.reject_symbols:
                return {"status": "REJECTED", "filled_quantity": 0,
                        "average_price": 0.0}
            return {"status": "COMPLETE",
                    "filled_quantity": 0, "average_price": 0.0}
        sym = order_id[len("exit-"):] if str(order_id).startswith("exit-") else None
        if sym is not None:
            total = sum(q for s, q in self.exits if s == sym)
            if total > 0:
                return {"status": "COMPLETE", "filled_quantity": int(total),
                        "average_price": float(self.ltps.get(sym) or 0.0)}
        return {"status": "COMPLETE", "filled_quantity": 0, "average_price": 0.0}

    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str,
                                kite_product: str | None = None,
                                direction: str = "long") -> OrderResult:
        # kite_product accepted (and ignored) to match the real ZerodhaBroker
        # signature after the MTF-exit-product fix; kill_switch/exit paths pass it.
        # direction ("long"|"short") records the CLOSING side so futures-short
        # tests can assert a BUY-to-cover exit. Kept as an extra tuple element so
        # existing (symbol, qty) unpacking in equity tests stays valid via slicing.
        if self.exit_delay_sec:
            await asyncio.sleep(self.exit_delay_sec)
        self.exits.append((symbol, qty))
        self.exit_calls.append({"symbol": symbol, "qty": qty,
                                "instrument_type": instrument_type,
                                "kite_product": kite_product,
                                "direction": direction})
        if symbol in self.fail_symbols:
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty, error="mock failure")
        return OrderResult(status="PLACED", broker_order_id="exit-" + symbol,
                           symbol=symbol, qty=qty)
