"""Zerodha broker client — WRAPS the existing Kite stack. No order logic here.

This adapter reuses, never reimplements:
  * services.kite_auth.get_kite_client      — authenticated KiteConnect
  * falcon.trade.services.kite_ticker.get_ltp — WS tick cache (REST fallback)
  * falcon.trade.services.order_executor._retry_kite_call / _autotrade_enabled
  * falcon.trade.services.mtf_eligibility.get_instrument_token / get_lot helpers

DRY-RUN SAFETY: when dry_run=True (default) NO real order is ever sent to Kite.
place_order / place_market_exit / cancel_order return synthetic DRY_RUN results.
Even when dry_run=False, the master env FALCON_AUTOTRADE_ENABLED must be 'true'
(checked via order_executor._autotrade_enabled) or the call refuses.
"""
from __future__ import annotations

import logging
from typing import Any, List, Optional

from .base import BrokerClient, OrderResult

log = logging.getLogger("kanida.autotrade.broker.zerodha")


class ZerodhaBroker(BrokerClient):
    broker_name = "zerodha"

    def __init__(self, profile, dry_run: bool = True):
        super().__init__(profile, dry_run=dry_run)
        self._kite = None

    # ── lazy kite client (only when we actually need it) ──────────────────────
    @property
    def kite(self):
        if self._kite is None:
            from services.kite_auth import get_kite_client
            self._kite = get_kite_client(check=False)
        return self._kite

    def _live_allowed(self) -> bool:
        """Real orders require dry_run off AND the master env switch on."""
        if self.dry_run:
            return False
        from falcon.trade.services.order_executor import _autotrade_enabled
        return _autotrade_enabled()

    # ── Market data ──────────────────────────────────────────────────────────
    def get_ltp(self, symbol: str) -> Optional[float]:
        # 1. Fast WS tick cache (reuse existing ticker).
        try:
            from falcon.trade.services.kite_ticker import get_ltp as ws_ltp
            v = ws_ltp(symbol)
            if v is not None and v > 0:
                return float(v)
        except Exception as e:  # pragma: no cover - defensive
            log.debug("ws get_ltp failed for %s: %s", symbol, e)
        # 2. REST fallback via kite.ltp().
        try:
            key = f"NSE:{symbol}"
            data = self.kite.ltp([key])
            return float(data[key]["last_price"])
        except Exception as e:
            log.warning("REST ltp failed for %s: %s", symbol, e)
            return None

    # ── Instrument master (F&O) — always runtime, never hardcoded ────────────
    def get_lot_size(self, contract: str) -> int:
        try:
            from falcon.trade.services import mtf_eligibility
            fn = getattr(mtf_eligibility, "get_lot_size", None)
            if fn:
                return int(fn(self.kite, contract))
        except Exception as e:  # pragma: no cover
            log.debug("mtf_eligibility lot_size miss for %s: %s", contract, e)
        # Fallback: scan the instrument dump for the contract's lot_size.
        try:
            for ins in self.kite.instruments("NFO"):
                if ins.get("tradingsymbol") == contract:
                    return int(ins["lot_size"])
        except Exception as e:
            log.warning("instrument master lot_size lookup failed for %s: %s",
                        contract, e)
        raise ValueError(f"lot_size not found for {contract}")

    def get_active_futures(self, symbol: str, expiry_preference: str) -> str:
        offset = {"near": 0, "next": 1, "far": 2}.get(expiry_preference, 0)
        futs = []
        for ins in self.kite.instruments("NFO"):
            if ins.get("name") == symbol and ins.get("instrument_type") == "FUT":
                futs.append(ins)
        if not futs:
            raise ValueError(f"no active futures for {symbol}")
        futs.sort(key=lambda i: i["expiry"])
        idx = min(offset, len(futs) - 1)
        return futs[idx]["tradingsymbol"]

    def get_option_chain(self, symbol: str) -> List[Any]:
        chain = []
        for ins in self.kite.instruments("NFO"):
            if ins.get("name") == symbol and ins.get("instrument_type") in ("CE", "PE"):
                chain.append({"strike": float(ins["strike"]),
                              "tradingsymbol": ins["tradingsymbol"],
                              "instrument_type": ins["instrument_type"],
                              "expiry": ins["expiry"]})
        return chain

    def get_option_contract(self, symbol: str, strike: float,
                            expiry_preference: str) -> str:
        chain = self.get_option_chain(symbol)
        cands = [c for c in chain if abs(c["strike"] - strike) < 1e-6]
        if not cands:
            raise ValueError(f"no option contract for {symbol} @ {strike}")
        cands.sort(key=lambda c: c["expiry"])
        offset = {"near": 0, "next": 1, "far": 2}.get(expiry_preference, 0)
        return cands[min(offset, len(cands) - 1)]["tradingsymbol"]

    # ── Order lifecycle ──────────────────────────────────────────────────────
    async def place_order(self, order) -> OrderResult:
        """Place an entry order. In dry-run, returns a synthetic DRY_RUN result.

        Real placement reuses the existing retry wrapper (_retry_kite_call) — it
        does NOT reimplement order semantics. order is an execution.orders.Order.
        """
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty,
                               error=None, raw={"dry_run": True})
        from falcon.trade.services.order_executor import _retry_kite_call
        kite = self.kite
        try:
            params = order.to_kite_params(kite)
            oid = _retry_kite_call(lambda: kite.place_order(**params),
                                   "place_order(autotrade)", order.symbol)
            return OrderResult(status="PLACED", broker_order_id=str(oid),
                               symbol=order.symbol, qty=order.qty)
        except Exception as e:
            log.error("place_order failed for %s: %s", order.symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty, error=str(e))

    async def get_pending_orders(self) -> List[Any]:
        if self.dry_run:
            return []
        try:
            orders = self.kite.orders()
            return [o for o in orders
                    if o.get("status") in ("OPEN", "TRIGGER PENDING", "PENDING")]
        except Exception as e:
            log.warning("get_pending_orders failed: %s", e)
            return []

    async def cancel_order(self, order_id: str) -> Any:
        if not self._live_allowed():
            return {"status": "DRY_RUN", "order_id": order_id}
        from falcon.trade.services.order_executor import _retry_kite_call
        kite = self.kite
        return _retry_kite_call(
            lambda: kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=order_id),
            "cancel_order(autotrade)", str(order_id),
        )

    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str) -> OrderResult:
        """Flatten one position with a MARKET sell.

        Reuses trail_manager.execute_exit_at_market when possible so we share the
        exact, battle-tested exit semantics (SL cancel + market_protection). In
        dry-run we never touch Kite.
        """
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=symbol, qty=qty, raw={"dry_run": True})
        try:
            from falcon.trade.services import trail_manager
            from falcon.trade.services import position_monitor
            kite = self.kite
            state = position_monitor.get_state(symbol) or {
                "symbol": symbol, "qty": qty, "product": self.profile.order_product,
                "sl_kite_order_id": None,
            }
            # Recompute open qty (spec: kill switch must use the full remaining qty).
            state = dict(state)
            state["qty"] = qty
            res = trail_manager.execute_exit_at_market(kite, state, "KILL_SWITCH")
            status = "PLACED" if res.get("status") == "PLACED" else "FAILED"
            return OrderResult(status=status,
                               broker_order_id=res.get("kite_order_id"),
                               symbol=symbol, qty=qty, error=res.get("error"))
        except Exception as e:
            log.error("place_market_exit failed for %s: %s", symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty, error=str(e))
