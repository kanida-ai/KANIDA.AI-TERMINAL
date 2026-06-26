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

    def get_ltps_batch(self, symbols: List[str]) -> dict:
        """SPEED PASS: ONE multi-symbol LTP fetch for the whole pick list.

        Pass 1 — the shared KiteTicker WS cache (sub-second fresh, zero network).
        Pass 2 — a SINGLE kite.ltp([...]) REST call for the symbols the cache
        missed (one round-trip instead of N). Symbols with no valid price are
        absent → the caller cash-sizes / skips per symbol (never over-deploys)."""
        out: dict = {}
        if not symbols:
            return out
        # Pass 1: WS tick cache.
        try:
            from falcon.trade.services.kite_ticker import get_ltp as ws_ltp
        except Exception:  # pragma: no cover - defensive
            ws_ltp = None
        misses: List[str] = []
        for s in symbols:
            v = None
            if ws_ltp is not None:
                try:
                    v = ws_ltp(s)
                except Exception:  # pragma: no cover
                    v = None
            if v is not None and v > 0:
                out[s] = float(v)
            else:
                misses.append(s)
        if not misses:
            return out
        # Pass 2: ONE batched REST call for the misses.
        try:
            keys = [f"NSE:{s}" for s in misses]
            data = self.kite.ltp(keys)
            for s in misses:
                row = data.get(f"NSE:{s}")
                if row and row.get("last_price"):
                    out[s] = float(row["last_price"])
        except Exception as e:
            log.warning("batch REST ltp failed for %d syms: %s", len(misses), e)
        return out

    # ── MTF margin (leverage) — reuse the legacy order_margins lookup ─────────
    def get_margins_batch(self, symbols: List[str],
                          product: str = "MTF") -> dict:
        """SPEED PASS: ONE kite.order_margins() probe for the WHOLE pick list via
        the legacy margin_calc.fetch_margins_batch (which also caches per
        symbol). Returns {symbol: per_share_margin}; symbols that error are
        ABSENT so the caller cash-falls-back per symbol (never over-deploys)."""
        if not symbols:
            return {}
        try:
            from falcon.trade.services.margin_calc import fetch_margins_batch
            items = [(s, product) for s in symbols]
            return dict(fetch_margins_batch(self.kite, items))
        except Exception as e:  # pragma: no cover
            log.warning("batch MTF margin lookup failed for %d syms (%s) — "
                        "cash fallback per symbol", len(symbols), e)
            return {}

    def get_margin_per_share(self, symbol: str, product: str = "MTF") -> Optional[float]:
        """Per-share margin Zerodha locks for `product`, via kite.order_margins —
        the SAME lookup the legacy order_planner uses for MTF sizing (so the new
        system's MTF quantities match deployable size + the backtest). Returns
        None on any failure → caller falls back to cash sizing (flagged)."""
        try:
            from falcon.trade.services.margin_calc import fetch_margins_batch
            res = fetch_margins_batch(self.kite, [(symbol, product)])
            return res.get(symbol)
        except Exception as e:  # pragma: no cover
            log.warning("MTF margin lookup failed for %s (%s) — cash fallback", symbol, e)
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

    # ── GTT-OCO (broker-held per-position backup) ─────────────────────────────
    def place_gtt_oco(self, symbol: str, qty: int, stop_price: float,
                      target_price: float, last_price: float,
                      product: str = "CNC", exchange: str = "NSE",
                      order_type: str = "LIMIT") -> Optional[str]:
        """Place a two-leg OCO GTT on Kite: a STOP leg (SELL when price <= stop)
        and a TARGET leg (SELL when price >= target). The broker holds it so a
        position is protected even if our software is down — the BACKUP floor
        under the portfolio kill switch.

        Dry-run / live-disabled → returns None (no real GTT). On any error →
        logs + returns None (best-effort; entry is never blocked on the GTT).
        """
        if not self._live_allowed():
            return None
        try:
            from falcon.trade.services.order_executor import _retry_kite_call
            kite = self.kite
            product_map = {"CNC": kite.PRODUCT_CNC, "MIS": kite.PRODUCT_MIS,
                           "NRML": kite.PRODUCT_NRML, "MTF": "MTF"}
            kprod = product_map.get(product, kite.PRODUCT_CNC)
            kotype = (kite.ORDER_TYPE_LIMIT if order_type == "LIMIT"
                      else kite.ORDER_TYPE_MARKET)
            kexch = getattr(kite, f"EXCHANGE_{exchange}", exchange)
            stop_price = round(float(stop_price), 2)
            target_price = round(float(target_price), 2)
            # Kite OCO trigger_values must be [lower, upper]; leg order matches.
            orders = [
                {"transaction_type": kite.TRANSACTION_TYPE_SELL, "quantity": int(qty),
                 "order_type": kotype, "product": kprod, "price": stop_price},
                {"transaction_type": kite.TRANSACTION_TYPE_SELL, "quantity": int(qty),
                 "order_type": kotype, "product": kprod, "price": target_price},
            ]
            gid = _retry_kite_call(
                lambda: kite.place_gtt(
                    trigger_type=kite.GTT_TYPE_OCO, tradingsymbol=symbol,
                    exchange=kexch, trigger_values=[stop_price, target_price],
                    last_price=round(float(last_price), 2), orders=orders),
                "place_gtt(autotrade)", symbol)
            # Kite returns {"trigger_id": <id>} or the id directly depending on ver.
            if isinstance(gid, dict):
                gid = gid.get("trigger_id") or gid.get("id")
            return str(gid) if gid is not None else None
        except Exception as e:
            log.error("place_gtt_oco failed for %s: %s", symbol, e)
            return None

    def cancel_gtt(self, gtt_id):
        """Delete a GTT by id. Dry-run / disabled → no-op. Best-effort on error."""
        if not self._live_allowed():
            return None
        try:
            from falcon.trade.services.order_executor import _retry_kite_call
            kite = self.kite
            return _retry_kite_call(
                lambda: kite.delete_gtt(trigger_id=gtt_id),
                "delete_gtt(autotrade)", str(gtt_id))
        except Exception as e:
            log.warning("cancel_gtt failed for %s: %s", gtt_id, e)
            return None

    def get_gtt(self, gtt_id):
        """Fetch a GTT's current state via kite.get_gtt (to detect it triggered).
        Dry-run / disabled → None. Returns None on any error."""
        if not self._live_allowed():
            return None
        try:
            return self.kite.get_gtt(trigger_id=gtt_id)
        except Exception as e:
            log.debug("get_gtt failed for %s: %s", gtt_id, e)
            return None
