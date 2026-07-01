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

import asyncio
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
        """The authenticated KiteConnect for THIS adapter.

        PHASE-2 MULTI-TENANT: if the profile carries account-bound creds
        (api_key + access_token, populated from the vault at session-build time
        OR a non-null broker_account_id), build a DEDICATED proxy-aware
        KiteConnect for that account — NO process-global state, so two concurrent
        sessions on different accounts never cross-contaminate. When the profile
        has NO account creds (broker_account_id is None), fall back to the
        PROCESS-GLOBAL get_kite_client() = today's operator path, byte-for-byte.
        """
        if self._kite is None:
            self._kite = self._build_kite()
        return self._kite

    def _build_kite(self):
        prof = self.profile
        api_key = getattr(prof, "api_key", "") or ""
        access_token = getattr(prof, "access_token", "") or ""
        bound = getattr(prof, "broker_account_id", None)
        # Per-account path: explicit creds supplied (vault-resolved) → build a
        # dedicated client. Requires BOTH api_key and access_token; a bound
        # account missing a token is a real error (caller should re-login), but
        # we surface it as a KiteAuthError-style ValueError rather than silently
        # using the operator's global token (which would trade the WRONG account).
        if bound is not None:
            if not api_key or not access_token:
                raise ValueError(
                    f"broker_account {bound}: api_key/access_token not resolved "
                    "(vault disabled, account missing, or token expired — "
                    "re-connect the account)")
            from services.kite_auth import _new_kite  # proxy-aware constructor
            kite = _new_kite(api_key)
            kite.set_access_token(access_token)
            log.info("zerodha: built per-account KiteConnect for account %s",
                     bound)
            return kite
        # Legacy / operator path: process-global client (env + kite_tokens).
        from services.kite_auth import get_kite_client
        return get_kite_client(check=False)

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

    def _resolve_symbol(self, symbol: str):
        """Return (trading_symbol, exchange) for Kite order placement.

        Symbols may arrive as bare NSE codes ("INFY") or with an exchange
        suffix ("INFY:BSE"). Split on ":" if present; default exchange NSE.
        """
        if ":" in symbol:
            parts = symbol.split(":", 1)
            return parts[0], parts[1].upper()
        return symbol, "NSE"

    def _resolve_product(self, instrument_type: str):
        """Map our instrument_type string to the Kite product constant.

        CNC  → PRODUCT_CNC   (delivery cash)
        MIS  → PRODUCT_MIS   (intraday margin)
        NRML → PRODUCT_NRML  (F&O / carry)
        MTF  → "MTF"         (margin trade facility — string, not a constant)
        EQ   → PRODUCT_CNC   (equity default)
        Anything else → PRODUCT_CNC (safe fallback, logged).
        """
        kite = self.kite
        mapping = {
            "CNC": kite.PRODUCT_CNC,
            "MIS": kite.PRODUCT_MIS,
            "NRML": kite.PRODUCT_NRML,
            "MTF": "MTF",
            "EQ": kite.PRODUCT_CNC,
        }
        prod = mapping.get(instrument_type.upper() if instrument_type else "CNC")
        if prod is None:
            log.warning("_resolve_product: unknown instrument_type %r — defaulting CNC",
                        instrument_type)
            prod = kite.PRODUCT_CNC
        return prod

    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str,
                                kite_product: str | None = None) -> OrderResult:
        """Flatten one position with a direct Kite MARKET SELL.

        kite_product: explicit Kite product string (e.g. "MTF", "CNC", "MIS").
        When provided it takes precedence over _resolve_product(instrument_type),
        which maps security-type ("EQ") not trading-product — wrong for MTF sessions.

        kite.place_order is a blocking HTTP call. Running it via asyncio.to_thread
        keeps the event loop responsive so ticks, ws_driver, and other coroutines
        continue while waiting for the Kite API response.
        """
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=symbol, qty=qty, raw={"dry_run": True})
        try:
            kite = self.kite
            trading_symbol, exchange = self._resolve_symbol(symbol)
            kexch = getattr(kite, f"EXCHANGE_{exchange}", exchange)
            product = kite_product if kite_product else self._resolve_product(instrument_type)

            order_id = await asyncio.to_thread(
                lambda: kite.place_order(
                    variety=kite.VARIETY_REGULAR,
                    exchange=kexch,
                    tradingsymbol=trading_symbol,
                    transaction_type=kite.TRANSACTION_TYPE_SELL,
                    quantity=int(qty),
                    product=product,
                    order_type=kite.ORDER_TYPE_MARKET,
                    # Kite API rejects MARKET orders without market_protection.
                    # 2.0 = allow up to 2% slippage from last traded price.
                    market_protection=2.0,
                )
            )
            log.info("place_market_exit: %s qty=%d product=%s order_id=%s",
                     symbol, qty, product, order_id)
            return OrderResult(status="PLACED", broker_order_id=str(order_id),
                               symbol=symbol, qty=qty)
        except Exception as e:
            log.error("place_market_exit failed for %s: %s", symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty, error=str(e))

    def get_order_status(self, order_id: str) -> dict:
        """Scan today's Kite orders and return the matching order dict.

        Returns {} when the order is not found (treat as unknown / still pending).
        Raises on a Kite API error so the caller can log and decide to retry.
        """
        try:
            orders = self.kite.orders()
            for o in orders:
                if str(o.get("order_id")) == str(order_id):
                    return o
            return {}
        except Exception as e:
            log.warning("get_order_status failed for order %s: %s", order_id, e)
            raise

    def cancel_order_sync(self, order_id: str) -> bool:
        """Synchronously cancel a regular Kite order.

        Returns True if the cancel request was submitted. Returns False (and logs)
        on any error — callers should still attempt a fresh exit after failure.
        """
        if not self._live_allowed():
            return True
        try:
            kite = self.kite
            kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=order_id)
            return True
        except Exception as e:
            log.warning("cancel_order_sync failed for %s: %s", order_id, e)
            return False

    # ── GTT-OCO (broker-held per-position backup) ─────────────────────────────
    def place_gtt_oco(self, symbol: str, qty: int, stop_price: float,
                      target_price: float, last_price: float,
                      product: str = "CNC", exchange: str = "NSE",
                      order_type: str = "LIMIT",
                      stop_limit_price: Optional[float] = None) -> Optional[str]:
        """Place a two-leg OCO GTT on Kite: a STOP leg (SELL when price <= stop)
        and a TARGET leg (SELL when price >= target). The broker holds it so a
        position is protected even if our software is down — the BACKUP floor
        under the portfolio kill switch.

        stop_limit_price: limit price for the stop leg's order. When provided it
        is set BELOW stop_price (the trigger) so the sell order fills even when
        price gaps below the trigger. Falls back to stop_price when None.

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
            # Stop leg: use stop_limit_price (below trigger) when provided,
            # otherwise fall back to the trigger price itself.
            stop_lim = round(float(stop_limit_price), 2) if stop_limit_price is not None \
                else stop_price
            target_price = round(float(target_price), 2)
            # Kite OCO trigger_values must be [lower, upper]; leg order matches.
            orders = [
                {"transaction_type": kite.TRANSACTION_TYPE_SELL, "quantity": int(qty),
                 "order_type": kotype, "product": kprod, "price": stop_lim},
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
