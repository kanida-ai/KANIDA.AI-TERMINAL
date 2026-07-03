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
    @staticmethod
    def _rest_key(symbol: str) -> str:
        """Kite instrument key 'EXCHANGE:TRADINGSYMBOL' for a REST ltp/quote call.

        Futures + options trade on NFO, cash equity on NSE. A hardcoded 'NSE:'
        made EVERY futures LTP fetch fail ('NSE:XYZ26JULFUT' doesn't exist), so
        FUT positions were never priced (trail/kill blind to the market) AND the
        failed REST calls hammered the broker API (slow portal). Detect the
        segment from the trading-symbol shape:
          * ends in 'FUT'                          -> NFO (futures)
          * ends in 'CE'/'PE' AFTER a strike digit -> NFO (options; the digit
            guard avoids mis-routing cash names that also end 'CE'/'PE', e.g.
            RELIANCE, ACE)
        """
        s = (symbol or "").upper()
        if s.endswith("FUT"):
            return f"NFO:{symbol}"
        if (s.endswith("CE") or s.endswith("PE")) and len(s) >= 3 and s[-3].isdigit():
            return f"NFO:{symbol}"
        return f"NSE:{symbol}"

    def get_ltp(self, symbol: str) -> Optional[float]:
        # 1. Fast WS tick cache (reuse existing ticker).
        try:
            from falcon.trade.services.kite_ticker import get_ltp as ws_ltp
            v = ws_ltp(symbol)
            if v is not None and v > 0:
                return float(v)
        except Exception as e:  # pragma: no cover - defensive
            log.debug("ws get_ltp failed for %s: %s", symbol, e)
        # 2. REST fallback via kite.ltp() — exchange-aware (NSE cash / NFO F&O).
        try:
            key = self._rest_key(symbol)
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
        # Pass 2: ONE batched REST call for the misses — exchange-aware per symbol
        # (NSE cash / NFO F&O), so futures resolve instead of silently failing.
        try:
            key_of = {s: self._rest_key(s) for s in misses}
            data = self.kite.ltp(list(key_of.values()))
            for s in misses:
                row = data.get(key_of[s])
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

    def get_active_futures_or_none(self, symbol: str,
                                   expiry_preference: str) -> Optional[str]:
        """The current-month (near) FUT contract for `symbol`, or None when the
        symbol has NO tradeable future — used by the F&O symbol-eligibility
        filter. NEVER fabricates a contract; a lookup error → None (skip)."""
        try:
            return self.get_active_futures(symbol, expiry_preference)
        except Exception as e:
            log.debug("no active future for %s (%s)", symbol, e)
            return None

    def get_fut_margin_per_lot(self, symbol: str,
                               expiry_preference: str = "near") -> Optional[float]:
        """Per-LOT NRML margin Zerodha locks to carry ONE lot of `symbol`'s
        current-month future, via kite.order_margins on the FUT contract (NFO).

        Retail futures are sized on THIS margin, not the full notional. Returns
        None on any failure so the caller REFUSES to size (never over-deploys on
        notional). Long + short both use the same initial margin (a probe uses
        BUY qty=lot; SPAN+exposure is symmetric for our sizing purposes)."""
        try:
            contract = self.get_active_futures(symbol, expiry_preference)
            lot = self.get_lot_size(contract)
            if not lot or lot <= 0:
                return None
            kite = self.kite
            payload = [{
                "exchange": "NFO",
                "tradingsymbol": contract,
                "transaction_type": "BUY",
                "variety": "regular",
                "product": "NRML",
                "order_type": "MARKET",
                "quantity": int(lot),
            }]
            resp = kite.order_margins(payload)
            if not isinstance(resp, list) or not resp:
                return None
            row = resp[0]
            total = row.get("total")
            if total is None:
                components = ("span", "exposure", "var", "additional", "bo", "cash")
                total = sum(float(row.get(k) or 0) for k in components)
            total = float(total)
            return total if total > 0 else None
        except Exception as e:  # pragma: no cover - defensive
            log.warning("FUT margin lookup failed for %s (%s) — refusing to "
                        "notional-size", symbol, e)
            return None

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
                                kite_product: str | None = None,
                                direction: str = "long") -> OrderResult:
        """Flatten one position with a direct Kite MARKET order in the CLOSING
        side.

        direction=="long"  (default) → SELL  (today's behaviour, unchanged).
        direction=="short" → BUY-to-cover (close a short future). A wrong side
        would DOUBLE the position, so the position's stored direction is threaded
        here by every exit path.

        kite_product: explicit Kite product string (e.g. "MTF", "CNC", "MIS",
        "NRML"). When provided it takes precedence over
        _resolve_product(instrument_type), which maps security-type ("EQ") not
        trading-product — wrong for MTF/FUT sessions.

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
            # FUT contracts live on NFO; default equity NSE (unchanged).
            if exchange == "NSE" and str(instrument_type).upper() == "FUT":
                exchange = "NFO"
            kexch = getattr(kite, f"EXCHANGE_{exchange}", exchange)
            product = kite_product if kite_product else self._resolve_product(instrument_type)
            # CLOSING side: long→SELL (unchanged), short→BUY-to-cover.
            closing_txn = (kite.TRANSACTION_TYPE_BUY
                           if str(direction).lower() == "short"
                           else kite.TRANSACTION_TYPE_SELL)

            order_id = await asyncio.to_thread(
                lambda: kite.place_order(
                    variety=kite.VARIETY_REGULAR,
                    exchange=kexch,
                    tradingsymbol=trading_symbol,
                    transaction_type=closing_txn,
                    quantity=int(qty),
                    product=product,
                    order_type=kite.ORDER_TYPE_MARKET,
                    # Kite API rejects MARKET orders without market_protection.
                    # 2.0 = allow up to 2% slippage from last traded price.
                    market_protection=2.0,
                )
            )
            log.info("place_market_exit: %s qty=%d product=%s side=%s order_id=%s",
                     symbol, qty, product, closing_txn, order_id)
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

    def get_net_position_qty(self, symbol: str,
                             instrument_type: str = "EQ"):
        """Signed net quantity Kite currently holds for `symbol` (via
        kite.positions()['net']), or None when unknown / not live.

        Matches on tradingsymbol AND (for F&O) the NFO exchange, so a FUT contract
        isn't confused with a cash symbol. Returns 0 when the symbol is genuinely
        flat at the broker. Used as a pre-exit reconciliation guard so we never
        place a naked order on a position the operator already closed.

        Paper / live-disabled → None (no real broker book to reconcile against;
        the caller then proceeds with its normal exit — paper is unchanged)."""
        if not self._live_allowed():
            return None
        try:
            trading_symbol, exchange = self._resolve_symbol(symbol)
            if exchange == "NSE" and str(instrument_type).upper() in (
                    "FUT", "OPT", "CE", "PE"):
                exchange = "NFO"
            pos = self.kite.positions() or {}
            net = pos.get("net") or []
            for row in net:
                if str(row.get("tradingsymbol")) != trading_symbol:
                    continue
                # When we know the F&O segment, require it to match so a cash
                # row can't shadow a contract of the same name.
                rexch = str(row.get("exchange") or "").upper()
                if exchange in ("NSE", "NFO") and rexch and rexch != exchange:
                    continue
                return int(row.get("quantity") or 0)
            return 0  # not in the net book → flat at the broker
        except Exception as e:  # pragma: no cover - defensive
            log.warning("get_net_position_qty failed for %s: %s", symbol, e)
            return None

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
                      stop_limit_price: Optional[float] = None,
                      direction: str = "long") -> Optional[str]:
        """Place a two-leg OCO GTT on Kite.

        LONG (default, UNCHANGED): a STOP leg (SELL when price <= stop, BELOW
        entry) + a TARGET leg (SELL when price >= target, ABOVE entry). Kite OCO
        trigger_values are [lower, upper] = [stop, target] and the leg order
        matches.

        SHORT (FUTURES): both legs are BUY-to-cover; the STOP is ABOVE entry
        (cover when price rises) and the TARGET is BELOW (cover when price falls).
        trigger_values sorted [lower, upper] = [target(below), stop(above)] and
        the legs are re-ordered to match. NEVER place a wrong-direction GTT.

        stop_limit_price: limit price for the stop leg's order — BELOW the trigger
        for a long (gap-down fill), ABOVE for a short (gap-up fill). Falls back to
        stop_price when None.

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
            # Stop leg limit: stop_limit_price when provided (below trigger for a
            # long, above for a short), otherwise the trigger itself.
            stop_lim = round(float(stop_limit_price), 2) if stop_limit_price is not None \
                else stop_price
            target_price = round(float(target_price), 2)
            is_short = str(direction).lower() == "short"
            txn = (kite.TRANSACTION_TYPE_BUY if is_short
                   else kite.TRANSACTION_TYPE_SELL)
            stop_leg = {"transaction_type": txn, "quantity": int(qty),
                        "order_type": kotype, "product": kprod, "price": stop_lim}
            target_leg = {"transaction_type": txn, "quantity": int(qty),
                          "order_type": kotype, "product": kprod, "price": target_price}
            if is_short:
                # SHORT: target(below) < stop(above). trigger_values must be
                # [lower, upper] and legs must match that order.
                trigger_values = [target_price, stop_price]
                orders = [target_leg, stop_leg]
            else:
                # LONG (unchanged): stop(below) < target(above).
                trigger_values = [stop_price, target_price]
                orders = [stop_leg, target_leg]
            gid = _retry_kite_call(
                lambda: kite.place_gtt(
                    trigger_type=kite.GTT_TYPE_OCO, tradingsymbol=symbol,
                    exchange=kexch, trigger_values=trigger_values,
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
