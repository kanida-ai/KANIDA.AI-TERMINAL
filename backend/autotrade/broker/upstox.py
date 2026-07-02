"""Upstox broker client.

IMPLEMENTED — UNVERIFIED (no test account). Zerodha remains the verified path.
This wraps the Upstox Uplink REST API v2 (https://upstox.com/developer/api) the
way zerodha.py wraps the Kite stack. It builds a PER-ACCOUNT client from the
profile's api_key + access_token (resolved from the vault at session-build time);
there is NO process-global Upstox client.

Upstox specifics encoded here:
  * Auth is a Bearer access_token in the Authorization header (OAuth code flow;
    the daily token is minted by autotrade.broker.upstox_auth, not here).
  * Instruments are addressed by an `instrument_key` (e.g. "NSE_EQ|INE002A01018").
    We resolve symbol → instrument_key via the published instrument master JSON
    (cached per process). NSE equity uses the "NSE_EQ" segment.
  * Quotes: GET /v2/market-quote/ltp?instrument_key=...
  * Orders: POST /v2/order/place (MARKET/LIMIT), product I (intraday) / D
    (delivery). DELETE /v2/order/cancel?order_id=...
  * NO server-side OCO/GTT equivalent is wired here → place_gtt_oco returns None
    (the BrokerClient default) so the SOFTWARE portfolio kill switch remains the
    exit; the per-position broker floor is Zerodha-only for now.

DRY-RUN SAFETY: identical contract to ZerodhaBroker — dry_run (default) places
NO real order; even live needs FALCON_AUTOTRADE_ENABLED. All network calls are
best-effort and never raise out of get_ltp (return None) so sizing degrades to a
cash fallback rather than crashing a session.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base import BrokerClient, OrderResult

log = logging.getLogger("kanida.autotrade.broker.upstox")

_API_BASE = "https://api.upstox.com/v2"
# Published instrument master (gzipped JSON). Loaded lazily + cached per process.
_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"

# Product mapping: our order_product → Upstox product code.
_PRODUCT_MAP = {"CNC": "D", "MIS": "I", "NRML": "D", "MTF": "D"}


class UpstoxBroker(BrokerClient):
    broker_name = "upstox"

    def __init__(self, profile, dry_run: bool = True):
        super().__init__(profile, dry_run=dry_run)
        self._instrument_cache: Dict[str, str] = {}

    # ── auth / live gate ─────────────────────────────────────────────────────
    def _access_token(self) -> Optional[str]:
        return getattr(self.profile, "access_token", "") or None

    def _live_allowed(self) -> bool:
        if self.dry_run:
            return False
        from falcon.trade.services.order_executor import _autotrade_enabled
        return _autotrade_enabled() and bool(self._access_token())

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _proxies(self):
        try:
            from services.kite_auth import _kite_proxies
            return _kite_proxies()
        except Exception:  # pragma: no cover
            return None

    # ── instrument key resolution ────────────────────────────────────────────
    def _instrument_key(self, symbol: str, segment: str = "NSE_EQ") -> Optional[str]:
        """Resolve a trading symbol → Upstox instrument_key. Cached. Returns None
        if it can't be resolved (caller cash-falls-back / skips)."""
        key = f"{segment}:{symbol}"
        if key in self._instrument_cache:
            return self._instrument_cache[key]
        try:
            import gzip
            import io
            import json
            import requests
            r = requests.get(_INSTRUMENTS_URL, timeout=30,
                             proxies=self._proxies() or None)
            r.raise_for_status()
            data = json.loads(gzip.GzipFile(fileobj=io.BytesIO(r.content)).read())
            for ins in data:
                if (ins.get("segment") == segment
                        and ins.get("trading_symbol") == symbol):
                    ikey = ins.get("instrument_key")
                    if ikey:
                        self._instrument_cache[key] = ikey
                        return ikey
        except Exception as e:  # pragma: no cover - network/format dependent
            log.warning("upstox instrument lookup failed for %s: %s", symbol, e)
        return None

    # ── market data ──────────────────────────────────────────────────────────
    def get_ltp(self, symbol: str) -> Optional[float]:
        ikey = self._instrument_key(symbol)
        if not ikey:
            return None
        try:
            import requests
            r = requests.get(f"{_API_BASE}/market-quote/ltp",
                             params={"instrument_key": ikey},
                             headers=self._headers(), timeout=10,
                             proxies=self._proxies() or None)
            r.raise_for_status()
            data = r.json().get("data", {})
            for _k, v in data.items():
                lp = v.get("last_price")
                if lp:
                    return float(lp)
        except Exception as e:
            log.warning("upstox get_ltp failed for %s: %s", symbol, e)
        return None

    # ── instrument master (F&O) — not yet supported for Upstox ───────────────
    def get_lot_size(self, contract: str) -> int:
        raise NotImplementedError("Upstox F&O lot_size not implemented")

    def get_active_futures(self, symbol: str, expiry_preference: str) -> str:
        raise NotImplementedError("Upstox futures selection not implemented")

    def get_option_chain(self, symbol: str) -> List[Any]:
        raise NotImplementedError("Upstox option chain not implemented")

    def get_option_contract(self, symbol: str, strike: float,
                            expiry_preference: str) -> str:
        raise NotImplementedError("Upstox option contract not implemented")

    # ── order lifecycle ──────────────────────────────────────────────────────
    async def place_order(self, order) -> OrderResult:
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty,
                               raw={"dry_run": True})
        ikey = self._instrument_key(order.symbol)
        if not ikey:
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty,
                               error="instrument_key not resolved")
        product = _PRODUCT_MAP.get(getattr(self.profile, "order_product", "CNC"),
                                   "D")
        otype = "MARKET" if getattr(order, "order_type", "MARKET") == "MARKET" \
            else "LIMIT"
        body = {
            "quantity": int(order.qty),
            "product": product,
            "validity": "DAY",
            "price": float(getattr(order, "price", 0) or 0),
            "instrument_token": ikey,
            "order_type": otype,
            "transaction_type": "BUY",
            "disclosed_quantity": 0,
            "trigger_price": 0,
            "is_amo": False,
        }
        try:
            import requests
            r = requests.post(f"{_API_BASE}/order/place", json=body,
                              headers=self._headers(), timeout=15,
                              proxies=self._proxies() or None)
            r.raise_for_status()
            oid = (r.json().get("data", {}) or {}).get("order_id")
            return OrderResult(status="PLACED", broker_order_id=str(oid),
                               symbol=order.symbol, qty=order.qty)
        except Exception as e:
            log.error("upstox place_order failed for %s: %s", order.symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty, error=str(e))

    async def get_pending_orders(self) -> List[Any]:
        if not self._access_token():
            return []
        try:
            import requests
            r = requests.get(f"{_API_BASE}/order/retrieve-all",
                             headers=self._headers(), timeout=10,
                             proxies=self._proxies() or None)
            r.raise_for_status()
            orders = r.json().get("data", []) or []
            return [o for o in orders
                    if o.get("status") in ("open", "trigger pending")]
        except Exception as e:
            log.warning("upstox get_pending_orders failed: %s", e)
            return []

    async def cancel_order(self, order_id: str) -> Any:
        if not self._live_allowed():
            return {"status": "DRY_RUN", "order_id": order_id}
        try:
            import requests
            r = requests.delete(f"{_API_BASE}/order/cancel",
                                params={"order_id": order_id},
                                headers=self._headers(), timeout=10,
                                proxies=self._proxies() or None)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning("upstox cancel_order failed for %s: %s", order_id, e)
            return {"status": "FAILED", "error": str(e)}

    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str,
                                kite_product: str | None = None,
                                direction: str = "long") -> OrderResult:
        # kite_product accepted (ignored) + direction for FUTURES cover:
        # long→SELL (default), short→BUY-to-cover.
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=symbol, qty=qty, raw={"dry_run": True})
        ikey = self._instrument_key(symbol)
        if not ikey:
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty,
                               error="instrument_key not resolved")
        product = _PRODUCT_MAP.get(getattr(self.profile, "order_product", "CNC"),
                                   "D")
        _side = "BUY" if str(direction).lower() == "short" else "SELL"
        body = {
            "quantity": int(qty), "product": product, "validity": "DAY",
            "price": 0, "instrument_token": ikey, "order_type": "MARKET",
            "transaction_type": _side, "disclosed_quantity": 0,
            "trigger_price": 0, "is_amo": False,
        }
        try:
            import requests
            r = requests.post(f"{_API_BASE}/order/place", json=body,
                              headers=self._headers(), timeout=15,
                              proxies=self._proxies() or None)
            r.raise_for_status()
            oid = (r.json().get("data", {}) or {}).get("order_id")
            return OrderResult(status="PLACED", broker_order_id=str(oid),
                               symbol=symbol, qty=qty)
        except Exception as e:
            log.error("upstox place_market_exit failed for %s: %s", symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty, error=str(e))
