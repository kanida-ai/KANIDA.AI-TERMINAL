"""Angel One (SmartAPI) broker client.

IMPLEMENTED — UNVERIFIED (no test account). Zerodha remains the verified path.
Wraps Angel One's SmartAPI REST (https://smartapi.angelbroking.com/docs) the way
zerodha.py wraps Kite. PER-ACCOUNT client built from the profile's
api_key + access_token (the SmartAPI JWT, minted daily by the login flow and
stored in the vault); NO process-global client.

Angel specifics:
  * Auth = Bearer JWT (access_token) + several fixed client headers SmartAPI
    requires (X-PrivateKey = api_key, X-ClientLocalIP / X-ClientPublicIP /
    X-MACAddress / X-UserType / X-SourceID). We send sane defaults.
  * Instruments are addressed by `symboltoken` (numeric) + tradingsymbol. We
    resolve symbol → token via the published scrip-master JSON (cached). NSE
    equity tradingsymbols carry a "-EQ" suffix in Angel's master.
  * Quotes: POST /rest/secure/angelbroking/order/v1/getLtpData
  * Orders: POST .../order/v1/placeOrder, cancel via .../order/v1/cancelOrder.
  * No GTT-OCO wired (place_gtt_oco → None default) — software kill switch is the
    exit; Angel GTT is a separate API, deferred.

DRY-RUN SAFETY: same contract — dry_run places nothing; live needs
FALCON_AUTOTRADE_ENABLED. Network calls never raise out of get_ltp.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base import BrokerClient, OrderResult

log = logging.getLogger("kanida.autotrade.broker.angel")

_API_BASE = "https://apiconnect.angelone.in"
_SCRIP_MASTER = ("https://margincalculator.angelbroking.com/OpenAPI_File/files/"
                 "OpenAPIScripMaster.json")

_PRODUCT_MAP = {"CNC": "DELIVERY", "MIS": "INTRADAY", "NRML": "CARRYFORWARD",
                "MTF": "DELIVERY"}


class AngelBroker(BrokerClient):
    broker_name = "angel"

    def __init__(self, profile, dry_run: bool = True):
        super().__init__(profile, dry_run=dry_run)
        self._token_cache: Dict[str, str] = {}

    def _access_token(self) -> Optional[str]:
        return getattr(self.profile, "access_token", "") or None

    def _api_key(self) -> str:
        return getattr(self.profile, "api_key", "") or ""

    def _live_allowed(self) -> bool:
        if self.dry_run:
            return False
        from falcon.trade.services.order_executor import _autotrade_enabled
        return _autotrade_enabled() and bool(self._access_token())

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": "127.0.0.1",
            "X-ClientPublicIP": "127.0.0.1",
            "X-MACAddress": "00:00:00:00:00:00",
            "X-PrivateKey": self._api_key(),
        }

    def _proxies(self):
        try:
            from services.kite_auth import _kite_proxies
            return _kite_proxies()
        except Exception:  # pragma: no cover
            return None

    def _symbol_token(self, symbol: str, exch: str = "NSE") -> Optional[str]:
        key = f"{exch}:{symbol}"
        if key in self._token_cache:
            return self._token_cache[key]
        try:
            import requests
            r = requests.get(_SCRIP_MASTER, timeout=30,
                             proxies=self._proxies() or None)
            r.raise_for_status()
            want = f"{symbol}-EQ"
            for ins in r.json():
                if ins.get("exch_seg") == exch and ins.get("symbol") == want:
                    tok = ins.get("token")
                    if tok:
                        self._token_cache[key] = str(tok)
                        return str(tok)
        except Exception as e:  # pragma: no cover - network/format dependent
            log.warning("angel scrip token lookup failed for %s: %s", symbol, e)
        return None

    def get_ltp(self, symbol: str) -> Optional[float]:
        tok = self._symbol_token(symbol)
        if not tok:
            return None
        try:
            import requests
            body = {"exchange": "NSE", "tradingsymbol": f"{symbol}-EQ",
                    "symboltoken": tok}
            r = requests.post(
                f"{_API_BASE}/rest/secure/angelbroking/order/v1/getLtpData",
                json=body, headers=self._headers(), timeout=10,
                proxies=self._proxies() or None)
            r.raise_for_status()
            lp = (r.json().get("data", {}) or {}).get("ltp")
            return float(lp) if lp else None
        except Exception as e:
            log.warning("angel get_ltp failed for %s: %s", symbol, e)
            return None

    def get_lot_size(self, contract: str) -> int:
        raise NotImplementedError("Angel F&O lot_size not implemented")

    def get_active_futures(self, symbol: str, expiry_preference: str) -> str:
        raise NotImplementedError("Angel futures selection not implemented")

    def get_option_chain(self, symbol: str) -> List[Any]:
        raise NotImplementedError("Angel option chain not implemented")

    def get_option_contract(self, symbol: str, strike: float,
                            expiry_preference: str) -> str:
        raise NotImplementedError("Angel option contract not implemented")

    def _order_body(self, symbol: str, qty: int, side: str,
                    otype: str = "MARKET", price: float = 0.0) -> Optional[dict]:
        tok = self._symbol_token(symbol)
        if not tok:
            return None
        product = _PRODUCT_MAP.get(getattr(self.profile, "order_product", "CNC"),
                                   "DELIVERY")
        return {
            "variety": "NORMAL", "tradingsymbol": f"{symbol}-EQ",
            "symboltoken": tok, "transactiontype": side, "exchange": "NSE",
            "ordertype": otype, "producttype": product, "duration": "DAY",
            "price": str(price), "squareoff": "0", "stoploss": "0",
            "quantity": str(int(qty)),
        }

    async def place_order(self, order) -> OrderResult:
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty,
                               raw={"dry_run": True})
        otype = "MARKET" if getattr(order, "order_type", "MARKET") == "MARKET" \
            else "LIMIT"
        body = self._order_body(order.symbol, order.qty, "BUY", otype,
                                float(getattr(order, "price", 0) or 0))
        if body is None:
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty,
                               error="symboltoken not resolved")
        try:
            import requests
            r = requests.post(
                f"{_API_BASE}/rest/secure/angelbroking/order/v1/placeOrder",
                json=body, headers=self._headers(), timeout=15,
                proxies=self._proxies() or None)
            r.raise_for_status()
            oid = (r.json().get("data", {}) or {}).get("orderid")
            return OrderResult(status="PLACED", broker_order_id=str(oid),
                               symbol=order.symbol, qty=order.qty)
        except Exception as e:
            log.error("angel place_order failed for %s: %s", order.symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty, error=str(e))

    async def get_pending_orders(self) -> List[Any]:
        if not self._access_token():
            return []
        try:
            import requests
            r = requests.get(
                f"{_API_BASE}/rest/secure/angelbroking/order/v1/getOrderBook",
                headers=self._headers(), timeout=10,
                proxies=self._proxies() or None)
            r.raise_for_status()
            orders = r.json().get("data", []) or []
            return [o for o in orders if o.get("status") in ("open", "trigger pending")]
        except Exception as e:
            log.warning("angel get_pending_orders failed: %s", e)
            return []

    async def cancel_order(self, order_id: str) -> Any:
        if not self._live_allowed():
            return {"status": "DRY_RUN", "order_id": order_id}
        try:
            import requests
            r = requests.post(
                f"{_API_BASE}/rest/secure/angelbroking/order/v1/cancelOrder",
                json={"variety": "NORMAL", "orderid": order_id},
                headers=self._headers(), timeout=10,
                proxies=self._proxies() or None)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning("angel cancel_order failed for %s: %s", order_id, e)
            return {"status": "FAILED", "error": str(e)}

    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str,
                                kite_product: str | None = None,
                                direction: str = "long") -> OrderResult:
        # kite_product accepted (ignored here) + direction for FUTURES cover:
        # long→SELL (default), short→BUY-to-cover.
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=symbol, qty=qty, raw={"dry_run": True})
        _side = "BUY" if str(direction).lower() == "short" else "SELL"
        body = self._order_body(symbol, qty, _side, "MARKET", 0.0)
        if body is None:
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty,
                               error="symboltoken not resolved")
        try:
            import requests
            r = requests.post(
                f"{_API_BASE}/rest/secure/angelbroking/order/v1/placeOrder",
                json=body, headers=self._headers(), timeout=15,
                proxies=self._proxies() or None)
            r.raise_for_status()
            oid = (r.json().get("data", {}) or {}).get("orderid")
            return OrderResult(status="PLACED", broker_order_id=str(oid),
                               symbol=symbol, qty=qty)
        except Exception as e:
            log.error("angel place_market_exit failed for %s: %s", symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty, error=str(e))
