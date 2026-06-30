"""Dhan broker client.

IMPLEMENTED — UNVERIFIED (no test account). Zerodha remains the verified path.
Wraps the Dhan HTTP API v2 (https://dhanhq.co/docs/v2/) the way zerodha.py wraps
Kite. PER-ACCOUNT client built from the profile's creds; NO process-global
client.

Dhan specifics:
  * Auth is a long-lived `access-token` header (Dhan tokens last ~24h+; the daily
    refresh friction is lower than Kite). api_key carries the Dhan client_id
    (dhanClientId), access_token the JWT.
  * Instruments are addressed by `securityId` (numeric) on an exchange segment
    ("NSE_EQ"). We resolve symbol → securityId via the published instrument CSV
    (cached).
  * Quotes: POST /v2/marketfeed/ltp with {"NSE_EQ": [securityId,...]}.
  * Orders: POST /v2/orders, cancel DELETE /v2/orders/{order-id}.
  * No GTT-OCO wired (place_gtt_oco → None default).

DRY-RUN SAFETY: same contract. Network calls never raise out of get_ltp.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base import BrokerClient, OrderResult

log = logging.getLogger("kanida.autotrade.broker.dhan")

_API_BASE = "https://api.dhan.co/v2"
_INSTRUMENT_CSV = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

_PRODUCT_MAP = {"CNC": "CNC", "MIS": "INTRADAY", "NRML": "MARGIN", "MTF": "MTF"}


class DhanBroker(BrokerClient):
    broker_name = "dhan"

    def __init__(self, profile, dry_run: bool = True):
        super().__init__(profile, dry_run=dry_run)
        self._sec_cache: Dict[str, str] = {}

    def _access_token(self) -> Optional[str]:
        return getattr(self.profile, "access_token", "") or None

    def _client_id(self) -> str:
        return getattr(self.profile, "api_key", "") or ""

    def _live_allowed(self) -> bool:
        if self.dry_run:
            return False
        from falcon.trade.services.order_executor import _autotrade_enabled
        return _autotrade_enabled() and bool(self._access_token())

    def _headers(self) -> Dict[str, str]:
        return {
            "access-token": self._access_token() or "",
            "client-id": self._client_id(),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _proxies(self):
        try:
            from services.kite_auth import _kite_proxies
            return _kite_proxies()
        except Exception:  # pragma: no cover
            return None

    def _security_id(self, symbol: str, segment: str = "NSE_EQ") -> Optional[str]:
        key = f"{segment}:{symbol}"
        if key in self._sec_cache:
            return self._sec_cache[key]
        try:
            import csv
            import io
            import requests
            r = requests.get(_INSTRUMENT_CSV, timeout=60,
                             proxies=self._proxies() or None)
            r.raise_for_status()
            reader = csv.DictReader(io.StringIO(r.text))
            for row in reader:
                # Column names vary across Dhan master versions; match defensively.
                tsym = (row.get("SEM_TRADING_SYMBOL")
                        or row.get("TRADING_SYMBOL") or "")
                seg = (row.get("SEM_EXM_EXCH_ID") or row.get("EXCH_ID") or "")
                series = (row.get("SEM_SERIES") or row.get("SERIES") or "")
                if tsym == symbol and seg == "NSE" and series in ("EQ", ""):
                    sid = (row.get("SEM_SMST_SECURITY_ID")
                           or row.get("SECURITY_ID"))
                    if sid:
                        self._sec_cache[key] = str(sid)
                        return str(sid)
        except Exception as e:  # pragma: no cover - network/format dependent
            log.warning("dhan security_id lookup failed for %s: %s", symbol, e)
        return None

    def get_ltp(self, symbol: str) -> Optional[float]:
        sid = self._security_id(symbol)
        if not sid:
            return None
        try:
            import requests
            r = requests.post(f"{_API_BASE}/marketfeed/ltp",
                              json={"NSE_EQ": [int(sid)]},
                              headers=self._headers(), timeout=10,
                              proxies=self._proxies() or None)
            r.raise_for_status()
            data = (r.json().get("data", {}) or {}).get("NSE_EQ", {})
            row = data.get(str(sid)) or data.get(sid)
            if row and row.get("last_price"):
                return float(row["last_price"])
        except Exception as e:
            log.warning("dhan get_ltp failed for %s: %s", symbol, e)
        return None

    def get_lot_size(self, contract: str) -> int:
        raise NotImplementedError("Dhan F&O lot_size not implemented")

    def get_active_futures(self, symbol: str, expiry_preference: str) -> str:
        raise NotImplementedError("Dhan futures selection not implemented")

    def get_option_chain(self, symbol: str) -> List[Any]:
        raise NotImplementedError("Dhan option chain not implemented")

    def get_option_contract(self, symbol: str, strike: float,
                            expiry_preference: str) -> str:
        raise NotImplementedError("Dhan option contract not implemented")

    def _order_body(self, symbol: str, qty: int, side: str,
                    otype: str = "MARKET", price: float = 0.0) -> Optional[dict]:
        sid = self._security_id(symbol)
        if not sid:
            return None
        product = _PRODUCT_MAP.get(getattr(self.profile, "order_product", "CNC"),
                                   "CNC")
        return {
            "dhanClientId": self._client_id(),
            "transactionType": side,
            "exchangeSegment": "NSE_EQ",
            "productType": product,
            "orderType": otype,
            "validity": "DAY",
            "securityId": str(sid),
            "quantity": int(qty),
            "price": float(price),
            "disclosedQuantity": 0,
            "afterMarketOrder": False,
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
                               error="securityId not resolved")
        try:
            import requests
            r = requests.post(f"{_API_BASE}/orders", json=body,
                              headers=self._headers(), timeout=15,
                              proxies=self._proxies() or None)
            r.raise_for_status()
            oid = (r.json() or {}).get("orderId")
            return OrderResult(status="PLACED", broker_order_id=str(oid),
                               symbol=order.symbol, qty=order.qty)
        except Exception as e:
            log.error("dhan place_order failed for %s: %s", order.symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty, error=str(e))

    async def get_pending_orders(self) -> List[Any]:
        if not self._access_token():
            return []
        try:
            import requests
            r = requests.get(f"{_API_BASE}/orders", headers=self._headers(),
                             timeout=10, proxies=self._proxies() or None)
            r.raise_for_status()
            orders = r.json() or []
            return [o for o in orders
                    if o.get("orderStatus") in ("PENDING", "TRANSIT")]
        except Exception as e:
            log.warning("dhan get_pending_orders failed: %s", e)
            return []

    async def cancel_order(self, order_id: str) -> Any:
        if not self._live_allowed():
            return {"status": "DRY_RUN", "order_id": order_id}
        try:
            import requests
            r = requests.delete(f"{_API_BASE}/orders/{order_id}",
                                headers=self._headers(), timeout=10,
                                proxies=self._proxies() or None)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning("dhan cancel_order failed for %s: %s", order_id, e)
            return {"status": "FAILED", "error": str(e)}

    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str) -> OrderResult:
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=symbol, qty=qty, raw={"dry_run": True})
        body = self._order_body(symbol, qty, "SELL", "MARKET", 0.0)
        if body is None:
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty,
                               error="securityId not resolved")
        try:
            import requests
            r = requests.post(f"{_API_BASE}/orders", json=body,
                              headers=self._headers(), timeout=15,
                              proxies=self._proxies() or None)
            r.raise_for_status()
            oid = (r.json() or {}).get("orderId")
            return OrderResult(status="PLACED", broker_order_id=str(oid),
                               symbol=symbol, qty=qty)
        except Exception as e:
            log.error("dhan place_market_exit failed for %s: %s", symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty, error=str(e))
