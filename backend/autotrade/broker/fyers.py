"""Fyers broker client.

IMPLEMENTED — UNVERIFIED (no test account). Zerodha remains the verified path.
Wraps the Fyers API v3 (https://myapi.fyers.in/docsv3) the way zerodha.py wraps
Kite. PER-ACCOUNT client built from the profile's creds; NO process-global
client.

Fyers specifics:
  * Auth header is `Authorization: <app_id>:<access_token>`. api_key carries the
    Fyers app_id (e.g. "XXXXXX-100"); access_token the daily token.
  * Symbols are addressed directly as "NSE:SYMBOL-EQ" (no numeric token lookup
    needed) — simpler than the others.
  * Quotes: GET /data/quotes?symbols=NSE:SYMBOL-EQ
  * Orders: POST /api/v3/orders/sync (side 1=BUY, -1=SELL; type 2=MARKET,
    1=LIMIT), cancel DELETE /api/v3/orders/sync with {"id": order_id}.
  * No GTT-OCO wired (place_gtt_oco → None default).

DRY-RUN SAFETY: same contract. Network calls never raise out of get_ltp.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base import BrokerClient, OrderResult

log = logging.getLogger("kanida.autotrade.broker.fyers")

_API_BASE = "https://api-t1.fyers.in/api/v3"
_DATA_BASE = "https://api-t1.fyers.in/data"

# Fyers productType strings.
_PRODUCT_MAP = {"CNC": "CNC", "MIS": "INTRADAY", "NRML": "MARGIN", "MTF": "MTF"}


class FyersBroker(BrokerClient):
    broker_name = "fyers"

    def __init__(self, profile, dry_run: bool = True):
        super().__init__(profile, dry_run=dry_run)

    def _access_token(self) -> Optional[str]:
        return getattr(self.profile, "access_token", "") or None

    def _app_id(self) -> str:
        return getattr(self.profile, "api_key", "") or ""

    def _live_allowed(self) -> bool:
        if self.dry_run:
            return False
        from falcon.trade.services.order_executor import _autotrade_enabled
        return _autotrade_enabled() and bool(self._access_token())

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"{self._app_id()}:{self._access_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _proxies(self):
        try:
            from services.kite_auth import _kite_proxies
            return _kite_proxies()
        except Exception:  # pragma: no cover
            return None

    def _fy_symbol(self, symbol: str) -> str:
        return f"NSE:{symbol}-EQ"

    def get_ltp(self, symbol: str) -> Optional[float]:
        try:
            import requests
            r = requests.get(f"{_DATA_BASE}/quotes",
                             params={"symbols": self._fy_symbol(symbol)},
                             headers=self._headers(), timeout=10,
                             proxies=self._proxies() or None)
            r.raise_for_status()
            data = r.json().get("d", []) or []
            if data:
                v = (data[0].get("v", {}) or {})
                lp = v.get("lp")
                if lp:
                    return float(lp)
        except Exception as e:
            log.warning("fyers get_ltp failed for %s: %s", symbol, e)
        return None

    def get_lot_size(self, contract: str) -> int:
        raise NotImplementedError("Fyers F&O lot_size not implemented")

    def get_active_futures(self, symbol: str, expiry_preference: str) -> str:
        raise NotImplementedError("Fyers futures selection not implemented")

    def get_option_chain(self, symbol: str) -> List[Any]:
        raise NotImplementedError("Fyers option chain not implemented")

    def get_option_contract(self, symbol: str, strike: float,
                            expiry_preference: str) -> str:
        raise NotImplementedError("Fyers option contract not implemented")

    def _order_body(self, symbol: str, qty: int, side: int,
                    otype: int = 2, price: float = 0.0) -> dict:
        product = _PRODUCT_MAP.get(getattr(self.profile, "order_product", "CNC"),
                                   "CNC")
        return {
            "symbol": self._fy_symbol(symbol),
            "qty": int(qty),
            "type": otype,             # 2 = MARKET, 1 = LIMIT
            "side": side,              # 1 = BUY, -1 = SELL
            "productType": product,
            "limitPrice": float(price),
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
        }

    async def place_order(self, order) -> OrderResult:
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty,
                               raw={"dry_run": True})
        otype = 2 if getattr(order, "order_type", "MARKET") == "MARKET" else 1
        body = self._order_body(order.symbol, order.qty, 1, otype,
                                float(getattr(order, "price", 0) or 0))
        try:
            import requests
            r = requests.post(f"{_API_BASE}/orders/sync", json=body,
                              headers=self._headers(), timeout=15,
                              proxies=self._proxies() or None)
            r.raise_for_status()
            oid = (r.json() or {}).get("id")
            return OrderResult(status="PLACED", broker_order_id=str(oid),
                               symbol=order.symbol, qty=order.qty)
        except Exception as e:
            log.error("fyers place_order failed for %s: %s", order.symbol, e)
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
            orders = (r.json() or {}).get("orderBook", []) or []
            return [o for o in orders if o.get("status") in (6, 4)]  # pending/transit
        except Exception as e:
            log.warning("fyers get_pending_orders failed: %s", e)
            return []

    async def cancel_order(self, order_id: str) -> Any:
        if not self._live_allowed():
            return {"status": "DRY_RUN", "order_id": order_id}
        try:
            import requests
            r = requests.delete(f"{_API_BASE}/orders/sync",
                                json={"id": order_id},
                                headers=self._headers(), timeout=10,
                                proxies=self._proxies() or None)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning("fyers cancel_order failed for %s: %s", order_id, e)
            return {"status": "FAILED", "error": str(e)}

    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str,
                                kite_product: str | None = None,
                                direction: str = "long",
                                *, exec_cfg=None,
                                client_order_id: str | None = None) -> OrderResult:
        # kite_product accepted (ignored) + direction for FUTURES cover. Fyers
        # side: -1 = SELL (long exit, default), 1 = BUY-to-cover (short exit).
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=symbol, qty=qty, raw={"dry_run": True})
        _side = 1 if str(direction).lower() == "short" else -1
        body = self._order_body(symbol, qty, _side, 2, 0.0)
        try:
            import requests
            r = requests.post(f"{_API_BASE}/orders/sync", json=body,
                              headers=self._headers(), timeout=15,
                              proxies=self._proxies() or None)
            r.raise_for_status()
            oid = (r.json() or {}).get("id")
            return OrderResult(status="PLACED", broker_order_id=str(oid),
                               symbol=symbol, qty=qty)
        except Exception as e:
            log.error("fyers place_market_exit failed for %s: %s", symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty, error=str(e))
