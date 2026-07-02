"""Rupeezy (Vortex API) broker client — real execution, Stage 2.

IMPLEMENTED — UNCERTIFIED. Wraps the Vortex HTTP API per
`docs/design/RUPEEZY_VORTEX_API_REFERENCE.md`, the way zerodha.py wraps Kite and
dhan.py wraps the Dhan API. PER-ACCOUNT client built from the profile's resolved
creds (access_token + x-api-key); NO process-global client, NO auth here.

Vortex specifics:
  * Auth header: `Authorization: Bearer <access_token>` (minted by RupeezyAuth).
  * Instruments addressed by a NUMERIC `token` on an `exchange` segment
    ("NSE_EQ"). symbol → token requires the instrument master (see below).
  * Orders: POST /trading/orders/regular; variety RL (limit) / RL-MKT (market);
    product INTRADAY / DELIVERY / MTF; cancel DELETE /trading/orders/regular/{id}.
  * GTT-OCO: inline `gtt` object on the entry order (stop + target) — TODO(certify).

DRY-RUN SAFETY (identical contract to every other adapter):
  * When dry_run=True (default), OR FALCON_AUTOTRADE_ENABLED != 'true', OR no
    access_token → every order/exit/cancel/GTT path returns a synthetic DRY_RUN
    (or None) result and makes ZERO real HTTP calls.
  * Every live failure returns a FAILED OrderResult — never raises into strategy.

INSTRUMENT MASTER: symbol → numeric token is the ONE piece that genuinely needs
the live Vortex master to place an order. `_resolve_token` reads a pluggable,
cached local map (env RUPEEZY_INSTRUMENT_MASTER path or a bundled cache file). It
NEVER fabricates a token — an unresolved symbol raises a clear
"instrument master not configured" error which the order path converts into a
FAILED OrderResult.

TODO(certify) items (see RUPEEZY_VORTEX_API_REFERENCE.md §"Still to CONFIRM"):
  * RUPEEZY_API_BASE default host (#1).
  * Positions/holdings paths (#2/#3).
  * Funds/margin path (#4).
  * Instrument master download URL + format (#5).
  * Quotes/LTP endpoint (#6).
  * GTT inline object exact shape + MTF/GTT semantics (#8, reference §Orders).
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import BrokerClient, OrderResult
from .rupeezy_auth_provider import _api_base, _proxies

log = logging.getLogger("kanida.autotrade.broker.rupeezy")

_HTTP_TIMEOUT = 15
_RETRY_MAX_ATTEMPTS = 3
_RETRY_BASE_SLEEP_SEC = 0.4

# Our product string → Vortex `product` (reference §Orders adapter mapping).
_PRODUCT_MAP = {
    "EQ": "DELIVERY",
    "CNC": "DELIVERY",
    "MTF": "MTF",
    "MIS": "INTRADAY",
    "NRML": "INTRADAY",  # F&O carry — TODO(certify): Vortex NRML equivalent
}

# Our exchange string → Vortex `exchange` segment (EQ-first; F&O later).
_EXCHANGE_MAP = {
    "NSE": "NSE_EQ",
    "NSE_EQ": "NSE_EQ",
    "BSE": "BSE_EQ",
    "NFO": "NSE_FO",   # TODO(certify): F&O routing
    "MCX": "MCX_FO",
}


def _is_transient(status_code: Optional[int]) -> bool:
    """5xx / 429 are transient (retry); 4xx (auth/validation) are permanent."""
    if status_code is None:
        return True  # network error → transient
    return status_code == 429 or 500 <= status_code < 600


class RupeezyBroker(BrokerClient):
    broker_name = "rupeezy"

    def __init__(self, profile, dry_run: bool = True):
        super().__init__(profile, dry_run=dry_run)
        self._token_cache: Dict[str, int] = {}
        self._master: Optional[Dict[str, int]] = None

    # ── creds / gating ────────────────────────────────────────────────────────
    def _access_token(self) -> Optional[str]:
        return getattr(self.profile, "access_token", "") or None

    def _x_api_key(self) -> str:
        # x-api-key (app secret) is stored in vault api_secret.
        return getattr(self.profile, "api_secret", "") or ""

    def _live_allowed(self) -> bool:
        """Real orders require dry_run off AND the master env switch on AND a
        resolved access token — same gate as every other live adapter."""
        if self.dry_run:
            return False
        from falcon.trade.services.order_executor import _autotrade_enabled
        return _autotrade_enabled() and bool(self._access_token())

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token() or ''}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    # ── HTTP with retry (never raises into strategy) ──────────────────────────
    def _request(self, method: str, path: str, *, json_body: Optional[dict] = None,
                 params: Optional[dict] = None):
        """Perform an HTTP call to the Vortex API with linear-backoff retry on
        transient errors. Returns the requests.Response, or raises the last
        exception (callers wrap and convert to a FAILED result)."""
        import requests

        url = f"{_api_base()}{path}"
        last_exc: Optional[BaseException] = None
        for attempt in range(1, _RETRY_MAX_ATTEMPTS + 1):
            try:
                r = requests.request(
                    method, url, headers=self._headers(), json=json_body,
                    params=params, timeout=_HTTP_TIMEOUT,
                    proxies=_proxies() or None)
                if r.status_code >= 400 and _is_transient(r.status_code) \
                        and attempt < _RETRY_MAX_ATTEMPTS:
                    time.sleep(_RETRY_BASE_SLEEP_SEC * attempt)
                    continue
                return r
            except Exception as e:  # network error
                last_exc = e
                if attempt >= _RETRY_MAX_ATTEMPTS:
                    raise
                time.sleep(_RETRY_BASE_SLEEP_SEC * attempt)
        if last_exc:
            raise last_exc
        raise RuntimeError("rupeezy: request failed with no response")

    # ── instrument master (symbol → numeric token) ────────────────────────────
    def _load_master(self) -> Dict[str, int]:
        """Load the symbol→token map from a local cache. Pluggable source:
          1. env RUPEEZY_INSTRUMENT_MASTER (path to a JSON {symbol: token} map), or
          2. a bundled cache file data/config/rupeezy_instruments.json.
        Returns {} when no cache is configured (→ _resolve_token raises clearly).

        TODO(certify): the live Vortex master DOWNLOAD URL + format (reference
        CONFIRM #5). Until certified we NEVER fabricate a token — orders fail
        loudly if the master is absent."""
        if self._master is not None:
            return self._master
        candidates: List[Path] = []
        env_path = os.environ.get("RUPEEZY_INSTRUMENT_MASTER", "").strip()
        if env_path:
            candidates.append(Path(env_path))
        # backend/autotrade/broker/rupeezy.py → project root = parents[3]
        here = Path(__file__).resolve()
        if len(here.parents) > 3:
            candidates.append(here.parents[3] / "data" / "config"
                              / "rupeezy_instruments.json")
        for p in candidates:
            try:
                if p.exists():
                    raw = json.loads(p.read_text(encoding="utf-8"))
                    # Accept {symbol: token} or {"NSE_EQ": {symbol: token}}.
                    flat: Dict[str, int] = {}
                    for k, v in raw.items():
                        if isinstance(v, dict):
                            for sym, tok in v.items():
                                flat[f"{k}:{sym}"] = int(tok)
                        else:
                            flat[str(k)] = int(v)
                    self._master = flat
                    log.info("rupeezy: loaded instrument master (%d entries) from %s",
                             len(flat), p)
                    return self._master
            except Exception as e:  # pragma: no cover - format dependent
                log.warning("rupeezy: failed reading instrument master %s: %s", p, e)
        self._master = {}
        return self._master

    def _resolve_token(self, symbol: str, exchange: str = "NSE_EQ") -> int:
        """Resolve a bare symbol → numeric Vortex instrument token. Raises a clear
        error when the master is not configured (NEVER fabricates a token)."""
        key = f"{exchange}:{symbol}"
        if key in self._token_cache:
            return self._token_cache[key]
        master = self._load_master()
        tok = master.get(key)
        if tok is None:
            tok = master.get(symbol)  # flat {symbol: token} fallback
        if tok is None:
            raise ValueError(
                f"rupeezy: instrument master not configured — cannot resolve "
                f"token for {symbol} ({exchange}). Set RUPEEZY_INSTRUMENT_MASTER "
                "or provide data/config/rupeezy_instruments.json "
                "(TODO(certify) live master URL — see RUPEEZY_VORTEX_API_REFERENCE.md #5).")
        self._token_cache[key] = int(tok)
        return int(tok)

    # ── mapping helpers ───────────────────────────────────────────────────────
    def _map_product(self, instrument_type: Optional[str]) -> str:
        it = (instrument_type or "CNC").upper()
        prod = _PRODUCT_MAP.get(it)
        if prod is None:
            log.warning("rupeezy: unknown product %r — defaulting DELIVERY", it)
            prod = "DELIVERY"
        return prod

    def _map_exchange(self, exchange: Optional[str]) -> str:
        ex = (exchange or "NSE").upper()
        return _EXCHANGE_MAP.get(ex, "NSE_EQ")

    def _order_body(self, symbol: str, qty: int, side: str, *,
                    order_type: str = "MARKET", price: Optional[float] = None,
                    product: str = "CNC", exchange: str = "NSE") -> dict:
        """Build the Vortex /trading/orders/regular body from our order fields.
        MARKET → variety=RL-MKT; LIMIT → variety=RL + price (reference §Orders)."""
        seg = self._map_exchange(exchange)
        token = self._resolve_token(symbol, seg)
        variety = "RL-MKT" if order_type.upper() == "MARKET" else "RL"
        body: dict = {
            "exchange": seg,
            "token": token,
            "transaction_type": side.upper(),
            "product": self._map_product(product),
            "variety": variety,
            "quantity": int(qty),
            "validity": "DAY",
            "disclosed_quantity": 0,
            "is_amo": False,
        }
        if variety == "RL":
            body["price"] = round(float(price or 0.0), 2)
        return body

    # ── Order lifecycle ───────────────────────────────────────────────────────
    async def place_order(self, order) -> OrderResult:
        """Place an entry order. Dry-run safe (zero HTTP). Real placement maps our
        Order → Vortex fields and POSTs /trading/orders/regular."""
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty,
                               raw={"dry_run": True})
        try:
            body = self._order_body(
                order.symbol, order.qty, "BUY",
                order_type=getattr(order, "order_type", "MARKET") or "MARKET",
                price=getattr(order, "price", None),
                product=getattr(order, "product", "CNC") or "CNC",
                exchange=getattr(order, "exchange", "NSE") or "NSE")
        except Exception as e:  # token not resolved / mapping error
            log.error("rupeezy place_order body build failed for %s: %s",
                      order.symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty, error=str(e))
        # Optional inline GTT-OCO backup (reference §Orders `gtt`) — TODO(certify).
        gtt = getattr(order, "_rupeezy_gtt", None)
        if gtt:
            body["gtt"] = gtt
        try:
            r = self._request("POST", "/trading/orders/regular", json_body=body)
            r.raise_for_status()
            data = (r.json() or {}).get("data") or {}
            oid = data.get("order_id")
            if not oid:
                return OrderResult(status="FAILED", broker_order_id=None,
                                   symbol=order.symbol, qty=order.qty,
                                   error=f"no order_id in response ({data})")
            return OrderResult(status="PLACED", broker_order_id=str(oid),
                               symbol=order.symbol, qty=order.qty)
        except Exception as e:
            log.error("rupeezy place_order failed for %s: %s", order.symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty, error=str(e))

    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str,
                                kite_product: str | None = None,
                                direction: str = "long") -> OrderResult:
        """Flatten one position with a Vortex MARKET order (variety=RL-MKT).

        kite_product: explicit trading-product override (e.g. "MTF", "CNC", "MIS")
        — takes precedence over instrument_type (which is the security type "EQ",
        not the trading product; wrong for MTF sessions). Mirrors the Zerodha
        adapter's kite_product precedence.

        direction: FUTURES cover side — long→SELL (default), short→BUY-to-cover."""
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=symbol, qty=qty, raw={"dry_run": True})
        product = kite_product if kite_product else instrument_type
        _side = "BUY" if str(direction).lower() == "short" else "SELL"
        try:
            body = self._order_body(symbol, qty, _side, order_type="MARKET",
                                    product=product or "CNC", exchange="NSE")
        except Exception as e:
            log.error("rupeezy place_market_exit body build failed for %s: %s",
                      symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty, error=str(e))
        try:
            r = self._request("POST", "/trading/orders/regular", json_body=body)
            r.raise_for_status()
            data = (r.json() or {}).get("data") or {}
            oid = data.get("order_id")
            if not oid:
                return OrderResult(status="FAILED", broker_order_id=None,
                                   symbol=symbol, qty=qty,
                                   error=f"no order_id in response ({data})")
            log.info("rupeezy place_market_exit: %s qty=%d product=%s order_id=%s",
                     symbol, qty, self._map_product(product), oid)
            return OrderResult(status="PLACED", broker_order_id=str(oid),
                               symbol=symbol, qty=qty)
        except Exception as e:
            log.error("rupeezy place_market_exit failed for %s: %s", symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty, error=str(e))

    async def get_pending_orders(self) -> List[Any]:
        if not self._access_token():
            return []
        try:
            r = self._request("GET", "/trading/orders")
            r.raise_for_status()
            data = (r.json() or {}).get("data") or []
            # Vortex "still alive" statuses — TODO(certify) exact enum (reference §Orders).
            alive = {"PENDING", "OPEN", "TRIGGER PENDING", "TRIGGER_PENDING"}
            return [o for o in data
                    if str(o.get("status", "")).upper() in alive]
        except Exception as e:
            log.warning("rupeezy get_pending_orders failed: %s", e)
            return []

    async def cancel_order(self, order_id: str) -> Any:
        if not self._live_allowed():
            return {"status": "DRY_RUN", "order_id": order_id}
        try:
            r = self._request("DELETE", f"/trading/orders/regular/{order_id}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning("rupeezy cancel_order failed for %s: %s", order_id, e)
            return {"status": "FAILED", "error": str(e)}

    def cancel_order_sync(self, order_id: str) -> bool:
        """Synchronous cancel for the exit_poller retry loop. Dry-run → True
        (no-op). Returns False on error so the caller re-attempts a fresh exit."""
        if not self._live_allowed():
            return True
        try:
            r = self._request("DELETE", f"/trading/orders/regular/{order_id}")
            r.raise_for_status()
            return True
        except Exception as e:
            log.warning("rupeezy cancel_order_sync failed for %s: %s", order_id, e)
            return False

    def get_order_status(self, order_id: str) -> dict:
        """Fetch an order's status and NORMALISE to the shape the exit_poller
        expects: {status, filled_quantity, average_price}.

        The exit poller (monitoring/exit_poller.py) checks:
          status == "COMPLETE"                → done
          status in ("REJECTED","CANCELLED")  → terminal fail
        so we map Vortex order states onto that Kite-compatible vocabulary.

        Vortex field mapping (TODO(certify) exact names — reference §Orders,
        `GET /trading/orders/{order_id}`):
          status           → order_status | status
          filled_quantity  → filled_quantity | traded_quantity | filled_qty
          average_price    → average_price | avg_price | traded_price
        Returns {} when the order is not found (treated as still-pending)."""
        if self.dry_run:
            return {"status": "COMPLETE", "filled_quantity": 0, "average_price": 0.0}
        try:
            r = self._request("GET", f"/trading/orders/{order_id}")
            r.raise_for_status()
            payload = r.json() or {}
            data = payload.get("data")
            # Some endpoints return a list (order history) — take the latest.
            if isinstance(data, list):
                data = data[-1] if data else {}
            data = data or {}
            raw_status = str(data.get("order_status")
                             or data.get("status") or "").upper()
            status = _normalise_status(raw_status)
            filled = (data.get("filled_quantity")
                      if data.get("filled_quantity") is not None
                      else data.get("traded_quantity")
                      if data.get("traded_quantity") is not None
                      else data.get("filled_qty") or 0)
            avg = (data.get("average_price")
                   if data.get("average_price") is not None
                   else data.get("avg_price")
                   if data.get("avg_price") is not None
                   else data.get("traded_price") or 0.0)
            return {"status": status,
                    "filled_quantity": int(filled or 0),
                    "average_price": float(avg or 0.0),
                    "raw_status": raw_status}
        except Exception as e:
            log.warning("rupeezy get_order_status failed for %s: %s", order_id, e)
            raise

    # ── Market data ───────────────────────────────────────────────────────────
    def get_ltp(self, symbol: str) -> Optional[float]:
        """Last traded price via the Vortex quotes endpoint. Returns None on any
        failure so sizing cash-falls-back (never over-deploys).

        TODO(certify): quotes/LTP path + response shape (reference CONFIRM #6).
        Requires the instrument token, so an unconfigured master → None."""
        if not self._access_token():
            return None
        try:
            token = self._resolve_token(symbol, "NSE_EQ")
        except Exception as e:
            log.debug("rupeezy get_ltp token miss for %s: %s", symbol, e)
            return None
        try:
            # TODO(certify): exact quotes path + params + field names.
            r = self._request("GET", "/data/quote",
                              params={"exchange": "NSE_EQ", "token": token})
            r.raise_for_status()
            data = (r.json() or {}).get("data") or {}
            ltp = data.get("last_trade_price") or data.get("ltp") \
                or data.get("last_price")
            if ltp:
                return float(ltp)
        except Exception as e:
            log.warning("rupeezy get_ltp failed for %s: %s", symbol, e)
        return None

    # get_ltps_batch inherits the base loop over get_ltp (safe None fallback).

    # ── Portfolio (design §6) ─────────────────────────────────────────────────
    def get_positions(self) -> List[dict]:
        """Current-day positions. TODO(certify) exact path — reference CONFIRM #2
        (`GET /trading/portfolio/positions`). Returns [] on any failure."""
        if not self._access_token():
            return []
        try:
            r = self._request("GET", "/trading/portfolio/positions")
            r.raise_for_status()
            data = (r.json() or {}).get("data")
            if isinstance(data, dict):
                # Some brokers nest under {"net": [...], "day": [...]}.
                data = data.get("net") or data.get("positions") or []
            return list(data or [])
        except Exception as e:
            log.warning("rupeezy get_positions failed: %s", e)
            return []

    def get_holdings(self) -> List[dict]:
        """Delivery holdings. TODO(certify) exact path — reference CONFIRM #3
        (`GET /trading/portfolio/holdings`). Returns [] on any failure."""
        if not self._access_token():
            return []
        try:
            r = self._request("GET", "/trading/portfolio/holdings")
            r.raise_for_status()
            data = (r.json() or {}).get("data") or []
            return list(data)
        except Exception as e:
            log.warning("rupeezy get_holdings failed: %s", e)
            return []

    # ── GTT-OCO (broker-held per-position backup) ─────────────────────────────
    def place_gtt_oco(self, symbol: str, qty: int, stop_price: float,
                      target_price: float, last_price: float,
                      product: str = "CNC", exchange: str = "NSE",
                      order_type: str = "LIMIT",
                      stop_limit_price: Optional[float] = None,
                      direction: str = "long") -> Optional[str]:
        """Place a two-leg OCO GTT via the inline `gtt` object on a regular order
        (reference §Orders `gtt`). Returns the broker GTT/order id, or None when
        not placed (dry-run / unsupported) so the SOFTWARE stop still protects.

        direction: for a SHORT the OCO would need BUY-to-cover legs with an
        inverted stop/target. That shape is UNVERIFIED on Vortex, so we place
        NONE for a short here (never a wrong-direction GTT) — the software stop
        protects. long is unchanged.

        TODO(certify): the exact inline `gtt` object shape + whether OCO is one
        object or two legs (reference CONFIRM #8). This best-effort attempt builds
        a plausible {stop_loss, profit} shape; any error → None (entry/protection
        is never blocked on the GTT)."""
        if not self._live_allowed():
            return None
        if str(direction).lower() == "short":
            # Never place a wrong-direction GTT on an unverified broker OCO shape.
            log.info("rupeezy: short GTT-OCO shape unverified — placing NONE "
                     "(software stop protects) for %s", symbol)
            return None
        try:
            seg = self._map_exchange(exchange)
            token = self._resolve_token(symbol, seg)
            stop_lim = round(float(stop_limit_price
                                   if stop_limit_price is not None else stop_price), 2)
            # TODO(certify): exact gtt object schema (reference CONFIRM #8).
            body = {
                "exchange": seg,
                "token": token,
                "transaction_type": "SELL",
                "product": self._map_product(product),
                "quantity": int(qty),
                "gtt": {
                    "stop_loss": {"trigger_price": round(float(stop_price), 2),
                                  "price": stop_lim},
                    "profit": {"trigger_price": round(float(target_price), 2),
                               "price": round(float(target_price), 2)},
                },
            }
            r = self._request("POST", "/trading/orders/regular", json_body=body)
            r.raise_for_status()
            data = (r.json() or {}).get("data") or {}
            gid = data.get("gtt_id") or data.get("order_id")
            return str(gid) if gid is not None else None
        except Exception as e:
            log.error("rupeezy place_gtt_oco failed for %s: %s", symbol, e)
            return None

    # cancel_gtt / get_gtt inherit the base no-op defaults (return None) until
    # the Vortex GTT lifecycle is certified.

    # ── Instrument master (F&O) — EQ-first, minimal ────────────────────────────
    def get_lot_size(self, contract: str) -> int:
        raise NotImplementedError("rupeezy F&O lot_size not implemented (EQ-first)")

    def get_active_futures(self, symbol: str, expiry_preference: str) -> str:
        raise NotImplementedError("rupeezy futures selection not implemented (EQ-first)")

    def get_option_chain(self, symbol: str) -> List[Any]:
        raise NotImplementedError("rupeezy option chain not implemented (EQ-first)")

    def get_option_contract(self, symbol: str, strike: float,
                            expiry_preference: str) -> str:
        raise NotImplementedError("rupeezy option contract not implemented (EQ-first)")


# Vortex order state → exit_poller (Kite-compatible) vocabulary.
# TODO(certify): confirm the exact Vortex status enum (reference §Orders).
_STATUS_MAP = {
    "COMPLETE": "COMPLETE",
    "COMPLETED": "COMPLETE",
    "EXECUTED": "COMPLETE",
    "FILLED": "COMPLETE",
    "TRADED": "COMPLETE",
    "REJECTED": "REJECTED",
    "CANCELLED": "CANCELLED",
    "CANCELED": "CANCELLED",
}


def _normalise_status(raw: str) -> str:
    """Map a Vortex order status to the exit_poller vocabulary. Unknown/in-flight
    states pass through unchanged (treated as still-pending by the poller)."""
    return _STATUS_MAP.get((raw or "").upper(), (raw or "").upper())
