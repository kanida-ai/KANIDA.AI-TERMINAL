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
  * Funds path (#4). Order-margin (MIS/MTF leverage sizing) is now WIRED but
    UNCERTIFIED: qty=1 order-margin probe → per-share margin, env-overridable
    path RUPEEZY_MARGIN_PATH, tolerant parser, safe None (cash) fallback. Needs
    one live response to confirm the endpoint path + field names (#4).
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
from urllib.parse import quote as _urlquote

import socket as _socket
import threading as _threading
import urllib3.util.connection as _urllib3_conn

from .base import BrokerClient, OrderResult
from .rupeezy_auth_provider import _api_base, _proxies

log = logging.getLogger("kanida.autotrade.broker.rupeezy")


def _ipv4_only() -> int:
    """Force IPv4 address selection. Vortex allowlists a STATIC IPv4 for ORDER
    placement; on a dual-stack host the OS prefers IPv6, so an order egresses an
    IPv6 address that is NOT on the allowlist → HTTP 403 IP_NOT_ALLOWED (reads do
    NOT check the allowlist, so they succeed and MASK the problem). Pinning to
    IPv4 makes the egress the allowlisted IPv4. live-verified 2026-07-15: forcing
    IPv4 turned the order-write 403 IP_NOT_ALLOWED into a normal 400 payload
    validation — i.e. the allowlist check then PASSED."""
    return _socket.AF_INET

_HTTP_TIMEOUT = 15
_RETRY_MAX_ATTEMPTS = 3
_RETRY_BASE_SLEEP_SEC = 0.4

# ── CONNECTION / TLS POOLING (latency) ───────────────────────────────────────
# _request() used a bare requests.request(), which builds a NEW connection per
# call → every probe/order paid a fresh TCP + TLS handshake. That is why a Vortex
# probe costs ~4.8s vs Kite's ~1.1s (kiteconnect keeps a pooled Session). A
# module-level Session reuses the connection, cutting the handshake off EVERY
# call — exits AND entries (entries measured 6.5-6.7s/pos on the same sessions).
# Purely additive: same requests API, same headers/timeout/proxies/retry, same
# scoped IPv4 pin (pooled sockets are CREATED inside the pin → they connect over
# the allowlisted IPv4 and are then reused, so the live-verified egress is
# preserved).
#
# Keyed by the PROXY config, never shared across different egress routes: today
# _proxies() is process-global, but a per-ACCOUNT egress proxy is in flight on
# another branch — keying by proxy means a connection opened for one account's
# egress can NEVER be reused for another's. Auth is per-REQUEST (headers=...),
# never set on the Session, so no token can leak across profiles.
_SESSIONS: Dict[str, Any] = {}
_SESSIONS_LOCK = _threading.Lock()


def _http_session(proxies):
    """Return the pooled requests.Session for this proxy config (created once).
    urllib3's underlying PoolManager is thread-safe; the Session carries no
    per-account state (no auth, no headers) — every call passes its own."""
    import requests
    key = repr(sorted((proxies or {}).items()))
    with _SESSIONS_LOCK:
        s = _SESSIONS.get(key)
        if s is None:
            s = requests.Session()
            _SESSIONS[key] = s
        return s

# Order-margin (equity intraday/MTF leverage) probe — see get_margin_per_share.
# The endpoint path is env-overridable (RUPEEZY_MARGIN_PATH) until the Vortex
# order-margin endpoint is certified (reference CONFIRM #4). Broker leverage is
# stable intraday, so a short process cache spares N round-trips on the hot
# preview path. Keyed (SYMBOL, vortex_product) — margin is account-independent.
_MARGIN_TTL_SEC = 300
_margin_cache: Dict[Any, Any] = {}  # (symbol, product) -> (per_share_margin, ts)

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

# REVERSE maps — a Vortex book (positions/holdings) speaks Vortex tokens; the
# broker-agnostic reconciler + alert monitor consume the KITE vocabulary
# (exchange ∈ {NSE,BSE,NFO,MCX}, product ∈ {CNC,MIS,NRML,MTF}). get_positions_net
# / get_holdings therefore NORMALISE each row back to Kite tokens so the
# reconciler's (symbol, product, exchange) matcher accepts a Rupeezy row.
# live-verified 2026-07-15: a Vortex net row carries exchange="NSE_EQ",
# product="DELIVERY"; unmapped, the reconciler rejected every CNC row on BOTH the
# exchange ("NSE_EQ"!="NSE") and product ("DELIVERY"!="CNC") checks → a held CNC
# was invisible to the invariant → a false UNATTRIBUTED_CLOSE every tick.
_VORTEX_EXCHANGE_TO_KITE = {
    "NSE_EQ": "NSE",
    "BSE_EQ": "BSE",
    "NSE_FO": "NFO",
    "MCX_FO": "MCX",
}
_VORTEX_PRODUCT_TO_KITE = {
    "DELIVERY": "CNC",
    "INTRADAY": "MIS",
    "MTF": "MTF",
}


def _vortex_exchange_to_kite(exch: Optional[str]) -> Optional[str]:
    if exch in (None, ""):
        return None
    return _VORTEX_EXCHANGE_TO_KITE.get(str(exch).upper(), str(exch).upper())


def _vortex_product_to_kite(prod: Optional[str]) -> Optional[str]:
    if prod in (None, ""):
        return None
    return _VORTEX_PRODUCT_TO_KITE.get(str(prod).upper(), str(prod).upper())


def _held_from_row(row: Dict[str, Any]) -> int:
    """Signed held quantity for a Vortex positions.net row, PRODUCT-AWARE.

    live-verified 2026-07-15: a DELIVERY (CNC) row reports net `quantity`=0 and
    carries the held shares in `buy_quantity`/`sell_quantity` (the intraday net
    `quantity` field is populated for MIS/INTRADAY, NOT for delivery). So a held
    same-day CNC buy of N reads quantity=0 while buy_quantity=N, sell_quantity=0.
    Rule: when `quantity` is 0/absent, the true held is buy−sell (delivery, or a
    genuinely flat intraday round-trip → buy−sell=0); when `quantity` is populated
    (a live MIS/INTRADAY leg, e.g. a −50 short) it IS the signed exposure. This
    never regresses the MIS path (quantity non-zero → used as-is) and fixes the
    delivery read (quantity 0 → buy−sell)."""
    bq = int(_to_int(row.get("buy_quantity", row.get("buy_qty"))))
    sq = int(_to_int(row.get("sell_quantity", row.get("sell_qty"))))
    raw_q = row.get("quantity", row.get("net_quantity", row.get("net_qty")))
    if raw_q in (None, 0, "0"):
        return bq - sq
    return int(_to_int(raw_q))


def _to_int(v) -> int:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def _is_transient(status_code: Optional[int]) -> bool:
    """5xx / 429 are transient (retry); 4xx (auth/validation) are permanent."""
    if status_code is None:
        return True  # network error → transient
    return status_code == 429 or 500 <= status_code < 600


class RupeezyBroker(BrokerClient):
    broker_name = "rupeezy"

    # BOOK SEMANTICS (see BrokerClient.broker_held_qty).
    # Vortex's holdings `t1_quantity` ALREADY contains TODAY's CNC buys (unlike
    # Kite, where a same-day buy sits only in the net book). So the shared Kite
    # rule `holdings + max(0, net)` DOUBLE-COUNTS a same-day CNC buy → a ~2× broker
    # SURPLUS → the bogus CORP_ACTION_SUSPECTED alerts on Rupeezy CNC.
    # CONFIRMED LIVE 2026-07-27: the operator's first live Rupeezy 5L CNC campaign
    # produced a UNIFORM 2.0× broker_held/db_held across 9 same-day CNC names
    # (DATAPATTNS, KARURVYSYA, M&MFIN, MANAPPURAM, GRAPHITE, SPLPETRO, TVSMOTOR,
    # FLUOROCHEM, AEGISVOPAK). broker_held = net + holdings = 2×db ⟹ net == holdings
    # == db ⟹ the same-day buy is present in BOTH the net book AND holdings.t1 —
    # exactly the suspected double-count. Zerodha MIS (never enters holdings) did
    # NOT trip, confirming the mechanism. So per the prior note ("if confirmed,
    # flip to True"), flipped: held = max(0, holdings) alone.
    # Still FAIL-SAFE if ever wrong: an under-count reads as a DEFICIT → an alert
    # with no same-day sell evidence → never a close / never a mutation.
    CNC_HOLDINGS_INCLUDE_SAME_DAY_BUYS = True

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
                # Pin THIS request to IPv4 so the egress is the allowlisted static
                # IPv4 (see _ipv4_only). Scoped: restored immediately after, so the
                # rest of the process (e.g. the live Kite path) is untouched.
                _orig_gai = _urllib3_conn.allowed_gai_family
                _urllib3_conn.allowed_gai_family = _ipv4_only
                try:
                    # Pooled Session (connection/TLS reuse) instead of a bare
                    # requests.request() that re-handshakes every call. The pin
                    # still wraps the call, so any socket the pool OPENS here
                    # connects over the allowlisted IPv4 and is then reused —
                    # egress behaviour is unchanged (live-verified 2026-07-15).
                    _prox = _proxies() or None
                    r = _http_session(_prox).request(
                        method, url, headers=self._headers(), json=json_body,
                        params=params, timeout=_HTTP_TIMEOUT,
                        proxies=_prox)
                finally:
                    _urllib3_conn.allowed_gai_family = _orig_gai
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
        # Vortex REQUIRES a `price` on EVERY order — including RL-MKT (market):
        # a market order with no price 400s ("RegularPlaceOrderRequest.Price").
        # live-verified 2026-07-15. For a limit (RL) use the given price; for a
        # market (RL-MKT) use the given price or the current LTP as the required
        # reference (variety=RL-MKT still governs market execution).
        px = price
        if variety == "RL-MKT" and not px:
            px = self.get_ltp(symbol)
        body["price"] = round(float(px or 0.0), 2)
        return body

    # ── Order lifecycle ───────────────────────────────────────────────────────
    async def place_order(self, order) -> OrderResult:
        """Place an entry order. Dry-run safe (zero HTTP). Real placement maps our
        Order → Vortex fields and POSTs /trading/orders/regular."""
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty,
                               raw={"dry_run": True})
        # ITEM 9 — hard-block REAL orders through the UNCERTIFIED rupeezy adapter.
        _blk = self._certification_block(action="entry order", symbol=order.symbol)
        if _blk:
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty, error=_blk)
        # TODO(certify): the Vortex /trading/orders/regular payload has NO documented
        # client-tag / client_order_id field (reference §Orders), so OUR
        # order.client_order_id / compact tag CANNOT be transmitted here yet — do NOT
        # fabricate a field. Confirm the tag field name at certification and thread it
        # into _order_body then (order.client_order_id is available on `order`).
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
            payload = r.json() or {}
            data = payload.get("data") or {}
            # live-verified: a successful place returns data.order_id (which itself
            # CONTAINS a '?', e.g. "NZXAH00003?7") — keep it verbatim.
            oid = data.get("order_id")
            if not oid:
                return OrderResult(status="FAILED", broker_order_id=None,
                                   symbol=order.symbol, qty=order.qty,
                                   error=_place_error(payload))
            return OrderResult(status="PLACED", broker_order_id=str(oid),
                               symbol=order.symbol, qty=order.qty)
        except Exception as e:
            log.error("rupeezy place_order failed for %s: %s", order.symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty, error=str(e))

    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str,
                                kite_product: str | None = None,
                                direction: str = "long",
                                *, exec_cfg=None,
                                client_order_id: str | None = None) -> OrderResult:
        """Flatten one position with a Vortex MARKET order (variety=RL-MKT).

        kite_product: explicit trading-product override (e.g. "MTF", "CNC", "MIS")
        — takes precedence over instrument_type (which is the security type "EQ",
        not the trading product; wrong for MTF sessions). Mirrors the Zerodha
        adapter's kite_product precedence.

        direction: FUTURES cover side — long→SELL (default), short→BUY-to-cover."""
        if not self._live_allowed():
            return OrderResult(status="DRY_RUN", broker_order_id=None,
                               symbol=symbol, qty=qty, raw={"dry_run": True})
        # ITEM 9 — hard-block REAL exits through the UNCERTIFIED rupeezy adapter.
        # TODO(certify): transmit client_order_id/compact tag once the Vortex
        # regular-order tag field is confirmed (no documented field today).
        _blk = self._certification_block(action="exit order", symbol=symbol)
        if _blk:
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty, error=_blk)
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
            payload = r.json() or {}
            data = payload.get("data") or {}
            oid = data.get("order_id")
            if not oid:
                return OrderResult(status="FAILED", broker_order_id=None,
                                   symbol=symbol, qty=qty,
                                   error=_place_error(payload))
            log.info("rupeezy place_market_exit: %s qty=%d product=%s order_id=%s",
                     symbol, qty, self._map_product(product), oid)
            return OrderResult(status="PLACED", broker_order_id=str(oid),
                               symbol=symbol, qty=qty)
        except Exception as e:
            log.error("rupeezy place_market_exit failed for %s: %s", symbol, e)
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty, error=str(e))

    async def get_pending_orders(self) -> List[Any]:
        """Live-still-open orders, NORMALISED to the Kite-shaped dict the
        cross-broker session guards consume (`tradingsymbol`, `transaction_type`,
        `pending_quantity`, `quantity`, `order_id`, `tag`, `status`).

        Two live-verified shape facts drive this (2026-07-15):
          * the order book is `{"status":"success","orders":[...]}` — the rows are
            under `orders`, NOT `data` (reading `data` returned [] → the OVERSELL /
            foreign-order guards in session.py were silently disabled for Rupeezy);
          * a Vortex row names the instrument `symbol` (NOT `tradingsymbol`) and has
            no client-tag field — so without mapping `symbol → tradingsymbol` those
            guards would never match a Rupeezy order even after the key fix.
        Returns [] on any failure (guards then fall back to their primary cancel).
        """
        if not self._access_token():
            return []
        try:
            r = self._request("GET", "/trading/orders")
            r.raise_for_status()
            payload = r.json() or {}
            rows = payload.get("orders")
            if rows is None:
                rows = payload.get("data") or []
            # Vortex "still alive" statuses — a PARTIALLY_EXECUTED order still has a
            # resting pending leg, so it counts as alive.
            alive = {"PENDING", "OPEN", "TRIGGER PENDING", "TRIGGER_PENDING",
                     "PARTIALLY_EXECUTED", "PARTIALLY EXECUTED", "AMO_SUBMITTED"}
            out: List[dict] = []
            for o in (rows or []):
                if not isinstance(o, dict):
                    continue
                if str(o.get("status", "")).upper() not in alive:
                    continue
                out.append({
                    "order_id": o.get("order_id"),
                    # Vortex `symbol` → the `tradingsymbol` the guards match on.
                    "tradingsymbol": o.get("symbol") or o.get("tradingsymbol"),
                    "transaction_type": o.get("transaction_type"),
                    "pending_quantity": o.get("pending_quantity"),
                    "quantity": o.get("total_quantity", o.get("quantity")),
                    "product": o.get("product"),
                    "status": o.get("status"),
                    # No client-tag field on Vortex → attribution is by order_id only.
                    "tag": o.get("tag"),
                })
            return out
        except Exception as e:
            log.warning("rupeezy get_pending_orders failed: %s", e)
            return []

    async def cancel_order(self, order_id: str) -> Any:
        if not self._live_allowed():
            return {"status": "DRY_RUN", "order_id": order_id}
        try:
            r = self._request("DELETE",
                              f"/trading/orders/regular/{_urlquote(str(order_id), safe='')}")
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
            r = self._request("DELETE",
                              f"/trading/orders/regular/{_urlquote(str(order_id), safe='')}")
            r.raise_for_status()
            return True
        except Exception as e:
            log.warning("rupeezy cancel_order_sync failed for %s: %s", order_id, e)
            return False

    def get_order_status(self, order_id: str) -> dict:
        """Fetch an order's status and NORMALISE to the shape the exit_poller /
        entry reconciler expect: {status, filled_quantity, average_price, ...}.

        The exit poller (monitoring/exit_poller.py) + entry reconciler check:
          status == "COMPLETE"                → done (real fill)
          status in ("REJECTED","CANCELLED")  → terminal fail
          anything else                       → STILL PENDING (keep polling)
        so we map Vortex order states onto that Kite-compatible vocabulary. A
        PARTIALLY_EXECUTED order maps to "PARTIAL" (which is NOT COMPLETE → the
        poller keeps waiting) while STILL surfacing the real filled_quantity so a
        partial that DID fill is honoured, never treated as a full or a zero fill.

        Vortex order-book field mapping (live-verified 2026-07-15):
          status           → status (order_status tolerated)
          filled_quantity  → traded_quantity (filled_quantity/filled_qty tolerated)
          average_price    → traded_price   (average_price/avg_price tolerated)
          pending_quantity → pending_quantity
          total_quantity   → total_quantity (quantity tolerated)
        Returns a zero/empty status ({} match) when the order is not yet in the
        book (treated as still-pending)."""
        if self.dry_run:
            return {"status": "COMPLETE", "filled_quantity": 0, "average_price": 0.0,
                    "pending_quantity": 0, "total_quantity": 0, "partial": False}
        try:
            # Read status from the ORDER-BOOK (GET /trading/orders) and match by
            # order_id. Two live-verified reasons NOT to use /trading/orders/{id}:
            # (1) Vortex order ids contain a '?' ('NZXAH00003?7') → the single-order
            # path mis-parses it; (2) even URL-encoded, the single-order endpoint
            # returns a STALE 'PENDING' while the BOOK carries the CURRENT status
            # (EXECUTED). Without this, the entry reconciler never sees the fill →
            # cancels a FILLED order → untracked position. live-verified 2026-07-15.
            r = self._request("GET", "/trading/orders")
            r.raise_for_status()
            payload = r.json() or {}
            orders = payload.get("orders")
            if orders is None:
                orders = payload.get("data") or []
            data = {}
            for _o in (orders or []):
                if isinstance(_o, dict) and str(_o.get("order_id")) == str(order_id):
                    data = _o
                    break
            raw_status = str(data.get("order_status")
                             or data.get("status") or "").upper()
            status = _normalise_status(raw_status)
            filled = (data.get("traded_quantity")
                      if data.get("traded_quantity") is not None
                      else data.get("filled_quantity")
                      if data.get("filled_quantity") is not None
                      else data.get("filled_qty") or 0)
            avg = (data.get("traded_price")
                   if data.get("traded_price") is not None
                   else data.get("average_price")
                   if data.get("average_price") is not None
                   else data.get("avg_price") or 0.0)
            filled_i = int(filled or 0)
            total_i = int(data.get("total_quantity")
                          if data.get("total_quantity") is not None
                          else data.get("quantity") or 0)
            _pend = data.get("pending_quantity")
            pending_i = (int(_pend) if _pend is not None
                         else max(total_i - filled_i, 0))
            # PARTIAL when Vortex says so, OR when a fill is in-progress (0 < filled
            # < total) but the book has not yet reported a terminal state. A
            # COMPLETE/EXECUTED order (filled == total) is NOT partial.
            partial = (status == "PARTIAL"
                       or (status not in ("COMPLETE", "REJECTED", "CANCELLED")
                           and 0 < filled_i and total_i > 0 and filled_i < total_i))
            return {"status": status,
                    "filled_quantity": filled_i,
                    "average_price": float(avg or 0.0),
                    "pending_quantity": pending_i,
                    "total_quantity": total_i,
                    "partial": bool(partial),
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
            # CERTIFIED 2026-07-15 (live-verified): the quotes endpoint is PLURAL
            # `/data/quotes`, keyed by `q=<exchange>-<token>` and REQUIRES
            # `mode=ltp` (without it → 200 {"message":"invalid mode"}). The
            # response is {"status":"success","data":{"<exch>-<token>":
            # {"last_trade_price": <px>}}}. Bearer auth only (no x-api-key).
            ident = f"NSE_EQ-{token}"
            r = self._request("GET", "/data/quotes",
                              params={"q": ident, "mode": "ltp"})
            r.raise_for_status()
            row = ((r.json() or {}).get("data") or {}).get(ident) or {}
            ltp = row.get("last_trade_price") or row.get("ltp") \
                or row.get("last_price")
            if ltp:
                return float(ltp)
        except Exception as e:
            log.warning("rupeezy get_ltp failed for %s: %s", symbol, e)
        return None

    # get_ltps_batch inherits the base loop over get_ltp (safe None fallback).

    def available_margin(self):
        """Free equity margin (₹) the RMS pre-trade gate sizes against.
        CERTIFIED 2026-07-15 (live-verified): GET /user/funds returns per-segment
        blocks (exchange_combined / nse / mcx), each with `net_available`,
        `total_trading_power`, `withdrawable_balance`. We use `net_available` off
        the combined view (fall back to nse). Returns None on ANY failure so the
        RMS fails-closed (never over-deploys on an unknown budget)."""
        if not self._access_token():
            return None
        try:
            r = self._request("GET", "/user/funds")
            r.raise_for_status()
            payload = r.json() or {}
            # live-verified: the per-segment blocks are at the TOP level. Some
            # deployments wrap them under `data` — check top level first (verified),
            # then the wrapper, so neither shape silently yields None.
            for root in (payload, payload.get("data") or {}):
                if not isinstance(root, dict):
                    continue
                for seg in ("exchange_combined", "nse"):
                    blk = root.get(seg) or {}
                    v = blk.get("net_available")
                    if isinstance(v, (int, float)) and not isinstance(v, bool) \
                            and v >= 0:
                        return float(v)
        except Exception as e:
            log.warning("rupeezy available_margin failed: %s", e)
        return None

    # ── Margin / leverage (equity MIS/MTF sizing) ─────────────────────────────
    def _margin_probe(self, symbol: str, prod_vortex: str,
                      ltp: float) -> Optional[float]:
        """POST a qty=1 order-margin probe and return the per-share margin ₹
        (qty=1 → the required margin IS the per-share figure), or None on any
        failure / implausible value. Cached per (symbol, product).

        SAFETY: any error, a missing margin field, or a value that exceeds the
        full cash price (which would imply leverage < 1x → a mis-parse) returns
        None so the caller cash-falls-back (never over-deploys)."""
        ckey = (symbol.upper(), prod_vortex)
        now = time.time()
        hit = _margin_cache.get(ckey)
        if hit and (now - hit[1]) < _MARGIN_TTL_SEC:
            return hit[0]
        if not ltp or ltp <= 0:
            return None
        try:
            seg = self._map_exchange("NSE")
            token = self._resolve_token(symbol, seg)
        except Exception as e:
            log.debug("rupeezy margin token miss for %s: %s", symbol, e)
            return None
        # TODO(certify): exact order-margin request/response shape (#4). We send
        # the same order fields as a real qty=1 order; unknown extra fields are
        # harmless and a wrong shape simply 4xx's → None → cash fallback.
        body = {
            "exchange": seg,
            "token": token,
            "transaction_type": "BUY",
            "product": prod_vortex,
            "variety": "RL-MKT",
            "quantity": 1,
            "price": round(float(ltp), 2),
            "old_quantity": 0,
            "old_price": 0.0,
            "mode": "NEW",
        }
        try:
            r = self._request("POST", _margin_path(), json_body=body)
            r.raise_for_status()
            mps = _extract_margin(r.json() or {})
        except Exception as e:
            log.warning("rupeezy margin probe failed for %s (%s) — cash fallback",
                        symbol, e)
            return None
        if mps is None or mps <= 0:
            return None
        # Per-share margin must be <= full cash price (leverage >= 1x). A larger
        # value means the wrong field was parsed (e.g. account-level margin) —
        # refuse it rather than mis-size. Small epsilon absorbs charges/rounding.
        if mps > float(ltp) * 1.05:
            log.warning("rupeezy margin %.2f > LTP %.2f for %s — refusing "
                        "(cash fallback)", mps, ltp, symbol)
            return None
        _margin_cache[ckey] = (float(mps), now)
        return float(mps)

    def get_margin_per_share(self, symbol: str,
                             product: str = "MTF") -> Optional[float]:
        """Per-share margin Vortex locks for `product`, so MIS/MTF equity sizes
        off the broker's REAL intraday/delivery leverage (qty = budget /
        margin_per_share) — the same contract the Zerodha adapter fulfils via
        kite.order_margins. Cash-equivalent products (CNC/DELIVERY/NRML) return
        None by design (1x → the caller cash-sizes on LTP).

        Returns None on ANY failure → caller falls back to cash sizing (never
        over-deploys). TODO(certify): the Vortex order-margin endpoint path +
        field names (reference CONFIRM #4); the path is env-overridable via
        RUPEEZY_MARGIN_PATH and the parser accepts the common spellings, so until
        a live response is confirmed this safely returns None (cash sizing)."""
        if not self._access_token():
            return None
        prod = self._map_product(product)
        if prod not in ("INTRADAY", "MTF"):
            return None  # cash product — 1x, size on LTP
        return self._margin_probe(symbol, prod, self.get_ltp(symbol) or 0.0)

    def get_margins_batch(self, symbols: List[str],
                          product: str = "MTF") -> dict:
        """{symbol: per_share_margin} for the whole pick list. Reuses ONE batched
        LTP pass (base get_ltps_batch) then a cached per-symbol margin probe, so
        the hot preview path re-fetches no prices. Symbols whose margin is
        unavailable are ABSENT (caller cash-falls-back per symbol)."""
        if not symbols or not self._access_token():
            return {}
        prod = self._map_product(product)
        if prod not in ("INTRADAY", "MTF"):
            return {}
        ltps = self.get_ltps_batch(list(symbols))
        targets = [(s, float(ltps[s])) for s in symbols
                   if ltps.get(s) and ltps[s] > 0]
        out: Dict[str, float] = {}
        if not targets:
            return out
        # Vortex has no basket-margin endpoint, so each symbol needs its OWN
        # order-margin probe. Run them CONCURRENTLY (the requests.Session is
        # pooled/thread-safe and the per-(symbol,product) cache write is atomic)
        # instead of one-at-a-time — a 15-name basket costs ~1 round-trip of
        # wall-time, not 15. Probes are independent + fail-soft (None -> cash
        # fallback), so a slow/failing symbol can't block or corrupt the rest.
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=min(12, len(targets))) as ex:
            futs = {ex.submit(self._margin_probe, s, prod, ltp): s
                    for s, ltp in targets}
            for f in as_completed(futs):
                s = futs[f]
                try:
                    m = f.result()
                except Exception:
                    m = None
                if m and m > 0:
                    out[s] = float(m)
        return out

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

    def get_positions_net(self):
        """Best-effort map of get_positions() to the Kite-shaped net book the
        AUTHORITATIVE reconciler consumes, or None when unavailable.

        SAFETY: None is the "do nothing" sentinel. We return None in paper /
        live-disabled and on ANY error, so an unreachable Vortex book can never
        flatten our DB. TODO(certify): the exact Vortex position field names —
        this maps a plausible shape; if a row can't be mapped it is skipped, and
        if the whole fetch is empty/unavailable we return None rather than [] so
        the reconciler treats an uncertified/empty response as "unknown, do
        nothing" (conservative — never mass-close on Rupeezy until certified)."""
        if not self._live_allowed():
            return None
        try:
            raw = self.get_positions()
        except Exception as e:  # pragma: no cover - defensive
            log.warning("rupeezy get_positions_net fetch failed: %s", e)
            return None
        if not raw:
            # Empty / unavailable → unknown (None), NOT an authoritative flat book.
            return None
        out: List[dict] = []
        for r in raw:
            if not isinstance(r, dict):
                continue
            # live-verified: a Vortex net row names the instrument `symbol` (bare).
            sym = r.get("tradingsymbol") or r.get("trading_symbol") \
                or r.get("symbol") or r.get("token")
            if not sym:
                continue
            buy_q = r.get("buy_quantity", r.get("buy_qty"))
            sell_q = r.get("sell_quantity", r.get("sell_qty"))
            # PRODUCT-AWARE held: DELIVERY carries the position in buy/sell with
            # quantity=0; INTRADAY populates quantity. _held_from_row picks the
            # right source so the reconciler's Kite-shaped `quantity` reflects the
            # TRUE held net (a held CNC buy → +N, not 0). live-verified 2026-07-15.
            qty = _held_from_row(r)
            out.append({
                "tradingsymbol": str(sym),
                # NORMALISE to Kite vocabulary so the reconciler/alert matchers
                # accept the row (NSE_EQ→NSE, DELIVERY→CNC). See the reverse maps.
                "exchange": _vortex_exchange_to_kite(
                    r.get("exchange") or r.get("exchange_segment")),
                "quantity": qty,
                "buy_quantity": buy_q,
                "sell_quantity": sell_q,
                "buy_price": r.get("buy_price", r.get("buy_average")),
                "sell_price": r.get("sell_price", r.get("sell_average")),
                "average_price": r.get("average_price", r.get("avg_price")),
                "pnl": r.get("pnl", r.get("net_pnl", r.get("value"))),
                "product": _vortex_product_to_kite(
                    r.get("product") or r.get("product_type")),
            })
        return out or None

    def get_net_position_qty(self, symbol: str,
                             instrument_type: str = "EQ"):
        """Signed net quantity Vortex currently holds for `symbol`, or None when
        not live. PRE-EXIT reconciliation guard (parity with the Zerodha adapter).

        REAL-MONEY FAIL-SAFE (2026-07-10 BRIGADE double-cover, ported here): a
        genuine broker/transport error MUST re-raise — NOT be swallowed to None.
        None is the PAPER / not-live sentinel; the caller treats None as "no book
        to reconcile, proceed with the exit". If we swallowed a ConnectionReset to
        None the caller would place a BLIND exit (a short's buy-to-cover doubling
        into a naked long). So: paper/not-live → None (unchanged); a genuine flat
        book → 0; a real fetch error → RAISE so the caller's guard ABORTS the exit
        and retries next tick.

        This deliberately does NOT reuse get_positions() / get_positions_net()
        (both swallow errors to []/None — correct for the reconciler's conservative
        'do nothing on unknown', but WRONG here where an error must abort the exit).
        TODO(certify): the exact Vortex positions field names are still unverified
        (same caveat as get_positions_net); the RE-RAISE-on-error property holds
        regardless of field mapping, so the safety guarantee is certification-
        independent — only the returned qty on the success path awaits cert."""
        if not self._live_allowed():
            return None
        # No try/except around the fetch: a genuine transport/HTTP error MUST
        # propagate (that is the whole point of the fail-safe).
        return self._net_qty_match(self._fetch_net_rows(), symbol,
                                   instrument_type)

    def _fetch_net_rows(self) -> List[dict]:
        """ONE Vortex round trip → the raw net position rows. Deliberately has NO
        try/except: a genuine transport/HTTP error MUST propagate (the BRIGADE
        fail-safe). Extracted so the per-leg and batched probes share one fetch.
        Deliberately NOT get_positions()/get_positions_net() — both swallow errors
        to []/None, which the pre-exit guard would misread as 'proceed'."""
        r = self._request("GET", "/trading/portfolio/positions")
        r.raise_for_status()
        data = (r.json() or {}).get("data")
        if isinstance(data, dict):
            data = data.get("net") or data.get("positions") or []
        return list(data or [])

    def _net_qty_match(self, net_rows, symbol: str,
                       instrument_type: str = "EQ") -> int:
        """Pure matcher over already-fetched Vortex net rows. NO I/O. SINGLE
        source of truth for the match semantics, shared by get_net_position_qty()
        (per-leg) and net_qty_from_book() (batched) so the two can NEVER diverge.
        NOTE (parity with today): Vortex rows are matched on the BARE symbol only
        — `instrument_type` is accepted for signature parity and intentionally
        unused, exactly as the per-leg probe has always behaved."""
        for row in list(net_rows or []):
            if not isinstance(row, dict):
                continue
            row_sym = (row.get("tradingsymbol") or row.get("trading_symbol")
                       or row.get("symbol") or row.get("token"))
            if str(row_sym) != str(symbol):
                continue
            # PRODUCT-AWARE held (parity with get_positions_net): a DELIVERY (CNC)
            # row reports quantity=0 with the position in buy_quantity/sell_quantity;
            # only MIS/INTRADAY populate the signed `quantity`. _held_from_row reads
            # buy−sell for the former and the signed `quantity` for the latter — so a
            # genuinely-held same-day CNC buy returns +N (not 0). This is the
            # positions.net view ONLY (Kite parity — get_net_position_qty does NOT
            # consult holdings; a settled/overnight CNC that has moved to holdings
            # reads 0 here exactly as Zerodha does, and the RECONCILER — which DOES
            # add holdings — is what tracks the carried BTST leg). live-verified
            # 2026-07-15: MAPMYINDIA DELIVERY net quantity=0, buy=2, sell=2 → 0.
            return _held_from_row(row)
        return 0  # book retrieved, symbol absent → flat at the broker

    def fetch_net_position_book(self):
        """Batched pre-exit probe (see BrokerAdapter.fetch_net_position_book).

        Paper / not-live → None (NO round trip). Live → the raw Vortex net rows
        in ONE round trip. A genuine transport/HTTP error RAISES — deliberately
        NOT swallowed to None like get_positions_net() does, because None is the
        paper sentinel and a caller reading it as 'no book, proceed' would place
        a BLIND exit (the 2026-07-10 BRIGADE double-cover class).

        This is what turns an N-leg Vortex kill from N×~4.8s of serial probing
        into ONE round trip; the per-leg probe remains the fallback."""
        if not self._live_allowed():
            return None
        return self._fetch_net_rows()

    def net_qty_from_book(self, book, symbol: str,
                          instrument_type: str = "EQ"):
        """Answer the pre-exit probe for `symbol` from an already-fetched book.
        Identical semantics to get_net_position_qty() by construction (same
        _net_qty_match). None → the book can't answer → the caller falls back to
        the per-leg probe. Never raises (pure, defensive)."""
        if book is None:
            return None
        try:
            return self._net_qty_match(book, symbol, instrument_type)
        except Exception as e:  # pragma: no cover - defensive
            log.warning("rupeezy net_qty_from_book match failed for %s: %s — "
                        "falling back to the per-leg probe", symbol, e)
            return None

    def get_holdings(self) -> List[dict]:
        """Delivery holdings, NORMALISED to the Kite-shaped rows the reconciler
        consumes (`tradingsymbol`, `quantity`, `t1_quantity`, `product`).

        Why normalise (live-verified 2026-07-15 shape):
          {"status":"Success","data":[{isin, nse:{token,symbol,...}, bse:{...},
           total_free, dp_free, pool_free, t1_quantity, average_price, last_price,
           product:"DELIVERY", ...}]}
        The symbol lives under `nse.symbol` (NOT a flat `tradingsymbol`) and the
        held quantity under `total_free` (settled/free demat) + `t1_quantity`
        (T+1, bought-not-yet-delivered) — NOT a flat `quantity`. Un-normalised, the
        reconciler read `tradingsymbol`→None and `quantity`→0, so a genuinely-held
        (settled / overnight BTST) CNC contributed ZERO to broker_held → a false
        deficit. We emit `quantity`=total_free and `t1_quantity`=t1 so the
        reconciler's `quantity + t1_quantity` = the true held (the SAME contract as
        Kite holdings). The two are disjoint in time (a share is EITHER settled-free
        OR T+1, never both), so the sum never double-counts.

        NOTE(certify): confirmed against an ALL-ZERO live book (today's shares had
        round-tripped flat). The total_free/t1_quantity non-overlap awaits one
        NON-zero held-holdings observation; a mis-estimate here fails SAFE — an
        over-count reads as a broker SURPLUS (invisible, no close), an under-count
        as a deficit with NO same-day sell evidence (alert at most, never a close).
        Returns [] on any failure. `nse`/`bse`/`isin` are preserved on each row."""
        if not self._access_token():
            return []
        try:
            r = self._request("GET", "/trading/portfolio/holdings")
            r.raise_for_status()
            data = (r.json() or {}).get("data") or []
        except Exception as e:
            log.warning("rupeezy get_holdings failed: %s", e)
            return []
        out: List[dict] = []
        for h in (data or []):
            if not isinstance(h, dict):
                continue
            nse = h.get("nse") if isinstance(h.get("nse"), dict) else {}
            bse = h.get("bse") if isinstance(h.get("bse"), dict) else {}
            sym = (h.get("tradingsymbol") or nse.get("symbol")
                   or bse.get("symbol") or h.get("symbol"))
            if not sym:
                continue
            exch = "NSE" if nse.get("symbol") else ("BSE" if bse.get("symbol")
                                                    else None)
            settled = _to_int(h.get("total_free", h.get("dp_free")))
            t1 = _to_int(h.get("t1_quantity"))
            row = dict(h)  # preserve original fields (isin/nse/bse/last_price/…)
            row.update({
                "tradingsymbol": str(sym),
                "exchange": exch,
                "token": nse.get("token") or bse.get("token") or h.get("token"),
                "quantity": settled,
                "t1_quantity": t1,
                "average_price": h.get("average_price", h.get("avg_price")),
                "product": _vortex_product_to_kite(h.get("product")) or "CNC",
            })
            out.append(row)
        return out

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


def _place_error(payload) -> str:
    """Best-effort human message from a Vortex order response that carried no
    order_id — surface Vortex's OWN reason (message / error_reason / status) so a
    live-order failure is diagnosable, instead of an opaque 'no order_id'."""
    if not isinstance(payload, dict):
        return f"no order_id in response ({payload})"
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    for src in (data, payload):
        for k in ("error_reason", "message", "error", "errorMessage", "reason"):
            v = src.get(k)
            if v:
                return f"rupeezy rejected the order — {v}"
    return f"no order_id in response ({payload.get('data') or payload})"


def _margin_path() -> str:
    """Vortex order-margin endpoint path. CERTIFIED 2026-07-15 (live-verified):
    POST /trading/margins/order → {"required_margin": <per-order ₹>,
    "available_margin": ..., "status":"success"}. Env-overridable."""
    return os.environ.get("RUPEEZY_MARGIN_PATH", "").strip() or "/trading/margins/order"


def _extract_margin(payload) -> Optional[float]:
    """Pull the required per-order margin (₹) from a Vortex margin response,
    tolerant of the field spelling (TODO(certify) exact name — #4). Accepts a
    bare object, a {'data': {...}} wrapper, or a single-row list, and a nested
    {'total': ...} value. Returns None when no positive margin field is found."""
    data = payload
    if isinstance(data, dict) and "data" in data:
        data = data.get("data")
    if isinstance(data, list):
        data = data[0] if data else {}
    if not isinstance(data, dict):
        return None
    keys = ("required_margin", "total_margin", "margin_required", "final_margin",
            "initial_margin", "total", "margin")
    for k in keys:
        v = data.get(k)
        if isinstance(v, bool):
            continue
        if isinstance(v, (int, float)) and v > 0:
            return float(v)
        if isinstance(v, dict):
            for kk in ("total", "required", "value", "amount"):
                vv = v.get(kk)
                if isinstance(vv, (int, float)) and not isinstance(vv, bool) \
                        and vv > 0:
                    return float(vv)
    return None


# Vortex order state → exit_poller (Kite-compatible) vocabulary. live-verified
# states 2026-07-15: "EXECUTED" (fully filled), "PENDING" (initial). The rest are
# the documented Vortex enum; anything unmapped passes through and is treated as
# still-pending by the poller (COMPLETE / REJECTED / CANCELLED / else = pending).
_STATUS_MAP = {
    # terminal — filled
    "COMPLETE": "COMPLETE",
    "COMPLETED": "COMPLETE",
    "EXECUTED": "COMPLETE",
    "FILLED": "COMPLETE",
    "TRADED": "COMPLETE",
    # terminal — rejected
    "REJECTED": "REJECTED",
    "REJECT": "REJECTED",
    # terminal — cancelled
    "CANCELLED": "CANCELLED",
    "CANCELED": "CANCELLED",
    # in-progress partial → NOT terminal; poller keeps waiting, but we still
    # surface the real filled_quantity so the partial fill is honoured.
    "PARTIALLY_EXECUTED": "PARTIAL",
    "PARTIALLY EXECUTED": "PARTIAL",
    "PARTIAL": "PARTIAL",
}


def _normalise_status(raw: str) -> str:
    """Map a Vortex order status to the exit_poller vocabulary. Unknown/in-flight
    states (PENDING / OPEN / TRIGGER_PENDING / AMO_SUBMITTED …) pass through
    unchanged — the poller treats anything that is not COMPLETE / REJECTED /
    CANCELLED as still-pending, so no explicit mapping is needed for them."""
    return _STATUS_MAP.get((raw or "").upper(), (raw or "").upper())
