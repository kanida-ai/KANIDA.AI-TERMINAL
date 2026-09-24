r"""Global Datafeeds (GDF) WebSocket adapter — provider id ``gdf``.

Turning it on is configuration, like every provider here::

    MARKET_DATA_PROVIDER=gdf          # or get_provider("gdf") explicitly
    # market_data/.env  (git-ignored; read into a private dict, never os.environ, never logged)
    GDF_API_KEY=...
    GDF_WS_URL=wss://<host>:<port>/

=============================================================================
WHAT WAS MEASURED ON THE LIVE TRIAL KEY, 21 SEP 2026 (not assumed)
=============================================================================
* Transport: WebSocket, JSON text frames. ``Authenticate`` -> ``AuthenticateResult {"Complete": true}``. The server
  then pushes ``AllowVMRunningResult``, ``AllowServerOSRunningResult`` and an ``Echo`` heartbeat about once a second;
  replies are therefore matched by type, never assumed to be "the next frame".
* **One session per key.** A second connection is refused ("Key already in use"): this provider holds exactly one
  connection per process, behind a lock.
* Functions enabled on the trial: ``GetHistory``, ``GetSnapshot``, ``GetExchangeSnapshot``, ``SubscribeSnapshot``
  (+ metadata: ``GetLimitation``, ``GetExchanges``, ``GetInstruments``). ``GetLastQuote*`` and ``SubscribeRealtime``
  answer "Function not enabled." — there is no live quote on this plan, only 15-minute bars.
* Limits (``GetLimitation``): 3,600 calls/hour; 100 NFO, 95 NSE, 5 NSE_IDX instruments; ``DataDelay`` 900 s;
  trial expiry 23 Sep 2026 23:59:59 IST. Both ``AllowVMRunning`` and ``AllowServerOSRunning`` are **false**.
* **Timestamps are epoch seconds marking the bar START.** Pinned against Kite's stored readings: GDF's NIFTY-I bar
  "15:30" (close 23,450.0, OI 17,242,810) is exactly the reading Kite captured at 15:45; its "15:15" bar matches the
  15:30 reading. So ``bar_start = LastTradeTime`` and a KANIDA reading at T corresponds to the bar starting T - 15m.
* **After-hours filler bars exist**: NSE_IDX "NIFTY 50" returned bars at 20:45, 21:00, 21:15 IST at one flat price.
  They are flagged OUTSIDE_SESSION here and must never be stored as market data.
* ``GetHistory`` rows carry ``OpenInterest`` and ``QuotationLot`` per bar; ``RawCandle`` has no OI column, so
  ``history()`` returns the raw rows with OI for F&O callers, and ``candles()`` serves the equity contract.

Identifiers: NSE equities plain (``RELIANCE``); indices as NSE_IDX names (``NIFTY 50``, ``NIFTY BANK``); NFO long form
``OPTIDX_NIFTY_22SEP2026_CE_23400`` / ``FUTIDX_NIFTY_29SEP2026_XX_0`` / continuous ``NIFTY-I``. ``gdf_identifier``
builds the long form from contract fields, so no KANIDA call site has to know the vendor's spelling.
"""
from __future__ import annotations

import json
import logging
import socket
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .provider import BaseProvider, register_provider
from .ratelimit import get_limiter
from .types import (
    IST,
    Instrument,
    ProviderError,
    QualityFlag,
    RawCandle,
    TokenError,
    bar_end_for,
    basic_quality_flags,
    in_session,
    is_on_grid,
    session_close,
    session_open,
    utc_now,
)
from .wsclient import WebSocket, WebSocketError

log = logging.getLogger("market_data.gdf")

ENV_FILE = Path(__file__).resolve().parent / ".env"
INDEX_NAMES = {"NIFTY": "NIFTY 50", "BANKNIFTY": "NIFTY BANK", "FINNIFTY": "NIFTY FIN SERVICE",
               "MIDCPNIFTY": "NIFTY MID SELECT", "NIFTYNXT50": "NIFTY NEXT 50"}
# Index F&O underlyings: they take the OPTIDX / FUTIDX family. NIFTYFPI is one (verified against the vendor's own
# GetInstruments, 22 Sep: 1,084 OPTIDX contracts, 0 OPTSTK). Its NSE_IDX spot name is NOT mapped: the vendor lists
# "NIFTY FPI 150", and that it is the same underlying is not yet confirmed - so it is not guessed.
INDEX_UNDERLYINGS = set(INDEX_NAMES) | {"NIFTYFPI"}
_MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
PERIODICITY = {"15minute": ("MINUTE", 15), "day": ("DAY", 1)}


def _read_env() -> Dict[str, str]:
    """``market_data/.env`` into a PRIVATE dict: the key never enters os.environ, a log line or an error message."""
    out: Dict[str, str] = {}
    try:
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    except OSError:
        pass
    return out


def gdf_identifier(underlying: str, instrument_type: str, expiry: Optional[date] = None,
                   strike: Optional[float] = None) -> str:
    """KANIDA contract fields -> the vendor's LONG identifier.

    ``instrument_type`` is CE / PE / FUT (the store's own spelling). Index vs stock is decided by the underlying.
    """
    u = underlying.strip().upper()
    kind = instrument_type.strip().upper()
    if kind in ("EQ", "IDX", "INDEX"):
        return INDEX_NAMES.get(u, u)
    if expiry is None:
        raise ValueError("an F&O identifier needs its expiry")
    d = f"{expiry.day:02d}{_MONTHS[expiry.month - 1]}{expiry.year}"
    family = "IDX" if u in INDEX_UNDERLYINGS else "STK"
    if kind == "FUT":
        return f"FUT{family}_{u}_{d}_XX_0"
    if kind not in ("CE", "PE") or strike is None:
        raise ValueError(f"unsupported contract {kind} / strike {strike}")
    k = int(strike) if float(strike).is_integer() else float(strike)
    return f"OPT{family}_{u}_{d}_{kind}_{k}"


def exchange_of(identifier: str) -> str:
    s = identifier.strip().upper()
    if s.startswith(("OPTIDX_", "OPTSTK_", "FUTIDX_", "FUTSTK_")) or s.endswith(("-I", "-II", "-III")):
        return "NFO"
    if s in {v.upper() for v in INDEX_NAMES.values()}:
        return "NSE_IDX"
    return "NSE"


class GdfProvider(BaseProvider):
    """GDF over one authenticated WebSocket. 15-minute bars, delayed 900 s by design."""

    provider_id = "gdf"
    # measured: the trial enables MINUTE 1 and 15 only ("DAY" and "HOUR": "Selected periodicity or period
    # disabled"). Daily bars come from aggregating 15-minute ones, which the pipeline already does.
    supported_timeframes = ("15minute",)
    adjustment_basis_id_default = "gdf-adjustsplits-true"

    def __init__(self, *, url: Optional[str] = None, api_key: Optional[str] = None, socket_factory=None,
                 reply_timeout: float = 20.0):
        env = _read_env()
        self._url = url or env.get("GDF_WS_URL", "")
        self._api_key = api_key if api_key is not None else env.get("GDF_API_KEY", "")
        self._socket_factory = socket_factory or (lambda u: WebSocket(u, timeout=5.0))
        self.reply_timeout = reply_timeout
        self.delay_seconds = 900
        # 3,600 calls/hour on the trial: one a second, for the whole key
        self.rate_limit_per_second = 1.0
        self.max_days_per_request = {"15minute": 5}
        self.adjustment_basis_id = self.adjustment_basis_id_default
        self.limiter = get_limiter("gdf", self.rate_limit_per_second)
        self._ws = None
        self._lock = threading.RLock()
        self.requests_made = 0
        self.last_timing: Dict[str, Any] = {}
        self.permissions: Dict[str, Any] = {}
        self._instruments: Optional[List[Instrument]] = None

    # --- session -------------------------------------------------------------------------------------------------
    def _connect(self):
        if not self._url or not self._api_key:
            raise TokenError("gdf: GDF_WS_URL / GDF_API_KEY are not configured in market_data/.env",
                             provider_id="gdf")
        ws = self._socket_factory(self._url).connect()
        ws.send(json.dumps({"MessageType": "Authenticate", "Password": self._api_key}))
        got = self._await(ws, lambda j: j.get("MessageType") == "AuthenticateResult")
        if not got.get("Complete"):
            try:
                ws.close()
            finally:
                raise TokenError(f"gdf: authentication refused ({got.get('Message', '')})", provider_id="gdf")
        self._ws = ws
        return ws

    def _await(self, ws, match, timeout: Optional[float] = None) -> dict:
        """The first JSON frame that `match`es. Heartbeats and permission pushes are recorded, never mistaken for
        the answer; a RequestError is raised, not returned."""
        end = time.monotonic() + (timeout or self.reply_timeout)
        while time.monotonic() < end:
            try:
                raw = ws.recv()
            except socket.timeout:
                continue
            try:
                j = json.loads(raw)
            except ValueError:
                # the API sends plain-text diagnostics (an expired or duplicated key, for example)
                raise ProviderError(f"gdf: {raw.strip()[:160]}", provider_id="gdf")
            kind = j.get("MessageType", "")
            if kind == "Echo":
                continue
            if kind in ("AllowVMRunningResult", "AllowServerOSRunningResult"):
                self.permissions[kind] = j
                continue
            if kind == "RequestError":
                raise ProviderError(f"gdf: {j.get('Message', 'request error')}", provider_id="gdf")
            if match(j):
                return j
        raise ProviderError("gdf: no reply within the timeout", provider_id="gdf")

    def call(self, request: dict, match) -> dict:
        """One request, one matched reply, on the single session — reconnecting once if the socket died."""
        with self._lock:
            for attempt in (1, 2):
                try:
                    ws = self._ws or self._connect()
                    self.limiter.wait()
                    ws.send(json.dumps(request))
                    self.requests_made += 1
                    sent = time.monotonic()
                    reply = self._await(ws, match)
                    done = time.monotonic()
                    first = getattr(ws, 'msg_first', 0.0) or done
                    self.last_timing = {'ttfb_ms': int((first - sent) * 1000), 'transfer_ms': int((done - first) * 1000),
                                        'bytes': getattr(ws, 'msg_bytes', 0)}
                    return reply
                except (WebSocketError, OSError) as error:
                    self.close()
                    if attempt == 2:
                        raise ProviderError(f"gdf: connection lost ({type(error).__name__})", provider_id="gdf")

    def close(self):
        with self._lock:
            if self._ws is not None:
                try:
                    self._ws.close()
                finally:
                    self._ws = None

    # --- metadata -----------------------------------------------------------------------------------------------
    def limitation(self) -> dict:
        return self.call({"MessageType": "GetLimitation"}, lambda j: j.get("MessageType") == "LimitationResult")

    def instruments(self, refresh: bool = False, *, exchange: str = "NSE", **filters) -> List[Instrument]:
        if self._instruments is not None and not refresh and not filters and exchange == "NSE":
            return self._instruments
        req = {"MessageType": "GetInstruments", "Exchange": exchange, **filters}
        rows = self.call(req, lambda j: "Result" in j and "Request" in j).get("Result") or []
        out = []
        for r in rows:
            expiry = None
            if r.get("Expiry"):
                try:
                    expiry = datetime.strptime(r["Expiry"], "%d%b%Y").date()
                except ValueError:
                    pass
            out.append(Instrument(
                instrument_id=str(r.get("Identifier") or r.get("TradeSymbol") or ""),
                symbol=str(r.get("TradeSymbol") or r.get("Identifier") or ""),
                exchange=exchange, name=str(r.get("Product") or ""),
                instrument_type=str(r.get("OptionType") or r.get("Name") or "EQ"),
                lot_size=int(float(r.get("QuotationLot") or 1)), expiry=expiry, vendor_id="gdf"))
        if exchange == "NSE" and not filters:
            self._instruments = out
        return out

    # --- history ------------------------------------------------------------------------------------------------
    def history(self, identifier: str, timeframe: str, start: datetime, end: datetime,
                exchange: Optional[str] = None, max_rows: int = 0) -> List[dict]:
        """Raw GetHistory rows, OLDEST FIRST, WITH open interest. ``bar_start`` (IST) is added to each row."""
        periodicity, period = PERIODICITY[timeframe]
        req = {"MessageType": "GetHistory", "Exchange": exchange or exchange_of(identifier),
               "InstrumentIdentifier": identifier, "Periodicity": periodicity, "Period": period,
               "From": int(start.timestamp()), "To": int(end.timestamp())}
        if max_rows:
            req["Max"] = max_rows
        rows = self.call(req, lambda j: "Result" in j and "Request" in j
                         and (j["Request"] or {}).get("InstrumentIdentifier") == identifier).get("Result") or []
        for r in rows:
            r["bar_start"] = datetime.fromtimestamp(int(r["LastTradeTime"]), tz=timezone.utc).astimezone(IST)
        rows.sort(key=lambda r: r["bar_start"])
        return rows

    def _fetch_window(self, symbol: str, timeframe: str, start_day: date, end_day: date):
        start = session_open(start_day)
        end = session_close(end_day)
        rows = self.history(symbol, timeframe, start, end)
        return self._normalize(rows, symbol, timeframe)

    def _normalize(self, rows: List[dict], symbol: str, timeframe: str) -> List[RawCandle]:
        fetched_at = utc_now()
        cutoff = fetched_at.timestamp() - self.delay_seconds
        out: List[RawCandle] = []
        for r in rows:
            ts = r["bar_start"]
            if timeframe == "day":
                bar_start, bar_end = session_open(ts.date()), session_close(ts.date())
            else:
                bar_start = ts                              # measured: GDF stamps the bar START
                bar_end = bar_end_for(bar_start, timeframe)
            o, h, l, c = (float(r.get(k) or 0) for k in ("Open", "High", "Low", "Close"))
            vol = int(r.get("TradedQty") or 0)
            flags = basic_quality_flags(o, h, l, c, vol)
            if not is_on_grid(bar_start, timeframe):
                flags |= QualityFlag.OFF_GRID
            if timeframe != 'day' and not in_session(bar_start, timeframe):
                # NOT RETURNED. The candle contract is session bars only (the conformance suite enforces it): the
                # 15:30-start post-close bar and the after-hours filler bars seen on NSE_IDX (20:45-21:15, one flat
                # price) are not market bars. history() still serves them raw, for F&O callers who map the 15:30 bar
                # to the 15:45 post-close reading on purpose.
                continue
            complete = bar_end.timestamp() <= cutoff
            if not complete:
                flags |= QualityFlag.PARTIAL_BAR
            out.append(RawCandle(
                instrument_id=symbol, symbol=symbol, exchange=exchange_of(symbol), timeframe=timeframe,
                bar_start=bar_start, bar_end=bar_end, open=o, high=h, low=l, close=c, volume=vol,
                candle_complete=complete, quality_flags=flags, adjustment_basis_id=self.adjustment_basis_id,
                vendor_id="gdf", vendor_revision="", fetched_at=fetched_at))
        return out


register_provider("gdf", lambda **kw: GdfProvider(**kw))
