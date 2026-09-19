"""A deterministic, offline stand-in for the Kite client (tests only).

It mimics exactly the three calls the derivatives pipeline makes —
``instruments('NFO')``, ``quote(tokens)`` and ``historical_data(token, …,
oi=True)`` — through the same guarded ``_call`` path the real provider exposes,
so the tests exercise the production code, not a parallel one.

It is obviously synthetic: prices come from a seeded hash, ``vendor_id`` stays
``kite`` only because the rows are never written anywhere but a temporary
database in the test.
"""
from __future__ import annotations

import hashlib
from datetime import date, datetime, time as _dtime, timedelta
from typing import Iterable, Mapping, Sequence

from .capture import marks_for
from .instruments import INDEX_SPOT

IST = timedelta(hours=5, minutes=30)
#: Kite stamps a daily candle at midnight of its trading day.
DAY_STAMP = _dtime(0, 0)


def _r(*parts) -> float:
    h = hashlib.sha256("|".join(str(p) for p in parts).encode()).digest()
    return int.from_bytes(h[:8], "big") / 2 ** 64


class _Inst:
    """Shaped like ``market_data.types.Instrument`` for the spot lookup."""

    def __init__(self, instrument_id, symbol, segment):
        self.instrument_id = instrument_id
        self.symbol = symbol
        self.segment = segment


class FakeNFOClient:
    """The Kite-client surface used by this package."""

    def __init__(self, underlyings: Sequence[str] = ("RELIANCE", "NIFTY"),
                 expiries: Sequence[date] = (), strikes: Sequence[float] = (100.0, 200.0),
                 *, dead_tokens: Iterable[int] = (), missing_tokens: Iterable[int] = (),
                 today: date | None = None,
                 listed_on: Mapping[int, date] | None = None,
                 no_oi_tokens: Iterable[int] = (),
                 no_trade_days: Iterable[date] = ()):
        self.today = today or date.today()
        #: token -> the first session the contract existed.  A future listed
        #: last week HAS almost no history, and the store must say so rather
        #: than invent the sessions before it.
        self.listed_on = {int(k): v for k, v in (listed_on or {}).items()}
        #: tokens the vendor sends no open interest for: it stays NULL.
        self.no_oi_tokens = {int(t) for t in no_oi_tokens}
        #: sessions the exchange was shut: a gap stays a gap.
        self.no_trade_days = set(no_trade_days)
        self.expiries = list(expiries) or [self.today + timedelta(days=7),
                                           self.today + timedelta(days=35),
                                           self.today + timedelta(days=70)]
        self.underlyings = list(underlyings)
        self.strikes = list(strikes)
        self.dead_tokens = set(dead_tokens)
        self.missing_tokens = set(missing_tokens)
        self.calls = {"instruments": 0, "quote": 0, "historical_data": 0}
        self.quote_batches: list[int] = []
        self._rows = self._build()

    # -- the instrument dump ------------------------------------------------
    def _build(self) -> list[dict]:
        rows = []
        token = 100000
        for u in self.underlyings:
            for expiry in self.expiries:
                token += 1
                rows.append(dict(instrument_token=token,
                                 tradingsymbol=f"{u}{expiry:%y%b}FUT".upper(),
                                 name=u, instrument_type="FUT", strike=0.0,
                                 expiry=expiry, lot_size=50, tick_size=0.05,
                                 exchange="NFO", segment="NFO-FUT"))
                for strike in self.strikes:
                    for side in ("CE", "PE"):
                        token += 1
                        rows.append(dict(
                            instrument_token=token,
                            tradingsymbol=f"{u}{expiry:%y%b}{int(strike)}{side}".upper(),
                            name=u, instrument_type=side, strike=strike, expiry=expiry,
                            lot_size=50, tick_size=0.05, exchange="NFO",
                            segment="NFO-OPT"))
        return rows

    def instruments(self, exchange: str = "NFO"):
        self.calls["instruments"] += 1
        if exchange == "NFO":
            return [dict(r) for r in self._rows]
        return []

    # -- quotes --------------------------------------------------------------
    def quote(self, tokens):
        self.calls["quote"] += 1
        tokens = list(tokens)
        self.quote_batches.append(len(tokens))
        if len(tokens) > 500:
            raise ValueError("Kite accepts at most 500 instruments per quote()")
        out = {}
        for t in tokens:
            token = int(t)
            if token in self.missing_tokens:
                continue          # the vendor simply did not answer for this one
            if token in self.dead_tokens:
                out[str(token)] = dict(instrument_token=token, last_price=0,
                                       average_price=0, volume=0, oi=0,
                                       buy_quantity=0, sell_quantity=0,
                                       ohlc={"open": 0, "high": 0, "low": 0, "close": 0},
                                       depth={"buy": [], "sell": []},
                                       timestamp=datetime(1970, 1, 1, 5, 30),
                                       last_trade_time=None)
                continue
            r = _r(token)
            price = round(10 + r * 90, 2)
            out[str(token)] = dict(
                instrument_token=token,
                last_price=price,
                average_price=round(price * 0.99, 2),
                volume=int(1000 + r * 500000),
                oi=int(5000 + r * 900000),
                oi_day_high=int(6000 + r * 900000),
                oi_day_low=int(4000 + r * 800000),
                buy_quantity=int(r * 10000),
                sell_quantity=int((1 - r) * 10000),
                ohlc={"open": price * 0.98, "high": price * 1.03,
                      "low": price * 0.95, "close": price * 0.97},
                depth={"buy": [{"price": price - 0.05, "quantity": 50, "orders": 1}],
                       "sell": [{"price": price + 0.05, "quantity": 75, "orders": 2}]},
                timestamp=datetime.now().replace(microsecond=0),
                last_trade_time=datetime.now().replace(microsecond=0),
            )
        return out

    # -- history -------------------------------------------------------------
    #: A contract listed on or after this date has no history before it, exactly
    #: as a real future has none before the exchange listed it.  ``None`` means
    #: "listed long ago", which is the default every existing test relies on.
    def _listed_on(self, token):
        return self.listed_on.get(int(token))

    def historical_data(self, token, frm, to, interval, oi=False, **kw):
        self.calls["historical_data"] += 1
        assert interval in ("15minute", "day"), interval
        assert oi is True, "the derivatives backfill must always ask for OI"
        out = []
        day = frm.date() if isinstance(frm, datetime) else frm
        end = to.date() if isinstance(to, datetime) else to
        listed = self._listed_on(token)
        while day <= end:
            traded = (day.weekday() < 5 and (listed is None or day >= listed)
                      and day not in self.no_trade_days)
            if traded:
                if interval == "day":
                    r = _r(token, "day", day.isoformat())
                    px = round(10 + r * 90, 2)
                    out.append({"date": datetime.combine(day, DAY_STAMP),
                                "open": px, "high": px * 1.03, "low": px * 0.97,
                                "close": px * 1.01,
                                "volume": int(20000 + r * 400000),
                                # a session the vendor sent no OI for stays
                                # unknown; it is never a zero and never the
                                # session before it carried forward
                                "oi": (None if int(token) in self.no_oi_tokens
                                       else int(10000 + r * 500000))})
                else:
                    for mark, _kind in marks_for(day):
                        bar_start = mark - timedelta(minutes=15)
                        r = _r(token, bar_start.isoformat())
                        px = round(10 + r * 90, 2)
                        out.append({"date": bar_start, "open": px, "high": px * 1.01,
                                    "low": px * 0.99, "close": px * 1.002,
                                    "volume": int(500 + r * 10000),
                                    "oi": int(10000 + r * 500000)})
            day += timedelta(days=1)
        return out


class FakeNFOProvider:
    """``KiteProvider``-shaped: one guarded call path, one request counter."""

    provider_id = "kite"
    rate_limit_per_second = 1000.0

    def __init__(self, client: FakeNFOClient | None = None, *, fail_times: int = 0):
        self.kite = client or FakeNFOClient()
        self.requests_made = 0
        self.fail_times = fail_times

    def _call(self, fn, **kw):
        self.requests_made += 1
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("simulated vendor failure")
        return fn(self.kite)

    def instruments(self):
        """The NSE spot list — indices and the stock underlyings."""
        out = []
        for i, u in enumerate(sorted({r["name"] for r in self.kite._rows})):
            symbol = INDEX_SPOT.get(u, u)
            segment = "INDICES" if u in INDEX_SPOT else "NSE"
            out.append(_Inst(str(900000 + i), symbol, segment))
        return out


__all__ = ["FakeNFOClient", "FakeNFOProvider"]
