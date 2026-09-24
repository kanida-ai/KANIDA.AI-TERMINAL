"""
Options Agent · snapshot store + POINT-IN-TIME chain loader tests.

No credentials, no network, no real Kite client. Every test runs against its OWN temp snapshot
directory (AGENT_OPTIONS_SNAP_DIR), so nothing touches a real store.

What these tests are actually defending (they are the point-in-time contract, not smoke tests):

  * a snapshot dated AFTER as_of is NEVER returned for as_of — neither by key nor by a mis-keyed
    payload that carries a later as_of inside it;
  * a MISSING snapshot yields an honest empty result + a named reason and performs NO FETCH — proved
    by a spy on services.kite_auth.get_kite_client that fails the test if it is ever called;
  * expiries resolve from the AS-OF ARCHIVED master, not a live instruments("NFO") call (which lists
    only contracts alive today -> look-ahead + survivorship);
  * lot_size comes from the snapshot, never a constant (the exchange changes it);
  * the store round-trips, reads guarded (missing/corrupt -> None, never an exception), and is
    IMMUTABLE (a re-write of an existing date does not silently overwrite);
  * snapshot_chain's consistency guard ABORTS the write on a crossed ATM quote or a parity violation
    — a bad snapshot is permanent poison because a past chain can never be re-fetched.

Run:  python -m pytest agents/options/tests/test_data_chain.py -q     (from backend/)
"""
from __future__ import annotations

import gzip
import json
import os
import sys
import types
from datetime import date, datetime, timedelta

import pytest

_BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from agents.options import data                    # noqa: E402
from agents.options import fetch_kite              # noqa: E402
from agents.options import pricing                 # noqa: E402
from agents.options import store                   # noqa: E402

UND = "NIFTY"
LOT_IN_SNAPSHOT = 65        # deliberately NOT the current NIFTY lot — proves it is read, not assumed


# ============================================================================ fixtures
@pytest.fixture(autouse=True)
def snap_dir(tmp_path, monkeypatch):
    """Every test gets a private local snapshot store; the S3 seam is explicitly disabled."""
    monkeypatch.delenv(store.ENV_URI, raising=False)
    monkeypatch.setenv(store.ENV_DIR, str(tmp_path / "options_chains"))
    monkeypatch.setenv(store.ENV_PARQUET, "0")     # sidecar off: no pyarrow dependency in CI
    monkeypatch.setenv(fetch_kite.ENV_BACKFILL_DIR, str(tmp_path / "options_backfill"))
    return str(tmp_path / "options_chains")


@pytest.fixture
def no_fetch_spy(monkeypatch):
    """Install a services.kite_auth whose get_kite_client EXPLODES. Any code path that tries to fetch
    on a point-in-time read fails the test loudly instead of silently returning live data."""
    calls = []
    fake = types.ModuleType("services.kite_auth")

    def _spy(*a, **k):
        calls.append((a, k))
        raise AssertionError("get_kite_client() called on a point-in-time read — "
                             "a live fallback on a past date is look-ahead")

    fake.get_kite_client = _spy
    monkeypatch.setitem(sys.modules, "services.kite_auth", fake)
    return calls


# ============================================================================ synthetic payloads
def _mk_payload(as_of: str, underlying: str = UND, expiries=None, lot_size: int = LOT_IN_SNAPSHOT,
                strikes=(23900.0, 24000.0, 24100.0)):
    """A minimal but structurally REAL snapshot: archived master + rows + forwards."""
    expiries = list(expiries or [(date.fromisoformat(as_of) + timedelta(days=7)).isoformat()])
    instruments, rows = [], []
    tok = 100000
    for exp in expiries:
        instruments.append({"tradingsymbol": f"NIFTY{exp}FUT", "instrument_token": tok,
                            "name": underlying, "instrument_type": "FUT", "strike": None,
                            "expiry": exp, "lot_size": lot_size, "tick_size": 0.05})
        tok += 1
        for k in strikes:
            for right in ("CE", "PE"):
                ts = f"NIFTY{exp}{int(k)}{right}"
                instruments.append({"tradingsymbol": ts, "instrument_token": tok,
                                    "name": underlying, "instrument_type": right, "strike": k,
                                    "expiry": exp, "lot_size": lot_size, "tick_size": 0.05})
                rows.append({"tradingsymbol": ts, "instrument_token": tok, "strike": k,
                             "right": right, "expiry": exp, "lot_size": lot_size,
                             "ltp": 100.0, "bid": 99.5, "ask": 100.5, "mid": 100.0,
                             "price_source": "mid", "spread_pct": 1.0, "oi": 1000,
                             "volume": 500, "iv": 0.14, "delta": 0.5, "gamma": 0.0001,
                             "theta": -5.0, "vega": 8.0, "forward": 24000.0,
                             "forward_source": "fut_same_expiry"})
                tok += 1
    return {
        "schema": store.SCHEMA, "underlying": underlying, "as_of": as_of,
        "captured_at": as_of + "T10:00:00+00:00", "r": pricing.R_DEFAULT,
        "lot_size": lot_size, "spot": 23990.0, "expiries": expiries,
        "forwards": {e: {"forward": 24000.0, "source": "fut_same_expiry", "T": 7 / 365.0, "dte": 7}
                     for e in expiries},
        "instruments": instruments, "rows": rows,
        "coverage": {"options_seen": len(rows), "quoted": len(rows)},
        "evidence_basis": "forward_tracked_snapshot",
    }


# ============================================================================ fake Kite client
class FakeKite:
    """A synthetic, internally CONSISTENT NFO chain built from Black-76 itself, so put-call parity
    holds exactly on a healthy chain — which means any guard failure in a test is caused by the
    injected break, not by fixture noise.

    ``break_mode``:
        None       healthy chain
        "crossed"  the ATM call is quoted bid > ask (a broken book)
        "parity"   the ATM call is 200 points off (a stale/mispriced leg)
        "empty"    no instruments at all

    ``hist_break="parity"`` does the same to the historical CLOSE series, for the backfill guard.
    ``historical_data`` mirrors the measured live behaviour: daily OHLC + OI, no bid/ask, and an
    assertion on an unknown token (Kite raises InputException there).
    """

    # > the backfill parity tolerance (0.6% of ~24k = ~144), < one strike step (100) x2, so the
    # broken leg still ranks inside the guard's near-ATM band instead of hiding on the wings.
    HIST_BREAK_POINTS = 175.0

    def __init__(self, as_of: date, F=24000.0, sigma=0.14, break_mode=None,
                 lot_size=LOT_IN_SNAPSHOT, dtes=(7, 28), underlying=UND,
                 hist_days=12, hist_break=None, post_market=False, strike_steps=5):
        self.post_market = bool(post_market)
        self.strike_steps = int(strike_steps)
        self.as_of = as_of
        self.F = float(F)
        self.sigma = float(sigma)
        self.break_mode = break_mode
        self.hist_break = hist_break
        self.lot_size = int(lot_size)
        self.underlying = underlying
        self.expiries = [(as_of + timedelta(days=d)).isoformat() for d in dtes]
        self.strikes = [self.F + 100.0 * i
                        for i in range(-self.strike_steps, self.strike_steps + 1)]
        self.listing_start = as_of - timedelta(days=int(hist_days))
        self.instrument_calls = 0
        self.quote_calls = 0
        self.hist_calls = []
        self._master = [] if break_mode == "empty" else self._build_master()
        self._by_token = {r["instrument_token"]: r for r in self._master}

    # -- the instrument master ----------------------------------------------------------
    def _build_master(self):
        rows, tok = [], 500000
        for exp in self.expiries:
            rows.append({"tradingsymbol": f"NIFTY{exp}FUT", "instrument_token": tok,
                         "exchange_token": tok, "name": self.underlying,
                         "instrument_type": "FUT", "strike": 0.0, "expiry": exp,
                         "lot_size": self.lot_size, "tick_size": 0.05,
                         "segment": "NFO-FUT", "exchange": "NFO"})
            tok += 1
            for k in self.strikes:
                for right in ("CE", "PE"):
                    rows.append({"tradingsymbol": f"NIFTY{exp}{int(k)}{right}",
                                 "instrument_token": tok, "exchange_token": tok,
                                 "name": self.underlying, "instrument_type": right,
                                 "strike": k, "expiry": exp, "lot_size": self.lot_size,
                                 "tick_size": 0.05, "segment": "NFO-OPT", "exchange": "NFO"})
                    tok += 1
        # a decoy underlying that must be filtered out
        rows.append({"tradingsymbol": "BANKNIFTY-DECOY", "instrument_token": 999999,
                     "name": "BANKNIFTY", "instrument_type": "CE", "strike": 50000.0,
                     "expiry": self.expiries[0], "lot_size": 15, "tick_size": 0.05,
                     "segment": "NFO-OPT", "exchange": "NFO"})
        return rows

    def instruments(self, exchange):
        self.instrument_calls += 1
        assert exchange == "NFO"
        return list(self._master)

    # -- quotes -------------------------------------------------------------------------
    def _atm(self):
        return min(self.strikes, key=lambda k: abs(k - self.F))

    def _theo(self, exp: str, k: float, right: str) -> float:
        T = pricing.year_fraction((date.fromisoformat(exp) - self.as_of).days)
        return pricing.price(self.F, k, T, self.sigma, right)

    def _one(self, ins):
        ts = datetime.combine(self.as_of, datetime.min.time().replace(hour=15, minute=30))
        if self.post_market:
            # MEASURED post-market behaviour: depth all zeros, volume/average_price reset to 0,
            # `timestamp` returned as the 1970 epoch, and `last_trade_time` carrying the real
            # session time. last_price / ohlc / oi stay correct.
            if ins["instrument_type"] == "FUT":
                px = self.F
            else:
                px = self._theo(ins["expiry"], ins["strike"], ins["instrument_type"])
            return {"instrument_token": ins["instrument_token"], "last_price": round(px, 2),
                    "oi": 12345, "volume": 0, "average_price": 0,
                    "timestamp": datetime(1970, 1, 1, 5, 30), "last_trade_time": ts,
                    "ohlc": {"open": px, "high": px, "low": px, "close": px},
                    "depth": {"buy": [{"price": 0, "quantity": 0, "orders": 0}],
                              "sell": [{"price": 0, "quantity": 0, "orders": 0}]}}
        if ins["instrument_type"] == "FUT":
            px = self.F
        else:
            px = self._theo(ins["expiry"], ins["strike"], ins["instrument_type"])
        bid, ask = px - 0.25, px + 0.25
        if (self.break_mode and ins["instrument_type"] == "CE"
                and ins.get("strike") == self._atm() and ins["expiry"] == self.expiries[0]):
            if self.break_mode == "crossed":
                bid, ask = ask, bid          # crossed book at the money
            elif self.break_mode == "parity":
                px += 200.0                  # stale / mispriced ATM call
                bid, ask = px - 0.25, px + 0.25
        return {"instrument_token": ins["instrument_token"], "last_price": round(px, 2),
                "oi": 12345, "volume": 6789, "timestamp": ts, "exchange_timestamp": ts,
                "depth": {"buy": [{"price": round(bid, 2), "quantity": 100, "orders": 1}],
                          "sell": [{"price": round(ask, 2), "quantity": 100, "orders": 1}]}}

    def quote(self, *instruments):
        self.quote_calls += 1
        keys = instruments[0] if len(instruments) == 1 and isinstance(
            instruments[0], (list, tuple)) else list(instruments)
        assert len(keys) <= fetch_kite.QUOTE_CHUNK, "quote() called above the Kite instrument cap"
        by_ts = {f"NFO:{r['tradingsymbol']}": r for r in self._master}
        out = {}
        for k in keys:
            ins = by_ts.get(k)
            if ins is not None:
                out[k] = self._one(ins)
            elif k.startswith("NSE:"):
                out[k] = {"last_price": self.F - 10.0}
        return out

    # -- historical daily bars (OHLC + OI, and NO bid/ask — as measured) ----------------
    def _fwd_on(self, d: date) -> float:
        """A forward that MOVES between dates (so the dates are not clones) but always lands exactly
        on a listed strike, which keeps the ATM strike unambiguous for the guard fixtures."""
        return self.F + 100.0 * (((d - self.listing_start).days % 3) - 1)

    def historical_data(self, token, from_date, to_date, interval, oi=False, continuous=False):
        self.hist_calls.append(token)
        inst = self._by_token.get(token)
        # Kite raises InputException("invalid token") for a contract not in today's master.
        assert inst is not None, "invalid token"
        assert interval == "day" and oi is True
        fd = from_date.date() if isinstance(from_date, datetime) else from_date
        td = to_date.date() if isinstance(to_date, datetime) else to_date
        exp = date.fromisoformat(inst["expiry"])
        # history exists from listing up to (but not including) today — today's bar is still forming
        start = max(fd, self.listing_start)
        end = min(td, exp, self.as_of - timedelta(days=1))
        bars, d = [], start
        while d <= end:
            if d.weekday() < 5:
                F = self._fwd_on(d)
                if inst["instrument_type"] == "FUT":
                    close = F
                else:
                    T = pricing.year_fraction((exp - d).days)
                    close = pricing.price(F, inst["strike"], T, self.sigma,
                                          inst["instrument_type"])
                    atm = min(self.strikes, key=lambda k: abs(k - F))
                    if (self.hist_break == "parity" and inst["instrument_type"] == "CE"
                            and inst["strike"] == atm and inst["expiry"] == self.expiries[0]):
                        close += self.HIST_BREAK_POINTS      # a stale/mispriced ATM close
                close = round(close, 2)
                bars.append({"date": datetime.combine(d, datetime.min.time()),
                             "open": close, "high": close, "low": close, "close": close,
                             "volume": 4321, "oi": 98765})
            d += timedelta(days=1)
        return bars

    # -- the execution boundary: any order-shaped call fails the test -------------------
    def _forbidden(self, *a, **k):
        raise AssertionError("agent code must never call a broker order/margin API")

    place_order = modify_order = cancel_order = _forbidden
    place_gtt = modify_gtt = delete_gtt = _forbidden
    order_margins = basket_order_margins = _forbidden


# ============================================================================ store tests
def test_store_round_trip_write_then_read():
    d = "2026-08-31"
    payload = _mk_payload(d)
    st = store.write(d, UND, payload)
    assert st["status"] == "written" and st["written"] is True
    back = store.read(d, UND)
    assert back is not None
    assert back["underlying"] == UND and back["as_of"] == d
    assert len(back["rows"]) == len(payload["rows"])
    assert store.exists(d, UND) is True
    assert store.backend_kind() == "local"


def test_store_read_of_missing_key_returns_none_not_raise():
    assert store.read("2026-01-01", UND) is None
    assert store.exists("2026-01-01", UND) is False


def test_store_read_of_corrupt_object_returns_none_not_raise():
    d = "2026-08-31"
    store.write(d, UND, _mk_payload(d))
    path = store.uri(d, UND)
    with open(path, "wb") as f:
        f.write(b"this is not gzipped json at all")
    assert store.read(d, UND) is None          # guarded: corrupt -> honest miss, never an exception


def test_store_read_survives_a_broken_backend(monkeypatch):
    """A backend that explodes (bad URI, no creds, boto3 absent, network down) must still surface as
    None all the way up — and the point-in-time loader must degrade to an honest miss, never raise
    and never fall back to live data."""
    class Boom(store.LocalSnapshotStore):
        def read(self, as_of, underlying):
            raise RuntimeError("s3 unavailable")

        def exists(self, as_of, underlying):
            raise RuntimeError("s3 unavailable")

    monkeypatch.setattr(store, "_store", lambda: Boom("/nonexistent"))
    assert store.read("2026-08-31", UND) is None
    assert store.exists("2026-08-31", UND) is False

    out = data.load_chain(UND, "2026-08-31")
    assert out["available"] is False and out["reason"] == data.REASON_NO_SNAPSHOT
    assert data.snapshot_available(UND, "2026-08-31") is False
    assert data.expiries_as_of(UND, "2026-08-31") == []


def test_parquet_sidecar_is_optional_and_never_load_bearing(monkeypatch):
    """With the columnar sidecar ENABLED the canonical write must still succeed and read back
    identically — and if pandas/pyarrow are missing, the sidecar is simply absent, not an error."""
    monkeypatch.setenv(store.ENV_PARQUET, "1")
    d = "2026-08-31"
    st = store.write(d, UND, _mk_payload(d))
    assert st["written"] is True
    assert store.read(d, UND)["as_of"] == d          # read never consults the sidecar
    if st["parquet"]:
        import pandas as pd
        df = pd.read_parquet(st["parquet"])
        assert {"as_of", "underlying", "strike", "right", "price_source"} <= set(df.columns)
        assert (df["as_of"] == d).all()


def test_snapshot_is_immutable_rewrite_does_not_overwrite():
    d = "2026-08-31"
    first = _mk_payload(d, strikes=(24000.0,))
    assert store.write(d, UND, first)["written"] is True
    n_first = len(store.read(d, UND)["rows"])

    second = _mk_payload(d, strikes=(23000.0, 23100.0, 23200.0, 23300.0))
    st = store.write(d, UND, second)
    assert st["status"] == "exists" and st["written"] is False
    assert "immutable" in st["reason"]

    kept = store.read(d, UND)
    assert len(kept["rows"]) == n_first, "an existing snapshot was silently overwritten"
    assert kept["rows"][0]["strike"] == 24000.0

    # explicit operator repair is the ONLY way through, and it is reported
    st2 = store.write(d, UND, second, overwrite=True)
    assert st2["written"] is True and st2["overwrote"] is True
    assert len(store.read(d, UND)["rows"]) == len(second["rows"])


# ============================================================================ point-in-time tests
def test_snapshot_dated_after_as_of_is_never_returned(no_fetch_spy):
    as_of = "2026-08-28"
    future = "2026-08-31"
    store.write(future, UND, _mk_payload(future))

    out = data.load_chain(UND, as_of)
    assert out["available"] is False
    assert out["reason"] == data.REASON_NO_SNAPSHOT
    assert out["rows"] == [] and out["lot_size"] is None
    assert data.snapshot_available(UND, as_of) is False
    assert data.expiries_as_of(UND, as_of) == []
    assert no_fetch_spy == []

    # the later date itself still reads fine — the miss is about as_of, not a broken store
    assert data.load_chain(UND, future)["available"] is True


def test_miskeyed_payload_carrying_a_later_as_of_is_rejected(no_fetch_spy):
    """Defence in depth: an object filed under D whose own as_of is D+3 is look-ahead. Reject it."""
    key_date = "2026-08-28"
    payload = _mk_payload("2026-08-31")            # payload says 08-31 ...
    store.write(key_date, UND, payload)            # ... but is filed under 08-28
    out = data.load_chain(UND, key_date)
    assert out["available"] is False
    assert out["reason"] == data.REASON_SNAPSHOT_AFTER_AS_OF
    assert out["rows"] == []
    assert no_fetch_spy == []


def test_missing_snapshot_is_honest_empty_and_performs_no_fetch(no_fetch_spy, monkeypatch):
    as_of = "2026-08-28"
    # Also make a live instruments() call impossible to reach unnoticed.
    out = data.load_chain(UND, as_of)
    assert out == {**out, "available": False}
    assert out["reason"] == data.REASON_NO_SNAPSHOT
    assert out["rows"] == [] and out["row_count"] == 0
    assert out["expiries"] == [] and out["lot_size"] is None
    assert out["evidence_basis"] == "forward_tracked_snapshot"

    # every read-side entry point stays fetch-free
    assert data.expiries_as_of(UND, as_of) == []
    assert data.lot_size_as_of(UND, as_of) is None
    assert data.instruments_as_of(UND, as_of) == []
    assert data.nearest_expiry(UND, as_of) is None
    assert data.snapshot_available(UND, as_of) is False
    assert no_fetch_spy == [], "a point-in-time read reached for a Kite client"


def test_data_module_holds_no_kite_client_reference():
    """Structural: the PIT loader must not even own a handle it could fetch with."""
    assert not hasattr(data, "get_kite_client")
    assert not any(n for n in dir(data) if n.lower().startswith("kite"))


def test_expiries_resolve_from_the_as_of_archived_master(no_fetch_spy):
    as_of = "2026-08-31"
    e1 = "2026-09-03"      # weekly
    e2 = "2026-09-24"      # monthly
    e_gone = "2026-08-27"  # already expired on as_of
    payload = _mk_payload(as_of, expiries=[e_gone, e1, e2])
    store.write(as_of, UND, payload)

    assert data.expiries_as_of(UND, as_of) == [e1, e2]
    assert data.expiries_as_of(UND, as_of, include_expired=True) == [e_gone, e1, e2]
    assert data.nearest_expiry(UND, as_of) == e1
    assert data.nearest_expiry(UND, as_of, min_dte=10) == e2

    # and it really is the ARCHIVED master, not a live call
    assert {i["tradingsymbol"] for i in data.instruments_as_of(UND, as_of)} == \
           {i["tradingsymbol"] for i in payload["instruments"]}
    assert no_fetch_spy == []


def test_expiries_ignore_a_master_row_from_a_later_snapshot(no_fetch_spy):
    """The as-of master is the ONLY master consulted: an expiry that was only listed later must not
    leak backwards into an earlier date's expiry set."""
    d1, d2 = "2026-08-28", "2026-08-31"
    store.write(d1, UND, _mk_payload(d1, expiries=["2026-09-03"]))
    store.write(d2, UND, _mk_payload(d2, expiries=["2026-09-03", "2026-10-29"]))
    assert data.expiries_as_of(UND, d1) == ["2026-09-03"]
    assert data.expiries_as_of(UND, d2) == ["2026-09-03", "2026-10-29"]
    assert no_fetch_spy == []


def test_lot_size_comes_from_the_snapshot(no_fetch_spy):
    as_of = "2026-08-31"
    exp = "2026-09-03"
    store.write(as_of, UND, _mk_payload(as_of, expiries=[exp], lot_size=LOT_IN_SNAPSHOT))

    assert data.lot_size_as_of(UND, as_of) == LOT_IN_SNAPSHOT
    assert data.lot_size_as_of(UND, as_of, expiry=exp) == LOT_IN_SNAPSHOT
    chain = data.load_chain(UND, as_of, expiry=exp)
    assert chain["available"] is True
    assert chain["lot_size"] == LOT_IN_SNAPSHOT
    assert chain["lot_size"] != 75, "lot size must never fall back to a current-day constant"

    # a DIFFERENT date with a DIFFERENT lot returns that date's lot — proving it is read per-snapshot
    later = "2026-09-30"
    store.write(later, UND, _mk_payload(later, expiries=["2026-10-29"], lot_size=75))
    assert data.lot_size_as_of(UND, later) == 75
    assert data.lot_size_as_of(UND, as_of) == LOT_IN_SNAPSHOT
    assert no_fetch_spy == []


def test_load_chain_filters_to_one_expiry_and_reports_unknown_expiry():
    as_of = "2026-08-31"
    e1, e2 = "2026-09-03", "2026-09-24"
    store.write(as_of, UND, _mk_payload(as_of, expiries=[e1, e2]))

    all_exp = data.load_chain(UND, as_of)
    one = data.load_chain(UND, as_of, expiry=e1)
    assert all_exp["row_count"] == 2 * one["row_count"]
    assert {r["expiry"] for r in one["rows"]} == {e1}

    missing = data.load_chain(UND, as_of, expiry="2026-12-31")
    assert missing["available"] is False
    assert missing["reason"] == data.REASON_NO_SUCH_EXPIRY
    assert missing["expiries"] == [e1, e2]


def test_diagnostics_expose_the_active_store(snap_dir):
    assert data.chain_dir() == snap_dir
    assert data.chain_uri() == snap_dir
    assert data.snapshot_uri(UND, "2026-08-31").endswith("chain_NIFTY_2026-08-31.json.gz")


def test_chain_uri_follows_the_s3_seam(monkeypatch):
    monkeypatch.setenv(store.ENV_URI, "s3://kanida-bucket/kanida/options_chains")
    assert data.chain_uri() == "s3://kanida-bucket/kanida/options_chains"
    assert store.backend_kind() == "s3"
    assert data.snapshot_uri(UND, "2026-08-31") == \
        "s3://kanida-bucket/kanida/options_chains/NIFTY/chain_NIFTY_2026-08-31.json.gz"
    # and a read against a bucket we cannot reach is still a guarded miss, never a raise
    assert data.load_chain(UND, "2026-08-31")["available"] is False


def test_load_underlying_daily_delegates_to_the_chart_agent_source(monkeypatch):
    """One daily store, one adjustment basis — the options agent must not grow a second loader."""
    from agents.chart import data as chart_data
    seen = {}

    def _fake_load_daily(sym):
        seen["symbol"] = sym
        return "FRAME"

    monkeypatch.setattr(chart_data, "load_daily", _fake_load_daily)
    assert data.load_underlying_daily("NIFTY") == "FRAME"
    assert seen["symbol"] == data.daily_symbol("NIFTY") == "NIFTY 50"


# ============================================================================ snapshot job tests
def test_snapshot_chain_writes_a_healthy_chain():
    today = date.today()
    kite = FakeKite(today)
    res = fetch_kite.snapshot_chain(today, UND, _client=kite)

    assert res["aborted"] is False, res["reason"]
    assert res["ok"] is True
    assert res["store"]["status"] == "written"
    cov = res["coverage"]
    for key in ("options_seen", "quoted", "iv_solved", "skipped_no_quote", "skipped_bad_quote",
                "parity_checked", "parity_failed"):
        assert key in cov, f"coverage accounting is missing {key}"
    assert cov["options_seen"] == len(kite.strikes) * 2 * len(kite.expiries)
    assert cov["quoted"] == cov["options_seen"]
    assert cov["skipped_no_quote"] == 0 and cov["skipped_bad_quote"] == 0
    assert cov["parity_failed"] == 0 and cov["parity_checked"] >= fetch_kite.MIN_PARITY_PAIRS
    assert cov["iv_solved"] > 0

    # the archive is readable by the point-in-time loader, with everything a strategy needs
    chain = data.load_chain(UND, today)
    assert chain["available"] is True
    assert chain["lot_size"] == LOT_IN_SNAPSHOT
    assert sorted(chain["expiries"]) == sorted(kite.expiries)
    r0 = chain["rows"][0]
    for f in ("tradingsymbol", "instrument_token", "strike", "right", "expiry", "lot_size",
              "ltp", "bid", "ask", "mid", "spread_pct", "oi", "volume", "iv",
              "delta", "gamma", "theta", "vega", "price_source"):
        assert f in r0, f"row is missing {f}"
    assert {r["price_source"] for r in chain["rows"]} <= {"mid", "ltp"}
    assert any(r["price_source"] == "mid" for r in chain["rows"])
    # the FUT rows are archived too — that is what makes the forward re-derivable later
    assert any(i["instrument_type"] == "FUT" for i in data.instruments_as_of(UND, today))
    assert all(v["source"] == "fut_same_expiry" for v in chain["forwards"].values())
    # recovered IV must match the vol the synthetic chain was generated with
    atm = min(chain["rows"], key=lambda r: abs(r["strike"] - kite.F))
    assert abs(atm["iv"] - kite.sigma) < 5e-3


def test_snapshot_chain_handles_a_post_market_capture_without_inventing_a_book():
    """MEASURED (live, 18:44 IST): after the close Kite zeroes the whole depth and resets volume,
    returns `timestamp` as the 1970 epoch, and keeps last_price/ohlc/oi correct. This agent runs
    POST-MARKET, so demanding a book would reject the entire chain — but synthesising one would
    fabricate the spread, the credit and the EV. The capture is taken, and LABELLED."""
    today = date.today()
    kite = FakeKite(today, post_market=True)
    res = fetch_kite.snapshot_chain(today, UND, _client=kite)

    assert res["ok"] is True, res["reason"]
    assert res["capture_mode"] == "post_market"
    assert res["guard"]["parity_tol_frac"] == fetch_kite.CLOSE_PARITY_TOL_FRAC
    assert res["coverage"]["quoted"] == res["coverage"]["options_seen"]

    chain = data.load_chain(UND, today)
    assert chain["capture_mode"] == "post_market"
    assert "bid/ask" in chain["quote_basis"]
    for row in chain["rows"]:
        assert row["price_source"] == "ltp" and row["price_basis"] == "close"
        assert row["bid"] is None and row["ask"] is None and row["spread_pct"] is None
        # volume post-market is UNKNOWN, never a false zero
        assert row["volume"] is None
        assert row["oi"] > 0
    assert any(r["iv"] for r in chain["rows"])


def test_post_market_capture_still_rejects_a_contract_that_did_not_trade_today():
    """The staleness rule survives the post-market relaxation: an epoch `timestamp` falls back to
    `last_trade_time`, and a contract whose last trade was another day has no price for today."""
    today = date.today()

    stale_strike = 24000.0 + 900.0        # 9 strike steps out — well beyond the guard's ATM band

    class OneStale(FakeKite):
        def _one(self, ins):
            q = super()._one(ins)
            if ins["tradingsymbol"].endswith("CE") and ins["strike"] == stale_strike:
                q["last_trade_time"] = datetime.combine(
                    self.as_of - timedelta(days=9), datetime.min.time())
            return q

    kite = OneStale(today, post_market=True, strike_steps=10)
    res = fetch_kite.snapshot_chain(today, UND, _client=kite)
    assert res["ok"] is True, res["reason"]
    assert res["coverage"]["skipped_bad_quote"] == len(kite.expiries)   # one wing per expiry
    syms = {r["tradingsymbol"] for r in data.load_chain(UND, today)["rows"]}
    assert not any(s.endswith(f"{int(stale_strike)}CE") for s in syms)


def test_snapshot_chain_aborts_the_write_when_every_expiry_is_crossed():
    """Single expiry, crossed at the money -> nothing survives the guard -> nothing is archived."""
    today = date.today()
    kite = FakeKite(today, break_mode="crossed", dtes=(7,))
    res = fetch_kite.snapshot_chain(today, UND, _client=kite)

    assert res["aborted"] is True and res["ok"] is False
    assert res["coverage"]["parity_failed"] > 0
    assert "rejected" in res["reason"] or "violation" in res["reason"]
    checks = {d["check"] for d in res["guard"]["detail"]}
    assert "atm_quote" in checks
    # NOTHING was archived — a bad snapshot is permanent poison
    assert store.exists(today.isoformat(), UND) is False
    assert data.load_chain(UND, today)["available"] is False


def test_snapshot_chain_aborts_the_write_when_every_expiry_breaks_parity():
    today = date.today()
    kite = FakeKite(today, break_mode="parity", dtes=(7,))
    res = fetch_kite.snapshot_chain(today, UND, _client=kite)

    assert res["aborted"] is True and res["ok"] is False
    assert res["coverage"]["parity_failed"] > 0
    checks = {d["check"] for d in res["guard"]["detail"]}
    assert "parity" in checks
    bad = next(d for d in res["guard"]["detail"] if d["check"] == "parity")
    assert abs(bad["residual"]) > bad["tolerance"]
    assert store.exists(today.isoformat(), UND) is False


@pytest.mark.parametrize("break_mode,check", [("crossed", "atm_quote"), ("parity", "parity")])
def test_snapshot_chain_quarantines_only_the_failing_expiry(break_mode, check):
    """A broken expiry is EXCLUDED from the archive with a named reason; the clean expiries are
    still captured. MEASURED motivation: on the real NIFTY chain the far-dated monthlies trade a
    handful of scattered strikes whose closes cannot be reconciled, so an all-or-nothing abort would
    throw away the liquid weeklies — the actual trading universe — on a day we can never re-fetch."""
    today = date.today()
    kite = FakeKite(today, break_mode=break_mode, dtes=(7, 28))
    broken, clean = kite.expiries[0], kite.expiries[1]
    res = fetch_kite.snapshot_chain(today, UND, _client=kite)

    assert res["ok"] is True and res["aborted"] is False
    assert res["expiries"] == [clean]
    assert [x["expiry"] for x in res["expiries_rejected"]] == [broken]
    assert res["expiries_rejected"][0]["reason"]
    assert res["coverage"]["expiries_rejected"] == 1
    assert {d["check"] for d in res["guard"]["detail"]} == {check}

    chain = data.load_chain(UND, today)
    assert {r["expiry"] for r in chain["rows"]} == {clean}
    # the broken expiry is not silently missing — it is reported as quarantined, with a reason
    q = data.load_chain(UND, today, expiry=broken)
    assert q["available"] is False
    assert q["reason"] == data.REASON_EXPIRY_NOT_ARCHIVED
    assert q["quarantine_reason"]


def test_snapshot_chain_guard_is_fail_closed_when_it_cannot_verify(monkeypatch):
    """Too few verifiable near-ATM pairs -> abort, never an optimistic archive."""
    monkeypatch.setattr(fetch_kite, "MIN_PARITY_PAIRS", 999)
    today = date.today()
    res = fetch_kite.snapshot_chain(today, UND, _client=FakeKite(today))
    assert res["aborted"] is True
    assert "could not verify" in res["reason"]
    assert store.exists(today.isoformat(), UND) is False


def test_snapshot_chain_refuses_to_file_a_live_chain_under_a_past_date():
    past = date.today() - timedelta(days=5)
    res = fetch_kite.snapshot_chain(past, UND, _client=FakeKite(past))
    assert res["aborted"] is True
    assert "not today" in res["reason"]
    assert store.exists(past.isoformat(), UND) is False


def test_snapshot_chain_respects_immutability_on_a_second_run():
    today = date.today()
    kite = FakeKite(today)
    assert fetch_kite.snapshot_chain(today, UND, _client=kite)["ok"] is True
    again = fetch_kite.snapshot_chain(today, UND, _client=kite)
    assert again["ok"] is False and again["aborted"] is False
    assert again["store"]["status"] == "exists"
    assert "immutable" in again["reason"]


def test_snapshot_chain_never_raises_on_a_broken_feed():
    today = date.today()

    class Dead:
        def instruments(self, exchange):
            raise RuntimeError("kite is down")

    res = fetch_kite.snapshot_chain(today, UND, _client=Dead())
    assert res["aborted"] is True and "instruments" in res["reason"]

    empty = fetch_kite.snapshot_chain(today, UND, _client=FakeKite(today, break_mode="empty"))
    assert empty["aborted"] is True and "no CE/PE instruments" in empty["reason"]


def test_snapshot_chain_dry_run_archives_nothing():
    today = date.today()
    res = fetch_kite.snapshot_chain(today, UND, _client=FakeKite(today), write=False)
    assert res["ok"] is True and res["store"]["status"] == "dry_run"
    assert store.exists(today.isoformat(), UND) is False


def test_snapshot_chain_touches_no_order_api():
    """FakeKite raises on every order/margin/GTT method; a clean run proves none were called."""
    today = date.today()
    res = fetch_kite.snapshot_chain(today, UND, _client=FakeKite(today))
    assert res["ok"] is True
    src = open(os.path.join(_BACKEND, "agents", "options", "fetch_kite.py"),
               encoding="utf-8").read()
    for forbidden in ("place_order", "modify_order", "cancel_order", "place_gtt",
                      "kite.order_margins", "kite.basket_order_margins"):
        assert f".{forbidden}(" not in src, f"fetch_kite calls {forbidden}"


def test_run_daily_snapshot_is_guarded_per_underlying():
    today = date.today()
    out = fetch_kite.run_daily_snapshot(("NIFTY", "BANKNIFTY"), as_of=today,
                                        _client=FakeKite(today))
    assert set(out["results"]) == {"NIFTY", "BANKNIFTY"}
    assert out["results"]["NIFTY"]["ok"] is True
    # BANKNIFTY has no CE/PE rows in this fake master -> honest abort, and NIFTY still succeeded
    assert out["results"]["BANKNIFTY"]["aborted"] is True


def test_written_snapshot_is_gzipped_json_with_the_archived_master():
    today = date.today()
    fetch_kite.snapshot_chain(today, UND, _client=FakeKite(today))
    with gzip.open(store.uri(today.isoformat(), UND), "rb") as f:
        payload = json.loads(f.read().decode("utf-8"))
    assert payload["schema"] == store.SCHEMA
    assert payload["as_of"] == today.isoformat()
    assert payload["instruments"] and payload["rows"]
    assert payload["guard"]["parity_failed"] == 0
    assert payload["evidence_basis"] == "forward_tracked_snapshot"


# ============================================================================ backfill tests
def _backfill(kite, **kw):
    kw.setdefault("pace_s", 0)
    return fetch_kite.backfill_history(UND, _client=kite, **kw)


def test_backfill_history_seeds_dated_snapshots_from_real_history():
    today = date.today()
    kite = FakeKite(today, hist_days=12)
    res = _backfill(kite)

    assert res["aborted"] is False, res["reason"]
    assert res["contracts_total"] == len(kite.strikes) * 2 * len(kite.expiries) + len(kite.expiries)
    assert res["contracts_fetched"] == res["contracts_total"]
    assert res["bars_total"] > 0
    assert res["dates_written"] > 0
    assert res["dates_rejected_guard"] == 0, res["dates_rejected_detail"]
    assert res["coverage"]["iv_solved"] > 0

    # every archived date is now readable point-in-time, and is LABELLED as backfilled
    d = sorted(os.listdir(os.path.join(data.chain_dir(), UND)))[0]
    d_iso = d.replace("chain_NIFTY_", "").replace(".json.gz", "")
    chain = data.load_chain(UND, d_iso)
    assert chain["available"] is True
    assert chain["basis"] == data.BASIS_BACKFILL
    assert chain["lot_size"] == LOT_IN_SNAPSHOT
    # IV recovered from a close must match the vol the synthetic history was generated with
    atm = min((r for r in chain["rows"] if r["iv"]),
              key=lambda r: abs(r["strike"] - r["forward"]))
    assert abs(atm["iv"] - kite.sigma) < 1e-2


def test_backfilled_row_cannot_be_mistaken_for_a_live_quoted_row():
    """The honesty caveat, enforced: no bid/ask exists in historical_data, so none is invented."""
    today = date.today()
    kite = FakeKite(today, hist_days=12)
    _backfill(kite)
    d_iso = sorted(os.listdir(os.path.join(data.chain_dir(), UND)))[0] \
        .replace("chain_NIFTY_", "").replace(".json.gz", "")
    back = data.load_chain(UND, d_iso)

    assert back["basis"] == data.BASIS_BACKFILL
    for row in back["rows"]:
        assert row["price_source"] == fetch_kite.PRICE_SOURCE_HIST == "hist_close"
        assert row["bid"] is None and row["ask"] is None
        assert row["mid"] is None and row["spread_pct"] is None
        assert row["lot_size_source"] == fetch_kite.BACKFILL_LOT_SIZE_SOURCE

    # the caveats travel with the chain, so a gate can report SKIPPED instead of silently passing
    assert "bid/ask" in back["quote_basis"].lower()
    assert "missing" in back["survivorship"].lower()
    assert back["lot_size_source"]
    assert back["evidence_basis"] == "backfilled_listed_contracts_only"

    # ... and a LIVE snapshot of the same underlying is unambiguously distinguishable
    live_kite = FakeKite(today)
    assert fetch_kite.snapshot_chain(today, UND, _client=live_kite)["ok"] is True
    live = data.load_chain(UND, today)
    assert live["basis"] == data.BASIS_LIVE
    assert live["quote_basis"] is None and live["lot_size_source"] is None
    assert {r["price_source"] for r in live["rows"]} == {"mid"}
    assert all(r["bid"] is not None and r["ask"] is not None for r in live["rows"])
    assert fetch_kite.PRICE_SOURCE_HIST not in {r["price_source"] for r in live["rows"]}


def test_backfill_is_resumable_and_never_double_writes():
    today = date.today()
    kite = FakeKite(today, hist_days=12)
    first = _backfill(kite)
    calls_after_first = len(kite.hist_calls)
    assert first["dates_written"] > 0 and first["contracts_cached"] == 0

    second = _backfill(kite)
    assert len(kite.hist_calls) == calls_after_first, "a resumed run re-fetched cached history"
    assert second["contracts_cached"] == second["contracts_total"]
    assert second["contracts_fetched"] == 0
    assert second["dates_written"] == 0, "a resumed run re-wrote dates it had already archived"
    assert second["dates_skipped_existing"] == first["dates_written"]


def test_backfill_paces_itself_to_the_kite_rate_limit(monkeypatch):
    import time as _time
    slept = []
    monkeypatch.setattr(_time, "sleep", lambda s: slept.append(s))
    today = date.today()
    kite = FakeKite(today, hist_days=6)
    res = fetch_kite.backfill_history(UND, _client=kite, pace_s=0.25)
    assert res["aborted"] is False
    assert len(slept) == len(kite.hist_calls), "pacing must apply to every historical call"
    assert set(slept) == {0.25}
    assert fetch_kite.BACKFILL_PACE_S >= 1 / 3.0 * 0.99      # ~3 req/s default


def test_backfill_never_archives_today_or_a_future_date():
    """Today belongs to the live snapshot job (and today's daily bar is still forming)."""
    today = date.today()
    kite = FakeKite(today, hist_days=12)
    res = _backfill(kite, through=today + timedelta(days=30))
    assert res["through"] == (today - timedelta(days=1)).isoformat()
    assert store.exists(today.isoformat(), UND) is False
    for name in os.listdir(os.path.join(data.chain_dir(), UND)):
        d_iso = name.replace("chain_NIFTY_", "").replace(".json.gz", "")
        assert date.fromisoformat(d_iso) < today


def test_backfill_never_overwrites_an_existing_live_snapshot():
    today = date.today()
    kite = FakeKite(today, hist_days=12)
    d_iso = (today - timedelta(days=1)).isoformat()
    while date.fromisoformat(d_iso).weekday() >= 5:
        d_iso = (date.fromisoformat(d_iso) - timedelta(days=1)).isoformat()
    store.write(d_iso, UND, _mk_payload(d_iso))            # a pre-existing (live-basis) snapshot

    res = _backfill(kite)
    assert res["dates_skipped_existing"] >= 1
    kept = data.load_chain(UND, d_iso)
    assert kept["basis"] == data.BASIS_LIVE, "a live snapshot was replaced by a weaker backfill"
    assert {r["price_source"] for r in kept["rows"]} == {"mid"}


def test_backfill_guard_rejects_a_broken_historical_chain():
    """The ONLY expiry's history is broken -> every date fails the guard -> nothing is archived."""
    today = date.today()
    kite = FakeKite(today, hist_days=12, hist_break="parity", dtes=(7,))
    res = _backfill(kite)
    assert res["dates_seen"] > 0
    assert res["dates_rejected_guard"] == res["dates_seen"]
    assert res["dates_written"] == 0
    assert res["coverage"]["parity_failed"] > 0
    assert not os.path.isdir(os.path.join(data.chain_dir(), UND)) or \
        os.listdir(os.path.join(data.chain_dir(), UND)) == []


def test_backfill_quarantines_only_the_broken_expiry():
    today = date.today()
    kite = FakeKite(today, hist_days=12, hist_break="parity", dtes=(7, 28))
    broken, clean = kite.expiries[0], kite.expiries[1]
    res = _backfill(kite)
    assert res["dates_written"] > 0 and res["dates_rejected_guard"] == 0

    d_iso = sorted(os.listdir(os.path.join(data.chain_dir(), UND)))[0] \
        .replace("chain_NIFTY_", "").replace(".json.gz", "")
    chain = data.load_chain(UND, d_iso)
    assert {r["expiry"] for r in chain["rows"]} == {clean}
    assert [x["expiry"] for x in chain["expiries_rejected"]] == [broken]


def test_backfill_is_guarded_against_a_dead_feed():
    today = date.today()

    class HalfDead(FakeKite):
        def historical_data(self, *a, **k):
            raise RuntimeError("rate limited")

    res = _backfill(HalfDead(today))
    assert res["aborted"] is False                  # the job completes and REPORTS, never raises
    assert res["contracts_failed"] == res["contracts_total"]
    assert res["dates_written"] == 0 and res["errors"]

    class NoMaster:
        def instruments(self, exchange):
            raise RuntimeError("kite is down")

    dead = fetch_kite.backfill_history(UND, _client=NoMaster(), pace_s=0)
    assert dead["aborted"] is True and "instruments" in dead["reason"]


def test_backfill_dry_run_archives_nothing():
    today = date.today()
    res = _backfill(FakeKite(today, hist_days=12), write=False)
    assert res["dates_written"] == 0 and res["dry_run_dates"] > 0
    assert not os.path.isdir(os.path.join(data.chain_dir(), UND)) or \
        os.listdir(os.path.join(data.chain_dir(), UND)) == []


def test_backfill_touches_no_order_api_and_reads_only():
    """FakeKite explodes on every order/margin/GTT method; a clean run proves none were called."""
    today = date.today()
    res = _backfill(FakeKite(today, hist_days=8))
    assert res["aborted"] is False and res["dates_written"] > 0


if __name__ == "__main__":       # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))


# ─────────────────────────────────────── lot-size trust horizon (added on review of backfill)
# historical_data carries no lot size, so a backfilled bar is stamped with TODAY's lot. When the
# exchange revises a lot (NIFTY's measured lot is 65 today and has been revised before), every
# rupee figure on a pre-revision bar is scaled wrong. Per-unit metrics are unaffected. These tests
# pin the per-row confidence flag so the compromise can never go silent.
import datetime as _dt

from agents.options import fetch_kite as _fk


def _bar(d, close=100.0):
    return {"date": _dt.datetime.fromisoformat(d + "T00:00:00"), "open": close, "high": close,
            "low": close, "close": close, "volume": 10, "oi": 1000}


_INST = {"tradingsymbol": "NIFTY26SEP24000CE", "instrument_token": 1, "strike": 24000.0,
         "instrument_type": "CE", "expiry": "2026-09-29", "tick_size": 0.05, "lot_size": 65}


def test_recent_backfilled_bar_is_marked_lot_size_trusted():
    ref = _dt.date(2026, 8, 31)
    row = _fk._hist_row(_INST, _bar("2026-08-15"), 65, master_date=ref)
    assert row["lot_size_confidence"] == _fk.LOT_CONF_RECENT


def test_old_backfilled_bar_is_marked_lot_size_unverifiable():
    """A 2022 bar stamped with the 2026 lot is exactly the silent mis-scaling this guards."""
    ref = _dt.date(2026, 8, 31)
    row = _fk._hist_row(_INST, _bar("2022-11-28"), 65, master_date=ref)
    assert row["lot_size_confidence"] == _fk.LOT_CONF_UNVERIFIABLE
    assert "RUPEE totals may be scaled wrong" in row["lot_size_confidence"]
    assert "Per-unit metrics" in row["lot_size_confidence"]


def test_lot_confidence_boundary_is_the_governed_horizon():
    ref = _dt.date(2026, 8, 31)
    inside = ref - _dt.timedelta(days=_fk.LOT_SIZE_TRUST_DAYS)
    outside = ref - _dt.timedelta(days=_fk.LOT_SIZE_TRUST_DAYS + 1)
    assert _fk._lot_confidence(inside, ref) == _fk.LOT_CONF_RECENT
    assert _fk._lot_confidence(outside, ref) == _fk.LOT_CONF_UNVERIFIABLE


def test_unparseable_bar_date_is_never_silently_trusted():
    assert _fk._lot_confidence("not-a-date", _dt.date(2026, 8, 31)) == _fk.LOT_CONF_UNVERIFIABLE


def test_lot_confidence_is_measured_against_the_master_read_date():
    """The reference is the date the INSTRUMENT MASTER was read -- not the bar's own date
    (which would make every row trivially age-0 'recent') and not an implicit clock read."""
    old_ref = _dt.date(2023, 1, 10)
    row = _fk._hist_row(_INST, _bar("2023-01-05"), 50, master_date=old_ref)
    assert row["lot_size_confidence"] == _fk.LOT_CONF_RECENT
    # the reference is RECORDED on the row, so the judgement is reproducible + auditable
    assert row["lot_size_master_date"] == "2023-01-10"


def test_bar_date_is_not_used_as_its_own_reference():
    """Regression: passing the bar's own date as the reference makes age 0 for EVERY row, so a
    2022 bar stamped with a 2026 lot would report as trusted."""
    bar_d = "2022-11-28"
    wrong = _fk._hist_row(_INST, _bar(bar_d), 65, master_date=_dt.date(2022, 11, 28))
    right = _fk._hist_row(_INST, _bar(bar_d), 65, master_date=_dt.date(2026, 8, 31))
    assert wrong["lot_size_confidence"] == _fk.LOT_CONF_RECENT      # what the bug looked like
    assert right["lot_size_confidence"] == _fk.LOT_CONF_UNVERIFIABLE  # the honest answer
