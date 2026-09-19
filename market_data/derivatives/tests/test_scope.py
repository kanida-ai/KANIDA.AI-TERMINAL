"""Scope selection (spec §1): front two expiries per underlying, per kind."""
from datetime import date, timedelta

import pytest

from market_data.derivatives.fake_nfo import FakeNFOClient, FakeNFOProvider
from market_data.derivatives.instruments import (
    fetch_nfo_instruments,
    select_scope,
    spot_map,
)

TODAY = date(2026, 9, 18)


def _client(**kw):
    return FakeNFOClient(underlyings=("RELIANCE", "NIFTY"),
                         expiries=[TODAY - timedelta(days=3),      # already expired
                                   TODAY,                          # expires today: live
                                   TODAY + timedelta(days=7),
                                   TODAY + timedelta(days=35)],
                         strikes=(100.0, 200.0, 300.0), today=TODAY, **kw)


def test_front_two_expiries_per_underlying_and_kind():
    provider = FakeNFOProvider(_client())
    contracts = fetch_nfo_instruments(provider, use_cache=False)
    scope = select_scope(contracts, TODAY)

    assert scope.total_listed == len(contracts)
    for underlying in ("RELIANCE", "NIFTY"):
        for kind in ("OPT", "FUT"):
            expiries = sorted({c.expiry for c in scope.contracts
                               if c.underlying == underlying and c.kind == kind})
            assert expiries == [TODAY, TODAY + timedelta(days=7)], (underlying, kind)


def test_an_expired_contract_is_never_in_scope():
    provider = FakeNFOProvider(_client())
    scope = select_scope(fetch_nfo_instruments(provider, use_cache=False), TODAY)
    assert scope.dropped_expired > 0
    assert all(c.expiry >= TODAY for c in scope.contracts)


def test_expiry_day_is_still_live():
    """A contract expiring today trades today — it is in scope until the close."""
    provider = FakeNFOProvider(_client())
    scope = select_scope(fetch_nfo_instruments(provider, use_cache=False), TODAY)
    assert any(c.expiry == TODAY for c in scope.contracts)


def test_counts_and_tokens_line_up():
    provider = FakeNFOProvider(_client())
    scope = select_scope(fetch_nfo_instruments(provider, use_cache=False), TODAY)
    counts = scope.counts()
    assert counts["total"] == len(scope.contracts) == len(scope.tokens)
    assert counts["CE"] + counts["PE"] + counts["FUT"] == counts["total"]
    assert counts["underlyings"] == 2
    # 2 underlyings × 2 expiries × (3 strikes × 2 sides + 1 future)
    assert counts["total"] == 2 * 2 * (3 * 2 + 1)


def test_scope_can_be_narrowed_to_named_underlyings():
    provider = FakeNFOProvider(_client())
    contracts = fetch_nfo_instruments(provider, use_cache=False)
    scope = select_scope(contracts, TODAY, underlyings=["NIFTY"])
    assert scope.underlyings == ["NIFTY"]


def test_front_future_is_the_nearest_expiry():
    provider = FakeNFOProvider(_client())
    scope = select_scope(fetch_nfo_instruments(provider, use_cache=False), TODAY)
    fut = scope.front_future("RELIANCE")
    assert fut is not None and fut.expiry == TODAY and fut.instrument_type == "FUT"


def test_index_underlyings_map_to_their_index_spot():
    provider = FakeNFOProvider(_client())
    scope = select_scope(fetch_nfo_instruments(provider, use_cache=False), TODAY)
    spots = spot_map(provider, scope.underlyings)
    assert spots["NIFTY"][0] == "NIFTY 50"
    assert spots["RELIANCE"][0] == "RELIANCE"


def test_an_empty_universe_is_an_error_not_an_empty_scope():
    class Empty(FakeNFOClient):
        def instruments(self, exchange="NFO"):
            return []

    provider = FakeNFOProvider(Empty(today=TODAY))
    with pytest.raises(RuntimeError):
        fetch_nfo_instruments(provider, use_cache=False)


def test_the_instrument_cache_is_isolable(tmp_path):
    """A fixture universe must never be able to poison the real cache."""
    provider = FakeNFOProvider(_client())
    fetch_nfo_instruments(provider, cache_dir=tmp_path)
    assert (tmp_path / "instruments_NFO.json").exists()
    calls = provider.kite.calls["instruments"]
    fetch_nfo_instruments(provider, cache_dir=tmp_path)          # served from cache
    assert provider.kite.calls["instruments"] == calls
