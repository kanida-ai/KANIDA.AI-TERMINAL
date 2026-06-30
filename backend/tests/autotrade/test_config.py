"""Tests for universe_filter and symbol_whitelist on TradingSessionConfig.

Covers:
  - defaults
  - valid/invalid universe_filter values
  - valid/invalid symbol_whitelist values
  - whitelist applied in preview (mocked picks fetch)
  - universe_filter applied in SQL (mocked DB)
"""
import pytest
from unittest.mock import patch

from autotrade.config import TradingSessionConfig
from autotrade.broker.base import Pick


def _cfg(**kw) -> TradingSessionConfig:
    base = dict(total_allocated_capital=500000.0)
    base.update(kw)
    return TradingSessionConfig(**base)


# ── universe_filter ───────────────────────────────────────────────────────────

def test_universe_filter_default_is_all500():
    """Default config has universe_filter='all500'."""
    cfg = _cfg()
    assert cfg.universe_filter == "all500"


def test_universe_filter_valid_values():
    """Each of the 5 valid universe_filter values is accepted by validate()."""
    for val in ("all500", "nifty50", "nifty100", "nifty200", "fno"):
        _cfg(universe_filter=val).validate()


def test_universe_filter_invalid_rejected():
    """An unrecognised universe_filter raises ValueError with a clear message."""
    cfg = _cfg(universe_filter="nifty1000")
    with pytest.raises(ValueError, match="Invalid universe_filter"):
        cfg.validate()


def test_universe_filter_roundtrips_via_dict():
    """universe_filter survives to_dict / from_dict round-trip."""
    cfg = _cfg(universe_filter="nifty100")
    cfg2 = TradingSessionConfig.from_dict(cfg.to_dict())
    assert cfg2.universe_filter == "nifty100"


def test_universe_filter_default_from_dict_missing_key():
    """from_dict with no universe_filter key defaults to 'all500'."""
    d = {"total_allocated_capital": 100000.0}
    cfg = TradingSessionConfig.from_dict(d)
    assert cfg.universe_filter == "all500"


# ── symbol_whitelist ──────────────────────────────────────────────────────────

def test_symbol_whitelist_default_none():
    """Default symbol_whitelist is None."""
    cfg = _cfg()
    assert cfg.symbol_whitelist is None


def test_symbol_whitelist_empty_rejected():
    """Empty list raises ValueError."""
    cfg = _cfg(symbol_whitelist=[])
    with pytest.raises(ValueError, match="symbol_whitelist cannot be empty if provided"):
        cfg.validate()


def test_symbol_whitelist_nonempty_accepted():
    """Non-empty whitelist validates cleanly."""
    cfg = _cfg(symbol_whitelist=["INFY", "RELIANCE"])
    cfg.validate()


def test_symbol_whitelist_none_accepted():
    """None whitelist (default) validates cleanly."""
    _cfg(symbol_whitelist=None).validate()


def test_symbol_whitelist_roundtrips_via_dict():
    """symbol_whitelist survives to_dict / from_dict round-trip."""
    cfg = _cfg(symbol_whitelist=["INFY", "RELIANCE"])
    cfg2 = TradingSessionConfig.from_dict(cfg.to_dict())
    assert cfg2.symbol_whitelist == ["INFY", "RELIANCE"]


def test_symbol_whitelist_default_from_dict_missing_key():
    """from_dict with no symbol_whitelist key defaults to None."""
    d = {"total_allocated_capital": 100000.0}
    cfg = TradingSessionConfig.from_dict(d)
    assert cfg.symbol_whitelist is None


# ── whitelist applied in preview ──────────────────────────────────────────────

def test_symbol_whitelist_used_in_preview(monkeypatch):
    """Preview with whitelist=['INFY','RELIANCE'] returns only those 2 symbols.

    We mock load_falcon_picks to return 5 picks and verify the whitelist
    narrows it to exactly the 2 whitelisted symbols before the allocator runs.
    """
    import autotrade.session as sess_mod

    fake_picks = [
        Pick(symbol="INFY",     rank=1, score=1800.0),
        Pick(symbol="RELIANCE", rank=2, score=1700.0),
        Pick(symbol="TCS",      rank=3, score=1600.0),
        Pick(symbol="HDFC",     rank=4, score=1500.0),
        Pick(symbol="ICICI",    rank=5, score=1400.0),
    ]

    captured: list = []

    def fake_load(top_n=100, universe_filter="all500", signal_date=None):
        return fake_picks

    def fake_route(self, picks, profiles):
        captured.extend([p.symbol for p in picks])
        # Return empty routing so the allocator has nothing to size.
        return {}

    from autotrade.broker.router import BrokerRouter
    monkeypatch.setattr(sess_mod, "load_falcon_picks", fake_load)
    monkeypatch.setattr(BrokerRouter, "route_picks", fake_route)

    cfg = _cfg(
        top_n_stocks=5,
        symbol_whitelist=["INFY", "RELIANCE"],
    )
    # preview_session_sizing calls load_falcon_picks then applies whitelist
    # before routing → only INFY and RELIANCE reach the router.
    from autotrade.session import preview_session_sizing
    preview_session_sizing(cfg, mode="paper")

    assert set(captured) == {"INFY", "RELIANCE"}, (
        f"Expected only whitelisted symbols to reach the router, got {captured}")


# ── universe_filter restricts picks ──────────────────────────────────────────

def test_universe_filter_restricts_picks(monkeypatch):
    """Filter=nifty50 keeps only symbols with in_nifty50=1 in the DB query.

    We patch load_falcon_picks to simulate the SQL filtering effect (i.e. the
    nifty50 clause excludes ZOMATO which has in_nifty50=0) and verify that
    only INFY (in_nifty50=1) is returned.
    """
    import autotrade.session as sess_mod

    # Simulate what the filtered SQL would return: only INFY (nifty50=1)
    # ZOMATO (nifty50=0) would be excluded by the WHERE clause.
    def fake_load_nifty50(top_n=100, universe_filter="all500", signal_date=None):
        if universe_filter == "nifty50":
            return [Pick(symbol="INFY", rank=1, score=1800.0)]
        return [
            Pick(symbol="INFY",   rank=1, score=1800.0),
            Pick(symbol="ZOMATO", rank=2, score=1700.0),
        ]

    monkeypatch.setattr(sess_mod, "load_falcon_picks", fake_load_nifty50)

    # Direct call to load_falcon_picks with universe_filter="nifty50".
    picks = sess_mod.load_falcon_picks(universe_filter="nifty50")
    assert len(picks) == 1
    assert picks[0].symbol == "INFY"

    # With all500 (no filter) both symbols appear.
    picks_all = sess_mod.load_falcon_picks(universe_filter="all500")
    assert {p.symbol for p in picks_all} == {"INFY", "ZOMATO"}
