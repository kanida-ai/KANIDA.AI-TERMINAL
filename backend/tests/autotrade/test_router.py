"""BrokerRouter.route_picks — symbols / rank_range / top_n + multi-broker."""
from autotrade.broker.base import Pick
from autotrade.broker.router import BrokerRouter
from autotrade.config import BrokerProfile


def _picks():
    return [Pick(symbol=s, rank=i + 1) for i, s in enumerate(
        ["A", "B", "C", "D", "E", "F", "G"])]


def test_explicit_symbols():
    prof = BrokerProfile("p1", "zerodha", symbols=["B", "D"])
    out = BrokerRouter(top_n_stocks=5).route_picks(_picks(), [prof])
    assert sorted(p.symbol for p in out["p1"]) == ["B", "D"]


def test_rank_range():
    prof = BrokerProfile("p1", "zerodha", rank_range=(1, 3))
    out = BrokerRouter(top_n_stocks=5).route_picks(_picks(), [prof])
    assert sorted(p.symbol for p in out["p1"]) == ["A", "B", "C"]


def test_top_n_default():
    prof = BrokerProfile("p1", "zerodha")
    out = BrokerRouter(top_n_stocks=3).route_picks(_picks(), [prof])
    assert [p.symbol for p in out["p1"]] == ["A", "B", "C"]


def test_symbol_in_multiple_profiles():
    p1 = BrokerProfile("zer", "zerodha", rank_range=(1, 3))
    p2 = BrokerProfile("fyr", "fyers", rank_range=(2, 4))
    out = BrokerRouter().route_picks(_picks(), [p1, p2])
    assert "B" in [p.symbol for p in out["zer"]]
    assert "B" in [p.symbol for p in out["fyr"]]


def test_disabled_profile_excluded():
    p1 = BrokerProfile("zer", "zerodha", enabled=True)
    p2 = BrokerProfile("fyr", "fyers", enabled=False)
    out = BrokerRouter(top_n_stocks=2).route_picks(_picks(), [p1, p2])
    assert "zer" in out and "fyr" not in out
