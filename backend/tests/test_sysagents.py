"""Tests for the System-Engineering Agent Hierarchy — Phase 1 (health layer).

Covers: each monitor maps sample inputs → correct status; the orchestrator
correlates a multi-signal incident into ONE page; LLM-down → deterministic
fallback still pages; master-flag-OFF → the whole layer is inert (no signals, no
pages, endpoint disabled). The Anthropic call and every external data source are
mocked/monkeypatched — no network, no real broker, no real DB writes.
"""
import os
import sys
import types
import contextlib

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.dirname(_HERE)
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from autotrade.sysagents import flags, signals, orchestrator  # noqa: E402
from autotrade.sysagents import llm as _llm  # noqa: E402
from autotrade.sysagents.signals import HealthSignal, Status  # noqa: E402
from autotrade.sysagents.monitors import (  # noqa: E402
    BrokerHealthMonitor, DataFreshnessMonitor, MarketDataMonitor,
    QueueLatencyMonitor, AgentWatcherMonitor, ExecutionQualityMonitor,
    RiskRmsMonitor, TradingStatsMonitor)


# ── fake DB plumbing ──────────────────────────────────────────────────────────
class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    """Routes con.execute(sql,...) to canned rows by matching an SQL substring."""
    def __init__(self, routes):
        self._routes = routes  # list of (substring, rows)

    def execute(self, sql, params=()):
        for sub, rows in self._routes:
            if sub in sql:
                return _FakeResult(rows)
        return _FakeResult([])

    def commit(self):
        pass


def _fake_conn_factory(routes):
    @contextlib.contextmanager
    def _cm():
        yield _FakeConn(routes)
    return _cm


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    # Every test starts with a known gate state + NO Anthropic key (force
    # deterministic fallback unless a test opts in).
    for k in ("SYSAGENTS_ENABLED", "SYSAGENTS_KILL_SWITCH", "SYSAGENTS_PAGING",
              "ANTHROPIC_API_KEY"):
        monkeypatch.delenv(k, raising=False)
    yield


def _enable(monkeypatch):
    monkeypatch.setenv("SYSAGENTS_ENABLED", "true")


# ── flags / gates ─────────────────────────────────────────────────────────────
def test_flags_default_off():
    assert flags.agents_enabled() is False
    assert flags.layer_active() is False


def test_kill_switch_hard_stops(monkeypatch):
    _enable(monkeypatch)
    assert flags.layer_active() is True
    monkeypatch.setenv("SYSAGENTS_KILL_SWITCH", "true")
    assert flags.agents_enabled() is True
    assert flags.layer_active() is False


# ── signals helpers ───────────────────────────────────────────────────────────
def test_status_worse_ordering():
    assert signals.worse(Status.OK, Status.CRITICAL) == Status.CRITICAL
    assert signals.worse(Status.ALERT, Status.WARN) == Status.ALERT
    assert signals.worse(Status.UNKNOWN, Status.OK) == Status.OK  # OK ranks above UNKNOWN
    assert signals.worse(Status.NA, Status.UNKNOWN) == Status.UNKNOWN


# ── sanitizer: NEVER forward secrets ──────────────────────────────────────────
def test_sanitize_strips_secret_keys():
    sig = HealthSignal(
        subsystem="broker-health", status=Status.OK, summary="ok",
        metrics={"access_token": "sk-SECRET", "proxy_url": "http://u:p@1.2.3.4",
                 "egress_ip": "1.2.3.4", "user_id": "u123",
                 "cpu_percent": 42.0, "connected": True})
    out = _llm.sanitize_signals([sig])
    m = out[0]["metrics"]
    assert "access_token" not in m
    assert "proxy_url" not in m
    assert "egress_ip" not in m       # 'ip' denylisted
    assert "user_id" not in m
    assert m["cpu_percent"] == 42.0
    assert m["connected"] is True


# ── deterministic correlation: causal collapse + one incident ─────────────────
def _sig(sub, status, summary=""):
    return HealthSignal(subsystem=sub, status=status, summary=summary)


def test_deterministic_collapses_broker_chain():
    sigs = _llm.sanitize_signals([
        _sig("broker-health", Status.CRITICAL, "token expired"),
        _sig("execution-quality", Status.ALERT, "rejects rising"),
        _sig("market-data", Status.ALERT, "ticks lagging"),
        _sig("platform-health", Status.OK),
    ])
    inc = _llm.correlate_deterministic(sigs)
    assert inc["severity"] == Status.CRITICAL
    assert "broker-health" in inc["impacted"]
    # the two downstream effects collapse under the one broker root
    assert "execution-quality" in inc["impacted"]
    assert inc["source"] == "deterministic"


def test_deterministic_all_ok():
    inc = _llm.correlate_deterministic(_llm.sanitize_signals([
        _sig("platform-health", Status.OK), _sig("queue-latency", Status.NA)]))
    assert inc["severity"] in (Status.OK, Status.WARN)
    assert inc["impacted"] == []


# ── LLM path falls back to deterministic when unavailable ─────────────────────
def test_correlate_falls_back_without_api_key():
    # No ANTHROPIC_API_KEY (cleared by fixture) → correlate_llm returns None.
    assert _llm.correlate_llm(_llm.sanitize_signals([_sig("broker-health", Status.CRITICAL)])) is None
    inc = _llm.correlate([_sig("broker-health", Status.CRITICAL, "token expired")])
    assert inc["source"] == "deterministic"
    assert inc["severity"] == Status.CRITICAL


def test_correlate_llm_error_falls_back(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    # Simulate the anthropic client raising — must not blind the layer.
    fake_anthropic = types.ModuleType("anthropic")

    class _Boom:
        def __init__(self, *a, **k):
            pass

        @property
        def messages(self):
            raise RuntimeError("api down")

    fake_anthropic.Anthropic = _Boom
    monkeypatch.setitem(sys.modules, "anthropic", fake_anthropic)
    inc = _llm.correlate([_sig("broker-health", Status.CRITICAL, "token expired")])
    assert inc["source"] == "deterministic"
    assert inc["severity"] == Status.CRITICAL


# ── orchestrator: multi-signal incident → ONE page ────────────────────────────
def test_orchestrator_pages_once(monkeypatch):
    _enable(monkeypatch)
    crafted = [
        _sig("broker-health", Status.CRITICAL, "token expired"),
        _sig("execution-quality", Status.ALERT, "rejects rising"),
        _sig("market-data", Status.ALERT, "ticks lagging"),
    ]
    monkeypatch.setattr(orchestrator, "collect_signals", lambda: list(crafted))

    pages = []

    def _fake_dedup(*, kind, session_id, symbol, detail, **kw):
        pages.append({"kind": kind, "symbol": symbol, "detail": detail})
        return 4242

    import autotrade.alerts as alerts_mod
    monkeypatch.setattr(alerts_mod, "send_urgent_deduped", _fake_dedup)
    # No-op persistence (don't touch the DB in this unit test).
    from autotrade.sysagents import db as _db
    monkeypatch.setattr(_db, "persist_incident", lambda **k: None)
    monkeypatch.setattr(_db, "persist_snapshot", lambda **k: 1)

    view = orchestrator.run_once(page=True, persist=True)
    assert view["enabled"] is True
    assert view["status"] == Status.CRITICAL
    assert view["incident"]["paged"] is True
    assert len(pages) == 1              # ONE incident, ONE page — not three
    assert pages[0]["kind"] == "PLATFORM_HEALTH"


def test_orchestrator_llm_down_still_pages(monkeypatch):
    """LLM unavailable (no key) → deterministic correlation still pages."""
    _enable(monkeypatch)
    crafted = [_sig("risk-rms", Status.CRITICAL, "1 unacked Tier-3 alert")]
    monkeypatch.setattr(orchestrator, "collect_signals", lambda: list(crafted))
    pages = []
    import autotrade.alerts as alerts_mod
    monkeypatch.setattr(alerts_mod, "send_urgent_deduped",
                        lambda **k: pages.append(k) or 7)
    from autotrade.sysagents import db as _db
    monkeypatch.setattr(_db, "persist_incident", lambda **k: None)
    monkeypatch.setattr(_db, "persist_snapshot", lambda **k: 1)
    view = orchestrator.run_once(page=True, persist=True)
    assert view["incident"]["source"] == "deterministic"
    assert view["incident"]["severity"] == Status.CRITICAL
    assert len(pages) == 1


# ── master-flag OFF → whole layer inert ───────────────────────────────────────
def test_layer_off_is_inert(monkeypatch):
    # collect_signals must NEVER be called while disabled.
    def _boom():
        raise AssertionError("collect_signals called while layer disabled!")
    monkeypatch.setattr(orchestrator, "collect_signals", _boom)
    pages = []
    import autotrade.alerts as alerts_mod
    monkeypatch.setattr(alerts_mod, "send_urgent_deduped",
                        lambda **k: pages.append(k))
    view = orchestrator.run_once(page=True, persist=True)
    assert view["enabled"] is False
    assert view["status"] == "disabled"
    assert view["signals"] == []
    assert pages == []


def test_current_view_disabled(monkeypatch):
    view = orchestrator.current_view()
    assert view["enabled"] is False
    assert view["status"] == "disabled"


def test_runner_start_noop_when_disabled():
    from autotrade.sysagents import runner
    assert runner.start() is False
    assert runner.is_running() is False


# ── individual monitors ───────────────────────────────────────────────────────
def test_queue_latency_is_na():
    sig = QueueLatencyMonitor().observe()
    assert sig.status == Status.NA


def test_data_freshness_stale_alerts(monkeypatch):
    import falcon.jobs._pipeline as pl
    monkeypatch.setattr(pl, "expected_signal_date_iso", lambda *a, **k: "2026-07-18")
    monkeypatch.setattr(pl, "_latest_emitted_signal_date_iso", lambda: "2026-07-17")
    sig = DataFreshnessMonitor().observe()
    assert sig.status == Status.ALERT

    monkeypatch.setattr(pl, "_latest_emitted_signal_date_iso", lambda: "2026-07-18")
    assert DataFreshnessMonitor().observe().status == Status.OK

    monkeypatch.setattr(pl, "expected_signal_date_iso", lambda *a, **k: None)
    assert DataFreshnessMonitor().observe().status == Status.OK


def test_data_freshness_empty_signals_alerts(monkeypatch):
    import falcon.jobs._pipeline as pl
    monkeypatch.setattr(pl, "expected_signal_date_iso", lambda *a, **k: "2026-07-18")
    monkeypatch.setattr(pl, "_latest_emitted_signal_date_iso", lambda: None)
    assert DataFreshnessMonitor().observe().status == Status.ALERT


def test_market_data_critical_when_stale_in_hours(monkeypatch):
    import autotrade.sysagents.monitors.market_data as md
    monkeypatch.setattr(md, "in_market_hours", lambda *a, **k: True)
    fake = types.ModuleType("kite_ticker")
    fake.status = lambda: {"connected": True, "subscribed_count": 5,
                           "tick_count": 100, "last_tick_at": None}
    # inject a status with a very old tick via age_seconds → patch age_seconds
    monkeypatch.setattr(md, "age_seconds", lambda ts, now=None: 30.0)
    fake_services = types.ModuleType("falcon.trade.services")
    fake_services.kite_ticker = fake
    monkeypatch.setitem(sys.modules, "falcon.trade.services", fake_services)
    sig = MarketDataMonitor().observe()
    assert sig.status == Status.CRITICAL


def test_market_data_na_off_hours(monkeypatch):
    import autotrade.sysagents.monitors.market_data as md
    monkeypatch.setattr(md, "in_market_hours", lambda *a, **k: False)
    fake = types.ModuleType("kite_ticker")
    fake.status = lambda: {"connected": False, "subscribed_count": 0,
                           "tick_count": 0, "last_tick_at": None}
    fake_services = types.ModuleType("falcon.trade.services")
    fake_services.kite_ticker = fake
    monkeypatch.setitem(sys.modules, "falcon.trade.services", fake_services)
    assert MarketDataMonitor().observe().status == Status.NA


def test_broker_health_critical_when_token_invalid_in_hours(monkeypatch):
    import autotrade.sysagents.monitors.broker_health as bh
    monkeypatch.setattr(bh, "in_market_hours", lambda *a, **k: True)
    fake_ka = types.ModuleType("services.kite_auth")
    fake_ka.get_cached_token_status = lambda max_age_sec=120.0: {
        "valid": False, "token_source": "db"}
    fake_services = types.ModuleType("services")
    fake_services.kite_auth = fake_ka
    monkeypatch.setitem(sys.modules, "services", fake_services)
    monkeypatch.setitem(sys.modules, "services.kite_auth", fake_ka)
    sig = BrokerHealthMonitor().observe()
    assert sig.status == Status.CRITICAL


def test_broker_health_unknown_on_cold_cache(monkeypatch):
    import autotrade.sysagents.monitors.broker_health as bh
    monkeypatch.setattr(bh, "in_market_hours", lambda *a, **k: True)
    fake_ka = types.ModuleType("services.kite_auth")
    fake_ka.get_cached_token_status = lambda max_age_sec=120.0: None
    fake_services = types.ModuleType("services")
    fake_services.kite_auth = fake_ka
    monkeypatch.setitem(sys.modules, "services", fake_services)
    monkeypatch.setitem(sys.modules, "services.kite_auth", fake_ka)
    assert BrokerHealthMonitor().observe().status == Status.UNKNOWN


def test_execution_quality_alerts_on_exit_failed(monkeypatch):
    import autotrade.sysagents.monitors.execution_quality as eq
    routes = [
        ("autotrade_slippage", []),
        ("event_type='REJECTED'", [{"n": 0}]),
        ("status='EXIT_FAILED'", [{"n": 2}]),
    ]
    monkeypatch.setattr(eq, "falcon_conn", _fake_conn_factory(routes))
    sig = ExecutionQualityMonitor().observe()
    assert sig.status == Status.ALERT
    assert sig.metrics["n_exit_failed_open"] == 2


def test_execution_quality_ok_when_clean(monkeypatch):
    import autotrade.sysagents.monitors.execution_quality as eq
    routes = [
        ("autotrade_slippage", [{"slippage_pct": 0.1}, {"slippage_pct": -0.2}]),
        ("event_type='REJECTED'", [{"n": 0}]),
        ("status='EXIT_FAILED'", [{"n": 0}]),
    ]
    monkeypatch.setattr(eq, "falcon_conn", _fake_conn_factory(routes))
    assert ExecutionQualityMonitor().observe().status == Status.OK


def test_risk_rms_critical_on_unacked_tier3(monkeypatch):
    import autotrade.sysagents.monitors.risk_rms as rr
    routes = [
        ("autotrade_recon_alerts", [{"n": 0}]),
        ("status='EXIT_FAILED'", [{"n": 0}]),
        ("autotrade_alerts", [{"n": 1}]),
    ]
    monkeypatch.setattr(rr, "falcon_conn", _fake_conn_factory(routes))
    sig = RiskRmsMonitor().observe()
    assert sig.status == Status.CRITICAL


def test_risk_rms_ok_when_clean(monkeypatch):
    import autotrade.sysagents.monitors.risk_rms as rr
    routes = [
        ("autotrade_recon_alerts", [{"n": 0}]),
        ("status='EXIT_FAILED'", [{"n": 0}]),
        ("autotrade_alerts", [{"n": 0}]),
    ]
    monkeypatch.setattr(rr, "falcon_conn", _fake_conn_factory(routes))
    assert RiskRmsMonitor().observe().status == Status.OK


def test_trading_stats_observational(monkeypatch):
    import autotrade.sysagents.monitors.trading_stats as ts
    routes = [("autotrade_positions",
               [{"realised_pnl": 100.0}, {"realised_pnl": -40.0}])]
    monkeypatch.setattr(ts, "falcon_conn", _fake_conn_factory(routes))
    sig = TradingStatsMonitor().observe()
    assert sig.status == Status.OK          # observational, never pages
    assert sig.metrics["closed_trades_today"] == 2
    assert sig.metrics["win_rate_today"] == 0.5


def test_agent_watcher_flags_errored_monitor():
    sigs = [
        _sig("broker-health", Status.OK, "ok"),
        HealthSignal(subsystem="market-data", status=Status.UNKNOWN,
                     summary="monitor error: RuntimeError"),
    ]
    sig = AgentWatcherMonitor().observe(context={"signals": sigs})
    assert sig.status == Status.WARN
    assert "market-data" in sig.metrics["errored"]


def test_agent_watcher_ok_full_batch():
    sigs = [_sig(f"sub{i}", Status.OK) for i in range(8)]
    sig = AgentWatcherMonitor().observe(context={"signals": sigs})
    assert sig.status == Status.OK


def test_safe_observe_never_raises():
    class _Boom(ExecutionQualityMonitor):
        def observe(self, context=None):
            raise RuntimeError("kaboom")
    sig = _Boom().safe_observe()
    assert sig.status == Status.UNKNOWN
    assert sig.summary.startswith("monitor error:")
