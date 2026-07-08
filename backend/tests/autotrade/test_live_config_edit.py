"""LIVE CONFIG EDIT — hot-reload of the whitelisted risk/exit knobs on a RUNNING
session/campaign, and the PATCH .../config endpoints.

Proves:
  * TradingSession.maybe_reload_config() picks up a config_version bump and copies
    ONLY the whitelisted risk/exit fields onto self.config, leaving invested_basis,
    trail_armed/trail_peak, and open positions UNTOUCHED; an unchanged version is a
    no-op (no re-parse).
  * session.tick() hot-reloads before the trail/kill decision.
  * PATCH /session/{id}/config: whitelisted change applies + version bumps; a
    locked field → 400; the RUNNING gate (409 on CLOSED); dry_run returns warnings
    without persisting; the immediate-exit warning fires past the new stop.
  * PATCH /ladder/{id}/config: a risk knob cascades to running children (their
    config_version bumped + config_json merged) AND updates the template; a capital
    change updates the ladder row but NOT a running child's capital.

All paper-safe (no real broker / Kite / orders).
"""
import asyncio
import json

import pytest
from fastapi import HTTPException

from autotrade.api.autotrade_routes import Caller
from autotrade.api import config_edit_routes as api
import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig
from autotrade.ladder import LadderCampaign
from autotrade.session import TradingSession
from falcon.db import falcon_conn
from tests.autotrade.mock_broker import MockBroker


ADMIN = Caller(user_id="tester", is_admin=True, authenticated=True)


@pytest.fixture
def mock_brokers(monkeypatch):
    """Mock build_client so tick() never touches the real kite_auth / prod .env
    (which would leak env vars into later tests). Returns the shared LTP map."""
    shared_ltps = {"A": 100.0, "B": 100.0, "C": 100.0}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=shared_ltps)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return shared_ltps


# ── helpers ───────────────────────────────────────────────────────────────────

def _intraday_cfg(**over):
    kw = dict(
        total_allocated_capital=300_000.0, top_n_stocks=3, sizing_mode="equal",
        strategy="intraday_basket", order_product="CNC",
        per_position_gtt_enabled=False, per_stock_stop_enabled=False,
        square_off_enabled=True, arm_pct=0.05, floor_pct=0.01,
        trail_giveback_pct=0.015, stop_pct=0.03,
        per_position_stop_pct=0.05, per_position_target_pct=0.06,
        square_off_time="15:29:00", mis_square_off_time="15:12:00")
    kw.update(over)
    return TradingSessionConfig(**kw)


def _insert_position(sid, symbol, qty=100, avg=100.0, ltp=100.0):
    with falcon_conn() as con:
        upnl = (ltp - avg) * qty
        con.execute(
            """INSERT INTO autotrade_positions
               (session_id, symbol, qty, avg_price, ltp, unrealised_pnl,
                status, direction, opened_at)
               VALUES (?,?,?,?,?,?, 'OPEN', 'long', '2026-06-25T09:20:00')""",
            (sid, symbol, qty, avg, ltp, upnl))
        con.commit()


def _make_running_session(cfg=None, invested_basis=30_000.0,
                          positions=(("A", 100, 100.0, 100.0),
                                     ("B", 100, 100.0, 100.0),
                                     ("C", 100, 100.0, 100.0)),
                          trail_armed=0, trail_peak=None):
    """Create a session, insert OPEN positions directly, mark it RUNNING + freeze
    invested_basis + set trail state — no broker needed."""
    cfg = cfg or _intraday_cfg()
    sess = TradingSession.create(cfg, mode="paper", user_id="tester")
    sid = sess.session_id
    for sym, qty, avg, ltp in positions:
        _insert_position(sid, sym, qty, avg, ltp)
    with falcon_conn() as con:
        con.execute(
            "UPDATE autotrade_sessions SET status='RUNNING', invested_basis=?, "
            "trail_armed=?, trail_peak=?, started_at='2026-06-25T09:15:00' "
            "WHERE session_id=?",
            (invested_basis, trail_armed, trail_peak, sid))
        con.commit()
    return sid


def _row(sid):
    with falcon_conn() as con:
        return dict(con.execute(
            "SELECT * FROM autotrade_sessions WHERE session_id=?", (sid,)
        ).fetchone())


# ── 1. maybe_reload_config: applies the whitelist, leaves state untouched ──────

def test_maybe_reload_applies_whitelist_and_bumps(clean_positions):
    sid = _make_running_session(trail_armed=1, trail_peak=0.06)
    sess = TradingSession.load(sid)
    assert sess._loaded_config_version == 0
    assert sess.config.arm_pct == 0.05
    assert sess.config.stop_pct == 0.03

    before = _row(sid)
    # Operator edit: change arm_pct + stop_pct in config_json + bump version.
    new_cfg = _intraday_cfg(arm_pct=0.08, stop_pct=0.02)
    with falcon_conn() as con:
        con.execute(
            "UPDATE autotrade_sessions SET config_json=?, config_version=1 "
            "WHERE session_id=?", (new_cfg.to_json(), sid))
        con.commit()

    assert sess.maybe_reload_config() is True
    assert sess.config.arm_pct == 0.08          # hot-reloaded
    assert sess.config.stop_pct == 0.02
    assert sess._loaded_config_version == 1

    after = _row(sid)
    # State columns UNTOUCHED by the reload.
    assert after["invested_basis"] == before["invested_basis"]
    assert after["trail_armed"] == before["trail_armed"] == 1
    assert after["trail_peak"] == before["trail_peak"] == 0.06
    # Open positions unchanged.
    assert sess.registry.get_open_positions().__len__() == 3


def test_maybe_reload_noop_when_version_unchanged(clean_positions):
    """Version unchanged → no reload, and config is NOT re-parsed (a config_json
    change without a version bump is IGNORED)."""
    sid = _make_running_session()
    sess = TradingSession.load(sid)
    # Rewrite config_json with a different arm_pct but DO NOT bump config_version.
    sneaky = _intraday_cfg(arm_pct=0.09)
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_sessions SET config_json=? WHERE session_id=?",
                    (sneaky.to_json(), sid))
        con.commit()
    assert sess.maybe_reload_config() is False
    assert sess.config.arm_pct == 0.05          # NOT re-parsed


def test_tick_hot_reloads_before_decision(clean_positions, mock_brokers):
    """session.tick() calls maybe_reload_config FIRST — an edit applied between
    ticks is reflected on self.config after a tick."""
    sid = _make_running_session()
    sess = TradingSession.load(sid)
    new_cfg = _intraday_cfg(arm_pct=0.07, per_position_target_pct=0.09)
    with falcon_conn() as con:
        con.execute(
            "UPDATE autotrade_sessions SET config_json=?, config_version=1 "
            "WHERE session_id=?", (new_cfg.to_json(), sid))
        con.commit()
    asyncio.run(sess.tick())
    assert sess.config.arm_pct == 0.07
    assert sess.config.per_position_target_pct == 0.09
    assert sess._loaded_config_version == 1


# ── 2. PATCH session endpoint ─────────────────────────────────────────────────

def test_patch_session_applies_and_bumps(clean_positions):
    sid = _make_running_session()
    resp = api.patch_session_config(sid, None, {"arm_pct": 0.06, "stop_pct": 0.025},
                                    False, ADMIN)
    assert resp["ok"] is True
    assert resp["applied"] == {"arm_pct": 0.06, "stop_pct": 0.025}
    assert resp["config_version"] == 1
    assert resp["effective_config"]["arm_pct"] == 0.06
    row = _row(sid)
    assert row["config_version"] == 1
    assert json.loads(row["config_json"])["arm_pct"] == 0.06
    assert json.loads(row["config_json"])["stop_pct"] == 0.025


def test_patch_session_rejects_locked_field(clean_positions):
    sid = _make_running_session()
    for locked in ("total_allocated_capital", "order_product", "top_n_stocks"):
        with pytest.raises(HTTPException) as ei:
            api.patch_session_config(sid, None, {locked: 1}, False, ADMIN)
        assert ei.value.status_code == 400
        assert ei.value.detail["field"] == locked
    # Nothing persisted.
    assert _row(sid)["config_version"] == 0


def test_patch_session_running_gate_409(clean_positions):
    sid = _make_running_session()
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_sessions SET status='CLOSED' WHERE session_id=?",
                    (sid,))
        con.commit()
    with pytest.raises(HTTPException) as ei:
        api.patch_session_config(sid, None, {"arm_pct": 0.06}, False, ADMIN)
    assert ei.value.status_code == 409
    assert _row(sid)["config_version"] == 0


def test_patch_session_dry_run_does_not_persist(clean_positions):
    # Position already -6% → a new 4% per-position stop would exit next tick.
    sid = _make_running_session(
        positions=(("A", 100, 100.0, 94.0),   # -6%
                   ("B", 100, 100.0, 100.0),
                   ("C", 100, 100.0, 100.0)))
    resp = api.patch_session_config(
        sid, None, {"per_position_stop_pct": 0.04}, True, ADMIN)
    assert resp["dry_run"] is True
    assert resp["config_version"] == 0                 # unchanged
    assert any("would exit next tick" in w for w in resp["warnings"])
    assert any("A at -6.0%" in w for w in resp["warnings"])
    # NOT persisted.
    row = _row(sid)
    assert row["config_version"] == 0
    assert json.loads(row["config_json"])["per_position_stop_pct"] == 0.05


def test_patch_session_immediate_exit_warning_on_apply(clean_positions):
    sid = _make_running_session(
        positions=(("A", 100, 100.0, 93.0),   # -7%
                   ("B", 100, 100.0, 100.0),
                   ("C", 100, 100.0, 100.0)))
    resp = api.patch_session_config(
        sid, None, {"per_position_stop_pct": 0.05}, False, ADMIN)
    assert resp["config_version"] == 1                 # applied
    assert any("A at -7.0%" in w and "5.00% per-position stop" in w
               for w in resp["warnings"])


def test_patch_session_invalid_range_400(clean_positions):
    sid = _make_running_session()
    with pytest.raises(HTTPException) as ei:
        api.patch_session_config(sid, None, {"arm_pct": 1.0}, False, ADMIN)  # 100% → mis-scaled
    assert ei.value.status_code == 400
    assert _row(sid)["config_version"] == 0


# ── 3. PATCH ladder endpoint ──────────────────────────────────────────────────

def _make_ladder_with_children(n=2):
    lad = LadderCampaign.create(total_capital=300_000.0, order_product="CNC",
                                mode="paper", user_id="tester")
    child_cfg = lad._build_child_config()
    child_ids = []
    for _ in range(n):
        sess = TradingSession.create(child_cfg, mode="paper", user_id="tester")
        with falcon_conn() as con:
            con.execute(
                "UPDATE autotrade_sessions SET status='RUNNING', ladder_id=? "
                "WHERE session_id=?", (lad.ladder_id, sess.session_id))
            con.commit()
        child_ids.append(sess.session_id)
    return lad, child_ids


def test_patch_ladder_risk_cascades_to_children_and_template(clean_positions):
    lad, child_ids = _make_ladder_with_children(2)
    # Baseline: children carry the preset arm_pct (0.03).
    for cid in child_ids:
        assert json.loads(_row(cid)["config_json"])["arm_pct"] == 0.03
        assert _row(cid)["config_version"] == 0

    resp = api.patch_ladder_config(lad.ladder_id, None, {"arm_pct": 0.04}, False, ADMIN)
    assert resp["ok"] is True
    assert set(resp["children_updated"]) == set(child_ids)
    assert resp["config_version"] == 1

    # Every RUNNING child: config_json merged + config_version bumped → its tick
    # will hot-reload.
    for cid in child_ids:
        crow = _row(cid)
        assert json.loads(crow["config_json"])["arm_pct"] == 0.04
        assert crow["config_version"] == 1

    # Template updated for FUTURE spawns.
    lad2 = LadderCampaign.load(lad.ladder_id)
    assert lad2.child_config_overrides()["arm_pct"] == 0.04
    assert lad2._build_child_config().arm_pct == 0.04
    assert lad2.config_version == 1


def test_patch_ladder_capital_updates_row_not_children(clean_positions):
    lad, child_ids = _make_ladder_with_children(2)
    child_caps_before = {cid: _row(cid)["total_allocated_capital"]
                         for cid in child_ids}
    child_ver_before = {cid: _row(cid)["config_version"] for cid in child_ids}

    resp = api.patch_ladder_config(
        lad.ladder_id, None, {"total_capital": 600_000.0}, False, ADMIN)
    assert resp["applied"]["total_capital"] == 600_000.0
    assert resp["children_updated"] == []          # capital never cascades

    lad2 = LadderCampaign.load(lad.ladder_id)
    assert lad2.total_capital == 600_000.0         # ladder row updated
    # Running children: capital + config_version UNTOUCHED.
    for cid in child_ids:
        crow = _row(cid)
        assert crow["total_allocated_capital"] == child_caps_before[cid]
        assert crow["config_version"] == child_ver_before[cid]


def test_patch_ladder_rejects_locked_field(clean_positions):
    lad, _ = _make_ladder_with_children(1)
    with pytest.raises(HTTPException) as ei:
        api.patch_ladder_config(lad.ladder_id, None, {"order_product": "MIS"},
                                False, ADMIN)
    assert ei.value.status_code == 400
    assert ei.value.detail["field"] == "order_product"
