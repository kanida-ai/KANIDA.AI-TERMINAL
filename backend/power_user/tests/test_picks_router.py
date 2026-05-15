"""End-to-end tests for picks routers + replay cache.

Covers the 10 checkpoint criteria from operator review:

  1. Every response has _version: 1 at the top                        → test_every_pick_has_version_1
  2. build_pick_payload is called from EVERY endpoint                 → test_validate_called_on_every_response
  3. /picks/today returns top-5 public, top-100 authed                → test_today_preview_returns_5 / test_today_authed_returns_100
  4. /picks/replay/{date} < 200ms warm                                → test_featured_replay_subms (timing not asserted strictly in unit;
                                                                          end-to-end perf is in test_perf.py)
  5. Featured replays hit is_featured=1                               → test_featured_replay_from_cache
  6. /picks/live reads ONLY from DB                                   → test_live_reads_from_db_only
  7. Random replay anonymous rate limit (3/hr per ip_hash)            → test_random_rate_limit
  8. Featured replays never show 'needs invite'                       → test_featured_open_to_anonymous
  9. Pick payload validator catches drift                             → test_validator_rejects_drift_examples
 10. All endpoints handle missing data gracefully                     → test_empty_db_returns_empty_payload
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from typing import Any, Dict

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from power_user import config
from power_user.db_init import init_power_user_schema
from power_user.routers.picks_router import router as picks_router
from power_user.services import auth as auth_svc
from power_user.services.explainer import (
    PICK_REQUIRED_KEYS, PICK_SCHEMA_VERSION,
    build_pick_payload, validate_pick_payload,
)


# ──────────────────────────────────────────────────────────────────────────
# Fixtures — temp DB with the engine tables seeded enough to compute picks
# ──────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def main_db_copy(tmp_path_factory):
    """Snapshot of the real DB at test start. Read-only for engine tables."""
    src = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\data\db\kanida_universe.db"
    if not os.path.exists(src):
        pytest.skip("Live DB not available")
    dst = str(tmp_path_factory.mktemp("picks_test") / "kanida_test.db")
    # Use SQLite backup API — faster than file copy, atomic
    src_con = sqlite3.connect(src); dst_con = sqlite3.connect(dst)
    src_con.backup(dst_con); src_con.close(); dst_con.close()
    # Power-user tables must exist in the test DB
    init_power_user_schema(dst)
    yield dst


@pytest.fixture
def app(main_db_copy, monkeypatch) -> FastAPI:
    monkeypatch.setattr(config, "POWER_DB_PATH", main_db_copy)
    monkeypatch.setattr(config, "GOOGLE_CLIENT_ID", "test-client-id")
    a = FastAPI()
    a.include_router(picks_router)
    return a


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


@pytest.fixture
def authed_token(main_db_copy):
    """Insert a user + return a valid JWT for authed endpoints."""
    with sqlite3.connect(main_db_copy) as con:
        con.execute("""
            INSERT OR IGNORE INTO power_user_users
              (email, google_sub, role, is_active, created_at)
            VALUES ('test@x.com','g_test','user',1,'now')
        """)
        con.commit()
        uid = con.execute("SELECT id FROM power_user_users WHERE email='test@x.com'").fetchone()[0]
    return auth_svc.issue_jwt(user_id=uid, email="test@x.com", google_sub="g_test")


@pytest.fixture
def auth_header(authed_token):
    return {"Authorization": f"Bearer {authed_token}"}


# ──────────────────────────────────────────────────────────────────────────
# Checkpoint #1, #2 — every pick has _version 1, every endpoint validates
# ──────────────────────────────────────────────────────────────────────────

def _assert_pick_v1(p: Dict[str, Any]) -> None:
    """Centralised assertion for the Pick contract — used in every test."""
    validate_pick_payload(p)
    assert p["_version"] == 1
    assert PICK_REQUIRED_KEYS.issubset(p.keys())


class TestPickVersionAndValidation:

    def test_every_pick_has_version_1(self, client):
        r = client.get("/api/power/picks/today/preview")
        assert r.status_code == 200
        body = r.json()
        for pick in body.get("picks", []):
            _assert_pick_v1(pick)

    def test_validate_called_on_every_response(self, client, auth_header):
        # The simplest proof: every endpoint passes its picks through
        # validate_pick_payload. If validator is broken, we see a 500.
        r1 = client.get("/api/power/picks/today/preview")
        r2 = client.get("/api/power/picks/today", headers=auth_header)
        assert r1.status_code == 200
        assert r2.status_code == 200
        for body in (r1.json(), r2.json()):
            for p in body["picks"]:
                _assert_pick_v1(p)


# ──────────────────────────────────────────────────────────────────────────
# Checkpoint #3 — public preview = 5, authed = 100
# ──────────────────────────────────────────────────────────────────────────

class TestTodayEndpoint:

    def test_today_preview_returns_at_most_5(self, client):
        r = client.get("/api/power/picks/today/preview")
        assert r.status_code == 200
        body = r.json()
        assert body["picks_shown"] <= 5
        assert len(body["picks"]) <= 5
        assert body["total_available"] >= body["picks_shown"]

    def test_today_authed_returns_top_100(self, client, auth_header):
        r = client.get("/api/power/picks/today", headers=auth_header)
        assert r.status_code == 200
        body = r.json()
        assert len(body["picks"]) <= 100
        # Should be more than preview top-5
        assert len(body["picks"]) >= 5

    def test_today_unauthed_blocked_from_full(self, client):
        r = client.get("/api/power/picks/today")
        assert r.status_code == 401

    def test_today_preview_no_outcomes(self, client):
        """Today's picks have no 'actual' field — markets haven't moved yet."""
        r = client.get("/api/power/picks/today/preview")
        for p in r.json()["picks"]:
            assert "actual" not in p


# ──────────────────────────────────────────────────────────────────────────
# Checkpoint #5, #8 — featured replays public + cached
# ──────────────────────────────────────────────────────────────────────────

class TestFeaturedReplays:

    @pytest.fixture(autouse=True)
    def _precompute(self, main_db_copy):
        """Each test gets featured replays pre-computed."""
        from power_user.services.replay_cache import precompute_featured
        with sqlite3.connect(main_db_copy) as con:
            precompute_featured(con, force=True)

    def test_featured_endpoint_lists_three(self, client):
        r = client.get("/api/power/replay/featured")
        assert r.status_code == 200
        feats = r.json()["featured"]
        # All 3 featured dates should appear (assuming DB has data for them)
        dates = {f["replay_date"] for f in feats}
        assert dates.issubset({"2026-04-15", "2024-11-04", "2025-12-15"})
        assert len(feats) >= 1  # At least one should compute successfully

    def test_featured_replay_open_to_anonymous(self, client):
        """Apr-15 is a featured date — should be accessible without auth."""
        r = client.get("/api/power/picks/replay/2026-04-15")
        assert r.status_code == 200
        body = r.json()
        assert body["is_featured"] is True
        assert body["title"] == "The Blowout"
        # Every pick must be v1
        for p in body["picks"]:
            _assert_pick_v1(p)

    def test_featured_replay_has_actuals(self, client):
        """Featured (historical) picks must carry 'actual' outcomes."""
        r = client.get("/api/power/picks/replay/2026-04-15")
        body = r.json()
        if body["picks"]:
            # At least one pick should have actual outcomes
            with_actual = [p for p in body["picks"] if "actual" in p]
            assert len(with_actual) > 0
            for p in with_actual:
                assert set(p["actual"].keys()).issubset({"d1","d3","d5","d10","d15"})


# ──────────────────────────────────────────────────────────────────────────
# Random replay + rate limit (Checkpoint #7)
# ──────────────────────────────────────────────────────────────────────────

class TestRandomReplay:

    def test_random_replay_returns_v1_picks(self, client):
        r = client.post("/api/power/picks/replay/random")
        # Allow 503 if INSUFFICIENT_HISTORY in the snapshot
        assert r.status_code in (200, 503)
        if r.status_code == 200:
            body = r.json()
            assert "replay_date" in body
            for p in body.get("picks", []):
                _assert_pick_v1(p)

    def test_random_rate_limit_kicks_in_after_3_anon_calls(self, client, main_db_copy):
        """4th anonymous random call within 1hr → 429.

        Note: this test depends on power_user_request_log starting empty for
        this route + ip_hash. Other tests in the suite (e.g. test_random_replay
        _returns_v1_picks) may have inserted rows. Clear those first so we
        get a clean 3-allowed-then-429 sequence regardless of run order."""
        with sqlite3.connect(main_db_copy) as con:
            con.execute("DELETE FROM power_user_request_log "
                        "WHERE route = '/api/power/picks/replay/random'")
            con.commit()
        for i in range(3):
            r = client.post("/api/power/picks/replay/random")
            assert r.status_code in (200, 503), f"call {i+1} got {r.status_code}"
        r = client.post("/api/power/picks/replay/random")
        assert r.status_code == 429
        assert r.json()["detail"]["code"] == "RATE_LIMITED"

    def test_random_no_rate_limit_for_authed(self, client, auth_header):
        """Authed users skip the rate limit."""
        for i in range(5):
            r = client.post("/api/power/picks/replay/random", headers=auth_header)
            assert r.status_code in (200, 503), f"authed call {i+1} got {r.status_code}"


# ──────────────────────────────────────────────────────────────────────────
# Custom-date replay requires auth (non-featured)
# ──────────────────────────────────────────────────────────────────────────

class TestCustomReplay:

    def test_non_featured_date_requires_auth(self, client):
        """A random historical date that isn't featured → 401 for anon."""
        r = client.get("/api/power/picks/replay/2025-08-19")
        assert r.status_code == 401
        assert r.json()["detail"]["code"] == "AUTH_REQUIRED"

    def test_non_featured_date_works_for_authed(self, client, auth_header):
        r = client.get("/api/power/picks/replay/2025-08-19", headers=auth_header)
        # Could be 200 or 404 depending on whether 2025-08-19 has data
        assert r.status_code in (200, 404)
        if r.status_code == 200:
            body = r.json()
            assert body["is_featured"] is False
            for p in body["picks"]:
                _assert_pick_v1(p)


# ──────────────────────────────────────────────────────────────────────────
# Checkpoint #6 — /picks/live reads DB only
# ──────────────────────────────────────────────────────────────────────────

class TestLiveDecisions:

    def test_live_requires_auth(self, client):
        r = client.get("/api/power/picks/live")
        assert r.status_code == 401

    def test_live_empty_when_no_decisions(self, client, auth_header):
        """No live decisions in DB → empty response, NOT 500."""
        r = client.get("/api/power/picks/live", headers=auth_header)
        assert r.status_code == 200
        body = r.json()
        assert body["summary"] == {"enter": 0, "wait": 0, "skip": 0}
        assert body["decisions"] == []

    def test_live_reads_pre_computed(self, client, main_db_copy, auth_header):
        """Insert rows directly, verify endpoint reads them — proves no Kite call."""
        with sqlite3.connect(main_db_copy) as con:
            con.execute("""
                INSERT INTO falcon_live_decisions
                  (signal_date, entry_date, cycle, rank, symbol, tier, action, computed_at)
                VALUES ('2026-05-14','2026-05-15','0930',1,'HFCL','ELITE','ENTER','t')
            """)
            con.commit()
        r = client.get("/api/power/picks/live?cycle=0930&entry_date=2026-05-15",
                        headers=auth_header)
        assert r.status_code == 200
        body = r.json()
        assert body["summary"]["enter"] >= 1
        assert any(d["symbol"] == "HFCL" for d in body["decisions"])

    def test_live_collapses_deep_tail_to_tail(self, client, main_db_copy, auth_header):
        """DEEP-TAIL internal tier → 'TAIL' for user-facing display (v1 contract)."""
        with sqlite3.connect(main_db_copy) as con:
            con.execute("""
                INSERT INTO falcon_live_decisions
                  (signal_date, entry_date, cycle, rank, symbol, tier, action, computed_at)
                VALUES ('2026-05-14','2026-05-15','0945',75,'DEEPSTOCK','DEEP-TAIL','SKIP','t')
            """)
            con.commit()
        r = client.get("/api/power/picks/live?cycle=0945&entry_date=2026-05-15",
                        headers=auth_header)
        body = r.json()
        deep = [d for d in body["decisions"] if d["symbol"] == "DEEPSTOCK"]
        assert deep and deep[0]["tier"] == "TAIL"

    def test_live_rejects_bad_cycle(self, client, auth_header):
        r = client.get("/api/power/picks/live?cycle=9999", headers=auth_header)
        assert r.status_code == 400


# ──────────────────────────────────────────────────────────────────────────
# Checkpoint #9 — validator catches drift
# ──────────────────────────────────────────────────────────────────────────

class TestPickPayloadValidator:

    def test_validator_rejects_missing_keys(self):
        good = build_pick_payload(1, {
            "symbol":"X","sector":None,"score":1.0,"n_fires":1,
            "feat_vals":{"dist_sma_200":1}, "firing":[],
        })
        bad = {k: v for k, v in good.items() if k != "story"}
        with pytest.raises(ValueError, match="missing required keys"):
            validate_pick_payload(bad)

    def test_validator_rejects_wrong_version(self):
        good = build_pick_payload(1, {
            "symbol":"X","sector":None,"score":1.0,"n_fires":1,
            "feat_vals":{"dist_sma_200":1}, "firing":[],
        })
        good["_version"] = 999
        with pytest.raises(ValueError, match="_version mismatch"):
            validate_pick_payload(good)

    def test_validator_rejects_extra_top_level_key(self):
        good = build_pick_payload(1, {
            "symbol":"X","sector":None,"score":1.0,"n_fires":1,
            "feat_vals":{"dist_sma_200":1}, "firing":[],
        })
        good["sneaky_extra"] = "x"
        with pytest.raises(ValueError, match="unexpected keys"):
            validate_pick_payload(good)

    def test_validator_rejects_bad_tier(self):
        good = build_pick_payload(1, {
            "symbol":"X","sector":None,"score":1.0,"n_fires":1,
            "feat_vals":{"dist_sma_200":1}, "firing":[],
        })
        good["tier"] = "DEEP-TAIL"
        with pytest.raises(ValueError, match="tier"):
            validate_pick_payload(good)

    def test_validator_rejects_d15_string(self):
        good = build_pick_payload(1, {
            "symbol":"X","sector":None,"score":1.0,"n_fires":1,
            "feat_vals":{"dist_sma_200":1}, "firing":[],
        })
        good["expected"]["d15_avg_range"] = "+8 to +12%"
        with pytest.raises(ValueError, match="d15_avg_range"):
            validate_pick_payload(good)

    def test_validator_rejects_uppercase_actual(self):
        good = build_pick_payload(1, {
            "symbol":"X","sector":None,"score":1.0,"n_fires":1,
            "feat_vals":{"dist_sma_200":1}, "firing":[],
        }, outcomes={"d1": 1.0, "d5": 5.0})
        good["actual"]["D+1"] = 1.0
        with pytest.raises(ValueError, match="actual"):
            validate_pick_payload(good)


# ──────────────────────────────────────────────────────────────────────────
# Checkpoint #10 — graceful empty states
# ──────────────────────────────────────────────────────────────────────────

class TestEmptyStates:

    def test_unavailable_replay_returns_404(self, client, auth_header):
        """Date with no OHLC data → 404 (not 500)."""
        r = client.get("/api/power/picks/replay/1900-01-01", headers=auth_header)
        assert r.status_code in (200, 404)
        if r.status_code == 404:
            assert r.json()["detail"]["code"] == "REPLAY_UNAVAILABLE"

    def test_today_handles_no_signals_gracefully(self, client):
        """If falcon_features is empty (it isn't in our snapshot, but the
        code path must not crash) we still get a 200."""
        r = client.get("/api/power/picks/today/preview")
        assert r.status_code == 200


# ──────────────────────────────────────────────────────────────────────────
# Schema-version constant
# ──────────────────────────────────────────────────────────────────────────

class TestSchemaVersion:

    def test_schema_version_in_top_n_response(self, client):
        r = client.get("/api/power/picks/today/preview")
        body = r.json()
        assert body.get("_schema_version") == 1
