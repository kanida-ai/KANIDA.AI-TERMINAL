"""Tests for power_user.services.web_push — Layer 2 magic-link + subscriptions.

What we cover here:
  - mint_magic_link inserts a row with sane defaults
  - consume_magic_link is atomic (the WHERE used_at IS NULL clause is the
    safety net for the race-condition that DOES happen in prod if two pings
    arrive within ~100ms — Chrome retries are real).
  - save_subscription is idempotent on endpoint (re-subscribe = update, not
    duplicate-key error)
  - deactivate_subscription flips is_active=0 and stores the reason

We do NOT exercise _send_one — that requires a live pywebpush + a real
push service endpoint. notify_auth_needed orchestrates it and is covered
end-to-end manually during 5d smoke test.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.normpath(os.path.join(_HERE, "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from power_user.services import web_push      # noqa: E402
from power_user.db_init import init_power_user_schema    # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))


@pytest.fixture
def tmp_db_con():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_power_user_schema(path)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    yield con
    con.close()
    try:
        os.unlink(path)
    except OSError:
        pass


# ──────────────────────────────────────────────────────────────────────────
# is_configured / public_key — env-var gates
# ──────────────────────────────────────────────────────────────────────────

def test_is_configured_false_when_unset(monkeypatch):
    monkeypatch.delenv("VAPID_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("VAPID_PUBLIC_KEY",  raising=False)
    assert web_push.is_configured() is False
    assert web_push.public_key() == ""


def test_is_configured_true_when_both_set(monkeypatch):
    monkeypatch.setenv("VAPID_PRIVATE_KEY", "pk")
    monkeypatch.setenv("VAPID_PUBLIC_KEY",  "Bk1234")
    assert web_push.is_configured() is True
    assert web_push.public_key() == "Bk1234"


def test_is_configured_false_when_only_one_set(monkeypatch):
    monkeypatch.setenv("VAPID_PRIVATE_KEY", "pk")
    monkeypatch.delenv("VAPID_PUBLIC_KEY",  raising=False)
    assert web_push.is_configured() is False


# ──────────────────────────────────────────────────────────────────────────
# save_subscription — idempotent on endpoint
# ──────────────────────────────────────────────────────────────────────────

def test_save_subscription_inserts_new_row(tmp_db_con):
    sub_id = web_push.save_subscription(
        tmp_db_con, user_id=None,
        endpoint="https://fcm.example/abc", p256dh="p1", auth="a1",
        user_agent="Chrome/x"
    )
    assert sub_id > 0
    rows = tmp_db_con.execute(
        "SELECT user_id, endpoint, p256dh, auth, is_active FROM power_user_push_subscriptions"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["endpoint"] == "https://fcm.example/abc"
    assert rows[0]["is_active"] == 1


def test_save_subscription_reactivates_existing_endpoint(tmp_db_con):
    """Re-subscribing same endpoint must NOT raise UNIQUE-constraint error;
    instead it should update p256dh/auth and bump is_active back to 1."""
    web_push.save_subscription(
        tmp_db_con, user_id=None,
        endpoint="https://fcm.example/abc", p256dh="old_p", auth="old_a"
    )
    web_push.deactivate_subscription(tmp_db_con, "https://fcm.example/abc", "test")
    # Now re-subscribe with rotated keys
    web_push.save_subscription(
        tmp_db_con, user_id=42,
        endpoint="https://fcm.example/abc",
        p256dh="new_p", auth="new_a", user_agent="reactivated"
    )

    rows = tmp_db_con.execute(
        "SELECT user_id, p256dh, auth, is_active FROM power_user_push_subscriptions"
    ).fetchall()
    assert len(rows) == 1   # idempotent — no second row
    assert rows[0]["user_id"]   == 42
    assert rows[0]["p256dh"]    == "new_p"
    assert rows[0]["auth"]      == "new_a"
    assert rows[0]["is_active"] == 1


def test_list_active_subscriptions_skips_inactive(tmp_db_con):
    web_push.save_subscription(tmp_db_con, user_id=None,
                                endpoint="https://e1", p256dh="p", auth="a")
    web_push.save_subscription(tmp_db_con, user_id=None,
                                endpoint="https://e2", p256dh="p", auth="a")
    web_push.deactivate_subscription(tmp_db_con, "https://e2", "GONE")
    active = web_push.list_active_subscriptions(tmp_db_con)
    assert {s["endpoint"] for s in active} == {"https://e1"}


def test_deactivate_records_reason(tmp_db_con):
    web_push.save_subscription(tmp_db_con, user_id=None,
                                endpoint="https://e1", p256dh="p", auth="a")
    web_push.deactivate_subscription(tmp_db_con, "https://e1",
                                       "WebPush 410: subscription gone")
    row = tmp_db_con.execute(
        "SELECT is_active, last_send_error FROM power_user_push_subscriptions "
        "WHERE endpoint = ?", ("https://e1",)
    ).fetchone()
    assert row["is_active"] == 0
    assert "410" in row["last_send_error"]


# ──────────────────────────────────────────────────────────────────────────
# mint_magic_link / consume_magic_link — atomic single-use
# ──────────────────────────────────────────────────────────────────────────

def test_mint_inserts_unused_row(tmp_db_con):
    token = web_push.mint_magic_link(tmp_db_con, kind="admin_auth_refresh",
                                       issued_for="admin")
    assert isinstance(token, str) and len(token) > 16
    row = tmp_db_con.execute(
        "SELECT kind, used_at, issued_for FROM power_user_magic_links WHERE token = ?",
        (token,)
    ).fetchone()
    assert row["kind"] == "admin_auth_refresh"
    assert row["used_at"] is None
    assert row["issued_for"] == "admin"


def test_consume_success_marks_used(tmp_db_con):
    token = web_push.mint_magic_link(tmp_db_con)
    res = web_push.consume_magic_link(tmp_db_con, token,
                                       kind="admin_auth_refresh", ip_hash="iphash123")
    assert res == {"ok": True, "reason": None}

    row = tmp_db_con.execute(
        "SELECT used_at, used_ip_hash FROM power_user_magic_links WHERE token = ?",
        (token,)
    ).fetchone()
    assert row["used_at"] is not None
    assert row["used_ip_hash"] == "iphash123"


def test_consume_second_time_returns_ALREADY_USED(tmp_db_con):
    """Single-use atomic semantics — the second consumer must lose."""
    token = web_push.mint_magic_link(tmp_db_con)
    first  = web_push.consume_magic_link(tmp_db_con, token, kind="admin_auth_refresh")
    second = web_push.consume_magic_link(tmp_db_con, token, kind="admin_auth_refresh")
    assert first["ok"]  is True
    assert second["ok"] is False
    assert second["reason"] == "ALREADY_USED"


def test_consume_unknown_token_returns_NOT_FOUND(tmp_db_con):
    res = web_push.consume_magic_link(tmp_db_con, "no_such_token",
                                        kind="admin_auth_refresh")
    assert res == {"ok": False, "reason": "NOT_FOUND"}


def test_consume_wrong_kind_returns_WRONG_KIND(tmp_db_con):
    token = web_push.mint_magic_link(tmp_db_con, kind="admin_auth_refresh")
    res = web_push.consume_magic_link(tmp_db_con, token, kind="some_other_kind")
    assert res["ok"] is False
    assert res["reason"] == "WRONG_KIND"
    # And the token must still be unused — wrong-kind is NOT a consumption
    row = tmp_db_con.execute(
        "SELECT used_at FROM power_user_magic_links WHERE token = ?", (token,)
    ).fetchone()
    assert row["used_at"] is None


def test_consume_expired_token_returns_EXPIRED(tmp_db_con):
    """Manually backdate the expires_at so the link is already stale."""
    token = web_push.mint_magic_link(tmp_db_con)
    past = (datetime.now(IST) - timedelta(minutes=20)).isoformat()
    tmp_db_con.execute(
        "UPDATE power_user_magic_links SET expires_at = ? WHERE token = ?",
        (past, token)
    )
    tmp_db_con.commit()

    res = web_push.consume_magic_link(tmp_db_con, token, kind="admin_auth_refresh")
    assert res["ok"] is False
    assert res["reason"] == "EXPIRED"


def test_consume_ttl_is_fifteen_minutes(tmp_db_con):
    """Operator spec: magic links expire 15 minutes after issue."""
    token = web_push.mint_magic_link(tmp_db_con)
    row = tmp_db_con.execute(
        "SELECT created_at, expires_at FROM power_user_magic_links WHERE token = ?",
        (token,)
    ).fetchone()
    created = datetime.fromisoformat(row["created_at"])
    expires = datetime.fromisoformat(row["expires_at"])
    delta_min = (expires - created).total_seconds() / 60
    assert 14.9 <= delta_min <= 15.1
