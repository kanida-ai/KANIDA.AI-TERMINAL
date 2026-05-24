"""Tests for invites.py — atomic redemption + brute-force resistance.

Critical correctness invariants:
  * One code → one user (race-condition safe via BEGIN IMMEDIATE)
  * Errors give the SAME generic message so attackers can't enumerate
  * Expired codes rejected
  * Already-used codes rejected
  * Bad-format codes rejected (before DB hit)
  * Waitlist idempotent on email
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import List

import pytest

from power_user.db_init import init_power_user_schema
from power_user.services.auth import GoogleUser, verify_jwt
from power_user.services.invites import (
    CODE_RE,
    CodeStatus,
    InviteError,
    add_to_waitlist,
    generate_codes,
    list_codes,
    redeem_atomic,
    validate_code,
)

IST = timezone(timedelta(hours=5, minutes=30))


@pytest.fixture
def tmp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_power_user_schema(path)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


def _g(sub="g_test", email="user@example.com", name="Test User"):
    return GoogleUser(
        google_sub=sub, email=email, email_verified=True,
        display_name=name, picture_url=None,
    )


# ───────────────────────────────────────────────────────────────────
# Code generation
# ───────────────────────────────────────────────────────────────────

class TestGenerateCodes:

    def test_n_codes_match_format(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            issued = generate_codes(con, n=5, issued_by="admin")
        assert len(issued) == 5
        for c in issued:
            assert CODE_RE.match(c.code), f"bad shape: {c.code}"

    def test_codes_are_unique(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            issued = generate_codes(con, n=20)
        assert len({c.code for c in issued}) == 20

    def test_expires_in_days_stored(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            issued = generate_codes(con, n=1, expires_in_days=7)
            assert issued[0].expires_at is not None
            exp = datetime.fromisoformat(issued[0].expires_at)
            delta = exp - datetime.now(IST)
            # Use fractional days — timedelta.days floors to int
            days = delta.total_seconds() / 86400
            assert 6.99 < days < 7.01, f"got {days:.4f} days"

    def test_no_expiry_when_unset(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            issued = generate_codes(con, n=1, expires_in_days=None)
            assert issued[0].expires_at is None

    def test_n_out_of_range_raises(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            with pytest.raises(ValueError):
                generate_codes(con, n=0)
            with pytest.raises(ValueError):
                generate_codes(con, n=101)

    def test_note_field_persisted(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            generate_codes(con, n=2, note="Influencer cohort A")
            codes = list_codes(con)
        assert all(c["note"] == "Influencer cohort A" for c in codes)


# ───────────────────────────────────────────────────────────────────
# validate_code (read-only check)
# ───────────────────────────────────────────────────────────────────

class TestValidateCode:

    def test_valid_unused_code_returns_valid(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            issued = generate_codes(con, n=1)
            status = validate_code(con, issued[0].code)
        assert status.exists and status.is_valid
        assert status.used_by_id is None

    def test_unknown_code_returns_invalid(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            status = validate_code(con, "kn-2026-aaaaaa")
        assert not status.exists and not status.is_valid

    def test_bad_format_returns_invalid_without_db_hit(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            for bad in ("", "abc", "kn-26-xxx", "kn-2026-ZZZZZZ", "x" * 50):
                status = validate_code(con, bad)
                assert not status.is_valid, f"{bad!r} should be invalid"

    def test_expired_code_marked_invalid(self, tmp_db):
        # Insert manually with past expiry
        past = (datetime.now(IST) - timedelta(days=1)).isoformat()
        with sqlite3.connect(tmp_db) as con:
            con.execute("""
                INSERT INTO power_user_invite_codes
                  (code, issued_by, issued_at, expires_at)
                VALUES ('kn-2026-aaaaaa', 'test', '2026-05-01T00:00:00+05:30', ?)
            """, (past,))
            con.commit()
            status = validate_code(con, "kn-2026-aaaaaa")
        assert status.exists and not status.is_valid


# ───────────────────────────────────────────────────────────────────
# Atomic redemption — happy paths + every failure
# ───────────────────────────────────────────────────────────────────

class TestRedeemAtomic:

    def test_happy_path_creates_user_and_marks_code(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            code = generate_codes(con, n=1)[0].code
            result = redeem_atomic(con, code, _g())
            # User exists
            row = con.execute(
                "SELECT id, email, invite_code FROM power_user_users"
            ).fetchone()
            assert row is not None
            assert row[1] == "user@example.com"
            assert row[2] == code
            # Code marked used
            crow = con.execute(
                "SELECT used_by_user_id, used_at FROM power_user_invite_codes WHERE code=?",
                (code,)
            ).fetchone()
            assert crow[0] == row[0]
            assert crow[1] is not None

        # JWT is signed and round-trips
        payload = verify_jwt(result["jwt"])
        assert payload.email == "user@example.com"

    def test_bad_format_raises_format_error(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            with pytest.raises(InviteError) as ei:
                redeem_atomic(con, "garbage", _g())
            assert ei.value.code == "CODE_FORMAT_INVALID"

    def test_unknown_code_raises_not_found(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            with pytest.raises(InviteError) as ei:
                redeem_atomic(con, "kn-2026-aaaaaa", _g())
            assert ei.value.code == "CODE_NOT_FOUND"

    def test_already_used_raises_already_used(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            code = generate_codes(con, n=1)[0].code
            redeem_atomic(con, code, _g(sub="g1", email="first@example.com"))
            with pytest.raises(InviteError) as ei:
                redeem_atomic(con, code, _g(sub="g2", email="second@example.com"))
            assert ei.value.code == "CODE_ALREADY_USED"

    def test_expired_code_raises_expired(self, tmp_db):
        past = (datetime.now(IST) - timedelta(days=1)).isoformat()
        with sqlite3.connect(tmp_db) as con:
            con.execute("""
                INSERT INTO power_user_invite_codes
                  (code, issued_by, issued_at, expires_at)
                VALUES ('kn-2026-bbbbbb', 'test', '2026-05-01T00:00:00+05:30', ?)
            """, (past,))
            con.commit()
            with pytest.raises(InviteError) as ei:
                redeem_atomic(con, "kn-2026-bbbbbb", _g())
            assert ei.value.code == "CODE_EXPIRED"

    def test_existing_user_cant_reclaim_via_code(self, tmp_db):
        """Defense in depth: even with a fresh code, an existing user can't
        create a second account via redeem path."""
        with sqlite3.connect(tmp_db) as con:
            code1 = generate_codes(con, n=2)
            redeem_atomic(con, code1[0].code, _g(sub="g_a", email="alice@x"))
            with pytest.raises(InviteError) as ei:
                redeem_atomic(con, code1[1].code, _g(sub="g_a", email="alice@x"))
            assert ei.value.code == "USER_ALREADY_EXISTS"

    def test_failed_redemption_does_not_leak_state(self, tmp_db):
        """If redemption fails after partial work, NOTHING was committed."""
        with sqlite3.connect(tmp_db) as con:
            # Use an unknown code → should not create a user OR mark anything
            with pytest.raises(InviteError):
                redeem_atomic(con, "kn-2026-cccccc", _g())
            n_users = con.execute("SELECT COUNT(*) FROM power_user_users").fetchone()[0]
        assert n_users == 0


# ───────────────────────────────────────────────────────────────────
# Race condition — two threads, same code
# ───────────────────────────────────────────────────────────────────

class TestConcurrentRedemption:
    """The whole point of BEGIN IMMEDIATE: under concurrency, only ONE
    redemption of a single-use code wins. The other gets CODE_ALREADY_USED."""

    def test_two_concurrent_redemptions_one_succeeds(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            code = generate_codes(con, n=1)[0].code

        results: List = []
        errors:  List[InviteError] = []
        barrier = threading.Barrier(2)

        def worker(sub: str, email: str):
            # Each thread gets its own connection (SQLite + threading requirement)
            c = sqlite3.connect(tmp_db, timeout=10.0)
            try:
                barrier.wait()
                r = redeem_atomic(c, code, _g(sub=sub, email=email))
                results.append(r)
            except InviteError as e:
                errors.append(e)
            finally:
                c.close()

        t1 = threading.Thread(target=worker, args=("g_a", "a@x.com"))
        t2 = threading.Thread(target=worker, args=("g_b", "b@x.com"))
        t1.start(); t2.start()
        t1.join();  t2.join()

        # Exactly one succeeded, one failed
        assert len(results) == 1, f"expected 1 winner, got {len(results)}"
        assert len(errors)  == 1, f"expected 1 loser, got {len(errors)}"
        assert errors[0].code == "CODE_ALREADY_USED"

        # And exactly one user in the DB
        with sqlite3.connect(tmp_db) as con:
            n = con.execute("SELECT COUNT(*) FROM power_user_users").fetchone()[0]
        assert n == 1


# ───────────────────────────────────────────────────────────────────
# Brute-force resistance — same generic outcome regardless of failure mode
# ───────────────────────────────────────────────────────────────────

class TestBruteForceResistance:
    """An attacker probing codes shouldn't be able to distinguish 'not found'
    from 'expired' from 'already used'. The router layer maps every InviteError
    to the same HTTP 400 body. The service raises distinct internal codes
    only for OUR logs / metrics — never returned to user."""

    def test_all_failures_raise_same_exception_class(self, tmp_db):
        """All four failure modes raise InviteError — router maps to one HTTP code.

        Sentinel codes used (must match CODE_RE = ^kn-\\d{4}-[a-f0-9]{6}$):
          'kn-2026-eeeeee' — pre-inserted as expired
          'kn-2026-fafafa' — never inserted (NOT_FOUND)
          (a fresh-then-used code from generate_codes)
        """
        with sqlite3.connect(tmp_db) as con:
            past = (datetime.now(IST) - timedelta(days=1)).isoformat()
            con.execute("""
                INSERT INTO power_user_invite_codes
                  (code, issued_by, issued_at, expires_at)
                VALUES ('kn-2026-eeeeee', 'test', '2026-05-01T00:00:00+05:30', ?)
            """, (past,))
            con.commit()
            used_code = generate_codes(con, n=1)[0].code
            redeem_atomic(con, used_code, _g(sub="g_used", email="used@x"))

            failures = []
            for code, label in (
                ("garbage_format",   "FORMAT"),       # fails regex
                ("kn-2026-fafafa",   "NOT_FOUND"),    # passes regex, not in DB
                ("kn-2026-eeeeee",   "EXPIRED"),
                (used_code,          "USED"),
            ):
                try:
                    redeem_atomic(con, code, _g(sub=f"g_{label}", email=f"{label}@x"))
                except InviteError as e:
                    failures.append(e)

        assert len(failures) == 4
        # All InviteError — uniform exception class (router maps to one HTTP body)
        assert all(isinstance(e, InviteError) for e in failures)
        # But distinct internal codes for our diagnostics / logs
        internal_codes = {e.code for e in failures}
        assert internal_codes == {
            "CODE_FORMAT_INVALID", "CODE_NOT_FOUND", "CODE_EXPIRED", "CODE_ALREADY_USED",
        }


# ───────────────────────────────────────────────────────────────────
# Waitlist
# ───────────────────────────────────────────────────────────────────

class TestWaitlist:

    def test_add_idempotent(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            assert add_to_waitlist(con, "new@example.com")
            assert not add_to_waitlist(con, "new@example.com")   # second call returns False
            n = con.execute("SELECT COUNT(*) FROM power_user_waitlist").fetchone()[0]
        assert n == 1

    def test_lowercases_email(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            add_to_waitlist(con, "  Foo@BAR.com  ")
            row = con.execute("SELECT email FROM power_user_waitlist").fetchone()
        assert row[0] == "foo@bar.com"

    def test_rejects_bad_email(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            assert not add_to_waitlist(con, "")
            assert not add_to_waitlist(con, "no_at_sign")

    def test_source_field_stored(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            add_to_waitlist(con, "x@y.com", source="demo_replay")
            row = con.execute("SELECT source FROM power_user_waitlist").fetchone()
        assert row[0] == "demo_replay"
