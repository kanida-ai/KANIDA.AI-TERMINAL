"""Tests for power_user schema init — idempotency + completeness."""
from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest

from power_user.db_init import (
    EXPECTED_INDICES,
    EXPECTED_TABLES,
    init_power_user_schema,
    reset_power_user_schema,
)


@pytest.fixture
def tmp_db():
    """Empty SQLite file. Auto-cleaned after the test."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


class TestInitPowerUserSchema:

    def test_fresh_db_creates_all_tables(self, tmp_db):
        m = init_power_user_schema(tmp_db)
        assert m["ok"] is True
        assert set(m["tables_present"])  == set(EXPECTED_TABLES)
        assert m["tables_missing"]       == []

    def test_fresh_db_creates_all_indices(self, tmp_db):
        m = init_power_user_schema(tmp_db)
        assert set(m["indices_present"]) == set(EXPECTED_INDICES)
        assert m["indices_missing"]      == []

    def test_idempotent(self, tmp_db):
        """Running init twice on the same DB must not raise + result identical."""
        m1 = init_power_user_schema(tmp_db)
        m2 = init_power_user_schema(tmp_db)
        assert m1["ok"] and m2["ok"]
        assert m1["tables_present"]  == m2["tables_present"]
        assert m1["indices_present"] == m2["indices_present"]

    def test_table_has_expected_columns(self, tmp_db):
        """Spot-check: power_user_users has the right columns."""
        init_power_user_schema(tmp_db)
        with sqlite3.connect(tmp_db) as con:
            cols = [r[1] for r in con.execute("PRAGMA table_info(power_user_users)")]
        expected = {"id","email","google_sub","display_name","picture_url",
                    "invite_code","role","is_active","created_at","last_seen_at"}
        assert expected.issubset(set(cols)), f"missing cols: {expected - set(cols)}"

    def test_falcon_live_decisions_unique_constraint(self, tmp_db):
        """The (entry_date, cycle, rank) uniqueness drives the UPSERT pattern."""
        init_power_user_schema(tmp_db)
        with sqlite3.connect(tmp_db) as con:
            con.execute("""
                INSERT INTO falcon_live_decisions
                  (signal_date, entry_date, cycle, rank, symbol, computed_at)
                VALUES (?,?,?,?,?,?)
            """, ("2026-05-14", "2026-05-15", "0930", 1, "HFCL", "2026-05-15T09:30:30+05:30"))
            with pytest.raises(sqlite3.IntegrityError):
                con.execute("""
                    INSERT INTO falcon_live_decisions
                      (signal_date, entry_date, cycle, rank, symbol, computed_at)
                    VALUES (?,?,?,?,?,?)
                """, ("2026-05-14", "2026-05-15", "0930", 1, "VEDL", "..."))

    def test_falcon_replay_cache_pk_on_replay_date(self, tmp_db):
        init_power_user_schema(tmp_db)
        with sqlite3.connect(tmp_db) as con:
            con.execute("""
                INSERT INTO falcon_replay_cache (replay_date, payload_json, computed_at)
                VALUES (?,?,?)
            """, ("2026-04-15", "{}", "2026-05-14T00:00:00+05:30"))
            with pytest.raises(sqlite3.IntegrityError):
                con.execute("""
                    INSERT INTO falcon_replay_cache (replay_date, payload_json, computed_at)
                    VALUES (?,?,?)
                """, ("2026-04-15", "{}", "..."))

    def test_users_email_uniqueness(self, tmp_db):
        init_power_user_schema(tmp_db)
        with sqlite3.connect(tmp_db) as con:
            con.execute("""
                INSERT INTO power_user_users (email, google_sub, created_at)
                VALUES (?,?,?)
            """, ("user@example.com", "sub1", "2026-05-14T00:00:00+05:30"))
            with pytest.raises(sqlite3.IntegrityError):
                con.execute("""
                    INSERT INTO power_user_users (email, google_sub, created_at)
                    VALUES (?,?,?)
                """, ("user@example.com", "sub2", "..."))


class TestCheckConstraints:
    """CHECK constraints catch data corruption at write-time. Operator-requested
    after Sprint 1 review (2026-05-14). If a future migration drops them,
    these tests break loudly."""

    def test_role_check_rejects_unknown_value(self, tmp_db):
        init_power_user_schema(tmp_db)
        with sqlite3.connect(tmp_db) as con:
            con.execute("""
                INSERT INTO power_user_users (email, google_sub, role, created_at)
                VALUES ('valid@example.com', 'sub_v', 'user', 'now')
            """)
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
                con.execute("""
                    INSERT INTO power_user_users (email, google_sub, role, created_at)
                    VALUES ('bad@example.com', 'sub_b', 'BOGUS_ROLE', 'now')
                """)

    def test_role_check_accepts_three_valid_values(self, tmp_db):
        init_power_user_schema(tmp_db)
        with sqlite3.connect(tmp_db) as con:
            for i, role in enumerate(("user", "partner", "admin")):
                con.execute("""
                    INSERT INTO power_user_users (email, google_sub, role, created_at)
                    VALUES (?, ?, ?, 'now')
                """, (f"u{i}@x", f"sub{i}", role))

    def test_is_active_check_rejects_non_boolean_int(self, tmp_db):
        init_power_user_schema(tmp_db)
        with sqlite3.connect(tmp_db) as con:
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
                con.execute("""
                    INSERT INTO power_user_users (email, google_sub, is_active, created_at)
                    VALUES ('a@b', 's', 2, 'now')
                """)

    def test_cycle_check_rejects_unknown_value(self, tmp_db):
        init_power_user_schema(tmp_db)
        with sqlite3.connect(tmp_db) as con:
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
                con.execute("""
                    INSERT INTO falcon_live_decisions
                      (signal_date, entry_date, cycle, rank, symbol, computed_at)
                    VALUES ('2026-05-14','2026-05-15','BOGUS_CYCLE',1,'X','t')
                """)

    def test_cycle_check_accepts_three_valid_values(self, tmp_db):
        init_power_user_schema(tmp_db)
        with sqlite3.connect(tmp_db) as con:
            for cy in ("0930", "0945", "1000"):
                con.execute("""
                    INSERT INTO falcon_live_decisions
                      (signal_date, entry_date, cycle, rank, symbol, computed_at)
                    VALUES ('2026-05-14','2026-05-15',?,?,'X','t')
                """, (cy, hash(cy) % 1000))   # unique rank per cycle

    def test_action_check_rejects_unknown_value(self, tmp_db):
        init_power_user_schema(tmp_db)
        with sqlite3.connect(tmp_db) as con:
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
                con.execute("""
                    INSERT INTO falcon_live_decisions
                      (signal_date, entry_date, cycle, rank, symbol, action, computed_at)
                    VALUES ('2026-05-14','2026-05-15','0930',1,'X','HOLD','t')
                """)

    def test_action_check_accepts_three_valid_values(self, tmp_db):
        init_power_user_schema(tmp_db)
        with sqlite3.connect(tmp_db) as con:
            for i, a in enumerate(("ENTER", "WAIT", "SKIP")):
                con.execute("""
                    INSERT INTO falcon_live_decisions
                      (signal_date, entry_date, cycle, rank, symbol, action, computed_at)
                    VALUES ('2026-05-14','2026-05-15','0930',?,'X',?,'t')
                """, (i + 1, a))

    def test_tier_check_rejects_unknown_value(self, tmp_db):
        init_power_user_schema(tmp_db)
        with sqlite3.connect(tmp_db) as con:
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
                con.execute("""
                    INSERT INTO falcon_live_decisions
                      (signal_date, entry_date, cycle, rank, symbol, tier, computed_at)
                    VALUES ('2026-05-14','2026-05-15','0930',1,'X','MEGA-ELITE','t')
                """)


class TestReset:

    def test_reset_requires_confirmation(self, tmp_db):
        init_power_user_schema(tmp_db)
        with pytest.raises(PermissionError):
            reset_power_user_schema(tmp_db)
        with pytest.raises(PermissionError):
            reset_power_user_schema(tmp_db, confirm="yes")

    def test_reset_with_confirmation_drops_tables(self, tmp_db):
        init_power_user_schema(tmp_db)
        n = reset_power_user_schema(tmp_db, confirm="I_KNOW_THIS_DROPS_USER_DATA")
        assert n == len(EXPECTED_TABLES)
        # Re-init should still work
        m = init_power_user_schema(tmp_db)
        assert m["ok"] is True
