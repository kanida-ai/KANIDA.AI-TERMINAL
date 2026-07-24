"""Cross-process DURABLE CLAIMS — a compare-and-set + lease authority (SPRINT
CLUSTER 8 ITEM 2).

The in-process guards (fire_guard._FIRED / _ENTRY_CLAIMED and the exit_gate
single-flight set) are correct WITHIN one process but are lost on a restart and
invisible to a second process. This module backs those claims with an atomic
SQLite compare-and-set + a lease timestamp so a claim:

  * survives a restart (persisted in autotrade_claims), and
  * holds ACROSS processes (the DB row is the shared authority).

The callers keep the in-process check as the FAST PATH (first check) and use
claim() here as the AUTHORITY: on a fresh process the in-process set is empty, so
the DB decides. A lease that has PASSED is takeable by a new claimant (so a crashed
holder that never released cannot wedge the key forever).

Isolation: writes ONLY autotrade_claims (never falcon_position_state / legacy).
Best-effort by design where noted — a DB error on release never raises into the
hot exit path.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.durable_claims")
IST = timezone(timedelta(hours=5, minutes=30))

# ── WHY POSTGRES MATTERS HERE (the desired_count>1 blocker) ──────────────────
# falcon_conn() is SQLite at FALCON_DB_PATH, which in the cloud is
# /localdb/kanida_universe.db — CONTAINER-LOCAL (entrypoint.sh copies EFS->local
# per task). So this CAS is cross-PROCESS but NOT cross-CONTAINER: with two
# tasks, each claims `entry:<session>` in its OWN file, BOTH win, and BOTH fire
# the 9:15 entry -> DUPLICATE LIVE ORDERS. That is the single reason
# desired_count must stay 1.
#
# Postgres is a genuinely shared authority, so the same CAS becomes correct
# across containers. Gated by KANIDA_PG_CLAIMS (default OFF) so nothing changes
# until it is deliberately enabled; it is INDEPENDENT of full OLTP routing
# because it is the prerequisite for horizontal scale on its own.
# autotrade_claims already exists in PG (migrated in Stage 3).


def _pg_claims() -> bool:
    return os.environ.get("KANIDA_PG_CLAIMS", "").strip().lower() in (
        "1", "true", "yes", "on")

# Lease lengths (seconds). A FIRE / ENTRY claim is effectively one-shot per session
# (a session fires / places entries at most once), so it uses a LONG lease — a
# restart within the window must NOT re-fire / re-enter. The exit single-flight is
# a seconds-to-~minute placement, so a shorter lease reclaims a crashed holder.
FIRE_LEASE_SEC = 6 * 3600
ENTRY_LEASE_SEC = 6 * 3600
EXIT_FLIGHT_LEASE_SEC = 300


def _ensure(con) -> None:
    """Defensive CREATE (the boot migration also creates it) so a standalone test
    DB that skipped the migration still has the table."""
    con.execute(
        """CREATE TABLE IF NOT EXISTS autotrade_claims (
               claim_key    TEXT PRIMARY KEY,
               holder       TEXT,
               leased_until TEXT NOT NULL,
               created_at   TEXT NOT NULL
           )""")


def claim(key: str, lease_sec: float, holder: str | None = None) -> bool:
    """Atomically claim `key` for `lease_sec` seconds. Returns True iff THIS caller
    won (no live claim existed, or the prior lease had expired).

    The CAS is a single INSERT (wins when the key is free) with an expired-lease
    takeover UPDATE on conflict — SQLite serialises writes, so exactly one racing
    caller wins. ISO-IST timestamps compare lexicographically (same tz suffix)."""
    now = datetime.now(IST)
    now_iso = now.isoformat()
    until_iso = (now + timedelta(seconds=lease_sec)).isoformat()

    if _pg_claims():
        # Single-statement CAS. The row is returned ONLY if we inserted (key was
        # free) or the takeover WHERE matched (prior lease expired) — so exactly
        # one racing container wins, across the whole cluster. Any failure
        # propagates: silently falling back to the container-local SQLite claim
        # would re-open the duplicate-fire window this exists to close.
        import pgdb
        with pgdb.pg_conn() as c:
            cur = c.cursor()
            cur.execute(
                "INSERT INTO autotrade_claims(claim_key, holder, leased_until,"
                " created_at) VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (claim_key) DO UPDATE SET holder=EXCLUDED.holder,"
                " leased_until=EXCLUDED.leased_until,"
                " created_at=EXCLUDED.created_at "
                "WHERE autotrade_claims.leased_until < %s "
                "RETURNING claim_key",
                (key, holder, until_iso, now_iso, now_iso))
            return cur.fetchone() is not None

    with falcon_conn() as con:
        _ensure(con)
        try:
            con.execute(
                "INSERT INTO autotrade_claims(claim_key, holder, leased_until, "
                "created_at) VALUES (?,?,?,?)",
                (key, holder, until_iso, now_iso))
            con.commit()
            return True
        except sqlite3.IntegrityError:
            # A row exists — take it over ONLY if its lease has already passed.
            cur = con.execute(
                "UPDATE autotrade_claims SET holder=?, leased_until=?, created_at=? "
                "WHERE claim_key=? AND leased_until < ?",
                (holder, until_iso, now_iso, key, now_iso))
            con.commit()
            return cur.rowcount == 1


def renew(key: str, lease_sec: float, holder: str) -> bool:
    """Extend a lease we ALREADY hold. True iff we still held it.

    claim() deliberately refuses a live lease (that is what stops a second
    container taking over), so a holder needs this to keep its own lease alive.
    Guarded on holder=? so one container can never extend another's lease.
    Returns False if the lease was lost/taken over — the caller must then STOP
    whatever it was doing for that key.
    """
    now = datetime.now(IST)
    until_iso = (now + timedelta(seconds=lease_sec)).isoformat()
    try:
        if _pg_claims():
            import pgdb
            with pgdb.pg_conn() as c:
                cur = c.cursor()
                cur.execute(
                    "UPDATE autotrade_claims SET leased_until=%s "
                    "WHERE claim_key=%s AND holder=%s RETURNING claim_key",
                    (until_iso, key, holder))
                return cur.fetchone() is not None
        with falcon_conn() as con:
            _ensure(con)
            cur = con.execute(
                "UPDATE autotrade_claims SET leased_until=? "
                "WHERE claim_key=? AND holder=?", (until_iso, key, holder))
            con.commit()
            return cur.rowcount == 1
    except Exception as e:  # pragma: no cover - defensive
        log.warning("durable_claims.renew(%s) failed: %s", key, e)
        return False


def release(key: str) -> None:
    """Release `key` (delete the row). Idempotent + best-effort (never raises into
    the exit hot path)."""
    try:
        if _pg_claims():
            import pgdb
            with pgdb.pg_conn() as c:
                c.cursor().execute(
                    "DELETE FROM autotrade_claims WHERE claim_key=%s", (key,))
            return
        with falcon_conn() as con:
            _ensure(con)
            con.execute("DELETE FROM autotrade_claims WHERE claim_key=?", (key,))
            con.commit()
    except Exception as e:  # pragma: no cover - defensive
        log.warning("durable_claims.release(%s) failed: %s", key, e)


def is_claimed(key: str) -> bool:
    """True iff a LIVE (unexpired) claim exists for `key`."""
    now_iso = datetime.now(IST).isoformat()
    if _pg_claims():
        import pgdb
        with pgdb.pg_conn() as c:
            cur = c.cursor()
            cur.execute(
                "SELECT leased_until FROM autotrade_claims WHERE claim_key=%s",
                (key,))
            r = cur.fetchone()
        return bool(r and str(r[0]) > now_iso)
    with falcon_conn() as con:
        _ensure(con)
        r = con.execute(
            "SELECT leased_until FROM autotrade_claims WHERE claim_key=?",
            (key,)).fetchone()
    return bool(r and str(r["leased_until"]) > now_iso)
