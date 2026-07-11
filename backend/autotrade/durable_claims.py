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
import sqlite3
from datetime import datetime, timedelta, timezone

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.durable_claims")
IST = timezone(timedelta(hours=5, minutes=30))

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


def release(key: str) -> None:
    """Release `key` (delete the row). Idempotent + best-effort (never raises into
    the exit hot path)."""
    try:
        with falcon_conn() as con:
            _ensure(con)
            con.execute("DELETE FROM autotrade_claims WHERE claim_key=?", (key,))
            con.commit()
    except Exception as e:  # pragma: no cover - defensive
        log.warning("durable_claims.release(%s) failed: %s", key, e)


def is_claimed(key: str) -> bool:
    """True iff a LIVE (unexpired) claim exists for `key`."""
    now_iso = datetime.now(IST).isoformat()
    with falcon_conn() as con:
        _ensure(con)
        r = con.execute(
            "SELECT leased_until FROM autotrade_claims WHERE claim_key=?",
            (key,)).fetchone()
    return bool(r and str(r["leased_until"]) > now_iso)
