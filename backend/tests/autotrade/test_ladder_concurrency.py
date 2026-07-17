"""LADDER 09:15 ENTRY-LATENCY FIX — atomic day-claim + CONCURRENT ladder firing.

Context (real money): on 2026-07-17 the Magnifier campaign entered 37s after the
open because tick_all_running fired ladders SERIALLY (Kite queued behind a 18.7s
Vortex entry) — a measured Rs36,337 slippage on Rs1.32cr notional.

Parallelising the fire is only safe if daily_tick's per-ladder+day idempotency
guard is ATOMIC. It was NOT (an in-memory read of self.last_tick_date, then an
unconditional UPDATE *after* _spawn_child had already placed the orders) — with
concurrency that becomes a DOUBLE-OPEN RACE: two live baskets, duplicate real
orders (the BRIGADE/double-fill class).

THE SAFETY TEST here is `test_concurrent_daily_tick_same_ladder_opens_exactly_one
_basket`: two genuinely concurrent daily_ticks for the SAME ladder+day must spawn
EXACTLY ONE basket. It is mutation-verified against the pre-fix read-then-write
shape (see test_atomic_claim_is_load_bearing_vs_read_then_write, which simulates
the old guard and proves it double-opens).

All paper-safe: _spawn_child is stubbed or MockBroker-backed; no real orders.
"""
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from autotrade.ladder import LadderCampaign, STATUS_RUNNING
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))


def _trading_day_now():
    # Thu 2026-06-25 10:00 IST — a real NSE trading day, market open.
    return datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


def _running_ladder(total_capital=900000.0):
    lad = LadderCampaign.create(total_capital=total_capital, order_product="CNC")
    lad.start()
    assert lad.status == STATUS_RUNNING
    return lad


def _last_tick_date(ladder_id):
    with falcon_conn() as con:
        r = con.execute("SELECT last_tick_date FROM autotrade_ladders "
                        "WHERE ladder_id=?", (ladder_id,)).fetchone()
    return r["last_tick_date"]


# ── THE SAFETY TEST ───────────────────────────────────────────────────────────

def test_concurrent_daily_tick_same_ladder_opens_exactly_one_basket(
        clean_positions, monkeypatch):
    """Two CONCURRENT daily_ticks, same ladder + same day → EXACTLY ONE basket.

    This is the whole risk of the concurrency change. A duplicated live Rs30L MIS
    basket is far worse than a 37s delay.
    """
    lad = _running_ladder()
    now = _trading_day_now()

    spawn_calls = []
    lock = threading.Lock()

    def slow_spawn(self, today):
        # Hold the (formerly unguarded) window WIDE open: the pre-fix code read
        # self.last_tick_date before this and only stamped it afterwards, so a
        # second thread had ~0.5s to sail straight through and spawn again.
        with lock:
            spawn_calls.append(self.ladder_id)
        self._entry_attempted = True
        time.sleep(0.5)
        return {"opened": True, "session_id": "sess-%d" % len(spawn_calls),
                "reason": "opened 5 legs"}

    monkeypatch.setattr(LadderCampaign, "_spawn_child", slow_spawn)

    # Two INDEPENDENTLY-LOADED instances (exactly what two scheduler threads /
    # the boot resume + the 09:15 beat would each hold).
    lad_a = LadderCampaign.load(lad.ladder_id)
    lad_b = LadderCampaign.load(lad.ladder_id)
    barrier = threading.Barrier(2)
    results = {}

    def run(name, l):
        barrier.wait()  # force a genuine simultaneous arrival
        results[name] = l.daily_tick(ref_now=now)

    ta = threading.Thread(target=run, args=("a", lad_a))
    tb = threading.Thread(target=run, args=("b", lad_b))
    ta.start(); tb.start(); ta.join(10); tb.join(10)

    # EXACTLY ONE basket opened. Not two.
    assert len(spawn_calls) == 1, (
        f"DOUBLE-OPEN: _spawn_child ran {len(spawn_calls)}x for one ladder+day")
    opened = [r for r in results.values() if r.get("opened")]
    assert len(opened) == 1, f"expected 1 opened tick, got {results}"
    loser = [r for r in results.values() if not r.get("opened")][0]
    assert "already opened today" in (loser["reason"] or "")
    assert _last_tick_date(lad.ladder_id) == now.date().isoformat()


def test_atomic_claim_is_load_bearing_vs_read_then_write(clean_positions,
                                                         monkeypatch):
    """MUTATION CONTROL: restore the PRE-FIX read-then-write guard and prove the
    exact same race DOES double-open. This is what makes the safety test above
    credible — it fails for a real reason, not by luck of timing."""
    lad = _running_ladder()
    now = _trading_day_now()
    spawn_calls = []
    lock = threading.Lock()

    def slow_spawn(self, today):
        with lock:
            spawn_calls.append(self.ladder_id)
        time.sleep(0.5)
        return {"opened": True, "session_id": "s", "reason": "opened"}

    # The OLD guard: in-memory read, unconditional stamp afterwards.
    def read_then_write_claim(self, today):
        if self.last_tick_date == today.isoformat():
            return False
        return True  # no atomic claim — every racer "wins"

    monkeypatch.setattr(LadderCampaign, "_spawn_child", slow_spawn)
    monkeypatch.setattr(LadderCampaign, "_claim_day", read_then_write_claim)

    lad_a = LadderCampaign.load(lad.ladder_id)
    lad_b = LadderCampaign.load(lad.ladder_id)
    barrier = threading.Barrier(2)

    def run(l):
        barrier.wait()
        l.daily_tick(ref_now=now)

    ta = threading.Thread(target=run, args=(lad_a,))
    tb = threading.Thread(target=run, args=(lad_b,))
    ta.start(); tb.start(); ta.join(10); tb.join(10)

    assert len(spawn_calls) == 2, (
        "the pre-fix read-then-write guard was expected to DOUBLE-OPEN under "
        "this race; if it didn't, the safety test above proves nothing")


def test_claim_day_is_atomic_under_many_threads(clean_positions):
    """_claim_day itself: N racing threads → exactly ONE rowcount==1 winner."""
    lad = _running_ladder()
    today = _trading_day_now().date()
    wins = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def claim():
        l = LadderCampaign.load(lad.ladder_id)
        barrier.wait()
        if l._claim_day(today):
            with lock:
                wins.append(1)

    threads = [threading.Thread(target=claim) for _ in range(8)]
    [t.start() for t in threads]
    [t.join(10) for t in threads]
    assert sum(wins) == 1, f"expected exactly 1 claim winner, got {sum(wins)}"


def test_claim_day_release_allows_a_retry_same_day(clean_positions):
    """A pre-entry error releases the claim so the next beat retries today."""
    lad = _running_ladder()
    today = _trading_day_now().date()
    assert lad._claim_day(today) is True
    assert lad._claim_day(today) is False          # claimed
    lad._release_day(None)                          # error before any entry
    assert _last_tick_date(lad.ladder_id) is None
    assert lad._claim_day(today) is True            # retry wins


def test_pre_entry_error_releases_claim_but_post_entry_error_does_not(
        clean_positions, monkeypatch):
    """The claim is released ONLY when no entry could have reached a broker."""
    now = _trading_day_now()

    # (a) error BEFORE any entry attempt → claim released → retry possible.
    lad = _running_ladder()
    def boom_free(self):
        raise RuntimeError("transient DB blip")
    monkeypatch.setattr(LadderCampaign, "free_capital", boom_free)
    res = lad.daily_tick(ref_now=now)
    assert not res["opened"] and "error" in (res["reason"] or "")
    assert _last_tick_date(lad.ladder_id) is None, \
        "a pre-entry error must NOT consume the day"
    monkeypatch.undo()

    # (b) error AFTER an entry was attempted → claim STANDS (never re-open a day
    #     whose orders may already have gone out).
    lad2 = _running_ladder()
    def boom_after_entry(self, today):
        self._entry_attempted = True
        raise RuntimeError("blew up after placing")
    monkeypatch.setattr(LadderCampaign, "_spawn_child", boom_after_entry)
    res2 = lad2.daily_tick(ref_now=now)
    assert not res2["opened"]
    assert _last_tick_date(lad2.ladder_id) == now.date().isoformat(), \
        "a post-entry error MUST consume the day (orders may have been placed)"
