"""FALCON POSITIONAL AUTO-LADDER ORCHESTRATOR — the campaign layer.

A "set once, run autonomously for a month" campaign that sits ON TOP OF the
existing positional child-session engine (strategy=intraday_basket +
square_off_enabled=False + max_hold_sessions). A trader sets total capital +
product (CNC|MTF) + duration once, hits Start, and this orchestrator
opens/manages/rolls positional baskets every trading day for the month with zero
further input, while always allowing Pause + Kill and surfacing plain-language
downturn alerts.

DESIGN (operator-approved, built exactly):
  * Per-basket sizing = total_capital / HOLD_LENGTH, FROZEN at create, where
    HOLD_LENGTH = the campaign's effective child max_hold_sessions (the number of
    trading sessions a basket is held = the number of overlapping baskets at
    steady state). A new basket opens only when free capital >= that slice →
    naturally <= HOLD_LENGTH concurrent, and the total IS the ceiling (no
    hard-coded account cap). Default max_hold=3 → total/3 (unchanged); a 2-session
    "BTST" campaign → total/2, so 2 concurrent sleeves fully deploy the pool.
  * Capital accounting is on the TRADER-MONEY (margin) basis:
        free = total_capital − Σ(child.total_allocated_capital of OPEN children)
    MTF leverage lives INSIDE each child and NEVER inflates this ceiling.
  * The daily 09:15 tick (ladder_scheduler.py, restart-durable) opens ONE
    positional child per RUNNING ladder per trading day when there's a free
    slice and we're on/before end_date. Idempotent per ladder+day.
  * KILL takes a mode: "flatten_now" (immediately flatten every open child) OR
    "stop_new_let_finish" (stop opening new, let open children exit naturally).
  * Downturn alert: when the trailing 5-DAY AVERAGE realized return < 0% it fires
    alerts.send() ONCE per down-crossing. Informational only — NEVER auto-stops.

REUSE, DON'T DUPLICATE: every child is a normal TradingSession created + started
+ killed through the existing session engine. This module owns ONLY the campaign
orchestration + capital accounting + the daily-open decision + the alert. All
strategy / trail / exit logic stays in session.py + monitoring/.

ISOLATION: this module reads/writes ONLY autotrade_ladders + autotrade_sessions
(+ autotrade_positions via the child sessions). It NEVER touches
falcon_position_state.

TRADER TERMS ONLY: the word "sleeve" never appears in any field, log, or comment
that could reach the UI — status() speaks capital deployed / active baskets /
positions / daily P&L.

PAPER-SAFE: children default to the ladder's mode (paper unless the operator
explicitly chose live); live children remain gated by FALCON_AUTOTRADE_ENABLED at
the broker layer exactly as a standalone session.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn

from . import alerts
from . import trading_calendar as _cal
from .config import TradingSessionConfig
from .session import TradingSession, now_ist

log = logging.getLogger("kanida.autotrade.ladder")


def _ladder_open_time() -> time:
    """Daily market-open / activation clock (IST). Shared with ladder_scheduler
    (env FALCON_LADDER_OPEN_TIME, default 09:15:00) so a campaign activates AND
    opens baskets AT the open — never merely because the calendar date arrived.
    A tick BEFORE this time (e.g. a pre-market restart's resume) leaves a
    SCHEDULED campaign SCHEDULED (with its countdown), matching single sessions."""
    raw = os.environ.get("FALCON_LADDER_OPEN_TIME", "09:15:00")
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            t = datetime.strptime(raw, fmt)
            return time(t.hour, t.minute, t.second)
        except ValueError:
            continue
    return time(9, 15, 0)

IST = timezone(timedelta(hours=5, minutes=30))

# Campaign statuses.
STATUS_CREATED = "CREATED"      # created, NOT started — a draft; spawns nothing
STATUS_SCHEDULED = "SCHEDULED"  # armed for a FUTURE start_date; auto-activates
STATUS_RUNNING = "RUNNING"
STATUS_PAUSED = "PAUSED"
STATUS_ENDED = "ENDED"          # past end_date, still winding down open children
STATUS_COMPLETED = "COMPLETED"  # terminal: no open children remain

# Kill modes.
KILL_FLATTEN_NOW = "flatten_now"
KILL_STOP_NEW_LET_FINISH = "stop_new_let_finish"

# Legacy per-basket sizing divisor — the DEFAULT hold length when a campaign
# carries no max_hold_sessions override. Kept == POSITIONAL_MAX_HOLD_SESSIONS so
# an un-overridden ladder is byte-identical to the old total_capital/3 sizing.
# The ACTUAL divisor is the campaign's effective hold length (see
# _effective_hold_length); sizing is total_capital / hold_length.
BASKET_DIVISOR = 3

# 5-day trailing window for the downturn alert.
ALERT_WINDOW_DAYS = 5

# The validated Falcon POSITIONAL trail preset (fractions). arm 3% / floor 1% /
# giveback 4% / stop 6%, max_hold 3 trading sessions. Matches the positional
# side of the Hold toggle in the session UI.
POSITIONAL_ARM_PCT = 0.03
POSITIONAL_FLOOR_PCT = 0.01
POSITIONAL_GIVEBACK_PCT = 0.04
POSITIONAL_STOP_PCT = 0.06
POSITIONAL_MAX_HOLD_SESSIONS = 3
POSITIONAL_TOP_N = 5


def _now_iso() -> str:
    return now_ist().isoformat()


def _last_trading_day_of_month(d: date) -> date:
    """The last NSE trading day of d's calendar month (used as the default
    end_date for a month-long campaign started this month)."""
    # First day of next month, minus one day = last calendar day of this month.
    if d.month == 12:
        first_next = date(d.year + 1, 1, 1)
    else:
        first_next = date(d.year, d.month + 1, 1)
    last_cal = first_next - timedelta(days=1)
    # Walk backwards to a trading day.
    probe = last_cal
    while not _cal.is_trading_day(probe):
        probe = probe - timedelta(days=1)
    return probe


# ── LadderCampaign ────────────────────────────────────────────────────────────

@dataclass
class LadderCampaign:
    ladder_id: str
    total_capital: float
    order_product: str
    per_basket_capital: float
    status: str
    mode: str = "paper"
    user_id: Optional[str] = None
    broker_account_id: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    mode_kill: Optional[str] = None
    daily_returns_json: Optional[str] = None
    alert_active: int = 0
    last_tick_date: Optional[str] = None
    # LIVE CONFIG EDIT: optional risk/exit override for FUTURE spawned children
    # (NULL = the built-in validated positional preset, unchanged) + a monotone
    # template version bumped on each edit.
    child_config_json: Optional[str] = None
    config_version: int = 0

    # ── Factory / persistence ─────────────────────────────────────────────────
    @classmethod
    def create(cls, *, total_capital: float, order_product: str = "CNC",
               mode: str = "paper", user_id: Optional[str] = None,
               broker_account_id: Optional[str] = None,
               end_date: Optional[str] = None,
               end_date_mode: str = "month_end",
               child_config: Optional[Dict[str, Any]] = None
               ) -> "LadderCampaign":
        """Create a campaign as a DRAFT (status CREATED). create() places nothing
        and spawns nothing — it just registers the plan. start() then transitions
        it to RUNNING (start now) or SCHEDULED (future start_date). This mirrors
        the single-session create→start lifecycle.

        end_date_mode:
          * "month_end" (default) → end_date = last NSE trading day of the start
            month.
          * "manual"              → end_date = NULL (runs until explicitly killed
            / ended).
          * an explicit end_date (YYYY-MM-DD) overrides both.

        child_config (optional): the risk/exit override applied to EVERY child
        this campaign spawns, filtered to LADDER_CHILD_WHITELIST (arm_pct,
        floor_pct, trail_giveback_pct, stop_pct, per_position_*_pct,
        max_hold_sessions). Persisted as child_config_json at create so a BTST-style
        campaign can be sized + configured in ONE step (no separate config-edit).
        Capital / product / duration are NEVER sourced from here.

        Rejects MIS (positional can't carry overnight) and anything but CNC|MTF.
        Freezes per_basket_capital = total_capital / HOLD_LENGTH, where HOLD_LENGTH
        = the effective child max_hold_sessions (default POSITIONAL_MAX_HOLD_SESSIONS
        = 3 when unset → total/3, unchanged). This deploys the FULL chosen capital
        as HOLD_LENGTH overlapping sleeves at steady state.
        """
        if total_capital is None or float(total_capital) <= 0:
            raise ValueError("total_capital must be > 0")
        prod = str(order_product or "CNC").upper()
        if prod not in ("CNC", "MTF"):
            # Positional carries overnight → MIS (and any intraday product) is a
            # contradiction and is rejected at the door.
            raise ValueError(
                f"order_product must be CNC or MTF for a positional ladder "
                f"(got {order_product!r}); MIS cannot carry overnight")
        if mode not in ("paper", "live"):
            mode = "paper"

        # Filter the child override to the whitelist (drops capital/product/other
        # keys). Empty → child_config_json stays NULL (byte-identical to legacy).
        overrides: Dict[str, Any] = {}
        if child_config:
            overrides = {k: child_config[k]
                         for k in cls.LADDER_CHILD_WHITELIST if k in child_config}

        # HOLD-LENGTH-AWARE SIZING: the sleeve = total / hold_length so the pool is
        # fully deployed at steady state (hold_length overlapping baskets). Default
        # hold=3 → total/3, IDENTICAL to the old BASKET_DIVISOR path.
        hold_length = cls._effective_hold_length(overrides)
        per_basket = round(float(total_capital) / hold_length, 2)

        # Validate the resulting child config at the door (rejects e.g. floor>arm,
        # an out-of-range pct, a bad max_hold) so a bad override never reaches a
        # daily spawn. Uses the exact same builder the spawn path uses.
        try:
            cls._make_child_config(per_basket, prod, overrides).validate()
        except ValueError as e:
            raise ValueError(f"child_config invalid: {e}")

        child_config_json = json.dumps(overrides) if overrides else None
        today = now_ist().date()
        start = today.isoformat()

        # Resolve end_date.
        resolved_end: Optional[str]
        if end_date is not None and str(end_date).strip():
            try:
                _d = datetime.strptime(str(end_date).strip(), "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(f"end_date must be YYYY-MM-DD, got {end_date!r}")
            resolved_end = _d.isoformat()
        elif end_date_mode == "manual":
            resolved_end = None
        else:  # month_end (default)
            resolved_end = _last_trading_day_of_month(today).isoformat()

        # Bind-account existence check (fail fast; NULL → operator/global path).
        if broker_account_id is not None:
            from . import vault
            if not vault.account_exists(broker_account_id, user_id=user_id):
                raise ValueError(
                    f"broker_account_id {broker_account_id} not found"
                    + (f" for user {user_id}" if user_id else ""))

        ladder_id = uuid.uuid4().hex
        row = cls(
            ladder_id=ladder_id, total_capital=float(total_capital),
            order_product=prod, per_basket_capital=per_basket,
            status=STATUS_CREATED, mode=mode, user_id=user_id,
            broker_account_id=broker_account_id, start_date=start,
            end_date=resolved_end, mode_kill=None, daily_returns_json="[]",
            alert_active=0, last_tick_date=None,
            child_config_json=child_config_json, config_version=0)
        with falcon_conn() as con:
            con.execute(
                """INSERT INTO autotrade_ladders
                   (ladder_id, user_id, broker_account_id, mode, total_capital,
                    order_product, per_basket_capital, status, start_date,
                    end_date, mode_kill, daily_returns_json, alert_active,
                    last_tick_date, child_config_json, config_version,
                    created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (ladder_id, user_id, broker_account_id, mode,
                 float(total_capital), prod, per_basket, STATUS_CREATED, start,
                 resolved_end, None, "[]", 0, None, child_config_json, 0,
                 _now_iso(), _now_iso()),
            )
            con.commit()
        log.info("ladder %s created (DRAFT/CREATED; capital=%.2f product=%s "
                 "per_basket=%.2f hold=%d mode=%s end=%s user=%s child_cfg=%s)",
                 ladder_id, total_capital, prod, per_basket, hold_length, mode,
                 resolved_end, user_id, overrides or None)
        return row

    @classmethod
    def load(cls, ladder_id: str) -> Optional["LadderCampaign"]:
        with falcon_conn() as con:
            r = con.execute(
                "SELECT * FROM autotrade_ladders WHERE ladder_id=?",
                (ladder_id,),
            ).fetchone()
        if not r:
            return None
        d = dict(r)
        return cls(
            ladder_id=d["ladder_id"], total_capital=d["total_capital"],
            order_product=d["order_product"],
            per_basket_capital=d["per_basket_capital"], status=d["status"],
            mode=d.get("mode", "paper"), user_id=d.get("user_id"),
            broker_account_id=d.get("broker_account_id"),
            start_date=d.get("start_date"), end_date=d.get("end_date"),
            mode_kill=d.get("mode_kill"),
            daily_returns_json=d.get("daily_returns_json"),
            alert_active=int(d.get("alert_active") or 0),
            last_tick_date=d.get("last_tick_date"),
            child_config_json=d.get("child_config_json"),
            config_version=int(d.get("config_version") or 0))

    @classmethod
    def list_ladders(cls, user_id: Optional[str] = None,
                     limit: int = 50) -> List[Dict[str, Any]]:
        base = "SELECT * FROM autotrade_ladders"
        with falcon_conn() as con:
            if user_id is not None:
                rows = con.execute(
                    base + " WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
                    (str(user_id), int(limit))).fetchall()
            else:
                rows = con.execute(
                    base + " ORDER BY created_at DESC LIMIT ?",
                    (int(limit),)).fetchall()
        return [cls._summary_row(dict(r)) for r in rows]

    @staticmethod
    def _summary_row(d: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "ladder_id": d["ladder_id"],
            "status": d["status"],
            "mode": d.get("mode", "paper"),
            "total_capital": d["total_capital"],
            "per_basket_capital": d["per_basket_capital"],
            "order_product": d["order_product"],
            "start_date": d.get("start_date"),
            "end_date": d.get("end_date"),
            "alert_active": bool(d.get("alert_active")),
            "user_id": d.get("user_id"),
        }

    # ── Persistence helpers ───────────────────────────────────────────────────
    def _update(self, **fields) -> None:
        if not fields:
            return
        fields["updated_at"] = _now_iso()
        cols = ", ".join(f"{k}=?" for k in fields)
        vals = list(fields.values()) + [self.ladder_id]
        with falcon_conn() as con:
            con.execute(f"UPDATE autotrade_ladders SET {cols} WHERE ladder_id=?",
                        vals)
            con.commit()
        for k, v in fields.items():
            if k != "updated_at":
                setattr(self, k, v)

    # ── Child bookkeeping (margin-basis capital accounting) ───────────────────
    def _child_rows(self, statuses: Optional[List[str]] = None
                    ) -> List[Dict[str, Any]]:
        """All child sessions tagged with this ladder_id (optionally filtered to
        a set of statuses)."""
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT session_id, status, total_allocated_capital,
                          started_at, closed_at, created_at
                   FROM autotrade_sessions WHERE ladder_id=?
                   ORDER BY created_at ASC""",
                (self.ladder_id,),
            ).fetchall()
        out = [dict(r) for r in rows]
        if statuses is not None:
            out = [r for r in out if r["status"] in statuses]
        return out

    # A child is "open" (holding capital) when it has been started and not yet
    # closed. RUNNING = actively holding; SCHEDULED = armed to fire (its slice is
    # committed); KILLING = flattening (still holding until CLOSED). CREATED means
    # not yet started → holds no capital. Terminal/non-holding states free the
    # slice back to the ceiling.
    _OPEN_CHILD_STATUSES = ("RUNNING", "SCHEDULED", "KILLING")

    def _open_children(self) -> List[Dict[str, Any]]:
        return self._child_rows(list(self._OPEN_CHILD_STATUSES))

    def free_capital(self) -> float:
        """TRADER-MONEY (margin) basis free capital.

        free = total_capital − Σ(child.total_allocated_capital of OPEN children).

        Each child's total_allocated_capital is its margin slice (= per_basket at
        spawn); MTF leverage lives INSIDE the child (its invested_basis may be
        several × the slice) but does NOT count here — the ceiling is the trader's
        deployed capital, never the leveraged notional.
        """
        committed = sum(float(r["total_allocated_capital"] or 0.0)
                        for r in self._open_children())
        return round(float(self.total_capital) - committed, 2)

    def n_active_baskets(self) -> int:
        return len(self._open_children())

    # ── Lifecycle ─────────────────────────────────────────────────────────────
    def start(self, start_date: Optional[str] = None) -> Dict[str, Any]:
        """Arm the campaign — mirrors the single-session start(when=...) lifecycle.

        start_date (YYYY-MM-DD, IST):
          * None, a PAST date, or TODAY once the 09:15 open has passed → status
            RUNNING, start_date=today. Starts immediately; the first positional
            basket opens on the next daily tick at/after the open. This is the
            DEFAULT so today's one-step create+start still lands RUNNING.
          * TODAY before the 09:15 open, or a FUTURE trading day → status
            SCHEDULED, start_date=that date. It stays SCHEDULED (with the UI
            countdown) and the daily tick activates it to RUNNING at 09:15 on
            start_date. Survives a restart (resume includes SCHEDULED).

        A future start_date on a weekend/holiday is REJECTED with a ValueError
        carrying the suggested next NSE trading day (route → 400 with a
        suggestion), mirroring the session non-trading-day rejection.

        Only startable from CREATED or SCHEDULED (re-scheduling a SCHEDULED
        campaign to a new date is allowed). RUNNING / ENDED / COMPLETED are
        rejected with a clear message.
        """
        if self.status not in (STATUS_CREATED, STATUS_SCHEDULED):
            if self.status == STATUS_RUNNING:
                raise ValueError(
                    f"ladder {self.ladder_id} is already RUNNING; use "
                    "pause/resume to control it")
            raise ValueError(f"ladder {self.ladder_id} is {self.status}; "
                             "create a new campaign")

        now = now_ist()
        today = now.date()

        # No date / empty → start immediately (RUNNING today).
        target: Optional[date] = None
        if start_date is not None and str(start_date).strip():
            try:
                target = datetime.strptime(
                    str(start_date).strip(), "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(
                    f"start_date must be YYYY-MM-DD, got {start_date!r}")

        # SCHEDULE (not start-now) when the requested date's 09:15 activation is
        # still ahead: a FUTURE trading day, OR TODAY when today is a trading day
        # and the daily open (09:15 IST) hasn't passed yet — so a pre-market
        # "schedule for today" lands SCHEDULED with a countdown, exactly like the
        # single-session scheduled flow. No date / a past date / today-after-open
        # → start now (RUNNING).
        schedulable_today = (
            target == today
            and _cal.is_trading_day(today)
            and now.time() < _ladder_open_time()
        )
        if target is None or target < today or (target == today and not schedulable_today):
            # Start now → RUNNING, start_date pinned to today.
            self._update(status=STATUS_RUNNING, start_date=today.isoformat())
            log.info("ladder %s started NOW (RUNNING, start_date=%s)",
                     self.ladder_id, today.isoformat())
            return {"ladder_id": self.ladder_id, "status": STATUS_RUNNING,
                    "start_date": today.isoformat(), "when": "now"}

        # COVERAGE GUARD (real-money safety): refuse to SCHEDULE a campaign start
        # into a year whose NSE holidays we don't authoritatively know — else the
        # trading-day gate is untrustworthy and a basket could open on a holiday.
        # Raises CalendarCoverageError naming the year + remedy. NO-OP for covered
        # years (2025/2026 today).
        _cal.assert_calendar_covers(target)
        # Future start_date → must be an NSE trading day.
        if not _cal.is_trading_day(target):
            suggested = _cal.next_trading_day(target, inclusive=True).isoformat()
            raise ValueError(
                f"{target.isoformat()} is not an NSE trading day "
                f"(weekend/holiday); next trading day is {suggested}"
                f"||suggested={suggested}")

        # A start_date strictly after end_date would never open a single basket.
        if self._past_end_date(target):
            raise ValueError(
                f"start_date {target.isoformat()} is after the campaign end_date "
                f"{self.end_date}; pick a start on or before the end date")

        self._update(status=STATUS_SCHEDULED, start_date=target.isoformat())
        log.info("ladder %s SCHEDULED to start %s", self.ladder_id,
                 target.isoformat())
        return {"ladder_id": self.ladder_id, "status": STATUS_SCHEDULED,
                "start_date": target.isoformat(), "when": "scheduled"}

    def pause(self) -> Dict[str, Any]:
        """Stop opening NEW baskets; keep managing the open children (their trail
        / stop / max-hold exits stay fully live via the child tick drivers)."""
        if self.status in (STATUS_COMPLETED,):
            raise ValueError(f"ladder {self.ladder_id} is COMPLETED")
        self._update(status=STATUS_PAUSED)
        log.info("ladder %s PAUSED (no new baskets; open baskets keep running)",
                 self.ladder_id)
        return {"ladder_id": self.ladder_id, "status": STATUS_PAUSED}

    def resume(self) -> Dict[str, Any]:
        if self.status == STATUS_COMPLETED:
            raise ValueError(f"ladder {self.ladder_id} is COMPLETED")
        # ENDED (past end_date) resumes only conceptually — it will still open
        # nothing because the end_date gate holds. RUNNING is the live state.
        self._update(status=STATUS_RUNNING)
        log.info("ladder %s RESUMED (RUNNING)", self.ladder_id)
        return {"ladder_id": self.ladder_id, "status": STATUS_RUNNING}

    async def kill(self, mode: str) -> Dict[str, Any]:
        """KILL with a MODE.

        * "flatten_now"          → immediately flatten EVERY open child (each
          child.kill() reuses the session engine's full flatten: GTT-cancel sweep
          → market exits → fill-confirm). Campaign → COMPLETED.
        * "stop_new_let_finish"  → stop opening new baskets; let the open children
          exit naturally on their own trail/stop/max-hold. Campaign → ENDED; it
          auto-COMPLETES when the last child closes.
        """
        if mode not in (KILL_FLATTEN_NOW, KILL_STOP_NEW_LET_FINISH):
            raise ValueError(
                f"invalid kill mode {mode!r} "
                f"(flatten_now | stop_new_let_finish)")
        self._update(mode_kill=mode)
        open_children = self._open_children()

        if mode == KILL_STOP_NEW_LET_FINISH:
            # Stop new; leave open children to exit naturally. ENDED, and the next
            # tick / a completion check will flip to COMPLETED once flat.
            self._update(status=STATUS_ENDED)
            log.info("ladder %s KILL(stop_new_let_finish): %d open children "
                     "left to exit naturally", self.ladder_id, len(open_children))
            self._maybe_complete()
            return {"ladder_id": self.ladder_id, "status": self.status,
                    "mode": mode, "children_left_open": len(open_children),
                    "children_flattened": 0}

        # flatten_now: flatten every open child through the session engine.
        flattened = 0
        details: List[Dict[str, Any]] = []
        for r in open_children:
            sid = r["session_id"]
            try:
                sess = TradingSession.load(sid)
                if sess is None:
                    continue
                res = await sess.kill(reason="LADDER_KILL")
                flattened += 1
                details.append({"session_id": sid,
                                "n_exited_ok": res.get("n_exited_ok"),
                                "n_exit_failed": res.get("n_exit_failed")})
            except Exception as e:  # never let one child abort the sweep
                log.exception("ladder %s: flatten of child %s failed: %s",
                              self.ladder_id, sid, e)
                details.append({"session_id": sid, "error": str(e)})
        self._update(status=STATUS_COMPLETED)
        log.info("ladder %s KILL(flatten_now): flattened %d children → COMPLETED",
                 self.ladder_id, flattened)
        return {"ladder_id": self.ladder_id, "status": STATUS_COMPLETED,
                "mode": mode, "children_flattened": flattened,
                "children_left_open": 0, "details": details}

    def delete(self) -> bool:
        """PERMANENTLY remove this campaign's row (what the UI Discard/Delete
        should do — not just hide it). Allowed only when NO basket is open: a
        CREATED draft, a SCHEDULED-but-not-armed campaign, or a finished
        (COMPLETED/ENDED-flat) one. A campaign with OPEN children must be
        Cancelled (kill) first — we refuse here so a live position is never
        orphaned. Child session rows (the trade history) are left intact.
        Returns True if a row was deleted."""
        open_n = len(self._open_children())
        if open_n > 0:
            raise ValueError(
                f"campaign {self.ladder_id} has {open_n} open basket(s); "
                "cancel it first, then delete")
        with falcon_conn() as con:
            cur = con.execute(
                "DELETE FROM autotrade_ladders WHERE ladder_id=?",
                (self.ladder_id,))
            con.commit()
            deleted = cur.rowcount > 0
        if deleted:
            log.info("ladder %s DELETED (was %s)", self.ladder_id, self.status)
        return deleted

    def _maybe_complete(self) -> bool:
        """If the campaign is past its opening life (ENDED, or RUNNING past
        end_date) AND no open children remain, transition to COMPLETED. Returns
        True if it flipped to COMPLETED."""
        if self.status == STATUS_COMPLETED:
            return False
        open_n = len(self._open_children())
        if open_n > 0:
            return False
        past_end = self._past_end_date(now_ist().date())
        # COMPLETE when: killed-to-finish (ENDED) with no open children, OR a
        # RUNNING campaign that has passed its end_date with no open children.
        if self.status == STATUS_ENDED or (self.status == STATUS_RUNNING
                                           and past_end):
            self._update(status=STATUS_COMPLETED)
            log.info("ladder %s → COMPLETED (last child closed / month-end)",
                     self.ladder_id)
            return True
        return False

    def _past_end_date(self, today: date) -> bool:
        """True when today is strictly AFTER end_date (so no new basket opens).
        NULL end_date (manual-only) is never past."""
        if not self.end_date:
            return False
        try:
            ed = datetime.strptime(self.end_date, "%Y-%m-%d").date()
        except ValueError:
            return False
        return today > ed

    def _start_date_reached(self, today: date) -> bool:
        """True when today (IST) >= start_date. A SCHEDULED campaign activates on
        or after this. A missing/unparseable start_date is treated as reached (so
        we never leave a campaign stuck armed forever)."""
        if not self.start_date:
            return True
        try:
            sd = datetime.strptime(self.start_date, "%Y-%m-%d").date()
        except ValueError:
            return True
        return today >= sd

    def _on_or_before_end_date(self, today: date) -> bool:
        """True when today <= end_date (or end_date is NULL = manual-only)."""
        if not self.end_date:
            return True
        try:
            ed = datetime.strptime(self.end_date, "%Y-%m-%d").date()
        except ValueError:
            return True
        return today <= ed

    # ── Daily tick (the one genuinely new mechanism) ──────────────────────────
    def daily_tick(self, ref_now: Optional[datetime] = None) -> Dict[str, Any]:
        """The 09:15 daily campaign step. NEVER raises (defensive throughout).

        For a RUNNING ladder, if today is a trading day AND (end_date is NULL OR
        today <= end_date) AND free_capital >= per_basket_capital: create + start
        ONE positional child sized to per_basket_capital, tagged with this
        ladder_id. Idempotent per ladder+day (last_tick_date guard).

        State handling:
          * CREATED (draft) → opens nothing, ever (never started).
          * SCHEDULED → if today (IST) >= start_date AND today is a trading day,
            transition to RUNNING and proceed to spawn; else stay SCHEDULED and
            open nothing.
          * PAUSED / ENDED / COMPLETED → open nothing.

        Also runs the 5-day realized-return alert and the month-end auto-complete
        each call.
        """
        now = ref_now or now_ist()
        today = now.date()
        # MARKET-OPEN TIME GATE: the campaign activates + opens baskets at the
        # daily open (09:15 IST), NOT merely because the date arrived. Before the
        # open (e.g. a pre-market restart's resume tick) it must stay SCHEDULED
        # and open nothing — parity with a single scheduled session's countdown.
        market_open = now.time() >= _ladder_open_time()
        out: Dict[str, Any] = {"ladder_id": self.ladder_id, "date": today.isoformat(),
                               "opened": False, "reason": None}
        try:
            # CREATED drafts do NOTHING (never started, never spawn, no alert
            # window to keep). Return before any accounting.
            if self.status == STATUS_CREATED:
                out["reason"] = "status=CREATED (draft; opens nothing)"
                return out

            # SCHEDULED: activate to RUNNING on/after start_date on a trading day;
            # otherwise stay armed and open nothing. Do this BEFORE the alert /
            # complete checks so the rest of the tick runs as RUNNING once armed.
            if self.status == STATUS_SCHEDULED:
                if not self._start_date_reached(today):
                    out["reason"] = (f"scheduled — starts {self.start_date} "
                                     f"(not yet reached)")
                    return out
                if not _cal.is_trading_day(today):
                    out["reason"] = (f"scheduled start {self.start_date} reached "
                                     "but today is not a trading day")
                    return out
                if not market_open:
                    # Start day + trading day, but market not open yet → STAY
                    # SCHEDULED (the UI keeps its countdown). Activates at 09:15.
                    out["reason"] = (f"scheduled start {self.start_date} reached "
                                     f"but before the daily open ({_ladder_open_time()}) "
                                     "— staying SCHEDULED")
                    return out
                # Activate. Pin start_date to today (the actual activation day) so
                # the campaign's lifetime + end-date gate reason from here.
                self._update(status=STATUS_RUNNING,
                             start_date=today.isoformat())
                log.info("ladder %s SCHEDULED→RUNNING (activated on %s)",
                         self.ladder_id, today.isoformat())
                out["activated"] = True

            # Refresh the alert window + fire on a down-crossing (every tick,
            # regardless of whether we open — informational only).
            self._refresh_alert(today)

            # Month-end / kill-to-finish auto-complete check first.
            if self._maybe_complete():
                out["reason"] = "completed"
                return out

            if self.status != STATUS_RUNNING:
                out["reason"] = f"status={self.status} (opens nothing)"
                return out
            if not _cal.is_trading_day(today):
                out["reason"] = "not a trading day"
                return out
            if not self._on_or_before_end_date(today):
                out["reason"] = "past end_date (no new baskets)"
                # Past end with nothing open → complete.
                self._maybe_complete()
                return out
            if not market_open:
                # A RUNNING campaign still opens its daily basket only AT the open
                # (09:15) — a pre-market tick must not spawn early. Do NOT stamp
                # last_tick_date, so the 09:15 scheduler beat opens it on time.
                out["reason"] = (f"before the daily open ({_ladder_open_time()}) "
                                 "— no basket opened yet today")
                return out
            # Idempotency: at most one basket per ladder per day.
            if self.last_tick_date == today.isoformat():
                out["reason"] = "already opened today"
                return out

            free = self.free_capital()
            if free + 1e-6 < float(self.per_basket_capital):
                # Fully deployed (or no free slice). Mark the day handled so we
                # don't re-evaluate every scheduler beat, but DON'T open.
                self._update(last_tick_date=today.isoformat())
                out["reason"] = (f"no free slice (free={free:.2f} < "
                                 f"per_basket={self.per_basket_capital:.2f})")
                out["free_capital"] = free
                return out

            # Open ONE positional child sized to per_basket_capital.
            res = self._spawn_child(today)
            self._update(last_tick_date=today.isoformat())
            out["opened"] = bool(res.get("opened"))
            out["reason"] = res.get("reason")
            out["session_id"] = res.get("session_id")
            out["free_capital"] = self.free_capital()
            return out
        except Exception as e:  # NEVER raise out of the tick
            log.exception("ladder %s daily_tick failed: %s", self.ladder_id, e)
            out["reason"] = f"error: {e}"
            return out

    @staticmethod
    def _make_child_config(per_basket_capital: float, order_product: str,
                           overrides: Dict[str, Any]) -> TradingSessionConfig:
        """Build a positional child config: strategy=intraday_basket,
        square_off_enabled=False, max_hold_sessions=3, the validated positional
        trail preset, given product + per_basket capital, with the whitelisted
        risk/exit `overrides` merged on top. Pure (no self) so create() (pre-row)
        and _build_child_config() (post-row) share ONE construction path."""
        cfg = TradingSessionConfig(
            total_allocated_capital=float(per_basket_capital),
            strategy="intraday_basket",
            top_n_stocks=POSITIONAL_TOP_N,
            order_product=order_product,
            instrument_type="EQ",
            arm_pct=POSITIONAL_ARM_PCT,
            floor_pct=POSITIONAL_FLOOR_PCT,
            trail_giveback_pct=POSITIONAL_GIVEBACK_PCT,
            stop_pct=POSITIONAL_STOP_PCT,
            square_off_enabled=False,          # POSITIONAL: carry across days
            max_hold_sessions=POSITIONAL_MAX_HOLD_SESSIONS,
            # POSITIONAL keeps its LEGACY fixed-floor trail (looser give-back to
            # breathe across days). Profit step-locking (a tight ratchet tuned for
            # the intraday MIS basket) would clip multi-day positionals on normal
            # pullbacks, so it is OFF for ladder children by design. The operator
            # can opt an individual campaign in via the live config edit.
            trail_step_lock_enabled=False,
            # per-position GTT is the broker floor, wider than the trail — keep the
            # session defaults (3% stop / 6% target).
        )
        # Merge the operator's risk/exit overrides (whitelist only). Never touches
        # capital/product/strategy/square_off_enabled.
        for f, v in (overrides or {}).items():
            setattr(cfg, f, v)
        return cfg

    @classmethod
    def _effective_hold_length(cls, overrides: Dict[str, Any]) -> int:
        """The number of trading sessions a child holds = the number of
        overlapping sleeves at steady state = the sizing divisor. Sourced from the
        effective child max_hold_sessions override, else POSITIONAL_MAX_HOLD_SESSIONS
        (=3). Must be >= 1 (a 0/negative divisor is nonsensical + would divide-by-
        zero the sleeve)."""
        raw = (overrides or {}).get("max_hold_sessions")
        if raw is None:
            return POSITIONAL_MAX_HOLD_SESSIONS
        try:
            h = int(raw)
        except (TypeError, ValueError):
            raise ValueError(
                f"max_hold_sessions must be an integer, got {raw!r}")
        if h < 1:
            raise ValueError(
                f"max_hold_sessions must be >= 1, got {h}")
        return h

    def _build_child_config(self) -> TradingSessionConfig:
        """The positional child config for THIS campaign — the shared preset merged
        with this ladder's saved whitelisted overrides (child_config_json), at this
        ladder's frozen per_basket capital + product. Capital/product/duration are
        NOT sourced from the template (they stay on the ladder columns), so an edit
        can never desync a running basket's frozen basis."""
        return self._make_child_config(
            self.per_basket_capital, self.order_product,
            self.child_config_overrides())

    # The risk/exit knobs a ladder edit may override on FUTURE spawned children.
    # A SUBSET of the session whitelist that is meaningful for a positional child
    # (square_off_time / mis_square_off_time are inert for a positional carry, but
    # accepted + stored harmlessly). Capital/product/duration are NOT here.
    LADDER_CHILD_WHITELIST = (
        "arm_pct",
        "floor_pct",
        "trail_giveback_pct",
        "stop_pct",
        "per_position_stop_pct",
        "per_position_target_pct",
        "max_hold_sessions",
    )

    def child_config_overrides(self) -> Dict[str, Any]:
        """The saved whitelisted risk/exit overrides for future children (empty
        when no template edit has been applied)."""
        if not self.child_config_json:
            return {}
        try:
            raw = json.loads(self.child_config_json)
        except Exception:
            return {}
        return {k: raw[k] for k in self.LADDER_CHILD_WHITELIST if k in raw}

    def _spawn_child(self, today: date) -> Dict[str, Any]:
        """Create + start ONE positional child for `today`, tagged ladder_id.
        Started with when='now' so it fires this day's Falcon Top-5 immediately
        (the tick runs at/after 09:15 on a trading day → the fire gate passes)."""
        import asyncio

        # COVERAGE GUARD (real-money safety): never open a basket for a day in a
        # year whose NSE holidays we don't authoritatively know (the trading-day
        # gate + the child's max-hold cap would be untrustworthy). Fail SAFE:
        # don't spawn, report the reason (daily_tick surfaces it), retry next day.
        try:
            _cal.assert_calendar_covers(today)
        except _cal.CalendarCoverageError as e:
            log.warning("ladder %s: refusing to open %s — %s",
                        self.ladder_id, today.isoformat(), e)
            return {"opened": False, "reason": f"calendar coverage gap: {e}"}

        cfg = self._build_child_config()
        try:
            cfg.validate()
        except Exception as e:
            return {"opened": False, "reason": f"child config invalid: {e}"}

        sess = TradingSession.create(
            cfg, mode=self.mode, user_id=self.user_id,
            broker_account_id=self.broker_account_id)
        # Tag the child with this ladder (the ONE new link on the session row).
        with falcon_conn() as con:
            con.execute("UPDATE autotrade_sessions SET ladder_id=? "
                        "WHERE session_id=?", (self.ladder_id, sess.session_id))
            con.commit()

        # Fire the entries NOW (through the session engine's trading-day gate).
        try:
            res = asyncio.run(sess.start(when="now"))
        except Exception as e:
            log.exception("ladder %s: child %s start failed: %s",
                          self.ladder_id, sess.session_id, e)
            return {"opened": False, "session_id": sess.session_id,
                    "reason": f"child start error: {e}"}

        status = res.get("status")
        n_placed = res.get("n_placed", 0)
        if status == "RUNNING" and n_placed:
            log.info("ladder %s opened child %s (%d legs, %.2f capital)",
                     self.ladder_id, sess.session_id, n_placed,
                     self.per_basket_capital)
            return {"opened": True, "session_id": sess.session_id,
                    "reason": f"opened {n_placed} legs", "child_status": status}
        # A child that failed to fill / had no picks / was gated: it holds no real
        # capital (0 open positions) so it frees its slice automatically (it's not
        # RUNNING). Log + report; the next trading day the tick retries.
        log.warning("ladder %s: child %s did not open (status=%s n_placed=%s)",
                    self.ladder_id, sess.session_id, status, n_placed)
        return {"opened": False, "session_id": sess.session_id,
                "reason": f"child did not fill (status={status})",
                "child_status": status}

    # ── 5-day realized-return downturn alert ──────────────────────────────────
    def _realized_return_for_day(self, day: date) -> Optional[float]:
        """Σ realized P&L of THIS ladder's children that CLOSED (positions with
        closed_at on `day`) ÷ total_capital, or None if nothing closed that day.

        Realized only — open positions do NOT count (they contribute unrealised,
        not a realized daily return)."""
        day_iso = day.isoformat()
        with falcon_conn() as con:
            row = con.execute(
                """SELECT COALESCE(SUM(p.realised_pnl), 0.0) AS pnl,
                          COUNT(*) AS n
                   FROM autotrade_positions p
                   JOIN autotrade_sessions s ON s.session_id = p.session_id
                   WHERE s.ladder_id = ?
                     AND p.status = 'CLOSED'
                     AND p.closed_at IS NOT NULL
                     AND substr(p.closed_at, 1, 10) = ?""",
                (self.ladder_id, day_iso),
            ).fetchone()
        if not row or int(row["n"] or 0) == 0:
            return None
        return float(row["pnl"] or 0.0) / float(self.total_capital)

    def _refresh_alert(self, today: date) -> None:
        """Recompute the rolling window of realized daily returns up to `today`,
        and fire alerts.send() ONCE on a down-crossing (trailing 5-day avg < 0).

        The window is stored as a JSON list of {date, ret}. We (re)record today's
        realized return (if any positions closed today) then evaluate the trailing
        ALERT_WINDOW_DAYS average. alert_active latches so we don't re-fire every
        tick while the average stays negative — only on the transition into < 0.
        """
        try:
            window = json.loads(self.daily_returns_json or "[]")
        except Exception:
            window = []

        today_iso = today.isoformat()
        ret_today = self._realized_return_for_day(today)
        if ret_today is not None:
            # Upsert today's entry (idempotent across multiple ticks in a day).
            window = [w for w in window if w.get("date") != today_iso]
            window.append({"date": today_iso, "ret": ret_today})
            window.sort(key=lambda w: w.get("date", ""))

        # Trailing ALERT_WINDOW_DAYS of RECORDED days (days with realized closes).
        tail = window[-ALERT_WINDOW_DAYS:]
        avg = (sum(float(w["ret"]) for w in tail) / len(tail)) if tail else 0.0

        was_active = bool(self.alert_active)
        now_below = (len(tail) >= ALERT_WINDOW_DAYS and avg < 0.0)

        new_active = 1 if now_below else 0
        self._update(daily_returns_json=json.dumps(window),
                     alert_active=new_active)

        if now_below and not was_active:
            # Fire once on the down-crossing (in-panel/logged only, v1).
            alerts.send(
                "The strategy has had a weak 5-day stretch — the 5-day average "
                "return is below 0%. You may continue, pause new baskets, or end "
                "the session; these soft patches have historically recovered.",
                severity="warn")
            log.info("ladder %s: 5-day-avg down-crossing (avg=%.4f) — alert sent",
                     self.ladder_id, avg)

    def _alert_state(self) -> Dict[str, Any]:
        try:
            window = json.loads(self.daily_returns_json or "[]")
        except Exception:
            window = []
        tail = window[-ALERT_WINDOW_DAYS:]
        avg = (sum(float(w["ret"]) for w in tail) / len(tail)) if tail else None
        active = bool(self.alert_active)
        msg = None
        if active:
            msg = ("The strategy has had a weak 5-day stretch — the 5-day "
                   "average return is below 0%. You may continue, pause new "
                   "baskets, or end the session; these soft patches have "
                   "historically recovered.")
        return {"active": active, "message": msg,
                "trailing_5d_avg_return": avg,
                "n_days_in_window": len(tail)}

    # ── Status (trader-facing) ────────────────────────────────────────────────
    def to_status(self) -> Dict[str, Any]:
        """Trader-facing campaign status. NO internal jargon — capital deployed,
        active baskets, positions, daily P&L only."""
        children = self._child_rows()
        open_children = [c for c in children
                         if c["status"] in self._OPEN_CHILD_STATUSES]
        committed = sum(float(c["total_allocated_capital"] or 0.0)
                        for c in open_children)
        free = round(float(self.total_capital) - committed, 2)

        # Aggregate P&L across the campaign's children (open → unrealised;
        # closed today → realized "today P&L").
        realized_total, unrealised_total, n_open_positions = self._aggregate_pnl()
        today = now_ist().date()
        today_realized = self._realized_return_for_day(today)
        today_pnl = (today_realized * float(self.total_capital)
                     if today_realized is not None else 0.0)

        child_list = []
        for c in children:
            child_list.append({
                "session_id": c["session_id"],
                "status": c["status"],
                "capital": c["total_allocated_capital"],
                "opened_at": c.get("started_at"),
                "closed_at": c.get("closed_at"),
            })

        return {
            "ladder_id": self.ladder_id,
            "status": self.status,
            "mode": self.mode,
            "total_capital": self.total_capital,
            "capital_deployed": round(committed, 2),
            "capital_free": free,
            "per_basket_capital": self.per_basket_capital,
            "n_active_baskets": len(open_children),
            "n_open_positions": n_open_positions,
            "realized_pnl": round(realized_total, 2),
            "unrealized_pnl": round(unrealised_total, 2),
            "today_pnl": round(today_pnl, 2),
            "order_product": self.order_product,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "mode_kill": self.mode_kill,
            "alert": self._alert_state(),
            "sessions": child_list,
            "user_id": self.user_id,
            "broker_account_id": self.broker_account_id,
            # LIVE CONFIG EDIT: current risk/exit template (effective values,
            # preset merged with any saved overrides) + capital/duration knobs +
            # the template version, so the UI can pre-fill the ladder edit form.
            "config_version": int(self.config_version or 0),
            "editable_config": self._editable_config(),
        }

    def _editable_config(self) -> Dict[str, Any]:
        """Effective editable knobs for the ladder edit form: the risk/exit
        template a FUTURE child would spawn with (built-in preset merged with any
        operator override) + the capital/duration knobs."""
        child = self._build_child_config()
        out: Dict[str, Any] = {
            f: getattr(child, f, None) for f in self.LADDER_CHILD_WHITELIST
        }
        out["per_basket_capital"] = self.per_basket_capital
        out["total_capital"] = self.total_capital
        out["end_date"] = self.end_date
        return out

    def _aggregate_pnl(self) -> "tuple[float, float, int]":
        """(realized_total, unrealised_total, n_open_positions) across all this
        ladder's children."""
        with falcon_conn() as con:
            r = con.execute(
                """SELECT
                       COALESCE(SUM(CASE WHEN p.status='CLOSED'
                                    THEN p.realised_pnl ELSE 0 END), 0.0) AS realized,
                       COALESCE(SUM(CASE WHEN p.status='OPEN'
                                    THEN p.unrealised_pnl ELSE 0 END), 0.0) AS unreal,
                       COALESCE(SUM(CASE WHEN p.status='OPEN' THEN 1 ELSE 0 END),
                                0) AS n_open
                   FROM autotrade_positions p
                   JOIN autotrade_sessions s ON s.session_id = p.session_id
                   WHERE s.ladder_id = ?""",
                (self.ladder_id,),
            ).fetchone()
        return (float(r["realized"] or 0.0), float(r["unreal"] or 0.0),
                int(r["n_open"] or 0))


# ── Boot-durable resume (mirrors recovery.resume_active_sessions) ─────────────

def resume_active_ladders() -> Dict[str, Any]:
    """On boot: run one daily_tick for every non-terminal, non-draft ladder
    (SCHEDULED / RUNNING / PAUSED / ENDED — CREATED drafts are skipped) so the
    campaign picks up where it left off (activate a SCHEDULED campaign whose
    start_date has arrived, open today's basket if due, refresh the alert,
    auto-complete if the last child has closed). Idempotent (last_tick_date guards
    a same-day re-open). NEVER raises. The recurring 09:15 scheduler
    (ladder_scheduler) is armed separately in main.py.
    """
    summary: Dict[str, Any] = {"resumed": 0, "opened": 0, "completed": 0,
                               "errors": 0, "ladders": []}
    try:
        with falcon_conn() as con:
            rows = con.execute(
                "SELECT ladder_id FROM autotrade_ladders "
                "WHERE status IN (?,?,?,?) ORDER BY created_at ASC",
                (STATUS_SCHEDULED, STATUS_RUNNING, STATUS_PAUSED,
                 STATUS_ENDED)).fetchall()
    except Exception as e:  # pragma: no cover - DB unavailable at boot
        log.exception("ladder resume: query failed: %s", e)
        return summary
    for row in rows:
        lid = row["ladder_id"]
        try:
            lad = LadderCampaign.load(lid)
            if lad is None:
                continue
            res = lad.daily_tick()
            summary["resumed"] += 1
            if res.get("opened"):
                summary["opened"] += 1
            if lad.status == STATUS_COMPLETED:
                summary["completed"] += 1
            summary["ladders"].append({"ladder_id": lid, "status": lad.status,
                                       "opened": res.get("opened")})
        except Exception as e:
            log.exception("ladder resume: %s failed: %s", lid, e)
            summary["errors"] += 1
    log.info("ladder resume: resumed=%d opened=%d completed=%d errors=%d",
             summary["resumed"], summary["opened"], summary["completed"],
             summary["errors"])
    return summary


def tick_all_running(ref_now: Optional[datetime] = None) -> Dict[str, Any]:
    """Run daily_tick for every non-terminal, non-draft ladder (SCHEDULED /
    RUNNING / PAUSED / ENDED — CREATED drafts excluded) — the body of the
    recurring 09:15 scheduler. This is what auto-activates a SCHEDULED campaign on
    its start_date. Idempotent + never raises."""
    summary: Dict[str, Any] = {"ticked": 0, "opened": 0, "errors": 0}
    try:
        with falcon_conn() as con:
            rows = con.execute(
                "SELECT ladder_id FROM autotrade_ladders "
                "WHERE status IN (?,?,?,?) ORDER BY created_at ASC",
                (STATUS_SCHEDULED, STATUS_RUNNING, STATUS_PAUSED,
                 STATUS_ENDED)).fetchall()
    except Exception as e:  # pragma: no cover
        log.exception("ladder tick_all: query failed: %s", e)
        return summary
    for row in rows:
        lid = row["ladder_id"]
        try:
            lad = LadderCampaign.load(lid)
            if lad is None:
                continue
            res = lad.daily_tick(ref_now=ref_now)
            summary["ticked"] += 1
            if res.get("opened"):
                summary["opened"] += 1
        except Exception as e:
            log.exception("ladder tick_all: %s failed: %s", lid, e)
            summary["errors"] += 1
    return summary
