"""TradingSession — orchestrates all layers.

Lifecycle:
  create()   → persist config, status CREATED. No orders.
  start(when)→ when="now"  : fire entries IMMEDIATELY (route → size → place →
                             register → status RUNNING). Entries respect
                             MARKET/LIMIT/VWAP. (Default — backward-compatible.)
               when="scheduled": parse config.entry_time as today-IST. Future →
                             status SCHEDULED + a background scheduler thread
                             that fires at entry_time. Past → fire now (fallback,
                             with a note). The order-firing leg itself lives in
                             _fire_entries() and is shared by both paths.

  SCHEDULED RESTART CAVEAT: a scheduled fire lives only in this process (same as
  the tick driver). If the backend restarts after a session is SCHEDULED but
  before entry_time, the in-memory timer is lost and it will NOT auto-fire — the
  operator must re-start it. See monitoring/entry_scheduler.py.
  tick()     → refresh LTPs → compute gross_return → snapshot → check kill
               switch threshold → fire if breached.
  kill()     → manual kill (same path as automatic).
  status()   → live status payload for the API.

Falcon picks are READ-ONLY from falcon_signals_live. We never write to it.

DATA-ISOLATION: all session positions live in autotrade_positions (the
PositionRegistry / PortfolioMonitor / kill switch operate ONLY on that table,
keyed by session_id). The autotrade system NEVER reads, writes, or locks
falcon_position_state — that belongs to the existing Falcon swing system.

TICK DRIVER: start() launches a per-session background tick driver that refreshes
LTPs + gross_return and AUTO-fires the kill switch on threshold breach; kill()
and the kill switch stop it. Paper = no real orders.

PAPER vs LIVE: mode defaults to 'paper'. In paper mode brokers are built with
dry_run=True → no real orders. Live requires mode='live' AND each broker's
own live gate (FALCON_AUTOTRADE_ENABLED). The kill switch is independently
gated by config.kill_switch_enabled (default False).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from falcon.db import falcon_conn

from .config import TradingSessionConfig
from . import trading_calendar
from .capital import CapitalAllocator, InsufficientCapitalError, _margin_product
from . import risk_manager
from .broker.base import Pick
from .broker.router import BrokerRouter, build_client
from .execution.orders import build_order, place_order_with_retry
from .execution.slippage import record_slippage
from . import order_ledger
from . import alerts
from .monitoring.registry import PositionRegistry
from .monitoring.monitor import PortfolioMonitor, compute_kill_preview
from .monitoring.kill_switch import KillSwitchExecutor
from .monitoring.gtt_manager import GTTManager
from . import exit_gate as _exit_gate_mod
from .monitoring import tick_driver
from .monitoring import entry_scheduler
from .monitoring import fire_guard
from .monitoring import ws_driver
from .monitoring import square_off_scheduler
from .monitoring import trail_engine

log = logging.getLogger("kanida.autotrade.session")
IST = timezone(timedelta(hours=5, minutes=30))

# SPEED PASS: max concurrent entry legs (asyncio.gather over _place_one).
# Kite caps order placement at ~10/s; 5–10 concurrent is safe. Configurable.
def _entry_concurrency() -> int:
    try:
        v = int(os.environ.get("FALCON_AUTOTRADE_ENTRY_CONCURRENCY", "8"))
        return max(1, min(v, 10))
    except ValueError:
        return 8


_ENTRY_CONCURRENCY = _entry_concurrency()

# Max seconds to wait for a per-position GTT cancel to confirm before proceeding
# to the market exit (R3). On a TIMEOUT the OCO may still be live, so the exit qty
# is clamped to the session-scoped live-held qty. Module-level so it is tunable
# (and testable) without touching the exit logic.
_GTT_CANCEL_TIMEOUT_SEC = 5.0


def _now_ist_iso() -> str:
    return datetime.now(IST).isoformat()


# ── Clock seam (DETERMINISM) ─────────────────────────────────────────────────
# Production reads the real IST wall clock. Tests (and only tests) may freeze
# "now" by setting FALCON_AUTOTRADE_FAKE_NOW="YYYY-MM-DDTHH:MM:SS" (interpreted
# IST) or by calling set_fake_now(dt). When UNSET this is byte-identical to
# datetime.now(IST) — no behaviour change in prod. Used by every fire-gate path
# so a frozen clock yields a deterministic trading-day/market-open verdict.
_FAKE_NOW: Optional[datetime] = None


def set_fake_now(dt: Optional[datetime]) -> None:
    """TEST ONLY: freeze (or clear with None) the IST 'now' used by fire gates."""
    global _FAKE_NOW
    _FAKE_NOW = dt


def now_ist() -> datetime:
    """The current IST time, honouring a test clock override if present.

    Order: explicit set_fake_now() > env FALCON_AUTOTRADE_FAKE_NOW > real now.
    Env value is parsed as a naive IST clock ("YYYY-MM-DDTHH:MM:SS" or with a
    space) and stamped with the IST tzinfo. Any parse failure falls back to the
    real clock (never crashes a fire path)."""
    if _FAKE_NOW is not None:
        return _FAKE_NOW
    raw = os.environ.get("FALCON_AUTOTRADE_FAKE_NOW", "").strip()
    if raw:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
            try:
                p = datetime.strptime(raw, fmt)
                return p.replace(tzinfo=IST)
            except ValueError:
                continue
    return datetime.now(IST)


def _owner_is_admin(user_id) -> bool:
    """True iff the owning user_id is an ADMIN/operator (power_user role='admin').

    ADMIN-owned sessions may use the operator's global broker account (the admin
    IS the operator); non-admin sessions must resolve their OWN account — see the
    _build_kite isolation guard. Fail-CLOSED: any lookup failure / unknown user →
    False (treat as non-admin → block the global fallback), so we never open the
    operator's account to an unverified owner. Cheap: one indexed-PK read, done
    once per session broker-build.
    """
    if user_id is None or str(user_id).strip() == "":
        return False
    try:
        import sqlite3
        from power_user import config as _pc
        con = sqlite3.connect(_pc.POWER_DB_PATH)
        try:
            row = con.execute(
                "SELECT role FROM power_user_users WHERE id = ?",
                (int(str(user_id).strip()),)).fetchone()
        finally:
            con.close()
        return bool(row and str(row[0]).lower() == "admin")
    except Exception as e:  # never let this crash a fire path
        log.warning("owner-admin lookup failed for user_id=%r: %s", user_id, e)
        return False


def _last_tick_age_ms() -> Optional[int]:
    """Age in ms of the newest WS tick the system has received (now − tick ts),
    for the SPEED-PASS status readout. None when the ticker is unavailable / has
    no ticks yet (e.g. tests, pre-open, WS down)."""
    try:
        from falcon.trade.services import kite_ticker
        last = kite_ticker.last_tick_at()
    except Exception:  # pragma: no cover - defensive
        return None
    if last is None:
        return None
    try:
        return int(max(0.0, (datetime.now(IST) - last).total_seconds() * 1000))
    except Exception:  # pragma: no cover
        return None


def _oldest_mark_age_ms(positions) -> Optional[int]:
    """Lifecycle#8 — age in ms of the OLDEST open-position mark (now − min
    ltp_as_of across the given positions). Surfaces a stalled tick (stale marks)
    in status(). None when no position carries a mark timestamp yet."""
    oldest = None
    for p in positions or []:
        ts = p.get("ltp_as_of")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            continue
        if oldest is None or dt < oldest:
            oldest = dt
    if oldest is None:
        return None
    try:
        return int(max(0.0, (datetime.now(IST) - oldest).total_seconds() * 1000))
    except Exception:  # pragma: no cover - defensive
        return None


# CLUSTER 5 ITEM 2 — the trail reasons that are PROFIT-side (abstained on a stale
# mark). SQUARE_OFF (time) + STOP (downside) are NOT profit-side and still fire.
_PROFIT_EXIT_REASONS = frozenset({"TRAIL_EXIT", "FLOOR_EXIT", "STEP_LOCK_EXIT"})


def _marks_stale_for_profit(positions, bound_sec, now=None) -> bool:
    """CLUSTER 5 ITEM 2 — True when the freshest mark feeding the basket is OLDER
    than bound_sec → the caller ABSTAINS from a PROFIT-side exit (trail/target).

    Reads autotrade_positions.ltp_as_of, which refresh_ltps advances to NOW only
    for a LIVE broker mark; a stale fallback (yesterday's ohlc close / entry price)
    keeps its last-live stamp so the mark AGES here. A position that HAS a mark
    (ltp set) but NO ltp_as_of at all is treated as STALE (it never received a
    live mark). bound_sec <= 0 disables the gate (never stale — pre-Cluster-5).
    Paper is unaffected: every paper tick marks to a live mock price (fresh)."""
    if not bound_sec or int(bound_sec) <= 0:
        return False
    now = now or datetime.now(IST)
    worst = None
    for p in positions or []:
        if p.get("ltp") is None:
            continue  # not yet marked → not part of the freshness judgement
        ts = p.get("ltp_as_of")
        if not ts:
            return True  # has a mark but never a live stamp → stale
        try:
            dt = datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            continue
        age = (now - dt).total_seconds()
        if worst is None or age > worst:
            worst = age
    if worst is None:
        return False
    return worst > int(bound_sec)


def _parse_entry_time_today_ist(entry_time: str) -> datetime:
    """Parse config.entry_time ("HH:MM" or "HH:MM:SS") as a time TODAY in IST.

    Returns an IST-aware datetime for today's date at the given clock time.
    Raises ValueError on an unparseable string so the caller can fall back.
    """
    s = (entry_time or "").strip()
    parsed = None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            parsed = datetime.strptime(s, fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        raise ValueError(f"unparseable entry_time: {entry_time!r}")
    now = datetime.now(IST)
    return now.replace(hour=parsed.hour, minute=parsed.minute,
                       second=parsed.second, microsecond=0)


def _parse_clock_hms(clock: str) -> Tuple[int, int, int]:
    """Parse an IST clock ("HH:MM"/"HH:MM:SS") to (h, m, s). Raises ValueError."""
    s = (clock or "").strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            p = datetime.strptime(s, fmt)
            return p.hour, p.minute, p.second
        except ValueError:
            continue
    raise ValueError(f"unparseable clock time: {clock!r}")


def compute_max_hold_cap_datetime(started_at_iso: Optional[str],
                                  max_hold_sessions: int,
                                  square_off_time: str) -> Optional[datetime]:
    """The IST-aware datetime at which a POSITIONAL basket must be squared off by
    the multi-session max-hold cap, or None when there is no cap / no anchor.

    The cap fires at square_off_time on the Nth NSE trading day counting the
    ENTRY day (the calendar date of started_at) as SESSION 1. Weekends + NSE
    holidays are SKIPPED via trading_calendar. Pure + deterministic: derived
    ENTIRELY from the persisted started_at, so it recomputes identically after a
    backend restart (no in-memory timer).

      max_hold_sessions <= 0      → None (no cap).
      started_at missing/garbled  → None (cannot anchor — never guess).
      N == 1                      → cap on the entry day itself.
      N  > 1                      → walk N-1 trading days forward from the entry
                                    day (entry-day-if-a-trading-day counts as 1).

    EDGE CASE — entry day is itself a non-trading day (e.g. started_at stamped on
    a weekend by a manual/paper action): we ANCHOR session 1 on the next trading
    day (inclusive), so the cap never lands on a closed market. The entry day
    almost always IS a trading day (entries only fire through the market-open
    gate), but this keeps the math total and safe.
    """
    from . import trading_calendar as _cal

    if max_hold_sessions is None or int(max_hold_sessions) <= 0:
        return None
    if not started_at_iso or not str(started_at_iso).strip():
        return None
    try:
        entry_dt = datetime.fromisoformat(str(started_at_iso).strip())
    except ValueError:
        return None
    entry_date = entry_dt.date()
    # COVERAGE GUARD (real-money safety): the max-hold cap walks trading days
    # forward from entry; if EITHER the entry day or the computed cap day lands in
    # a year whose NSE holidays we don't authoritatively know, the trading-day
    # walk is untrustworthy (a holiday could be miscounted as a session) → refuse
    # rather than compute a wrong flatten date. NO-OP for covered years.
    _cal.assert_calendar_covers(entry_date)
    # Session 1 = the entry day if it is a trading day, else the next trading day.
    session1 = _cal.next_trading_day(entry_date, inclusive=True)
    cap_date = session1
    for _ in range(int(max_hold_sessions) - 1):
        cap_date = _cal.next_trading_day(cap_date, inclusive=False)
    _cal.assert_calendar_covers(cap_date)
    try:
        hh, mm, ss = _parse_clock_hms(square_off_time)
    except ValueError:
        hh, mm, ss = 15, 29, 0   # safe default flatten time
    return datetime(cap_date.year, cap_date.month, cap_date.day,
                    hh, mm, ss, tzinfo=IST)


# ── EXECUTION-DATE / TRADING-DAY fire gate ───────────────────────────────────
# Terminal/deferred session statuses introduced by the trading-day rule. These
# are ADDITIVE — existing CREATED/RUNNING/SCHEDULED/CLOSED/KILLING are untouched.
STATUS_REJECTED_NON_TRADING_DAY = "REJECTED_NON_TRADING_DAY"
STATUS_EXPIRED_MISSED_WINDOW = "EXPIRED_MISSED_WINDOW"
STATUS_DEFERRED_MARKET_CLOSED = "DEFERRED_MARKET_CLOSED"


class FireGate:
    """Result of evaluating whether a fire may proceed RIGHT NOW.

    allow=True  → place orders.
    allow=False → DO NOT place; `status` is the terminal/deferred state to set,
                  `carry_to` (if set) is the next entry_date to roll to, and
                  `reason` is human-readable for status()/logs.
    """
    __slots__ = ("allow", "status", "reason", "carry_to", "fire_dt")

    def __init__(self, allow, status=None, reason=None, carry_to=None,
                 fire_dt=None):
        self.allow = allow
        self.status = status
        self.reason = reason
        self.carry_to = carry_to
        self.fire_dt = fire_dt


def evaluate_fire_gate(config: TradingSessionConfig, now_ist: datetime,
                       fire_dt: Optional[datetime] = None) -> FireGate:
    """The SINGLE trading-day/market-open decision shared by every fire path.

    Given the resolved target fire datetime and `now`, decide whether to fire.
    NEVER fires into a closed market or on a non-trading day. Pure + deterministic
    (pass now_ist in tests).

    Rules:
      1. The FIRE DATE must be a real NSE trading day. If not → per
         on_missed_window: expire (REJECTED_NON_TRADING_DAY) or carry forward.
      2. The market must be OPEN at `now` (09:15–15:30 IST on a trading day).
         If now is BEFORE today's open on a trading day → defer (still SCHEDULED,
         the scheduler waits). If now is AFTER close / a non-trading day now →
         per policy.
      3. If the target is in the PAST: fire ONLY if it is still the same trading
         day, the market is open, and we are within entry_grace_seconds of the
         target. Otherwise → expire/carry (the window was missed).
    """
    fdt = fire_dt or config.resolve_fire_datetime(now_ist)
    fire_date = fdt.date()
    grace = max(0, int(getattr(config, "entry_grace_seconds", 120)))
    policy = getattr(config, "on_missed_window", "expire")

    # COVERAGE GUARD (real-money safety): if the resolved fire date lands in a
    # year whose NSE holidays we don't authoritatively know, DO NOT fire — a wrong
    # "trading day" answer could place a real trade on a holiday. Refuse rather
    # than trust the heuristic. NO-OP for covered years (2025/2026 today).
    if not trading_calendar.is_calendar_authoritative(fire_date):
        return FireGate(
            False, status=STATUS_REJECTED_NON_TRADING_DAY,
            reason=(f"no authoritative NSE holiday coverage for "
                    f"{fire_date.year} — refusing to fire (auto-fetch may have "
                    f"failed; add {fire_date.year} to data/config/"
                    f"nse_holidays.txt)"),
            fire_dt=fdt)

    def _missed(reason: str) -> FireGate:
        if policy == "carry_next_trading_day":
            nxt = trading_calendar.next_trading_day(now_ist.date(), inclusive=True)
            # If now is already past today's open on a trading day, "today" is no
            # good — roll to a STRICTLY future trading day.
            if nxt == now_ist.date():
                nxt = trading_calendar.next_trading_day(now_ist.date())
            return FireGate(False, status="SCHEDULED",
                            reason=f"{reason} — carried to {nxt.isoformat()}",
                            carry_to=nxt.isoformat(), fire_dt=fdt)
        return FireGate(False, status=STATUS_EXPIRED_MISSED_WINDOW,
                        reason=reason, fire_dt=fdt)

    # 1. Fire DATE must be a trading day.
    if not trading_calendar.is_trading_day(fire_date):
        if policy == "carry_next_trading_day":
            nxt = trading_calendar.next_trading_day(fire_date, inclusive=True)
            return FireGate(False, status="SCHEDULED",
                            reason=(f"{fire_date.isoformat()} is not an NSE "
                                    f"trading day — carried to {nxt.isoformat()}"),
                            carry_to=nxt.isoformat(), fire_dt=fdt)
        return FireGate(False, status=STATUS_REJECTED_NON_TRADING_DAY,
                        reason=(f"{fire_date.isoformat()} is not an NSE trading "
                                f"day (weekend/holiday)"), fire_dt=fdt)

    # The fire date IS a trading day. Now consider WHEN relative to now.
    if fdt > now_ist:
        # Future target. If it's a future trading day, just wait (SCHEDULED).
        # The scheduler thread sleeps until fdt; nothing to fire now.
        return FireGate(False, status="SCHEDULED",
                        reason=f"waiting until {fdt.isoformat()}", fire_dt=fdt)

    # Target is now-or-past. We may fire only inside the open window + grace.
    if fire_date != now_ist.date():
        # The target day already fully elapsed (we are on a later day) → missed.
        return _missed(f"entry window on {fire_date.isoformat()} elapsed")

    # Same calendar day as the target. Is the market open right now?
    if not trading_calendar.is_market_open(now_ist):
        open_dt, close_dt = trading_calendar.market_open_for_date(fire_date)
        if now_ist < open_dt:
            # Before the bell on a trading day → defer (wait for open).
            return FireGate(False, status=STATUS_DEFERRED_MARKET_CLOSED,
                            reason=(f"market not yet open (opens "
                                    f"{open_dt.strftime('%H:%M')} IST)"),
                            fire_dt=fdt)
        # After close → window missed.
        return _missed(f"market closed for {fire_date.isoformat()}")

    # Market is OPEN and we're on the target trading day. If the target is in the
    # past, enforce the grace window (don't fire a long-stale target).
    behind = (now_ist - fdt).total_seconds()
    if behind > grace:
        return _missed(
            f"target {fdt.strftime('%H:%M:%S')} passed {int(behind)}s ago "
            f"(> grace {grace}s)")

    # All clear: trading day, market open, within grace of the target.
    return FireGate(True, status="RUNNING",
                    reason="trading day + market open", fire_dt=fdt)


# ── Falcon picks (read-only consumer) ────────────────────────────────────────

# Universe-filter SQL fragments — copied verbatim from the portal's
# falcon_top20_explainer._UNIVERSE_WHERE_CLAUSE dict. Applied IN the SQL query
# so the ranked ORDER BY respects the filtered universe, not post-fetch.
# "all500" = no extra clause = identical to previous behaviour (default).
_UNIVERSE_WHERE: dict = {
    "all500":   "",
    "nifty50":  "AND s.symbol IN (SELECT symbol FROM universe_master WHERE in_nifty50=1  AND is_active=1)",
    "nifty100": "AND s.symbol IN (SELECT symbol FROM universe_master WHERE in_nifty100=1 AND is_active=1)",
    "nifty200": "AND s.symbol IN (SELECT symbol FROM universe_master WHERE in_nifty200=1 AND is_active=1)",
    "fno":      "AND s.symbol IN (SELECT symbol FROM universe_master WHERE in_nifty200=1 AND is_active=1)",
}


def load_falcon_picks(top_n: int = 100,
                      universe_filter: str = "all500",
                      signal_date: Optional[str] = None) -> List[Pick]:
    """Read latest Falcon Top-N picks, optionally restricted to a universe
    membership filter (applied in SQL so ranked ordering respects the filter).

    NEVER writes to falcon_signals_live.

    signal_date: if None, uses MAX(signal_date) (i.e. today's picks).
    universe_filter: one of "all500"|"nifty50"|"nifty100"|"nifty200"|"fno".
      "all500" is the default and produces IDENTICAL results to the old call.
    """
    where_universe = _UNIVERSE_WHERE.get(universe_filter, "")
    with falcon_conn() as con:
        if signal_date is None:
            latest = con.execute(
                "SELECT MAX(signal_date) FROM falcon_signals_live"
            ).fetchone()[0]
        else:
            latest = signal_date
        if latest is None:
            return []
        sql = f"""
            SELECT s.symbol, s.rank, s.score, s.sector, s.close_at_signal,
                   s.n_fires
            FROM falcon_signals_live s
            WHERE s.signal_date = ?
              {where_universe}
            ORDER BY s.rank ASC
            LIMIT ?
        """
        rows = con.execute(sql, (latest, top_n)).fetchall()
    result = []
    for r in rows:
        n_fires = r["n_fires"] if r["n_fires"] else None
        avg_lift = (float(r["score"]) / float(n_fires)
                    if (r["score"] and n_fires and n_fires > 0) else None)
        result.append(Pick(
            symbol=r["symbol"], rank=r["rank"],
            score=r["score"] or 0.0, sector=r["sector"],
            close_at_signal=r["close_at_signal"],
            n_fires=n_fires, avg_lift=avg_lift))
    return result


def _resolve_falcon_selection(
        config, log_ctx: Optional[str] = None) -> Tuple[List[Pick], int]:
    """SINGLE shared Falcon-pick resolution for the preview, warm-resolve and
    real-fire paths (so they can never drift). Does: load → rank_filter →
    symbol_whitelist filter, and returns ``(picks, router_cap)`` where
    ``router_cap`` is the value to hand ``BrokerRouter(top_n_stocks=...)`` for
    its default (no explicit-symbols / no rank_range) branch.

    LOAD DEPTH:
      * ``symbol_whitelist`` set → load the full Top-50 selectable range, so a
        whitelisted name at rank 11..50 is actually loaded (the bug this fixes:
        the old ``max(top_n_stocks, 10)`` silently truncated the custom list).
      * else → ``max(top_n_stocks, 10)`` — byte-identical to the old default.
      * ``rank_filter`` set → deepen to cover its highest rank so a rank_filter
        referencing rank > depth isn't silently dropped (additive; only ADDS
        coverage, never removes a pick, and never changes ``router_cap``).

    ROUTER CAP:
      * ``symbol_whitelist`` set → the number of whitelist-matched picks, i.e.
        "custom list wins" — every matched name routes, the top_n_stocks cap is
        BYPASSED (operator decision).
      * else → ``config.top_n_stocks`` — unchanged; governs the DEFAULT top-N.

    ``log_ctx`` (e.g. ``"preview: "`` or ``f"session {sid}: "``) prefixes the
    "whitelisted symbol(s) not in today's picks" warning for names genuinely
    absent from the ranked set. ``None`` suppresses it (the best-effort warm
    path stays silent, as before).
    """
    if config.symbol_whitelist:
        depth = 50
    else:
        depth = max(config.top_n_stocks, 10)
    if config.rank_filter:
        depth = max(depth, max(config.rank_filter))

    picks = load_falcon_picks(
        top_n=depth, universe_filter=config.universe_filter)
    if config.rank_filter:
        picks = [p for p in picks if p.rank in config.rank_filter]

    if config.symbol_whitelist is not None:
        whitelist_set = set(config.symbol_whitelist)
        if log_ctx:
            missing = whitelist_set - {p.symbol for p in picks}
            if missing:
                log.warning("%swhitelisted symbol(s) not in today's picks: %s",
                            log_ctx, sorted(missing))
        picks = [p for p in picks if p.symbol in whitelist_set]
        router_cap = len(picks)
    else:
        router_cap = config.top_n_stocks

    return picks, router_cap


def _get_tick_for(broker, symbol: str) -> float:
    """The instrument tick size for `symbol` (marketable-limit rounding).

    Reuses the legacy mtf_eligibility.get_tick_size (per-scrip tick cache) via the
    broker's live kite client. Best-effort: any failure (paper / mock broker with
    no kite / lookup miss) falls back to 0.05 — round_to_tick_size also defends
    the same default, so a wrong tick never crashes the fire, at worst rounds to
    the NSE minimum tick (which stays inside the circuit cap the pricer applies)."""
    try:
        from falcon.trade.services.mtf_eligibility import get_tick_size
        kite = getattr(broker, "kite", None)
        if kite is not None:
            t = get_tick_size(kite, symbol)
            if t and t > 0:
                return float(t)
    except Exception as e:  # pragma: no cover - defensive
        log.debug("tick lookup failed for %s: %s", symbol, e)
    return 0.05


def _filter_fno_eligible(picks: List[Pick], broker, expiry_preference: str,
                         session_id: str = "") -> List[Pick]:
    """FUTURES symbol eligibility: keep ONLY picks that HAVE a tradeable
    current-month future. Names without a future are logged + SKIPPED — we never
    fabricate a contract. Returns the filtered pick list (order preserved).

    Uses broker.get_active_futures_or_none (None → no future → drop). Best-effort:
    a broker/lookup error for one symbol drops that symbol, never aborts the set.
    Equity/MTF sessions never call this (their picks are always eligible)."""
    out: List[Pick] = []
    for p in picks:
        try:
            contract = broker.get_active_futures_or_none(p.symbol, expiry_preference)
        except Exception as e:  # pragma: no cover - defensive
            log.warning("session %s: F&O eligibility lookup failed for %s (%s) "
                        "— skipping", session_id, p.symbol, e)
            contract = None
        if contract:
            out.append(p)
        else:
            log.info("session %s: %s has no tradeable current-month future — "
                     "skipping (F&O-ineligible)", session_id, p.symbol)
    return out


def _resolve_basket_symbols(session) -> List[str]:
    """Resolve the (approx) symbols a session will trade — the same Falcon picks +
    filters + routing _fire_entries uses, WITHOUT sizing/placing. Best-effort:
    used only to pre-subscribe/pre-prime the WARM path, so a miss just means one
    symbol needs a REST quote at fire time (never a failure). FUT profiles map to
    the current-month contract symbol so the FULL subscription + circuit prime hit
    the tradeable instrument."""
    try:
        picks, router_cap = _resolve_falcon_selection(session.config)
        router = BrokerRouter(top_n_stocks=router_cap)
        routed = router.route_picks(picks, session.config.broker_profiles)
    except Exception as e:  # pragma: no cover - defensive
        log.debug("prewarm: pick resolution failed for %s: %s",
                  session.session_id, e)
        return []
    syms: List[str] = []
    for prof in session.config.broker_profiles:
        if not prof.enabled:
            continue
        prof_picks = routed.get(prof.profile_id, [])
        broker = session.brokers.get(prof.profile_id)
        if prof.instrument_type == "FUT" and broker is not None:
            for p in prof_picks:
                try:
                    c = broker.get_active_futures_or_none(
                        p.symbol, session.config.expiry_preference)
                except Exception:  # pragma: no cover
                    c = None
                syms.append(c or p.symbol)
        else:
            syms.extend(p.symbol for p in prof_picks)
    # De-dup preserving order.
    seen: set = set()
    out: List[str] = []
    for s in syms:
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def prewarm_execution(session) -> Dict[str, Any]:
    """PHASE-2 PRE-OPEN WARM (best-effort, never blocks/raises). Only meaningful
    when execution_mode=="marketable_limit": subscribe the resolved basket to the
    shared KiteTicker in MODE_FULL (so bid/ask/ltp stream over the WS, network-
    free) AND prime the per-day circuit-limit cache with ONE REST kite.quote (per
    live broker). At the 09:15 fire, get_quotes then reads the whole book from the
    WS + the cached circuit → ZERO REST calls in the hot path.

    SAFE: a no-op for the default "market" mode and for paper/disabled brokers
    (subscribe_full no-ops if the ticker isn't connected; prime_circuit_limits
    no-ops unless _live_allowed()). Any error is swallowed — a failed prewarm just
    means the first fire falls back to the (already-correct) batched REST quote."""
    out: Dict[str, Any] = {"subscribed_full": 0, "circuits_primed": 0,
                           "symbols": []}
    try:
        if getattr(session.config, "execution_mode", "market") != "marketable_limit":
            return out
        if not session.brokers:
            session._build_brokers()
        symbols = _resolve_basket_symbols(session)
        out["symbols"] = symbols
        if not symbols:
            return out
        # Subscribe the WHOLE basket to FULL on the shared ticker (one call).
        try:
            from falcon.trade.services.kite_ticker import subscribe_full
            out["subscribed_full"] = int(subscribe_full(symbols) or 0)
        except Exception as e:  # pragma: no cover - best-effort
            log.debug("prewarm subscribe_full failed for %s: %s",
                      session.session_id, e)
        # Prime the circuit day-cache per live broker (one REST each).
        primed = 0
        for prof in session.config.broker_profiles:
            if not prof.enabled:
                continue
            broker = session.brokers.get(prof.profile_id)
            if broker is None:
                continue
            try:
                primed += int(broker.prime_circuit_limits(symbols) or 0)
            except Exception as e:  # pragma: no cover - best-effort
                log.debug("prewarm prime_circuit_limits failed for %s: %s",
                          prof.profile_id, e)
        out["circuits_primed"] = primed
        log.info("session %s prewarm: full=%d circuits=%d (%d symbols)",
                 session.session_id, out["subscribed_full"], primed, len(symbols))
    except Exception as e:  # pragma: no cover - never raise out of prewarm
        log.debug("prewarm_execution failed for %s: %s", session.session_id, e)
    return out


def _preview_resolve_creds(prof, user_id: Optional[str]) -> None:
    """Best-effort vault cred resolution for the PREVIEW path (paper-only).

    Mirrors TradingSession._resolve_account_creds but is standalone (preview
    creates no session). NO-OP + global fallback when there is no bound account,
    the vault is disabled, or the account can't be resolved — preview never fails
    on creds."""
    acct_id = getattr(prof, "broker_account_id", None)
    if acct_id is None:
        return
    try:
        from . import vault
        if not vault.vault_enabled():
            prof.broker_account_id = None
            return
        creds = vault.get_decrypted_creds(acct_id, user_id=user_id)
        if creds is None or not creds.api_key or not creds.access_token:
            prof.broker_account_id = None
            return
        prof.api_key = creds.api_key
        prof.api_secret = creds.api_secret or ""
        prof.access_token = creds.access_token
        prof.broker_account_id = acct_id
        # Bind the adapter to the ACCOUNT'S broker. The default preview profile is
        # hardcoded broker_name="zerodha"; without this a Rupeezy (or any non-Kite)
        # account is sized through the Zerodha adapter → every Kite LTP/margin call
        # fails → "no sizable positions". Keep the default when the vault omits it.
        if getattr(creds, "broker", None):
            prof.broker_name = creds.broker
    except Exception:  # pragma: no cover - preview must never crash on creds
        prof.broker_account_id = None


def preview_session_sizing(config: TradingSessionConfig,
                           mode: str = "paper",
                           user_id: Optional[str] = None,
                           broker_account_id: Optional[str] = None
                           ) -> Dict[str, Any]:
    """Size the picks EXACTLY as _fire_entries would (route → allocate →
    per-position qty incl. MTF margin) but PLACE NOTHING and create NO session.

    Returns the estimated invested_basis, total_allocated_capital, leverage, the
    per-position rows, and the kill_preview — to power the UI preview before
    Start. Brokers are built in PAPER mode (dry_run) only to read LTP / MTF
    margin; no order is ever placed and no DB row is created.

    invested_basis = Σ(qty * ref_price), where ref_price is the broker LTP at
    preview time (the entry mark). leverage = invested_basis /
    total_allocated_capital (>1 under MTF, ~≤1 for CNC). Falls back to the fund
    capital when nothing is sizable (no picks / no LTP).
    """
    config.validate()

    # Ensure broker_profiles is populated the same way _build_brokers does.
    profiles = list(config.broker_profiles or [])
    if not profiles:
        from .config import BrokerProfile
        profiles = [BrokerProfile(
            profile_id="zerodha_default", broker_name="zerodha",
            allocated_capital=config.total_allocated_capital,
            order_product=config.order_product,
            instrument_type=config.instrument_type,
            broker_account_id=broker_account_id)]
    # PHASE-2: best-effort per-account cred resolution (global fallback on miss).
    for _p in profiles:
        _preview_resolve_creds(_p, user_id)

    falcon_picks, router_cap = _resolve_falcon_selection(
        config, log_ctx="preview: ")
    router = BrokerRouter(top_n_stocks=router_cap)
    routed = router.route_picks(falcon_picks, profiles)

    allocator = CapitalAllocator(config)
    positions: List[Dict[str, Any]] = []
    invested_basis = 0.0
    dry_run = (mode != "live")
    for prof in profiles:
        if not prof.enabled:
            continue
        try:
            broker = build_client(prof, dry_run=dry_run)
        except Exception as e:  # unknown/unimplemented broker — skip, note it
            log.warning("preview: broker build failed for %s: %s",
                        prof.profile_id, e)
            continue
        picks = routed.get(prof.profile_id, [])
        # FUTURES symbol eligibility (preview mirrors fire): drop picks with no
        # tradeable current-month future so the preview count matches what fires.
        if prof.instrument_type == "FUT":
            picks = _filter_fno_eligible(
                picks, broker, config.expiry_preference)
        amounts = allocator.allocate([p.symbol for p in picks])
        # SPEED PASS: ONE batched LTP + MTF/MIS-margin prefetch for the preview.
        fund_syms = [p.symbol for p in picks if amounts.get(p.symbol, 0.0) > 0]
        try:
            pcache = allocator.prefetch(fund_syms, broker)
        except Exception:  # pragma: no cover - per-symbol fallback inside
            pcache = {}
        # FEATURE C: preview mirrors fire — the whole-portfolio plan (redistribute
        # + skip) so the preview count / invested_basis MATCH what will fire.
        plan = allocator.plan_quantities(fund_syms, broker, cache=pcache)
        plan_qtys = plan["quantities"]
        for sk in plan["skipped"]:
            positions.append({"symbol": sk["symbol"],
                              "broker_profile": prof.profile_id,
                              "status": "SKIPPED", "reason": sk["reason"]})
        plan_units = plan.get("units", {})
        for pick in picks:
            qty = plan_qtys.get(pick.symbol)
            if not qty or qty <= 0:
                continue
            _c = pcache.get(pick.symbol, {})
            ref_price = float(_c.get("ltp") or 0.0) or (broker.get_ltp(pick.symbol) or 0.0)
            invested = qty * ref_price
            invested_basis += invested
            row = {"symbol": pick.symbol,
                   "broker_profile": prof.profile_id,
                   "qty": qty, "ref_price": ref_price,
                   "invested_value": invested,
                   "order_product": prof.order_product,
                   "instrument_type": prof.instrument_type}
            # SIZING TRANSPARENCY: surface lots + margin per position. unit_budget =
            # ₹ per increment (margin_per_lot for FUT / margin_per_share for MTF);
            # unit_qty = shares per increment (lot_size for F&O, 1 for cash equity).
            _u = plan_units.get(pick.symbol) or {}
            _ub = float(_u.get("unit_budget") or 0.0)
            _uq = int(_u.get("unit_qty") or 1) or 1
            _n_units = (qty // _uq) if _uq > 0 else 0
            row["margin"] = _n_units * _ub          # ₹ capital/margin deployed here
            if prof.instrument_type in ("FUT", "CE", "PE"):
                row["lots"] = _n_units
                row["lot_size"] = _uq
                row["margin_per_lot"] = _ub
                row["notional"] = invested          # qty * price = contract exposure
            # ICEBERG (CAP 4) — surface when this leg will slice, so the operator
            # sees n_slices + slice_qty BEFORE Start. Inert (no keys) unless
            # iceberg is enabled AND the qty exceeds the effective slice cap.
            from . import iceberg as _iceberg
            _eff_slice = _iceberg.effective_slice_qty(
                config, pick.symbol, int(qty), ref_price or None)
            _legs = _iceberg.plan_iceberg_legs(int(qty), _eff_slice)
            if config.iceberg_enabled and len(_legs) > 1:
                row["iceberg"] = True
                row["n_slices"] = len(_legs)
                row["slice_qty"] = int(_eff_slice)
            positions.append(row)

    total_alloc = float(config.total_allocated_capital)
    non_skipped = [p for p in positions if p.get("status") != "SKIPPED"]
    # F&O PARITY with the LIVE frozen basis: freeze_invested_basis() stores the
    # FUND (total_allocated_capital) for F&O, NOT the Σ(qty*price) notional. Mirror
    # that here so /preview's invested_basis, leverage and kill_preview MATCH what
    # the session reports once running (each F&O row keeps its contract exposure on
    # its own "notional" field). Equity (EQ/MTF/CNC) is unchanged — leveraged
    # notional, so preview already equals the frozen basis there.
    is_fno = any(str(p.get("instrument_type", "")).upper() in ("FUT", "CE", "PE")
                 for p in non_skipped)
    reported_basis = total_alloc if is_fno else invested_basis
    basis = reported_basis if reported_basis > 0 else total_alloc
    leverage = (basis / total_alloc) if total_alloc > 0 else 0.0
    # Total ₹ margin/capital actually deployed across sized positions (F&O margin /
    # MTF margin). For cash CNC this ≈ invested_basis.
    total_margin = sum(float(p.get("margin") or 0.0) for p in non_skipped)
    return {
        "invested_basis": reported_basis,
        "total_allocated_capital": total_alloc,
        "total_margin": total_margin,
        "leverage": leverage,
        "n_positions": len(non_skipped),
        "positions": positions,
        "kill_preview": compute_kill_preview(
            kill_switch_enabled=config.kill_switch_enabled,
            kill_switch_pct=config.kill_switch_pct,
            kill_switch_direction=config.kill_switch_direction,
            invested_basis=basis,
            total_allocated_capital=total_alloc,
            kill_switch_target_pct=config.kill_switch_target_pct,
            kill_switch_stop_pct=config.kill_switch_stop_pct),
        "skipped_picks": [p for p in positions if p.get("status") == "SKIPPED"],
        # ITEM 3 — explicit risk_basis label + the ₹ concentration/fat-finger
        # thresholds so the leverage math is unambiguous in the preview.
        "risk_basis": getattr(config, "risk_basis", "notional"),
        "concentration_limits": risk_manager.concentration_thresholds_rs(config),
    }


def estimate_session_charges_rs(session_id: str, product: str) -> float:
    """SPRINT CLUSTER 8 ITEM 5 — estimate the round-trip statutory + broker charges
    (₹) across a session's positions, for surfacing a NET P&L (gross − charges) in
    the live panel. For each still-relevant row: buy turnover = qty×avg_price, sell
    turnover = qty×exit_price (CLOSED) or qty×ltp (OPEN mark = "if exited now").
    Uses the per-row instrument_type (F&O vs equity charge model) and the session's
    product. Best-effort — never raises; any error → 0.0 (a floor, not optimistic)."""
    from .charges import estimate_charges
    total = 0.0
    try:
        with falcon_conn() as con:
            rows = con.execute(
                "SELECT qty, avg_price, ltp, exit_price, status, instrument_type "
                "FROM autotrade_positions WHERE session_id=? "
                "AND status IN ('OPEN','EXIT_FAILED','CLOSED')",
                (session_id,)).fetchall()
        for r in rows:
            qty = int(r["qty"] or 0)
            avg = float(r["avg_price"] or 0.0)
            if qty <= 0 or avg <= 0:
                continue
            itype = r["instrument_type"] or "EQ"
            if str(r["status"]) == "CLOSED":
                sell_px = float(r["exit_price"] or 0.0) or avg
            else:
                sell_px = float(r["ltp"] or 0.0) or avg
            buy_value = qty * avg
            sell_value = qty * sell_px
            ch = estimate_charges(product, buy_value, sell_value, legs=2,
                                  instrument_type=itype)
            total += float(ch.get("total") or 0.0)
    except Exception as e:  # pragma: no cover - never block status()
        log.debug("estimate_session_charges_rs(%s) failed: %s", session_id, e)
        return 0.0
    return round(total, 2)


def _falcon_owned_exit_ids_tags(session_id: str, symbol: str,
                                broker_profile: Optional[str] = None):
    """CLUSTER 9 ITEM 4 (2026-07-11) — the set of broker order-ids AND compact tags
    that provably belong to THIS session's (symbol[, profile]) leg: the recorded
    entry/exit/GTT order-ids + the compact_tag of the persisted client_order_id /
    exit_client_order_id. A resting broker order is ONLY ours if its order-id is in
    the id-set OR its `tag` is in the tag-set. Scoped to (session, symbol
    [, broker_profile]) so a foreign / other-session / manual resting order is
    never mistaken for ours. Best-effort → empty sets on any error."""
    ids: set = set()
    tags: set = set()
    try:
        from autotrade.order_ledger import compact_tag
        with falcon_conn() as con:
            if broker_profile is not None:
                rows = con.execute(
                    """SELECT entry_order_id, exit_order_id, gtt_id,
                              client_order_id, exit_client_order_id
                       FROM autotrade_positions
                       WHERE session_id=? AND symbol=?
                         AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                    (session_id, symbol, broker_profile)).fetchall()
            else:
                rows = con.execute(
                    """SELECT entry_order_id, exit_order_id, gtt_id,
                              client_order_id, exit_client_order_id
                       FROM autotrade_positions
                       WHERE session_id=? AND symbol=?""",
                    (session_id, symbol)).fetchall()
        for r in rows:
            for v in (r["entry_order_id"], r["exit_order_id"], r["gtt_id"]):
                if v not in (None, ""):
                    ids.add(str(v))
            for c in (r["client_order_id"], r["exit_client_order_id"]):
                if c not in (None, ""):
                    tags.add(compact_tag(str(c)))
    except Exception as e:  # pragma: no cover - defensive
        log.debug("_falcon_owned_exit_ids_tags(%s/%s) failed: %s",
                  session_id, symbol, e)
    return ids, tags


async def _our_working_exit_qty(broker: Any, symbol: str,
                                direction: str = "long",
                                owned_ids: Optional[set] = None,
                                owned_tags: Optional[set] = None) -> int:
    """Best-effort Σ qty of our still-RESTING exit-side orders for `symbol` at the
    broker (Fix 2 Part 2, 2026-07-11). A resting/unfilled exit does NOT reduce
    positions()['net'], so without netting it a next-tick re-fire would place a
    SECOND exit while the first still rests → both fill on a tick-back → OVERSELL.

    Reads broker.get_pending_orders() ONLY. Counts a pending order iff it matches
    the symbol AND the CLOSING transaction side (long→SELL, short→BUY-cover) — so a
    resting ENTRY (or an order on another symbol) is never counted. Any error /
    unknown shape / missing field → 0 (fall back to the existing behaviour; the
    resting-order CANCEL is the primary guard, this is belt-and-suspenders).

    CLUSTER 9 ITEM 4 (2026-07-11): a matching order is counted ONLY when it is
    provably FALCON-OWNED — its order-id is in `owned_ids` OR its `tag` is in
    `owned_tags` (the recorded ids / compact tags for THIS session+account leg).
    A manual / foreign / other-session resting same-side order is NEVER counted, so
    it can never block a legit exit. When both sets are empty/None NOTHING is
    counted (a foreign order must never gate our exit)."""
    try:
        pending = await broker.get_pending_orders()
    except Exception:  # pragma: no cover - defensive; primary guard is the cancel
        return 0
    if not pending:
        return 0
    owned_ids = owned_ids or set()
    owned_tags = owned_tags or set()
    exit_side = "BUY" if str(direction).lower() == "short" else "SELL"
    base = str(symbol).split(":", 1)[0]
    total = 0
    for o in pending:
        if not isinstance(o, dict):
            continue
        ts = str(o.get("tradingsymbol") or "")
        if ts not in (base, str(symbol)):
            continue
        txn = str(o.get("transaction_type") or "").upper()
        if txn != exit_side:
            continue
        # OWNERSHIP GATE (ITEM 4): only our own recorded id / tag counts.
        oid = str(o.get("order_id") or "")
        otag = str(o.get("tag") or "")
        if not ((oid and oid in owned_ids) or (otag and otag in owned_tags)):
            continue
        q = o.get("pending_quantity")
        if q in (None, ""):
            q = o.get("quantity") or 0
        try:
            total += int(q)
        except (TypeError, ValueError):
            continue
    return int(total)


async def _foreign_same_side_pending(broker: Any, symbol: str, side: str,
                                     owned_ids: Optional[set] = None,
                                     owned_tags: Optional[set] = None
                                     ) -> List[Dict[str, Any]]:
    """CLUSTER 9c FIX F5 (2026-07-11) — FOREIGN (non-Falcon-owned) PENDING orders on
    the SAME symbol + SAME `side` (BUY/SELL) in the SAME account.

    THE FUNGIBLE-ACCOUNT RISK: when AutoTrade shares the operator's own broker login,
    a MANUAL resting order and a Falcon order on the same symbol+side can BOTH fill —
    Falcon has no way to prevent the manual order and cannot tell the account's net
    apart cleanly. This detector surfaces that conflict so ENTRY can REFUSE it and
    EXIT can PAGE it (an exit is more urgent than the conflict, so we do NOT block it;
    the C1 our_held clamp already limits Falcon to ITS qty). THE REAL FIX is a
    DEDICATED AutoTrade broker account — a shared login makes this irreducible in code.

    Reads broker.get_pending_orders() ONLY (per-account = this profile's own client,
    so detection is already account-scoped). Returns a list of
    {order_id, tag, qty, txn} for each pending order matching symbol+side that is NOT
    provably ours (id not in owned_ids AND tag not in owned_tags). Paper / no pending
    / probe error → [] (byte-identical; conservative — a probe error never blocks)."""
    try:
        pending = await broker.get_pending_orders()
    except Exception:  # pragma: no cover - defensive; never block on a probe error
        return []
    if not pending:
        return []
    owned_ids = owned_ids or set()
    owned_tags = owned_tags or set()
    want = str(side).upper()
    base = str(symbol).split(":", 1)[0]
    out: List[Dict[str, Any]] = []
    for o in pending:
        if not isinstance(o, dict):
            continue
        ts = str(o.get("tradingsymbol") or "")
        if ts not in (base, str(symbol)):
            continue
        txn = str(o.get("transaction_type") or "").upper()
        if txn != want:
            continue
        oid = str(o.get("order_id") or "")
        otag = str(o.get("tag") or "")
        if (oid and oid in owned_ids) or (otag and otag in owned_tags):
            continue  # provably OURS — not a conflict
        q = o.get("pending_quantity")
        if q in (None, ""):
            q = o.get("quantity") or 0
        out.append({"order_id": oid, "tag": otag, "qty": q, "txn": txn})
    return out


async def _exit_single_position(
        session_id: str,
        position: Dict[str, Any],
        reason: str,
        brokers: Dict[str, Any],
        registry: Any,
        gtt_manager: Any,
        kite_product: Optional[str] = None,
        exec_cfg: Any = None,
) -> Dict[str, Any]:
    """SINGLE-FLIGHT wrapper (Fix B2, 2026-07-10) around the real per-position exit.

    While an exit ORDER is in flight for this (session_id, symbol), a SECOND
    concurrent placement — even the same reason, even from the other driver
    (tick_driver + ws_driver both ran STOP_STOCK → a short covered TWICE → a naked
    long; BRIGADE 2026-07-10) — is a NO-OP. The in-process mutex is held for the
    WHOLE exit (place + fill confirmation) so a re-fire DURING confirmation cannot
    double-place either. The legitimate sequential EXIT_FAILED retry is preserved:
    the prior flight has ended (slot cleared) before the next tick re-attempts."""
    symbol = position["symbol"]
    # CLUSTER 9 ITEM 1: the single-flight slot is keyed by the FULL leg identity
    # (session_id, symbol, broker_profile) so the same symbol on two broker
    # profiles flies independently — one profile's in-flight exit never blocks the
    # other's.
    _prof = position.get("broker_profile")
    if not _exit_gate_mod.begin_exit_flight(session_id, symbol,
                                            broker_profile=_prof):
        log.warning("exit %s/%s: an exit order is already IN FLIGHT — skipping "
                    "(no second order, reason=%s)", session_id, symbol, reason)
        return {"symbol": symbol, "status": "BLOCKED_INFLIGHT", "reason": reason}
    try:
        return await _exit_single_position_inner(
            session_id=session_id, position=position, reason=reason,
            brokers=brokers, registry=registry, gtt_manager=gtt_manager,
            kite_product=kite_product, exec_cfg=exec_cfg)
    finally:
        _exit_gate_mod.end_exit_flight(session_id, symbol, broker_profile=_prof)


async def _exit_single_position_inner(
        session_id: str,
        position: Dict[str, Any],
        reason: str,
        brokers: Dict[str, Any],
        registry: Any,
        gtt_manager: Any,
        kite_product: Optional[str] = None,
        exec_cfg: Any = None,
) -> Dict[str, Any]:
    """Our backend directly exits one position (per-stock software stop).

    Sequence:
      1. Claim the exit via the session-scoped exit gate (prevents double-fire
         with the portfolio kill switch or a concurrent GTT reconcile).
      2. Cancel the position's GTT best-effort BEFORE placing the sell, so the
         broker-held backup can't re-fire after we flatten.
      3. Place a market sell via the position's broker.
      4. Mark CLOSED (with the confirmed fill price) or EXIT_FAILED on error.

    Reuses the same exit_gate.claim_exit_session / registry.mark_closed /
    registry.mark_exit_failed patterns as kill_switch.fire().
    Returns a result dict with at least {"symbol", "status"}.
    """
    symbol = position["symbol"]
    prof_id = position.get("broker_profile")
    broker = brokers.get(prof_id) or next(iter(brokers.values()), None)
    if broker is None:
        log.warning("per-stock stop %s/%s: no broker found — skip", session_id, symbol)
        return {"symbol": symbol, "status": "NO_BROKER"}

    # 1. Claim the exit gate so no other path double-fires this position.
    # CLUSTER 9 ITEM 1: scoped to the (session, symbol, broker_profile) leg.
    if not _exit_gate_mod.claim_exit_session(session_id, symbol, reason,
                                             broker_profile=prof_id):
        log.info("per-stock stop %s/%s: exit already claimed — skip",
                 session_id, symbol)
        return {"symbol": symbol, "status": "BLOCKED"}

    # 1b. CLUSTER 3 ITEM 3(b) — QUERY-BEFORE-PLACE (retry / restart-resume
    # exactly-once). If this position already carries a persisted exit
    # client_order_id (a PRIOR attempt minted + placed it), ask the broker
    # orderbook whether OUR tag is already there and ADOPT that order instead of
    # placing a duplicate — this closes the cross-process window the in-process
    # single-flight lock can't (a restart mid-flight). On the FIRST exit (no
    # persisted id) mint one, persist it, and thread it onto the placement so the
    # tag is STABLE for any future retry / restart. Paper's get_orders() is None →
    # no adoption, place exactly as today (byte-for-byte unchanged).
    _direction_pre = position.get("direction") or "long"
    _exit_qty_pre = int(position.get("qty") or 0)
    exit_coid = position.get("exit_client_order_id")
    if exit_coid:
        try:
            from autotrade.monitoring.exit_poller import (
                adopt_tagged_exit_if_present as _adopt_tagged)
            _adopted = await _adopt_tagged(
                session_id=session_id, symbol=symbol,
                exit_client_order_id=exit_coid, qty=_exit_qty_pre,
                broker=broker, registry=registry, close_reason=reason,
                broker_profile=prof_id, direction=_direction_pre)
        except Exception as e:  # pragma: no cover - never block the exit
            log.warning("query-before-place adopt raised %s/%s: %s",
                        session_id, symbol, e)
            _adopted = None
        if _adopted is not None:
            log.warning("per-stock stop %s/%s: ADOPTED existing tagged exit "
                        "(status=%s) — placed NO new order", session_id, symbol,
                        _adopted.get("status"))
            return {"symbol": symbol, "status": _adopted.get("status", "ADOPTED"),
                    "reason": reason, "adopted": True,
                    "exit_price": _adopted.get("exit_price")}
    else:
        exit_coid = order_ledger.make_client_order_id(session_id, symbol, attempt=1)
        registry.set_exit_client_order_id(symbol, exit_coid,
                                          broker_profile=prof_id)

    # 2. Cancel the broker GTT for this position (best-effort, never block exit).
    # asyncio.wait_for caps the wait at 5s — kiteconnect's default requests.Session
    # has no timeout, so delete_gtt can hang indefinitely on a stale connection,
    # preventing place_market_exit from ever running.  The underlying thread
    # continues to completion (cannot be interrupted), but we stop waiting for it.
    gtt_id = position.get("gtt_id")
    gtt_cancel_timed_out = False
    if gtt_manager and gtt_id:
        try:
            await asyncio.wait_for(
                asyncio.to_thread(broker.cancel_gtt, gtt_id),
                timeout=_GTT_CANCEL_TIMEOUT_SEC,
            )
            log.info("per-stock stop %s/%s: GTT %s cancelled", session_id, symbol, gtt_id)
        except asyncio.TimeoutError:
            # R3 — the cancel didn't confirm in 5s: the OCO may STILL be live and
            # could FIRE concurrently with our market exit → a double-sell window.
            # We flag it so the exit qty is re-probed + clamped below.
            gtt_cancel_timed_out = True
            log.warning("per-stock stop %s/%s: GTT %s cancel timed out — will "
                        "clamp exit qty to live-held", session_id, symbol, gtt_id)
        except Exception as e:
            log.warning("per-stock stop %s/%s: GTT cancel failed (%s): %s",
                        session_id, symbol, gtt_id, e)

    # 3. Place the market exit in the CLOSING side.
    qty = int(position.get("qty") or 0)
    itype = position.get("instrument_type") or "EQ"
    # FUTURES long/short: long→SELL (unchanged), short→BUY-to-cover. Threaded
    # from the position's stored direction so a per-stock stop / retry can never
    # place a wrong-side order that would DOUBLE a short instead of covering it.
    direction = position.get("direction") or "long"

    # PRE-EXIT RECONCILIATION GUARD (real-money safety — 2026-07-02 incident).
    # If the operator (or a fired broker SL/GTT) already closed this position at
    # the broker, our DB still shows it OPEN and a blind market exit would place a
    # NAKED order (a fresh short on a flat book, or a cover that opens the other
    # side). Ask the broker for its live net qty FIRST: when it reports the
    # position is already flat, mark our row CLOSED (reconcile) and place NOTHING.
    # This is the fix for the EXIT_FAILED retry loop that kept re-attempting an
    # already-closed FUT. Returns None in paper / when the broker can't answer →
    # we then proceed with the normal exit (paper is byte-for-byte unchanged).
    probe_raised = False
    try:
        net_qty = broker.get_net_position_qty(position.get("symbol"), itype)
    except Exception as _net_e:
        # FAIL-SAFE (Fix B1, 2026-07-10 BRIGADE double-cover). The pre-exit position
        # read RAISED (broker connection/timeout — ConnectionResetError 10054 mid
        # buy-to-cover). We CANNOT confirm the live position, so placing a market
        # exit now would be BLIND: a short's buy-to-cover would DOUBLE into a naked
        # long. NEVER place an exit without a successful position read — abort THIS
        # attempt, leave the leg OPEN, release the gate so the next tick retries once
        # the broker is reachable. (Paper / not-live returns None WITHOUT raising →
        # probe_raised stays False → the normal exit path runs, byte-for-byte
        # unchanged. A confirmed clean 0 still reconciles-flat below.)
        log.error("pre-exit net-position probe RAISED %s/%s: %s — ABORTING exit "
                  "(no blind order; leg left OPEN for retry)",
                  session_id, symbol, _net_e)
        net_qty = None
        probe_raised = True
    if probe_raised:
        _exit_gate_mod.release_exit_session(session_id, symbol,
                                            broker_profile=prof_id)
        return {"symbol": symbol, "status": "EXIT_ABORTED_PROBE_FAILED",
                "reason": reason}
    # SESSION-SCOPED flat decision (C1): the broker net is ACCOUNT-wide, so when a
    # sibling session holds the same symbol we must NOT read its shares as ours.
    # our_held = the qty attributable to THIS session still at the broker; 0 ⟺ our
    # shares are gone (reconcile + place nothing), never the whole-account net.
    # CLUSTER 9 ITEM 3: sibling subtraction scoped to the SAME broker_account_id/
    # profile (+ product) so a DIFFERENT account's same-symbol lot is never
    # subtracted (never misread as flat).
    from autotrade.monitoring.registry import our_held_at_broker as _our_held
    our_held = _our_held(session_id, position.get("symbol"), itype, qty, net_qty,
                         broker_profile=prof_id,
                         broker_account_id=position.get("broker_account_id"),
                         product=position.get("product"))
    if net_qty is not None and our_held == 0:
        log.warning(
            "pre-exit reconcile %s/%s: broker net qty is 0 (already closed "
            "externally) — marking CLOSED, placing NO order", session_id, symbol)
        # POSITIVE-EVIDENCE RECONCILE (2026-07-07 CEMPRO circuit incident): the leg
        # was closed by ANOTHER order (a fired GTT / manual / RMS). Resolve at the
        # REAL fill from the broker orderbook via the framework's order-id-driven
        # primitive — NEVER fabricate a 0/mark price (a 0 exit books a phantom
        # ~-100% realised loss). Confirmed fill → CLOSE at it; else a POSITIVE mark
        # → RECONCILED_FLAT at the mark (pre-existing behaviour, never 0); else
        # (no evidence AND no positive mark) → EXIT_FAILED for the reconciler.
        from autotrade.monitoring.position_reconciler import _confirmed_close
        try:
            ev = await asyncio.to_thread(_confirmed_close, position, broker)
        except Exception as _ce:  # pragma: no cover - defensive
            log.debug("pre-exit reconcile _confirmed_close raised %s/%s: %s",
                      session_id, symbol, _ce)
            ev = None
        ltp_val = position.get("ltp")
        if ev is not None:
            registry.mark_closed(symbol, f"{reason}_RECONCILED_FLAT",
                                 exit_price=ev.get("exit_price"),
                                 broker_profile=prof_id,
                                 exit_order_id=ev.get("exit_order_id"))
            _exit_gate_mod.release_exit_session(session_id, symbol,
                                                broker_profile=prof_id)
            return {"symbol": symbol, "status": "RECONCILED_FLAT", "reason": reason,
                    "exit_price": ev.get("exit_price")}
        if ltp_val and float(ltp_val) > 0:
            registry.mark_closed(symbol, f"{reason}_RECONCILED_FLAT",
                                 exit_price=ltp_val, broker_profile=prof_id)
            _exit_gate_mod.release_exit_session(session_id, symbol,
                                                broker_profile=prof_id)
            return {"symbol": symbol, "status": "RECONCILED_FLAT", "reason": reason}
        # Flat at broker, NO attributable fill AND no positive mark → do NOT book a
        # phantom CLOSED@0. mark_exit_failed releases the gate itself.
        registry.mark_exit_failed(
            symbol, f"{reason}: broker flat, no attributable exit fill",
            broker_profile=prof_id)
        return {"symbol": symbol, "status": "EXIT_FAILED",
                "confirm_status": "RECONCILE_UNATTRIBUTED", "reason": reason}
    # STANDING INVARIANT (Fix 3, 2026-07-11): NEVER place an exit larger than the
    # qty THIS session still holds at the broker — regardless of WHY it shrank. The
    # clamp is UNCONDITIONAL (was gated on gtt_cancel_timed_out): a partial RMS/GTT
    # fill can shrink our_held WITHOUT any GTT-cancel timeout, and the old conjunct
    # would then skip the clamp → a full-qty sell → OVERSELL of (qty-our_held) into
    # a reverse/naked position. our_held is None in paper (get_net_position_qty
    # None) → no clamp, byte-for-byte unchanged. our_held >= qty → no-op.
    # (gtt_cancel_timed_out is retained only for the log context below.)
    if our_held is not None and 0 < our_held < qty:
        log.warning("per-stock stop %s/%s: clamping exit qty %d→%d (session-scoped "
                    "live-held; broker shrank under us%s)", session_id, symbol, qty,
                    int(our_held),
                    "; GTT cancel timed out" if gtt_cancel_timed_out else "")
        qty = int(our_held)
    # WORKING-EXIT NETTING (Fix 2 Part 2, 2026-07-11): a resting exit we ALREADY
    # placed does NOT reduce the broker net, so the clamp above (net-based) can't
    # see it — a re-fire would place a SECOND exit alongside the resting one. Net
    # out our own still-working exit-side qty for this symbol so a resting exit
    # BLOCKS a second placement. Live only (our_held is not None); paper (None) and
    # the no-resting-order case are byte-for-byte unchanged (working==0). Best-
    # effort: a probe error → 0 (the resting-order CANCEL above is the primary
    # guard).
    if our_held is not None:
        # CLUSTER 9 ITEM 4: count ONLY our own resting exit orders (recorded id /
        # our compact tag for this session+profile leg) — a foreign/manual resting
        # order must never block our exit.
        _owned_ids, _owned_tags = _falcon_owned_exit_ids_tags(
            session_id, position.get("symbol"), broker_profile=prof_id)
        # ── CLUSTER 9c FIX F5 — FUNGIBLE-ACCOUNT EXIT CONFLICT PAGE ─────────────
        # A FOREIGN (manual / non-Falcon) same-EXIT-side order resting for this
        # symbol in this account means the operator may ALSO be exiting the same
        # name on a shared login. We do NOT block the exit (needing to exit is more
        # urgent than the conflict, and the C1 our_held clamp already caps Falcon to
        # ITS qty) — but we page URGENT so the operator sees the fungible risk. THE
        # REAL FIX is a DEDICATED AutoTrade account. Best-effort; never blocks.
        _exit_txn = "BUY" if str(direction).lower() == "short" else "SELL"
        _foreign_exit = await _foreign_same_side_pending(
            broker, position.get("symbol"), _exit_txn,
            owned_ids=_owned_ids, owned_tags=_owned_tags)
        if _foreign_exit:
            _foids = ", ".join(f.get("order_id") or "?" for f in _foreign_exit[:5])
            _detail = (f"MANUAL_CONFLICT_ON_EXIT: a foreign {_exit_txn} order is "
                       f"resting for {symbol} in this account (order(s) {_foids}) "
                       f"while AutoTrade is exiting the same leg — proceeding with "
                       f"our clamped exit, but a shared broker login risks a double "
                       f"fill. A dedicated AutoTrade account is the real fix.")
            log.error("exit %s/%s: %s", session_id, symbol, _detail)
            try:
                alerts.send_urgent_deduped(
                    kind="MANUAL_CONFLICT_ON_EXIT", session_id=session_id,
                    symbol=symbol, detail=_detail)
            except Exception:  # noqa: BLE001 — paging must never block the exit
                pass
        working = await _our_working_exit_qty(broker, position.get("symbol"),
                                              direction,
                                              owned_ids=_owned_ids,
                                              owned_tags=_owned_tags)
        if working > 0:
            placeable = qty - working
            if placeable <= 0:
                log.warning("per-stock stop %s/%s: a working exit for %d already "
                            "rests (>= intended %d) — placing NO second order, "
                            "leaving for retry", session_id, symbol, working, qty)
                _exit_gate_mod.release_exit_session(session_id, symbol,
                                                    broker_profile=prof_id)
                return {"symbol": symbol, "status": "EXIT_PENDING_WORKING",
                        "reason": reason, "working_qty": working}
            log.warning("per-stock stop %s/%s: netting a working exit of %d — "
                        "placing only %d (was %d)", session_id, symbol, working,
                        placeable, qty)
            qty = placeable
    # kite_product overrides instrument_type mapping: positions table stores security
    # type ("EQ"), not the trading product ("MTF"/"CNC"). Without this, MTF exits
    # become CNC sells and Kite rejects them with "Holding quantity: 0".
    effective_product = kite_product or position.get("order_product")
    # ── WORKED EXIT (participation / TWAP, execution_mode=="worked") ──────────
    # A large exit for a worked-mode session is PACED over time (POV + TWAP floor
    # + freeze cap) instead of one shot / one immediate iceberg burst. `qty` here
    # is ALREADY the C1-clamped our_held qty, so the paced children can never
    # oversell. Runs under the SAME single exit-flight + exit_gate claim. Default-
    # off: any other execution_mode falls through to the unchanged iceberg / one-
    # shot path below (byte-for-byte). Paper fills each child immediately.
    if getattr(exec_cfg, "execution_mode", "market") == "worked":
        from autotrade.monitoring.exit_poller import work_and_confirm_exit
        _vf = None
        if not (getattr(broker, "dry_run", False)):
            from autotrade.execution.worked_order import recent_interval_volume
            _vf = recent_interval_volume
        log.warning("exit %s/%s: WORKED (paced) exit of %d (reason=%s)",
                    session_id, symbol, int(qty), reason)
        return await work_and_confirm_exit(
            session_id=session_id, symbol=symbol, total_qty=int(qty),
            broker=broker, registry=registry, close_reason=reason,
            exec_cfg=exec_cfg, broker_profile=prof_id, direction=direction,
            instrument_type=itype, kite_product=effective_product,
            parent_exit_coid=exit_coid, deadline_ts=None, volume_fn=_vf)
    # ── EXIT ICEBERG (SPRINT CLUSTER 8, additive, DEFAULT-OFF) ────────────────
    # A large exit (qty > the effective slice/freeze cap) is sliced into child
    # exits placed SEQUENTIALLY, each confirmed, aggregated; the position CLOSES
    # only at full cover — a child reject/partial leaves the REMAINDER EXIT_FAILED
    # (guarded retry). `qty` here is ALREADY the C1-clamped live-held qty (clamp +
    # working-exit netting above), so we slice the CLAMPED qty and can never
    # oversell (clamp-BEFORE-slice). INERT when iceberg is off or qty <= slice (a
    # single order, byte-for-byte the path below). The whole sliced exit runs under
    # this position's single exit-flight (begin_exit_flight in _exit_single_position)
    # + the exit_gate claim = ONE flight.
    if getattr(exec_cfg, "iceberg_enabled", False):
        from autotrade import iceberg as _iceberg_mod
        _ice_price = position.get("ltp") or position.get("avg_price")
        _ice_legs = _iceberg_mod.plan_for(exec_cfg, symbol, int(qty),
                                          _ice_price or None)
        if len(_ice_legs) > 1:
            from autotrade.monitoring.exit_poller import slice_and_confirm_exit
            log.warning("per-stock stop %s/%s: ICEBERG exit %d → %d slices %s",
                        session_id, symbol, int(qty), len(_ice_legs), _ice_legs)
            return await slice_and_confirm_exit(
                session_id=session_id, symbol=symbol, total_qty=int(qty),
                legs=_ice_legs, broker=broker, registry=registry,
                close_reason=reason, broker_profile=prof_id,
                direction=direction, instrument_type=itype,
                kite_product=effective_product, exec_cfg=exec_cfg,
                parent_exit_coid=exit_coid)
    try:
        res = await broker.place_market_exit(symbol, qty, itype,
                                             kite_product=effective_product,
                                             direction=direction,
                                             exec_cfg=exec_cfg,
                                             client_order_id=exit_coid)
    except Exception as e:
        log.error("per-stock stop %s/%s: place_market_exit raised: %s",
                  session_id, symbol, e)
        registry.mark_exit_failed(symbol, str(e), broker_profile=prof_id)
        return {"symbol": symbol, "status": "EXIT_FAILED", "error": str(e)}

    # 4. Handle the placement result — then confirm the fill.
    if res is None or getattr(res, "status", None) == "FAILED":
        err = getattr(res, "error", None) or "exit failed"
        registry.mark_exit_failed(symbol, err, broker_profile=prof_id)
        log.error("per-stock stop EXIT_FAILED %s/%s: %s", session_id, symbol, err)
        return {"symbol": symbol, "status": "EXIT_FAILED", "error": err}

    order_id = getattr(res, "broker_order_id", None)
    is_dry = (order_id is None or
              str(order_id).upper() in ("DRY_RUN", "NONE", ""))

    # Import here to avoid top-level circular dependency.
    from autotrade.monitoring.exit_poller import confirm_exit as _confirm_exit

    confirm_result = await _confirm_exit(
        session_id=session_id,
        symbol=symbol,
        order_id=order_id,
        qty=qty,
        broker=broker,
        registry=registry,
        close_reason=reason,
        max_wait_sec=60,
        poll_interval_sec=5.0,
        broker_profile=prof_id,
        client_order_id=exit_coid,
    )
    confirm_status = confirm_result.get("status", "UNKNOWN")
    exit_price = confirm_result.get("exit_price") or position.get("ltp")

    if confirm_status in ("COMPLETE", "DRY_RUN"):
        log.warning("per-stock stop FIRED %s/%s (reason=%s) exit_price=%s",
                    session_id, symbol, reason, exit_price)
        return {"symbol": symbol, "status": "EXITED", "reason": reason,
                "exit_price": exit_price,
                "broker_order_id": order_id}
    elif confirm_status in ("PARTIAL", "TIMEOUT"):
        # Fix 2 (2026-07-11 audit): the exit order is STILL RESTING at the broker
        # (unconfirmed). A resting SELL/BUY-cover does NOT reduce positions()['net'],
        # so a naive next-tick re-fire would read the FULL net → place a SECOND exit
        # → both fill on a tick-back → OVERSELL / naked reverse. So:
        #   (1) CANCEL the resting order (confirm the cancel) so it cannot fill
        #       alongside the retry, then
        #   (2) mark the row EXIT_FAILED (records the exit order-id + releases the
        #       gate) so the GUARDED EXIT_FAILED retry sweep re-attempts it — the
        #       guard re-probes the broker net AND nets out any still-working exit
        #       (see the pre-exit block above) — never a naive re-fire.
        cancelled_ok = False
        if order_id and not is_dry:
            try:
                cancelled_ok = bool(await asyncio.to_thread(
                    broker.cancel_order_sync, order_id))
            except Exception as _ce:
                log.warning("per-stock stop %s/%s: cancel of resting order %s "
                            "failed: %s", session_id, symbol, order_id, _ce)
        registry.mark_exit_failed(
            symbol,
            f"{confirm_status} (resting order {order_id} "
            f"cancel={'ok' if cancelled_ok else 'unconfirmed'})",
            broker_profile=prof_id, exit_order_id=order_id)
        log.error("per-stock stop EXIT_FAILED %s/%s (confirm_status=%s; resting "
                  "order cancelled=%s) — routed to guarded retry",
                  session_id, symbol, confirm_status, cancelled_ok)
        return {"symbol": symbol, "status": "EXIT_FAILED",
                "confirm_status": confirm_status,
                "resting_order_cancelled": cancelled_ok}
    else:
        # REJECTED / CANCELLED — mark_exit_failed already called by confirm_exit
        # (which released the gate). Needs a fresh retry next tick.
        log.error("per-stock stop EXIT_FAILED %s/%s (confirm_status=%s)",
                  session_id, symbol, confirm_status)
        return {"symbol": symbol, "status": "EXIT_FAILED",
                "confirm_status": confirm_status}


# ── LIVE CONFIG EDIT — hot-reload whitelist ──────────────────────────────────
# The ONLY config fields a RUNNING session may hot-reload post-launch. RISK/EXIT
# knobs only. Capital / product / picks / entry params stay LOCKED once fired:
# editing them would desync the frozen invested_basis + the placed orders / GTTs.
# maybe_reload_config() re-parses config_json and copies ONLY these onto the live
# self.config — it NEVER touches invested_basis, trail_armed/trail_peak, open
# positions, order-ids, broker_profiles, capital, product, or entry timing.
LIVE_EDITABLE_SESSION_FIELDS = (
    "arm_pct",
    "floor_pct",
    "trail_giveback_pct",
    "stop_pct",
    # PROFIT STEP-LOCK — tunable on a RUNNING session (the ladder is a list;
    # validated the same way as config.validate). maybe_reload_config copies
    # these onto the live config within one tick without touching positions.
    "trail_step_lock_enabled",
    "trail_step_lock_ladder",
    "trail_large_peak_pct",
    "trail_large_giveback_rel",
    # STEP-LOCK SCOPE — flip a RUNNING session basket<->stock (normally set at
    # create). maybe_reload_config copies it onto the live config within one tick;
    # positions / basis / per-stock ratchet state are untouched.
    "step_lock_scope",
    # PER-STOCK CAPITAL STOP — the % of a stock's OWN deployed capital at which it
    # is cut (step_lock_scope=="stock"). Hot-editable on a running session.
    "per_stock_stop_pct",
    "per_position_stop_pct",
    "per_position_target_pct",
    "square_off_time",
    "mis_square_off_time",
    "max_hold_sessions",
)


class TradingSession:
    def __init__(self, session_id: str, config: TradingSessionConfig,
                 mode: str = "paper", user_id: Optional[str] = None,
                 broker_account_id: Optional[str] = None):
        self.session_id = session_id
        self.config = config
        # LIVE CONFIG EDIT: the config_version this in-memory session last loaded.
        # maybe_reload_config() compares the persisted config_version to this and
        # hot-reloads the whitelist when the row is newer. Set from the row in
        # load(); a freshly create()d session starts at 0 (matches the DB default).
        self._loaded_config_version = 0
        self.mode = mode  # 'paper' | 'live'
        self.dry_run = (mode != "live")
        # PHASE-2 MULTI-TENANT (additive). Both default None → operator/global
        # creds path (today's behaviour, byte-for-byte). user_id scopes ownership
        # + vault lookups; broker_account_id binds the session's default broker
        # leg to a specific vaulted account.
        self.user_id = user_id
        self.broker_account_id = broker_account_id
        self.registry = PositionRegistry(session_id, config.total_allocated_capital)
        self.monitor = PortfolioMonitor(session_id, config.total_allocated_capital)
        self.brokers: Dict[str, Any] = {}
        self.kill_switch: Optional[KillSwitchExecutor] = None
        self.gtt_manager: Optional[GTTManager] = None

    # ── Persistence / factory ─────────────────────────────────────────────────
    @classmethod
    def create(cls, config: TradingSessionConfig, mode: str = "paper",
               user_id: Optional[str] = None,
               broker_account_id: Optional[str] = None) -> "TradingSession":
        """Create a session.

        PHASE-2 MULTI-TENANT (additive, backward-compatible): user_id +
        broker_account_id are OPTIONAL. When both are None the session is the
        operator/global session exactly as before (the INSERT writes NULLs, which
        the columns default to). When broker_account_id is set, it is validated
        to exist (and be owned by user_id, if given) so a session can't be bound
        to a non-existent / someone else's account; the per-leg cred resolution
        happens at _build_brokers time from the vault."""
        config.validate()
        # If a broker account is bound, verify it exists + ownership BEFORE
        # creating the session (fail fast, no orphan binding). NULL → skip.
        if broker_account_id is not None:
            from . import vault
            if not vault.account_exists(broker_account_id, user_id=user_id):
                raise ValueError(
                    f"broker_account_id {broker_account_id} not found"
                    + (f" for user {user_id}" if user_id else ""))
        session_id = uuid.uuid4().hex
        with falcon_conn() as con:
            con.execute(
                """INSERT INTO autotrade_sessions
                   (session_id, created_at, status, mode,
                    total_allocated_capital, config_json, user_id,
                    broker_account_id)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (session_id, _now_ist_iso(), "CREATED", mode,
                 config.total_allocated_capital, config.to_json(), user_id,
                 broker_account_id),
            )
            con.commit()
        log.info("AutoTrade session %s created (mode=%s, user=%s, account=%s)",
                 session_id, mode, user_id, broker_account_id)
        return cls(session_id, config, mode=mode, user_id=user_id,
                   broker_account_id=broker_account_id)

    @classmethod
    def load(cls, session_id: str) -> Optional["TradingSession"]:
        with falcon_conn() as con:
            row = con.execute(
                "SELECT mode, config_json, user_id, broker_account_id, "
                "config_version "
                "FROM autotrade_sessions WHERE session_id=?",
                (session_id,),
            ).fetchone()
        if not row:
            return None
        cfg = TradingSessionConfig.from_json(row["config_json"])
        # user_id / broker_account_id are NULL for pre-Phase-2 sessions → the
        # operator/global path, unchanged.
        d = dict(row)
        obj = cls(session_id, cfg, mode=row["mode"],
                  user_id=d.get("user_id"),
                  broker_account_id=d.get("broker_account_id"))
        # LIVE CONFIG EDIT: remember the config_version this session was loaded at
        # so maybe_reload_config() can detect a newer persisted edit.
        obj._loaded_config_version = int(d.get("config_version") or 0)
        return obj

    # ── LIVE CONFIG EDIT — hot-reload of the whitelisted risk/exit knobs ───────
    def maybe_reload_config(self) -> bool:
        """Pick up an operator's live risk/exit edit WITHOUT a restart.

        Reads the row's config_version (one tiny SELECT). If it is GREATER than
        the version this in-memory session last loaded, re-parse config_json and
        copy ONLY the LIVE_EDITABLE_SESSION_FIELDS (risk/exit knobs) onto
        self.config; everything else on the live config is left EXACTLY as first
        loaded (capital, order_product, broker_profiles, top_n/picks, entry timing,
        strategy, kill-switch pct, direction, …).

        HARD GUARANTEES (real money): this method NEVER touches invested_basis,
        trail_armed / trail_peak (they are separate autotrade_sessions columns,
        not part of self.config), any open position, or any order-id. It only
        mutates in-memory self.config attributes in the whitelist. When the
        version is unchanged it is an O(1) no-op (no re-parse). Returns True iff a
        reload happened.
        """
        with falcon_conn() as con:
            row = con.execute(
                "SELECT config_version, config_json "
                "FROM autotrade_sessions WHERE session_id=?",
                (self.session_id,),
            ).fetchone()
        if row is None:
            return False
        d = dict(row)
        ver = int(d.get("config_version") or 0)
        if ver <= int(getattr(self, "_loaded_config_version", 0)):
            return False  # unchanged → do nothing (no re-parse)
        try:
            new_cfg = TradingSessionConfig.from_json(d["config_json"])
        except Exception as e:  # pragma: no cover - never break the tick
            log.warning("config hot-reload parse failed for %s: %s",
                        self.session_id, e)
            return False
        changed: List[str] = []
        for f in LIVE_EDITABLE_SESSION_FIELDS:
            old = getattr(self.config, f, None)
            new = getattr(new_cfg, f, None)
            if old != new:
                setattr(self.config, f, new)
                changed.append(f"{f}: {old!r}->{new!r}")
        self._loaded_config_version = ver
        log.info("config hot-reloaded v%d for %s: %s", ver, self.session_id,
                 ", ".join(changed) if changed else "(no whitelisted change)")
        return True

    @classmethod
    def list_sessions(cls, limit: int = 50,
                      user_id: Optional[str] = None,
                      owner_user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Recent sessions (newest first) for the session list.

        This is what lets the panel SHOW existing sessions instead of resetting
        to a blank create form — a created session stays visible/resumable.

        PHASE-2 MULTI-TENANT scoping (two params, both strict — NO 'OR IS NULL'):
          * owner_user_id (per-user portal isolation): return ONLY sessions
            WHERE s.user_id = ? EXACTLY. NULL-owned (operator/legacy) sessions
            are NOT returned. Use this for a non-admin portal user so they see
            ONLY their own sessions.
          * user_id (legacy operator-console scope): same strict WHERE s.user_id
            = ? filter, kept UNCHANGED for the operator console's existing
            ?user_id call. When None → the full operator view (all sessions,
            today's behaviour).

        NOTE: neither path uses an 'OR user_id IS NULL' branch, so no NULL-owned
        session ever leaks into a scoped result.
        """
        base = (
            """SELECT s.session_id, s.created_at, s.started_at, s.closed_at,
                      s.status, s.mode, s.total_allocated_capital,
                      s.last_gross_return,
                      s.last_gross_return AS gross_return,
                      s.user_id, s.broker_account_id,
                      json_extract(s.config_json, '$.strategy') AS strategy,
                      (SELECT COUNT(*) FROM autotrade_positions p
                       WHERE p.session_id = s.session_id
                         AND p.status = 'OPEN') AS n_open_positions
               FROM autotrade_sessions s
            """)
        scope_id = owner_user_id if owner_user_id is not None else user_id
        with falcon_conn() as con:
            if scope_id is not None:
                rows = con.execute(
                    base + " WHERE s.user_id = ? ORDER BY s.created_at DESC "
                    "LIMIT ?", (str(scope_id), int(limit)),
                ).fetchall()
            else:
                rows = con.execute(
                    base + " ORDER BY s.created_at DESC LIMIT ?",
                    (int(limit),),
                ).fetchall()
        return [dict(r) for r in rows]

    # ── Broker construction ───────────────────────────────────────────────────
    def _build_brokers(self) -> None:
        profiles = self.config.broker_profiles
        if not profiles:
            # Default single zerodha profile spanning the whole capital. PHASE-2:
            # inherit the session's broker_account_id so a single-leg session
            # bound to a vaulted account trades that account (None → global).
            from .config import BrokerProfile
            profiles = [BrokerProfile(
                profile_id="zerodha_default", broker_name="zerodha",
                allocated_capital=self.config.total_allocated_capital,
                order_product=self.config.order_product,
                instrument_type=self.config.instrument_type,
                broker_account_id=self.broker_account_id)]
            self.config.broker_profiles = profiles
        for prof in profiles:
            if not prof.enabled:
                continue
            # PHASE-2 MULTI-TENANT: resolve per-account creds from the vault into
            # this profile (in memory only). No-op when the profile has no bound
            # account / the vault is disabled → the adapter uses the global path.
            self._resolve_account_creds(prof)
            # FIX A (real-money isolation): stamp the OWNING user_id onto the
            # profile so the broker adapter can enforce "a user-owned LIVE session
            # never falls back to the operator's global Kite client". Note this is
            # threaded AFTER _resolve_account_creds, which may have CLEARED a bad
            # binding (vault disabled / account absent / not-owned / decrypt fail)
            # → owner_user_id set + broker_account_id None makes the adapter REFUSE
            # a live build instead of silently going global. None (operator/global
            # session) leaves today's global-fallback behaviour unchanged.
            prof.owner_user_id = self.user_id
            # ADMIN/operator owners may use the global operator account; non-admin
            # owners are held to their OWN account (isolation guard in _build_kite).
            prof.owner_is_admin = _owner_is_admin(self.user_id)
            self.brokers[prof.profile_id] = build_client(prof, dry_run=self.dry_run)
        self.gtt_manager = GTTManager(
            self.session_id, self.config, self.brokers, self.registry)
        self.kill_switch = KillSwitchExecutor(
            self.session_id, self.config, self.brokers, self.registry,
            gtt_manager=self.gtt_manager)

    def _resolve_account_creds(self, prof) -> None:
        """PHASE-2 MULTI-TENANT cred resolution (in memory only).

        If the profile binds a vaulted account (prof.broker_account_id, or the
        session's broker_account_id falling through onto the default leg), AND the
        vault is enabled, decrypt that account's api_key + access_token into the
        profile so the adapter builds its OWN client for that account. Defense in
        depth: the account must be owned by this session's user_id (when set).

        SAFETY HINGE: this is a NO-OP (leaves prof secrets empty + clears any
        bound id so the adapter takes the global path) when:
          * the profile has no bound account, OR
          * the vault is disabled (no FALCON_VAULT_KEY), OR
          * the account is absent / not owned / decryption fails.
        i.e. a NULL account or a disabled vault behaves EXACTLY as today."""
        acct_id = getattr(prof, "broker_account_id", None)
        if acct_id is None:
            return  # no bound account → global path (unchanged)
        # CLUSTER 5 ITEM 5 fix — STASH the ORIGINAL bound account id (runtime-only,
        # never persisted) BEFORE any clearing below. The profile-level account
        # gate in _fire_entries runs AFTER prewarm_execution has already called
        # _build_brokers under execution_mode=="marketable_limit" (now the default),
        # which clears broker_account_id on an unresolvable binding — that would
        # SILENTLY DEFEAT the revoked/expired-profile gate. The gate consults this
        # stash so a non-tradeable profile is still detected + degraded.
        setattr(prof, "_bound_account_id_original", acct_id)
        # CLUSTER 9d FIX F6 — a profile that SPECIFIED a broker_account_id must NOT
        # silently trade the operator's GLOBAL account when that account cannot be
        # resolved. When we clear the binding below, stamp a runtime-only marker so
        # the broker adapter's _build_kite FAILS CLOSED on a LIVE build — UNLESS the
        # operator set the explicit break_glass_global override (in which case the
        # marker is cleared and the historic silent global fallback is allowed).
        _break_glass = bool(getattr(self.config, "break_glass_global", False))
        from . import vault
        if not vault.vault_enabled():
            # Bound account but vault disabled: we CANNOT trade the right
            # account. Clear the binding so the adapter doesn't error trying to
            # build a per-account client. FAIL CLOSED on a live build (F6) unless
            # break_glass_global is set — never silently trade the global account
            # in place of a SPECIFIED one. (Live trades still need
            # FALCON_AUTOTRADE_ENABLED; this affects WHICH account.)
            log.warning("session %s: profile %s bound to account %s but vault "
                        "is DISABLED — %s", self.session_id, prof.profile_id,
                        acct_id, ("break_glass_global set → global fallback"
                                  if _break_glass else
                                  "refusing a live build (fail-closed F6)"))
            prof.broker_account_id = None
            setattr(prof, "_account_specified_unresolvable", not _break_glass)
            return
        creds = vault.get_decrypted_creds(acct_id, user_id=self.user_id)
        if creds is None:
            log.warning("session %s: could not resolve creds for account %s "
                        "(absent / not owned / decrypt failed) — %s",
                        self.session_id, acct_id,
                        ("break_glass_global set → global fallback" if _break_glass
                         else "refusing a live build (fail-closed F6)"))
            prof.broker_account_id = None
            setattr(prof, "_account_specified_unresolvable", not _break_glass)
            return
        # Resolved cleanly — ensure no stale fail-closed marker survives a retry.
        setattr(prof, "_account_specified_unresolvable", False)
        # Populate in-memory creds (NEVER persisted). The adapter's _build_kite
        # uses these to build a dedicated proxy-aware client for this account.
        prof.api_key = creds.api_key or ""
        prof.api_secret = creds.api_secret or ""
        prof.access_token = creds.access_token or ""
        prof.broker_account_id = acct_id
        # Bind the LIVE adapter to the ACCOUNT'S broker so it matches what the
        # preview sized (a Rupeezy account must build a RupeezyBroker, not the
        # hardcoded default zerodha leg). Keep the default when the vault omits it.
        if getattr(creds, "broker", None):
            prof.broker_name = creds.broker

    def _record_account_allocations(self) -> None:
        """CLUSTER 9d FIX F2 — persist a MULTI-ACCOUNT session's PER-ACCOUNT reserved
        capital so risk_manager.committed_capital budgets each broker account
        correctly. Sum each enabled profile's allocated_capital by its INTENDED
        broker account (the live binding, or the original binding stashed before a
        fail-closed/unresolvable clear). Writes ONLY when the session spans >1
        distinct account — a single-account session writes NOTHING and
        committed_capital falls back to the session-level total_allocated_capital
        (byte-identical). Best-effort — never blocks a fire."""
        try:
            profiles = [p for p in (self.config.broker_profiles or [])
                        if getattr(p, "enabled", True)]
            if not profiles:
                return
            acct_alloc: Dict[Any, float] = {}
            for p in profiles:
                acct = (getattr(p, "broker_account_id", None)
                        or getattr(p, "_bound_account_id_original", None))
                acct = None if acct in (None, "") else str(acct)
                acct_alloc[acct] = acct_alloc.get(acct, 0.0) + float(
                    getattr(p, "allocated_capital", 0.0) or 0.0)
            # Only a genuinely multi-account session needs the per-account split;
            # a single-account session stays on the session-level ledger path.
            if len(acct_alloc) <= 1:
                return
            from . import risk_manager
            risk_manager.record_session_account_allocations(
                self.session_id, acct_alloc)
        except Exception as e:  # pragma: no cover - never block a fire
            log.warning("session %s: account-allocation ledger write skipped (%s)",
                        self.session_id, e)

    # ── Start: now | scheduled ──────────────────────────────────────────────────
    async def start(self, when: str = "now") -> Dict[str, Any]:
        """Start the session — GATED by the trading-day / market-open rule.

        when="now": fire entries IMMEDIATELY, but ONLY after the fire gate passes
            (a real NSE trading day AND the market open at this moment). If the
            gate refuses (weekend/holiday/closed market) NO order is placed and
            the session moves to a clear terminal/deferred state.

        when="scheduled": resolve the fire datetime from entry_date@entry_time
            (or the next valid trading session if entry_date is unset).
            * Future trading day → status SCHEDULED + a scheduler thread that
              sleeps until the target, then fires THROUGH THE GATE.
            * Non-trading-day target → REJECTED_NON_TRADING_DAY or carried per
              on_missed_window. Never fires into a closed market.
        """
        if when == "scheduled":
            return await self._start_scheduled()
        return await self._start_now()

    async def _start_now(self) -> Dict[str, Any]:
        """when='now' — fire immediately IF the gate allows. The 'fire into a
        closed market' hole is closed here: an instant start on a Sunday / after
        hours is REFUSED with a clear status, never placed."""
        now = now_ist()
        # For an instant start the intended fire moment is NOW (subject to the
        # gate's trading-day + open-window checks).
        gate = evaluate_fire_gate(self.config, now, fire_dt=now)
        if not gate.allow:
            return self._refuse_fire(gate, when="now")
        # PHASE-2 WARM: subscribe the basket to FULL + prime circuits so the
        # imminent fire prices network-free (no-op for market mode / paper).
        try:
            prewarm_execution(self)
        except Exception:  # pragma: no cover - never block the fire
            pass
        return await self._fire_entries()

    async def _start_scheduled(self) -> Dict[str, Any]:
        now = now_ist()
        try:
            target = self.config.resolve_fire_datetime(now)
        except ValueError as e:
            # Unparseable entry_time → safe refusal (never fire blind).
            log.warning("scheduled start for %s: %s — refusing",
                        self.session_id, e)
            gate = FireGate(False, status=STATUS_EXPIRED_MISSED_WINDOW,
                            reason=f"entry_time unparseable: {e}")
            return self._refuse_fire(gate, when="scheduled")

        gate = evaluate_fire_gate(self.config, now, fire_dt=target)

        # Gate says fire NOW (we're inside the open window + grace of the target).
        if gate.allow:
            res = await self._fire_entries()
            res["note"] = ("target within grace + market open — fired now")
            res["when"] = "scheduled"
            res["entry_time"] = self.config.entry_time
            res["fires_at"] = target.isoformat()
            return res

        # Carry policy (missed/non-trading-day → roll entry_date forward) — must
        # be handled BEFORE the plain WAIT branch, since a carry also reports
        # status SCHEDULED but additionally needs entry_date rolled.
        if gate.carry_to:
            return self._refuse_fire(gate, when="scheduled")

        # Gate says WAIT (future trading day, or deferred before the bell): arm
        # the scheduler for the resolved target. Place NOTHING yet.
        if gate.status in ("SCHEDULED", STATUS_DEFERRED_MARKET_CLOSED):
            self._set_status("SCHEDULED", reason=gate.reason)
            armed = entry_scheduler.start_for_session(
                self.session_id, gate.fire_dt, now_fn=now_ist)
            # PHASE-2 WARM: subscribe the basket to FULL at ARM time so bid/ask
            # stream over the WS well before the fire. The entry_scheduler ALSO
            # re-prewarms within ~60s of the target (circuits are day-cached; a
            # same-day prime here is valid). No-op for market mode / paper.
            try:
                prewarm_execution(self)
            except Exception:  # pragma: no cover - never block scheduling
                pass
            seconds = int(max(0.0, (gate.fire_dt - now).total_seconds()))
            log.info("session %s SCHEDULED — entry at %s (in %ss, armed=%s, %s)",
                     self.session_id, gate.fire_dt.isoformat(), seconds, armed,
                     gate.reason)
            return {"session_id": self.session_id, "status": "SCHEDULED",
                    "mode": self.mode, "when": "scheduled",
                    "entry_time": self.config.entry_time,
                    "entry_date": self.config.entry_date,
                    "fires_at": gate.fire_dt.isoformat(),
                    "trading_day": True,
                    "seconds_remaining": seconds,
                    "scheduler_armed": armed, "n_placed": 0, "orders": [],
                    "note": gate.reason}

        # Gate refuses (non-trading-day with expire policy, or carry).
        return self._refuse_fire(gate, when="scheduled")

    def _refuse_fire(self, gate: "FireGate", when: str) -> Dict[str, Any]:
        """Apply a NON-firing gate decision: set the terminal/deferred status,
        carry entry_date forward + re-arm the scheduler if the policy says so, and
        return a clear payload. Places NOTHING."""
        if gate.carry_to:
            # carry_next_trading_day: roll entry_date forward + stay SCHEDULED.
            self._persist_entry_date(gate.carry_to)
            self.config.entry_date = gate.carry_to
            now = now_ist()
            try:
                new_target = self.config.resolve_fire_datetime(now)
            except ValueError:
                new_target = None
            self._set_status("SCHEDULED", reason=gate.reason)
            armed = False
            seconds = None
            if new_target is not None:
                armed = entry_scheduler.start_for_session(
                    self.session_id, new_target, now_fn=now_ist)
                seconds = int(max(0.0, (new_target - now).total_seconds()))
            log.info("session %s CARRIED to %s (%s, armed=%s)",
                     self.session_id, gate.carry_to, gate.reason, armed)
            out = {"session_id": self.session_id, "status": "SCHEDULED",
                   "mode": self.mode, "when": when, "n_placed": 0, "orders": [],
                   "entry_date": gate.carry_to,
                   "trading_day": True,
                   "scheduler_armed": armed,
                   "deferred_reason": gate.reason, "note": gate.reason}
            if new_target is not None:
                out["fires_at"] = new_target.isoformat()
            if seconds is not None:
                out["seconds_remaining"] = seconds
            return out
        # expire / reject: terminal, place nothing.
        self._set_status(gate.status, reason=gate.reason,
                         closed_at=_now_ist_iso())
        log.warning("session %s NOT fired (%s): %s", self.session_id,
                    gate.status, gate.reason)
        return {"session_id": self.session_id, "status": gate.status,
                "mode": self.mode, "when": when, "n_placed": 0, "orders": [],
                "trading_day": gate.status != STATUS_REJECTED_NON_TRADING_DAY,
                "expired_reason": gate.reason, "note": gate.reason}

    # ── Fire entries: route → size → place → register (THE order-firing leg) ────
    async def _fire_entries(self, *, gate_checked: bool = False) -> Dict[str, Any]:
        # DEFENCE-IN-DEPTH trading-day/market-open gate. Every caller of
        # _fire_entries (instant start, the entry_scheduler thread at wake,
        # recovery's past-due fire) passes THROUGH here, so even if an upstream
        # check is bypassed we NEVER place an order on a non-trading day or into a
        # closed market. `gate_checked=True` from callers that already evaluated
        # the gate this same instant (start_now / scheduled-fire-now) skips the
        # redundant re-check but the result is identical.
        if not gate_checked:
            now = now_ist()
            gate = evaluate_fire_gate(self.config, now, fire_dt=now)
            if not gate.allow:
                return self._refuse_fire(gate, when="fire")
        # BROKER-AGNOSTIC AUTH FOUNDATION (Stage 1): if this session is bound to a
        # vaulted broker account AND the vault is enabled, the bound account MUST
        # be ACTIVE before any order fires — a session never trades against a
        # PENDING/EXPIRED/REVOKED/ERROR account. The NULL-account / disabled-vault
        # (operator/global) path is UNCHANGED: no bound id → no check; a disabled
        # vault clears the binding in _resolve_account_creds → global fallback.
        if self.broker_account_id is not None:
            from . import vault as _vault
            if _vault.vault_enabled():
                from .broker.account_lifecycle import (assert_account_tradeable,
                                                       AccountNotTradeable)
                try:
                    assert_account_tradeable(self.user_id, self.broker_account_id)
                except AccountNotTradeable as e:
                    log.warning("session %s: bound account not tradeable — %s",
                                self.session_id, e)
                    self._set_status("FAILED", reason=str(e))
                    return {"session_id": self.session_id, "status": "FAILED",
                            "mode": self.mode, "when": "fire", "n_placed": 0,
                            "orders": [], "error": str(e),
                            "note": "bound broker account is not ACTIVE"}
        # CLUSTER 3 ITEM 5 — PROFILE-LEVEL account health gate. The session-level
        # check above validates the session binding; a MULTI-PROFILE session can
        # still pair an ACTIVE account with a REVOKED / EXPIRED one, and the revoked
        # leg would otherwise fire. Validate EVERY enabled profile's BOUND account
        # here, BEFORE building brokers or placing: a non-tradeable profile is
        # DEGRADED (disabled in memory) so it is NOT routed/fired while the healthy
        # legs still fire; a clear per-profile reason surfaces. If NO enabled
        # profile survives, refuse the whole fire. Unbound profiles (global/operator
        # path) + a disabled vault are UNAFFECTED (byte-for-byte unchanged).
        degraded_profiles: List[Dict[str, Any]] = []
        try:
            from . import vault as _vault_prof
            _vault_on = _vault_prof.vault_enabled()
        except Exception:  # pragma: no cover - defensive; vault import failure
            _vault_on = False
        if _vault_on and self.config.broker_profiles:
            from .broker.account_lifecycle import (assert_account_tradeable,
                                                   AccountNotTradeable)
            for prof in self.config.broker_profiles:
                if not getattr(prof, "enabled", True):
                    continue
                # Prefer the live binding; fall back to the ORIGINAL binding stashed
                # before prewarm/_build_brokers may have cleared it (ITEM 5 fix) so
                # a revoked/expired profile is still gated even under marketable_limit.
                acct_id = (getattr(prof, "broker_account_id", None)
                           or getattr(prof, "_bound_account_id_original", None))
                if acct_id is None:
                    continue  # unbound → global/operator path (validated upstream)
                try:
                    assert_account_tradeable(self.user_id, acct_id)
                except AccountNotTradeable as e:
                    log.warning("session %s: profile %s account %s NOT tradeable — "
                                "NOT firing this leg (%s)", self.session_id,
                                prof.profile_id, acct_id, e)
                    prof.enabled = False
                    degraded_profiles.append({
                        "profile_id": prof.profile_id,
                        "broker_account_id": acct_id, "reason": str(e)})
            if degraded_profiles and not any(
                    getattr(p, "enabled", True)
                    for p in self.config.broker_profiles):
                reason = "; ".join(
                    f"{d['profile_id']}: {d['reason']}" for d in degraded_profiles)
                log.warning("session %s: ALL enabled profiles non-tradeable — "
                            "refusing fire (%s)", self.session_id, reason)
                self._set_status("FAILED", reason=reason)
                return {"session_id": self.session_id, "status": "FAILED",
                        "mode": self.mode, "when": "fire", "n_placed": 0,
                        "orders": [], "error": reason,
                        "degraded_profiles": degraded_profiles,
                        "note": "no enabled broker profile has a tradeable account"}
        self._build_brokers()
        # C5 — ENTRY IDEMPOTENCY. Two racing start()/scheduled-fire invocations (or
        # a double call) must place orders ONCE. (1) A DB check: a session that
        # already has OPEN positions has already fired → refuse. (2) An in-memory
        # per-session claim closes the concurrent-invocation window (both callers
        # can pass the DB check before either places; only one wins the claim).
        # Claimed HERE (after the gate + account checks pass) so a refused fire on
        # a non-trading day does NOT burn the claim and block a legitimate carry.
        _already_open = self.registry.get_open_positions()
        if _already_open:
            log.warning("session %s: already has %d OPEN positions — refusing "
                        "entry re-fire (idempotency)", self.session_id,
                        len(_already_open))
            return {"session_id": self.session_id,
                    "status": self._current_status(), "mode": self.mode,
                    "when": "fire", "n_placed": 0, "orders": [],
                    "already_fired": True,
                    "note": "session already has open positions"}
        if not fire_guard.claim_entry(self.session_id):
            log.warning("session %s: entry already claimed — refusing double-fire",
                        self.session_id)
            return {"session_id": self.session_id,
                    "status": self._current_status(), "mode": self.mode,
                    "when": "fire", "n_placed": 0, "orders": [],
                    "already_fired": True,
                    "note": "entry already claimed (idempotency guard)"}
        self._set_status("RUNNING", started_at=_now_ist_iso())
        self._record_account_allocations()

        # TESLA SHORT ROTATION: the entry model is a live-signal seat fill, not a
        # one-shot Falcon basket. Reuses the gate/account/idempotency preamble
        # above, then fills seats via _place_one + arms the drivers. Existing
        # strategies fall through to the unchanged Falcon-pick basket path below.
        if self.config.strategy == "tesla_short":
            return await self._fire_tesla_initial()

        falcon_picks, router_cap = _resolve_falcon_selection(
            self.config, log_ctx=f"session {self.session_id}: ")
        router = BrokerRouter(top_n_stocks=router_cap)
        routed = router.route_picks(falcon_picks, self.config.broker_profiles)

        # SPEED PASS: time the whole fire (start → all legs settled) for the
        # entry_latency_ms observability field.
        _fire_t0 = time.monotonic()

        # SPEED PASS: place all legs CONCURRENTLY. Each profile's picks are sized
        # off ONE batched LTP + ONE batched MTF-margin prefetch (zero per-symbol
        # round-trips in the common case), then all _place_one coroutines are
        # fanned out with asyncio.gather. A bounded semaphore respects the Kite
        # order rate limit (~10/s). Per-leg failure is ISOLATED inside _place_one
        # (it returns a FAILED/SKIPPED dict, never raises) so one bad leg can
        # never abort the others. invested_basis is frozen AFTER all legs settle.
        sem = asyncio.Semaphore(_ENTRY_CONCURRENCY)

        async def _guarded_place(broker, prof, pick, amount, allocator, cache,
                                 forced_qty=None, quote=None):
            async with sem:
                try:
                    return await self._place_one(
                        broker, prof, pick, amount, allocator, prefetch=cache,
                        forced_qty=forced_qty, quote=quote)
                except Exception as e:  # belt-and-braces leg isolation
                    log.error("entry leg crashed for %s: %s", pick.symbol, e)
                    return {"symbol": pick.symbol, "status": "FAILED",
                            "error": str(e)}

        # RMS CAP 1: collect the fully-sized leg specs + accumulate the total
        # PLANNED DEPLOYED capital (Σ plan["deployed"] = the real cash/margin the
        # orders will consume) across ALL profiles BEFORE placing anything, so the
        # pre-trade margin gate can refuse an over-deploy and place NOTHING.
        leg_specs: List[tuple] = []
        total_planned_deployed = 0.0
        # CLUSTER 9c FIX F4 (2026-07-11): accumulate planned-deployed + the broker
        # clients PER broker_account_id, so a session whose profiles resolve to
        # DIFFERENT accounts is budgeted per-account (not as one pooled account).
        # Keyed by the profile's broker_account_id (None = operator/global account,
        # grouped together). Single-account sessions produce ONE group → the
        # original single gate call (byte-identical).
        account_groups: Dict[Any, Dict[str, Any]] = {}
        skipped_picks: List[Dict[str, Any]] = []
        # RMS CAP 4: surface a silent MIS/MTF margin-API fallback as an amber
        # warning (never silently shrink deployment). A leg cash-falls-back when a
        # margin-sized product (MTF/MIS) has NO margin cache entry AND its slice
        # was NOT sized off margin — detected here by comparing the plan's unit
        # economics to the margin cache.
        margin_fallback_warnings: List[Dict[str, Any]] = []
        for prof in self.config.broker_profiles:
            if not prof.enabled:
                continue
            broker = self.brokers[prof.profile_id]
            picks = routed.get(prof.profile_id, [])
            # FUTURES symbol eligibility: a futures session may only trade Falcon
            # picks that HAVE a tradeable current-month future. Names without one
            # are logged + skipped (never fabricated). Equity/MTF unaffected.
            if prof.instrument_type == "FUT":
                picks = _filter_fno_eligible(
                    picks, broker, self.config.expiry_preference, self.session_id)
            allocator = CapitalAllocator(self.config)
            amounts = allocator.allocate([p.symbol for p in picks])
            fund_picks = [p for p in picks if amounts.get(p.symbol, 0.0) > 0]
            # ONE batched prefetch per profile (LTP + MTF/MIS margin for all picks).
            try:
                cache = allocator.prefetch([p.symbol for p in fund_picks], broker)
            except Exception as e:  # pragma: no cover - per-symbol fallback inside
                log.warning("prefetch failed for %s (%s) — per-symbol fallback",
                            prof.profile_id, e)
                cache = {}
            # QUOTE-DRIVEN MARKETABLE-LIMIT (execution_mode=="marketable_limit"
            # only): ONE batched broker.get_quotes() for the WHOLE basket, exactly
            # like the LTP/margin prefetch above — NOT a per-leg network call, so
            # the concurrent asyncio.gather over _place_one below is unchanged. The
            # per-symbol book (bid/ask/circuit) is threaded into _place_one via the
            # quote_cache; the market path never fetches this (byte-for-byte
            # unchanged). None (paper / disabled / error) → every leg SKIPs on a
            # missing quote inside _place_one; conservative by design.
            quote_cache: Dict[str, Any] = {}
            if getattr(self.config, "execution_mode", "market") == "marketable_limit":
                try:
                    q = broker.get_quotes([p.symbol for p in fund_picks])
                    quote_cache = q or {}
                except Exception as e:  # pragma: no cover - defensive
                    log.warning("get_quotes failed for %s (%s) — marketable-limit "
                                "legs will skip", prof.profile_id, e)
                    quote_cache = {}
            # FEATURE C: whole-portfolio plan — floors each slice, SKIPS a pick
            # whose 1 unit > slice (logged + slice freed), and (default on)
            # redistributes the stranded remainder to affordable picks. Never
            # over-deploys. redistribute_unused_capital=False → plain floor.
            plan = allocator.plan_quantities(
                [p.symbol for p in fund_picks], broker, cache=cache)
            plan_qtys = plan["quantities"]
            _leg_deployed = float(plan.get("deployed") or 0.0)
            total_planned_deployed += _leg_deployed
            # FIX F4: fold this profile's deployed + its broker client into its
            # broker_account_id group (for the per-account pre-trade gate below).
            _grp = account_groups.setdefault(
                prof.broker_account_id,
                {"deployed": 0.0, "brokers": {},
                 "broker_account_id": prof.broker_account_id})
            _grp["deployed"] += _leg_deployed
            _grp["brokers"][prof.profile_id] = broker
            # RMS CAP 4: amber margin-fallback warning. When the product is
            # margin-sized (MTF/MIS) but a leg was priced off LTP (no margin in the
            # cache), the deployment CASH-fell-back → the operator sized fewer
            # shares than the leverage would allow. Surface it (never silent).
            _mprod = _margin_product(self.config)
            if _mprod and prof.instrument_type in ("EQ", "MTF"):
                for pick in fund_picks:
                    if plan_qtys.get(pick.symbol, 0) <= 0:
                        continue
                    if not (cache.get(pick.symbol, {}) or {}).get("margin"):
                        margin_fallback_warnings.append({
                            "symbol": pick.symbol,
                            "broker_profile": prof.profile_id,
                            "product": _mprod,
                            "reason": (f"{_mprod} margin unavailable — cash-sized "
                                       f"(fewer shares than leverage allows)")})
            for sk in plan["skipped"]:
                skipped_picks.append({**sk, "broker_profile": prof.profile_id})
            for pick in fund_picks:
                qty = plan_qtys.get(pick.symbol)
                if not qty or qty <= 0:
                    continue  # skipped/unaffordable — logged in plan["skipped"]
                amount = amounts.get(pick.symbol, 0.0)
                leg_specs.append((broker, prof, pick, amount, allocator, cache,
                                  qty, quote_cache.get(pick.symbol)))

        # RMS CAP 1 — PRE-TRADE MARGIN GATE + PER-USER CAPITAL LEDGER. Refuse a
        # session whose planned deployed capital exceeds the account's FREE margin
        # minus the user's already-committed capital (their other live sessions).
        # INERT when no broker reports available margin (paper / stub → None), so
        # paper is byte-for-byte unchanged. On refusal: place NOTHING, mark FAILED.
        if leg_specs:
            # CLUSTER 9b ITEM 8 — the gate FAILS CLOSED on an unknown/errored margin
            # only when a broker will actually place REAL orders (its own
            # _live_allowed() = dry_run off AND the master env switch on), and scopes
            # the budget + committed-capital ledger to THIS session's broker account.
            # Deriving "live" from the BROKER (not just mode/env) keeps paper + every
            # paper-style mock broker (no _live_allowed / returns False) byte-for-byte
            # inert — only a genuinely live-order-capable adapter engages fail-closed.
            # An explicit operator override (config.rms_allow_unknown_margin, default
            # off) lets a live deploy proceed on an unknown budget.
            _rms_live = False
            if not self.dry_run:
                for _b in (self.brokers or {}).values():
                    try:
                        _la = getattr(_b, "_live_allowed", None)
                        if callable(_la) and _la():
                            _rms_live = True
                            break
                    except Exception:  # pragma: no cover - never block on the probe
                        continue
            _rms_allow_unknown = bool(getattr(
                self.config, "rms_allow_unknown_margin", False))
            # FIX F4 (2026-07-11): run ONE pre-trade decision PER account group. A
            # single-account session collapses to ONE group whose (deployed,brokers,
            # broker_account_id) equal the whole-session values → the original single
            # call (byte-identical). A multi-account session gates each account's Σ
            # against THAT account's budget; the WHOLE fire is refused if ANY group
            # fails (with that account's reason).
            # CLUSTER 9d FIX F1 (2026-07-11): ALWAYS build the groups from the
            # per-profile account_groups (each group carries its OWN, correct
            # broker_account_id) — even when there is exactly ONE group. The old
            # `len<=1` branch reused self.broker_account_id, which mis-budgets a
            # single explicit BrokerProfile(broker_account_id=acctA) on a session
            # whose session-level broker_account_id is None against the GLOBAL
            # (None) budget instead of acctA. A genuine single-account session
            # (account == self.broker_account_id) is byte-identical. The
            # session-level fallback is used ONLY when there are no groups at all
            # (no enabled profiles produced a group — cannot happen inside this
            # `if leg_specs:` block, but kept as a defensive default).
            if account_groups:
                _rms_groups = list(account_groups.values())
            else:
                _rms_groups = [{"deployed": total_planned_deployed,
                                "brokers": self.brokers,
                                "broker_account_id": self.broker_account_id}]
            _rms_refusal = None
            for _g in _rms_groups:
                try:
                    _rms = risk_manager.pre_trade_gate(
                        user_id=self.user_id, session_id=self.session_id,
                        planned_deployed=_g["deployed"], brokers=_g["brokers"],
                        live=_rms_live, broker_account_id=_g["broker_account_id"],
                        allow_unknown_margin=_rms_allow_unknown)
                except Exception as _rms_e:
                    # FIX F3 (2026-07-11): a pre_trade_gate EXCEPTION must FAIL CLOSED
                    # in LIVE — a gate bug is not permission to deploy real money.
                    # Paper/non-live (or the explicit rms_allow_unknown_margin
                    # override) stays INERT: log + proceed (byte-identical for paper).
                    if _rms_live and not _rms_allow_unknown:
                        reason = (f"RMS_GATE_ERROR: pre-trade risk gate raised "
                                  f"({_rms_e}) on a LIVE deploy of "
                                  f"₹{float(_g['deployed']):,.0f} for account "
                                  f"{_g['broker_account_id']} — refusing "
                                  f"(fail-closed).")
                        log.error("session %s: %s", self.session_id, reason)
                        _rms_refusal = risk_manager.RiskDecision(
                            allow=False, reason=reason, available_margin=None,
                            committed_other=0.0,
                            planned_deployed=float(_g["deployed"]), free=None)
                        break
                    log.warning("session %s: pre_trade_gate raised (%s) — proceeding "
                                "(degraded, paper/override)", self.session_id, _rms_e)
                    continue
                if _rms is not None and not _rms.allow:
                    _rms_refusal = _rms
                    break
            if _rms_refusal is not None:
                self._set_status("FAILED", reason=_rms_refusal.reason,
                                 closed_at=_now_ist_iso())
                log.error("session %s REFUSED by pre-trade RMS: %s",
                          self.session_id, _rms_refusal.reason)
                return {"session_id": self.session_id, "status": "FAILED",
                        "mode": self.mode, "when": "fire", "n_placed": 0,
                        "orders": [], "skipped_picks": skipped_picks,
                        "risk_refused": True, "reason": _rms_refusal.reason,
                        "available_margin": _rms_refusal.available_margin,
                        "committed_other": _rms_refusal.committed_other,
                        "planned_deployed": _rms_refusal.planned_deployed}

        # ── WORKED-ORDER (participation / TWAP) entry (execution_mode=="worked").
        # Default-off: "market" / "marketable_limit" fall through to the unchanged
        # one-shot gather below (byte-for-byte). Worked mode spawns one background
        # build task per leg (paced over the window) and returns RUNNING now — the
        # RMS pre-trade gate + skipped-picks above already ran on the TARGET size.
        if getattr(self.config, "execution_mode", "market") == "worked":
            return await self._fire_entries_worked(
                leg_specs, skipped_picks, _fire_t0, margin_fallback_warnings,
                degraded_profiles)

        leg_coros = [
            _guarded_place(broker, prof, pick, amount, allocator, cache,
                           forced_qty=qty, quote=quote)
            for (broker, prof, pick, amount, allocator, cache, qty, quote)
            in leg_specs]
        placed: List[Dict[str, Any]] = list(
            await asyncio.gather(*leg_coros)) if leg_coros else []
        entry_latency_ms = int((time.monotonic() - _fire_t0) * 1000)
        try:
            self._record_latency(entry_latency_ms=entry_latency_ms)
        except Exception as e:  # pragma: no cover - never block on observability
            log.debug("entry_latency record failed for %s: %s", self.session_id, e)
        log.info("session %s entry fired %d legs in %dms (concurrency=%d)",
                 self.session_id, len(placed), entry_latency_ms, _ENTRY_CONCURRENCY)

        # ENTRY-OUTCOME GATE (real-money safety). If NO leg produced a real
        # filled position (all rejected/failed), the session has NOTHING to
        # manage — mark it FAILED, not RUNNING, and place NO GTT / start NO
        # drivers / arm NO square-off. Prevents a phantom-basket session that
        # sits RUNNING and tries to square off non-existent positions.
        # (2026-07-01 incident: all 5 NRML-on-equity legs rejected.)
        # DRY_RUN counts as a (paper) fill — a paper session places DRY_RUN legs
        # and must go RUNNING to monitor its paper positions, NOT be failed.
        # (2026-07-02: the gate was marking paper sessions FAILED because DRY_RUN
        # was excluded here.)
        n_filled = sum(1 for p in placed
                       if p.get("status") in ("PLACED", "PARTIAL", "COMPLETE",
                                               "DRY_RUN"))
        # A session with ZERO filled legs has NOTHING to manage → mark FAILED,
        # not RUNNING. This covers BOTH cases: (a) legs were attempted but all
        # rejected/failed at the broker, and (b) NO leg was even attempted because
        # every pick was skipped upstream (unaffordable / not F&O-eligible), which
        # leaves `placed` EMPTY — the earlier `if placed and …` guard missed this
        # and left the session RUNNING with 0 positions (2026-07-02 paper-FUT bug:
        # a ₹1L futures session skipped all picks — futures margins exceeded each
        # slice — yet showed RUNNING). No GTT / drivers / square-off armed here.
        if n_filled == 0:
            if skipped_picks:
                syms = ", ".join(str(s.get("symbol")) for s in skipped_picks[:8])
                reason = (f"no positions placed — all {len(skipped_picks)} pick(s) "
                          f"were skipped (unaffordable for the per-slice budget or "
                          f"not F&O-eligible): {syms}. Increase capital or narrow "
                          f"the basket.")
            elif placed:
                reason = (f"all {len(placed)} entry legs rejected/failed at the "
                          "broker — no positions placed")
            else:
                reason = ("no positions placed — no tradeable picks for this "
                          "session (empty universe/whitelist or none eligible)")
            self._set_status("FAILED", reason=reason, closed_at=_now_ist_iso())
            log.error("session %s marked FAILED: %s", self.session_id, reason)
            return {"session_id": self.session_id, "status": "FAILED",
                    "mode": self.mode, "n_placed": 0, "orders": placed,
                    "skipped_picks": skipped_picks, "reason": reason}
        if placed and n_filled < len(placed):
            log.warning("session %s: %d/%d entry legs failed — continuing with "
                        "the %d filled legs", self.session_id,
                        len(placed) - n_filled, len(placed), n_filled)

        # INVESTED-CAPITAL-BASIS: freeze Σ(qty*avg_price) across the positions
        # just placed. This is the product-aware capital actually put to work
        # (MTF leveraged value / CNC cash) and becomes the kill-switch + gross
        # return denominator. Captured ONCE here, never recomputed as positions
        # close. Falls back to total_allocated_capital if nothing was placed.
        try:
            invested_basis = self.monitor.freeze_invested_basis()
            log.info("session %s invested_basis frozen at ₹%.2f (fund ₹%.2f)",
                     self.session_id, invested_basis,
                     self.config.total_allocated_capital)
        except Exception as e:  # never block start on the basis capture
            log.warning("invested_basis freeze failed for %s: %s",
                        self.session_id, e)

        # PER-STOCK STEP-LOCK Fix A (2026-07-10): freeze the ENTRY basket notional
        # (Σ qty*avg over all filled legs) as the FIXED denominator for the per-stock
        # capital slice, so a survivor's slice (and thus its -per_stock_stop_pct rupee
        # stop) stays CONSTANT as siblings close — instead of inflating toward the
        # whole session capital. Only read in step_lock_scope=="stock"; best-effort.
        try:
            ebn = self.monitor.freeze_entry_basket_notional()
            log.info("session %s entry_basket_notional frozen at ₹%.2f",
                     self.session_id, ebn)
        except Exception as e:  # never block start on the basis capture
            log.warning("entry_basket_notional freeze failed for %s: %s",
                        self.session_id, e)

        # FEATURE 1: place the per-position GTT-OCO broker backup on every open
        # session position that lacks one. LIVE places real GTTs; paper records
        # the intended levels only. Best-effort — never blocks the start.
        gtt_results: List[Dict[str, Any]] = []
        try:
            if self.config.per_position_gtt_enabled and self.gtt_manager:
                gtt_results = self.gtt_manager.backfill_missing()
        except Exception as e:  # never block start on the backup floor
            log.warning("GTT backfill failed for %s: %s", self.session_id, e)

        # Launch the per-session background tick driver: refreshes ltp +
        # gross_return and AUTO-fires the kill switch on breach. Idempotent;
        # self-stops when the session leaves RUNNING. Paper = no real orders.
        try:
            tick_driver.start_for_session(self.session_id)
        except Exception as e:  # never block start on the driver
            log.warning("tick driver start failed for %s: %s", self.session_id, e)

        # FEATURE 2: arm the sub-second WebSocket-driven kill-switch path (in
        # ADDITION to the 5s poll). Subscribes the session's symbols and fires
        # the flatten on a live tick crossing ±kill_switch_pct, coordinated with
        # the poll via the per-session fire guard. Idempotent; paper = no real
        # orders on fire. Never blocks the start.
        try:
            ws_driver.start_for_session(self.session_id)
        except Exception as e:  # never block start on the WS driver
            log.warning("ws driver start failed for %s: %s", self.session_id, e)

        # SQUARE-OFF ARMING (intraday_basket 15:29 AND/OR the MIS defensive time).
        # FEATURE A: a MIS session on EITHER strategy is squared off BY US before
        # the broker's ~15:20 compulsory auto-square. intraday_basket also arms at
        # its own square_off_time. When both apply we arm ONE scheduler at the
        # EARLIEST of the two (don't double-arm; the MIS time is validated < the
        # basket time). The tick-driver backstops both if this timer is dropped.
        # Best-effort, never blocks the start.
        try:
            self._arm_square_off()
        except Exception as e:  # never block start on the square-off scheduler
            log.warning("square-off arm failed for %s: %s", self.session_id, e)

        _ok = {"session_id": self.session_id, "status": "RUNNING",
               "mode": self.mode, "n_placed": len(placed), "orders": placed,
               "gtt": gtt_results, "skipped_picks": skipped_picks}
        # RMS CAP 4: surface any amber MIS/MTF margin-fallback (cash-sized) so the
        # operator sees the deployment shrank vs the leverage — never silent.
        if margin_fallback_warnings:
            _ok["margin_fallback_warnings"] = margin_fallback_warnings
        # CLUSTER 3 ITEM 5 — surface any DEGRADED (non-tradeable) profiles whose
        # leg(s) were intentionally NOT fired, so the operator sees WHY a broker's
        # legs are missing (a revoked/expired account needs reconnect).
        if degraded_profiles:
            _ok["degraded_profiles"] = degraded_profiles
        return _ok

    def _arm_square_off(self) -> bool:
        """Arm the per-session square-off scheduler at the EARLIEST applicable
        square-off time TODAY (IST):
          * intraday_basket → config.square_off_time (the 15:29 basket flatten).
          * MIS product (either strategy) → config.mis_square_off_time (FEATURE A
            defensive flatten, before the broker's ~15:20 window).
        When both apply, the earlier time wins (single scheduler, no double-arm).
        No-op if no time applies, the time is unparseable, or it is already past
        (the tick driver squares off defensively in that case). Returns True if a
        scheduler was armed."""
        candidates: List[datetime] = []
        # POSITIONAL (square_off_enabled False): do NOT arm the basket square-off
        # — the trail carries across days. The MIS defensive candidate below is
        # unaffected (positional can't be MIS per validate(), so no collision).
        if (self.config.strategy == "intraday_basket"
                and getattr(self.config, "square_off_enabled", True)):
            try:
                candidates.append(
                    _parse_entry_time_today_ist(self.config.square_off_time))
            except ValueError:
                log.warning("session %s: unparseable square_off_time %r",
                            self.session_id, self.config.square_off_time)
        if self.config.is_intraday_product():
            try:
                candidates.append(_parse_entry_time_today_ist(
                    self.config.mis_square_off_time))
            except ValueError:
                log.warning("session %s: unparseable mis_square_off_time %r",
                            self.session_id, self.config.mis_square_off_time)
        if not candidates:
            return False  # not intraday_basket and not MIS → no square-off (unchanged)
        target = min(candidates)
        if datetime.now(IST) >= target:
            log.info("session %s: square-off time %s already passed — in-tick "
                     "square-off will fire", self.session_id, target.isoformat())
            return False
        log.info("session %s: arming square-off at %s (MIS=%s, strategy=%s)",
                 self.session_id, target.isoformat(),
                 self.config.is_intraday_product(), self.config.strategy)
        return square_off_scheduler.start_for_session(self.session_id, target)

    async def _place_one(self, broker, prof, pick: Pick, amount: float,
                         allocator: CapitalAllocator,
                         prefetch: Optional[Dict[str, Any]] = None,
                         forced_qty: Optional[int] = None,
                         quote: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        symbol = pick.symbol
        # FEATURE C: when the whole-portfolio planner already computed the qty
        # (with redistribution / skip), use it verbatim so the placed size matches
        # the plan. Otherwise size this leg on its own (unchanged path).
        if forced_qty is not None:
            if forced_qty <= 0:
                return {"symbol": symbol, "status": "SKIPPED",
                        "reason": "planned qty 0"}
            qty = int(forced_qty)
        else:
            try:
                qty = allocator.calculate_quantity_cached(
                    symbol, amount, broker, cache=prefetch)
            except InsufficientCapitalError as e:
                log.warning("skip %s: %s", symbol, e)
                return {"symbol": symbol, "status": "SKIPPED", "reason": str(e)}

        # Reuse the prefetched LTP (the entry mark) when present; else one lookup.
        ref_price = 0.0
        if prefetch and symbol in prefetch and prefetch[symbol].get("ltp"):
            ref_price = float(prefetch[symbol]["ltp"])
        else:
            ref_price = broker.get_ltp(symbol) or 0.0
        # ── CONCENTRATION / FAT-FINGER CAP (SPRINT CLUSTER 8 ITEM 3) ────────────
        # A per-order safety cap on how much a SINGLE name may take (max_pct_per_name
        # × capital, an absolute ₹ notional cap, and a qty sanity cap). A breaching
        # leg is CLAMPED (default) or REFUSED with a clear reason — e.g. a whitelist
        # collapsing to ONE name would otherwise pour the whole book into it. ALL
        # caps DEFAULT None → INERT (byte-for-byte unchanged) until an operator opts
        # in. Applied AFTER sizing, BEFORE the iceberg slice / placement.
        from .risk_manager import check_concentration as _check_conc
        _conc = _check_conc(self.config, symbol, int(qty), ref_price or None)
        if _conc.refused:
            log.warning("session %s: %s", self.session_id, _conc.reason)
            return {"symbol": symbol, "status": "SKIPPED",
                    "reason": _conc.reason, "concentration_refused": True,
                    "broker_profile": prof.profile_id}
        if _conc.clamped:
            log.warning("session %s: %s", self.session_id, _conc.reason)
            qty = int(_conc.qty)
        # ── ICEBERG ENTRY (SPRINT CLUSTER 7, CAP 2/3, additive, DEFAULT-OFF) ────
        # When iceberg is enabled AND the sized qty exceeds the effective slice cap
        # (min of the configured slice + the per-symbol exchange FREEZE quantity),
        # split this ENTRY into child legs each <= the slice, placed SEQUENTIALLY,
        # each confirmed, then register ONE aggregate position at the qty-weighted-
        # average fill. A child reject / under-fill STOPS further children and
        # registers the FILLED-so-far (never the intended qty — the phantom class).
        # When disabled / qty <= slice this is INERT (a single leg) and the
        # existing single-order path below runs BYTE-FOR-BYTE unchanged.
        # ── CLUSTER 9c FIX F5 — FUNGIBLE-ACCOUNT ENTRY CONFLICT GUARD ───────────
        # Before opening a NEW position, refuse if a FOREIGN (manual / non-Falcon)
        # same-side order is already RESTING for this symbol in this account: on a
        # shared broker login the manual order and our entry could BOTH fill, so we
        # do NOT compound the exposure — REFUSE this leg + page (MANUAL_CONFLICT).
        # THE REAL FIX is a DEDICATED AutoTrade account. Live only (paper's
        # get_pending_orders → [] → no conflict → byte-identical). A fresh entry has
        # no position row yet, so owned sets are empty → any same-side resting order
        # is foreign by definition. Best-effort: a probe error → no conflict.
        if not self.dry_run:
            _entry_txn = "SELL" if getattr(self.config, "direction", "long") == \
                "short" else "BUY"
            _own_ids, _own_tags = _falcon_owned_exit_ids_tags(
                self.session_id, symbol, broker_profile=prof.profile_id)
            _foreign = await _foreign_same_side_pending(
                broker, symbol, _entry_txn, owned_ids=_own_ids, owned_tags=_own_tags)
            if _foreign:
                _oids = ", ".join(f.get("order_id") or "?" for f in _foreign[:5])
                reason = (f"MANUAL_CONFLICT: a foreign {_entry_txn} order is already "
                          f"resting for {symbol} in this account (order(s) {_oids}) "
                          f"— refusing the AutoTrade entry to avoid a double fill on "
                          f"a shared broker login. A dedicated AutoTrade account is "
                          f"the real fix.")
                log.error("session %s: %s", self.session_id, reason)
                try:
                    alerts.send_urgent_deduped(
                        kind="MANUAL_CONFLICT", session_id=self.session_id,
                        symbol=symbol, detail=reason)
                except Exception:  # noqa: BLE001 — paging must never block the fire
                    pass
                return {"symbol": symbol, "status": "SKIPPED", "reason": reason,
                        "manual_conflict": True, "broker_profile": prof.profile_id}
        from . import iceberg as _iceberg
        _ice_legs = _iceberg.plan_for(
            self.config, symbol, int(qty), ref_price or None)
        if getattr(self.config, "iceberg_enabled", False) and len(_ice_legs) > 1:
            return await self._place_iceberg(
                broker, prof, symbol, int(qty), _ice_legs, ref_price, quote)
        order = build_order(symbol, qty, self.config, broker)
        # CAP 1 — mint a durable client_order_id BEFORE broker submission and set
        # its compact tag on the order so OUR order is recognisable at the broker.
        # The FULL id is persisted on the position row + the ledger (maps to the
        # broker order-id). Paper: the tag is set but place_order returns DRY_RUN
        # before to_kite_params, so no real tag is sent (paper byte-identical).
        client_order_id = order_ledger.make_client_order_id(self.session_id, symbol)
        order.client_order_id = client_order_id
        order.tag = order_ledger.compact_tag(client_order_id)
        _entry_side = "SELL" if getattr(self.config, "direction", "long") == "short" \
            else "BUY"
        # CAP 3 — durable ORDER_CREATED intent, written BEFORE the broker call, so a
        # crash between broker-accept and the position-row insert leaves an orphan
        # the recovery/reconcile path can find (best-effort; never blocks the order).
        order_ledger.record_intent(
            session_id=self.session_id, symbol=symbol,
            client_order_id=client_order_id, qty=qty, side=_entry_side,
            product=prof.order_product, broker_profile=prof.profile_id,
            instrument_type=prof.instrument_type, source="entry")
        if order.order_type == "LIMIT" and ref_price > 0:
            order.price = order.compute_limit_price(ref_price)

        # QUOTE-DRIVEN ENTRY (execution_mode=="marketable_limit" only). The
        # market path above is UNTOUCHED. Here we consult the pre-fetched live book
        # (bid/ask/circuit, threaded in from the ONE batched get_quotes in
        # _fire_entries) in ENTRY mode (entry=True) — GUARANTEE the fill:
        #   * a NORMAL book → a genuine MARKET order (fills instantly at the touch
        #     for ANY gap; Kite margins it on LTP, no limit-price margin inflation);
        #   * a stock LOCKED at its circuit → a LIMIT queued exactly AT the circuit
        #     (a valid, ACCEPTED order that fills when the lock breaks — the CEMPRO
        #     fix, no rejection, no dropped pick).
        # (2026-07-09 KALYANKJIL: the OLD 0.3%-through-touch entry LIMIT sat BELOW a
        # +2-3% opening gap and never filled → the day's best pick was missed.)
        # Auto-trade executes; it never hands the decision back.
        if getattr(self.config, "execution_mode", "market") == "marketable_limit":
            from .execution.quote_pricer import plan_marketable_order
            # BUY side for a long entry; a short FUT entry SELLs to open.
            side = "SELL" if getattr(self.config, "direction", "long") == "short" \
                else "BUY"
            tick = _get_tick_for(broker, order.symbol)
            # CLUSTER 5 ITEM 3 — enforce the ENTRY quote-freshness SLA: an entry
            # is NEVER priced off a stale book. plan_marketable_order returns a
            # skip (policy=="skip") or a degraded MARKET fallback (policy=="market")
            # when the quote age exceeds entry_quote_max_age_sec.
            plan = plan_marketable_order(
                side, order.symbol, qty, quote, tick, self.config,
                ltp_fallback=(ref_price or None), entry=True,
                now_ts=time.time(),
                max_quote_age_sec=getattr(self.config,
                                          "entry_quote_max_age_sec", 10.0),
                stale_policy=getattr(self.config,
                                     "entry_stale_quote_policy", "market"))
            if plan.get("skip"):
                # Stale-quote SKIP policy: do NOT place, do NOT register (no
                # phantom). The leg is dropped with a clear degraded_quote reason.
                log.warning("ENTRY %s: %s SKIPPED — %s",
                            symbol, side, plan.get("reason"))
                return {"symbol": symbol, "status": "SKIPPED",
                        "reason": plan.get("reason"),
                        "degraded_quote": True,
                        "broker_profile": prof.profile_id}
            if plan.get("ok"):
                # Locked at the circuit → LIMIT queued AT the circuit. Per
                # entry_circuit_locked_policy="drop", _reconcile_entry_fill DROPS it
                # if it does not fill within the reconcile window (don't chase).
                order.order_type = "LIMIT"
                order.price = float(plan["price"])
                log.info("ENTRY %s: %s LIMIT @ %.2f (%s)",
                         symbol, side, order.price, plan.get("reason"))
            else:
                # Normal book (or a stale-quote degrade under policy=="market") →
                # a genuine MARKET order; NOT priced off the (possibly stale) book.
                if plan.get("degraded_quote"):
                    log.warning("ENTRY %s: %s MARKET (degraded quote) — %s",
                                symbol, side, plan.get("reason"))
                else:
                    log.info("ENTRY %s: %s MARKET — %s",
                             symbol, side, plan.get("reason"))

        # VWAP: observe the window then place MARKET (skip the wait in paper to
        # keep smoke tests fast; live honours the window).
        if order.order_type == "VWAP" and not self.dry_run:
            await asyncio.sleep(min(self.config.vwap_window_seconds, 1))

        try:
            res = await place_order_with_retry(order, broker)
        except Exception as e:
            log.error("place failed %s: %s", symbol, e)
            return {"symbol": symbol, "status": "FAILED", "error": str(e)}

        # REJECTION GUARD (real-money safety). A FAILED result — or, live, a
        # missing broker_order_id — means the broker REJECTED the order (invalid
        # product, margin, IP, market-hours…). It must NEVER be registered as a
        # position: doing so creates a PHANTOM position + orphan GTT and falsely
        # marks the session RUNNING. (2026-07-01 incident: NRML-on-equity rejected
        # all legs, but they were registered anyway.)
        if getattr(res, "status", None) == "FAILED" or (
                not self.dry_run and not getattr(res, "broker_order_id", None)):
            err = getattr(res, "error", None) or "order rejected by broker"
            log.error("entry REJECTED %s: %s — NOT registering", symbol, err)
            return {"symbol": symbol, "status": "FAILED", "error": err,
                    "broker_profile": prof.profile_id}

        # FILL RECONCILIATION (real-money accuracy). A live PLACED order returns
        # no avg_price/filled_qty, so poll the broker for the ACTUAL fill and
        # register the position at the REAL fill price — not the pre-trade 9:15
        # reference mark. Registering at the mark mis-states P&L (can flip a loss
        # into a shown win) AND feeds the trail engine inflated returns.
        # (2026-07-01 incident: panel +₹2,298 vs broker +₹1,340.)
        fill_price = res.avg_price
        fill_qty = res.filled_qty
        if not self.dry_run and res.broker_order_id and (
                not fill_price or not fill_qty):
            rec = await self._reconcile_entry_fill(
                broker, res.broker_order_id, qty)
            if rec and rec.get("rejected"):
                # POST-PLACEMENT REJECTION (real-money safety). The broker gave an
                # order_id (so the upfront guard passed) but the EXCHANGE rejected
                # the order asynchronously (circuit-limit breach / RMS) → filled_qty
                # is 0, there is NO position. Do NOT fall back to the entry mark and
                # register a PHANTOM: it would inflate basket return + trail and make
                # a later exit try to SELL shares we never bought. Drop the leg.
                # (2026-07-06 incident: CEMPRO pinned at its upper circuit.)
                err = (f"broker rejected order {res.broker_order_id} "
                       f"post-placement ({rec.get('status')})")
                log.error("entry REJECTED (post-placement) %s: %s — NOT "
                          "registering", symbol, err)
                return {"symbol": symbol, "status": "FAILED", "error": err,
                        "broker_profile": prof.profile_id}
            if rec:
                fill_price = rec.get("avg_price") or fill_price
                fill_qty = rec.get("filled_qty") or fill_qty
        reconciled = bool(fill_price and fill_qty)
        # PHANTOM-FILL GUARD (real-money, 2026-07-09). NEVER register a LIVE
        # position at the pre-trade reference mark. In live a fill is booked ONLY
        # when the broker CONFIRMED it — either place_order returned fill numbers,
        # or _reconcile_entry_fill returned a real (possibly partial) fill. An
        # unconfirmed live order is already dropped upstream via the rejected /
        # CANCELLED_UNFILLED path, so it never reaches here with empty fill data;
        # this is defence-in-depth so no future change can re-introduce a phantom.
        # The reference-mark fallback is for DRY-RUN (paper) ONLY, where no real
        # order exists. (KALYANKJIL 2026-07-09: a cancelled 0-fill order booked at
        # the ₹384.55 mark → 1338 phantom shares that tripped the basket exit.)
        if not (fill_price and fill_qty):
            if not self.dry_run:
                log.error("entry %s: no confirmed fill in LIVE — dropping leg "
                          "(phantom guard; refusing to register at the mark)",
                          symbol)
                return {"symbol": symbol, "status": "FAILED",
                        "error": "no confirmed fill (phantom guard)",
                        "broker_profile": prof.profile_id}
            fill_price = fill_price or ref_price
            fill_qty = fill_qty or qty
        acct_id = getattr(prof, "broker_account_id", None)
        # FUTURES long/short: persist the session direction on the position so
        # the P&L sign, exit side, and GTT orientation invert ONLY for shorts.
        _direction = getattr(self.config, "direction", "long")
        # For a FUT SHORT the position symbol is the FUT contract (order.symbol),
        # not the bare underlying — register under the contract so the exit /
        # GTT / mark refer to the same tradeable instrument.
        register_symbol = order.symbol if prof.instrument_type == "FUT" else symbol
        # EXCHANGE-CONSISTENCY (F&O): persist the ORDER's exchange (NFO for
        # FUT/CE/PE, NSE for cash) on the position row. Downstream paths that key
        # off the stored exchange — notably GTTManager.place_for_position, which
        # passes pos["exchange"] straight to kite.place_gtt — would otherwise
        # default a FUT contract to NSE and place the OCO on the wrong segment
        # (Kite rejects it → the F&O position runs with NO broker-held backup).
        _exchange = getattr(order, "exchange", None)
        # RECONCILIATION FRAMEWORK (Phase 1): thread the broker ENTRY order-id
        # onto the position row so this fill is attributable to THIS session by
        # order-id (never the account aggregate). None in dry-run / when the
        # broker gave no id (reconcilers handle absent ids).
        _entry_oid = getattr(res, "broker_order_id", None)
        # CAP 2/3 — the broker accepted (order-id assigned): transition the durable
        # intent to ORDER_SUBMITTED, mapping client_order_id → broker order-id.
        # Paper (_entry_oid None) still records the transition keyed by the
        # synthetic client_order_id (broker id NULL). Best-effort, never blocks.
        order_ledger.append_event(
            session_id=self.session_id, symbol=register_symbol,
            event_type=order_ledger.EV_ORDER_SUBMITTED,
            position_ref=f"{self.session_id}:{register_symbol}",
            product=prof.order_product, broker_profile=prof.profile_id,
            broker_order_id=_entry_oid, client_order_id=client_order_id,
            qty=fill_qty, price=fill_price, source="entry")
        # Lifecycle#9 — classify a partial ENTRY by fill_qty < ORDERED qty, not
        # only res.status=="PARTIAL". A broker can report COMPLETE while filling
        # fewer shares than ordered (RMS trim / thin book); that under-fill must be
        # flagged (register_partial's warning + a PARTIAL status the panel shows),
        # not silently registered as a clean full fill.
        _under_fill = (fill_qty is not None and qty
                       and int(fill_qty) < int(qty))
        _is_partial = (res.status == "PARTIAL") or bool(_under_fill)
        if _is_partial:
            if _under_fill and res.status != "PARTIAL":
                log.warning("session %s: entry UNDER-FILL for %s — ordered %d, "
                            "filled %d (broker status=%s) → flagged PARTIAL",
                            self.session_id, register_symbol, int(qty),
                            int(fill_qty), res.status)
            self.registry.register_partial(register_symbol, prof.profile_id,
                                           fill_qty, fill_price,
                                           product=prof.order_product,
                                           instrument_type=prof.instrument_type,
                                           exchange=_exchange,
                                           broker_account_id=acct_id,
                                           direction=_direction,
                                           entry_order_id=_entry_oid,
                                           client_order_id=client_order_id)
        else:
            self.registry.register(symbol=register_symbol,
                                   broker_profile=prof.profile_id,
                                   qty=fill_qty, avg_price=fill_price,
                                   product=prof.order_product,
                                   instrument_type=prof.instrument_type,
                                   exchange=_exchange,
                                   broker_account_id=acct_id,
                                   direction=_direction,
                                   entry_order_id=_entry_oid,
                                   client_order_id=client_order_id)
        if ref_price > 0 and reconciled:
            record_slippage(symbol, ref_price, fill_price, fill_qty,
                            session_id=self.session_id,
                            broker_profile=prof.profile_id)
        _ret_status = "PARTIAL" if _is_partial else res.status
        return {"symbol": symbol, "status": _ret_status, "qty": fill_qty,
                "price": fill_price, "broker_order_id": res.broker_order_id,
                "broker_profile": prof.profile_id, "order_type": order.order_type,
                "reconciled": reconciled, "ordered_qty": int(qty) if qty else None}

    async def _place_confirm_child(self, broker, prof, symbol: str,
                                   leg_qty: int, ref_price: float,
                                   quote: Optional[Dict[str, Any]],
                                   client_order_id: str, tag: str,
                                   force_marketable: bool = False
                                   ) -> Dict[str, Any]:
        """ICEBERG (CAP 2) — place + confirm ONE child leg of an iceberg ENTRY.

        force_marketable (WORKED-ORDER): price the child as a marketable-LIMIT
        within the circuit band REGARDLESS of execution_mode. A worked-order
        session uses execution_mode=="worked" (not "marketable_limit"), but every
        worked CHILD must still be a circuit-aware marketable-limit — this flag
        makes that pricing engage without changing the one-shot execution_mode
        semantics. Default False = the existing iceberg behaviour (byte-for-byte).

        Mirrors _place_one's place+reconcile CORE (marketable-limit pricing,
        rejection guard, fill reconciliation, phantom-fill guard) but does NOT
        register a position — the iceberg caller aggregates the confirmed child
        fills and registers ONE position. Returns a dict:
          * {skip:True, reason}                    — stale-quote SKIP policy.
          * {rejected:True, error}                 — child rejected / no fill.
          * {fill_price, fill_qty, broker_order_id, res_status, reconciled,
             order_symbol, exchange, under_fill, ordered_qty} — a confirmed
             (possibly under-)fill.
        Each child carries the shared PARENT client_order_id + a leg-indexed tag
        (attributable + idempotent per C2/C3)."""
        order = build_order(symbol, leg_qty, self.config, broker)
        order.client_order_id = client_order_id
        order.tag = tag
        _entry_side = "SELL" if getattr(self.config, "direction", "long") \
            == "short" else "BUY"
        # CAP 3 — durable ORDER_CREATED intent BEFORE the broker call, per child.
        order_ledger.record_intent(
            session_id=self.session_id, symbol=symbol,
            client_order_id=client_order_id, qty=leg_qty, side=_entry_side,
            product=prof.order_product, broker_profile=prof.profile_id,
            instrument_type=prof.instrument_type, source="entry_iceberg")
        if order.order_type == "LIMIT" and ref_price and ref_price > 0:
            order.price = order.compute_limit_price(ref_price)
        # QUOTE-DRIVEN pricing (marketable_limit OR a forced worked child) —
        # mirrors _place_one.
        if force_marketable or getattr(
                self.config, "execution_mode", "market") == "marketable_limit":
            from .execution.quote_pricer import plan_marketable_order
            side = "SELL" if getattr(self.config, "direction", "long") \
                == "short" else "BUY"
            tick = _get_tick_for(broker, order.symbol)
            plan = plan_marketable_order(
                side, order.symbol, leg_qty, quote, tick, self.config,
                ltp_fallback=(ref_price or None), entry=True, now_ts=time.time(),
                max_quote_age_sec=getattr(self.config,
                                          "entry_quote_max_age_sec", 10.0),
                stale_policy=getattr(self.config,
                                     "entry_stale_quote_policy", "market"))
            if plan.get("skip"):
                log.warning("ICEBERG ENTRY %s: child %s SKIPPED — %s",
                            symbol, side, plan.get("reason"))
                return {"skip": True, "reason": plan.get("reason")}
            if plan.get("ok"):
                order.order_type = "LIMIT"
                order.price = float(plan["price"])
        if order.order_type == "VWAP" and not self.dry_run:
            await asyncio.sleep(min(self.config.vwap_window_seconds, 1))
        try:
            res = await place_order_with_retry(order, broker)
        except Exception as e:
            log.error("ICEBERG place failed %s: %s", symbol, e)
            return {"rejected": True, "error": str(e)}
        # REJECTION GUARD (same as _place_one).
        if getattr(res, "status", None) == "FAILED" or (
                not self.dry_run and not getattr(res, "broker_order_id", None)):
            err = getattr(res, "error", None) or "order rejected by broker"
            return {"rejected": True, "error": err}
        fill_price = res.avg_price
        fill_qty = res.filled_qty
        if not self.dry_run and res.broker_order_id and (
                not fill_price or not fill_qty):
            rec = await self._reconcile_entry_fill(
                broker, res.broker_order_id, leg_qty)
            if rec and rec.get("rejected"):
                return {"rejected": True,
                        "error": f"post-placement reject ({rec.get('status')})"}
            if rec:
                fill_price = rec.get("avg_price") or fill_price
                fill_qty = rec.get("filled_qty") or fill_qty
        reconciled = bool(fill_price and fill_qty)
        # PHANTOM-FILL GUARD — a LIVE order with no confirmed fill is DROPPED (never
        # booked at the mark); the mark fallback is DRY-RUN only.
        if not (fill_price and fill_qty):
            if not self.dry_run:
                return {"rejected": True,
                        "error": "no confirmed fill (phantom guard)"}
            fill_price = fill_price or ref_price
            fill_qty = fill_qty or leg_qty
        _entry_oid = getattr(res, "broker_order_id", None)
        # CAP 2 — a ledger ORDER_SUBMITTED event PER CHILD (maps this child's
        # client_order_id → its broker order-id).
        order_ledger.append_event(
            session_id=self.session_id, symbol=order.symbol,
            event_type=order_ledger.EV_ORDER_SUBMITTED,
            position_ref=f"{self.session_id}:{order.symbol}",
            product=prof.order_product, broker_profile=prof.profile_id,
            broker_order_id=_entry_oid, client_order_id=client_order_id,
            qty=fill_qty, price=fill_price, source="entry_iceberg")
        _under = (fill_qty is not None and leg_qty
                  and int(fill_qty) < int(leg_qty))
        return {"fill_price": float(fill_price), "fill_qty": int(fill_qty),
                "broker_order_id": _entry_oid, "res_status": res.status,
                "reconciled": reconciled, "order_symbol": order.symbol,
                "exchange": getattr(order, "exchange", None),
                "under_fill": bool(_under), "ordered_qty": int(leg_qty)}

    async def _place_iceberg(self, broker, prof, symbol: str, total_qty: int,
                             legs: List[int], ref_price: float,
                             quote: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """ICEBERG (CAP 2 + CAP 3-entry) — place N child ENTRY orders (each <=
        slice) SEQUENTIALLY, confirm each, then register ONE aggregate position at
        the QUANTITY-WEIGHTED-AVERAGE fill price with the total filled qty.

        ROBUSTNESS (CAP 3, entry side): a child reject / under-fill STOPS further
        children and registers the FILLED-so-far as a smaller REAL position
        (correct qty + avg), surfacing a warning — NEVER the intended qty when
        less filled (the phantom-fill class). Every child is recorded in the ledger
        (an ORDER_CREATED intent + an ORDER_SUBMITTED event per child); all child
        broker order-ids are collected on the result. The registered position's
        downstream (GTT/SL-M/trail/exit/reconcile) sees ONE aggregate position
        exactly as a non-iceberg entry."""
        parent_coid = order_ledger.make_client_order_id(self.session_id, symbol)
        filled_total = 0
        weighted_px = 0.0
        child_oids: List[str] = []
        n_children = 0
        stopped_reason: Optional[str] = None
        order_symbol = symbol
        exchange: Optional[str] = None
        any_reconciled = False
        last_status = "PLACED"
        for i, leg_qty in enumerate(legs):
            child_coid = f"{parent_coid}-L{i}"
            child_tag = order_ledger.compact_tag(child_coid)
            child = await self._place_confirm_child(
                broker, prof, symbol, int(leg_qty), ref_price, quote,
                child_coid, child_tag)
            if child.get("skip"):
                stopped_reason = f"child {i} skipped ({child.get('reason')})"
                log.warning("ICEBERG %s: %s — stopping at %d filled",
                            symbol, stopped_reason, filled_total)
                break
            if child.get("rejected"):
                stopped_reason = f"child {i} rejected ({child.get('error')})"
                log.error("ICEBERG %s: %s — STOPPING; registering filled-so-far "
                          "%d (NOT the intended %d)", symbol, stopped_reason,
                          filled_total, int(total_qty))
                break
            n_children += 1
            filled_total += child["fill_qty"]
            weighted_px += child["fill_qty"] * child["fill_price"]
            if child.get("broker_order_id"):
                child_oids.append(str(child["broker_order_id"]))
            order_symbol = child.get("order_symbol") or order_symbol
            exchange = child.get("exchange") or exchange
            any_reconciled = any_reconciled or bool(child.get("reconciled"))
            last_status = child.get("res_status") or last_status
            if child.get("under_fill"):
                stopped_reason = (f"child {i} under-filled "
                                  f"{child['fill_qty']}/{int(leg_qty)}")
                log.warning("ICEBERG %s: %s — STOPPING; registering filled-so-far "
                            "%d", symbol, stopped_reason, filled_total)
                break
        if filled_total <= 0:
            # No child filled → NO position (never a phantom). Dropped leg.
            return {"symbol": symbol, "status": "FAILED",
                    "error": stopped_reason or "iceberg: no child filled",
                    "broker_profile": prof.profile_id, "iceberg": True,
                    "n_children": 0}
        avg_px = weighted_px / filled_total
        partial_iceberg = filled_total < int(total_qty)
        acct_id = getattr(prof, "broker_account_id", None)
        _direction = getattr(self.config, "direction", "long")
        register_symbol = order_symbol if prof.instrument_type == "FUT" else symbol
        # Attribute the aggregate position to the FIRST child's broker order-id
        # (every child id is on child_oids + in the ledger). The PARENT
        # client_order_id ties the whole iceberg together on the position row.
        primary_oid = child_oids[0] if child_oids else None
        if partial_iceberg:
            log.warning("session %s: ICEBERG %s PARTIAL — intended %d, filled %d "
                        "across %d child(ren) [%s]; registering the REAL filled "
                        "qty (not the intended)", self.session_id, register_symbol,
                        int(total_qty), filled_total, n_children,
                        stopped_reason or "")
            self.registry.register_partial(
                register_symbol, prof.profile_id, filled_total, avg_px,
                product=prof.order_product,
                instrument_type=prof.instrument_type, exchange=exchange,
                broker_account_id=acct_id, direction=_direction,
                entry_order_id=primary_oid, client_order_id=parent_coid)
        else:
            self.registry.register(
                symbol=register_symbol, broker_profile=prof.profile_id,
                qty=filled_total, avg_price=avg_px, product=prof.order_product,
                instrument_type=prof.instrument_type, exchange=exchange,
                broker_account_id=acct_id, direction=_direction,
                entry_order_id=primary_oid, client_order_id=parent_coid)
        if ref_price and ref_price > 0 and any_reconciled:
            record_slippage(symbol, ref_price, avg_px, filled_total,
                            session_id=self.session_id,
                            broker_profile=prof.profile_id)
        _status = "PARTIAL" if partial_iceberg else last_status
        return {"symbol": symbol, "status": _status, "qty": filled_total,
                "price": avg_px, "broker_order_id": primary_oid,
                "broker_profile": prof.profile_id, "iceberg": True,
                "n_children": n_children, "child_order_ids": child_oids,
                "reconciled": any_reconciled, "ordered_qty": int(total_qty),
                "partial": partial_iceberg, "stopped_reason": stopped_reason}

    # ══════════════════════════════════════════════════════════════════════════
    # WORKED ORDER (participation / TWAP) — large-size entry BUILD + exit UNWIND
    # ══════════════════════════════════════════════════════════════════════════

    def _worked_deadline_ts(self, *, exit_side: bool = False) -> Optional[float]:
        """The epoch-seconds deadline the worked engine paces toward (delegates to
        worked_order.resolve_deadline_ts — worked_deadline else square_off_time;
        exit_side tightens it)."""
        from .execution.worked_order import resolve_deadline_ts
        return resolve_deadline_ts(self.config, tighten_exit=exit_side)

    def _register_worked_position(self, prof, register_symbol: str,
                                  filled_total: int, avg_px: float,
                                  exchange: Optional[str], first_oid: Optional[str],
                                  parent_coid: str, direction: str) -> None:
        """Incrementally UPSERT the growing worked position (running total qty +
        quantity-weighted-average fill) so the monitor/kill/trail see it LIVE during
        the build, then keep the kill/trail denominator matching the built book
        (refreeze_invested_basis). register() is an UPSERT keyed by
        (session, symbol, profile) → the SAME row grows child by child."""
        acct_id = getattr(prof, "broker_account_id", None)
        self.registry.register(
            symbol=register_symbol, broker_profile=prof.profile_id,
            qty=int(filled_total), avg_price=float(avg_px),
            product=prof.order_product, instrument_type=prof.instrument_type,
            exchange=exchange, broker_account_id=acct_id, direction=direction,
            entry_order_id=first_oid, client_order_id=parent_coid)
        try:
            self.monitor.refreeze_invested_basis()
        except Exception as e:  # pragma: no cover - never block the build
            log.debug("worked %s refreeze_invested_basis failed: %s",
                      register_symbol, e)

    async def _work_entry_leg(self, broker, prof, symbol: str, target_qty: int,
                              ref_price: float, *,
                              now_fn=None, sleep_fn=None, volume_fn=None,
                              deadline_ts=None, child_sizer=None) -> Dict[str, Any]:
        """WORK one entry leg into `target_qty` shares PACED over time, reusing the
        safe child path (_place_confirm_child: ledger, unique client_order_id per
        child, query-before-retry, fill reconciliation, phantom-fill guard). Each
        confirmed child grows ONE aggregate position (incrementally registered) and
        refreezes the kill/trail basis. Accepts partials; surfaces the SHORTFALL.

        Injection (tests): now_fn / sleep_fn / volume_fn / deadline_ts. Production
        uses time.time / asyncio.sleep / the poller volume reader / the resolved
        worked deadline."""
        from .execution import worked_order as _wo
        _direction = getattr(self.config, "direction", "long")
        _side = "SELL" if _direction == "short" else "BUY"
        parent_coid = order_ledger.make_client_order_id(self.session_id, symbol)
        # Accumulators shared with the place_child closure so the position grows
        # child-by-child (live-monitored) rather than only at the end.
        agg = {"filled": 0, "weighted": 0.0, "order_symbol": symbol,
               "exchange": None, "first_oid": None}

        async def _place_child(*, idx: int, qty: int, recent_volume=None):
            child_coid = f"{parent_coid}-W{idx}"
            child_tag = order_ledger.compact_tag(child_coid)
            # A FRESH live book per child (the book moves across the window) so each
            # child is a circuit-aware marketable-limit.
            quote = None
            try:
                qmap = broker.get_quotes([symbol])
                quote = (qmap or {}).get(symbol)
            except Exception:  # pragma: no cover - defensive; MARKET fallback inside
                quote = None
            child = await self._place_confirm_child(
                broker, prof, symbol, int(qty), ref_price, quote,
                child_coid, child_tag, force_marketable=True)
            if child.get("skip"):
                return {"filled_qty": 0, "avg_price": 0.0, "status": "SKIP",
                        "skip": True, "reason": child.get("reason"),
                        "client_order_id": child_coid}
            if child.get("rejected"):
                return {"filled_qty": 0, "avg_price": 0.0, "status": "REJECTED",
                        "rejected": True, "error": child.get("error"),
                        "client_order_id": child_coid}
            fq = int(child["fill_qty"])
            fp = float(child["fill_price"])
            if fq > 0:
                agg["filled"] += fq
                agg["weighted"] += fq * fp
                if agg["first_oid"] is None:
                    agg["first_oid"] = child.get("broker_order_id")
                agg["order_symbol"] = child.get("order_symbol") or agg["order_symbol"]
                agg["exchange"] = child.get("exchange") or agg["exchange"]
                _reg_symbol = (agg["order_symbol"] if prof.instrument_type == "FUT"
                               else symbol)
                self._register_worked_position(
                    prof, _reg_symbol, agg["filled"],
                    agg["weighted"] / agg["filled"], agg["exchange"],
                    agg["first_oid"], parent_coid, _direction)
            return {"filled_qty": fq, "avg_price": fp,
                    "status": child.get("res_status") or "OK",
                    "broker_order_id": child.get("broker_order_id"),
                    "client_order_id": child_coid}

        freeze_cap = None
        try:
            from . import iceberg as _ice
            freeze_cap = _ice.freeze_cap_for(self.config, symbol)
        except Exception:  # pragma: no cover - defensive
            freeze_cap = None
        parent = _wo.WorkedParent(
            symbol=symbol, side=_side, target_qty=int(target_qty), kind="entry",
            product=prof.order_product, instrument_type=prof.instrument_type,
            session_id=self.session_id, broker_profile=prof.profile_id,
            deadline_ts=(deadline_ts if deadline_ts is not None
                         else self._worked_deadline_ts()),
            interval_sec=float(getattr(self.config, "worked_interval_sec", 20)),
            participation_pct=float(getattr(self.config,
                                            "worked_participation_pct", 0.10)),
            freeze_cap=freeze_cap,
            min_child_qty=int(getattr(self.config, "worked_min_child_qty", 1)),
            max_children=int(getattr(self.config, "worked_max_children", 500)))
        _vol_fn = volume_fn
        if _vol_fn is None and not self.dry_run:
            _vol_fn = _wo.recent_interval_volume
        # v2 (VWAP-curve pacing): build the pluggable child sizer when
        # worked_vwap_enabled (else None → v1 flat POV, byte-identical). A symbol
        # with no profile → make_vwap_sizer returns None → v1 fallback.
        _sizer = child_sizer
        if _sizer is None:
            _sizer = _wo.make_vwap_sizer(self.config, symbol)
        result = await _wo.work_order(
            parent, place_child=_place_child, volume_fn=_vol_fn,
            now_fn=now_fn, sleep_fn=sleep_fn, child_sizer=_sizer)
        # GTT-OCO backup on the built worked position (best-effort; positions
        # appear over the window so this runs after the build).
        try:
            if self.config.per_position_gtt_enabled and self.gtt_manager:
                self.gtt_manager.backfill_missing()
        except Exception as e:  # pragma: no cover - never block on the backup
            log.warning("worked %s GTT backfill failed: %s", symbol, e)
        if result.shortfall > 0:
            log.warning("session %s: WORKED entry %s SHORTFALL — target %d filled "
                        "%d (%d short) across %d children (%s)", self.session_id,
                        symbol, result.target_qty, result.filled_qty,
                        result.shortfall, result.n_children, result.stopped_reason)
        _status = ("FAILED" if result.filled_qty == 0
                   else ("PARTIAL" if result.shortfall > 0 else "PLACED"))
        return {"symbol": symbol, "status": _status, "qty": result.filled_qty,
                "price": result.avg_fill_price, "broker_order_id": agg["first_oid"],
                "broker_profile": prof.profile_id, "worked": True,
                "n_children": result.n_children, "shortfall": result.shortfall,
                "target_qty": result.target_qty,
                "stopped_reason": result.stopped_reason,
                "ordered_qty": int(target_qty)}

    async def _fire_entries_worked(self, leg_specs: List[tuple],
                                   skipped_picks: List[Dict[str, Any]],
                                   fire_t0: float,
                                   margin_fallback_warnings: List[Dict[str, Any]],
                                   degraded_profiles: List[Dict[str, Any]]
                                   ) -> Dict[str, Any]:
        """WORKED-mode entry: spawn ONE background worked-build task per leg and
        return RUNNING immediately (the build runs over the pacing window; blocking
        _fire_entries for hours would be wrong). Each task grows + protects its
        position as it fills. Drivers/square-off arm now so the growing book is
        monitored from the first child. A worked session with NO tradeable legs is
        FAILED like the one-shot path."""
        specs: List[tuple] = []
        for (broker, prof, pick, amount, allocator, cache, qty, quote) in leg_specs:
            ref_price = 0.0
            if cache and pick.symbol in cache and (cache.get(pick.symbol) or {}).get("ltp"):
                ref_price = float(cache[pick.symbol]["ltp"])
            elif ref_price <= 0:
                try:
                    ref_price = float(broker.get_ltp(pick.symbol) or 0.0)
                except Exception:  # pragma: no cover - defensive
                    ref_price = 0.0
            specs.append((broker, prof, pick.symbol, int(qty), ref_price))
        if not specs:
            reason = ("no positions worked — no tradeable picks for this worked "
                      "session (all skipped / not eligible)")
            self._set_status("FAILED", reason=reason, closed_at=_now_ist_iso())
            log.error("session %s marked FAILED: %s", self.session_id, reason)
            return {"session_id": self.session_id, "status": "FAILED",
                    "mode": self.mode, "n_placed": 0, "orders": [],
                    "skipped_picks": skipped_picks, "reason": reason}
        # Seed the kill/trail basis (falls back to total_allocated_capital with 0
        # positions); each leg refreezes to the actual built notional as it fills.
        try:
            self.monitor.freeze_invested_basis()
        except Exception as e:  # pragma: no cover - never block start
            log.warning("worked invested_basis seed failed for %s: %s",
                        self.session_id, e)
        # Arm the monitors NOW so the growing book is watched from the first child.
        for _name, _fn in (("tick", lambda: tick_driver.start_for_session(self.session_id)),
                           ("ws", lambda: ws_driver.start_for_session(self.session_id))):
            try:
                _fn()
            except Exception as e:  # pragma: no cover - never block start
                log.warning("worked %s driver start failed for %s: %s",
                            _name, self.session_id, e)
        try:
            self._arm_square_off()
        except Exception as e:  # pragma: no cover
            log.warning("worked square-off arm failed for %s: %s", self.session_id, e)
        if not hasattr(self, "_worked_entry_tasks"):
            self._worked_entry_tasks = set()
        loop = asyncio.get_event_loop()
        for (broker, prof, symbol, target_qty, ref_price) in specs:
            task = loop.create_task(
                self._work_entry_leg(broker, prof, symbol, target_qty, ref_price))
            self._worked_entry_tasks.add(task)
            task.add_done_callback(self._worked_entry_tasks.discard)
        entry_latency_ms = int((time.monotonic() - fire_t0) * 1000)
        try:
            self._record_latency(entry_latency_ms=entry_latency_ms)
        except Exception as e:  # pragma: no cover - observability only
            log.debug("worked entry_latency record failed for %s: %s",
                      self.session_id, e)
        log.info("session %s WORKED entry building %d legs over the pacing window",
                 self.session_id, len(specs))
        _ok = {"session_id": self.session_id, "status": "RUNNING",
               "mode": self.mode, "n_placed": len(specs), "orders": [],
               "worked_building": True, "worked_legs": len(specs),
               "skipped_picks": skipped_picks}
        if margin_fallback_warnings:
            _ok["margin_fallback_warnings"] = margin_fallback_warnings
        if degraded_profiles:
            _ok["degraded_profiles"] = degraded_profiles
        return _ok

    async def _reconcile_entry_fill(self, broker, order_id: str,
                                    expected_qty: int,
                                    max_wait_sec: float = 8.0,
                                    poll_interval: float = 1.0
                                    ) -> Optional[Dict[str, Any]]:
        """Poll the broker for an entry order's ACTUAL fill (avg_price + filled
        qty). Market orders fill near-instantly; we poll briefly for COMPLETE so
        the position's entry is the REAL fill, not the 9:15 mark. Returns
        {'avg_price','filled_qty'} on a confirmed fill, else None (caller falls
        back to the mark). get_order_status runs in a thread so the event loop is
        never blocked.

        PHASE-2 (sub-second, event-driven): FIRST consult the KiteTicker order
        POSTBACK for a TERMINAL state (COMPLETE / REJECTED / CANCELLED) with a
        short wait (~1.5s). A COMPLETE postback resolves the fill in milliseconds
        (no polling); a REJECTED/CANCELLED postback returns {'rejected':True}
        immediately. ONLY if no terminal postback arrives do we fall through to
        the existing get_order_status poll (the reliable backstop). The postback
        path is skipped entirely in DRY_RUN (paper never places a real order → no
        postback), so paper behaviour is byte-for-byte unchanged."""
        if not self.dry_run:
            rec = await self._await_order_postback(order_id, timeout=1.5)
            if rec is not None:
                return rec
        deadline = time.monotonic() + max_wait_sec
        while time.monotonic() < deadline:
            try:
                st = await asyncio.to_thread(broker.get_order_status, order_id)
            except Exception as e:  # pragma: no cover - defensive
                log.debug("entry reconcile %s: get_order_status err %s",
                          order_id, e)
                await asyncio.sleep(poll_interval)
                continue
            status = str(st.get("status", "")).upper()
            filled = int(st.get("filled_quantity") or 0)
            avg = float(st.get("average_price") or 0.0)
            if status == "COMPLETE" and filled > 0 and avg > 0:
                return {"avg_price": avg, "filled_qty": filled}
            if status in ("REJECTED", "CANCELLED"):
                # TERMINAL REJECTION (distinct from 'still pending'). The broker
                # ACCEPTED the order (we have an order_id) but the EXCHANGE later
                # rejected it (circuit-limit breach, RMS…). Before dropping, honour
                # any real partial that DID fill (a CANCELLED order can still have a
                # filled_quantity>0). Otherwise signal a drop so the caller does NOT
                # fall back to the mark and register a phantom. (2026-07-06: CEMPRO.)
                if filled > 0 and avg > 0:
                    return {"avg_price": avg, "filled_qty": filled}
                return {"rejected": True, "status": status}
            await asyncio.sleep(poll_interval)
        # POLL TIMEOUT — the order is STILL PENDING (never confirmed a fill). Do
        # NOT fall back to the reference mark: that books a PHANTOM position the
        # broker never actually filled, which then pollutes basket return + trail
        # and makes a later exit try to SELL shares we never bought (2026-07-09
        # KALYANKJIL: its entry sat unfilled on a gap-up, was later cancelled 0-
        # filled, yet was booked at the ₹384.55 mark). Force a TERMINAL state:
        # CANCEL the unfilled order and read its FINAL fill. A real (even partial)
        # fill → register exactly those shares; zero filled → DROP the leg. This is
        # race-safe: we read the definitive state AFTER the cancel, so a fill that
        # landed just before the cancel is still registered, never lost or faked.
        log.warning("entry reconcile %s: no confirmed fill within %.0fs — "
                    "cancelling the unfilled order to force a terminal state",
                    order_id, max_wait_sec)
        cancel = getattr(broker, "cancel_order_sync", None)
        if callable(cancel):
            try:
                await asyncio.to_thread(cancel, order_id)
            except Exception as e:  # pragma: no cover - defensive
                log.warning("entry reconcile %s: cancel failed %s", order_id, e)
        await asyncio.sleep(poll_interval)  # let the cancel settle at the broker
        try:
            st = await asyncio.to_thread(broker.get_order_status, order_id)
        except Exception:  # pragma: no cover - defensive
            st = {}
        filled = int(st.get("filled_quantity") or 0)
        avg = float(st.get("average_price") or 0.0)
        if filled > 0 and avg > 0:
            log.warning("entry reconcile %s: PARTIAL %d @ %.2f after cancel — "
                        "registering only the real fill", order_id, filled, avg)
            return {"avg_price": avg, "filled_qty": filled}
        log.error("entry reconcile %s: 0 filled after cancel — DROPPING leg "
                  "(refusing to register a phantom at the mark)", order_id)
        return {"rejected": True, "status": "CANCELLED_UNFILLED"}

    async def _await_order_postback(self, order_id: str, timeout: float = 1.5
                                    ) -> Optional[Dict[str, Any]]:
        """Consult the KiteTicker order POSTBACK for a TERMINAL state within
        `timeout` seconds. Returns:
          * {'avg_price','filled_qty'} on a COMPLETE postback (with a real fill),
          * {'rejected':True,'status':...} on REJECTED/CANCELLED,
          * None if no usable terminal postback arrived (caller polls).
        Best-effort: any import/lookup error → None (fall through to the poll).
        The blocking wait runs in a thread so the event loop is never blocked."""
        try:
            from falcon.trade.services.kite_ticker import wait_order_terminal
        except Exception:  # pragma: no cover - ticker unavailable → poll
            return None
        try:
            upd = await asyncio.to_thread(wait_order_terminal, order_id, timeout)
        except Exception as e:  # pragma: no cover - defensive
            log.debug("entry reconcile %s: postback wait err %s", order_id, e)
            return None
        if not upd:
            return None
        status = str(upd.get("status", "")).upper()
        if status == "COMPLETE":
            filled = int(upd.get("filled_quantity") or 0)
            avg = float(upd.get("average_price") or 0.0)
            if filled > 0 and avg > 0:
                log.info("entry reconcile %s: COMPLETE via postback (%d @ %.2f)",
                         order_id, filled, avg)
                return {"avg_price": avg, "filled_qty": filled}
            # COMPLETE but no fill numbers on the postback → let the poll confirm.
            return None
        if status in ("REJECTED", "CANCELLED"):
            log.warning("entry reconcile %s: %s via postback — dropping leg",
                        order_id, status)
            return {"rejected": True, "status": status}
        return None

    # ── Tick: monitor + GTT reconcile + kill switch / trail engine ─────────────
    async def tick(self) -> Dict[str, Any]:
        # LIVE CONFIG EDIT: hot-reload any operator risk/exit edit FIRST, before
        # the trail/stop/kill decision this tick, so an Apply takes effect within
        # one tick without a restart. Safe O(1) no-op when nothing changed; never
        # touches invested_basis / trail state / positions. (Covers the tick_driver
        # path, which holds a long-lived session and calls tick() each interval.)
        try:
            self.maybe_reload_config()
        except Exception as e:  # pragma: no cover - never block the tick
            log.warning("config hot-reload failed for %s: %s", self.session_id, e)
        if not self.brokers:
            self._build_brokers()
        # AUTHORITATIVE BROKER→DB RECONCILE (real-money truth): BEFORE anything
        # else, validate our OPEN/EXIT_FAILED rows against the broker's live net
        # book (ONE call). Closes the systemic gap where a position closed at the
        # broker outside our path (RMS auto-square, manual exit, missed GTT fill)
        # stayed stale OPEN/EXIT_FAILED with phantom P&L. LIVE only; paper no-ops.
        # SAFE: an unreachable/empty book NEVER mutates the DB. Never blocks the tick.
        broker_reconciled: List[Dict[str, Any]] = []
        try:
            from .monitoring.position_reconciler import reconcile_broker_positions
            broker_reconciled = reconcile_broker_positions(self)
        except Exception as e:  # pragma: no cover - never block the tick
            log.warning("broker position reconcile failed for %s: %s",
                        self.session_id, e)
        # COORDINATION (FEATURE 3): detect positions a fired broker GTT closed
        # externally BEFORE marking to market, so gross_return recomputes on the
        # remaining positions only (denominator stays total_allocated_capital).
        gtt_closed = []
        try:
            gtt_closed = await self.gtt_manager.reconcile_gtt_fills()
        except Exception as e:  # pragma: no cover - never block the tick
            log.warning("GTT reconcile failed for %s: %s", self.session_id, e)
        self.monitor.refresh_ltps(self.brokers)

        # R4 — STAMP the reconcile-validated open-position set so the sub-second
        # ws_driver (which never runs the broker reconcile) can tell when its view
        # diverged from the last broker-validated basket and DEFER firing on a
        # phantom leg. Cheap (one hash); no broker call.
        # CLUSTER 3 ITEM 2 — stamp ONLY on a HEALTHY reconcile. When the reconcile
        # was UNHEALTHY (broker book None/unreachable this cycle) we must NOT stamp
        # the basket as validated — leaving the last-unhealthy signal in place so
        # basket_reconcile_validated() defers the fast-path fire. `reconcile_healthy`
        # is None for paper / reconcile-disabled → stamp as before (unchanged).
        try:
            from .monitoring import basket_gen
            if basket_gen.reconcile_healthy(self.session_id) is not False:
                basket_gen.stamp_reconciled(
                    self.session_id, self.registry.get_open_positions())
        except Exception as e:  # pragma: no cover - never block the tick
            log.debug("basket_gen stamp failed for %s: %s", self.session_id, e)

        # Market-hours guard: only fire software stops and retries during 09:15–15:29 IST.
        # After-hours restarts read stale closing prices which falsely trigger exits.
        _now_ist_tick = datetime.now(IST)
        _in_market_hours = (
            _now_ist_tick.replace(hour=9, minute=15, second=0, microsecond=0)
            <= _now_ist_tick <=
            _now_ist_tick.replace(hour=15, minute=29, second=0, microsecond=0)
        )

        # ── CLUSTER 6 — money-losing-event PAGING (LIVE only; deduped; NON-mutating).
        # Route each detected real-money failure through alerts.send_urgent so a
        # HUMAN is paged. Paper (dry_run) never pages → byte-identical paper. Fully
        # guarded: an alerting failure NEVER blocks the tick / touches a position.
        try:
            if not self.dry_run:
                from .monitoring import alert_monitor as _am6
                from .monitoring import basket_gen as _bg6
                _status6 = self._current_status()
                _open6 = self.registry.get_open_positions()
                # (b) reconcile divergences (UNATTRIBUTED_CLOSE / ORPHAN / CORP_ACTION)
                _am6.page_recon_divergences(self.session_id, broker_reconciled, True)
                # (a) EXIT_FAILED legs still held.
                try:
                    _ef6 = self.monitor.get_exit_failed_positions()
                except Exception:  # noqa: BLE001
                    _ef6 = []
                _am6.page_exit_failed(self.session_id, _ef6, False, True)
                # (c) reconcile-staleness for a RUNNING session during market hours.
                _am6.page_reconcile_stale(
                    self.session_id,
                    _bg6.last_successful_reconcile_age_seconds(self.session_id),
                    _status6 == "RUNNING", _in_market_hours, True)
                # (d) mark-staleness (a stalled ticker → stale marks).
                _am6.page_mark_stale(
                    self.session_id, _oldest_mark_age_ms(_open6), False, True)
                # ITEM 3 — NAKED / unmanaged real broker position (throttled scan).
                _am6.maybe_detect_naked(self)
                # Re-push any UNACKED urgent alert past the escalation threshold.
                alerts.maybe_escalate()
        except Exception as _al6_e:  # noqa: BLE001 — never block the tick
            log.warning("cluster6 alert wiring failed for %s: %s",
                        self.session_id, _al6_e)

        # EXIT_FAILED RETRY: after the GTT reconcile step, re-attempt any
        # position whose exit previously failed and whose exit_gate was
        # released by registry.mark_exit_failed. Uses the same
        # _exit_single_position path (which now calls confirm_exit).
        # Guard: skip retries outside market hours — market orders will be
        # rejected by Kite and the retry will just keep failing until open.
        try:
            failed_positions = self.monitor.get_exit_failed_positions()
            if failed_positions and not _in_market_hours:
                log.debug("EXIT_RETRY suppressed outside market hours for %s (%d positions)",
                          self.session_id, len(failed_positions))
            elif failed_positions:
                # Await all exit coroutines directly (not create_task) so they
                # complete before tick() returns. asyncio.run() per tick cancels
                # any pending tasks on tick completion, which would leave exit_lock=1
                # permanently stuck if create_task were used here.
                _exit_coros = []
                for fp in failed_positions:
                    if _exit_gate_mod.claim_exit_session(
                            self.session_id, fp["symbol"], "EXIT_RETRY",
                            broker_profile=fp.get("broker_profile")):
                        _exit_coros.append(_exit_single_position(
                            session_id=self.session_id,
                            position=fp,
                            reason="EXIT_RETRY",
                            brokers=self.brokers,
                            registry=self.registry,
                            gtt_manager=self.gtt_manager,
                            kite_product=self.config.order_product,
                            exec_cfg=self.config,
                        ))
                if _exit_coros:
                    await asyncio.gather(*_exit_coros, return_exceptions=True)
        except Exception as _efr_e:
            log.warning("EXIT_FAILED retry sweep failed for %s: %s",
                        self.session_id, _efr_e)

        # C2 — KILLING_INCOMPLETE servicing. A kill/exit that left a stranded
        # EXIT_FAILED leg set this NON-terminal status so the retry sweep above
        # keeps re-attempting the leg. Do NOT re-run the kill-threshold fire here
        # (the basket is already being flattened; a fresh fire would churn GTTs /
        # could prematurely mark CLOSED). Once every leg is flat → CLOSED (terminal
        # → the tick driver stops next iteration). This never fires on the normal
        # all-success kill (that path is CLOSED immediately).
        if self._current_status() == "KILLING_INCOMPLETE":
            snap = self.monitor.snapshot()
            if not self._has_unflat_positions():
                self._set_status("CLOSED",
                                 closed_at=datetime.now(IST).isoformat())
                log.critical("KILLING_INCOMPLETE resolved — all legs flat, "
                             "session CLOSED %s", self.session_id)
                still_incomplete = False
            else:
                still_incomplete = True
            # CLUSTER 6 (a) — page while the kill is still incomplete (live only).
            if not self.dry_run:
                try:
                    from .monitoring import alert_monitor as _am6b
                    _am6b.page_exit_failed(
                        self.session_id, self.monitor.get_exit_failed_positions(),
                        still_incomplete, True)
                except Exception as _al6b_e:  # noqa: BLE001 — never block the tick
                    log.warning("cluster6 KILLING_INCOMPLETE page failed for %s: %s",
                                self.session_id, _al6b_e)
            return {
                "gross_return": self.monitor.compute_gross_return_invested(),
                "gross_return_fund": snap["gross_return"], "snapshot": snap,
                "kill_switch_fired": False, "kill_reason": None,
                "fire_result": None, "gtt_closed": gtt_closed,
                "broker_reconciled": broker_reconciled,
                "killing_incomplete": still_incomplete}

        # RMS CAP 2 — PORTFOLIO DAILY-LOSS CIRCUIT BREAKER. Evaluated by any
        # session that OPTS IN (max_daily_loss_pct / amount set); it sums the
        # user's aggregate realised+unrealised P&L across ALL their live sessions
        # (of this mode) and, on a breach, FLATTENS them all in one sweep. A fresh
        # default config (both None) NEVER reaches this block → byte-for-byte
        # unchanged. Cooldown-guarded so N sessions don't each re-fire. If it
        # fires, THIS session is being flattened too → return early.
        if (getattr(self.config, "max_daily_loss_pct", None) is not None
                or getattr(self.config, "max_daily_loss_amount", None) is not None):
            try:
                breaker = await risk_manager.maybe_fire_breaker(
                    self.user_id, self.mode)
            except Exception as _bk_e:  # never crash a tick on the breaker
                log.error("breaker check failed for %s: %s", self.session_id, _bk_e)
                breaker = None
            if breaker is not None:
                # CLUSTER 6 (e) — page the daily-loss breaker firing (live only).
                if not self.dry_run:
                    try:
                        from .monitoring import alert_monitor as _am6c
                        _am6c.page_breaker(self.session_id, breaker, True)
                    except Exception as _al6c_e:  # noqa: BLE001 — never block tick
                        log.warning("cluster6 breaker page failed for %s: %s",
                                    self.session_id, _al6c_e)
                return {"gross_return": self.monitor.compute_gross_return_invested(),
                        "gross_return_fund": self.monitor.compute_gross_return(),
                        "kill_switch_fired": True,
                        "kill_reason": "PORTFOLIO_DAILY_LOSS_BREAKER",
                        "fire_result": None, "gtt_closed": gtt_closed,
                        "broker_reconciled": broker_reconciled,
                        "portfolio_breaker": breaker}

        snap = self.monitor.snapshot()
        # KILL BASIS: both strategies measure the INVESTED-basis gross return
        # (÷ frozen invested_basis), not the on-fund gross. snapshot() keeps the
        # on-fund gross_return for the history/charts.
        gr_invested = self.monitor.compute_gross_return_invested()

        if self.config.strategy == "tesla_short":
            # TESLA SHORT ROTATION: per-seat step-lock + mandatory MIS square-off
            # + seat back-fill, all on the ALLOCATED-CAPITAL basis (÷ total, a
            # FIXED denominator — correct for a rotating book). Existing strategies
            # are untouched by this branch.
            gr_capital = self.monitor.compute_gross_return()
            return await self._tick_tesla(gr_capital, snap, gtt_closed,
                                          broker_reconciled)

        if self.config.strategy == "intraday_basket":
            # CAPITAL-BASIS TRAIL (2026-07-07): the intraday_basket trail measures
            # arm/floor/giveback/basket-stop as % of ALLOCATED CAPITAL
            # (compute_gross_return = (uPnL+realised)/total_allocated_capital), NOT
            # the notional/invested basis. On a leveraged product (MIS/MTF/FUT/CE/PE)
            # invested_basis >> deployed capital, so a notional-basis arm fired at
            # leverage× the intended % of the trader's money. For a 1x CNC basket
            # capital ≈ invested_basis so this is a no-op. The KILL SWITCH
            # (portfolio_kill_switch strategy) and the per-stock software stop stay
            # on their own bases — unchanged.
            gr_capital = self.monitor.compute_gross_return()
            return await self._tick_intraday(gr_capital, snap, gtt_closed,
                                             broker_reconciled)

        # DEFAULT strategy: portfolio_kill_switch (UNCHANGED).
        reason = (self.kill_switch.check_threshold(gr_invested)
                  if self.kill_switch else None)
        # CLUSTER 5 ITEM 2 — MARK-STALENESS ABSTAIN. A PROFIT_TARGET must NOT fire
        # on a stale mark (a daily-close fallback masquerading as an intraday
        # profit). The LOSS side still fires conservatively. Flagged + logged.
        mark_stale = False
        if reason and str(reason).startswith("PROFIT_TARGET"):
            mark_stale = _marks_stale_for_profit(
                self.registry.get_open_positions(),
                getattr(self.config, "mark_staleness_abstain_sec", 30))
            if mark_stale:
                log.warning("session %s: ABSTAIN profit kill — marks stale "
                            "(>%ss); holding (downside stop still armed)",
                            self.session_id,
                            getattr(self.config, "mark_staleness_abstain_sec", 30))
                reason = None
        # FEATURE A — MIS DEFENSIVE SQUARE-OFF TICK BACKSTOP. A MIS session must be
        # flattened BEFORE the broker's ~15:20 auto-square even on the kill-switch
        # strategy. The precise-time square_off_scheduler is the primary path; this
        # backstop fires if that in-memory timer was dropped (e.g. restart). Only
        # applies to MIS sessions; CNC/MTF/NRML are UNCHANGED. Single-fire-guarded
        # (shared with the kill switch + scheduler) so it can never double-fire.
        mis_square_off = False
        if reason is None and self.kill_switch and self.config.is_intraday_product():
            try:
                mis_t = _parse_entry_time_today_ist(self.config.mis_square_off_time)
                if datetime.now(IST) >= mis_t:
                    reason = "MIS_SQUARE_OFF (tick backstop)"
                    mis_square_off = True
            except ValueError:  # pragma: no cover - validate() rejects unparseable
                pass
        fired = None
        if reason:
            # Single-fire guard: the 5s poll and the sub-second WS path must
            # never double-fire. Whoever wins the per-session lock fires.
            with fire_guard.claim_fire(self.session_id) as won:
                if won:
                    fired = await self.kill_switch.fire(
                        reason, gross_return=gr_invested,
                        close_reason=("MIS_SQUARE_OFF" if mis_square_off
                                      else "KILL_SWITCH"))
                else:
                    reason = None  # another path already fired/is firing
        return {"gross_return": gr_invested, "gross_return_fund": snap["gross_return"],
                "snapshot": snap, "kill_switch_fired": bool(fired),
                "kill_reason": reason, "fire_result": fired,
                "gtt_closed": gtt_closed,
                "mark_stale_abstain": mark_stale,
                "broker_reconciled": broker_reconciled}

    async def _tick_intraday(self, gr_capital: float, snap: Dict[str, Any],
                             gtt_closed,
                             broker_reconciled: Optional[List[Dict[str, Any]]] = None
                             ) -> Dict[str, Any]:
        """One tick for strategy=="intraday_basket": run the pure trail engine
        over the ALLOCATED-CAPITAL gross return + persisted (armed, peak) state.

        gr_capital = compute_gross_return() = (uPnL+realised)/total_allocated_capital
        (2026-07-07: switched from the notional/invested basis so arm/floor/
        giveback/basket-stop are "% of deployed capital", leverage-correct). The
        returned/logged gross_return is this same capital-basis number the engine
        decided on; gross_return_fund is retained for the on-fund snapshot view.

        The engine DECIDES only; on EXIT we REUSE the existing flatten
        (kill_switch.fire) passing the trail reason through as close_reason. State
        changes (arm / peak ratchet) are persisted on autotrade_sessions so a
        restart resumes the trail mid-day. Square-off is enforced defensively here
        even if the timer thread was dropped by a restart.

        PER-STOCK SOFTWARE STOP: before running the portfolio trail engine, each
        open position is checked against config.stop_pct (fraction; DEFAULT 0.03 =
        3%). If a single stock has fallen more than stop_pct from its entry, OUR
        backend exits just that position (cancel its GTT first, then market sell).
        The broker-held per-position GTT (config.per_position_stop_pct, DEFAULT
        0.08 = 8%) remains the wider backup; our software stop fires earlier.
        After per-stock exits the trail engine runs on the remaining positions."""
        from .monitoring import trail_engine

        broker_reconciled = broker_reconciled or []

        # ── MULTI-SESSION MAX-HOLD CAP (positional) ───────────────────────────
        # A positional basket (square_off_enabled=False, max_hold_sessions>0) is
        # squared off at square_off_time on the Nth trading session — regardless
        # of trail arm/peak state. This is the durable, restart-safe enforcement:
        # the cap datetime is recomputed from the PERSISTED started_at every tick,
        # so a restart re-derives it identically (no in-memory timer). Takes
        # precedence over the per-stock stop + trail engine. Single-fire-guarded
        # so it can never double-fire with a trail exit / manual kill. INERT when
        # the cap is 0, when intraday (the daily square-off fires first), or before
        # the cap moment. Uses the SAME flatten path (kill_switch.fire) as every
        # other basket exit → same GTT-cancel-before-exit + fill-confirm guarantees.
        try:
            if (int(getattr(self.config, "max_hold_sessions", 0)) > 0
                    and not getattr(self.config, "square_off_enabled", True)):
                cap_dt = compute_max_hold_cap_datetime(
                    self._started_at(),
                    int(self.config.max_hold_sessions),
                    self.config.square_off_time)
                if cap_dt is not None and now_ist() >= cap_dt:
                    with fire_guard.claim_fire(self.session_id) as won:
                        if won:
                            log.warning(
                                "MAX_HOLD_EXIT %s: cap %s reached (sessions=%d) "
                                "— flattening basket regardless of trail state",
                                self.session_id, cap_dt.isoformat(),
                                self.config.max_hold_sessions)
                            fired = await self.kill_switch.fire(
                                f"MAX_HOLD_EXIT max_hold_sessions="
                                f"{self.config.max_hold_sessions} "
                                f"gross_return={gr_capital:.4f}",
                                gross_return=gr_capital,
                                close_reason="MAX_HOLD_EXIT")
                            return {"gross_return": gr_capital,
                                    "gross_return_fund": snap["gross_return"],
                                    "snapshot": snap,
                                    "strategy": "intraday_basket",
                                    "trail_action": "EXIT",
                                    "kill_switch_fired": bool(fired),
                                    "kill_reason": "MAX_HOLD_EXIT",
                                    "fire_result": fired,
                                    "gtt_closed": gtt_closed,
                                    "broker_reconciled": broker_reconciled,
                                    "per_stock_exits": []}
                        # Another path is firing this same tick — let it win.
        except Exception as e:  # never block the tick on the max-hold check
            log.error("max-hold cap check failed for %s: %s", self.session_id, e)

        # PER-STOCK SOFTWARE STOP LOOP.
        # Runs BEFORE the portfolio-level trail engine so the trail sees the
        # updated (smaller) basket on this same tick.
        _now_ist_intraday = datetime.now(IST)
        _in_market_hours = (
            _now_ist_intraday.replace(hour=9, minute=15, second=0, microsecond=0)
            <= _now_ist_intraday <=
            _now_ist_intraday.replace(hour=15, minute=29, second=0, microsecond=0)
        )
        per_stock_exits: List[Dict[str, Any]] = []
        # Layer A — per-stock software stop. OFF by default: the validated config is
        # BASKET-ONLY (config.per_stock_stop_enabled=False). Across 530 days a
        # per-stock stop whipsawed (cut a name at its stop that then recovered inside
        # the basket), reducing return at every level — so we skip it entirely unless
        # explicitly enabled for the (worse-returning) two-layer variant.
        if getattr(self.config, "per_stock_stop_enabled", False):
            try:
                stop_pct = float(getattr(self.config, "stop_pct", 0.015))
                open_positions = self.monitor._open_positions()
                for pos in open_positions:
                    ltp = pos.get("ltp")
                    avg_price = float(pos.get("avg_price") or 0)
                    if ltp is None or avg_price <= 0:
                        continue
                    stock_return = (float(ltp) - avg_price) / avg_price
                    if stock_return <= -stop_pct and _in_market_hours:
                        result = await _exit_single_position(
                            session_id=self.session_id,
                            position=pos,
                            reason="STOP_STOCK",
                            brokers=self.brokers,
                            registry=self.registry,
                            gtt_manager=self.gtt_manager,
                            kite_product=self.config.order_product,
                            exec_cfg=self.config,
                        )
                        per_stock_exits.append(result)
                        log.warning(
                            "per-stock stop TRIGGERED %s/%s: return=%.4f <= -%.4f",
                            self.session_id, pos["symbol"], stock_return, stop_pct)
            except Exception as e:  # never block the tick on per-stock stop errors
                log.error("per-stock stop loop error for %s: %s", self.session_id, e)

        # ── STEP-LOCK SCOPE == "stock": PER-POSITION step-locking ─────────────
        # The SAME ladder + give-back run per stock (each on its own capital-slice
        # return g_stock), exiting individual stocks independently. The BASKET-
        # level safety nets still flatten ALL positions: the time SQUARE_OFF and
        # the catastrophic basket hard stop (basket G <= -stop_pct). Only the
        # PROFIT trail (arm/ratchet/trail-exit) moves per-stock. The basket path
        # below (scope=="basket", default) is left byte-for-byte unchanged.
        if getattr(self.config, "step_lock_scope", "basket") == "stock":
            params = trail_engine.params_from_config(self.config)
            safety_reason = self._basket_safety_decision(gr_capital, params)
            if safety_reason is not None:
                fired = None
                with fire_guard.claim_fire(self.session_id) as won:
                    if won:
                        fired = await self.kill_switch.fire(
                            f"INTRADAY_BASKET {safety_reason} "
                            f"gross_return={gr_capital:.4f}",
                            gross_return=gr_capital, close_reason=safety_reason)
                    else:
                        safety_reason = None  # another path already fired
                return {"gross_return": gr_capital,
                        "gross_return_fund": snap["gross_return"],
                        "snapshot": snap, "strategy": "intraday_basket",
                        "step_lock_scope": "stock",
                        "trail_action": "EXIT" if fired else "HOLD",
                        "kill_switch_fired": bool(fired),
                        "kill_reason": safety_reason, "fire_result": fired,
                        "gtt_closed": gtt_closed,
                        "broker_reconciled": broker_reconciled,
                        "per_stock_exits": per_stock_exits}
            # No basket safety-net trip → run the per-stock profit trail. It exits
            # only the individual position(s) whose own ratchet/give-back tripped.
            step_exits = await self._run_per_stock_step_lock(params)
            per_stock_exits.extend(step_exits)
            return {"gross_return": gr_capital,
                    "gross_return_fund": snap["gross_return"],
                    "snapshot": snap, "strategy": "intraday_basket",
                    "step_lock_scope": "stock",
                    "trail_action": "EXIT" if step_exits else "HOLD",
                    "kill_switch_fired": False,
                    "kill_reason": None, "fire_result": None,
                    "gtt_closed": gtt_closed,
                    "broker_reconciled": broker_reconciled,
                    "per_stock_exits": per_stock_exits}

        state = self.monitor.load_trail_state()
        params = trail_engine.params_from_config(self.config)
        decision = trail_engine.decide(gr_capital, state, params)

        # CLUSTER 5 ITEM 2 — MARK-STALENESS ABSTAIN. A PROFIT-side move (ARM/peak
        # ratchet or a TRAIL/FLOOR/STEP_LOCK exit) must NOT act on a stale mark: a
        # daily-close fallback could fake a high peak or a give-back exit. When the
        # marks are stale we HOLD the profit trail AND do NOT persist a stale-driven
        # ratchet. The downside STOP and the time SQUARE_OFF are NOT profit-side and
        # still fire (conservative). bound=0 disables → byte-for-byte unchanged.
        mark_stale = _marks_stale_for_profit(
            self.registry.get_open_positions(),  # SELECT * includes ltp_as_of
            getattr(self.config, "mark_staleness_abstain_sec", 30))
        _profit_side = (decision.action == "ARM") or (
            decision.action == "EXIT" and decision.reason in _PROFIT_EXIT_REASONS)
        fired = None
        reason = None
        action = decision.action
        if mark_stale and _profit_side:
            log.warning("session %s: ABSTAIN intraday %s (%s) — marks stale "
                        "(>%ss); holding, not ratcheting (STOP/SQUARE_OFF active)",
                        self.session_id, decision.action, decision.reason,
                        getattr(self.config, "mark_staleness_abstain_sec", 30))
            action = "HOLD"  # do NOT persist a stale-driven ratchet / exit
        else:
            # Persist any state change (arm transition or peak ratchet) so the
            # trail is durable across restarts BEFORE any exit fires.
            if decision.state_changed:
                self.monitor.save_trail_state(decision.state)
            if decision.action == "EXIT":
                reason = decision.reason
                with fire_guard.claim_fire(self.session_id) as won:
                    if won:
                        fired = await self.kill_switch.fire(
                            f"INTRADAY_BASKET {reason} "
                            f"gross_return={gr_capital:.4f}",
                            gross_return=gr_capital, close_reason=reason)
                    else:
                        reason = None  # another path already fired/is firing
        return {"gross_return": gr_capital,
                "gross_return_fund": snap["gross_return"],
                "snapshot": snap,
                "strategy": "intraday_basket",
                "mark_stale_abstain": bool(mark_stale and _profit_side),
                "trail_action": action,
                "trail_armed": decision.state.armed,
                "trail_peak": decision.state.peak,
                "trail_trigger": decision.trigger,
                "kill_switch_fired": bool(fired),
                "kill_reason": reason, "fire_result": fired,
                "gtt_closed": gtt_closed,
                "broker_reconciled": broker_reconciled,
                "per_stock_exits": per_stock_exits}

    # ── PER-STOCK step-lock (step_lock_scope == "stock") ──────────────────────
    def _basket_safety_decision(self, gr_capital: float, params) -> Optional[str]:
        """In step_lock_scope=="stock", evaluate ONLY the basket-level safety nets
        via the pure trail engine: the time SQUARE_OFF and the catastrophic
        downside STOP (basket G <= -stop_pct). Returns the reason ("SQUARE_OFF" |
        "STOP") when the WHOLE basket must flatten, else None. The engine's PROFIT
        trail (arm/ratchet/give-back) is intentionally IGNORED here — that is run
        per-stock. A fresh TrailState is used (SQUARE_OFF/STOP do not depend on
        armed/peak) so the basket trail state is neither ratcheted nor persisted
        in stock mode."""
        decision = trail_engine.decide(
            gr_capital, trail_engine.TrailState(), params)
        if decision.action == "EXIT" and decision.reason in ("SQUARE_OFF", "STOP"):
            return decision.reason
        return None

    async def _run_per_stock_step_lock(self, params) -> List[Dict[str, Any]]:
        """PER-STOCK STEP-LOCK profit trail (config.step_lock_scope=="stock").

        Runs the SAME ladder + give-back PER POSITION on each stock's slice-
        relative return and exits individual stocks independently. The basket
        safety nets (time SQUARE_OFF + catastrophic -stop_pct) are the CALLER's
        responsibility and are NOT re-evaluated here.

        g_stock = position_uPnL / capital_slice, where
            capital_slice = total_allocated_capital
                            * (position_notional / Σ position_notional over OPEN),
            position_notional = qty * avg_price.
        This makes each stock's % directly comparable to the BASKET's %-of-capital
        (leverage cancels in the proportion; equal-weight → total_capital / N), so
        an A/B basket-vs-stock test is fair.

        Each position loads/persists its OWN (pos_trail_armed, pos_trail_peak) and,
        on a profit EXIT (STEP_LOCK_EXIT / TRAIL_EXIT), ONLY that position is
        flattened via _exit_single_position; the others keep running. Returns the
        list of per-position exit result dicts."""
        from dataclasses import replace as _dc_replace
        positions = self.monitor._open_positions()
        if not positions:
            return []
        # FROZEN DENOMINATOR (Fix A, 2026-07-10). The per-stock slice weight is
        # leg_notional / BASKET_notional. Using Σ notional over CURRENTLY-OPEN
        # positions INFLATED survivors' slices as siblings closed (the denominator
        # shrank), so each name's "per_stock_stop_pct of its slice" fired at a
        # progressively larger rupee loss — the last name standing got a slice of
        # the whole session capital (GRANULES stopped at 2.2x, BRIGADE/others ~2x,
        # only AFTER others exited; 2026-07-10). Prefer the entry-frozen basket
        # notional (captured once in _fire_entries, never shrinks). Fall back to the
        # live Σ-over-OPEN ONLY for a legacy session that never froze it (identical
        # to the old behaviour — no regression). Leverage still cancels in the
        # proportion (equal-weight → total_cap/N).
        live_total_notional = 0.0
        for p in positions:
            live_total_notional += float(p.get("qty") or 0) * float(p.get("avg_price") or 0)
        frozen_notional = self.monitor.entry_basket_notional()
        total_notional = frozen_notional if (frozen_notional and frozen_notional > 0) \
            else live_total_notional
        if total_notional <= 0:
            return []
        total_cap = float(self.config.total_allocated_capital)
        # Per-stock params: the SAME ladder / give-back / arm-at-first-rung /
        # large-tier knobs, the time SQUARE_OFF suppressed (basket-level), and the
        # engine's STOP branch REPURPOSED as the PER-STOCK CAPITAL STOP. Because
        # g_stock is on the stock's own capital slice, feeding per_stock_stop_pct as
        # stop_pct makes the engine exit a name (reason STOP → relabelled STOP_STOCK)
        # the instant g_stock <= -per_stock_stop_pct — i.e. down that % of the money
        # deployed on THAT stock (leverage-correct, same basis as arm/give/ladder).
        # (2026-07-09: this is the previously-missing per-name stop; before, a name
        # drifting down had only the basket-AGGREGATE stop.) 0.0 → keep it suppressed
        # (unreachable 10.0) so the aggregate basket stop remains the only downside.
        _ps_stop = float(getattr(self.config, "per_stock_stop_pct", 0.03) or 0.0)
        ps_params = _dc_replace(params, square_off_enabled=False,
                                stop_pct=(_ps_stop if _ps_stop > 0 else 10.0))

        exits: List[Dict[str, Any]] = []
        for p in positions:
            ltp = p.get("ltp")
            avg = float(p.get("avg_price") or 0.0)
            qty = float(p.get("qty") or 0.0)
            if ltp is None or avg <= 0 or qty <= 0:
                continue
            notional = qty * avg
            slice_cap = total_cap * (notional / total_notional)
            if slice_cap <= 0:
                continue
            sign = -1.0 if str(p.get("direction") or "long").lower() == "short" \
                else 1.0
            upnl = sign * (float(ltp) - avg) * qty
            g_stock = upnl / slice_cap
            state = trail_engine.TrailState(
                armed=bool(p.get("pos_trail_armed")),
                peak=float(p.get("pos_trail_peak") or 0.0))
            decision = trail_engine.decide(g_stock, state, ps_params)
            if decision.state_changed:
                self.monitor.save_per_stock_trail_state(p["symbol"], decision.state)
            if decision.action == "EXIT":
                # The engine emits "STOP" for the downside hard stop; in per-stock
                # scope that IS the per-stock CAPITAL stop → relabel for clarity so
                # the close_reason/journal reads STOP_STOCK, not the basket "STOP".
                reason = "STOP_STOCK" if decision.reason == "STOP" else decision.reason
                result = await _exit_single_position(
                    session_id=self.session_id, position=p,
                    reason=reason, brokers=self.brokers,
                    registry=self.registry, gtt_manager=self.gtt_manager,
                    kite_product=self.config.order_product, exec_cfg=self.config)
                result["reason"] = reason
                result["g_stock"] = g_stock
                exits.append(result)
                log.warning(
                    "PER-STOCK EXIT %s/%s: g_stock=%.4f reason=%s",
                    self.session_id, p["symbol"], g_stock, reason)
        return exits

    # ── TESLA SHORT ROTATION (strategy=="tesla_short") ────────────────────────
    # Order-flow-native intraday SHORT capital-rotation engine. Entries + exits go
    # through the SAME hardened paths as every other strategy (_place_one /
    # _exit_single_position / kill_switch.fire / fire_guard / reconciler). The ONLY
    # new logic is (a) the live signal source, (b) the seat back-fill decision
    # (strategies/tesla_rotation.py), and (c) the per-SEAT stop denominator (the
    # FIXED seat allocation = total/n_seats — NOT the frozen basket notional, which
    # would mis-scale as seats turn over). Paper (dry_run) simulates fills; live is
    # gated by _live_allowed() exactly like every other order.

    def _tesla_live_signals(self) -> List[Any]:
        """Fetch the CURRENT A++/A+++ SHORT signals used to back-fill FREE seats.

        This reads a PROCESS CACHE that a background refresher updates AT MOST
        once per 1-min bar (tesla_signal_cache) — it NEVER runs the multi-second
        full-universe recompute inline on the 5s tick (that would block the
        event loop and stall every session). It:
          1. triggers a once-per-minute background refresh (returns immediately),
          2. reads the last cached signals (a few ms),
          3. ABSTAINS (returns []) when the cache is STALE (> the staleness
             bound) so NO new seat is opened on stale data — and pages (live,
             market hours). Exits/square-off do NOT call this, so they are
             unaffected by a stale signal cache.
        Never crashes a tick (returns [] on any error). Overridable in tests
        (monkeypatch) so the rotation path is exercised without the DB."""
        try:
            from .strategies import tesla_signal_cache as _cache
            db = getattr(self.config, "tesla_signal_db_path", None)
            pwd = int(getattr(self.config, "tesla_personality_window_days", 5))
            mg = getattr(self.config, "tesla_min_grade", "A++")
            cd = int(getattr(self.config, "tesla_cooldown_minutes", 30))
            bound = int(getattr(self.config, "tesla_signal_staleness_sec", 90))
            did = bool(getattr(self.config, "tesla_did_layer_enabled", False))
            # DEFAULT-OFF sub-10s vectorised feature path (byte-identical to the
            # committed loop; proven by the full-day parity test). Loop stays the
            # default until the operator flips tesla_vectorized_features=true.
            vec = bool(getattr(self.config, "tesla_vectorized_features", False))
            # DEFAULT-OFF incremental infer-day read (round-2). Byte-identical to a
            # full re-read (the poller's bars are immutable); only takes effect on
            # the vectorised path, matching tesla_incremental_read's contract.
            inc = vec and bool(getattr(self.config, "tesla_incremental_read", False))
            now = datetime.now(IST)
            # 1. trigger (non-blocking) — recompute runs in a background thread.
            _cache.refresh_if_needed(
                db_path=db, personality_window_days=pwd, min_grade=mg,
                cooldown_minutes=cd, did_layer_enabled=did, vectorized=vec,
                incremental=inc, now=now, block=False)
            # 2. read the cached signals + staleness.
            signals, stale = _cache.get_signals(
                db_path=db, personality_window_days=pwd, min_grade=mg,
                did_layer_enabled=did, vectorized=vec, incremental=inc, now=now,
                staleness_bound_sec=bound)
            if stale:
                # 3. abstain from NEW entries + page (live, market hours only).
                self._page_tesla_signal_stale()
                return []
            return list(signals)
        except Exception as e:  # pragma: no cover - defensive (never crash a tick)
            log.warning("tesla signal engine failed for %s: %s",
                        self.session_id, e)
            return []

    def _page_tesla_signal_stale(self) -> None:
        """Page (live, market-hours, deduped) that the tesla signal cache is
        stale → new seat entries are being suppressed. Best-effort; never raises."""
        try:
            if self.dry_run:
                return
            from .monitoring import alert_monitor as _am
            now = datetime.now(IST)
            try:
                in_hours = trading_calendar.is_market_open(now)
            except Exception:  # noqa: BLE001
                in_hours = True
            _am.page_signal_stale(
                self.session_id, age_seconds=None, in_market_hours=in_hours,
                is_live=True,
                bound_sec=int(getattr(self.config, "tesla_signal_staleness_sec", 90)))
        except Exception as e:  # noqa: BLE001 — never block the tick on a page
            log.warning("tesla signal-stale page failed for %s: %s",
                        self.session_id, e)

    def _tesla_primary_broker(self):
        """(broker, profile) for the FIRST enabled profile with a built client.
        None → cannot place (session not built / no enabled profile)."""
        for prof in (self.config.broker_profiles or []):
            if not getattr(prof, "enabled", True):
                continue
            broker = (self.brokers or {}).get(prof.profile_id)
            if broker is not None:
                return broker, prof
        return None, None

    async def _rotate_tesla_seats(self) -> List[Dict[str, Any]]:
        """Fill every FREE seat from the current live signals (used at fire AND on
        every tick). Reuses _place_one for each entry (all entry safety intact).
        Returns the list of per-entry order-result dicts (may be empty)."""
        from .strategies import tesla_rotation as _trot
        open_pos = self.registry.get_open_positions()
        open_symbols = [p["symbol"] for p in open_pos]
        n_seats = int(getattr(self.config, "n_seats", 3))
        if len(open_symbols) >= n_seats:
            return []
        signals = self._tesla_live_signals()
        if not signals:
            return []
        all_pos = self.registry.get_all_positions()
        held_ever = [p["symbol"] for p in all_pos]
        last_entry_at: Dict[str, datetime] = {}
        for p in all_pos:
            ts = p.get("entered_at") or p.get("created_at") or p.get("entry_date")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(str(ts))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=IST)
                prev = last_entry_at.get(p["symbol"])
                if prev is None or dt > prev:
                    last_entry_at[p["symbol"]] = dt
            except (ValueError, TypeError):
                pass
        plan = _trot.plan_backfill(
            signals=signals, open_symbols=open_symbols, n_seats=n_seats,
            total_capital=float(self.config.total_allocated_capital),
            now=datetime.now(IST), last_entry_at=last_entry_at,
            held_ever=held_ever,
            cooldown_minutes=int(getattr(self.config, "tesla_cooldown_minutes", 30)),
            min_grade=getattr(self.config, "tesla_min_grade", "A++"),
            allow_reentry=bool(getattr(self.config, "tesla_allow_reentry", False)))
        if not plan:
            return []
        broker, prof = self._tesla_primary_broker()
        if broker is None:
            log.warning("session %s: tesla rotation has no built broker profile",
                        self.session_id)
            return []
        allocator = CapitalAllocator(self.config)
        results: List[Dict[str, Any]] = []
        for entry in plan:
            pick = Pick(symbol=entry.symbol, rank=0, score=entry.short_drive,
                        sector=entry.setup or None,
                        close_at_signal=entry.ref_price)
            try:
                res = await self._place_one(broker, prof, pick, entry.allocation,
                                            allocator)
            except Exception as e:  # per-seat isolation — one bad seat never aborts
                log.error("tesla seat entry crashed for %s: %s", entry.symbol, e)
                res = {"symbol": entry.symbol, "status": "FAILED", "error": str(e)}
            res["seat_grade"] = entry.grade
            res["seat_allocation"] = entry.allocation
            results.append(res)
        # Keep the display invested_basis current with the live book (the trail
        # decides on total_allocated_capital, not this — display only).
        try:
            self.monitor.freeze_invested_basis()
        except Exception:  # pragma: no cover - never block on the basis capture
            pass
        return results

    async def _run_tesla_seat_step_lock(self, params,
                                        mark_stale: bool = False
                                        ) -> List[Dict[str, Any]]:
        """PER-SEAT step-lock profit trail + per-seat capital stop.

        g_seat = position_uPnL / seat_allocation, where
            seat_allocation = total_allocated_capital / n_seats (FIXED).
        This is the CORRECT rotation denominator — it does NOT shrink as sibling
        seats turn over (the frozen-basket-notional slice used by the basket path
        would). Each seat carries its OWN (pos_trail_armed, pos_trail_peak). A
        profit EXIT (STEP_LOCK/TRAIL/FLOOR) is SUPPRESSED when the mark is stale
        (a daily-close fallback must not fake a give-back); the per-seat downside
        STOP (STOP_SEAT) still fires. Returns the per-seat exit result dicts."""
        from dataclasses import replace as _dc_replace
        positions = self.monitor._open_positions()
        if not positions:
            return []
        seat_alloc = float(self.config.total_allocated_capital) / int(
            getattr(self.config, "n_seats", 3))
        if seat_alloc <= 0:
            return []
        _ps_stop = float(getattr(self.config, "per_stock_stop_pct", 0.03) or 0.0)
        ps_params = _dc_replace(params, square_off_enabled=False,
                                stop_pct=(_ps_stop if _ps_stop > 0 else 10.0))
        exits: List[Dict[str, Any]] = []
        for p in positions:
            ltp = p.get("ltp")
            avg = float(p.get("avg_price") or 0.0)
            qty = float(p.get("qty") or 0.0)
            if ltp is None or avg <= 0 or qty <= 0:
                continue
            sign = -1.0 if str(p.get("direction") or "long").lower() == "short" \
                else 1.0
            upnl = sign * (float(ltp) - avg) * qty
            g_seat = upnl / seat_alloc   # FIXED seat allocation (see tesla_rotation.seat_g)
            state = trail_engine.TrailState(
                armed=bool(p.get("pos_trail_armed")),
                peak=float(p.get("pos_trail_peak") or 0.0))
            decision = trail_engine.decide(g_seat, state, ps_params)
            _profit = (decision.action == "ARM") or (
                decision.action == "EXIT"
                and decision.reason in _PROFIT_EXIT_REASONS)
            if mark_stale and _profit:
                # do NOT ratchet or profit-exit on a stale mark; STOP still armed.
                continue
            if decision.state_changed:
                self.monitor.save_per_stock_trail_state(p["symbol"], decision.state)
            if decision.action == "EXIT":
                reason = "STOP_SEAT" if decision.reason == "STOP" else decision.reason
                result = await _exit_single_position(
                    session_id=self.session_id, position=p, reason=reason,
                    brokers=self.brokers, registry=self.registry,
                    gtt_manager=self.gtt_manager,
                    kite_product=self.config.order_product, exec_cfg=self.config)
                result["reason"] = reason
                result["g_seat"] = g_seat
                exits.append(result)
                log.warning("TESLA SEAT EXIT %s/%s: g_seat=%.4f reason=%s",
                            self.session_id, p["symbol"], g_seat, reason)
        return exits

    async def _tick_tesla(self, gr_capital: float, snap: Dict[str, Any],
                          gtt_closed,
                          broker_reconciled: Optional[List[Dict[str, Any]]] = None
                          ) -> Dict[str, Any]:
        """One tick for strategy=="tesla_short":
          1. BASKET SAFETY (flatten ALL): the catastrophic basket stop
             (basket G <= -stop_pct) + the MANDATORY MIS ~15:12 buy-to-cover
             square-off. Fires through kill_switch.fire (GTT-cancel-before-exit +
             fill-confirm), single-fire-guarded.
          2. PER-SEAT step-lock exits (seat denominator), mark-stale-guarded.
          3. BACK-FILL free seats from the current signals via _place_one.
        Downside/mandatory exits ALWAYS proceed; profit exits + new entries abstain
        on stale marks / are guarded, matching the intraday engine."""
        from .monitoring import trail_engine as _te
        broker_reconciled = broker_reconciled or []
        params = _te.params_from_config(self.config)

        # 1. Basket safety net (catastrophic stop) + MANDATORY MIS square-off.
        safety_reason = self._basket_safety_decision(gr_capital, params)
        if safety_reason is None and self.config.is_intraday_product():
            try:
                mis_t = _parse_entry_time_today_ist(self.config.mis_square_off_time)
                if datetime.now(IST) >= mis_t:
                    safety_reason = "MIS_SQUARE_OFF"
            except ValueError:  # pragma: no cover - validate() rejects unparseable
                pass
        if safety_reason is not None:
            fired = None
            with fire_guard.claim_fire(self.session_id) as won:
                if won:
                    fired = await self.kill_switch.fire(
                        f"TESLA_SHORT {safety_reason} gross_return={gr_capital:.4f}",
                        gross_return=gr_capital, close_reason=safety_reason)
                else:
                    safety_reason = None
            return {"gross_return": gr_capital,
                    "gross_return_fund": snap["gross_return"], "snapshot": snap,
                    "strategy": "tesla_short",
                    "trail_action": "EXIT" if fired else "HOLD",
                    "kill_switch_fired": bool(fired), "kill_reason": safety_reason,
                    "fire_result": fired, "gtt_closed": gtt_closed,
                    "broker_reconciled": broker_reconciled}

        # 2. Per-seat step-lock (profit trail + per-seat capital stop).
        mark_stale = _marks_stale_for_profit(
            self.registry.get_open_positions(),
            getattr(self.config, "mark_staleness_abstain_sec", 30))
        seat_exits = await self._run_tesla_seat_step_lock(params, mark_stale=mark_stale)

        # 3. Back-fill free seats (never on a stale mark — don't enter on stale data).
        backfilled: List[Dict[str, Any]] = []
        if not mark_stale:
            try:
                backfilled = await self._rotate_tesla_seats()
            except Exception as e:  # never block the tick on a backfill error
                log.warning("tesla backfill failed for %s: %s", self.session_id, e)

        return {"gross_return": gr_capital,
                "gross_return_fund": snap["gross_return"], "snapshot": snap,
                "strategy": "tesla_short",
                "trail_action": "EXIT" if seat_exits else "HOLD",
                "seat_exits": seat_exits, "backfilled": backfilled,
                "mark_stale_abstain": bool(mark_stale),
                "kill_switch_fired": False, "kill_reason": None,
                "fire_result": None, "gtt_closed": gtt_closed,
                "broker_reconciled": broker_reconciled}

    async def _fire_tesla_initial(self) -> Dict[str, Any]:
        """Initial fill for a tesla_short session: fill whatever seats have a
        signal NOW, freeze the display basis, arm the drivers + the MANDATORY MIS
        square-off, and go RUNNING. Unlike the basket path this does NOT FAIL on 0
        fills — a tesla session ARMS and rotates in as signals appear (the regime
        gate may not have fired yet)."""
        placed: List[Dict[str, Any]] = []
        try:
            placed = await self._rotate_tesla_seats()
        except Exception as e:
            log.error("tesla initial fill failed for %s: %s", self.session_id, e)
        for _label, _starter in (("tick", tick_driver.start_for_session),
                                 ("ws", ws_driver.start_for_session)):
            try:
                _starter(self.session_id)
            except Exception as e:  # never block start on a driver
                log.warning("%s driver start failed for %s: %s", _label,
                            self.session_id, e)
        try:
            self._arm_square_off()
        except Exception as e:  # never block start on the square-off scheduler
            log.warning("square-off arm failed for %s: %s", self.session_id, e)
        n_ok = sum(1 for r in placed
                   if r.get("status") in ("PLACED", "DRY_RUN", "PARTIAL"))
        return {"session_id": self.session_id, "status": "RUNNING",
                "mode": self.mode, "n_placed": n_ok, "orders": placed,
                "strategy": "tesla_short",
                "note": ("tesla_short armed; seats rotate in on live signals"
                         if n_ok == 0 else "tesla_short seats filled; rotating")}

    # ── Manual kill ────────────────────────────────────────────────────────────
    async def kill(self, reason: str = "MANUAL") -> Dict[str, Any]:
        if not self.brokers:
            self._build_brokers()
        # Stop the entry scheduler FIRST so a SCHEDULED session that hasn't fired
        # yet can be cancelled/killed and place NOTHING. Then stop the tick
        # driver so it can't race the manual kill.
        try:
            entry_scheduler.stop_for_session(self.session_id)
        except Exception:  # pragma: no cover - defensive
            pass
        try:
            tick_driver.stop_for_session(self.session_id)
        except Exception:  # pragma: no cover - defensive
            pass
        try:
            ws_driver.stop_for_session(self.session_id)
        except Exception:  # pragma: no cover - defensive
            pass
        try:
            square_off_scheduler.stop_for_session(self.session_id)
        except Exception:  # pragma: no cover - defensive
            pass
        self.monitor.refresh_ltps(self.brokers)
        # Report the INVESTED-basis gross in the manual-kill reason (the kill
        # basis), matching the automatic path.
        gr = self.monitor.compute_gross_return_invested()
        # Single-fire guard so a manual kill can't double-fire with an in-flight
        # WS/poll fire on the same session.
        with fire_guard.claim_fire(self.session_id) as won:
            if not won:
                return {"session_id": self.session_id,
                        "trigger_reason": f"MANUAL {reason} (already firing)",
                        "n_positions": 0, "n_exited_ok": 0, "n_exit_failed": 0,
                        "details": [], "already_fired": True}
            return await self.kill_switch.fire(
                f"MANUAL {reason} gross_return={gr:.4f}", gross_return=gr)

    # ── Status ─────────────────────────────────────────────────────────────────
    def status(self) -> Dict[str, Any]:
        with falcon_conn() as con:
            row = con.execute(
                "SELECT * FROM autotrade_sessions WHERE session_id=?",
                (self.session_id,),
            ).fetchone()
        sess = dict(row) if row else {}
        status = sess.get("status")
        positions = self.registry.get_open_positions()
        # C3 — SURFACE EXIT_FAILED (still-held) legs. They are dropped from
        # open_positions (OPEN-only) and from the realised total (CLOSED-only), yet
        # invested_basis (frozen) still counts them — so without this the panel is
        # blind to real exposure. Their live uPnL is already in gross_return via
        # monitor._total_exit_failed_unrealised(). Each row is flagged explicitly.
        exit_failed = self.registry.get_exit_failed_all()
        for _p in exit_failed:
            _p["exit_failed"] = True
        # Two clearly-named gross returns. gross_return is the KILL BASIS
        # (÷ frozen invested_basis); gross_return_fund is the on-fund view.
        invested_basis = self.monitor.invested_basis()
        gr_invested = self.monitor.compute_gross_return_invested()
        gr_fund = self.monitor.compute_gross_return()
        # ITEM 5 — NET P&L estimate (computed once).
        _charges_est = estimate_session_charges_rs(self.session_id,
                                                   self.config.order_product)
        _gross_pnl_rs = (gr_invested * invested_basis) if invested_basis else 0.0
        out = {
            "session_id": self.session_id,
            "status": status,
            "strategy": getattr(self.config, "strategy", "intraday_basket"),
            "mode": sess.get("mode", self.mode),
            "gross_return": gr_invested,          # kill basis (÷ invested_basis)
            "gross_return_fund": gr_fund,         # on-fund (÷ allocated)
            "invested_basis": invested_basis,
            "total_allocated_capital": self.config.total_allocated_capital,
            # ITEM 3 — explicit risk_basis label + ₹ concentration/fat-finger caps.
            "risk_basis": getattr(self.config, "risk_basis", "notional"),
            "concentration_limits":
                risk_manager.concentration_thresholds_rs(self.config),
            # ITEM 5 — NET P&L (gross − estimated charges) surfaced alongside gross
            # so the user sees the REAL number. This is a DISPLAY figure only — the
            # kill/trail DECISION basis stays the GROSS invested-basis return.
            "gross_pnl": round(_gross_pnl_rs, 2),
            "estimated_charges": _charges_est,
            "net_pnl": round(_gross_pnl_rs - _charges_est, 2)
            if invested_basis else None,
            "net_return": round((_gross_pnl_rs - _charges_est) / invested_basis, 6)
            if invested_basis else None,
            "kill_switch_enabled": self.config.kill_switch_enabled,
            "kill_switch_pct": self.config.kill_switch_pct,
            "kill_switch_direction": self.config.kill_switch_direction,
            "kill_preview": compute_kill_preview(
                kill_switch_enabled=self.config.kill_switch_enabled,
                kill_switch_pct=self.config.kill_switch_pct,
                kill_switch_direction=self.config.kill_switch_direction,
                invested_basis=invested_basis,
                total_allocated_capital=self.config.total_allocated_capital,
                kill_switch_target_pct=self.config.kill_switch_target_pct,
                kill_switch_stop_pct=self.config.kill_switch_stop_pct),
            "n_open_positions": len(positions),
            "open_positions": positions,
            # C3: stranded EXIT_FAILED legs (still held; not in open_positions).
            "exit_failed_positions": exit_failed,
            "n_exit_failed_positions": len(exit_failed),
            "has_exit_failed": bool(exit_failed),
            # Lifecycle#8: mark freshness — the age (ms) of the OLDEST open-position
            # mark, so a stalled tick (stale marks) is visible. None when no marks.
            "oldest_mark_age_ms": _oldest_mark_age_ms(positions),
            # SPEED-PASS observability so the operator can SEE the latency.
            "entry_latency_ms": sess.get("entry_latency_ms"),
            "exit_latency_ms": sess.get("exit_latency_ms"),
            "last_tick_age_ms": _last_tick_age_ms(),
            # PHASE-2 MULTI-TENANT ownership/binding (NULL for operator sessions).
            "user_id": sess.get("user_id"),
            "broker_account_id": sess.get("broker_account_id"),
        }
        # EXECUTION-DATE / TRADING-DAY surface — ALWAYS expose the resolved fire
        # datetime + the trading-day verdict so the UI can show "Fires
        # <date> 09:15 (trading day)" and any deferred/expired reason.
        out["entry_time"] = self.config.entry_time
        out["entry_date"] = self.config.entry_date
        out["on_missed_window"] = self.config.on_missed_window
        _now = now_ist()
        try:
            resolved = self.config.resolve_fire_datetime(_now)
            out["resolved_fire_datetime"] = resolved.isoformat()
            out["resolved_fire_date"] = resolved.date().isoformat()
            out["is_trading_day"] = trading_calendar.is_trading_day(
                resolved.date())
        except Exception:  # pragma: no cover - never break status on calendar
            resolved = None
            out["resolved_fire_datetime"] = None
            out["is_trading_day"] = None
        out["market_open_now"] = trading_calendar.is_market_open(_now)
        # Surface the terminal/deferred reason (stored in notes).
        if status in (STATUS_REJECTED_NON_TRADING_DAY,
                      STATUS_EXPIRED_MISSED_WINDOW,
                      STATUS_DEFERRED_MARKET_CLOSED):
            out["deferred_reason"] = sess.get("notes")

        # SCHEDULED: surface the armed entry time so the UI can show
        # "Scheduled for 09:15" + a live countdown.
        if status == "SCHEDULED":
            target = entry_scheduler.target_for_session(self.session_id)
            if target is None:
                # In-memory timer lost (e.g. backend restarted) — derive the
                # resolved target from config so the UI still shows the time.
                target = resolved
                out["scheduler_armed"] = False
            else:
                out["scheduler_armed"] = entry_scheduler.is_running(self.session_id)
            if target is not None:
                out["fires_at"] = target.isoformat()
                out["seconds_remaining"] = int(
                    max(0.0, (target - datetime.now(IST)).total_seconds()))

        # Always surface the strategy so the UI can pick the right panel.
        out["strategy"] = self.config.strategy

        # LIVE CONFIG EDIT: surface the CURRENT whitelisted risk/exit knobs + the
        # config_version so the UI can pre-fill the "edit while running" form and
        # PATCH .../config with the version it saw. These are the ONLY fields the
        # PATCH endpoint accepts on a running session (capital/product/picks/entry
        # stay locked). Present for BOTH strategies (kill-switch sessions still
        # expose per_position_* + mis_square_off_time + square_off_time).
        out["config_version"] = int(sess.get("config_version") or 0)
        out["editable_config"] = {
            f: getattr(self.config, f, None)
            for f in LIVE_EDITABLE_SESSION_FIELDS
        }

        # INTRADAY BASKET: surface the full trailing-engine state + the per-day
        # dual-return report so the UI can render the trail status panel.
        if self.config.strategy == "intraday_basket":
            state = self.monitor.load_trail_state()
            params = trail_engine.params_from_config(self.config)
            trigger = trail_engine.compute_trigger(state, params)
            out["trail"] = {
                "armed": state.armed,
                "peak": state.peak,
                # CAPITAL-BASIS (2026-07-07): the trail keys on allocated-capital
                # G (compute_gross_return), so surface THAT here — arm/floor/
                # giveback/stop below are all % of deployed capital. (The top-level
                # gross_return stays the invested/kill basis.)
                "current_gross_return": gr_fund,          # allocated-capital G
                "trigger": trigger,                       # live exit-trigger G
                "arm_pct": self.config.arm_pct,
                "floor_pct": self.config.floor_pct,
                "trail_giveback_pct": self.config.trail_giveback_pct,
                "stop_pct": self.config.stop_pct,
                # PROFIT STEP-LOCK panel: the ladder + large-day tier + the CURRENT
                # locked floor (of the ratcheted peak). step_lock_floor is 0 until
                # the peak crosses the first rung. Inert display when disabled.
                "trail_step_lock_enabled": bool(
                    getattr(self.config, "trail_step_lock_enabled", False)),
                "trail_step_lock_ladder": getattr(
                    self.config, "trail_step_lock_ladder", []),
                "trail_large_peak_pct": getattr(
                    self.config, "trail_large_peak_pct", 0.20),
                "trail_large_giveback_rel": getattr(
                    self.config, "trail_large_giveback_rel", 0.175),
                "step_lock_floor": (
                    trail_engine.step_lock_floor(
                        state.peak, params.step_lock_ladder)
                    if (state.armed and params.step_lock_enabled
                        and params.step_lock_ladder) else None),
                "square_off_time": self.config.square_off_time,
                # INTRADAY (True) vs POSITIONAL (False). When False the basket
                # carries across days; seconds_to_square_off / square_off_armed
                # are inert (no forced flatten).
                "square_off_enabled": bool(
                    getattr(self.config, "square_off_enabled", True)),
                "trail_mode": ("intraday"
                               if getattr(self.config, "square_off_enabled", True)
                               else "positional"),
                "seconds_to_square_off": trail_engine.seconds_to_square_off(
                    self.config.square_off_time),
                "square_off_armed": square_off_scheduler.is_running(
                    self.session_id),
                # POSITIONAL max-hold cap: 0 = no cap. When set, expose the
                # resolved cap datetime (Nth trading session @ square_off_time,
                # computed from started_at) so the UI can show "Force-close on
                # <date>". None until the session has an entry timestamp.
                "max_hold_sessions": int(
                    getattr(self.config, "max_hold_sessions", 0)),
                "max_hold_cap_datetime": (
                    lambda dt: dt.isoformat() if dt else None)(
                    compute_max_hold_cap_datetime(
                        sess.get("started_at"),
                        int(getattr(self.config, "max_hold_sessions", 0)),
                        self.config.square_off_time)),
            }
            # Flat aliases (handy for the frontend + matches the spec wording).
            out["trail_armed"] = state.armed
            out["trail_peak"] = state.peak
            out["trail_trigger"] = trigger
            out["square_off_time"] = self.config.square_off_time
            out["seconds_to_square_off"] = out["trail"]["seconds_to_square_off"]

            # On close: the exit reason + the final dual return. close_reason is
            # stored on the (now CLOSED) position rows by kill_switch.fire;
            # own_funds_return = notional G × leverage = the on-fund gross.
            if status in ("CLOSED", "KILLING"):
                out["exit_reason"] = self._last_exit_reason()
                out["notional_return"] = gr_invested
                out["own_funds_return"] = gr_fund
        return out

    def _last_exit_reason(self) -> Optional[str]:
        """The close_reason written to this session's positions on flatten
        (TRAIL_EXIT / FLOOR_EXIT / STOP / SQUARE_OFF / KILL_SWITCH), or None."""
        with falcon_conn() as con:
            row = con.execute(
                """SELECT close_reason FROM autotrade_positions
                   WHERE session_id=? AND close_reason IS NOT NULL
                   ORDER BY closed_at DESC LIMIT 1""",
                (self.session_id,),
            ).fetchone()
        if row and row["close_reason"]:
            return row["close_reason"]
        # Fall back to the session-level kill_reason (e.g. nothing was open).
        with falcon_conn() as con:
            row = con.execute(
                "SELECT kill_reason FROM autotrade_sessions WHERE session_id=?",
                (self.session_id,),
            ).fetchone()
        return row["kill_reason"] if row and row["kill_reason"] else None

    def _started_at(self) -> Optional[str]:
        """The persisted ISO-IST entry timestamp (autotrade_sessions.started_at),
        set when entries fire (RUNNING transition). The DURABLE anchor for the
        multi-session max-hold cap — read fresh each call so the cap survives a
        restart. None until the session has fired."""
        with falcon_conn() as con:
            row = con.execute(
                "SELECT started_at FROM autotrade_sessions WHERE session_id=?",
                (self.session_id,),
            ).fetchone()
        return row["started_at"] if row and row["started_at"] else None

    def positions(self) -> List[Dict[str, Any]]:
        return self.registry.get_all_positions()

    # ── helpers ─────────────────────────────────────────────────────────────────
    def _record_latency(self, *, entry_latency_ms: Optional[int] = None,
                        exit_latency_ms: Optional[int] = None) -> None:
        """Persist the speed-pass observability fields on the session row
        (idempotent COALESCE-style UPDATE; only the provided field is written)."""
        with falcon_conn() as con:
            if entry_latency_ms is not None:
                con.execute(
                    "UPDATE autotrade_sessions SET entry_latency_ms=? "
                    "WHERE session_id=?", (int(entry_latency_ms), self.session_id))
            if exit_latency_ms is not None:
                con.execute(
                    "UPDATE autotrade_sessions SET exit_latency_ms=? "
                    "WHERE session_id=?", (int(exit_latency_ms), self.session_id))
            con.commit()

    def _current_status(self) -> Optional[str]:
        """The session's CURRENT persisted status (fresh read), or None."""
        with falcon_conn() as con:
            r = con.execute(
                "SELECT status FROM autotrade_sessions WHERE session_id=?",
                (self.session_id,)).fetchone()
        return r["status"] if r else None

    def _has_unflat_positions(self) -> bool:
        """True while ANY position is still OPEN or EXIT_FAILED (qty>0) — i.e. not
        yet flat. Used to decide when a KILLING_INCOMPLETE session may be promoted
        to CLOSED (C2). Counts a locked in-flight retry (exit_lock=1) too, so we
        never close while an exit is mid-flight."""
        with falcon_conn() as con:
            r = con.execute(
                "SELECT COUNT(*) AS n FROM autotrade_positions "
                "WHERE session_id=? AND status IN ('OPEN','EXIT_FAILED') "
                "AND qty>0", (self.session_id,)).fetchone()
        return bool(r and int(r["n"] or 0) > 0)

    def _set_status(self, status: str, started_at: Optional[str] = None,
                    reason: Optional[str] = None,
                    closed_at: Optional[str] = None) -> None:
        with falcon_conn() as con:
            con.execute(
                "UPDATE autotrade_sessions SET status=?, "
                "started_at=COALESCE(?, started_at), "
                "closed_at=COALESCE(?, closed_at), "
                "notes=COALESCE(?, notes) WHERE session_id=?",
                (status, started_at, closed_at, reason, self.session_id))
            con.commit()

    def _persist_entry_date(self, entry_date: str) -> None:
        """Roll the stored config_json's entry_date forward (carry policy) so a
        restart resumes from the carried date. Idempotent rewrite of config_json.
        """
        self.config.entry_date = entry_date
        with falcon_conn() as con:
            con.execute(
                "UPDATE autotrade_sessions SET config_json=? WHERE session_id=?",
                (self.config.to_json(), self.session_id))
            con.commit()
