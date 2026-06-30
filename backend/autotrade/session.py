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
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn

from .config import TradingSessionConfig
from . import trading_calendar
from .capital import CapitalAllocator, InsufficientCapitalError
from .broker.base import Pick
from .broker.router import BrokerRouter, build_client
from .execution.orders import build_order, place_order_with_retry
from .execution.slippage import record_slippage
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

def load_falcon_picks(top_n: int = 100) -> List[Pick]:
    """Read latest Falcon Top-N picks. NEVER writes to falcon_signals_live."""
    with falcon_conn() as con:
        latest = con.execute(
            "SELECT MAX(signal_date) FROM falcon_signals_live"
        ).fetchone()[0]
        if latest is None:
            return []
        rows = con.execute(
            """SELECT symbol, rank, score, sector, close_at_signal
               FROM falcon_signals_live WHERE signal_date=?
               ORDER BY rank ASC LIMIT ?""",
            (latest, top_n),
        ).fetchall()
    return [Pick(symbol=r["symbol"], rank=r["rank"],
                 score=r["score"] or 0.0, sector=r["sector"],
                 close_at_signal=r["close_at_signal"]) for r in rows]


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

    falcon_picks = load_falcon_picks(top_n=max(config.top_n_stocks, 10))
    if config.rank_filter:
        falcon_picks = [p for p in falcon_picks if p.rank in config.rank_filter]

    router = BrokerRouter(top_n_stocks=config.top_n_stocks)
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
        amounts = allocator.allocate([p.symbol for p in picks])
        # SPEED PASS: ONE batched LTP + MTF-margin prefetch for the preview too.
        fund_syms = [p.symbol for p in picks if amounts.get(p.symbol, 0.0) > 0]
        try:
            pcache = allocator.prefetch(fund_syms, broker)
        except Exception:  # pragma: no cover - per-symbol fallback inside
            pcache = {}
        for pick in picks:
            amount = amounts.get(pick.symbol, 0.0)
            if amount <= 0:
                continue
            try:
                qty = allocator.calculate_quantity_cached(
                    pick.symbol, amount, broker, cache=pcache)
            except InsufficientCapitalError as e:
                positions.append({"symbol": pick.symbol,
                                  "broker_profile": prof.profile_id,
                                  "status": "SKIPPED", "reason": str(e)})
                continue
            _c = pcache.get(pick.symbol, {})
            ref_price = float(_c.get("ltp") or 0.0) or (broker.get_ltp(pick.symbol) or 0.0)
            invested = qty * ref_price
            invested_basis += invested
            positions.append({"symbol": pick.symbol,
                              "broker_profile": prof.profile_id,
                              "qty": qty, "ref_price": ref_price,
                              "invested_value": invested,
                              "order_product": prof.order_product,
                              "instrument_type": prof.instrument_type})

    total_alloc = float(config.total_allocated_capital)
    basis = invested_basis if invested_basis > 0 else total_alloc
    leverage = (invested_basis / total_alloc) if total_alloc > 0 else 0.0
    return {
        "invested_basis": invested_basis,
        "total_allocated_capital": total_alloc,
        "leverage": leverage,
        "n_positions": len([p for p in positions if p.get("status") != "SKIPPED"]),
        "positions": positions,
        "kill_preview": compute_kill_preview(
            kill_switch_enabled=config.kill_switch_enabled,
            kill_switch_pct=config.kill_switch_pct,
            kill_switch_direction=config.kill_switch_direction,
            invested_basis=basis,
            total_allocated_capital=total_alloc),
    }


async def _exit_single_position(
        session_id: str,
        position: Dict[str, Any],
        reason: str,
        brokers: Dict[str, Any],
        registry: Any,
        gtt_manager: Any,
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
    if not _exit_gate_mod.claim_exit_session(session_id, symbol, reason):
        log.info("per-stock stop %s/%s: exit already claimed — skip",
                 session_id, symbol)
        return {"symbol": symbol, "status": "BLOCKED"}

    # 2. Cancel the broker GTT for this position (best-effort, never block exit).
    gtt_id = position.get("gtt_id")
    if gtt_manager and gtt_id:
        try:
            await asyncio.to_thread(broker.cancel_gtt, gtt_id)
            log.info("per-stock stop %s/%s: GTT %s cancelled", session_id, symbol, gtt_id)
        except Exception as e:
            log.warning("per-stock stop %s/%s: GTT cancel failed (%s): %s",
                        session_id, symbol, gtt_id, e)

    # 3. Place the market sell.
    qty = int(position.get("qty") or 0)
    itype = position.get("instrument_type") or "EQ"
    try:
        res = await broker.place_market_exit(symbol, qty, itype)
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
    )
    confirm_status = confirm_result.get("status", "UNKNOWN")
    exit_price = confirm_result.get("exit_price") or position.get("ltp")

    if confirm_status in ("COMPLETE", "DRY_RUN"):
        log.warning("per-stock stop FIRED %s/%s (reason=%s) exit_price=%s",
                    session_id, symbol, reason, exit_price)
        return {"symbol": symbol, "status": "EXITED", "reason": reason,
                "exit_price": exit_price,
                "broker_order_id": order_id}
    else:
        # PARTIAL / TIMEOUT / REJECTED — mark_exit_failed already called by confirm_exit
        # for REJECTED/CANCELLED. For PARTIAL/TIMEOUT the gate was NOT released by
        # confirm_exit so we release it here to allow a future retry.
        if confirm_status in ("PARTIAL", "TIMEOUT"):
            from autotrade.exit_gate import release_exit_session as _release
            _release(session_id, symbol)
        log.error("per-stock stop EXIT_FAILED %s/%s (confirm_status=%s)",
                  session_id, symbol, confirm_status)
        return {"symbol": symbol, "status": "EXIT_FAILED",
                "confirm_status": confirm_status}


class TradingSession:
    def __init__(self, session_id: str, config: TradingSessionConfig,
                 mode: str = "paper", user_id: Optional[str] = None,
                 broker_account_id: Optional[str] = None):
        self.session_id = session_id
        self.config = config
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
                "SELECT mode, config_json, user_id, broker_account_id "
                "FROM autotrade_sessions WHERE session_id=?",
                (session_id,),
            ).fetchone()
        if not row:
            return None
        cfg = TradingSessionConfig.from_json(row["config_json"])
        # user_id / broker_account_id are NULL for pre-Phase-2 sessions → the
        # operator/global path, unchanged.
        d = dict(row)
        return cls(session_id, cfg, mode=row["mode"],
                   user_id=d.get("user_id"),
                   broker_account_id=d.get("broker_account_id"))

    @classmethod
    def list_sessions(cls, limit: int = 50,
                      user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Recent sessions (newest first) for the operator UI session list.

        This is what lets the panel SHOW existing sessions instead of resetting
        to a blank create form — a created session stays visible/resumable.

        PHASE-2 MULTI-TENANT: when user_id is provided, ONLY that user's sessions
        are returned (per-user isolation). When user_id is None the list is the
        full operator view (today's behaviour) — used by the operator console.
        """
        base = (
            """SELECT s.session_id, s.created_at, s.started_at, s.closed_at,
                      s.status, s.mode, s.total_allocated_capital,
                      s.last_gross_return,
                      s.last_gross_return AS gross_return,
                      s.user_id, s.broker_account_id,
                      (SELECT COUNT(*) FROM autotrade_positions p
                       WHERE p.session_id = s.session_id
                         AND p.status = 'OPEN') AS n_open_positions
               FROM autotrade_sessions s
            """)
        with falcon_conn() as con:
            if user_id is not None:
                rows = con.execute(
                    base + " WHERE s.user_id = ? ORDER BY s.created_at DESC "
                    "LIMIT ?", (user_id, int(limit)),
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
        from . import vault
        if not vault.vault_enabled():
            # Bound account but vault disabled: we CANNOT trade the right
            # account. Clear the binding so the adapter doesn't error trying to
            # build a per-account client; it falls back to the global operator
            # path. (Live trades still need FALCON_AUTOTRADE_ENABLED; this only
            # affects WHICH account — surfaced via a warning.)
            log.warning("session %s: profile %s bound to account %s but vault "
                        "is DISABLED — falling back to global operator creds",
                        self.session_id, prof.profile_id, acct_id)
            prof.broker_account_id = None
            return
        creds = vault.get_decrypted_creds(acct_id, user_id=self.user_id)
        if creds is None:
            log.warning("session %s: could not resolve creds for account %s "
                        "(absent / not owned / decrypt failed) — global fallback",
                        self.session_id, acct_id)
            prof.broker_account_id = None
            return
        # Populate in-memory creds (NEVER persisted). The adapter's _build_kite
        # uses these to build a dedicated proxy-aware client for this account.
        prof.api_key = creds.api_key or ""
        prof.api_secret = creds.api_secret or ""
        prof.access_token = creds.access_token or ""
        prof.broker_account_id = acct_id

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
        self._build_brokers()
        self._set_status("RUNNING", started_at=_now_ist_iso())

        falcon_picks = load_falcon_picks(top_n=max(self.config.top_n_stocks, 10))
        if self.config.rank_filter:
            falcon_picks = [p for p in falcon_picks
                            if p.rank in self.config.rank_filter]

        router = BrokerRouter(top_n_stocks=self.config.top_n_stocks)
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

        async def _guarded_place(broker, prof, pick, amount, allocator, cache):
            async with sem:
                try:
                    return await self._place_one(
                        broker, prof, pick, amount, allocator, prefetch=cache)
                except Exception as e:  # belt-and-braces leg isolation
                    log.error("entry leg crashed for %s: %s", pick.symbol, e)
                    return {"symbol": pick.symbol, "status": "FAILED",
                            "error": str(e)}

        leg_coros = []
        for prof in self.config.broker_profiles:
            if not prof.enabled:
                continue
            broker = self.brokers[prof.profile_id]
            picks = routed.get(prof.profile_id, [])
            allocator = CapitalAllocator(self.config)
            amounts = allocator.allocate([p.symbol for p in picks])
            fund_picks = [p for p in picks if amounts.get(p.symbol, 0.0) > 0]
            # ONE batched prefetch per profile (LTP + MTF margin for all picks).
            try:
                cache = allocator.prefetch([p.symbol for p in fund_picks], broker)
            except Exception as e:  # pragma: no cover - per-symbol fallback inside
                log.warning("prefetch failed for %s (%s) — per-symbol fallback",
                            prof.profile_id, e)
                cache = {}
            for pick in fund_picks:
                amount = amounts.get(pick.symbol, 0.0)
                leg_coros.append(_guarded_place(
                    broker, prof, pick, amount, allocator, cache))

        placed: List[Dict[str, Any]] = list(
            await asyncio.gather(*leg_coros)) if leg_coros else []
        entry_latency_ms = int((time.monotonic() - _fire_t0) * 1000)
        try:
            self._record_latency(entry_latency_ms=entry_latency_ms)
        except Exception as e:  # pragma: no cover - never block on observability
            log.debug("entry_latency record failed for %s: %s", self.session_id, e)
        log.info("session %s entry fired %d legs in %dms (concurrency=%d)",
                 self.session_id, len(placed), entry_latency_ms, _ENTRY_CONCURRENCY)

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

        # INTRADAY BASKET: arm the precise-time square-off scheduler so the basket
        # is flattened at config.square_off_time (never overnight). The tick
        # driver's in-tick square-off (trail_engine.decide) is the restart-safe
        # backstop; this is the on-the-second path. Future time only — if
        # square_off_time has already passed the next tick squares off. Best-
        # effort, never blocks the start.
        try:
            if self.config.strategy == "intraday_basket":
                self._arm_square_off()
        except Exception as e:  # never block start on the square-off scheduler
            log.warning("square-off arm failed for %s: %s", self.session_id, e)

        return {"session_id": self.session_id, "status": "RUNNING",
                "mode": self.mode, "n_placed": len(placed), "orders": placed,
                "gtt": gtt_results}

    def _arm_square_off(self) -> bool:
        """Arm the per-session square-off scheduler at config.square_off_time
        (today, IST). No-op if the time is unparseable or already past (the tick
        driver squares off defensively in that case). Returns True if armed."""
        try:
            target = _parse_entry_time_today_ist(self.config.square_off_time)
        except ValueError:
            log.warning("session %s: unparseable square_off_time %r — relying on "
                        "in-tick square-off", self.session_id,
                        self.config.square_off_time)
            return False
        if datetime.now(IST) >= target:
            log.info("session %s: square_off_time %s already passed — in-tick "
                     "square-off will fire", self.session_id, target.isoformat())
            return False
        return square_off_scheduler.start_for_session(self.session_id, target)

    async def _place_one(self, broker, prof, pick: Pick, amount: float,
                         allocator: CapitalAllocator,
                         prefetch: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        symbol = pick.symbol
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
        order = build_order(symbol, qty, self.config, broker)
        if order.order_type == "LIMIT" and ref_price > 0:
            order.price = order.compute_limit_price(ref_price)

        # VWAP: observe the window then place MARKET (skip the wait in paper to
        # keep smoke tests fast; live honours the window).
        if order.order_type == "VWAP" and not self.dry_run:
            await asyncio.sleep(min(self.config.vwap_window_seconds, 1))

        try:
            res = await place_order_with_retry(order, broker)
        except Exception as e:
            log.error("place failed %s: %s", symbol, e)
            return {"symbol": symbol, "status": "FAILED", "error": str(e)}

        # Register the (paper or real) position. In dry-run we register the
        # intended qty at the reference price so monitoring works in paper mode.
        fill_price = res.avg_price or ref_price
        fill_qty = res.filled_qty or qty
        acct_id = getattr(prof, "broker_account_id", None)
        if res.status == "PARTIAL":
            self.registry.register_partial(symbol, prof.profile_id,
                                           fill_qty, fill_price,
                                           product=prof.order_product,
                                           instrument_type=prof.instrument_type,
                                           broker_account_id=acct_id)
        else:
            self.registry.register(symbol=symbol, broker_profile=prof.profile_id,
                                   qty=fill_qty, avg_price=fill_price,
                                   product=prof.order_product,
                                   instrument_type=prof.instrument_type,
                                   broker_account_id=acct_id)
        if ref_price > 0 and res.avg_price:
            record_slippage(symbol, ref_price, res.avg_price, fill_qty,
                            session_id=self.session_id,
                            broker_profile=prof.profile_id)
        return {"symbol": symbol, "status": res.status, "qty": fill_qty,
                "price": fill_price, "broker_order_id": res.broker_order_id,
                "broker_profile": prof.profile_id, "order_type": order.order_type}

    # ── Tick: monitor + GTT reconcile + kill switch / trail engine ─────────────
    async def tick(self) -> Dict[str, Any]:
        if not self.brokers:
            self._build_brokers()
        # COORDINATION (FEATURE 3): detect positions a fired broker GTT closed
        # externally BEFORE marking to market, so gross_return recomputes on the
        # remaining positions only (denominator stays total_allocated_capital).
        gtt_closed = []
        try:
            gtt_closed = self.gtt_manager.reconcile_gtt_fills()
        except Exception as e:  # pragma: no cover - never block the tick
            log.warning("GTT reconcile failed for %s: %s", self.session_id, e)
        self.monitor.refresh_ltps(self.brokers)

        # EXIT_FAILED RETRY: after the GTT reconcile step, re-attempt any
        # position whose exit previously failed and whose exit_gate was
        # released by registry.mark_exit_failed. Uses the same
        # _exit_single_position path (which now calls confirm_exit).
        # Fire-and-forget via create_task so we don't block the tick.
        try:
            failed_positions = self.monitor.get_exit_failed_positions()
            for fp in failed_positions:
                if _exit_gate_mod.claim_exit_session(
                        self.session_id, fp["symbol"], "EXIT_RETRY"):
                    asyncio.create_task(_exit_single_position(
                        session_id=self.session_id,
                        position=fp,
                        reason="EXIT_RETRY",
                        brokers=self.brokers,
                        registry=self.registry,
                        gtt_manager=self.gtt_manager,
                    ))
        except Exception as _efr_e:
            log.warning("EXIT_FAILED retry sweep failed for %s: %s",
                        self.session_id, _efr_e)

        snap = self.monitor.snapshot()
        # KILL BASIS: both strategies measure the INVESTED-basis gross return
        # (÷ frozen invested_basis), not the on-fund gross. snapshot() keeps the
        # on-fund gross_return for the history/charts.
        gr_invested = self.monitor.compute_gross_return_invested()

        if self.config.strategy == "intraday_basket":
            return await self._tick_intraday(gr_invested, snap, gtt_closed)

        # DEFAULT strategy: portfolio_kill_switch (UNCHANGED).
        reason = (self.kill_switch.check_threshold(gr_invested)
                  if self.kill_switch else None)
        fired = None
        if reason:
            # Single-fire guard: the 5s poll and the sub-second WS path must
            # never double-fire. Whoever wins the per-session lock fires.
            with fire_guard.claim_fire(self.session_id) as won:
                if won:
                    fired = await self.kill_switch.fire(
                        reason, gross_return=gr_invested)
                else:
                    reason = None  # another path already fired/is firing
        return {"gross_return": gr_invested, "gross_return_fund": snap["gross_return"],
                "snapshot": snap, "kill_switch_fired": bool(fired),
                "kill_reason": reason, "fire_result": fired,
                "gtt_closed": gtt_closed}

    async def _tick_intraday(self, gr_invested: float, snap: Dict[str, Any],
                             gtt_closed) -> Dict[str, Any]:
        """One tick for strategy=="intraday_basket": run the pure trail engine
        over the invested-basis gross return + persisted (armed, peak) state.

        The engine DECIDES only; on EXIT we REUSE the existing flatten
        (kill_switch.fire) passing the trail reason through as close_reason. State
        changes (arm / peak ratchet) are persisted on autotrade_sessions so a
        restart resumes the trail mid-day. Square-off is enforced defensively here
        even if the timer thread was dropped by a restart.

        PER-STOCK SOFTWARE STOP: before running the portfolio trail engine, each
        open position is checked against config.stop_pct. If a single stock
        has fallen more than stop_pct from its entry, OUR backend exits just that
        position (cancel its GTT first, then market sell). The GTT at -3% remains
        the broker-held backup; our software stop fires earlier (default -1.5%).
        After per-stock exits the trail engine runs on the remaining positions."""
        from .monitoring import trail_engine

        # PER-STOCK SOFTWARE STOP LOOP.
        # Runs BEFORE the portfolio-level trail engine so the trail sees the
        # updated (smaller) basket on this same tick.
        per_stock_exits: List[Dict[str, Any]] = []
        try:
            stop_pct = float(getattr(self.config, "stop_pct", 0.015))
            open_positions = self.monitor._open_positions()
            for pos in open_positions:
                ltp = pos.get("ltp")
                avg_price = float(pos.get("avg_price") or 0)
                if ltp is None or avg_price <= 0:
                    continue
                stock_return = (float(ltp) - avg_price) / avg_price
                if stock_return <= -stop_pct:
                    result = await _exit_single_position(
                        session_id=self.session_id,
                        position=pos,
                        reason="STOP_STOCK",
                        brokers=self.brokers,
                        registry=self.registry,
                        gtt_manager=self.gtt_manager,
                    )
                    per_stock_exits.append(result)
                    log.warning(
                        "per-stock stop TRIGGERED %s/%s: return=%.4f <= -%.4f",
                        self.session_id, pos["symbol"], stock_return, stop_pct)
        except Exception as e:  # never block the tick on per-stock stop errors
            log.error("per-stock stop loop error for %s: %s", self.session_id, e)

        state = self.monitor.load_trail_state()
        params = trail_engine.params_from_config(self.config)
        decision = trail_engine.decide(gr_invested, state, params)

        # Persist any state change (arm transition or peak ratchet) so the trail
        # is durable across restarts BEFORE any exit fires.
        if decision.state_changed:
            self.monitor.save_trail_state(decision.state)

        fired = None
        reason = None
        if decision.action == "EXIT":
            reason = decision.reason
            with fire_guard.claim_fire(self.session_id) as won:
                if won:
                    fired = await self.kill_switch.fire(
                        f"INTRADAY_BASKET {reason} "
                        f"gross_return={gr_invested:.4f}",
                        gross_return=gr_invested, close_reason=reason)
                else:
                    reason = None  # another path already fired/is firing
        return {"gross_return": gr_invested,
                "gross_return_fund": snap["gross_return"],
                "snapshot": snap,
                "strategy": "intraday_basket",
                "trail_action": decision.action,
                "trail_armed": decision.state.armed,
                "trail_peak": decision.state.peak,
                "trail_trigger": decision.trigger,
                "kill_switch_fired": bool(fired),
                "kill_reason": reason, "fire_result": fired,
                "gtt_closed": gtt_closed,
                "per_stock_exits": per_stock_exits}

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
        # Two clearly-named gross returns. gross_return is the KILL BASIS
        # (÷ frozen invested_basis); gross_return_fund is the on-fund view.
        invested_basis = self.monitor.invested_basis()
        gr_invested = self.monitor.compute_gross_return_invested()
        gr_fund = self.monitor.compute_gross_return()
        out = {
            "session_id": self.session_id,
            "status": status,
            "mode": sess.get("mode", self.mode),
            "gross_return": gr_invested,          # kill basis (÷ invested_basis)
            "gross_return_fund": gr_fund,         # on-fund (÷ allocated)
            "invested_basis": invested_basis,
            "total_allocated_capital": self.config.total_allocated_capital,
            "kill_switch_enabled": self.config.kill_switch_enabled,
            "kill_switch_pct": self.config.kill_switch_pct,
            "kill_switch_direction": self.config.kill_switch_direction,
            "kill_preview": compute_kill_preview(
                kill_switch_enabled=self.config.kill_switch_enabled,
                kill_switch_pct=self.config.kill_switch_pct,
                kill_switch_direction=self.config.kill_switch_direction,
                invested_basis=invested_basis,
                total_allocated_capital=self.config.total_allocated_capital),
            "n_open_positions": len(positions),
            "open_positions": positions,
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

        # INTRADAY BASKET: surface the full trailing-engine state + the per-day
        # dual-return report so the UI can render the trail status panel.
        if self.config.strategy == "intraday_basket":
            state = self.monitor.load_trail_state()
            params = trail_engine.params_from_config(self.config)
            trigger = trail_engine.compute_trigger(state, params)
            out["trail"] = {
                "armed": state.armed,
                "peak": state.peak,
                "current_gross_return": gr_invested,     # notional G (kill basis)
                "trigger": trigger,                       # live exit-trigger G
                "arm_pct": self.config.arm_pct,
                "floor_pct": self.config.floor_pct,
                "trail_giveback_pct": self.config.trail_giveback_pct,
                "stop_pct": self.config.stop_pct,
                "square_off_time": self.config.square_off_time,
                "seconds_to_square_off": trail_engine.seconds_to_square_off(
                    self.config.square_off_time),
                "square_off_armed": square_off_scheduler.is_running(
                    self.session_id),
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
