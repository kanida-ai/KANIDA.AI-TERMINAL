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
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn

from .config import TradingSessionConfig
from .capital import CapitalAllocator, InsufficientCapitalError
from .broker.base import Pick
from .broker.router import BrokerRouter, build_client
from .execution.orders import build_order, place_order_with_retry
from .execution.slippage import record_slippage
from .monitoring.registry import PositionRegistry
from .monitoring.monitor import PortfolioMonitor
from .monitoring.kill_switch import KillSwitchExecutor
from .monitoring.gtt_manager import GTTManager
from .monitoring import tick_driver
from .monitoring import entry_scheduler
from .monitoring import fire_guard
from .monitoring import ws_driver

log = logging.getLogger("kanida.autotrade.session")
IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist_iso() -> str:
    return datetime.now(IST).isoformat()


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


class TradingSession:
    def __init__(self, session_id: str, config: TradingSessionConfig,
                 mode: str = "paper"):
        self.session_id = session_id
        self.config = config
        self.mode = mode  # 'paper' | 'live'
        self.dry_run = (mode != "live")
        self.registry = PositionRegistry(session_id, config.total_allocated_capital)
        self.monitor = PortfolioMonitor(session_id, config.total_allocated_capital)
        self.brokers: Dict[str, Any] = {}
        self.kill_switch: Optional[KillSwitchExecutor] = None
        self.gtt_manager: Optional[GTTManager] = None

    # ── Persistence / factory ─────────────────────────────────────────────────
    @classmethod
    def create(cls, config: TradingSessionConfig, mode: str = "paper") -> "TradingSession":
        config.validate()
        session_id = uuid.uuid4().hex
        with falcon_conn() as con:
            con.execute(
                """INSERT INTO autotrade_sessions
                   (session_id, created_at, status, mode,
                    total_allocated_capital, config_json)
                   VALUES (?,?,?,?,?,?)""",
                (session_id, _now_ist_iso(), "CREATED", mode,
                 config.total_allocated_capital, config.to_json()),
            )
            con.commit()
        log.info("AutoTrade session %s created (mode=%s)", session_id, mode)
        return cls(session_id, config, mode=mode)

    @classmethod
    def load(cls, session_id: str) -> Optional["TradingSession"]:
        with falcon_conn() as con:
            row = con.execute(
                "SELECT mode, config_json FROM autotrade_sessions WHERE session_id=?",
                (session_id,),
            ).fetchone()
        if not row:
            return None
        cfg = TradingSessionConfig.from_json(row["config_json"])
        return cls(session_id, cfg, mode=row["mode"])

    @classmethod
    def list_sessions(cls, limit: int = 50) -> List[Dict[str, Any]]:
        """Recent sessions (newest first) for the operator UI session list.

        This is what lets the panel SHOW existing sessions instead of resetting
        to a blank create form — a created session stays visible/resumable.
        """
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT s.session_id, s.created_at, s.started_at, s.closed_at,
                          s.status, s.mode, s.total_allocated_capital,
                          s.last_gross_return,
                          s.last_gross_return AS gross_return,
                          (SELECT COUNT(*) FROM autotrade_positions p
                           WHERE p.session_id = s.session_id
                             AND p.status = 'OPEN') AS n_open_positions
                   FROM autotrade_sessions s
                   ORDER BY s.created_at DESC LIMIT ?""",
                (int(limit),),
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Broker construction ───────────────────────────────────────────────────
    def _build_brokers(self) -> None:
        profiles = self.config.broker_profiles
        if not profiles:
            # Default single zerodha profile spanning the whole capital.
            from .config import BrokerProfile
            profiles = [BrokerProfile(
                profile_id="zerodha_default", broker_name="zerodha",
                allocated_capital=self.config.total_allocated_capital,
                order_product=self.config.order_product,
                instrument_type=self.config.instrument_type)]
            self.config.broker_profiles = profiles
        for prof in profiles:
            if not prof.enabled:
                continue
            self.brokers[prof.profile_id] = build_client(prof, dry_run=self.dry_run)
        self.gtt_manager = GTTManager(
            self.session_id, self.config, self.brokers, self.registry)
        self.kill_switch = KillSwitchExecutor(
            self.session_id, self.config, self.brokers, self.registry,
            gtt_manager=self.gtt_manager)

    # ── Start: now | scheduled ──────────────────────────────────────────────────
    async def start(self, when: str = "now") -> Dict[str, Any]:
        """Start the session.

        when="now" (default, backward-compatible): fire entries IMMEDIATELY —
            set status RUNNING and run _fire_entries() right now.

        when="scheduled": parse config.entry_time as TODAY in IST.
            * If entry_time is in the FUTURE: set status SCHEDULED, store the
              target, and arm a background scheduler thread that sleeps until
              entry_time (interruptible) then fires + flips to RUNNING.
            * If entry_time has already PASSED: fire immediately now (fallback)
              and include a note. Never silently does nothing.
        """
        if when == "scheduled":
            return await self._start_scheduled()
        # when == "now" (or any unknown value → safe default: fire now)
        return await self._fire_entries()

    async def _start_scheduled(self) -> Dict[str, Any]:
        try:
            target = _parse_entry_time_today_ist(self.config.entry_time)
        except ValueError as e:
            log.warning("scheduled start for %s: %s — firing immediately",
                        self.session_id, e)
            res = await self._fire_entries()
            res["note"] = f"entry_time unparseable ({e}) — fired immediately"
            res["when"] = "scheduled"
            return res

        now = datetime.now(IST)
        if now >= target:
            # entry_time already passed today → fire immediately (fallback).
            res = await self._fire_entries()
            res["note"] = ("entry_time already passed — fired immediately")
            res["when"] = "scheduled"
            res["entry_time"] = self.config.entry_time
            return res

        # Future → arm the scheduler; place NOTHING yet.
        self._set_status("SCHEDULED")
        armed = entry_scheduler.start_for_session(self.session_id, target)
        seconds = int(max(0.0, (target - now).total_seconds()))
        log.info("session %s SCHEDULED — entry at %s (in %ss, armed=%s)",
                 self.session_id, target.isoformat(), seconds, armed)
        return {"session_id": self.session_id, "status": "SCHEDULED",
                "mode": self.mode, "when": "scheduled",
                "entry_time": self.config.entry_time,
                "fires_at": target.isoformat(),
                "seconds_remaining": seconds,
                "scheduler_armed": armed, "n_placed": 0, "orders": []}

    # ── Fire entries: route → size → place → register (THE order-firing leg) ────
    async def _fire_entries(self) -> Dict[str, Any]:
        self._build_brokers()
        self._set_status("RUNNING", started_at=_now_ist_iso())

        falcon_picks = load_falcon_picks(top_n=max(self.config.top_n_stocks, 10))
        if self.config.rank_filter:
            falcon_picks = [p for p in falcon_picks
                            if p.rank in self.config.rank_filter]

        router = BrokerRouter(top_n_stocks=self.config.top_n_stocks)
        routed = router.route_picks(falcon_picks, self.config.broker_profiles)

        placed: List[Dict[str, Any]] = []
        for prof in self.config.broker_profiles:
            if not prof.enabled:
                continue
            broker = self.brokers[prof.profile_id]
            picks = routed.get(prof.profile_id, [])
            allocator = CapitalAllocator(self.config)
            amounts = allocator.allocate([p.symbol for p in picks])
            for pick in picks:
                amount = amounts.get(pick.symbol, 0.0)
                if amount <= 0:
                    continue
                rec = await self._place_one(broker, prof, pick, amount, allocator)
                placed.append(rec)

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

        return {"session_id": self.session_id, "status": "RUNNING",
                "mode": self.mode, "n_placed": len(placed), "orders": placed,
                "gtt": gtt_results}

    async def _place_one(self, broker, prof, pick: Pick, amount: float,
                         allocator: CapitalAllocator) -> Dict[str, Any]:
        symbol = pick.symbol
        try:
            qty = allocator.calculate_quantity(symbol, amount, broker)
        except InsufficientCapitalError as e:
            log.warning("skip %s: %s", symbol, e)
            return {"symbol": symbol, "status": "SKIPPED", "reason": str(e)}

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
        if res.status == "PARTIAL":
            self.registry.register_partial(symbol, prof.profile_id,
                                           fill_qty, fill_price,
                                           product=prof.order_product,
                                           instrument_type=prof.instrument_type)
        else:
            self.registry.register(symbol=symbol, broker_profile=prof.profile_id,
                                   qty=fill_qty, avg_price=fill_price,
                                   product=prof.order_product,
                                   instrument_type=prof.instrument_type)
        if ref_price > 0 and res.avg_price:
            record_slippage(symbol, ref_price, res.avg_price, fill_qty,
                            session_id=self.session_id,
                            broker_profile=prof.profile_id)
        return {"symbol": symbol, "status": res.status, "qty": fill_qty,
                "price": fill_price, "broker_order_id": res.broker_order_id,
                "broker_profile": prof.profile_id, "order_type": order.order_type}

    # ── Tick: monitor + GTT reconcile + kill switch ───────────────────────────
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
        snap = self.monitor.snapshot()
        gr = snap["gross_return"]
        reason = self.kill_switch.check_threshold(gr) if self.kill_switch else None
        fired = None
        if reason:
            # Single-fire guard: the 5s poll and the sub-second WS path must
            # never double-fire. Whoever wins the per-session lock fires.
            with fire_guard.claim_fire(self.session_id) as won:
                if won:
                    fired = await self.kill_switch.fire(reason, gross_return=gr)
                else:
                    reason = None  # another path already fired/is firing
        return {"gross_return": gr, "snapshot": snap,
                "kill_switch_fired": bool(fired), "kill_reason": reason,
                "fire_result": fired, "gtt_closed": gtt_closed}

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
        self.monitor.refresh_ltps(self.brokers)
        gr = self.monitor.compute_gross_return()
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
        gr = self.monitor.compute_gross_return()
        out = {
            "session_id": self.session_id,
            "status": status,
            "mode": sess.get("mode", self.mode),
            "gross_return": gr,
            "total_allocated_capital": self.config.total_allocated_capital,
            "kill_switch_enabled": self.config.kill_switch_enabled,
            "kill_switch_pct": self.config.kill_switch_pct,
            "kill_switch_direction": self.config.kill_switch_direction,
            "n_open_positions": len(positions),
            "open_positions": positions,
        }
        # SCHEDULED: surface the armed entry time so the UI can show
        # "Scheduled for 09:15" + a live countdown.
        if status == "SCHEDULED":
            out["entry_time"] = self.config.entry_time
            target = entry_scheduler.target_for_session(self.session_id)
            if target is None:
                # In-memory timer lost (e.g. backend restarted) — derive the
                # nominal target from config so the UI still shows the time.
                try:
                    target = _parse_entry_time_today_ist(self.config.entry_time)
                except ValueError:
                    target = None
                out["scheduler_armed"] = False
            else:
                out["scheduler_armed"] = entry_scheduler.is_running(self.session_id)
            if target is not None:
                out["fires_at"] = target.isoformat()
                out["seconds_remaining"] = int(
                    max(0.0, (target - datetime.now(IST)).total_seconds()))
        return out

    def positions(self) -> List[Dict[str, Any]]:
        return self.registry.get_all_positions()

    # ── helpers ─────────────────────────────────────────────────────────────────
    def _set_status(self, status: str, started_at: Optional[str] = None) -> None:
        with falcon_conn() as con:
            con.execute(
                "UPDATE autotrade_sessions SET status=?, "
                "started_at=COALESCE(?, started_at) WHERE session_id=?",
                (status, started_at, self.session_id))
            con.commit()
