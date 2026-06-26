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
from .capital import CapitalAllocator, InsufficientCapitalError
from .broker.base import Pick
from .broker.router import BrokerRouter, build_client
from .execution.orders import build_order, place_order_with_retry
from .execution.slippage import record_slippage
from .monitoring.registry import PositionRegistry
from .monitoring.monitor import PortfolioMonitor, compute_kill_preview
from .monitoring.kill_switch import KillSwitchExecutor
from .monitoring.gtt_manager import GTTManager
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


def preview_session_sizing(config: TradingSessionConfig,
                           mode: str = "paper") -> Dict[str, Any]:
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
            instrument_type=config.instrument_type)]

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
        even if the timer thread was dropped by a restart."""
        from .monitoring import trail_engine

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
                "gtt_closed": gtt_closed}

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

    def _set_status(self, status: str, started_at: Optional[str] = None) -> None:
        with falcon_conn() as con:
            con.execute(
                "UPDATE autotrade_sessions SET status=?, "
                "started_at=COALESCE(?, started_at) WHERE session_id=?",
                (status, started_at, self.session_id))
            con.commit()
