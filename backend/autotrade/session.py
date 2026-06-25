"""TradingSession — orchestrates all layers.

Lifecycle:
  create()   → persist config, status CREATED. No orders.
  start()    → route picks → size → place entries (dry-run-safe) → register
               positions → status RUNNING. Entries respect MARKET/LIMIT/VWAP.
  tick()     → refresh LTPs → compute gross_return → snapshot → check kill
               switch threshold → fire if breached.
  kill()     → manual kill (same path as automatic).
  status()   → live status payload for the API.

Falcon picks are READ-ONLY from falcon_signals_live. We never write to it.

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

log = logging.getLogger("kanida.autotrade.session")
IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist_iso() -> str:
    return datetime.now(IST).isoformat()


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
        self.kill_switch = KillSwitchExecutor(
            self.session_id, self.config, self.brokers, self.registry)

    # ── Start: route → size → place → register ────────────────────────────────
    async def start(self) -> Dict[str, Any]:
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

        return {"session_id": self.session_id, "status": "RUNNING",
                "mode": self.mode, "n_placed": len(placed), "orders": placed}

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
                                           product=prof.order_product)
        else:
            self.registry.register(symbol=symbol, broker_profile=prof.profile_id,
                                   qty=fill_qty, avg_price=fill_price,
                                   product=prof.order_product)
        if ref_price > 0 and res.avg_price:
            record_slippage(symbol, ref_price, res.avg_price, fill_qty,
                            session_id=self.session_id,
                            broker_profile=prof.profile_id)
        return {"symbol": symbol, "status": res.status, "qty": fill_qty,
                "price": fill_price, "broker_order_id": res.broker_order_id,
                "broker_profile": prof.profile_id, "order_type": order.order_type}

    # ── Tick: monitor + kill switch ───────────────────────────────────────────
    async def tick(self) -> Dict[str, Any]:
        if not self.brokers:
            self._build_brokers()
        self.monitor.refresh_ltps(self.brokers)
        snap = self.monitor.snapshot()
        gr = snap["gross_return"]
        reason = self.kill_switch.check_threshold(gr) if self.kill_switch else None
        fired = None
        if reason:
            fired = await self.kill_switch.fire(reason, gross_return=gr)
        return {"gross_return": gr, "snapshot": snap,
                "kill_switch_fired": bool(reason), "kill_reason": reason,
                "fire_result": fired}

    # ── Manual kill ────────────────────────────────────────────────────────────
    async def kill(self, reason: str = "MANUAL") -> Dict[str, Any]:
        if not self.brokers:
            self._build_brokers()
        self.monitor.refresh_ltps(self.brokers)
        gr = self.monitor.compute_gross_return()
        return await self.kill_switch.fire(f"MANUAL {reason} gross_return={gr:.4f}",
                                           gross_return=gr)

    # ── Status ─────────────────────────────────────────────────────────────────
    def status(self) -> Dict[str, Any]:
        with falcon_conn() as con:
            row = con.execute(
                "SELECT * FROM autotrade_sessions WHERE session_id=?",
                (self.session_id,),
            ).fetchone()
        sess = dict(row) if row else {}
        positions = self.registry.get_open_positions()
        gr = self.monitor.compute_gross_return()
        return {
            "session_id": self.session_id,
            "status": sess.get("status"),
            "mode": sess.get("mode", self.mode),
            "gross_return": gr,
            "total_allocated_capital": self.config.total_allocated_capital,
            "kill_switch_enabled": self.config.kill_switch_enabled,
            "kill_switch_pct": self.config.kill_switch_pct,
            "kill_switch_direction": self.config.kill_switch_direction,
            "n_open_positions": len(positions),
            "open_positions": positions,
        }

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
