"""RiskManager — the RMS (Risk Management Service) layer (SPRINT CLUSTER 4).

This is the risk SYSTEM the audits found missing: a pre-trade margin gate + a
per-user committed-capital ledger (CAP 1), and a portfolio-level daily-loss
circuit breaker + a global break-glass kill-all (CAP 2). It sits ABOVE the
per-basket trailing/stops — those decide WHEN one basket exits; this decides
whether a NEW session may deploy at all, and flattens ALL of a user's live
sessions when their aggregate loss breaches a limit.

DATA ISOLATION: reads/writes ONLY autotrade_sessions / autotrade_positions.
NEVER touches falcon_position_state or the legacy path.

DEFAULT-SAFE:
  * CAP 1 gate is INERT when the broker can't report available margin (paper /
    stub / no live creds → available_margin() None). Paper sizing is byte-for-byte
    unchanged: a paper session's broker returns None → no refusal.
  * CAP 2 breaker is DISABLED unless a session sets max_daily_loss_pct /
    max_daily_loss_amount (both default None). A fresh default config never fires
    it.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.risk_manager")
IST = timezone(timedelta(hours=5, minutes=30))

# A session is "committing capital" while it is armed to fire or holding.
_ACTIVE_STATUSES = ("RUNNING", "SCHEDULED", "KILLING_INCOMPLETE")
# A session is "holding real exposure" (contributes P&L to the breaker) while
# RUNNING or mid-incomplete-kill.
_HOLDING_STATUSES = ("RUNNING", "KILLING_INCOMPLETE")

_EPS = 1e-6


# ── CONCENTRATION / FAT-FINGER LIMITS (SPRINT CLUSTER 8 ITEM 3) ───────────────

@dataclass
class ConcentrationDecision:
    qty: int                       # the (possibly clamped) qty to place
    refused: bool = False          # True → drop the leg entirely
    clamped: bool = False          # True → qty was reduced from the requested
    reason: Optional[str] = None
    notional: float = 0.0          # requested notional (requested_qty × price)
    cap_rs: Optional[float] = None  # the effective ₹ notional cap that governed


def concentration_thresholds_rs(config) -> Dict[str, Optional[float]]:
    """The ₹ concentration/fat-finger thresholds for a config, for surfacing in the
    preview/start response so the leverage math is unambiguous. None where a cap is
    not set (inert)."""
    account = float(getattr(config, "total_allocated_capital", 0.0) or 0.0)
    mpn = getattr(config, "max_pct_per_name", None)
    return {
        "max_per_name_rs": (float(mpn) * account) if mpn else None,
        "max_notional_per_order_rs": getattr(
            config, "fatfinger_max_notional_per_order", None),
        "max_qty_per_order": getattr(config, "fatfinger_max_qty_per_order", None),
    }


def check_concentration(config, symbol: str, requested_qty: int,
                        price: Optional[float]) -> ConcentrationDecision:
    """Apply the per-order concentration / fat-finger caps to ONE leg.

    Caps (each INERT when unset — default None → byte-for-byte unchanged):
      * max_pct_per_name × total_allocated_capital → a ₹ notional cap per name.
      * fatfinger_max_notional_per_order            → an absolute ₹ notional cap.
      * fatfinger_max_qty_per_order                 → an absolute qty cap.

    The SMALLEST governing ₹ notional cap → a max qty = floor(cap / price); the qty
    cap is applied on top. fatfinger_policy: "clamp" (DEFAULT) reduces the qty to
    fit; "refuse" drops the whole leg. A cap that collapses the qty to 0 REFUSES
    regardless of policy (a 0-qty order is meaningless). Returns a decision the
    caller (sizing/_place_one + pre-trade) acts on."""
    req = int(requested_qty or 0)
    px = float(price or 0.0)
    notional = req * px if px > 0 else 0.0
    account = float(getattr(config, "total_allocated_capital", 0.0) or 0.0)
    policy = getattr(config, "fatfinger_policy", "clamp")

    caps_rs: List[float] = []
    mpn = getattr(config, "max_pct_per_name", None)
    if mpn and account > 0:
        caps_rs.append(float(mpn) * account)
    mno = getattr(config, "fatfinger_max_notional_per_order", None)
    if mno:
        caps_rs.append(float(mno))
    notional_cap = min(caps_rs) if caps_rs else None

    qty = req
    clamped = False
    reason: Optional[str] = None

    # ₹ notional cap → a max qty (needs a positive price to convert).
    if notional_cap is not None and px > 0 and notional > notional_cap + _EPS:
        max_qty = int(notional_cap // px)
        if max_qty <= 0:
            return ConcentrationDecision(
                qty=0, refused=True, notional=notional, cap_rs=notional_cap,
                reason=(f"fat-finger REFUSE {symbol}: ₹{px:,.2f}/share exceeds the "
                        f"₹{notional_cap:,.0f} per-order notional cap (max qty 0)"))
        if policy == "refuse":
            return ConcentrationDecision(
                qty=0, refused=True, notional=notional, cap_rs=notional_cap,
                reason=(f"fat-finger REFUSE {symbol}: notional ₹{notional:,.0f} "
                        f"exceeds cap ₹{notional_cap:,.0f} (policy=refuse)"))
        qty = max_qty
        clamped = True
        reason = (f"concentration CLAMP {symbol}: qty {req}→{qty} "
                  f"(notional ₹{notional:,.0f} > cap ₹{notional_cap:,.0f})")

    # Absolute qty sanity cap (applied on top of any notional clamp).
    mq = getattr(config, "fatfinger_max_qty_per_order", None)
    if mq is not None and qty > int(mq):
        if policy == "refuse":
            return ConcentrationDecision(
                qty=0, refused=True, notional=notional, cap_rs=notional_cap,
                reason=(f"fat-finger REFUSE {symbol}: qty {qty} exceeds the "
                        f"{int(mq)} per-order qty cap (policy=refuse)"))
        qty = int(mq)
        clamped = True
        reason = (f"fat-finger CLAMP {symbol}: qty {req}→{qty} "
                  f"(per-order qty cap {int(mq)})")

    return ConcentrationDecision(qty=qty, refused=False, clamped=clamped,
                                 reason=reason, notional=notional,
                                 cap_rs=notional_cap)


# ── CAP 1: per-user committed-capital ledger + pre-trade margin gate ──────────

@dataclass
class RiskDecision:
    allow: bool
    reason: Optional[str] = None
    available_margin: Optional[float] = None
    committed_other: float = 0.0
    planned_deployed: float = 0.0
    free: Optional[float] = None


def _user_match_clause(user_id: Optional[str]) -> tuple:
    """SQL fragment + params matching a user's sessions. NULL (operator/global)
    sessions all share the single None bucket."""
    if user_id is None:
        return ("user_id IS NULL", ())
    return ("user_id = ?", (str(user_id),))


def committed_capital(user_id: Optional[str],
                      exclude_session_id: Optional[str] = None) -> float:
    """Σ total_allocated_capital over the user's sessions currently committing
    capital (RUNNING / SCHEDULED / KILLING_INCOMPLETE), excluding one session.

    This is the LEDGER: a new session's slice must fit inside the account's free
    margin MINUS what the user's other live sessions have already committed, so
    three ₹5L sessions on ₹5L of funds can't each size as if alone."""
    where, params = _user_match_clause(user_id)
    q = (f"SELECT COALESCE(SUM(total_allocated_capital), 0.0) AS c "
         f"FROM autotrade_sessions "
         f"WHERE {where} AND status IN ({','.join('?' * len(_ACTIVE_STATUSES))})")
    args: List[Any] = list(params) + list(_ACTIVE_STATUSES)
    if exclude_session_id is not None:
        q += " AND session_id <> ?"
        args.append(exclude_session_id)
    with falcon_conn() as con:
        row = con.execute(q, tuple(args)).fetchone()
    return float(row["c"]) if row else 0.0


def _probe_available_margin(brokers: Dict[str, Any]) -> Optional[float]:
    """Sum available_margin() over the DISTINCT broker clients (paper / stub
    return None). Returns None only when NO broker can answer (unknown budget →
    the gate is inert). Distinct by object id so a shared account isn't summed
    twice."""
    seen: set = set()
    total: Optional[float] = None
    for b in (brokers or {}).values():
        if b is None or id(b) in seen:
            continue
        seen.add(id(b))
        try:
            m = b.available_margin()
        except Exception as e:  # pragma: no cover - defensive
            log.warning("available_margin probe raised (%s) — treating as unknown",
                        e)
            m = None
        if m is None:
            continue
        total = (total or 0.0) + float(m)
    return total


def pre_trade_gate(*, user_id: Optional[str], session_id: str,
                   planned_deployed: float,
                   brokers: Dict[str, Any]) -> RiskDecision:
    """RMS CAP 1 pre-trade gate. Refuse a session whose planned deployed capital
    exceeds the account's FREE margin minus the user's already-committed capital.

    planned_deployed: Σ(plan["deployed"]) across the session's broker profiles —
        the real cash / margin the entry orders will consume (MTF = Σ margin,
        CNC = Σ cash).

    SAFE-BY-DEFAULT: when no broker reports available margin (paper / stub / no
    live creds → available_margin() None) the budget is UNKNOWN and we DO NOT
    block (log only) — paper sizing is byte-for-byte unchanged. Only when a real
    budget is known do we compute free = budget - committed_other and refuse an
    over-deploy."""
    budget = _probe_available_margin(brokers)
    committed_other = committed_capital(user_id, exclude_session_id=session_id)
    if budget is None:
        log.debug("pre_trade_gate %s: available margin unknown — gate inert "
                  "(committed_other=₹%.0f, planned=₹%.0f)",
                  session_id, committed_other, planned_deployed)
        return RiskDecision(
            allow=True, reason="available margin unknown — gate inert",
            available_margin=None, committed_other=committed_other,
            planned_deployed=float(planned_deployed), free=None)
    free = budget - committed_other
    if float(planned_deployed) > free + _EPS:
        reason = (
            f"pre-trade RMS REFUSED: planned deploy ₹{planned_deployed:,.0f} "
            f"exceeds free margin ₹{free:,.0f} (available ₹{budget:,.0f} − "
            f"committed by your other live sessions ₹{committed_other:,.0f}). "
            f"Reduce capital or free up an existing session.")
        log.warning("session %s: %s", session_id, reason)
        return RiskDecision(
            allow=False, reason=reason, available_margin=budget,
            committed_other=committed_other,
            planned_deployed=float(planned_deployed), free=free)
    return RiskDecision(
        allow=True, reason="ok", available_margin=budget,
        committed_other=committed_other,
        planned_deployed=float(planned_deployed), free=free)


# ── CAP 2: portfolio daily-loss breaker + global kill-all ─────────────────────

def _active_session_rows(user_id: Optional[str], mode: Optional[str],
                         statuses: tuple) -> List[Dict[str, Any]]:
    where, params = _user_match_clause(user_id)
    q = (f"SELECT session_id, status, mode, total_allocated_capital, config_json "
         f"FROM autotrade_sessions "
         f"WHERE {where} AND status IN ({','.join('?' * len(statuses))})")
    args: List[Any] = list(params) + list(statuses)
    if mode is not None:
        q += " AND mode = ?"
        args.append(mode)
    with falcon_conn() as con:
        rows = con.execute(q, tuple(args)).fetchall()
    return [dict(r) for r in rows]


def aggregate_daily_pnl(session_ids: List[str]) -> float:
    """Σ realised (CLOSED) + unrealised (OPEN + EXIT_FAILED, still-held) P&L over
    the given sessions. unrealised_pnl is persisted sign-aware (short = profit on
    fall) by monitor.refresh_ltps, so this reads live exposure directly."""
    if not session_ids:
        return 0.0
    ph = ",".join("?" * len(session_ids))
    with falcon_conn() as con:
        u = con.execute(
            f"SELECT COALESCE(SUM(unrealised_pnl),0.0) AS t "
            f"FROM autotrade_positions "
            f"WHERE session_id IN ({ph}) AND status IN ('OPEN','EXIT_FAILED') "
            f"AND qty > 0 AND unrealised_pnl IS NOT NULL",
            tuple(session_ids)).fetchone()
        r = con.execute(
            f"SELECT COALESCE(SUM(realised_pnl),0.0) AS t "
            f"FROM autotrade_positions "
            f"WHERE session_id IN ({ph}) AND status='CLOSED' "
            f"AND realised_pnl IS NOT NULL",
            tuple(session_ids)).fetchone()
    return float(u["t"]) + float(r["t"])


def _cohort_loss_limit_rs(rows: List[Dict[str, Any]]) -> Optional[float]:
    """The TIGHTEST (smallest ₹) daily-loss limit configured across the cohort's
    sessions, or None when NONE configure one (breaker disabled).

    A session's pct limit is applied to the SUM of allocated capital across the
    whole cohort (the user's total deployed budget); an amount limit is absolute
    ₹. The most-protective (min) configured limit wins."""
    from .config import TradingSessionConfig
    total_cap = sum(float(r.get("total_allocated_capital") or 0.0) for r in rows)
    limit: Optional[float] = None
    for r in rows:
        cj = r.get("config_json")
        if not cj:
            continue
        try:
            cfg = TradingSessionConfig.from_json(cj)
        except Exception:  # pragma: no cover - a malformed config never arms it
            continue
        candidates: List[float] = []
        if getattr(cfg, "max_daily_loss_amount", None) is not None:
            candidates.append(float(cfg.max_daily_loss_amount))
        if getattr(cfg, "max_daily_loss_pct", None) is not None and total_cap > 0:
            candidates.append(float(cfg.max_daily_loss_pct) * total_cap)
        for c in candidates:
            if c > 0 and (limit is None or c < limit):
                limit = c
    return limit


@dataclass
class BreakerDecision:
    breached: bool
    aggregate_pnl: float = 0.0
    limit_rs: Optional[float] = None
    session_ids: List[str] = field(default_factory=list)
    reason: Optional[str] = None


def check_portfolio_breaker(user_id: Optional[str],
                            mode: Optional[str] = None) -> BreakerDecision:
    """Evaluate the per-user daily-loss breaker over the user's HOLDING sessions
    (of `mode`, when given). Breached ⟺ aggregate realised+unrealised loss <=
    -limit. Returns a decision; the CALLER flattens (check-then-act)."""
    rows = _active_session_rows(user_id, mode, _HOLDING_STATUSES)
    limit = _cohort_loss_limit_rs(rows)
    if limit is None:
        return BreakerDecision(breached=False, session_ids=[r["session_id"] for r in rows],
                               reason="no daily-loss limit configured")
    sids = [r["session_id"] for r in rows]
    pnl = aggregate_daily_pnl(sids)
    breached = pnl <= -abs(limit)
    return BreakerDecision(
        breached=breached, aggregate_pnl=pnl, limit_rs=abs(limit),
        session_ids=sids,
        reason=(f"aggregate P&L ₹{pnl:,.0f} <= -₹{abs(limit):,.0f}" if breached
                else f"aggregate P&L ₹{pnl:,.0f} within -₹{abs(limit):,.0f}"))


# Per-user break-glass guard: never run two flatten sweeps for the same
# (user, mode) concurrently, and don't re-fire within a short cooldown.
_KILL_LOCK = threading.Lock()
_KILL_INPROGRESS: set = set()
_KILL_LAST: Dict[Any, datetime] = {}
_KILL_COOLDOWN_SEC = 30.0


def _kill_key(user_id: Optional[str], mode: Optional[str]):
    return (user_id, mode)


async def global_kill(user_id: Optional[str], mode: Optional[str] = None,
                      reason: str = "GLOBAL_KILL") -> Dict[str, Any]:
    """BREAK-GLASS: flatten EVERY live session for a user in one sweep.

    Loads each active (RUNNING / KILLING_INCOMPLETE) session for the user (of
    `mode`, when given) and calls its kill() — the same guarded flatten the
    manual/portfolio-kill path uses (exit_gate + fire_guard prevent double-exit).
    Idempotent + reentrancy-guarded: two concurrent invocations for the same
    (user, mode) collapse to one. Paper sessions flatten with DRY_RUN orders."""
    key = _kill_key(user_id, mode)
    with _KILL_LOCK:
        if key in _KILL_INPROGRESS:
            return {"user_id": user_id, "mode": mode, "reason": reason,
                    "skipped": "already in progress", "killed": []}
        _KILL_INPROGRESS.add(key)
    try:
        rows = _active_session_rows(user_id, mode, _HOLDING_STATUSES)
        from .session import TradingSession
        killed: List[Dict[str, Any]] = []
        for r in rows:
            sid = r["session_id"]
            try:
                sess = TradingSession.load(sid)
                if sess is None:
                    continue
                res = await sess.kill(reason=reason)
                killed.append({"session_id": sid,
                               "status": res.get("status") if isinstance(res, dict)
                               else None})
            except Exception as e:  # never let one session block the sweep
                log.error("global_kill: session %s kill failed: %s", sid, e)
                killed.append({"session_id": sid, "error": str(e)})
        _KILL_LAST[key] = datetime.now(IST)
        log.critical("GLOBAL KILL [%s] user=%s mode=%s — flattened %d session(s)",
                     reason, user_id, mode, len(killed))
        return {"user_id": user_id, "mode": mode, "reason": reason,
                "killed": killed, "n_killed": len(killed)}
    finally:
        with _KILL_LOCK:
            _KILL_INPROGRESS.discard(key)


async def maybe_fire_breaker(user_id: Optional[str],
                             mode: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Automatic breaker pass (called from session.tick). Evaluate + flatten ALL
    the user's live sessions if the aggregate daily loss breaches the limit.

    Returns the global_kill summary when it FIRES, else None. Guarded by a short
    per-(user, mode) cooldown so it evaluates at most once per window. Fully
    defensive — any error is swallowed (never crashes a tick)."""
    try:
        key = _kill_key(user_id, mode)
        now = datetime.now(IST)
        with _KILL_LOCK:
            if key in _KILL_INPROGRESS:
                return None
            last = _KILL_LAST.get(key)
            if last is not None and (now - last).total_seconds() < _KILL_COOLDOWN_SEC:
                return None
        decision = check_portfolio_breaker(user_id, mode)
        if not decision.breached:
            return None
        log.critical("PORTFOLIO BREAKER TRIPPED user=%s mode=%s: %s — "
                     "flattening ALL live sessions", user_id, mode, decision.reason)
        return await global_kill(
            user_id, mode,
            reason=f"PORTFOLIO_DAILY_LOSS_BREAKER ({decision.reason})")
    except Exception as e:  # pragma: no cover - never crash a tick
        log.error("maybe_fire_breaker raised (%s) — ignored", e)
        return None
