"""AutoTrade REST endpoints (spec Section 9).

Includes:
  GET /autotrade/session/{id}/journal   — Daily Trade Journal (BUILD 2)


All 9 endpoints are AUTH-GATED with the SAME operator-token dependency the
Falcon trade router uses (require_operator_token / X-Operator-Token), imported
directly from that router so the gate stays identical (fail-closed if
FALCON_OPERATOR_TOKEN is unset, constant-time compare). The secret never
reaches the browser — it's injected server-side by the Next.js proxy.

Endpoints:
  POST /autotrade/preview            (config-time sizing + kill preview, no session)
  POST /autotrade/session/create
  POST /autotrade/session/{id}/start
  GET  /autotrade/session/{id}/status
  POST /autotrade/session/{id}/kill
  GET  /autotrade/session/{id}/positions
  POST /autotrade/config/save
  GET  /autotrade/config/list
  POST /autotrade/broker/add
  GET  /autotrade/broker/list

SAFETY: session create defaults to mode='paper'. Live mode + a kill switch must
both be explicitly requested AND backed by the master env switch; otherwise no
real order is ever sent.
"""
from __future__ import annotations

import logging
import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

from falcon.db import falcon_conn

from .. import config as cfgmod
from ..config import TradingSessionConfig
from ..session import TradingSession, preview_session_sizing, load_falcon_picks
from ..monitoring import tick_driver, entry_scheduler
from .journal_routes import build_journal

log = logging.getLogger("kanida.autotrade.api")
IST = timezone(timedelta(hours=5, minutes=30))


def require_operator_token(x_operator_token: Optional[str] = Header(default=None)) -> None:
    """Operator-token gate — IDENTICAL semantics to the Falcon trade router's
    gate (X-Operator-Token vs server-side FALCON_OPERATOR_TOKEN env, fail-closed
    if unset, constant-time compare). The same shared secret protects the live
    order-execution surface; the token is injected server-side by the Next.js
    proxy so it never reaches the browser. Auth only — no trading logic here.

    (Defined locally because the autotrade-portfolio branch was cut from main,
    which predates the trade_router gate commit. When this branch is rebased
    onto a base that has trade_router.require_operator_token, this can be
    swapped for a direct import — the behaviour is the same.)
    """
    expected = os.environ.get("FALCON_OPERATOR_TOKEN", "").strip()
    if not expected:
        raise HTTPException(503, "AutoTrade operator token not configured on server")
    if not x_operator_token or not secrets.compare_digest(x_operator_token, expected):
        raise HTTPException(403, "operator token required")


router = APIRouter(dependencies=[Depends(require_operator_token)])


# ── PER-USER TENANT IDENTITY (Phase-2 multi-tenant isolation) ─────────────────
# The router-level operator-token gate above stays intact (server-to-server /
# operator-console gate). On top of it we resolve WHO is calling from the
# power-user JWT (Authorization: Bearer <jwt> OR the power_jwt cookie) and
# enforce per-user ownership on every session/account endpoint.
#
# Backward-compatibility hinge: with NO user JWT and AUTOTRADE_PORTAL_STRICT
# unset, resolve_caller() returns an ADMIN, UNAUTHENTICATED caller → the
# operator console behaves EXACTLY as today (full view, all endpoints work).
# Flip AUTOTRADE_PORTAL_STRICT=true once the frontend forwards the user JWT to
# require a user identity on every call.


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


@dataclass
class Caller:
    """Resolved per-request identity.

    user_id       : str | None — the authenticated portal user's id (as a STRING,
                    to match autotrade_sessions.user_id which is stored as text),
                    or None for the operator/server-to-server path.
    is_admin      : bool — True for the operator (JWT role=='admin') OR the
                    unauthenticated operator-console path (no user JWT, non-strict).
    authenticated : bool — True only when a valid user JWT was presented.
    """
    user_id: Optional[str]
    is_admin: bool
    authenticated: bool


def _extract_jwt(request: Request) -> Optional[str]:
    """Pull the power-user JWT from 'Authorization: Bearer <jwt>' OR the
    `power_jwt` cookie (browser session). Returns None if neither present."""
    auth = request.headers.get("authorization")
    if auth:
        parts = auth.strip().split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            return parts[1]
    cookie = request.cookies.get("power_jwt")
    if cookie:
        return cookie
    return None


def resolve_caller(request: Request) -> Caller:
    """Resolve the calling identity for tenant isolation.

    * Valid user JWT (Bearer header OR power_jwt cookie) →
        Caller(user_id=str(payload.user_id),
               is_admin=(role=='admin'), authenticated=True)
    * No / invalid JWT (operator-console or server-to-server path, gated only by
      the shared operator token) →
        - default: Caller(user_id=None, is_admin=True, authenticated=False)
          (operator console keeps working exactly as today)
        - AUTOTRADE_PORTAL_STRICT truthy: raise 401 (force a user identity).

    Defensive: any import/config problem in the power_user auth stack must NEVER
    500 the whole router — it degrades to the unauthenticated operator path
    (which is still rejected when strict is on).
    """
    token = _extract_jwt(request)
    if token:
        try:
            from power_user.services.auth import verify_jwt  # lazy, defensive
            payload = verify_jwt(token)
            return Caller(
                user_id=str(payload.user_id),
                is_admin=(payload.role == "admin"),
                authenticated=True,
            )
        except Exception as e:  # invalid/expired JWT OR auth-stack import problem
            # Do NOT 500. Treat as unauthenticated → falls to the operator path
            # below (still rejected if strict). An invalid token is not an
            # authenticated user.
            log.debug("resolve_caller: JWT not accepted (%s: %s) — "
                      "falling back to operator path", type(e).__name__, e)

    if _env_truthy("AUTOTRADE_PORTAL_STRICT"):
        raise HTTPException(401, "user authentication required")
    # Operator-console / server-to-server path: shared operator token only, no
    # user JWT. Full admin view, unchanged from today.
    return Caller(user_id=None, is_admin=True, authenticated=False)


# Operator/admin default used when an endpoint function is invoked DIRECTLY
# (e.g. unit tests, internal callers) without FastAPI resolving the dependency.
# Under the real HTTP stack FastAPI always injects a resolved Caller, so this
# only affects direct-function callers — which are the operator/admin path.
_OPERATOR_CALLER = Caller(user_id=None, is_admin=True, authenticated=False)


def _caller(caller: Any) -> Caller:
    """Normalise the caller param. When an endpoint is called directly (not via
    the HTTP DI stack) the default is FastAPI's Depends sentinel, not a Caller —
    treat that as the operator/admin path (today's behaviour, backward-compat)."""
    return caller if isinstance(caller, Caller) else _OPERATOR_CALLER


def _assert_user_scope(param_user_id: Optional[str], caller: Any) -> str:
    """Ownership gate for endpoints that take an explicit user_id (broker-account
    vault endpoints). Admin → unrestricted (returns param as-is). Non-admin →
    the param MUST equal the caller's user_id (else 404, no cross-user probing);
    if the non-admin omitted it, default to the caller's own id."""
    caller = _caller(caller)
    if caller.is_admin:
        return param_user_id  # operator/global path unchanged (may be None)
    if param_user_id is None:
        return caller.user_id
    if str(param_user_id) != caller.user_id:
        raise HTTPException(404, "broker account not found")
    return caller.user_id


def _assert_session_access(session_id: str, caller: Any) -> None:
    """Ownership gate for a single session.

    Reads autotrade_sessions.user_id for session_id (single query):
      * row missing              → 404 "session not found"
      * caller.is_admin          → allow (operator / admin sees everything)
      * owner mismatch           → 404 "session not found"  (NOT 403: a non-owner
                                    must not even be able to confirm the session
                                    exists; NULL-owned sessions are invisible to
                                    non-admin users)
    """
    caller = _caller(caller)
    with falcon_conn() as con:
        row = con.execute(
            "SELECT user_id FROM autotrade_sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(404, "session not found")
    if caller.is_admin:
        return
    owner = row["user_id"]
    if owner is None or str(owner) != caller.user_id:
        raise HTTPException(404, "session not found")


def _assert_ladder_access(ladder_id: str, caller: Any) -> None:
    """Ownership gate for a ladder campaign — same semantics as
    _assert_session_access (admin sees all; non-owner → 404, no existence leak)."""
    caller = _caller(caller)
    with falcon_conn() as con:
        row = con.execute(
            "SELECT user_id FROM autotrade_ladders WHERE ladder_id=?",
            (ladder_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(404, "ladder not found")
    if caller.is_admin:
        return
    owner = row["user_id"]
    if owner is None or str(owner) != caller.user_id:
        raise HTTPException(404, "ladder not found")


# ── Request models ────────────────────────────────────────────────────────────

def _coerce_id(v):
    """Coerce numeric user/account IDs to str. Portal sends integers; backend stores strings."""
    return None if v is None else str(v)


class CreateSessionRequest(BaseModel):
    config: Dict[str, Any] = Field(..., description="TradingSessionConfig dict")
    mode: str = Field("paper", description="'paper' (default, no real orders) | 'live'")
    # PHASE-2 MULTI-TENANT (additive, optional). Both None → operator/global
    # session, byte-for-byte as today. The portal supplies user_id server-side
    # (the authenticated user) + broker_account_id chosen from the user's vault.
    user_id: Optional[str] = Field(None, description="Portal user id (optional)")
    broker_account_id: Optional[str] = Field(
        None, description="Vaulted broker account to trade (optional)")

    @field_validator('user_id', 'broker_account_id', mode='before')
    @classmethod
    def _coerce_str(cls, v): return _coerce_id(v)


class PreviewRequest(BaseModel):
    config: Dict[str, Any] = Field(..., description="TradingSessionConfig dict")
    mode: str = Field("paper", description="'paper' (default) | 'live' — sizing "
                      "is identical; only LTP/margin source differs")
    user_id: Optional[str] = Field(None, description="Portal user id (optional)")
    broker_account_id: Optional[str] = Field(
        None, description="Vaulted broker account to size against (optional)")

    @field_validator('user_id', 'broker_account_id', mode='before')
    @classmethod
    def _coerce_str(cls, v): return _coerce_id(v)


# ── PHASE-2 MULTI-TENANT broker-account (vault) request models ───────────────

class ConnectBrokerAccountRequest(BaseModel):
    user_id: str = Field(..., description="Portal user id (owner of the account)")

    @field_validator('user_id', mode='before')
    @classmethod
    def _coerce_str(cls, v): return _coerce_id(v)
    broker: str = Field(..., description="zerodha|upstox|angel|dhan|fyers")
    account_label: str = Field(..., description="User-chosen label, e.g. 'Main Kite'")
    api_key: str = Field(..., description="Broker app api_key")
    api_secret: str = Field(..., description="Broker app api_secret (encrypted at rest)")


class RefreshTokenRequest(BaseModel):
    user_id: Optional[str] = Field(None, description="Portal user id (enforced if given)")
    request_token: str = Field(..., description="Broker request_token from the login redirect")

    # The portal sends user_id as an INTEGER (user.id); the backend stores/compares
    # it as text. Coerce before validation — WITHOUT this, a numeric user_id fails
    # the `str` type check with HTTP 422 and the token exchange never runs (this is
    # the Activate-step 422 bug). Mirrors ConnectBrokerAccountRequest / *Session.
    @field_validator('user_id', mode='before')
    @classmethod
    def _coerce_str(cls, v): return _coerce_id(v)


class StartSessionRequest(BaseModel):
    when: str = Field(
        "now",
        description="'now' (default, fire immediately — backward-compatible) | "
                    "'scheduled' (fire at config.entry_time IST)")


class SavePresetRequest(BaseModel):
    name: str
    config: Dict[str, Any]


# ── LADDER ORCHESTRATOR request models ────────────────────────────────────────

class CreateLadderRequest(BaseModel):
    total_capital: float = Field(..., description="Deployed-capital ceiling (₹)")
    order_product: str = Field("CNC", description="CNC | MTF (MIS rejected)")
    mode: str = Field("paper", description="'paper' (default) | 'live'")
    user_id: Optional[str] = Field(None, description="Portal user id (optional)")
    broker_account_id: Optional[str] = Field(
        None, description="Vaulted broker account (optional)")
    # Duration: month_end (default = last trading day of the start month) |
    # manual (runs until killed, end_date NULL) | an explicit YYYY-MM-DD.
    end_date_mode: str = Field(
        "month_end", description="month_end | manual")
    end_date: Optional[str] = Field(
        None, description="Explicit end date YYYY-MM-DD (overrides end_date_mode)")

    @field_validator('user_id', 'broker_account_id', mode='before')
    @classmethod
    def _coerce_str(cls, v): return _coerce_id(v)


class StartLadderRequest(BaseModel):
    # Optional. Omit / null → start NOW (RUNNING today, backward-compatible).
    # A future YYYY-MM-DD (IST) → SCHEDULED to auto-activate on that trading day.
    # A weekend/holiday is rejected 400 with a suggested next trading day.
    start_date: Optional[str] = Field(
        None, description="YYYY-MM-DD (IST). Omit → start now (RUNNING). "
                          "Future trading day → SCHEDULED.")


class KillLadderRequest(BaseModel):
    mode: str = Field(...,
                      description="flatten_now | stop_new_let_finish")


class DeleteSessionsRequest(BaseModel):
    session_ids: List[str] = Field(..., description="Session ids to delete")


class AddBrokerRequest(BaseModel):
    profile_id: str
    broker_name: str
    allocated_capital: float = 0.0
    symbols: Optional[List[str]] = None
    rank_range: Optional[List[int]] = None
    order_product: str = "CNC"
    instrument_type: str = "EQ"
    enabled: bool = True


# ── Helpers ────────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(IST).isoformat()


async def _maybe_run(coro):
    return await coro


def _delete_one_session(session_id: str) -> bool:
    """Stop a session's in-memory threads and delete its persisted rows.

    Steps (paper-safe, isolated table only):
      1. Stop the entry scheduler + tick driver (idempotent no-ops if absent) so
         neither daemon thread can fire/tick after the rows are gone.
      2. Delete the session's rows from autotrade_positions (+ snapshots +
         kill-switch log for completeness) — scoped strictly to session_id.
      3. Delete the autotrade_sessions row.

    NEVER touches falcon_position_state. For a live RUNNING session we still
    delete (paper has no real broker orders); a warning is logged so the
    operator has a trail. Returns True if a session row was deleted.
    """
    # 1. Stop background threads first (best-effort; never raise).
    try:
        entry_scheduler.stop_for_session(session_id)
    except Exception:  # pragma: no cover - defensive
        pass
    try:
        tick_driver.stop_for_session(session_id)
    except Exception:  # pragma: no cover - defensive
        pass

    with falcon_conn() as con:
        row = con.execute(
            "SELECT status, mode FROM autotrade_sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
        if row is None:
            return False
        if row["status"] == "RUNNING":
            log.warning("delete: session %s is RUNNING (mode=%s) — deleting anyway "
                        "(paper has no real broker orders)", session_id, row["mode"])
        # 2. Delete owned rows (scoped to session_id only — never any other
        # session and NEVER falcon_position_state).
        con.execute("DELETE FROM autotrade_positions WHERE session_id=?", (session_id,))
        con.execute("DELETE FROM autotrade_portfolio_snapshots WHERE session_id=?",
                    (session_id,))
        con.execute("DELETE FROM autotrade_kill_switch_log WHERE session_id=?",
                    (session_id,))
        # 3. Delete the session row.
        con.execute("DELETE FROM autotrade_sessions WHERE session_id=?", (session_id,))
        con.commit()
    log.info("delete: removed AutoTrade session %s + its positions", session_id)
    return True


# ── Endpoints ──────────────────────────────────────────────────────────────────

_VALID_UNIVERSE_FILTERS = ("all500", "nifty50", "nifty100", "nifty200", "fno")


@router.get("/autotrade/session/picks")
def session_picks(
    universe: str = "all500",
    top_n: int = 10,
    signal_date: Optional[str] = None,
):
    """Return the ranked picks list for today (or signal_date) after applying
    the universe filter. Creates NO session and places NO orders. Used by the
    frontend to show the manual stock picker before creating a session.

    Query params:
      universe     : all500 | nifty50 | nifty100 | nifty200 | fno  (default all500)
      top_n        : max number of picks to return (default 10)
      signal_date  : YYYY-MM-DD; omit to use the latest available signal date
    """
    if universe not in _VALID_UNIVERSE_FILTERS:
        raise HTTPException(
            400,
            f"invalid universe: {universe!r}. "
            f"Must be one of {_VALID_UNIVERSE_FILTERS}")
    if top_n < 1 or top_n > 200:
        raise HTTPException(400, "top_n must be between 1 and 200")
    if signal_date is not None:
        try:
            datetime.strptime(signal_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(400, f"signal_date must be YYYY-MM-DD, got {signal_date!r}")
    try:
        picks = load_falcon_picks(
            top_n=top_n,
            universe_filter=universe,
            signal_date=signal_date)
    except Exception as e:
        log.exception("session/picks failed: %s", e)
        raise HTTPException(500, f"picks fetch failed: {e}")

    # Resolve the actual signal_date used (may differ if signal_date was None)
    used_date: Optional[str] = None
    if picks:
        # All picks share the same signal_date; read it from the first pick's
        # rank ordering in the DB. Since load_falcon_picks returns the rows from
        # the latest (or provided) date, re-read it from the DB here.
        with falcon_conn() as con:
            row = con.execute(
                "SELECT MAX(signal_date) FROM falcon_signals_live"
            ).fetchone()
            if row:
                used_date = row[0]
    resolved_date = signal_date or used_date or datetime.now(IST).strftime("%Y-%m-%d")

    return {
        "signal_date": resolved_date,
        "universe_filter": universe,
        "top_n": top_n,
        "picks": [
            {
                "rank": p.rank,
                "symbol": p.symbol,
                "sector": p.sector,
                "score": p.score,
                "n_fires": p.n_fires,
                "avg_lift": p.avg_lift,
                "close_at_signal": p.close_at_signal,
            }
            for p in picks
        ],
    }


@router.post("/autotrade/session/create")
def session_create(req: CreateSessionRequest,
                   caller: Caller = Depends(resolve_caller)):
    caller = _caller(caller)
    try:
        cfg = TradingSessionConfig.from_dict(req.config)
        cfg.validate()
    except Exception as e:
        raise HTTPException(400, f"invalid config: {e}")
    mode = req.mode if req.mode in ("paper", "live") else "paper"
    # TENANT ISOLATION: a non-admin user MUST own the session they create — the
    # client-supplied user_id is ignored so a user can't create a session owned
    # by someone else. Admin (operator) keeps full control of user_id.
    owner_user_id = req.user_id if caller.is_admin else caller.user_id
    try:
        # PHASE-2: user_id/broker_account_id default None → operator/global
        # session, unchanged. A bound account is validated to exist + be owned.
        sess = TradingSession.create(
            cfg, mode=mode, user_id=owner_user_id,
            broker_account_id=req.broker_account_id)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"session_id": sess.session_id, "mode": mode, "status": "CREATED",
            "user_id": owner_user_id, "broker_account_id": req.broker_account_id}


@router.post("/autotrade/preview")
def autotrade_preview(req: PreviewRequest,
                      caller: Caller = Depends(resolve_caller)):
    """Config-time sizing preview — creates NO session and places NO orders.

    Sizes the Falcon picks exactly as session start would (CapitalAllocator +
    per-position MTF margin/qty) and returns the estimated invested_basis,
    total_allocated_capital, leverage, the per-position rows, and the same
    kill_preview (potential profit at +pct / loss at -pct, in ₹ and as a % on
    both bases). Operator-token gated (router-level dependency). Powers the UI
    preview before Start."""
    try:
        cfg = TradingSessionConfig.from_dict(req.config)
        cfg.validate()
    except Exception as e:
        raise HTTPException(400, f"invalid config: {e}")
    caller = _caller(caller)
    mode = req.mode if req.mode in ("paper", "live") else "paper"
    # TENANT ISOLATION: a non-admin caller sizes against their OWN vaulted creds;
    # a client-supplied user_id is ignored so a user can't preview/size against
    # someone else's broker account. Admin keeps full control.
    scope_user_id = req.user_id if caller.is_admin else caller.user_id
    try:
        return preview_session_sizing(
            cfg, mode=mode, user_id=scope_user_id,
            broker_account_id=req.broker_account_id)
    except Exception as e:
        log.exception("preview failed: %s", e)
        raise HTTPException(500, f"preview failed: {e}")


@router.post("/autotrade/session/{session_id}/start")
async def session_start(session_id: str,
                        req: Optional[StartSessionRequest] = None,
                        caller: Caller = Depends(resolve_caller)):
    """Start a session. Optional JSON body {"when": "now" | "scheduled"};
    defaults to "now" for backward-compatibility when the body is omitted."""
    _assert_session_access(session_id, caller)
    sess = TradingSession.load(session_id)
    if not sess:
        raise HTTPException(404, "session not found")
    when = (req.when if req else "now")
    if when not in ("now", "scheduled"):
        raise HTTPException(400, f"invalid 'when': {when!r} (now|scheduled)")
    try:
        return await sess.start(when=when)
    except Exception as e:
        log.exception("session start failed: %s", e)
        raise HTTPException(500, f"start failed: {e}")


@router.get("/autotrade/session/{session_id}/status")
def session_status(session_id: str,
                   caller: Caller = Depends(resolve_caller)):
    _assert_session_access(session_id, caller)
    sess = TradingSession.load(session_id)
    if not sess:
        raise HTTPException(404, "session not found")
    return sess.status()


@router.post("/autotrade/session/{session_id}/kill")
async def session_kill(session_id: str,
                       caller: Caller = Depends(resolve_caller)):
    _assert_session_access(session_id, caller)
    sess = TradingSession.load(session_id)
    if not sess:
        raise HTTPException(404, "session not found")
    return await sess.kill(reason="OPERATOR")


@router.get("/autotrade/session/{session_id}/positions")
def session_positions(session_id: str,
                      caller: Caller = Depends(resolve_caller)):
    _assert_session_access(session_id, caller)
    sess = TradingSession.load(session_id)
    if not sess:
        raise HTTPException(404, "session not found")
    return {"session_id": session_id, "positions": sess.positions()}


@router.get("/autotrade/session/{session_id}/journal")
def session_journal(session_id: str,
                    caller: Caller = Depends(resolve_caller)):
    """Daily Trade Journal for a session — available for CREATED/RUNNING/CLOSED.

    Builds a structured summary + per-position journal from autotrade_sessions
    and autotrade_positions (never falcon_position_state).  Open positions
    contribute unrealised_pnl; closed positions contribute realised_pnl.
    Returns 404 if the session_id is not found.
    """
    _assert_session_access(session_id, caller)
    return build_journal(session_id)


@router.get("/autotrade/sessions")
def session_list(user_id: Optional[str] = None,
                 caller: Caller = Depends(resolve_caller)):
    """List recent sessions (newest first) so the UI can show + resume them.

    TENANT ISOLATION:
      * admin (operator) → full operator view. Honours an optional ?user_id=<id>
        to scope to one user (today's behaviour), else all sessions.
      * non-admin user   → ONLY sessions STRICTLY owned by the caller
        (owner_user_id=caller.user_id → WHERE s.user_id = ?, with NO
        'OR user_id IS NULL' branch). NULL-owned / other users' sessions are
        never returned. A client-supplied ?user_id is ignored for non-admins."""
    caller = _caller(caller)
    if caller.is_admin:
        return {"sessions": TradingSession.list_sessions(user_id=user_id)}
    return {"sessions": TradingSession.list_sessions(
        owner_user_id=caller.user_id)}


@router.post("/autotrade/sessions/delete")
def sessions_delete(req: DeleteSessionsRequest,
                    caller: Caller = Depends(resolve_caller)):
    """Bulk-delete paper sessions. For each id: assert the caller owns it (admin
    may delete any), then stop its tick driver + entry scheduler, delete its
    autotrade_positions rows, delete the session row. Operator-token gated
    (router-level dependency). Paper-safe: no real broker orders are cancelled;
    live RUNNING sessions are still deleted with a logged warning. Returns
    {deleted: n, ids: [...]} — ids the caller could NOT access (missing or not
    owned) are silently skipped (no existence leak)."""
    caller = _caller(caller)
    deleted: List[str] = []
    for sid in req.session_ids:
        try:
            # Ownership gate: skip (don't raise) ids the caller can't access so a
            # bulk request over a mix stays non-enumerating and best-effort.
            try:
                _assert_session_access(sid, caller)
            except HTTPException:
                continue
            if _delete_one_session(sid):
                deleted.append(sid)
        except Exception as e:
            log.exception("delete failed for session %s: %s", sid, e)
    return {"deleted": len(deleted), "ids": deleted}


@router.delete("/autotrade/session/{session_id}")
def session_delete(session_id: str,
                   caller: Caller = Depends(resolve_caller)):
    """Single-session delete — same logic as the bulk endpoint."""
    _assert_session_access(session_id, caller)
    ok = _delete_one_session(session_id)
    if not ok:
        raise HTTPException(404, "session not found")
    return {"deleted": 1, "ids": [session_id]}


# ── LADDER ORCHESTRATOR endpoints (operator-token gated + per-user ownership) ──

@router.post("/autotrade/ladder/create")
def ladder_create(req: CreateLadderRequest,
                  caller: Caller = Depends(resolve_caller)):
    """Create a positional auto-ladder campaign (RUNNING-capable; opens nothing
    until started + the daily tick). Rejects MIS / non-CNC-MTF products. Freezes
    per_basket_capital = total_capital / 3."""
    from ..ladder import LadderCampaign
    caller = _caller(caller)
    mode = req.mode if req.mode in ("paper", "live") else "paper"
    owner_user_id = req.user_id if caller.is_admin else caller.user_id
    try:
        lad = LadderCampaign.create(
            total_capital=req.total_capital, order_product=req.order_product,
            mode=mode, user_id=owner_user_id,
            broker_account_id=req.broker_account_id,
            end_date=req.end_date, end_date_mode=req.end_date_mode)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return lad.to_status()


@router.post("/autotrade/ladder/{ladder_id}/start")
def ladder_start(ladder_id: str,
                 req: Optional[StartLadderRequest] = None,
                 caller: Caller = Depends(resolve_caller)):
    """Start a campaign. Optional JSON body {"start_date": "YYYY-MM-DD"}:
      * omitted / null → start NOW → RUNNING (backward-compatible one-step).
      * future trading day → SCHEDULED (auto-activates on that day).
    A weekend/holiday start_date returns HTTP 400 with a structured `detail`
    carrying the human message + the suggested next trading day, so the frontend
    can offer "did you mean {suggested_date}?".
    """
    from ..ladder import LadderCampaign
    _assert_ladder_access(ladder_id, caller)
    lad = LadderCampaign.load(ladder_id)
    if not lad:
        raise HTTPException(404, "ladder not found")
    start_date = req.start_date if req else None
    try:
        lad.start(start_date=start_date)
    except ValueError as e:
        # The engine embeds a "||suggested=YYYY-MM-DD" sentinel on the
        # non-trading-day rejection. Split it off, surface a structured 400.
        raw = str(e)
        message, _, tail = raw.partition("||suggested=")
        suggested = tail.strip() or None
        detail: Dict[str, Any] = {"message": message.strip()}
        if suggested:
            detail["suggested_date"] = suggested
            detail["code"] = "NON_TRADING_START_DATE"
        raise HTTPException(400, detail)
    return lad.to_status()


@router.post("/autotrade/ladder/{ladder_id}/pause")
def ladder_pause(ladder_id: str, caller: Caller = Depends(resolve_caller)):
    from ..ladder import LadderCampaign
    _assert_ladder_access(ladder_id, caller)
    lad = LadderCampaign.load(ladder_id)
    if not lad:
        raise HTTPException(404, "ladder not found")
    try:
        lad.pause()
    except ValueError as e:
        raise HTTPException(400, str(e))
    return lad.to_status()


@router.post("/autotrade/ladder/{ladder_id}/resume")
def ladder_resume(ladder_id: str, caller: Caller = Depends(resolve_caller)):
    from ..ladder import LadderCampaign
    _assert_ladder_access(ladder_id, caller)
    lad = LadderCampaign.load(ladder_id)
    if not lad:
        raise HTTPException(404, "ladder not found")
    try:
        lad.resume()
    except ValueError as e:
        raise HTTPException(400, str(e))
    return lad.to_status()


@router.post("/autotrade/ladder/{ladder_id}/kill")
async def ladder_kill(ladder_id: str, req: KillLadderRequest,
                      caller: Caller = Depends(resolve_caller)):
    """Kill a campaign with a MODE: flatten_now (immediately flatten every open
    basket) OR stop_new_let_finish (stop opening new, let open baskets exit
    naturally)."""
    from ..ladder import LadderCampaign
    _assert_ladder_access(ladder_id, caller)
    lad = LadderCampaign.load(ladder_id)
    if not lad:
        raise HTTPException(404, "ladder not found")
    try:
        return await lad.kill(mode=req.mode)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.get("/autotrade/ladder/{ladder_id}/status")
def ladder_status(ladder_id: str, caller: Caller = Depends(resolve_caller)):
    from ..ladder import LadderCampaign
    _assert_ladder_access(ladder_id, caller)
    lad = LadderCampaign.load(ladder_id)
    if not lad:
        raise HTTPException(404, "ladder not found")
    return lad.to_status()


@router.get("/autotrade/ladders")
def ladders_list(user_id: Optional[str] = None,
                 caller: Caller = Depends(resolve_caller)):
    """List campaigns (newest first). Admin → all (honours ?user_id); non-admin →
    only the caller's own ladders."""
    from ..ladder import LadderCampaign
    caller = _caller(caller)
    if caller.is_admin:
        return {"ladders": LadderCampaign.list_ladders(user_id=user_id)}
    return {"ladders": LadderCampaign.list_ladders(user_id=caller.user_id)}


@router.post("/autotrade/config/save")
def config_save(req: SavePresetRequest):
    try:
        cfg = TradingSessionConfig.from_dict(req.config)
        pid = cfgmod.save_preset(req.name, cfg)
    except Exception as e:
        raise HTTPException(400, f"save failed: {e}")
    return {"id": pid, "name": req.name}


@router.get("/autotrade/config/list")
def config_list():
    return {"presets": cfgmod.list_presets()}


@router.post("/autotrade/broker/add")
def broker_add(req: AddBrokerRequest):
    rr = req.rank_range
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_broker_profiles
               (profile_id, broker_name, allocated_capital, symbols_json,
                rank_low, rank_high, order_product, instrument_type, enabled,
                creds_configured, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(profile_id) DO UPDATE SET
                   broker_name=excluded.broker_name,
                   allocated_capital=excluded.allocated_capital,
                   symbols_json=excluded.symbols_json,
                   rank_low=excluded.rank_low, rank_high=excluded.rank_high,
                   order_product=excluded.order_product,
                   instrument_type=excluded.instrument_type,
                   enabled=excluded.enabled, updated_at=excluded.updated_at""",
            (req.profile_id, req.broker_name, req.allocated_capital,
             None if req.symbols is None else __import__("json").dumps(req.symbols),
             rr[0] if rr else None, rr[1] if rr and len(rr) > 1 else None,
             req.order_product, req.instrument_type, 1 if req.enabled else 0,
             0, _now(), _now()),
        )
        con.commit()
    return {"profile_id": req.profile_id, "status": "saved"}


@router.get("/autotrade/broker/list")
def broker_list():
    with falcon_conn() as con:
        rows = con.execute(
            "SELECT * FROM autotrade_broker_profiles ORDER BY profile_id"
        ).fetchall()
    return {"brokers": [dict(r) for r in rows]}


# ── PHASE-2 MULTI-TENANT: broker-account (credential vault) endpoints ─────────
# Operator-token gated at the router level (same as everything here); user
# isolation is enforced by the user_id supplied by the portal (server-side).
# Secrets are encrypted at rest and NEVER returned — only status + masked
# previews. All endpoints are NO-OP-safe when the vault is disabled (no
# FALCON_VAULT_KEY): connect/refresh return a clear 400; list returns [].

@router.post("/autotrade/broker-account")
def broker_account_connect(req: ConnectBrokerAccountRequest,
                           caller: Caller = Depends(resolve_caller)):
    """Connect (store) a broker account: encrypt api_secret at rest under the
    vault, status PENDING (no token yet). Re-posting the same
    (user_id, broker, account_label) UPDATES the creds in place. Returns the
    PUBLIC dict (no secrets). 400 if the vault is disabled.

    TENANT ISOLATION: a non-admin caller may only connect an account under their
    OWN user_id (client-supplied user_id is forced to the caller's id)."""
    caller = _caller(caller)
    from .. import vault
    owner = req.user_id if caller.is_admin else caller.user_id
    try:
        return vault.put_account(
            user_id=owner, broker=req.broker,
            account_label=req.account_label, api_key=req.api_key,
            api_secret=req.api_secret)
    except vault.VaultDisabledError as e:
        raise HTTPException(400, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.get("/autotrade/broker-accounts")
def broker_accounts_list(user_id: str,
                         caller: Caller = Depends(resolve_caller)):
    """List a user's broker accounts (masked, no secrets). Scoped to user_id.

    TENANT ISOLATION: a non-admin caller may only list their OWN accounts (a
    user_id != caller → 404); admin unrestricted."""
    scope = _assert_user_scope(user_id, caller)
    from .. import vault
    return {"accounts": vault.list_accounts(user_id=scope),
            "vault_enabled": vault.vault_enabled()}


@router.delete("/autotrade/broker-account/{broker_account_id}")
def broker_account_delete(broker_account_id: str, user_id: str,
                          caller: Caller = Depends(resolve_caller)):
    """Delete a broker account, scoped to (broker_account_id, user_id).

    TENANT ISOLATION: a non-admin caller may only delete under their OWN user_id;
    admin unrestricted."""
    scope = _assert_user_scope(user_id, caller)
    from .. import vault
    ok = vault.delete_account(broker_account_id, user_id=scope)
    if not ok:
        raise HTTPException(404, "broker account not found")
    return {"deleted": 1, "broker_account_id": broker_account_id}


@router.get("/autotrade/broker-account/{broker_account_id}/login-url")
def broker_account_login_url(broker_account_id: str,
                             user_id: Optional[str] = None,
                             redirect_uri: str = "",
                             state: str = "",
                             caller: Caller = Depends(resolve_caller)):
    """Build the broker login URL for THIS account (so the user can mint a fresh
    token). BROKER-AGNOSTIC: routes through account_lifecycle → the registry's
    auth provider for whatever broker this account uses (Zerodha, Rupeezy, …).
    400 if the vault is disabled / account not found / adapter not certified.

    TENANT ISOLATION: a non-admin caller is scoped to their OWN user_id (so an
    account they don't own is 404-invisible)."""
    scope = _assert_user_scope(user_id, caller)
    from .. import vault
    from ..broker import account_lifecycle
    acct = vault.get_account_public(broker_account_id, user_id=scope)
    if acct is None:
        raise HTTPException(404, "broker account not found")
    try:
        url = account_lifecycle.login_url(
            scope, broker_account_id, redirect_uri=redirect_uri, state=state)
        return {"broker_account_id": broker_account_id, "login_url": url}
    except account_lifecycle.AccountLifecycleError as e:
        raise HTTPException(400, str(e))


@router.post("/autotrade/broker-account/{broker_account_id}/refresh-token")
def broker_account_refresh_token(broker_account_id: str,
                                 req: RefreshTokenRequest,
                                 caller: Caller = Depends(resolve_caller)):
    """Exchange a broker request_token for an access_token and store it
    (encrypted) in the vault → status ACTIVE, token_date today. Zerodha
    implemented; other brokers are a follow-up. Returns the PUBLIC account dict
    (no secrets).

    TENANT ISOLATION: a non-admin caller is scoped to their OWN user_id."""
    scope = _assert_user_scope(req.user_id, caller)
    from .. import vault
    from ..broker import account_lifecycle
    acct = vault.get_account_public(broker_account_id, user_id=scope)
    if acct is None:
        raise HTTPException(404, "broker account not found")
    try:
        # BROKER-AGNOSTIC: complete_connect resolves the account's broker via the
        # registry, exchanges the request_token through THAT broker's auth
        # provider, and stores the tokens (access + optional refresh) in the vault.
        return account_lifecycle.complete_connect(
            scope, broker_account_id, req.request_token)
    except account_lifecycle.AccountLifecycleError as e:
        raise HTTPException(400, str(e))


@router.get("/autotrade/brokers/supported")
def brokers_supported(caller: Caller = Depends(resolve_caller)):
    """List brokers the platform can connect (for the onboarding dropdown) with
    each broker's capabilities + whether it is live-certified. Broker-agnostic:
    sourced from the registry, so adding a broker surfaces here automatically."""
    from ..broker import registry
    return {"brokers": registry.list_supported()}


@router.get("/autotrade/broker-account/{broker_account_id}/health")
def broker_account_health(broker_account_id: str,
                          user_id: Optional[str] = None,
                          caller: Caller = Depends(resolve_caller)):
    """Live connection-health probe for an account (auth.validate → records
    last_health_*). Broker-agnostic. Non-admin scoped to their own user_id."""
    scope = _assert_user_scope(user_id, caller)
    from .. import vault
    from ..broker import account_lifecycle
    acct = vault.get_account_public(broker_account_id, user_id=scope)
    if acct is None:
        raise HTTPException(404, "broker account not found")
    try:
        health = account_lifecycle.health_check(scope, broker_account_id)
        return {"broker_account_id": broker_account_id,
                "ok": health.ok, "status": health.status, "detail": health.detail}
    except account_lifecycle.AccountLifecycleError as e:
        raise HTTPException(400, str(e))
