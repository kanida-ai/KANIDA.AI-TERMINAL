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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

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


# ── Request models ────────────────────────────────────────────────────────────

class CreateSessionRequest(BaseModel):
    config: Dict[str, Any] = Field(..., description="TradingSessionConfig dict")
    mode: str = Field("paper", description="'paper' (default, no real orders) | 'live'")
    # PHASE-2 MULTI-TENANT (additive, optional). Both None → operator/global
    # session, byte-for-byte as today. The portal supplies user_id server-side
    # (the authenticated user) + broker_account_id chosen from the user's vault.
    user_id: Optional[str] = Field(None, description="Portal user id (optional)")
    broker_account_id: Optional[str] = Field(
        None, description="Vaulted broker account to trade (optional)")


class PreviewRequest(BaseModel):
    config: Dict[str, Any] = Field(..., description="TradingSessionConfig dict")
    mode: str = Field("paper", description="'paper' (default) | 'live' — sizing "
                      "is identical; only LTP/margin source differs")
    user_id: Optional[str] = Field(None, description="Portal user id (optional)")
    broker_account_id: Optional[str] = Field(
        None, description="Vaulted broker account to size against (optional)")


# ── PHASE-2 MULTI-TENANT broker-account (vault) request models ───────────────

class ConnectBrokerAccountRequest(BaseModel):
    user_id: str = Field(..., description="Portal user id (owner of the account)")
    broker: str = Field(..., description="zerodha|upstox|angel|dhan|fyers")
    account_label: str = Field(..., description="User-chosen label, e.g. 'Main Kite'")
    api_key: str = Field(..., description="Broker app api_key")
    api_secret: str = Field(..., description="Broker app api_secret (encrypted at rest)")


class RefreshTokenRequest(BaseModel):
    user_id: Optional[str] = Field(None, description="Portal user id (enforced if given)")
    request_token: str = Field(..., description="Broker request_token from the login redirect")


class StartSessionRequest(BaseModel):
    when: str = Field(
        "now",
        description="'now' (default, fire immediately — backward-compatible) | "
                    "'scheduled' (fire at config.entry_time IST)")


class SavePresetRequest(BaseModel):
    name: str
    config: Dict[str, Any]


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
def session_create(req: CreateSessionRequest):
    try:
        cfg = TradingSessionConfig.from_dict(req.config)
        cfg.validate()
    except Exception as e:
        raise HTTPException(400, f"invalid config: {e}")
    mode = req.mode if req.mode in ("paper", "live") else "paper"
    try:
        # PHASE-2: user_id/broker_account_id default None → operator/global
        # session, unchanged. A bound account is validated to exist + be owned.
        sess = TradingSession.create(
            cfg, mode=mode, user_id=req.user_id,
            broker_account_id=req.broker_account_id)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"session_id": sess.session_id, "mode": mode, "status": "CREATED",
            "user_id": req.user_id, "broker_account_id": req.broker_account_id}


@router.post("/autotrade/preview")
def autotrade_preview(req: PreviewRequest):
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
    mode = req.mode if req.mode in ("paper", "live") else "paper"
    try:
        return preview_session_sizing(
            cfg, mode=mode, user_id=req.user_id,
            broker_account_id=req.broker_account_id)
    except Exception as e:
        log.exception("preview failed: %s", e)
        raise HTTPException(500, f"preview failed: {e}")


@router.post("/autotrade/session/{session_id}/start")
async def session_start(session_id: str,
                        req: Optional[StartSessionRequest] = None):
    """Start a session. Optional JSON body {"when": "now" | "scheduled"};
    defaults to "now" for backward-compatibility when the body is omitted."""
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
def session_status(session_id: str):
    sess = TradingSession.load(session_id)
    if not sess:
        raise HTTPException(404, "session not found")
    return sess.status()


@router.post("/autotrade/session/{session_id}/kill")
async def session_kill(session_id: str):
    sess = TradingSession.load(session_id)
    if not sess:
        raise HTTPException(404, "session not found")
    return await sess.kill(reason="OPERATOR")


@router.get("/autotrade/session/{session_id}/positions")
def session_positions(session_id: str):
    sess = TradingSession.load(session_id)
    if not sess:
        raise HTTPException(404, "session not found")
    return {"session_id": session_id, "positions": sess.positions()}


@router.get("/autotrade/session/{session_id}/journal")
def session_journal(session_id: str):
    """Daily Trade Journal for a session — available for CREATED/RUNNING/CLOSED.

    Builds a structured summary + per-position journal from autotrade_sessions
    and autotrade_positions (never falcon_position_state).  Open positions
    contribute unrealised_pnl; closed positions contribute realised_pnl.
    Returns 404 if the session_id is not found.
    """
    return build_journal(session_id)


@router.get("/autotrade/sessions")
def session_list(user_id: Optional[str] = None):
    """List recent sessions (newest first) so the UI can show + resume them.

    PHASE-2: pass ?user_id=<id> to scope to one user's sessions (per-user
    isolation). Omit it for the full operator view (today's behaviour)."""
    return {"sessions": TradingSession.list_sessions(user_id=user_id)}


@router.post("/autotrade/sessions/delete")
def sessions_delete(req: DeleteSessionsRequest):
    """Bulk-delete paper sessions. For each id: stop its tick driver + entry
    scheduler, delete its autotrade_positions rows, delete the session row.
    Operator-token gated (router-level dependency). Paper-safe: no real broker
    orders are cancelled; live RUNNING sessions are still deleted with a logged
    warning. Returns {deleted: n, ids: [...]} where ids are the rows actually
    removed (missing ids are silently skipped)."""
    deleted: List[str] = []
    for sid in req.session_ids:
        try:
            if _delete_one_session(sid):
                deleted.append(sid)
        except Exception as e:
            log.exception("delete failed for session %s: %s", sid, e)
    return {"deleted": len(deleted), "ids": deleted}


@router.delete("/autotrade/session/{session_id}")
def session_delete(session_id: str):
    """Single-session delete — same logic as the bulk endpoint."""
    ok = _delete_one_session(session_id)
    if not ok:
        raise HTTPException(404, "session not found")
    return {"deleted": 1, "ids": [session_id]}


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
def broker_account_connect(req: ConnectBrokerAccountRequest):
    """Connect (store) a broker account: encrypt api_secret at rest under the
    vault, status PENDING (no token yet). Re-posting the same
    (user_id, broker, account_label) UPDATES the creds in place. Returns the
    PUBLIC dict (no secrets). 400 if the vault is disabled."""
    from .. import vault
    try:
        return vault.put_account(
            user_id=req.user_id, broker=req.broker,
            account_label=req.account_label, api_key=req.api_key,
            api_secret=req.api_secret)
    except vault.VaultDisabledError as e:
        raise HTTPException(400, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.get("/autotrade/broker-accounts")
def broker_accounts_list(user_id: str):
    """List a user's broker accounts (masked, no secrets). Scoped to user_id."""
    from .. import vault
    return {"accounts": vault.list_accounts(user_id=user_id),
            "vault_enabled": vault.vault_enabled()}


@router.delete("/autotrade/broker-account/{broker_account_id}")
def broker_account_delete(broker_account_id: str, user_id: str):
    """Delete a broker account, scoped to (broker_account_id, user_id)."""
    from .. import vault
    ok = vault.delete_account(broker_account_id, user_id=user_id)
    if not ok:
        raise HTTPException(404, "broker account not found")
    return {"deleted": 1, "broker_account_id": broker_account_id}


@router.get("/autotrade/broker-account/{broker_account_id}/login-url")
def broker_account_login_url(broker_account_id: str, user_id: Optional[str] = None):
    """Build the broker login URL for THIS account (so the user can mint a fresh
    daily token). Zerodha implemented; other brokers' login flows are a
    follow-up. 400 if the vault is disabled / account not found."""
    from ..broker import zerodha_auth
    from .. import vault
    acct = vault.get_account_public(broker_account_id, user_id=user_id)
    if acct is None:
        raise HTTPException(404, "broker account not found")
    if acct["broker"] != "zerodha":
        raise HTTPException(
            400, f"login-url not implemented for broker {acct['broker']!r} "
            "(Zerodha verified; others are a follow-up)")
    try:
        return {"broker_account_id": broker_account_id,
                "login_url": zerodha_auth.login_url(broker_account_id,
                                                    user_id=user_id)}
    except zerodha_auth.AccountAuthError as e:
        raise HTTPException(400, str(e))


@router.post("/autotrade/broker-account/{broker_account_id}/refresh-token")
def broker_account_refresh_token(broker_account_id: str,
                                 req: RefreshTokenRequest):
    """Exchange a broker request_token for an access_token and store it
    (encrypted) in the vault → status ACTIVE, token_date today. Zerodha
    implemented; other brokers are a follow-up. Returns the PUBLIC account dict
    (no secrets)."""
    from ..broker import zerodha_auth
    from .. import vault
    acct = vault.get_account_public(broker_account_id, user_id=req.user_id)
    if acct is None:
        raise HTTPException(404, "broker account not found")
    if acct["broker"] != "zerodha":
        raise HTTPException(
            400, f"refresh-token not implemented for broker {acct['broker']!r} "
            "(Zerodha verified; others are a follow-up)")
    try:
        return zerodha_auth.exchange_token(
            broker_account_id, req.request_token, user_id=req.user_id)
    except zerodha_auth.AccountAuthError as e:
        raise HTTPException(400, str(e))
