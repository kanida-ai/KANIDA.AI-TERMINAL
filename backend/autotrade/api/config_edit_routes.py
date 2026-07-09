"""LIVE CONFIG EDIT API — PATCH /api/power/autotrade/{session|ladder}/{id}/config.

Portal-facing, power_jwt-gated (Bearer header OR the power_jwt cookie), scoped to
the caller's user_id (a non-admin edits ONLY their own sessions/ladders; an admin
edits any). Mounted UNDER /api/power (no operator-token gate) alongside the other
power_user routers — strictly authenticated by the portal JWT here, reading/
writing only the isolated autotrade_* tables (never falcon_position_state).

Lets the operator hot-edit the RISK/EXIT knobs of a RUNNING session / campaign
post-launch and Apply. The running session hot-reloads them within one tick
WITHOUT a restart and WITHOUT touching live positions / invested_basis / order-ids
/ trail-armed state (see TradingSession.maybe_reload_config).

WHITELIST (session): arm_pct, floor_pct, trail_giveback_pct, stop_pct,
  per_position_stop_pct, per_position_target_pct, square_off_time,
  mis_square_off_time, max_hold_sessions.  Everything else (capital / product /
  picks / entry timing / strategy / kill-switch pct) is LOCKED on a running
  session — a non-whitelisted key is a hard 400.

WHITELIST (ladder): the same risk/exit knobs (minus the intraday square-off
  times, inert for a positional child) which UPDATE the child-config template AND
  cascade to every RUNNING child; PLUS per_basket_capital / total_capital /
  end_date which affect FUTURE spawns only (never a running child's frozen basis).

Endpoints:
  PATCH /api/power/autotrade/session/{session_id}/config[?dry_run=true]
  PATCH /api/power/autotrade/ladder/{ladder_id}/config[?dry_run=true]
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request

from falcon.db import falcon_conn

from ..config import (
    TradingSessionConfig, _parse_clock_to_seconds, validate_step_lock_ladder)
from ..ladder import LadderCampaign
from ..session import LIVE_EDITABLE_SESSION_FIELDS, TradingSession
from .autotrade_routes import _assert_ladder_access, _assert_session_access
from .pnl_routes import require_power_caller

log = logging.getLogger("kanida.autotrade.config_edit.api")
IST = timezone(timedelta(hours=5, minutes=30))

router = APIRouter(prefix="/api/power/autotrade", tags=["Power-User"])


# ── Whitelists ────────────────────────────────────────────────────────────────

SESSION_WHITELIST = set(LIVE_EDITABLE_SESSION_FIELDS)

# The risk/exit knobs a ladder edit cascades to running children + stores in the
# child template (subset of the session whitelist meaningful for a positional
# child). Reuses the ladder's own declared set so the two never drift.
LADDER_RISK_EXIT = set(LadderCampaign.LADDER_CHILD_WHITELIST)
# Capital / duration knobs → FUTURE spawns only, never a running child.
LADDER_CAPITAL = {"per_basket_capital", "total_capital", "end_date"}
LADDER_WHITELIST = LADDER_RISK_EXIT | LADDER_CAPITAL

# Fields validated as FRACTION percentages in (0, 0.5).
_PCT_FIELDS = {
    "arm_pct", "floor_pct", "trail_giveback_pct", "stop_pct",
    "per_position_stop_pct", "per_position_target_pct",
    # PROFIT STEP-LOCK: large-day peak threshold is a fraction of capital.
    "trail_large_peak_pct",
}
_TIME_FIELDS = {"square_off_time", "mis_square_off_time"}


# ── Range validation ──────────────────────────────────────────────────────────

def _coerce_and_validate(field: str, value: Any) -> Any:
    """Range-validate ONE whitelisted field, returning the coerced value or
    raising HTTPException(400). Units are FRACTIONS (0.01 = 1%)."""
    if field in _PCT_FIELDS:
        try:
            v = float(value)
        except (TypeError, ValueError):
            raise HTTPException(400, f"{field} must be a number (fraction), got {value!r}")
        if not (0.0 < v < 0.5):
            raise HTTPException(
                400, f"{field} must be a fraction in (0, 0.5) (e.g. 0.03 = 3%), got {v}")
        return v
    if field in _TIME_FIELDS:
        try:
            _parse_clock_to_seconds(str(value))
        except ValueError:
            raise HTTPException(400, f"{field} must be an IST clock HH:MM:SS, got {value!r}")
        return str(value)
    if field == "max_hold_sessions":
        try:
            v = int(value)
        except (TypeError, ValueError):
            raise HTTPException(400, f"max_hold_sessions must be an integer, got {value!r}")
        if v < 0:
            raise HTTPException(400, f"max_hold_sessions must be an integer >= 0, got {v}")
        return v
    # ── PROFIT STEP-LOCK live-edit fields ─────────────────────────────────────
    if field == "trail_step_lock_enabled":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        if isinstance(value, str) and value.strip().lower() in (
                "true", "false", "1", "0", "yes", "no", "on", "off"):
            return value.strip().lower() in ("true", "1", "yes", "on")
        raise HTTPException(
            400, f"trail_step_lock_enabled must be a boolean, got {value!r}")
    if field == "trail_large_giveback_rel":
        try:
            v = float(value)
        except (TypeError, ValueError):
            raise HTTPException(
                400, f"trail_large_giveback_rel must be a number, got {value!r}")
        if not (0.0 < v < 1.0):
            raise HTTPException(
                400, "trail_large_giveback_rel must be a fraction in (0, 1) "
                     f"(e.g. 0.175 = trail 17.5% from peak), got {v}")
        return v
    if field == "trail_step_lock_ladder":
        # Validate the ladder shape exactly as config.validate() does (non-empty,
        # 0<lock<peak<1 per rung, strictly ascending by peak AND lock), then
        # normalise to plain [float, float] rungs. cfg.validate() re-checks the
        # cross-field set after the merge, but reject an obviously-bad ladder here
        # with a clear 400.
        try:
            validate_step_lock_ladder(value)
        except ValueError as e:
            raise HTTPException(400, str(e))
        return [[float(r[0]), float(r[1])] for r in value]
    # Ladder capital/duration.
    if field in ("per_basket_capital", "total_capital"):
        try:
            v = float(value)
        except (TypeError, ValueError):
            raise HTTPException(400, f"{field} must be a number, got {value!r}")
        if v <= 0:
            raise HTTPException(400, f"{field} must be > 0, got {v}")
        return v
    if field == "end_date":
        try:
            datetime.strptime(str(value).strip(), "%Y-%m-%d")
        except ValueError:
            raise HTTPException(400, f"end_date must be YYYY-MM-DD, got {value!r}")
        return str(value).strip()
    # Should be unreachable (caller checks whitelist membership first).
    raise HTTPException(400, f"unsupported field: {field}")


def _check_whitelist(body: Dict[str, Any], whitelist: set) -> None:
    """Reject a non-whitelisted / locked key with a 400 naming the offending
    key. Empty body → 400 (nothing to apply)."""
    if not isinstance(body, dict) or not body:
        raise HTTPException(400, "empty body: provide at least one editable field")
    for k in body:
        if k not in whitelist:
            raise HTTPException(
                400, {"code": "FIELD_NOT_EDITABLE",
                      "field": k,
                      "message": (f"'{k}' is not a live-editable field. "
                                  "Capital / product / picks / entry stay locked "
                                  "on a running session/campaign.")})


# ── Warning computation (shared by session + ladder-child) ────────────────────

def _stock_return(pos: Dict[str, Any]) -> Optional[float]:
    """Signed per-position return (long: (ltp-avg)/avg; short inverted), or None
    when un-markable."""
    try:
        avg = float(pos.get("avg_price") or 0.0)
        ltp = pos.get("ltp")
        if avg <= 0 or ltp is None:
            return None
        r = (float(ltp) - avg) / avg
        if str(pos.get("direction") or "long") == "short":
            r = -r
        return r
    except Exception:
        return None


def _session_warnings(sess: "TradingSession",
                      new_vals: Dict[str, Any],
                      now_ist: datetime) -> List[str]:
    """Human warnings for the effect of new thresholds on the CURRENT market
    state of one session's open positions. Never raises."""
    warnings: List[str] = []
    try:
        positions = sess.registry.get_open_positions()
    except Exception:
        positions = []

    # 1. Tightened per-position stop → a name already at/below the new stop.
    if "per_position_stop_pct" in new_vals:
        nstop = float(new_vals["per_position_stop_pct"])
        for p in positions:
            r = _stock_return(p)
            if r is not None and r <= -nstop:
                warnings.append(
                    f"{p.get('symbol')} at {r*100:.1f}% would exit next tick "
                    f"under the new {nstop*100:.2f}% per-position stop")

    # 2. Tightened BASKET stop (intraday_basket) → basket already at/below it.
    if "stop_pct" in new_vals and getattr(sess.config, "strategy", "") == "intraday_basket":
        nbstop = float(new_vals["stop_pct"])
        try:
            g = sess.monitor.compute_gross_return()  # allocated-capital basis
        except Exception:
            g = None
        if g is not None and g <= -nbstop:
            warnings.append(
                f"basket at {g*100:.1f}% (of capital) is already at/below the new "
                f"{nbstop*100:.2f}% basket stop — it would exit next tick")

    # 3. square_off_time already passed today → will flatten next tick.
    if "square_off_time" in new_vals:
        try:
            sq = _parse_clock_to_seconds(str(new_vals["square_off_time"]))
            nowsec = now_ist.hour * 3600 + now_ist.minute * 60 + now_ist.second
            if positions and nowsec >= sq:
                warnings.append(
                    f"square_off_time {new_vals['square_off_time']} has already "
                    f"passed ({now_ist.strftime('%H:%M:%S')} IST) — the basket "
                    f"will square off next tick")
        except ValueError:
            pass

    # 4. mis_square_off_time already passed on a MIS session.
    if "mis_square_off_time" in new_vals:
        try:
            mis = _parse_clock_to_seconds(str(new_vals["mis_square_off_time"]))
            nowsec = now_ist.hour * 3600 + now_ist.minute * 60 + now_ist.second
            if positions and sess.config.is_intraday_product() and nowsec >= mis:
                warnings.append(
                    f"mis_square_off_time {new_vals['mis_square_off_time']} has "
                    f"already passed — this MIS session will square off next tick")
        except ValueError:
            pass
    return warnings


def _split_new_vals(body: Dict[str, Any], keys: set) -> Dict[str, Any]:
    """Coerce + validate the subset of `body` whose keys are in `keys`."""
    out: Dict[str, Any] = {}
    for k in keys:
        if k in body:
            out[k] = _coerce_and_validate(k, body[k])
    return out


# ── SESSION endpoint ──────────────────────────────────────────────────────────

@router.patch("/session/{session_id}/config")
def patch_session_config(
    session_id: str,
    request: Request,
    body: Dict[str, Any] = Body(...),
    dry_run: bool = Query(False),
    caller=Depends(require_power_caller),
):
    """Live-edit a RUNNING session's risk/exit knobs. Whitelisted subset only.

    dry_run=true → compute + return warnings WITHOUT persisting.
    Applies → merge into config_json, bump config_version (+1), persist. The
    running tick_driver / ws_driver hot-reload the change within one tick."""
    _assert_session_access(session_id, caller)
    _check_whitelist(body, SESSION_WHITELIST)

    # Current row (status + version) — read fresh.
    with falcon_conn() as con:
        row = con.execute(
            "SELECT status, config_version FROM autotrade_sessions "
            "WHERE session_id=?", (session_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "session not found")
    status = row["status"]
    cur_version = int(dict(row).get("config_version") or 0)
    if status != "RUNNING":
        raise HTTPException(
            409, {"code": "NOT_RUNNING",
                  "message": f"session status is {status}; config is live-editable "
                             "only while RUNNING"})

    sess = TradingSession.load(session_id)
    if not sess:
        raise HTTPException(404, "session not found")

    # Coerce + range-validate each provided field.
    new_vals = _split_new_vals(body, SESSION_WHITELIST)

    # Apply onto a candidate config and full-validate (catches cross-field
    # constraints: floor<=arm, mis<square_off, square_off>entry, basket bounds…).
    for f, v in new_vals.items():
        setattr(sess.config, f, v)
    try:
        sess.config.validate()
    except ValueError as e:
        raise HTTPException(400, {"code": "INVALID_CONFIG", "message": str(e)})

    now = datetime.now(IST)
    warnings = _session_warnings(sess, new_vals, now)

    if dry_run:
        return {
            "ok": True, "session_id": session_id, "dry_run": True,
            "applied": new_vals, "rejected": [],
            "effective_config": {f: getattr(sess.config, f, None)
                                 for f in LIVE_EDITABLE_SESSION_FIELDS},
            "warnings": warnings, "config_version": cur_version,
        }

    # Persist: rewrite config_json + bump config_version atomically.
    new_json = sess.config.to_json()
    with falcon_conn() as con:
        con.execute(
            "UPDATE autotrade_sessions "
            "SET config_json=?, config_version=config_version+1 "
            "WHERE session_id=?", (new_json, session_id))
        con.commit()
        v2 = con.execute(
            "SELECT config_version FROM autotrade_sessions WHERE session_id=?",
            (session_id,)).fetchone()
    new_version = int(dict(v2)["config_version"]) if v2 else cur_version + 1
    log.info("session %s config edited v%d->%d by %s: %s", session_id,
             cur_version, new_version, caller.user_id, list(new_vals.keys()))
    return {
        "ok": True, "session_id": session_id, "dry_run": False,
        "applied": new_vals, "rejected": [],
        "effective_config": {f: getattr(sess.config, f, None)
                             for f in LIVE_EDITABLE_SESSION_FIELDS},
        "warnings": warnings, "config_version": new_version,
    }


# ── LADDER endpoint ───────────────────────────────────────────────────────────

@router.patch("/ladder/{ladder_id}/config")
def patch_ladder_config(
    ladder_id: str,
    request: Request,
    body: Dict[str, Any] = Body(...),
    dry_run: bool = Query(False),
    caller=Depends(require_power_caller),
):
    """Live-edit a ladder campaign. Risk/exit knobs update the child-config
    template (future spawns) AND cascade to every RUNNING child (each child's
    config_json merged + config_version bumped so its tick hot-reloads).
    Capital/duration (per_basket_capital/total_capital/end_date) affect FUTURE
    spawns only — running children's frozen basis is never touched."""
    _assert_ladder_access(ladder_id, caller)
    _check_whitelist(body, LADDER_WHITELIST)

    lad = LadderCampaign.load(ladder_id)
    if lad is None:
        raise HTTPException(404, "ladder not found")

    risk_vals = _split_new_vals(body, LADDER_RISK_EXIT)
    cap_vals = _split_new_vals(body, LADDER_CAPITAL)

    # Validate the risk/exit overrides against a candidate child config (so an
    # inconsistent set — e.g. floor>arm — is rejected before we store/cascade).
    if risk_vals:
        candidate = lad._build_child_config()   # includes existing template
        for f, v in risk_vals.items():
            setattr(candidate, f, v)
        try:
            candidate.validate()
        except ValueError as e:
            raise HTTPException(400, {"code": "INVALID_CONFIG", "message": str(e)})

    # Warnings: the effect of the new risk thresholds on each RUNNING child.
    now = datetime.now(IST)
    warnings: List[str] = []
    running = lad._child_rows(["RUNNING"])
    child_ids = [c["session_id"] for c in running]
    if risk_vals:
        for cid in child_ids:
            csess = TradingSession.load(cid)
            if csess is None:
                continue
            for w in _session_warnings(csess, risk_vals, now):
                warnings.append(f"[{cid[:8]}] {w}")

    # Effective template = current overrides merged with the new risk_vals.
    eff_template = dict(lad.child_config_overrides())
    eff_template.update(risk_vals)
    effective_config = dict(eff_template)
    for f in LADDER_CAPITAL:
        effective_config[f] = (cap_vals[f] if f in cap_vals
                               else getattr(lad, f, None))

    if dry_run:
        return {
            "ok": True, "ladder_id": ladder_id, "dry_run": True,
            "applied": {**risk_vals, **cap_vals}, "rejected": [],
            "effective_config": effective_config,
            "children_updated": [], "warnings": warnings,
            "config_version": int(lad.config_version or 0),
        }

    # ── Persist template + capital/duration on the ladder row ────────────────
    update_fields: Dict[str, Any] = {}
    if risk_vals:
        update_fields["child_config_json"] = json.dumps(eff_template)
    for f, v in cap_vals.items():
        update_fields[f] = v
    # Bump the ladder template version whenever anything changed.
    with falcon_conn() as con:
        if update_fields:
            cols = ", ".join(f"{k}=?" for k in update_fields)
            con.execute(
                f"UPDATE autotrade_ladders SET {cols}, "
                "config_version=config_version+1, updated_at=? WHERE ladder_id=?",
                list(update_fields.values()) + [_now_iso(), ladder_id])
        else:
            con.execute(
                "UPDATE autotrade_ladders SET config_version=config_version+1, "
                "updated_at=? WHERE ladder_id=?", (_now_iso(), ladder_id))
        con.commit()
        v2 = con.execute(
            "SELECT config_version FROM autotrade_ladders WHERE ladder_id=?",
            (ladder_id,)).fetchone()
    new_version = int(dict(v2)["config_version"]) if v2 else int(lad.config_version or 0) + 1

    # ── Cascade the risk/exit knobs to every RUNNING child ───────────────────
    children_updated: List[str] = []
    if risk_vals:
        for cid in child_ids:
            try:
                if _merge_child_config(cid, risk_vals):
                    children_updated.append(cid)
            except Exception as e:  # never fail the whole edit on one child
                log.warning("ladder %s: cascade to child %s failed: %s",
                            ladder_id, cid, e)

    log.info("ladder %s config edited v%d by %s: risk=%s cap=%s children=%s",
             ladder_id, new_version, caller.user_id,
             list(risk_vals.keys()), list(cap_vals.keys()), children_updated)
    return {
        "ok": True, "ladder_id": ladder_id, "dry_run": False,
        "applied": {**risk_vals, **cap_vals}, "rejected": [],
        "effective_config": effective_config,
        "children_updated": children_updated, "warnings": warnings,
        "config_version": new_version,
    }


def _merge_child_config(session_id: str, risk_vals: Dict[str, Any]) -> bool:
    """Merge the whitelisted risk/exit knobs into a RUNNING child's config_json +
    bump its config_version so its tick hot-reloads. Returns True if updated."""
    with falcon_conn() as con:
        row = con.execute(
            "SELECT config_json FROM autotrade_sessions WHERE session_id=?",
            (session_id,)).fetchone()
        if row is None:
            return False
        cfg = TradingSessionConfig.from_json(row["config_json"])
        for f, v in risk_vals.items():
            setattr(cfg, f, v)
        cfg.validate()  # each child is a positional intraday_basket → validates
        con.execute(
            "UPDATE autotrade_sessions "
            "SET config_json=?, config_version=config_version+1 "
            "WHERE session_id=?", (cfg.to_json(), session_id))
        con.commit()
    return True


def _now_iso() -> str:
    return datetime.now(IST).isoformat()
