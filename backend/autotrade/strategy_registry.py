"""Canonical AutoTrade STRATEGY REGISTRY + admin-controlled visibility.

ONE backend source of truth for the strategies/campaigns the create-form offers,
their human-facing DISPLAY NAMES, and whether each is shown to non-admin power
users. This replaces the frontend-only hardcoded STRATEGY_OPTIONS list so that:

  * display names live server-side (Task 1: "Falcon BTST Oscillator");
  * an admin can show/hide a strategy per power user (Task 2);
  * a new/experimental strategy (Falcon Intraday Magnifier) ships HIDDEN.

VISIBILITY MODEL (real-money-safe, additive):
  effective_visible(strategy_id) =
      the strategy_visibility row's flag if an admin has SET one,
      else the registry descriptor's default_visible,
      else (unknown id) FALSE.
  → "new/unknown strategies default hidden" (the Task-2 contract) without
    regressing the long-shipped strategies that stay visible by default.
  Admins ALWAYS see every strategy (the filter only applies to non-admins).

ISOLATION: reads/writes ONLY the strategy_visibility table. Touches no session /
position / falcon_position_state row. Never raises into a request path — a DB
miss degrades to the descriptor default.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.strategy_registry")
IST = timezone(timedelta(hours=5, minutes=30))


@dataclass(frozen=True)
class StrategyDescriptor:
    strategy_id: str          # STABLE key (persisted in strategy_visibility)
    display_name: str         # human-facing label the create-form shows
    kind: str                 # "session" | "campaign"
    default_visible: bool     # shown to non-admins when no admin override exists
    description: str
    # For kind=="session": the value written to config.strategy.
    session_strategy: Optional[str] = None
    # For kind=="campaign": the ladder campaign_type + ladder preset.
    campaign_type: Optional[str] = None   # "positional" | "magnifier"
    ladder_preset: Optional[str] = None   # "positional" | "btst" (positional ladders)
    experimental: bool = False


# ── The canonical list. ORDER is the create-form display order. ─────────────────
# default_visible=True for every strategy that has been shipping (unchanged UX);
# the two NEW/experimental strategies (Falcon BTST Oscillator, Falcon Intraday
# Magnifier) ship default_visible=False → HIDDEN until an admin flips them.
STRATEGY_REGISTRY: List[StrategyDescriptor] = [
    StrategyDescriptor(
        strategy_id="portfolio_kill_switch",
        display_name="Fixed — flat ±% basket exit (kill switch)",
        kind="session", session_strategy="portfolio_kill_switch",
        default_visible=True,
        description=("Flat portfolio kill switch: exit the whole basket at a fixed "
                     "±% on the invested basis.")),
    StrategyDescriptor(
        strategy_id="intraday_basket",
        display_name="Dynamic (Trailing) — arm, lock a floor & trail, square-off",
        kind="session", session_strategy="intraday_basket",
        default_visible=True,
        description=("Basket-level trailing-profit engine (arm / floor / giveback / "
                     "stop on the capital basis) with a same-day square-off.")),
    StrategyDescriptor(
        strategy_id="auto_ladder",
        display_name="Falcon Positional — Auto-Ladder (monthly campaign)",
        kind="campaign", campaign_type="positional", ladder_preset="positional",
        default_visible=True,
        description=("Set-once monthly campaign: auto-spawns a positional Falcon "
                     "Top-5 basket each trading day (~3-session hold).")),
    StrategyDescriptor(
        strategy_id="tesla_short",
        display_name="Falcon Tesla (order-flow short) — intraday MIS seat rotation",
        kind="session", session_strategy="tesla_short",
        default_visible=True,
        description=("Order-flow-native intraday SHORT capital rotation on EQ+MIS "
                     "seats.")),
    # ── NEW / experimental — ship HIDDEN (default_visible=False) ──────────────
    StrategyDescriptor(
        strategy_id="falcon_btst_oscillator",
        display_name="Falcon BTST Oscillator",
        kind="campaign", campaign_type="positional", ladder_preset="btst",
        default_visible=False, experimental=True,
        description=("Falcon Top-5 bought 09:15, sold Day-2 15:29 · CNC, no "
                     "leverage · no trail · −6% stop (a 2-session BTST campaign).")),
    StrategyDescriptor(
        strategy_id="intraday_magnifier",
        display_name="Falcon Intraday Magnifier",
        kind="campaign", campaign_type="magnifier",
        session_strategy="intraday_magnifier",
        default_visible=False, experimental=True,
        description=("Falcon Top-15 high-tier (~9 names) on 5× MIS, split entry "
                     "(50% @09:15 + 50% @09:16) with the basket trail armed only "
                     "at 09:16 on the blended cost, squared off 15:29 — a monthly "
                     "campaign that runs one basket per trading day.")),
]

_BY_ID: Dict[str, StrategyDescriptor] = {d.strategy_id: d for d in STRATEGY_REGISTRY}


def get_descriptor(strategy_id: str) -> Optional[StrategyDescriptor]:
    return _BY_ID.get(strategy_id)


def _now_iso() -> str:
    return datetime.now(IST).isoformat()


def _visibility_overrides() -> Dict[str, bool]:
    """Read all explicit admin visibility overrides {strategy_id: bool}. Never
    raises — a missing table / DB error → {} (fall back to descriptor defaults)."""
    try:
        with falcon_conn() as con:
            rows = con.execute(
                "SELECT strategy_id, visible_to_power_users "
                "FROM strategy_visibility").fetchall()
        return {r[0]: bool(r[1]) for r in rows}
    except Exception as e:  # noqa: BLE001 - degrade to defaults
        log.warning("strategy_registry: visibility read failed: %s", e)
        return {}


def is_visible(strategy_id: str,
               overrides: Optional[Dict[str, bool]] = None) -> bool:
    """Effective visibility for a NON-admin: the admin override if set, else the
    descriptor default, else False (unknown id → hidden)."""
    ov = overrides if overrides is not None else _visibility_overrides()
    if strategy_id in ov:
        return ov[strategy_id]
    d = _BY_ID.get(strategy_id)
    return bool(d.default_visible) if d else False


def set_visibility(strategy_id: str, visible: bool,
                   updated_by: Optional[str] = None) -> Dict[str, Any]:
    """Upsert an admin visibility override. Rejects an unknown strategy_id (an
    admin can only toggle a registered strategy). Returns the resulting row."""
    if strategy_id not in _BY_ID:
        raise ValueError(f"unknown strategy_id: {strategy_id!r}")
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO strategy_visibility
                   (strategy_id, visible_to_power_users, updated_by, updated_at)
               VALUES (?,?,?,?)
               ON CONFLICT(strategy_id) DO UPDATE SET
                   visible_to_power_users = excluded.visible_to_power_users,
                   updated_by             = excluded.updated_by,
                   updated_at             = excluded.updated_at""",
            (strategy_id, 1 if visible else 0, updated_by or "operator",
             _now_iso()))
        con.commit()
    return {"strategy_id": strategy_id, "visible": bool(visible),
            "updated_by": updated_by or "operator"}


def _descriptor_public(d: StrategyDescriptor) -> Dict[str, Any]:
    return {
        "strategy_id": d.strategy_id,
        "display_name": d.display_name,
        "kind": d.kind,
        "session_strategy": d.session_strategy,
        "campaign_type": d.campaign_type,
        "ladder_preset": d.ladder_preset,
        "experimental": d.experimental,
        "description": d.description,
    }


def list_for_caller(is_admin: bool) -> List[Dict[str, Any]]:
    """The create-form strategy list appropriate for the caller:
      * admin      → EVERY strategy (each annotated with visible_to_power_users +
                     whether it is an explicit override).
      * non-admin  → ONLY strategies whose effective visibility is True.
    """
    overrides = _visibility_overrides()
    out: List[Dict[str, Any]] = []
    for d in STRATEGY_REGISTRY:
        vis = is_visible(d.strategy_id, overrides)
        if not is_admin and not vis:
            continue
        row = _descriptor_public(d)
        if is_admin:
            row["visible_to_power_users"] = vis
            row["visibility_overridden"] = d.strategy_id in overrides
            row["default_visible"] = d.default_visible
        out.append(row)
    return out
