"""The live-order gate checklist for strategy intents. Every gate must pass for a LIVE basket; a dry run reports the
same list so the caller can show the operator exactly what stands between it and a real order."""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from . import store


def _env_true(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() == "true"


def _master_enabled() -> bool:
    """Exactly falcon.trade.services.order_executor._autotrade_enabled (the switch ZerodhaBroker._live_allowed reads).
    Mirrored rather than imported: importing the falcon.trade package opens the legacy quant DB as a side effect."""
    return os.environ.get("FALCON_AUTOTRADE_ENABLED", "").lower() == "true"


def certified_brokers() -> set:
    return {b.strip().lower() for b in os.environ.get("AUTOTRADE_STRATEGY_INTENTS_CERTIFIED", "").split(",") if b.strip()}


def _broker_certified(broker: str) -> bool:
    try:
        from ..broker.registry import is_certified
        base = is_certified(broker)
    except Exception:
        base = False
    return base and broker in certified_brokers()


def _market_open() -> bool:
    try:
        from ..trading_calendar import is_market_open
        return bool(is_market_open(store.now_ist()))
    except Exception:
        return False                                   # unknown calendar -> closed (fail-closed for live)


def _g(key: str, label: str, ok: bool, detail: str) -> Dict[str, Any]:
    return {"gate": key, "label": label, "pass": bool(ok), "detail": detail}


def evaluate(*, broker: str, user_id: Optional[str], broker_account_id: Optional[str],
             max_loss: Optional[float] = None) -> Dict[str, Any]:
    """Return {'gates': [...], 'live_allowed': bool, 'arm': arm-or-None}. Pure reads; never raises."""
    arm = None
    try:
        arm = store.active_arm(user_id, broker_account_id)
    except Exception:
        arm = None
    gates: List[Dict[str, Any]] = [
        _g("master", "AutoTrade live switch", _master_enabled(), "FALCON_AUTOTRADE_ENABLED"),
        _g("options", "Options certified for AutoTrade", _env_true("FALCON_AUTOTRADE_OPTIONS_ENABLED"),
           "FALCON_AUTOTRADE_OPTIONS_ENABLED"),
        _g("intents_live", "Strategy intents allowed to go live", _env_true("AUTOTRADE_STRATEGY_INTENTS_LIVE"),
           "AUTOTRADE_STRATEGY_INTENTS_LIVE"),
        _g("broker_certified", f"{broker} certified for strategy baskets", _broker_certified(broker),
           "registry live_certified + AUTOTRADE_STRATEGY_INTENTS_CERTIFIED"),
    ]
    if arm is None:
        gates.append(_g("armed", "Operator armed this account", False, "No active arm for this user and account"))
    else:
        left = int(arm["max_baskets"]) - int(arm["baskets_used"])
        gates.append(_g("armed", "Operator armed this account", left > 0,
                        f"Armed until {arm['expires_at']} IST by {arm['armed_by']}; {max(left, 0)} basket(s) left"))
        if max_loss is not None:
            cap = float(arm["max_loss_per_basket"])
            gates.append(_g("arm_loss_cap", "Within the arm's max loss per basket", max_loss <= cap,
                            f"max loss {max_loss:,.2f} vs cap {cap:,.2f}"))
    gates.append(_g("market_open", "Market open", _market_open(), "NSE continuous session"))
    # not checkable before dispatch (needs the live broker): does not block here, and says so - never shown as passed
    gates.append({**_g("margin", "Margin is checked at dispatch", True,
                       "The broker's basket margin must be at or below free margin just before the first order"),
                  "deferred": True})
    return {"gates": gates, "live_allowed": all(g["pass"] for g in gates), "arm": arm}


def failing(gates: List[Dict[str, Any]]) -> List[str]:
    return [g["gate"] for g in gates if not g["pass"]]
