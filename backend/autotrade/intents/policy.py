"""Pure validation of a strategy-intent basket: shape, defined risk, hedge-first grouping, exact max loss.

No I/O, no broker, no clock except the `today` passed in. Every refusal is a PolicyError with a stable code.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional

UNDERLYINGS = {"NIFTY"}          # what the Strategy Builder produces today; widen deliberately, never by default
MAX_LEGS = 8
PRODUCTS = {"NRML", "MIS"}
TICK = 0.05
# NSE quantity-freeze limit per order (units). Configured, not fetched: a leg above it is refused, never sliced here.
FREEZE_UNITS = {"NIFTY": 1800}


class PolicyError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass
class Leg:
    tradingsymbol: str
    underlying: str
    expiry: str
    strike: float
    option_type: str       # CE | PE
    side: str              # BUY | SELL
    quantity: int          # units, a multiple of lot_size
    lot_size: int
    limit_price: float
    product: str
    group: int

    @property
    def signed(self) -> int:
        return self.quantity if self.side == "BUY" else -self.quantity

    def as_dict(self) -> Dict[str, Any]:
        return {k: getattr(self, k) for k in ("tradingsymbol", "underlying", "expiry", "strike", "option_type",
                                             "side", "quantity", "lot_size", "limit_price", "product", "group")}


def _num(v: Any, name: str, i: int) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        raise PolicyError("BAD_LEG", f"leg {i + 1}: {name} must be a number")
    if x != x or x in (float("inf"), float("-inf")):
        raise PolicyError("BAD_LEG", f"leg {i + 1}: {name} must be finite")
    return x


_MON = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
_WEEK_M = "123456789OND"


def expected_symbols(und: str, exp: date, strike_txt: str, ot: str) -> set:
    """NSE/Kite option symbols for this contract: weekly UND+YY+M+DD+STRIKE+TYPE (M = 1-9,O,N,D) or monthly
    UND+YY+MON+STRIKE+TYPE. The expiry, strike and type are all encoded, so a symbol for another contract (or
    another underlying such as NIFTYNXT50) can never pass."""
    yy = f"{exp.year % 100:02d}"
    return {f"{und}{yy}{_WEEK_M[exp.month - 1]}{exp.day:02d}{strike_txt}{ot}",
            f"{und}{yy}{_MON[exp.month - 1]}{strike_txt}{ot}"}


def parse_legs(raw_legs: Any, today: date) -> List[Leg]:
    if not isinstance(raw_legs, list) or not raw_legs:
        raise PolicyError("NO_LEGS", "a basket needs at least one leg")
    if len(raw_legs) > MAX_LEGS:
        raise PolicyError("TOO_MANY_LEGS", f"at most {MAX_LEGS} legs")
    legs: List[Leg] = []
    for i, r in enumerate(raw_legs):
        if not isinstance(r, dict):
            raise PolicyError("BAD_LEG", f"leg {i + 1} must be an object")
        if str(r.get("exchange", "NFO")).upper() != "NFO":
            raise PolicyError("BAD_LEG", f"leg {i + 1}: only NFO options are accepted")
        und = str(r.get("underlying") or "").upper()
        if und not in UNDERLYINGS:
            raise PolicyError("UNDERLYING_NOT_ALLOWED", f"leg {i + 1}: underlying {und or '?'} is not allowed "
                              f"(allowed: {', '.join(sorted(UNDERLYINGS))})")
        ot = str(r.get("option_type") or "").upper()
        if ot not in ("CE", "PE"):
            raise PolicyError("BAD_LEG", f"leg {i + 1}: option_type must be CE or PE")
        side = str(r.get("side") or "").upper()
        if side not in ("BUY", "SELL"):
            raise PolicyError("BAD_LEG", f"leg {i + 1}: side must be BUY or SELL")
        strike = _num(r.get("strike"), "strike", i)
        if strike <= 0:
            raise PolicyError("BAD_LEG", f"leg {i + 1}: strike must be positive")
        try:
            exp = date.fromisoformat(str(r.get("expiry")))
        except ValueError:
            raise PolicyError("BAD_LEG", f"leg {i + 1}: expiry must be YYYY-MM-DD")
        if exp < today:
            raise PolicyError("EXPIRED", f"leg {i + 1}: expiry {exp} has passed")
        sym = str(r.get("tradingsymbol") or "").upper()
        strike_txt = str(int(strike)) if strike == int(strike) else f"{strike:g}"
        if sym not in expected_symbols(und, exp, strike_txt, ot):
            raise PolicyError("SYMBOL_MISMATCH", f"leg {i + 1}: {sym or '?'} does not match {und} {exp} {strike_txt} {ot}")
        lot = int(_num(r.get("lot_size"), "lot_size", i))
        qty = int(_num(r.get("quantity"), "quantity", i))
        if lot <= 0 or qty <= 0 or qty % lot:
            raise PolicyError("BAD_QUANTITY", f"leg {i + 1}: quantity {qty} must be a positive multiple of the lot {lot}")
        if qty > FREEZE_UNITS.get(und, 0):
            raise PolicyError("ABOVE_FREEZE", f"leg {i + 1}: {qty} units exceeds the {FREEZE_UNITS.get(und)} freeze quantity "
                              "(order slicing is not certified for strategy intents)")
        px = _num(r.get("limit_price"), "limit_price", i)
        if px <= 0:
            raise PolicyError("BAD_PRICE", f"leg {i + 1}: a LIMIT price above zero is required (no market orders)")
        if abs(round(px / TICK) * TICK - px) > 1e-6:
            raise PolicyError("BAD_PRICE", f"leg {i + 1}: {px} is not a multiple of the {TICK} tick")
        prod = str(r.get("product") or "NRML").upper()
        if prod not in PRODUCTS:
            raise PolicyError("BAD_LEG", f"leg {i + 1}: product must be NRML or MIS")
        grp = int(_num(r.get("group", 1), "group", i))
        if grp < 1:
            raise PolicyError("BAD_LEG", f"leg {i + 1}: group must be 1 or more")
        legs.append(Leg(sym, und, exp.isoformat(), strike, ot, side, qty, lot, round(px, 2), prod, grp))
    if len({l.tradingsymbol for l in legs}) != len(legs):
        raise PolicyError("DUPLICATE_LEG", "each contract may appear once")
    if len({l.expiry for l in legs}) > 1:
        raise PolicyError("MULTI_EXPIRY", "all legs must share one expiry (multi-expiry is not certified)")
    if len({l.underlying for l in legs}) > 1:
        raise PolicyError("MULTI_UNDERLYING", "all legs must share one underlying")
    if len({l.product for l in legs}) > 1:
        raise PolicyError("MIXED_PRODUCT", "all legs must use one product")
    return legs


def check_hedge_first(legs: List[Leg]) -> None:
    buys = [l.group for l in legs if l.side == "BUY"]
    sells = [l.group for l in legs if l.side == "SELL"]
    if buys and sells and max(buys) >= min(sells):
        raise PolicyError("HEDGE_NOT_FIRST", "every BUY (hedge) group must come before every SELL group")


def check_defined_risk(legs: List[Leg]) -> None:
    for ot in ("CE", "PE"):
        long_q = sum(l.quantity for l in legs if l.option_type == ot and l.side == "BUY")
        short_q = sum(l.quantity for l in legs if l.option_type == ot and l.side == "SELL")
        if short_q > long_q:
            raise PolicyError("UNDEFINED_RISK", f"{short_q - long_q} short {ot} units are not covered by a long {ot}; "
                              "only defined-risk structures are accepted")


def payoff_at_expiry(legs: List[Leg], spot: float) -> float:
    """P&L at expiry at `spot`, after the premiums paid/received at the limit prices (gross of charges)."""
    total = 0.0
    for l in legs:
        intrinsic = max(spot - l.strike, 0.0) if l.option_type == "CE" else max(l.strike - spot, 0.0)
        total += l.signed * (intrinsic - l.limit_price)
    return total


def max_loss(legs: List[Leg]) -> float:
    """Exact worst expiry P&L: the payoff is piecewise linear with kinks at the strikes, and (defined risk) its
    slope beyond the top strike is >= 0, so the minimum is at 0 or a strike."""
    pts = [0.0] + sorted({l.strike for l in legs})
    worst = min(payoff_at_expiry(legs, s) for s in pts)
    return round(max(0.0, -worst), 2)


def payload_hash(body: Dict[str, Any]) -> str:
    keep = {k: body.get(k) for k in ("broker", "broker_account_id", "mode", "legs", "user_id")}
    return hashlib.sha256(json.dumps(keep, sort_keys=True, default=str).encode()).hexdigest()


def validate(body: Dict[str, Any], today: date) -> Dict[str, Any]:
    """Return the normalised basket (legs, groups, max_loss, net_premium) or raise PolicyError."""
    if not isinstance(body, dict):
        raise PolicyError("BAD_BODY", "JSON object expected")
    key = str(body.get("idempotency_key") or "").strip()
    src = str(body.get("source") or "").strip()
    if not (8 <= len(key) <= 128):
        raise PolicyError("IDEMPOTENCY_KEY", "idempotency_key (8-128 chars) is required")
    if not (1 <= len(src) <= 64):
        raise PolicyError("SOURCE", "source is required")
    mode = str(body.get("mode") or "dry_run").lower()
    if mode not in ("dry_run", "live"):
        raise PolicyError("MODE", "mode must be dry_run or live")
    broker = str(body.get("broker") or "zerodha").lower()
    legs = parse_legs(body.get("legs"), today)
    check_defined_risk(legs)
    check_hedge_first(legs)
    net = round(-sum(l.signed * l.limit_price for l in legs), 2)      # + = credit received
    return {"source": src, "idempotency_key": key, "mode": mode, "broker": broker,
            "user_id": body.get("user_id"), "broker_account_id": body.get("broker_account_id"),
            "reference": body.get("reference") if isinstance(body.get("reference"), dict) else {},
            "legs": [l.as_dict() for l in legs], "groups": sorted({l.group for l in legs}),
            "max_loss": max_loss(legs), "net_premium": net, "payload_hash": payload_hash(body)}


def legs_from_rows(rows: List[Dict[str, Any]]) -> List[Leg]:
    return [Leg(**{k: r[k] for k in Leg.__dataclass_fields__}) for r in rows]
