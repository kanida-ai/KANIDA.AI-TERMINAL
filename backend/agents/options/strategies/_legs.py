"""
Options Agent · shared leg-selection engine.

The options analogue of backend/agents/chart/patterns/_geometry.py: pure functions over an
already-sliced chain snapshot, shared by every strategy so credit spreads, debit spreads and
future structures reuse ONE selection engine rather than each re-deriving strike picking.

POINT-IN-TIME BY CONSTRUCTION: every function here takes rows that are already the as-of
snapshot and returns a selection. Nothing fetches, nothing looks forward.

PRICING CONVENTION — stated once, obeyed everywhere
---------------------------------------------------
A leg is valued at the MID when a two-sided quote exists, and the cost of actually crossing
the spread is charged separately as slippage in payoff.total_costs (a governed fraction of
that leg's own spread). Mid + explicit half-spread slippage is arithmetically the same as
selling the bid and buying the ask, but it keeps the two effects VISIBLE and separately
auditable instead of burying the fill assumption inside the price.

Rows with no two-sided quote (notably BACKFILLED history rows, which carry OHLC+OI but no
bid/ask) are valued at their close and marked price_source="hist_close". Those rows can
never be treated as live-quoted: liquidity gating must report skipped for them rather than
silently pass, because there is no measured spread to gate on.
"""
from __future__ import annotations

from typing import Optional


def _f(row: dict, key: str):
    v = row.get(key)
    return None if v is None else float(v)


# ─────────────────────────────────────────────────────────────────── snapshot slicing
def rows_for(chain: list, expiry: str, right: Optional[str] = None) -> list:
    """All contracts of one expiry (optionally one right), sorted by strike."""
    out = [r for r in chain if str(r.get("expiry"))[:10] == str(expiry)[:10]
           and (right is None or str(r.get("right", "")).upper() == right)]
    return sorted(out, key=lambda r: float(r["strike"]))


def strike_ladder(chain: list, expiry: str) -> list:
    """The strikes that ACTUALLY exist for this expiry in this snapshot.

    Wings must land on a listed strike; inventing a strike that the exchange does not list
    produces a structure that could never have been traded."""
    return sorted({float(r["strike"]) for r in chain
                   if str(r.get("expiry"))[:10] == str(expiry)[:10]})


def snap_strike(ladder: list, target: float) -> Optional[float]:
    """Nearest LISTED strike to `target`. None when the ladder is empty."""
    if not ladder:
        return None
    return min(ladder, key=lambda k: abs(k - float(target)))


def snap_strike_outward(ladder: list, target: float, direction: str) -> Optional[float]:
    """Snap to a listed strike AWAY from the money, never toward it.

    `direction` is "up" for a call wing (want a strike >= target) and "down" for a put wing
    (want <= target). Snapping to the NEAREST strike can land inside the requested width,
    which narrows the condor and therefore UNDERSTATES its max loss -- measured: a requested
    200-wide condor silently became 100-wide with half the max loss and double the apparent
    R:R. Erring outward makes the structure wider (more max loss, less credit), which is the
    safe direction to be wrong in. Falls back to the nearest available end of the ladder."""
    if not ladder:
        return None
    target = float(target)
    if direction == "up":
        cands = [k for k in ladder if k >= target]
        return min(cands) if cands else max(ladder)
    cands = [k for k in ladder if k <= target]
    return max(cands) if cands else min(ladder)


def strike_step(ladder: list) -> Optional[float]:
    """The modal gap in the ladder — the exchange's strike increment (measured 50 for NIFTY,
    never assumed)."""
    if len(ladder) < 2:
        return None
    gaps = {}
    for i in range(len(ladder) - 1):
        g = round(ladder[i + 1] - ladder[i], 4)
        gaps[g] = gaps.get(g, 0) + 1
    return max(gaps.items(), key=lambda kv: kv[1])[0]


# ────────────────────────────────────────────────────────────────────── leg pricing
def price_leg(row: dict) -> tuple:
    """(price_used, price_source) for one contract row.

    Order of preference: a two-sided quote -> mid; otherwise LTP; otherwise the historical
    close for a backfilled row. Never synthesises a price."""
    bid, ask = _f(row, "bid"), _f(row, "ask")
    if bid is not None and ask is not None and ask > 0 and bid > 0 and ask >= bid:
        return (bid + ask) / 2.0, "mid"
    ltp = _f(row, "ltp")
    src = str(row.get("price_source") or "")
    if src == "hist_close":
        close = _f(row, "close")
        if close is not None and close > 0:
            return close, "hist_close"
    if ltp is not None and ltp > 0:
        return ltp, "ltp"
    close = _f(row, "close")
    if close is not None and close > 0:
        return close, "hist_close"
    return None, "none"


def spread_pct(row: dict) -> Optional[float]:
    """(ask-bid)/mid as a percentage. None when there is no two-sided quote — and None must
    stay None: a missing spread is unknown, not zero."""
    bid, ask = _f(row, "bid"), _f(row, "ask")
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = (bid + ask) / 2.0
    return ((ask - bid) / mid * 100.0) if mid > 0 else None


def make_leg(row: dict, action: str, leg_id: int, lots: int = 1) -> Optional[dict]:
    """Build the frozen leg dict payoff.py consumes. None when the row cannot be priced."""
    px, src = price_leg(row)
    if px is None:
        return None
    return {
        "leg_id": leg_id,
        "action": action,
        "right": str(row.get("right", "")).upper(),
        "strike": float(row["strike"]),
        "expiry": str(row.get("expiry"))[:10],
        "tradingsymbol": row.get("tradingsymbol"),
        "instrument_token": row.get("instrument_token"),
        "lot_size": int(row.get("lot_size") or 0),
        "lots": int(lots),
        "ltp": _f(row, "ltp"),
        "bid": _f(row, "bid"),
        "ask": _f(row, "ask"),
        "mid": px if src == "mid" else None,
        "price_used": px,
        "price_source": src,
        "spread_pct": spread_pct(row),
        "oi": _f(row, "oi"),
        "volume": _f(row, "volume"),
        "iv": _f(row, "iv"),
        "delta": _f(row, "delta"),
        "gamma": _f(row, "gamma"),
        "theta": _f(row, "theta"),
        "vega": _f(row, "vega"),
    }


# ──────────────────────────────────────────────────────────────── strike selection
def pick_by_delta(chain: list, expiry: str, right: str, target_abs_delta: float,
                  band: tuple = (0.10, 0.25)) -> Optional[dict]:
    """The contract whose |delta| is closest to `target_abs_delta`, within `band`.

    Delta is the standard way to place a condor's shorts because it normalises across vol
    regimes: 0.16-delta is roughly a 1-sigma strike whether vol is 10% or 30%, whereas a
    fixed % OTM is a much wider strike in a calm market than in a stressed one.

    Returns None when no contract has a usable delta — an UNKNOWN delta must never be
    treated as 0, which would silently select the farthest wing.
    """
    lo, hi = band
    best, best_d = None, None
    for r in rows_for(chain, expiry, right):
        d = _f(r, "delta")
        if d is None:
            continue
        ad = abs(d)
        if not (lo <= ad <= hi):
            continue
        dist = abs(ad - target_abs_delta)
        if best_d is None or dist < best_d:
            best, best_d = r, dist
    return best


def pick_by_pct_otm(chain: list, expiry: str, right: str, spot: float,
                    pct: float) -> Optional[dict]:
    """Fallback selector when deltas are unavailable (e.g. a backfilled row set where IV
    could not be solved). Explicitly a FALLBACK — the caller must record which selector was
    used, because the two are not equivalent across vol regimes."""
    target = spot * (1.0 + pct) if right == "CE" else spot * (1.0 - pct)
    rows = rows_for(chain, expiry, right)
    if not rows:
        return None
    return min(rows, key=lambda r: abs(float(r["strike"]) - target))


def find_row(chain: list, expiry: str, right: str, strike: float) -> Optional[dict]:
    for r in rows_for(chain, expiry, right):
        if abs(float(r["strike"]) - float(strike)) < 1e-6:
            return r
    return None


# ────────────────────────────────────────────────────────────────────── liquidity
def liquidity_ok(row: dict, params: dict) -> tuple:
    """(ok, reason). A leg with NO measured spread is NOT silently passed — it returns
    ok=False with a named reason so a backfilled row can never masquerade as liquid."""
    oi = _f(row, "oi")
    vol = _f(row, "volume")
    sp = spread_pct(row)

    if sp is None:
        return False, "no two-sided quote (spread unmeasurable)"
    if sp > params["max_spread_pct"]:
        return False, "spread %.1f%% > max %.1f%%" % (sp, params["max_spread_pct"])
    if oi is None or oi < params["min_oi"]:
        return False, "OI %s < min %s" % (int(oi) if oi is not None else "unknown",
                                          params["min_oi"])
    if vol is not None and vol < params["min_volume"]:
        return False, "volume %d < min %d" % (int(vol), params["min_volume"])
    return True, "ok"


# ───────────────────────────────────────────────────────────────── expiry helpers
def expected_move(forward: float, atm_iv: float, T: float) -> float:
    """One-sigma expected move to expiry, in points: F * iv * sqrt(T)."""
    import math
    return float(forward) * float(atm_iv) * math.sqrt(max(float(T), 0.0))


def atm_iv(chain: list, expiry: str, forward: float) -> Optional[float]:
    """The IV of the strike nearest the forward, averaged across CE and PE where both solve.
    None when neither solves — never a default."""
    ivs = []
    for right in ("CE", "PE"):
        rows = [r for r in rows_for(chain, expiry, right) if _f(r, "iv") is not None]
        if not rows:
            continue
        r = min(rows, key=lambda x: abs(float(x["strike"]) - float(forward)))
        ivs.append(float(r["iv"]))
    return (sum(ivs) / len(ivs)) if ivs else None


def dte(as_of_date, expiry) -> int:
    """Calendar days from the DECISION DATE to expiry.

    Takes as_of_date explicitly and never reads the clock: a replay of a past snapshot must
    compute the DTE that was true on that date, not the DTE as of today. Reading
    date.today() here would silently corrupt every replayed decision."""
    import datetime as dt

    def _d(x):
        if isinstance(x, dt.datetime):
            return x.date()
        if isinstance(x, dt.date):
            return x
        return dt.date.fromisoformat(str(x)[:10])

    return (_d(expiry) - _d(as_of_date)).days


def classify_expiry(expiry: str, all_expiries: list, as_of_date) -> str:
    """weekly | monthly | long_dated, decided from the snapshot's own expiry ladder rather
    than from a hardcoded calendar rule.

    An expiry is MONTHLY if it is the last listed expiry within its calendar month, WEEKLY if
    it is an earlier expiry in a month that has several, and LONG_DATED beyond ~100 DTE.
    Measured NIFTY reality this was checked against: weeklies on Tuesdays, then monthlies,
    then quarterly/half-yearly out to 2031.

    as_of_date is REQUIRED and is the decision date -- see dte() on why the clock is never
    read here."""
    e = str(expiry)[:10]
    try:
        horizon = dte(as_of_date, e)
    except Exception:
        return "unknown"
    same_month = sorted(x[:10] for x in all_expiries if str(x)[:7] == e[:7])
    is_last_of_month = bool(same_month) and e == same_month[-1]
    if horizon > 100:
        return "long_dated"
    if is_last_of_month and len(same_month) > 1:
        return "monthly"
    if len(same_month) == 1:
        return "monthly" if horizon > 20 else "weekly"
    return "weekly"
