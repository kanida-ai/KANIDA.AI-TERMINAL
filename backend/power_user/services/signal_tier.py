"""Signal-time TIER classification for the picks panel.

Derived (not assigned) tiers from the v1 (volume dry-up) + v2 (price pullback)
walk-forward OOS analysis. Each pick is classified at SIGNAL TIME from its
signal-day price + volume — entry_gap is deliberately excluded (look-ahead).

Tiers (best → worst, layered — a pick gets its single highest matching tier):
  PREMIUM-Pullback    flat/down + 2-day pullback + strong pattern  (~83-85% WR)
  PREMIUM-Compression flat/down + narrow range + strong pattern    (~83% WR)
  ENTERPRISE-Dryup    flat/down + volume dry-up                     (~74% OOS)
  GOLD                flat/down + low turnover                      (~71% OOS)
  GOLD-baseline       flat/down                                     (~66-70%)
  STANDARD            modestly up (≤+5%)
  STANDARD-weak       up +5..+10% (not froth)
  AVOID               extended (>+10%) or up + heavy turnover (froth)

Honesty note: the PREMIUM cells are ~83-85% in-sample / per-year but NOT yet
certified at 80% on the thin 2026 walk-forward holdout. See
scripts/tier_price_volume_derivation_v2.py and memory tier_pullback_breakthrough.

Read-only: classification only; writes nothing.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any, Dict, List, Optional

log = logging.getLogger("kanida.power_user.signal_tier")

# Plain-English one-liners surfaced to the operator next to the badge.
TIER_REASON: Dict[str, str] = {
    "PREMIUM-Pullback":    "Bounce-from-drawdown: fell into the signal, strong pattern (~83-85% WR)",
    "PREMIUM-Compression": "Tight-range coil on a strong pattern (~83% WR)",
    "ENTERPRISE-Dryup":    "Flat/down on drying-up volume (~74% OOS WR)",
    "GOLD":                "Flat/down on light turnover (~71% OOS WR)",
    "GOLD-baseline":       "Flat/down signal day (~66-70% WR)",
    "STANDARD":            "Modestly up (≤+5%) — average edge",
    "STANDARD-weak":       "Up +5..+10% — weak edge",
    "AVOID":               "Extended / heavy-turnover froth — low historical WR",
}

# Colors the frontend TierBadge understands (reuses the existing palette).
TIER_COLOR: Dict[str, str] = {
    "PREMIUM-Pullback":    "amber",
    "PREMIUM-Compression": "amber",
    "ENTERPRISE-Dryup":    "green",
    "GOLD":                "yellow",
    "GOLD-baseline":       "yellow",
    "STANDARD":            "gray",
    "STANDARD-weak":       "gray",
    "AVOID":               "red",
}

SIGNAL_TIER_ENUM = frozenset(TIER_REASON.keys())


def _ok(v: Any) -> bool:
    return v is not None and not (isinstance(v, float) and v != v)   # not None / not NaN


def classify_signal_tier(sret: Optional[float],
                         twoday: Optional[float],
                         rng: Optional[float],
                         avg_lift: Optional[float],
                         trend3_20: Optional[float],
                         turn_pct: Optional[float]) -> str:
    """Layered best→worst classifier. Parity with scripts/export_tier_excel.classify."""
    if _ok(sret) and sret > 10:
        return "AVOID"
    # Froth: only AVOID extended moves on heavy turnover from >+7% (the point
    # where WR falls to ~53%). The 5-7% high-turnover band is N=586 @ 55.8% WR
    # / +1.55% avg — a positive edge — so it must NOT be avoided (it falls
    # through to STANDARD-weak). [validated 2026-06-19, scripts/validate_tier_feedback.py]
    if _ok(sret) and sret > 7 and _ok(turn_pct) and turn_pct >= 0.75:
        return "AVOID"
    if _ok(sret) and sret <= 2 and _ok(twoday) and twoday < -5 and _ok(avg_lift) and avg_lift > 15:
        return "PREMIUM-Pullback"
    if _ok(sret) and sret <= 2 and _ok(rng) and rng < 2 and _ok(avg_lift) and avg_lift > 15:
        return "PREMIUM-Compression"
    if _ok(sret) and sret <= 2 and _ok(trend3_20) and trend3_20 < 0.9:
        return "ENTERPRISE-Dryup"
    if _ok(sret) and sret <= 2 and _ok(turn_pct) and turn_pct < 0.75:
        return "GOLD"
    if _ok(sret) and sret <= 2:
        return "GOLD-baseline"
    if _ok(sret) and sret <= 5:
        return "STANDARD"
    if _ok(sret) and sret <= 10:
        return "STANDARD-weak"
    return "STANDARD-weak"   # sret unknown → conservative middle (never silently GOLD)


# ──────────────────────────────────────────────────────────────────────────
#  DATA-DRIVEN RULEBOOK (Phase 0 of the self-learning loop)
#  The active rulebook lives in falcon_tier_rules (status='active'); the
#  classifier evaluates it priority-ordered. If the table is empty/unavailable
#  the hardcoded classify_signal_tier above is the fallback — so the live
#  system never depends on the DB being populated.
# ──────────────────────────────────────────────────────────────────────────
_OPS = {"<": lambda a, b: a < b, "<=": lambda a, b: a <= b,
        ">": lambda a, b: a > b, ">=": lambda a, b: a >= b}
_rb_cache: Dict[str, Any] = {"rules": None, "at": 0.0}
_RB_TTL = 300.0   # 5 min — learner publishes infrequently; staleness is harmless


def load_active_rulebook(con: sqlite3.Connection) -> List[Any]:
    """Load status='active' tier rules, parsed + priority-sorted. Cached 5 min.
    Returns [] on any error/empty so the caller falls back to the hardcoded rules."""
    now = time.time()
    if _rb_cache["rules"] is not None and (now - _rb_cache["at"]) < _RB_TTL:
        return _rb_cache["rules"]
    rules: List[Any] = []
    try:
        rows = con.execute(
            "SELECT tier, conditions_json FROM falcon_tier_rules WHERE status='active'"
        ).fetchall()
        parsed = []
        for tier, cj in rows:
            spec = json.loads(cj)
            parsed.append((int(spec.get("priority", 50)), tier, spec.get("all", [])))
        parsed.sort(key=lambda r: r[0])
        rules = parsed
    except Exception as e:                                  # noqa: BLE001
        log.warning("signal_tier.load_active_rulebook: %s", e)
        rules = []
    _rb_cache.update(rules=rules, at=now)
    return rules


def classify_from_rulebook(feat: Dict[str, Optional[float]], rules: List[Any]) -> str:
    """Evaluate priority-ordered DB rules; first whose ALL conditions hold wins.
    `feat` keys: sret, twoday, rng, avg_lift, trend3_20, turn_pct."""
    for _prio, tier, conds in rules:
        match = True
        for field, op, val in conds:
            v = feat.get(field)
            if not _ok(v) or op not in _OPS or not _OPS[op](v, val):
                match = False
                break
        if match:
            return tier
    return "STANDARD-weak"


def _signal_day_features(bars: List[sqlite3.Row]) -> Dict[str, Optional[float]]:
    """From a symbol's ohlc bars (ascending, last row == signal day) compute the
    signal-time price + volume features. Backward windows only → look-ahead safe."""
    n = len(bars)
    if n < 2:
        return {}
    sd = bars[-1]
    close = sd["close"]; high = sd["high"]; low = sd["low"]; vol = sd["volume"]
    prev_close = bars[-2]["close"]
    out: Dict[str, Optional[float]] = {}
    if prev_close:
        out["signal_day_ret_pct"] = (close / prev_close - 1) * 100
        out["range_pct"] = (high - low) / prev_close * 100
    if n >= 3 and bars[-3]["close"]:
        out["two_day_ret_pct"] = (close / bars[-3]["close"] - 1) * 100
    vols = [b["volume"] for b in bars if b["volume"] is not None]
    if len(vols) >= 11:
        w20 = vols[-20:]
        avg20 = sum(w20) / len(w20)
        if avg20 > 0:
            out["rvol20"] = vol / avg20
            avg3 = sum(vols[-3:]) / len(vols[-3:])
            out["trend3_20"] = avg3 / avg20
    turns = [(b["close"] * b["volume"]) for b in bars
             if b["close"] is not None and b["volume"] is not None]
    if len(turns) >= 60:
        window = turns[-252:]
        today_turn = turns[-1]
        out["turn_pct"] = sum(1 for t in window if t <= today_turn) / len(window)
    return out


def enrich_picks(con: sqlite3.Connection,
                 picks: List[Dict[str, Any]],
                 signal_date: str) -> None:
    """In-place: add signal_tier / signal_tier_reason / signal_day_ret_pct /
    two_day_ret_pct to each pick. ONE batched ohlc query for all symbols.

    Never raises — on any failure a pick simply gets signal_tier=None and the
    payload/frontend degrade gracefully. The live-trade path is untouched.
    """
    if not picks:
        return
    symbols = sorted({p["symbol"] for p in picks})
    try:
        rows = con.execute(
            "SELECT symbol, trade_date, close, high, low, volume "
            "FROM ohlc_daily "
            "WHERE symbol IN (%s) AND trade_date <= ? "
            "ORDER BY symbol, trade_date" % ",".join("?" * len(symbols)),
            (*symbols, signal_date),
        ).fetchall()
    except Exception as e:                                  # noqa: BLE001
        log.warning("signal_tier.enrich_picks: ohlc query failed: %s", e)
        return

    # Index access (cols: symbol, trade_date, close, high, low, volume) so this
    # works whether or not the connection has a sqlite3.Row factory.
    by_sym: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_sym.setdefault(r[0], []).append(
            {"close": r[2], "high": r[3], "low": r[4], "volume": r[5]})
    # keep only the trailing window we need (perf: ≤260 bars per symbol)
    for sym in by_sym:
        by_sym[sym] = by_sym[sym][-260:]

    # Phase 0: prefer the data-driven rulebook; empty/unavailable → code fallback.
    rules = load_active_rulebook(con)

    for p in picks:
        feats = _signal_day_features(by_sym.get(p["symbol"], []))
        sret  = feats.get("signal_day_ret_pct")
        twod  = feats.get("two_day_ret_pct")
        rng   = feats.get("range_pct")
        nf    = p.get("n_fires") or 0
        avg_lift = (p["score"] / nf) if nf else None
        if rules:
            tier = classify_from_rulebook(
                {"sret": sret, "twoday": twod, "rng": rng, "avg_lift": avg_lift,
                 "trend3_20": feats.get("trend3_20"), "turn_pct": feats.get("turn_pct")},
                rules)
        else:
            tier = classify_signal_tier(sret, twod, rng, avg_lift,
                                        feats.get("trend3_20"), feats.get("turn_pct"))
        p["signal_tier"]        = tier
        p["signal_tier_reason"] = TIER_REASON.get(tier)
        p["signal_tier_color"]  = TIER_COLOR.get(tier, "gray")
        p["signal_day_ret_pct"] = round(sret, 2) if _ok(sret) else None
        p["two_day_ret_pct"]    = round(twod, 2) if _ok(twod) else None
