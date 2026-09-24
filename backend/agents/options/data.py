"""
Options Agent · POINT-IN-TIME chain loader.

THE CONTRACT, AND WHY IT IS SHAPED LIKE THIS
--------------------------------------------
``instruments("NFO")`` is **live-only** and Kite has **no historical instrument master**
(docs/agents/options_STEP0_FINDINGS.md §3). So the archived daily snapshot is not a cache of
something we could re-derive — it is the ONLY record that a given strike/expiry/token/lot_size
existed on date D. Everything below follows from that:

  * ``load_chain(underlying, as_of)`` reads **ONLY the archived snapshot keyed to ``as_of``**.
    There is deliberately **NO live-chain fallback**. A live fallback on a past date would silently
    hand the caller *today's* chain (today's strike ladder, today's IV, today's lot size, and only
    the contracts that still exist) labelled as date D — simultaneously look-ahead AND survivorship
    bias, in the one place the whole agent's honesty rests. A missing snapshot therefore returns an
    honest EMPTY result with a ``reason``; it never fetches. This module imports no Kite client and
    no network library at all — the absence of a fallback is structural, not a promise.
  * ``expiries_as_of()`` resolves expiries from the snapshot's **archived instrument master**, never
    from ``instruments("NFO")`` (which would list only the contracts alive today).
  * ``lot_size`` comes from the snapshot. It is NOT a constant: NIFTY's lot has been 75 / 50 / 25 /
    50 / 65 / 75 at different times, and pricing a past condor with today's lot silently rescales
    every credit, every max-loss and therefore every EV.
  * A snapshot whose own recorded ``as_of`` is AFTER the requested date is REJECTED, not returned —
    that is the mis-keyed-object failure mode, and it is look-ahead if it slips through.

The underlying's daily bars come from the SAME source the Chart Agent reads (``agents.chart.data``)
— one daily store, one adjustment basis, no second source to drift.

Execution boundary: read-only. No broker, no orders, no fetch.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime

from . import store as _store

log = logging.getLogger("agents.options.data")

# Underlyings whose *daily bar* symbol in the Chart Agent's store differs from the F&O `name`.
# Resolved through agents.chart.data so there is exactly one daily source (never a second fetch).
_DAILY_SYMBOL_ALIAS = {
    "NIFTY": os.environ.get("AGENT_NIFTY_SYMBOL", "NIFTY 50"),
    "NIFTY 50": os.environ.get("AGENT_NIFTY_SYMBOL", "NIFTY 50"),
    "BANKNIFTY": "NIFTY BANK",
}

# Reasons are named constants so the router/UI and the tests key off the same strings.
# How a snapshot was produced. This travels with every chain the loader returns, because the two
# kinds are NOT interchangeable evidence and nothing downstream may quietly blend them:
#   LIVE     — captured intraday from quote(): real bid/ask, real spreads, no survivorship.
#   BACKFILL — reconstructed from historical_data() on the CURRENTLY-LISTED contracts. Real closes
#              and OI, but NO bid/ask (so no measured spread/slippage), and only the contracts that
#              still exist today (contracts that existed on that date and have since expired are
#              missing -> survivorship bias). Lot size on those rows is today's, not that date's.
BASIS_LIVE = "live_quote_snapshot"
BASIS_BACKFILL = "backfill_from_listed_contracts"

REASON_NO_SNAPSHOT = "no_snapshot_for_as_of"
REASON_SNAPSHOT_AFTER_AS_OF = "snapshot_as_of_after_requested_date"
REASON_EMPTY_SNAPSHOT = "snapshot_has_no_option_rows"
REASON_NO_SUCH_EXPIRY = "expiry_not_in_as_of_snapshot"
# The expiry WAS listed on as_of (the archived master proves it) but its quotes failed the snapshot
# job's consistency guard and were quarantined rather than archived. Distinct from "never existed".
REASON_EXPIRY_NOT_ARCHIVED = "expiry_listed_but_quarantined_by_consistency_guard"


# --------------------------------------------------------------------------------- date helpers
def _iso(d) -> str:
    if isinstance(d, datetime):
        return d.date().isoformat()
    if isinstance(d, date):
        return d.isoformat()
    return str(d)[:10]


def _as_date(d):
    """Parse to a date, or None. Never raises — a malformed expiry in one archived row must not
    sink the whole chain."""
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    try:
        return date.fromisoformat(str(d)[:10])
    except Exception:  # noqa: BLE001
        return None


# --------------------------------------------------------------------------------- diagnostics
def chain_dir() -> str:
    """The local snapshot directory (AGENT_OPTIONS_SNAP_DIR or the backend/var default)."""
    return os.environ.get(_store.ENV_DIR) or _store._default_dir()


def chain_uri() -> str:
    """The ACTIVE snapshot root — an s3:// prefix when AGENT_OPTIONS_SNAP_URI is set, else the
    local directory. Use this in diagnostics so an operator can see which store is being read."""
    return _store.base_uri()


def snapshot_uri(underlying: str, as_of) -> str:
    """Exactly where the (underlying, as_of) snapshot would live on the active backend."""
    return _store.uri(_iso(as_of), underlying)


def snapshot_available(underlying: str, as_of) -> bool:
    """True iff an archived snapshot exists for (underlying, as_of). Guarded — never raises.

    Point-in-time: this asks about EXACTLY that date. It never answers "the nearest snapshot",
    because the nearest snapshot to a past date is usually a LATER one."""
    try:
        return bool(_store.exists(_iso(as_of), underlying))
    except Exception as e:  # noqa: BLE001
        log.debug("snapshot_available(%s,%s) err: %s", underlying, as_of, e)
        return False


def _read_snapshot(underlying: str, as_of_iso: str):
    """The raw archived payload for (underlying, as_of), or (None, reason).

    Rejects a payload whose OWN recorded as_of is after the requested date (mis-keyed object) —
    returning it would be look-ahead."""
    payload = _store.read(as_of_iso, underlying)
    if not payload:
        return None, REASON_NO_SNAPSHOT
    snap_as_of = _iso(payload.get("as_of") or as_of_iso)
    if snap_as_of > as_of_iso:
        log.warning("snapshot for %s keyed %s carries as_of=%s (after the requested date) — REJECTED",
                    underlying, as_of_iso, snap_as_of)
        return None, REASON_SNAPSHOT_AFTER_AS_OF
    return payload, None


# --------------------------------------------------------------------------------- instrument master
def instruments_as_of(underlying: str, as_of) -> list:
    """The ARCHIVED NFO instrument-master rows for ``underlying`` as they existed on ``as_of``.

    This is the thing Kite can never give us again. Empty list when there is no snapshot — never a
    live ``instruments("NFO")`` call, which would be today's contracts wearing a past date's label."""
    as_of_iso = _iso(as_of)
    payload, _reason = _read_snapshot(underlying, as_of_iso)
    if not payload:
        return []
    return list(payload.get("instruments") or [])


def expiries_as_of(underlying: str, as_of, include_expired: bool = False) -> list:
    """Sorted ISO expiry dates that were LISTED for ``underlying`` on ``as_of``, from the archived
    master. Expiries at/after ``as_of`` only (an already-expired contract is not tradable on D);
    pass ``include_expired=True`` for audit/diagnostics.

    NEVER derived from a live instruments() call — that would both look ahead (expiries listed after
    D) and lose the contracts that existed on D and have since expired (survivorship)."""
    as_of_iso = _iso(as_of)
    as_of_d = _as_date(as_of_iso)
    seen = set()
    for inst in instruments_as_of(underlying, as_of_iso):
        if str(inst.get("instrument_type") or "").upper() not in ("CE", "PE"):
            continue
        ed = _as_date(inst.get("expiry"))
        if ed is None:
            continue
        if not include_expired and as_of_d is not None and ed < as_of_d:
            continue
        seen.add(ed.isoformat())
    return sorted(seen)


def lot_size_as_of(underlying: str, as_of, expiry=None):
    """The lot size RECORDED IN THE SNAPSHOT for (underlying, as_of[, expiry]), or None.

    Never a hardcoded constant: the exchange changes lot sizes, and using today's lot on a past
    snapshot rescales credit / max-loss / margin / EV without any error surfacing. None means
    "unknown from the archive" and callers must treat it as unknown, not substitute a guess."""
    as_of_iso = _iso(as_of)
    payload, _reason = _read_snapshot(underlying, as_of_iso)
    if not payload:
        return None
    return _lot_size_from_payload(payload, expiry)


def _lot_size_from_payload(payload: dict, expiry=None):
    exp_iso = _iso(expiry) if expiry else None
    # 1) the per-expiry lot recorded on the option rows (most specific + most trustworthy)
    counts: dict = {}
    for row in (payload.get("rows") or []):
        if exp_iso and _iso(row.get("expiry")) != exp_iso:
            continue
        ls = row.get("lot_size")
        if ls:
            counts[int(ls)] = counts.get(int(ls), 0) + 1
    if not counts:
        for inst in (payload.get("instruments") or []):
            if str(inst.get("instrument_type") or "").upper() not in ("CE", "PE"):
                continue
            if exp_iso and _iso(inst.get("expiry")) != exp_iso:
                continue
            ls = inst.get("lot_size")
            if ls:
                counts[int(ls)] = counts.get(int(ls), 0) + 1
    if counts:
        return max(counts.items(), key=lambda kv: kv[1])[0]     # modal lot for that expiry
    ls = payload.get("lot_size")
    return int(ls) if ls else None


# --------------------------------------------------------------------------------- the chain
def _empty_chain(underlying: str, as_of_iso: str, expiry, reason: str) -> dict:
    return {"underlying": str(underlying), "as_of": as_of_iso,
            "expiry": _iso(expiry) if expiry else None,
            "available": False, "reason": reason, "rows": [], "expiries": [],
            "expiries_rejected": [], "quarantine_reason": None,
            "lot_size": None, "lot_size_source": None, "forward": None, "forward_source": None,
            "spot": None, "captured_at": None, "source_uri": None, "coverage": None,
            "basis": None, "survivorship": None, "quote_basis": None, "capture_mode": None,
            "evidence_basis": "forward_tracked_snapshot", "row_count": 0}


def load_chain(underlying: str, as_of, expiry=None) -> dict:
    """The point-in-time option chain for ``underlying`` on ``as_of``, from the ARCHIVE ONLY.

    Args:
        underlying: F&O ``name`` (e.g. "NIFTY").
        as_of:      the decision date D (str / date / datetime). Only the snapshot keyed to D is read.
        expiry:     optional ISO date — restrict to one expiry (must exist in the as-of snapshot).

    Returns (always a dict, never raises):
        {underlying, as_of, expiry, available, reason, rows, expiries, lot_size, forward,
         forward_source, spot, captured_at, source_uri, coverage, evidence_basis, row_count}

    ``available=False`` + a named ``reason`` is the honest answer when the day was not snapshotted.
    NO FETCH HAPPENS ON THAT PATH — this module never imports a Kite client. Backfilling a past date
    is impossible in principle (STEP0 §3), so a miss is permanent and must be reported, not papered
    over with live data."""
    as_of_iso = _iso(as_of)
    payload, reason = _read_snapshot(underlying, as_of_iso)
    if not payload:
        return _empty_chain(underlying, as_of_iso, expiry, reason or REASON_NO_SNAPSHOT)

    as_of_d = _as_date(as_of_iso)
    all_rows = list(payload.get("rows") or [])
    expiries = expiries_as_of_payload(payload, as_of_d)

    exp_iso = _iso(expiry) if expiry else None
    if exp_iso and exp_iso not in expiries:
        out = _empty_chain(underlying, as_of_iso, exp_iso, REASON_NO_SUCH_EXPIRY)
        out["expiries"] = expiries
        out["source_uri"] = snapshot_uri(underlying, as_of_iso)
        return out

    rows = []
    for row in all_rows:
        r_exp = _iso(row.get("expiry"))
        if exp_iso and r_exp != exp_iso:
            continue
        ed = _as_date(r_exp)
        if ed is not None and as_of_d is not None and ed < as_of_d:
            continue                        # already expired on D — not tradable, never returned
        rows.append(dict(row))

    if not all_rows or not rows:
        # Distinguish "the snapshot is empty" from "this expiry was listed but quarantined by the
        # snapshot job's guard" — the second is a data-quality fact the caller must be able to report.
        quarantined = {str(x.get("expiry")): x.get("reason")
                       for x in (payload.get("expiries_rejected") or [])}
        reason = REASON_EMPTY_SNAPSHOT
        if exp_iso and exp_iso in quarantined:
            reason = REASON_EXPIRY_NOT_ARCHIVED
        elif rows == [] and all_rows:
            reason = REASON_EXPIRY_NOT_ARCHIVED if exp_iso else REASON_EMPTY_SNAPSHOT
        out = _empty_chain(underlying, as_of_iso, exp_iso, reason)
        out["expiries"] = expiries
        out["expiries_rejected"] = payload.get("expiries_rejected") or []
        out["quarantine_reason"] = quarantined.get(exp_iso) if exp_iso else None
        out["captured_at"] = payload.get("captured_at")
        out["source_uri"] = snapshot_uri(underlying, as_of_iso)
        out["coverage"] = payload.get("coverage")
        out["basis"] = payload.get("basis") or BASIS_LIVE
        return out

    forwards = payload.get("forwards") or {}
    fwd = forwards.get(exp_iso) if exp_iso else None

    # Stamp snapshot-level identity onto every row. A caller that (very naturally) passes
    # load_chain(...)["rows"] instead of the whole dict would otherwise hand a constructor rows
    # with no as_of_date -- which produced ZERO candidates and only a log line. Rows now carry
    # their own anchor, so neither call shape can silently produce nothing. setdefault, so a
    # row that already carries its own value is never overwritten.
    for _r in rows:
        _r.setdefault("as_of_date", as_of_iso)
        _r.setdefault("underlying", str(underlying))
        if payload.get("spot") is not None:
            _r.setdefault("spot", payload.get("spot"))

    return {
        "underlying": str(underlying),
        "as_of": as_of_iso,
        "expiry": exp_iso,
        "available": True,
        "reason": None,
        "rows": rows,
        "row_count": len(rows),
        "expiries": expiries,
        # Listed on as_of but NOT archived (guard-quarantined), each with its reason. Surfaced so a
        # caller can say "we have no clean chain for that expiry" instead of silently seeing nothing.
        "expiries_rejected": payload.get("expiries_rejected") or [],
        # lot_size resolved for THIS expiry from THIS snapshot (never a constant). On a BACKFILLED
        # snapshot the archive says so via lot_size_source — today's lot on a past date is not
        # point-in-time, and callers must be able to see that rather than infer it.
        "lot_size": _lot_size_from_payload(payload, exp_iso),
        "lot_size_source": payload.get("lot_size_source"),
        # How this chain was produced, and what it is therefore missing. Never inferred downstream.
        "basis": payload.get("basis") or BASIS_LIVE,
        "survivorship": payload.get("survivorship"),
        "quote_basis": payload.get("quote_basis"),
        # "market_hours" (a real book, price_source=mid) vs "post_market" (no book after the close,
        # price_source=ltp, no measured spread) vs None on a backfilled chain.
        "capture_mode": payload.get("capture_mode"),
        "forward": (fwd or {}).get("forward"),
        "forward_source": (fwd or {}).get("source"),
        "forwards": forwards,
        "spot": payload.get("spot"),
        "r": payload.get("r"),
        "captured_at": payload.get("captured_at"),
        "source_uri": snapshot_uri(underlying, as_of_iso),
        "coverage": payload.get("coverage"),
        "guard": payload.get("guard"),
        # Labelled at the SOURCE (taken from the payload, not re-asserted here) so no downstream
        # report can quietly present a backfilled chain as a live one, or either as a backtest.
        "evidence_basis": payload.get("evidence_basis") or "forward_tracked_snapshot",
    }


def expiries_as_of_payload(payload: dict, as_of_d) -> list:
    """Expiries from an ALREADY-LOADED payload (saves a second store read inside load_chain)."""
    seen = set()
    for inst in (payload.get("instruments") or []):
        if str(inst.get("instrument_type") or "").upper() not in ("CE", "PE"):
            continue
        ed = _as_date(inst.get("expiry"))
        if ed is None or (as_of_d is not None and ed < as_of_d):
            continue
        seen.add(ed.isoformat())
    if not seen:                    # snapshot without an archived master: fall back to the rows
        for row in (payload.get("rows") or []):
            ed = _as_date(row.get("expiry"))
            if ed is None or (as_of_d is not None and ed < as_of_d):
                continue
            seen.add(ed.isoformat())
    return sorted(seen)


def nearest_expiry(underlying: str, as_of, min_dte: int = 0):
    """The first archived expiry at least ``min_dte`` calendar days after ``as_of``, or None."""
    as_of_d = _as_date(_iso(as_of))
    for e in expiries_as_of(underlying, as_of):
        ed = _as_date(e)
        if ed is None or as_of_d is None:
            continue
        if (ed - as_of_d).days >= int(min_dte):
            return e
    return None


# --------------------------------------------------------------------------------- underlying bars
def daily_symbol(underlying: str) -> str:
    """The Chart Agent daily-store symbol for an F&O underlying name ("NIFTY" -> "NIFTY 50")."""
    return _DAILY_SYMBOL_ALIAS.get(str(underlying).upper(), str(underlying))


def load_underlying_daily(symbol: str):
    """Daily OHLCV for the underlying from the **same daily source the Chart Agent uses**
    (``agents.chart.data.load_daily`` — Parquet via AGENT_DATA_URI, else the SQLite fallback).

    Deliberately a thin delegation, NOT a second loader: one daily store means one adjustment basis
    (corporate-action-adjusted, dividend-unadjusted) and no chance of the two agents disagreeing
    about what the underlying did. Imported lazily so a missing pandas/duckdb can never crash boot.

    Raises whatever the Chart loader raises for an unknown symbol (callers guard/skip), mirroring
    load_daily's contract exactly."""
    from ..chart import data as chart_data
    return chart_data.load_daily(daily_symbol(symbol))


def underlying_source() -> str:
    """Which daily source ``load_underlying_daily`` is reading (diagnostics). Guarded."""
    try:
        from ..chart import data as chart_data
        return chart_data.db_path()
    except Exception as e:  # noqa: BLE001
        return f"unavailable ({type(e).__name__}: {e})"
