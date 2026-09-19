"""Step 4 -- reconcile fresh vendor bars against what we hold -- contract 3.3/3.4.

For every ``(symbol, bar_start)`` present in both the freshly fetched 15-minute
bars (``db/market15.db``, written by ``refresh``) and the legacy intraday rows
(``db/kanida.db``, read-only, aggregated to 15 minutes), the disagreement is
put in exactly one class:

``agree``
    The two match inside the price tolerance.

``source_error``
    Our row contradicts Kite **and** contradicts the freshly fetched daily bar
    for the same session.  Two independent witnesses against one.  This is
    PIIND's class.

``adjustment_basis``
    ``held / fresh`` is the *same constant* across the whole session (and a
    run of sessions), i.e. a level shift, not a shape difference -- and
    ``corp_actions`` in ``kanida.db`` carries an ex-date at the segment
    boundary.  The old prices are not wrong, they are on another basis.

``genuine_extreme``
    Kite confirms our value.  The coarse screen fired on a real move.

``session_regime_cas``
    The session is 24 bars long because of NSE's **Closing Auction Session**,
    introduced 2026-08-03 for cash stocks that have F&O contracts: continuous
    trading for those stocks now ends at 15:15 and the official close is set by
    an auction at 15:30-15:35.  A 24-bar session ending at ``bar_start 15:00``
    is therefore the *expected* shape for an F&O stock on or after that date --
    not a feed defect, not a source error, and nothing to synthesise.  The
    regime is derived empirically from the session's last bar start and then
    cross-checked against ``instrument_labels.is_fno``; intraday volume running
    a couple of percent under the daily bar is the auction volume.  Contract
    section 2A.

``vendor_bad_print``
    We and the vendor both hold a price that breaks out of the vendor's own
    daily range for that session.  Measured: INFY 2015-04-24 prints 2090.90
    against a daily high of 526.20 -- in the legacy DB *and* in a fetch made
    today.  A re-fetch reproduces it, so it is not our error and not something
    we can correct; it is quarantined as an unusable price.

``vendor_zero_print``
    We and the vendor agree -- on a row that is O=H=L=C=0.  Measured: INFY
    2015-04-27..05-13 comes back all-zero from Kite itself, so this is the
    vendor's own defect and not something our pipeline introduced.  The rows
    match, so there is nothing to correct; they are quarantined as unusable
    prices rather than counted as agreement.

``wrong_instrument``
    We hold intraday bars for sessions that precede the **vendor's own first
    daily bar** for the symbol by years.  Measured: AWL's intraday starts
    2015-12-21 at a price of 1.80 while Kite's daily series for AWL starts
    2022-02-08; SONACOMS 2015-05-11 vs 2021-06-24; FIVESTAR vs 2022-11-21;
    PTCIL vs 2023-06-09.  Kite serves the same pre-listing junk under the same
    instrument token, so re-fetching cannot fix it -- the token had an earlier
    life.  Quarantined with the evidence, never deleted.

``unresolved``
    Anything else, including "Kite returned nothing for a window we hold data
    for".  Quarantined and labelled -- never guessed at.

A deliberate non-rule: **a daily close that differs from the last intraday
close is not evidence of an error.**  Before 2026-08-03 the official close was
the VWAP of the last 30 minutes; from 2026-08-03 it is the closing-auction
price for F&O cash stocks.  Either way the last continuous-trading close never
has to equal the daily close (measured: RELIANCE 2026-07-31, intraday last
close 1305.00 vs daily 1307.80, identical highs).  ``source_error`` is decided
on *range containment*, impossible bar geometry and intrabucket discontinuity
only -- never on a close mismatch.

What this module will not do (contract section 6): edit a row in place,
replace a suspicious low with a daily low, or delete an unfavourable trade.
Corrections are recorded against the new revision ``refresh`` already wrote,
each one carrying the ``raw_archive`` request id that justifies it.
"""

from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Sequence

from market_data.repair import (
    ARTIFACTS,
    INVISIBLE_TO_ACCOUNT,
    KANIDA_DB,
    KNOWN_SUSPENSIONS,
    MARKET15_DB,
    read_only_conn,
    resolve_store,
)
from market_data.repair.diagnose import (
    DAILY_HIGH_FACTOR,
    DAILY_LOW_FACTOR,
    HALF_BODY_FACTOR,
    DOUBLE_BODY_FACTOR,
    aggregate_15m,
)

# -- tolerances ---------------------------------------------------------------
PRICE_REL_TOL = 0.0005        # 0.05%
PRICE_ABS_TOL = 0.011         # one tick of rounding on a 2-dp price
VOLUME_REL_TOL = 0.02
BASIS_MIN_SHIFT = 0.002       # below this a level shift is just rounding
BASIS_MAX_SPREAD = 0.005      # the per-bar ratios must agree this tightly
CORP_ACTION_WINDOW_DAYS = 400 # how far after a segment we look for an ex-date
WRONG_INSTRUMENT_MIN_SESSIONS = 20   # below this it is a gap, not a reused token
#: intraday must precede the vendor's own first daily bar by at least this much
#: before we call it a reused token rather than a ragged first week.
WRONG_INSTRUMENT_MIN_LEAD_DAYS = 180

CLASSES = ("agree", "source_error", "adjustment_basis", "genuine_extreme",
           "session_regime_cas", "vendor_zero_print", "vendor_bad_print",
           "wrong_instrument", "unresolved")
PRICE_FIELDS = ("open", "high", "low", "close")

# -- NSE Closing Auction Session (contract section 2A) ------------------------
# Measured on this account: RELIANCE (F&O) returns 25 bars ending bar_start
# 15:15 on 2026-07-28 and 2026-07-31, and 24 bars ending 15:00 on 2026-08-04
# and 2026-09-15. Non-F&O names return 25 bars on both dates. Continuous
# trading for F&O cash stocks now ends at 15:15 and the close is auctioned at
# 15:30-15:35, which is why intraday volume runs a little under the daily bar.
SESSION_LAST_BAR_START = "15:15:00"       # regular regime
CAS_LAST_BAR_START = "15:00:00"           # closing-auction regime
CAS_TAIL_STARTS = ("15:15:00",)
CAS_FROM = "2026-08-03"


def close_enough(a: Optional[float], b: Optional[float]) -> bool:
    if a is None or b is None:
        return False
    if abs(a - b) <= PRICE_ABS_TOL:
        return True
    return b != 0 and abs(a / b - 1.0) <= PRICE_REL_TOL


@dataclass
class BarVerdict:
    symbol: str
    bar_start: str
    klass: str
    reason: str
    held: dict
    fresh: dict
    daily_fresh: Optional[dict] = None
    ratio: Optional[float] = None
    corp_action: Optional[dict] = None
    evidence_request_id: Optional[str] = None
    fields_changed: list = field(default_factory=list)


@dataclass
class SymbolReconciliation:
    symbol: str
    compared_bars: int = 0
    counts: dict = field(default_factory=dict)
    examples: dict = field(default_factory=dict)
    corrections_written: int = 0
    quarantine: list = field(default_factory=list)
    cas_sessions: list = field(default_factory=list)
    is_fno: Optional[int] = None
    wrong_instrument: Optional[dict] = None
    fresh_sessions: int = 0
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# loading
# ---------------------------------------------------------------------------


def fresh_15m(store, symbol: str, run_id: Optional[str] = None) -> dict[str, dict]:
    """Freshly fetched 15-minute bars, newest revision wins."""
    con = store.con
    sql = ("SELECT bar_start, open, high, low, close, volume, revision, "
           "source_request_id, adjustment_basis_id, vendor_id "
           "FROM candles_15m WHERE symbol=?")
    args: list = [symbol]
    if run_id:
        sql += " AND run_id=?"
        args.append(run_id)
    sql += " ORDER BY bar_start, revision"
    out: dict[str, dict] = {}
    for r in con.execute(sql, args):
        out[r["bar_start"][:19]] = dict(r)
    return out


def fresh_daily(store, symbol: str, archive_root: Optional[Path] = None,
                run_id: Optional[str] = None) -> dict[str, dict]:
    """Freshly fetched daily bars, read back out of the raw archive.

    Daily bars are an independent cross-check, not 15-minute candles, so they
    live in ``raw_archive`` only -- never in ``candles_15m``.
    """
    import gzip

    con = store.con
    sql = ("SELECT request_id, payload_path FROM raw_archive "
           "WHERE symbol=? AND timeframe='day'")
    args: list = [symbol]
    if run_id:
        sql += " AND run_id=?"
        args.append(run_id)
    out: dict[str, dict] = {}
    for r in con.execute(sql, args):
        p = Path(r["payload_path"])
        if not p.exists():
            continue
        try:
            payload = json.loads(gzip.decompress(p.read_bytes()))
        except Exception:
            continue
        for row in payload.get("rows", []):
            day = str(row.get("bar_start", ""))[:10]
            if day:
                out[day] = {**row, "_request_id": r["request_id"]}
    return out


def held_15m(symbol: str, db: Path, spans: Sequence[tuple]) -> dict[str, dict]:
    """The legacy rows we hold, aggregated to 15 minutes.  Read-only.

    Restricted to the windows we actually asked the vendor for.  Loading the
    whole span between the first and last fetched bar would drag in years we
    never requested and then report every one of those sessions as "the vendor
    returned nothing" -- a false accusation, and a slow one.
    """
    if not spans:
        return {}
    con = read_only_conn(db)
    rows: list[tuple] = []
    try:
        for a, b, _rid in merge_date_spans(spans):
            rows += [tuple(r) for r in con.execute(
                "SELECT bar_time,open,high,low,close,volume FROM ohlc_5min "
                "WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",
                (symbol, a, b + " 23:59:59"))]
    finally:
        con.close()
    rows.sort()
    bars, _w2, _eng = aggregate_15m(rows)
    return {b["bar_start"][:19]: b for b in bars}


def merge_date_spans(spans: Sequence[tuple]) -> list[tuple]:
    """Merge overlapping ``(start, end, request_id)`` windows."""
    out: list[list] = []
    for a, b, rid in sorted(spans):
        if out and a <= out[-1][1]:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b, rid])
    return [(a, b, rid) for a, b, rid in out]


def corp_actions(symbol: str, db: Path) -> list[dict]:
    con = read_only_conn(db)
    try:
        return [dict(r) for r in con.execute(
            "SELECT symbol, ex_date, action_type, subject FROM corp_actions "
            "WHERE symbol=? ORDER BY ex_date", (symbol,))]
    finally:
        con.close()


# ---------------------------------------------------------------------------
# classification
# ---------------------------------------------------------------------------


def _outside_daily(bar: dict, daily: Optional[dict], who: str):
    """``(is_outside, why)`` for one bar against the freshly fetched daily range."""
    if not daily or not daily.get("low") or not daily.get("high"):
        return False, ""
    dl, dh = float(daily["low"]), float(daily["high"])
    lo, hi = bar.get("low"), bar.get("high")
    if lo and lo > 0 and lo < DAILY_LOW_FACTOR * dl:
        return True, (f"{who} low {lo:.4f} < {DAILY_LOW_FACTOR:g} x freshly fetched "
                      f"daily low {dl:.4f}")
    if hi and hi > DAILY_HIGH_FACTOR * dh:
        return True, (f"{who} high {hi:.4f} > {DAILY_HIGH_FACTOR:g} x freshly fetched "
                      f"daily high {dh:.4f}")
    return False, ""


def session_ratio(held_bars: Sequence[dict], fresh_bars: Sequence[dict]):
    """``(median ratio, spread)`` of held/fresh across a session's open+close.

    ``spread`` is ``max/min - 1``; a level shift keeps it near zero, a shape
    difference does not.
    """
    ratios = []
    for h, f in zip(held_bars, fresh_bars):
        for key in ("open", "close"):
            a, b = h.get(key), f.get(key)
            if a and b and a > 0 and b > 0:
                ratios.append(a / b)
    if len(ratios) < 2:
        return None, None
    return statistics.median(ratios), max(ratios) / min(ratios) - 1.0


def match_corp_action(actions: Sequence[dict], after: str, ratio: float) -> Optional[dict]:
    """The first ex-date after ``after`` that could plausibly cause ``ratio``.

    Dividends are excluded: a dividend adjustment is a fraction of a percent
    and cannot explain a 4x level shift, and claiming it could would be
    inventing a cause.
    """
    if ratio is None:
        return None
    limit = (date.fromisoformat(after) + timedelta(days=CORP_ACTION_WINDOW_DAYS)).isoformat()
    candidates = [a for a in actions
                  if after < a["ex_date"] <= limit
                  and a["action_type"] not in ("dividend", "agm")]
    if not candidates and abs(ratio - 1.0) < 0.05:
        # a small shift is consistent with a dividend-adjusted series
        candidates = [a for a in actions if after < a["ex_date"] <= limit]
    return candidates[0] if candidates else None


def regime_for(symbol: str, fresh_by_day: dict, is_fno_flag=None):
    """``day -> regime`` using W2's ``RegimeBook.from_observed_last_bars``.

    Derived from the bars, cross-checked against ``instrument_labels.is_fno``,
    exactly as contract section 2A requires.  Falls back to a plain
    last-bar-start rule only if W2's calendar is unavailable.
    """
    last_by_day = {}
    for day, bars in fresh_by_day.items():
        last = max(b["bar_start"][:19] for b in bars)
        last_by_day[date.fromisoformat(day)] = datetime.fromisoformat(last)
    try:
        from market_data.calendar import RegimeBook  # type: ignore

        book = RegimeBook.from_observed_last_bars(symbol, last_by_day,
                                                  is_fno=is_fno_flag)
        return lambda d: book.regime(symbol, date.fromisoformat(d))
    except Exception:
        def _fallback(d: str) -> str:
            bars = fresh_by_day.get(d) or []
            last = max((b["bar_start"][11:19] for b in bars), default="")
            return "cas" if (last == CAS_LAST_BAR_START and d >= CAS_FROM) else "regular"
        return _fallback


def classify_session(symbol: str, day: str, held: list[dict], fresh: list[dict],
                     daily: Optional[dict], actions: Sequence[dict],
                     regime: str = "regular") -> list[BarVerdict]:
    by_start_f = {b["bar_start"][:19]: b for b in fresh}
    paired = [(h, by_start_f[h["bar_start"][:19]]) for h in held
              if h["bar_start"][:19] in by_start_f]
    ratio, spread = session_ratio([h for h, _ in paired], [f for _, f in paired])

    basis_session = (ratio is not None and spread is not None
                     and spread <= BASIS_MAX_SPREAD
                     and abs(ratio - 1.0) > BASIS_MIN_SHIFT)
    action = match_corp_action(actions, day, ratio) if basis_session else None

    out: list[BarVerdict] = []
    for h, f in paired:
        changed = [k for k in PRICE_FIELDS if not close_enough(h.get(k), f.get(k))]
        req = f.get("source_request_id")
        held_dead = all(not (h.get(k) or 0) > 0 for k in PRICE_FIELDS)
        fresh_dead = all(not (f.get(k) or 0) > 0 for k in PRICE_FIELDS)
        if held_dead and fresh_dead:
            out.append(BarVerdict(
                symbol, h["bar_start"], "vendor_zero_print",
                "we and the vendor hold the same O=H=L=C=0 row: the vendor's own "
                "defect, not ours. Nothing to correct; quarantined as an "
                "unusable price rather than counted as agreement.",
                h, f, daily, None, None, req))
            continue
        if not changed:
            out.append(BarVerdict(symbol, h["bar_start"], "agree",
                                  "fresh vendor bar matches the held bar", h, f,
                                  daily, ratio, None, req))
            continue

        if basis_session:
            # every field moves by the same factor -> a level shift
            scaled_ok = all(close_enough(h.get(k), (f.get(k) or 0) * ratio)
                            for k in PRICE_FIELDS if h.get(k) and f.get(k))
            if scaled_ok:
                out.append(BarVerdict(
                    symbol, h["bar_start"], "adjustment_basis",
                    f"held/fresh = {ratio:.6f} across the whole session "
                    f"(spread {spread:.2%})"
                    + (f"; matches {action['action_type']} ex {action['ex_date']}"
                       if action else "; no matching corporate action on file"),
                    h, f, daily, ratio, action, req, changed))
                continue

        # is the held row contradicted by the INDEPENDENT fresh daily bar too?
        # Two independent witnesses, or none.  A held row that breaks out of the
        # freshly fetched daily range is only OUR error if the vendor's own
        # 15-minute bar for the same slot stays inside that range.  When both
        # break out it is the vendor's bad print (INFY 2015-04-24 prints 2090.90
        # against its own daily high of 526.20, in the legacy DB *and* in a
        # fetch made today), and re-fetching cannot repair it.
        # The freshly fetched daily bar is on the VENDOR's basis, so a held bar
        # from a basis-shifted session has to be divided by the session ratio
        # before it can be compared with it -- otherwise a corporate action
        # would look like every bar breaking out of the daily range.
        h_cmp = h
        if basis_session and ratio:
            h_cmp = {**h, **{k: (h[k] / ratio) for k in PRICE_FIELDS
                             if h.get(k) is not None}}
        contradicted, why = _outside_daily(h_cmp, daily, "held")
        if contradicted and h_cmp is not h:
            why += f" (held re-based by the session ratio {ratio:.6f})"
        fresh_contradicted, fresh_why = _outside_daily(f, daily, "vendor")
        if contradicted and not fresh_contradicted:
            out.append(BarVerdict(symbol, h["bar_start"], "source_error",
                                  why + "; the vendor's 15-minute bar for the same "
                                        "slot stays inside its own daily range",
                                  h, f, daily, ratio, None, req, changed))
            continue
        if contradicted and fresh_contradicted:
            out.append(BarVerdict(
                symbol, h["bar_start"], "vendor_bad_print",
                why + f"; and so does the vendor's own bar ({fresh_why}). The "
                      f"defect is in the vendor's series, not introduced by us, "
                      f"and a re-fetch reproduces it. Quarantined as an unusable "
                      f"price; nothing is substituted.",
                h, f, daily, ratio, None, req, changed))
            continue

        # Kite confirms our extreme?
        body_lo = min(h.get("open") or 0, h.get("close") or 0)
        body_hi = max(h.get("open") or 0, h.get("close") or 0)
        extreme = ((h.get("low") and body_lo and h["low"] < HALF_BODY_FACTOR * body_lo)
                   or (h.get("high") and body_hi and h["high"] > DOUBLE_BODY_FACTOR * body_hi))
        if extreme and all(close_enough(h.get(k), f.get(k)) for k in ("low", "high")):
            out.append(BarVerdict(symbol, h["bar_start"], "genuine_extreme",
                                  "the vendor returns the same extreme; the coarse "
                                  "screen fired on a real move",
                                  h, f, daily, ratio, None, req, changed))
            continue

        out.append(BarVerdict(
            symbol, h["bar_start"], "unresolved",
            "held and fresh disagree, no constant level shift explains it and no "
            "freshly fetched daily bar settles it",
            h, f, daily, ratio, None, req, changed))

    # -- bars we hold that the vendor no longer serves ----------------------
    fresh_starts = set(by_start_f)
    last_fresh = max(fresh_starts) if fresh_starts else ""
    any_req = next((f.get("source_request_id") for f in fresh if f.get("source_request_id")),
                   None)
    for h in held:
        key = h["bar_start"][:19]
        if key in fresh_starts:
            continue
        cas = (key[11:19] in CAS_TAIL_STARTS
               and (not last_fresh or key > last_fresh)
               and str(regime).lower().endswith("cas")
               and day >= CAS_FROM)
        if cas:
            out.append(BarVerdict(
                symbol, h["bar_start"], "session_regime_cas",
                f"continuous trading ends {last_fresh[11:16]} on this session: "
                f"NSE's Closing Auction Session (from {CAS_FROM}, F&O cash "
                f"stocks) replaces the 15:15-15:30 continuous bar with a "
                f"15:30-15:35 auction. A 24-bar session is the expected shape, "
                f"not a defect; nothing is synthesised and the official close "
                f"comes from the daily series.",
                h, {}, daily, None, None, any_req))
        else:
            out.append(BarVerdict(
                symbol, h["bar_start"], "unresolved",
                "we hold this bar and the vendor returned nothing for it, and the "
                "closing-auction regime does not explain it",
                h, {}, daily, None, None, any_req))
    return out


# ---------------------------------------------------------------------------
# per symbol
# ---------------------------------------------------------------------------

MAX_EXAMPLES = 20
MAX_BASIS_VERDICT_LINES = 200   # per symbol, in verdicts.jsonl


def reconcile_symbol(symbol: str, store, *, db: Path = KANIDA_DB,
                     run_id: Optional[str] = None, write: bool = True,
                     ) -> tuple[SymbolReconciliation, list[BarVerdict]]:
    res = SymbolReconciliation(symbol=symbol)
    fresh = fresh_15m(store, symbol, run_id)
    if not fresh:
        res.error = "no freshly fetched 15-minute bars for this symbol"
        if symbol in INVISIBLE_TO_ACCOUNT:
            res.quarantine.append({
                "symbol": symbol, "span": "all",
                "reason": "invisible to this Kite account; nothing was returned",
                "label": "quarantined_not_invented"})
        return res, []

    spans = requested_spans(store, symbol, run_id)
    held = held_15m(symbol, db, spans)
    if not held:
        res.error = "no legacy intraday rows inside the windows we requested"
        return res, []

    daily = fresh_daily(store, symbol, run_id=run_id)
    vendor_daily_first = min(daily) if daily else None
    actions = corp_actions(symbol, db)

    by_day_h: dict[str, list[dict]] = defaultdict(list)
    by_day_f: dict[str, list[dict]] = defaultdict(list)
    for k, v in held.items():
        by_day_h[k[:10]].append(v)
    for k, v in fresh.items():
        by_day_f[k[:10]].append({**v, "bar_start": k})

    res.is_fno = is_fno(symbol, db)
    regime_of = regime_for(symbol, by_day_f, res.is_fno)

    # -- held history that predates the instrument itself --------------------
    # The legacy DB shows 24 symbols whose 15m history starts years before they
    # listed (DELHIVERY, SONACOMS, AWL and STARHEALTH all list in 2021-22 yet
    # carry a first bar in 2015), which is the signature of an instrument token
    # that used to belong to something else.  The proof is: we asked Kite for
    # that window and Kite returned nothing, and the listing date agrees.  Those
    # rows are quarantined with evidence -- never deleted.
    listing = listing_date(symbol, db)
    fresh_first = min(by_day_f) if by_day_f else None
    # Two independent signals, and the second is the one that actually works:
    #   (a) the vendor returned nothing for a window we asked for, and
    #   (b) the intraday bars predate the vendor's OWN first daily bar.
    # (a) alone misses the real cases, because Kite serves the same pre-listing
    # junk under the same reused token. (b) is what exposes AWL and SONACOMS.
    cutoff = vendor_daily_first
    if cutoff:
        lead = (date.fromisoformat(cutoff)
                - timedelta(days=WRONG_INSTRUMENT_MIN_LEAD_DAYS)).isoformat()
    else:
        lead = None
    pre_listing: list[str] = []
    probe_req = None
    for d in sorted(by_day_h):
        before_vendor_daily = bool(lead and d < lead)
        before_vendor_intraday = bool(fresh_first and d < fresh_first)
        if not (before_vendor_daily or before_vendor_intraday):
            continue
        rid = span_containing(spans, d)
        if rid:
            pre_listing.append(d)
            probe_req = probe_req or rid
    requested_from = min((a for a, _b, _r in spans), default=None)
    if len(pre_listing) < WRONG_INSTRUMENT_MIN_SESSIONS:
        # Too few sessions to call it a reused token; let them be classified
        # normally rather than quarantined on a hunch.
        pre_listing = []
    if pre_listing:
        res.wrong_instrument = {
            "symbol": symbol,
            "held_first_session": pre_listing[0],
            "held_last_pre_listing_session": pre_listing[-1],
            "sessions": len(pre_listing),
            "held_bars": sum(len(by_day_h[d]) for d in pre_listing),
            "vendor_first_intraday_session": fresh_first,
            "vendor_first_daily_session": vendor_daily_first,
            "lead_days_before_vendor_daily": (
                (date.fromisoformat(vendor_daily_first)
                 - date.fromisoformat(pre_listing[0])).days
                if vendor_daily_first else None),
            "requested_from": requested_from,
            "probed_sessions_note": (
                "these are the sessions inside a window we actually requested; "
                "the full pre-listing span is inferred below and is wider"),
            "inferred_quarantine_span": (
                [min(by_day_h), vendor_daily_first] if vendor_daily_first else None),
            "inferred_quarantine_sessions": (
                sum(1 for d in by_day_h if d < vendor_daily_first)
                if vendor_daily_first else None),
            "inferred_quarantine_bars": (
                sum(len(v) for d, v in by_day_h.items() if d < vendor_daily_first)
                if vendor_daily_first else None),
            "instrument_labels_listing_date": listing,
            "sample_held_price": next(
                (by_day_h[pre_listing[0]][0].get("close") for _ in [0]), None),
            "evidence_request_id": probe_req,
            "label": "wrong_instrument",
            "reason": ("we hold intraday bars for sessions inside a window we did "
                       "request that precede the vendor's own first daily bar for "
                       "this symbol; the vendor serves the same pre-listing rows "
                       "under the same instrument token, so this is a reused "
                       "token in the legacy mapping, not something a re-fetch "
                       "can repair"),
        }
        res.quarantine.append({**res.wrong_instrument, "session":
                               f"{pre_listing[0]} .. {pre_listing[-1]}"})

    pre_listing_set = set(pre_listing)
    counts: Counter = Counter()
    examples: dict[str, list] = defaultdict(list)
    verdicts: list[BarVerdict] = []
    for day in pre_listing:
        counts["wrong_instrument"] += len(by_day_h[day])
    for day in sorted(set(by_day_h) & set(by_day_f)):
        if day in pre_listing_set:
            continue
        vs = classify_session(symbol, day,
                              sorted(by_day_h[day], key=lambda b: b["bar_start"]),
                              sorted(by_day_f[day], key=lambda b: b["bar_start"]),
                              daily.get(day), actions, regime_of(day))
        for v in vs:
            counts[v.klass] += 1
            if v.klass != "agree" and len(examples[v.klass]) < MAX_EXAMPLES:
                examples[v.klass].append(asdict(v))
        verdicts += vs

    # sessions we hold but the vendor did not return: labelled, never filled.
    # Only sessions inside a window we requested -- otherwise "the vendor has
    # nothing here" would really mean "we never asked".
    pre_set = set(pre_listing)
    missing_days = sorted(d for d in (set(by_day_h) - set(by_day_f) - pre_set)
                          if span_containing(spans, d))
    special = special_sessions()
    for day in missing_days:
        note = next((n for a, b, n in KNOWN_SUSPENSIONS.get(symbol, []) if a <= day <= b),
                    None)
        if note:
            label, reason = "known_suspension", note
        elif day in special:
            # Muhurat and other special sessions sit outside 09:15-15:30, so a
            # plain calendar-day request does not return them. Not a data
            # defect and not something to fill in.
            label, reason = ("special_session_not_returned",
                             "special (Muhurat) session: the vendor returned no "
                             "bars for a plain calendar-day request; the session "
                             "is outside the regular window, not missing")
        else:
            label, reason = ("quarantined_not_invented",
                             "vendor returned no bars for a session we hold data for")
        res.quarantine.append({
            "symbol": symbol, "session": day, "reason": reason, "label": label,
            "held_bars": len(by_day_h[day])})

    # -- closing-auction regime, measured over every freshly fetched session
    # (not only the ones we already held), so the number is the real exposure
    # of the refreshed window.  The regime is derived from the data and then
    # cross-checked against instrument_labels.is_fno -- neither is assumed.
    res.fresh_sessions = len(by_day_f)
    for day, bars in sorted(by_day_f.items()):
        last = max(b["bar_start"][11:19] for b in bars)
        if last >= SESSION_LAST_BAR_START:
            continue
        iv = sum(int(b.get("volume") or 0) for b in bars)
        dv = (daily.get(day) or {}).get("volume")
        reg = str(regime_of(day)).lower()
        res.cas_sessions.append({
            "session": day, "bars_returned": len(bars), "last_bar_start": last,
            "regular_last_bar_start": SESSION_LAST_BAR_START,
            "regime": ("cas" if (reg.endswith("cas") and last == CAS_LAST_BAR_START
                                 and day >= CAS_FROM) else "short_session_other"),
            "regime_from_w2": reg,
            "is_fno": res.is_fno,
            "daily_volume": dv, "intraday_volume": iv,
            "auction_share_of_daily_volume": (
                round(1 - iv / dv, 4) if (iv and dv) else None),
        })

    res.compared_bars = sum(counts.values())
    res.counts = dict(counts)
    res.examples = {k: v for k, v in examples.items()}

    if write:
        res.corrections_written = write_corrections(store, verdicts, run_id)
        res.corrections_written += flag_wrong_instrument(store, res.wrong_instrument,
                                                        run_id)
        flag_cas_sessions(store, symbol, res.cas_sessions, run_id)
    return res, verdicts


def requested_spans(store, symbol: str, run_id: Optional[str]) -> list[tuple]:
    """The 15-minute windows we actually asked the vendor for.

    Without this we could not tell "the vendor has no data here" from "we never
    asked", and a quarantine decision has to know the difference.  Membership is
    tested span by span, not by a single earliest date: one 200-day probe in
    2015 licenses a claim about 2015, not about the whole decade.
    """
    sql = ("SELECT start, end, request_id FROM raw_archive "
           "WHERE symbol=? AND timeframe='15minute'")
    args: list = [symbol]
    if run_id:
        sql += " AND run_id=?"
        args.append(run_id)
    return [(str(r["start"])[:10], str(r["end"])[:10], r["request_id"])
            for r in store.con.execute(sql, args)]


def span_containing(spans: Sequence[tuple], day: str):
    for a, b, rid in spans:
        if a <= day <= b:
            return rid
    return None


def special_sessions() -> set:
    """Muhurat / special session dates, from W2's calendar (never hard-coded)."""
    try:
        from market_data.calendar import MUHURAT_SESSIONS  # type: ignore

        return set(MUHURAT_SESSIONS)
    except Exception:
        return set()


def listing_date(symbol: str, db: Path) -> Optional[str]:
    con = read_only_conn(db)
    try:
        row = con.execute("SELECT listing_date FROM instrument_labels WHERE symbol=?",
                          (symbol,)).fetchone()
        return str(row[0])[:10] if row and row[0] else None
    finally:
        con.close()


def is_fno(symbol: str, db: Path) -> Optional[int]:
    """``instrument_labels.is_fno`` -- the independent cross-check on the regime."""
    con = read_only_conn(db)
    try:
        row = con.execute("SELECT is_fno FROM instrument_labels WHERE symbol=?",
                          (symbol,)).fetchone()
        return int(row[0]) if row and row[0] is not None else None
    finally:
        con.close()


def flag_cas_sessions(store, symbol: str, sessions: Sequence[dict],
                      run_id: Optional[str]) -> int:
    """Record one finding per short session, labelled with its regime.

    A CAS session is ``info``: it is the expected shape, not a problem.  A
    short session the closing-auction regime does *not* explain stays a
    ``warn``.  Either way no bar is synthesised.
    """
    if not sessions or not store.has("record_finding"):
        return 0
    rows = []
    for s in sessions:
        cas = s.get("regime") == "cas"
        share = s.get("auction_share_of_daily_volume")
        rows.append({
            "symbol": symbol, "timeframe": "15minute",
            "bar_start": f"{s['session']} {SESSION_LAST_BAR_START}",
            "code": "SESSION_REGIME_CAS" if cas else "SHORT_SESSION",
            "severity": "info" if cas else "warn",
            "message": (
                (f"closing-auction regime: continuous trading ends "
                 f"{s['last_bar_start']} ({s['bars_returned']} bars); NSE CAS "
                 f"from {CAS_FROM} for F&O cash stocks "
                 f"(instrument_labels.is_fno={s.get('is_fno')})"
                 if cas else
                 f"session is {s['bars_returned']} bars ending "
                 f"{s['last_bar_start']}, which the closing-auction regime does "
                 f"not explain")
                + (f"; {share:.2%} of the daily volume trades outside the "
                   f"continuous session" if share is not None else "")
                + ". Nothing synthesised; the official close comes from the "
                  "daily series."),
            "evidence": json.dumps(s, default=str),
        })
    return store.record_findings(rows, run_id)


def flag_wrong_instrument(store, finding: Optional[dict], run_id: Optional[str]) -> int:
    """Record the quarantine decision.  One corrections row, no rows deleted."""
    if not finding:
        return 0
    store.record_correction(
        symbol=finding["symbol"], timeframe="15minute",
        bar_start=f"{finding['held_first_session']} 09:15:00",
        field="quarantine_status",
        old_value="active (legacy rows in kanida.db)",
        new_value="quarantined:wrong_instrument",
        reason=(f"wrong_instrument: {finding['sessions']} sessions / "
                f"{finding['held_bars']} held bars over "
                f"{finding['held_first_session']} .. "
                f"{finding['held_last_pre_listing_session']} precede the vendor's "
                f"own first DAILY bar ({finding['vendor_first_daily_session']}) by "
                f"{finding['lead_days_before_vendor_daily']} days, inside a window "
                f"we did request (from {finding['requested_from']}); "
                f"vendor first intraday session "
                f"{finding['vendor_first_intraday_session']}, "
                f"instrument_labels.listing_date="
                f"{finding['instrument_labels_listing_date']}. Rows are "
                f"quarantined, not deleted."),
        evidence_request_id=finding.get("evidence_request_id"), run_id=run_id)
    if store.has("record_finding"):
        store.record_findings([{
            "symbol": finding["symbol"], "timeframe": "15minute",
            "bar_start": f"{finding['held_first_session']} 09:15:00",
            "code": "WRONG_INSTRUMENT", "severity": "error",
            "message": finding["reason"],
            "evidence": json.dumps(finding, default=str),
        }], run_id)
    return 1


def _regime_summary(cas: dict, fresh_sessions_total: int) -> dict:
    """Blast radius of the closing-auction change over the refreshed window."""
    flat = [(sym, s) for sym, rows in cas.items() for s in rows]
    cas_rows = [(sym, s) for sym, s in flat if s.get("regime") == "cas"]
    other = [(sym, s) for sym, s in flat if s.get("regime") != "cas"]
    fno = [s for _, s in cas_rows if s.get("is_fno") == 1]
    return {
        "cas_from": CAS_FROM,
        "regular_last_bar_start": SESSION_LAST_BAR_START,
        "cas_last_bar_start": CAS_LAST_BAR_START,
        "fresh_sessions_examined": fresh_sessions_total,
        "cas_symbols": len({sym for sym, _ in cas_rows}),
        "cas_symbol_sessions": len(cas_rows),
        "cas_distinct_sessions": len({s["session"] for _, s in cas_rows}),
        "cas_sessions": sorted({s["session"] for _, s in cas_rows}),
        "cas_symbol_sessions_with_is_fno_1": len(fno),
        "cas_symbol_sessions_with_is_fno_not_1": len(cas_rows) - len(fno),
        "unexplained_short_symbol_sessions": len(other),
        "unexplained_examples": [{"symbol": sym, **s} for sym, s in other[:10]],
        "example": ({"symbol": cas_rows[0][0], **cas_rows[0][1]} if cas_rows else None),
        "note": ("NSE Closing Auction Session from 2026-08-03 for F&O cash stocks: "
                 "continuous trading ends 15:15, close auctioned 15:30-15:35. A "
                 "24-bar session is expected, not a defect. Nothing synthesised; "
                 "1D/close comes from the provider daily series."),
    }


def write_corrections(store, verdicts: Iterable[BarVerdict],
                      run_id: Optional[str]) -> int:
    """One ``corrections`` row per changed field, with its evidence request id.

    The corrected *values* are already in the store as a new revision written
    by ``refresh``; this records what changed, why, and what proves it.  Old
    rows are kept.  ``genuine_extreme`` is recorded too -- a decision *not* to
    treat a difference as an error is itself a decision that has to be
    auditable.

    ``adjustment_basis`` is recorded once per contiguous run of sessions, not
    once per field per bar.  A single corporate action can re-level a decade of
    history; writing four rows per 15-minute bar for it would bury the handful
    of real price corrections under millions of rows that all say the same
    thing.  The segment row carries the ratio, the span, the bar count and the
    matched ex-date, which is strictly more informative.
    """
    n = 0
    basis_run: Optional[dict] = None

    def flush_basis() -> int:
        nonlocal basis_run
        if not basis_run:
            return 0
        store.record_correction(
            symbol=basis_run["symbol"], timeframe="15minute",
            bar_start=basis_run["first_bar"], field="adjustment_basis_id",
            old_value=f"legacy(kanida.db) x{basis_run['ratio']:.6f}",
            new_value="vendor(kite) as fetched",
            reason=(f"adjustment_basis: {basis_run['bars']} bars over "
                    f"{basis_run['first_bar'][:10]} .. {basis_run['last_bar'][:10]} "
                    f"are a constant level shift of {basis_run['ratio']:.6f}; "
                    + basis_run["reason"]),
            evidence_request_id=basis_run["req"], run_id=run_id,
            new_revision=basis_run["revision"])
        basis_run = None
        return 1

    for v in verdicts:
        if v.klass in ("agree", "unresolved"):
            continue
        if v.klass == "adjustment_basis":
            same = (basis_run and basis_run["symbol"] == v.symbol
                    and v.ratio and abs(v.ratio / basis_run["ratio"] - 1) <= BASIS_MAX_SPREAD)
            if same:
                basis_run["last_bar"] = v.bar_start
                basis_run["bars"] += 1
            else:
                n += flush_basis()
                basis_run = {"symbol": v.symbol, "first_bar": v.bar_start,
                             "last_bar": v.bar_start, "bars": 1,
                             "ratio": v.ratio or 1.0, "reason": v.reason,
                             "req": v.evidence_request_id,
                             "revision": v.fresh.get("revision")}
            continue
        n += flush_basis()
        for fld in v.fields_changed:
            store.record_correction(
                symbol=v.symbol, timeframe="15minute", bar_start=v.bar_start,
                field=fld, old_value=v.held.get(fld), new_value=v.fresh.get(fld),
                reason=f"{v.klass}: {v.reason}",
                evidence_request_id=v.evidence_request_id, run_id=run_id,
                new_revision=v.fresh.get("revision"))
            n += 1
    n += flush_basis()
    return n


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def run(symbols: Optional[Sequence[str]] = None, *, db: Path = KANIDA_DB,
        market15: Path = MARKET15_DB, run_id: Optional[str] = None,
        write: bool = True, out_dir: Path = ARTIFACTS / "reconcile",
        progress_every: int = 25) -> dict:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    store = resolve_store(market15)
    try:
        if symbols is None:
            sql = "SELECT DISTINCT symbol FROM candles_15m"
            args: list = []
            if run_id:
                sql += " WHERE run_id=?"
                args.append(run_id)
            symbols = [r[0] for r in store.con.execute(sql, args)]
        symbols = sorted(symbols)

        totals: Counter = Counter()
        per_symbol: dict[str, dict] = {}
        quarantine: list[dict] = []
        cas: dict[str, list] = {}
        wrong_instrument: list[dict] = []
        fresh_sessions_total = 0
        all_examples: dict[str, list] = defaultdict(list)
        corrections = 0
        verdict_path = out_dir / "verdicts.jsonl"
        with verdict_path.open("w", encoding="utf-8") as fh:
            for i, sym in enumerate(symbols, 1):
                res, verdicts = reconcile_symbol(sym, store, db=db, run_id=run_id,
                                                 write=write)
                per_symbol[sym] = {k: v for k, v in asdict(res).items()
                                   if k != "examples"}
                for k, v in res.counts.items():
                    totals[k] += v
                corrections += res.corrections_written
                quarantine += res.quarantine
                fresh_sessions_total += res.fresh_sessions
                if res.cas_sessions:
                    cas[sym] = res.cas_sessions
                if res.wrong_instrument:
                    wrong_instrument.append(res.wrong_instrument)
                for k, items in res.examples.items():
                    if len(all_examples[k]) < 40:
                        all_examples[k] += items[:3]
                # Every non-agree verdict is written, except that a long run of
                # identical adjustment_basis bars is capped per symbol: the full
                # count stays in per_symbol counts and the segment-level
                # corrections row carries the span.
                basis_written = 0
                for v in verdicts:
                    if v.klass == "agree":
                        continue
                    if v.klass == "adjustment_basis":
                        basis_written += 1
                        if basis_written > MAX_BASIS_VERDICT_LINES:
                            continue
                    fh.write(json.dumps(asdict(v), default=str) + "\n")
                if i % progress_every == 0 or i == len(symbols):
                    print(f"[reconcile] {i}/{len(symbols)} {sym} "
                          f"{dict(totals)}", flush=True)

        summary = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "run_id": run_id,
            "symbols": len(symbols),
            "bars_compared": sum(totals.values()),
            "by_class": dict(totals),
            "corrections_written": corrections,
            "quarantined": quarantine,
            "session_regime": _regime_summary(cas, fresh_sessions_total),
            "wrong_instrument": {
                "symbols": len(wrong_instrument),
                "sessions": sum(w["sessions"] for w in wrong_instrument),
                "held_bars": sum(w["held_bars"] for w in wrong_instrument),
                "inferred_sessions": sum(w.get("inferred_quarantine_sessions") or 0
                                         for w in wrong_instrument),
                "inferred_bars": sum(w.get("inferred_quarantine_bars") or 0
                                     for w in wrong_instrument),
                "with_vendor_daily_evidence": sum(
                    1 for w in wrong_instrument if w.get("vendor_first_daily_session")),
                "median_lead_days_before_vendor_daily": (
                    statistics.median([w["lead_days_before_vendor_daily"]
                                       for w in wrong_instrument
                                       if w.get("lead_days_before_vendor_daily")])
                    if any(w.get("lead_days_before_vendor_daily")
                           for w in wrong_instrument) else None),
                "min_sessions_threshold": WRONG_INSTRUMENT_MIN_SESSIONS,
                "detail": sorted(wrong_instrument, key=lambda w: -w["sessions"]),
                "note": ("held intraday history that predates the vendor's own "
                         "first session for a window we did request; consistent "
                         "with a reused instrument token. Quarantined with "
                         "evidence, never deleted."),
            },
            "tolerances": {"price_rel": PRICE_REL_TOL, "price_abs": PRICE_ABS_TOL,
                           "basis_min_shift": BASIS_MIN_SHIFT,
                           "basis_max_spread": BASIS_MAX_SPREAD},
            "examples": {k: v for k, v in all_examples.items()},
            "per_symbol": per_symbol,
        }
        (out_dir / "summary.json").write_text(
            json.dumps(summary, indent=2, default=str), encoding="utf-8")
        print(json.dumps({k: summary[k] for k in
                          ("symbols", "bars_compared", "by_class",
                           "corrections_written")}
                         | {"session_regime": {
                             k: summary["session_regime"][k] for k in
                             ("cas_symbols", "cas_symbol_sessions",
                              "cas_distinct_sessions", "fresh_sessions_examined",
                              "unexplained_short_symbol_sessions")}},
                         indent=2), flush=True)
        return summary
    finally:
        if hasattr(store, "close"):
            store.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="classify fresh-vs-held disagreements")
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--run-id")
    ap.add_argument("--dry-run", action="store_true",
                    help="classify but do not write corrections")
    ap.add_argument("--out", default=str(ARTIFACTS / "reconcile"))
    a = ap.parse_args(argv)
    run(a.symbols, run_id=a.run_id, write=not a.dry_run, out_dir=Path(a.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
