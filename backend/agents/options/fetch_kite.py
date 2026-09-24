"""
Options Agent · DAILY CHAIN SNAPSHOT (the one feed-touching module).

Captures the live NFO option chain for an underlying and ARCHIVES it — the archived snapshot is the
Options Agent's entire evidence store. Rationale (docs/agents/options_STEP0_FINDINGS.md §3):
``instruments("NFO")`` lists live contracts only and Kite has no historical instrument master, so a
past-date chain can never be reconstructed. Evidence can only be ACCRUED forward, one snapshot at a
time, and a day we fail to capture is lost permanently.

That asymmetry drives every design choice here:

  * WHAT IS ARCHIVED — not just quotes, but the **instrument-master rows themselves** (tradingsymbol,
    token, strike, right, expiry, lot_size, tick_size) for the CE/PE ladder AND the FUT rows. The
    master is what makes ``data.expiries_as_of()`` and a point-in-time lot_size possible later; the
    FUT rows are what let us re-derive the forward without re-fetching anything.
  * THE FORWARD — options are priced off the **futures price from the SAME snapshot**, same expiry
    where one exists. Using spot (or today's future for a past chain) puts the whole basis into the
    IV. Where an option expiry has no matching future — NIFTY weeklies, always — the forward falls
    back to the **parity-implied** forward from near-ATM CE/PE pairs in the same snapshot, and the
    row records ``forward_source`` so no downstream number can pretend the basis was measured when
    it was inferred.
  * THE CONSISTENCY GUARD — a bad snapshot is permanent poison (we can never re-fetch the day), so
    the guard runs BEFORE any write and ABORTS on failure, exactly like the Chart Agent's
    adjustment-drift guard in agents/chart/fetch_kite.py. It is fail-closed: if it cannot verify, it
    aborts rather than assumes.
  * PRICE PROVENANCE — every row records ``price_source`` ("mid" or "ltp"). Mid vs LTP changes the
    credit and therefore the EV of every structure built on the row; that is recorded, never assumed.
  * COVERAGE ACCOUNTING — every instrument lands in exactly one bucket and the buckets are reported,
    the same way agents/chart/screener.py accounts for every universe symbol. Nothing hidden.

TWO JOBS LIVE HERE
  * ``snapshot_chain(as_of, underlying)`` — the DAILY live capture. quote() carries no IV and no
    greeks (measured, live probe 2026-08-31: last_price / oi / volume / depth / ohlc / circuit
    limits / timestamps only), so pricing.py solves both here. This is the primary evidence source.
  * ``backfill_history(underlying, ...)`` — a ONE-SHOT seed from ``historical_data(..., oi=True)``,
    which does serve an option token back to its listing date. It gives us a real dataset today
    instead of accruing one session at a time, but it is WEAKER evidence in three named ways
    (no bid/ask, survivorship-limited to the currently-listed ladder, non-point-in-time lot size).
    Those caveats are written into every backfilled payload, not just documented — see the
    BACKFILL_* constants and ``data.load_chain(...)["basis"]``.

EXECUTION BOUNDARY (hard): this module makes READ-ONLY market-data calls — ``instruments()``,
``quote()`` and ``historical_data()`` — plus local/S3 writes. It does NOT place, modify or cancel
orders, does NOT touch
GTT, and deliberately does NOT call ``order_margins``/``basket_order_margins``: those POST an
order-shaped payload to the broker, and agent code does not send order-shaped payloads anywhere.
Margin, when it is needed, is obtained outside the agent boundary.

All heavy imports are lazy so importing this module can never crash app boot.
"""
from __future__ import annotations

import logging
import math
from datetime import date, datetime, timedelta, timezone

log = logging.getLogger("agents.options.fetch_kite")

# ------------------------------------------------------------------------------- tunables (governed)
# Kite caps a single quote() call at ~500 instruments. 400 leaves headroom.
QUOTE_CHUNK = 400

# Near-ATM band (strikes each side of the forward, per expiry) used by the consistency guard and by
# the parity-implied forward. Small on purpose: parity is only clean where there is real two-sided
# liquidity, and inventing a check on the illiquid wings would produce noise, not evidence.
ATM_BAND = 5

# Parity tolerance as a fraction of the forward. A clean NIFTY chain prices near-ATM pairs to within
# a tick or two; 15 bps of a ~24,000 forward is ~36 points — far above bid/ask noise, far below any
# real inconsistency (a stale leg or a crossed book moves it by hundreds).
PARITY_TOL_FRAC = 0.0015

# Extra allowance added to the tolerance for the pair's own quoted half-spreads, so an honestly wide
# book is not scored as an inconsistent one.
PARITY_SPREAD_ALLOWANCE = 1.0

# Basis tolerance between the parity-implied forward and the SAME-EXPIRY future (fraction of F).
FUT_BASIS_TOL_FRAC = 0.0030

# Fail-closed: with fewer verifiable near-ATM pairs than this we cannot certify the snapshot, so we
# abort rather than archive something unverified (same posture as the Chart Agent's drift guard,
# which aborts when no probe bar is fetchable).
MIN_PARITY_PAIRS = 3

# Any near-ATM parity/crossed violation aborts. Zero tolerance: the snapshot is unrepeatable.
MAX_PARITY_FAILS = 0

# Index spot lookups (best effort, guarded — spot is context, never the pricing forward).
_SPOT_KEY = {"NIFTY": "NSE:NIFTY 50", "BANKNIFTY": "NSE:NIFTY BANK",
             "FINNIFTY": "NSE:NIFTY FIN SERVICE", "MIDCPNIFTY": "NSE:NIFTY MID SELECT"}

FORWARD_POLICY = ("same-expiry NFO future from the SAME snapshot where one exists; else the "
                  "parity-implied forward from near-ATM CE/PE pairs in the same snapshot; else the "
                  "nearest later-expiry future (labelled). Recorded per expiry as forward_source.")

# ------------------------------------------------------------------------------- backfill (history)
# MEASURED (live probe, 2026-08-31): historical_data(token, ..., interval="day", oi=True) DOES serve
# an NFO option token back to its LISTING date (NIFTY26DEC24000CE -> 833 daily bars from 2022-11-28),
# subject only to the documented ~2000-calendar-day-per-request limit; and a token that is NOT in
# today's master raises InputException("invalid token"). So we can reconstruct history for the
# CURRENTLY-LISTED ladder only — which is exactly why the forward daily snapshot still matters.
BACKFILL_PACE_S = 0.34               # ~3 req/s, the documented Kite historical rate limit
BACKFILL_MAX_WINDOW_DAYS = 1990      # under the ~2000-day per-request cap
BACKFILL_DEFAULT_LOOKBACK_DAYS = 1990
ENV_BACKFILL_DIR = "AGENT_OPTIONS_BACKFILL_DIR"

# CLOSE-BASED legs (a post-market capture, or a backfilled day) are two independent closing prints,
# not a simultaneous quote pair, so they get their own LOOSER, explicitly-named parity tolerance.
# Reusing the live tolerance would reject honest data; silently reusing this loose one on a live
# in-hours capture would let a broken chain through — which is why the mode is detected and recorded.
CLOSE_PARITY_TOL_FRAC = 0.006
BACKFILL_PARITY_TOL_FRAC = CLOSE_PARITY_TOL_FRAC

PRICE_SOURCE_HIST = "hist_close"

# MEASURED post-market caveats for a LIVE capture taken after the close (see _capture_mode).
POSTMARKET_QUOTE_BASIS = ("captured after the close: Kite returns an all-zero 5-level depth and "
                          "resets volume/average_price, so bid/ask/mid/spread_pct are None, "
                          "price_source='ltp' (the day's close) and volume is None (unknown, "
                          "NOT zero). OI and last_price are real.")

# The honesty caveats that TRAVEL WITH every backfilled snapshot. They are written into the payload
# (not just documented here) so no downstream consumer can treat a backfilled chain as a live one.
BACKFILL_QUOTE_BASIS = ("daily OHLC + OI from historical_data(); there is NO bid/ask in that feed, "
                        "so bid/ask/mid/spread_pct are None and price_source='hist_close'")
BACKFILL_SLIPPAGE_POLICY = ("backfilled rows carry NO measured spread — slippage MUST come from a "
                            "governed, labelled assumption; a spread must never be synthesised")
BACKFILL_LIQUIDITY_POLICY = ("the liquidity gate must report SKIPPED-with-reason on backfilled "
                             "dates (no archived bid/ask/spread), never silently pass")
BACKFILL_SURVIVORSHIP = ("reconstructed from the CURRENTLY-LISTED contract ladder only — contracts "
                         "that existed on this date and have since expired are unreachable "
                         "(invalid token) and are therefore MISSING from this chain")
BACKFILL_LOT_SIZE_SOURCE = ("current master, NOT point-in-time — the exchange changes lot sizes and "
                            "historical_data carries none")

# ── lot-size trust horizon ───────────────────────────────────────────────────────────────────
# A backfilled row is stamped with TODAY's lot size because historical_data carries none and the
# instrument master reports only the CURRENT lot for every live contract. When the exchange revises
# a lot size, every already-listed contract is revised with it — so a bar from before the revision
# gets stamped with the post-revision lot, and every RUPEE figure derived from it (credit, max loss,
# margin) is wrong by the ratio of the two lots.
#
# This is not hypothetical for NIFTY: the measured lot today is 65, and it has been revised more
# than once in recent years. Per-UNIT quantities (credit/unit, breakevens, POP, R:R, EV/unit) are
# unaffected — only rupee totals scale wrong — so the honest move is to mark confidence per row
# rather than to drop the data or to silently trust it.
#
# Within the trust horizon a revision is unlikely AND the phase-1 usable backfill window is only
# ~30-60 days anyway (beyond that the near-dated contracts have expired and are unreachable), so
# recent rows are the ones that actually feed the condor evidence. Older rows stay usable for
# per-unit work but must never have their rupee totals compared across a possible revision.
LOT_SIZE_TRUST_DAYS = 90
LOT_CONF_RECENT = "current_master_recent"
LOT_CONF_UNVERIFIABLE = ("unverifiable — bar predates the lot-size trust horizon; the exchange may "
                         "have revised the lot since, so RUPEE totals may be scaled wrong. "
                         "Per-unit metrics (credit/unit, breakevens, POP, R:R) are unaffected.")


def _lot_confidence(bar_date, master_date=None) -> str:
    """Per-row lot-size confidence, measured against the date the INSTRUMENT MASTER was read.

    The reference is the master's read date -- NOT the bar's own date (which would make every
    row trivially 'recent', age 0) and NOT an implicit clock read at an arbitrary moment. The
    master date is also stamped onto the row as ``lot_size_master_date``, so the judgement is
    reproducible and auditable rather than depending on when the backfill happened to run.

    True run-to-run idempotency is impossible in principle here -- the confidence genuinely
    depends on how old the bar is relative to the master we read -- so the honest design is to
    RECORD the reference, not to pretend it away."""
    try:
        ref = _as_date(master_date) if master_date is not None else date.today()
        age = (ref - _as_date(bar_date)).days
    except Exception:  # noqa: BLE001 — an unparseable date is never silently trusted
        return LOT_CONF_UNVERIFIABLE
    return LOT_CONF_RECENT if age <= LOT_SIZE_TRUST_DAYS else LOT_CONF_UNVERIFIABLE


# --------------------------------------------------------------------------------- small helpers
def _as_date(d):
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    try:
        return date.fromisoformat(str(d)[:10])
    except Exception:  # noqa: BLE001
        return None


def _iso(d) -> str:
    dd = _as_date(d)
    return dd.isoformat() if dd else str(d)[:10]


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _f(x):
    """float(x) or None — never raises on a junk field."""
    try:
        if x is None:
            return None
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:  # noqa: BLE001
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get_kite_client():
    """The backend-native authenticated Kite client. ``services.kite_auth`` is the single source of
    truth for Kite credentials — we REUSE it and never build a second client. Imported lazily so this
    module imports fine where kiteconnect/services are absent (guarded boot)."""
    from services.kite_auth import get_kite_client
    return get_kite_client()


# --------------------------------------------------------------------------------- master + quotes
def _split_master(kite, underlying: str):
    """(option_rows, future_rows) for ``underlying`` from ONE ``instruments("NFO")`` call, normalized
    to the exact field set we archive. This is the row set that makes a past-date chain legible at
    all — without it, the snapshot would be quotes with no way to know what they were quotes ON."""
    opts, futs = [], []
    for ins in kite.instruments("NFO"):
        try:
            if str(ins.get("name") or "").upper() != str(underlying).upper():
                continue
            itype = str(ins.get("instrument_type") or "").upper()
            if itype not in ("CE", "PE", "FUT"):
                continue
            row = {
                "tradingsymbol": ins.get("tradingsymbol"),
                "instrument_token": int(ins.get("instrument_token") or 0) or None,
                "exchange_token": ins.get("exchange_token"),
                "name": ins.get("name"),
                "instrument_type": itype,
                "strike": _f(ins.get("strike")) if itype in ("CE", "PE") else None,
                "expiry": _iso(ins.get("expiry")) if ins.get("expiry") else None,
                "lot_size": int(ins.get("lot_size") or 0) or None,
                "tick_size": _f(ins.get("tick_size")),
                "segment": ins.get("segment"),
                "exchange": ins.get("exchange") or "NFO",
            }
            if not row["tradingsymbol"] or not row["expiry"]:
                continue
            (opts if itype in ("CE", "PE") else futs).append(row)
        except Exception as e:  # noqa: BLE001 — one malformed master row never sinks the batch
            log.debug("master row skipped: %s", e)
            continue
    return opts, futs


def _quote_all(kite, keys, errors: list):
    """Batched ``quote()`` over ``keys`` (["NFO:SYM", ...]), chunked under the Kite cap.
    A failed CHUNK is recorded and skipped — its instruments then land in skipped_no_quote, which is
    reported, rather than silently reducing the chain."""
    out: dict = {}
    batches = 0
    for chunk in _chunks(list(keys), QUOTE_CHUNK):
        batches += 1
        try:
            res = kite.quote(chunk) or {}
            if isinstance(res, dict):
                out.update(res)
        except Exception as e:  # noqa: BLE001
            errors.append({"stage": "quote", "n": len(chunk),
                           "error": f"{type(e).__name__}: {e}"})
            log.warning("quote chunk of %d failed: %s", len(chunk), e)
    return out, batches


def _best_bid_ask(q: dict):
    """(bid, ask) from the 5-level depth, or (None, None). Guarded on every field."""
    try:
        depth = q.get("depth") or {}
        buys = depth.get("buy") or []
        sells = depth.get("sell") or []
        bid = _f(buys[0].get("price")) if buys else None
        ask = _f(sells[0].get("price")) if sells else None
        return bid, ask
    except Exception:  # noqa: BLE001
        return None, None


def _quote_ts(q: dict):
    """The most recent MEANINGFUL timestamp on a quote, or None.

    MEASURED post-market (live, 2026-08-31 18:44 IST): Kite returns ``timestamp`` =
    ``1970-01-01 05:30:00`` (epoch) once the session has closed, while ``last_trade_time`` still
    carries the true session time (15:39:58). So we take the LATEST non-epoch value across
    exchange_timestamp / timestamp / last_trade_time. Falling back to last_trade_time is not a
    loosening: post-market the last trade IS the price we are archiving, and a contract that did not
    trade today then correctly reads as stale rather than being archived at a price from another day.
    """
    best = None
    for k in ("exchange_timestamp", "timestamp", "last_trade_time"):
        d = _as_date(q.get(k)) if q.get(k) else None
        if d is None or d.year <= 1971:        # epoch sentinel -> not a real timestamp
            continue
        best = d if best is None else max(best, d)
    return best


def _capture_mode(quotes: dict) -> str:
    """"market_hours" or "post_market", DETECTED from the depth actually present — never assumed
    from the clock (the clock lies on holidays, half-days and a delayed feed).

    MEASURED post-market: the 5-level depth is returned as all zeros and ``volume`` /
    ``average_price`` / ``net_change`` reset to 0, while ``last_price``, ``ohlc`` and ``oi`` remain
    correct. So after the close there is no book to quote a mid from — and inventing one would
    fabricate the spread, the credit and therefore the EV. We record the mode instead."""
    total = booked = 0
    for q in quotes.values():
        if not isinstance(q, dict):
            continue
        total += 1
        b, a = _best_bid_ask(q)
        if (b and b > 0) or (a and a > 0):
            booked += 1
    if total == 0:
        return "post_market"
    return "market_hours" if (booked / total) >= 0.05 else "post_market"


def _price_row(inst: dict, q, as_of_d, mode: str = "market_hours"):
    """Turn one master row + its quote into a priced entry, or (None, reason).

    Rejections are MODE-AWARE, because the feed itself is:

      market_hours — a real book exists. Reject: no quote; CROSSED (bid > ask); zero bid AND zero
        ask (no two-sided market — an LTP with no book is a print from an unknown moment, and
        accepting it would fabricate a transactable credit); a stale timestamp.
      post_market  — MEASURED: Kite zeroes the whole depth (and volume/average_price) after the
        close while last_price / ohlc / oi stay correct. Demanding a book here would reject the
        ENTIRE chain, which is exactly when this post-market agent runs. So the close is taken as
        the price with ``price_source="ltp"``, bid/ask/mid/spread_pct are left None (NOT synthesised),
        and the snapshot is stamped ``capture_mode="post_market"`` so every consumer can see that
        no spread was measured. Stale timestamps are still rejected — a contract that did not trade
        today has no today price."""
    if not q:
        return None, "no_quote"
    ltp = _f(q.get("last_price"))
    bid, ask = _best_bid_ask(q)
    ts = _quote_ts(q)

    if ts is not None and as_of_d is not None and ts != as_of_d:
        return None, "stale_quote_timestamp"

    has_bid = bool(bid and bid > 0)
    has_ask = bool(ask and ask > 0)

    if mode == "post_market":
        # No book exists at all after the close; take the day's last traded price.
        px, price_source, mid = ltp, "ltp", None
        bid = ask = None
        spread_pct = None
        if not px or px <= 0:
            return None, "no_usable_price"
    else:
        if has_bid and has_ask and bid > ask:
            return None, "crossed_quote"
        if not has_bid and not has_ask:
            return None, "no_two_sided_market"
        # price_source is RECORDED, not assumed: mid vs LTP changes the credit and therefore the EV.
        if has_bid and has_ask:
            mid = (bid + ask) / 2.0
            px, price_source = mid, "mid"
        else:
            mid = None
            px, price_source = ltp, "ltp"   # one-sided book: fall back to LTP, and say so
        if not px or px <= 0:
            return None, "no_usable_price"
        spread_pct = ((ask - bid) / mid * 100.0) if (mid and mid > 0) else None
    return {
        "tradingsymbol": inst["tradingsymbol"],
        "instrument_token": inst["instrument_token"],
        "strike": inst["strike"],
        "right": inst["instrument_type"],
        "expiry": inst["expiry"],
        "lot_size": inst["lot_size"],
        "tick_size": inst["tick_size"],
        "ltp": ltp,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "px": px,
        "price_source": price_source,
        "spread_pct": spread_pct,
        "oi": int(q.get("oi") or 0),
        # MEASURED: post-market Kite resets volume/average_price to 0 while OI stays correct. A 0
        # there would be a FALSE zero (an ATM strike that traded 4 lakh lots would read as untraded),
        # so post-market volume is recorded as None = unknown, never as zero.
        "volume": (None if mode == "post_market"
                   else int(q.get("volume") or q.get("volume_traded") or 0)),
        "price_basis": ("close" if mode == "post_market" else "live_book"),
        "quote_date": ts.isoformat() if ts else None,
    }, None


# --------------------------------------------------------------------------------- forwards
def _implied_forwards(pairs: dict, T: float, r: float):
    """[(K, F_implied)] from put-call parity: F = K + e^(rT)(C - P), for strikes quoting both legs."""
    out = []
    grow = math.exp(r * T)
    for K, leg in pairs.items():
        c, p = leg.get("CE"), leg.get("PE")
        if c is None or p is None:
            continue
        out.append((K, K + grow * (c["px"] - p["px"])))
    return out


def _median(xs):
    s = sorted(xs)
    n = len(s)
    if not n:
        return None
    return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])


def _pick_atm(pairs: dict, band: int):
    """The ``band`` strikes with the smallest |C - P| (the ATM neighbourhood), no seed price needed."""
    scored = []
    for K, leg in pairs.items():
        c, p = leg.get("CE"), leg.get("PE")
        if c is None or p is None:
            continue
        scored.append((abs(c["px"] - p["px"]), K))
    scored.sort()
    return [K for _, K in scored[:max(int(band), 1)]]


def _forward_for_expiry(exp_iso: str, T: float, pairs: dict, fut_quotes: dict, r: float):
    """(forward, source, detail) for ONE expiry, from THIS snapshot only.

    Preference order: same-expiry future -> parity-implied from near-ATM pairs -> nearest LATER
    future (labelled, basis-contaminated). ``source`` is archived per expiry so nothing downstream
    can treat an inferred forward as a measured one."""
    detail = {}
    same = fut_quotes.get(exp_iso)
    atm = _pick_atm(pairs, ATM_BAND)
    implied = [fk for K, fk in _implied_forwards({k: pairs[k] for k in atm}, T, r)] if atm else []
    f_implied = _median(implied) if implied else None
    if f_implied is not None:
        detail["parity_implied_forward"] = f_implied
        detail["parity_implied_n"] = len(implied)

    if same and same.get("px"):
        detail["futures_tradingsymbol"] = same.get("tradingsymbol")
        detail["futures_px"] = same["px"]
        return float(same["px"]), "fut_same_expiry", detail

    if f_implied is not None and f_implied > 0:
        return float(f_implied), "parity_implied", detail

    later = sorted([(e, v) for e, v in fut_quotes.items() if e >= exp_iso and v.get("px")])
    if later:
        e, v = later[0]
        detail["futures_tradingsymbol"] = v.get("tradingsymbol")
        detail["futures_expiry"] = e
        detail["futures_px"] = v["px"]
        return float(v["px"]), f"fut_nearest_{e}", detail

    return None, "unavailable", detail


# --------------------------------------------------------------------------------- consistency guard
def _strike_step(strikes):
    """The modal gap of a strike ladder (e.g. 50 for NIFTY), or None. Median of positive diffs —
    robust to the gaps a sparsely-traded expiry leaves behind."""
    s = sorted({float(k) for k in strikes if k})
    diffs = [b - a for a, b in zip(s, s[1:]) if b > a]
    return _median(diffs) if diffs else None


def _consistency_guard(by_expiry: dict, forwards: dict, bad_atm: list, r: float,
                       tol_frac=None, min_pairs=None, label: str = "live", ladders=None):
    """The gate that runs BEFORE any write. Returns (ok, report).

    Three checks, all on the near-ATM band where a real two-sided market exists:
      1. CROSSED / rejected quotes inside the ATM band -> the book itself is broken.
      2. PUT-CALL PARITY (pricing.parity_residual) of every near-ATM pair against the expiry's
         internally-consistent reference forward (the median parity-implied forward). A stale leg or
         a mispriced strike shows up here as a residual far outside the quoted spread.
      3. FUTURES BASIS: where a same-expiry future exists, the parity-implied forward must agree with
         it. Disagreement means the option and future legs were captured at different moments.

    Fail-closed: fewer than MIN_PARITY_PAIRS verifiable pairs -> NOT ok ("could not verify"), never
    an optimistic pass. A snapshot we cannot certify must not become permanent evidence."""
    from . import pricing

    # Resolved at CALL time (not as default args) so the module constants stay monkeypatchable and so
    # the backfill can pass its own, LOOSER, explicitly-named tolerance: daily closes on two legs are
    # not simultaneous prints, unlike a live quote pair, so holding them to the live tolerance would
    # reject honest data. The tolerance actually used is reported in the guard report.
    tol_frac = PARITY_TOL_FRAC if tol_frac is None else float(tol_frac)
    min_pairs = MIN_PARITY_PAIRS if min_pairs is None else int(min_pairs)

    checked = failed = 0
    detail: list = []
    per_expiry: dict = {}

    for exp_iso, pairs in by_expiry.items():
        e_checked = e_failed = 0
        e_reason = None
        fwd = forwards.get(exp_iso) or {}
        F = fwd.get("forward")
        T = fwd.get("T")
        if not F or not T or T <= 0:
            per_expiry[exp_iso] = {"ok": False, "checked": 0, "failed": 0,
                                   "reason": "no usable forward / already at expiry"}
            continue
        atm = _pick_atm(pairs, ATM_BAND)
        if not atm:
            per_expiry[exp_iso] = {"ok": False, "checked": 0, "failed": 0,
                                   "reason": "no CE/PE pair quoted for this expiry"}
            continue
        # "Near-ATM" is ATM_BAND STRIKE STEPS from the forward — derived from the expiry's own
        # listed ladder, not from the span of the surviving pairs. MEASURED on the live chain: a
        # far-dated expiry trades only a handful of scattered strikes, so a span-based band swept in
        # deep wings 6,000 points away and scored their (genuinely, honestly) stale quotes as ATM
        # violations. The ladder step keeps "near-ATM" meaning near-ATM on every expiry.
        step = _strike_step((ladders or {}).get(exp_iso) or sorted(pairs.keys()))
        band_width = ATM_BAND * step if step else max(abs(min(atm) - F), abs(max(atm) - F))

        # (1) any rejected/crossed quote inside the ATM band is a hard violation
        for bq in bad_atm:
            if bq.get("expiry") != exp_iso:
                continue
            k = bq.get("strike")
            if k is None or abs(k - F) > band_width:
                continue
            if bq.get("issue") in ("crossed_quote", "stale_quote_timestamp"):
                e_failed += 1
                detail.append({"expiry": exp_iso, "strike": k, "check": "atm_quote",
                               "issue": bq.get("issue"), "tradingsymbol": bq.get("tradingsymbol")})

        implied = _implied_forwards({k: pairs[k] for k in atm}, T, r)
        if not implied:
            per_expiry[exp_iso] = {"ok": False, "checked": 0, "failed": e_failed,
                                   "reason": "no near-ATM CE/PE pair to verify against"}
            failed += e_failed
            continue
        f_ref = _median([fk for _, fk in implied])

        # (2) parity residual of every near-ATM pair vs the reference forward
        for K in atm:
            leg = pairs[K]
            c, p = leg.get("CE"), leg.get("PE")
            if c is None or p is None:
                continue
            e_checked += 1
            resid = pricing.parity_residual(c["px"], p["px"], f_ref, K, T, r)
            half_spread = 0.0
            for legq in (c, p):
                b, a = legq.get("bid"), legq.get("ask")
                if b is not None and a is not None and a >= b:
                    half_spread += (a - b) / 2.0
            tol = max(tol_frac * f_ref, half_spread + PARITY_SPREAD_ALLOWANCE)
            if abs(resid) > tol:
                e_failed += 1
                detail.append({"expiry": exp_iso, "strike": K, "check": "parity",
                               "residual": resid, "tolerance": tol,
                               "call": c["tradingsymbol"], "put": p["tradingsymbol"]})

        # (3) parity-implied forward vs the same-expiry future
        if fwd.get("source") == "fut_same_expiry" and f_ref:
            basis = abs(f_ref - F)
            if basis > FUT_BASIS_TOL_FRAC * F:
                e_failed += 1
                detail.append({"expiry": exp_iso, "check": "futures_basis",
                               "parity_implied_forward": f_ref, "futures_forward": F,
                               "diff": basis, "tolerance": FUT_BASIS_TOL_FRAC * F})

        if e_checked < min_pairs:
            e_reason = (f"could not verify — only {e_checked} near-ATM CE/PE pair(s) checkable "
                        f"(need {min_pairs})")
        elif e_failed > MAX_PARITY_FAILS:
            e_reason = f"{e_failed} near-ATM consistency violation(s) (parity/crossed/basis)"
        per_expiry[exp_iso] = {"ok": e_reason is None, "checked": e_checked, "failed": e_failed,
                               "reason": e_reason, "forward_ref": f_ref}
        checked += e_checked
        failed += e_failed

    accepted = sorted(e for e, v in per_expiry.items() if v["ok"])
    rejected = sorted(e for e, v in per_expiry.items() if not v["ok"])
    report = {"parity_checked": checked, "parity_failed": failed, "guard_label": label,
              "parity_tol_frac": tol_frac, "fut_basis_tol_frac": FUT_BASIS_TOL_FRAC,
              "atm_band": ATM_BAND, "min_pairs": min_pairs, "detail": detail[:50],
              "per_expiry": per_expiry, "expiries_accepted": accepted,
              "expiries_rejected": rejected,
              "policy": ("QUARANTINE-PER-EXPIRY: an expiry that fails is EXCLUDED from the "
                         "snapshot (recorded with its reason); the write aborts only when NO "
                         "expiry passes. Bad data still never enters the archive.")}

    if not accepted:
        if checked < min_pairs:
            report["reason"] = (f"consistency guard could not verify — only {checked} near-ATM "
                                f"CE/PE pair(s) checkable across the whole chain (need {min_pairs})")
        else:
            report["reason"] = (f"every expiry failed the consistency guard "
                                f"({failed} near-ATM violation(s)) — snapshot rejected")
        return False, report
    return True, report


# --------------------------------------------------------------------------------- public entry point
def snapshot_chain(as_of, underlying: str = "NIFTY", _client=None, overwrite: bool = False,
                   allow_backdate: bool = False, write: bool = True) -> dict:
    """Capture and ARCHIVE the live option chain for ``underlying`` as of ``as_of``.

    Args:
        as_of:          the session date being archived. Must be TODAY unless ``allow_backdate``
                        (a live chain filed under a past date is fabricated history — see below).
        underlying:     F&O ``name`` (e.g. "NIFTY").
        _client:        test seam — an already-authenticated Kite client. Prod passes nothing and
                        the client comes from services.kite_auth.
        overwrite:      operator repair only. Snapshots are immutable by default.
        write:          False = dry run (build + guard, archive nothing).

    Returns a status dict — NEVER raises:
        {as_of, underlying, ok, aborted, reason, store, coverage, guard, forwards, expiries,
         lot_size, spot, errors, captured_at, forward_policy}

    ABORT semantics: on any consistency-guard failure NOTHING is written and ``reason`` says why.
    That is deliberately harsher than the Chart Agent's equivalent guard, because a bad daily bar can
    be re-fetched tomorrow and a bad option chain can NEVER be re-fetched.
    """
    from . import pricing
    from . import store as snap_store

    as_of_iso = _iso(as_of)
    as_of_d = _as_date(as_of_iso)
    r = pricing.R_DEFAULT

    coverage = {
        "options_seen": 0, "quoted": 0, "iv_solved": 0,
        "skipped_no_quote": 0, "skipped_bad_quote": 0,
        "parity_checked": 0, "parity_failed": 0,
        # extra buckets so every instrument is accounted for (nothing hidden)
        "iv_unsolved": 0, "greeks_failed": 0, "skipped_expired": 0,
        "skipped_no_forward": 0, "futures_seen": 0, "futures_quoted": 0,
        "expiries_seen": 0, "expiries_priced": 0, "quote_batches": 0,
    }
    result = {"as_of": as_of_iso, "underlying": str(underlying), "ok": False, "aborted": False,
              "reason": None, "store": None, "coverage": coverage, "guard": None,
              "forwards": {}, "expiries": [], "expiries_rejected": [], "capture_mode": None,
              "lot_size": None, "spot": None,
              "errors": [], "captured_at": _now_iso(), "forward_policy": FORWARD_POLICY,
              "evidence_basis": "forward_tracked_snapshot"}

    def _abort(reason: str):
        result["aborted"] = True
        result["reason"] = reason
        log.warning("snapshot_chain(%s,%s) ABORT — %s (nothing archived)",
                    underlying, as_of_iso, reason)
        return result

    # ---- GUARD 0: a LIVE chain may only ever be filed under TODAY's date --------------------
    # quote() returns the chain as it is right now. Archiving that under a past date would
    # manufacture history that never existed — the exact failure this whole store exists to avoid.
    today = datetime.now().date()
    if as_of_d is None:
        return _abort(f"unparseable as_of {as_of!r}")
    if as_of_d != today and not allow_backdate:
        return _abort(f"as_of {as_of_iso} is not today ({today.isoformat()}) — a LIVE chain cannot "
                      f"be archived under another date (that would fabricate history). Pass "
                      f"allow_backdate=True only for a synthetic/test client.")

    # ---- client + instrument master ---------------------------------------------------------
    try:
        kite = _client if _client is not None else _get_kite_client()
    except Exception as e:  # noqa: BLE001
        return _abort(f"no Kite client ({type(e).__name__}: {e})")

    try:
        opts, futs = _split_master(kite, underlying)
    except Exception as e:  # noqa: BLE001
        return _abort(f"instruments('NFO') failed ({type(e).__name__}: {e})")

    coverage["options_seen"] = len(opts)
    coverage["futures_seen"] = len(futs)
    if not opts:
        return _abort(f"no CE/PE instruments found for {underlying} in the NFO master")

    # Drop contracts already expired on as_of (they are not part of the tradable chain).
    live_opts = []
    for o in opts:
        ed = _as_date(o["expiry"])
        if ed is None or (as_of_d and ed < as_of_d):
            coverage["skipped_expired"] += 1
            continue
        live_opts.append(o)
    coverage["expiries_seen"] = len({o["expiry"] for o in live_opts})

    # ---- quotes (batched, chunked under the Kite cap) ---------------------------------------
    keys = [f"NFO:{o['tradingsymbol']}" for o in live_opts] + \
           [f"NFO:{f['tradingsymbol']}" for f in futs]
    quotes, batches = _quote_all(kite, keys, result["errors"])
    coverage["quote_batches"] = batches
    if not quotes:
        return _abort("quote() returned nothing for the whole chain")

    # ---- capture mode: DETECTED from the feed, not from the clock ---------------------------
    mode = _capture_mode(quotes)
    result["capture_mode"] = mode
    coverage["capture_mode"] = mode

    # ---- futures leg (the forward source we archive alongside the options) ------------------
    fut_quotes: dict = {}
    for fr in futs:
        q = quotes.get(f"NFO:{fr['tradingsymbol']}")
        entry, issue = _price_row(fr, q, as_of_d, mode)
        if entry is None:
            # A future without a clean quote is not fatal — parity-implied forwards still work.
            result["errors"].append({"stage": "future", "tradingsymbol": fr["tradingsymbol"],
                                     "issue": issue})
            continue
        coverage["futures_quoted"] += 1
        prev = fut_quotes.get(fr["expiry"])
        if prev is None:
            fut_quotes[fr["expiry"]] = entry

    # ---- price every option row (guarded per strike) ----------------------------------------
    by_expiry: dict = {}          # {expiry -> {strike -> {"CE": entry, "PE": entry}}}
    priced: list = []
    bad_quotes: list = []
    for o in live_opts:
        try:
            q = quotes.get(f"NFO:{o['tradingsymbol']}")
            entry, issue = _price_row(o, q, as_of_d, mode)
            if entry is None:
                if issue == "no_quote":
                    coverage["skipped_no_quote"] += 1
                else:
                    coverage["skipped_bad_quote"] += 1
                bad_quotes.append({"tradingsymbol": o["tradingsymbol"], "expiry": o["expiry"],
                                   "strike": o["strike"], "right": o["instrument_type"],
                                   "issue": issue})
                continue
            coverage["quoted"] += 1
            priced.append(entry)
            by_expiry.setdefault(o["expiry"], {}).setdefault(entry["strike"], {})[
                entry["right"]] = entry
        except Exception as e:  # noqa: BLE001 — one bad strike is skipped and reported, never fatal
            coverage["skipped_bad_quote"] += 1
            bad_quotes.append({"tradingsymbol": o.get("tradingsymbol"), "expiry": o.get("expiry"),
                               "strike": o.get("strike"), "issue": f"{type(e).__name__}: {e}"})

    if not priced:
        return _abort("no option row survived the quote checks (nothing to archive)")

    # ---- forward per expiry (from THIS snapshot only) ---------------------------------------
    forwards: dict = {}
    for exp_iso, pairs in by_expiry.items():
        ed = _as_date(exp_iso)
        dte = (ed - as_of_d).days if (ed and as_of_d) else 0
        T = pricing.year_fraction(dte)
        F, src, detail = _forward_for_expiry(exp_iso, max(T, 1e-9), pairs, fut_quotes, r)
        forwards[exp_iso] = {"forward": F, "source": src, "T": T, "dte": dte, **detail}

    # ---- consistency guard: ABORT before any write ------------------------------------------
    # Post-market legs are two independent CLOSES, not a simultaneous quote pair, so they get the
    # same looser, explicitly-named tolerance the backfill uses. The tolerance actually applied is
    # written into the guard report, so it can never be loosened silently for a live capture.
    ladders: dict = {}
    for o in live_opts:
        if o["strike"]:
            ladders.setdefault(o["expiry"], set()).add(o["strike"])
    ok, guard = _consistency_guard(
        by_expiry, forwards, bad_quotes, r,
        tol_frac=(CLOSE_PARITY_TOL_FRAC if mode == "post_market" else PARITY_TOL_FRAC),
        label=mode, ladders=ladders)
    coverage["parity_checked"] = guard.get("parity_checked", 0)
    coverage["parity_failed"] = guard.get("parity_failed", 0)
    result["guard"] = guard
    result["forwards"] = forwards
    if not ok:
        result["expiries"] = []
        return _abort(guard.get("reason") or "consistency guard failed")

    # ---- QUARANTINE the expiries that failed: they are EXCLUDED from the archive, with their
    # reason recorded. A far-dated NIFTY expiry trades a handful of scattered strikes whose closes
    # genuinely cannot be reconciled — aborting the whole capture for that would throw away the
    # liquid weeklies, which are the actual trading universe, and we can never re-fetch the day.
    accepted = set(guard.get("expiries_accepted") or [])
    rejected_expiries = [{"expiry": e, "reason": (guard["per_expiry"].get(e) or {}).get("reason")}
                         for e in (guard.get("expiries_rejected") or [])]
    forwards = {e: v for e, v in forwards.items() if e in accepted}
    priced = [e for e in priced if e["expiry"] in accepted]
    coverage["expiries_accepted"] = len(accepted)
    coverage["expiries_rejected"] = len(rejected_expiries)
    coverage["skipped_failed_expiry"] = coverage["quoted"] - len(priced)
    result["expiries"] = sorted(accepted)
    result["expiries_rejected"] = rejected_expiries
    if not priced:
        return _abort("no option row survived the per-expiry consistency guard")

    # ---- IV + greeks (guarded per strike) ----------------------------------------------------
    rows: list = []
    for e in priced:
        row = dict(e)
        fwd = forwards.get(e["expiry"]) or {}
        F, T = fwd.get("forward"), fwd.get("T")
        row["forward"] = F
        row["forward_source"] = fwd.get("source")
        row["dte"] = fwd.get("dte")
        row["T_years"] = T
        row["iv"] = row["delta"] = row["gamma"] = row["theta"] = row["vega"] = None
        try:
            if not F or not T or T <= 0 or not e["strike"]:
                coverage["skipped_no_forward"] += 1
            else:
                iv = pricing.implied_vol(e["px"], F, e["strike"], T, e["right"], r)
                if iv is None:
                    coverage["iv_unsolved"] += 1     # honest: no IV exists for this quote
                else:
                    row["iv"] = iv
                    coverage["iv_solved"] += 1
                    g = pricing.greeks(F, e["strike"], T, iv, e["right"], r)
                    row["delta"] = g["delta"]
                    row["gamma"] = g["gamma"]
                    row["theta"] = g["theta_per_day"]
                    row["vega"] = g["vega_per_1pct_vol"]
        except Exception as ex:  # noqa: BLE001 — one bad strike never sinks the batch
            coverage["greeks_failed"] += 1
            result["errors"].append({"stage": "iv", "tradingsymbol": e["tradingsymbol"],
                                     "error": f"{type(ex).__name__}: {ex}"})
        row.pop("px", None)
        rows.append(row)
    coverage["expiries_priced"] = len({r_["expiry"] for r_ in rows})

    # ---- spot (context only — NOT the pricing forward). Best effort, fully guarded. ----------
    spot = None
    skey = _SPOT_KEY.get(str(underlying).upper())
    if skey:
        try:
            sq = kite.quote([skey]) or {}
            spot = _f((sq.get(skey) or {}).get("last_price"))
        except Exception as e:  # noqa: BLE001
            result["errors"].append({"stage": "spot", "error": f"{type(e).__name__}: {e}"})
    result["spot"] = spot

    lot_counts: dict = {}
    for row in rows:
        if row.get("lot_size"):
            lot_counts[row["lot_size"]] = lot_counts.get(row["lot_size"], 0) + 1
    lot_size = max(lot_counts.items(), key=lambda kv: kv[1])[0] if lot_counts else None
    result["lot_size"] = lot_size

    payload = {
        "schema": snap_store.SCHEMA,
        "underlying": str(underlying),
        "as_of": as_of_iso,
        "captured_at": result["captured_at"],
        "source": "kite: instruments('NFO') + quote()",
        "basis": "live_quote_snapshot",
        "capture_mode": mode,
        # Post-market captures carry the SAME kind of caveat a backfill does — no measured spread —
        # so the liquidity gate must report SKIPPED-with-reason and slippage must come from a
        # governed labelled assumption, never from a synthesised spread.
        "quote_basis": (POSTMARKET_QUOTE_BASIS if mode == "post_market" else None),
        "slippage_policy": (BACKFILL_SLIPPAGE_POLICY if mode == "post_market" else None),
        "liquidity_gate_policy": (BACKFILL_LIQUIDITY_POLICY if mode == "post_market" else None),
        "r": r,
        "lot_size": lot_size,
        "spot": spot,
        "expiries": result["expiries"],
        # Expiries the guard QUARANTINED: listed in the master (that fact is archived below) but not
        # archived with quotes, each with its named reason. Never silently dropped.
        "expiries_rejected": rejected_expiries,
        "forwards": forwards,
        "forward_policy": FORWARD_POLICY,
        # THE archive: the instrument master rows as they existed today. Without these, a past-date
        # chain is unreadable — expiries_as_of() and the point-in-time lot_size both read them.
        "instruments": live_opts + futs,
        "rows": rows,
        "rejected": bad_quotes[:500],
        "coverage": coverage,
        "guard": guard,
        "evidence_basis": "forward_tracked_snapshot",
    }

    if not write:
        result["ok"] = True
        result["store"] = {"status": "dry_run", "written": False,
                           "uri": snap_store.uri(as_of_iso, underlying)}
        return result

    st = snap_store.write(as_of_iso, underlying, payload, overwrite=overwrite)
    result["store"] = st
    result["ok"] = bool(st.get("written"))
    if not st.get("written"):
        # "exists" is the immutability rule doing its job, not an error. Reported either way.
        result["reason"] = st.get("reason") or st.get("status")
    log.info("snapshot_chain(%s,%s): %s rows=%d quoted=%d iv=%d parity=%d/%d",
             underlying, as_of_iso, st.get("status"), len(rows), coverage["quoted"],
             coverage["iv_solved"], coverage["parity_failed"], coverage["parity_checked"])
    return result


# =============================================================================== HISTORY BACKFILL
# One-shot seeding of the archive from historical_data() on the currently-listed ladder. This buys us
# a REAL dataset today instead of accruing one session at a time — but it is a STRICTLY WEAKER kind
# of evidence than a live snapshot, in three named ways that are written into every payload it
# produces (BACKFILL_QUOTE_BASIS / BACKFILL_SURVIVORSHIP / BACKFILL_LOT_SIZE_SOURCE) and surfaced by
# data.load_chain as ``basis``. It does NOT replace the daily snapshot job; it precedes it.

def _backfill_dir(underlying: str) -> str:
    """Raw per-contract history cache (the RESUME point). Separate from the snapshot store: this is
    fetch scratch, keyed by contract; the store is the evidence archive, keyed by date."""
    import os
    root = os.environ.get(ENV_BACKFILL_DIR)
    if not root:
        backend = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        root = os.path.join(backend, "var", "options_backfill")
    return os.path.join(root, str(underlying).upper())


def _cache_path(underlying: str, tradingsymbol: str) -> str:
    import os
    safe = "".join(c if (c.isalnum() or c in "-_.") else "_" for c in str(tradingsymbol))
    return os.path.join(_backfill_dir(underlying), f"{safe}.json.gz")


def _cache_read(path):
    """Cached raw history, or None. Guarded — a corrupt cache entry is simply re-fetched."""
    import gzip
    import json
    import os
    try:
        if not os.path.exists(path):
            return None
        with gzip.open(path, "rb") as f:
            return json.loads(f.read().decode("utf-8"))
    except Exception as e:  # noqa: BLE001
        log.debug("backfill cache read %s failed (will refetch): %s", path, e)
        return None


def _cache_write(path, obj) -> bool:
    import gzip
    import json
    import os
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with gzip.open(tmp, "wb") as f:
            f.write(json.dumps(obj, separators=(",", ":"), default=str).encode("utf-8"))
        os.replace(tmp, path)          # atomic: an interrupted run never leaves a half cache entry
        return True
    except Exception as e:  # noqa: BLE001
        log.debug("backfill cache write %s failed: %s", path, e)
        return False


def _day_windows(lo: date, hi: date):
    cur = lo
    while cur <= hi:
        end = min(cur + timedelta(days=BACKFILL_MAX_WINDOW_DAYS - 1), hi)
        yield cur, end
        cur = end + timedelta(days=1)


def _fetch_contract_history(kite, inst: dict, lo: date, hi: date, pace_s: float, errors: list):
    """Full daily history (with OI) for ONE contract, chunked under the per-request day cap.
    Returns a list of normalized bar dicts, or None on failure (recorded, never raised)."""
    import time
    out = []
    try:
        for w_lo, w_hi in _day_windows(lo, hi):
            raw = kite.historical_data(
                inst["instrument_token"],
                datetime.combine(w_lo, datetime.min.time()),
                datetime.combine(w_hi, datetime.max.time()),
                "day", oi=True)
            for b in (raw or []):
                bd = _as_date(b.get("date"))
                if bd is None or bd > hi or bd < lo:
                    continue
                out.append({"date": bd.isoformat(),
                            "open": _f(b.get("open")), "high": _f(b.get("high")),
                            "low": _f(b.get("low")), "close": _f(b.get("close")),
                            "volume": int(b.get("volume") or 0),
                            "oi": int(b.get("oi") or 0)})
            if pace_s:
                time.sleep(float(pace_s))     # respect the ~3 req/s historical rate limit
    except Exception as e:  # noqa: BLE001 — one bad contract never sinks a 1,594-call job
        errors.append({"stage": "history", "tradingsymbol": inst.get("tradingsymbol"),
                       "error": f"{type(e).__name__}: {e}"})
        return None
    return out


def _hist_row(inst: dict, bar: dict, lot_size, master_date=None):
    """One backfilled chain row. bid/ask/mid/spread_pct are DELIBERATELY None and price_source is
    'hist_close' — the feed has no book, and synthesising one would manufacture a spread, a credit
    and therefore an EV out of nothing.

    lot_size carries a per-row CONFIDENCE (see LOT_SIZE_TRUST_DAYS): today's lot is stamped onto
    every historical bar because the feed carries none, which silently mis-scales rupee totals for
    bars predating a lot revision."""
    close = bar.get("close")
    if close is None or close <= 0:
        return None
    return {
        "tradingsymbol": inst["tradingsymbol"],
        "instrument_token": inst["instrument_token"],
        "strike": inst["strike"],
        "right": inst["instrument_type"],
        "expiry": inst["expiry"],
        "lot_size": lot_size,
        "lot_size_source": BACKFILL_LOT_SIZE_SOURCE,
        "lot_size_confidence": _lot_confidence(bar.get("date"), master_date),
        "lot_size_master_date": (_as_date(master_date).isoformat()
                                 if master_date is not None else None),
        "tick_size": inst.get("tick_size"),
        "ltp": close,                 # the day's last traded price genuinely IS the close
        "close": close,
        "open": bar.get("open"), "high": bar.get("high"), "low": bar.get("low"),
        "bid": None, "ask": None, "mid": None, "spread_pct": None,
        "price_source": PRICE_SOURCE_HIST,
        "oi": bar.get("oi"), "volume": bar.get("volume"),
        "px": close,
    }


def backfill_history(underlying: str = "NIFTY", max_contracts=None, since=None, through=None,
                     pace_s: float = BACKFILL_PACE_S, _client=None, resume: bool = True,
                     write: bool = True, max_dates=None) -> dict:
    """Seed the archive from ``historical_data()`` on every CURRENTLY-LISTED contract, then reshape
    the per-contract series into per-DATE chain snapshots written through the SAME store seam.

    Args:
        underlying:    F&O ``name`` (e.g. "NIFTY").
        max_contracts: cap on OPTION contracts fetched (smoke tests). Futures are always fetched —
                       they are the forward, and dropping them would silently degrade every IV.
        since:         earliest bar date to keep (default ~BACKFILL_DEFAULT_LOOKBACK_DAYS back).
        through:       latest date to archive. ALWAYS clamped to yesterday: today's daily bar is
                       still forming, and today's date belongs to the live snapshot job.
        pace_s:        sleep between historical calls (~3 req/s cap). Tunable, not hardcoded.
        resume:        skip contracts already cached AND dates already archived. A ~1,594-call run
                       (~9 min) must survive an interruption without redoing work or double-writing.
        write:         False = build + guard only, archive nothing.
        max_dates:     cap on dates archived (smoke tests).

    Returns a status dict — NEVER raises:
        {underlying, since, through, basis, contracts_total, contracts_fetched, contracts_cached,
         contracts_failed, bars_total, dates_seen, dates_written, dates_skipped_existing,
         dates_rejected_guard, coverage, caveats, errors}

    Every date is guarded exactly like a live snapshot (parity/crossed, at the backfill tolerance);
    a date that fails is SKIPPED and counted, never archived. Immutability still holds, so a date
    already covered by a real live snapshot is never replaced by a weaker backfilled one."""
    from . import pricing
    from . import store as snap_store

    r = pricing.R_DEFAULT
    today = datetime.now().date()
    # The date the instrument master is read. Every backfilled row's lot_size comes from THIS
    # master, so this is the reference its lot_size_confidence is measured against, and it is
    # stamped on each row as lot_size_master_date. Reading the clock is legitimate here -- we
    # are about to read the live master -- and it is recorded rather than left implicit.
    master_date = today
    hi = min(_as_date(through) or (today - timedelta(days=1)), today - timedelta(days=1))
    lo = _as_date(since) or (hi - timedelta(days=BACKFILL_DEFAULT_LOOKBACK_DAYS))

    res = {"underlying": str(underlying), "since": lo.isoformat(), "through": hi.isoformat(),
           "basis": "backfill_from_listed_contracts", "pace_s": pace_s,
           "lot_size_master_date": master_date.isoformat(),
           "contracts_total": 0, "contracts_fetched": 0, "contracts_cached": 0,
           "contracts_failed": 0, "bars_total": 0,
           "dates_seen": 0, "dates_written": 0, "dates_skipped_existing": 0,
           "dates_rejected_guard": 0, "dates_rejected_detail": [],
           "coverage": {"options_seen": 0, "quoted": 0, "iv_solved": 0,
                        "skipped_no_quote": 0, "skipped_bad_quote": 0,
                        "parity_checked": 0, "parity_failed": 0,
                        "iv_unsolved": 0, "greeks_failed": 0, "skipped_no_forward": 0,
                        "futures_seen": 0, "futures_quoted": 0},
           "caveats": {"quote_basis": BACKFILL_QUOTE_BASIS,
                       "survivorship": BACKFILL_SURVIVORSHIP,
                       "lot_size": BACKFILL_LOT_SIZE_SOURCE,
                       "slippage": BACKFILL_SLIPPAGE_POLICY,
                       "liquidity_gate": BACKFILL_LIQUIDITY_POLICY},
           "aborted": False, "reason": None, "errors": []}

    if hi < lo:
        res["aborted"] = True
        res["reason"] = f"empty window ({lo} .. {hi})"
        return res

    try:
        kite = _client if _client is not None else _get_kite_client()
    except Exception as e:  # noqa: BLE001
        res["aborted"] = True
        res["reason"] = f"no Kite client ({type(e).__name__}: {e})"
        return res

    try:
        opts, futs = _split_master(kite, underlying)
    except Exception as e:  # noqa: BLE001
        res["aborted"] = True
        res["reason"] = f"instruments('NFO') failed ({type(e).__name__}: {e})"
        return res

    if not opts:
        res["aborted"] = True
        res["reason"] = f"no CE/PE instruments found for {underlying} in the NFO master"
        return res

    # Deterministic order so a resumed run walks the ladder identically.
    opts.sort(key=lambda o: (o["expiry"], o["strike"] or 0.0, o["instrument_type"]))
    futs.sort(key=lambda f_: f_["expiry"])
    if max_contracts:
        opts = opts[:int(max_contracts)]
    res["contracts_total"] = len(opts) + len(futs)
    res["coverage"]["options_seen"] = len(opts)
    res["coverage"]["futures_seen"] = len(futs)

    lot_by_expiry: dict = {}
    for o in opts:
        if o.get("lot_size"):
            lot_by_expiry.setdefault(o["expiry"], o["lot_size"])

    # ---- phase 1: fetch (resumable via the per-contract cache) -------------------------------
    hist: dict = {}                       # tradingsymbol -> [bars]
    for inst in list(opts) + list(futs):
        path = _cache_path(underlying, inst["tradingsymbol"])
        bars = _cache_read(path) if resume else None
        if bars is not None:
            res["contracts_cached"] += 1
        else:
            # A contract listed after `hi` has no bars in the window; clamp to its expiry so we never
            # request a window that ends after the contract can possibly have traded.
            c_hi = min(hi, _as_date(inst["expiry"]) or hi)
            bars = ([] if c_hi < lo
                    else _fetch_contract_history(kite, inst, lo, c_hi, pace_s, res["errors"]))
            if bars is None:
                res["contracts_failed"] += 1
                continue
            res["contracts_fetched"] += 1
            _cache_write(path, bars)
        hist[inst["tradingsymbol"]] = bars
        res["bars_total"] += len(bars)

    # ---- phase 2: reshape per-contract series -> per-DATE chains -----------------------------
    inst_by_ts = {i["tradingsymbol"]: i for i in (list(opts) + list(futs))}
    by_date: dict = {}                    # date -> {"opts": [(inst,bar)], "futs": {expiry: bar}}
    for ts, bars in hist.items():
        inst = inst_by_ts.get(ts)
        if inst is None:
            continue
        exp_d = _as_date(inst["expiry"])
        for b in bars:
            bd = _as_date(b.get("date"))
            if bd is None or bd < lo or bd > hi:
                continue
            if exp_d and bd > exp_d:      # a bar after the contract's own expiry is not real
                continue
            slot = by_date.setdefault(bd.isoformat(), {"opts": [], "futs": {}})
            if inst["instrument_type"] == "FUT":
                slot["futs"].setdefault(inst["expiry"], {"bar": b, "inst": inst})
            else:
                slot["opts"].append((inst, b))
    res["dates_seen"] = len(by_date)

    written = 0
    for d_iso in sorted(by_date):
        if max_dates and written >= int(max_dates):
            break
        try:
            if resume and snap_store.exists(d_iso, underlying):
                res["dates_skipped_existing"] += 1
                continue
            ok, payload, per_date = _build_backfill_payload(
                d_iso, underlying, by_date[d_iso], lot_by_expiry, r, lo, hi,
                master_date=master_date)
            res["coverage"]["parity_checked"] += per_date["guard"].get("parity_checked", 0)
            res["coverage"]["parity_failed"] += per_date["guard"].get("parity_failed", 0)
            for k in ("quoted", "iv_solved", "iv_unsolved", "greeks_failed",
                      "skipped_bad_quote", "skipped_no_forward", "futures_quoted"):
                res["coverage"][k] += per_date["coverage"].get(k, 0)
            if not ok:
                res["dates_rejected_guard"] += 1
                if len(res["dates_rejected_detail"]) < 25:
                    res["dates_rejected_detail"].append(
                        {"date": d_iso, "reason": per_date["guard"].get("reason")})
                continue
            if not write:
                written += 1
                continue
            st = snap_store.write(d_iso, underlying, payload)
            if st.get("written"):
                res["dates_written"] += 1
                written += 1
            elif st.get("status") == "exists":
                res["dates_skipped_existing"] += 1
            else:
                res["errors"].append({"stage": "write", "date": d_iso,
                                      "error": st.get("reason")})
        except Exception as e:  # noqa: BLE001 — one bad date never sinks the backfill
            res["errors"].append({"stage": "date", "date": d_iso,
                                  "error": f"{type(e).__name__}: {e}"})
    if not write:
        res["dates_written"] = 0
        res["dry_run_dates"] = written
    log.info("backfill_history(%s): contracts %d (fetched %d / cached %d / failed %d), "
             "dates %d -> written %d, skipped %d, guard-rejected %d",
             underlying, res["contracts_total"], res["contracts_fetched"],
             res["contracts_cached"], res["contracts_failed"], res["dates_seen"],
             res["dates_written"], res["dates_skipped_existing"], res["dates_rejected_guard"])
    return res


def _build_backfill_payload(d_iso: str, underlying: str, slot: dict, lot_by_expiry: dict,
                            r: float, lo: date, hi: date, master_date=None):
    """(ok, payload, per_date_accounting) for ONE backfilled date. Runs the SAME consistency guard as
    the live path (at the backfill tolerance); ``ok=False`` means the date is skipped, not archived."""
    from . import pricing
    from . import store as snap_store

    d = _as_date(d_iso)
    cov = {"quoted": 0, "iv_solved": 0, "iv_unsolved": 0, "greeks_failed": 0,
           "skipped_bad_quote": 0, "skipped_no_forward": 0, "futures_quoted": 0}

    # futures closes -> the forward candidates for this date
    fut_quotes: dict = {}
    for exp_iso, fx in (slot.get("futs") or {}).items():
        close = fx["bar"].get("close")
        if close and close > 0:
            fut_quotes[exp_iso] = {"tradingsymbol": fx["inst"]["tradingsymbol"], "px": float(close),
                                   "bid": None, "ask": None}
            cov["futures_quoted"] += 1

    by_expiry: dict = {}
    entries: list = []
    for inst, bar in (slot.get("opts") or []):
        row = _hist_row(inst, bar, lot_by_expiry.get(inst["expiry"]) or inst.get("lot_size"),
                        master_date=master_date)
        if row is None:
            cov["skipped_bad_quote"] += 1
            continue
        cov["quoted"] += 1
        entries.append(row)
        by_expiry.setdefault(inst["expiry"], {}).setdefault(row["strike"], {})[row["right"]] = row

    forwards: dict = {}
    for exp_iso, pairs in by_expiry.items():
        ed = _as_date(exp_iso)
        dte = (ed - d).days if (ed and d) else 0
        T = pricing.year_fraction(dte)
        F, src, detail = _forward_for_expiry(exp_iso, max(T, 1e-9), pairs, fut_quotes, r)
        forwards[exp_iso] = {"forward": F, "source": (src + "_hist") if src != "unavailable" else src,
                             "T": T, "dte": dte, **detail}

    ladders: dict = {}
    for inst, _b in (slot.get("opts") or []):
        if inst.get("strike"):
            ladders.setdefault(inst["expiry"], set()).add(inst["strike"])
    ok, guard = _consistency_guard(by_expiry, forwards, [], r, ladders=ladders,
                                   tol_frac=BACKFILL_PARITY_TOL_FRAC, label="backfill")
    if not ok:
        return False, None, {"coverage": cov, "guard": guard}

    # Same per-expiry quarantine as the live path: a far-dated expiry whose sparse closes cannot be
    # reconciled is EXCLUDED (with its reason), it does not sink the whole date.
    accepted = set(guard.get("expiries_accepted") or [])
    rejected_expiries = [{"expiry": e, "reason": (guard["per_expiry"].get(e) or {}).get("reason")}
                         for e in (guard.get("expiries_rejected") or [])]
    entries = [e for e in entries if e["expiry"] in accepted]
    forwards = {e: v for e, v in forwards.items() if e in accepted}
    cov["expiries_accepted"] = len(accepted)
    cov["expiries_rejected"] = len(rejected_expiries)
    if not entries:
        guard.setdefault("reason", "no row survived the per-expiry consistency guard")
        return False, None, {"coverage": cov, "guard": guard}

    rows: list = []
    for e in entries:
        row = dict(e)
        fwd = forwards.get(e["expiry"]) or {}
        F, T = fwd.get("forward"), fwd.get("T")
        row["forward"] = F
        row["forward_source"] = fwd.get("source")
        row["dte"] = fwd.get("dte")
        row["T_years"] = T
        row["iv"] = row["delta"] = row["gamma"] = row["theta"] = row["vega"] = None
        try:
            if not F or not T or T <= 0 or not e["strike"]:
                cov["skipped_no_forward"] += 1
            else:
                iv = pricing.implied_vol(e["px"], F, e["strike"], T, e["right"], r)
                if iv is None:
                    cov["iv_unsolved"] += 1
                else:
                    row["iv"] = iv
                    cov["iv_solved"] += 1
                    g = pricing.greeks(F, e["strike"], T, iv, e["right"], r)
                    row["delta"] = g["delta"]
                    row["gamma"] = g["gamma"]
                    row["theta"] = g["theta_per_day"]
                    row["vega"] = g["vega_per_1pct_vol"]
        except Exception:  # noqa: BLE001
            cov["greeks_failed"] += 1
        row.pop("px", None)
        rows.append(row)

    lot_counts: dict = {}
    for row in rows:
        if row.get("lot_size"):
            lot_counts[row["lot_size"]] = lot_counts.get(row["lot_size"], 0) + 1
    lot_size = max(lot_counts.items(), key=lambda kv: kv[1])[0] if lot_counts else None

    # The archived "master" for a backfilled date is TODAY's master restricted to the contracts that
    # actually traded that day. It is therefore SURVIVORSHIP-LIMITED, and says so in the payload.
    instruments = []
    seen = set()
    for inst, _bar in (slot.get("opts") or []):
        if inst["tradingsymbol"] not in seen:
            seen.add(inst["tradingsymbol"])
            instruments.append(inst)
    for _e, fx in (slot.get("futs") or {}).items():
        if fx["inst"]["tradingsymbol"] not in seen:
            seen.add(fx["inst"]["tradingsymbol"])
            instruments.append(fx["inst"])

    payload = {
        "schema": snap_store.SCHEMA,
        "underlying": str(underlying),
        "as_of": d_iso,
        "captured_at": _now_iso(),
        "source": "kite: instruments('NFO') master + historical_data(interval='day', oi=True)",
        "basis": "backfill_from_listed_contracts",
        "quote_basis": BACKFILL_QUOTE_BASIS,
        "survivorship": BACKFILL_SURVIVORSHIP,
        "lot_size_source": BACKFILL_LOT_SIZE_SOURCE,
        "slippage_policy": BACKFILL_SLIPPAGE_POLICY,
        "liquidity_gate_policy": BACKFILL_LIQUIDITY_POLICY,
        "r": r,
        "lot_size": lot_size,
        "spot": None,
        "expiries": sorted(accepted),
        "expiries_rejected": rejected_expiries,
        "forwards": forwards,
        "forward_policy": FORWARD_POLICY,
        "instruments": instruments,
        "rows": rows,
        "rejected": [],
        "coverage": cov,
        "guard": guard,
        "evidence_basis": "backfilled_listed_contracts_only",
    }
    return True, payload, {"coverage": cov, "guard": guard}


def run_daily_snapshot(underlyings=("NIFTY",), as_of=None, _client=None) -> dict:
    """Post-market entry point: snapshot each underlying, guarded per underlying so one failure never
    stops the rest. Returns {as_of, results: {underlying -> status dict}}. Never raises."""
    as_of_iso = _iso(as_of or datetime.now().date())
    out = {"as_of": as_of_iso, "results": {}}
    for u in underlyings:
        try:
            out["results"][u] = snapshot_chain(as_of_iso, u, _client=_client)
        except Exception as e:  # noqa: BLE001 — should be unreachable (snapshot_chain never raises)
            out["results"][u] = {"as_of": as_of_iso, "underlying": u, "ok": False, "aborted": True,
                                 "reason": f"unhandled {type(e).__name__}: {e}"}
    return out
