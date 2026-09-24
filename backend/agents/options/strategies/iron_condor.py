"""
Options Agent · Iron Condor — the phase-1 strategy.

An iron condor = sell OTM call + buy further-OTM call (bear call spread) AND sell OTM put +
buy further-OTM put (bull put spread), same expiry. Defined risk: the long wings cap the
loss, which is why phase 1 trades only this shape.

WHAT THIS FILE DOES AND DOES NOT DO
-----------------------------------
It CONSTRUCTS candidates by governed rules and EVALUATES them (credit, max loss, breakevens,
POP, EV, margin, greeks, liquidity). It deliberately does NOT decide TRADE/WATCH/NO_TRADE —
that is the gate stack's job in evidence.py, fed by realised tracked outcomes. A constructor
that both builds and blesses its own candidates is how a backtest starts flattering itself.

GOVERNED PARAMETERS
-------------------
CONDOR_PARAMS below is FROZEN and SPEC-until-OOS. The values are reasoned defaults, not
fitted ones: nothing here has been calibrated on any outcome data, because no outcome data
exists yet. When calibration happens it must be walk-forward on a discovery/OOS split and
never on the deciding sample. The table is reported verbatim in every occurrence's context
so a reviewer can audit which knobs produced a candidate.

POINT-IN-TIME
-------------
Reads only the chain snapshot handed in (already the as-of snapshot) and underlying bars
<= as_of_idx. DTE is computed from the DECISION DATE, never from the clock.
"""
from __future__ import annotations

import logging

from . import _legs as L
from .base import OptionsOccurrence, StrategyConstructor
from . import registry
from .. import payoff as PO
from .. import pricing as P

log = logging.getLogger("agents.options.iron_condor")

PARAMS_VERSION = "IC-v1 (frozen, SPEC-until-OOS — reasoned defaults, never fitted)"

CONDOR_PARAMS = {
    # ── short-strike placement ──────────────────────────────────────────────────────
    # 0.16 delta ~ the 1-sigma strike. Delta rather than % OTM because delta normalises
    # across vol regimes; a fixed % OTM is a far wider strike in a calm tape than a stressed
    # one, which would silently make the strategy regime-dependent.
    "short_delta_target": 0.16,
    "short_delta_band": (0.10, 0.25),
    # ── wings ───────────────────────────────────────────────────────────────────────
    # Width in POINTS, snapped to the listed ladder (measured NIFTY step = 50).
    "wing_width_points": 200.0,
    # ── expiry windows (DTE from the decision date) ──────────────────────────────────
    "dte_weekly": (1, 10),
    "dte_monthly": (15, 45),
    "expiry_kinds": ("weekly", "monthly"),
    # ── liquidity floors (all four legs must pass) ───────────────────────────────────
    "max_spread_pct": 8.0,
    "min_oi": 500,
    "min_volume": 0,          # far-OTM weeklies can legitimately print no volume intraday
    # ── structural floors ───────────────────────────────────────────────────────────
    # Credit must be a meaningful fraction of the width, else the risk/reward is indefensible
    # regardless of POP: collecting 2% of the width means being right ~50x to pay for one loss.
    "min_credit_pct_width": 0.08,
    # The two sides should be roughly balanced, or it is a directional trade wearing a
    # condor's clothes.
    "delta_symmetry_tol": 0.08,
    # How far the snapped wing may sit from the requested width before it is named as a
    # deviation. One NIFTY strike step (measured 50) -- beyond that the structure's risk is
    # materially different from the one that was asked for.
    "wing_width_tol_points": 50.0,
    "lots": 1,
}


class IronCondorConstructor(StrategyConstructor):
    strategy_id = "iron_condor"
    name = "Iron Condor"
    status = "built"
    legs_count = 4
    defined_risk = True
    underlyings = ("NIFTY",)
    expiries = ("weekly", "monthly")
    params_version = PARAMS_VERSION

    # ------------------------------------------------------------------ construct
    def construct(self, chain, underlying_df=None, as_of_idx=None, params=None) -> list:
        """Build one candidate condor per eligible expiry in this snapshot.

        `chain` is a list of contract rows for ONE underlying from ONE as-of snapshot, each
        row carrying at minimum: expiry, right, strike, lot_size, and a price (bid/ask, ltp,
        or a backfilled close), plus iv/delta/... where they could be solved.
        """
        p = dict(CONDOR_PARAMS)
        if params:
            p.update(params)
        out: list = []
        if not chain:
            return out

        # Accept EITHER the data.load_chain() dict (production) or a bare row list (tests and
        # ad-hoc tooling). The dict form is preferred and is what the archive path uses:
        # snapshot-level provenance -- basis, survivorship, capture_mode, the archived expiry
        # master, forward_source -- lives at the top level of that dict, and taking a bare row
        # list silently discards all of it. It also used to break outright: rows produced by
        # fetch_kite carry no as_of_date/underlying/spot, so a bare list from the real archive
        # yielded ZERO candidates and only a log line. Both shapes are now supported and the
        # provenance is carried onto every occurrence.
        meta = _snapshot_meta(chain)
        rows = meta["rows"]
        if not rows:
            return out
        as_of = meta["as_of_date"]
        if as_of is None:
            log.warning("iron_condor: snapshot has no as_of_date — refusing to construct "
                        "(DTE would be unanchored)")
            return out

        # Classify expiries against the ARCHIVED MASTER where we have it, not against the
        # expiries whose rows survived the consistency guard. Deriving the ladder from
        # surviving rows made weekly-vs-monthly a function of data quality: quarantining an
        # unrelated expiry could flip a 22-DTE weekly into a "monthly" and push it through the
        # wider (15,45) DTE window that should have rejected it.
        all_exp = meta["all_expiries"] or sorted(
            {str(r.get("expiry"))[:10] for r in rows if r.get("expiry")})
        for expiry in all_exp:
            try:
                occ = self._build_one(rows, expiry, all_exp, as_of, meta, p,
                                      underlying_df, as_of_idx)
            except Exception as e:  # noqa: BLE001 — one bad expiry never sinks the scan
                log.warning("iron_condor: expiry %s failed (non-fatal): %s", expiry, e)
                continue
            if occ is not None:
                out.append(occ)
        return out

    # ------------------------------------------------------------------ one expiry
    def _build_one(self, chain, expiry, all_exp, as_of, meta, p,
                   underlying_df, as_of_idx):
        kind = L.classify_expiry(expiry, all_exp, as_of)
        d = L.dte(as_of, expiry)

        if kind not in p["expiry_kinds"]:
            return None
        window = p["dte_weekly"] if kind == "weekly" else p["dte_monthly"]
        if not (window[0] <= d <= window[1]):
            return None

        spot = meta["spot"]
        reasons = []

        # NEVER fall back to spot for the forward. Spot is not the forward -- at 24,080 and 45
        # DTE the basis is ~190 points (~0.8%), and that error feeds straight into POP, into
        # the ATM-IV strike pick and into the expected move. If the archive has no forward for
        # this expiry, the honest output is INCOMPLETE, not a quietly substituted spot.
        forward = meta["forwards"].get(expiry)
        if forward in (None, 0):
            return self._incomplete(expiry, kind, d, spot, None, meta, p, "n/a",
                                    ["no forward for this expiry in the snapshot — refusing "
                                     "to substitute spot (basis error would corrupt POP/EV)"])
        forward = float(forward)
        forward_source = (meta.get("forward_sources") or {}).get(expiry)

        # ── short strikes by delta, with an explicitly-labelled fallback ─────────────
        selector = "delta"
        sc = L.pick_by_delta(chain, expiry, "CE", p["short_delta_target"],
                             p["short_delta_band"])
        sp = L.pick_by_delta(chain, expiry, "PE", p["short_delta_target"],
                             p["short_delta_band"])
        if sc is None or sp is None:
            # Deltas unavailable (e.g. backfilled rows where IV could not be solved).
            # The fallback is NOT equivalent across vol regimes, so it is recorded.
            selector = "pct_otm_fallback"
            iv = L.atm_iv(chain, expiry, forward)
            if iv is not None:
                em = L.expected_move(forward, iv, P.year_fraction(d))
                pct = em / forward if forward else 0.03
            else:
                pct = 0.03
                reasons.append("no ATM IV in snapshot — fixed 3% OTM fallback used")
            sc = sc or L.pick_by_pct_otm(chain, expiry, "CE", spot, pct)
            sp = sp or L.pick_by_pct_otm(chain, expiry, "PE", spot, pct)
            reasons.append("short strikes chosen by %s, not delta" % selector)

        if sc is None or sp is None:
            return self._incomplete(expiry, kind, d, spot, forward, meta, p, selector,
                                    reasons + ["could not select both short strikes"])

        # ── wings snapped to the LISTED ladder ──────────────────────────────────────
        ladder = L.strike_ladder(chain, expiry)
        width = float(p["wing_width_points"])
        # Snap OUTWARD, never toward the money: landing inside the requested width narrows the
        # condor and understates its max loss. Measured on a truncated (survivorship-thinned)
        # ladder, nearest-snapping turned a requested 200-wide condor into a 100-wide one with
        # half the max loss and double the apparent R:R, with no reason recorded.
        lc_k = L.snap_strike_outward(ladder, float(sc["strike"]) + width, "up")
        lp_k = L.snap_strike_outward(ladder, float(sp["strike"]) - width, "down")
        if lc_k is None or lp_k is None or lc_k <= float(sc["strike"]) \
                or lp_k >= float(sp["strike"]):
            return self._incomplete(expiry, kind, d, spot, forward, meta, p, selector,
                                    reasons + ["ladder has no listed wing at width %.0f"
                                               % width])
        lc = L.find_row(chain, expiry, "CE", lc_k)
        lp = L.find_row(chain, expiry, "PE", lp_k)
        if lc is None or lp is None:
            return self._incomplete(expiry, kind, d, spot, forward, meta, p, selector,
                                    reasons + ["wing contract missing from snapshot"])

        lots = int(p["lots"])
        legs = []
        for i, (row, action) in enumerate(((sp, "SELL"), (lp, "BUY"),
                                           (sc, "SELL"), (lc, "BUY")), start=1):
            leg = L.make_leg(row, action, i, lots=lots)
            if leg is None:
                return self._incomplete(expiry, kind, d, spot, forward, meta, p, selector,
                                        reasons + ["leg %d has no usable price" % i])
            legs.append(leg)

        # ── liquidity: EVERY leg must pass; a missing spread fails, never passes ─────
        liq_fail = []
        for leg, row in zip(legs, (sp, lp, sc, lc)):
            ok, why = L.liquidity_ok(row, p)
            if not ok:
                liq_fail.append("%s %s: %s" % (leg["action"], leg["tradingsymbol"], why))

        # ── evaluate (real metrics, from the chain) ─────────────────────────────────
        iv_for_ev = L.atm_iv(chain, expiry, forward)
        T = P.year_fraction(d)
        if iv_for_ev is None or T <= 0:
            metrics = {"note": "EV/POP unavailable: no ATM IV solved in snapshot"
                               if iv_for_ev is None else "expired snapshot (T<=0)"}
            greeks, liq = PO.net_greeks(legs), PO.liquidity(legs)
            reasons.append(metrics["note"])
        else:
            # Build the smile from the WHOLE expiry, not just the four legs: POP depends on
            # the vol at each boundary, and the breakevens sit between listed strikes. A
            # legs-only smile has 4 knots; the full expiry has the whole ladder.
            smile = PO.build_smile(L.rows_for(chain, expiry)) or None
            ev = PO.evaluate(legs, forward, T, iv_for_ev, smile=smile)
            metrics, greeks, liq = ev, ev["greeks"], ev["liquidity"]

        # ── structural checks ───────────────────────────────────────────────────────
        credit_unit = PO.net_credit(legs)
        actual_width = min(float(lc_k) - float(sc["strike"]),
                           float(sp["strike"]) - float(lp_k))
        credit_pct_width = (credit_unit / actual_width) if actual_width > 0 else 0.0
        if credit_pct_width < p["min_credit_pct_width"]:
            reasons.append("credit is %.1f%% of width (floor %.0f%%)"
                           % (credit_pct_width * 100.0,
                              p["min_credit_pct_width"] * 100.0))
        dc, dp = sc.get("delta"), sp.get("delta")
        if dc is not None and dp is not None:
            skew = abs(abs(float(dc)) - abs(float(dp)))
            if skew > p["delta_symmetry_tol"]:
                reasons.append("delta skew %.3f > tol %.3f — directional, not neutral"
                               % (skew, p["delta_symmetry_tol"]))

        # Width deviation must be NAMED. actual_width was previously computed and used but
        # never compared against the request, so a thinned ladder silently changed the
        # structure's risk with no trace outside signature["width"].
        width_dev = abs(actual_width - width)
        width_off = width_dev > float(p["wing_width_tol_points"])
        if width_off:
            reasons.append("wing width %.0f != requested %.0f (ladder has no listed strike "
                           "at the requested width) — max loss and R:R reflect the ACTUAL "
                           "width" % (actual_width, width))

        no_book = any(l.get("bid") is None or l.get("ask") is None for l in legs)
        backfilled = any(l["price_source"] == "hist_close" for l in legs)
        if backfilled:
            reasons.append("built from BACKFILLED rows (no bid/ask) — liquidity is "
                           "UNMEASURABLE on this date, not passed")
        elif no_book:
            reasons.append("no two-sided book in this snapshot (post-market capture) — "
                           "liquidity is UNMEASURABLE, not passed; slippage is a governed "
                           "assumption, not a measured half-spread")

        # STAGE now reflects every failure mode, not just liquidity. Previously a candidate
        # with no expectancy math, or one breaching the credit floor or the delta-skew
        # tolerance, still came out as CANDIDATE with the problem buried in `reasons` -- a
        # downstream gate keying on `stage` would have promoted it.
        priced = "ev" in metrics
        if not priced:
            stage = "UNPRICED"                       # no EV/POP could be computed at all
        elif liq_fail and no_book:
            # "we could not measure liquidity" is NOT the same claim as "this is illiquid".
            stage = "LIQUIDITY_SKIPPED"
            reasons.extend(liq_fail)
        elif liq_fail:
            stage = "ILLIQUID"
            reasons.extend(liq_fail)
        elif credit_pct_width < p["min_credit_pct_width"] or width_off:
            stage = "STRUCTURE_FAIL"
        elif (dc is not None and dp is not None
              and abs(abs(float(dc)) - abs(float(dp))) > p["delta_symmetry_tol"]):
            stage = "STRUCTURE_FAIL"
        else:
            stage = "CANDIDATE"

        return OptionsOccurrence(
            strategy=self.strategy_id, underlying=meta["underlying"], stage=stage,
            as_of_date=str(as_of), expiry=expiry, expiry_kind=kind, dte=d,
            spot=spot, forward=forward, legs=legs, metrics=metrics, greeks=greeks,
            liquidity=liq, direction="neutral",
            signal_idx=(as_of_idx if as_of_idx is not None else -1),
            entry_idx=((as_of_idx + 1) if as_of_idx is not None else -1),
            reasons=reasons,
            provenance={"basis": meta.get("basis"),
                        "survivorship": meta.get("survivorship"),
                        "evidence_basis": meta.get("evidence_basis"),
                        "capture_mode": meta.get("capture_mode"),
                        "quote_basis": meta.get("quote_basis"),
                        "lot_size_source": meta.get("lot_size_source"),
                        "forward_source": forward_source,
                        "expiries_quarantined": [str(x.get("expiry")) for x in
                                                 (meta.get("expiries_rejected") or [])],
                        "liquidity_measurable": not no_book},
            regime={"atm_iv": iv_for_ev,
                    "expected_move_1sigma": (L.expected_move(forward, iv_for_ev, T)
                                             if iv_for_ev else None),
                    "iv_rank": None,
                    "iv_rank_status": "skipped: no IV history archived yet — a percentile "
                                      "from a few days of data would be fabricated"},
            signature={"short_call": float(sc["strike"]), "long_call": float(lc_k),
                       "short_put": float(sp["strike"]), "long_put": float(lp_k),
                       "width": actual_width, "dte_bucket": kind,
                       "delta_target": p["short_delta_target"], "selector": selector,
                       "width_requested": width, "width_deviation": width_dev},
            context={"as_of_date": str(as_of), "params_version": PARAMS_VERSION,
                     "params": {k: v for k, v in p.items()},
                     "lot_size": legs[0]["lot_size"], "lots": lots,
                     "strike_step": L.strike_step(ladder),
                     "credit_pct_width": credit_pct_width,
                     "price_sources": sorted({l["price_source"] for l in legs}),
                     "backfilled": backfilled,
                     "snapshot_id": meta.get("snapshot_id")},
        ).to_dict()

    # ------------------------------------------------------------------ helpers
    def _incomplete(self, expiry, kind, d, spot, forward, meta, p, selector, reasons):
        return OptionsOccurrence(
            strategy=self.strategy_id, underlying=meta["underlying"], stage="INCOMPLETE",
            as_of_date=str(meta["as_of_date"]), expiry=expiry, expiry_kind=kind, dte=d,
            spot=spot, forward=forward, legs=[], metrics={}, greeks={}, liquidity={},
            reasons=reasons,
            provenance={"basis": meta.get("basis"),
                        "survivorship": meta.get("survivorship"),
                        "evidence_basis": meta.get("evidence_basis"),
                        "capture_mode": meta.get("capture_mode"),
                        "lot_size_source": meta.get("lot_size_source"),
                        "expiries_quarantined": [str(x.get("expiry")) for x in
                                                 (meta.get("expiries_rejected") or [])]},
            signature={"selector": selector},
            context={"as_of_date": str(meta["as_of_date"]),
                     "params_version": PARAMS_VERSION},
        ).to_dict()


def _snapshot_meta(chain) -> dict:
    """Snapshot-level facts, from EITHER a data.load_chain() dict or a bare row list.

    Everything here comes from the archived snapshot — never from a live call and never from
    the clock — so a replayed past date sees exactly what was true then. When given the dict
    form we take the provenance the archive recorded (basis / survivorship / capture_mode /
    forward_source / the archived expiry master) rather than re-deriving or guessing it."""
    if isinstance(chain, dict):
        rows = chain.get("rows") or []
        # load_chain's `forwards` is {expiry: {"forward":…, "source":…, "T":…, "dte":…}} — a
        # RECORD per expiry, not a bare float. Flatten it here and keep the source alongside,
        # so a forward can never be used without its provenance travelling with it.
        forwards, forward_sources = {}, {}
        for e, v in (chain.get("forwards") or {}).items():
            key = str(e)[:10]
            if isinstance(v, dict):
                f = v.get("forward")
                if f is not None:
                    forwards[key] = float(f)
                    forward_sources[key] = v.get("source")
            elif v is not None:
                forwards[key] = float(v)
        if not forwards and chain.get("forward") is not None and chain.get("expiry"):
            key = str(chain["expiry"])[:10]
            forwards[key] = float(chain["forward"])
            forward_sources[key] = chain.get("forward_source")
        return {
            "rows": rows,
            "underlying": chain.get("underlying") or "NIFTY",
            "as_of_date": chain.get("as_of"),
            "spot": chain.get("spot"),
            "forwards": forwards,
            "forward_sources": forward_sources,
            "snapshot_id": chain.get("source_uri"),
            "basis": chain.get("basis"),
            "survivorship": chain.get("survivorship"),
            "evidence_basis": chain.get("evidence_basis"),
            "capture_mode": chain.get("capture_mode"),
            "quote_basis": chain.get("quote_basis"),
            "lot_size_source": chain.get("lot_size_source"),
            "expiries_rejected": chain.get("expiries_rejected") or [],
            "all_expiries": sorted({str(e)[:10] for e in (chain.get("expiries") or [])}),
        }

    # Bare row list (tests / ad-hoc tooling): fall back to row-stamped keys, and say so.
    underlying = as_of = spot = snapshot_id = None
    forwards = {}
    for r in chain:
        underlying = underlying or r.get("underlying") or r.get("name")
        as_of = as_of or r.get("as_of_date") or r.get("date")
        snapshot_id = snapshot_id or r.get("snapshot_id")
        if spot is None and r.get("spot") is not None:
            spot = float(r["spot"])
        if str(r.get("instrument_type") or r.get("right") or "").upper() == "FUT":
            e = str(r.get("expiry"))[:10]
            px, _ = L.price_leg(r)
            if px:
                forwards[e] = px
        elif r.get("forward") is not None:
            forwards.setdefault(str(r.get("expiry"))[:10], float(r["forward"]))
    return {"rows": list(chain), "underlying": underlying or "NIFTY", "as_of_date": as_of,
            "spot": float(spot) if spot is not None else None,
            "forwards": forwards, "forward_sources": {}, "snapshot_id": snapshot_id,
            "basis": None, "survivorship": None, "evidence_basis": None,
            "capture_mode": None, "quote_basis": None, "lot_size_source": None,
            "expiries_rejected": [], "all_expiries": []}


registry.register(IronCondorConstructor())
