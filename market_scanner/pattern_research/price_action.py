"""PA01-PA08 price-action detectors (research only, Claude Code owned).

Interface (docs/pattern_research/CLAUDE_HANDOFF.md):
    specifications() -> list[dict]
    detect(bars, timeframe) -> {(pattern_id, variant, side): [event, ...]}

Package-wide conventions used by every family here:
- ``setup``: the shape is complete on closed bars; signal_index = last bar of the shape.
- ``confirmed``: the predeclared close-based confirmation, emitted once at the confirming bar
  with the same episode; detected_index = setup bar, confirmed_index = signal_index.
- ``episode`` = formation_start_index, except PA02/PA06 where it is the mother-bar index.
- Every bar of the formation window and of the confirmation window must be in one clean
  segment (``Series.clean``).  ``gap`` is the data-quality flag, never a price gap; a
  gap-flagged bar always starts a new segment, so it can never complete a two-bar shape.
- Buffers/tolerances use the prior ATR at the setup bar, frozen for the episode.  No event
  is emitted when that ATR is unavailable.  Event ``atr`` is the prior ATR at signal_index.
- ``prior_trend`` (Series.trend at formation start) is geometry metadata; only variants that
  explicitly require context (PA01, PA04 *_context) filter on it and skip when it is None.
- Direction-unknown shapes (PA02 inside bars, PA05 narrow ranges) never become longs by
  default: ``*_break_up`` (long) / ``*_break_down`` (short) keys share a neutral setup, and
  only the confirmed event carries a direction.  For these two-sided shapes the FIRST
  qualifying close beyond either side decides; the opposite key receives no confirmation.
- ``score`` is a geometric quality in [0, 1] (documented per family), not a probability.
"""
from __future__ import annotations

import math

import numpy as np

from . import common as C

FAMILY = "price_action"
BUF = C.BREAKOUT_BUFFER_ATR            # 0.12 ATR confirmation buffer
WINDOW = C.CANDLE_CONFIRM_WINDOW       # 3 complete candles

TWEEZER_TOL_ATR = 0.10
TWEEZER_TOL_FLOOR = 0.01               # price units; stands in for the unknown tick size
TWEEZER_AWAY_ATR = 0.25
OUTSIDE_QUARTER = 0.25
PIN_BODY_MULT = 2.0
PIN_DOMINANT_RANGE = 0.60
PIN_OPPOSITE_RANGE = 0.15
FAKEY_FAIL_WINDOW = 3                  # breach bar + 2 more bars to close back inside
FAKEY_CONFIRM_WINDOW = 3               # bars after the failure close
NR_LENGTHS = {"nr4": 4, "nr7": 7}

_DEFAULT_CONFIRM = (f"Confirmation (catalogue default): the first of the next {WINDOW} complete candles, "
                    f"all in the setup's clean segment, whose close is beyond the stated level by "
                    f"{BUF} x prior ATR at the setup bar.")
_TWO_SIDED = (f"Two-sided break: within the next {WINDOW} complete candles in the same clean segment, "
              f"the first close above HIGH + {BUF} ATR(setup) confirms break_up (long) or below "
              f"LOW - {BUF} ATR(setup) confirms break_down (short); the first such close decides and "
              f"the other side gets no confirmation. Setup direction is neutral.")


# ------------------------------------------------------------------ specifications
def specifications() -> list[dict]:
    S = []
    def add(pattern_id, variant, side, name, lookback, definition):
        S.append(C.spec(pattern_id, variant, side, name, FAMILY, list(C.STATES), lookback, definition))

    tw = (f"Two adjacent clean bars (i-1, i) with range > 0. Tolerance tol = max({TWEEZER_TOL_FLOOR} price "
          f"units, {TWEEZER_TOL_ATR} x prior ATR(i)); tick size is not in the bars, so the fixed "
          f"{TWEEZER_TOL_FLOOR} floor stands in for one tick. ")
    add("PA01", "top", "short", "Tweezer top", 22,
        tw + "Top: |high[i] - high[i-1]| <= tol; bar i closes away from the tested level: close[i] in the "
        f"lower half of its own range AND max(high[i-1], high[i]) - close[i] >= {TWEEZER_AWAY_ATR} ATR(i). "
        "Requires prior_trend == 'up' at the pair start i-1 (20-close fit). " + _DEFAULT_CONFIRM +
        " Level: min(low[i-1], low[i]) (the pair's opposite extreme), close below it.")
    add("PA01", "bottom", "long", "Tweezer bottom", 22,
        tw + "Bottom: |low[i] - low[i-1]| <= tol; close[i] in the upper half of its own range AND "
        f"close[i] - min(low[i-1], low[i]) >= {TWEEZER_AWAY_ATR} ATR(i). Requires prior_trend == 'down' at "
        "i-1. " + _DEFAULT_CONFIRM + " Level: max(high[i-1], high[i]), close above it.")

    ib = ("Inside bar: high[k] <= high[M] and low[k] >= low[M] with at least one strict edge, bars M..k in one "
          "clean segment. Mother identity: bar k joins the active mother of bar k-1 when it is inside that "
          "mother; otherwise, if k is inside k-1, bar k-1 becomes a new mother. Nested bars and multiple "
          "inside bars therefore share the original mother (episode = mother index). ")
    ib_conf = (f"Confirmation: within the {WINDOW} complete candles after the setup bar, in the mother's clean "
               f"segment, the first close above mother_high + {BUF} ATR(setup) -> break_up, or below "
               f"mother_low - {BUF} ATR(setup) -> break_down. The search expires at the first bar that "
               f"breaches the mother range without such a close, at a segment break, or after {WINDOW} bars. "
               "Setup direction is neutral.")
    ib_var = {
        "first": "Setup at the first inside bar of a mother (k = M+1); whether the cluster grows is unknown then. ",
        "cluster": "Setup at the second consecutive inside bar of the same mother (k = M+2). ",
        "nested": "Setup at the first bar k > M+1 of a mother cluster that is itself inside bar k-1 (strict edge). ",
    }
    for var, txt in ib_var.items():
        for brk, side in (("break_up", "long"), ("break_down", "short")):
            add("PA02", f"{var}_{brk}", side, f"Inside bar ({var}), {brk.replace('_', ' ')}", 24,
                ib + txt + ib_conf)

    ob = ("Outside bar: high[i] > high[i-1] and low[i] < low[i-1], both bars clean and adjacent. ")
    add("PA03", "bullish_close", "long", "Outside bar, bullish close", 22,
        ob + f"close[i] >= low[i] + {1 - OUTSIDE_QUARTER} x range[i] (top quarter). Outside bars closing in the "
        "middle half are not emitted. " + _DEFAULT_CONFIRM + " Level: high[i], close above it.")
    add("PA03", "bearish_close", "short", "Outside bar, bearish close", 22,
        ob + f"close[i] <= low[i] + {OUTSIDE_QUARTER} x range[i] (bottom quarter). Middle-half closes are not "
        "emitted. " + _DEFAULT_CONFIRM + " Level: low[i], close below it.")

    pin = (f"Single clean bar with range > 0. body = |close-open|; dominant shadow >= {PIN_BODY_MULT} x body AND "
           f">= {PIN_DOMINANT_RANGE} x range; opposite shadow <= {PIN_OPPOSITE_RANGE} x range. ")
    lo_txt = pin + "Lower shadow dominant. " + _DEFAULT_CONFIRM + " Level: high[i] (short-shadow end), close above."
    up_txt = pin + "Upper shadow dominant. " + _DEFAULT_CONFIRM + " Level: low[i] (short-shadow end), close below."
    add("PA04", "lower_shadow", "long", "Pin bar, lower shadow", 21, lo_txt + " prior_trend is metadata only.")
    add("PA04", "upper_shadow", "short", "Pin bar, upper shadow", 21, up_txt + " prior_trend is metadata only.")
    add("PA04", "lower_shadow_context", "long", "Pin bar, lower shadow after decline", 21,
        lo_txt + " Requires prior_trend == 'down' at bar i.")
    add("PA04", "upper_shadow_context", "short", "Pin bar, upper shadow after advance", 21,
        up_txt + " Requires prior_trend == 'up' at bar i.")

    for var, n in NR_LENGTHS.items():
        for brk, side in (("break_up", "long"), ("break_down", "short")):
            add("PA05", f"{var}_{brk}", side, f"{var.upper()} narrow range, {brk.replace('_', ' ')}", 20 + n,
                f"range[i] = high-low > 0 and strictly smaller than each range of bars i-{n - 1}..i-1; bars "
                f"i-{n - 1}..i in one clean segment. Equal minima (ties) are not emitted (no tie variant). "
                + _TWO_SIDED.replace("HIGH", "high[i]").replace("LOW", "low[i]"))
    for brk, side in (("break_up", "long"), ("break_down", "short")):
        add("PA05", f"inside_nr4_{brk}", side, f"Inside NR4, {brk.replace('_', ' ')}", 24,
            "NR4 (range[i] > 0, strictly smallest of bars i-3..i, one clean segment) AND bar i is inside bar i-1 "
            "(high <= , low >= , at least one strict edge), so it also satisfies PA02. "
            + _TWO_SIDED.replace("HIGH", "high[i]").replace("LOW", "low[i]"))

    fk = ("Mother M with an inside-bar cluster M+1..k (PA02 identity). Bar b = k+1 (first bar leaving the cluster, "
          "same clean segment) breaches exactly one mother edge. The failure close is the first bar f in "
          f"[b, b+{FAKEY_FAIL_WINDOW - 1}] whose close is back inside; no bar in b..f may breach the opposite edge. "
          "Setup at f (episode = M; geometry keeps breach_index and failure_index). ")
    add("PA06", "bull", "long", "Bullish fakey", 30,
        fk + "Bull: low[b] < mother_low, close[f] > mother_low. Confirmation: within the "
        f"{FAKEY_CONFIRM_WINDOW} bars after f (clean segment), the first close > mother_high + {BUF} ATR(f); "
        "invalidated (no event) if a close < mother_low comes first.")
    add("PA06", "bear", "short", "Bearish fakey", 30,
        fk + "Bear: high[b] > mother_high, close[f] < mother_high. Confirmation: within the "
        f"{FAKEY_CONFIRM_WINDOW} bars after f (clean segment), the first close < mother_low - {BUF} ATR(f); "
        "invalidated (no event) if a close > mother_high comes first.")

    add("PA07", "rising", "long", "Rising window", 22,
        "low[i] > high[i-1] (full-range window, OHLC only); bars i-1 and i clean and adjacent (a gap-flagged bar "
        "i is never eligible). Gap-hold/fill/retest are strategy variants and not detected here. "
        + _DEFAULT_CONFIRM + " Level: high[i], close above it.")
    add("PA07", "falling", "short", "Falling window", 22,
        "high[i] < low[i-1]; bars i-1 and i clean and adjacent. " + _DEFAULT_CONFIRM +
        " Level: low[i], close below it.")

    add("PA08", "bullish", "long", "Gap-and-reversal, bullish", 22,
        "Bars i-1, i clean and adjacent, range[i] > 0. open[i] < low[i-1]; low[i-1] < close[i] <= high[i-1] "
        "(closed back inside the prior full range on the same candle); close[i] >= low[i] + 0.5 x range[i]. "
        "Setup at bar i's close. " + _DEFAULT_CONFIRM + " Level: max(high[i-1], high[i]), close above it.")
    add("PA08", "bearish", "short", "Gap-and-reversal, bearish", 22,
        "Bars i-1, i clean and adjacent, range[i] > 0. open[i] > high[i-1]; low[i-1] <= close[i] < high[i-1]; "
        "close[i] <= low[i] + 0.5 x range[i]. Setup at bar i's close. " + _DEFAULT_CONFIRM +
        " Level: min(low[i-1], low[i]), close below it.")
    return S


# ------------------------------------------------------------------------ helpers
def _shift(a, k, fill=np.nan):
    out = np.full(len(a), fill, dtype=a.dtype if fill is not np.nan else float)
    if k < len(a):
        out[k:] = a[:-k] if k else a
    return out


def _clip01(x):
    x = float(x)
    return 0.0 if not math.isfinite(x) else min(1.0, max(0.0, x))


class _Emitter:
    def __init__(self, s, col):
        self.s, self.col = s, col

    def setup(self, key, *, i, start, episode, direction, score, geometry, alias=None, tags=None):
        self.col.add(key, C.make_event(self.s, signal_index=i, episode=episode, state="setup",
                                       direction=direction, formation_start=start, detected_index=i,
                                       score=score, geometry=geometry, alias_group=alias, quality_tags=tags))

    def confirmed(self, key, *, j, i, start, episode, direction, score, geometry, alias=None, tags=None):
        self.col.add(key, C.make_event(self.s, signal_index=j, episode=episode, state="confirmed",
                                       direction=direction, formation_start=start, detected_index=i,
                                       score=score, confirmed_index=j, geometry=geometry,
                                       alias_group=alias, quality_tags=tags))

    def directional(self, key, *, i, start, bullish, score, geometry, level_hi, level_lo, atr,
                    alias=None, tags=None):
        """Setup + default one-sided candle confirmation."""
        direction = "bullish" if bullish else "bearish"
        self.setup(key, i=i, start=start, episode=start, direction=direction, score=score,
                   geometry=geometry, alias=alias, tags=tags)
        j = C.confirm_close_beyond(self.s, i, level_hi, level_lo, bullish, atr=atr)
        if j is not None:
            level = level_hi + BUF * atr if bullish else level_lo - BUF * atr
            g = dict(geometry, confirmation_index=j, confirmation_level=level)
            self.confirmed(key, j=j, i=i, start=start, episode=start, direction=direction, score=score,
                           geometry=g, alias=alias, tags=tags)


def _two_sided_break(s, d, hi, lo, atr, window=WINDOW, clean_from=None, stop_on_breach=False):
    """First close beyond hi/lo +/- BUF*atr in (d, d+window]; returns (j, 'up'|'down') or (None, None)."""
    a = d if clean_from is None else clean_from
    up, dn = hi + BUF * atr, lo - BUF * atr
    for j in range(d + 1, min(d + window, s.n - 1) + 1):
        if not s.clean(a, j):
            return None, None
        cj = s.c[j]
        if cj > up:
            return j, "up"
        if cj < dn:
            return j, "down"
        if stop_on_breach and (s.h[j] > hi or s.l[j] < lo):
            return None, None
    return None, None


def _emit_two_sided(em, pid, var, *, i, start, episode, score, geometry, hi, lo, atr, alias, window=WINDOW,
                    clean_from=None, stop_on_breach=False):
    up_key, dn_key = (pid, f"{var}_break_up", "long"), (pid, f"{var}_break_down", "short")
    for key in (up_key, dn_key):
        em.setup(key, i=i, start=start, episode=episode, direction="neutral", score=score,
                 geometry=geometry, alias=alias)
    j, side = _two_sided_break(em.s, i, hi, lo, atr, window, clean_from, stop_on_breach)
    if j is None:
        return
    key = up_key if side == "up" else dn_key
    level = hi + BUF * atr if side == "up" else lo - BUF * atr
    g = dict(geometry, confirmation_index=j, confirmation_level=level)
    em.confirmed(key, j=j, i=i, start=start, episode=episode, direction="bullish" if side == "up" else "bearish",
                 score=score, geometry=g, alias=alias)


def _mothers(s, inside_prev):
    """mother[k] = mother-bar index of inside bar k (PA02 identity), -1 when bar k is not inside."""
    n = s.n
    h, l, seg = s.h.tolist(), s.l.tolist(), s.seg_id.tolist()
    ip = inside_prev.tolist()
    mother = [-1] * n
    for k in range(1, n):
        m = mother[k - 1]
        if m >= 0:
            hm, lm, hk, lk = h[m], l[m], h[k], l[k]
            if seg[k] == seg[m] and hk <= hm and lk >= lm and (hk < hm or lk > lm):
                mother[k] = m
                continue
        if ip[k]:
            mother[k] = k - 1
    return mother


# ---------------------------------------------------------------------- detection
def detect(bars: list[dict], timeframe: str) -> dict:
    specs = specifications()
    col = C.empty_result(specs)
    s = C.prepare(bars, timeframe)
    n = s.n
    if n < 2:
        return col.result()
    em = _Emitter(s, col)
    o, h, l, c, atr = s.o, s.h, s.l, s.c, s.atr
    rng = h - l
    with np.errstate(invalid="ignore"):
        pair = np.zeros(n, dtype=bool)
        pair[1:] = (s.seg_id[1:] == s.seg_id[:-1]) & s.valid[1:] & s.valid[:-1]
        has_atr = np.isfinite(atr)
        ph, pl = _shift(h, 1), _shift(l, 1)
        base = pair & has_atr
        inside_prev = pair & (h <= ph) & (l >= pl) & ((h < ph) | (l > pl))

        # ---- PA01 tweezers
        tol = np.maximum(TWEEZER_TOL_FLOOR, TWEEZER_TOL_ATR * atr)
        rng_ok = base & (rng > 0) & (_shift(rng, 1) > 0)
        top = rng_ok & (np.abs(h - ph) <= tol) & (c <= l + 0.5 * rng) & (np.maximum(h, ph) - c >= TWEEZER_AWAY_ATR * atr)
        bot = rng_ok & (np.abs(l - pl) <= tol) & (c >= l + 0.5 * rng) & (c - np.minimum(l, pl) >= TWEEZER_AWAY_ATR * atr)
    for i in np.flatnonzero(top | bot):
        i = int(i)
        start = i - 1
        trend = s.trend(start)
        a = float(atr[i])
        for is_top, mask, need in ((True, top, "up"), (False, bot, "down")):
            if not mask[i] or trend != need:
                continue
            diff = abs(h[i] - h[i - 1]) if is_top else abs(l[i] - l[i - 1])
            geom = {"prior_trend": trend, "tolerance": float(tol[i]), "extreme_diff": float(diff),
                    "pair_high": float(max(h[i], h[i - 1])), "pair_low": float(min(l[i], l[i - 1]))}
            flip = (c[i - 1] > o[i - 1] and c[i] < o[i]) if is_top else (c[i - 1] < o[i - 1] and c[i] > o[i])
            em.directional(("PA01", "top" if is_top else "bottom", "short" if is_top else "long"),
                           i=i, start=start, bullish=not is_top, score=_clip01(1 - diff / tol[i]), geometry=geom,
                           level_hi=geom["pair_high"], level_lo=geom["pair_low"], atr=a,
                           alias="tweezer_equal_extremes", tags=["candle_color_flip"] if flip else None)

    # ---- PA02 inside bars (mother identity) and PA06 fakey
    mother = _mothers(s, inside_prev)
    ip = inside_prev.tolist()
    nested_done = set()
    for k in range(1, n):
        m = mother[k]
        if m < 0:
            continue
        count = k - m
        variants = []
        if count == 1:
            variants.append("first")
        elif count == 2:
            variants.append("cluster")
        if count >= 2 and ip[k] and m not in nested_done:
            nested_done.add(m)
            variants.append("nested")
        a = s.atr_at(k)
        if not variants or a is None:
            continue
        mh, ml = float(h[m]), float(l[m])
        mrange = mh - ml
        geom = {"mother_index": m, "mother_high": mh, "mother_low": ml, "inside_count": count,
                "prior_trend": s.trend(m)}
        score = _clip01(1 - rng[k] / mrange) if mrange > 0 else 0.0
        for var in variants:
            _emit_two_sided(em, "PA02", var, i=k, start=m, episode=m, score=score, geometry=geom, hi=mh, lo=ml,
                            atr=a, alias="inside_range", clean_from=m, stop_on_breach=True)

    for b in range(2, n):
        m = mother[b - 1]
        if m < 0 or mother[b] == m or not s.clean(m, b):
            continue
        mh, ml = float(h[m]), float(l[m])
        if l[b] < ml and h[b] <= mh:
            bull = True
        elif h[b] > mh and l[b] >= ml:
            bull = False
        else:
            continue
        f = None
        for x in range(b, min(b + FAKEY_FAIL_WINDOW - 1, n - 1) + 1):
            if not s.clean(m, x) or (h[x] > mh if bull else l[x] < ml):
                break
            if (c[x] > ml) if bull else (c[x] < mh):
                f = x
                break
        if f is None:
            continue
        a = s.atr_at(f)
        if a is None:
            continue
        key = ("PA06", "bull", "long") if bull else ("PA06", "bear", "short")
        direction = "bullish" if bull else "bearish"
        mrange = mh - ml
        geom = {"mother_index": m, "mother_high": mh, "mother_low": ml, "last_inside_index": b - 1,
                "inside_count": b - 1 - m, "breach_index": b, "failure_index": f,
                "breach_extreme": float(np.min(l[b:f + 1]) if bull else np.max(h[b:f + 1])),
                "prior_trend": s.trend(m)}
        score = _clip01(((c[f] - ml) if bull else (mh - c[f])) / mrange) if mrange > 0 else 0.0
        em.setup(key, i=f, start=m, episode=m, direction=direction, score=score, geometry=geom,
                 alias="inside_bar_trap")
        level = mh + BUF * a if bull else ml - BUF * a
        for j in range(f + 1, min(f + FAKEY_CONFIRM_WINDOW, n - 1) + 1):
            if not s.clean(m, j):
                break
            if (c[j] > level) if bull else (c[j] < level):
                g = dict(geom, confirmation_index=j, confirmation_level=level)
                em.confirmed(key, j=j, i=f, start=m, episode=m, direction=direction, score=score, geometry=g,
                             alias="inside_bar_trap")
                break
            if (c[j] < ml) if bull else (c[j] > mh):
                break

    # ---- PA03 outside bars
    with np.errstate(invalid="ignore"):
        outside = base & (h > ph) & (l < pl) & (rng > 0)
        ob_bull = outside & (c >= l + (1 - OUTSIDE_QUARTER) * rng)
        ob_bear = outside & (c <= l + OUTSIDE_QUARTER * rng)
    for i in np.flatnonzero(ob_bull | ob_bear):
        i = int(i)
        bull = bool(ob_bull[i])
        loc = (c[i] - l[i]) / rng[i]
        geom = {"prior_trend": s.trend(i - 1), "close_location": float(loc),
                "range_ratio": float(rng[i] / rng[i - 1]) if rng[i - 1] > 0 else None}
        prior_body = (min(o[i - 1], c[i - 1]), max(o[i - 1], c[i - 1]))
        tags = ["close_top_quarter" if bull else "close_bottom_quarter"]
        if min(o[i], c[i]) < prior_body[0] and max(o[i], c[i]) > prior_body[1]:
            tags.append("body_engulfs_prior_body")
        em.directional(("PA03", "bullish_close", "long") if bull else ("PA03", "bearish_close", "short"),
                       i=i, start=i - 1, bullish=bull, score=_clip01(loc if bull else 1 - loc), geometry=geom,
                       level_hi=float(h[i]), level_lo=float(l[i]), atr=float(atr[i]), alias="outside_range",
                       tags=tags)

    # ---- PA04 pin bars
    with np.errstate(invalid="ignore"):
        body = np.abs(c - o)
        upper = h - np.maximum(o, c)
        lower = np.minimum(o, c) - l
        single = s.valid & has_atr & (rng > 0)
        pin_lo = single & (lower >= PIN_BODY_MULT * body) & (lower >= PIN_DOMINANT_RANGE * rng) & (upper <= PIN_OPPOSITE_RANGE * rng)
        pin_up = single & (upper >= PIN_BODY_MULT * body) & (upper >= PIN_DOMINANT_RANGE * rng) & (lower <= PIN_OPPOSITE_RANGE * rng)
    for i in np.flatnonzero(pin_lo | pin_up):
        i = int(i)
        trend = s.trend(i)
        for is_lo, mask in ((True, pin_lo), (False, pin_up)):
            if not mask[i]:
                continue
            dom, opp = (lower[i], upper[i]) if is_lo else (upper[i], lower[i])
            score = _clip01(0.5 * (dom / rng[i] - PIN_DOMINANT_RANGE) / (1 - PIN_DOMINANT_RANGE)
                            + 0.5 * (1 - opp / (PIN_OPPOSITE_RANGE * rng[i])))
            geom = {"prior_trend": trend, "dominant_shadow_frac": float(dom / rng[i]),
                    "opposite_shadow_frac": float(opp / rng[i]), "body_frac": float(body[i] / rng[i])}
            tags = ["up_candle"] if c[i] > o[i] else (["down_candle"] if c[i] < o[i] else None)
            alias = "lower_shadow_rejection" if is_lo else "upper_shadow_rejection"
            name = "lower_shadow" if is_lo else "upper_shadow"
            side = "long" if is_lo else "short"
            variants = [name]
            if trend == ("down" if is_lo else "up"):
                variants.append(name + "_context")
            for var in variants:
                em.directional(("PA04", var, side), i=i, start=i, bullish=is_lo, score=score, geometry=geom,
                               level_hi=float(h[i]), level_lo=float(l[i]), atr=float(atr[i]), alias=alias, tags=tags)

    # ---- PA05 narrow range
    with np.errstate(invalid="ignore"):
        for var, length in NR_LENGTHS.items():
            prev_min = np.full(n, np.nan)
            if n > length - 1:
                prev_min = np.minimum.reduce([_shift(rng, k) for k in range(1, length)])
            start_seg = _shift(s.seg_id.astype(float), length - 1)
            nr = s.valid & has_atr & (rng > 0) & (start_seg == s.seg_id) & (rng < prev_min)
            for i in np.flatnonzero(nr):
                i = int(i)
                start = i - length + 1
                geom = {"prior_trend": s.trend(start), "range": float(rng[i]), "prior_min_range": float(prev_min[i]),
                        "nr_high": float(h[i]), "nr_low": float(l[i])}
                score = _clip01(1 - rng[i] / prev_min[i])
                _emit_two_sided(em, "PA05", var, i=i, start=start, episode=start, score=score, geometry=geom,
                                hi=float(h[i]), lo=float(l[i]), atr=float(atr[i]), alias="narrow_range")
                if var == "nr4" and inside_prev[i]:
                    _emit_two_sided(em, "PA05", "inside_nr4", i=i, start=start, episode=start, score=score,
                                    geometry=dict(geom, mother_candidate_index=i - 1), hi=float(h[i]),
                                    lo=float(l[i]), atr=float(atr[i]), alias="inside_range")

    # ---- PA07 windows and PA08 gap-and-reversal
    with np.errstate(invalid="ignore"):
        rising = base & (l > ph)
        falling = base & (h < pl)
        pr = ph - pl
        gr_bull = base & (rng > 0) & (o < pl) & (c > pl) & (c <= ph) & (c >= l + 0.5 * rng)
        gr_bear = base & (rng > 0) & (o > ph) & (c < ph) & (c >= pl) & (c <= l + 0.5 * rng)
    for i in np.flatnonzero(rising | falling):
        i = int(i)
        up = bool(rising[i])
        size = (l[i] - h[i - 1]) if up else (l[i - 1] - h[i])
        a = float(atr[i])
        geom = {"prior_trend": s.trend(i - 1), "window_size": float(size), "window_atr": float(size / a)}
        em.directional(("PA07", "rising", "long") if up else ("PA07", "falling", "short"), i=i, start=i - 1,
                       bullish=up, score=_clip01(size / a), geometry=geom, level_hi=float(h[i]),
                       level_lo=float(l[i]), atr=a, alias="full_range_window")
    for i in np.flatnonzero(gr_bull | gr_bear):
        i = int(i)
        bull = bool(gr_bull[i])
        a = float(atr[i])
        gap = (l[i - 1] - o[i]) if bull else (o[i] - h[i - 1])
        depth = ((c[i] - l[i - 1]) if bull else (h[i - 1] - c[i])) / pr[i] if pr[i] > 0 else 0.0
        geom = {"prior_trend": s.trend(i - 1), "opening_gap": float(gap), "opening_gap_atr": float(gap / a),
                "close_depth_in_prior_range": float(depth), "prior_high": float(h[i - 1]), "prior_low": float(l[i - 1])}
        em.directional(("PA08", "bullish", "long") if bull else ("PA08", "bearish", "short"), i=i, start=i - 1,
                       bullish=bull, score=_clip01(depth), geometry=geom,
                       level_hi=float(max(h[i - 1], h[i])), level_lo=float(min(l[i - 1], l[i])), atr=a,
                       alias="opening_gap_reversal")
    return col.result()
