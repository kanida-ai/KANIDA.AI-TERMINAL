"""Harmonic research detectors HA01-HA10 (Claude Code owned, research only).

Contract: docs/pattern_research/CLAUDE_HANDOFF.md
    specifications() -> list[dict]
    detect(bars, timeframe) -> {(pattern_id, variant, side): [event, ...]}

Frozen construction (definition_version 1.0.0), applied to every cell:

Swings
- Only causal radius-3 pivots (common.Series.pivots).  Pivot p is used at bar p+3.
- Pivots are processed in bar order into an alternating high/low swing list.  A pivot
  of the same type as the last swing replaces it only when strictly more extreme
  (higher high / lower low), at the new pivot's availability bar; a less or equally
  extreme one is ignored.  Emitted events are never rewritten.
- A bar that is simultaneously a high and a low pivot has no causal ordering and is
  skipped.  The list is cleared when a pivot lies in a different clean segment than
  the last swing, and is bounded to the last 8 swings.
- Candidates are evaluated only when the swing list changes; the terminal point is
  the newest swing, so every pattern uses the last 4 (AB=CD) or 5 (X-A-B-C-D,
  O-X-A-B-C) consecutive swings.

Gates (all at the setup bar S = terminal pivot + 3)
- Bullish = terminal point is a swing low (long); bearish = swing high (short).
- Every leg moves in its alternating direction and is >= 1.0 ATR(S); the first leg
  (XA, or AB for AB=CD, or OX for Shark) is >= 2.0 ATR(S).
- First point -> terminal point span is 20..150 bars.
- Bars first point..S are one clean gap-free segment (Series.clean).
- All ratio bands below hold (inclusive unless stated); the terminal-point ratio band
  is the PRZ (price interval stored in geometry).

Confirmation (frozen, all cells)
- Within the 10 closed bars after S: a close above max(high[D..S]) + 0.12*ATR(S)
  (bull) / below min(low[D..S]) - 0.12*ATR(S) (bear) confirms.  A close below
  low[D] - 0.12*ATR(S) (bull) / above high[D] + 0.12*ATR(S) (bear) on or before that
  bar is a failure: no confirmation.  A segment break ends the window.  The failure
  check runs before the confirmation check on each bar.
- Confirmed event: same episode, detected_index = S, confirmed_index = signal_index.

Identity
- episode = formation_start_index = first point's bar (X; A for AB=CD; O for Shark).
- score = 1 - mean normalized distance of each scored ratio from its ideal inside its
  band (distance / farthest band edge), clipped to [0, 1].  Not a probability.
- prior trend (Series.trend(first point)) is geometry metadata only.
"""
from __future__ import annotations

from . import common

FAMILY = "harmonic"
MAX_SWINGS = 8
MIN_SPAN_BARS = 20
MAX_SPAN_BARS = 150
MIN_LEG_ATR = 1.0
MIN_FIRST_LEG_ATR = 2.0
CONFIRM_WINDOW = 10
BUFFER_ATR = common.BREAKOUT_BUFFER_ATR
LOOKBACK = MAX_SPAN_BARS + common.PIVOT_RADIUS + CONFIRM_WINDOW

# Ratio keys by position.  5-point legs L1..L4 between P0..P4; 4-point legs L1..L3.
#   5-point: r21 = L2/L1, r32 = L3/L2, r43 = L4/L3, r42 = L4/L2, ret41 = |P1-P4|/L1 signed
#   4-point: r21 = L2/L1, r31 = L3/L1, r32 = L3/L2
_XABCD = ("X", "A", "B", "C", "D")
_SHARK = ("O", "X", "A", "B", "C")
_ABCD = ("A", "B", "C", "D")


def _c(ratio, lo, hi, ideal, hi_open=False, lo_open=False):
    return {"ratio": ratio, "lo": lo, "hi": hi, "ideal": ideal, "hi_open": hi_open, "lo_open": lo_open}


_BC = _c("r32", 0.382, 0.886, 0.634)  # BC/AB, midpoint ideal (range constraint)
# Minimum AB=CD completion (lower bound only, hi=None, unscored): CD/AB >= 0.90, the declared 10% equality
# tolerance.  Typical extensions (1.27 / 1.618) are annotated in geometry["abcd_extension"], never gated.
ABCD_MIN_COMPLETION = 0.90
_ABCD_MIN = _c("r42", ABCD_MIN_COMPLETION, None, None)

TEMPLATES = [
    dict(pattern_id="HA01", variant="canonical", name="AB=CD", labels=_ABCD, alias="abcd", prz="r31",
         constraints=[_c("r21", 0.382, 0.886, 0.634), _c("r31", 0.90, 1.10, 1.0), _c("r32", 1.13, 2.618, 1.874)]),
    dict(pattern_id="HA02", variant="ext_1_27", name="Alternate AB=CD 1.27", labels=_ABCD, alias="abcd", prz="r31",
         constraints=[_c("r21", 0.382, 0.886, 0.634), _c("r31", 1.20, 1.34, 1.27)]),
    dict(pattern_id="HA02", variant="ext_1_618", name="Alternate AB=CD 1.618", labels=_ABCD, alias="abcd", prz="r31",
         constraints=[_c("r21", 0.382, 0.886, 0.634), _c("r31", 1.53, 1.70, 1.618)]),
    dict(pattern_id="HA03", variant="canonical", name="Gartley", labels=_XABCD, alias="xabcd", prz="ret41",
         constraints=[_c("r21", 0.585, 0.65, 0.618), _BC, _c("ret41", 0.75, 0.82, 0.786), _c("r43", 1.13, 1.618, 1.27),
                      _c("r42", 0.90, 1.10, 1.0)]),
    dict(pattern_id="HA04", variant="b_0_382", name="Bat (B 0.382 subtype)", labels=_XABCD, alias="xabcd", prz="ret41",
         constraints=[_c("r21", 0.382, 0.45, 0.382, hi_open=True), _BC, _c("ret41", 0.85, 0.92, 0.886),
                      _c("r43", 1.618, 2.618, 2.0), _ABCD_MIN], typical_ext=(1.27,)),
    dict(pattern_id="HA04", variant="b_0_50", name="Bat (B 0.50 subtype)", labels=_XABCD, alias="xabcd", prz="ret41",
         constraints=[_c("r21", 0.45, 0.50, 0.50), _BC, _c("ret41", 0.85, 0.92, 0.886), _c("r43", 1.618, 2.618, 2.0), _ABCD_MIN], typical_ext=(1.27,)),
    dict(pattern_id="HA05", variant="canonical", name="Alternate Bat", labels=_XABCD, alias="xabcd", prz="ret41",
         constraints=[_c("r21", 0.30, 0.40, 0.382), _BC, _c("ret41", 1.10, 1.16, 1.13), _c("r43", 2.0, 3.618, 2.618)]),
    dict(pattern_id="HA06", variant="ext_1_27", name="Butterfly 1.27", labels=_XABCD, alias="xabcd", prz="ret41",
         constraints=[_c("r21", 0.76, 0.81, 0.786), _BC, _c("ret41", 1.20, 1.34, 1.27), _c("r43", 1.618, 2.24, 2.0), _ABCD_MIN],
         typical_ext=(1.27, 1.618)),
    dict(pattern_id="HA06", variant="ext_1_618", name="Butterfly 1.618", labels=_XABCD, alias="xabcd", prz="ret41",
         constraints=[_c("r21", 0.76, 0.81, 0.786), _BC, _c("ret41", 1.55, 1.68, 1.618), _c("r43", 1.618, 2.618, 2.24), _ABCD_MIN],
         typical_ext=(1.27, 1.618)),
    dict(pattern_id="HA07", variant="canonical", name="Crab", labels=_XABCD, alias="xabcd", prz="ret41",
         constraints=[_c("r21", 0.382, 0.618, 0.50), _BC, _c("ret41", 1.55, 1.68, 1.618), _c("r43", 2.24, 3.618, 3.14), _ABCD_MIN], typical_ext=(1.618,)),
    dict(pattern_id="HA08", variant="canonical", name="Deep Crab", labels=_XABCD, alias="xabcd", prz="ret41",
         constraints=[_c("r21", 0.85, 0.92, 0.886), _BC, _c("ret41", 1.55, 1.68, 1.618), _c("r43", 2.0, 3.618, 2.618)]),
    dict(pattern_id="HA09", variant="canonical", name="Shark", labels=_SHARK, alias="shark", prz="ret41",
         constraints=[_c("r21", 0.0, 1.0, None, hi_open=True, lo_open=True), _c("r32", 1.13, 1.618, 1.27),
                      _c("ret41", 0.886, 1.13, 1.0), _c("r43", 1.618, 2.24, 1.93)]),
    dict(pattern_id="HA10", variant="canonical", name="5-0", labels=_XABCD, alias="five_zero", prz="r43",
         constraints=[_c("r21", 1.13, 1.618, 1.27), _c("r32", 1.618, 2.24, 1.93), _c("r43", 0.45, 0.55, 0.50),
                      _c("r42", 0.90, 1.10, 1.0)]),
]

_NOTES = {
    "HA01": "Time symmetry (CD bars / AB bars) is recorded, not gated.",
    "HA02": "CD/BC recorded only (implied >= 1.35 by the bands).",
    "HA03": "B and D stay inside X (implied by AB/XA < 1 and AD/XA < 1). Required AB=CD confluence is gated: "
            "CD/AB in [0.90, 1.10] (same band as HA01).",
    "HA04": "B-point subtype split is exclusive: [0.382, 0.45) vs [0.45, 0.50]. Minimum AB=CD completion "
            "CD/AB >= 0.90 (declared 10% equality tolerance; already implied by the other Bat bands, stated "
            "explicitly); no upper limit.",
    "HA05": "Distinct from standard Bat: D beyond X at ~1.13 XA.",
    "HA06": "1.618 XA extension is a separately versioned wider variant (Carney's alternate Butterfly terminal); "
            "wider-variant assumptions: same B band, AD/XA 1.55-1.68, CD/BC widened to 2.618. Operational "
            "tolerance: minimum AB=CD completion CD/AB >= 0.90 (declared 10% equality tolerance), no upper "
            "limit; binding only in a narrow corner of the 1.27 variant, implied for the 1.618 variant.",
    "HA07": "Keep distinct from Deep Crab: B shallower than 0.618. Minimum AB=CD completion CD/AB >= 0.90 "
            "(declared 10% equality tolerance; already implied by the other Crab bands, stated explicitly); "
            "no upper limit.",
    "HA08": "Deep B (0.85-0.92 XA) is the distinguishing constraint.",
    "HA09": "O-X-A-B-C notation; XC/OX = (X-C)/(X-O) i.e. C at 0.886 retracement to 1.13 extension of OX; "
            "A strictly inside OX; C is the terminal pivot.",
    "HA10": "Five swing points X-A-B-C-D (the preceding origin '0' is not required); AB extends XA (B beyond X), "
            "C beyond A; required reciprocal AB=CD is gated: CD/AB in [0.90, 1.10] (same band as HA01).",
}


def _ratio_name(key, labels):
    if len(labels) == 4:
        a, b, c, d = labels
        return {"r21": f"{b}{c}/{a}{b}", "r31": f"{c}{d}/{a}{b}", "r32": f"{c}{d}/{b}{c}"}[key]
    p0, p1, p2, p3, p4 = labels
    return {"r21": f"{p1}{p2}/{p0}{p1}", "r32": f"{p2}{p3}/{p1}{p2}", "r43": f"{p3}{p4}/{p2}{p3}",
            "r42": f"{p3}{p4}/{p1}{p2}", "ret41": f"{p1}{p4}/{p0}{p1}"}[key]


def _definition(t):
    labels = t["labels"]
    parts = []
    for c in t["constraints"]:
        if c["hi"] is None:
            op = ">" if c["lo_open"] else ">="
            parts.append(f"{_ratio_name(c['ratio'], labels)} {op} {c['lo']} (minimum only, no upper limit, unscored)")
            continue
        lo = "(" if c["lo_open"] else "["
        hi = ")" if c["hi_open"] else "]"
        ideal = "" if c["ideal"] is None else f" ideal {c['ideal']}"
        parts.append(f"{_ratio_name(c['ratio'], labels)} in {lo}{c['lo']}, {c['hi']}{hi}{ideal}")
    first = f"{labels[0]}{labels[1]}"
    return (f"{t['name']} on the last {len(labels)} consecutive causal alternating radius-3 swings "
            f"{'-'.join(labels)} (bull: terminal {labels[-1]} is a swing low, long; bear mirror, short). "
            f"Ratios of absolute leg lengths: {'; '.join(parts)}. PRZ = {_ratio_name(t['prz'], labels)} band. "
            f"{_NOTES[t['pattern_id']]} {_typical_text(t)}Gates: every leg in its alternating direction and >= {MIN_LEG_ATR} ATR(S), "
            f"{first} >= {MIN_FIRST_LEG_ATR} ATR(S); {labels[0]}->{labels[-1]} span {MIN_SPAN_BARS}-{MAX_SPAN_BARS} bars; "
            f"bars {labels[0]}..S clean. Setup at S = {labels[-1]} pivot + 3 (availability). Swing list: same-type "
            f"pivot replaces the last swing only if strictly more extreme, at its availability; double pivots skipped; "
            f"reset on segment change. Confirmation: within {CONFIRM_WINDOW} bars after S, close beyond "
            f"max high (bull) / min low (bear) of bars {labels[-1]}..S by {BUFFER_ATR} ATR(S); failure first = close "
            f"beyond {labels[-1]}'s extreme by {BUFFER_ATR} ATR(S) -> no confirmation; segment break expires. "
            f"episode = {labels[0]} bar. score = 1 - mean normalized ratio distance to ideal. "
            f"Prior trend at {labels[0]} is metadata only.")


def _typical_text(t):
    ext = t.get("typical_ext")
    if not ext:
        return ""
    return (f"Typical AB=CD extension levels {list(ext)} are annotated descriptively in geometry.abcd_extension "
            f"(nearest level and distance); never a gate. ")


def specifications() -> list[dict]:
    out = []
    for t in TEMPLATES:
        for side in common.SIDES:
            out.append(common.spec(
                t["pattern_id"], t["variant"], side, f"{'Bullish' if side == 'long' else 'Bearish'} {t['name']}",
                FAMILY, common.STATES, LOOKBACK, _definition(t), alias_group=t["alias"],
                points=list(t["labels"]),
                ratio_bands=[{"ratio": _ratio_name(c["ratio"], t["labels"]), "lo": c["lo"], "hi": c["hi"],
                              "lo_open": c["lo_open"], "hi_open": c["hi_open"], "ideal": c["ideal"],
                              "bound": "minimum" if c["hi"] is None else "band"}
                             for c in t["constraints"]],
                confirm_window=CONFIRM_WINDOW, buffer_atr=BUFFER_ATR, min_span_bars=MIN_SPAN_BARS,
                max_span_bars=MAX_SPAN_BARS, min_leg_atr=MIN_LEG_ATR, min_first_leg_atr=MIN_FIRST_LEG_ATR))
    return out


# ---------------------------------------------------------------------------- detect
def detect(bars: list[dict], timeframe: str) -> dict:
    specs = specifications()
    col = common.empty_result(specs)
    if not bars:
        return col.result()
    s = common.prepare(bars, timeframe)
    highs = s.pivots(True)
    lows = s.pivots(False)
    both = set(highs.tolist()) & set(lows.tolist())
    order = sorted([(int(p), "H") for p in highs if int(p) not in both]
                   + [(int(p), "L") for p in lows if int(p) not in both])
    by_n = {}
    for t in TEMPLATES:
        by_n.setdefault(len(t["labels"]), []).append(t)

    swings = []  # (index, price, kind, seg)
    for p, kind in order:
        price = float(s.h[p] if kind == "H" else s.l[p])
        seg = int(s.seg_id[p])
        if swings and swings[-1][3] != seg:
            swings.clear()
        if swings and swings[-1][2] == kind:
            last = swings[-1][1]
            if (price > last) if kind == "H" else (price < last):
                swings[-1] = (p, price, kind, seg)
            else:
                continue
        else:
            swings.append((p, price, kind, seg))
            if len(swings) > MAX_SWINGS:
                del swings[0]
        _evaluate(s, swings, by_n, col)
    return col.result()


def _evaluate(s, swings, by_n, col):
    setup = swings[-1][0] + common.PIVOT_RADIUS
    if setup >= s.n:
        return
    atr = s.atr_at(setup)
    if atr is None:
        return
    for npts, templates in by_n.items():
        if len(swings) < npts:
            continue
        pts = swings[-npts:]
        base = _measure(s, pts, setup, atr)
        if base is None:
            continue
        ratios, bull = base["ratios"], base["bull"]
        for t in templates:
            errs = _check(t, ratios)
            if errs is not None:
                _emit(s, t, pts, base, setup, atr, errs, col)


def _check(t, ratios):
    """Normalized ideal-distance errors when every constraint holds, else None.  hi=None = minimum only."""
    errs = []
    for c in t["constraints"]:
        r = ratios[c["ratio"]]
        if r <= c["lo"] if c["lo_open"] else r < c["lo"]:
            return None
        if c["hi"] is not None and (r >= c["hi"] if c["hi_open"] else r > c["hi"]):
            return None
        if c["ideal"] is not None and c["hi"] is not None:
            span = max(c["ideal"] - c["lo"], c["hi"] - c["ideal"])
            errs.append(min(abs(r - c["ideal"]) / span, 1.0) if span > 0 else 0.0)
    return errs


def _measure(s, pts, setup, atr):
    first, term = pts[0][0], pts[-1][0]
    span = term - first
    if span < MIN_SPAN_BARS or span > MAX_SPAN_BARS:
        return None
    if not s.clean(first, setup):
        return None
    legs = []
    for k in range(1, len(pts)):
        prev, cur = pts[k - 1], pts[k]
        move = (cur[1] - prev[1]) if prev[2] == "L" else (prev[1] - cur[1])
        if move < MIN_LEG_ATR * atr or move <= 0:
            return None
        legs.append(move)
    if legs[0] < MIN_FIRST_LEG_ATR * atr:
        return None
    bull = pts[-1][2] == "L"
    if len(pts) == 4:
        ratios = {"r21": legs[1] / legs[0], "r31": legs[2] / legs[0], "r32": legs[2] / legs[1]}
    else:
        sgn = 1.0 if pts[1][2] == "H" else -1.0
        ratios = {"r21": legs[1] / legs[0], "r32": legs[2] / legs[1], "r43": legs[3] / legs[2],
                  "r42": legs[3] / legs[1], "ret41": sgn * (pts[1][1] - pts[4][1]) / legs[0]}
    return {"ratios": ratios, "legs": legs, "bull": bull}


def _prz(t, pts, legs, bull):
    """Price interval of the terminal point implied by the PRZ ratio band."""
    c = next(c for c in t["constraints"] if c["ratio"] == t["prz"])
    d = -1.0 if bull else 1.0  # terminal point lies below (bull) / above (bear) its reference
    if t["prz"] == "ret41":
        ref, unit = pts[1][1], legs[0]
    elif t["prz"] == "r31":
        ref, unit = pts[2][1], legs[0]
    else:  # r43: terminal leg as a fraction of the previous leg
        ref, unit = pts[3][1], legs[2]
    a, b = ref + d * c["lo"] * unit, ref + d * c["hi"] * unit
    return [min(a, b), max(a, b)]


def _emit(s, t, pts, base, setup, atr, errs, col):
    bull = base["bull"]
    side = "long" if bull else "short"
    key = (t["pattern_id"], t["variant"], side)
    labels = t["labels"]
    first, term = pts[0][0], pts[-1][0]
    legs = base["legs"]
    ratios = {_ratio_name(k, labels): v for k, v in base["ratios"].items()}
    if len(pts) == 4:
        ratios["time_" + _ratio_name("r31", labels)] = (pts[3][0] - pts[2][0]) / max(pts[1][0] - pts[0][0], 1)
    else:
        ratios["time_" + _ratio_name("r42", labels)] = (pts[4][0] - pts[3][0]) / max(pts[2][0] - pts[1][0], 1)
    buf = BUFFER_ATR * atr
    if bull:
        confirm_level = float(s.h[term:setup + 1].max()) + buf
        fail_level = float(s.l[term]) - buf
    else:
        confirm_level = float(s.l[term:setup + 1].min()) - buf
        fail_level = float(s.h[term]) + buf
    score = 1.0 - (sum(errs) / len(errs) if errs else 0.0)
    score = min(max(score, 0.0), 1.0)
    geometry = {
        "points": {lab: {"index": p[0], "price": p[1], "available_index": p[0] + common.PIVOT_RADIUS,
                         "kind": "high" if p[2] == "H" else "low"} for lab, p in zip(labels, pts)},
        "ratios": ratios,
        "legs": {f"{labels[k]}{labels[k + 1]}": {"size": legs[k], "size_atr": legs[k] / atr,
                                               "bars": pts[k + 1][0] - pts[k][0]} for k in range(len(legs))},
        "span_bars": term - first,
        "prz": {"ratio": _ratio_name(t["prz"], labels), "price_band": _prz(t, pts, legs, bull)},
        "atr_setup": atr,
        "prior_trend": s.trend(first),
        "confirmation": {"window": CONFIRM_WINDOW, "buffer_atr": BUFFER_ATR, "level": confirm_level,
                         "failure_level": fail_level},
    }
    if t.get("typical_ext"):
        cd_ab = base["ratios"]["r42"]
        nearest = min(t["typical_ext"], key=lambda lvl: abs(cd_ab - lvl))
        geometry["abcd_extension"] = {"cd_ab": cd_ab, "minimum": ABCD_MIN_COMPLETION,
                                      "typical_levels": list(t["typical_ext"]), "nearest_typical": nearest,
                                      "distance_to_nearest": abs(cd_ab - nearest), "descriptive_only": True}
    direction = "bullish" if bull else "bearish"
    ev = common.make_event(s, signal_index=setup, episode=first, state="setup", direction=direction,
                           formation_start=first, detected_index=setup, score=score, geometry=geometry,
                           alias_group=t["alias"])
    if not col.add(key, ev):
        return
    j = _confirm(s, setup, bull, confirm_level, fail_level)
    if j is None:
        return
    col.add(key, common.make_event(s, signal_index=j, episode=first, state="confirmed", direction=direction,
                                   formation_start=first, detected_index=setup, score=score,
                                   confirmed_index=j, geometry=geometry, alias_group=t["alias"]))


def _confirm(s, setup, bull, level, fail):
    for j in range(setup + 1, min(setup + CONFIRM_WINDOW, s.n - 1) + 1):
        if not s.clean(setup, j):
            return None
        c = s.c[j]
        if (c < fail) if bull else (c > fail):
            return None
        if (c > level) if bull else (c < level):
            return j
    return None
