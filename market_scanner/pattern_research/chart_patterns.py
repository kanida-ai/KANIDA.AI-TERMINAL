"""CH11-CH28 research chart-pattern detectors (Claude Code owned, research only).

Contract: docs/pattern_research/CLAUDE_HANDOFF.md
    specifications() -> list[dict]
    detect(bars, timeframe) -> {(pattern_id, variant, side): [event, ...]}

Engine (point-in-time by construction)
- Radius-3 pivots from common.Series; a pivot at p is used only at p+3.
- Geometric candidates are evaluated only at *trigger bars*: the availability bar of
  a new pivot (or, for zigzag families, a new/replaced zigzag anchor; for CH19/CH20
  the first close beyond a rim; for CH24 the second full-range gap bar).  The new
  pivot must be the formation's final anchor, so a setup is emitted once, at the
  first bar its last anchor is known.  Families are processed in chronological
  trigger order.
- A setup is then tracked bar by bar from its detection bar d through d+expiry for
  a close beyond its confirmation boundary (confirmed, same episode) or beyond its
  failure boundary (terminated).  Buffers/tolerances use ATR(d), frozen.  Tracking
  stops at a segment break (quality gap/invalid bar) or at a converging apex.
  A setup and its confirmation may share bar d.
- Episode = formation_start_index (first anchor).  Per family group a new formation
  must start after the last anchor of every previously emitted formation of that
  group (first-detected wins, continuing observations emit nothing).
- Mirrored families run the same code on a negated price view (high' = -low,
  low' = -high, close' = -close, pivot types swapped, trend up<->down); every
  price stored in events is converted back to real prices.
- Scores are geometric fit quality in [0, 1], not probabilities (see each definition).
"""
from __future__ import annotations

import math

import numpy as np

from . import common

FAMILY = "chart"
SPAN_MIN, SPAN_MAX = 20, 150          # formation window [formation_start, detection] in bars
SEP = 5                               # bars between consecutive major turning points
HEIGHT_ATR = 2.0                      # minimum pattern height in ATR(d)
TOL = common.EQUAL_TOL_ATR            # 0.5 ATR equality tolerance
BUF = common.BREAKOUT_BUFFER_ATR      # 0.12 ATR close-beyond buffer
EXPIRY = common.CHART_SETUP_EXPIRY    # 10 subsequent closed bars
R = common.PIVOT_RADIUS
LOOKBACK = SPAN_MAX + common.TREND_LOOKBACK + common.ATR_PERIOD + 45

# Family-specific frozen parameters
PENNANT = dict(pole_min_bars=5, pole_max_bars=20, pole_atr=5.0, pole_eff=0.72, sep=3,
               dur_min=6, dur_max=25, width_pole=0.5, contraction=0.7, window=(15, 50))
BOWL = dict(pos=(0.25, 0.75), r2=0.78, v_ratio=0.9, floor_band=0.28, floor=(0.18, 0.60), handle_dd=0.33)
CUP = dict(width_min=24, depth_atr=3.0, depth_pct=0.04, depth_max_pct=0.40, align=0.22, prior=0.5,
           handle_len=0.45, handle_depth=(0.07, 0.40))
BROAD = dict(anchors=5, widen=1.3, slope_tag_atr=1.0)
ISLAND = dict(max_bars=30)
VSHAPE = dict(decline_min=5, decline_max=15, decline_atr=4.0, steep_atr=0.4, eff=0.6, sharp_frac=0.1,
              sharp_max=3, retrace=0.5)
MEASURED = dict(leg_atr=3.0, leg_eff=0.5, retr=(0.25, 0.75), corr_mult=2.0, expiry_cap=40)
BUMP = dict(lead_min=10, bump_search=60, slope=(0.05, 0.6), mult=2.0, height_mult=2.0, expiry_cap=40)
VCP = dict(contraction=0.8)

COMMON_TEXT = (
    "Common gates v1.0.0: radius-3 pivots used only at pivot+3; formation window "
    "[formation_start, detection] 20-150 bars unless an exception is named; >=5 bars between consecutive "
    "anchoring turning points; pattern height >=2 ATR; equality tolerance 0.5 ATR; ATR = common prior ATR "
    "at the detection bar, frozen for the episode; the whole window must be quality-gap free "
    "(Series.clean); confirmation = close beyond the boundary by 0.12 ATR within detection..detection+10 "
    "closed bars (setup and confirmation may share the detection bar); a close beyond the failure boundary "
    "by 0.12 ATR, a segment break, or expiry terminates the setup and later confirmations are not emitted. "
    "Volume ratio (confirm-bar volume / median of prior 20 bars) is recorded, never gated. "
    "Episode = formation_start_index; a new formation must start after the last anchor of any earlier "
    "emitted formation of the same family. ")


# =========================================================================== helpers
class _View:
    """Price view in 'bullish orientation'; mirror=True negates prices."""

    def __init__(self, s: common.Series, mirror: bool):
        self.s, self.mirror = s, mirror
        if mirror:
            self.h, self.l, self.c = -s.l, -s.h, -s.c
            self.hp, self.lp = s.pivots(False), s.pivots(True)
        else:
            self.h, self.l, self.c = s.h, s.l, s.c
            self.hp, self.lp = s.pivots(True), s.pivots(False)
        self.bull = "bearish" if mirror else "bullish"
        self.bear = "bullish" if mirror else "bearish"

    def trend(self, start):
        t = self.s.trend(start)
        if self.mirror and t in ("up", "down"):
            return "down" if t == "up" else "up"
        return t

    def real(self, y):
        return -float(y) if self.mirror else float(y)

    def avail(self, piv, i, since):
        lo = np.searchsorted(piv, since, side="left")
        hi = np.searchsorted(piv, i - R, side="right")
        return piv[lo:hi]


class _Ctx:
    def __init__(self, s, specs):
        self.s = s
        self.col = common.empty_result(specs)
        self.groups = {}
        self.doubles = {False: set(), True: set()}   # (t1, t2) pairs emitted by CH16 / CH15
        self._zz = None

    def claim(self, group, start, anchor_end):
        g = self.groups.get(group)
        if g is None:
            g = self.groups[group] = [set(), -1]
        if start in g[0] or start <= g[1]:
            return False
        g[0].add(start)
        g[1] = max(g[1], int(anchor_end))
        return True

    def zigzag(self):
        """Causal zigzag trace over available pivots (append alternating, replace same-type
        anchor only by a more extreme pivot in the same segment).  Returns (final, types,
        trace) where trace rows are (trigger_bar, length, last_anchor, last_is_high); the
        anchor list at a trace row is final[:length-1] + [last_anchor]."""
        if self._zz is None:
            s = self.s
            ev = sorted([(int(p), 0) for p in s.pivots(True)] + [(int(p), 1) for p in s.pivots(False)])
            final, types, trace = [], [], []
            for p, kind in ev:
                is_high = kind == 0
                if final and types[-1] == is_high and s.seg_id[p] == s.seg_id[final[-1]]:
                    better = s.h[p] > s.h[final[-1]] if is_high else s.l[p] < s.l[final[-1]]
                    if not better:
                        continue
                    final[-1] = p
                else:
                    final.append(p)
                    types.append(is_high)
                trace.append((p + R, len(final), p, is_high))
            self._zz = (final, types, trace)
        return self._zz


def _clip01(x):
    x = float(x)
    return 0.0 if not math.isfinite(x) else min(1.0, max(0.0, x))


def _sep_latest(idx, sep=SEP):
    out = []
    for p in reversed([int(x) for x in idx]):
        if not out or out[-1] - p >= sep:
            out.append(p)
    return out[::-1]


def _gaps_ok(anchors, sep=SEP):
    return all(b - a >= sep for a, b in zip(anchors, anchors[1:]))


def _fit(xs, ys):
    """Least-squares line -> (j0, y0, slope, rms)."""
    x = np.asarray(xs, dtype=float)
    y = np.asarray(ys, dtype=float)
    if len(x) == 2:
        return float(x[0]), float(y[0]), float((y[1] - y[0]) / (x[1] - x[0])), 0.0
    xm, ym = x.mean(), y.mean()
    slope = float(((x - xm) * (y - ym)).sum() / ((x - xm) ** 2).sum())
    resid = y - (ym + slope * (x - xm))
    return float(xm), float(ym), slope, float(math.sqrt(float(np.mean(resid ** 2))))


def _lv(line, j):
    j0, y0, slope = line[0], line[1], line[2]
    return y0 + slope * (np.asarray(j, dtype=float) - j0) if not np.isscalar(j) else y0 + slope * (j - j0)


def _eff(c, a, b):
    path = float(np.abs(np.diff(c[a:b + 1])).sum())
    return (float(c[b] - c[a]) / path) if path > 0 else 0.0


def _bowl(y, bottom_off):
    m = len(y)
    x = np.linspace(-1.0, 1.0, m)
    X = np.column_stack((x * x, x, np.ones(m)))
    coef = np.linalg.lstsq(X, y, rcond=None)[0]
    fitted = X @ coef
    ss = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - float(((y - fitted) ** 2).sum()) / ss if ss > 0 else 0.0
    vx = np.column_stack((np.abs(np.arange(m) - bottom_off), np.ones(m)))
    vfit = vx @ np.linalg.lstsq(vx, y, rcond=None)[0]
    return float(coef[0]), r2, float(np.mean((y - fitted) ** 2)), float(np.mean((y - vfit) ** 2))


def _vol_ratio(s, j):
    if j < 20 or not s.clean(j - 20, j):
        return None
    med = float(np.median(s.v[j - 20:j]))
    return float(s.v[j] / med) if med > 0 else None


def _key(pid, variant, side):
    return (pid, variant, side)


def _track(ctx, v, *, group, start, anchor_end, detected, A, setup_keys, exits, geometry, score,
           alias=None, tags=None, expiry=EXPIRY, stop_at=None):
    """Claim the episode, emit setup events, then track confirmation/failure/expiry.

    exits: list of (sign, line, key_or_None, direction, extra_geometry); in view space a
    close satisfies an exit when sign * (close - line(j)) > 0.12 * ATR(d).  key None marks
    a failure boundary.  Only bars <= j are used for an event at j.
    """
    s = ctx.s
    if group is not None and not ctx.claim(group, start, anchor_end):
        return False
    for key, direction in setup_keys:
        ctx.col.add(key, common.make_event(
            s, signal_index=detected, episode=start, state="setup", direction=direction,
            formation_start=start, detected_index=detected, score=score, geometry=geometry,
            alias_group=alias, quality_tags=tags))
    buf = BUF * A
    last = min(detected + expiry, s.n - 1)
    c = v.c
    for j in range(detected, last + 1):
        if j > detected and not s.clean(start, j):
            break
        if stop_at is not None and j >= stop_at:
            break
        for sign, line, key, direction, extra in exits:
            level = _lv(line, j)
            if sign * (c[j] - level) > buf:
                if key is not None:
                    geo = dict(geometry)
                    geo.update(confirm_level=v.real(level), bars_after_setup=j - detected,
                               volume_ratio=_vol_ratio(s, j))
                    if extra:
                        geo.update(extra)
                    ctx.col.add(key, common.make_event(
                        s, signal_index=j, episode=start, state="confirmed", direction=direction,
                        formation_start=start, detected_index=detected, confirmed_index=j,
                        score=score, geometry=geo, alias_group=alias, quality_tags=tags))
                return True
    return True


def _confirmed_only(ctx, key, *, group, start, anchor_end, j, direction, geometry, score, alias=None, tags=None):
    if not ctx.claim(group, start, anchor_end):
        return False
    geo = dict(geometry)
    geo["volume_ratio"] = _vol_ratio(ctx.s, j)
    ctx.col.add(key, common.make_event(
        ctx.s, signal_index=j, episode=start, state="confirmed", direction=direction, formation_start=start,
        detected_index=j, confirmed_index=j, score=score, geometry=geo, alias_group=alias, quality_tags=tags))
    return True


def _pivot_triggers(v):
    """Unique trigger bars with the new view high/low pivot (or None) at p = i-3."""
    rows = {}
    for p in v.hp:
        rows.setdefault(int(p) + R, [None, None])[0] = int(p)
    for p in v.lp:
        rows.setdefault(int(p) + R, [None, None])[1] = int(p)
    return sorted((i, hp, lp) for i, (hp, lp) in rows.items())


def _atr_ok(s, i):
    a = s.atr_at(i)
    return a if a is not None and a > 0 else None


# ====================================================================== CH11 / CH13
def _flat_cluster(v, i, top_side, A, lo):
    """Flat boundary cluster ending at the latest available pivot of that side.

    Region = latest suffix [k, i] whose highs never exceed latest-pivot-high + 0.5 ATR
    (lows never below latest-pivot-low - 0.5 ATR for support); level = extreme of the
    region; touches = region pivots within 0.5 ATR of the level, >=5 bars apart (latest kept).
    """
    tol = TOL * A
    if top_side:
        piv = v.avail(v.hp, i, lo)
        if len(piv) == 0:
            return None
        ref = v.h[piv[-1]]
        bad = np.flatnonzero(v.h[lo:i + 1] > ref + tol)
        region = lo + (int(bad[-1]) + 1 if bad.size else 0)
        if region > piv[-1]:
            return None
        level = float(v.h[region:i + 1].max())
        touch = piv[(piv >= region) & (v.h[piv] >= level - tol)]
    else:
        piv = v.avail(v.lp, i, lo)
        if len(piv) == 0:
            return None
        ref = v.l[piv[-1]]
        bad = np.flatnonzero(v.l[lo:i + 1] < ref - tol)
        region = lo + (int(bad[-1]) + 1 if bad.size else 0)
        if region > piv[-1]:
            return None
        level = float(v.l[region:i + 1].min())
        touch = piv[(piv >= region) & (v.l[piv] <= level + tol)]
    touches = _sep_latest(touch)
    vals = [float(v.h[t] if top_side else v.l[t]) for t in touches]
    return dict(region=region, level=level, touches=touches, spread=(max(vals) - min(vals)) if vals else 0.0)


def _ch11(ctx):
    s = ctx.s
    v = _View(s, False)
    k_up, k_dn = _key("CH11", "upside", "long"), _key("CH11", "downside_resolution", "short")
    for i, hp, lp in _pivot_triggers(v):
        A = _atr_ok(s, i)
        if A is None:
            continue
        lo = max(i - SPAN_MAX + 1, int(s.seg_start[i]))
        cl = _flat_cluster(v, i, True, A, lo)
        if cl is None or len(cl["touches"]) < 2:
            continue
        T, top = cl["touches"], cl["level"]
        troughs = []
        for a, b in zip(T, T[1:]):
            if b - a < 2:
                troughs = None
                break
            troughs.append(a + 1 + int(np.argmin(v.l[a + 1:b])))
        if troughs is None:
            continue
        if lp is not None and lp > T[-1]:
            if v.l[lp] > v.l[T[-1] + 1:i + 1].min():
                continue
            troughs.append(lp)
        elif hp is None or hp != T[-1]:
            continue
        if len(troughs) < 2:
            continue
        anchors = sorted(T + troughs)
        if not _gaps_ok(anchors):
            continue
        tl = [float(v.l[t]) for t in troughs]
        if any(b - a <= TOL * A for a, b in zip(tl, tl[1:])):
            continue
        line = _fit(troughs, tl)
        if line[2] <= 0 or (len(troughs) >= 3 and line[3] > TOL * A):
            continue
        start = T[0]
        if not (SPAN_MIN <= i - start + 1 <= SPAN_MAX) or not s.clean(start, i):
            continue
        height = top - _lv(line, start)
        if height < HEIGHT_ATR * A:
            continue
        apex = line[0] + (top - line[1]) / line[2]
        if apex <= i:
            continue
        js = np.arange(start, i)
        if np.any(v.c[start:i] < _lv(line, js) - BUF * A):
            continue
        score = _clip01(1 - (cl["spread"] + line[3]) / (2 * TOL * A))
        geo = dict(resistance=top, resistance_touches=T, resistance_available=[t + R for t in T],
                   resistance_spread_atr=cl["spread"] / A, support_troughs=troughs,
                   support_available=[t + R for t in troughs], support_slope_atr_per_bar=line[2] / A,
                   support_rms_atr=line[3] / A, support_line=[line[0], line[1], line[2]],
                   height_atr=height / A, apex_index=apex, atr_frozen=A)
        _track(ctx, v, group="CH11", start=start, anchor_end=anchors[-1], detected=i, A=A,
               setup_keys=[(k_up, "bullish")],
               exits=[(1, (0.0, top, 0.0), k_up, "bullish", None),
                      (-1, line, k_dn, "bearish", None)],
               geometry=geo, score=score, stop_at=int(math.ceil(apex)))


def _ch13(ctx):
    s = ctx.s
    v = _View(s, False)
    k_up, k_dn = _key("CH13", "upside", "long"), _key("CH13", "downside", "short")
    for i, hp, lp in _pivot_triggers(v):
        A = _atr_ok(s, i)
        if A is None:
            continue
        lo = max(i - SPAN_MAX + 1, int(s.seg_start[i]))
        top = _flat_cluster(v, i, True, A, lo)
        bot = _flat_cluster(v, i, False, A, lo)
        if top is None or bot is None:
            continue
        region = max(top["region"], bot["region"])
        if region != top["region"] or region != bot["region"]:
            top = _flat_cluster(v, i, True, A, region)
            bot = _flat_cluster(v, i, False, A, region)
            if top is None or bot is None:
                continue
        T, B = top["touches"], bot["touches"]
        if len(T) < 2 or len(B) < 2:
            continue
        if not ((hp is not None and hp == T[-1]) or (lp is not None and lp == B[-1])):
            continue
        merged = sorted([(t, 1) for t in T] + [(b, 0) for b in B])
        anchors = [m[0] for m in merged]
        if not _gaps_ok(anchors):
            continue
        if sum(a[1] != b[1] for a, b in zip(merged, merged[1:])) < 2:
            continue
        start = anchors[0]
        if not (SPAN_MIN <= i - start + 1 <= SPAN_MAX) or not s.clean(start, i):
            continue
        hi, lo_level = top["level"], bot["level"]
        if hi - lo_level < HEIGHT_ATR * A:
            continue
        if np.any(v.c[start:i] > hi + BUF * A) or np.any(v.c[start:i] < lo_level - BUF * A):
            continue
        score = _clip01(1 - (top["spread"] + bot["spread"]) / (2 * TOL * A))
        geo = dict(resistance=hi, support=lo_level, resistance_touches=T, support_touches=B,
                   resistance_available=[t + R for t in T], support_available=[b + R for b in B],
                   resistance_spread_atr=top["spread"] / A, support_spread_atr=bot["spread"] / A,
                   height_atr=(hi - lo_level) / A, atr_frozen=A)
        _track(ctx, v, group="CH13", start=start, anchor_end=anchors[-1], detected=i, A=A,
               setup_keys=[(k_up, "neutral"), (k_dn, "neutral")],
               exits=[(1, (0.0, hi, 0.0), k_up, "bullish", None),
                      (-1, (0.0, lo_level, 0.0), k_dn, "bearish", None)],
               geometry=geo, score=score, alias="horizontal_range")


# ============================================================================ CH12
def _ch12(ctx):
    s = ctx.s
    v = _View(s, False)
    key = _key("CH12", "support_breakdown", "short")
    for p in v.lp:
        p = int(p)
        i = p + R
        A = _atr_ok(s, i)
        if A is None:
            continue
        lo = max(i - SPAN_MAX + 1, int(s.seg_start[i]))
        cl = _flat_cluster(v, i, False, A, lo)
        if cl is None or len(cl["touches"]) < 3 or cl["touches"][-1] != p:
            continue
        T, level = cl["touches"], cl["level"]
        start = T[0]
        if not (SPAN_MIN <= i - start + 1 <= SPAN_MAX) or not s.clean(start, i):
            continue
        ceiling = float(v.h[start:i + 1].max())
        if ceiling - level < HEIGHT_ATR * A:
            continue
        score = _clip01(1 - cl["spread"] / (TOL * A))
        geo = dict(support=level, support_touches=T, support_available=[t + R for t in T],
                   support_spread_atr=cl["spread"] / A, base_high=ceiling, height_atr=(ceiling - level) / A,
                   atr_frozen=A)
        _track(ctx, v, group="CH12", start=start, anchor_end=p, detected=i, A=A,
               setup_keys=[(key, "bearish")],
               exits=[(-1, (0.0, level, 0.0), key, "bearish", None),
                      (1, (0.0, ceiling, 0.0), None, None, None)],
               geometry=geo, score=score)


# ============================================================================ CH14
def _ch14(ctx, mirror):
    s = ctx.s
    v = _View(s, mirror)
    key = _key("CH14", "bear" if mirror else "bull", "short" if mirror else "long")
    final, types, trace = ctx.zigzag()
    P = PENNANT
    for i, length, last, last_high in trace:
        A = _atr_ok(s, i)
        if A is None or length < 4:
            continue
        m = min(length, 10)
        anchors = final[length - m:length - 1] + [last]
        kinds = types[length - m:length - 1] + [last_high]
        if mirror:
            kinds = [not k for k in kinds]
        done = False
        for bpos in range(len(anchors) - 4, -1, -1):   # b = pole top (view high anchor), latest first
            b = anchors[bpos]
            if not kinds[bpos] or last - b > P["dur_max"] or done:
                continue
            seq, sk = anchors[bpos:], kinds[bpos:]
            if last - b < P["dur_min"] or not _gaps_ok(seq, P["sep"]):
                continue
            if any(x == y for x, y in zip(sk, sk[1:])):
                continue
            H = [a for a, k in zip(seq, sk) if k]
            L = [a for a, k in zip(seq, sk) if not k]
            if len(H) < 2 or len(L) < 2:
                continue
            if v.h[b] < v.h[b:i + 1].max():
                continue
            lo = b - P["pole_max_bars"]
            if lo < s.seg_start[b]:
                continue
            a = lo + int(np.argmin(v.l[lo:b - P["pole_min_bars"] + 1]))
            pole = float(v.h[b] - v.l[a])
            eff = _eff(v.c, a, b)
            if pole < P["pole_atr"] * A or eff < P["pole_eff"]:
                continue
            up = _fit(H, [float(v.h[x]) for x in H])
            dn = _fit(L, [float(v.l[x]) for x in L])
            if up[2] >= 0 or (len(H) >= 3 and up[3] > TOL * A) or (len(L) >= 3 and dn[3] > TOL * A):
                continue
            w0 = float(v.h[b] - _lv(dn, b))
            w1 = float(_lv(up, last) - _lv(dn, last))
            if w0 <= 0 or w0 > P["width_pole"] * pole or w1 > P["contraction"] * w0:
                continue
            if up[2] - dn[2] >= 0:
                continue
            apex = up[0] + ((dn[1] - dn[2] * (dn[0] - up[0])) - up[1]) / (up[2] - dn[2])
            if apex <= i:
                continue
            if float(v.l[b:i + 1].min()) < v.h[b] - 0.5 * pole:
                continue
            if not (P["window"][0] <= i - a + 1 <= P["window"][1]) or not s.clean(a, i):
                continue
            js = np.arange(b, i)
            cc = v.c[b:i]
            if np.any(cc > _lv(up, js) + BUF * A) or np.any(cc < _lv(dn, js) - BUF * A):
                continue
            done = True
            score = _clip01(0.5 * eff + 0.5 * (1 - (up[3] + dn[3]) / (2 * TOL * A)))
            geo = dict(pole_start=a, pole_top=b, pole_height_atr=pole / A, pole_efficiency=eff,
                       upper_anchors=H, lower_anchors=L, anchors_available=[x + R for x in seq],
                       upper_line=[up[0], v.real(up[1]), -up[2] if mirror else up[2]],
                       lower_line=[dn[0], v.real(dn[1]), -dn[2] if mirror else dn[2]],
                       width_start_atr=w0 / A, width_end_atr=w1 / A, apex_index=apex, atr_frozen=A)
            if mirror:
                geo["upper_line"], geo["lower_line"] = geo["lower_line"], geo["upper_line"]
            _track(ctx, v, group=key, start=a, anchor_end=last, detected=i, A=A,
                   setup_keys=[(key, v.bull)],
                   exits=[(1, up, key, v.bull, None), (-1, dn, None, None, None)],
                   geometry=geo, score=score, stop_at=int(math.ceil(apex)))


# ====================================================================== CH15-CH18
def _adam_eve(v, t, A):
    lo = v.l[t - R:t + R + 1]
    return "adam" if int(np.count_nonzero(lo <= v.l[t] + 0.25 * A)) <= 2 else "eve"


def _double(ctx, mirror):
    """Double bottom in view orientation (real: CH16 long; mirror: CH15 double top short)."""
    s = ctx.s
    v = _View(s, mirror)
    pid, name = ("CH15", "double_top") if mirror else ("CH16", "double_bottom")
    key = _key(pid, "canonical", "short" if mirror else "long")
    lp = v.lp
    for p in lp:
        p = int(p)
        i = p + R
        A = _atr_ok(s, i)
        if A is None:
            continue
        tol = TOL * A
        lo = max(i - SPAN_MAX + 1, int(s.seg_start[i]))
        cands = v.avail(lp, i, lo)
        cands = cands[(cands <= p - 2 * SEP) & (np.abs(v.l[cands] - v.l[p]) <= tol)]
        for q in cands[::-1]:
            q = int(q)
            trough = float(min(v.l[q], v.l[p]))
            if float(v.l[q + 1:p].min()) < trough:
                continue
            mid = q + 1 + int(np.argmax(v.h[q + 1:p]))
            if mid - q < SEP or p - mid < SEP:
                continue
            neck = float(v.h[mid])
            if neck - trough < HEIGHT_ATR * A:
                continue
            if i - q + 1 < SPAN_MIN or not s.clean(q, i):
                continue
            if v.trend(q) != "down":
                continue
            if np.any(v.c[q:i] > neck + BUF * A):
                continue
            sub = f"{_adam_eve(v, q, A)}_{_adam_eve(v, p, A)}"
            geo = dict(troughs=[q, p], troughs_available=[q + R, p + R], trough_levels=[v.real(v.l[q]), v.real(v.l[p])],
                       intervening_extreme=mid, neckline=v.real(neck), height_atr=(neck - trough) / A,
                       level_difference_atr=abs(float(v.l[q] - v.l[p])) / A, prior_trend=s.trend(q),
                       subtype=sub, subtype_rule="adam if <=2 of bars t-3..t+3 lie within 0.25 ATR of the extreme else eve",
                       atr_frozen=A)
            score = _clip01(1 - abs(float(v.l[q] - v.l[p])) / tol)
            if _track(ctx, v, group=key, start=q, anchor_end=p, detected=i, A=A,
                      setup_keys=[(key, v.bull)],
                      exits=[(1, (0.0, neck, 0.0), key, v.bull, None),
                             (-1, (0.0, trough, 0.0), None, None, None)],
                      geometry=geo, score=score, alias=name, tags=[f"subtype:{sub}"]):
                ctx.doubles[mirror].add((q, p))
            break


def _triple(ctx, mirror):
    s = ctx.s
    v = _View(s, mirror)
    pid, name = ("CH17", "double_top") if mirror else ("CH18", "double_bottom")
    key = _key(pid, "canonical", "short" if mirror else "long")
    lp = v.lp
    for p in lp:
        p = int(p)
        i = p + R
        A = _atr_ok(s, i)
        if A is None:
            continue
        tol = TOL * A
        lo = max(i - SPAN_MAX + 1, int(s.seg_start[i]))
        cands = v.avail(lp, i, lo)
        cands = [int(x) for x in cands[(cands <= p - 2 * SEP) & (np.abs(v.l[cands] - v.l[p]) <= tol)]]
        found = None
        for t2 in reversed(cands):
            for t1 in reversed([x for x in cands if x <= t2 - 2 * SEP]):
                lows = [float(v.l[t1]), float(v.l[t2]), float(v.l[p])]
                if max(lows) - min(lows) > tol:
                    continue
                trough = min(lows)
                if float(v.l[t1:p + 1].min()) < trough:
                    continue
                m1 = t1 + 1 + int(np.argmax(v.h[t1 + 1:t2]))
                m2 = t2 + 1 + int(np.argmax(v.h[t2 + 1:p]))
                if min(m1 - t1, t2 - m1, m2 - t2, p - m2) < SEP:
                    continue
                res = float(max(v.h[m1], v.h[m2]))
                if float(min(v.h[m1], v.h[m2])) - trough < HEIGHT_ATR * A:
                    continue
                if i - t1 + 1 < SPAN_MIN or not s.clean(t1, i) or v.trend(t1) != "down":
                    continue
                if np.any(v.c[t1:i] > res + BUF * A):
                    continue
                found = (t1, t2, m1, m2, res, trough, lows)
                break
            if found:
                break
        if not found:
            continue
        t1, t2, m1, m2, res, trough, lows = found
        extends = (t1, t2) in ctx.doubles[mirror]
        geo = dict(troughs=[t1, t2, p], troughs_available=[t1 + R, t2 + R, p + R],
                   trough_levels=[v.real(x) for x in lows], intervening_extremes=[m1, m2],
                   resistance=v.real(res), height_atr=(float(min(v.h[m1], v.h[m2])) - trough) / A,
                   level_range_atr=(max(lows) - min(lows)) / A, prior_trend=s.trend(t1),
                   extends_double_episode=t1 if extends else None, atr_frozen=A)
        _track(ctx, v, group=key, start=t1, anchor_end=p, detected=i, A=A, setup_keys=[(key, v.bull)],
               exits=[(1, (0.0, res, 0.0), key, v.bull, None), (-1, (0.0, trough, 0.0), None, None, None)],
               geometry=geo, score=_clip01(1 - (max(lows) - min(lows)) / tol),
               alias=name if extends else None)


# ====================================================================== CH19 / CH20
def _rounding(ctx, mirror):
    s = ctx.s
    v = _View(s, mirror)
    pid = "CH20" if mirror else "CH19"
    key = _key(pid, "canonical", "short" if mirror else "long")
    seg_end = _segment_end(s)
    atr = s.atr
    B = BOWL
    found = []
    for L in v.hp:
        L = int(L)
        j_hi = min(L + SPAN_MAX - 1, int(seg_end[L]))
        if L + R + 1 > j_hi:
            continue
        js = np.arange(L + R + 1, j_hi + 1)
        with np.errstate(invalid="ignore"):
            cross = v.c[js] > v.h[L] + BUF * atr[js]
        hits = np.flatnonzero(cross)
        if hits.size == 0:
            continue
        j = int(js[hits[0]])
        if j - L + 1 < SPAN_MIN:
            continue
        A = _atr_ok(s, j)
        if A is None or not s.clean(L, j):
            continue
        rim = float(v.h[L])
        if float(v.h[L + 1:j].max()) > rim + TOL * A:
            continue
        bot = L + int(np.argmin(v.l[L:j + 1]))
        pos = (bot - L) / (j - L)
        depth = rim - float(v.l[bot])
        if not (B["pos"][0] <= pos <= B["pos"][1]) or depth < HEIGHT_ATR * A:
            continue
        y = v.c[L:j + 1]
        a2, r2, mse_q, mse_v = _bowl(y, bot - L)
        if a2 <= 0 or r2 < B["r2"] or mse_q > B["v_ratio"] * mse_v:
            continue
        floor = float(np.mean(y < v.l[bot] + B["floor_band"] * depth))
        if not (B["floor"][0] <= floor <= B["floor"][1]):
            continue
        right = v.c[bot:j + 1]
        dd = float((np.maximum.accumulate(right) - right).max())
        if dd > B["handle_dd"] * depth:
            continue
        geo = dict(left_rim=L, left_rim_available=L + R, rim_level=v.real(rim), bowl_extreme=bot,
                   bowl_position=pos, depth_atr=depth / A, quadratic_r2=r2, quadratic_mse=mse_q, v_fit_mse=mse_v,
                   floor_fraction=floor, right_side_max_pullback_depth=dd / depth, prior_trend=s.trend(L),
                   confirm_level=v.real(rim + BUF * A), atr_frozen=A)
        found.append((j, -r2, L, geo, _clip01(r2)))
    for j, _, L, geo, score in sorted(found, key=lambda x: (x[0], x[1], x[2])):
        _confirmed_only(ctx, key, group=key, start=L, anchor_end=j, j=j, direction=v.bull, geometry=geo, score=score)


def _segment_end(s):
    n = s.n
    end = np.empty(n, dtype=np.int64)
    if n:
        change = np.flatnonzero(np.diff(s.seg_id) != 0)
        bounds = np.r_[change, n - 1]
        ids = np.searchsorted(bounds, np.arange(n), side="left")
        end[:] = bounds[ids]
    return end


# ============================================================================ CH21
def _inverted_cup(ctx):
    """Bearish inverted cup & handle = legacy cup rules on the negated view."""
    s = ctx.s
    v = _View(s, True)
    key = _key("CH21", "canonical", "short")
    C = CUP
    for p in v.lp:     # handle extreme (view low = real handle high)
        p = int(p)
        i = p + R
        A = _atr_ok(s, i)
        if A is None:
            continue
        rights = v.avail(v.hp, p, 0)
        rights = rights[rights <= p - SEP]
        if len(rights) == 0:
            continue
        right = int(rights[-1])
        if float(v.l[p]) > float(v.l[right + 1:i + 1].min()) or float(v.h[right + 1:i + 1].max()) > v.h[right]:
            continue
        lo = max(i - SPAN_MAX + 1, int(s.seg_start[i]) + common.TREND_LOOKBACK)
        lefts = v.hp[(v.hp >= lo) & (v.hp <= right - C["width_min"])]
        best = None
        for left in lefts[::-1]:
            left = int(left)
            width = right - left
            if i - right > C["handle_len"] * width:
                continue
            bottom = left + int(np.argmin(v.l[left:right + 1]))
            if not 0.25 < (bottom - left) / width < 0.75:
                continue
            rim = float(v.h[left] + v.h[right]) / 2
            depth = rim - float(v.l[bottom])
            mag = abs(rim)
            if not (max(C["depth_atr"] * A, C["depth_pct"] * mag) < depth < C["depth_max_pct"] * mag):
                continue
            align = abs(float(v.h[left] - v.h[right])) / depth
            if align > C["align"] or float(v.h[left] - v.c[left - 20]) < C["prior"] * depth:
                continue
            hdepth = float(v.h[right] - v.l[p])
            if not C["handle_depth"][0] * depth < hdepth < C["handle_depth"][1] * depth:
                continue
            if not s.clean(left - 20, i):
                continue
            resist = float(max(v.h[left], v.h[right]))
            if np.any(v.c[left:i] > resist + BUF * A):
                continue
            y = v.c[left:right + 1]
            a2, r2, mse_q, mse_v = _bowl(y, bottom - left)
            if a2 <= 0 or r2 < BOWL["r2"] or mse_q > BOWL["v_ratio"] * mse_v:
                continue
            floor = float(np.mean(y < v.l[bottom] + BOWL["floor_band"] * depth))
            if not BOWL["floor"][0] < floor < BOWL["floor"][1]:
                continue
            if best is None or r2 > best[0]:
                best = (r2, left, bottom, depth, align, hdepth, resist, floor)
        if best is None:
            continue
        r2, left, bottom, depth, align, hdepth, resist, floor = best
        cup_vol = float(np.mean(s.v[left:right + 1]))
        handle_vol = float(np.mean(s.v[right + 1:i + 1]))
        geo = dict(left_rim=left, right_rim=right, handle_extreme=p,
                   anchors_available=[left + R, right + R, p + R], dome_extreme=bottom,
                   support=v.real(resist), depth_atr=depth / A, rim_alignment=align, quadratic_r2=r2,
                   floor_fraction=floor, handle_retrace=hdepth / depth, handle_failure_level=v.real(v.l[p]),
                   handle_to_cup_volume=(handle_vol / cup_vol) if cup_vol > 0 else None,
                   prior_decline_depth=float(v.h[left] - v.c[left - 20]) / depth, atr_frozen=A)
        _track(ctx, v, group=key, start=left, anchor_end=p, detected=i, A=A, setup_keys=[(key, "bearish")],
               exits=[(1, (0.0, resist, 0.0), key, "bearish", None), (-1, (0.0, float(v.l[p]), 0.0), None, None, None)],
               geometry=geo, score=_clip01(r2 * (1 - align)))


# ============================================================================ CH22
def _trend_tag(t):
    return {"up": "top", "down": "bottom", "neutral": "neutral"}.get(t, "unknown")


def _broadening(ctx):
    s = ctx.s
    v = _View(s, False)
    k_up, k_dn = _key("CH22", "upside", "long"), _key("CH22", "downside", "short")
    final, types, trace = ctx.zigzag()
    n_anc = BROAD["anchors"]
    for i, length, last, last_high in trace:
        if length < n_anc:
            continue
        A = _atr_ok(s, i)
        if A is None:
            continue
        anchors = final[length - n_anc:length - 1] + [last]
        kinds = types[length - n_anc:length - 1] + [last_high]
        if any(x == y for x, y in zip(kinds, kinds[1:])) or not _gaps_ok(anchors):
            continue
        start = anchors[0]
        if not (SPAN_MIN <= i - start + 1 <= SPAN_MAX) or not s.clean(start, i):
            continue
        H = [a for a, k in zip(anchors, kinds) if k]
        L = [a for a, k in zip(anchors, kinds) if not k]
        up = _fit(H, [float(s.h[x]) for x in H])
        dn = _fit(L, [float(s.l[x]) for x in L])
        if max(up[3], dn[3]) > TOL * A or up[2] - dn[2] <= 0:
            continue
        w0 = float(_lv(up, start) - _lv(dn, start))
        w1 = float(_lv(up, last) - _lv(dn, last))
        if w0 <= 0 or w1 < HEIGHT_ATR * A or w1 < BROAD["widen"] * w0:
            continue
        js = np.arange(start, i)
        cc = s.c[start:i]
        if np.any(cc > _lv(up, js) + BUF * A) or np.any(cc < _lv(dn, js) - BUF * A):
            continue
        mid = (up[2] + dn[2]) / 2 * (last - start) / A
        slope_tag = "ascending" if mid > BROAD["slope_tag_atr"] else "descending" if mid < -BROAD["slope_tag_atr"] else "flat"
        ctx_tag = _trend_tag(s.trend(start))
        geo = dict(anchors=anchors, anchors_available=[a + R for a in anchors], upper_anchors=H, lower_anchors=L,
                   upper_line=list(up[:3]), lower_line=list(dn[:3]), upper_rms_atr=up[3] / A,
                   lower_rms_atr=dn[3] / A, width_start_atr=w0 / A, width_end_atr=w1 / A,
                   slope=slope_tag, prior_trend=s.trend(start), context=ctx_tag, atr_frozen=A)
        _track(ctx, v, group="CH22", start=start, anchor_end=last, detected=i, A=A,
               setup_keys=[(k_up, "neutral"), (k_dn, "neutral")],
               exits=[(1, up, k_up, "bullish", None), (-1, dn, k_dn, "bearish", None)],
               geometry=geo, score=_clip01(1 - (up[3] + dn[3]) / (2 * TOL * A)),
               tags=[f"slope:{slope_tag}", f"context:{ctx_tag}"])


# ============================================================================ CH23
def _diamond(ctx):
    s = ctx.s
    v = _View(s, False)
    k_up, k_dn = _key("CH23", "upside", "long"), _key("CH23", "downside", "short")
    final, types, trace = ctx.zigzag()
    for i, length, last, last_high in trace:
        if length < 6:
            continue
        A = _atr_ok(s, i)
        if A is None:
            continue
        tol = TOL * A
        anchors = final[length - 6:length - 1] + [last]
        kinds = types[length - 6:length - 1] + [last_high]
        if any(x == y for x, y in zip(kinds, kinds[1:])) or not _gaps_ok(anchors):
            continue
        start = anchors[0]
        if not (SPAN_MIN <= i - start + 1 <= SPAN_MAX) or not s.clean(start, i):
            continue
        H = [a for a, k in zip(anchors, kinds) if k]
        L = [a for a, k in zip(anchors, kinds) if not k]
        h1, h2, h3 = (float(s.h[x]) for x in H)
        l1, l2, l3 = (float(s.l[x]) for x in L)
        if not (h2 - h1 > tol and h2 - h3 > tol and l1 - l2 > tol and l3 - l2 > tol):
            continue
        if h2 - l2 < HEIGHT_ATR * A:
            continue
        ul, ur = _fit(H[:2], [h1, h2]), _fit(H[1:], [h2, h3])
        ll, lr = _fit(L[:2], [l1, l2]), _fit(L[1:], [l2, l3])
        if ur[2] - lr[2] >= 0:
            continue
        apex = ur[0] + ((lr[1] - lr[2] * (lr[0] - ur[0])) - ur[1]) / (ur[2] - lr[2])
        if apex <= i:
            continue
        js = np.arange(start, i)
        upper = np.where(js <= H[1], _lv(ul, js), _lv(ur, js))
        lower = np.where(js <= L[1], _lv(ll, js), _lv(lr, js))
        cc = s.c[start:i]
        if np.any(cc > upper + BUF * A) or np.any(cc < lower - BUF * A):
            continue
        prior = s.trend(start)
        up_ctx = "bottom" if prior == "down" else "continuation" if prior == "up" else "undetermined"
        dn_ctx = "top" if prior == "up" else "continuation" if prior == "down" else "undetermined"
        left_d, right_d = max(H[1], L[1]) - start, last - min(H[1], L[1])
        geo = dict(anchors=anchors, anchors_available=[a + R for a in anchors], highs=H, lows=L,
                   upper_right_line=list(ur[:3]), lower_right_line=list(lr[:3]),
                   widest_width_atr=(h2 - l2) / A, apex_index=apex, prior_trend=prior, atr_frozen=A)
        _track(ctx, v, group="CH23", start=start, anchor_end=last, detected=i, A=A,
               setup_keys=[(k_up, "neutral"), (k_dn, "neutral")],
               exits=[(1, ur, k_up, "bullish", {"context": up_ctx}), (-1, lr, k_dn, "bearish", {"context": dn_ctx})],
               geometry=geo, score=_clip01(1 - abs(left_d - right_d) / max(left_d + right_d, 1)),
               stop_at=int(math.ceil(apex)))


# ============================================================================ CH24
def _island(ctx, mirror):
    """View orientation = island top (gap up, isolated bars, gap down).  Mirror = bottom."""
    s = ctx.s
    v = _View(s, mirror)
    side, kind = ("long", "bottom") if mirror else ("short", "top")
    k_single, k_multi = _key("CH24", f"{kind}_single", side), _key("CH24", f"{kind}_multi", side)
    n = s.n
    if n < 3:
        return
    same = s.seg_id[1:] == s.seg_id[:-1]
    cand = np.flatnonzero(same & (v.h[1:] < v.l[:-1])) + 1
    for j in cand:
        j = int(j)
        A = _atr_ok(s, j)
        if A is None:
            continue
        buf = BUF * A
        run_min = math.inf
        found = None
        for a in range(j - 1, max(j - ISLAND["max_bars"], int(s.seg_start[j]) + 1) - 1, -1):
            run_min = min(run_min, float(v.l[a]))
            if run_min - float(v.h[j]) <= buf:
                break
            if run_min - float(v.h[a - 1]) > buf:
                found = a
                break
        if found is None:
            continue
        a = found
        start = a - 1
        if not s.clean(start, j) or v.trend(start) != "up":
            continue
        gap1 = run_min - float(v.h[a - 1])
        gap2 = run_min - float(v.h[j])
        bars = j - a
        key = k_single if bars == 1 else k_multi
        geo = dict(first_gap_bar=a, second_gap_bar=j, island_bars=bars, island_extreme_edge=v.real(run_min),
                   first_gap_atr=gap1 / A, second_gap_atr=gap2 / A, prior_trend=s.trend(start),
                   gap_measure="full-range OHLC gap (never the quality gap flag)")
        _confirmed_only(ctx, key, group=("CH24", kind), start=start, anchor_end=j, j=j, direction=v.bear,
                        geometry=geo, score=_clip01(min(gap1, gap2) / A))


# ============================================================================ CH25
def _vshape(ctx, mirror):
    s = ctx.s
    v = _View(s, mirror)
    key = _key("CH25", "inverted_v_top" if mirror else "v_bottom", "short" if mirror else "long")
    P = VSHAPE
    for p in v.lp:
        p = int(p)
        i = p + R
        A = _atr_ok(s, i)
        if A is None:
            continue
        lo = p - P["decline_max"]
        if lo < s.seg_start[p]:
            continue
        a = lo + int(np.argmax(v.h[lo:p - P["decline_min"] + 1]))
        if float(v.h[a:p + 1].max()) > v.h[a]:
            continue
        decline = float(v.h[a] - v.l[p])
        bars = p - a
        if decline < P["decline_atr"] * A or decline / bars < P["steep_atr"] * A:
            continue
        eff = -_eff(v.c, a, p)
        if eff < P["eff"]:
            continue
        near = int(np.count_nonzero(v.l[p - R:p + R + 1] <= v.l[p] + P["sharp_frac"] * decline))
        if near > P["sharp_max"] or not s.clean(a, i):
            continue
        level = float(v.l[p]) + P["retrace"] * decline
        if np.any(v.c[p:i] > level + BUF * A):
            continue
        geo = dict(decline_start=a, extreme=p, extreme_available=i, decline_atr=decline / A, decline_bars=bars,
                   decline_efficiency=eff, bars_near_extreme=near, retracement_level=v.real(level),
                   failure_level=v.real(v.l[p]), atr_frozen=A)
        _track(ctx, v, group=key, start=a, anchor_end=p, detected=i, A=A, setup_keys=[(key, v.bull)],
               exits=[(1, (0.0, level, 0.0), key, v.bull, None), (-1, (0.0, float(v.l[p]), 0.0), None, None, None)],
               geometry=geo, score=_clip01(eff))


# ============================================================================ CH26
def _measured(ctx, mirror):
    s = ctx.s
    v = _View(s, mirror)
    key = _key("CH26", "bear" if mirror else "bull", "short" if mirror else "long")
    final, types, trace = ctx.zigzag()
    P = MEASURED
    for i, length, last, last_high in trace:
        if length < 3 or (last_high != mirror):     # last anchor must be a view low
            continue
        A = _atr_ok(s, i)
        if A is None:
            continue
        a0, b, c = final[length - 3], final[length - 2], last
        kinds = [types[length - 3], types[length - 2], last_high]
        if mirror:
            kinds = [not k for k in kinds]
        if kinds != [False, True, False] or not _gaps_ok([a0, b, c]):
            continue
        if not (SPAN_MIN <= i - a0 + 1 <= SPAN_MAX) or not s.clean(a0, i):
            continue
        leg = float(v.h[b] - v.l[a0])
        eff = _eff(v.c, a0, b)
        if leg < P["leg_atr"] * A or eff < P["leg_eff"]:
            continue
        retr = float(v.h[b] - v.l[c]) / leg
        if not (P["retr"][0] <= retr <= P["retr"][1]) or c - b > P["corr_mult"] * (b - a0):
            continue
        if float(v.h[b:i + 1].max()) > v.h[b] or float(v.l[a0:b + 1].min()) < v.l[a0] or float(v.l[b:i + 1].min()) < v.l[c]:
            continue
        if np.any(v.c[b:i] > v.h[b] + BUF * A):
            continue
        expiry = int(min(P["expiry_cap"], max(EXPIRY, b - a0)))
        geo = dict(leg_start=a0, leg_end=b, correction_end=c, anchors_available=[a0 + R, b + R, c + R],
                   leg_atr=leg / A, leg_efficiency=eff, retracement=retr, breakout_level=v.real(v.h[b]),
                   failure_level=v.real(v.l[c]), projection_target=v.real(v.l[c] + leg), expiry_bars=expiry,
                   atr_frozen=A)
        _track(ctx, v, group=key, start=a0, anchor_end=c, detected=i, A=A, setup_keys=[(key, v.bull)],
               exits=[(1, (0.0, float(v.h[b]), 0.0), key, v.bull, None), (-1, (0.0, float(v.l[c]), 0.0), None, None, None)],
               geometry=geo, score=_clip01(1 - abs(retr - 0.5) / 0.25), expiry=expiry)


# ============================================================================ CH27
def _bump(ctx, mirror):
    """View orientation = bump-and-run top (real CH27 top short; mirror = bottom long)."""
    s = ctx.s
    v = _View(s, mirror)
    key = _key("CH27", "bottom" if mirror else "top", "long" if mirror else "short")
    P = BUMP
    lp = v.lp
    for p in v.hp:
        p = int(p)
        i = p + R
        A = _atr_ok(s, i)
        if A is None:
            continue
        lo = max(i - SPAN_MAX + 1, int(s.seg_start[i]))
        lows = lp[(lp >= max(lo, p - P["bump_search"])) & (lp <= p - SEP)]
        L2 = None
        for x in lows[::-1]:
            if float(v.l[x]) <= float(v.l[x:p + 1].min()):
                L2 = int(x)
                break
        if L2 is None:
            continue
        bump_h = float(v.h[p])
        found = None
        for L1 in lp[(lp >= lo) & (lp <= L2 - P["lead_min"])]:
            L1 = int(L1)
            m = float(v.l[L2] - v.l[L1]) / (L2 - L1)
            if not (P["slope"][0] * A <= m <= P["slope"][1] * A):
                continue
            bump_slope = (bump_h - float(v.l[L2])) / (p - L2)
            if bump_slope < P["mult"] * m:
                continue
            line = (float(L1), float(v.l[L1]), m)
            js = np.arange(L1, i + 1)
            base = _lv(line, js)
            if np.any(v.l[L1:L2 + 1] < base[:L2 - L1 + 1] - TOL * A):
                continue
            if np.any(v.c[L1:i] < base[:-1] - BUF * A):
                continue
            lead_h = float((v.h[L1:L2 + 1] - base[:L2 - L1 + 1]).max())
            bump_height = bump_h - float(_lv(line, p))
            if bump_height < max(HEIGHT_ATR * A, P["height_mult"] * lead_h):
                continue
            if i - L1 + 1 < SPAN_MIN or not s.clean(L1, i):
                continue
            found = (L1, line, m, bump_slope, lead_h, bump_height)
            break
        if found is None:
            continue
        L1, line, m, bump_slope, lead_h, bump_height = found
        expiry = int(min(P["expiry_cap"], max(EXPIRY, p - L2)))
        geo = dict(lead_in_lows=[L1, L2], bump_peak=p, anchors_available=[L1 + R, L2 + R, p + R],
                   lead_in_slope_atr_per_bar=m / A, bump_slope_atr_per_bar=bump_slope / A,
                   slope_multiple=bump_slope / m, lead_in_height_atr=lead_h / A, bump_height_atr=bump_height / A,
                   trendline=[line[0], v.real(line[1]), -m if mirror else m], failure_level=v.real(bump_h),
                   expiry_bars=expiry, atr_frozen=A)
        _track(ctx, v, group=key, start=L1, anchor_end=p, detected=i, A=A, setup_keys=[(key, v.bear)],
               exits=[(-1, line, key, v.bear, None), (1, (0.0, bump_h, 0.0), None, None, None)],
               geometry=geo, score=_clip01(bump_slope / m / (2 * P["mult"])), expiry=expiry)


# ============================================================================ CH28
def _vcp(ctx, mirror):
    s = ctx.s
    v = _View(s, mirror)
    side = "short" if mirror else "long"
    base_key = _key("CH28", "bearish" if mirror else "bullish", side)
    vol_key = _key("CH28", ("bearish" if mirror else "bullish") + "_volume_contraction", side)
    final, types, trace = ctx.zigzag()
    for i, length, last, last_high in trace:
        if length < 6 or last_high != mirror:
            continue
        A = _atr_ok(s, i)
        if A is None:
            continue
        anchors = final[length - 6:length - 1] + [last]
        kinds = types[length - 6:length - 1] + [last_high]
        if mirror:
            kinds = [not k for k in kinds]
        if kinds != [True, False, True, False, True, False] or not _gaps_ok(anchors):
            continue
        start = anchors[0]
        if not (SPAN_MIN <= i - start + 1 <= SPAN_MAX) or not s.clean(start, i):
            continue
        H, L = anchors[0::2], anchors[1::2]
        d = [float(v.h[a] - v.l[b]) for a, b in zip(H, L)]
        if d[0] < HEIGHT_ATR * A or d[1] > VCP["contraction"] * d[0] or d[2] > VCP["contraction"] * d[1]:
            continue
        if max(float(v.h[H[1]]), float(v.h[H[2]])) > float(v.h[H[0]]) + TOL * A:
            continue
        ranges = [float(np.mean(s.h[a:b + 1] - s.l[a:b + 1])) for a, b in zip(H, L)]
        if not (ranges[0] > ranges[1] > ranges[2]):
            continue
        base_high = float(v.h[start:i + 1].max())
        if np.any(v.c[start:i] > base_high + BUF * A):
            continue
        vols = [float(np.mean(s.v[a:b + 1])) for a, b in zip(H, L)]
        contracting = vols[0] > vols[1] > vols[2]
        geo = dict(pullback_highs=H, pullback_lows=L, anchors_available=[a + R for a in anchors],
                   pullback_depths_atr=[x / A for x in d], mean_ranges_atr=[x / A for x in ranges],
                   pullback_mean_volume=vols, volume_contracting=contracting, base_boundary=v.real(base_high),
                   failure_level=v.real(v.l[last]), prior_trend=s.trend(start), atr_frozen=A)
        keys = [(base_key, v.bull)] + ([(vol_key, v.bull)] if contracting else [])
        score = _clip01(1 - d[2] / d[0])
        if not ctx.claim(("CH28", mirror), start, last):
            continue
        for key, direction in keys:
            _track(ctx, v, group=None, start=start, anchor_end=last, detected=i, A=A, setup_keys=[(key, direction)],
                   exits=[(1, (0.0, base_high, 0.0), key, direction, None),
                          (-1, (0.0, float(v.l[last]), 0.0), None, None, None)],
                   geometry=geo, score=score)


# ==================================================================== specifications
_DEFS = {
    ("CH11", "upside"): ("Ascending triangle - upside", ["setup", "confirmed"],
        "Flat resistance: region = latest suffix whose highs stay <= latest high pivot + 0.5 ATR; level = region max high; "
        ">=2 high-pivot touches within 0.5 ATR of level, >=5 bars apart; formation starts at the first touch. Rising support: "
        "troughs = lowest low between consecutive touches plus, when the trigger pivot is a low after the last touch, that "
        "pivot (must be the lowest low since the last touch); >=2 troughs, each > previous + 0.5 ATR; least-squares line "
        "slope > 0, RMS <= 0.5 ATR when >=3 troughs. Height resistance - support(start) >= 2 ATR; apex (line meets "
        "resistance) after detection; closes before detection within [support - 0.12 ATR, resistance + 0.12 ATR]. Detection "
        "at availability of the final anchor. Confirm: close > resistance + 0.12 ATR. Failure boundary: close < support line "
        "- 0.12 ATR (recorded as CH11/downside_resolution). Exception: tracking also ends at the apex bar. "
        "Score = 1 - (resistance spread + support RMS)/(2*0.5 ATR)."),
    ("CH11", "downside_resolution"): ("Ascending triangle - downside resolution", ["confirmed"],
        "Same setup as CH11/upside (no separate setup event in this key). Confirmed at the first close < rising support line "
        "- 0.12 ATR(detection) within detection..detection+10 bars and before the apex, if no upside confirmation came first. "
        "Episode and detected_index are those of the CH11/upside setup."),
    ("CH12", "support_breakdown"): ("Horizontal breakdown", ["setup", "confirmed"],
        "Mirror of legacy CH02 on support. Region = latest suffix whose lows stay >= latest low pivot - 0.5 ATR; level = "
        "region min low; >=3 low-pivot touches within 0.5 ATR of level, >=5 bars apart (CH02-mirror exception: 3 touches); "
        "trigger pivot is the latest touch; base height = max high since first touch - level >= 2 ATR; no flat resistance "
        "required. Confirm: close < level - 0.12 ATR. Failure: close > base high + 0.12 ATR. Score = 1 - touch spread/0.5 ATR."),
    ("CH13", "upside"): ("Rectangle - upside break", ["setup", "confirmed"],
        "Resistance and support clusters as in CH11/CH12 over the common region (both flat by the 0.5 ATR band); >=2 "
        "touches per side, >=5 bars apart; all consecutive anchors >=5 bars apart and >=2 side alternations; the trigger "
        "pivot is the latest touch of its side; height >= 2 ATR; closes contained within +-0.12 ATR. Setup is NEUTRAL and "
        "emitted in both CH13 keys; alias_group horizontal_range. Confirm upside: close > resistance + 0.12 ATR; the "
        "opposite break is the failure boundary (and CH13/downside confirmation). Score = 1 - mean touch spread/0.5 ATR."),
    ("CH13", "downside"): ("Rectangle - downside break", ["setup", "confirmed"],
        "Same neutral rectangle setup as CH13/upside. Confirm downside: close < support - 0.12 ATR within 10 bars; an "
        "upside break first terminates it."),
    ("CH14", "bull"): ("Bull pennant", ["setup", "confirmed"],
        "Zigzag anchors. Pole top b = view-high zigzag anchor that is the highest high b..detection; pole start a = argmin "
        "low in [b-20, b-5]; pole height high[b]-low[a] >= 5 ATR, close path efficiency >= 0.72 (legacy flag gates). "
        "Pennant = alternating anchors from b, >=2 highs and >=2 lows, last anchor = trigger; exceptions: anchors >=3 bars "
        "apart, pennant duration 6-25 bars, window a..detection 15-50 bars. Upper LS line slope < 0, lower line slope > upper "
        "slope (converging, non-parallel); RMS <= 0.5 ATR for >=3-point lines; start width <= 0.5 pole; end width <= 0.7 "
        "start width; apex after detection; lowest low >= top - 0.5 pole; closes inside lines +-0.12 ATR. Confirm: close > "
        "upper line + 0.12 ATR; failure: close < lower line - 0.12 ATR; tracking ends at apex. Score = 0.5*efficiency + "
        "0.5*(1 - mean RMS/0.5 ATR)."),
    ("CH14", "bear"): ("Bear pennant", ["setup", "confirmed"], "Exact price mirror of CH14/bull (negated view)."),
    ("CH15", "canonical"): ("Double top", ["setup", "confirmed"],
        "Mirror of CH16 (negated view): two high pivots within 0.5 ATR, >=10 bars apart, no higher high between, intervening "
        "trough low = lowest low between (>=5 bars from each peak), peak - trough >= 2 ATR, prior trend at first peak = up "
        "(Series.trend; None = no event), window 20-150 bars, no close below trough - 0.12 ATR before detection. Latest "
        "qualifying earlier peak is paired. Confirm: close < trough - 0.12 ATR; failure: close > higher peak + 0.12 ATR. "
        "Adam/Eve subtype (geometry+quality_tags only, no separate variants): a peak is adam if <=2 of bars t-3..t+3 have "
        "high within 0.25 ATR of the peak, else eve. alias_group double_top. Score = 1 - peak difference/0.5 ATR."),
    ("CH16", "canonical"): ("Double bottom", ["setup", "confirmed"],
        "Two low pivots q<p within 0.5 ATR, p-q >= 10, no lower low between, intervening peak = highest high between (>=5 "
        "bars from each trough), neckline - lower trough >= 2 ATR, prior trend at q = down (Series.trend; None = no event), "
        "window q..detection 20-150, no close above neckline + 0.12 ATR before detection; detection at p+3; latest "
        "qualifying q. Confirm: close > neckline + 0.12 ATR; failure: close < lower trough - 0.12 ATR. Adam/Eve subtype "
        "in geometry/quality_tags (adam: <=2 of bars t-3..t+3 within 0.25 ATR of the trough low). alias_group double_bottom."),
    ("CH17", "canonical"): ("Triple top", ["setup", "confirmed"],
        "Mirror of CH18 (negated view). alias_group double_top only when the first two peaks were an emitted CH15 "
        "episode (earlier events untouched)."),
    ("CH18", "canonical"): ("Triple bottom", ["setup", "confirmed"],
        "Three low pivots t1<t2<t3, consecutive >=10 bars apart, max-min of the three lows <= 0.5 ATR, no lower low in "
        "t1..t3, intervening peaks = highest high between each pair (>=5 bars from troughs); resistance = higher "
        "intervening peak; lower intervening peak - lowest trough >= 2 ATR; prior trend at t1 = down; window 20-150; no "
        "close above resistance + 0.12 ATR before detection; latest qualifying t2 then t1. Confirm: close > resistance + "
        "0.12 ATR; failure: close < lowest trough - 0.12 ATR. alias_group double_bottom when (t1,t2) was an emitted CH16 "
        "episode. Score = 1 - trough range/0.5 ATR."),
    ("CH19", "canonical"): ("Rounding bottom / saucer", ["confirmed"],
        "No setup state (exception). Left rim L = high pivot; evaluated once, at the first close j >= L+4 above high[L] + "
        "0.12 ATR(j) in L's segment within 150 bars (the rim is consumed even if the bowl fails). Gates at j: window L..j "
        "20-150; highs between <= rim + 0.5 ATR; bowl low position (argmin low) in [0.25, 0.75] of L..j; depth >= 2 ATR; "
        "quadratic fit of closes (x in [-1,1]): curvature > 0, R^2 >= 0.78, MSE <= 0.9 x V-shape fit MSE; closes within "
        "the bottom 28% of depth make up 18-60% of bars (flat middle); no handle: largest close pullback after the low <= "
        "33% of depth. Rim alignment is the crossing itself. Score = R^2."),
    ("CH20", "canonical"): ("Rounding top / dome", ["confirmed"], "Exact price mirror of CH19 (first close below a low-pivot rim)."),
    ("CH21", "canonical"): ("Inverted cup & handle", ["setup", "confirmed"],
        "Legacy cups() gates on the negated view: rim low pivots left/right 24+ bars apart (window left..detection <= 150), "
        "dome extreme in the central half, depth > max(3 ATR, 4% of rim) and < 40% of rim, rims align within 22% of depth, "
        "prior decline close[left-20] - low[left] >= 0.5 depth, quadratic R^2 >= 0.78, MSE <= 0.9 V fit, 18-60% floor "
        "fraction; handle = high pivot >=5 bars after the right rim, highest high since the rim, retrace 7-40% of depth, "
        "handle length <= 45% of cup width; detection at handle pivot +3; best R^2 left rim. Legacy handle volume gate "
        "REMOVED (ratio recorded). Confirm: close < lower rim - 0.12 ATR; failure: close > handle high + 0.12 ATR. Score = R^2 x (1-alignment)."),
    ("CH22", "upside"): ("Broadening formation - upside", ["setup", "confirmed"],
        "Last 5 alternating zigzag anchors (trigger = last), >=5 bars apart, window 20-150. LS lines through high and low "
        "anchors, RMS <= 0.5 ATR, upper slope > lower slope, width at last anchor >= 2 ATR and >= 1.3 x width at first "
        "anchor; closes inside lines +-0.12 ATR. Tags: slope ascending/descending/flat (mid-slope x span > +-1 ATR), context "
        "top/bottom/neutral/unknown from Series.trend(start). Neutral setup in both keys. Confirm: close > upper line + 0.12 "
        "ATR; the lower-line break terminates. Score = 1 - mean RMS/0.5 ATR."),
    ("CH22", "downside"): ("Broadening formation - downside", ["setup", "confirmed"], "Same as CH22/upside; confirm close < lower line - 0.12 ATR."),
    ("CH23", "upside"): ("Diamond - upside", ["setup", "confirmed"],
        "Last 6 alternating zigzag anchors (3 highs h1..h3, 3 lows l1..l3), >=5 bars apart, window 20-150; h2 exceeds h1 "
        "and h3 by > 0.5 ATR, l2 below l1 and l3 by > 0.5 ATR (distinct widest middle), h2 - l2 >= 2 ATR; closes inside the "
        "piecewise left/right boundaries +-0.12 ATR; right lines (h2-h3, l2-l3) converge with apex after detection. "
        "Neutral setup in both keys. Confirm: close > right upper line + 0.12 ATR; tracking ends at apex. Context on "
        "confirmation: prior down -> bottom, prior up -> continuation, else undetermined. Score = duration symmetry."),
    ("CH23", "downside"): ("Diamond - downside", ["setup", "confirmed"],
        "Same as CH23/upside; confirm close < right lower line - 0.12 ATR; context prior up -> top, prior down -> continuation."),
    ("CH24", "top_single"): ("Island reversal top - single bar", ["confirmed"],
        "Exceptions: no window/height/touch/expiry gates, confirmed only. Full-range OHLC gaps only (the gap flag is a "
        "quality flag and must be absent on the whole window). At bar j with high[j] < low[j-1]: walk back a = j-1.. "
        "(max 30 island bars); island low = min low a..j-1; stop when island low - high[j] <= 0.12 ATR(j); first (latest) a "
        "with island low - high[a-1] > 0.12 ATR is the island. Prior trend at a-1 = up. Confirmed at j (second gap). "
        "Single = one island bar. Score = min(gap sizes)/1 ATR capped at 1."),
    ("CH24", "top_multi"): ("Island reversal top - multi bar", ["confirmed"], "CH24/top_single with 2-30 island bars."),
    ("CH24", "bottom_single"): ("Island reversal bottom - single bar", ["confirmed"], "Price mirror of CH24/top_single (prior trend down)."),
    ("CH24", "bottom_multi"): ("Island reversal bottom - multi bar", ["confirmed"], "Price mirror of CH24/top_multi (prior trend down)."),
    ("CH25", "v_bottom"): ("V bottom", ["setup", "confirmed"],
        "Exception: window decline-start..detection 9-19 bars. Extreme = low pivot p (setup at p+3); decline start a = "
        "argmax high in [p-15, p-5] and highest high a..p; decline >= 4 ATR and >= 0.4 ATR/bar; close efficiency >= 0.6; "
        "sharp: <=3 of bars p-3..p+3 have low within 10% of decline of the extreme (excludes bowls). Level = extreme + 50% of "
        "the decline (from the causal pivot extreme); no close above level + 0.12 ATR before detection. Confirm: close > "
        "level + 0.12 ATR within 10 bars; failure: close < extreme - 0.12 ATR. Score = decline efficiency."),
    ("CH25", "inverted_v_top"): ("Inverted V top", ["setup", "confirmed"], "Exact price mirror of CH25/v_bottom."),
    ("CH26", "bull"): ("Measured move - bullish", ["setup", "confirmed"],
        "Last 3 zigzag anchors low A, high B, low C (trigger C), >=5 bars apart, window A..detection 20-150. Leg B-A >= 3 "
        "ATR with close efficiency >= 0.5; correction retracement 25-75% of the leg; correction bars <= 2 x leg bars; B is "
        "the highest high since B, C the lowest low since B; no close above B + 0.12 ATR before detection. Confirm "
        "(second-leg breakout): close > high[B] + 0.12 ATR; failure: close < low[C] - 0.12 ATR. Exception: expiry = "
        "min(40, max(10, leg bars)). Equal-leg projection low[C]+leg is a reference only. Score = 1 - |retr-0.5|/0.25."),
    ("CH26", "bear"): ("Measured move - bearish", ["setup", "confirmed"], "Exact price mirror of CH26/bull."),
    ("CH27", "top"): ("Bump-and-run reversal top", ["setup", "confirmed"],
        "Trigger = bump peak high pivot P. L2 = latest low pivot in [P-60, P-5] that is the lowest low L2..P. Lead-in "
        "trendline through low pivots L1->L2 (earliest qualifying L1, L2-L1 >= 10) with slope 0.05-0.6 ATR/bar; lows L1..L2 "
        ">= line - 0.5 ATR; closes L1..detection-1 >= line - 0.12 ATR. Bump slope (high[P]-low[L2])/(P-L2) >= 2 x lead-in "
        "slope; bump height above line at P >= max(2 ATR, 2 x max lead-in high above line). Window 20-150. Confirm: close < "
        "trendline - 0.12 ATR; failure: close > bump high + 0.12 ATR. Exception: expiry = min(40, max(10, P-L2)). "
        "Score = min(1, slope multiple/4)."),
    ("CH27", "bottom"): ("Bump-and-run reversal bottom", ["setup", "confirmed"], "Exact price mirror of CH27/top (proposed mirror)."),
    ("CH28", "bullish"): ("Volatility-contraction base - bullish", ["setup", "confirmed"],
        "Last 6 zigzag anchors H1 L1 H2 L2 H3 L3 (trigger L3), >=5 bars apart, window 20-150. Pullback depths d_k = "
        "high[Hk]-low[Lk]: d1 >= 2 ATR, d2 <= 0.8 d1, d3 <= 0.8 d2; H2,H3 <= H1 + 0.5 ATR; mean bar range over each "
        "pullback strictly decreasing; base boundary = max high H1..detection; no close above boundary + 0.12 ATR before "
        "detection. Volume NOT gated (pullback mean volumes recorded). Confirm: close > boundary + 0.12 ATR; failure: close "
        "< low[L3] - 0.12 ATR. Research geometric contraction model, not a named discretionary system. Score = 1 - d3/d1."),
    ("CH28", "bearish"): ("Volatility-contraction base - bearish", ["setup", "confirmed"], "Exact price mirror of CH28/bullish."),
    ("CH28", "bullish_volume_contraction"): ("Volatility-contraction base - bullish, volume contraction", ["setup", "confirmed"],
        "Separately named volume variant: CH28/bullish plus mean volume strictly decreasing across the three pullbacks. "
        "Same episode as CH28/bullish."),
    ("CH28", "bearish_volume_contraction"): ("Volatility-contraction base - bearish, volume contraction", ["setup", "confirmed"],
        "Mirror of CH28/bullish_volume_contraction."),
}
_SIDES = {("CH11", "upside"): "long", ("CH11", "downside_resolution"): "short", ("CH12", "support_breakdown"): "short",
          ("CH13", "upside"): "long", ("CH13", "downside"): "short", ("CH14", "bull"): "long", ("CH14", "bear"): "short",
          ("CH15", "canonical"): "short", ("CH16", "canonical"): "long", ("CH17", "canonical"): "short",
          ("CH18", "canonical"): "long", ("CH19", "canonical"): "long", ("CH20", "canonical"): "short",
          ("CH21", "canonical"): "short", ("CH22", "upside"): "long", ("CH22", "downside"): "short",
          ("CH23", "upside"): "long", ("CH23", "downside"): "short", ("CH24", "top_single"): "short",
          ("CH24", "top_multi"): "short", ("CH24", "bottom_single"): "long", ("CH24", "bottom_multi"): "long",
          ("CH25", "v_bottom"): "long", ("CH25", "inverted_v_top"): "short", ("CH26", "bull"): "long",
          ("CH26", "bear"): "short", ("CH27", "top"): "short", ("CH27", "bottom"): "long",
          ("CH28", "bullish"): "long", ("CH28", "bearish"): "short", ("CH28", "bullish_volume_contraction"): "long",
          ("CH28", "bearish_volume_contraction"): "short"}


def specifications() -> list[dict]:
    out = []
    for (pid, variant), (name, states, text) in _DEFS.items():
        out.append(common.spec(pid, variant, _SIDES[(pid, variant)], name, FAMILY, states, LOOKBACK,
                               COMMON_TEXT + text))
    return out


# ============================================================================ detect
def detect(bars: list[dict], timeframe: str) -> dict:
    specs = specifications()
    s = common.prepare(bars, timeframe)
    ctx = _Ctx(s, specs)
    if s.n <= common.ATR_PERIOD:        # no prior ATR can exist; any longer prefix must be evaluated (causality)
        return ctx.col.result()
    _ch11(ctx)
    _ch12(ctx)
    _ch13(ctx)
    for mirror in (False, True):
        _ch14(ctx, mirror)
        _double(ctx, mirror)          # before triples (alias linking uses earlier emitted doubles)
        _triple(ctx, mirror)
        _rounding(ctx, mirror)
        _island(ctx, mirror)
        _vshape(ctx, mirror)
        _measured(ctx, mirror)
        _bump(ctx, mirror)
        _vcp(ctx, mirror)
    _inverted_cup(ctx)
    _broadening(ctx)
    _diamond(ctx)
    return ctx.col.result()
