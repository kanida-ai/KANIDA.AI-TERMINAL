"""All 61 canonical TA-Lib candlestick recognitions (research; Claude Code owned).

Canonical means the pinned TA-Lib release, its default candle settings (restored
before every run) and pinned penetration parameters.  Library functions are run
separately on every gap-free valid segment, so no recognition, candle-average
window or confirmation spans a data-quality gap.  TA-Lib candle functions are
causal (verified: end truncation and future perturbation leave earlier outputs
unchanged).

Variants per function (see ``_cells``):
- ``canonical``          library recognition; side = library sign convention.
- ``canonical_context``  plus prior-trend rule (common.Series.trend at formation start):
                         reversal long needs ``down``, reversal short needs ``up``;
                         continuation long needs ``up``, continuation short ``down``.
- ``color_direction``    colour-sign momentum shapes; explicitly descriptive direction.
- ``break_up`` / ``break_down``  neutral shapes: setup direction ``neutral``; direction
                         only from the confirming close.  Four-price doji excluded.
Confirmation (unless intrinsic): within 3 bars, close beyond the pattern candles'
high (long) / low (short) by 0.12 x ATR(setup bar), same segment.
Intrinsic: CDL3INSIDE/CDL3OUTSIDE emit ``confirmed`` at the third candle only;
CDLHIKKAKE/CDLHIKKAKEMOD use the library's own +-200 confirmation.
"""
from __future__ import annotations

import numpy as np
import talib
import talib.abstract as _abstract
from talib import _ta_lib

from . import common as C

EXPECTED_TALIB_VERSION = "0.6.8"
TALIB_VERSION = talib.__version__
ALL_CANDLE_SETTINGS = 11  # TA_AllCandleSettings
PENETRATION = {"CDLABANDONEDBABY": 0.3, "CDLDARKCLOUDCOVER": 0.5, "CDLEVENINGDOJISTAR": 0.3,
               "CDLEVENINGSTAR": 0.3, "CDLMATHOLD": 0.5, "CDLMORNINGDOJISTAR": 0.3, "CDLMORNINGSTAR": 0.3}
# TA-Lib C defaults (ta_global.c): setting -> (range type, average period, factor).
CANDLE_SETTINGS = {
    "BodyLong": ("RealBody", 10, 1.0), "BodyVeryLong": ("RealBody", 10, 3.0),
    "BodyShort": ("RealBody", 10, 1.0), "BodyDoji": ("HighLow", 10, 0.1),
    "ShadowLong": ("RealBody", 0, 1.0), "ShadowVeryLong": ("RealBody", 0, 2.0),
    "ShadowShort": ("Shadows", 10, 1.0), "ShadowVeryShort": ("HighLow", 10, 0.1),
    "Near": ("HighLow", 5, 0.2), "Far": ("HighLow", 5, 0.6), "Equal": ("HighLow", 5, 0.05),
}

REV, CONT, DIR = "reversal", "continuation", "direction_only"
# id, name, pattern candles (incl. referenced prior bars), kind, interpretation, library sides, alias group
TABLE = [
    ("CDLDOJI", "Doji", 1, "neutral", None, "+", "doji"),
    ("CDLLONGLEGGEDDOJI", "Long-legged doji", 1, "neutral", None, "+", "doji"),
    ("CDLDRAGONFLYDOJI", "Dragonfly doji", 1, "context", REV, "+", "doji"),
    ("CDLGRAVESTONEDOJI", "Gravestone doji", 1, "context", REV, "-", "doji"),
    ("CDLRICKSHAWMAN", "Rickshaw man", 1, "neutral", None, "+", "doji"),
    ("CDLTAKURI", "Takuri", 1, "context", REV, "+", "doji"),
    ("CDLSPINNINGTOP", "Spinning top", 1, "neutral", None, "+-", "small_body"),
    ("CDLHIGHWAVE", "High-wave candle", 1, "neutral", None, "+-", "small_body"),
    ("CDLSHORTLINE", "Short-line candle", 1, "neutral", None, "+-", "small_body"),
    ("CDLLONGLINE", "Long-line candle", 1, "color", None, "+-", "long_body"),
    ("CDLMARUBOZU", "Marubozu", 1, "color", None, "+-", "long_body"),
    ("CDLCLOSINGMARUBOZU", "Closing marubozu", 1, "color", None, "+-", "long_body"),
    ("CDLBELTHOLD", "Belt hold", 1, "directional", REV, "+-", "long_body"),
    ("CDLHAMMER", "Hammer", 2, "directional", REV, "+", "lower_shadow_rejection"),
    ("CDLHANGINGMAN", "Hanging man", 2, "directional", REV, "-", "lower_shadow_rejection"),
    ("CDLINVERTEDHAMMER", "Inverted hammer", 2, "directional", REV, "+", "upper_shadow_rejection"),
    ("CDLSHOOTINGSTAR", "Shooting star", 2, "directional", REV, "-", "upper_shadow_rejection"),
    ("CDLENGULFING", "Engulfing", 2, "directional", REV, "+-", "engulfing"),
    ("CDLHARAMI", "Harami", 2, "directional", REV, "+-", "harami"),
    ("CDLHARAMICROSS", "Harami cross", 2, "directional", REV, "+-", "harami"),
    ("CDLPIERCING", "Piercing", 2, "directional", REV, "+", "piercing_dark_cloud"),
    ("CDLDARKCLOUDCOVER", "Dark cloud cover", 2, "directional", REV, "-", "piercing_dark_cloud"),
    ("CDLDOJISTAR", "Doji star", 2, "directional", REV, "+-", "star"),
    ("CDLCOUNTERATTACK", "Counterattack", 2, "directional", REV, "+-", "counterattack"),
    ("CDLHOMINGPIGEON", "Homing pigeon", 2, "directional", REV, "+", "harami"),
    ("CDLMATCHINGLOW", "Matching low", 2, "directional", REV, "+", "matching_low"),
    ("CDLKICKING", "Kicking", 2, "directional", DIR, "+-", "kicking"),
    ("CDLKICKINGBYLENGTH", "Kicking by length", 2, "directional", DIR, "+-", "kicking"),
    ("CDLSEPARATINGLINES", "Separating lines", 2, "directional", CONT, "+-", "separating_lines"),
    ("CDLONNECK", "On-neck", 2, "directional", CONT, "-", "neck"),
    ("CDLINNECK", "In-neck", 2, "directional", CONT, "-", "neck"),
    ("CDLTHRUSTING", "Thrusting", 2, "directional", CONT, "-", "neck"),
    ("CDLMORNINGSTAR", "Morning star", 3, "directional", REV, "+", "star"),
    ("CDLEVENINGSTAR", "Evening star", 3, "directional", REV, "-", "star"),
    ("CDLMORNINGDOJISTAR", "Morning doji star", 3, "directional", REV, "+", "star"),
    ("CDLEVENINGDOJISTAR", "Evening doji star", 3, "directional", REV, "-", "star"),
    ("CDLABANDONEDBABY", "Abandoned baby", 3, "directional", REV, "+-", "star"),
    ("CDL3WHITESOLDIERS", "Three white soldiers", 3, "directional", REV, "+", "three_soldiers_crows"),
    ("CDL3BLACKCROWS", "Three black crows", 4, "directional", REV, "-", "three_soldiers_crows"),
    ("CDLIDENTICAL3CROWS", "Identical three crows", 3, "directional", REV, "-", "three_soldiers_crows"),
    ("CDL3INSIDE", "Three inside up/down", 3, "intrinsic", REV, "+-", "harami"),
    ("CDL3OUTSIDE", "Three outside up/down", 3, "intrinsic", REV, "+-", "engulfing"),
    ("CDL2CROWS", "Two crows", 3, "directional", REV, "-", "two_crows"),
    ("CDLUPSIDEGAP2CROWS", "Upside-gap two crows", 3, "directional", REV, "-", "two_crows"),
    ("CDLADVANCEBLOCK", "Advance block", 3, "directional", REV, "-", "weakening_advance"),
    ("CDLSTALLEDPATTERN", "Stalled pattern", 3, "directional", REV, "-", "weakening_advance"),
    ("CDL3STARSINSOUTH", "Three stars in the south", 3, "directional", REV, "+", "three_stars_in_south"),
    ("CDLUNIQUE3RIVER", "Unique three-river bottom", 3, "directional", REV, "+", "unique_three_river"),
    ("CDLTRISTAR", "Tristar", 3, "directional", REV, "+-", "doji"),
    ("CDLSTICKSANDWICH", "Stick sandwich", 3, "directional", REV, "+", "stick_sandwich"),
    ("CDLGAPSIDESIDEWHITE", "Gap side-by-side white lines", 3, "directional", CONT, "+-", "gap_continuation"),
    ("CDLTASUKIGAP", "Tasuki gap", 3, "directional", CONT, "+-", "gap_continuation"),
    ("CDLXSIDEGAP3METHODS", "Gap three methods", 3, "directional", CONT, "+-", "gap_continuation"),
    ("CDL3LINESTRIKE", "Three-line strike", 4, "directional", CONT, "+-", "three_line_strike"),
    ("CDLRISEFALL3METHODS", "Rising/falling three methods", 5, "directional", CONT, "+-", "three_methods"),
    ("CDLMATHOLD", "Mat hold", 5, "directional", CONT, "+", "three_methods"),
    ("CDLBREAKAWAY", "Breakaway", 5, "directional", REV, "+-", "breakaway"),
    ("CDLLADDERBOTTOM", "Ladder bottom", 5, "directional", REV, "+", "ladder_bottom"),
    ("CDLCONCEALBABYSWALL", "Concealing baby swallow", 4, "directional", REV, "+", "concealing_baby_swallow"),
    ("CDLHIKKAKE", "Hikkake", 3, "hikkake", DIR, "+-", "inside_bar_trap"),
    ("CDLHIKKAKEMOD", "Modified hikkake", 4, "hikkake", DIR, "+-", "inside_bar_trap"),
]
_ROWS = {r[0]: r for r in TABLE}


def _restore_default_candle_settings():
    for setting in range(ALL_CANDLE_SETTINGS + 1):
        try:
            _ta_lib._ta_restore_candle_default_settings(setting)
        except Exception:  # pragma: no cover - older enum layouts
            pass


# Lookbacks depend on candle-setting averaging periods: restore defaults FIRST so a
# caller that altered TA-Lib settings before import cannot contaminate the cache.
_restore_default_candle_settings()
_LIB_LOOKBACK = {}
for _fid in _ROWS:
    _fn = _abstract.Function(_fid)
    if _fid in PENETRATION:
        _fn.parameters = {"penetration": PENETRATION[_fid]}
    _LIB_LOOKBACK[_fid] = int(_fn.lookback)


def _side_of(sign: int) -> str:
    return "long" if sign > 0 else "short"


def _cells(row):
    """[(variant, side, states, mode)] for one TA-Lib function."""
    fid, _name, _candles, kind, interp, sides, _alias = row
    signs = [s for s in (1, -1) if ("+" if s > 0 else "-") in sides]
    out = []
    if kind in ("directional", "intrinsic", "hikkake"):
        states = ["confirmed"] if kind == "intrinsic" else ["setup", "confirmed"]
        for sg in signs:
            out.append(("canonical", _side_of(sg), states, "library"))
        if interp in (REV, CONT):
            for sg in signs:
                out.append(("canonical_context", _side_of(sg), states, "context"))
    elif kind == "context":
        for sg in signs:
            out.append(("canonical_context", _side_of(sg), ["setup", "confirmed"], "context"))
    elif kind == "color":
        for sg in (1, -1):
            out.append(("color_direction", _side_of(sg), ["setup", "confirmed"], "color"))
    if kind in ("neutral", "context", "color"):
        out.append(("break_up", "long", ["setup", "confirmed"], "break"))
        out.append(("break_down", "short", ["setup", "confirmed"], "break"))
    return out


def _definition(row, variant, side, mode) -> str:
    fid, name, candles, kind, interp, _sides, _alias = row
    pen = f" penetration={PENETRATION[fid]};" if fid in PENETRATION else ""
    base = (f"TA-Lib {EXPECTED_TALIB_VERSION} {fid} with default candle settings restored before each run;{pen} "
            f"computed independently on each gap-free valid segment; pattern candles={candles} "
            f"(formation_start = signal_index-{candles - 1}); requires prior ATR(20) at the signal bar.")
    want = "bullish" if side == "long" else "bearish"
    if mode in ("library", "context"):
        if kind == "context":
            what = f" Setup: nonzero library output (shape recognition, no library direction) treated as a {want} {interp} study only with context."
        else:
            what = f" Setup: library output {'> 0' if side == 'long' else '< 0'} (library {want} convention); |80| edge-equality grades kept as library_value and quality tag."
        if mode == "context":
            need = ("down" if side == "long" else "up") if interp == REV else ("up" if side == "long" else "down")
            what += (f" Context: least-squares fit of the 20 closes before formation_start must change "
                     f"{'<= -1' if need == 'down' else '>= +1'} ATR(formation_start) ('{need}' trend, {interp}).")
        else:
            what += " No trend context check (the library does not test trend)."
    elif mode == "color":
        what = (f" Setup: library output {'> 0 (up candle)' if side == 'long' else '< 0 (down candle)'}; "
                "direction follows candle colour — explicitly a descriptive momentum study, not a reversal claim.")
    else:
        what = (" Setup: nonzero library output; neutral shape (setup direction 'neutral'); four-price doji "
                f"(O=H=L=C) excluded. Direction only from the confirming {'upside' if side == 'long' else 'downside'} close.")
    if kind == "intrinsic":
        conf = " State: 'confirmed' at the third candle (intrinsic confirmation); no extra candle is added."
    elif kind == "hikkake":
        conf = (" Setup: library +-100 at the last pattern candle. Confirmed: library +-200 (close beyond the "
                "inside-bar high/low within 3 bars), linked to the most recent setup within the prior 3 bars.")
    else:
        conf = (f" Confirmed: first close within the next {C.CANDLE_CONFIRM_WINDOW} bars "
                f"{'above max(high)' if side == 'long' else 'below min(low)'} of the pattern candles "
                f"{'+' if side == 'long' else '-'} {C.BREAKOUT_BUFFER_ATR} x ATR(setup bar), no segment break.")
    return base + what + conf


def _lookback(row, mode) -> int:
    fid, _n, candles = row[0], row[1], row[2]
    lb = max(_LIB_LOOKBACK[fid], C.ATR_PERIOD)
    if mode == "context":
        lb = max(lb, C.TREND_LOOKBACK + candles - 1)
    return lb + 1


def specifications() -> list[dict]:
    specs = []
    for row in TABLE:
        for variant, side, states, mode in _cells(row):
            specs.append(C.spec(row[0], variant, side, row[1], "candlestick", states, _lookback(row, mode),
                                _definition(row, variant, side, mode), talib_version=EXPECTED_TALIB_VERSION,
                                penetration=PENETRATION.get(row[0]), interpretation=row[4] or "neutral",
                                alias_group=row[6]))
    return specs


# ----------------------------------------------------------------------------- run
def library_outputs(s: C.Series) -> dict:
    """{fid: int array}; zeros where the library could not evaluate inside a segment."""
    for setting in range(ALL_CANDLE_SETTINGS + 1):
        try:
            _ta_lib._ta_restore_candle_default_settings(setting)
        except Exception:  # pragma: no cover - older enum layouts
            pass
    out = {fid: np.zeros(s.n, dtype=np.int64) for fid in _ROWS}
    if s.n == 0:
        return out
    bounds = np.flatnonzero(np.r_[True, s.seg_id[1:] != s.seg_id[:-1]])
    ends = np.r_[bounds[1:], s.n]
    for a, b in zip(bounds, ends):
        if not s.valid[a]:
            continue
        o, h, l, c = (np.ascontiguousarray(x[a:b]) for x in (s.o, s.h, s.l, s.c))
        for fid in _ROWS:
            if b - a <= _LIB_LOOKBACK[fid]:
                continue
            fn = getattr(talib, fid)
            res = fn(o, h, l, c, **({"penetration": PENETRATION[fid]} if fid in PENETRATION else {}))
            out[fid][a:b] = res.astype(np.int64)
    return out


def _volume_ratio(s: C.Series) -> np.ndarray:
    ratio = np.full(s.n, np.nan)
    if s.n <= C.ATR_PERIOD:
        return ratio
    med = np.median(np.lib.stride_tricks.sliding_window_view(s.v, C.ATR_PERIOD)[:-1], axis=1)
    i = np.arange(C.ATR_PERIOD, s.n)
    ok = (i - C.ATR_PERIOD >= s.seg_start[i]) & (med > 0)
    ratio[i] = np.where(ok, s.v[i] / np.where(med > 0, med, 1.0), np.nan)
    return ratio


def _context_ok(trend, side, interp) -> bool:
    if trend is None:
        return False
    if interp == REV:
        return trend == ("down" if side == "long" else "up")
    return trend == ("up" if side == "long" else "down")


def detect(bars: list[dict], timeframe: str) -> dict:
    specs = specifications()
    col = C.empty_result(specs)
    s = C.prepare(bars, timeframe)
    if s.n == 0:
        return col.result()
    raw = library_outputs(s)
    vol = _volume_ratio(s)
    four_price = (s.o == s.h) & (s.h == s.l) & (s.l == s.c)
    for row in TABLE:
        fid, _name, candles, kind, interp, _sides, alias = row
        out = raw[fid]
        cells = _cells(row)
        setups = {}  # hikkake: setup bar -> episode, for library-confirmation linkage
        for i in np.flatnonzero(out):
            i = int(i)
            val = int(out[i])
            if kind == "hikkake" and abs(val) == 200:
                _hikkake_confirm(s, col, row, cells, out, setups, i, val)
                continue
            start = i - candles + 1
            atr = s.atr_at(i)
            if start < s.seg_start[i] or atr is None:
                continue
            sign = 1 if val > 0 else -1
            hi, lo = float(s.h[start:i + 1].max()), float(s.l[start:i + 1].min())
            trend = None
            geom = {"high": hi, "low": lo, "color": int(np.sign(s.c[i] - s.o[i])),
                    "volume_ratio": None if not np.isfinite(vol[i]) else float(vol[i])}
            tags = []
            if abs(val) == 80:
                tags.append("edge_equality_grade")
            if four_price[i]:
                tags.append("four_price_doji")
            confirm_cache = {}
            for variant, side, states, mode in cells:
                if mode in ("library", "context", "color"):
                    # 'context' shapes emit a constant recognition code; their side is the
                    # conventional interpretation in TABLE, never the library sign.
                    if kind != "context" and _side_of(sign) != side:
                        continue
                    if mode == "context":
                        if trend is None:
                            trend = s.trend(start)
                        if not _context_ok(trend, side, interp):
                            continue
                    direction = "bullish" if side == "long" else "bearish"
                else:  # break
                    if four_price[i]:
                        continue
                    direction = "neutral"
                g = dict(geom, prior_trend=s.trend(start) if trend is None else trend)
                key = (fid, variant, side)
                episode = start
                if kind == "hikkake":
                    setups[i] = episode
                if "setup" in states:
                    col.add(key, C.make_event(s, signal_index=i, episode=episode, state="setup",
                                              direction=direction, formation_start=start, detected_index=i,
                                              geometry=g, alias_group=alias, library_value=val, quality_tags=tags))
                if kind == "intrinsic":
                    col.add(key, C.make_event(s, signal_index=i, episode=episode, state="confirmed",
                                              direction=direction, formation_start=start, detected_index=i,
                                              confirmed_index=i, geometry=g, alias_group=alias,
                                              library_value=val, quality_tags=tags + ["intrinsic_confirmation"]))
                    continue
                if kind == "hikkake":
                    continue
                bullish = side == "long"
                if bullish not in confirm_cache:
                    confirm_cache[bullish] = C.confirm_close_beyond(s, i, hi, lo, bullish, atr=atr)
                j = confirm_cache[bullish]
                if j is not None:
                    level = hi + C.BREAKOUT_BUFFER_ATR * atr if bullish else lo - C.BREAKOUT_BUFFER_ATR * atr
                    col.add(key, C.make_event(s, signal_index=j, episode=episode, state="confirmed",
                                              direction="bullish" if bullish else "bearish",
                                              formation_start=start, detected_index=i, confirmed_index=j,
                                              geometry=dict(g, confirm_level=level, setup_atr=atr),
                                              alias_group=alias, library_value=val, quality_tags=tags))
    return col.result()


def _hikkake_confirm(s, col, row, cells, out, setups, j, val):
    fid, _name, candles, _kind, _interp, _sides, alias = row
    sign = 1 if val > 0 else -1
    k = next((x for x in range(j - 1, max(j - 4, -1), -1) if abs(int(out[x])) == 100), None)
    if k is None or int(np.sign(out[k])) != sign or k not in setups:
        return
    side = _side_of(sign)
    start = k - candles + 1
    for variant, cell_side, _states, _mode in cells:
        if cell_side != side:
            continue
        col.add((fid, variant, side), C.make_event(
            s, signal_index=j, episode=setups[k], state="confirmed",
            direction="bullish" if sign > 0 else "bearish", formation_start=start, detected_index=k,
            confirmed_index=j, geometry={"high": float(s.h[start:k + 1].max()), "low": float(s.l[start:k + 1].min()),
                                         "setup_index": k}, alias_group=alias, library_value=int(val),
            quality_tags=["library_confirmation"]))
