"""
Chart Agent · SETUP-QUALITY score (0-100).  [SPEC-until-OOS]

A single, governed, DOCUMENTED number for "how textbook-clean is this occurrence?", built ONLY from
measurements the detector already computes point-in-time — never a fabricated or hand-tuned constant.

HONESTY LABEL (v3 §5.5 / §14): the weights below are FROZEN and PRINCIPLED (each maps to a distinct,
real property of a clean pattern) but they are NOT yet OOS-calibrated. This score is a readability /
ranking aid for the UI, NOT part of the §9 trade gate (the gate reads strategy-replay ETV, not this).
Every sub-score traces to a real quantity present in the occurrence / geometry:

  structure            r2 of the line fit (sloped: mean of upper/lower r2; cup: base-quadratic r2;
                       horizontal: FLAT-CLEANLINESS = 1 - dispersion(touch prices)/tol, a real proxy
                       for how tightly the touches sit on one flat level).
  touch_quality        number of confirming touches (more distinct touches = a more-tested line).
  breakout_strength    distance_pct of the close PAST the level at the breakout bar (how decisively
                       price cleared the line). Non-positive for an as-yet-unbroken APPROACHING setup
                       (honest: it hasn't broken, so this sub-score is ~0).
  volume_confirmation  volume_x = breakout volume / 20d average (thrust behind the move).
  contraction          for apex patterns (triangles/wedges): how far the two lines have converged from
                       the base to now (tighter coil = higher). NULL for rectangle/channel/horizontal
                       (no apex) — those renormalize over the remaining sub-scores.
  level_quality        a REPORTED composite (0.5*structure + 0.5*touch_quality). It is a diagnostic
                       view only and is NOT double-counted in the weighted total.

FORMULA
  score = 100 * sum_i( w_i * s_i )   over the AVAILABLE primitive sub-scores
          {structure, touch_quality, breakout_strength, volume_confirmation, contraction}
  with frozen base weights (below). When a sub-score is null for a pattern (e.g. contraction for a
  rectangle), its weight is dropped and the remaining weights are RENORMALIZED to sum to 1, so the
  score stays on the same 0-100 scale and no pattern is silently penalised for a property it can't have.
"""
from __future__ import annotations
from typing import Optional
import numpy as np

# --- Frozen governed constants (SPEC-until-OOS Loop-4). Each has a stated meaning; none is fit here.
WEIGHTS = {
    "structure": 0.25,            # a clean line/base is the backbone of the pattern
    "touch_quality": 0.20,        # more confirmed touches = a more-tested level
    "breakout_strength": 0.25,    # a decisive close past the line is the trigger
    "volume_confirmation": 0.20,  # volume thrust corroborates the break
    "contraction": 0.10,          # a tight coil (apex patterns) precedes cleaner expansion
}

# Normalisation anchors — the value at which a sub-score saturates to 1.0 (documented, frozen).
TOUCH_FULL = 4.0        # 4+ distinct touches -> full touch_quality (min_touches=2 -> 0.5)
BREAK_FULL_PCT = 2.0    # a close 2% past the level -> full breakout_strength
VOL_FULL_X = 2.0        # 2.0x the 20d average -> full volume_confirmation (1.0x -> 0)
FLAT_TOL = 0.01         # horizontal touch-dispersion is measured against tol=1% (detector PARAMS.tol)

# Patterns whose two lines converge to an apex (contraction applies); others have no apex.
APEX_PATTERNS = {"ascending_triangle", "descending_triangle", "symmetrical_triangle",
                 "rising_wedge", "falling_wedge"}


def _clip01(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return float(min(1.0, max(0.0, x)))


def compute_quality(pattern_id: str, measurements: dict) -> dict:
    """Compute the 0-100 quality score + sub-scores + weights from REAL measurements.

    ``measurements`` (all optional; missing -> that sub-score is dropped honestly):
        r2                float in [0,1] — line/base fit quality (sloped mean / cup base).
        flatness          float in [0,1] — horizontal flat-cleanliness (used when r2 is None).
        n_touches         int            — confirming touches on the broken line.
        distance_pct      float          — close past the level at the break (percent; may be <=0).
        volume_x          float          — breakout volume / 20d avg.
        contraction       float in [0,1] — apex convergence (apex patterns only; else None).

    Returns {score, subscores, weights, note}. Fully guarded — never raises."""
    try:
        r2 = measurements.get("r2")
        flatness = measurements.get("flatness")
        n_touches = measurements.get("n_touches")
        distance_pct = measurements.get("distance_pct")
        volume_x = measurements.get("volume_x")
        contraction = measurements.get("contraction")

        # structure: r2 when the pattern fits lines/curves; flat-cleanliness for the horizontal level.
        if r2 is not None:
            structure = _clip01(r2)
        elif flatness is not None:
            structure = _clip01(flatness)
        else:
            structure = None

        # touch_quality: normalize touches to [0,1], saturating at TOUCH_FULL.
        touch_quality = _clip01(n_touches / TOUCH_FULL) if n_touches is not None else None

        # breakout_strength: how far past the level (percent). max(0,.) so an unbroken APPROACHING
        # setup (distance <= 0 for a long) honestly scores ~0 here rather than borrowing credit.
        if distance_pct is not None:
            breakout_strength = _clip01(max(0.0, float(distance_pct)) / BREAK_FULL_PCT)
        else:
            breakout_strength = None

        # volume_confirmation: (volume_x - 1)/(VOL_FULL - 1), clipped. 1.0x -> 0, VOL_FULL -> 1.
        if volume_x is not None:
            volume_confirmation = _clip01((float(volume_x) - 1.0) / (VOL_FULL_X - 1.0))
        else:
            volume_confirmation = None

        # contraction only for apex patterns; explicitly None otherwise (documented).
        contraction_s = _clip01(contraction) if (pattern_id in APEX_PATTERNS and contraction is not None) else None

        primitives = {
            "structure": structure,
            "touch_quality": touch_quality,
            "breakout_strength": breakout_strength,
            "volume_confirmation": volume_confirmation,
            "contraction": contraction_s,
        }

        # weighted sum over AVAILABLE primitives, renormalized so the scale stays 0-100.
        avail = {k: v for k, v in primitives.items() if v is not None}
        wsum = sum(WEIGHTS[k] for k in avail) or 1.0
        score = 100.0 * sum(WEIGHTS[k] * v for k, v in avail.items()) / wsum

        # level_quality: reported composite diagnostic (NOT added to the weighted total).
        lq_parts = [p for p in (structure, touch_quality) if p is not None]
        level_quality = round(float(np.mean(lq_parts)), 3) if lq_parts else None

        subscores = {k: (round(v, 3) if v is not None else None) for k, v in primitives.items()}
        subscores["level_quality"] = level_quality

        used_weights = {k: round(WEIGHTS[k] / wsum, 3) for k in avail}
        note = ("SPEC-until-OOS: sub-scores are real detector measurements; weights are frozen and "
                "principled but not yet OOS-calibrated. Ranking/readability aid only — NOT the §9 "
                "trade gate (that reads strategy-replay ETV). level_quality is a reported composite, "
                "not double-counted in the total.")
        return {"score": round(score, 1), "subscores": subscores,
                "weights": used_weights, "note": note}
    except Exception as e:  # noqa: BLE001 — quality must never crash the setup endpoint
        return {"score": None, "subscores": {}, "weights": {}, "note": f"quality unavailable: {e}"}
