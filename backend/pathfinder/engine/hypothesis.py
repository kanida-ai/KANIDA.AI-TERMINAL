"""
The canonical hypothesis — ported from `Kanida_Falcon/scripts/agent_arena.py`.

    WHEN [trigger] happens to [scope] under [context],
    WHAT is [outcome] over [horizon], vs [baseline], after [costs]?

Two properties matter more than the primitives themselves:

1. **The library is CLOSED.** The LLM may only compose a hypothesis out of the
   primitives below, with parameters inside the Constitution-approved ranges. It
   cannot invent a feature, and it cannot invent a threshold. `HypothesisSpec.parse`
   is where a model's structured output becomes a testable object, and where an
   out-of-range parameter is rejected rather than clamped.
2. **Every primitive is evaluated at the CLOSE of the signal bar t**, from bars ≤ t
   only. `replay.py` then enters at the open of t+1. There is no primitive here that
   can see tomorrow; that is a property of the code, not of the caller's discipline.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Literal

import numpy as np
import pandas as pd

from .market import PriceFrames

Direction = Literal["long", "short"]


# ── trigger primitives ───────────────────────────────────────────────────────
# Each returns a (date × symbol) boolean frame, TRUE on the signal bar t.

def _gap_up(f: PriceFrames, *, thr_pct: float) -> pd.DataFrame:
    return (f.o / f.c.shift(1) - 1) * 100 >= thr_pct


def _gap_down(f: PriceFrames, *, thr_pct: float) -> pd.DataFrame:
    return (f.o / f.c.shift(1) - 1) * 100 <= -thr_pct


def _breakout(f: PriceFrames, *, lookback: int) -> pd.DataFrame:
    return f.c > f.rolling_max(lookback)


def _breakdown(f: PriceFrames, *, lookback: int) -> pd.DataFrame:
    return f.c < f.rolling_min(lookback)


def _oversold(f: PriceFrames, *, lookback: int, thr_pct: float) -> pd.DataFrame:
    return (f.c / f.c.shift(lookback) - 1) * 100 <= -thr_pct


def _overbought(f: PriceFrames, *, lookback: int, thr_pct: float) -> pd.DataFrame:
    return (f.c / f.c.shift(lookback) - 1) * 100 >= thr_pct


def _streak_up(f: PriceFrames, *, sessions: int) -> pd.DataFrame:
    return (f.c.diff() > 0).rolling(sessions, min_periods=sessions).sum() == sessions


def _streak_down(f: PriceFrames, *, sessions: int) -> pd.DataFrame:
    return (f.c.diff() < 0).rolling(sessions, min_periods=sessions).sum() == sessions


def _volume_dry_up(f: PriceFrames, *, sessions: int, ratio: float) -> pd.DataFrame:
    """`sessions` consecutive closes on below-`ratio`× median volume."""
    quiet = f.vol_ratio(20) <= ratio
    return quiet.rolling(sessions, min_periods=sessions).sum() == sessions


def _pullback_in_uptrend(f: PriceFrames, *, trend_lookback: int, pullback_pct: float) -> pd.DataFrame:
    """Above the trend SMA, but `pullback_pct` off the recent high — the classic dip."""
    above = f.c > f.sma(trend_lookback)
    off_high = (f.c / f.rolling_max(trend_lookback, exclude_today=False) - 1) * 100 <= -pullback_pct
    return above & off_high


def _inside_day(f: PriceFrames, *, sessions: int) -> pd.DataFrame:
    """Range contraction: `sessions` consecutive bars inside the prior bar's range."""
    inside = (f.h <= f.h.shift(1)) & (f.l >= f.l.shift(1))
    return inside.rolling(sessions, min_periods=sessions).sum() == sessions


def _high_range_expansion(f: PriceFrames, *, mult: float) -> pd.DataFrame:
    """Today's true range ≥ `mult` × its own 20-session average."""
    rng = (f.h - f.l) / f.c.shift(1) * 100
    return rng >= mult * rng.rolling(20, min_periods=20).mean()


TRIGGERS: dict[str, Callable[..., pd.DataFrame]] = {
    "gap_up": _gap_up,
    "gap_down": _gap_down,
    "breakout": _breakout,
    "breakdown": _breakdown,
    "oversold": _oversold,
    "overbought": _overbought,
    "streak_up": _streak_up,
    "streak_down": _streak_down,
    "volume_dry_up": _volume_dry_up,
    "pullback_in_uptrend": _pullback_in_uptrend,
    "inside_day": _inside_day,
    "range_expansion": _high_range_expansion,
}

#: Human-readable rule text, used verbatim in the rulebook. A rule DEFINITION may
#: carry numerals (docs/STRATEGY_METHODOLOGY.md §2.1) — a narrative CLAIM may not.
TRIGGER_TEXT: dict[str, str] = {
    "gap_up": "opens at least {thr_pct:g}% above the prior close",
    "gap_down": "opens at least {thr_pct:g}% below the prior close",
    "breakout": "closes above its highest close of the prior {lookback} sessions",
    "breakdown": "closes below its lowest close of the prior {lookback} sessions",
    "oversold": "has fallen {thr_pct:g}% or more over {lookback} sessions",
    "overbought": "has risen {thr_pct:g}% or more over {lookback} sessions",
    "streak_up": "closes higher {sessions} sessions in a row",
    "streak_down": "closes lower {sessions} sessions in a row",
    "volume_dry_up": "trades {sessions} consecutive sessions at or below {ratio:g}x its 20-session median volume",
    "pullback_in_uptrend": (
        "trades above its {trend_lookback}-session average yet at least {pullback_pct:g}% "
        "below its {trend_lookback}-session high"
    ),
    "inside_day": "prints {sessions} consecutive inside bars",
    "range_expansion": "prints a true range at least {mult:g}x its 20-session average",
}


# ── context filters (regime conditioning, evaluated at t) ────────────────────

def _ctx_any(f: PriceFrames) -> pd.Series:
    return pd.Series(True, index=f.dates)


def _ctx_index_above_200dma(f: PriceFrames) -> pd.Series:
    return f.index_above_200dma.reindex(f.dates).fillna(False)


def _ctx_index_below_200dma(f: PriceFrames) -> pd.Series:
    return ~f.index_above_200dma.reindex(f.dates).fillna(True)


def _ctx_high_vol(f: PriceFrames) -> pd.Series:
    from .market import expanding_percentile
    return (expanding_percentile(f.index_realised_vol_20d) >= 0.75).fillna(False)


def _ctx_calm(f: PriceFrames) -> pd.Series:
    from .market import expanding_percentile
    return (expanding_percentile(f.index_realised_vol_20d) <= 0.50).fillna(False)


CONTEXTS: dict[str, Callable[[PriceFrames], pd.Series]] = {
    "any": _ctx_any,
    "index_above_200dma": _ctx_index_above_200dma,
    "index_below_200dma": _ctx_index_below_200dma,
    "high_volatility": _ctx_high_vol,
    "calm": _ctx_calm,
}

CONTEXT_TEXT: dict[str, str] = {
    "any": "in any market",
    "index_above_200dma": "while the index is above its 200-session average",
    "index_below_200dma": "while the index is below its 200-session average",
    "high_volatility": "in the upper quartile of trailing index volatility (ranked point-in-time)",
    "calm": "in the calmer half of trailing index volatility (ranked point-in-time)",
}


class ParameterOutOfRange(ValueError):
    """L2: the agent may tune a threshold ONLY inside its Constitution-approved range."""


@dataclass(frozen=True)
class HypothesisSpec:
    """
    One testable hypothesis. Everything `replay.py` needs and nothing it does not.

    `stop_pct` / `target_pct` are exit RULES, not price targets: they are distances
    from the entry fill, symmetric in direction, and they are what makes the
    historical evidence a *strategy replay* of the thing actually traded rather than
    a hold-to-close statistic (docs/STRATEGY_METHODOLOGY.md §1).
    """
    trigger: str
    params: dict[str, float | int]
    context: str
    direction: Direction
    horizon_sessions: int
    stop_pct: float
    target_pct: float | None = None
    scope: str = "nifty500 liquid at t"
    label: str = ""

    # ── construction from a model's structured output ───────────────────────

    @staticmethod
    def parse(payload: dict[str, Any], *, approved: dict[str, dict[str, Any]]) -> "HypothesisSpec":
        """
        Turn a `pathfinder.reason.v1` proposal into a testable object, or refuse it.

        Refusing is the point. An unknown primitive, an unknown context, or a
        parameter outside its Constitution range raises — the engine never clamps a
        model's number into range and calls it agreement.
        """
        trigger = str(payload["trigger"])
        if trigger not in TRIGGERS:
            raise ParameterOutOfRange(f"unknown trigger primitive: {trigger!r}")
        context = str(payload.get("context", "any"))
        if context not in CONTEXTS:
            raise ParameterOutOfRange(f"unknown context filter: {context!r}")
        direction = str(payload["direction"])
        if direction not in ("long", "short"):
            raise ParameterOutOfRange(f"unknown direction: {direction!r}")

        params = {k: float(v) for k, v in dict(payload.get("params", {})).items()}
        spec_ranges = approved.get(trigger)
        if spec_ranges is None:
            raise ParameterOutOfRange(f"trigger {trigger!r} carries no approved range")
        for name, value in params.items():
            rng = spec_ranges.get(name)
            if rng is None:
                raise ParameterOutOfRange(f"{trigger}.{name} is not an approved parameter")
            lo, hi = float(rng["min"]), float(rng["max"])
            if not (lo <= value <= hi):
                raise ParameterOutOfRange(
                    f"{trigger}.{name}={value:g} is outside the approved range [{lo:g}, {hi:g}]"
                )
        missing = set(spec_ranges) - set(params)
        if missing:
            raise ParameterOutOfRange(f"{trigger} is missing required parameters: {sorted(missing)}")

        for name, value in (("horizon_sessions", payload["horizon_sessions"]),
                            ("stop_pct", payload["stop_pct"]),
                            ("target_pct", payload.get("target_pct"))):
            if value is None:
                continue
            rng = approved["_exits"][name]
            if not (float(rng["min"]) <= float(value) <= float(rng["max"])):
                raise ParameterOutOfRange(
                    f"{name}={float(value):g} is outside the approved range "
                    f"[{rng['min']:g}, {rng['max']:g}]"
                )

        # Integer-valued primitives must stay integral — a 20.5-session lookback is
        # not a thing, and silently rounding one is how a spec drifts from its text.
        int_params = {"lookback", "sessions", "trend_lookback"}
        params = {k: (int(v) if k in int_params else float(v)) for k, v in params.items()}

        return HypothesisSpec(
            trigger=trigger,
            params=params,
            context=context,
            direction=direction,  # type: ignore[arg-type]
            horizon_sessions=int(payload["horizon_sessions"]),
            stop_pct=float(payload["stop_pct"]),
            target_pct=(None if payload.get("target_pct") is None else float(payload["target_pct"])),
        )

    # ── evaluation ──────────────────────────────────────────────────────────

    def signals(self, f: PriceFrames) -> pd.DataFrame:
        """
        The (date × symbol) signal mask at the close of t.

        Composed of: the primitive, the regime context, and the point-in-time
        liquidity filter. Every term uses bars ≤ t.
        """
        raw = TRIGGERS[self.trigger](f, **self.params).fillna(False)
        ctx = CONTEXTS[self.context](f).reindex(raw.index).fillna(False)
        liquid = f.liquid_mask.reindex_like(raw).fillna(False)
        return raw & liquid & np.asarray(ctx)[:, None]

    # ── the human-readable rule (numerals allowed: this is a DEFINITION) ────

    @property
    def entry_text(self) -> str:
        return (
            f"A {self.scope} name that {TRIGGER_TEXT[self.trigger].format(**self.params)}, "
            f"{CONTEXT_TEXT[self.context]}. Fill = next session's open."
        )

    @property
    def exit_text(self) -> str:
        side = "below" if self.direction == "long" else "above"
        parts = [f"hard stop {self.stop_pct:g}% {side} the fill"]
        if self.target_pct is not None:
            parts.append(f"profit exit {self.target_pct:g}% in favour")
        parts.append(f"otherwise exit at the close of session {self.horizon_sessions} after entry")
        return "; ".join(parts) + "."

    @property
    def invalidation_text(self) -> str:
        """Replaces 'target price'. What makes the idea WRONG, not what we hope for."""
        return (
            f"The idea is wrong if the {self.stop_pct:g}% stop is hit before the "
            f"{self.horizon_sessions}-session horizon, or if expectancy net of costs at "
            f"2x the slippage assumption is not positive over the out-of-sample window."
        )

    @property
    def signature(self) -> str:
        """Stable identity, used for the novelty check against past work."""
        ps = ",".join(f"{k}={v:g}" for k, v in sorted(self.params.items()))
        tgt = "none" if self.target_pct is None else f"{self.target_pct:g}"
        return (
            f"{self.trigger}({ps})|{self.context}|{self.direction}"
            f"|h={self.horizon_sessions}|stop={self.stop_pct:g}|target={tgt}"
        )

    @property
    def cassette_key(self) -> str:
        """
        A STABLE, readable address for this hypothesis.

        Experiment ids shift between runs (they are minted from a shared counter), so
        keying recorded completions by experiment id would make a cassette rot the
        moment the scan changes. The hypothesis itself does not move.
        """
        tgt = "none" if self.target_pct is None else f"{self.target_pct:g}"
        return (f"{self.trigger}|{self.context}|{self.direction}"
                f"|h{self.horizon_sessions}|s{self.stop_pct:g}|t{tgt}")

    def to_json(self) -> dict[str, Any]:
        return {
            "trigger": self.trigger, "params": self.params, "context": self.context,
            "direction": self.direction, "horizon_sessions": self.horizon_sessions,
            "stop_pct": self.stop_pct, "target_pct": self.target_pct, "scope": self.scope,
        }
