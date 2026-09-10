"""
The CLOSED hypothesis library of the experiment loop — `finding -> family -> variants`.

Spec principle 3 / addendum 2 hold here exactly as they do for S1's question library: the
model never invents a test. An S1 finding names a FAMILY (the group rule its card computed —
"after a hard one-day fall, does the group bounce?", "does chasing a big jump pay?") and its
founder-authored follow-up question names the RESEARCH: does the answer depend on the day the
signal fired on? The engine evaluates every variant of the family over a closed, ordered set
of CONDITIONS (the whole market's move that day, the regime, breadth) at the S1 horizons, and
COUNTS every one of them as a trial. Nothing outside `CONDITIONS` x `HORIZONS` can be tested,
and the count is published next to the result so the loop cannot become silent p-hacking
(addendum 3).

Every number is measured the way the S1 card measured it and the way the forward book will
trade it: entry at the NEXT OPEN after the signal close, exit at the close of the horizon
session, net of the S1 hurdle (costs + slippage both ways) — `data.py`'s `f{h}`. The evidence
is the strategy that is traded (quant rule), never hold-to-close.

Null calibration (reused from P1 `engine/replay.py`, which reused `Kanida_Falcon`'s NDP idea):
  * `edge vs baseline` — the signal's mean minus the SAME window's eligible pool on the SAME
    kind of day (the rule's conditions) under the SAME exits, so survivorship, drift and the
    day-context cancel (P1 `eligible_mask` restricts the pool to the hypothesis's context);
  * a DAY-BLOCKED placebo — these signals cluster on event days (a market-wide fall fires the
    dip rule on twenty names at once), so the null draws days, not trades;
  * a CLUSTER-ROBUST t on signal days, the t that should be believed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd

from ..research.config import ResearchConfig
from ..research.data import MarketData
from ..research.library import _wmean
from ..research.regime import build_regime
from ..schemas import Decision, Finding

HORIZONS: tuple[int, ...] = (1, 5)          # the S1 horizons; both inside the Constitution's _exits range


# ── conditions: the closed set ───────────────────────────────────────────────

@dataclass(frozen=True)
class Condition:
    id: str
    label: str                  # digit-free, engine text for narratives
    fn: Callable[["Conditioning"], pd.Series]     # date -> bool (causal: uses bars <= that date)


def _c_all(c: "Conditioning") -> pd.Series:
    return pd.Series(True, index=c.mkt.index)


def _c_mkt_down(c: "Conditioning") -> pd.Series:
    return c.mkt < 0


def _c_mkt_up(c: "Conditioning") -> pd.Series:
    return c.mkt >= 0


def _c_mkt_down_1pct(c: "Conditioning") -> pd.Series:
    return c.mkt < -0.01


def _c_risk_on(c: "Conditioning") -> pd.Series:
    return c.regime_state == "RISK_ON"


def _c_neutral(c: "Conditioning") -> pd.Series:
    return c.regime_state == "NEUTRAL"


def _c_risk_off(c: "Conditioning") -> pd.Series:
    return c.regime_state == "RISK_OFF"


def _c_breadth_high(c: "Conditioning") -> pd.Series:
    return c.breadth200 >= 50.0


def _c_breadth_low(c: "Conditioning") -> pd.Series:
    return c.breadth200 < 50.0


CONDITIONS: tuple[Condition, ...] = (
    Condition("all", "on any session", _c_all),
    Condition("market_down", "on a session the whole market fell", _c_mkt_down),
    Condition("market_up", "on a session the whole market rose", _c_mkt_up),
    Condition("market_down_1pct", "on a session the whole market fell by more than a percent", _c_mkt_down_1pct),
    Condition("regime_risk_on", "in a risk-on regime", _c_risk_on),
    Condition("regime_neutral", "in a neutral regime", _c_neutral),
    Condition("regime_risk_off", "in a risk-off regime", _c_risk_off),
    Condition("breadth_high", "when at least half the market was above its long average", _c_breadth_high),
    Condition("breadth_low", "when fewer than half the market's names were above their long average", _c_breadth_low),
)
CONDITION_BY_ID: dict[str, Condition] = {c.id: c for c in CONDITIONS}

#: Logical structure of the set, so a revision can never be a no-op or an empty set: a
#: condition IMPLIED by one the rule already carries adds nothing (a "revision" that changes
#: nothing would be a free trial); one EXCLUSIVE with it selects nothing. Neither is a candidate.
IMPLIES: dict[str, frozenset[str]] = {"market_down_1pct": frozenset({"market_down"})}
_REGIMES = ("regime_risk_on", "regime_neutral", "regime_risk_off")
_BREADTH = ("breadth_high", "breadth_low")
EXCLUSIVE: dict[str, frozenset[str]] = {
    "market_down": frozenset({"market_up"}), "market_down_1pct": frozenset({"market_up"}),
    "market_up": frozenset({"market_down", "market_down_1pct"}),
    **{r: frozenset(set(_REGIMES) - {r}) for r in _REGIMES},
    **{b: frozenset(set(_BREADTH) - {b}) for b in _BREADTH},
}


def candidate_conditions(current: tuple[str, ...]) -> list[str]:
    """Conditions a revision may ADD to `current`: not `all`, not already carried, not implied, not exclusive."""
    carried = set(current)
    implied = set().union(*(IMPLIES.get(c, frozenset()) for c in carried))
    excluded = set().union(*(EXCLUSIVE.get(c, frozenset()) for c in carried))
    return [c.id for c in CONDITIONS if c.id != "all" and c.id not in carried and c.id not in implied and c.id not in excluded]


class Conditioning:
    """Per-date, causal context for a sealed frame: the market's move, the regime, breadth."""

    def __init__(self, md: MarketData) -> None:
        self.md = md
        self.mkt = md.df.groupby("d")["ret"].mean()                  # equal-weight universe move, close to close
        reg = build_regime(md)
        self.regime_state = reg["state"].reindex(self.mkt.index).astype(object)
        self.breadth200 = reg["breadth200"].reindex(self.mkt.index).astype(float)
        self._cache: dict[str, pd.Series] = {}
        self._liq: Optional[pd.Series] = None

    @property
    def liquidity(self) -> pd.Series:
        """Rolling median traded value per row, as of each session (causal). Computed once per seal."""
        if self._liq is None:
            df = self.md.df
            tv = (df["close"] * df["volume"]).astype(float)
            self._liq = tv.groupby(df["symbol"], sort=False).transform(
                lambda s: s.rolling(20, min_periods=20).median())
        return self._liq

    def by_date(self, cond_id: str) -> pd.Series:
        if cond_id not in self._cache:
            s = CONDITION_BY_ID[cond_id].fn(self).reindex(self.mkt.index)
            self._cache[cond_id] = s.fillna(False).astype(bool)
        return self._cache[cond_id]

    def row_mask(self, cond_ids: tuple[str, ...]) -> pd.Series:
        """Conjunction of conditions, aligned to the long frame's rows."""
        m = pd.Series(True, index=self.md.df.index)
        for cid in cond_ids:
            m &= self.md.df["d"].map(self.by_date(cid)).fillna(False).astype(bool)
        return m


# ── families: which S1 cards may open which rule ──────────────────────────────

@dataclass(frozen=True)
class Family:
    id: str
    template_id: str
    decisions: frozenset[str]           # S1 decisions that name this family
    direction: str                      # long | short
    label: str                          # digit-free theme, public-safe (no names, no instructions)
    question: str                       # the S1 follow-up the research answers
    base_mask: Callable[[pd.DataFrame, float], pd.Series]


def _dip_mask(df: pd.DataFrame, thr_pct: float) -> pd.Series:
    return df["ret"] <= -abs(thr_pct) / 100.0


def _surge_mask(df: pd.DataFrame, thr_pct: float) -> pd.Series:
    return df["ret"] >= abs(thr_pct) / 100.0


FAMILIES: tuple[Family, ...] = (
    Family("dip_bounce", "dip", frozenset({"no_trade", "virtual_long"}), "long",
           "the bounce after a hard one-day fall, across the whole market",
           "Does the bounce depend on whether the whole market fell that day?", _dip_mask),
    Family("surge_fade", "surge", frozenset({"reject", "no_trade"}), "short",
           "fading the day's biggest one-day jump, across the whole market",
           "Does chasing or fading a big jump depend on the kind of day it came on?", _surge_mask),
    Family("surge_chase", "surge", frozenset({"virtual_long"}), "long",
           "riding the day's biggest one-day jump, across the whole market",
           "Does the continuation depend on the kind of day it came on?", _surge_mask),
)
FAMILY_BY_ID: dict[str, Family] = {f.id: f for f in FAMILIES}


def family_for(finding: Finding) -> Optional[Family]:
    """The family an S1 finding names, or None when the card carries no rule S2 may test."""
    if finding.continues is not None:
        return None
    for f in FAMILIES:
        if f.template_id == finding.template_id and finding.decision.value in f.decisions:
            return f
    return None


def not_derivable_reason(finding: Finding) -> str:
    """Why a finding opens nothing — recorded on the candidate row, never silent."""
    if finding.continues is not None:
        return "a continuation of an open claim, not a new observation"
    t, d = finding.template_id, finding.decision.value
    if t in ("dip", "surge"):
        return f"no hypothesis family for decision {d} on template {t}"
    if t == "volume_anomaly":
        return ("the anomaly names no direction — a lift without a side is a question, not a rule; "
                "no closed family exists for it yet (founder input)")
    if t == "relationship":
        return ("a spread trade carries a multi-session short leg that a retail customer cannot hold in the cash "
                "segment under the stated delivery convention (governance.check_implementable); no family")
    if t in ("theme_cycle", "market_regime"):
        return f"no closed hypothesis family for template {t} in this build (founder input) — decision {d}"
    return f"no closed hypothesis family for template {t}"


def threshold_of(finding: Finding, family: Family) -> float:
    """The card's own group threshold (a frozen parameter fact), never re-derived from live config."""
    for f in finding.facts:
        if f.id.endswith("_threshold") and f.unit.value == "pct":
            return float(f.value)
    raise ValueError(f"{finding.id}: the card carries no threshold fact; cannot derive {family.id}")


# ── variants ─────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Variant:
    family_id: str
    threshold_pct: float
    conditions: tuple[str, ...]         # conjunction; ("all",) is the unconditioned rule
    horizon: int

    @property
    def family(self) -> Family:
        return FAMILY_BY_ID[self.family_id]

    @property
    def signature(self) -> str:
        return f"{self.family_id}|{self.threshold_pct:.1f}|{'+'.join(self.conditions)}|h{self.horizon}"

    @property
    def direction(self) -> str:
        return self.family.direction

    @property
    def sign(self) -> float:
        return 1.0 if self.direction == "long" else -1.0

    def rule_text(self) -> str:
        """Engine text for the in-app record. A rule DEFINITION may carry digits; a narrative may not."""
        fam = self.family
        what = ("a stock closed at least {t:g}% below its previous close" if fam.template_id == "dip"
                else "a stock closed at least {t:g}% above its previous close").format(t=abs(self.threshold_pct))
        conds = "; ".join(CONDITION_BY_ID[c].label for c in self.conditions)
        side = "long" if fam.direction == "long" else "short"
        return (f"When {what}, {conds}: a virtual {side} position from the next session's open, closed at the close "
                f"{self.horizon} session{'s' if self.horizon != 1 else ''} later; costs and slippage charged both ways")

    def with_condition(self, cond_id: str) -> "Variant":
        conds = tuple(c for c in self.conditions if c != "all") + (cond_id,)
        return Variant(self.family_id, self.threshold_pct, conds, self.horizon)

    def signals(self, md: MarketData, cx: Conditioning) -> pd.Series:
        df = md.df
        return self.family.base_mask(df, self.threshold_pct) & cx.row_mask(self.conditions)

    def as_dict(self) -> dict[str, Any]:
        return {"family_id": self.family_id, "threshold_pct": self.threshold_pct,
                "conditions": list(self.conditions), "horizon": self.horizon, "direction": self.direction,
                "signature": self.signature, "rule_text": self.rule_text()}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Variant":
        return cls(d["family_id"], float(d["threshold_pct"]), tuple(d["conditions"]), int(d["horizon"]))


def variants_for(family: Family, threshold_pct: float) -> list[Variant]:
    """Every variant the engine may test for a family: CONDITIONS x HORIZONS, in a fixed order."""
    return [Variant(family.id, threshold_pct, (c.id,), h) for h in HORIZONS for c in CONDITIONS]


# ── replay: the strategy that is traded, measured on the seal ────────────────

@dataclass(frozen=True)
class ReplayStats:
    """Everything the gate and the card need, and no interpretation of it."""
    signature: str
    window_start: str
    window_end: str
    n: int
    signal_days: int
    hit_pct: Optional[float]
    median_net: Optional[float]
    expectancy_net: Optional[float]         # winsorised mean net of the hurdle, % per trade
    expectancy_2x_net: Optional[float]      # the same at twice the slippage
    baseline_net: Optional[float]           # the whole pool, same window, same exits, net
    edge_pct: Optional[float]               # signal mean minus pool mean (gross of both)
    baseline_n: int
    placebo_p: Optional[float]              # day-blocked permutation p (floored at 1/draws)
    placebo_draws: int
    cluster_t: Optional[float]
    net_returns: tuple[float, ...] = field(default=(), repr=False)     # per trade, % net
    signal_dates: tuple[str, ...] = field(default=(), repr=False)
    symbols: tuple[str, ...] = field(default=(), repr=False)

    @property
    def empty(self) -> bool:
        return self.n == 0


def cluster_robust_t(r: np.ndarray, days: np.ndarray) -> Optional[float]:
    """CR0 t on the mean with observations clustered on the signal date (P1 replay.cluster_robust_t)."""
    if r.size < 2:
        return None
    mean = float(r.mean())
    resid = r - mean
    uniq = np.unique(days)
    if uniq.size < 2:
        return None
    cs = np.array([resid[days == d].sum() for d in uniq], dtype=float)
    se = float(np.sqrt((cs ** 2).sum())) / r.size
    if not np.isfinite(se) or se <= 0:
        return None
    return mean / se


def block_placebo_means(pool_days: np.ndarray, pool_vals: np.ndarray, day_counts: np.ndarray,
                        *, draws: int, seed: int) -> np.ndarray:
    """
    Day-blocked null (P1 `replay.block_placebo`): draw as many days as the signal used, take from
    each drawn day as many outcomes as the signal took on one of its days, compare the mean.
    `pool_days` must be sorted; `pool_vals` finite.
    """
    if pool_vals.size == 0 or day_counts.size == 0:
        return np.array([], dtype=float)
    day_ids, starts, lengths = np.unique(pool_days, return_index=True, return_counts=True)
    if day_ids.size < 2:
        return np.array([], dtype=float)
    counts = day_counts.astype(np.int64)
    total = int(counts.sum())
    rng = np.random.default_rng(seed)
    means = np.empty(draws, dtype=float)
    for k in range(draws):
        picked = rng.integers(0, day_ids.size, size=counts.size)
        offs = (rng.random(total) * np.repeat(lengths[picked], counts)).astype(np.int64)
        idx = np.repeat(starts[picked], counts) + offs
        means[k] = float(pool_vals[idx].mean())
    return means


def replay(variant: Variant, md: MarketData, cx: Conditioning, *, start: str, end: str,
           rcfg: ResearchConfig, draws: int, seed: int, with_placebo: bool = True) -> ReplayStats:
    """
    Replay `variant` over signal sessions in [start, end] on a frame sealed at or after `end`.
    A signal whose horizon has not resolved inside the seal is NaN and is not a trade (the seal).
    """
    if end > md.as_of:
        raise ValueError(f"replay window ends {end} after the seal {md.as_of} — look-ahead")
    df = md.df
    col = f"f{variant.horizon}"
    inw = (df["d"] >= start) & (df["d"] <= end)
    sig = variant.signals(md, cx) & inw & df[col].notna()
    H = rcfg.hurdle_pct
    rows = df.loc[sig, ["symbol", "d", col]].sort_values(["d", "symbol"], kind="mergesort")
    gross = variant.sign * rows[col].to_numpy(dtype=float) * 100.0
    net = gross - H
    n = int(net.size)
    days = rows["d"].to_numpy()
    # THE POOL (P1 `replay.eligible_mask`, audit finding 10): every resolved stock-session in the
    # same window on the sessions the rule's CONDITIONS hold — the same context, so the baseline and
    # the null carry the same day-context as the signal, and the edge is the signal's excess over
    # its own kind of day rather than over calm days.
    pool = df.loc[inw & df[col].notna() & cx.row_mask(variant.conditions), ["d", col]].sort_values("d", kind="mergesort")
    pool_gross = variant.sign * pool[col].to_numpy(dtype=float) * 100.0
    baseline_n = int(pool_gross.size)
    baseline_net = (_wmean(pool_gross, rcfg.expectancy_winsor_pct) - H) if baseline_n else None
    if n == 0:
        return ReplayStats(variant.signature, start, end, 0, 0, None, None, None, None, baseline_net, None,
                           baseline_n, None, 0, None)
    etv = _wmean(net, rcfg.expectancy_winsor_pct)
    placebo_p: Optional[float] = None
    used = 0
    if with_placebo and baseline_n:
        counts = pd.Series(days).value_counts(sort=False).reindex(pd.unique(days)).to_numpy()
        means = block_placebo_means(pool["d"].to_numpy(), pool_gross, counts, draws=draws, seed=seed)
        if means.size:
            used = int(means.size)
            placebo_p = max(float((means >= gross.mean()).mean()), 1.0 / used)
    return ReplayStats(
        signature=variant.signature, window_start=start, window_end=end, n=n,
        signal_days=int(pd.unique(days).size),
        hit_pct=float((net > 0).mean() * 100.0), median_net=float(np.median(net)),
        expectancy_net=float(etv), expectancy_2x_net=float(etv - 2.0 * rcfg.slippage_pct),
        baseline_net=baseline_net,
        # the edge on the same convention as the expectancy: winsorised mean vs winsorised mean
        edge_pct=(float(etv + H - (baseline_net + H)) if baseline_n else None),
        baseline_n=baseline_n, placebo_p=placebo_p, placebo_draws=used,
        cluster_t=cluster_robust_t(net, days),
        net_returns=tuple(float(x) for x in net), signal_dates=tuple(str(x) for x in days),
        symbols=tuple(str(x) for x in rows["symbol"].to_numpy()),
    )


@dataclass(frozen=True)
class Windows:
    """The three windows a variant is measured on, all inside the seal."""
    whole: tuple[str, str]          # first session -> seal
    discovery: tuple[str, str]      # first session -> discovery_end
    trailing: tuple[str, str]       # first session after discovery_end -> seal


def windows_for(md: MarketData, discovery_end: str) -> Windows:
    s = md.sessions
    first = s[0]
    after = [d for d in s if d > discovery_end]
    trailing_start = after[0] if after else md.as_of
    return Windows(whole=(first, md.as_of), discovery=(first, min(discovery_end, md.as_of)),
                   trailing=(trailing_start, md.as_of))


@dataclass(frozen=True)
class VariantEvidence:
    variant: Variant
    whole: ReplayStats
    discovery: ReplayStats
    trailing: ReplayStats


def measure(variant: Variant, md: MarketData, cx: Conditioning, w: Windows, *, rcfg: ResearchConfig,
            draws: int, seed: int) -> VariantEvidence:
    return VariantEvidence(
        variant=variant,
        whole=replay(variant, md, cx, start=w.whole[0], end=w.whole[1], rcfg=rcfg, draws=draws, seed=seed),
        # The placebo is judged on the whole history (the gate); the two sub-windows carry
        # expectancies only, so their null is not drawn — nothing reads it.
        discovery=replay(variant, md, cx, start=w.discovery[0], end=w.discovery[1], rcfg=rcfg, draws=draws, seed=seed + 1,
                         with_placebo=False),
        trailing=replay(variant, md, cx, start=w.trailing[0], end=w.trailing[1], rcfg=rcfg, draws=draws, seed=seed + 2,
                        with_placebo=False),
    )
