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

**The measured strategy is the BOOK's strategy, not "every signal" (re-audit N1).** The book
can take at most `max_new_positions_per_session` names per signal session, ranked by liquidity
descending, at most `max_concurrent_positions` at once and one per symbol. On a day the dip
rule fires on twenty names the book owns the five most liquid; the other fifteen are not
trades it could have made. So the expectation that is FROZEN, and every gate value, comes from
`book_select()` — the book's own selection replayed over history under the same limits — and
the equal-weighted figure over every firing is minted next to it as a labelled CONTEXT fact.
On the real warehouse the two disagree in sign (equal-weighted +0.25% per trade; the book's
own population −0.29%).

Null calibration (reused from P1 `engine/replay.py`, which reused `Kanida_Falcon`'s NDP idea):
  * `edge vs baseline` — the signal's mean minus the SAME window's eligible pool on the SAME
    kind of day (the rule's conditions) under the SAME exits, so survivorship, drift and the
    day-context cancel (P1 `eligible_mask` restricts the pool to the hypothesis's context);
  * a DAY-BLOCKED placebo — these signals cluster on event days (a market-wide fall fires the
    dip rule on twenty names at once), so the null draws days, not trades; the statistic
    compared is the WINSORISED mean on both sides (the convention the expectation is frozen
    on) and the draw count rises to `draws_near_bar` when the p sits within two standard
    errors of the bar, so a verdict near the bar is not a seed's luck (re-audit N8);
  * a CLUSTER-ROBUST t on signal days (CR3 — the small-cluster correction — judged against
    Student's t with G−1 degrees of freedom, G the number of signal days; re-audit N5);
  * CONCENTRATION facts on every window: the share of the window's net P&L that its three
    best signal days contribute, and the expectancy with the best day removed (re-audit N3:
    on the real warehouse three days carried the whole trailing "persistence").
"""
from __future__ import annotations

import math
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
        # a STUDY definition (re-audit N6): what is measured, over which window, on which side — not what to do
        return (f"When {what}, {conds}: the return of a {side} leg measured from the following session's open to the close "
                f"{self.horizon} session{'s' if self.horizon != 1 else ''} later, net of costs and slippage both ways")

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


# ── the book's own selection, replayed over history (re-audit N1) ────────────

@dataclass(frozen=True)
class BookLimits:
    """The Constitution's position limits, as the virtual book applies them (`book.run_book`)."""
    max_new_per_session: int
    max_concurrent: int
    selection_rule: str = "liquidity_desc"      # one per symbol always

    @property
    def label(self) -> str:
        return (f"book-selected: {self.selection_rule}, at most {self.max_new_per_session} new per signal session, "
                f"at most {self.max_concurrent} concurrent, one per symbol")


EQUAL_WEIGHTED = "equal_weighted: every resolved signal, one unit each"


def book_select(rows: pd.DataFrame, *, session_index: dict[str, int], horizon: int, limits: BookLimits) -> np.ndarray:
    """
    Which of the signal `rows` (columns `symbol`, `d`, `_liq`) the book would have taken, walking
    the signal sessions in order exactly as `run_book` does: on each session the positions whose
    exit session is today (or earlier) are closed first; then names are taken in liquidity-
    descending order (ties by symbol) until the session's new-position cap or the concurrency
    cap binds, skipping a symbol already held. A position taken on signal session i occupies a
    slot until session i + horizon. Returns a boolean mask aligned to `rows.index`.

    Point-in-time: only the signal day's own liquidity (a causal rolling median) ranks the day.
    The book's cash check is not modelled here (a fresh book of the same capital opens every
    period, so it binds only inside a losing period); the four limits the re-audit named are.
    """
    take = pd.Series(False, index=rows.index)
    if rows.empty:
        return take.to_numpy()
    ordered = rows.sort_values(["d", "_liq", "symbol"], ascending=[True, False, True], kind="mergesort")
    open_until: dict[str, int] = {}
    for d, g in ordered.groupby("d", sort=True):
        i = session_index[d]
        for s in [s for s, e in open_until.items() if e <= i]:
            del open_until[s]
        opened = 0
        for idx, sym in zip(g.index, g["symbol"].to_numpy()):
            if opened >= limits.max_new_per_session or len(open_until) >= limits.max_concurrent:
                break
            if sym in open_until:
                continue
            open_until[sym] = i + horizon
            opened += 1
            take.at[idx] = True
    return take.to_numpy()


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
    cluster_t: Optional[float]              # CR3 on signal days (re-audit N5)
    net_returns: tuple[float, ...] = field(default=(), repr=False)     # per trade, % net
    signal_dates: tuple[str, ...] = field(default=(), repr=False)
    symbols: tuple[str, ...] = field(default=(), repr=False)
    #: re-audit N1: which population was measured, and how many firings the book could not take
    selection: str = EQUAL_WEIGHTED
    n_fired: int = 0                        # every resolved signal in the window (equal-weighted n)
    n_skipped: int = 0                      # firings the book's limits could not take
    #: re-audit N3: concentration of the window's net P&L on its best signal days
    top3_days_share_pct: Optional[float] = None       # share of total net P&L from the 3 best signal days (total > 0 only)
    best_day: Optional[str] = None
    expectancy_without_best_day_net: Optional[float] = None    # winsorised mean net with the best day's trades removed
    #: re-audit N8: the placebo's own resolution and convention
    placebo_se: Optional[float] = None                 # binomial standard error of placebo_p at placebo_draws
    placebo_convention: str = "winsorised mean vs winsorised draw means"

    @property
    def empty(self) -> bool:
        return self.n == 0


# ── statistics ────────────────────────────────────────────────────────────────

def _wmean_np(x: np.ndarray, winsor_pct: float) -> float:
    """`library._wmean` on a finite ndarray, without the pandas round-trip (same quantile definition)."""
    if x.size == 0:
        return float("nan")
    if winsor_pct <= 0:
        return float(x.mean())
    lo, hi = np.quantile(x, [winsor_pct / 100.0, 1.0 - winsor_pct / 100.0])
    return float(np.clip(x, lo, hi).mean())


def cluster_robust_se(r: np.ndarray, days: np.ndarray) -> Optional[float]:
    """
    CR3 standard error of the mean with observations clustered on the signal date. CR3 rescales
    each cluster's residual sum by 1 / (1 − n_g / n) — the leverage of that cluster in the mean —
    which is the small-cluster correction; with a handful of clusters the plain CR0 estimate is
    biased low and turns noise into a verdict (re-audit N5). CR3 ≥ CR0 always.
    """
    n = r.size
    if n < 2:
        return None
    uniq = np.unique(days)
    if uniq.size < 2:
        return None
    resid = r - float(r.mean())
    acc = 0.0
    for d in uniq:
        m = days == d
        n_g = int(m.sum())
        if n_g >= n:
            return None
        acc += (resid[m].sum() / (1.0 - n_g / n)) ** 2
    se = math.sqrt(acc) / n
    if not np.isfinite(se) or se <= 0:
        return None
    return float(se)


def cluster_robust_t(r: np.ndarray, days: np.ndarray) -> Optional[float]:
    """CR3 t on the mean with observations clustered on the signal date (see `cluster_robust_se`)."""
    se = cluster_robust_se(r, days)
    if se is None:
        return None
    return float(r.mean()) / se


def _betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for the incomplete beta function (Numerical Recipes `betacf`)."""
    MAXIT, EPS, FPMIN = 300, 3e-14, 1e-300
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c, d = 1.0, 1.0 - qab * x / qap
    d = 1.0 / (d if abs(d) >= FPMIN else FPMIN)
    h = d
    for m in range(1, MAXIT + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) >= FPMIN else FPMIN)
        c = 1.0 + aa / c
        c = c if abs(c) >= FPMIN else FPMIN
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) >= FPMIN else FPMIN)
        c = 1.0 + aa / c
        c = c if abs(c) >= FPMIN else FPMIN
        de = d * c
        h *= de
        if abs(de - 1.0) < EPS:
            break
    return h


def _betai(a: float, b: float, x: float) -> float:
    """Regularised incomplete beta I_x(a, b)."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    bt = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1.0 - x))
    if x < (a + 1.0) / (a + b + 2.0):
        return bt * _betacf(a, b, x) / a
    return 1.0 - bt * _betacf(b, a, 1.0 - x) / b


def student_t_two_sided_p(t: float, dof: int) -> float:
    """P(|T| >= |t|) under Student's t with `dof` degrees of freedom (no SciPy dependency)."""
    if dof < 1:
        return float("nan")
    x = dof / (dof + t * t)
    return _betai(dof / 2.0, 0.5, x)


def t_critical(alpha_two_sided: float, dof: int) -> float:
    """The |t| a two-sided test at `alpha` needs with `dof` degrees of freedom (bisection on the CDF)."""
    if dof < 1:
        return float("inf")
    lo, hi = 0.0, 1000.0
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        if student_t_two_sided_p(mid, dof) > alpha_two_sided:
            lo = mid
        else:
            hi = mid
        if hi - lo < 1e-10:
            break
    return 0.5 * (lo + hi)


def concentration(net: np.ndarray, days: np.ndarray, winsor_pct: float
                  ) -> tuple[Optional[float], Optional[str], Optional[float]]:
    """
    (share of total net P&L from the three best signal days in %, the best day, the winsorised
    expectancy with the best day's trades removed). The share is defined only when the window's
    total net P&L is positive — a share of a loss says nothing (re-audit N3).
    """
    if net.size == 0:
        return None, None, None
    s = pd.Series(net).groupby(pd.Series(days)).sum().sort_values(ascending=False, kind="mergesort")
    total = float(net.sum())
    share = float(s.head(3).sum() / total * 100.0) if total > 0 else None
    best = str(s.index[0])
    rest = net[days != best]
    loo = _wmean_np(rest, winsor_pct) if rest.size else None
    return share, best, loo


def block_placebo_means(pool_days: np.ndarray, pool_vals: np.ndarray, day_counts: np.ndarray,
                        *, draws: int, seed: int, winsor_pct: float = 0.0) -> np.ndarray:
    """
    Day-blocked null (P1 `replay.block_placebo`): draw as many days as the signal used, take from
    each drawn day as many outcomes as the signal took on one of its days, and record the draw's
    mean — WINSORISED at `winsor_pct` when asked, the convention the expectation is frozen on
    (re-audit N8). `pool_days` must be sorted; `pool_vals` finite.
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
        means[k] = _wmean_np(pool_vals[idx], winsor_pct)
    return means


def placebo_p_value(means_fn: Callable[[int, int], np.ndarray], stat: float, *, draws: int, draws_near_bar: int,
                    bar: Optional[float], seed: int) -> tuple[Optional[float], int, Optional[float]]:
    """
    p = share of null draws at least as good as `stat`, floored at 1 / draws. `means_fn(draws, seed)`
    produces null means. Resolution (re-audit N8): 1,000 draws put ±0.009 (two binomial standard
    errors) around a p near 0.05 — a verdict at the bar was the seed's luck. When |p − bar| lies
    within two standard errors the null is re-drawn to `draws_near_bar` (a second, independent
    stream appended to the first) and the p re-read. Returns (p, draws used, standard error).
    """
    def _se(q: float, k: int) -> float:
        q = min(max(q, 1.0 / k), 1.0 - 1.0 / k)
        return math.sqrt(q * (1.0 - q) / k)

    first = means_fn(draws, seed)
    if first.size == 0:
        return None, 0, None
    means = first
    p = float((means >= stat).mean())
    se = _se(p, means.size)
    # "near": within two standard errors measured at the observed p OR at the bar itself (the larger)
    if bar is not None and draws_near_bar > means.size and abs(p - bar) < 2.0 * max(se, _se(bar, means.size)):
        more = means_fn(draws_near_bar - means.size, seed + 7_919)
        means = np.concatenate([means, more])
        p = float((means >= stat).mean())
        se = _se(p, means.size)
    used = int(means.size)
    return max(p, 1.0 / used), used, float(se)


def replay(variant: Variant, md: MarketData, cx: Conditioning, *, start: str, end: str,
           rcfg: ResearchConfig, draws: int, seed: int, with_placebo: bool = True,
           limits: Optional[BookLimits] = None, bar: Optional[float] = None,
           draws_near_bar: Optional[int] = None) -> ReplayStats:
    """
    Replay `variant` over signal sessions in [start, end] on a frame sealed at or after `end`.
    A signal whose horizon has not resolved inside the seal is NaN and is not a trade (the seal).
    With `limits` the population is the BOOK's own selection over the window (re-audit N1) —
    the measured strategy; without, every resolved signal equal-weighted (a context figure).
    """
    if end > md.as_of:
        raise ValueError(f"replay window ends {end} after the seal {md.as_of} — look-ahead")
    df = md.df
    col = f"f{variant.horizon}"
    inw = (df["d"] >= start) & (df["d"] <= end)
    sig = variant.signals(md, cx) & inw & df[col].notna()
    H = rcfg.hurdle_pct
    rows = df.loc[sig, ["symbol", "d", col]].sort_values(["d", "symbol"], kind="mergesort")
    n_fired = int(len(rows))
    selection = EQUAL_WEIGHTED
    if limits is not None:
        rows = rows.assign(_liq=cx.liquidity.reindex(rows.index))
        mask = book_select(rows, session_index={d: i for i, d in enumerate(md.sessions)}, horizon=variant.horizon,
                           limits=limits)
        rows = rows.loc[mask]
        selection = limits.label
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
    wp = rcfg.expectancy_winsor_pct
    baseline_net = (_wmean(pool_gross, wp) - H) if baseline_n else None
    if n == 0:
        return ReplayStats(signature=variant.signature, window_start=start, window_end=end, n=0, signal_days=0,
                           hit_pct=None, median_net=None, expectancy_net=None, expectancy_2x_net=None,
                           baseline_net=baseline_net, edge_pct=None, baseline_n=baseline_n, placebo_p=None,
                           placebo_draws=0, cluster_t=None, selection=selection, n_fired=n_fired, n_skipped=n_fired)
    etv = _wmean(net, wp)
    placebo_p: Optional[float] = None
    used = 0
    p_se: Optional[float] = None
    if with_placebo and baseline_n:
        counts = pd.Series(days).value_counts(sort=False).reindex(pd.unique(days)).to_numpy()
        pool_d, pool_v = pool["d"].to_numpy(), pool_gross

        def _means(k: int, s: int) -> np.ndarray:
            return block_placebo_means(pool_d, pool_v, counts, draws=k, seed=s, winsor_pct=wp)

        # the statistic on the SAME convention as the frozen expectation: the winsorised gross mean
        placebo_p, used, p_se = placebo_p_value(_means, float(etv + H), draws=draws,
                                                draws_near_bar=(draws_near_bar or draws), bar=bar, seed=seed)
    share, best, loo = concentration(net, days, wp)
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
        selection=selection, n_fired=n_fired, n_skipped=n_fired - n,
        top3_days_share_pct=share, best_day=best, expectancy_without_best_day_net=loo, placebo_se=p_se,
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
    """
    The three windows are the BOOK-SELECTED population when `limits` were given to `measure()`
    (the measured strategy — what is frozen and gated; re-audit N1); `equal_weighted` is the
    whole window over every resolved firing, a labelled context figure and nothing else.
    """
    variant: Variant
    whole: ReplayStats
    discovery: ReplayStats
    trailing: ReplayStats
    equal_weighted: Optional[ReplayStats] = None


def measure(variant: Variant, md: MarketData, cx: Conditioning, w: Windows, *, rcfg: ResearchConfig,
            draws: int, seed: int, limits: Optional[BookLimits] = None, bar: Optional[float] = None,
            draws_near_bar: Optional[int] = None) -> VariantEvidence:
    kw = dict(rcfg=rcfg, draws=draws, limits=limits)
    return VariantEvidence(
        variant=variant,
        whole=replay(variant, md, cx, start=w.whole[0], end=w.whole[1], seed=seed, bar=bar, draws_near_bar=draws_near_bar, **kw),
        # The placebo is judged on the whole history (the gate); the two sub-windows carry
        # expectancies only, so their null is not drawn — nothing reads it. Each sub-window is
        # its own fresh book (as every forward period is), so the selection restarts at its start.
        discovery=replay(variant, md, cx, start=w.discovery[0], end=w.discovery[1], seed=seed + 1, with_placebo=False, **kw),
        trailing=replay(variant, md, cx, start=w.trailing[0], end=w.trailing[1], seed=seed + 2, with_placebo=False, **kw),
        equal_weighted=(replay(variant, md, cx, start=w.whole[0], end=w.whole[1], rcfg=rcfg, draws=draws, seed=seed + 3,
                               with_placebo=False) if limits is not None else None),
    )
