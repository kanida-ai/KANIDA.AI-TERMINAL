"""
THE QUESTION LIBRARY — `question -> parameters -> computation -> evidence card`.

Spec addendum 2 / principle 3: Pathfinder's research questions come from an expandable,
founder-reviewed library of templates. The model decides which approved questions matter
today and explains the answers; it never fabricates a test. **Every computation in this
engine lives in one of the templates below, and nothing outside this module computes a
statistic.** `scan.py` iterates `LIBRARY` and nothing else.

Six seeded templates, ported from the two proven prototypes (do not re-derive):

    market_regime     pathfinder_demo.py §1   does today's market move carry into tomorrow?
    theme_cycle       pathfinder_theme.py     which sector is in play, with proof + watchlist,
                                              plus the demo's laggard->leader rotation flip
    dip               pathfinder_demo.py §2   after a hard one-day fall, does the group bounce?
    surge             pathfinder_demo.py §3   after a big one-day jump, does chasing pay?
    volume_anomaly    pathfinder_demo.py §4   huge volume, flat close: does it resolve?
    relationship      pathfinder_demo.py §6   a pair's spread is stretched: does it snap back?

Conventions that differ from the prototypes, required by the quant rules and the S1 audit:
  * Every base rate a decision rests on is measured the way the finding is GRADED: entry at
    the NEXT OPEN after the signal close, exit at the horizon close (data.py `f{h}`). The
    only close-to-close statistics left are the ones explicitly labelled so (the anomaly's
    week-later move, the pair's spread-width convergence) and they are not decision inputs.
  * Expectancy (mean net of the hurdle, winsorised) is minted next to every median and gates
    every call — a median that clears costs with a negative expectancy is not an edge (P5).
  * A base rate is judged against a NAMED control: the unconditional universe rate for the
    anomaly (C4), "leaders like this" for the theme (C7), the coin flip for hit rates.
  * A parameter or a single observation is minted as such — never with an invented n (C8).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd

from ..schemas import Decision, EvidenceLevel, GradingKind, GradingRule, SubjectKind
from .config import ResearchConfig
from .data import MarketData
from .facts import FactSet, slug
from .grading import build_rule, theme_universe
from .regime import RegimeSnapshot


# ── shapes ───────────────────────────────────────────────────────────────────

@dataclass
class ScanContext:
    md: MarketData
    cfg: ResearchConfig
    regime: RegimeSnapshot
    computed_at: datetime

    @property
    def hurdle(self) -> float:
        """Round-trip hurdle = costs + slippage both ways (S1 second audit A5)."""
        return self.cfg.hurdle_pct

    @property
    def today(self) -> str:
        return self.md.as_of


@dataclass
class CardDraft:
    """A computed evidence card before ranking and narration. Every number is a Fact."""
    template_id: str
    question: str
    subject: str
    subject_kind: SubjectKind
    level: EvidenceLevel
    decision: Decision
    decision_reason: str                 # digit-free
    facts: FactSet
    key_facts: list[str]                 # fact ids, render-first
    n: int                               # primary sample behind the decision
    comparison_group: str
    grading_rule: GradingRule
    magnitude: float                     # 0..1, how unusual today's observation is
    evidence_z: float                    # |z| of the primary base rate vs its control
    headline: str                        # digit-free, token-free (engine template)
    body: str                            # digit-free with {{fact:...}} tokens
    slug: str
    #: S1 second audit A2 — what the card's CLAIM rests on, at display precision: the
    #: statistic and its comparison group for a group card (NOT the day's subject, which
    #: changes daily while the claim does not); the sector / pair and the decision for a
    #: card whose subject is the evidence. Novelty and continuation are keyed on this.
    evidence_signature: str = ""
    related_symbols: list[str] = field(default_factory=list)
    follow_ups: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class QuestionTemplate:
    id: str
    question: str
    parameters: dict[str, Any]
    computation: Callable[[ScanContext, dict[str, Any]], list[CardDraft]]
    level: EvidenceLevel
    grading_kinds: tuple[GradingKind, ...]
    trader_relevance: float              # FOUNDER INPUT — 0..1 weight in the ranking
    source: str                          # which prototype proved it
    review: str = "founder review pending — prototype-proven on real data"
    #: S1 third audit N2 — how many leading `|`-parts of the evidence signature are the CLAIM
    #: (template, decision, comparison group). The part right after them, if any, is the
    #: PRIMARY STATISTIC: a repeat inside `cfg.claim_tolerance_pp` of an open claim is the same
    #: claim (a continuation), not a new one. 0 = the whole signature is the claim, no statistic.
    claim_parts: int = 0


# ── helpers ──────────────────────────────────────────────────────────────────

def _z(hit_pct: Optional[float], n: int) -> float:
    """Binomial z of a hit-rate against the coin flip. The evidence-strength input."""
    if hit_pct is None or n <= 0:
        return 0.0
    return abs(hit_pct / 100.0 - 0.5) / np.sqrt(0.25 / n)


def _z_lift(lift_pp: float, base_pct: float, n: int, min_lift_pp: float) -> float:
    """
    z of a conditional rate's LIFT over its unconditional control, beyond the smallest lift
    that would matter (S1 audit C4). A rate of 47% on 13,000 cases against a universe rate
    of 48% is a null result, not six sigma of evidence: the old z was measured against 50%.
    """
    if n <= 0:
        return 0.0
    p0 = min(max(base_pct / 100.0, 1e-6), 1 - 1e-6)
    se = np.sqrt(p0 * (1 - p0) / n)
    return float(max(0.0, (abs(lift_pp) - min_lift_pp) / 100.0) / se)


def _wmean(x: pd.Series | np.ndarray, winsor_pct: float) -> float:
    """Winsorised mean (both tails at `winsor_pct`), so one glitch bar cannot fabricate it."""
    s = pd.Series(np.asarray(x, dtype=float)).dropna()
    if s.empty:
        return float("nan")
    lo, hi = s.quantile(winsor_pct / 100.0), s.quantile(1 - winsor_pct / 100.0)
    return float(s.clip(lo, hi).mean())


def _rate(df: pd.DataFrame, mask: pd.Series, h: int, cfg: ResearchConfig
          ) -> tuple[int, Optional[float], Optional[float], Optional[float]]:
    """
    Group base rate over a horizon: (n, hit-rate %, median move %, expectancy %).
    Expectancy = winsorised mean net of the round-trip hurdle. Unresolved rows are NaN.
    """
    r = df.loc[mask & df[f"f{h}"].notna(), f"f{h}"]
    if len(r) == 0:
        return 0, None, None, None
    return (int(len(r)), float((r > 0).mean() * 100.0), float(r.median() * 100.0),
            _wmean(r, cfg.expectancy_winsor_pct) * 100.0 - cfg.hurdle_pct)


def claim_of(template_id: str, signature: str) -> tuple[str, Optional[float]]:
    """
    (claim key, primary statistic) of an evidence signature (N2). The claim is the first
    `claim_parts` parts of the signature as the template declares them; the statistic is the
    next part. A template that declares none (or an unknown template) claims the whole
    signature, with no statistic — exact match only.
    """
    t = TEMPLATES.get(template_id)
    k = int(t.claim_parts) if t is not None else 0
    parts = signature.split("|")
    if k <= 0 or len(parts) <= k:
        return signature, None
    try:
        return "|".join(parts[:k]), float(parts[k])
    except ValueError:
        return signature, None


def same_claim(template_id: str, sig_a: str, sig_b: str, tolerance_pp: float) -> bool:
    """The same claim: identical claim key and a primary statistic inside the tolerance (or none)."""
    ka, sa = claim_of(template_id, sig_a)
    kb, sb = claim_of(template_id, sig_b)
    if ka != kb:
        return False
    if sa is None or sb is None:
        return sa is None and sb is None
    return abs(sa - sb) <= float(tolerance_pp)


def _sig(*parts: Any) -> str:
    """An evidence signature (A2). Floats are rendered at the precision the card displays."""
    out = []
    for x in parts:
        if isinstance(x, float):
            out.append(f"{x:.2f}")
        elif isinstance(x, Decision):
            out.append(x.value)
        else:
            out.append(str(x))
    return "|".join(out)


def _fs(ctx: ScanContext, tag: str, subject: str, *, start: Optional[str] = None) -> FactSet:
    fslug = f"{ctx.today.replace('-', '')}_{tag}_{slug(subject)}"
    return FactSet(finding_slug=fslug, cfg=ctx.cfg, as_of=ctx.today,
                   period_start=start or ctx.md.first_session, period_end=ctx.today,
                   component=f"template_{tag}", computed_at=ctx.computed_at)


def _fslug(fs: FactSet) -> str:
    return fs.prefix[len("fct_"):]


# ── the decision rules, as pure functions (pinned by tests) ──────────────────

def dip_decision(*, wr1: float, med1: float, etv1: float, wr5: Optional[float], med5: Optional[float],
                 etv5: Optional[float], H: float, min_hit: float) -> tuple[Decision, int]:
    """Buy the dip only if the median clears costs, the hit rate clears the bar AND expectancy is positive."""
    if med5 is not None and etv5 is not None and wr5 is not None \
            and med5 > 2 * H and wr5 >= min_hit and etv5 > 0:
        return Decision.virtual_long, 5
    if med1 > H and wr1 >= min_hit and etv1 > 0:
        return Decision.virtual_long, 1
    return Decision.no_trade, 1


def surge_decision(*, wr1: float, med1: float, etv1: float, H: float, min_hit: float,
                   fade_max_hit: float) -> Decision:
    """Reject the chase when it lost after costs (median AND expectancy); go with it only when both clear."""
    if wr1 <= fade_max_hit and med1 < -H and etv1 < 0:
        return Decision.reject
    if wr1 >= min_hit and med1 > H and etv1 > 0:
        return Decision.virtual_long
    return Decision.no_trade


def carry_decision(*, med: float, etv: float, H: float) -> Decision:
    return Decision.virtual_long if (med > H and etv > 0) else Decision.no_trade


def call_on_excess(*, n: int, up_pct: float, med: float, etv: float, H: float, min_hit: float,
                   min_n: int = 20) -> bool:
    """A sector call (theme / rotation) needs n, a hit rate over the bar, a median AND an expectancy that clear costs."""
    return n >= min_n and up_pct >= min_hit and med > H and etv > 0


# ═════════════════════════════════════════════════════════════════════════════
# 1. MARKET REGIME  (pathfinder_demo.py §1)
# ═════════════════════════════════════════════════════════════════════════════

def compute_market_regime(ctx: ScanContext, p: dict[str, Any]) -> list[CardDraft]:
    md, cfg, H = ctx.md, ctx.cfg, ctx.hurdle
    n50 = md.df[md.df["in_nifty50"] == 1]
    if n50.empty:
        return []
    mkt = n50.groupby("d")["ret"].mean()                 # close-to-close, equal weight
    nxt = n50.groupby("d")["f1"].mean()                  # next open -> next close, equal weight
    if ctx.today not in mkt.index or pd.isna(mkt.loc[ctx.today]):
        return []
    m = float(mkt.loc[ctx.today])
    band = p["band_pct"] / 100.0
    sim = (mkt >= m - band) & (mkt <= m + band)
    out = nxt[sim].dropna()                              # today's own f1 is NaN inside the seal
    n = int(len(out))
    if n == 0:
        return []
    wr = float((out > 0).mean() * 100.0)
    med = float(out.median() * 100.0)
    etv = _wmean(out, cfg.expectancy_winsor_pct) * 100.0 - H
    names_today = int(n50[n50["d"] == ctx.today]["symbol"].nunique())

    fs = _fs(ctx, "regime", "market")
    fs.add("move_today", "equal-weight Nifty-fifty move today, close to close (mean over the names)", m * 100, "pct",
           n=names_today)
    fs.add("band", "the band around today's move that defines a comparable session", p["band_pct"], "pct",
           sample="parameter")
    fs.add("comparable", "comparable sessions in history (market move inside the band)", n, "count")
    fs.add("hit_rate", "share of comparable sessions where the next session closed above its open",
           wr, "pct", n=n)
    fs.add("typical", "typical next-session move, next open to close (median)", med, "pct", n=n)
    fs.add("expectancy", "expectancy of the next-session move net of the hurdle (winsorised mean)", etv, "pct", n=n)
    fs.add("hurdle", "round-trip hurdle: costs plus slippage both ways", H * 100, "bps")
    fs.add("regime", "market regime today", ctx.regime.state, "text")
    if ctx.regime.breadth200_pct is not None:
        fs.add("breadth", "share of Nifty-five-hundred names above their two-hundred-day average",
               ctx.regime.breadth200_pct, "pct", n=ctx.regime.n_names)

    decision = carry_decision(med=med, etv=etv, H=H)
    if decision == Decision.no_trade:
        reason = "The typical follow-through, and its expectancy, do not beat trading costs."
        kind, spec = GradingKind.no_trade_call, {"subject_kind": "market", "nifty50_only": 1}
        headline = "Today's market move: history says it does not carry into tomorrow"
    else:
        reason = "The typical follow-through and its expectancy both clear costs."
        kind, spec = GradingKind.directional_call, {"subject_kind": "market", "direction": "long", "nifty50_only": 1}
        headline = "Today's market move: history says the strength tends to carry"
    direction = "rose" if m >= 0 else "fell"
    body = (
        f"The market {direction} {fs.token('move_today')} today. I asked whether a move like this "
        f"carries into the next session. Across {fs.token('comparable')} comparable sessions, the "
        f"next session closed above its open {fs.token('hit_rate')} of the time, with a typical "
        f"move of {fs.token('typical')} from the open and an expectancy of {fs.token('expectancy')} "
        f"net of a round-trip cost hurdle of {fs.token('hurdle')}. "
        + ("That edge is too small to act on, so the call is no trade."
           if decision == Decision.no_trade else
           "That clears costs, so the research book takes a virtual market tilt.")
        + f" Regime: {fs.token('regime')}."
    )
    rule = build_rule(kind, horizon=1, hurdle_pct=H, spec=spec, frozen_at=ctx.computed_at,
                      subject="equal-weight Nifty-fifty")
    return [CardDraft(
        template_id="market_regime", question=MARKET_REGIME.question, subject="NIFTY 50 (equal weight)",
        subject_kind=SubjectKind.market, level=EvidenceLevel.whole_market, decision=decision,
        decision_reason=reason, facts=fs,
        key_facts=[fs.id("move_today"), fs.id("hit_rate"), fs.id("typical"), fs.id("expectancy")],
        n=n, comparison_group=f"all sessions since {md.first_session} where the equal-weight "
                              f"Nifty-fifty moved within ±{p['band_pct']:.1f}% of today's move; "
                              "hit rate judged against the coin flip",
        grading_rule=rule, magnitude=min(1.0, abs(m * 100) / 2.0), evidence_z=_z(wr, n),
        headline=headline, body=body, slug=_fslug(fs),
        evidence_signature=_sig("market_regime", decision, ctx.regime.state, round(wr, 1), med, etv),
        follow_ups=["Does the answer change when breadth is weak?",
                    "Is the follow-through different in a risk-off regime?"],
    )]


MARKET_REGIME = QuestionTemplate(
    id="market_regime",
    question="The market moved today. Does a move like this carry into the next session?",
    parameters={"band_pct": 0.2},
    computation=compute_market_regime, level=EvidenceLevel.whole_market,
    grading_kinds=(GradingKind.no_trade_call, GradingKind.directional_call),
    trader_relevance=1.0, source="pathfinder_demo.py §1 MARKET REGIME",
    claim_parts=3,      # market_regime | decision | regime ; primary statistic = hit rate
)


# ═════════════════════════════════════════════════════════════════════════════
# 2. THE DIP / THE SURGE  (pathfinder_demo.py §2/§3) — group base rate, not one thin sample
# ═════════════════════════════════════════════════════════════════════════════

def _group_move_card(ctx: ScanContext, p: dict[str, Any], *, tag: str, is_dip: bool) -> list[CardDraft]:
    md, cfg, H = ctx.md, ctx.cfg, ctx.hurdle
    df = md.df
    today = md.today_rows().dropna(subset=["ret"])
    if today.empty:
        return []
    thr = p["threshold_pct"] / 100.0
    subj = today.sort_values("ret").iloc[0 if is_dip else -1]
    if (is_dip and subj["ret"] > thr) or (not is_dip and subj["ret"] < thr):
        return []                                             # no hard dip / big surge today
    sym, sector = str(subj["symbol"]), subj["sector"]
    mask = (df["ret"] <= thr) if is_dip else (df["ret"] >= thr)
    n1, wr1, med1, etv1 = _rate(df, mask, 1, cfg)
    n5, wr5, med5, etv5 = _rate(df, mask, 5, cfg)
    if n1 == 0:
        return []
    fs = _fs(ctx, tag, sym)
    fs.add("subject", "the stock", sym, "text", level=EvidenceLevel.same_stock)
    fs.add("move_today", "today's close-to-close move", float(subj["ret"]) * 100, "pct",
           sample="observation", level=EvidenceLevel.same_stock)
    fs.add("threshold", "the one-day move that defines the group", abs(p["threshold_pct"]), "pct",
           sample="parameter")
    fs.add("cases", "historical cases across the whole universe", n1, "count")
    fs.add("hit_1", "share higher the next session (next open to close)", wr1, "pct", n=n1)
    fs.add("typical_1", "typical next-session move (median)", med1, "pct", n=n1)
    fs.add("expectancy_1", "expectancy of the next-session move net of the hurdle (winsorised mean)", etv1, "pct", n=n1)
    if n5:
        fs.add("hit_5", "share higher over the next trading week", wr5, "pct", n=n5)
        fs.add("typical_5", "typical one-week move (median)", med5, "pct", n=n5)
        fs.add("expectancy_5", "expectancy of the one-week move net of the hurdle (winsorised mean)", etv5, "pct", n=n5)
    fs.add("hurdle", "round-trip hurdle: costs plus slippage both ways", H * 100, "bps")
    fs.add("regime", "market regime today", ctx.regime.state, "text")
    # Peer group (same sector) and the stock's own history, each labelled by its n.
    if isinstance(sector, str) and sector:
        pn, pwr, pmed, _ = _rate(df, mask & (df["sector"] == sector), 5 if is_dip else 1, cfg)
        if pn:
            fs.add("peer_cases", f"cases among {sector} peers", pn, "count", level=EvidenceLevel.peer_group)
            fs.add("peer_hit", "share higher among sector peers", pwr, "pct", n=pn, level=EvidenceLevel.peer_group)
            fs.add("peer_typical", "typical move among sector peers", pmed, "pct", n=pn, level=EvidenceLevel.peer_group)
    on, owr, omed, _ = _rate(df, mask & (df["symbol"] == sym), 5 if is_dip else 1, cfg)
    if on:
        fs.add("own_cases", "this stock's own prior cases", on, "count", level=EvidenceLevel.same_stock)
        fs.add("own_hit", "share higher in this stock's own history", owr, "pct", n=on, level=EvidenceLevel.same_stock)

    if is_dip:
        decision, h = dip_decision(wr1=wr1, med1=med1, etv1=etv1, wr5=wr5, med5=med5, etv5=etv5,
                                   H=H, min_hit=p["min_hit_pct"])
        if decision == Decision.virtual_long and h == 5:
            reason = "The bounce clears costs over the week, in median and in expectancy."
            kind, spec = GradingKind.directional_call, {"subject_kind": "stock", "symbol": sym, "direction": "long"}
            headline = "The day's hardest fall: history says the group tends to bounce over a week"
            n = n5
        elif decision == Decision.virtual_long:
            reason = "The next-session bounce clears costs, in median and in expectancy."
            kind, spec = GradingKind.directional_call, {"subject_kind": "stock", "symbol": sym, "direction": "long"}
            headline = "The day's hardest fall: history says the group tends to bounce next session"
            n = n1
        else:
            reason = "A real drop, but the bounce does not beat costs."
            kind, spec = GradingKind.no_trade_call, {"subject_kind": "stock", "symbol": sym}
            headline = "The day's hardest fall: history says do not buy the dip"
            n = n1
        body = (
            f"{fs.token('subject')} fell {fs.token('move_today')} today, the day's hardest drop. Rather "
            f"than judge one stock on one day, I checked how every stock in the universe behaved after "
            f"a one-day fall of at least {fs.token('threshold')}. Across {fs.token('cases')} cases, the "
            f"next session was higher {fs.token('hit_1')} of the time with a typical move of "
            f"{fs.token('typical_1')} and an expectancy of {fs.token('expectancy_1')} net of costs"
            + (f", and over the following week {fs.token('hit_5')} were higher with a typical move of "
               f"{fs.token('typical_5')} and an expectancy of {fs.token('expectancy_5')}" if n5 else "")
            + f". Against a round-trip cost hurdle of {fs.token('hurdle')}, "
            + ("the bounce does not pay, so the call is no trade." if decision == Decision.no_trade
               else "the bounce clears costs, so the research book takes a virtual long.")
            + f" Regime: {fs.token('regime')}."
        )
        follow = ["Does the bounce depend on whether the whole market fell that day?",
                  "Are falls on heavy volume different from falls on light volume?"]
    else:
        decision = surge_decision(wr1=wr1, med1=med1, etv1=etv1, H=H, min_hit=p["min_hit_pct"],
                                  fade_max_hit=p["fade_max_hit_pct"])
        h = 1
        if decision == Decision.reject:
            reason = "Buying big surges lost money after costs, in median and in expectancy; the fade is worth testing across the group."
            kind, spec = GradingKind.no_trade_call, {"subject_kind": "stock", "symbol": sym}
            headline = "The day's biggest jump: history rejects chasing it"
        elif decision == Decision.virtual_long:
            reason = "Momentum continues after costs, in median and in expectancy."
            kind, spec = GradingKind.directional_call, {"subject_kind": "stock", "symbol": sym, "direction": "long"}
            headline = "The day's biggest jump: history says the move tends to continue"
        else:
            reason = "No reliable continuation edge either way."
            kind, spec = GradingKind.no_trade_call, {"subject_kind": "stock", "symbol": sym}
            headline = "The day's biggest jump: history says chasing it does not pay"
        n = n1
        body = (
            f"{fs.token('subject')} jumped {fs.token('move_today')} today. Does chasing a move this big "
            f"pay? Across {fs.token('cases')} cases where a stock in the universe jumped at least "
            f"{fs.token('threshold')} in a day, only {fs.token('hit_1')} continued higher the next "
            f"session, with a typical move of {fs.token('typical_1')} and an expectancy of "
            f"{fs.token('expectancy_1')} net of costs. Against a round-trip cost hurdle of {fs.token('hurdle')}, "
            + ("there is no reliable continuation edge, so the call is no trade."
               if decision == Decision.no_trade else
               "chasing lost money on average, so the chase is rejected and the fade becomes an experiment."
               if decision == Decision.reject else
               "momentum has continued after costs, so the research book takes a virtual long.")
            + f" Regime: {fs.token('regime')}."
        )
        follow = ["Does the answer differ when the jump came with a volume surge?",
                  "Is the fade stronger for small names than for large ones?"]
    key = [fs.id("move_today"), fs.id("hit_1"), fs.id("typical_1"), fs.id("expectancy_1")] \
        + ([fs.id("typical_5")] if (is_dip and n5) else [])
    rule = build_rule(kind, horizon=h, hurdle_pct=H, spec=spec, frozen_at=ctx.computed_at, subject=sym)
    label = "fell" if is_dip else "jumped"
    return [CardDraft(
        template_id=tag, question=(DIP if is_dip else SURGE).question, subject=sym,
        subject_kind=SubjectKind.stock, level=EvidenceLevel.whole_market, decision=decision,
        decision_reason=reason, facts=fs, key_facts=key, n=n,
        comparison_group=f"every Nifty-five-hundred stock on sessions it {label} at least "
                         f"{abs(p['threshold_pct']):.0f}% close to close, since {md.first_session}; "
                         "hit rate judged against the coin flip, moves against the cost hurdle",
        grading_rule=rule, magnitude=min(1.0, abs(float(subj["ret"])) / 0.12), evidence_z=_z(wr1, n1),
        headline=headline, body=body, slug=_fslug(fs), related_symbols=[sym], follow_ups=follow,
        # The claim is the GROUP statistic; the subject changes every day while the claim does
        # not (A2: twenty-three identical dip/surge cards were published as "new" findings).
        evidence_signature=_sig(tag, decision, abs(p["threshold_pct"]), round(wr1, 1), med1, etv1,
                                *(() if not (is_dip and n5) else (round(wr5, 1), med5, etv5))),
    )]


def compute_dip(ctx: ScanContext, p: dict[str, Any]) -> list[CardDraft]:
    return _group_move_card(ctx, p, tag="dip", is_dip=True)


def compute_surge(ctx: ScanContext, p: dict[str, Any]) -> list[CardDraft]:
    return _group_move_card(ctx, p, tag="surge", is_dip=False)


DIP = QuestionTemplate(
    id="dip",
    question="A stock just had a hard one-day fall. Across its whole group, does the dip bounce after costs?",
    parameters={"threshold_pct": -6.0, "min_hit_pct": 58.0},
    computation=compute_dip, level=EvidenceLevel.whole_market,
    grading_kinds=(GradingKind.directional_call, GradingKind.no_trade_call),
    trader_relevance=0.7, source="pathfinder_demo.py §2 THE DIP",
    claim_parts=3,      # dip | decision | threshold ; primary statistic = next-session hit rate
)

SURGE = QuestionTemplate(
    id="surge",
    question="A stock just had a big one-day jump. Across its whole group, does chasing it pay after costs?",
    parameters={"threshold_pct": 6.0, "min_hit_pct": 58.0, "fade_max_hit_pct": 45.0},
    computation=compute_surge, level=EvidenceLevel.whole_market,
    grading_kinds=(GradingKind.directional_call, GradingKind.no_trade_call),
    trader_relevance=0.6, source="pathfinder_demo.py §3 THE SURGE",
    claim_parts=3,      # surge | decision | threshold ; primary statistic = next-session hit rate
)


# ═════════════════════════════════════════════════════════════════════════════
# 3. VOLUME ANOMALY  (pathfinder_demo.py §4) — judged against the UNCONDITIONAL rate
# ═════════════════════════════════════════════════════════════════════════════

def compute_volume_anomaly(ctx: ScanContext, p: dict[str, Any]) -> list[CardDraft]:
    md, cfg, H = ctx.md, ctx.cfg, ctx.hurdle
    df = md.df
    today = md.today_rows().dropna(subset=["ret", "vol20"]).copy()
    today = today[today["vol20"] > 0]
    today["volx"] = today["volume"] / today["vol20"]
    flat = p["flat_pct"] / 100.0
    unu = today[(today["volx"] >= p["volume_x"]) & (today["ret"].abs() < flat)].sort_values("volx", ascending=False)
    if unu.empty:
        return []
    u = unu.iloc[0]
    sym = str(u["symbol"])
    thr = p["move_pct"] / 100.0
    m = (df["volume"] / df["vol20"] >= p["volume_x"]) & (df["ret"].abs() < flat)
    resolved = df["r5cc"].notna()
    r = df.loc[m & resolved]
    nn = int(len(r))
    if nn == 0:
        return []
    # THE CONTROL (S1 audit C4): every resolved stock-session in the universe. The prototype
    # never asked what the base rate was without the anomaly; on the real warehouse it is the
    # same number, and a card that does not say so is a null result dressed as evidence.
    allr = df.loc[resolved, "r5cc"]
    nall = int(len(allr))
    big = float((r["r5cc"].abs() >= thr).mean() * 100.0)
    base_big = float((allr.abs() >= thr).mean() * 100.0)
    lift = big - base_big
    up = float((r["r5cc"] > 0).mean() * 100.0)
    base_up = float((allr > 0).mean() * 100.0)
    own = df.loc[m & resolved & (df["symbol"] == sym)]
    min_lift = float(cfg.anomaly_min_lift_pp)
    z = _z_lift(lift, base_big, nn, min_lift)
    worth_testing = lift >= min_lift

    fs = _fs(ctx, "volume", sym)
    fs.add("subject", "the stock", sym, "text", level=EvidenceLevel.same_stock)
    fs.add("volume_x", "today's volume as a multiple of its twenty-session average", float(u["volx"]), "x",
           sample="observation", level=EvidenceLevel.same_stock)
    fs.add("move_today", "today's close-to-close move", float(u["ret"]) * 100, "pct",
           sample="observation", level=EvidenceLevel.same_stock)
    fs.add("cases", "anomaly days across the whole universe (volume multiple and flat close as configured)", nn, "count")
    fs.add("move_threshold", "the week-later move the anomaly is judged on", p["move_pct"], "pct", sample="parameter")
    fs.add("big_move", "share of anomaly days that had moved at least the threshold, either way, a week later (close to close)",
           big, "pct", n=nn)
    fs.add("base_big_move", "share of ALL stock-sessions in the universe that had moved at least the threshold a week later — the unconditional control",
           base_big, "pct", n=nall)
    fs.add("lift", "anomaly-day share minus the unconditional share, in percentage points", lift, "pct", n=nn)
    fs.add("min_lift", "the smallest lift over the unconditional rate that would make the anomaly worth testing",
           min_lift, "pct", sample="parameter")
    fs.add("up_share", "share of anomaly days that were higher a week later", up, "pct", n=nn)
    fs.add("base_up_share", "share of ALL stock-sessions that were higher a week later", base_up, "pct", n=nall)
    fs.add("hurdle", "round-trip hurdle: costs plus slippage both ways", H * 100, "bps")
    fs.add("regime", "market regime today", ctx.regime.state, "text")
    if len(own):
        fs.add("own_cases", "this stock's own prior anomaly days", int(len(own)), "count", level=EvidenceLevel.same_stock)
        fs.add("own_big_move", "share of its own cases that had moved at least the threshold a week later",
               float((own["r5cc"].abs() >= thr).mean() * 100.0), "pct", n=int(len(own)),
               level=EvidenceLevel.same_stock)
    opening = (
        f"{fs.token('subject')} traded {fs.token('volume_x')} its normal volume today but closed almost "
        f"flat at {fs.token('move_today')}. Big interest with no direction is worth a question: does it "
        f"resolve? Across {fs.token('cases')} anomaly days in the universe, {fs.token('big_move')} had "
        f"moved at least {fs.token('move_threshold')} either way a week later. The control matters: across "
        f"every stock-session in the universe the same share is {fs.token('base_big_move')}, so the lift the "
        f"anomaly adds is {fs.token('lift')}. "
    )
    if worth_testing:
        decision = Decision.new_experiment
        reason = "Anomaly days resolve into a move more often than ordinary days; worth isolating what tells the winners from the losers."
        headline = "Huge volume, flat close: days like this resolve more often than most, worth an experiment"
        body = opening + (
            f"That is a real lift over the unconditional rate, though {fs.token('up_share')} were higher against "
            f"{fs.token('base_up_share')} ordinarily, so there is no clean direction to trade. It opens an "
            f"experiment rather than a call: what separates the days that resolved up from the ones that "
            f"resolved down? Regime: {fs.token('regime')}."
        )
        rule = build_rule(GradingKind.anomaly_move, horizon=5, hurdle_pct=H,
                          spec={"symbol": sym, "move_pct": p["move_pct"]}, frozen_at=ctx.computed_at, subject=sym)
    else:
        decision = Decision.no_trade
        reason = "A striking day, but days like this resolve no more often than any other day; the volume signal carries no edge to trade or to test."
        headline = "Huge volume, flat close: a striking day that history says means nothing"
        body = opening + (
            f"That lift is below the {fs.token('min_lift')} that would make the anomaly worth testing: days like "
            f"this resolve no more often than ordinary days, and {fs.token('up_share')} were higher against "
            f"{fs.token('base_up_share')} ordinarily. The honest finding is a debunk: the volume signal carries "
            f"no edge, so the call is no trade. Regime: {fs.token('regime')}."
        )
        rule = build_rule(GradingKind.no_trade_call, horizon=5, hurdle_pct=H,
                          spec={"subject_kind": "stock", "symbol": sym}, frozen_at=ctx.computed_at, subject=sym)
    return [CardDraft(
        template_id="volume_anomaly", question=VOLUME_ANOMALY.question, subject=sym,
        subject_kind=SubjectKind.stock, level=EvidenceLevel.whole_market, decision=decision,
        decision_reason=reason, facts=fs,
        key_facts=[fs.id("volume_x"), fs.id("big_move"), fs.id("base_big_move"), fs.id("lift")], n=nn,
        comparison_group=f"control = every Nifty-five-hundred stock-session with a resolved week-later move "
                         f"since {md.first_session} (the unconditional rate); the anomaly days (volume at least "
                         f"{p['volume_x']:.0f}x the twenty-session average, close within ±{p['flat_pct']:.1f}%) "
                         "are judged by their LIFT over that control",
        grading_rule=rule, magnitude=min(1.0, float(u["volx"]) / 10.0), evidence_z=z,
        headline=headline, body=body, slug=_fslug(fs),
        # N2: the claim is (template, decision) against the unconditional control; the LIFT is
        # the primary statistic, so a re-reading of −1.1 pp against an open −1.2 pp is the same
        # null claim, not a new one (ANURAS -> TORNTPHARM -> SOBHA were graded three times).
        evidence_signature=_sig("volume_anomaly", decision, round(lift, 1), round(big, 1), round(base_big, 1)),
        related_symbols=[sym],
        follow_ups=["Do anomaly days that came after a fall resolve differently from those after a rise?",
                    "Does the direction of the following session predict the week?"],
    )]


VOLUME_ANOMALY = QuestionTemplate(
    id="volume_anomaly",
    question="A stock traded many times its normal volume but closed flat. Does that kind of day resolve into a move more often than any other day?",
    parameters={"volume_x": 3.0, "flat_pct": 1.5, "move_pct": 3.0},
    computation=compute_volume_anomaly, level=EvidenceLevel.whole_market,
    grading_kinds=(GradingKind.anomaly_move, GradingKind.no_trade_call),
    trader_relevance=0.5, source="pathfinder_demo.py §4 VOLUME ANOMALY",
    claim_parts=2,      # volume_anomaly | decision ; primary statistic = lift over the control (pp)
)


# ═════════════════════════════════════════════════════════════════════════════
# 4. RELATIONSHIP  (pathfinder_demo.py §6) — the spread TRADE, on episodes
# ═════════════════════════════════════════════════════════════════════════════

def _episodes(ext: np.ndarray) -> np.ndarray:
    """First index of each run of consecutive extreme sessions (S1 audit C5: 412 event-days are 121 episodes)."""
    if len(ext) == 0:
        return ext
    return ext[np.r_[True, np.diff(ext) > 1]]


def compute_relationship(ctx: ScanContext, p: dict[str, Any]) -> list[CardDraft]:
    md, cfg, H = ctx.md, ctx.cfg, ctx.hurdle
    pairs = list(p.get("pairs") or cfg.pairs)
    syms = sorted({s for pr in pairs for s in pr})
    cl = md.close_wide(syms)
    best = None
    for a, b in pairs:
        if a not in cl or b not in cl:
            continue
        sp = (np.log(cl[a]) - np.log(cl[b])).dropna()
        if len(sp) < p["min_history"]:
            continue
        z = (sp - sp.rolling(p["z_window"]).mean()) / sp.rolling(p["z_window"]).std()
        if ctx.today not in z.index or pd.isna(z.loc[ctx.today]):
            continue
        zt = float(z.loc[ctx.today])
        if abs(zt) < p["z_extreme"]:
            continue
        if best is None or abs(zt) > abs(best[2]):
            best = (a, b, zt, z)
    if best is None:
        return []
    a, b, zt, z = best
    h = p["horizon"]
    zz = z.dropna()
    zarr = zz.to_numpy()
    dates = zz.index.to_numpy()
    ext_all = np.where(np.abs(zarr) >= p["z_extreme"])[0]
    # The prototype's statistic: did the spread WIDTH narrow h sessions later (close to close)?
    ext_c = ext_all[ext_all + h < len(zarr)]                    # resolved inside the seal only
    conv = np.abs(zarr[ext_c + h]) < np.abs(zarr[ext_c])
    nc = int(len(conv))
    c6 = float(conv.mean() * 100.0) if nc else None       # withheld, never 0.0 "of 0 times" (A6)
    # THE GRADED METRIC (S1 audit C5): the spread trade from the NEXT OPEN, long the cheap leg,
    # short the rich leg, judged against twice the hurdle (two legs) — exactly what the frozen
    # rule measures. Computed per event-day, then on EPISODES (first day of each run of
    # consecutive extreme sessions), which are the independent observations.
    f5a = md.symbol(a).set_index("d")[f"f{h}"].reindex(dates[ext_all]).to_numpy()
    f5b = md.symbol(b).set_index("d")[f"f{h}"].reindex(dates[ext_all]).to_numpy()
    sign = np.where(zarr[ext_all] > 0, -1.0, 1.0)
    pnl = sign * (np.log1p(f5a) - np.log1p(f5b)) * 100.0
    ok = ~np.isnan(pnl)
    ev_pnl = pnl[ok]
    ne = int(len(ev_pnl))
    first = np.r_[True, np.diff(ext_all) > 1] if len(ext_all) else np.array([], dtype=bool)
    ep_pnl = pnl[first & ok]
    nep = int(len(ep_pnl))
    if nep == 0:
        return []
    H2 = 2 * H

    def _stats(x: np.ndarray) -> tuple[float, float, float, float, float]:
        return (float((x > H2).mean() * 100), float((x < -H2).mean() * 100), float((x > 0).mean() * 100),
                float(np.median(x)), _wmean(x, cfg.expectancy_winsor_pct) - H2)

    e_right, e_wrong, e_up, e_med, e_etv = _stats(ep_pnl)
    d_right, d_wrong, d_up, d_med, d_etv = _stats(ev_pnl)
    short_leg, long_leg = (a, b) if zt > 0 else (b, a)

    fs = _fs(ctx, "pair", f"{a}_{b}", start=str(zz.index[0]))
    fs.add("leg_a", "first leg", a, "text", level=EvidenceLevel.same_stock)
    fs.add("leg_b", "second leg", b, "text", level=EvidenceLevel.same_stock)
    fs.add("rich_leg", "the leg that ran ahead", short_leg, "text", level=EvidenceLevel.same_stock)
    fs.add("cheap_leg", "the leg that lagged", long_leg, "text", level=EvidenceLevel.same_stock)
    fs.add("sigma", "today's spread gap in standard deviations of its rolling history", zt, "ratio",
           sample="observation", level=EvidenceLevel.same_stock)
    fs.add("episodes", "prior stretches of this pair (a run of consecutive sessions at least this far apart counts once)",
           nep, "count", level=EvidenceLevel.same_stock)
    fs.add("spread_right", "share of those stretches where the spread trade from the next open returned more than twice the hurdle a week later",
           e_right, "pct", n=nep, level=EvidenceLevel.same_stock)
    fs.add("spread_wrong", "share where the spread trade lost more than twice the hurdle", e_wrong, "pct",
           n=nep, level=EvidenceLevel.same_stock)
    fs.add("spread_up", "share where the spread trade finished positive", e_up, "pct", n=nep, level=EvidenceLevel.same_stock)
    fs.add("spread_typical", "typical spread-trade return over the week (median)", e_med, "pct", n=nep,
           level=EvidenceLevel.same_stock)
    fs.add("spread_expectancy", "expectancy of the spread trade net of both legs' costs (winsorised mean)", e_etv, "pct",
           n=nep, level=EvidenceLevel.same_stock)
    fs.add("event_days", "stretched sessions counted one by one — overlapping, not independent", ne, "count",
           level=EvidenceLevel.same_stock)
    fs.add("event_spread_right", "share of the overlapping session-days where the spread trade returned more than twice the hurdle",
           d_right, "pct", n=ne, level=EvidenceLevel.same_stock)
    fs.add("event_spread_wrong", "share of the overlapping session-days where it lost more than twice the hurdle",
           d_wrong, "pct", n=ne, level=EvidenceLevel.same_stock)
    fs.add("event_spread_typical", "typical spread-trade return over the overlapping session-days (median)",
           d_med, "pct", n=ne, level=EvidenceLevel.same_stock)
    if nc:
        fs.add("stretched_before", "stretched sessions with a resolved week-later spread width (overlapping)", nc, "count",
               level=EvidenceLevel.same_stock)
        fs.add("snapped_back", "share of those where the spread WIDTH had narrowed a week later, close to close — a width statistic, not a trade result",
               c6, "pct", n=nc, level=EvidenceLevel.same_stock)
    fs.add("hurdle", "round-trip hurdle per leg: costs plus slippage both ways", H * 100, "bps")
    fs.add("regime", "market regime today", ctx.regime.state, "text")

    experiment = call_on_excess(n=nep, up_pct=e_up, med=e_med, etv=e_etv, H=H2, min_hit=p.get("min_hit_pct", 58.0))
    body = (
        f"{fs.token('leg_a')} and {fs.token('leg_b')} normally move together. Today their gap reached "
        f"{fs.token('sigma')} standard deviations, an extreme for this pair, with {fs.token('rich_leg')} "
        f"running ahead of {fs.token('cheap_leg')}. The traded question is whether a spread trade from the "
        f"next open, long the lagging leg and short the leading one, pays over a week. Across "
        f"{fs.token('episodes')} prior stretches of this pair it cleared both legs' costs "
        f"{fs.token('spread_right')} of the time and lost more than costs {fs.token('spread_wrong')} of the time, "
        f"with a typical return of {fs.token('spread_typical')} and an expectancy of {fs.token('spread_expectancy')} "
        f"net of costs. "
        + (f"The gap itself had narrowed a week later {fs.token('snapped_back')} of the time, but a "
           "narrower gap is not a paid trade, so that is context, not the claim. " if nc else "")
        + ("The trade has paid more often than not, but the short leg is hard for retail to hold, so it stays a "
           "research experiment in convergence rather than a call." if experiment else
           "The spread trade does not clear costs reliably enough to test, so this is a watch, not a call.")
        + f" Regime: {fs.token('regime')}."
    )
    # N3: the kind follows the DECISION. An experiment claims convergence and is graded on it;
    # a watch claims "not worth calling" and is graded on that (Right if the declined spread
    # trade lost more than both legs' costs) — the theme template's watch -> theme_watch device.
    rule = build_rule(GradingKind.pair_convergence if experiment else GradingKind.pair_watch, horizon=h, hurdle_pct=H,
                      spec={"a": a, "b": b, "direction": "short_a_long_b" if zt > 0 else "long_a_short_b",
                            "z_window": p["z_window"]},
                      frozen_at=ctx.computed_at, subject=f"{a}/{b}")
    return [CardDraft(
        template_id="relationship", question=RELATIONSHIP.question, subject=f"{a} / {b}",
        subject_kind=SubjectKind.pair, level=EvidenceLevel.same_stock,
        decision=Decision.new_experiment if experiment else Decision.watch,
        decision_reason=("The spread trade has cleared costs more often than not, but the short leg is hard for retail; it stays an experiment."
                         if experiment else "The spread trade does not clear costs reliably; watching the pair, not calling it."),
        facts=fs, key_facts=[fs.id("sigma"), fs.id("spread_right"), fs.id("spread_typical"), fs.id("episodes")], n=nep,
        comparison_group=f"this pair's own stretches of at least {p['z_extreme']:.0f} standard deviations since "
                         f"{zz.index[0]}, one per run of consecutive sessions; the spread trade's hit rate is judged "
                         "against the coin flip and its return against twice the cost hurdle (two legs)",
        grading_rule=rule, magnitude=min(1.0, abs(zt) / 3.0), evidence_z=_z(e_up, nep),
        evidence_signature=_sig("relationship", a, b, "short_a_long_b" if zt > 0 else "long_a_short_b",
                                Decision.new_experiment if experiment else Decision.watch),
        headline=("A stretched pair: history says the spread trade has paid more often than not" if experiment
                  else "A stretched pair, but history says the spread trade does not reliably pay"),
        body=body, slug=_fslug(fs), related_symbols=[a, b],
        follow_ups=["Does convergence come from the rich leg falling or the cheap leg rising?",
                    "Is the snap-back faster when the whole sector is moving?"],
    )]


RELATIONSHIP = QuestionTemplate(
    id="relationship",
    question="Two stocks that normally move together have come apart. Does a spread trade from the next open pay within a week?",
    parameters={"pairs": None, "z_window": 60, "z_extreme": 2.0, "min_history": 160, "horizon": 5, "min_hit_pct": 58.0},
    computation=compute_relationship, level=EvidenceLevel.same_stock,
    grading_kinds=(GradingKind.pair_convergence, GradingKind.pair_watch),
    trader_relevance=0.5, source="pathfinder_demo.py §6 RELATIONSHIP",
    claim_parts=5,      # relationship | a | b | direction | decision ; no statistic — the pair and the decision ARE the claim
)


# ═════════════════════════════════════════════════════════════════════════════
# 5. THEME / CYCLE  (pathfinder_theme.py + pathfinder_demo.py §5 rotation flip)
# ═════════════════════════════════════════════════════════════════════════════

def _sector_tables(md: MarketData, window: int, min_names: int, h: int):
    """Sector x date tables. `exn` = sector-minus-market f{h} (next open -> horizon close): the graded metric."""
    d2 = theme_universe(md, md.today, min_names)
    sret = d2.groupby(["d", "sector"])["ret"].mean().unstack()      # date x sector, close to close
    mkt = d2.groupby("d")["ret"].mean()                              # equal-weight market
    roll = sret.rolling(window).sum()
    mroll = mkt.rolling(window).sum()
    rel = roll.sub(mroll, axis=0)
    rank = rel.rank(axis=1, ascending=False)
    fsec = d2.groupby(["d", "sector"])[f"f{h}"].mean().unstack().reindex(index=sret.index, columns=sret.columns)
    fmkt = d2.groupby("d")[f"f{h}"].mean().reindex(sret.index)
    exn = fsec.sub(fmkt, axis=0)                                     # NaN where unresolved (the seal)
    return d2, sret, mkt, roll, rel, rank, exn


def _persistence(rel: pd.DataFrame, rank: pd.DataFrame, h: int, *, stride: int = 1) -> tuple[Optional[float], int]:
    """
    Any leader-session: was the day's leader still top-three h sessions later? (the prototype's
    statistic). `stride = h` samples NON-OVERLAPPING windows (S1 second audit A3: at h = 5 with
    every session counted, 70% is a window-overlap artefact — the same leader is measured five
    times over the same week; at h = 15 on non-overlapping windows it is ~25%). Returns
    (None, 0) rather than a zero sentinel when nothing resolved (A6).
    """
    valid = rel.notna().any(axis=1).to_numpy()
    cols = list(rel.columns)
    lead = rel[valid].idxmax(axis=1).reindex(rel.index)
    rk = rank.to_numpy()
    hits: list[bool] = []
    for i in range(0, len(rel) - h, max(1, stride)):
        if not valid[i]:
            continue
        s = lead.iloc[i]
        if not isinstance(s, str):
            continue
        v = rk[i + h, cols.index(s)]
        if not np.isnan(v):
            hits.append(v <= 3)
    return (float(np.mean(hits) * 100.0) if hits else None), len(hits)


def _effective_n(session_idx: np.ndarray, sector_idx: np.ndarray, h: int) -> int:
    """
    Independent cases among overlapping leader-sessions (A3 "use an effective n"): the same
    sector leading on consecutive sessions is one episode per non-overlapping h-session
    window, not one case per day. Greedy: a case is counted, then the same sector's cases
    inside the next h - 1 sessions are folded into it.
    """
    last: dict[int, int] = {}
    n = 0
    for t, sct in zip(session_idx.tolist(), sector_idx.tolist()):
        if sct in last and t < last[sct] + h:
            continue
        last[sct] = t
        n += 1
    return n


def _leaders_like_this(sret: pd.DataFrame, mkt: pd.Series, rel: pd.DataFrame, exn: pd.DataFrame,
                       window: int, min_days: int, h: int = 5) -> tuple[np.ndarray, int]:
    """
    The graded metric on the card's OWN criteria (S1 audit C7): for every past session whose
    leader had beaten the market on at least `min_days` of `window` sessions with a higher
    cumulative return, the leader's sector-minus-market f{h}. Returns (the array in %, the
    effective number of independent cases — see `_effective_n`).
    """
    days_out = sret.gt(mkt, axis=0).rolling(window).sum()
    cum = np.expm1(np.log1p(sret).rolling(window).sum())
    mcum = np.expm1(np.log1p(mkt).rolling(window).sum())
    valid = rel.notna().any(axis=1).to_numpy()
    lead_idx = np.where(valid, rel.fillna(-np.inf).to_numpy().argmax(axis=1), -1)
    rows = np.where(lead_idx >= 0)[0]
    li = lead_idx[rows]
    do = days_out.to_numpy()[rows, li]
    sc = cum.to_numpy()[rows, li]
    mc = mcum.to_numpy()[rows]
    ex = exn.to_numpy()[rows, li]
    like = (sc > mc) & (do >= min_days) & ~np.isnan(ex)
    return ex[like] * 100.0, _effective_n(rows[like], li[like], h)


def compute_theme_cycle(ctx: ScanContext, p: dict[str, Any]) -> list[CardDraft]:
    md, cfg, H = ctx.md, ctx.cfg, ctx.hurdle
    WIN, h = p["window"], p["horizon"]
    min_days = int(np.ceil(WIN * 0.6))
    d2, sret, mkt, roll, rel, rank, exn = _sector_tables(md, WIN, p["min_names"], h)
    if ctx.today not in rel.index or rel.loc[ctx.today].dropna().empty:
        return []
    lead_today = rel.loc[ctx.today].dropna().sort_values(ascending=False)
    top = str(lead_today.index[0])
    recent = sret.tail(WIN)
    mrec = mkt.tail(WIN)
    days_out = int((recent[top] > mrec).sum())
    sec_cum = float((recent[top] + 1).prod() - 1)
    mkt_cum = float((mrec + 1).prod() - 1)
    tdy = md.today_rows()
    secn = tdy[tdy["sector"] == top].dropna(subset=["ma20"])
    persist, nP = _persistence(rel, rank, h)                       # overlapping, context only
    persist_nx, nPx = _persistence(rel, rank, WIN, stride=WIN)      # non-overlapping, mechanical (A3)
    like, nL_eff = _leaders_like_this(sret, mkt, rel, exn, WIN, min_days, h)
    nL = int(len(like))
    members = sorted(tdy[tdy["sector"] == top]["symbol"].unique().tolist())
    cards: list[CardDraft] = []
    prev = md.prev_session
    prev_top = str(rel.loc[prev].idxmax()) if prev in rel.index and rel.loc[prev].notna().any() else None
    if nL > 0:
        cards.append(_theme_leader_card(ctx, p, top=top, members=members, sret=sret, mkt=mkt, rel=rel, rank=rank,
                                        recent=recent, mrec=mrec, days_out=days_out, sec_cum=sec_cum, mkt_cum=mkt_cum,
                                        secn=secn, tdy=tdy, like=like, nL=nL, nL_eff=nL_eff, persist=persist, nP=nP,
                                        persist_nx=persist_nx, nPx=nPx, prev_top=prev_top, min_days=min_days))
    # else: no past leader-session like this one has resolved — there is no base rate to
    # judge the leader by, so the theme card is WITHHELD (A6), never rendered with zeros.
    cards.extend(_rotation_cards(ctx, p, sret=sret, roll=roll, exn=exn, rel=rel, tdy=tdy))
    return cards


def _theme_leader_card(ctx: ScanContext, p: dict[str, Any], *, top: str, members: list[str], sret, mkt, rel, rank,
                       recent, mrec, days_out: int, sec_cum: float, mkt_cum: float, secn, tdy, like: np.ndarray,
                       nL: int, nL_eff: int, persist: Optional[float], nP: int, persist_nx: Optional[float],
                       nPx: int, prev_top: Optional[str], min_days: int) -> CardDraft:
    md, cfg, H = ctx.md, ctx.cfg, ctx.hurdle
    WIN, h = p["window"], p["horizon"]
    l_beat = float((like > H).mean() * 100.0)
    l_lag = float((like < -H).mean() * 100.0)
    l_up = float((like > 0).mean() * 100.0)
    l_med = float(np.median(like))
    l_etv = _wmean(like, cfg.expectancy_winsor_pct) - H
    lead_today = rel.loc[ctx.today].dropna().sort_values(ascending=False)
    breadth = float((secn["close"] > secn["ma20"]).mean() * 100.0) if len(secn) else None
    wl = tdy[tdy["sector"] == top].dropna(subset=["r15"]).sort_values("r15", ascending=False).head(p["watchlist"])
    looks_like_a_cycle = sec_cum > mkt_cum and days_out >= min_days
    # THE STRONG TEST (A3/A5): the graded metric's conditional base rate on leaders meeting the
    # card's own criteria must clear the hurdle in EXPECTANCY (mean excess > costs + slippage),
    # in median, and in hit rate — on the EFFECTIVE n, not the overlapping session count.
    strong = looks_like_a_cycle and call_on_excess(n=nL_eff, up_pct=l_up, med=l_med, etv=l_etv, H=H,
                                                   min_hit=p.get("min_hit_pct", 58.0))
    market_names = int(tdy[tdy["sector"].isin(sret.columns)]["symbol"].nunique())

    fs = _fs(ctx, "theme", top, start=str(rel.index[0]))
    fs.add("sector", "the sector in play", top, "text", level=EvidenceLevel.sector)
    fs.add("window", "sessions that define the cycle", WIN, "sessions", sample="parameter", level=EvidenceLevel.sector)
    fs.add("days_out", "sessions in the window the sector beat the market", days_out, "count", level=EvidenceLevel.sector)
    fs.add("sector_return", "sector return over the window (equal weight over its names)", sec_cum * 100, "pct",
           n=len(members), level=EvidenceLevel.sector)
    fs.add("market_return", "market return over the same window (equal weight over every name in a sector "
                            "with at least the minimum number of names)", mkt_cum * 100, "pct",
           n=market_names, level=EvidenceLevel.whole_market)
    if breadth is not None:
        fs.add("breadth", "share of the sector's names above their twenty-session average", breadth, "pct",
               n=int(len(secn)), level=EvidenceLevel.sector)
    fs.add("like_this_cases", f"past leader-sessions like this one: the leader had beaten the market on at least "
                              f"{min_days} of {WIN} sessions with a higher cumulative return (overlapping sessions)",
           nL, "count")
    fs.add("like_this_independent", "independent cases among them: one per sector per non-overlapping horizon "
                                    "window — the effective n the evidence is weighed on", nL_eff, "count")
    # N4: the statistics are minted on the EFFECTIVE n — the independent cases the decision,
    # the z and the provenance are weighed on — not on the overlapping session count, which
    # would inflate every fact-level sample flag. The overlapping count is its own fact above.
    fs.add("like_this_beat", "share of leaders like this whose sector then beat the market by more than the hurdle over the next week, next open to close",
           l_beat, "pct", n=nL_eff, level=EvidenceLevel.whole_market)
    fs.add("like_this_lagged", "share of leaders like this whose sector then lagged the market by more than the hurdle",
           l_lag, "pct", n=nL_eff, level=EvidenceLevel.whole_market)
    fs.add("like_this_up", "share of leaders like this whose sector then beat the market at all",
           l_up, "pct", n=nL_eff, level=EvidenceLevel.whole_market)
    fs.add("like_this_typical_excess", "typical sector-minus-market over the next week for leaders like this (median)",
           l_med, "pct", n=nL_eff, level=EvidenceLevel.whole_market)
    fs.add("like_this_expectancy", "expectancy of sector-minus-market net of the hurdle for leaders like this (winsorised mean)",
           l_etv, "pct", n=nL_eff, level=EvidenceLevel.whole_market)
    if persist is not None:
        fs.add("persistence", "share of ALL past leader-sessions (any leader, however thin its lead) still a top-three "
                              "sector a week later — the prototype's statistic on OVERLAPPING windows, context only",
               persist, "pct", n=nP, level=EvidenceLevel.whole_market,
               degenerate_ok=(len(rel.columns) <= 3))          # "top three" of three sectors is a tautology
        fs.add("persistence_cases", "past leader-sessions the any-leader persistence rate is measured on (overlapping)", nP, "count")
    if persist_nx is not None:
        fs.add("persistence_mechanical", f"share of past leaders still a top-three sector {WIN} sessions later, measured on "
                                         "NON-overlapping windows — the mechanical persistence, no overlap artefact",
               persist_nx, "pct", n=nPx, level=EvidenceLevel.whole_market,
               degenerate_ok=(len(rel.columns) <= 3))
    fs.add("hurdle", "round-trip hurdle: costs plus slippage both ways", H * 100, "bps")
    fs.add("regime", "market regime today", ctx.regime.state, "text")
    for i, (_, r) in enumerate(wl.iterrows(), start=1):
        fs.add(f"watch_{i}", "watchlist name inside the sector", str(r["symbol"]), "text", level=EvidenceLevel.same_stock)
        fs.add(f"watch_{i}_move", "its move over the window", float(r["r15"]) * 100, "pct",
               sample="observation", level=EvidenceLevel.same_stock)
    others = []
    for j, s in enumerate(lead_today.index[1:4], start=1):
        dO = int((recent[s] > mrec).sum())
        cum = float((recent[s] + 1).prod() - 1)
        state = "building" if (cum > mkt_cum and dO >= WIN * 0.5) else "fading"
        fs.add(f"other_{j}", "another theme on the radar", str(s), "text", level=EvidenceLevel.sector)
        fs.add(f"other_{j}_return", "its return over the window", cum * 100, "pct", sample="observation",
               level=EvidenceLevel.sector)
        fs.add(f"other_{j}_state", "building or fading", state, "text", level=EvidenceLevel.sector)
        others.append(j)
    if prev_top is not None:
        fs.add("prev_leader", "yesterday's leading sector", prev_top, "text", level=EvidenceLevel.sector)
        fs.add("prev_leader_rank_today", "where yesterday's leader ranks today", int(rank.loc[ctx.today, prev_top]),
               "count", level=EvidenceLevel.sector)

    if strong:
        decision = Decision.virtual_long
        reason = "A real rotation, not noise: the sector beat the market on most sessions, the move is broad, and leaders like this went on to beat the market by more than costs and slippage, in expectancy and more often than not."
        kind = GradingKind.theme_call
        headline = "A sector is in play, and the evidence says it is a real rotation"
        verdict_line = "The research book takes a virtual sector tilt; I will say so the moment the evidence weakens."
    else:
        decision = Decision.watch
        kind = GradingKind.theme_watch
        if looks_like_a_cycle:
            reason = "The day's leader looks like a cycle, but leaders like this did not beat the market after costs often enough to call it; watching."
            headline = "A sector leads today, but leaders like this have not reliably kept beating the market"
        else:
            reason = "The day's leader, but the evidence for a cycle is thin; watching, not calling it."
            headline = "A sector leads today, but the evidence for a cycle is thin"
        verdict_line = "I am watching, not calling it."
    wl_txt = ", ".join(f"{fs.token(f'watch_{i}')} ({fs.token(f'watch_{i}_move')} over the window)"
                       for i in range(1, len(wl) + 1))
    others_txt = "; ".join(f"{fs.token(f'other_{j}')} {fs.token(f'other_{j}_return')}, {fs.token(f'other_{j}_state')}"
                           for j in others)
    body = (
        f"{fs.token('sector')} is the strongest sector by relative strength over the last "
        f"{fs.token('window')} sessions. The proof: it beat the market on {fs.token('days_out')} of those "
        f"sessions, returning {fs.token('sector_return')} against the market's {fs.token('market_return')}"
        + (f"; the move is broad, with {fs.token('breadth')} of its names above their twenty-session average. "
           if breadth is not None else ". ")
        + f"The test that matters: across {fs.token('like_this_cases')} past sessions when a sector led like "
        f"this ({fs.token('like_this_independent')} independent cases once overlapping sessions are folded), it "
        f"went on to beat the market by more than costs and slippage over the next week {fs.token('like_this_beat')} "
        f"of the time and lagged by more than that {fs.token('like_this_lagged')} of the time, with a typical "
        f"edge of {fs.token('like_this_typical_excess')} and an expectancy of {fs.token('like_this_expectancy')} "
        f"net of the hurdle of {fs.token('hurdle')}. "
        + (f"For context, a leader was still a top-three sector {fs.token('window')} sessions later "
           f"{fs.token('persistence_mechanical')} of the time on non-overlapping windows; staying ranked is not "
           "the same as paying. " if persist_nx is not None else "")
        + f"{verdict_line}"
        + (f" Inside the sector, the strongest names are {wl_txt}." if wl_txt else "")
        + (f" Other themes on the radar: {others_txt}." if others_txt else "")
        + (f" Yesterday I flagged {fs.token('prev_leader')}; today it ranks {fs.token('prev_leader_rank_today')}."
           if prev_top is not None else "")
        + f" Regime: {fs.token('regime')}."
    )
    rule = build_rule(kind, horizon=h, hurdle_pct=H,
                      spec={"sector": top, "members": members, "window": WIN, "min_names": p["min_names"]},
                      frozen_at=ctx.computed_at, subject=top)
    return CardDraft(
        template_id="theme_cycle", question=THEME_CYCLE.question, subject=top, subject_kind=SubjectKind.sector,
        level=EvidenceLevel.sector, decision=decision, decision_reason=reason, facts=fs,
        key_facts=[fs.id("days_out"), fs.id("sector_return"), fs.id("market_return"), fs.id("like_this_beat"),
                   fs.id("like_this_expectancy")],
        n=nL_eff, comparison_group=f"the equal-weight market of all sectors with at least {p['min_names']} names, "
                               f"over the last {WIN} sessions; 'leaders like this' = every past leader-session since "
                               f"{rel.index[0]} whose leader had beaten that market on at least {min_days} of {WIN} "
                               "sessions with a higher cumulative return, judged on sector-minus-market from the next "
                               "open to the horizon close against the cost hurdle",
        grading_rule=rule, magnitude=min(1.0, 0.5 * days_out / WIN + 0.5 * (breadth or 0.0) / 100.0),
        evidence_z=_z(l_up, nL_eff), headline=headline, body=body, slug=_fslug(fs),
        evidence_signature=_sig("theme_cycle", "leader", top, decision),
        related_symbols=[str(r["symbol"]) for _, r in wl.iterrows()],
        follow_ups=["Which names inside the sector are doing the work, and which are lagging?",
                    "Has this sector led in this regime before, and for how long?"],
    )


def _rotation_cards(ctx: ScanContext, p: dict[str, Any], *, sret, roll, exn, rel, tdy) -> list[CardDraft]:
    """The laggard -> leader rotation flip (pathfinder_demo.py §5)."""
    md, cfg, H = ctx.md, ctx.cfg, ctx.hurdle
    WIN, h = p["window"], p["horizon"]
    cards: list[CardDraft] = []
    trank = sret.loc[ctx.today].rank(ascending=False)
    lrank = roll.loc[ctx.today].rank(ascending=False)
    ncol = len(sret.columns)
    rot = [s for s in sret.columns if trank.get(s, np.nan) <= 2 and lrank.get(s, np.nan) >= ncol - 3]
    if rot:
        s = str(rot[0])
        # Base rate on the GRADED metric (S1 audit C6): when a bottom-four sector became a
        # top-two sector for a day, sector-minus-market from the next open to the close five
        # sessions later. The prototype summed close-to-close returns; that is not what is
        # traded or graded.
        tr_all = sret.rank(axis=1, ascending=False)
        lr_all = roll.rank(axis=1, ascending=False)
        cond = (tr_all <= 2) & (lr_all >= ncol - 3)
        hitmask = cond & exn.notna()
        vals = exn.to_numpy()[hitmask.to_numpy()] * 100.0
        nr = int(len(vals))
        if nr == 0:
            return cards          # no resolved flip in history: no base rate, no card (A6)
        beat = float((vals > 0).mean() * 100.0)
        beat_h = float((vals > H).mean() * 100.0)
        lag_h = float((vals < -H).mean() * 100.0)
        med = float(np.median(vals))
        etv = _wmean(vals, cfg.expectancy_winsor_pct) - H
        smem = sorted(tdy[tdy["sector"] == s]["symbol"].unique().tolist())
        fr = _fs(ctx, "rotation", s, start=str(rel.index[0]))
        fr.add("sector", "the sector that flipped", s, "text", level=EvidenceLevel.sector)
        fr.add("today_rank", "its rank among sectors today", int(trank[s]), "count", level=EvidenceLevel.sector)
        fr.add("window_rank", "its rank over the window", int(lrank[s]), "count", level=EvidenceLevel.sector)
        fr.add("sectors", "sectors ranked", ncol, "count")
        fr.add("flips", "past sessions when a bottom sector flipped to the top like this", nr, "count")
        fr.add("beat_market", "share of those where the sector then beat the market over the following week, next open to close",
               beat, "pct", n=nr, level=EvidenceLevel.whole_market)
        fr.add("beat_after_costs", "share where it beat the market by more than the hurdle", beat_h, "pct",
               n=nr, level=EvidenceLevel.whole_market)
        fr.add("lagged_after_costs", "share where it lagged the market by more than the hurdle", lag_h, "pct",
               n=nr, level=EvidenceLevel.whole_market)
        fr.add("typical_excess", "typical sector-minus-market over the following week, next open to close (median)", med, "pct",
               n=nr, level=EvidenceLevel.whole_market)
        fr.add("expectancy", "expectancy of sector-minus-market net of the hurdle (winsorised mean)", etv, "pct",
               n=nr, level=EvidenceLevel.whole_market)
        fr.add("hurdle", "round-trip hurdle: costs plus slippage both ways", H * 100, "bps")
        fr.add("regime", "market regime today", ctx.regime.state, "text")
        # The decision follows the base rate, not the story. The prototype opened an
        # experiment unconditionally; the history says a flip like this usually does NOT
        # start a rotation, and the card must say so when that is what the data shows.
        starts = call_on_excess(n=nr, up_pct=beat, med=med, etv=etv, H=H, min_hit=p.get("min_hit_pct", 58.0))
        if starts:
            rdecision, rkind = Decision.new_experiment, GradingKind.theme_call
            rreason = "History says a flip like this often does start a rotation; the experiment tests whether this one sticks."
            rhead = "A laggard sector flipped to the top today, and history says that often sticks"
            rclose = "So this becomes an experiment: track whether the leadership sticks for a week."
        else:
            rdecision, rkind = Decision.reject, GradingKind.rotation_reject
            rreason = "History says a one-day flip like this is usually noise; not chasing it."
            rhead = "A laggard sector flipped to the top today, but history says it is usually noise"
            rclose = "A one-day flip is not a rotation, so I am not chasing it; if it holds for a week, the theme card will say so."
        rbody = (
            f"{fr.token('sector')} was a bottom sector for weeks, ranked {fr.token('window_rank')} of "
            f"{fr.token('sectors')} over the window, and today it flipped to rank {fr.token('today_rank')}. "
            f"That is the kind of turn that sometimes starts a multi-week rotation, and sometimes is one "
            f"day of noise. Across {fr.token('flips')} such flips in history, the sector went on to beat "
            f"the market over the following week {fr.token('beat_market')} of the time, beat it by more than "
            f"costs {fr.token('beat_after_costs')} of the time, with a typical edge of {fr.token('typical_excess')} "
            f"and an expectancy of {fr.token('expectancy')} net of a hurdle of {fr.token('hurdle')} for costs and "
            f"slippage. {rclose} Regime: {fr.token('regime')}."
        )
        rrule = build_rule(rkind, horizon=h, hurdle_pct=H,
                           spec={"sector": s, "members": smem, "window": WIN, "min_names": p["min_names"]},
                           frozen_at=ctx.computed_at, subject=s)
        cards.append(CardDraft(
            template_id="theme_cycle", question="A laggard sector just flipped to the top for a day. Does that start a rotation?",
            subject=s, subject_kind=SubjectKind.sector, level=EvidenceLevel.sector,
            decision=rdecision, decision_reason=rreason,
            facts=fr, key_facts=[fr.id("window_rank"), fr.id("today_rank"), fr.id("beat_market"), fr.id("typical_excess")],
            n=nr,
            comparison_group=f"every past session since {rel.index[0]} when a sector ranked in the bottom four "
                             f"over {WIN} sessions was a top-two sector for the day; sector-minus-market from the "
                             "next open to the horizon close, judged against the coin flip and the cost hurdle",
            grading_rule=rrule, magnitude=min(1.0, 0.5 * int(lrank[s]) / ncol), evidence_z=_z(beat, nr),
            headline=rhead, body=rbody, slug=_fslug(fr),
            evidence_signature=_sig("theme_cycle", "rotation", s, rdecision),
            related_symbols=[], follow_ups=["Did the flip come on broad participation or one large name?"],
        ))
    return cards


THEME_CYCLE = QuestionTemplate(
    id="theme_cycle",
    question="What theme is in play right now, and is it a real rotation or noise?",
    parameters={"window": 15, "min_names": 5, "horizon": 5, "watchlist": 4, "min_hit_pct": 58.0},
    computation=compute_theme_cycle, level=EvidenceLevel.sector,
    grading_kinds=(GradingKind.theme_call, GradingKind.theme_watch, GradingKind.rotation_reject),
    trader_relevance=1.0, source="pathfinder_theme.py + pathfinder_demo.py §5",
    claim_parts=4,      # theme_cycle | leader/rotation | sector | decision ; no statistic
)


# ── the registry — the ONLY source of computations ──────────────────────────

LIBRARY: tuple[QuestionTemplate, ...] = (
    MARKET_REGIME, THEME_CYCLE, DIP, SURGE, VOLUME_ANOMALY, RELATIONSHIP,
)
TEMPLATES: dict[str, QuestionTemplate] = {t.id: t for t in LIBRARY}


def parameters_for(t: QuestionTemplate, cfg: ResearchConfig) -> dict[str, Any]:
    """Template defaults, overridden by the config (founder inputs) where they overlap."""
    p = dict(t.parameters)
    overrides = {
        "market_regime": {"band_pct": cfg.regime_band_pct},
        "dip": {"threshold_pct": cfg.dip_pct, "min_hit_pct": cfg.base_rate_min_hit_pct},
        "surge": {"threshold_pct": cfg.surge_pct, "min_hit_pct": cfg.base_rate_min_hit_pct,
                  "fade_max_hit_pct": cfg.surge_fade_max_hit_pct},
        "volume_anomaly": {"volume_x": cfg.volume_x, "flat_pct": cfg.flat_pct, "move_pct": cfg.move_pct},
        "relationship": {"pairs": list(cfg.pairs), "z_window": cfg.pair_z_window,
                         "z_extreme": cfg.pair_z_extreme, "min_history": cfg.pair_min_history,
                         "horizon": cfg.pair_horizon, "min_hit_pct": cfg.base_rate_min_hit_pct},
        "theme_cycle": {"window": cfg.theme_window, "min_names": cfg.theme_min_names,
                        "horizon": cfg.theme_horizon, "watchlist": cfg.theme_watchlist,
                        "min_hit_pct": cfg.base_rate_min_hit_pct},
    }
    p.update(overrides.get(t.id, {}))
    return p
