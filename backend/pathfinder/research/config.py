"""
Every knob of the research engine, in one place, overridable by environment.

FOUNDER INPUTS (docs/sessions/PATHFINDER_S1_ENGINE.md) are stubbed with the prototype's
defaults and marked `# FOUNDER INPUT` — see docs/handbacks/PF-S1.md for the list.
"""
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

# ── DETERMINISM GUARD (S1 third pass, found while rebuilding the store) ──────────────────
# pandas routes large-frame arithmetic (> 1e6 elements: the forward-return columns
# `_c{h} / next_open - 1` on the 1.25M-row universe) through numexpr when it is installed.
# On this host (numexpr 2.14.1, 12 threads; pandas 2.3.3; numpy 2.3.5) that path
# INTERMITTENTLY returned an all-zero `f5` column: in 4 passes over 13 seals one pass gave
# f5.abs().mean() == 0.0 with 7 more non-NaN rows than every other pass, and two of three
# full rebuilds were refused by the C1 degenerate-share guard ("like_this_beat 0.0% over
# 877", "big_move 0.0% over 13,100"). With numexpr off: 0 mismatches in the same probe and
# two byte-identical rebuilds. The first pass's "smoke store" (`big_move = 0.0%` over
# 13,119 cases, hand-back §5.1) has the same signature and was very likely this, not
# "superseded code". Every number this engine mints must be reproducible, so the numexpr
# and bottleneck backends are OFF for any process that imports the research engine.
# (`test_pathfinder_s1_audit3.py` pins the option; the A-C1 determinism rows pin the effect.)
pd.set_option("compute.use_numexpr", False)
pd.set_option("compute.use_bottleneck", False)

IST = timezone(timedelta(hours=5, minutes=30))
REPO_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_DIR = Path(__file__).resolve().parent

#: R&D price warehouse (read-only). Override with KANIDA_DB.
DEFAULT_PRICE_DB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
#: The research store (editions, findings, grades, scoreboard).
DEFAULT_RESEARCH_DB = str(REPO_ROOT / "var" / "pathfinder_research.db")


def code_hash(*paths: Path) -> str:
    """
    Content hash of the given source files (sorted by name) — 12 hex chars.

    S1 audit C1: a run must be attributable to the code that produced it. A semantic
    version string is a promise; a content hash is a fact. The engine version stamped on
    every edition and every `computed_by` carries the hash of `research/*.py`, and the
    grading-rule version carries the hash of `grading.py` (P2).
    """
    h = hashlib.sha256()
    for p in sorted(paths, key=lambda x: x.name):
        h.update(p.name.encode())
        h.update(b"\0")
        h.update(p.read_bytes())
        h.update(b"\0")
    return h.hexdigest()[:12]


def research_code_hash() -> str:
    return code_hash(*sorted(RESEARCH_DIR.glob("*.py")))


ENGINE_SEMVER = "1.3.0"
ENGINE_VERSION = f"pathfinder_research@{ENGINE_SEMVER}+code.{research_code_hash()}"
GRADING_RULES_SEMVER = "1.3.0"           # the hashed form lives in grading.py (P2)
RANKING_VERSION = "usefulness@1.0.0"


def now_ist() -> datetime:
    return datetime.now(IST)


def _f(env: str, default: float) -> float:
    return float(os.environ.get(env, default))


def _pairs_from_env(default: tuple[tuple[str, str], ...]) -> tuple[tuple[str, str], ...]:
    raw = os.environ.get("KANIDA_PF_PAIRS")
    if not raw:
        return default
    out = []
    for item in raw.split(";"):
        a, b = item.split(",")
        out.append((a.strip(), b.strip()))
    return tuple(out)


@dataclass(frozen=True)
class ResearchConfig:
    # ── data ────────────────────────────────────────────────────────────────
    price_db: str = field(default_factory=lambda: os.environ.get("KANIDA_DB", DEFAULT_PRICE_DB))
    research_db: str = field(
        default_factory=lambda: os.environ.get("KANIDA_PATHFINDER_RESEARCH_DB", DEFAULT_RESEARCH_DB))
    #: S1 second audit A4 — the honest label. The warehouse is split/bonus-adjusted; demergers
    #: (CGPOWER 2016-03-15 −71.7%, TATACHEM 2020-03-04 −56.2%, ABFRL 2025-05-22 −55.9%,
    #: ADANIENT 2015-06-03 −41.9%) and some split-like events (JBCHEPHARM 2023-09-18,
    #: SPLPETRO 2022-06-07, −49/−50%) are NOT adjusted. Corporate-action days are excluded
    #: from every base rate and from subject selection (see data.py); the exclusion is on
    #: every card's provenance.
    data_source: str = ("kanida_falcon.ohlc_daily (NSE EOD; split/bonus-adjusted; demergers and some "
                        "other corporate actions UNADJUSTED — corporate-action days excluded, see provenance)")
    universe_id: str = "nifty500_current_membership (in_nifty500=1 AND is_active=1)"
    #: A4 — exclude split / bonus / demerger / rights ex-dates (the warehouse's `corp_actions`
    #: table, NSE, 2020-01 →) and any single-day |close-to-close| move beyond
    #: `corp_action_ret_guard_pct` (flagged as a SUSPECTED corporate action; the table starts
    #: in 2020 and the four named demergers before it print −42% to −72%).
    exclude_corp_actions: bool = True
    corp_action_ret_guard_pct: float = 30.0
    #: A7 — a bar whose open equals its close to the tick is treated as a SYNTHETIC open (a
    #: placeholder the feed filled from the close: 1.5% of bars, 4.2% in 2013, 0.24% in 2026).
    #: The next-open entry is not a verifiable price there, so every forward outcome that
    #: would enter at it is NaN (unresolved). Before this the dip card's "typical next-session
    #: move" was exactly 0.0% — the median sat on the pile of zero outcomes.
    exclude_synthetic_opens: bool = True
    #: First bar considered. The warehouse starts 2013-01-01.
    history_start: str = "2013-01-01"
    index_symbol: str = "NIFTY 50"
    vix_symbol: str = "INDIA VIX"

    # ── the cost hurdle (FOUNDER INPUT — prototype default) ─────────────────
    #: Round-trip transaction-cost hurdle, % of notional. A base rate whose typical
    #: net move does not clear this is "NO TRADE"; a graded outcome inside ±hurdle is
    #: "Inconclusive". pathfinder_demo.py uses 0.30.
    cost_hurdle_pct: float = field(default_factory=lambda: _f("KANIDA_PF_COST_HURDLE_PCT", 0.30))
    #: FOUNDER INPUT (S1 second audit A5) — slippage, % of notional, EACH WAY. The prototype
    #: had no slippage term though the contract (`Provenance.cost_convention`) promised one.
    #: Stub: 0.10% each way. The hurdle every decision and every grade is judged against is
    #: `hurdle_pct` = cost_hurdle_pct + 2 * slippage_pct.
    slippage_pct: float = field(default_factory=lambda: _f("KANIDA_PF_SLIPPAGE_PCT", 0.10))

    # ── usefulness threshold (FOUNDER INPUT) ────────────────────────────────
    #: Publish only cards whose usefulness clears this. NO minimum count, NO padding.
    usefulness_threshold: float = field(
        default_factory=lambda: _f("KANIDA_PF_USEFULNESS_THRESHOLD", 0.65))
    #: The first N published findings are "what matters now".
    what_matters_now: int = 3
    #: FOUNDER INPUT (S1 audit C4) — the smallest lift over the unconditional base rate,
    #: in percentage points, that makes an anomaly worth an experiment. Below it the card
    #: is a null result and says so.
    anomaly_min_lift_pp: float = field(default_factory=lambda: _f("KANIDA_PF_ANOMALY_MIN_LIFT_PP", 5.0))
    #: S1 audit P5 — expectancy (mean net of hurdle) is minted on every group base rate and
    #: gates the decision. The mean is winsorised at this percentile either side because the
    #: warehouse carries a handful of listing-day glitch bars (see data.py) and a single
    #: 800x "return" would otherwise fabricate the mean.
    expectancy_winsor_pct: float = 1.0
    #: Novelty: a (template, subject, decision) already published within this many
    #: editions is discounted.
    novelty_lookback_editions: int = 5
    #: S1 third audit N2 — a group card's CLAIM is (template, decision, comparison group); a
    #: fresh reading of the same statistic is the same claim unless the primary statistic
    #: moved by more than this many percentage points. A rounded signature ("|-1.20" vs
    #: "|-1.10") re-opened one null claim three times and graded it three times.
    claim_tolerance_pp: float = 1.0

    # ── template parameters (ported verbatim from the prototypes) ───────────
    dip_pct: float = -6.0            # a "hard dip" is a close-to-close drop of 6%+
    surge_pct: float = 6.0           # a "big surge" is a close-to-close jump of 6%+
    base_rate_min_hit_pct: float = 58.0   # the hit-rate bar a base rate must clear to be a call
    surge_fade_max_hit_pct: float = 45.0  # below this hit-rate a surge is a fade candidate
    volume_x: float = 3.0            # volume anomaly = ≥3x the 20-session average
    flat_pct: float = 1.5            # ...while closing within ±1.5%
    move_pct: float = 3.0            # the "big move within the week" the anomaly is judged on
    regime_band_pct: float = 0.2     # "comparable sessions" = market move within ±0.2%
    theme_window: int = 15           # sessions that define "a cycle"
    theme_min_names: int = 5         # sectors with fewer names are ignored
    theme_horizon: int = 5           # persistence horizon (sessions)
    theme_watchlist: int = 4
    pair_z_window: int = 60
    pair_z_extreme: float = 2.0
    pair_min_history: int = 160
    pair_horizon: int = 5
    #: FOUNDER INPUT — the pair list. The prototype's TATAMOTORS no longer trades under
    #: that symbol (demerged 2025; passenger vehicles = TMPV), so TMPV stands in.
    pairs: tuple[tuple[str, str], ...] = field(default_factory=lambda: _pairs_from_env((
        ("TMPV", "M&M"), ("HDFCBANK", "ICICIBANK"), ("INFY", "TCS"), ("SBIN", "BANKBARODA"))))

    # ── identity ────────────────────────────────────────────────────────────
    engine_version: str = ENGINE_VERSION

    def component(self, name: str) -> str:
        """`computed_by` for a deterministic component, pinned to the code hash. Never names a model."""
        return f"{name}@{self.engine_version.split('@', 1)[1]}"

    @property
    def code_hash(self) -> str:
        return self.engine_version.rsplit("+code.", 1)[-1]

    @property
    def hurdle_pct(self) -> float:
        """The round-trip hurdle every decision and grade is judged against: costs + slippage both ways."""
        return round(self.cost_hurdle_pct + 2.0 * self.slippage_pct, 6)

    @property
    def cost_convention(self) -> str:
        return (
            f"pf_cost_hurdle_v2: {self.hurdle_pct:.2f}% round-trip hurdle = {self.cost_hurdle_pct:.2f}% "
            f"costs + {self.slippage_pct:.2f}% slippage each way (prototype cost default, slippage stub; "
            "founder inputs pending); entry = next session's open after the signal close, exit = close "
            "of the horizon session; a result inside ±hurdle is Inconclusive"
        )

    @property
    def data_disclosures(self) -> list[str]:
        """User-facing sentences carried on every finding's provenance (A4, A7, A8)."""
        out = [
            "Survivorship: the universe is today's Nifty-500 membership with today's sector labels applied "
            "to the whole history; it holds no delisted name, so absolute base rates flatter the past.",
            "Prices are split/bonus-adjusted only; demergers and some other corporate actions are not adjusted.",
        ]
        if self.exclude_corp_actions:
            out.append(
                "Corporate-action days are excluded from every base rate and from subject selection: split, "
                "bonus, demerger and rights ex-dates from the NSE corporate-action table, plus any single-day "
                f"move beyond {self.corp_action_ret_guard_pct:.0f}% flagged as a suspected corporate action.")
        if self.exclude_synthetic_opens:
            out.append(
                "A bar whose open equals its close to the tick is treated as a synthetic open; no outcome is "
                "measured from an entry at such an open.")
        return out


def load_config() -> ResearchConfig:
    return ResearchConfig()
