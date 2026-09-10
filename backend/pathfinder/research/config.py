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


ENGINE_SEMVER = "1.1.0"
ENGINE_VERSION = f"pathfinder_research@{ENGINE_SEMVER}+code.{research_code_hash()}"
GRADING_RULES_SEMVER = "1.1.0"           # the hashed form lives in grading.py (P2)
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
    data_source: str = "kanida_falcon.ohlc_daily (NSE EOD, corporate-action back-adjusted)"
    universe_id: str = "nifty500_current_membership (in_nifty500=1 AND is_active=1)"
    #: First bar considered. The warehouse starts 2013-01-01.
    history_start: str = "2013-01-01"
    index_symbol: str = "NIFTY 50"
    vix_symbol: str = "INDIA VIX"

    # ── the cost hurdle (FOUNDER INPUT — prototype default) ─────────────────
    #: Round-trip transaction-cost hurdle, % of notional. A base rate whose typical
    #: net move does not clear this is "NO TRADE"; a graded outcome inside ±hurdle is
    #: "Inconclusive". pathfinder_demo.py uses 0.30.
    cost_hurdle_pct: float = field(default_factory=lambda: _f("KANIDA_PF_COST_HURDLE_PCT", 0.30))

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
    def cost_convention(self) -> str:
        return (
            f"pf_cost_hurdle_v1: {self.cost_hurdle_pct:.2f}% round-trip hurdle (prototype "
            "default; founder input pending); entry = next session's open after the signal "
            "close, exit = close of the horizon session; a result inside ±hurdle is Inconclusive"
        )


def load_config() -> ResearchConfig:
    return ResearchConfig()
