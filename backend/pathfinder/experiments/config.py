"""
Every knob of the experiment loop, in one place, overridable by environment.

FOUNDER INPUTS are stubbed and marked `# FOUNDER INPUT` — see docs/handbacks/PF-S2.md §6.
Where the UNSIGNED Constitution draft (config/pathfinder_constitution.yaml) already states a
number (virtual capital, position limits, the gauntlet bars, the LLM budget) the loop reads it
from there and nowhere else, so the founder changes one document.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from ..research.config import IST, REPO_ROOT, ResearchConfig, code_hash, now_ist  # noqa: F401  (re-exported)

EXPERIMENTS_DIR = Path(__file__).resolve().parent
DEFAULT_EXPERIMENTS_DB = str(REPO_ROOT / "var" / "pathfinder_experiments.db")
DEFAULT_CONSTITUTION = str(REPO_ROOT / "config" / "pathfinder_constitution.yaml")


def experiments_code_hash() -> str:
    return code_hash(*sorted(EXPERIMENTS_DIR.glob("*.py")))


ENGINE_SEMVER = "1.0.0"
ENGINE_VERSION = f"pathfinder_experiments@{ENGINE_SEMVER}+code.{experiments_code_hash()}"


def _f(env: str, default: float) -> float:
    return float(os.environ.get(env, default))


def _i(env: str, default: int) -> int:
    return int(os.environ.get(env, default))


@dataclass(frozen=True)
class ExperimentConfig:
    #: The registry (experiments, versions, periods, marks, trades, outcomes, proposals).
    experiments_db: str = field(
        default_factory=lambda: os.environ.get("KANIDA_PATHFINDER_EXPERIMENTS_DB", DEFAULT_EXPERIMENTS_DB))
    constitution_path: str = field(
        default_factory=lambda: os.environ.get("KANIDA_PATHFINDER_CONSTITUTION", DEFAULT_CONSTITUTION))

    # ── the research split (NDP: mine on the past, keep only what held after) ──────────
    #: FOUNDER INPUT — the last session of the DISCOVERY window (`mine_phase1.py` TRAIN_MAX =
    #: 2024). Everything after it up to the opening seal is the TRAILING VALIDATION window: a
    #: variant is worth testing only if it clears the gauntlet on the whole sealed history AND
    #: on this trailing window on its own (promote-only-if-it-holds). The pre-2025 window's own
    #: numbers are recorded and an advisory gate says whether the edge stood there alone.
    discovery_end: str = field(default_factory=lambda: os.environ.get("KANIDA_PFX_DISCOVERY_END", "2024-12-31"))

    # ── the forward period ──────────────────────────────────────────────────────────────
    #: FOUNDER INPUT — a version is tracked in periods of this many SIGNAL sessions; the period
    #: is graded once every trade it opened has resolved (period end + horizon).
    period_sessions: int = field(default_factory=lambda: _i("KANIDA_PFX_PERIOD_SESSIONS", 10))
    #: FOUNDER INPUT — a period with fewer closed trades than this is Inconclusive, never judged.
    min_trades_to_grade: int = field(default_factory=lambda: _i("KANIDA_PFX_MIN_TRADES_TO_GRADE", 5))

    # ── the registry's memory: versions, trials, retirement ────────────────────────────
    #: FOUNDER INPUT — the retirement rule (spec addendum 3): an idea that would need a version
    #: beyond this is BURIED with a post-mortem. v1 -> v2 -> v3 then the grave.
    max_versions: int = field(default_factory=lambda: _i("KANIDA_PFX_MAX_VERSIONS", 3))

    # ── the worth-testing gate ─────────────────────────────────────────────────────────
    #: FOUNDER INPUT — the S1 card's evidence strength (its usefulness component) must clear this.
    min_evidence_strength: float = field(default_factory=lambda: _f("KANIDA_PFX_MIN_EVIDENCE_STRENGTH", 0.5))
    #: FOUNDER INPUT — the smallest sample a variant may be opened on, on the whole history and
    #: on the trailing window. Below it the number is labelled and the variant is not tested.
    min_n_history: int = field(default_factory=lambda: _i("KANIDA_PFX_MIN_N_HISTORY", 100))
    min_n_trailing: int = field(default_factory=lambda: _i("KANIDA_PFX_MIN_N_TRAILING", 30))
    #: FOUNDER INPUT — a family the gate declined is not researched again on the next card that
    #: repeats the claim: re-testing one hypothesis every day on nearly the same data is p-hacking
    #: by repetition. It may be re-evaluated after this many sessions.
    retry_after_sessions: int = field(default_factory=lambda: _i("KANIDA_PFX_RETRY_AFTER_SESSIONS", 60))
    #: Day-blocked placebo draws (the null the gauntlet's `max_placebo_p_value` is judged on).
    placebo_draws: int = field(default_factory=lambda: _i("KANIDA_PFX_PLACEBO_DRAWS", 1000))
    #: Re-audit N8 — when the p sits within two binomial standard errors of the bar, the null is
    #: re-drawn to this many draws before the gate reads it (1,000 draws put ±0.009 around 0.05).
    placebo_draws_near_bar: int = field(default_factory=lambda: _i("KANIDA_PFX_PLACEBO_DRAWS_NEAR_BAR", 5000))
    #: FOUNDER INPUT (re-audit N2/N5) — the fewest independent signal days the FORWARD record must
    #: hold before its fixed-day permutation null and its cluster t are read at all; below it the
    #: graduation gate is `insufficient`, never passed. The frozen cumulative kill keeps its own
    #: floor (`grading.CUMULATIVE_MIN_SIGNAL_DAYS`).
    min_forward_signal_days: int = field(default_factory=lambda: _i("KANIDA_PFX_MIN_FORWARD_SIGNAL_DAYS", 5))
    rng_seed: int = 20260910

    # ── identity ────────────────────────────────────────────────────────────
    engine_version: str = ENGINE_VERSION

    @property
    def code_hash(self) -> str:
        return self.engine_version.rsplit("+code.", 1)[-1]

    def fact_cfg(self, rcfg: ResearchConfig) -> ResearchConfig:
        """The research config with THIS engine's version, so every S2 fact is pinned to S2's code hash."""
        return replace(rcfg, engine_version=self.engine_version)

    def as_params(self) -> dict[str, Any]:
        return {
            "discovery_end": self.discovery_end, "period_sessions": self.period_sessions,
            "min_trades_to_grade": self.min_trades_to_grade, "max_versions": self.max_versions,
            "min_evidence_strength": self.min_evidence_strength, "min_n_history": self.min_n_history,
            "min_n_trailing": self.min_n_trailing, "placebo_draws": self.placebo_draws, "rng_seed": self.rng_seed,
            "retry_after_sessions": self.retry_after_sessions, "placebo_draws_near_bar": self.placebo_draws_near_bar,
            "min_forward_signal_days": self.min_forward_signal_days,
        }


def load_experiment_config() -> ExperimentConfig:
    return ExperimentConfig()
