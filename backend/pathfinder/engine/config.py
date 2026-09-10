"""
Pathfinder engine configuration — every knob in one place, all overridable by env.

Windows are the spine of the honesty story, so they are declared here and nowhere
else:

    DISCOVERY   [discovery_start, discovery_end]   the ONLY window a rule may be chosen on
    VALIDATION  [validation_start, validation_end] out-of-sample gate; never used to choose
    SEALED      [seal_start, as_of]                the virtual book. Sealed at experiment
                                                   open: no candidate was ranked, filtered
                                                   or tuned using a single bar from it.

`market.PriceFrames` enforces the seal mechanically — a discovery-phase frame
physically cannot see a bar after `discovery_end`.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

IST = timezone(timedelta(hours=5, minutes=30))

REPO_ROOT = Path(__file__).resolve().parents[3]

#: R&D price warehouse. Read-only. Kanida_Falcon is the engine-room (CLAUDE.md).
DEFAULT_PRICE_DB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"

#: Where the authoritative Pathfinder tables live for a local run.
DEFAULT_RUN_DB = str(REPO_ROOT / "var" / "pathfinder.db")


def now_ist() -> datetime:
    """Every timestamp in this product is IST, computed explicitly."""
    return datetime.now(IST)


def today_ist() -> date:
    return now_ist().date()


def _d(env: str, default: str) -> date:
    return date.fromisoformat(os.environ.get(env, default))


@dataclass(frozen=True)
class EngineConfig:
    # ── data ────────────────────────────────────────────────────────────────
    price_db: str = field(default_factory=lambda: os.environ.get("KANIDA_PATHFINDER_PRICE_DB", DEFAULT_PRICE_DB))
    run_db: str = field(default_factory=lambda: os.environ.get("KANIDA_PATHFINDER_DB", DEFAULT_RUN_DB))
    data_source: str = "kanida_falcon.ohlc_daily (NSE EOD, corporate-action back-adjusted)"
    universe_id: str = "nse_nifty500_master_liquidity_pit"
    index_symbol: str = "NIFTY 50"

    # ── windows ─────────────────────────────────────────────────────────────
    history_start: date = field(default_factory=lambda: _d("KANIDA_PATHFINDER_HISTORY_START", "2015-01-01"))
    discovery_start: date = field(default_factory=lambda: _d("KANIDA_PATHFINDER_DISCOVERY_START", "2016-01-01"))
    discovery_end: date = field(default_factory=lambda: _d("KANIDA_PATHFINDER_DISCOVERY_END", "2021-12-31"))
    validation_start: date = field(default_factory=lambda: _d("KANIDA_PATHFINDER_VALIDATION_START", "2022-01-01"))
    validation_end: date = field(default_factory=lambda: _d("KANIDA_PATHFINDER_VALIDATION_END", "2024-06-30"))
    seal_start: date = field(default_factory=lambda: _d("KANIDA_PATHFINDER_SEAL_START", "2024-07-01"))
    #: Ceiling for everything. Defaults to the last bar in the price warehouse.
    as_of: date | None = field(
        default_factory=lambda: (
            date.fromisoformat(os.environ["KANIDA_PATHFINDER_AS_OF"])
            if os.environ.get("KANIDA_PATHFINDER_AS_OF") else None
        )
    )

    # ── universe liquidity filter (computed point-in-time, at every signal) ──
    min_median_turnover_inr: float = 5.0e7      # ₹5 crore median 20-session turnover
    turnover_lookback: int = 20
    #: DISABLED. A floor on a back-adjusted price is a floor on future split factors.
    #: See market.liquid_mask. Turnover is point-in-time and sufficient.
    min_price_inr: float = 0.0

    # ── virtual book ────────────────────────────────────────────────────────
    capital_inr: float = 10_00_000.0            # ₹10 lakh of VIRTUAL money
    risk_fraction_per_trade: float = 0.10       # 10% of capital per position
    max_concurrent_positions: int = 10

    # ── gauntlet (docs/STRATEGY_METHODOLOGY.md §1) ──────────────────────────
    min_n_to_promote: int = 50
    observation_review_threshold: int = 30      # the LLM wakes at 30 new observations
    #: Cheap null for RANKING the scan. It is not the significance gate — see
    #: `placebo_draws_gate`, which has to resolve a Bonferroni-adjusted bar.
    placebo_draws: int = 200
    #: The gate's null. An empirical p cannot resolve below 1/draws, so a gate at
    #: alpha/288 ≈ 1.7e-4 is unevidenced at 200 draws and would pass everything whose
    #: p merely rounds to zero.
    placebo_draws_gate: int = 6000
    rng_seed: int = 20260908

    # ── governance ──────────────────────────────────────────────────────────
    constitution_path: str = field(
        default_factory=lambda: os.environ.get(
            "KANIDA_PATHFINDER_CONSTITUTION",
            str(REPO_ROOT / "config" / "pathfinder_constitution.yaml"),
        )
    )

    # ── engine identity, stamped into every `computed_by` ───────────────────
    engine_version: str = "pathfinder_engine@1.0.0"

    def component(self, name: str) -> str:
        """`computed_by` for a deterministic component. Never names a model."""
        return f"{name}@{self.engine_version.split('@', 1)[1]}"

    def validate(self) -> None:
        """The windows must not overlap, or the seal means nothing."""
        if not (self.history_start <= self.discovery_start < self.discovery_end
                < self.validation_start < self.validation_end < self.seal_start):
            raise ValueError("engine windows must be strictly ordered and non-overlapping")
        if self.as_of is not None and self.as_of <= self.seal_start:
            raise ValueError("as_of must be after seal_start — there would be no book window")


def load_config() -> EngineConfig:
    cfg = EngineConfig()
    cfg.validate()
    return cfg
