"""Central configuration. Everything tunable lives here so experiments are reproducible."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import List
import json


@dataclass
class Config:
    # ---------- target definition ----------
    side: str = "long"             # long | short  -- mined SEPARATELY, never pooled.
    # Long and short are different trades with different base rates and different
    # states. Running side='short' flips the excursion definitions so the whole
    # engine downstream is unchanged; it does not simply negate the long signal.
    use_timeframes: bool = False   # weekly + monthly point-in-time structure
    horizons: List[int] = field(default_factory=lambda: [1, 2, 3, 5, 10])
    structure: str = "bracket"     # bracket | hybrid
    # 'hybrid' = disaster stop only, otherwise exit at the close. Use it when the
    # symbol has high RETENTION (it holds direction into the close): a target then
    # caps the winners while the stop still cuts the losers.
    cost_pct: float = 0.0011       # round-trip cost. Indian intraday equity is
    # ~11bps all-in for retail (brokerage + STT 2.5bps sell side + exchange + GST
    # + stamp), BEFORE slippage. The 5bps default used elsewhere is optimistic.
    horizon_days: int = 1          # predict over T+1 .. T+horizon
    target_pct: float = 0.01       # +1%
    stop_pct: float = 0.005        # -0.5%
    label_mode: str = "conservative"  # conservative | optimistic | close
    # conservative: hit = MFE >= target AND MAE never breached -stop  (daily bars cannot
    #               resolve touch ORDER, so this is the pessimistic reading)
    # optimistic:   hit = MFE >= target (ignores stop)
    # close:        hit = close-to-close return >= target

    # ---------- feature factory grammar ----------
    windows: List[int] = field(default_factory=lambda: [3, 5, 10, 20, 60])
    operators: List[str] = field(
        default_factory=lambda: ["z", "pctrank", "slope", "ratio", "accel", "distmax", "distmin"]
    )
    bases: List[str] = field(
        default_factory=lambda: [
            "close", "volume", "ret1", "range_pct", "clv", "gap",
            "tr_pct", "rel_str", "vwap_dist", "dollar_vol",
        ]
    )
    use_interactions: bool = True
    interaction_top_k: int = 12    # pairwise products built from top-K surviving features

    # ---------- graph layer ----------
    use_graph: bool = True
    graph_lookback: int = 120      # trailing days of returns used to build edges
    graph_rebuild_days: int = 21   # rebuild cadence (walk-forward, never global)
    graph_max_neighbours: int = 8
    graph_min_corr: float = 0.30
    graph_min_lead_corr: float = 0.08   # lag-1 correlations are small by nature

    # ---------- state discovery ----------
    n_bins: int = 5                # quantile bins per feature (edges fit on TRAIN only)
    min_feature_coverage: float = 0.95  # a feature must be non-NaN this often in TRAIN.
    # This is the guard that stops a low-coverage intraday feature (only available
    # from May 2024) from silently discarding your entire 2022-2024 history the
    # moment it gets selected into a state signature.
    state_features: int = 3        # how many features combine into one state signature
    max_corr_between_selected: float = 0.70
    min_support_train: int = 60    # min occurrences of a state in the training window
    min_support_test: int = 8      # min occurrences before an OOS fold verdict counts
    min_lift: float = 0.03         # state hit rate must beat base rate by this margin
    wilson_z: float = 1.64         # ~90% one-sided lower bound
    state_method: str = "grid"     # grid | tree
    tree_max_leaves: int = 32
    tree_min_samples_leaf: int = 80

    # ---------- walk-forward ----------
    train_months_min: int = 12     # minimum training history before first test month
    test_months: int = 1           # length of each out-of-sample block
    expanding: bool = True         # True = expanding window, False = rolling
    rolling_train_months: int = 24 # only used when expanding=False

    # ---------- lifecycle ----------
    promote_after: int = 2         # consecutive passing folds -> validated
    production_after: int = 4      # consecutive passing folds -> production
    retire_after: int = 3          # consecutive failing folds -> retired

    # ---------- misc ----------
    seed: int = 7
    min_price: float = 5.0
    min_dollar_vol: float = 0.0

    def to_json(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(asdict(self), f, indent=2)

    @staticmethod
    def from_json(path: str) -> "Config":
        with open(path) as f:
            return Config(**json.load(f))
