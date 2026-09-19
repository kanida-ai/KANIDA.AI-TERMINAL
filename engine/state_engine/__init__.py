"""A leak-proof state-discovery research engine for daily OHLCV panels.

This is a LAB, not a strategy. It is judged by how many discovered states
survive walk-forward validation, not by a backtest equity curve.

Pipeline:
    raw OHLCV
      -> primitives              (features.add_primitives)
      -> feature factory grammar (features.build_features)
      -> graph features          (graph.build_graph_features)
      -> forward labels          (labels.add_labels)          <- only future-aware module
      -> state discovery         (states.fit_state_model)
      -> probability engine      (states.state_stats)
      -> walk-forward + lifecycle(walkforward.run_walkforward)
"""
from .config import Config
from . import data, features, labels, graph, states, walkforward, leakcheck
from . import sqlite_io, intraday
from .pipeline import run_pipeline, prepare_panel

__all__ = ["Config", "data", "features", "labels", "graph", "states",
           "walkforward", "leakcheck", "sqlite_io", "intraday",
           "run_pipeline", "prepare_panel"]
__version__ = "0.1.0"
