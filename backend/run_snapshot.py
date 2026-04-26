"""
KANIDA — Snapshot Runner
========================
Refreshes agent_signal_snapshots so the API has fresh data.

Usage:
  python run_snapshot.py --status              # check freshness
  python run_snapshot.py --market NSE          # refresh NSE (all biases)
  python run_snapshot.py --market US           # refresh US (all biases)
  python run_snapshot.py --all                 # refresh everything
  python run_snapshot.py --market NSE --bias bullish  # single combo
  python run_snapshot.py --errors              # show recent errors
"""

import os, sys

# Ensure agents/ folder is on the path
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "agents"))

# Set KANIDA_DB_PATH so all modules find the correct DB
_DB = os.path.normpath(os.path.join(_HERE, "..", "data", "db", "kanida_fingerprints.db"))
os.environ.setdefault("KANIDA_DB_PATH", _DB)

from snapshot_builder import main

if __name__ == "__main__":
    main()
