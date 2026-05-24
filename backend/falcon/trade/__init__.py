"""Falcon Auto-Trade — Phase 1 pre-market entry module.

Reads top-N signals from falcon_signals_live, generates an MTF batch order plan,
places via Kite, surfaces holdings overlaps, and provides a kill switch.

Operator-supervised. NOT an unattended algo.
"""
__version__ = "0.3.0"
