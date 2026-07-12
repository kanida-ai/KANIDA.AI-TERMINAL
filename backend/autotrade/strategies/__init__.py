"""AutoTrade live-decision strategy engines (additive, default-off).

Each module here is a SIGNAL / DECISION layer only — it NEVER places or exits an
order. Execution always goes through the hardened session/_place_one /
_exit_single_position / kill_switch paths so every C1–C10 safety guarantee holds.

Current engines:
- tesla_features    : vendored order-flow microstructure features (faithful copy
                      of the batch falcons_tesla_probe.py, attribution inside).
- tesla_short_engine: live inference of the "Falcon Tesla" order-flow-native
                      intraday SHORT signal (A++/A+++), ported from the batch
                      falcons_tesla_short_engine_v2.py with a ROLLING personality
                      window instead of a hardcoded train range.
- tesla_rotation    : PURE seat/capital-rotation decision layer (no I/O, no
                      orders) consumed by the tesla_short AutoTrade strategy.
"""
