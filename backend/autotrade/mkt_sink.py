"""LEG 3.a — the SINGLE default-off resolver for the live market-data sink DB.

R&D-DB split, leg 3.a (the SAFE, additive half). The 38 GB research DB
(`universe_engine/data/db/kanida_universe.db`) fuses the research panel with the
live market-data sink the pollers write and execution reads. This module is the
ONE place that decides *which file* those live reads/writes hit, so read-source
and write-target move together as a SINGLE switch.

ENV (both default-OFF — unset = byte-identical to today):

  FALCON_MKT_SINK_DB
      A path to the production market-data sink. When SET it overrides the DB
      path used by BOTH the execution reads (`worked_order.recent_interval_volume`
      + Tesla's `connect_db_readonly`) AND the pollers' write target
      (`mkt_poller` / `mkt_reference` / `mkt_backfill_ohlc`). UNSET → each caller's
      own current default (the 38 GB R&D file) — the reads/writes are unchanged,
      byte-for-byte. This is the atomic move-together property: you can never
      point execution at the sink while leaving the pollers on R&D, or vice versa.

  FALCON_MKT_PROFILE_ARTIFACT
      OPTIONAL explicit path to the precomputed `ohlc_1min` volume-PROFILE
      artifact DB (`scripts/publish_volume_profile.py`). When UNSET the profile is
      read from the sink DB itself (the `mkt_intraday_profile` table lives in the
      sink per the design). This env only lets an operator keep the profile in a
      standalone file. It is only consulted when FALCON_MKT_SINK_DB is set (the
      profile read is gated by the SAME sink switch — see below).

PRECEDENCE (execution reads):
  1. An EXPLICIT `db_path` argument passed by the caller ALWAYS wins (tests, and
     Tesla's per-session `config.tesla_signal_db_path`). This preserves Tesla's
     existing per-session override — session config beats the env.
  2. Else, if FALCON_MKT_SINK_DB is set → the sink.
  3. Else → the caller's own current default (the R&D DB) = today's behavior.

FAIL-SAFE (unchanged by this module): with the sink env SET but the sink/artifact
missing or empty, the execution consumers still degrade fail-safe —
`worked_order` returns None → flat-POV / TWAP-floor pacing (the order still
completes, same target size); Tesla finds no candidates → no signal → no trade
(it never breaks an exit). The kill switch and per-position GTT never read the
sink at all (they run off live LTP / broker state), so basket safety is
untouched. This module changes ONLY path resolution — no decision, sizing, or
pacing math changes.

NOTHING here is imported at module load by anything that changes default
behavior: every read of the env happens per-call, so flipping the env (leg 3.b)
takes effect on the next call without touching this default-off code path.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Union

ENV_SINK_DB = "FALCON_MKT_SINK_DB"
ENV_PROFILE_ARTIFACT = "FALCON_MKT_PROFILE_ARTIFACT"

# The profile artifact table + manifest names (also written by the publisher).
PROFILE_TABLE = "mkt_intraday_profile"
PROFILE_MANIFEST_TABLE = "mkt_intraday_profile_manifest"


def _env_path(name: str) -> Optional[Path]:
    v = os.environ.get(name)
    if v is not None:
        v = v.strip()
    return Path(v) if v else None


def sink_db_override() -> Optional[Path]:
    """The sink path from FALCON_MKT_SINK_DB, or None when unset/blank (default).

    None is the entire default-off safety property: unset → callers keep their own
    current path, byte-identical to today."""
    return _env_path(ENV_SINK_DB)


def sink_enabled() -> bool:
    """True iff FALCON_MKT_SINK_DB is set (the sink switch is ON)."""
    return sink_db_override() is not None


def resolve_sink_db(default: Union[str, Path]) -> Path:
    """The effective DB path for a caller whose current default is `default`.

    FALCON_MKT_SINK_DB set → the sink; unset → `default` unchanged. Each caller
    passes ITS OWN current default so unset is provably byte-identical to today."""
    ov = sink_db_override()
    return ov if ov is not None else Path(default)


def profile_artifact_db() -> Optional[Path]:
    """Where `load_intraday_profile` should read the volume-profile artifact when
    the sink is ON: an explicit FALCON_MKT_PROFILE_ARTIFACT path if set, else the
    sink DB itself (the `mkt_intraday_profile` table lives in the sink per design).
    None when the sink switch is OFF (→ the caller reads `ohlc_1min` as today)."""
    explicit = _env_path(ENV_PROFILE_ARTIFACT)
    if explicit is not None:
        return explicit
    return sink_db_override()
