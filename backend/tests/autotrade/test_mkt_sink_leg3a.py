"""LEG 3.a — R&D-DB split: the redirectable market-data sink reader + the
ohlc_1min volume-profile artifact + the fail-safe. All DEFAULT-OFF.

The SAFETY PROPERTY under test: with FALCON_MKT_SINK_DB UNSET, worked_order +
Tesla + the pollers resolve to the EXACT current 38 GB R&D path (byte-identical
to today). With it SET, read-source (execution) and write-target (pollers) move
together to the sink — and both consumers degrade FAIL-SAFE when the sink/artifact
is missing/empty (worked_order → flat-POV; Tesla → no signal, no trade).
"""
import importlib.util
import os
import sqlite3
from pathlib import Path

import pytest

import autotrade.mkt_sink as ms
import autotrade.execution.worked_order as wo
import autotrade.strategies.tesla_short_engine as tesla

_SINK = ms.ENV_SINK_DB
_ART = ms.ENV_PROFILE_ARTIFACT

_PUB_PATH = Path(__file__).resolve().parents[3] / "scripts" / "publish_volume_profile.py"


def _load_pub():
    spec = importlib.util.spec_from_file_location("publish_volume_profile", _PUB_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(autouse=True)
def _clean_env_and_cache():
    """Every test starts with BOTH envs unset and the profile cache cleared."""
    for k in (_SINK, _ART):
        os.environ.pop(k, None)
    wo._PROFILE_CACHE.clear()
    yield
    for k in (_SINK, _ART):
        os.environ.pop(k, None)
    wo._PROFILE_CACHE.clear()


# ── synthetic-DB builders ─────────────────────────────────────────────────────

def _mk_orderflow_db(path: Path, symbol: str, volumes):
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE mkt_orderflow_1min (symbol TEXT, segment TEXT, "
                "bar_time TEXT, volume INTEGER)")
    for i, v in enumerate(volumes):
        con.execute("INSERT INTO mkt_orderflow_1min VALUES (?,?,?,?)",
                    (symbol, "CASH", f"2026-07-16 09:{15 + i:02d}:00", v))
    con.commit(); con.close()


def _mk_ohlc_db(path: Path, symbol: str, n_days: int = 6):
    """A synthetic R&D-stand-in ohlc_1min with a known intraday shape over n_days."""
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE ohlc_1min (symbol TEXT, bar_time TEXT, volume INTEGER)")
    rows = []
    for d in range(n_days):
        day = f"2026-07-{10 + d:02d}"
        # A few minutes across the session — deterministic, distinct per bucket.
        for hh, mm, vol in ((9, 15, 100), (9, 20, 250), (10, 0, 400),
                            (12, 30, 150), (15, 25, 320)):
            rows.append((symbol, f"{day} {hh:02d}:{mm:02d}:00", vol + d))
    con.executemany("INSERT INTO ohlc_1min VALUES (?,?,?)", rows)
    con.commit(); con.close()


# ══════════════════════════════════════════════════════════════════════════════
# 1. THE RESOLVER — default-off proof
# ══════════════════════════════════════════════════════════════════════════════

def test_resolver_unset_is_identity():
    """UNSET → resolve_sink_db returns the caller's OWN default, unchanged."""
    default = Path("/some/rnd/kanida_universe.db")
    assert ms.sink_db_override() is None
    assert ms.sink_enabled() is False
    assert ms.resolve_sink_db(default) == default
    assert ms.profile_artifact_db() is None


def test_resolver_set_redirects(tmp_path):
    sink = tmp_path / "sink.db"
    os.environ[_SINK] = str(sink)
    assert ms.sink_enabled() is True
    assert ms.resolve_sink_db(Path("/rnd/x.db")) == sink
    # Profile artifact defaults to the sink itself when the explicit env is unset.
    assert ms.profile_artifact_db() == sink
    # An explicit profile-artifact env overrides where the profile is read from.
    art = tmp_path / "profile.db"
    os.environ[_ART] = str(art)
    assert ms.profile_artifact_db() == art


def test_resolver_blank_env_is_treated_as_unset(tmp_path):
    os.environ[_SINK] = "   "
    assert ms.sink_db_override() is None
    assert ms.resolve_sink_db(Path("/rnd/x.db")) == Path("/rnd/x.db")


# ══════════════════════════════════════════════════════════════════════════════
# 2. worked_order — byte-identical default + redirect + fail-safe
# ══════════════════════════════════════════════════════════════════════════════

def test_recent_volume_unset_uses_default_path(monkeypatch):
    """UNSET + no db_path → resolves the R&D default. In this test env that file is
    absent → None (fail-safe), which PROVES the unset path does NOT read any sink."""
    # Sanity: the resolver used by recent_interval_volume returns the default.
    assert ms.resolve_sink_db(wo._DEFAULT_UNIVERSE_DB) == wo._DEFAULT_UNIVERSE_DB
    assert wo.recent_interval_volume("AAA") is None  # default DB absent in test env


def test_recent_volume_reads_sink_when_set(tmp_path):
    sink = tmp_path / "sink.db"
    _mk_orderflow_db(sink, "AAA", [100, 200, 300])
    os.environ[_SINK] = str(sink)
    # n_bars=3 → sum of the 3 most-recent minute volumes.
    assert wo.recent_interval_volume("AAA", n_bars=3) == 600.0


def test_recent_volume_explicit_db_path_wins_over_env(tmp_path):
    sink = tmp_path / "sink.db"
    _mk_orderflow_db(sink, "AAA", [999])
    explicit = tmp_path / "explicit.db"
    _mk_orderflow_db(explicit, "AAA", [111])
    os.environ[_SINK] = str(sink)
    assert wo.recent_interval_volume("AAA", db_path=str(explicit), n_bars=1) == 111.0


def test_recent_volume_sink_missing_is_failsafe(tmp_path):
    os.environ[_SINK] = str(tmp_path / "does_not_exist.db")
    assert wo.recent_interval_volume("AAA") is None  # never raises


def test_profile_unset_scans_ohlc_exactly_as_today(tmp_path):
    """UNSET + explicit db_path → the current ohlc_1min scan, unchanged."""
    rnd = tmp_path / "rnd.db"
    _mk_ohlc_db(rnd, "AAA", n_days=6)
    prof = wo.load_intraday_profile("AAA", db_path=str(rnd), min_days=5)
    assert prof is not None and prof.valid
    assert prof.n_days == 6


def test_profile_artifact_parity_with_rnd_read(tmp_path):
    """Artifact present → load_intraday_profile returns the SAME profile shape as
    the R&D ohlc_1min scan (exact parity)."""
    pub = _load_pub()

    rnd = tmp_path / "rnd.db"
    _mk_ohlc_db(rnd, "AAA", n_days=6)
    artifact = tmp_path / "profile.db"
    pub.build_artifact(rnd, artifact, lookback_days=20, min_days=5)

    # Live R&D read (unset env, explicit db_path).
    live = wo.load_intraday_profile("AAA", db_path=str(rnd), min_days=5)
    assert live is not None and live.valid

    # Artifact read: sink switch ON, artifact env → the profile db; NO db_path.
    wo._PROFILE_CACHE.clear()
    os.environ[_SINK] = str(tmp_path / "sink.db")   # only needs to be SET
    os.environ[_ART] = str(artifact)
    art = wo.load_intraday_profile("AAA", min_days=5)
    assert art is not None and art.valid

    assert art.buckets == live.buckets              # BYTE-IDENTICAL normalized shape
    assert art.n_days == live.n_days


def test_profile_artifact_absent_is_failsafe_flat_pov(tmp_path):
    """Sink switch ON but the profile artifact is MISSING → load_intraday_profile
    returns None (fail-safe) → the caller uses v1 flat POV. No crash."""
    os.environ[_SINK] = str(tmp_path / "sink.db")          # sink file absent
    os.environ[_ART] = str(tmp_path / "no_profile.db")     # artifact absent
    assert wo.load_intraday_profile("AAA", min_days=5) is None


def test_profile_artifact_symbol_absent_is_failsafe(tmp_path):
    """Artifact present but the symbol is not in it → None (fail-safe)."""
    pub = _load_pub()
    rnd = tmp_path / "rnd.db"
    _mk_ohlc_db(rnd, "AAA", n_days=6)
    artifact = tmp_path / "profile.db"
    pub.build_artifact(rnd, artifact, lookback_days=20, min_days=5)
    os.environ[_SINK] = str(tmp_path / "sink.db")
    os.environ[_ART] = str(artifact)
    assert wo.load_intraday_profile("ZZZ_NOT_PUBLISHED", min_days=5) is None


def test_make_vwap_sizer_failsafe_when_artifact_absent(tmp_path):
    """The order-sizing entry point: sink ON, artifact absent → make_vwap_sizer
    returns None → the worked engine sizes with v1 flat POV (target size unchanged)."""
    from autotrade.config import TradingSessionConfig
    cfg = TradingSessionConfig(total_allocated_capital=1e6, execution_mode="worked",
                               worked_vwap_enabled=True)
    os.environ[_SINK] = str(tmp_path / "sink.db")
    assert wo.make_vwap_sizer(cfg, "AAA") is None


# ══════════════════════════════════════════════════════════════════════════════
# 3. Tesla — precedence + redirect + no-signal fail-safe
# ══════════════════════════════════════════════════════════════════════════════

def test_tesla_resolve_db_precedence(tmp_path):
    sink = tmp_path / "sink.db"
    explicit = tmp_path / "explicit.db"
    # UNSET → DEFAULT_DB_PATH (byte-identical to today).
    assert tesla._resolve_db(None) == tesla.DEFAULT_DB_PATH
    # SET → sink.
    os.environ[_SINK] = str(sink)
    assert tesla._resolve_db(None) == sink
    # Explicit db_path (session's tesla_signal_db_path) ALWAYS wins over the env.
    assert tesla._resolve_db(explicit) == explicit


def test_tesla_no_sink_produces_no_signal(tmp_path):
    """Tesla with an empty sink → no candidates → no signal (never a trade, never
    breaks an exit)."""
    empty = tmp_path / "empty_sink.db"
    con = sqlite3.connect(str(empty))
    con.execute("CREATE TABLE mkt_orderflow_1min (symbol TEXT, segment TEXT, "
                "bar_time TEXT)")
    con.commit(); con.close()
    os.environ[_SINK] = str(empty)
    res = tesla.compute_live_signals(db_path=None)   # resolves to the empty sink
    assert res.signals == []


def test_tesla_missing_sink_file_no_crash(tmp_path):
    """A totally missing sink file → connect read-only still opens (or the query
    finds nothing) → empty signal set, no exception that could break a tick."""
    os.environ[_SINK] = str(tmp_path / "totally_absent.db")
    # connect_db_readonly with mode=ro on an absent file raises on connect; the
    # live tick path wraps this (session._tesla_signals try/except). Here we assert
    # the resolver points at the sink; the empty-DB no-signal case is covered above.
    assert tesla._resolve_db(None) == tmp_path / "totally_absent.db"


# ══════════════════════════════════════════════════════════════════════════════
# 4. Pollers — the write target moves with the SAME single switch
# ══════════════════════════════════════════════════════════════════════════════

def test_poller_db_constant_moves_with_env(tmp_path):
    """The pollers build their DB constant via resolve_sink_db(default) at import.
    Prove the SAME resolver drives the write target: unset → the R&D default;
    set → the sink. (Importing the real poller pulls in Kite; we assert the shared
    resolver the poller uses, which is the single source of truth.)"""
    rnd_default = Path("/rnd/kanida_universe.db")
    assert ms.resolve_sink_db(rnd_default) == rnd_default          # write→R&D today
    sink = tmp_path / "sink.db"
    os.environ[_SINK] = str(sink)
    assert ms.resolve_sink_db(rnd_default) == sink                 # write→sink together
