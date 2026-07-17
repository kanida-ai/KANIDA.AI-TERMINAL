"""Cloud-migration A2 — the SILENT R&D-database fallback must stay dead.

Contract under test:
  1. Prod DB present  → resolution unchanged (canonical path).
  2. Prod DB absent    → NO silent substitution of the R&D DB; startup verify
                         raises a clear, actionable error naming the path + env var.
  3. Explicit opt-in   → research tooling can still reach the R&D DB
                         (FALCON_DB_PATH / POWER_DB_PATH / the legacy fallback flag).
  4. POWER_RND_DB_PATH → unaffected; a separate explicit research handle.

These are importlib.reload-based because both configs resolve at import time.
"""
from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))


# ── helpers ──────────────────────────────────────────────────────────────
_DB_ENV = (
    "FALCON_DB_PATH", "POWER_DB_PATH", "POWER_RND_DB_PATH",
    "KANIDA_ALLOW_RND_DB_FALLBACK", "KANIDA_DATA_DIR",
)


@pytest.fixture
def clean_env(monkeypatch):
    for v in _DB_ENV:
        monkeypatch.delenv(v, raising=False)
    return monkeypatch


def _reload_falcon():
    import falcon.config as fc
    return importlib.reload(fc)


def _reload_power():
    import power_user.config as pc
    return importlib.reload(pc)


def _make_tree(tmp_path: Path, *, prod: bool, rnd: bool) -> Path:
    """Build a fake repo root with/without each DB file."""
    if prod:
        d = tmp_path / "data" / "db"
        d.mkdir(parents=True, exist_ok=True)
        (d / "kanida_universe.db").write_bytes(b"PROD")
    if rnd:
        d = tmp_path / "universe_engine" / "data" / "db"
        d.mkdir(parents=True, exist_ok=True)
        (d / "kanida_universe.db").write_bytes(b"RND")
    return tmp_path


# ── 1. prod present → unchanged ──────────────────────────────────────────
def test_falcon_prod_db_present_resolves_canonical(clean_env, tmp_path):
    root = _make_tree(tmp_path, prod=True, rnd=True)
    clean_env.setenv("KANIDA_DATA_DIR", str(root / "data"))
    fc = _reload_falcon()
    assert fc.FALCON_DB == root / "data" / "db" / "kanida_universe.db"
    assert fc.FALCON_DB.read_bytes() == b"PROD"
    # and verification passes silently
    assert fc.verify_falcon_db() == fc.FALCON_DB


# ── 2. prod absent → NO fallback, fail loud ──────────────────────────────
def test_falcon_prod_absent_does_NOT_fall_back_to_rnd(clean_env, tmp_path):
    """THE BUG: this used to silently return the 35 GB R&D DB."""
    root = _make_tree(tmp_path, prod=False, rnd=True)
    clean_env.setenv("KANIDA_DATA_DIR", str(root / "data"))
    fc = _reload_falcon()
    rnd = root / "universe_engine" / "data" / "db" / "kanida_universe.db"
    assert rnd.exists(), "fixture sanity: R&D DB is present and tempting"
    assert fc.FALCON_DB != rnd, "SILENT R&D FALLBACK HAS RETURNED"
    assert "universe_engine" not in str(fc.FALCON_DB)


def test_falcon_verify_raises_actionable_error(clean_env, tmp_path):
    root = _make_tree(tmp_path, prod=False, rnd=True)
    clean_env.setenv("KANIDA_DATA_DIR", str(root / "data"))
    fc = _reload_falcon()
    with pytest.raises(fc.ProductionDBMissingError) as ei:
        fc.verify_falcon_db()
    msg = str(ei.value)
    assert "FALCON_DB_PATH" in msg            # names the env var
    assert "kanida_universe.db" in msg        # names the expected path
    assert "refusing to start" in msg.lower()
    assert "volume" in msg.lower()            # container hint


def test_error_is_filenotfounderror_for_backcompat(clean_env, tmp_path):
    """connect_falcon() historically raised FileNotFoundError — keep that."""
    root = _make_tree(tmp_path, prod=False, rnd=False)
    clean_env.setenv("KANIDA_DATA_DIR", str(root / "data"))
    fc = _reload_falcon()
    assert issubclass(fc.ProductionDBMissingError, FileNotFoundError)
    with pytest.raises(FileNotFoundError):
        fc.verify_falcon_db()


def test_power_prod_absent_does_NOT_fall_back_to_rnd(clean_env, tmp_path, monkeypatch):
    root = _make_tree(tmp_path, prod=False, rnd=True)
    pc = _reload_power()
    monkeypatch.setattr(pc, "_REPO_ROOT", root)
    resolved = pc._resolve_power_db_path()
    assert "universe_engine" not in resolved, "SILENT R&D FALLBACK HAS RETURNED"
    assert resolved == str(root / "data" / "db" / "kanida_universe.db")


def test_power_verify_raises_naming_power_db_path(clean_env, tmp_path):
    pc = _reload_power()
    missing = tmp_path / "nope" / "kanida_universe.db"
    with pytest.raises(FileNotFoundError) as ei:
        pc.verify_power_db(str(missing))
    assert "POWER_DB_PATH" in str(ei.value)


# ── 3. explicit opt-in still works (research must not regress) ───────────
def test_explicit_falcon_db_path_env_wins(clean_env, tmp_path):
    root = _make_tree(tmp_path, prod=True, rnd=True)
    rnd = root / "universe_engine" / "data" / "db" / "kanida_universe.db"
    clean_env.setenv("KANIDA_DATA_DIR", str(root / "data"))
    clean_env.setenv("FALCON_DB_PATH", str(rnd))
    fc = _reload_falcon()
    assert fc.FALCON_DB == rnd            # explicit opt-in → R&D DB, allowed
    assert fc.verify_falcon_db() == rnd


def test_legacy_fallback_flag_restores_old_behaviour(clean_env, tmp_path, monkeypatch):
    """Reversibility: the operator can opt back into the old fallback.

    NOTE: RND_DB_PATH derives from ROOT (__file__-based) and is deliberately
    NOT influenced by KANIDA_DATA_DIR, so we point the module global at a
    fixture file rather than writing into the real repo tree.
    """
    root = _make_tree(tmp_path, prod=False, rnd=True)
    rnd = root / "universe_engine" / "data" / "db" / "kanida_universe.db"
    clean_env.setenv("KANIDA_DATA_DIR", str(root / "data"))
    clean_env.setenv("KANIDA_ALLOW_RND_DB_FALLBACK", "true")
    fc = _reload_falcon()
    monkeypatch.setattr(fc, "RND_DB_PATH", rnd)
    assert fc._resolve_falcon_db() == rnd

    # …and with the flag OFF the very same tree resolves to canonical, NOT R&D.
    clean_env.delenv("KANIDA_ALLOW_RND_DB_FALLBACK")
    assert fc._resolve_falcon_db() != rnd


def test_power_explicit_env_wins(clean_env, tmp_path):
    target = tmp_path / "explicit.db"
    target.write_bytes(b"X")
    clean_env.setenv("POWER_DB_PATH", str(target))
    pc = _reload_power()
    assert pc.POWER_DB_PATH == str(target)
    assert pc.verify_power_db() == str(target)


# ── 4. POWER_RND_DB_PATH is a separate explicit handle, not a fallback ───
def test_power_rnd_db_path_still_honoured(clean_env, tmp_path):
    rnd = tmp_path / "rnd.db"
    rnd.write_bytes(b"R")
    clean_env.setenv("POWER_RND_DB_PATH", str(rnd))
    pc = _reload_power()
    assert pc.POWER_RND_DB_PATH == str(rnd)


def test_power_rnd_default_unchanged(clean_env):
    """Offline research default (universe_engine path) must not regress."""
    pc = _reload_power()
    assert pc.POWER_RND_DB_PATH.endswith("kanida_universe.db")
    assert "universe_engine" in pc.POWER_RND_DB_PATH


# ── 5. real-world invariants ─────────────────────────────────────────────
def test_error_message_is_cp1252_safe(clean_env, tmp_path):
    """start_backend.bat pipes stderr into logs\\backend.log on a cp1252
    console. A non-ASCII char here raises UnicodeEncodeError while printing,
    masking the fatal error behind an encoding traceback. Keep it ASCII."""
    fc = _reload_falcon()
    msg = fc._db_error_message(tmp_path / "kanida_universe.db", "FALCON_DB_PATH")
    msg.encode("cp1252")          # must not raise
    assert msg.isascii(), "DB error message must be pure ASCII"

    # the raised exception text (incl. any appended suffix) must also survive
    with pytest.raises(fc.ProductionDBMissingError) as ei:
        fc.verify_falcon_db(tmp_path / "definitely_absent.db")
    str(ei.value).encode("cp1252")
    assert str(ei.value).isascii()


def test_paths_are_absolute_and_cwd_independent(clean_env, monkeypatch, tmp_path):
    """A relative path would make resolution cwd-dependent — the .bat and a
    manual `uvicorn` run must agree."""
    monkeypatch.chdir(tmp_path)
    fc = _reload_falcon()
    pc = _reload_power()
    assert fc.FALCON_DB.is_absolute()
    assert os.path.isabs(pc.POWER_DB_PATH)
    assert os.path.isabs(pc.POWER_RND_DB_PATH)


def test_startup_preflight_semantics(clean_env, tmp_path, monkeypatch):
    """Mirror of main.py's lifespan preflight: a missing prod DB blocks boot,
    KANIDA_SKIP_DB_PREFLIGHT downgrades it, an unrelated crash never blocks."""
    fc = _reload_falcon()
    missing = tmp_path / "absent.db"

    def preflight(skip: bool, verifier):
        # same control flow as main.py lifespan
        try:
            verifier()
        except fc.ProductionDBMissingError:
            if skip:
                return "CONTINUED_WITH_SKIP"
            raise
        except Exception:
            return "CONTINUED_NONFATAL"
        return "OK"

    # 1. missing DB, no skip → blocks boot
    with pytest.raises(fc.ProductionDBMissingError):
        preflight(False, lambda: fc.verify_falcon_db(missing))
    # 2. missing DB + escape hatch → boots
    assert preflight(True, lambda: fc.verify_falcon_db(missing)) == "CONTINUED_WITH_SKIP"
    # 3. unrelated preflight crash → never blocks boot
    def boom():
        raise ImportError("unrelated")
    assert preflight(False, boom) == "CONTINUED_NONFATAL"
    # 4. DB present → OK
    present = tmp_path / "present.db"
    present.write_bytes(b"X")
    assert preflight(False, lambda: fc.verify_falcon_db(present)) == "OK"


def teardown_module(module):
    """Leave the real modules resolved from the real environment."""
    for v in _DB_ENV:
        os.environ.pop(v, None)
    _reload_falcon()
    _reload_power()
