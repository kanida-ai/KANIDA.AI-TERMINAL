"""R&D-DB-split / Leg 2 — the sim's unfiltered pattern-set read.

Safety property: with FALCON_SIM_PATTERNS_ARTIFACT UNSET, load_full_patterns
reads the R&D JOIN EXACTLY as today (byte-identical). When the flag points at
the published artifact it reads that instead, and the returned pattern list is
IDENTICAL — same rows, SAME ORDER (compute_year_signals' float sum is
order-sensitive), same drawdown_bounce filtering. Missing/malformed artifact →
warn + fall back to R&D (never wrong data). The publisher is idempotent.

Exercises the real publisher (scripts/publish_full_patterns.py) against a small
synthetic R&D DB.
"""
from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
ROOT = BACKEND.parent

from power_user.services import persona_engine_core as pec  # noqa: E402


def _load_publisher():
    path = ROOT / "scripts" / "publish_full_patterns.py"
    spec = importlib.util.spec_from_file_location("_publish_full_patterns", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Synthetic promoted/candidate rows. Mix of classifications (only universal +
# regime_dependent are selected) and one drawdown_bounce rule (must be dropped
# by the Python filter on BOTH paths). rule_json is a list of [feat, op, thr].
_CANDIDATES = [
    # pattern_id, mined_year, rule_json
    (10, 2021, '[["rsi_14", "<=", "30"], ["roc_20", ">", "5"]]'),
    (11, 2022, '[["dist_high_252", "<=", "-15"]]'),          # drawdown_bounce → dropped
    (12, 2020, '[["vol_vs_20d", ">=", "1.5"]]'),
    (13, 2023, '[["gap_pct", ">", "2"]]'),
    (14, 2021, '[["close_loc", ">=", "0.8"]]'),              # sector_specific → NOT selected
    (15, 2022, '[["dist_high_120", "<=", "-12"]]'),          # drawdown_bounce → dropped
]
_PROMOTED = [
    # pattern_id, classification, avg_oos_year_lift_pp
    (10, "universal",        3.1),
    (11, "universal",        4.2),
    (12, "regime_dependent", 2.7),
    (13, "universal",        5.5),
    (14, "sector_specific",  9.9),   # excluded by the classification filter
    (15, "regime_dependent", 1.3),
]


@pytest.fixture
def rnd_db(tmp_path):
    p = tmp_path / "rnd.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE falcon_pattern_candidates "
                "(pattern_id INT, mined_year INT, rule_json TEXT)")
    con.execute("CREATE TABLE falcon_promoted_patterns "
                "(pattern_id INT, classification TEXT, avg_oos_year_lift_pp REAL)")
    con.executemany("INSERT INTO falcon_pattern_candidates VALUES (?,?,?)", _CANDIDATES)
    con.executemany("INSERT INTO falcon_promoted_patterns VALUES (?,?,?)", _PROMOTED)
    con.commit()
    con.close()
    return p


@pytest.fixture
def artifact(tmp_path, rnd_db, monkeypatch):
    pub = _load_publisher()
    monkeypatch.setattr(pub, "RND_DB", rnd_db)
    out = tmp_path / "falcon_sim_patterns.db"
    pub.build_artifact(out)
    return out


@pytest.fixture(autouse=True)
def _clean_flag(monkeypatch):
    monkeypatch.delenv(pec._SIM_PATTERNS_ARTIFACT_ENV, raising=False)
    yield


# ── flag OFF: byte-identical to today ──────────────────────────────────────

def test_flag_off_reads_rnd(rnd_db, monkeypatch):
    monkeypatch.delenv(pec._SIM_PATTERNS_ARTIFACT_ENV, raising=False)
    out = pec.load_full_patterns(str(rnd_db))
    # universal/regime_dependent minus the two drawdown_bounce rules = 3 kept
    mined = sorted(p["mined_year"] for p in out)
    assert mined == [2020, 2021, 2023]
    lifts = sorted(p["lift"] for p in out)
    assert lifts == pytest.approx([2.7, 3.1, 5.5])


# ── flag ON: reads the artifact, IDENTICAL list incl. order ────────────────

def test_flag_on_matches_rnd_exactly(rnd_db, artifact, monkeypatch):
    monkeypatch.delenv(pec._SIM_PATTERNS_ARTIFACT_ENV, raising=False)
    rnd_out = pec.load_full_patterns(str(rnd_db))
    monkeypatch.setenv(pec._SIM_PATTERNS_ARTIFACT_ENV, str(artifact))
    art_out = pec.load_full_patterns(str(rnd_db))
    # exact list equality: same rows, SAME ORDER, same float values
    assert art_out == rnd_out


def test_flag_on_does_not_touch_rnd(artifact, monkeypatch):
    """With the flag set + a valid artifact, a bogus R&D path must NOT be read."""
    monkeypatch.setenv(pec._SIM_PATTERNS_ARTIFACT_ENV, str(artifact))
    out = pec.load_full_patterns("Z:/does/not/exist/rnd.db")
    assert len(out) == 3      # served purely from the artifact


# ── artifact schema / row-count parity vs the R&D query ────────────────────

def test_artifact_rowcount_and_schema(rnd_db, artifact):
    con = sqlite3.connect(f"file:{artifact.as_posix()}?mode=ro", uri=True)
    try:
        cols = [r[1] for r in con.execute("PRAGMA table_info(falcon_sim_patterns)")]
        assert cols == ["seq", "mined_year", "rule_json", "avg_oos_year_lift_pp"]
        n_art = con.execute("SELECT COUNT(*) FROM falcon_sim_patterns").fetchone()[0]
    finally:
        con.close()
    # RAW JOIN rows (pre drawdown_bounce filter): universal+regime_dependent = 5
    r = sqlite3.connect(rnd_db)
    try:
        n_rnd = r.execute(
            "SELECT COUNT(*) FROM falcon_promoted_patterns p "
            "JOIN falcon_pattern_candidates c ON p.pattern_id=c.pattern_id "
            "WHERE p.classification IN ('universal','regime_dependent')").fetchone()[0]
    finally:
        r.close()
    assert n_art == n_rnd == 5


# ── publisher idempotency: same source → same checksum ─────────────────────

def test_publisher_idempotent(tmp_path, rnd_db, monkeypatch):
    pub = _load_publisher()
    monkeypatch.setattr(pub, "RND_DB", rnd_db)
    m1 = pub.build_artifact(tmp_path / "a1.db")
    m2 = pub.build_artifact(tmp_path / "a2.db")
    assert m1["checksum_sha256_32"] == m2["checksum_sha256_32"]
    assert m1["row_count"] == m2["row_count"] == "5"


# ── missing / malformed artifact → warn + fall back to R&D ─────────────────

def test_missing_artifact_falls_back(rnd_db, tmp_path, monkeypatch, caplog):
    monkeypatch.setenv(pec._SIM_PATTERNS_ARTIFACT_ENV, str(tmp_path / "nope.db"))
    with caplog.at_level("WARNING"):
        out = pec.load_full_patterns(str(rnd_db))
    assert len(out) == 3            # correct R&D answer
    assert any("unusable" in r.message for r in caplog.records)


def test_malformed_artifact_falls_back(rnd_db, tmp_path, monkeypatch, caplog):
    bad = tmp_path / "bad.db"
    b = sqlite3.connect(bad)
    b.execute("CREATE TABLE wrong (x INT)"); b.commit(); b.close()
    monkeypatch.setenv(pec._SIM_PATTERNS_ARTIFACT_ENV, str(bad))
    with caplog.at_level("WARNING"):
        out = pec.load_full_patterns(str(rnd_db))
    assert len(out) == 3
    assert any("unusable" in r.message for r in caplog.records)
