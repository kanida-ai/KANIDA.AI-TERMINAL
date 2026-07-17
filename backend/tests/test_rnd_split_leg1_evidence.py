"""R&D-DB-split / Leg 1 — the explainer's serve-time evidence read.

Safety property under test: with FALCON_OUTCOMES_ARTIFACT UNSET the explainer
reads the R&D `falcon_outcomes` table EXACTLY as today (byte-identical). When
the flag points at the published artifact it reads the artifact instead, and
the fully-built bucket2 payload is IDENTICAL both ways (shadow-parity). A
missing/malformed artifact falls back to R&D with a warning (never wrong data).

Fixtures are small synthetic SQLite DBs; the real A1 publisher
(scripts/publish_outcomes_evidence.py) builds the artifact from the synthetic
R&D DB, so the actual publish path is exercised too.
"""
from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
ROOT = BACKEND.parent

from power_user.services import falcon_top20_explainer as ex  # noqa: E402


def _load_publisher():
    """Import scripts/publish_outcomes_evidence.py as a module."""
    path = ROOT / "scripts" / "publish_outcomes_evidence.py"
    spec = importlib.util.spec_from_file_location("_publish_outcomes_evidence", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Synthetic falcon_outcomes: two symbols, rows both before and after the
# 2021-01-01 lookback floor (to exercise the baseline denominator trap).
_OUTCOME_ROWS = [
    # symbol, trade_date, ret_20d, hit_10pc_20d, mae_20d, mfe_20d
    ("AAA", "2019-06-03",  5.0, 0, -3.0,  6.0),   # pre-2021: counts in baseline only
    ("AAA", "2020-02-10", -2.0, 0, -4.0,  1.0),   # pre-2021
    ("AAA", "2021-03-15", 12.0, 1, -1.5, 13.0),   # in evidence + baseline
    ("AAA", "2021-09-20",  8.0, 0, -2.0,  9.0),
    ("AAA", "2022-01-05", 15.0, 1, -0.5, 16.0),
    ("AAA", "2022-06-01", None, None, None, None),  # NULL hit → excluded from AVG
    ("BBB", "2020-11-11",  1.0, 0, -1.0,  2.0),   # pre-2021
    ("BBB", "2021-07-07", 20.0, 1, -0.2, 21.0),
    ("BBB", "2023-02-02", -5.0, 0, -7.0,  0.5),
]


@pytest.fixture
def rnd_db(tmp_path):
    p = tmp_path / "rnd.db"
    con = sqlite3.connect(p)
    con.execute("""
        CREATE TABLE falcon_outcomes (
            symbol TEXT, trade_date TEXT, ret_5d REAL, ret_10d REAL,
            ret_20d REAL, ret_30d REAL, hit_10pc_20d INTEGER, hit_15pc_20d INTEGER,
            mae_20d REAL, mfe_20d REAL, failure_10d INTEGER
        )""")
    con.executemany(
        "INSERT INTO falcon_outcomes (symbol, trade_date, ret_20d, hit_10pc_20d, "
        "mae_20d, mfe_20d) VALUES (?,?,?,?,?,?)", _OUTCOME_ROWS)
    con.commit()
    con.close()
    return p


@pytest.fixture
def artifact(tmp_path, rnd_db, monkeypatch):
    """Build the real artifact from the synthetic R&D DB via the A1 publisher."""
    pub = _load_publisher()
    monkeypatch.setattr(pub, "RND_DB", rnd_db)
    out = tmp_path / "falcon_serve_evidence.db"
    pub.build_artifact(out)
    return out


@pytest.fixture(autouse=True)
def _reset_artifact_state(monkeypatch):
    """Ensure the module-level artifact connection cache never leaks between
    tests and the flag starts UNSET."""
    monkeypatch.delenv(ex._OUTCOMES_ARTIFACT_ENV, raising=False)
    ex._artifact_state.update({"path": None, "con": None, "ok": None})
    yield
    ex._artifact_state.update({"path": None, "con": None, "ok": None})


def _ro(path):
    con = sqlite3.connect(f"file:{Path(path).as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


# ── flag OFF: byte-identical to today (the safety property) ────────────────

def test_flag_off_reads_rnd_unchanged(rnd_db, monkeypatch):
    monkeypatch.delenv(ex._OUTCOMES_ARTIFACT_ENV, raising=False)
    con = _ro(rnd_db)
    try:
        # evidence lookup goes to R&D
        assert ex._get_outcomes_artifact_con() is None
        rows = ex._evidence_lookup(con, "AAA", ["2021-03-15", "2022-01-05"])
        got = sorted((r["trade_date"], r["ret_20d"], r["hit_10pc_20d"]) for r in rows)
        assert got == [("2021-03-15", 12.0, 1), ("2022-01-05", 15.0, 1)]
        # baseline computed over ALL-TIME rows (incl pre-2021), NULL excluded
        # AAA: hits over non-null rows = [0,0,1,0,1] → 2/5 = 40.0
        assert ex._stock_lifetime_baseline(con, "AAA") == pytest.approx(40.0)
        assert ex._stock_lifetime_baseline(con, "BBB") == pytest.approx(100.0 / 3)
    finally:
        con.close()


# ── flag ON: reads the artifact ────────────────────────────────────────────

def test_flag_on_reads_artifact(rnd_db, artifact, monkeypatch):
    monkeypatch.setenv(ex._OUTCOMES_ARTIFACT_ENV, str(artifact))
    # A DIFFERENT (empty) R&D connection proves reads come from the artifact.
    empty = sqlite3.connect(":memory:")
    empty.execute("CREATE TABLE falcon_outcomes (symbol TEXT, trade_date TEXT, "
                  "ret_20d REAL, hit_10pc_20d INT, mae_20d REAL, mfe_20d REAL)")
    empty.row_factory = sqlite3.Row
    assert ex._get_outcomes_artifact_con() is not None
    rows = ex._evidence_lookup(empty, "AAA", ["2021-03-15", "2022-01-05"])
    got = sorted((r["trade_date"], r["ret_20d"]) for r in rows)
    assert got == [("2021-03-15", 12.0), ("2022-01-05", 15.0)]
    # baseline from the precomputed table, still all-time 40.0 (not recomputed
    # over the 2021+ subset, which would be 2/3 = 66.7)
    assert ex._stock_lifetime_baseline(empty, "AAA") == pytest.approx(40.0)
    empty.close()


# ── shadow-parity: full bucket2 payload identical both ways ─────────────────

def _build_bucket2_both_ways(rnd_db, artifact, monkeypatch, symbol):
    """Return (rnd_payload, artifact_payload) for _build_bucket2 on `symbol`.

    A single synthetic pattern fires on every date present, so the fire-date set
    equals this symbol's evidence dates — exercising the real IN(...) lookup and
    the baseline, then the full aggregation path.
    """
    import numpy as np  # noqa: F401  (ensures numpy present for explainer)

    prod = sqlite3.connect(":memory:")
    prod.row_factory = sqlite3.Row
    # falcon_features must carry every numeric column the explainer selects;
    # only rsi_14 is populated (the rule references it), the rest stay NULL/NaN.
    feat_cols = sorted(ex._FEATURE_COLS_NUMERIC)
    col_ddl = ", ".join(f"{c} REAL" for c in feat_cols)
    prod.execute(f"CREATE TABLE falcon_features (symbol TEXT, trade_date TEXT, {col_ddl})")
    dates = [r[1] for r in _OUTCOME_ROWS if r[0] == symbol and r[1] >= "2021-01-01"]
    rsi_idx = feat_cols.index("rsi_14")
    for d in dates:
        vals = [None] * len(feat_cols)
        vals[rsi_idx] = 50.0
        prod.execute(
            f"INSERT INTO falcon_features (symbol, trade_date, {', '.join(feat_cols)}) "
            f"VALUES (?, ?, {', '.join('?' * len(feat_cols))})",
            [symbol, d, *vals])
    prod.commit()

    taxonomy = {1: {"rule_json": '[["rsi_14", ">", "0"]]'}}
    signal_date = max(dates)

    def _run():
        rnd = _ro(rnd_db)
        try:
            return ex._build_bucket2(prod, rnd, symbol, [1], signal_date, taxonomy)
        finally:
            rnd.close()

    monkeypatch.delenv(ex._OUTCOMES_ARTIFACT_ENV, raising=False)
    ex._artifact_state.update({"path": None, "con": None, "ok": None})
    rnd_payload = _run()

    monkeypatch.setenv(ex._OUTCOMES_ARTIFACT_ENV, str(artifact))
    ex._artifact_state.update({"path": None, "con": None, "ok": None})
    art_payload = _run()

    prod.close()
    return rnd_payload, art_payload


@pytest.mark.parametrize("symbol", ["AAA", "BBB"])
def test_shadow_parity_bucket2_payload_identical(rnd_db, artifact, monkeypatch, symbol):
    rnd_payload, art_payload = _build_bucket2_both_ways(rnd_db, artifact, monkeypatch, symbol)
    assert rnd_payload == art_payload, (
        f"bucket2 payload differs for {symbol}:\n R&D={rnd_payload}\n ART={art_payload}")
    # Sanity: the payload actually used outcomes (not an empty short-circuit).
    assert rnd_payload["n_with_outcome"] > 0


# ── missing / malformed artifact → warn + fall back to R&D ─────────────────

def test_missing_artifact_falls_back_to_rnd(rnd_db, monkeypatch, caplog):
    monkeypatch.setenv(ex._OUTCOMES_ARTIFACT_ENV, str(rnd_db.parent / "nope.db"))
    con = _ro(rnd_db)
    try:
        with caplog.at_level("WARNING"):
            assert ex._get_outcomes_artifact_con() is None       # unusable → None
            # falls back to the R&D connection and still answers correctly
            assert ex._stock_lifetime_baseline(con, "AAA") == pytest.approx(40.0)
        assert any("unusable" in r.message for r in caplog.records)
    finally:
        con.close()


def test_malformed_artifact_falls_back_to_rnd(tmp_path, rnd_db, monkeypatch, caplog):
    bad = tmp_path / "bad.db"
    b = sqlite3.connect(bad)
    b.execute("CREATE TABLE wrong_table (x INT)")   # missing required tables
    b.commit(); b.close()
    monkeypatch.setenv(ex._OUTCOMES_ARTIFACT_ENV, str(bad))
    con = _ro(rnd_db)
    try:
        with caplog.at_level("WARNING"):
            assert ex._get_outcomes_artifact_con() is None
            rows = ex._evidence_lookup(con, "BBB", ["2021-07-07"])
            assert [(r["trade_date"], r["ret_20d"]) for r in rows] == [("2021-07-07", 20.0)]
        assert any("missing tables" in r.message or "unusable" in r.message
                   for r in caplog.records)
    finally:
        con.close()
