"""Focused tests for the additive persona enhancements (2026-06-22):

  - yearly rows carry `winning_months` (int count of that year's months > 0)
  - monthly rows carry `max_dd_pct` (float <= 0, or None)
  - existing fields are preserved (backward-compatible / additive only)

The `_intramonth_max_dd` helper is unit-tested directly (fast); the end-to-end
shape is asserted against one real persona sim (slower — marked accordingly).
"""
from __future__ import annotations

import pytest

from power_user.services.persona_simulator import (
    PERSONA_CONFIGS,
    _intramonth_max_dd,
    simulate_persona,
)


# ── unit: the drawdown helper ───────────────────────────────────────────────

def test_intramonth_max_dd_fall_only():
    # carry-in 100, falls to 90 → -10%
    assert _intramonth_max_dd([100, 95, 90], 100) == -10.0

def test_intramonth_max_dd_up_only():
    # never below running peak → 0
    assert _intramonth_max_dd([100, 110, 120], 100) == 0.0

def test_intramonth_max_dd_peak_then_trough():
    # rises to 120 then drops to 108 → (108/120-1) = -10%
    assert _intramonth_max_dd([100, 120, 108], 100) == -10.0

def test_intramonth_max_dd_empty_is_none():
    assert _intramonth_max_dd([], 100) is None

def test_intramonth_max_dd_bad_carry_in_uses_first():
    # carry-in 0 → seed from first element; no dip → 0
    assert _intramonth_max_dd([100, 105], 0) == 0.0


# ── end-to-end shape (one real persona sim) ─────────────────────────────────

@pytest.mark.slow
def test_persona_carries_winning_months_and_monthly_dd(monkeypatch):
    # The simulator needs the RND pattern DB (14 GB) which lives only in the
    # main prod tree, not the worktree. Point POWER_DB_PATH at the prod DB so
    # the simulator's RND/PROD resolution finds both; skip if absent.
    import os
    from power_user import config as _cfg
    from power_user.services import persona_simulator as _ps

    prod_db = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\data\db\kanida_universe.db"
    rnd_db = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\universe_engine\data\db\kanida_universe.db"
    if not (os.path.exists(prod_db) and os.path.exists(rnd_db)):
        pytest.skip("Prod/RND DBs not available for full persona sim")
    monkeypatch.setattr(_cfg, "POWER_DB_PATH", prod_db)
    monkeypatch.setattr(_ps, "PROD_DB", prod_db)
    _ps.invalidate_cache()   # ensure a fresh run against the patched paths

    # weekly-trader is one of the cheaper sims (fewer trades than daily/BTST).
    slug = "weekly-trader"
    assert slug in PERSONA_CONFIGS
    result = simulate_persona(slug, force=True)

    assert result["yearly"], "expected at least one yearly row"
    for yr in result["yearly"]:
        # additive field present + correct type
        assert "winning_months" in yr
        assert isinstance(yr["winning_months"], int)
        assert 0 <= yr["winning_months"] <= 12
        # backward-compat: original fields still present
        for k in ("year", "return_pct", "max_dd_pct", "win_rate_pct", "n_closed"):
            assert k in yr

    assert result["monthly"], "expected at least one monthly row"
    for mo in result["monthly"]:
        assert "max_dd_pct" in mo
        dd = mo["max_dd_pct"]
        assert dd is None or (isinstance(dd, (int, float)) and dd <= 0.0)
        # backward-compat
        for k in ("year", "month", "end_equity", "return_pct"):
            assert k in mo

    # cross-check: winning_months equals count of that year's months with ret > 0
    by_year: dict = {}
    for mo in result["monthly"]:
        if mo["return_pct"] > 0:
            by_year[mo["year"]] = by_year.get(mo["year"], 0) + 1
    for yr in result["yearly"]:
        assert yr["winning_months"] == by_year.get(yr["year"], 0)
