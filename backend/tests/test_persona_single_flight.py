"""Persona sim single-flight — DoS hardening for the PUBLIC persona endpoints.

2026-07-16. `GET /api/power/personas/{slug}` is intentionally public: it is
called with NO auth by user-facing pages (CoTradingExperience.tsx:218 and
app/power/portfolios/[slug]/DashboardClient.tsx:130 both call
`PowerAPI.persona(slug)`, which sends no JWT). So it cannot be token-gated
without breaking those pages.

The abuse vector is a CACHE STAMPEDE: on a cold cache each concurrent request
launched its own 30-90s simulation. These tests assert concurrent cold-cache
callers collapse into ONE run and all still get the correct result.
"""
import threading
import time

import pytest

from power_user.services import persona_simulator as ps


@pytest.fixture(autouse=True)
def _clean_cache():
    ps._CACHE.clear()
    yield
    ps._CACHE.clear()


def test_concurrent_cold_cache_calls_run_the_sim_once(monkeypatch):
    """8 parallel cold-cache callers → exactly 1 underlying sim run."""
    slug = next(iter(ps.PERSONA_CONFIGS))
    runs = []

    def fake_uncached(persona_slug, force=False):
        runs.append(persona_slug)
        time.sleep(0.2)                       # simulate the heavy 30-90s sim
        result = {"persona_id": persona_slug, "marker": "computed"}
        ps._CACHE[persona_slug] = {"_cached_at": time.time(), "result": result}
        return result

    monkeypatch.setattr(ps, "_simulate_persona_uncached", fake_uncached)

    results, errors = [], []

    def worker():
        try:
            results.append(ps.simulate_persona(slug))
        except Exception as e:          # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not errors, f"workers raised: {errors}"
    assert len(runs) == 1, f"cache stampede: sim ran {len(runs)}x, expected 1"
    assert len(results) == 8
    assert all(r["marker"] == "computed" for r in results), \
        "every concurrent caller must still get the real result"


def test_warm_cache_does_not_run_the_sim(monkeypatch):
    slug = next(iter(ps.PERSONA_CONFIGS))
    ps._CACHE[slug] = {"_cached_at": time.time(), "result": {"marker": "cached"}}

    def boom(*a, **k):                   # pragma: no cover
        raise AssertionError("warm cache must not recompute")

    monkeypatch.setattr(ps, "_simulate_persona_uncached", boom)
    assert ps.simulate_persona(slug)["marker"] == "cached"


def test_unknown_slug_still_raises_fast(monkeypatch):
    """Unknown slug must be rejected before any lock/sim work."""
    def boom(*a, **k):                   # pragma: no cover
        raise AssertionError("must not simulate an unknown slug")

    monkeypatch.setattr(ps, "_simulate_persona_uncached", boom)
    with pytest.raises(ValueError):
        ps.simulate_persona("definitely-not-a-persona")


def test_force_still_recomputes(monkeypatch):
    """force=True (admin-gated) must bypass the cache, single-flight or not."""
    slug = next(iter(ps.PERSONA_CONFIGS))
    ps._CACHE[slug] = {"_cached_at": time.time(), "result": {"marker": "cached"}}
    monkeypatch.setattr(ps, "_simulate_persona_uncached",
                        lambda s, force=False: {"marker": "recomputed", "force": force})
    out = ps.simulate_persona(slug, force=True)
    assert out["marker"] == "recomputed" and out["force"] is True
