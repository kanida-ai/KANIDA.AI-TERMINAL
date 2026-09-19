# KANIDA ARENA — the consolidated Autonomous Learning / Constitutional layer

**This is THE Arena.** It supersedes the throwaway comparison scripts
(`kanida_engine/arena.py` v1, `arena_v2.py`, `arena_v3.py`) — those stay only as an audit trail.

## What it is
One autonomous **worker per stock**. Objective = maximize long-term **Expected Trade Value (ETV)**
under the **Constitutional Score** (ETV + statistical confidence + downside-variance + recent-drift),
with **tail-control stops**, **proportional allocation**, and a **Keep / Watch / Test / Retire** roster.
Strictly point-in-time, frozen hyperparameters. Reads the fast frame cache → a full run is seconds.

Spec: `kanida_engine/ARENA_SPEC.md`. Constitution constants are frozen at the top of `arena.py`.

## Proven so far (v2 base, consolidated here)
Across every version the learning layer **cut drawdown ~74–87% and improved P&L ~73%** vs a static
"trade-everything" baseline — e.g. 5-stock check: STATIC −₹357k / DD −₹802k → **ARENA +₹23k / DD −₹99k**,
beat static 4/5. The *mechanism* is proven; the *edge ceiling* is what the roadmap raises.

## Run
```
python arena/arena.py                 # all stocks in unified_patterns (off cache, seconds)
python arena/arena.py ADANIENT TCS    # specific stocks
```

## Roadmap (built ON this, not new versions)
1. **Score-conditional NRML-long leverage** (flag `NRML_LONG`; gate 5x by score band — not blanket)
2. **Pattern-level ETV** option (segment-level is the current, sample-rich default)
3. **2015→ full walk-forward replay** (learn+evolve over a decade, not just 2025-26)
4. **EVENT module** (expiry / Monday / results-day / corp-actions) — raises the actual edge
5. **Persistent paper-trade infra**: `signal_events`, `paper_trades`, `roster_state`, `constitution_log`
6. **Multi-dimensional paper-trade analytics** (stock/signal/segment/timeframe/direction/portfolio)
7. **Cross-learning + Global Discovery Pool**
8. **Sellable serializable per-stock agents**

## Depends on
`kanida_engine/features.py` (atoms + frame cache `load_frame`), `KANIDA_SNR.db.unified_patterns`.
