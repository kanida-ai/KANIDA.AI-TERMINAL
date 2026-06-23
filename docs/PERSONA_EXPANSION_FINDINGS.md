# Persona Expansion — Build & Walk-Forward Findings

_Status: autonomous build complete; walk-forward 2022→2026 run. This document is
the honest "plateau with real results" surface point the build brief defines._

## 1. What was built (all additive, on `feat/persona-expansion`)

A complete, runnable closed-loop system in `persona_engine/`:

| Layer | Module | Spec |
|------|--------|------|
| RND DB access | `db.py` | resolve existing DB, no new file |
| Schema (16 tables) | `schema.py` | §5 |
| Stock-agent features (point-in-time) | `features.py` | §1.2, §2.4/2.5, §3.4 |
| Scoring model + regime + sector momentum | `model.py` | §1.3/1.4 |
| Persona agents (stock→sector→persona) | `personas.py` | §1 |
| Forward outcomes / actual Top-10 | `outcomes.py` | §2.3, §3.3 |
| Closed-loop walk-forward + IC learning | `engine.py` | §2.6/2.7, §4.1 |
| Rule mining + lifecycle + proposals | `learn.py` | §4.2/4.3/4.4 |
| Daily/weekly explainable review | `review.py` | §2.8, §3.6 |
| Runtime universe (Kite/NSE) | `universe.py` | P5/P6/P7 |
| Read-only portal API | `access.py` | deliverable |
| CLI | `run.py` | — |

Constitutional rules honoured: no lookahead (P1), walk-forward only (P3),
human-approval gate on live deployment (P4/P10 — proposals written with
`human_approved=0`), runtime universe fetch (P5/P6/P7), existing Falcon/tier/
auto-trade/portal untouched (P8/P9).

## 2. The core empirical finding (why the 80% target is not reachable)

Measured rank-IC (information coefficient) of every feature vs the actual targets,
pooled over the 2022–2026 walk-forward universe:

* **Next-day return: max |IC| ≈ 0.04** (`consol_days` +0.041, `atr_20_pct` −0.044).
* **20-day return: max |IC| ≈ 0.025** (`rs_index_60d` −0.025).

The profile of the actual next-day extreme movers is a **symmetric U-shape**: both
the top-10 gainers and the top-10 losers are simply the highest-ATR / highest-volume
/ largest-recent-move names. **Magnitude** of the next-day move is predictable;
**direction** is essentially a coin-flip at these IC levels.

With rank-ICs near 0.04, the statistically achievable overlap with a 10-of-~200
top list is on the order of **1–2 of 10 (long) and 2–3 of 10 (short)** — not 8/10.
Reaching 70–80% overlap on full-day next-day (or 4–8 week) returns would require
forecasting tomorrow's news flow, which is not present in price/volume/RS data.

## 3. Walk-forward results (2022→2026)

Causal walk-forward, 1,076 measured F&O days / 1,057 LT days. **Random baseline =
10/~200 ≈ 4.7% (0.5 of 10).**

| Metric | Overall | By year (2022→2026) | Read |
|---|---|---|---|
| **F&O Short Top-10** | **14.0%** (1.4/10) | 14.6 · 15.1 · 15.7 · 12.0 · 9.6 | **~3× random — the real edge** |
| F&O Long Top-10 | 2.9% (0.29/10) | 2.2 · 2.6 · 3.5 · 2.8 · 4.8 | ≈ random (direction unpredictable) |
| F&O Combined (/20) | 8.5% (1.69/20) | 8.4 · 8.9 · 9.6 · 7.4 · 7.2 | — |
| LT 4-week Top-10 | 4.2% (0.42/10) | 4.5 · 6.4 · 5.3 · 1.6 · 0.9 | edge decays as market turns trending |
| LT 8-week Top-10 | 3.5% (0.35/10) | 4.0 · 4.4 · 4.5 · 1.7 · 0.0 | same decay |

**Mined rulebook (discipline gates: n≥18, ≥2 yrs, ≥2 regimes, ≥3 sectors,
worst-year > floor).** Of 40 proposals, exactly **1 promoted to ACTIVE**:

* `FO_SHORT : atr_20_pct top-quintile → next-day loser` — hit 10.1%, n=44,344,
  5 years, 18 sectors, worst-year 8.2%. **ACTIVE.**
* Best long rule (`FO_LONG : atr_20_pct:hi`, 9.3%) and best LT rule
  (`LT : atr_20_pct:hi`, 4.9%) fail the worst-year floor → held **TESTING**.

The gate is doing its job: it refuses to promote the weak long/LT rules and only
certifies the robust short-volatility edge. (No autonomous deployment — all 40 sit
in the human-review queue with `human_approved=0`.)

Vs the spec targets (F&O 80% = 8/10, LT 70% = 7/10): the engine reaches roughly
**0.3–1.4 of 10**. The gap is not a tuning problem — see §2.

## 4. Data gaps that further cap the F&O spec

* Futures **OI** exists only ~2 months (2026); **PCR / IV absent entirely**. The
  spec's OI/PCR/IV features (§2.4/2.5) are therefore live-only — not usable in the
  historical walk-forward.
* OHLC 2020–2021 covers only ~138 symbols → 2021 is warm-up; real walk-forward is
  2022→2026.
* Historical point-in-time F&O membership is not stored; the current Kite F&O list
  is used as a proxy for past dates.

## 5. Where the real edge is

* **Short-side next-day persistence** is the most reliable directional signal
  (weak, high-ATR names populate the next-day loser tail).
* **Mean-reversion / bounce-from-drawdown** is the strongest 4–8 week signal
  (`rs_index_60d` negative IC) — consistent with prior Falcon tier research.
* The **"will move big" (magnitude) prediction** is genuinely strong and could be
  productised differently (e.g. event/volatility screening), but it does not
  satisfy the directional-overlap objective as written.

## 6. Recommendation

The engine, loop, and explainable output are built and self-improving. The honest
ceiling on the spec's directional-overlap metric is single-digit-to-low-teens
percent, far below 70–80%. Options to discuss with the operator (see chat).
