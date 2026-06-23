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

## 6. v2 — two-stage (EOD shortlist → next-open confirm) + daily loop

Per operator direction (2026-06-22, "don't relax the target — chase it with more
data: D-1/D-2, volume, next-day opening; robust model; daily feedback loop"):

* v1 analysis preserved → `outputs/persona_v1/persona_v1_analysis_2026-06-22.xlsx`.
* Built `opening.py` (overnight gap full-history + 9:15→9:45 1-min momentum, 2024+),
  `engine_v2.py` (Stage-1 EOD shortlist of 30 → Stage-2 morning confirm), and
  `daily_cycle.py` (the real-time EOD-predict → open-confirm → measure+learn loop).

**v2 walk-forward (random ≈ 4.7%):**

| Model | Long | Short | Capturable? |
|---|---|---|---|
| Stage-1 EOD (magnitude + lags) | **10.3%** | **14.6%** | yes (predictive) |
| GAP-confirm vs open→close | 9.6% | 9.2% | yes (from open) |
| 9:45-confirm vs **9:45→close** | 8.5% | 4.9% | **yes (the honest tradeable number)** |
| 9:45-NAME vs open→close | 18.4% | 25.2% | **no — screening only, look-ahead** |
| 9:45-NAME, *full universe* (no shortlist) | ~38% | — | no — screening only, look-ahead |

**Two key results:**
1. The **opening confirmation does not improve capturable returns** over the EOD
   model. IC of 9:15→9:45 momentum vs the *rest-of-day* (9:45→close) is **−0.015**
   (≈ 0). The big "38%" only appears when the metric (open→close) includes the
   09:15–09:45 window we already observed — a look-ahead artifact, honest only as a
   "today's movers" *screen*, never as predicted/capturable alpha.
2. v1's online IC weight-learning was **hurting longs**; the robust **static
   magnitude model lifts capturable long 2.9% → 10.3%** — a real, kept gain.

## 7. Bottom line

The directional next-day **capturable** top-10 overlap plateaus at **~10–15%
(≈ 2–3× random)** across price/volume/RS/lag/opening data. The spec's 70–80% is
not reachable on capturable returns — the rest-of-day after the open is efficient
(IC ≈ 0). The only path to a large overlap number is a **09:45 "today's movers"
screen measured on open→close (~25–38%)**, which is useful as a watch-list but is
partly look-ahead and not tradeable as stated.

Genuinely capturable edges that DID survive: **short-side weakness (~14–15%)**,
the **EOD magnitude long (~10%)**, and (separately) the **"will move big"
magnitude screen** (direction-agnostic). All are real and productisable — none is
80%-directional.

## 8. Data-class hunt — new NON-price data (operator: "keep hunting")

Fetched fresh classes from NSE at runtime (not derivable from price), to attack the
news-driven movers that price data can't see:

* **Earnings/results dates** — `corporate-board-meetings` API → `corp_earnings_dates`
  (66,295 rows / 2,454 symbols, 2022–26). Results-day stocks have a **2.0× lift** to
  be a top-10 mover (19.2% vs 9.8%), abs move 1.94% vs 1.41% — real, BUT only **3.2%
  of the actual daily top-10 movers had results that day** (too sparse in the F&O
  cross-section to move the aggregate).
* **Delivery %** — `sec_bhavdata_full` → `delivery_daily` (1.2M rows, 2024–26).
  Standalone it was the **best single feature** (next-day rank-IC +0.049, 20d +0.046).

**Integrated result (v2.1, 2024–26):** adding earnings + delivery to the F&O model
**did not help — it slightly hurt** the long side every year (2024 9.8 vs 11.6,
2025 8.0 vs 8.6, 2026 8.1 vs 10.0). Delivery's standalone IC **did not survive
integration / out-of-sample**: it is collinear with the volume/magnitude signals
already present and is regime-specific. Earnings dilute the top-10. Both weights were
reverted; the **data infrastructure is retained** (real, reusable: `events.py`,
`event_features.py`, `corp_earnings_dates`, `delivery_daily`, `persona_event_features`).

**Conclusion after the hunt:** across price/volume/RS/lags/opening/earnings/delivery,
the capturable directional next-day top-10 overlap is **~10–15% (2–3× random)** and
does not improve with the new classes. The daily top-10 movers are a long tail of
idiosyncratic catalysts (results ≈ 3%, plus block deals, broker actions, sector news,
macro) — no obtainable data class explains a large share. **70–80% directional
overlap is not achievable with any data we could acquire.** Classes that remain
genuinely unobtainable: deep OI/options (Kite doesn't expose expired-contract tokens),
real-time news/broker upgrades, and historical bulk-deal API (blocked).
