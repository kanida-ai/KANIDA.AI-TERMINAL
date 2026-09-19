# KANIDA ARENA — Autonomous Learning & Constitutional Layer (SPEC v1)

*A virtual city of autonomous traders: **one worker per stock**, each learning point-in-time to trade
its stock better every day. The worker never replaces the prediction engine — it continuously improves
**how the engine's signals are used**. Each worker is a serializable, auditable, **sellable per-stock
agent** (roster + constitution + track record). The Arena's collective intelligence compounds daily.*

Sits on top of the unified micro+macro engine. Real-money orders stay human-gated. Paper/research.

---

## 1. Two planes (share the engine; isolate the learning)
- **Static plane (precompute once):** for every historical day, which signals fired on a stock + their
  **realized outcome distributions** (full-win / partial / scratch / small-loss / large-loss) + minute
  MFE/MAE. The engine already produces this. Do NOT re-mine each day.
- **Learning plane (online replay):** each worker replays the timeline day-by-day, scoring its signals,
  allocating capital, promoting/retiring — cheap, and cleanly separated from "what the engine knew".

## 2. THE OBJECTIVE — maximize long-term Expected Trade Value (ETV), not precision
Precision / win-rate are **diagnostics, not objectives.** A 95%-win signal with a −10% tail is a losing
system (the *steamroller*); a 65%-win signal whose losers behave well can out-earn an 85%-win one.

**ETV = Σ Pᵢ · Rᵢ** over the *full* realized-outcome distribution — so partial winners, scratches, small
losses and large losses all count. This is the reconciliation of our earlier debate: **ETV automatically
accounts for both the intrinsic outcome (the +X% target) AND the loss side/tail.** The system therefore
optimizes **ETV**, and treats precision as a health check.

> Core lesson (from the ETV scenarios): to improve a good signal, **eliminate the 5% tail** — don't chase
> precision 70%→72%. **Tail-loss control is therefore first-class** (see §5).

## 3. The Constitutional Score (what actually ranks a signal)
Never optimize ETV alone. Every signal is judged, point-in-time, on a multi-dimensional score:

| Dimension | Purpose |
|---|---|
| **Expected Trade Value (ETV)** | profitability (the objective) |
| Win-rate / precision | reliability *diagnostic* only |
| **Sample size → Confidence** | statistical trust (10=v.low … 300+=v.high) — never trust high-ETV on tiny n |
| **Return variance / downside deviation** | stability |
| **Max drawdown** | tail risk |
| **Holding time** (ret/day, /hr, /min) | capital efficiency — measured *independently*, does NOT bias ETV |
| **Signal frequency / Capacity** | deployable return, scalability, liquidity |
| **Market-regime robustness** | works in bull/bear/sideways |
| **Recent drift** (recent vs historical) | detect decay *before* capital is lost |
| **Net-of-costs** (slippage, brokerage, taxes, impact) | optimize *net*, not theoretical |

`Score = f(ETV, Confidence, Variance, MaxDD, Capacity, HoldingTime, Regime, RecentDrift, NetCosts)`
→ drives **proportional** capital allocation and the roster.

## 4. Roster (Keep / Watch / Test / Retire) — tied to confidence + score
- **Test** — low confidence (thin sample): trade *small* to gather evidence (exploration).
- **Watch** — moderate confidence, marginal score: *reduced* size.
- **Keep** — high confidence + positive score + acceptable variance/drift: *full* size.
- **Retire** — negative ETV or adverse drift (with confidence): *zero* size (auto-revisit).
Allocation is **proportional to score × confidence**, not binary — this fixes the v1 whipsaw.

## 5. Tail-loss control (the v1→v2 fix)
v1 had no stops → time-stop exits produced the "steamroller" losses that made books negative. v2 makes
**per-signal loss control first-class**: a stop (a-priori-sized, e.g. ~2× target adverse — wide enough to
avoid whipsaw, tight enough to bound the tail) truncates the left tail → **directly lifts ETV.** This is
more impactful than any signal-selection tweak.

## 6. Leak-proof walk-forward replay (the make-or-break)
One global clock, 2015→2026. For each day D, each worker: (1) sees only data ≤ D; (2) scores its signals
from its **own prior closed trades**; (3) allocates + paper-trades the best-scored *firing* signal (single
book per stock → no over-trading); (4) advances to close, observes realized outcome + MFE/MAE; (5) updates
score/roster. **Frozen hyperparameters** (window sizes, confidence bands, stop, thresholds) set a-priori
(or tuned only on 2015–2019 and frozen) so we validate the *whole* system on the sealed years — we must
not overfit the learner.

## 7. Feature / pattern escalation (add knowledge only when needed)
1. **Re-weight** the existing roster (always on, cheap).
2. **Adopt from a curated library** when under target — the neutral micro/macro basis **+ an EVENT module**
   (corp-actions, F&O expiry week, day-of-week, Monday gap/volatility, results-day, month-end). A worker
   admits a library feature only if it shows point-in-time lift on **its** stock on a held-out window.
3. **Trigger fresh discovery** (mine new combinations) — last resort, only when 1–2 can't reach target.

## 8. Cross-learning & Global Discovery Pool
A signal that is high-ETV + confident + consistent + frequent + robust **across many stocks** is promoted
to a **Global Discovery Pool** in `KANIDA_SNR.db`. Every worker may *independently validate* it on its own
stock (in Test) before adopting — a marketplace of reusable intelligence, validated per-stock.

## 9. Constitution — immutable vs mutable
- **Immutable:** strict point-in-time; sealed-vault; min-sample before trading; significance gate for
  promotion; hard risk limits (max exposure, per-trade stop, daily loss limit, kill-switch); ETV is the
  objective; **risk limits always win over the return target.**
- **Mutable (the worker evolves):** roster membership, capital weights, thresholds, long/short mix, active
  library features/events, per-signal stop widths within bounds.

## 10. Objective bar & honesty
North star ≈ **1%/day (or 5%/5d) risk-adjusted** — pursued by raising ETV (better pre-behavior mining +
tail control) × up-to-5× leverage on *genuine* edge. The target **drives better signal quality, never
reckless sizing**; the immutable risk limits can refuse a target-chasing trade. Precision is the ceiling
the learning layer keeps raising; ETV-net-of-costs is what we bank.

## 11. Data model & sellable agent
`signal_events`, `paper_trades`, `roster_state` (stock×signal×date: status, weight, ETV, confidence,
variance, drift), `constitution_log` (every decision + why, point-in-time), `global_discovery_pool`. Each
**worker = a serializable object** (roster + weights + constitution params + track record) → a portable,
auditable, **sellable per-stock agent**.

## 12. Build status
`arena.py` = v1 (proved the mechanism: cut loss ~34% + drawdown, but over-traded + whipsawed → precisely
the failures this spec fixes). `arena_v2.py` = this spec: **ETV + Constitutional Score + tail-control +
proportional allocation + significance-gated roster.**
