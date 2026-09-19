# KANIDA Unified Micro+Macro Engine — SPEC v1

*One mini quant engine per stock. Outcome → Behavior → Opportunity. Micro (1-min) and macro (daily +
WTD + MTD, all point-in-time to-date) features live in the SAME pattern, so a pre-move signature can be
micro-only, macro-only, or a combination. The same learned patterns emit **EOD signals** and **live
intraday signals** (every 1-min candle). Architected for low-latency live serving.*

Non-negotiables carried from the playbook: leak-free point-in-time features, sealed-vault validation,
Keep/Watch/Test/Retire lifecycle, never fabricate a pass. Real-money orders stay human-gated.

---

## 1. The core idea (why unified, not two engines)
A separate "intraday engine" could never discover a pattern that mixes *"T-1 showed heavy hidden
accumulation (micro)"* **AND** *"stock is in a 20-day compression near its 60-day high (macro)"*.
Real edge is often exactly that combination. So there is **one feature vector per stock per moment**
containing both micro and macro features, and **one miner** that discovers combinations across both.

## 2. The two planes (share the SAME atom code → backtest = live)
- **Mining plane (offline, heavy):** run the atoms over deep 1-min history (2015→) to build the
  point-in-time feature frame, mine outcome-first combination patterns, validate on the sealed year,
  tag Keep/Watch/Test/Retire per stock. Writes rules to `KANIDA_SNR.db`.
- **Serving plane (live, light):** the SAME atoms consume the live 1-min stream; every minute they
  update in O(1) and the compiled Keep rules are matched vectorised across all stocks. Emits signals.
- **Parity guarantee:** identical `Atom.update(bar)` code in both planes ⇒ what is backtested is what
  fires live. No re-implementation, no drift.

## 3. Atom framework (the speed core)
An **atom** is a tiny, independent, composable feature computer with an **incremental O(1)/bar** update:
```
class Atom:
    def update(self, bar): ...      # 1-min bar (ts,o,h,l,c,v) or daily bar — O(1), no rescans
    def value(self): ...            # current point-in-time value (as-of now / to-time)
    def reset_day(self): ...        # intraday atoms reset at the 09:15 boundary; macro atoms don't
```
- **No look-ahead by construction:** an atom only ever sees bars up to *now*. WTD/MTD/to-time features
  are running accumulators — leak-free by design.
- **Micro atoms** (reset each day): running ATP/VWAP, cum-volume-to-time, return-from-open,
  range-to-time, absorption counter, iceberg counter, ATP-vs-price, ATP-push.
- **Persistent baselines** (do NOT reset daily): EWMA of per-bar volume and per-bar range — used to
  decide "is THIS bar's volume/volatility unusual". In production these are seeded from the
  per-minute-of-day historical profile for accuracy.
- **Macro atoms** (update on daily bars / at EOD, all point-in-time to-date): daily technicals (ROC,
  ATR%, dist-from-highs, RSI, MA slope, RS-vs-Nifty, compression counts) **plus WTD (week-to-date),
  MTD (month-to-date)** running accumulators (extensible to QTD/YTD). "Daily" is shorthand for this
  whole slower-horizon to-date context, not just the daily bar.

## 4. The micro↔macro bridge (the elegant unification)
Each micro atom's **end-of-day value becomes a daily (macro) feature** carried forward as **T-N**
context. Example: today's total `absorption_score` is stored as a daily feature; a later EOD signal can
reference `absorption_score[T-1..T-N]`. So the **same atom** feeds:
- the **live intraday** signal (its value *right now*, mid-day), and
- the **EOD / next-day** signal (its completed values from prior days).

**Lookback depth is a PARAMETER, not hardcoded.** T-1/T-2/T-3 is only an illustration — N is
configurable (T-4, T-5, … any), and the miner is *given* T-1…T-N and **discovers which depths matter
per stock** (some footprints show at T-1, others at T-5). More depths ⇒ more feature surface ⇒ the
sealed-vault + occurrence/stability gates do the pruning. Same principle as the feature basis: we don't
pre-decide; the data does.

## 5. Micro-structure feature BASIS — GENERAL discovery, NOT a fixed list of use cases
**CRITICAL PRINCIPLE.** The iceberg/absorption examples are ILLUSTRATIONS, not the target set. Markets
have countless pre-move footprints, **different for every stock**. The engine is OUTCOME-FIRST: it
exposes a **rich, NEUTRAL basis** of 1-min measurements and the **miner DISCOVERS** whichever
combinations — and their thresholds — precede each stock's outcomes, **including behaviours we never
hypothesised**. Put the hypothesis in the DATA (miner learns), not in hand-coded detectors.

- **Neutral per-minute primitives (raw, unopinionated):** volume z-score vs typical-for-this-minute;
  signed volume (up-vol vs down-vol); price-impact per unit volume (|ret|/vol); realized vol / range;
  close-location; ATP slope; price↔ATP crossings; gap follow-through; opening-range interaction.
- **Intraday to-time aggregates:** volume concentration/entropy, session-third volume split, counts by
  volume decile, # tight vs # wide bars, max-burst z-score, buy/sell imbalance, **compression→expansion
  and dry-up→surge transitions (sequences A→B→outcome)**.
- **Thresholds are DATA-DISCOVERED** — the random forest picks its own split points per stock; we do
  NOT fix "high volume = 2×". The iceberg/absorption/ATP-push counts are just a FEW convenience features
  inside this basis, given no special weight.
- **The basis is designed to keep GROWING** — richer, more expressive vocabulary ⇒ more discoverable
  behaviours. Nothing privileges the two examples.
- Combined with the macro features and rolled over T-1/T-2/T-3 for the EOD signal.

**Honest bound:** the engine discovers behaviours *expressible in the basis*, with enough samples, that
survive the sealed vault — not literally "everything". So the job is to maximise a neutral, expressive
basis and let the outcome-first miner do the discovery. We expand the basis as new raw measures are
identified; we never pre-decide the answer.

## 6. Outcomes / labels (unchanged)
Forward moves per stock: **±1% (1D), ±2% (2D), ±5% (5D)** touch-targets. Minute-level **MFE / MAE**
(max favourable / adverse excursion) tracked to the target for every historical firing → the vision's
"track drawdown at minute level before reaching target".

## 7. Mining (outcome-first, combinations, per stock)
Reuse the proven framework: per (stock × outcome), shallow random-forest leaf-rule extraction over the
**combined micro+macro** feature frame → dedup → gate on occurrence + lift → promote on the validation
year → confirm on the sealed year → Keep/Watch/Test/Retire. High-occurrence + stable + recently-working
only (no low-sample fake edges).

## 8. Signal emission (two modes, one rule library)
- **EOD signal:** at close, evaluate rules whose features reference completed days (incl. T-1/T-2/T-3
  micro footprints) → "these stocks match a pre-move state as of today's close."
- **Intraday signal:** at **every 1-min candle** (default; can switch to event-triggered), evaluate
  rules whose features are to-time intraday → "these stocks are forming the pre-move footprint RIGHT
  NOW." Order-type drives which mode matters (MIS/intraday vs CNC/NRML/EOD).
- Output is decision-ready: *"TORNTPHARM — compression + hidden accumulation (T-1,T-2). This state →
  72% / +1% in 1D, 81% / +2% in 2D, 90% / +4% in 5D (n=…); minute MFE +x% / MAE −y%."*

## 9. Live-fast architecture (payload + modules)
- **Struct-of-Arrays (SoA):** per feature, one `float32` numpy array of length N=#stocks. The live
  **payload** is the `N×F` feature matrix — compact, cache-friendly, vectorisable.
- **Incremental atoms:** O(1)/bar ⇒ per-minute cost = O(N × #atoms), no rescans of history.
- **Compiled rules:** each Keep pattern pre-compiled to arrays `(feature_idx, op, threshold)`. Matching
  all P patterns × N stocks = a handful of vectorised numpy comparisons + AND-reductions (no Python
  loop over patterns per stock). Target: **full-market match in single-digit milliseconds**.
- **Two clocks:** macro atoms update once per day (EOD/open, cached); only micro atoms update per
  minute → the per-minute hot path stays tiny.
- **Modules:** `atoms.py` (compute core) · `state.py` (StockState + BatchState SoA) · `features.py`
  (registry, micro+macro) · `patterns.py` (rule compile + vectorised match) · `signals.py` (EOD +
  intraday emit + MFE/MAE) · `miner.py` (offline mining) · served behind the existing OMS.
- **Cloud/live:** stateless minute-workers over the SoA payload, one signal computed per symbol then
  fanned out per user/broker (compute-once, route-many) — scales to 1000s of users on the messaging
  layer, not the compute layer.

## 10. Honest limits
- 1-min OHLCV + ATP ≠ true tick/order-book flow. We mine the **observable footprint** of
  accumulation/iceberg (volume-price-volatility signatures); real order flow (live-only, deferred)
  will sharpen it.
- Needs the deep 1-min history (2015→) — the backfill running now is that fuel.
- Intraday multiplies sample count (more statistical power) but also more multiple-testing surface →
  the sealed-vault + occurrence/stability gates matter even more.

## 11. Module layout (`kanida_engine/`)
`SPEC.md` (this) · `atoms.py` (built) · `selftest.py` (built) · then `state.py`, `features.py`,
`patterns.py`, `signals.py`, `miner.py` wired once the 1-min backfill completes.
