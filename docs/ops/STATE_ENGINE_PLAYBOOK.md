# KANIDA.AI — State Engine Playbook (v0.1)

**Owner:** KANIDA AI · **Date:** 2026-08-04 (IST) · **Status:** DESIGN — direction phase

> Purpose: replace the current rank brain (`score = Σ oos_lift`, rank by `avg_lift`) with a
> **profitability-first, leak-proof, direction-aware ranking** driven by net-cost expectancy.
> Same pipeline, same `falcon_signals_live` output contract — only *how patterns are weighted into rank* changes.

---

## 1. Core principle — detect STATES, do not predict

What we call a "pattern" is really a **compound condition that defines a STATE** — a point-in-time
regime snapshot, e.g.:

```
atr_20_pct > 10.6338  AND  dist_sma_20 <= -2.1675  AND  n_sub_3_range_7d <= 0.5
= "violently volatile, below its 20-day mean, no quiet days in the last 7"
```

The engine's job is **recognition, not forecasting**:

| | STATE DETECTION (what we build) | POINT-IN-TIME PREDICTION (what fails) |
|---|---|---|
| Question | "Is the stock in state S today?" | "What return follows this snapshot?" |
| Output | **Yes / No — a fact** | A forecast — a guess |
| Fragility | none (it's a definition) | where leak / beta / overfit enter |

The state is clean and real. The *prediction stapled onto it* is what rots. We measure outcomes
**empirically and net of cost** — we never bolt a predicted lift onto the state.

---

## 2. The measuring instrument — one fixed trade, zero tunable knobs

```
Detect state at EOD(T)  →  ENTER T+1 @ 09:15 (open)  →  EXIT T+1 @ EOD (close)
```

- One intraday session. Hold open → close.
- Measured **NET of all friction**: brokerage + STT + exchange charges + GST + stamp duty + slippage.
- Evaluated on **BOTH** sides, independently — never assume a state is only Long or only Short:

```
Long  Net Return = (close − open) / open  −  cost
Short Net Return = (open − close) / open  −  cost
```

Direction is decided on this **parameter-free** trade precisely because it has nothing to overfit.
(Long and Short are near mirror images; the real question is whether the state produces a reliable
one-way net drift large enough to clear costs on either side. Near-zero drift → **Reject**.)

**Trailing is Stage 2, not now.** A trail can raise captured return (ride toward the intraday high /
max-favorable-excursion) and cut drawdown (exit an adverse move early) — but it introduces parameters,
so we fix **direction first** on the clean baseline, then *prove* whether a trail beats it net of its
own whipsaw. Trailing never decides Long vs Short.

---

## 3. What the engine computes — per (STATE × STOCK)

Unit of analysis is the **(State S, Stock X)** pair — stock-specific, not one pooled number per state.

```
1. DETECT     every EOD(T) where stock X satisfies state S  → list of occurrence dates
2. SIMULATE   for each occurrence, the fixed T+1 open→close trade, Long AND Short, net of friction
3. LABEL      Win = Net Return > 0 ;  Loss/Scratch = Net Return ≤ 0     (per side)
4. AGGREGATE  per side: N, WinRate, AvgWin, AvgLoss, NetExpectancy, ExpectedCumReturn, PF, MaxDD, Persistence
5. DIRECTION  Long ETV vs Short ETV → LONG / SHORT / DUAL / REJECT
6. STATUS     KEEP / WATCH / TEST / RETIRE   (ETV sign + confidence + drift)
7. RANK       all records by ExpectedCumReturn ▸ NetExpectancy ▸ Confidence ▸ PF ▸ MaxDD ▸ Persistence
```

---

## 4. The scorecard (one record per State × Stock)

```
STATE #40921  ×  STOCK: KEC
├─ Preferred Direction : LONG        [ side with higher ETV, and ETV > 0 ]
│   └─ Long ETV +0.85%   vs   Short ETV −0.40%
├─ Trade Basis         : detect EOD(T) → 09:15 T+1 open → EOD T+1 close, NET of friction
├─ Occurrences (N)     : 142 trades / 3 yrs        [ Confidence: HIGH ]
├─ Trade Distribution  :
│   ├─ Win Rate  P_win : 64.2%   │ Avg Win  : +1.80%   [ mean Net Return of winners ]
│   └─ Loss Rate P_loss: 35.8%   │ Avg Loss : −0.90%   [ mean |Net Return| of losers ]
├─ Expectancy Engine   :
│   ├─ Net Expectancy  : +0.85% / trade      = P_win·AvgWin − P_loss·|AvgLoss|
│   ├─ Expected Cum.Ret: +120.7%  ──► STRATEGY KPI (earning power)  = NetExp × N
│   └─ Profit Factor   : 3.59                = (P_win·AvgWin) ÷ (P_loss·|AvgLoss|)
├─ Risk & Stability    :
│   ├─ Max Drawdown    : −12.4%              [ peak-to-trough of the occurrence-sequence equity curve ]
│   └─ Recent Persist. : 1.04                [ recent-window ETV ÷ lifetime ETV; ~1 stable, <1 decaying ]
└─ Lifecycle Status    : KEEP               [ Ranked #14 by Expected Cumulative Return ]
```

---

## 5. Field definitions (exact)

```
Win                   = Net Return > 0%      (friction already subtracted)
Loss / Scratch        = Net Return ≤ 0%
Net Expectancy (ETV)  = P_win·AvgWin − P_loss·|AvgLoss|        ; Loss Rate = 1 − Win Rate
Expected Cum. Return  = Net Expectancy × N   ── STRATEGY KPI (pattern earning power), NOT portfolio return
Profit Factor         = gross profit ÷ gross loss = (P_win·AvgWin) ÷ (P_loss·|AvgLoss|)
Max Drawdown          = worst peak-to-trough on the cumulative net-return curve of the N trades, in date order
Recent Persistence    = ETV(last ~12 mo) ÷ ETV(full history)          ; ~1 = stable, <1 = decaying
Confidence ladder     = <20 VeryLow · 20–49 Low · 50–99 Medium · 100–199 High · 200+ VeryHigh   (per-stock)
```

**Direction rule**
```
Long ETV > Short ETV  → LONG
Short ETV > Long ETV  → SHORT
both > 0              → DUAL   (prefer higher ETV, or run both if applicable)
both ≤ 0             → REJECT / RETIRE
```

**Lifecycle status**
```
KEEP   : ETV > 0 · Confidence ≥ High · Persistence ≥ ~0.8
WATCH  : ETV > 0 but decaying, or Confidence = Medium
TEST   : ETV > 0 but Confidence < Medium (too few occurrences to trust)
RETIRE : both directions ETV ≤ 0
```

---

## 6. Ranking key (the new rank brain)

Rank every (State × Stock) record — **not by precision** — by:

```
1. Expected Cumulative Return   (primary — earning power across all occurrences)
2. Net Expectancy               (per-trade edge)
3. Confidence / Sample Size
4. Profit Factor
5. Max Drawdown                 (lower is better)
6. Recent Persistence
```

**Objective:** maximize long-term trading profitability by choosing the **direction (Long/Short)** and
**pattern combination** with the highest Expected Cumulative Return, positive Net Expectancy, and
acceptable risk. Precision / win rate are diagnostics only — never the optimization target.

---

## 7. Strategy KPI vs Portfolio KPI — never conflate

| | Expected Cumulative Return | Portfolio Return |
|---|---|---|
| Meaning | earning power of one pattern/worker across **all** its occurrences | realized growth of the **investor's capital** (e.g. ₹10L → ₹18.43L) |
| Answers | *which states are worth trading?* | *what did the account actually make?* |
| Ignores | sizing, concurrency, capital limits, compounding | — |
| Accounts for | — | position sizing, capital allocation, concurrent trades, costs, compounding |
| Type | **Strategy KPI** | **Portfolio KPI** |

Track **both, independently**. Expected Cumulative Return selects the patterns; Portfolio Return is
computed only after position sizing / allocation / concurrency / compounding on real invested capital.
Never substitute one for the other.

---

## 8. Open items (to define in later parts)

- **Cost model** — exact ₹ friction per trade (brokerage, STT buy/sell, exchange txn, GST, stamp, slippage bps), Long vs Short (equity short = MIS-only, borrow/availability).
- **Stage 2 — trailing** — trail spec, and the test that it beats the clean open→close net of whipsaw.
- **Dual-direction handling** — run both sides, or take the higher-ETV side only.
- **Portfolio assembly** — how ranked (State × Stock) records become the daily basket (sizing, concurrency caps, max positions) and feed `falcon_signals_live` → 09:15 deploy → AutoTrade.

---
*This playbook is the direction-phase spec. It changes only the ranking objective; the feature pipeline,
pre-market deployer, and AutoTrade execution layer are untouched.*
