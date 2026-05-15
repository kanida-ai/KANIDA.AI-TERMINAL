# Falcon Auto-Trade — Requirements

**Spec version:** 0.3 (DRAFT — broker-decided leverage + holdings overlap + positions tab)
**Owner:** Pudhuraja
**Author:** Claude (AI implementer)
**Last updated:** 2026-05-05

---

## 1. Intent

Operator-supervised auto-execution of Falcon V7.1 daily picks on Zerodha. Reads top-N signals, generates an MTF batch order plan, places via Kite, surfaces overlaps with existing holdings (skip vs. average), tracks held positions with live SL/Target/Trail visibility, and provides a kill switch.

Engine code stays pure (signal generation only). The trade panel handles **how to act on signals** — sizing, MTF on/off, stops, exits, holdings reconciliation.

**This is NOT an unattended algo.** A human authorizes each batch.

---

## 2. Success criteria

**Phase 1 (this spec — pre-market entry + positions visibility):**

| # | Criterion | Measurable outcome |
|---|---|---|
| 1 | Token roundtrip works | "Refresh Token" → Kite OAuth → status flips to "Valid" |
| 2 | 3-section config form drives the plan | Operator adjusts inputs; preview updates correspondingly |
| 3 | TOP_N picks selected from `falcon_signals_live` | Default 14; configurable; honors walk-forward eligibility (engine-enforced) |
| 4 | MTF-ineligible stocks skipped | UI shows skipped count + symbols; excluded from plan |
| 5 | Position sizing uses PER_TRADE × broker leverage | qty estimated as floor((PER_TRADE × ASSUMED_MTF_LEVERAGE) / close); actual position size determined by Zerodha at order time |
| 6 | Cost model is read-only | Section 4 fields display only |
| 7 | Existing holdings fetched and reconciled | Panel calls `kite.holdings()` + `kite.positions()` at preview time; intersects with today's picks |
| 8 | Overlap decision per pick | For each pick that matches a held position: dropdown {Skip, Average} (default Skip) |
| 9 | Smoke test gates the batch | "Place Orders" disabled until 1-share BUY = PLACED |
| 10 | Two-step confirmation | Type "CONFIRM" + click |
| 11 | Idempotent placement | Re-clicking with same preview_id → no Kite duplicates |
| 12 | Audit trail | Every order has a row in `falcon_trade_orders` |
| 13 | Kill switch | "Cancel All" cancels every pending order in batch |
| 14 | Atomic-failure mode safe | BUY ok + SL fails → batch aborts, alert, naked long logged |
| 15 | `/falcon/positions` shows live SL/Target/Trail | Per held position: avg entry, current, P&L%, SL ₹ + Δ, target ₹ + Δ, trail status, days held / HOLD_DAYS, manual exit button |
| 16 | Trigger-watch surfaces what's about to fire | Top 3 closest-to-SL, closest-to-target, closest-to-time-stop visible at top of positions tab |

**Phase 2 (separate spec):** real-time price stream, daily SL re-place, trail enforcement, automatic time-stop. Out of scope.

---

## 3. Non-goals

- ❌ Unattended/scheduled execution
- ❌ Pre-open auction orders (9:00–9:08 IST window)
- ❌ Per-stock margin computation via `kite.order_margins()` — broker (Zerodha) determines per-stock margin/leverage at order time. Panel uses an internal default for preview-time qty estimation only
- ❌ User-selectable LEVERAGE — removed from the panel; not a strategy decision
- ❌ MAX_OPEN slot cap — cash-bounded, no UI element
- ❌ CNC fallback for MTF-ineligible — skip
- ❌ Bracket / Cover orders. SL is a separate regular order
- ❌ GTT-based stop-loss
- ❌ Multi-strategy selection (Falcon V7.1 only for now)
- ❌ Editing Score method, walk-forward, or family filter (engine invariants)
- ❌ Real-time price streaming (Phase 2)
- ❌ Auto-flatten EOD

---

## 4. Engine invariants (enforced in code, never exposed)

| ID | Invariant | Where enforced |
|---|---|---|
| E1 | Walk-forward eligibility (mined_year < signal_year) | `pattern_loader.load_promoted_patterns()` |
| E2 | V7.1 family filter (drops drawdown_bounce) | `pattern_loader` |
| E3 | Score method = sum_oos_lift | `signal_runner` |

Exposing any of these would let someone accidentally turn the engine into a backtest-overfit machine.

---

## 5. Safety constraints (non-negotiable)

| ID | Constraint | Enforcement |
|---|---|---|
| S1 | Kite token must be valid | Pre-flight `kite.profile()`; expired → block UI |
| S2 | Market open (≥9:15:00 IST, weekday) | `/place` rejects with `MARKET_CLOSED` |
| S3 | Smoke test required before batch | "Place Orders" stays disabled until smoke = PLACED |
| S4 | Two-step confirmation | Type "CONFIRM" + click |
| S5 | Idempotency on every order | `tag = f-{batch_8hex}-{sym_8}-{role[0]}` |
| S6 | Atomic per-stock or abort | BUY ok + SL fails → abort, alert, naked long logged |
| S7 | Equity check before placing | `(TOP_N × PER_TRADE) ≤ Total capital` else preview rejected |
| S8 | High-utilization warning | Yellow when `(TOP_N × PER_TRADE) > 0.9 × Total capital` |
| S9 | Kill switch one click | "Cancel All" |
| S10 | Full audit log | Every attempt → row in `falcon_trade_orders` |
| S11 | No CNC fallback for MTF-ineligible | Skip |
| S12 | Only one active batch per day | Reject `/place` if existing batch is PLACING for today's `entry_date` |
| S13 | Holdings overlap surfaced before placing | `/preview` always returns `existing_positions[]` and overlapping picks; default action = SKIP. Operator must explicitly choose AVERAGE |
| S14 | Manual exit confirms first | `/falcon/positions` exit button → modal → cancel SL → market sell |

---

## 6. Inputs — three-section panel (no LEVERAGE knob)

### Section 1 — Strategy / Signal

| Field | Default | Type | Notes |
|---|---|---|---|
| Strategy | `Falcon V7.1` | dropdown | Read-only for now |
| Quick top-N preset | `14` | int input + Apply button | UI helper; pressing Apply fills `selected_symbols` with the top-N MTF-eligible. Operator can then add/remove via per-row checkboxes |
| Selection (X selected) | top-14 MTF on load | derived list | The actual list of symbols that will be considered for the batch. Driven by per-row checkboxes in the signals table. Two quick actions next to it: `all MTF` (select every MTF-eligible signal) and `clear` (deselect all) |
| Score method | `sum_oos_lift` | hidden | Engine-enforced |
| Walk-forward | `auto` | hidden | Engine-enforced |

**Selection rules:**
- Every row in the signals table has a checkbox in the leftmost "Pick" column
- Non-MTF rows: checkbox disabled (cannot be selected)
- Held-overlap rows: still selectable; the per-row Action dropdown (Skip/Average) only appears when both checked AND held
- Operator workflow: type a number → click Apply → top-N MTF-eligible auto-checked → manually adjust (e.g. uncheck held overlaps you don't want, check stocks below TOP_N like rank 15 PTCIL to substitute)

### Section 2 — Sizing / Capital

| Field | Default | Type | Notes |
|---|---|---|---|
| Total capital | `₹50,00,000` | int input | Account size |
| Cash per trade (PER_TRADE) | `₹3,00,000` | int input | Margin you put up per trade |
| Use leverage (MTF) | `ON` | toggle | Switches product MTF ↔ CNC. **No LEVERAGE multiplier knob** — broker-decided |

**No leverage assumption anywhere.** No user knob, no internal constant. Preview shows only certain numbers (notional = `selected_count × PER_TRADE`). Zerodha's actual per-stock margin is computed at order placement time — preview cannot and does not predict it.

For order quantity at placement: `qty = floor(PER_TRADE / close)` — the operator is committing ₹PER_TRADE worth of stock per pick. Whether MTF makes that cheaper for them in cash terms is the broker's concern, not the panel's.

UI caveat beneath Section 2: *"MTF ON: Zerodha determines actual margin per stock at order time. With leverage your real cash usage is below the notional shown."*

### Section 3 — Exit / Risk

| Field | Default | Type | Notes |
|---|---|---|---|
| Hold days (HOLD_DAYS) | `7` | int input | |
| Initial stop loss | `-7%` | float | |
| Trail trigger (TRAIL_TRIGGER) | `+10%` HW | float | |
| Trail lookback | `10-day low` | int | |

### Section 4 — Cost model (read-only)

| Field | Default | Type |
|---|---|---|
| MTF interest rate (MTF_RATE) | `15% p.a.` | display |
| Brokerage round-trip | `30 bps` | display |
| Slippage each side | `5 bps` | display |

---

## 6.5 Holdings overlap handling (NEW)

### Detection
At `/preview`, the panel:
1. Calls `kite.holdings()` → list of all CNC + MTF longs (including overnight positions)
2. Calls `kite.positions().net` → list of intraday + same-day MTF positions
3. Builds `held_by_symbol = {symbol: HeldPosition}` from the union
4. For each pick in TOP_N (or custom subset), tags `is_overlap = (pick.symbol in held_by_symbol)`

### Decision UI
For each overlapping pick, the signals table shows:
- Existing-holding badge: `"Hold {qty} sh @ ₹{avg_entry} ({pnl_pct:+.1f}%)"`
- Inline dropdown {`Skip` (default), `Average`}

### Plan execution
- `Skip` (default) → pick excluded from order plan; existing position rides alone
- `Average` → pick included; placed as MTF BUY adding to existing position. Avg entry is recomputed by Kite. Initial SL recalculated from **new position avg entry**, not the original

### Banner
If any overlap exists, a blue banner above the signals table summarizes:
- Count of held positions
- List of overlapping symbols (pills)
- Default action (Skip) and how to override

---

## 7. Derived values (live in UI)

Only certain numbers — no fake leverage estimates:

- **Selected (count)** = number of MTF-eligible symbols checked in the table
- **PER_TRADE** = operator input (₹ committed per pick)
- **Total deployed** = `selected_count × PER_TRADE` — total notional being committed today
- **Utilization** = `total_deployed / total_capital × 100` — conservative; with MTF on, real cash used is lower
- **Headroom** = `total_capital - total_deployed` — conservative; with MTF on, real headroom is higher
- **Overlap with held** = count of selected picks that match a held position symbol

Drop entirely (no fake numbers): "Est position each", "Est total notional", "Est equity used", "Est borrowed", "Est daily MTF interest". These all required a leverage assumption; broker decides at order time, so preview can't know.

`Total deployed > total_capital` → preview rejected with `INSUFFICIENT_CAPITAL`.
`Utilization > 90%` → yellow warning (conservative; with MTF on, you may have more room).
`Overlap > 0` → blue banner.

---

## 8. Outputs

- `falcon_trade_runs` row per batch
- `falcon_trade_orders` rows per order
- `/falcon/positions` view of held positions (read-derived from `falcon_trade_orders` + `kite.holdings()` + `kite.positions()`)

---

## 9. Operator workflow (happy path)

```
T-45 (8:30 IST)  Open /falcon/trade. Token check.
                 Glance at /falcon/positions to see what's held + what's near triggers.
T-30 (8:45 IST)  Section 1: TOP_N=14. Section 2: Total=₹50L, PER_TRADE=₹3L, MTF=ON.
                 Section 3: HOLD=7, SL=-7%, Trail=+10%/10d.
                 See blue banner: "5 of today's 14 picks overlap with held positions"
                 For each overlap, decide Skip (default) vs Average. Default: hold existing.
T-15 (9:00 IST)  Click "Generate Preview". 
                 14 picks → 12 MTF-eligible → 7 new + 5 skipped (overlaps held alone).
                 Or override 1 to Average → 8 new + 4 skipped + 1 averaging.
T-2  (9:13 IST)  Click "Smoke Test". 1-share BUY of cheapest. Confirm fill.
T 0  (9:15 IST)  "Place Orders" enables. CONFIRM → batch fires.
T+30 (9:15:30)   Watch per-row PLACING → PLACED.
T+1  (9:16 IST)  Goto /falcon/positions to see updated held list with new SL/Target.
```

---

## 10. Failure modes

| Mode | Trigger | Response |
|---|---|---|
| F1: Token expired at start | Status invalid | Block UI, refresh banner |
| F2: Token expires mid-batch | Kite 403 | Abort, alert, log |
| F3: MTF cache stale | TTL > 24h | Auto-refresh `kite.instruments()` on first /preview after 6 IST |
| F4: Smoke test rejection | Kite rejects | Show error, place stays disabled |
| F5: BUY ok + SL fails | SL placement errors after BUY filled | Abort, alert, audit (S6) |
| F6: Operator clicks Place < 9:15:00 | Time check | Reject `MARKET_CLOSED` |
| F7: Margin breach (per-stock) | Kite rejects | Skip that order, log, **continue** batch |
| F8: Kite rate-limit (429) | Burst | Linear backoff per order; abort if any 429 retry fails |
| F9: Network blip | HTTP timeout | Retry once; mark FAILED if still fails; continue |
| F10: Operator double-clicks Place | Two clicks within 1s | Idempotency dedupes |
| F11: Equity check fail | TOP_N × PER_TRADE > Total | Reject `INSUFFICIENT_CAPITAL` |
| F12: Concurrent batch attempt | Existing PLACING run | Reject `BATCH_IN_PROGRESS` |
| F13: `kite.holdings()` fails | API error at /preview | Soft-fail: show banner "Holdings unavailable — overlap check disabled. Manual review recommended" + allow proceed |
| F14: Manual exit during batch placement | Operator clicks Exit on /positions while /place is running | Defer: queue the exit; execute after batch completes |

---

## 11. Out-of-scope failure modes

- Kite SDK breaking change → manual ops
- Power loss mid-batch → restart from `falcon_trade_runs` audit
- Wrong parameter values → operator preview-step responsibility
- Falcon engine produces bad signals → upstream concern

---

## 12. `/falcon/positions` page requirements

A separate tab listing every currently-held position with:

| Column | Source | Notes |
|---|---|---|
| Symbol + Sector | `falcon_trade_orders` join `falcon_signals_live` | |
| Qty | `kite.holdings()` / `positions()` | live |
| Avg entry | broker | |
| Current price | broker | refresh every 5s in v1 (later: Ticker) |
| P&L % | derived | green if ≥0, red if <0 |
| SL ₹ | `falcon_trade_orders` (role=STOP, status=PLACED) | the live SL trigger |
| Δ to SL | derived `(current - sl) / current × 100` | red <3%, yellow <5% |
| Target ₹ | derived from SL + initial trail trigger | the +10% HW level |
| Δ to Target | derived | amber <3% |
| Trail status | derived from HW + trail_lookback | "trailing @ ₹X" once HW reached, else "not yet" |
| Days held | `today - entry_date` | red if ≤1 to T+HOLD_DAYS, yellow if ≤3 |
| SL type | `falcon_trade_orders.order_type` | SL-L / SL-M |
| Action | UI button | "Exit now" → confirm modal → cancel SL → market sell |

Plus a **Trigger Watch** strip at top showing top 3 closest-to-SL, closest-to-target, closest-to-time-stop. Helps operator spot what's about to fire today.

---

## 13. Open items

None outstanding. Ready for approval review.
