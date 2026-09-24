# Options Screener: layout sketch and field map (APPROVED 22 Sep 2026; built, see OPTIONS_SCREENER_RESULT.md)

Requirements: `OPTIONS_SCREENER_SPEC.md`. No code has been written yet.

## Facts checked in the stores (read-only, 22 Sep 2026, pre-open)
- `db/derivatives.db` covers 216 underlyings, with 27,379 contracts in scope and 26 readings per session,
  09:30 → 15:30 plus a 15:45 post-close mark. The last capture was 21 Sep 15:45, status ok.
- "All available expiries" means the **front two expiries** per underlying. That is the capture scope
  (`contracts.in_scope`). Nothing further out is captured.
- Each expiry carries far more strikes than ATM ±5 (NIFTY has 96 to 128 a side), so the default ATM ±5
  band and any custom band both fit.
- NIFTY's weekly expiry is **today (22 Sep)**. `implied_vol.py` refuses IV on expiry day (`expiry_today`),
  so today's NIFTY contracts will show IV as "withheld" rather than a number.

---

## (1) Condition builder

```
┌─ New scanner ─────────────────────────────────────────────────────────────┐
│  Tell KANIDA the behaviour you want to find                                │
│  ┌──────────────────────────────────────────────────────────────┐ [Build] │
│  │ call OI building for 45 min and call IV expanding, F&O stocks │         │
│  └──────────────────────────────────────────────────────────────┘         │
│   "Understood as ↓" (the same definition fills the chips below)            │
│                                                                            │
│  Look in   [ F&O stocks ▾ ]  [ Nearest expiry ▾ ]  [ ATM ±5 ▾ ]            │
│                                                                            │
│  ① [ OI ] › [ Calls ] › [ ↑↑ Increasing continuously ] › [ 45 min ]   ✕    │
│         AND  ⇄ (tap to switch AND / OR)                                    │
│  ② [ IV ] › [ Calls ] › [ ↑ Expanding ] › [ 30 min ]                  ✕    │
│                                                                            │
│  [ + Add condition ]                                   ⚙ More options      │
│                                                                            │
│  Reads as: "Call OI has risen at every reading for the last 45 min AND     │
│  call IV has expanded over the last 30 min, within ATM ±5 of the nearest   │
│  expiry, across F&O stocks."                                               │
│                                                                            │
│  [ Run now ]   [ Save as… ]   🔔 Notify: ☐ new match ☐ ended ☐ changed    │
└────────────────────────────────────────────────────────────────────────────┘
```

Every chip is a tap-to-pick sheet: a short list of words, never a number box.

```
 Tap [ OI ▾ ]               Tap [ Calls ▾ ]      Tap [ state ▾ ] (only the words valid for OI)
 ┌───────────────────────┐  ┌─────────────┐      ┌──────────────────────────────────┐
 │ POSITIONS  OI         │  │ Calls       │      │ ↑  Increasing                    │
 │            Build-up   │  │ Puts        │      │ ↑↑ Increasing continuously       │
 │ PRICE      Premium    │  │ Either side │      │ ⇈  Expanding rapidly             │
 │            Underlying │  │ Both sides  │      │ →  Stable                        │
 │ VOLATILITY IV         │  └─────────────┘      │ ↓  Decreasing                    │
 │ ACTIVITY   Volume     │                       │ ↓↓ Decreasing continuously       │
 │ BOOK       PCR        │  Tap [ 45 min ▾ ]     │ ⇊  Contracting rapidly           │
 │            Max pain   │  15 · 30 · 45 · 60    │ ↗  Reversing higher              │
 │ SPREAD     Breadth    │  Since open           │ ↘  Reversing lower               │
 │ (more…)    Futures OI │  Last 3 readings      │ ⇔  Broadening across strikes     │
 └───────────────────────┘  Custom 10:15–11:30   │ ⇥  Concentrating at fewer strikes│
                                                  └──────────────────────────────────┘
```

- **Chip shape adapts to the metric.** PCR and max pain have no side chip. Breadth has no time-window
  chip beyond "vs previous reading".
- **⚙ More options** (hidden by default) holds:
  - a custom strike band (ATM −2…+5, ITM only, OTM only)
  - expiry choice (both, far only)
  - an index or stock list
  - OR groups: `(A AND B) OR C`, which is one level of grouping only
  - a "measure against" switch: previous reading vs window start
- **Natural language** produces the same definition. It fills the chips, and the "Reads as" sentence is
  regenerated from the definition. Anything it couldn't map is shown as a red chip
  ("didn't understand: 'aggressively'"), never silently dropped.

---

## (2) Results and match lifecycle

```
┌─ Call OI Building · F&O stocks · ATM ±5 · as of 11:15 (next reading ~11:30) ┐
│  [ Active 7 ]  [ Ended today 4 ]  [ All 11 ]          sort: newest change ▾ │
│                                                                              │
│  ● NEW   RELIANCE · 29 Sep · 2,960 CE                          first 11:15  │
│    Matched because call OI rose at each of the last 3 readings (+4.1L        │
│    since 10:30), and activity is concentrated at ATM to ATM+2.               │
│                                                                              │
│  ▲ STRENGTHENING   NIFTY · 29 Sep · 24,500 CE     first 10:15 · 5 readings  │
│    Matched because call OI rose across the last 3 readings and this          │
│    interval's addition was larger than the one before. IV expanding          │
│    (COMPUTED).                                                  [timeline ▸] │
│                                                                              │
│  ■ STILL MATCHING   HDFCBANK · 29 Sep · 1,720 CE  first 10:30 · 4 readings  │
│  ▼ WEAKENING        SBIN · 29 Sep · 800 CE        first 09:45 · 6 readings  │
│  ○ ENDED 11:00      TCS · 29 Sep · 4,100 CE       09:45 → 11:00 · 5 read.   │
│    Ended because call OI was flat at the 11:00 reading.                      │
└──────────────────────────────────────────────────────────────────────────────┘

 [timeline ▸] expanded, current session only:
   09:30 09:45 10:00 10:15 10:30 10:45 11:00 11:15
     ·     ·     ·     ●     ■     ■     ▲     ▲      ● new ■ matching ▲▼ pace ○ ended ⋯ not captured
   First matched 10:15 · matching for 60 min · 5 of 5 readings · still active
```

- **Grain.** One card per contract for contract metrics (OI, premium, IV, volume). One card per
  underlying and expiry for book metrics (PCR, max pain). A scanner that mixes both groups its cards under
  the underlying.
- **No verdict words.** Result tones reuse `STATE_TONE`: green means expanding activity, red means
  contracting, amber means a caveat. None of them means a direction.
- **The "Matched because" text** is built only from fields in the stored evaluation row. Nothing is
  written at render time.

### Lifecycle rules (evaluated at each reading T, using only readings at or before T)

| Status | Rule |
|---|---|
| New match | Matched at T, and not at the previous covered reading. |
| Still matching | Matched at T and at the previous reading, pace inside the band. |
| Strengthening | Still matching, and the leading condition's interval move is more than 1.25× the previous one (`PACE_UP`, summary.ts). |
| Weakening | Still matching, and the move is less than 0.75× the previous one (`PACE_DOWN`). |
| Condition ended | Matched at the previous reading, not at T. The card keeps its start and end times. |
| Re-match after ended | A new episode on the same card with a new start time. It is not a separate "new" card. |
| Missing reading | Neither extends nor ends a match (summary.ts RULE 2). Shown as ⋯. |

**Alerts** fire only on transitions: new, ended, or strengthening ↔ weakening. They are deduplicated per
(scanner, instrument, transition, reading). A match that is still matching never re-notifies.

---

## (3) Default scanner list (seeded as data rows, owner-editable, easy to add to)

| # | Name | Definition (universe · strikes · conditions) |
|---|---|---|
| 1 | Call OI building | All · ATM ±5 · OI › Calls › Increasing continuously › 45 min |
| 2 | Put OI building | All · ATM ±5 · OI › Puts › Increasing continuously › 45 min |
| 3 | Call IV expanding | All · ATM ±5 · IV › Calls › Increasing continuously › 45 min |
| 4 | Put IV expanding | All · ATM ±5 · IV › Puts › Increasing continuously › 45 min |
| 5 | Call premium surge | All · ATM ±5 · Premium › Calls › Expanding rapidly › 30 min (see Q1) |
| 6 | Put premium surge | All · ATM ±5 · Premium › Puts › Expanding rapidly › 30 min (see Q1) |
| 7 | Max pain moving higher | All · Max pain › Shifting higher › Since open |
| 8 | Max pain moving lower | All · Max pain › Shifting lower › Since open |
| 9 | PCR building | All · PCR › Increasing continuously › 45 min |
| 10 | PCR falling | All · PCR › Decreasing continuously › 45 min |
| 11 | Broad OI build-up | All · ATM ±5 · OI › Either side › Broadening across strikes › 30 min |
| 12 | Unusual activity near ATM | All · ATM ±2 · Volume › Either side › Unusually active › last reading |
| 13 | Call writing near ATM | All · ATM ±3 · Build-up › Calls › "Call writing increasing" › 45 min (FLOW_LABELS words) |
| 14 | Put writing near ATM | Same as 13, on the put side |

---

## (4) Metric → stored field map

Legend:
- **S** = stored column.
- **D** = derived at evaluation from stored columns across readings. No new data.
- **C** = computed by a model (BSM) and tagged COMPUTED.
- **✗** = not available anywhere today.

| Metric (builder word) | Grain | Exact field(s) | Status | State rule reused |
|---|---|---|---|---|
| Premium | contract | `metrics.last_price`, `price_change_15m`, `price_change_pct_15m`, `price_change_pct_day` | S | `gridPriceDirection`: 5% of the contract's own day range = flat. Rapid = `PACE_UP` 1.25 / `PACE_DOWN` 0.75 on consecutive interval moves |
| OI | contract / side total | `metrics.oi`, `oi_change_15m`, `oi_change_pct_15m`, `oi_change_day`; `underlying_snapshots.total_ce_oi`, `total_pe_oi` | S | `gridDirection` BUILDING / FLAT / UNWINDING, where under 5% of its own peak ΔOI = flat. Pace per summary.ts |
| Change in OI | — | same columns as OI | S | Folded into OI's states (the pace words), not a separate metric |
| Build-up (who is active) | contract | `metrics.buildup_15m`, `buildup_day` | S | `FLOW_LABELS` / `BUILDUP_LABELS` wording, verbatim |
| Volume | contract | `metrics.volume`, `vol_tod_ratio`, `vol_tod_status`, `vol_oi_ratio`, `vol_oi_spike`, `unusual`, `unusual_reasons` | S | Unusually active = `unusual=1` (`vol_tod_ratio ≥ 2.0` `UNUSUAL_VOL_TOD_RATIO`, or the vol/OI spike). "Quiet": see Q3 |
| Premium turnover (₹ cr) | contract | `metrics.premium_cr`, `premium_status` | S | Used only as the existing liquidity floor (₹2 cr), not as a state |
| PCR | underlying + expiry | `metrics.pcr_oi`, `pcr_volume`, `pcr_trend` (rising / flat / falling), `pcr_trend_change`; `underlying_snapshots.pcr_oi`, `pcr_volume` | S | `PCR_CHIPS` via `sessionDirection` (5% of span) |
| Change in PCR | — | `pcr_trend_change` + consecutive `pcr_oi` | S | Folded into PCR's states |
| Max pain | underlying + expiry | `metrics.max_pain_strike`, `max_pain_distance`, `max_pain_status`; `underlying_snapshots.max_pain_strike` | S | `MAX_PAIN_CHIPS` Shifting up / Stable / Shifting down |
| Max-pain shift | — | consecutive `max_pain_strike` | D | Same chips. "Since open" = first reading vs now |
| Underlying movement | underlying | `underlying_snapshots.spot`, `fut_price`; `metrics.spot` | S (the level); D (the move) | `sessionDirection` on spot. No stored spot-change column |
| Breadth across strikes | side, within band | per-strike `oi_change_15m` + `buildup_15m` across readings | D | summary.ts `joined` / `left` → broadened / narrowed; `Breadth` isolated / clustered / dispersed |
| Futures OI | underlying | `metrics.fut_oi_vs_avg`, `fut_oi_vs_avg_status`, `oi` on the FUT row | S | `FUTURES_CHIPS` building / flat / unwinding. Optional; see Q5 |
| **IV** | contract / ATM | **no stored column.** Solved on request by `implied_vol.py` from `last_price` + `spot` + expiry; served by the derivatives IV route (`iv_pct`, `ce_iv_pct`, `pe_iv_pct`) | **C, not stored** | `IV_CHIPS` Expanding / Stable / Cooling via `sessionDirection`. Plan: solve per contract at evaluation, **cache in var/screener.db**, tagged COMPUTED, with refusal reasons shown |
| Change in IV | — | consecutive solved IV | C | Folded into IV's states |
| **Delta** | contract | **✗: nothing computes or stores it** | ✗ | No existing rule. See Q2 |
| **Gamma** | contract | **✗: nothing computes or stores it** | ✗ | No existing rule. See Q2 |
| Bid/ask, basis | — | `snapshots.bid`/`ask`, `metrics.basis` | S | Not proposed. Nothing in the brief needs them |

### How each state word maps to existing logic (no new thresholds)

| State word | Rule |
|---|---|
| Increasing / Decreasing / Stable | The existing direction functions (`gridDirection`, `gridPriceDirection`, `sessionDirection`), with the lookback set to the chosen window (45 min = 3 intervals) instead of the fixed 4. Same 5% flat band. |
| Increasing / Decreasing continuously | Every interval in the window gets the same direction word from the same rule. One flat or opposite interval breaks it. |
| Expanding / Contracting rapidly | The latest interval move is more than 1.25× the previous one, in the same direction (`PACE_UP`, summary.ts "strengthened"). See Q1 for premium. |
| Reversing higher / lower | The previous window's direction was the opposite, and the latest interval clears the flat band the other way (summary.ts "unwound", signal.ts "reversing"). |
| Unusually active | `metrics.unusual = 1` (the stored rule). |
| Quiet | summary.ts behaviour `quiet`: premium flat and OI flat (the `FLOW_LABELS` `flat\|flat` cell). See Q3. |
| Broadening / Concentrating across strikes | summary.ts `broadened` / `narrowed`: strikes joined and none left, or the reverse, within the strike band. |
| Shifting higher / lower | `MAX_PAIN_CHIPS` for max pain. For OI, the lead strike moved up or down (summary.ts `shifted`). |

---

## (5) Build plan, once the layout is approved (all new files)
- `server/kanida_pilot/screener/`, containing:
  - `definition.py`: the schema, validation, and the "Reads as" sentence
  - `states.py`: a Python port of the rules above, with a parity test against the TS functions using
    shared fixtures, in the style of `check-derivative.cjs`
  - `evaluate.py`: point-in-time evaluation, one reading at a time
  - `lifecycle.py`
  - `nl.py`
  - `store.py`: `var/screener.db`
  - `routes.py`: an APIRouter
- `src/screener/`: the builder, results, timeline and saved-scanner list.
- Dev on **:8092** with its own dist folder (`dist-screener/`). It reads `db/derivatives.db` read-only.
- **Hooks for the owner to apply after close:**
  - `app.py`: `app.include_router(screener_router)`
  - one nav entry in the app shell (the file is to be confirmed; it will be named, not edited)

## Open questions for the owner (Q1–Q6 are in the chat reply)
