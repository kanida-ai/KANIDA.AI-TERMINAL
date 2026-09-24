# AutoTrade Execution & Simulation Logic — Complete Reference

**Status:** Source-grounded (every claim below was read from the live backend code, not from memory).
**Scope:** The two deployed AutoTrade "Sessions" strategies — `portfolio_kill_switch` and `intraday_basket`.
**Audience:** Operator (you). Purpose: make AutoTrade behaviour completely transparent, scenario by scenario.

> **Units convention (critical):** Every percentage in the backend config is a **FRACTION**. `0.01 = 1%`, `0.012 = 1.2%`, `0.015 = 1.5%`. The UI multiplies by 100 for display and divides by 100 on save. Wherever this document says "+1%" it means the stored value `0.01`.

> **All times are IST (Asia/Kolkata).**

---

## Part 0 — The runtime: what is actually running while a session is live

When a session goes `RUNNING` (entries placed), the backend starts **three independent loops per session**, plus the broker-held GTT backups. They are layered deliberately so no single failure leaves a position unprotected.

| Loop | Cadence | What it does | Files |
|---|---|---|---|
| **`tick_driver`** | every **5 s** | The full `session.tick()`: GTT-fill reconcile → refresh LTPs → EXIT_RETRY sweep → **per-stock stop loop (intraday only)** → snapshot → kill-switch / trail decision | `monitoring/tick_driver.py`, `session.py:tick()` |
| **`ws_driver`** | **sub-second** (event-driven on every KiteTicker tick, 0.1 s backstop poll) | Low-latency path: update LTPs from the live WebSocket cache → recompute portfolio return → portfolio kill-switch / portfolio trail decision. **No per-stock stop, no GTT reconcile, no retry sweep.** | `monitoring/ws_driver.py` |
| **`square_off_scheduler`** | fires once at `square_off_time` | (Intraday only) precise-time basket flatten | `monitoring/square_off_scheduler.py` |
| **GTT-OCO (broker-held)** | broker-side, always | Per-position OCO backup placed at entry. Protects even if our laptop/backend is down. | `monitoring/gtt_manager.py`, `broker/zerodha.py:place_gtt_oco` |

Both the 5 s poll and the sub-second WS path go through a **single per-session fire-guard lock** (`monitoring/fire_guard.py`), so the two paths and the square-off timer can **never double-fire** the same basket.

> A separate legacy **Falcon `PortfolioMonitor`** thread polls every **60 s** — that is the `Monitor poll: {...}` line you see in the log. It is **not** part of AutoTrade Sessions; ignore it when reasoning about AutoTrade.

---

## Part 1 — Shared foundation (identical for both strategies)

Both strategies share the **entry, sizing, monitoring, and exit-confirmation** machinery. Only the **exit decision rule** differs. Understanding this shared core once makes both strategies clear.

### 1.1 Entry firing sequence (`_fire_entries`, `session.py:915`)

At `entry_time` on the resolved trading day:

1. **Trading-day / market-open gate** (`evaluate_fire_gate`). Defence-in-depth: every caller (instant start, the scheduled wake, recovery's past-due fire) passes through this. A non-trading day or a closed market → **refuse, place nothing** (`EXPIRED` unless `on_missed_window="carry_next_trading_day"`).
2. **Build broker client(s)**, set session `RUNNING`.
3. **Load Falcon picks** — `load_falcon_picks(top_n)` from today's published Falcon Top-N, narrowed by `universe_filter` (default `all500`) and any `symbol_whitelist`.
4. **Route** picks to broker profile(s) (`BrokerRouter`).
5. **Size + place all legs CONCURRENTLY** (semaphore-bounded to respect Kite's ~10 orders/sec). Each leg is isolated — one bad leg returns `FAILED`/`SKIPPED` and never aborts the others.
6. **Freeze the invested basis** (see §1.4) once all legs settle.
7. **Backfill the per-position GTT-OCO** broker backups (§1.6).
8. **Start `tick_driver` + `ws_driver`**; (intraday) **arm `square_off_scheduler`**.

### 1.2 Order type at entry: MARKET vs LIMIT (`execution/orders.py`)

- `order_type` default is **`MARKET`**. Entry market orders are sent with **`market_protection=1.0`** (Kite rejects bare API market orders; this caps fill slippage at ~1% from LTP).
- `LIMIT` → price = `ltp × (1 + limit_offset_pct)`, default offset `0.001` (0.1%). If unfilled in the entry window the orchestrator cancels and falls back to MARKET.
- `VWAP` → waits `vwap_window_seconds` then places MARKET.

**So at 9:15 with default config: MARKET orders, market-protected.**

### 1.3 Quantity calculation (`capital.py`) — uses LIVE open prices, not EOD

Sizing is two steps:

**Step A — ₹ per stock** (`CapitalAllocator.allocate`), by `sizing_mode`:
- `equal` (your case): `per = total_capital / n`. **₹1,00,000 / 5 = ₹20,000 per stock.**
- `pct_cap`: `min(capital/n, capital × max_pct_per_position)`.
- `manual`: exact per-symbol ₹ from `manual_amounts`.

**Step B — integer quantity** (`calculate_quantity_cached`), using a **live batched LTP fetch at entry time** (`get_ltps_batch`) — **the actual market-open price, never the prior EOD close**:
- **EQ / CNC:** `qty = floor(amount / ltp)`.
- **MTF (your case):** `qty = floor(amount / margin_per_share)`, where `margin_per_share` is the **per-share margin Kite locks** (fetched live via `get_margins_batch(..., "MTF")`). This is what makes MTF **leveraged** — you control more notional than the ₹20,000 cash. If the margin lookup fails for a symbol → **cash-sizing fallback** (`floor(amount/ltp)`), logged amber, so we **never over-deploy**.
- **FUT / CE / PE:** lot-aware, `floor(amount / (price × lot_size)) × lot_size`. Lot size always from the broker instrument master.

**Answer to "what if market-open price differs from EOD":** quantity is computed off the **live open LTP/margin fetched at 9:15**, so a gap up/down simply changes the integer quantity. There is no EOD-price assumption anywhere in sizing.

`InsufficientCapitalError` (amount < one share/lot) → that leg is **SKIPPED**, never placed at qty 0.

### 1.4 The invested basis — the denominator for ALL P&L

After entries settle, `freeze_invested_basis()` stores **`invested_basis = Σ(qty × avg_price)`** across the placed positions, **frozen once** on the session row.

- This is the **NOTIONAL** capital put to work — for MTF it is the **full leveraged value** (e.g. ₹1L cash at ~5× ≈ ₹5L notional), for CNC it is the deployed cash.
- It **never shrinks** as positions close. A per-stock exit reduces the **numerator** (P&L) only.
- **Every "%" you see — arm, trail, kill — is measured against THIS frozen notional**, via `compute_gross_return_invested()`:

```
gross_return G = ( Σ unrealised_pnl(open) + Σ realised_pnl(closed) ) / invested_basis
```

Including realised P&L from already-closed legs means a stock that was stopped out at a loss **stays** in the numerator — a closed loss cannot "disappear" and make the remaining basket look healthier than it is.

### 1.5 How orders reach the broker

`broker.place_order` → `_retry_kite_call(lambda: kite.place_order(**params), ...)` → KiteConnect REST → your Zerodha account. The REST client is forced **IPv4-only** for Kite hostnames (`kite_auth.py`, the SEBI static-IP allowlist fix) and has a **10 s per-request timeout** (`kite.timeout = 10`). All blocking Kite calls on the exit path run inside `asyncio.to_thread` so they never freeze the event loop.

### 1.6 Per-position GTT-OCO (broker-held backup — both strategies)

At entry, for every position, a **two-leg OCO GTT** is placed on Kite (`place_gtt_oco`):
- **STOP leg:** SELL when price ≤ `entry × (1 − per_position_stop_pct)` (default **−3%**). The limit is set slightly **below** the trigger (a 0.3% buffer) so it still fills on a gap-down.
- **TARGET leg:** SELL when price ≥ `entry × (1 + per_position_target_pct)` (default **+6%**).
- OCO = if one leg fires, the broker cancels the other.

This is the **broker-side floor**: it protects every position even if the laptop sleeps, the backend crashes, or the network drops. It is intentionally **wider** than the software stops so the software exits usually fire first. LIVE only (paper records the intended levels but places no real GTT).

### 1.7 Entry fill handling vs exit fill handling — an honest distinction

- **Exits** run a real **fill-confirmation poll** (`exit_poller.confirm_exit`): poll Kite order status until `COMPLETE` / `REJECTED` / `CANCELLED` / `TIMEOUT`.
- **Entries** do **not** run that poll. `_place_one` registers the position from the immediate `place_order` response: intended `qty` at the **entry reference LTP** (the 9:15 mark). Market orders fill near-instantly so this is normally accurate, and slippage is recorded separately (`record_slippage`); the broker-side average is reconciled afterward. If `place_order` times out it is retried up to **3×** (2 s each, `place_order_with_retry`). A leg that still fails is marked `FAILED` and is simply not held.

---

## Part 2 — Strategy 1: Portfolio Kill Switch (Flat-% Basket Exit)

**Your config:** Top 5, ₹1L, equal sizing, MTF, entry 9:15, kill switch ACTIVE.

This strategy holds the basket and watches **one number**: the portfolio gross return `G` on the frozen notional. If `G` crosses a flat threshold in either direction, it flattens the **entire basket** at once. There is **no per-stock software stop and no arming/trailing** — those belong to Strategy 2.

### 2.1 The exit rule (`kill_switch.check_threshold`, `monitor.py`)

With `kill_switch_pct` (default **0.012 = ±1.2%**) and `kill_switch_direction` (`both` default):

```
if direction in (profit, both) and G >=  +kill_switch_pct  → FIRE  "PROFIT_TARGET"
if direction in (loss,   both) and G <=  -kill_switch_pct  → FIRE  "LOSS_LIMIT"
else                                                        → HOLD
```

Checked on **both** the sub-second WS path and the 5 s poll. The frozen notional denominator means: with MTF ~5× leverage, a +1.2% move on notional is roughly a +6% move on your cash — that is the lever you are setting.

### 2.2 What `G` is when stocks diverge (your core question)

`G` is a **single basket number**, not per-stock. With 5 legs:

```
G = ( Σ over all 5 legs of (ltp − avg_price) × qty ) / invested_basis
```

So the kill switch reacts to the **net** of winners and losers:

| Scenario | Behaviour |
|---|---|
| **One falls, others rise** | Only the **net** matters. A faller is offset by risers; the switch fires only if the **net** `G` reaches ±1.2%. A single stock dropping does **not** trigger anything on its own in Strategy 1 (its only individual protection is the broker GTT-OCO at −3%). |
| **All five in profit** | `G` climbs; at `G ≥ +1.2%` → **PROFIT_TARGET**, flatten all five. |
| **All five in loss** | `G` falls; at `G ≤ −1.2%` → **LOSS_LIMIT**, flatten all five. |
| **Mostly flat** | `G` stays inside (−1.2%, +1.2%) → **HOLD**. Nothing happens; positions ride. The only exits possible are an individual broker GTT-OCO firing at −3%/+6%, or you closing manually. |

### 2.3 Exit mechanics when the kill switch fires (`kill_switch.fire`)

The critical sequence (spec 7.3):

1. **Cancel all pending orders** across the broker (parallel).
2. **Cancel each position's GTT-OCO** first (parallel, time-boxed) so no orphan broker GTT can re-fire on a symbol you just flattened.
3. **Flatten every open position in parallel** — each via `place_market_exit` (MARKET SELL, `market_protection=2.0`, correct MTF product), each claimed through the **exit gate** so no other path double-exits.
4. **Confirm each fill** (`confirm_exit`, poll up to 60 s @ 5 s).
5. **Close the session**, log `exit_latency_ms`.

So a kill-switch exit uses **MARKET orders** (not GTT, not LIMIT) for the flatten; the GTT-OCO is cancelled first and only serves as the backup if the software never gets there.

### 2.4 Exit-confirmation outcomes (`confirm_exit` / `cancel_and_retry_exit`)

| Broker reports | System does |
|---|---|
| `COMPLETE` (full) | `mark_closed` at the actual fill price. |
| `PARTIAL` | `update_partial_exit`; **cancel-and-retry the remainder** up to 3×. |
| `REJECTED` / `CANCELLED` | `mark_exit_failed` (releases the gate) → urgent alert → the **5 s EXIT_RETRY sweep** re-attempts on the next tick. |
| `TIMEOUT` (still pending after 60 s) | cancel + place a fresh market sell, retry up to 3×; if still unfilled → `EXIT_FAILED` + alert. |
| Liquidity / no fill | Same path — escalates to retry, then `EXIT_FAILED` + alert for manual review. |

**Retry behaviour:** yes — the system **cancels and replaces** with a fresh market sell, escalates through retries, and if all fail marks `EXIT_FAILED` and raises an urgent alert. `EXIT_FAILED` positions are re-swept every 5 s **during market hours** (suppressed after 15:29 so a stale close price can't trigger a doomed retry loop).

### 2.5 EOD behaviour — important

**Strategy 1 has no automatic square-off.** It is not an intraday strategy: `square_off_scheduler` is armed **only** for `intraday_basket`. At EOD, if the kill switch never crossed, **positions remain open and carry overnight** (MTF carries; the broker-held GTT-OCO at −3%/+6% remains the standing protection). If you want a hard daily flat-out, use Strategy 2 (which has `square_off_time`) or close manually.

> Note (from ops history): plain Kite day-validity SL/SL-M orders auto-cancel at 15:30. The **GTT-OCO is not** a day order — it persists across days until it fires or is cancelled, so the overnight floor stays in place.

### 2.6 What is monitored every (sub-)second

Per WS tick / 5 s poll: each held symbol's **live LTP** (from the KiteTicker cache → broker LTP → last daily close → entry price, in that fallback order), recomputed **unrealised P&L per leg**, the **basket `G`**, **order status** (during exits), and **broker-side GTT state** (reconciled on the 5 s poll). Positions are reconciled DB ↔ Kite via the EOD reconcile path; the AutoTrade layer reads/writes **only** `autotrade_positions` for this session and never touches `falcon_position_state`.

---

## Part 3 — Strategy 2: Falcon Intraday Basket (Arm-and-Trail)

**Your config:** Top 5, ₹1L, equal, MTF, entry 9:15, **arm +1%, floor +1%, trail-giveback 1%, stop −1.5%, square-off 15:29**.

Entry and sizing are **identical to Strategy 1** (§1.1–§1.6). What changes is the exit brain: a **two-layer** system running every tick:

- **Layer A — per-stock software stop** (5 s poll only): exits an **individual** stock that falls past `−stop_pct`.
- **Layer B — portfolio arm-and-trail** (sub-second + 5 s): arms on the **basket** `G` and trails the **whole basket** out.
- Plus the broker GTT-OCO (−3%/+6%) backup and the 15:29 square-off, as always.

### 3.1 Layer A — per-stock software stop (`session.py:_tick_intraday`)

On each **5 s** tick, **before** the portfolio engine runs:

```
for each open position:
    stock_return = (ltp − avg_price) / avg_price
    if stock_return <= −stop_pct (−1.5%) and within 09:15–15:29:
        → exit THAT stock via _exit_single_position (cancel its GTT, MARKET SELL, confirm)
```

So a single stock that drops −1.5% from **its own** entry is sold individually; the other four are untouched. The basket then continues with the remaining legs. (This per-stock loop runs on the 5 s cadence, not sub-second — the sub-second WS path runs the portfolio engine only.)

### 3.2 Layer B — portfolio arm-and-trail (`trail_engine.decide`) — the pure decision table

Evaluated top-to-bottom **every tick** on the basket `G`, against persisted state `(armed, peak)`:

```
1. SQUARE-OFF :  now >= square_off_time (15:29)        → EXIT "SQUARE_OFF"
2. STOP       :  G <= −stop_pct (−1.5%)                → EXIT "STOP"   (reachable pre-arm)
3. PRE-ARM    :  not armed AND G >= arm_pct (+1%)      → ARM  (armed=True, peak=G); no exit
4. ARMED      :  peak = max(peak, G)            (ratchet up only)
                 trigger = max(peak − trail_giveback_pct, floor_pct)
                 if G <= trigger:
                       EXIT "TRAIL_EXIT"  if trigger == peak − giveback
                       EXIT "FLOOR_EXIT"  if trigger == floor_pct
                 else → HOLD
```

State `(armed, peak)` is **persisted to the session row** on every change, so a backend restart **resumes the trail mid-day** exactly where it was.

### 3.3 Walking your exact numbers (arm +1%, floor +1%, giveback 1%, stop −1.5%)

- **Before arming:** basket `G` < +1%. Only the hard **STOP at −1.5%** (whole basket) and the per-stock −1.5% stop (Layer A) and the broker GTT (−3%) can exit.
- **Arming:** the first tick `G ≥ +1%` → **ARM**, `peak = G`. No exit this tick. A protective floor now exists.
- **Trailing as it rises:** each tick, `peak` ratchets to the new high. `trigger = max(peak − 1%, +1%)`.
  - At `peak = +1%` → trigger = `max(0%, +1%)` = **+1%** (the floor binds; `FLOOR_EXIT` if hit).
  - At `peak = +2%` → trigger = `max(+1%, +1%)` = **+1%**.
  - At `peak = +3%` → trigger = `max(+2%, +1%)` = **+2%** (giveback now binds; `TRAIL_EXIT` if hit).
  - At `peak = +5%` → trigger = **+4%**.
- **The trailing stop only moves UP, never down** — `peak` is a high-water mark and `trigger` is derived from it. A dip cannot lower the trigger.
- **Rise-then-reverse:** if after arming `G` falls back to the trigger, the basket exits at the locked level (`TRAIL_EXIT`, or `FLOOR_EXIT` if the floor is the binding level). You keep at least the floor (+1%) once armed.
- **Gap below the level:** the software exit is MARKET (fills through the gap); if the software is down, the broker GTT-OCO (−3% per stock) is the catch.

### 3.4 What activates each exit, and what gets sent to the broker

| Trigger | Layer | Order sent | Scope |
|---|---|---|---|
| Stock −1.5% from its entry | A (5 s) | cancel that stock's GTT → **MARKET SELL** → confirm | that **one** stock |
| Basket `G` −1.5% (pre-arm) | B (sub-sec+5 s) | full flatten via `kill_switch.fire` → **MARKET SELLs** | **whole** basket |
| Basket trail/floor hit (armed) | B | full flatten → **MARKET SELLs** | **whole** basket |
| 15:29 square-off | square_off_scheduler (+ tick backstop) | full flatten → **MARKET SELLs** | **whole** basket |
| Stock −3% / +6% | broker GTT-OCO | broker-held **OCO** (backup if software down) | that **one** stock |

Exit-confirmation, partial/rejected/timeout handling, and the EXIT_RETRY sweep are **identical to §2.4** (shared `confirm_exit` / `cancel_and_retry_exit` / 5 s retry).

### 3.5 Mixed-portfolio scenarios

| Scenario | Behaviour |
|---|---|
| One stock −1.5%, other four rising | **Layer A** sells the one stock individually (5 s). The remaining four continue; the basket trail keeps running on the smaller basket. The realised loss from the sold stock **stays in `G`** (numerator includes realised P&L). |
| All profitable | Basket `G` arms at +1%, trails up, exits the whole basket on giveback or at 15:29 — whichever comes first. |
| All losing | Basket `G` hits −1.5% → whole-basket STOP. Individual stocks may also hit their −1.5% Layer-A stops first; either way you're flat. |
| All flat | No arm, no stop. Positions ride until 15:29 square-off (or a broker GTT). |
| One profitable stock falls sharply, others keep rising | The **basket** trail triggers only if **net** `G` falls to the trigger. If the risers keep `G` above the trigger, no portfolio exit; but if that one faller drops −1.5% from its own entry, **Layer A** sells it individually regardless of the basket. |
| Some stocks hit individual stops but basket still positive | Layer A sells the stopped names; the basket (now fewer legs, with realised losses booked into `G`) keeps trailing. No portfolio exit while `G` stays above its trigger. |
| Basket turns negative after being positive | If **armed**, it already exited at the locked floor (≥ +1%) before going negative — so an armed basket should not reach negative. If **not yet armed**, it exits at the −1.5% STOP. |

### 3.6 EOD / square-off (`square_off_scheduler` + tick backstop)

At **15:29** the square-off scheduler fires the full flatten (`kill_switch.fire`, `close_reason="SQUARE_OFF"`). If that in-memory timer was lost to a restart, the 5 s tick's `trail_engine.decide` returns `SQUARE_OFF` once `now ≥ 15:29` as the **restart-safe backstop**. Both go through the single fire-guard, so they cannot double-fire. **Never overnight.** Confirmation that every leg is flat uses the same `confirm_exit` poll; any leg that won't fill escalates to retry → `EXIT_FAILED` + alert.

---

## Part 4 — Portfolio-level behaviour, precedence, and the exact math (your deep-dive questions)

### 4.1 How portfolio return is calculated with mixed winners/losers

One number, net of everything, on the frozen notional:

```
G = ( Σ_open (ltp − avg_price)×qty  +  Σ_closed realised_pnl ) / invested_basis
```

Example, 3 winners + 2 losers (illustrative ₹ P&L on legs, notional ₹5,00,000 under MTF):
`(+4,000 +3,000 +2,000 −2,500 −1,500) / 5,00,000 = +5,000 / 5,00,000 = +1.0%`. That +1.0% is what arms the trail / trips the kill switch — **not** any individual stock's return.

### 4.2 What the trail is based on

- **Arm trigger:** the **basket `G`** crossing `+arm_pct`. Not individual returns, not absolute rupee value.
- **Peak:** the **high-water mark of basket `G`** since arming.
- **Floor / giveback:** computed from that peak: `trigger = max(peak − giveback, floor)`.

### 4.3 How the floor updates as the basket rises

Only the **peak** updates (ratchets up; never down), and the trigger is recomputed from it each tick: `trigger = max(peak − trail_giveback_pct, floor_pct)`. Below `peak = floor + giveback` the **floor** binds (flat trigger = `floor_pct`); above it the **giveback** binds (trigger rises 1-for-1 with peak).

### 4.4 Does a single faller trigger the portfolio trail?

Only through its effect on **net `G`**. A profitable stock falling sharply pulls `G` down; the portfolio exits **only if `G` reaches the trigger**. If the other risers hold `G` up, no portfolio exit. (That same faller can still be sold by its **own** Layer-A −1.5% stop in Strategy 2, or its broker GTT at −3% in either strategy.)

### 4.5 If some stocks hit individual stops but the basket is still positive

(Strategy 2) Layer A sells the stopped names individually; their realised losses are booked into `G`; the basket keeps trailing on the remaining legs. No whole-basket exit while `G` stays above its trigger. (Strategy 1 has no individual software stop — only the broker GTT can take a single name out.)

### 4.6 When does a portfolio exit fire after going from positive to negative?

- **Strategy 1:** when `G ≤ −kill_switch_pct` (−1.2% default), regardless of prior peak (it has no memory/arming).
- **Strategy 2:** if **armed**, it would already have exited at the locked floor (≥ +1%) on the way down, so it shouldn't reach negative. If **not yet armed**, at `G ≤ −stop_pct` (−1.5%).

### 4.7 Whole-basket vs single-position exits

- **Portfolio exits** (kill switch, portfolio trail, square-off) close **all positions together** — they are basket-level by design.
- **Single-position exits** (Strategy-2 Layer-A −1.5% stop; a broker GTT firing) close **only that one** position; the basket continues.

### 4.8 Precedence when an individual and a portfolio exit fire "at the same time"

- They use **different locks** and cannot corrupt each other: a portfolio flatten claims the per-session **fire-guard**; each single-stock exit claims that symbol's **exit gate**. The flatten also claims each symbol's exit gate per leg — whichever claimed a given symbol first wins it; the other path skips that symbol (`BLOCKED`). No double-sell.
- **Ordering within a 5 s tick (Strategy 2):** the **per-stock stop loop runs first**, then the portfolio trail decision runs on the updated (smaller) basket. So an individual −1.5% stop is acted on before the portfolio decision that same tick.
- **Reason priority inside the trail engine:** `SQUARE_OFF` > `STOP` > arm > trail/floor (top-to-bottom in `decide`).

### 4.9 How individual and portfolio rules interact — summary

Three protective layers operate concurrently, narrowest-to-widest, with the broker layer as the floor:

```
 Per-stock software stop (Strat-2 only, −1.5% vs own entry, 5s)   → sells ONE stock
 Broker GTT-OCO (−3% / +6%, always, broker-held)                 → sells ONE stock (backup)
 Portfolio kill switch / trail (basket G, sub-second + 5s)        → flattens WHOLE basket
 Square-off 15:29 (Strat-2 only)                                  → flattens WHOLE basket
```

---

## Part 5 — Decision-flow quick reference

### Strategy 1 — each evaluation (sub-second WS + 5 s poll)
```
refresh LTPs → G = netP&L / invested_basis
   G >= +kill_switch_pct ? → FIRE PROFIT_TARGET (flatten all, MARKET)
   G <= −kill_switch_pct ? → FIRE LOSS_LIMIT    (flatten all, MARKET)
   else                    → HOLD
(broker GTT-OCO −3%/+6% per stock runs independently as backup; no auto EOD square-off)
```

### Strategy 2 — each 5 s tick
```
GTT-fill reconcile → refresh LTPs → EXIT_RETRY sweep (mkt hours)
PER-STOCK STOP: any stock <= −1.5% vs its entry ? → sell that stock (MARKET, confirm)
G = netP&L / invested_basis
trail_engine.decide(G, state):
   now>=15:29              → EXIT SQUARE_OFF (flatten all)
   G<=−1.5% (pre-arm)      → EXIT STOP       (flatten all)
   not armed & G>=+1%      → ARM (peak=G)
   armed: peak=max(peak,G); trig=max(peak−1%, +1%); G<=trig → EXIT TRAIL/FLOOR (flatten all)
   else                    → HOLD (persist peak)
```
### Strategy 2 — sub-second WS path
```
update LTPs from WS cache → G → trail_engine.decide(G, state) → maybe flatten all
(no per-stock stop, no GTT reconcile on this path — those are the 5s job)
```

---

## Part 6 — Config field reference (both strategies)

| Field | Default | Meaning | Applies to |
|---|---|---|---|
| `strategy` | `portfolio_kill_switch` | strategy selector | — |
| `total_allocated_capital` | — | cash to deploy | both |
| `top_n_stocks` | 5 | basket size (intraday: must be 3–10) | both |
| `sizing_mode` | `equal` | equal / pct_cap / manual | both |
| `order_product` | `CNC` | CNC / MIS / MTF / NRML | both |
| `order_type` | `MARKET` | MARKET / LIMIT / VWAP | both |
| `instrument_type` | `EQ` | EQ / MTF / FUT / CE / PE | both |
| `kill_switch_pct` | `0.012` | ±1.2% flat basket exit | Strat 1 |
| `kill_switch_direction` | `both` | profit / loss / both | Strat 1 |
| `kill_switch_enabled` | `False` | master enable (fail-safe off) | Strat 1 |
| `per_position_stop_pct` | `0.03` | broker GTT stop = entry×(1−this) | both |
| `per_position_target_pct` | `0.06` | broker GTT target = entry×(1+this) | both |
| `arm_pct` | `0.01` | basket arms at +this | Strat 2 |
| `floor_pct` | `0.01` | locked floor once armed (≤ arm_pct) | Strat 2 |
| `trail_giveback_pct` | `0.0075` | giveback from peak | Strat 2 |
| `stop_pct` | `0.015` | per-stock & pre-arm basket stop | Strat 2 |
| `square_off_time` | `15:29:00` | precise-time flatten | Strat 2 |
| `entry_time` / `entry_date` | `09:15:00` / next trading day | when to fire | both |

---

## Part 7 — Safety invariants (always true)

- **Paper-default.** Real orders require `mode='live'` **and** `FALCON_AUTOTRADE_ENABLED=true`. Paper runs the full logic but every broker call returns `DRY_RUN`.
- **Single fire-guard** per session → kill switch, trail, square-off, and manual kill can never double-fire.
- **Exit gate** per symbol → individual and basket exits never double-sell a position.
- **Data isolation** → AutoTrade reads/writes only `autotrade_positions` / `autotrade_sessions` for its `session_id`; never `falcon_position_state`.
- **Frozen notional denominator** → no exit math can be gamed by closing legs; the % basis is fixed at entry.
- **Broker GTT-OCO** → a standing broker-held floor under every position, independent of our software being up.
- **Market-hours guard** → software stops/retries only fire 09:15–15:29 so stale closing prices can't trigger phantom exits.

---

*Generated by reading: `session.py`, `capital.py`, `config.py`, `execution/orders.py`, `monitoring/{tick_driver,ws_driver,square_off_scheduler,trail_engine,kill_switch,exit_poller,gtt_manager,monitor}.py`, `broker/zerodha.py`, `services/kite_auth.py`. Every behavioural claim traces to one of these files.*
