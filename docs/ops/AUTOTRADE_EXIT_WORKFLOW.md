# AutoTrade — Exit Workflow (End-to-End)

**Last updated:** 2026-06-29  
**Applies to:** All AutoTrade strategies (intraday_basket, portfolio_kill_switch)

---

## 1. Overview

Every exit — whether triggered by a stop, a trail, a time-based square-off, or a manual kill — flows through the **same exit execution pipeline**. The strategy layer decides WHEN to exit and WHY. The execution layer (kill_switch → exit_poller → broker) decides HOW, with confirmation and retry.

```
Strategy decides EXIT
       │
       ▼
kill_switch.fire(close_reason)
  ├─ Cancel all GTT-OCO backup orders (best-effort, parallel)
  ├─ Place MARKET SELL for each open position (parallel)
  └─ For each order → exit_poller.confirm_exit()
           ├─ COMPLETE → mark_closed(actual_fill_price)
           ├─ PARTIAL  → mark_partial + retry remainder
           ├─ TIMEOUT  → cancel + fresh market sell (3x retry)
           └─ REJECTED → gate released + URGENT alert
```

---

## 2. Strategy Comparison — what triggers the exit

| Trigger | portfolio_kill_switch | intraday_basket |
|---------|----------------------|-----------------|
| **Hard loss stop** | `kill_switch_pct` (loss direction) | `stop_pct` |
| **Profit exit** | `kill_switch_pct` (profit direction, if enabled) | Trail: arm → ratchet → giveback → `floor_pct` |
| **Time-based force exit** | ❌ not available | `square_off_time` (default 15:29:00 IST) |
| **Per-stock software stop** | ❌ (portfolio-level only) | ✅ (per-stock loop in `_tick_intraday`, fires when `(ltp-avg)/avg ≤ -stop_pct`) |
| **GTT broker backup** | ✅ -3% per stock | ✅ -3% per stock (wider than stop_pct so software always fires first) |

### Important: intraday_basket DOES have a kill switch

The `stop_pct` parameter for `intraday_basket` is functionally identical to `kill_switch_pct` in the loss direction. When the portfolio gross return (unrealised + realised) falls to or below `-stop_pct`, the trail engine fires a `STOP` decision which calls `kill_switch.fire()` — the same function the `portfolio_kill_switch` strategy uses. The naming differs; the execution path is shared.

---

## 3. Exit Execution Pipeline (shared by all strategies)

### Step 1 — Exit decision

| Strategy | Decision source | Exit reasons |
|----------|----------------|--------------|
| `portfolio_kill_switch` | `kill_switch.check_threshold(gross_return)` | `LOSS_LIMIT`, `PROFIT_TARGET` |
| `intraday_basket` (portfolio) | `trail_engine.decide(gross_return, state, params)` | `STOP`, `TRAIL_EXIT`, `FLOOR_EXIT`, `SQUARE_OFF` |
| `intraday_basket` (per-stock) | `_tick_intraday()` per-position loop | `STOP_STOCK` |
| Manual | `/api/autotrade/session/{id}/kill` | `MANUAL_KILL` |

All decisions call `kill_switch.fire(session_id, close_reason)` except `STOP_STOCK` which calls `_exit_single_position()` (same confirmation logic).

### Step 2 — Pre-exit safety gate

Before any order is placed, two locks are checked:

- **fire_guard** (per-session): prevents WS driver + 5s tick + square-off scheduler all firing simultaneously. First caller wins; others skip silently.
- **exit_gate** (per-session, per-symbol): prevents the same position being exited twice. `claim_exit_session(session_id, symbol, reason)` does an atomic DB UPDATE WHERE exit_lock=0. Only one path gets rowcount=1.

### Step 3 — Cancel GTT-OCO backup orders (parallel)

Before placing any market exits, all broker-held GTT-OCO orders for the session are cancelled in parallel via `cancel_session_gtts_async()`. This prevents a GTT from firing AFTER our market sell — which would create a duplicate exit attempt on an already-closed position.

Cancellation failures are logged and skipped (best-effort). The market exits proceed regardless.

### Step 4 — Place MARKET SELL orders (parallel)

`ZerodhaBroker.place_market_exit()` calls `kite.place_order()` directly:

```
variety  = VARIETY_REGULAR
order_type = ORDER_TYPE_MARKET   ← always market, never limit
transaction_type = TRANSACTION_TYPE_SELL
product  = resolved from position (CNC / MIS / MTF / NRML)
quantity = full open qty
```

Market orders are guaranteed to fill at the best available bid. No price is specified — the order takes whatever the market offers. This eliminates the "limit order stuck in queue" failure mode.

All legs are placed via `asyncio.gather()` in parallel. One leg failing does not block others.

### Step 5 — Fill confirmation (exit_poller.confirm_exit)

This is the core new component (2026-06-29). Every placed order is polled until a terminal state is confirmed:

```
poll every 5 seconds, up to 60 seconds

kite order status → action
─────────────────────────────────────────────────────
COMPLETE, filled_qty == pos.qty     → mark_closed(exit_price=fill_avg_price) ✓ DONE
COMPLETE, filled_qty < pos.qty      → update_partial_exit(filled_qty, price)
                                      retry remainder with new market sell → poll again
OPEN / TRIGGER PENDING              → wait, poll next interval
REJECTED / CANCELLED                → release exit_gate, urgent alert → DONE (retry in tick)
timeout (60s) with OPEN order       → cancel original order
                                      place fresh market sell (retry #1)
                                      re-run confirm_exit (up to 3 retries total)
after 3 retries still unconfirmed   → mark EXIT_FAILED, release exit_gate, urgent alert
```

**mark_closed is NEVER called until fill is confirmed.** Portfolio P&L (gross_return_invested) is only updated with the actual fill price from the broker's order data, not an estimate.

### Step 6 — EXIT_FAILED recovery

Every 5-second `tick()` sweeps for positions with status=EXIT_FAILED (gate released). For each one, it claims the `EXIT_RETRY` gate and dispatches a fresh `_exit_single_position()` call. This means a transient broker API error that fails an exit will be automatically retried on the next heartbeat — no human intervention needed for temporary outages.

---

## 4. Portfolio Gross Return Calculation

All exit thresholds are evaluated against `gross_return_invested`:

```
gross_return_invested = (unrealised_PnL_open + realised_PnL_closed) / invested_basis
```

- **`unrealised_PnL_open`**: `Σ (ltp - avg_price) × qty` for all OPEN positions
- **`realised_PnL_closed`**: `Σ realised_pnl` for all CLOSED positions this session
- **`invested_basis`**: frozen at entry = `Σ (qty × avg_price)`, never changes

The denominator is frozen at entry and never shrinks as positions close. This means the loss of a position that was stopped out stays in the calculation — the trail engine sees the true total session loss, not a reset view.

---

## 5. GTT-OCO Broker Backup

For every open position, a Kite GTT-OCO (One-Cancels-Other) order is placed at entry:

- **Stop leg**: LIMIT sell at `entry × (1 - stop_pct)` with limit price `stop_trigger × (1 - 0.3% buffer)`  
  The 0.3% buffer absorbs small price gaps so the limit sell can fill even if the market jumps slightly below the trigger.
- **Target leg**: LIMIT sell at `entry × (1 + target_pct)`

The GTT fires independently on Kite's servers — no backend involvement. It is a **safety net only**, set wider than the software stop so our backend always fires first:

| | Software stop (our backend) | GTT backup (Kite) |
|---|---|---|
| intraday_basket | `stop_pct` (default 1.5%) | 3% |
| portfolio_kill_switch | `kill_switch_pct` (operator-set) | 3% |

If our backend fires first: `kill_switch.fire()` cancels the GTT before placing market exits (Step 3 above).  
If the GTT fires first (backend offline): `reconcile_gtt_fills()` detects the COMPLETE fill on the next tick and marks the position closed. The realized P&L is included in subsequent gross_return calculations.

---

## 6. intraday_basket Trail Logic (detailed)

The trail engine (`monitoring/trail_engine.py`) is a pure function evaluated every tick:

```
decide(G = gross_return_invested, state = {armed, peak}, params)
  │
  ├─ now_IST >= square_off_time?       → EXIT: SQUARE_OFF  (always wins)
  │
  ├─ G <= -stop_pct?                   → EXIT: STOP
  │    (portfolio hard stop, pre-arm)
  │
  ├─ not armed AND G >= arm_pct?       → ARM (set armed=True, peak=G), HOLD
  │
  └─ armed:
       peak = max(peak, G)             ← ratchets up only, never down
       trigger = max(peak - trail_giveback_pct, floor_pct)
       G <= trigger?
         ├─ trigger == peak - giveback → EXIT: TRAIL_EXIT
         └─ trigger == floor_pct      → EXIT: FLOOR_EXIT
       else → HOLD (save peak if changed)
```

State (armed, peak) is persisted to `autotrade_sessions.trail_armed / trail_peak` every tick — survives backend restarts.

Per-stock stop (runs before the portfolio trail check each tick):
```
for each OPEN position:
  stock_return = (ltp - avg_price) / avg_price
  if stock_return <= -stop_pct:
    cancel that stock's GTT → place market sell → confirm_exit()
    (portfolio trail then runs on remaining positions)
```

---

## 7. Scenario Reference

| Scenario | What happens |
|----------|-------------|
| GTT triggered, sell order still PENDING | `reconcile_gtt_fills` waits for COMPLETE status. Position stays OPEN. |
| GTT sell order REJECTED | Not marked COMPLETE — position stays OPEN. No auto-escalation to market (GTT path is passive backup). Manual intervention or next trail tick fires. |
| Software exit order REJECTED | `confirm_exit` detects REJECTED → `mark_exit_failed` + gate released + urgent alert. Next `tick()` sweep retries automatically. |
| Exit order PARTIALLY filled | `update_partial_exit` records the filled qty. Fresh market sell placed for remainder. `confirm_exit` runs again on the new order. |
| Price gaps far below stop | Market order fills at next available bid (any price). No stuck limit. GTT limit has 0.3% buffer below trigger but large gaps may still slip. |
| Broker API error | Exception caught → `mark_exit_failed` + gate released. Auto-retry on next tick. Alert sent. |
| Backend restarts mid-session | `recovery.py` re-arms tick_drivers + entry_schedulers. Trail state (armed, peak) reloaded from DB. GTT orders on Kite continue independently during downtime. |
| System believes exited but still open | Cannot happen after 2026-06-29: `mark_closed` only called after `confirm_exit` returns COMPLETE fill. |
| Duplicate exits | `fire_guard` (session-level) + `exit_gate` (position-level) block all duplicate attempts. |
| Manual exit on Kite (via GTT) | `reconcile_gtt_fills` detects next tick. |
| Manual exit on Kite (direct sell, bypassing GTT) | NOT auto-detected. Position stays OPEN in DB. Operator must sync via tradebook upload or manual close. |

---

## 8. Configuration Reference

### portfolio_kill_switch

| Parameter | Type | Description |
|-----------|------|-------------|
| `kill_switch_enabled` | bool | Whether the kill switch is active |
| `kill_switch_pct` | fraction | Gross-return threshold (e.g. 0.01 = 1%) |
| `kill_switch_direction` | string | `"loss"`, `"profit"`, or `"both"` |
| `per_position_stop_pct` | fraction | GTT backup stop per stock (default 0.03) |
| `per_position_target_pct` | fraction | GTT backup target per stock (default 0.06) |

### intraday_basket

| Parameter | Type | Description |
|-----------|------|-------------|
| `stop_pct` | fraction | Hard portfolio stop before arming (= kill switch, loss side) |
| `arm_pct` | fraction | Portfolio return at which trailing activates |
| `floor_pct` | fraction | Minimum profit floor — trail never lets return drop below this |
| `trail_giveback_pct` | fraction | How much to give back from peak before exiting |
| `square_off_time` | string | Force-exit all at this IST time (default "15:29:00") |
| `per_position_stop_pct` | fraction | GTT backup stop per stock (default 0.03) |
| `per_position_target_pct` | fraction | GTT backup target per stock (default 0.06) |

All percentages are stored and sent as **fractions** (0.01 = 1%). The UI divides by 100 before sending and multiplies by 100 for display.

---

## 9. Code Locations

| Component | File |
|-----------|------|
| Trail engine (pure decision fn) | `backend/autotrade/monitoring/trail_engine.py` |
| Kill switch (fire, cancel GTTs, parallel exits) | `backend/autotrade/monitoring/kill_switch.py` |
| Exit fill confirmation + retry | `backend/autotrade/monitoring/exit_poller.py` |
| GTT placement, reconciliation | `backend/autotrade/monitoring/gtt_manager.py` |
| Portfolio gross return calculation | `backend/autotrade/monitoring/monitor.py` |
| Per-stock stop loop | `backend/autotrade/session.py` → `_tick_intraday()` |
| Square-off scheduler | `backend/autotrade/monitoring/square_off_scheduler.py` |
| WebSocket sub-second driver | `backend/autotrade/monitoring/ws_driver.py` |
| Position registry (mark_closed, mark_exit_failed) | `backend/autotrade/monitoring/registry.py` |
| Exit gate (claim, release) | `backend/autotrade/exit_gate.py` |
| Zerodha market order placement | `backend/autotrade/broker/zerodha.py` → `place_market_exit()` |
