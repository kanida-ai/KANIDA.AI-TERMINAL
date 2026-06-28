# AutoTrade Sessions — End-to-End Entry Execution Sequence

Status: VERIFIED against `backend/autotrade/` (build worktree
`feat/autotrade-portfolio`) as of 2026-06-28. This documents the NEW multi-broker
Portfolio AutoTrade ("Sessions") path — the one with the portfolio kill switch
and per-position GTT-OCO backup. It is NOT the legacy `/falcon` swing path.

This is the canonical operator-facing description of what happens, in order, when
a session fires its entries — including the NEW trading-day / market-open gate
that now precedes everything, and an HONEST account of timing and one known
limitation (no hard notional cap).

---

## 0. PRE-FIRE GATE — trading-day + market-open (NEW, real-money safety)

Before ANY of the steps below run, every fire path
(`start(when='now')`, the scheduled `entry_scheduler` wake, and boot `recovery`)
passes through `session.evaluate_fire_gate(config, now_ist, fire_dt)`:

1. The resolved FIRE DATE (`entry_date` if set, else the next valid trading
   session derived from `entry_time`) must be a real NSE trading day
   (`trading_calendar.is_trading_day`).
2. The market must be OPEN at `now` (09:15–15:30 IST on a trading day,
   env-overridable via `FALCON_MARKET_OPEN`/`FALCON_MARKET_CLOSE`).
3. A past target fires ONLY if it is still the same trading day, the market is
   open, AND we are within `entry_grace_seconds` (default 120s) of the target.

If the gate refuses, NO order is placed and the session takes a clear
terminal/deferred state:
`REJECTED_NON_TRADING_DAY` (weekend/holiday, expire policy),
`EXPIRED_MISSED_WINDOW` (window missed / market closed, expire policy),
`DEFERRED_MARKET_CLOSED` (before the bell — stays scheduled), or — with
`on_missed_window="carry_next_trading_day"` — the `entry_date` is rolled forward
and the session stays `SCHEDULED`. The gate is re-checked inside `_fire_entries`
itself (defence in depth): even a bypassed upstream check cannot place an order
on a non-trading day or into a closed market.

The trading calendar (`backend/autotrade/trading_calendar.py`) combines a weekday
check + a hand-maintained NSE holiday set + (for past dates within coverage)
`ohlc_daily.trade_date` confirmation. KNOWN MAINTENANCE ITEM: the NSE holiday set
must be refreshed annually; an optional override file
(`data/config/nse_holidays.txt` or `FALCON_NSE_HOLIDAYS_FILE`) lets a
newly-announced holiday be patched without a code deploy.

---

## 1. Market-open validation
Covered by step 0 above. `is_market_open(now_ist)` = trading day AND inside the
09:15–15:30 IST window. A session firing outside the window is refused, never
sent to the broker.

## 2. Broker-session validation
`_build_brokers()` constructs each enabled `BrokerProfile`'s client via
`broker.router.build_client(profile, dry_run)`. The Zerodha adapter WRAPS the
legacy Kite stack (`kite_auth`, `order_executor`, `margin_calc`). Live orders
require BOTH `mode='live'` AND env `FALCON_AUTOTRADE_ENABLED=true`
(`_live_allowed()`); otherwise the broker is built `dry_run=True` and places no
real orders (paper). A missing/expired Kite token surfaces here.

## 3. Live price retrieval (WS cache → REST)
`CapitalAllocator.prefetch(symbols, broker)` issues ONE batched LTP fetch per
broker profile: the WebSocket tick cache first (sub-second, in-memory), falling
back to a SINGLE batched `kite.ltp([...])` REST call for any symbol not in the
cache. This LTP is the entry mark used for sizing + the `invested_basis` freeze.

## 4. Margin + MTF leverage check
The same `prefetch` issues ONE batched MTF-margin fetch
(`margin_calc.fetch_margins_batch` / `kite.order_margins`) per profile. For
`order_product=="MTF"` the per-share MARGIN (not cash) is the sizing
denominator → leveraged quantity. On a margin-API miss the code falls back to
per-symbol CASH sizing (never over-deploys). CNC/EQ size on cash (LTP).

## 5. Equal-allocation recompute
`CapitalAllocator.allocate(symbols)` splits `total_allocated_capital` across the
routed picks per `sizing_mode` (`equal` = capital/N, `pct_cap`, or `manual`).

## 6. Integer quantity per stock
`calculate_quantity_cached(symbol, amount, broker, cache)` computes a whole-share
(or whole-lot for F&O) integer quantity from the prefetched LTP/margin:
EQ/CNC `floor(amount/ltp)`; MTF `floor(amount/margin_per_share)`; FUT/options
floor to lot multiples. Lot sizes come from the broker instrument master, never
hardcoded. A symbol that can't fund one share/lot is SKIPPED
(`InsufficientCapitalError`), not forced.

## 7. Order placement (parallel MARKET)
All legs are fanned out CONCURRENTLY with `asyncio.gather` under a bounded
`asyncio.Semaphore` (default 8, env `FALCON_AUTOTRADE_ENTRY_CONCURRENCY`, capped
1..10 for the Kite ~10/s order limit). Order type is MARKET (fastest fill).
`entry_latency_ms` (fire start → all legs settled) is recorded on the session.

## 8. Fill verification / partial-fill handling
Each leg's `OrderResult` is inspected. A `PARTIAL` fill registers the filled
quantity at the fill price (`registry.register_partial`); a full fill registers
the whole quantity. Paper mode registers the intended qty at the entry mark so
monitoring works without real fills. Slippage (expected vs actual) is recorded.

## 9. Retry / reject / per-leg isolation fallback
`place_order_with_retry` retries with backoff. Per-leg failures are ISOLATED:
`_guarded_place` / `_place_one` return a `FAILED`/`SKIPPED` dict and NEVER raise,
so one bad leg cannot abort its siblings. The session still completes with the
successful legs.

## 10. invested_basis freeze + deployed-notional reconciliation
After all legs settle, `monitor.freeze_invested_basis()` captures
`Σ(qty × avg_price)` ONCE into `autotrade_sessions.invested_basis`. This is the
product-aware NOTIONAL capital actually deployed (MTF = leveraged value, CNC =
cash) and is THE kill-switch + gross-return denominator thereafter — it does not
shrink as positions close. The on-fund view (`gross_return_fund` = uPnL ÷
`total_allocated_capital`) is kept for display only. Per-position GTT-OCO backups
are then placed (live) or recorded (paper), and the tick + WS + (intraday)
square-off drivers are armed.

---

## Timing (HONEST)

| Stage | Mechanism | Typical latency |
|------|-----------|-----------------|
| Live price (step 3) | WS tick cache (in-memory) | sub-second / instant |
| Live price fallback | single batched `kite.ltp` REST | ~hundreds of ms |
| Margin (step 4) | batched `kite.order_margins` REST | ~hundreds of ms–1s |
| Order placement (step 7) | parallel MARKET REST | bounded by the SLOWEST single round-trip, not N×serial |
| End-to-end entry | steps 3–10 | ~1–3s (`entry_latency_ms`), dominated by the REST round-trips |
| Exit / kill / trail monitoring | event-driven WS tick listener + 0.1s backstop poll | sub-second |

Entry is REST-bound (margin + order APIs over the broker's network + Kite rate
limits are the floor). Detection/monitoring/exit is sub-second (WS event-driven,
parallel GTT-cancel-then-flatten). Software serialisation has been removed; the
remaining floor is the broker network/rate-limit.

## Known limitation — NO HARD NOTIONAL CAP

Sizing is per-leg from per-share margin / LTP against the equal (or pct_cap /
manual) allocation. There is NO single hard ceiling on TOTAL deployed notional
beyond the sum of per-leg sizes — i.e. under MTF the realised
`invested_basis` (leveraged notional) can substantially exceed
`total_allocated_capital` (this IS the intended leverage, and the kill switch
correctly measures returns on that leveraged notional). The operator controls
exposure via `total_allocated_capital`, `top_n_stocks`, `max_pct_per_position`
(pct_cap mode), and `order_product`. A portfolio-level hard notional cap is a
future enhancement, not a current guarantee. The `/preview` endpoint surfaces the
resulting `invested_basis` + `leverage` BEFORE Start so the operator can see the
deployed notional first.
