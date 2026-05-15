# Falcon Auto-Trade — Tasks

**Spec version:** 0.3 (DRAFT — broker-decided leverage + holdings overlap + positions tab)
**Author:** Claude (AI implementer)
**Last updated:** 2026-05-05

---

## Track A — Backend (Python)

### A1. DB schema (10 min)
- Append CREATE TABLE blocks per Design §4 (with `is_averaging`, overlap counters)
- Verify boot apply

### A2. trade_db.py (25 min)
- `create_run(batch_id, **params)`
- `insert_order(batch_id, symbol, side, role, is_averaging, **fields)`
- `update_order_status(order_id, status, kite_order_id?, error?)`
- `finalize_run(batch_id, status, n_filled, n_failed, n_skip_action, n_average_action, ...)`
- `get_run(batch_id)`
- `get_orders_for_batch(batch_id)`
- `has_active_batch_for_today()` (S12)
- `get_open_sl_orders() -> {symbol: {sl_price, kite_order_id, sl_type, batch_id}}` (NEW for /positions)
- `get_entry_dates() -> {symbol: entry_date}` (NEW for /positions)
- `get_open_sl_order(symbol)` (NEW for manual exit)

### A3. mtf_eligibility.py (15 min)
- Same as v0.2

### A3.5 holdings_fetch.py (25 min) — **NEW**
- `get_held_positions(kite, force_refresh=False) -> {symbol: HeldPosition}`
- 5-min in-memory cache
- Combines `kite.holdings()` + `kite.positions().net`
- Enriches with entry dates from `falcon_trade_orders`
- Soft-fail: returns `{}` + log warning if Kite API fails (caller decides handling)

### A4. volatility_gate.py (10 min)
- Same as v0.2

### A5. order_planner.py (35 min)
- `OrderSpec` dataclass with `is_averaging` + `existing_qty` fields
- `plan_orders(...)` signature includes `held_by_symbol` + `hold_actions` (no `leverage` param — uses `ASSUMED_MTF_LEVERAGE=3` internally)
- Returns `(orders, skipped, action_counts)` where action_counts has `n_skip_action`, `n_average_action`
- **Test:** TOP_N modes, MTF skip, overlap skip/average paths, qty floor, edge cases

### A6. order_executor.py (45 min)
- Same as v0.2 plus: stamp `is_averaging` on order rows when persisting
- Order role can now be ENTRY|STOP|SMOKE|EXIT

### A7. trade_router.py (50 min)
- Endpoints per Design §3.1–3.5 (preview, smoke-test, place, cancel-all, status)
- **NEW** §3.6 `GET /positions` — joins holdings_fetch + trade_db.get_open_sl_orders + entry_dates
- **NEW** §3.7 `POST /positions/exit` — manual exit (cancel SL → market sell)
- Pydantic models for all
- Equity check (S7), concurrent-batch check (S12), confirm-text (S4) gates

### A8. Register router in main.py (5 min)
- Same as v0.2

### A9. Backend smoke (15 min)
- Verify `/preview`, `/positions`, `/positions/exit` return shapes match Design §3
- Verify `falcon_trade_*` tables exist

---

## Track B — Frontend (Next.js)

### B0. Mockup pages (DONE)
- `frontend/app/falcon/trade/page.tsx` — 3-section panel, holdings overlap UI, no LEVERAGE knob
- `frontend/app/falcon/positions/page.tsx` — held positions table, trigger watch, manual exit modal
- `frontend/app/falcon/layout.tsx` — nav has Today / Trade / Positions / Signals / Backtest / Patterns / Admin

### B1. Wire mockups to live backend (75 min)
- Replace `MOCK_SIGNALS` with `FalconAPI.signalsToday()`
- Replace `MOCK_HELD_POSITIONS` with new `FalconAPI.tradePositions()`
- Add `FalconAPI.tradePreview / tradeSmokeTest / tradePlace / tradeCancelAll / tradeStatus / tradePositionsExit`
- Replace simulated delays / status transitions with real API calls
- Wire token banner to existing `fetchKiteStatus()` from `admin-api.ts`
- Positions page: poll `/api/falcon/trade/positions` every 5s

### B2. Extend FalconAPI client (20 min)
Add to `frontend/lib/falcon-api.ts`:
- `tradePreview(req: PreviewRequest): Promise<PreviewResponse>`
- `tradeSmokeTest(previewId): Promise<SmokeResponse>`
- `tradePlace(previewId, confirm): Promise<PlaceResponse>`
- `tradeCancelAll(batchId): Promise<CancelResponse>`
- `tradeStatus(batchId): Promise<PlaceResponse>`
- `tradePositions(): Promise<PositionsResponse>`
- `tradePositionsExit(symbol, confirm): Promise<ExitResponse>`
- TypeScript types for all

### B3. Frontend smoke (15 min)
- Walk all paths: token expired, insufficient capital, overlap-skip, overlap-average, smoke success, smoke fail, place blocked, place ok, manual exit
- Verify positions tab updates after batch placement (poll picks up new orders)

---

## Track C — Integration / dry-run

### C1. Pre-market smoke (8:30 IST May 6) — operator runs (45 min)
- Open `/falcon/trade`. Refresh token via UI
- Open `/falcon/positions` — verify held list matches Kite app exactly
- Generate Preview with overlap decisions per held position
- Verify est_position vs Zerodha actual margin numbers (should be ballpark)
- Smoke test → real 1-share order
- Cancel via `/cancel-all` to clean up
- Reset for 9:15 batch

### C2. Batch entry (9:15:00 IST May 6) — operator runs
- Click Place at 9:15:00 sharp
- Watch placement; verify audit log
- Refresh `/falcon/positions` — new positions appear; SL distances make sense

---

## Time budget

| Track | Tasks | Estimate |
|---|---|---|
| A — Backend | A1–A9 (incl. A3.5 holdings + new endpoints) | ~3h 45m |
| B — Frontend | B0 done; B1–B3 | ~1h 50m |
| C — Integration | C1 prep | ~45m |
| Buffer | spec discoveries / debugging | ~1h |
| **Total tonight** | | **~7h 20m** |

Tighter against the 8h target. If running over: defer manual-exit endpoint (A7 §3.7) and positions page (B0 done, but B1 wiring) to morning daytime — Phase 1 batch entry doesn't strictly require positions tab.

---

## Dependency graph

```
A1 (schema) ──► A2 (trade_db) ──┐
                                 │
A3   (mtf)                      │
A3.5 (holdings)                 │
A4   (vol)         ──► A5 (planner) ──┐
                                       ├──► A6 (executor) ──► A7 (router incl. positions + exit) ──► A8 ──► A9
A2 ──────────────────────────────────┘
                                                                  │
B0 (mockups) — done                                              │
B2 (api client) ──► B1 (wire) ── needs A9                       ─┘
B3 (smoke) ──► B1
```

---

## Definition of done — Phase 1

- [ ] All A1–A9 green
- [ ] B0–B3 green; mockup pages wired to live backend
- [ ] `/falcon/trade`: token, configure, preview (with overlap), smoke, confirm, place, cancel — all work end-to-end
- [ ] `/falcon/positions`: lists every held position with correct SL/target/Δ; trigger watch surfaces closest-to-fire; manual exit works
- [ ] Equity check S7 rejects when over capital
- [ ] Holdings overlap detected; default skip; operator can override to average per pick
- [ ] F13 (holdings fetch fails) → soft-fail banner; allow proceed with `ignore_holdings=true`
- [ ] Audit trail in `falcon_trade_orders` matches Kite's order book + reflects `is_averaging` correctly
