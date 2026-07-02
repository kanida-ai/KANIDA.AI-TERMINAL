# AutoTrade — F&O (Derivatives) Design

**Status:** Design (for review). Covers the full derivatives matrix. Futures (long/short, current-month) is being built first; options follow after Futures stabilizes.
**Principle (unchanged):** one strategy engine, broker-independent. F&O adds an **instrument + side + strike-rule + expiry-rule** dimension in front of the existing entry→size→monitor→exit pipeline — it does NOT fork the engine.

---

## 1. The matrix to support

| # | Product | Side | Entry order | Exit order | P&L sign | Risk profile |
|---|---|---|---|---|---|---|
| 1 | Futures | **Buy** (long) | BUY | SELL | (ltp−entry)×qty | margin; loss capped at notional |
| 2 | Futures | **Sell** (short) | SELL | BUY-to-cover | (entry−ltp)×qty | margin; **unbounded loss** |
| 3 | Call (CE) | **Buy** | BUY | SELL | (ltp−entry)×qty | debit; loss capped at premium |
| 4 | Call (CE) | **Sell** (write) | SELL | BUY-to-cover | (entry−ltp)×qty | margin; **unbounded loss** |
| 5 | Put (PE) | **Buy** | BUY | SELL | (ltp−entry)×qty | debit; loss capped at premium |
| 6 | Put (PE) | **Sell** (write) | SELL | BUY-to-cover | (entry−ltp)×qty | margin; loss large (to strike) |

**Rollout order (risk-graded):** (1)(2) Futures long/short → (3)(5) option **buying** (defined risk) → (4)(6) option **writing** (margin + unbounded risk, most controls). Options buying before writing because buying is capital-defined-risk.

---

## 2. Config contract (additive to `TradingSessionConfig`)

Reuses the existing `instrument_type` (EQ/FUT/CE/PE) + `expiry_preference`, adds:

```
instrument_type : "EQ" | "FUT" | "CE" | "PE"         (exists)
direction       : "long" | "short"                   (NEW — built for FUT now)
strike_rule     : "ATM" | "ITM_1" | "ITM_2" | "OTM_1" | "OTM_2"   (NEW, CE/PE)
expiry_rule     : "weekly" | "monthly" | "next_weekly" | "next_monthly"   (NEW)
                  (default for FUT = monthly / current month)
lots_mode       : "by_capital" (premium/margin sized) | "fixed_lots"      (NEW)
fixed_lots      : int (when lots_mode=fixed_lots)
```

Validation: `short` requires FUT/CE/PE (never EQ); `strike_rule`/premium fields only for CE/PE; option **writing** (CE/PE + short) gated behind a separate `allow_option_writing` flag + margin check (highest risk).

---

## 3. Strike selection (rule-based, configurable — not AI)

Given the underlying spot and the option chain:
1. Compute the **ATM strike** = nearest listed strike to spot (`capital._select_atm_strike`, exists).
2. Apply `strike_rule` by stepping N strikes along the chain's strike interval:
   - CE: ITM = strikes **below** spot, OTM = **above**. PE: inverted.
   - `ITM_1/ITM_2` = 1/2 steps in-the-money; `OTM_1/OTM_2` = 1/2 steps out.
3. Resolve to the exact tradeable **option symbol** via the broker instrument master for the chosen expiry.

Selection is **deterministic + rule-based** (operator picks the rule). An AI/signal-driven strike selector is a later, separate layer — out of scope here.

## 4. Expiry selection (weekly vs monthly)

From the chain's distinct expiries (sorted):
- **weekly** = nearest expiry; **next_weekly** = the one after.
- **monthly** = last expiry of the current month (the monthly contract); **next_monthly** = next month's.
- **Futures:** current-month = the monthly future until its expiry, then roll to next (default `monthly`).
- Guard: never select an expiry ≤ today; on/near expiry day, roll forward.

`broker.get_option_contract(symbol, strike, expiry_rule)` / `get_active_futures(symbol, expiry_rule)` resolve the contract; both already scan the broker instrument master (Zerodha implemented via `kite.instruments("NFO")`).

## 5. Sizing (margin- and premium-aware)

The equity path sizes on price/MTF-margin. F&O adds:
- **Futures:** `lots = floor(capital_per_leg / margin_per_lot)` (SPAN+exposure from the broker's order-margins), qty = lots×lot_size. **Not** notional (notional-sizing under-sizes retail capital → 0 lots — the current bug being fixed in the Futures build).
- **Option buy:** `lots = floor(capital_per_leg / (premium × lot_size))` (debit = premium×qty; capital.py CE/PE branch exists).
- **Option write / futures short:** sized on **margin per lot**; PLUS an exposure cap so a single short can't exceed a configured % of capital (unbounded-loss guard).
- `min 1 lot or InsufficientCapital` (never place 0/partial-lot).

## 6. Direction plumbing (the core-engine change — shared with the Futures build)

`direction` threads through the pipeline; `long` = today's behavior exactly:
- **Entry side:** long→BUY, short→SELL.
- **Exit side:** long→SELL, short→**BUY-to-cover**.
- **P&L sign (monitor):** long `(ltp−avg)×qty`, short `(avg−ltp)×qty`.
- **Stop/target + GTT-OCO:** short inverts — stop ABOVE entry, target BELOW.
- **Kill/trail:** run on `gross_return` (fraction) — sign-correct P&L makes them work unchanged for shorts.

This is built once (Futures) and reused for option writing.

## 7. Risk controls (short & writing — mandatory before live)

- **Margin preflight:** confirm available margin ≥ required (order-margins) before entry; block otherwise.
- **Exposure cap:** max notional/margin per short leg + per session (config).
- **Hard stop always on:** short/writing sessions REQUIRE a stop (software + broker GTT where supported) — no naked unbounded position without a stop.
- **Square-off enforced** for intraday F&O; carry only if explicitly configured + margin sufficient.
- **Assignment/expiry handling** (options): flag/flatten near expiry; never let a written option get assigned unmanaged.

## 8. Broker adapter surface

Each `BrokerClient` already exposes `get_active_futures / get_option_chain / get_option_contract / get_lot_size`. Add per-broker: futures/option **margin per lot**, F&O order params (exchange NFO/segment, product NRML/MIS), and short-OCO expression. Capabilities (`fno`, `supports_gtt`) already gate this per broker in the registry.

## 9. UI (session config)

Add to the AutoTrade config form:
- **Instrument** selector: Equity | Futures | Call (CE) | Put (PE).
- For FUT: **Buy / Sell** toggle → `direction`; expiry note (current month).
- For CE/PE: **Buy / Sell**, **strike rule** (ATM / ±1/±2 ITM/OTM), **expiry** (weekly/monthly).
- Show computed preview: resolved contract, spot, expiry, strike, lot size, lots, margin/premium — before Start.
- Option **writing** behind an explicit "advanced / high-risk" confirm.

## 10. Staged rollout

- **Stage F1 (today):** Futures **long + short**, current-month expiry, margin sizing, direction plumbing, paper-safe + tested. Deploy behind the paper gate; certify live before real shorts.
- **Stage F2:** Options **buy** (CE/PE), strike rules (ATM/±ITM/±OTM), weekly/monthly expiry, premium sizing.
- **Stage F3:** Options **writing** (CE/PE short) — margin, exposure caps, assignment handling, mandatory stops.
- **Stage F4:** UI for all of the above + preview; per-broker F&O certification.

## 11. Acceptance criteria

1. Equity-long path byte-for-byte unchanged at every stage (`direction="long"` = today).
2. A futures short: SELL entry, BUY-to-cover exit, profit when price falls, stop above / target below, margin-sized — verified in paper then certified live.
3. "Buy CE → OTM_1 → next weekly": engine resolves underlying→spot→chain→expiry→strike→symbol→lot→qty deterministically and sizes by premium.
4. No naked short/written position ever runs without a stop; margin preflight blocks under-margined entries.
5. Adding a broker's F&O = adapter methods + capability flags; zero strategy-engine change.
