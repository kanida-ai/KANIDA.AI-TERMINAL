# 05 · Consolidated comparison — Sensibull · TrendSpider · Rupeezy/Aastha · 5paisa

Based only on hands-on sessions in the live, logged-in products on 25 Sep 2026 (see files 01–04). ✅ = present and working · ◐ = partial or broken · ❌ = absent.

## 1. Capability matrix

| Capability | Sensibull | TrendSpider | Rupeezy | 5paisa |
|---|---|---|---|---|
| Ready-made library | ✅ 38, by intent | ❌ (options) | ◐ 12 recipes, no intent | ✅ 38 incl. diagonals |
| Relative-strike recipe shown | ❌ | — | ✅ "B 1 ATM CE" | ❌ |
| Auto strike variants per template | ❌ | — | ✅ 4 widths | ❌ |
| View-based wizard | ✅ Above/Between/Below + filters | ❌ | ◐ unranked flood | ◐ single-leg only |
| Beginner risk tiers + plain English | ✅ Easy Options | ❌ | ❌ | ◐ budget Customize |
| Chain leg picker | ✅ 4 modes, hover B/S | ❌ | ❌ (single orders) | ✅ persistent B/S, inline lots |
| Leg edit (strike±, B/S, expiry, lots, price) | ✅ | — | ❌ read-only | ✅ |
| Include/exclude a leg | ✅ | — | ✅ (basket analyse) | ✅ |
| Bulk Shift / Width / Hedge | ✅ unique | — | ❌ | ❌ |
| Auto strategy recognition | ✅ | — | ❌ | ❌ |
| Futures / synthetic futures | ✅ / ✅ | — | ◐ via basket | ✅ / ❌ |
| Payoff: expiry + target-date curve | ✅ | — | ✅ | ✅ |
| SD bands (fixed/dynamic) | ✅ | — | ❌ | ◐ fixed lines |
| OI overlay on payoff | ✅ | — | ❌ | ◐ broken tab |
| Payoff table | ✅ | — | ❌ | ❌ |
| P&L table per leg | ✅ | — | ❌ | ✅ |
| Greeks per leg + total | ✅ | — | ◐ total only, static | ✅ |
| POP | ✅ | — | ❌ | ✅ (updates with what-if) |
| Breakeven target vs expiry | ✅ | — | ◐ (inconsistent) | ◐ expiry only |
| What-if spot | ✅ | — | ❌ | ✅ |
| What-if date | ✅ | — | ✅ DTE slider | ✅ |
| What-if IV | ✅ | — | ❌ | ✅ |
| Live margin + funds | ✅ + existing-position toggle | — | ✅ + hedge benefit | ✅ (no shortfall warning) |
| Charges breakdown | ✅ | — | ◐ per leg | ❌ |
| Save / Save As / rename / delete | ✅ | ✅ (tester) | ❌ (baskets only) | ❌ |
| Share link | ✅ | ◐ | ❌ | ❌ |
| Persist unsaved work | ❌ | ✅ workspace | ❌ | ❌ |
| Paper / virtual trading | ✅ draft portfolios, full loop | ❌ | ❌ | ◐ VTT single-scrip, broken |
| **Backtest** | ❌ | ✅ **best** | ❌ | ❌ |
| Broker basket: limit/market per leg | ✅ | — | ◐ strategy MKT only; basket ✅ | ✅ + SL trigger |
| Leg order / re-arrange | ✅ | — | ✅ drag | ❌ |
| Freeze-qty awareness | ❌ seen | — | ◐ max qty | ✅ |
| Strategy-level alerts | ❌ | ◐ chart alerts/bots | ❌ (GTT per order) | ❌ (price alert per contract) |
| Adjustment suggestions | ❌ | ❌ | ❌ | ❌ |
| Positions grouped by strategy | ✅ Groups | — | ❌ | ✅ Groups (New) |
| Timezone-correct dates | ◐ basket bug | ✅ | ✅ | ❌ |

## 2. What each does best

### Sensibull — the most complete analysis-to-execution loop
- It is the only product where **one screen** carries template → chain → leg edit → payoff, tables and Greeks → IV/spot/date what-if → broker margin → basket.
- **Shift / Width / Hedge** turn adjusting or rolling into one-click operations.
- **Draft Portfolios** is a complete paper-trading lifecycle: enter, mark-to-market, exit at a chosen price and time, notes, import broker trades, convert to real.
- **Three-tier discovery**: Wizard for view holders, Easy Options (plain English, Defensive / Balanced / Aggressive) for beginners, Expiry Trades for expiry day.

### TrendSpider — evidence and composability
- A **Strategy Tester** that shows *whether an idea has worked*:
  - a random-entry control;
  - a post-entry behaviour fan;
  - equity vs buy-and-hold with drawdown, Sharpe and Sortino;
  - expectancy and an explicit sample size ("153 positions across 27.3 years").
- **AI authoring** ("describe your strategy") and **Explain**.
- **Widget workspace**, where alerts and bots are first-class.

### Rupeezy / Aastha — "compare structures" and execution hygiene
- **Recipe templates** (Buy 1 ATM CE / Sell 1 OTM CE) teach strike logic.
- **Auto-generated variants** (4 widths) are compared side by side on Max P/L, breakeven and margin.
- **Product type chosen up front.**
- A **basket with per-leg SL and limit**, drag re-order, and **hedge-benefit margin** ("Final (approx)").
- A funds guard with an inline Add-funds CTA on every execution surface.

### 5paisa — fast picking and real order controls
- **Chain drawer ergonomics:** always-visible B/S on every strike and an inline lot stepper.
- **The broadest templates** (includes diagonals).
- **POP that recomputes** with the what-if; separate **Margin Required vs Funds Needed**.
- **Order Pad** with Market / Limit / **SL + trigger**, a **freeze-qty** hint and Execute-at-Market.
- **Budget-first sizing** (amount, max-loss %, profit %).

## 3. Common across all (the table stakes)
1. Instrument and expiry selection with days to expiry.
2. Buy/sell × call/put legs with lots.
3. Payoff at expiry plus a T+0 curve.
4. Max profit / max loss / breakeven.
5. Required margin or funds.
6. A path to the broker.
7. Some discovery surface (templates or ideas).
8. Dark/light trader UIs with dense tables.

TrendSpider shares only the discovery/scanner and alert parts.

## 4. Important differences

| Dimension | Range observed |
|---|---|
| **Where custom structures are built** | Full editor (Sensibull, 5paisa) · basket-only (Rupeezy) · none (TrendSpider) |
| **How templates become strikes** | ATM-relative fill with fixed width (Sensibull 200, 5paisa 250) · recipe → 4 width variants (Rupeezy) |
| **Discovery logic** | Model-ranked by view and filters (Sensibull) · unranked enumeration (Rupeezy) · fixed % targets on single legs (5paisa) · rule backtests (TrendSpider) |
| **Evidence** | None of the three F&O apps show *historical performance of the strategy*. Only TrendSpider backtests, and not for options |
| **Practice** | Full paper portfolios (Sensibull) · broken single-scrip VTT (5paisa) · none (Rupeezy) |
| **Execution** | Basket with depth + re-arrange (Sensibull) · market-only strategy orders + limit/SL basket (Rupeezy) · Order Pad with SL/freeze qty (5paisa) |
| **Persistence** | Saved + deep-link + share (Sensibull) · baskets only (Rupeezy) · nothing (5paisa) |

## 5. Missing everywhere (the KANIDA opportunity)
1. **Strategy backtests on NSE options**, run as the same strategy that is traded (entry = next open, costs and slippage, exact stop/target/horizon, point-in-time). None of the four has this.
2. **Expectancy-ranked discovery.** Nobody ranks structures by historical ETV, return on margin or drawdown. Sensibull ranks by model POP; the others don't rank at all.
3. **Strategy-level monitoring:** P&L, breakeven-breach, delta-drift, IV-crush and time-to-expiry alerts on the *strategy*, not the contract.
4. **Adjustment and roll assistant:** "tested breakeven — here are 3 adjustments with new payoff and cost". Only manual Shift/Width exists.
5. **Autosave and version history** of a strategy (every edit, what-if and trade).
6. **Exchange-time correctness** for every date and time.
7. **Unified paper → live** with the same object model: one strategy object that is simulated, backtested, paper-traded, executed and monitored.
8. **Multi-broker execution** with pre-trade risk checks (margin shortfall, freeze qty, leg sequencing, max-loss cap) enforced server-side.
9. **Explainability everywhere:** Sensibull's "How does this trade work?" exists only in the beginner tool, not in the pro builder.
10. **Honest labelling** of sample size, model assumptions and risk. "LOW RISK" on OTM buys and "Expected profit 50%" are anti-patterns.

## 6. UX problems observed (with the product)
| Problem | Where |
|---|---|
| Unsaved work lost on navigation | Sensibull, 5paisa |
| Bulk-delete pre-selects 5 user portfolios | Sensibull Draft Portfolios |
| Basket delete with no confirmation | Rupeezy |
| Single-leg "send" inside a multi-leg basket (leg risk) | Rupeezy |
| Market-only strategy execution | Rupeezy |
| No margin/shortfall on the order screen while account margin is negative | 5paisa |
| Timezone-shifted expiry and dates | 5paisa (everywhere), Sensibull (basket) |
| Broken tabs with no empty or error state | 5paisa Strategy Charts, MultiStrike OI |
| Wizard offline before 09:15 | Sensibull |
| Stale "check back by" time in the past | Sensibull Expiry Trades |
| Unranked flood of identically named results | Rupeezy prediction builder |
| Inconsistent breakeven between two views | Rupeezy |
| Greeks don't react to the DTE slider | Rupeezy |
| Stale inputs carried across underlyings; silent Continue; late toast | Rupeezy |
| Short straddle is the silent default | Sensibull chain |
| Misleading risk labels | 5paisa Idea |
| Terms-acceptance modal before any expert idea | Rupeezy |
| More than 40 controls on the core screen; beginners sent to a different product | Sensibull |
| VTT flow navigates away and breaks | 5paisa |
| Slide-up dock tabs with tight hit targets | 5paisa |

## 7. Features that should be combined
1. **Template recipe (Rupeezy) + intent grouping (Sensibull) + diagonals (5paisa) + auto-variants (Rupeezy)** → one library where each card shows the recipe and expands into ranked concrete variants.
2. **Wizard (Sensibull) + budget sizing (5paisa) + backtest evidence (TrendSpider)** → one "Discover" flow: *view + budget + risk appetite → ranked strategies with historical expectancy*.
3. **Chain picker:** 5paisa's persistent B/S + inline lots with Sensibull's Straddles/Strangles/Futures modes and the LTP/OI/Greeks views.
4. **Builder analysis:** Sensibull's full stack (payoff + table + SD + OI + IV) with 5paisa's POP-updating what-if and "Funds Needed vs Margin Required".
5. **Execution:** Sensibull basket (depth, re-arrange, shortfall) + Rupeezy per-leg SL/limit + hedge-benefit margin + 5paisa freeze-qty and SL trigger. **No single-leg send and no market-only mode.**
6. **Practice:** Sensibull Draft Portfolios as the base, fed by *the same strategy object* used by backtest and live.
7. **Monitoring:** TrendSpider-style alerts/bots, but defined on strategy metrics.

## 8. Features that should be simplified
1. **The builder's first screen:** progressive disclosure. Show legs + payoff + 4 key numbers (max P/L, breakeven, POP, margin). Tables, SD, OI, IV and Greeks go into one "Analyse" drawer.
2. **Discovery results:** show at most 5 ranked, deduplicated structures with a reason line, not 43 rows.
3. **What-if:** one combined control ("NIFTY at ___ on ___ with IV ___"), not three separate slider blocks.
4. **Templates:** fewer names on the surface (about 12 core), with exotic ones (Batman, Double Plateau, Strip/Strap) behind "More".
5. **Order screen:** one screen with a margin check, charges and leg sequence, and one primary "Place strategy" button.
6. **Practice vs live:** a single toggle (Paper | Live) on the same screen, not separate products (VTT, Drafts).
