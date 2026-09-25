# Rupeezy (flow.rupeezy.in/web — Aastha/Rupeezy broker web) — raw field notes, logged-in, 2026-09-25 ~08:35–08:48 IST (pre-market; last-close data)
Account state: funds Available 0.00 (every execution screen shows "Add more funds to execute this strategy" + "+ Add funds"). Dark theme.

## R0 Home (/web)
- Header: NIFTY / BANKNIFTY tickers (value, change, %), "Ask FinAI" (AI assistant pill), Home · Market · Orders · Portfolio · avatar.
- Left rail: global search "Stocks & FO…" (tabs All/Equity/Options/Futures; chips All/CALL/PUT; exchange ALL/NSE/BSE/MCX; row hover: link, chart, +watchlist; B/S on F&O rows), 3 watchlist pages, "watchlist" empty state "Your watchlist is empty… Add Stocks".
- "RECOMMENDED FOR YOU by SEBI Registered Experts — Stocks and F&O" carousel: NIFTY 06 Oct 2026 CE 23000 "Expected profit 50.38%", "Target left 53.82%" bar; View all. Clicking a card → modal "Investment Disclaimer" (market risk; no guaranteed returns; decisions mine; "By proceeding I confirm I have read and accepted the Terms & Conditions") Cancel/Proceed — NOT accepted (T&C acceptance needs owner OK).
- Curated ETF Baskets; Trading Products (MTF Trends, ETFs, Pledge, T+5, Bids, Commodities, Rupeezy Global); For Traders: FO Scanner ("Spot High-Potential Trades Instantly"), T+5, **Option Strategy ("Build or use expert strategies")**; Tools: Corporate Actions, **Basket Order**, User Preference, Keyboard Shortcuts; Market News with importance + sentiment tags.
- Right: Positions/Holding/MTF P&L card (Overall/Unrealised/Realised; "P&L based on market depth" toggle); mini Option Chain / Scanner widget (F&O | MCX, underlying + expiry, OI/Call price/Strike+PCR/Put price/OI).

## R1 Option Strategy → Strategy Store (/web/options-strategy/?tab=store)
- Tabs: Strategy Store | Strategy Builder. Underlying chips: NIFTY, BANKNIFTY, SENSEX, FINNIFTY, MIDCPNIFTY, NIFTYNXT50, BANKEX + "Search Option Symbol" (stocks e.g. RELIANCE → monthly expiries only 29 Sep/27 Oct/23 Nov).
- "Select expiry" chip row (18 expiries 29 Sep 2026 … 24 Jun 2031, horizontal scroll). Product Type: Intraday | Carryforward.
- 12 templates as cards with mini payoff + RELATIVE leg recipe: Bull Call Spread (B 1 ATM CE, S 1 OTM CE), Bull Put Spread (B 1 OTM PE, S 1 ITM PE), Ratio Call Spread (B1 ATM CE, S2 OTM CE), Ratio Put Spread, Bear Call Spread, Bear Put Spread, Short Strangle, Long Strangle, Iron Condor (4 legs), Iron Butterfly, Short Straddle, Long Straddle. (i) info icon per card. No bull/bear/neutral grouping.
## R2 Template instantiation (click card)
- Header: ← NIFTY [INDEX][NSE] 29 Sep 2026 ₹23063.10 (chg).
- Accordion list of 4 auto-generated VARIANTS ("Bull Call Spread 1…4" = progressively wider strikes): each row Max profit / Breakevens / Max loss + Multiplier stepper. Expanded: legs "1x (B) 23050 CE 29 Sep 152.55  264.60(63.43%)" (change vs prev close looks wrong pre-open), fund bar "Add more funds…", Required 40,116.83 / Available 0.00, Analyse | Continue.
- DATA BUG: card breakeven 23,078.85 vs analysis breakeven 23,087.50 for the same strategy.
## R3 Analysis modal ("Bull Call Spread 1 Analysis")
- Underlying + chg; payoff chart (expiry red/green fill + blue T+0 curve), hover tooltip "Expected P&L at 23375 — Thu, Sep 24: 1075.45 — At expiry: 1374.75".
- "Days To Expiry" slider 4→0 (moves blue curve only; Greeks/BE do NOT update).
- Right: Breakeven points, Max profit, Max loss, Delta/Gamma/Theta/Vega (strategy), Multiplier, legs (read-only — no strike/qty edit), funds bar, Required/Available, Continue.
## R4 Order review ("Bull Call Spread 1") — broker handoff
- Table Name (NIFTY [FO][NSE][RL-MKT] 29 September 2026 CE 23050), Product Type Intraday, LTP (+chg), Order Price MKT, Lots/Qty 65; Required/Available; Analyse | **Execute** (NOT clicked). All legs market orders; no limit/leg-sequencing control.
## R5 Strategy Builder tab (?tab=builder) = PREDICTION WIZARD (not a leg builder)
- Same underlying/expiry/product selectors; card "Select prediction: Above | Between | Below" + "Enter predicted value" (two inputs for Between) + Continue.
- Tested NIFTY Between 22900–23300 → flat list of ~43 results: 1 Short Straddle, ~41 "Short Strangle" (all named identically, strikes only visible when expanded), 1 Iron Condor at the very bottom. Unlimited-loss strategies first; no ranking by POP / fit / return-on-margin; no filters; no dedupe.
- Short Straddle expanded: Max profit 16,916.25, BE 22,789.75/23,310.25, Max loss Unlimited, Required 1,82,630.82.
- BUG: switching underlying to RELIANCE (₹1219) kept NIFTY predicted values 22900/23300; Continue → nothing, then late toast "No strategies found — Try a different prediction or expiry" appeared only after navigating away (no inline validation).
## R6 Stock detail → Option Chain (/web/markets/stockDetail/?symbol=NSE:NIFTYIDX&tab=Option+Chain)
- Header: NIFTY [INDEX][NSE][Reco], price, "+ Add"; expert reco strip (B 06 Oct CE 23000 Potential Gain 50.38% Target left …; View All).
- Tabs Overview | Chart | Option Chain | Future Chain | News (AI Powered).
- Chain: expiry dropdown centre; Calls: Delta, Gamma, Theta, Vega, OI, IV, Volume, LTP(%chg) | Strike + PCR(OI) | Puts mirror. ATM spot pill. Row hover: S / B / chart icons → single order ticket (no multi-select into a strategy).
## R7 Orders → Basket Order (/web/orders/?tab=basket) — the only custom multi-leg path
- Order tabs: Executed · Pending · Rejected · GTT · Basket Order (count). GTT table cols: Symbol, LTP, Product Type, Lots/Qty, Status, StopLoss, Trail Jump, Target (empty).
- Basket list (user had 4: nifyy, nifty×3, 0 orders each), "+ Create New Basket" inline name field (max 16) with ✕/✓; bug: first ✓ said "Basket name is required!" although typed (focus issue). Global Execute button.
- Basket modal "KANIDATEST (0/10)" (max 10 legs): "Search & add to Basket" (All/Equity/Options/Futures) → order ticket: B/S toggle, Intraday/Carryforward (Max Qty), Lots (1 lot = 65 shares, Top Ask/Bid), Limit|Market, Stop Loss toggle + trigger, Validity DAY, Required + View Charges, Available, "Add to Basket". Margin for the leg loads late (layout shift).
- Basket table: Name, Product, LTP, Order Price, Lots/Qty; row hover: drag-reorder, edit, duplicate, delete, send (place single leg!), chart. Footer Required (approx) ₹38,336.94 / Final (approx) (hedge benefit shown) / Available; Analyse | Execute.
- Basket Analyse: underlying selector, per-leg include checkboxes, Breakeven 23,137.50, Max profit 3,467.75, Max loss 3,032.25, strategy Greeks, payoff chart (T+0 curve appeared only after toggling a leg; prices briefly 0.00 pre-open).
- Delete basket: hover row → trash/eye/edit; trash deletes IMMEDIATELY (no confirm) → toast "Basket deleted successfully". Test basket deleted; user's 4 baskets untouched.
## R8 Market → FO Scanners: Index/NIFTY/Expiry filters; Most Active, OI Gainers, OI Losers, Price Gainers, Price Losers; empty "No data found" pre-open.
NOT FOUND: saved strategies, virtual/paper trading, backtest, strategy-level alerts, adjustments/roll, P&L table, target-date what-if by spot, IV what-if.
