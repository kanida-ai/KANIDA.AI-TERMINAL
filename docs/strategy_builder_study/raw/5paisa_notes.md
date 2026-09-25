# 5paisa FNO 360 (fno.5paisa.com) — raw field notes, logged-in, 2026-09-25 ~08:50–09:00 IST (pre-market, last-close data)
Account: MARGIN -88.50 (negative), user PUDHURAJA 54174431. Dark theme. Builder is visibly a Sensibull-pattern clone.

## P0 Shell
- Top: logo · NIFTY / BANKNIFTY tickers · MARGIN value + "+" (add funds) · "Search & Trade" · Products · bell · settings · user.
- Left nav: Overview · Option Chain · Open Interest · Scalper · Idea (NEW) · F&O Stats · Strategy Chart · Strategy Builder · Company Info.
- Bottom dock (panels slide up): Positions (P/L) · Orders · Holdings · VTT · Basket · Watchlist · Screener · News.

## P1 Strategy Builder — empty (/strategy-builder)
- Instrument search "NIFTY 23063.10 -1.63%"; Clear · refresh · gear.
- Empty card with "Build your Strategy" button (opens chain drawer).
- Ready-made: Bullish | Bearish | Neutral | Others + expiry dropdown (28 Sep 2026, 05 Oct (W), 12 Oct (W), 18 Oct (W), 26 Oct, 02 Nov …).
  - Bullish(12): Buy Call, Sell Put, Bull Call Spread, Bull Put Spread, Call Ratio Back Spread, Long Calendar With Calls, Long Call Diagonal Spread, Bull Condor, Bull Butterfly, Range Forward, Synthetic Long Call.
  - Bearish(12): Buy Put, Sell Call, Bear Put Spread, Bear Call Spread, Put Ratio Back Spread, Long Calendar With Puts, Long Put Diagonal Spread, Bear Condor, Bear Butterfly, Risk Reversal, Synthetic Short Call.
  - Neutral(8): Short Straddle, Iron Butterfly, Short Strangle, Short Iron Condor, Batman, Double Plateau, Jade Lizard, Reverse Jade Lizard.
  - Others(8): Call Ratio Spread, Put Ratio Spread, Long Straddle, Long Iron Butterfly, Long Strangle, Long Iron Condor, Strip, Strap.
  - Chips = small payoff sparkline + name (compact pill buttons, not tiles).
- !! TIMEZONE BUG: expiries rendered in browser TZ (PT) → "28 Sep" while the contract is 29 SEP (Order Pad says "NIFTY 29SEP 23050 CE"; URL expiry=2026-09-29T00:00:00Z). Target-date stepper shows "Wed, 23 Sep 15:30" for last session (actually Thu 24 Sep IST) and walks through "Sun, 27 Sep". Strategy Chart x-axis 8:45 PM–2:55 AM (= 09:15–15:25 IST). Sensibull's basket also printed "28th Sep" (same class of bug).

## P2 Builder — loaded (Bull Call Spread → 23050/23300; note 250 wide vs Sensibull 200)
- Leg table: [✓] B/S pill · Expiry dropdown · – Strike + · CE/PE · – Lots + · Entry Price (editable) · LTP · trash. Master checkbox.
- Net Premium "Pay 104.40"; Lot Multiplier stepper; Trade · Add / Edit. NO Save / Save As / Share / Drafts / Charges / Shift-Width-Hedge / auto strategy name.
- After adding a 3rd leg manually the ready-made chip "Bull Call Spread" stays highlighted (stale) and no strategy name is shown.
- Summary strip: Max. Profit 9,464 (22.86%) · Prob. of Profit 42.87% · Margin Required 41,396.88 · Funds Needed 44,526.63 · Max. Loss -6,786 (-16.39%) · RR Ratio 0.72 · Breakevens 23,154.41 (0.40%) — each with (i) tooltip. 3-leg custom: Max profit 14,115, Max loss Unlimited, POP 51.93%, Margin 2.01 L, RR NA.
- Tabs: Payoff charts | P&L Table | Greeks (i) | Strategy Charts | MultiStrike OI.
  - Payoff: expiry (red/green) + T+0 (blue) curves, ±1SD/±2SD lines, "Price (23,063.10)" marker, "P&L (0.00)" marker at target.
  - What-if: NIFTY Target stepper + % + slider + Reset; "Target Date: 5 days to Expiry (i)" ‹ › + slider + Reset. Tested 23300 & 1 day to expiry → P&L (4,699.50); POP recalculated 41.97%.
  - P&L Table: Instrument, Target P&L, Target Price, Entry Price, LTP, Total.
  - Greeks: per-leg Delta, Theta, Decay, Gamma, Vega + Total.
  - Strategy Charts: blank axes (NIFTY vs Net Premium vs Time) — no data, no empty-state message (broken pre-market).
  - MultiStrike OI: "Please select strategy to draw charts" even with a strategy loaded (broken).
- Strikewise IV: Offset stepper + Reset IV + per-leg IV steppers (e.g. 28 Sep 23050 CE 13.24).
- Builder settings (gear): Multiply By Lot Size toggle · Manual P&L toggle.
- State NOT persisted: navigating to another page and back → empty builder. No saved strategies at all.

## P3 Add/Edit / Build your Strategy — chain drawer
- OPT | FUT toggle · expiry dropdown · NIFTY spot · Greeks checkbox · Reset · Done.
- Rows: Call LTP | B S (always visible) | OI bar+value | Strike | OI | B S | Put LTP. Greeks on → Gamma, Vega, Theta, Delta, LTP each side. Selected leg shows inline lot stepper under the B/S. ATM row highlighted.
- FUT: 3 expiries with B/S + LTP (no synthetic futures).
## P4 Trade → "Order Pad" modal (broker handoff)
- Delivery | Intraday radio; per leg: B/S, name "NIFTY 29SEP 23050 CE", Product (DELIVERY), Order Type Market|Limit|SL, Qty stepper 65 + "Freeze Qty 1755", Price, Trigger, LTP. Buttons "Execute At Market" · "Execute" (NOT clicked). No margin/charges in pad.
## P5 VTT (bottom dock) — virtual trading
- Empty: "No VTT orders placed" + "Start New VTT" → "Search Scrip for VTT" (All/FNO/Equity/ETF, Recent View) — SINGLE-SCRIP, not strategy-level. Picking a contract NAVIGATED to the Company Info page and the search modal then showed "No Records Found!" — broken/confusing flow; could not place a virtual order from the builder.
## P6 Company Info (contract page /stock-info?...)
- Header: contract, LTP/chg, Price Alert, Add to Basket, Add to Watchlist, Buy, Sell; stats strip Open/High/Low/Pclose/Volume/Upper & Lower Circuit/ATP/Lot Size 65/OI 71.71L/Change in OI/Spot Rate; TradingView chart; Market Depth.
- Set Price Alert modal: "Alert me when LTP price" Rise Above | Falls Below; Limit Price; Alert at [value]; Add Message; Set Alert — contract-level only, no strategy P&L alert.
## P7 Idea (NEW) (/ideas/derivatives) — discovery
- Tabs Derivatives (NEW) | Basket Ideas | Commodity (NEW).
- QUICK OPTION TRADE: NIFTY / BANKNIFTY / SENSEX cards "Bearish ←Guess the trend→ Bullish".
- Bullish NIFTY → 3 single-leg call cards: ATM 23050 CE [RECOMMENDED][HIGH RISK], OTM1 23100 CE [LOW RISK], OTM2 23150 CE [LOW RISK]; each CMP, Enter, Target (=entry×1.4), Stop Loss (=entry×0.7), Investment Req (₹9.92K), Expected Loss / Expected Profit, Lots stepper, TRADE NOW. Risk labels are misleading (far-OTM buys labelled "LOW RISK").
- Customize ▾: Amount you want to trade with (10000) · Max loss % ⇄ ₹ (30 / 3000, "for calculating stoploss") · Profit % ⇄ ₹ (40 / 4000) · Save.
- SHORT TERM / INTRADAY expert calls: "No active calls available at this time" + View History / View All. Basket Ideas: "Derivative Baskets by Vibhore Gupta (i)" — none active.
## P8 Strategy Chart (/straddleChart): STRADDLE CHART | STRANGLE CHART dropdown; NIFTY/BANKNIFTY/FINNIFTY/MIDCPNIFTY; Call strike, Put strike, Expiry 29 Sep'26; intraday line Strangle premium vs NIFTY (dual axis); "Place Strategy" CTA.
## P9 Basket (dock): empty "No Baskets — Create a basket and place multiple order at one time" + Add New Basket. Not linked from builder.
## P10 Positions (dock): Positions | Groups (New); All/Open/Closed/Day counts; cols Company/Contract, B/S, Qty, Avg Price, LTP, P&L; Total Buy/Sell, Booked/Unbooked/Total P&L; refresh, search, collapse, filter, pop-out, close. Empty "There are no open Positions".
NOT FOUND: save/duplicate/delete strategy, strategy-level virtual trade, backtest, strategy alerts, adjust/roll helpers, share, charges breakdown in builder.
