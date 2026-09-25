# Sensibull — raw field notes (live study, logged-in account, 2026-09-24/25 IST)

## S0 Home (/home)
- Top nav: Home · Trade ▾ · Analyse ▾ · Watchlist (New) · Positions · Orders · avatar menu.
- Hero: "6020 people are sharing their live positions" -> Showcase carousel of verified live P&L cards (name, handle, capital e.g. 19.94L, return %). Links /verified-pnl/<slug>/live-positions.
- Entry tiles: Easy options (/trade-options: "just guess up or down") · Strategy wizard (/trade-options-strategies: "predict index/stock, get best strategies") · Strategy builder (/option-strategy-builder) · Draft Portfolios (/draft-portfolios) · NIFTY Heatmap · Share Verified P&L · Mindful Trading promo (Try now / Dismiss popup that also appears inside builder).
- Advanced tools: Option chain, OI analysis, Multi-strike OI, FII/DII, Multi Straddle-Strangle charts, Live options charts, Stock data; Find trades: Screener, Technical signals; Others: IV chart, Events calendar.
- Trade ▾ mega-menu: Strategy Builder (Popular) · Strategy Wizard · Expiry Trades · Easy Options · Draft Portfolios · Mindful Trading (New) · Learn Options Strategies (New) · Learn Options Trading (videos) · Verified by Sensibull.

## S1 Strategy Builder — empty state (/option-strategy-builder?instrument_symbol=NIFTY)
- Left column: instrument search box "NIFTY 23063.10 -1.64%" + chart icon + Info; Settings button.
- Trades card empty state: illustration "No Trades Added" + primary CTA "Build a new custom strategy"; footer checkbox "Manual P/L (?)" + "Add Manual P/L" (disabled).
- Tabs under it: Ready-made | Positions | Saved Strategies | Draft Portfolios.
- Ready-made: "Please click on a ready-made strategy to load it" + link Learn Options Strategies. Chips Bullish/Bearish/Neutral/Others; Expiry dropdown (29 Sep (4 Days), 06 Oct (11), 13 Oct, 19 Oct, 27 Oct, 03 Nov, 23 Nov, 29 Dec (95 Days)).
  - Bullish(11): Buy Call, Sell Put, Bull Call Spread, Bull Put Spread, Call Ratio Back Spread, Long Calendar with Calls, Bull Condor, Bull Butterfly, Range Forward, Buy Future, Long Synthetic Future.
  - Bearish(11): Buy Put, Sell Call, Bear Call Spread, Bear Put Spread, Put Ratio Back Spread, Long Calendar with Puts, Bear Condor, Bear Butterfly, Risk Reversal, Sell Future, Short Synthetic Future.
  - Neutral(8): Short Straddle, Iron Butterfly, Short Strangle, Short Iron Condor, Batman, Double Plateau, Jade Lizard, Reverse Jade Lizard.
  - Others(8): Call Ratio Spread, Put Ratio Spread, Long Straddle, Long Iron Butterfly, Long Strangle, Long Iron Condor, Strip, Strap.
  - Each tile = mini payoff sketch (red loss / green profit) + name.
- Footer: "Prices last updated at --. (Prices are auto-refreshed every 30 seconds)"; "Important info" accordion.
- Right column (collapsed accordions until trades exist): Summary · Charts and Graphs · Greeks and IVs.

## S2 Strategy Builder — loaded strategy (Bull Call Spread from Ready-made, 29 Sep)
Pre-market 08:16 IST; data = last close (Thu 24 Sep 3:40 PM). "Prices auto-refresh every 30s".
LEFT "New Strategy" card:
- Title "New Strategy" (after save: name + pencil rename) · "Insights" link · "Clear New Trades".
- Master checkbox "2 selected - Bull Call Spread" (strategy auto-named from legs; renamed live e.g. Short Straddle -> Short Strangle when width changed) · "Reset Prices".
- Leg rows: [checkbox include] [B/S pill (click toggles)] [Expiry dropdown] [– Strike +] [Type CE/PE/FUT] [Lots dropdown] [Price editable; corner mark when edited] [≡ = market-depth popover: OHLC + 5-level bids/offers qty] [trash].
- Bulk adjusters: Shift (– N +: moves ALL strikes by step; tested +50 → 23100→23150 both legs) · Width (– N +: distance between legs; straddle +100 → CE 23200/PE 23100, auto-renamed Short Strangle) · Hedge (– 200 +: wing distance; disabled for straddle, enabled for spreads).
- Multiplier dropdown (x strategy) · "Price Pay 90.75" (net per unit, Pay/Get) · "Premium Pay 5,899" · "Charges" (modal).
- Buttons: Add/Edit (opens option-chain drawer) · Add to Drafts (virtual) · Trade All (broker basket) · "…" menu = Save / Save As (enabled only after first save; = duplicate) / Share ▸ Facebook, Twitter, WhatsApp, Telegram, Copy Link.
- Manual P/L checkbox + Add Manual P/L (inject manual realised P&L).
RIGHT summary strip:
- Max Profit +7,101 (+20%) · Max Loss -5,899 (-17%) (shows "Unlimited" for naked short) · Breakeven with Target|Expiry toggle (23141 (+0.3%); two BEs for strangle) .
- Reward/Risk 1.2 with "1/x" inverter · POP 44% · Time Value 2,974 · Intrinsic Value 2,925.
- Funds & Margins: Funds Needed 39,898 · Margin Needed 35,582 · Margin Available 1,89,594 (live from broker). Gear = toggles "Include existing positions for funds calculation" / "...for margin calculation".
Analysis tabs: Payoff Graph | P&L Table | Greeks | Strategy Chart; toggle "Add Booked P&L".
- Payoff Graph sub-tabs Payoff Graph | Payoff Table. Graph: two curves On Expiry (green/red) and On Target Date (blue); -2SD/-1SD/1SD/2SD vertical bands; "Current price" marker; hover/marker "Projected loss: -68 (-0.19%)" badge at target; OI bars overlay (OI Change dropdown: Show Open Interest toggle, OI vs Change in OI, Expiry-used multi-select; note "Intraday OI change not available 8:00–9:18am and holidays"; tooltip "OI overlay not available for MCX, USDINR and far expiries"). "SD Fixed" dropdown = SD Mode: Fixed SD (N days, default 7) vs Dynamic SD (from target date/time). Zoom In button.
- Payoff Table: Target Interval dropdown (10/25/50/100/200; default 50), Show % toggle; rows of spot targets (22750 (-1.4%) … ) × columns On Target Date / On Expiry; current-spot row highlighted with editable dropdown.
- WHAT-IF controls under chart: "NIFTY Target" % + stepper input + slider + Reset; "Date: 5D to expiry" slider 24 Sep→29 Sep + ‹ › day-steppers + timestamp "Thu, 24 Sep 3:40 PM" + Reset. Tested target 23300 (+1.0%) and date → Mon 28 Sep (1D to expiry): badge flips to "Projected profit: 5,155 (+14%)"; BE (Target) recalculated 23123.
- P&L Table: toggles Multiply by Lot Size / by No. of Lots; cols Instrument, Target P&L, Target Price, Entry Price, LTP; Total row labelled "Projected".
- Greeks tab: per-leg Delta/Theta/Decay/Gamma/Vega + Total (per-unit unless multiply toggles on).
- Strategy Chart: historical intraday line of combined Strategy Price vs NIFTY FUT (dual axis) over ~7 sessions; "Invert Price" toggle.
Below: Strikewise IVs (Offset stepper for all IVs, per-strike IV stepper, Chg vs live, Reset IVs) — IV what-if. Greeks panel (portfolio totals, multiply toggles): Delta 16, Theta -109, Decay -169, Gamma 0.006, Vega 72. Target Day Futures Prices (29 Sep FUT 23095). Standard Deviation table 1SD 374.9 (1.6%) → 22688/23438; 2SD 749.8 (3.3%).
Floating refresh FAB bottom-right.

## S3 Add/Edit — option-chain drawer (left overlay)
- Header: instrument search + chart + Info, close X; "Show Editor »" vertical tab to peek builder.
- Mode tabs: Straddles | Strangles | Strikes | Futures + Settings (Lots vs Qty).
- Expiry dropdown; view toggle LTP | OI | Greeks.
  - LTP: Delta, Call LTP, Call OI bar, Strike, IV, Put OI bar, Put LTP, Delta; ITM shaded yellow; ATM strike highlighted; hover row -> B / S buttons each side; after pick: Qty dropdown (65) under it.
  - OI: Call OI(Chg%) bars | Strike | Put OI(Chg%).
  - Greeks: Call Delta, Strike, IV, Put Delta, Theta, Vega, Gamma.
  - Straddles mode: a radio per strike; picking one adds S CE + S PE at that strike (defaults SHORT) and payoff updates live behind drawer.
  - Futures: expiries (29 Sep, 27 Oct, 23 Nov) B/S + "Synthetic Futures" for weekly expiries (06 Oct 23111.43 …).
- Switching mode with legs present → modal "You are changing strategy type from Futures to Straddles." Cancel/Proceed (clears selection).
- Footer: "N legs selected" / "1 straddle selected" · Clear All · Done (disabled when 0).
## S4 Insights drawer = "Your warnings": chips per warning (e.g. "Decay") + Hide.
## S5 Charges modal: Total ₹72.94; Brokerage 40, STT 20.10, Exchange txn 4.76, GST 8.06, Stamp 0, SEBI 0.01; Disclaimer accordion; Close.
## S6 Builder Settings popover: Lots | Qty radio; Show Manual P&L toggle.
## S7 Save flow: "…">Save → modal "Save New Strategy", Strategy Name (20-char limit, counter), Cancel/Save Changes → toast "Strategy saved successfully"; URL gains &builder_strategy_id=<uuid>; Saved Strategies tab auto-opens: search "name underlying etc.", list rows (name, "NIFTY26SEP23200CE and 1 more", hover pencil + trash; ✓ = loaded). After save: Save disabled until dirty; Save As enabled.
## S8 Trade All → "Basket Order" floating window (min/popout/close)
- Broker account header (name, Client ID DC2940 – Zerodha), Settings; Intraday | Overnight (product); Prices refresh; Charges & Margin.
- One card per leg: SELL NIFTY 28th Sep 23200 CE (NB: builder said 29 Sep → date label inconsistency), 5-level depth, OHLC/LTP/Avg, Market|Limit, Price (refresh), Lots, per-leg Sell button.
- Footer: "Always open Basket in new tab" checkbox; Margin Needed 1,97,221 vs Available 1,89,594 (red = shortfall); "Changing price or qty auto-updates margin"; Re-arrange (order legs e.g. buys first); "Place All at Market ▾". NOT clicked.
## S9 Add to Drafts (virtual trade)
- Modal "Add to draft portfolio": radio list of existing portfolios + "Create new" → Portfolio Name (40) + Strategy Name (40) → Create → review "The following trades will be added" (legs, price, qty) + "This is not a real trade…" → Confirm → success "Trades added to draft portfolio" → Load in Builder | Open Full Draft Portfolio.
## S10 Draft Portfolio page (/draft-portfolios/<uuid>) — "Drafts Mode" dashed banner
- Left: "1 of 1 Strategy", Total/Unbooked/Booked P&L, Total Decay +1,165, toggle Show closed positions, gear, strategy checklist (select to aggregate), "Like this feature? Give us feedback".
- Main: breadcrumb Portfolios > [portfolio ▾]; "+ Create New Strategy"; strategy header (name, rename, delete, positions count, Total/Unbooked/Booked P&L, Notes, collapse).
- Tabs Net Positions | Orderbook. Net: underlying + BE/MaxP/MaxL line, Show All, expiry filter chip; table Instrument, Qty, Avg, LTP, Total P&L, Unbooked, Booked (checkbox rows). Orderbook: "Showing 2/2 orders based on your selection", See Selection ▾, Tip/How does this work; cols Instrument, Qty, Price, Exit Price, LTP, P&L.
- Action rail: Open in Builder (↗) · Add Orders ▾ (Add Orders Manually / Import Zerodha Trades) · Exit Orders (n) ▾ (Exit Orders Manually / Exit using Zerodha Trades) · Edit (n) · Delete (n) · Convert To Real Trades · Greeks ▾ (Delta 8.57, Gamma -0.16, Vega -1365, Decay 1165). Orderbook rail: Open in Builder, Add Orders, Import Orders, Edit, Greeks.
- Exit Draft Trade Orders modal: rows Type(Buy), Instrument, Contract, Expiry, Strike, Qty, Exit Price (editable + refresh to LTP), Time (8:25 AM) → Cancel / Exit All (2).

## S11 Draft Portfolios list (/draft-portfolios)
- Left: "5 of 64 Portfolios" aggregate Total/Unbooked/Booked P&L, "Total no. of Portfolios: 64".
- Header: breadcrumb Portfolios; list/grid view toggle; "Delete portfolios(5)" (!! 5 user portfolios PRE-TICKED by default = aggregate selection doubles as delete selection — dangerous UX); "+ Create New".
- Sort by: Recently Updated ▾. Table: checkbox, Portfolio Name, Total P&L, Unbooked P&L, Booked P&L, "N Strategy/Strategies" ›.
- Notes modal (per strategy): textarea "Add Note" + "Add New Note"; right pane list/empty "you haven't added any notes yet" = trade journal.

## S12 Strategy Wizard (/trade-options-strategies?tradingsymbol=NIFTY) — view-based discovery
- Inputs row: Stock Name (search) · Prediction (Above / Between / Below) · NIFTY Target (number; Between → two) · Target Date (29 Sep Expiry (4 Days) / 27 Oct Expiry (32 Days) / Specific date) · Filters · Go (disabled until prediction chosen).
- Empty: illustration + "Tell us where a stock is going and we will give you smart option strategies to trade."
- Pre-market: "Strategy wizard is offline till next market open (9:15 AM). Please come back…" (offline state).
- Filters panel: Premium (Get/Pay) · Expiry checklist (+ "Expiry Missing?" warning) · Hedged strategies (Buy Call, Buy Put, Call Spread, Put Spread, Iron Condor, Iron Butterfly — each with (count)) · Unhedged (Sell Call, Sell Put, Straddle, Strangle) · ATM IV on target per expiry (stepper: IV what-if baked into ranking) · Spread gap · Max loss limit (slider, 1000000) · Min profit (slider) · Delta range (0–1 dual slider).

## S13 Easy Options (/trade-options) — beginner discovery
- "Simple, low-risk, and fun option trading". Per index card (NIFTY, BANKNIFTY, FINNIFTY): "Where do you think NIFTY will go in 4 days?" + chip "Expiring coming Tuesday at 3:40 PM"; explainer text of the index; 1-month price chart (BANKNIFTY "chart not found" transient empty state); UP / NEUTRAL / DOWN.
- Pick → 3 cards: Defensive (green bar) / Balanced (yellow) / Aggressive (red). Each: "Choose this if you think NIFTY will not go down, and stay above 22972 (-0.4%)", Max profit, Max loss, Quantity (1 Lot) stepper 65, Standalone funds, Add to Drafts / Trade. Neutral: ranges e.g. "stay between 22836 (-1%) and 23364 (1.3%)".
- More Details modal: risk bar + thesis; "How does this trade work?" plain-English bullets (buy 23100 call…, sell 23200 call to reduce cost…, hold till expiry or exit early); mini "Profit & Loss on Expiry" chart w/ hover; Max Profit "When NIFTY is above 23200"; Max Loss "When below 23100"; Expiry; Breakeven 23147 "move by +0.4%"; Quantity; Standalone Margin 33,014 "Refundable" tag; Add to Drafts / Trade.

## S14 Expiry Trades (/expiry-trades)
- "Where will FINNIFTY close on Expiry?" Bullish/Bearish/Neutral toggle; left list of underlyings with expiry weekday & availability ("available in 0 hours", "available from 26 Oct 8:45PM").
- Empty state: "No strategies available to trade currently. Please check back by 24 Sep 2026, 8:45 pm" (stale timestamp in the past = bug).

## S15 Positions (/positions)
- Positions | Groups toggle; Total/Booked/Unbooked P&L, Total Decay; Charges; Share Verified P&L; gear; Closed positions toggle; View Funds. Empty: "You do not have any open F&O positions." / "No positions to show". Groups empty: "No groups yet — Keep a track on your strategies by grouping different positions together" with illustration of row menu (Open in Builder, Exit Positions, Add to Group, Add to Drafts, Shadow…).
- Avatar menu: Settings, Verified by Sensibull, FAQ, Help/Support, About, What's New, Logout. NO alerts / strategy-monitoring feature found anywhere in nav.
- Analyse ▾: Option Chain, Open Interest (New & Improved), Multi Strike OI, FII DII, Live Options Charts, Multi Straddle-Strangle Charts | Stock Data, Screener, Technical Signals, Market Analysis | IV Chart, Stock Market Calendar, Heatmap.
## S16 Screener (/options-screener) — stock-level F&O discovery
- Left filters: Stock, Sector, Expiry (+Expiry missing?), Liquid only, Volume spike, Upcoming events, OI action (Bullish: Long buildup, Short cover; Bearish: Long unwind, Short buildup), IV range, IVP range, PCR range, Future % change, OI % change.
- Table|Heat Map; cols Stock, Fut Price, ATM IV, IV Chg, IVP, Result (date/TBA), Volume, OI % Chg, PCR, Max Pain; row hover quick-actions (chart, option chain, OI, watchlist, strategy?); pagination 20 rows; Download as Excel; "Caution: Do not trade without reading this completely".
