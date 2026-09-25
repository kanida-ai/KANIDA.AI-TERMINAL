# TrendSpider — raw field notes (charts.trendspider.com, logged-in, 2026-09-25 ~08:45 IST, US market closed)

VERDICT: TrendSpider has NO F&O / options strategy builder (no multi-leg, payoff, Greeks-of-strategy, margin, broker basket).
Options features exist only as DATA WIDGETS, and the account has NOT accepted the exchange data agreement:
"An exchange agreement is required in order to access this data. Click here to review and accept." (not accepted — agreement acceptance needs owner OK).
US equities/options only; no NSE/BSE F&O. Per owner instruction ("if not available move to next"), studied only what's relevant to the blueprint.
See also the broader earlier study: kanida-app/docs/TRENDSPIDER_STUDY.md.

## T0 Workspace picker (on load)
- "Open a workspace of yours" list (Default Workspace, copies of Main View Day/Swing/TrendSpider Official/Dan's, Multi Symbol View, Dashboard, ML Quant Lab) + "Or create a new workspace" template gallery with thumbnails; plan limit note (10 workspaces).
## T1 Main chart workspace
- Top bar: symbol, MTFA, Fibs, Trends, Indicators, Candle Patterns, Chart Patterns, Heatmap, Other, Alerts&Bots (chart-overlay toggle), Visual Scripts, Sidekick (AI assistant), apps grid, help, notifications.
- Left drawing toolbar; right watchlist/scanner column ("Donchian Channel…" scan result "Scanned through S&P 500 Index @ 08:07pm. 38 charts found", Symbol/Last/Chg%).
- Right icon rail: watch, alerts, news, analysts, season, notes, insiders, reports, checklist, options, bots, learn, trading. Each icon → "Add <X> widget" / "Put into a new column" (modular widget docking).
- Bottom dock tabs: Strategies | Scanner | Events | Stock Maps | Options | Fundamentals | Custom Indicators | ML Lab + "Add a widget…".
## T2 Options dock
- Options Grid: expiries as chips with DTE (Oct'26 16 M 22 dte, Nov'26 20 M 57 dte, Dec'26 18 M 85, Jan'27 15 M 113, Mar'27 19 M 176, Dec'27 17 M 449; "M"=monthly); "WST contracts expiring at 16 Oct'26, 22 days to expiration"; CALLS (OI, Last, Bid, Ask) | STRIKE | PUTS (Ask, Bid, Last, OI) — all "no data" due to agreement gate. "WST: Customize…" header, kebab menu.
- "Add a options data widget": 2 templates — Options Chain ("full detailed view for a given expiration, multiple metrics at a time") and Options Map ("heatmap visualizing one metric across all strikes at all expirations").
- Right-rail "options" = Unusual Options widget (options flow).
## T3 Strategy Tester (Strategies dock) — the relevant reference for KANIDA backtest UX
- Library: search, filters all/yours/built-in/subscr/store; items tagged #sample/#subscription; "NEW STRATEGY".
- Modes: "Test a strategy on a single symbol" | "across a list of symbols" | "Test multiple strategies on a single symbol".
- AI entry: "Describe your strategy and AI will create and configure it" (+ mic) · "skip to point&click editor" · "examples".
- Editor: name, timeframe (Daily), history depth (7000 candles), gear; Entry Conditions / Exit Conditions blocks: "All of the following ▾ happened ▾", condition chips e.g. EMA(8,0,close)(last) Crossed Up EMA(21,0,close); add a condition; named signals ("Delta"/"Oscar"). Banner: pre-made strategy — "create your own copy to save". Buttons: Explain (AI) · More… · Run · Save.
- Results after Run: entries/exits annotated on chart ("Entry, long 355.73", "Exit +36.29% (Oscar)").
  - Price behavior explorer: post-entry path fan — mean/median change %, random control (mean), # winning/losing positions, min/max for winners/losers, 96% bands (winners/losers, pre-entry), raw data. "153 positions analyzed across 27.3 years of WST data (6865 candles). Mean trade return: 2.21%".
  - Performance chart: strategy equity vs asset (1129.3% vs 3979.9%) with toggles Equity, Equity(asset), Positions, Drawdown, Drawdown(asset), Sharpe, Sortino (+asset), Rvol, Rvol(asset), Correlation; range brush.
  - Tabular data: Market, Trade cost (0% default!), Net Perf all, Asset Perf, Beta, Positions 153, Wins 36%, Losses 64%, Max DD -53.7%, Avg Win 12.59%, Avg Loss -3.61%, Avg Return 2.21%, Rew/Risk 3.49, Expectancy 0.6. Win/loss histogram.
## T4 Alerts & Bots
- Right-rail alerts/bots → docking widgets (Add Alerts widget / new column). Alerts&Bots top button toggles alert overlays on chart. Bots = automated alert→action (webhook/broker) (from prior study).
