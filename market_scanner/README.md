# KANIDA local pattern scanner

A local, read-only market scanner backed by `../db/kanida.db`. It evaluates 10 enabled geometric pattern types independently across 1H, 4H, 1D and 1W. Results and chart candles are persisted separately under `output/scanner.sqlite3`; the source database is opened with SQLite `mode=ro` and `query_only=ON`.

## Start

From the workspace root, run `./market_scanner/start.ps1`, or:

```powershell
& 'C:\Users\SPS\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' -m market_scanner.server
```

Open http://127.0.0.1:8765. The first full scan runs automatically. Cached results survive restarts. The server must remain running for automatic scanning. `start.ps1` launches it hidden with logs and reuses an existing KANIDA instance. Use `stop.ps1` to stop only the recorded KANIDA server.

The scanner and web server require Python 3.11+ and NumPy. The bundled runtime already includes NumPy. An alternative runtime can use `python -m pip install -r market_scanner/requirements.txt`.

## Product behavior

- Ranked matches with pattern, stock, timeframe, status, and freshness filters.
- Pattern × timeframe matrix with all qualifying counts and click-through.
- Stock explorer with independent results and charts for all four timeframes.
- Candlestick and volume charts with real detected lines, pivots, labels, zoom, pan and OHLC hover.
- Data coverage and next scheduled closes; insufficient history is distinct from no pattern.
- Rule definitions and source limitations available directly in the interface.
- Stock-specific historical behavior, independently mined rules, held-out tests,
  win rates, expectancy, MFE/MAE and reproducible historical trade charts.

See [BACKTESTING.md](BACKTESTING.md) for the research method, costs, sample thresholds,
data limits and restart instructions. Open **Pattern backtests** in the local app.
The batch covers the full history available for every active database stock.
The worker uses the isolated `.venv` with `requirements-backtest.txt`; the web
server can continue using the bundled Python runtime.

See RULES.md for each gate and config.json for the enabled detector registry, source path, calendar and timing. No profit accuracy claim is made: this first detector version needs a trader-labeled validation corpus before claiming reliable market-wide precision.

## Timing and freshness

All time logic uses IST. The finalization delay is 120 seconds. Intraday bars are built from complete 5-minute bars with no session crossing. 4H uses 09:15–13:15 and a 13:15–15:30 closing stub; 1H also includes the 15-minute closing stub. Weekly bars require all expected sessions and finish on the last scheduled session, including Friday holidays.

The supplied database has 1,431 active equity metadata records, with the newest source bars on 31 July 2026, many large stocks on 29 July, and several missing entirely. Current-only results are empty until fresh data arrives. The scanner watches the local database; it does not download data or infer unavailable exchange listings. Small corporate-action discontinuities and unverified historical special-session timings remain data limitations.

2026 calendar sources: [NSE holiday circular](https://nsearchives.nseindia.com/content/circulars/CMTR71775.pdf), [Budget session](https://nsearchives.nseindia.com/content/circulars/CMTR72349.pdf), [January 15 holiday amendment](https://nsearchives.nseindia.com/content/circulars/CMTR72260.pdf). Pattern background: [StockCharts ChartSchool](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/chart-patterns). Numerical thresholds are KANIDA implementation choices, not claimed universal standards.

## APIs and validation

GET `/api/state`, `/api/matches`, `/api/stocks?q=`, `/api/stock?symbol=TITAN`, `/api/chart?symbol=TITAN&timeframe=4H`, `/api/coverage`, `/api/rules`.

GET `/api/matches` supports `pattern`, `timeframe`, `symbol`, `state`, and `current=true`. POST `/api/scan` starts a scan if one is not already running. The server binds only to 127.0.0.1 and rejects cross-origin scan requests. This is a local development service, not an Internet-facing deployment.

## Sector, universe and historical-performance filters

Scanner, stock explorer and backtests include sector and stock-universe filters.
Nifty 50/100/200/500 and F&O use the existing read-only `instrument_labels` flags.
The supplied July 29, 2026 label snapshot contains 48/84/145/501 index members
respectively and 208 F&O equities. These are incomplete database subsets, not
complete current constituent lists. 930 stocks have no sector or dated membership
metadata and remain explicitly unclassified. Large/mid/small-cap classification
is unavailable; the UI does not infer market cap from price or index membership.

The main performance control is **Past average return per trade**: All returns,
0–0.5%, 0.5–1%, 1–2%, 2–5%, 5–10%, and Above 10%. It uses the exact stock, pattern,
timeframe and side, averaging all winning and losing trades after saved costs.
The independent **Minimum historical trades** input defaults to **5**, and applies
even with All returns selected. Set it to **0** to include any history size,
including setups with no prior trades. A return band always requires an actual
measured average; missing results are never treated as zero.

Bands classify the displayed average, rounded to two decimals using decimal
half-up rounding. The lower edge is included and the upper edge belongs to the
next band, except 10.00% belongs to 5–10%; Above 10% starts strictly above 10.00%.
The server and scanner cards use the same rounded classification. SQL filtering
uses the registered `kanida_display_return` function to avoid SQLite-version
differences at values such as 0.995%. Sorting retains the original precision.

The main result shows past average return, actual trade count and tested holding
duration with an explicit overnight label. Win rate, losses, excursions and rule
audits remain under View history. Later rule tests and original evidence-status
filters are available under Advanced research. Neutral scanner setups keep their
long and hypothetical short histories separate.

GET `/api/filter-options` provides classification counts and provenance.
`/api/matches` and `/api/backtests` accept `sector`, `universe`, `mode=reference|test`,
`return_band`, and `min_trades`. Band keys are `0_0.5`, `0.5_1`, `1_2`, `2_5`,
`5_10`, and `over_10`; blank means All returns. The default minimum is 5, with
0 meaning any history size. The scanner loads `/api/matches?min_trades=0` and
applies the user's independent controls locally. Listing filters apply before
pagination. The older `performance=positive|high_wr|strong` API screens remain
compatible; they are no longer main-screen controls.

These three (`performance`, `return_band`, `min_trades`) screen a match on its
**legacy backtest history**. In the **research** pattern set a detection carries
none by design, so the screen cannot run on one — it would empty the response
rather than filter it. There it is not applied and never silently: every research
body carries `history_screen` (`applied: false`, a machine-readable `reason`, the
screen that was skipped, and where the evidence actually is), and a request that
explicitly asks for the screen is refused with **400**. `min_trades=0` is the
explicit "any sample size", so it is served. Legacy mode is unchanged.
See docs/LIVE_DETECTION.md §5.

`/api/stocks` accepts `sector` and `universe`. Universe keys are `nifty50`,
`nifty100`, `nifty200`, `nifty500`, `fno`, and `unknown`. Sector and universe are
intersected. Membership is a browsing snapshot, not historical entry-date membership.

Open a backtest row to simulate ₹10,000 / ₹30,000 / ₹50,000, a custom amount, or an
automatic starting budget based only on the first eligible entry. The result shows
final cash, net profit, costs, skipped unaffordable trades, a cash curve and a full
downloadable ledger. See BACKTESTING.md for the accounting assumptions.

Filter/capital integration audit (with the server running):

```powershell
& '.\market_scanner\.venv\Scripts\python.exe' -m market_scanner.audit_filters_capital
```

The audit writes `output/filters_capital_validation.json`. It checks API filtering,
pagination, evidence scope and cash reconciliation against frozen trade fills.

```powershell
& '.\market_scanner\.venv\Scripts\python.exe' -m unittest discover -s market_scanner/tests -v
```
