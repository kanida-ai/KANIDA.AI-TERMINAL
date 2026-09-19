# Historical study worker

`studies.py` orchestrates owner-scoped jobs over the immutable research snapshot. `study_engine.py` contains the chronological execution/account calculations. The pilot proxies the research API after checking account access and CSRF; the research HTTP service remains bound to loopback.

## Data and computation

Completed studies retain their source run, engine version, selected instruments, settings, selected fold rules and candidate rule family. Historical drawings use the detector modules under `output/research-code/<run>`, not whichever detector is currently installed.

`output/studies/signals` caches detector occurrences. Compressed results and metadata are separate from the account database. Memory-mapped open/close mark files avoid holding full OHLC dictionaries for an entire selected universe at once. All requested pattern/timeframe combinations run independently; opening a replay does not rerun a market-wide scan.

Two worker threads process private-pilot jobs. Each account may have at most two queued/running jobs. Cancellation is cooperative. A service restart marks unfinished jobs as interrupted on retrieval; saved settings can be run again. This is a local pilot worker, not a distributed cloud queue.

## Execution assumptions

Signals become available after a completed candle; fills begin at the next candle open with configured slippage. Capital is one shared account, using whole shares, configurable allocation/position/risk limits and deterministic ordering by known pattern fit. Existing positions in a stock prevent another position in that stock. Unaffordable signals and ineligible instruments are recorded as skips.

CNC is unleveraged long cash equity. MIS is also unleveraged here and uses intraday candles with an exit at the last completed candle no later than 15:15. NRML/derivatives are unavailable without actual contract, lot, expiry and margin histories. Being in the F&O equity filter does not synthesize a futures contract.

Stop and target collisions resolve to the stop. Intrabar fills settle at candle end. Excursions at an uncertain intrabar exit are conservative bounds. Gaps after entry exit at the first available open; they are not used retrospectively to avoid taking a trade. Percentage fees are charged on entry notional and round-trip slippage is represented through both fill prices. Equity includes observed open-position P&L, and maximum decline uses all observations even when the transported curve is sampled.

## Walk-forward

For each stock × pattern × timeframe × direction, candidate rules are selected only using earlier completed, non-overlapping trades. Potential holding horizons plus a maximum-hold embargo are purged before each test window. A rule must meet the chosen sample count and positive mean-minus-standard-error score; otherwise that cell opens no new test trades.

Rules are frozen for each subsequent test window. Positions retain their original rule across a window boundary, and cash is not reset. Only subsequent test fills enter the displayed walk-forward account. These rolling tests do not create a final untouched release holdout; repeatedly selecting runs after inspecting their results consumes test independence.

The five-trade Discover filter is separate from the learning threshold (default 20, configurable down to five). Neither threshold constitutes a guarantee of statistical reliability.

## Run and verify (PowerShell)

From `C:\Users\SPS\Documents\Kanida_Falcon`:

```powershell
& '.\market_scanner\start.ps1' -ReuseSnapshot
& '.\kanida-app\scripts\start-pilot.ps1'
```

The pilot starter changes its shell's working directory. Use absolute paths if chaining subsequent operations. The research starter prefers its existing `.venv` with NumPy/Numba.

```powershell
& '.\market_scanner\.venv\Scripts\python.exe' -m unittest market_scanner.tests.test_study_engine -v
& '.\market_scanner\.venv\Scripts\python.exe' -m market_scanner.tests.smoke_studies
```

The optional smoke script creates studies under the reserved `workflow_qa` owner. It checks actual histories, cash reconciliation, frozen geometry, future-data invariance and owner isolation. It does not modify source OHLC or place orders.

Build Expo to a staging directory, copy assets into `dist-pilot`, and replace `index.html` last. Exporting directly over the served directory would temporarily remove the active web app.
