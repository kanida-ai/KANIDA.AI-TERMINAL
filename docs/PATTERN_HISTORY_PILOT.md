# Pattern history pilot

## Delivered scope

Open `http://127.0.0.1:8082/pattern-history`, or choose **Pattern history pilot** in Discover. Eligible daily long Double Bottom and Bullish Engulfing selections use the compact history card automatically.

- Double Bottom (`CH16`, canonical).
- Bullish Engulfing (`CDLENGULFING`, canonical and canonical_context).
- Daily candles, long direction; setup and confirmed events are separate histories.
- Fixed horizons: 1, 3, 5, 10 candles.
- Stock-specific occurrence count, first/last occurrence, measured/higher/lower/unchanged counts, median closing change, median largest rise/fall and timing.
- Same-stock baseline over daily signal dates from the first through last occurrence, using the same reference, horizons and quality rules.
- Latest five completed occurrences at the selected horizon. Up to five pending and five quality-excluded cases are listed separately, with their full counts.
- Exact historical chart with frozen geometry, detection marker, next-open reference, future window and price extrema. Outcomes can be hidden and candles inspected individually.

This is descriptive historical evidence. It does not select a trading rule, train a detector, simulate executed orders, or report net trading returns. The existing strategy backtest/walk-forward engine remains available for other strategies.

## Measurement contract

The signal is the stored event's signal index: setup recognition for setup history, confirmation for confirmed history. The reference is the next daily candle's open. Horizon 1 ends at that candle's close; horizon 10 ends at reference index + 9.

Closing change = `(last close / reference open - 1) × 100`.

Largest rise = `max(0, maximum high / reference open - 1) × 100`.

Largest fall is stored as the nonnegative magnitude `max(0, 1 - minimum low / reference open) × 100`; the UI displays it with a minus sign. Both extrema include the reference candle. Timing counts that candle as 1; 0 denotes no excursion. Ties use the first extremum.

An incomplete future window is pending. Invalid OHLC or a quality gap affecting the signal/reference/measurement window excludes that horizon. Neither status becomes a zero return. Every aggregate shows the denominator for its horizon. Completed rows can differ between horizons.

Occurrences may overlap; counts are not independent trials. The baseline includes pattern dates, is not matched on market conditions, and does not prove predictive skill. Largest rise/fall are not captured profits. No costs are deducted. Windows are fixed, and the app does not promote a historical maximum as the optimal future holding period.

## Data and storage

Pinned research run: `4b33a5249562631524d6`, clean source `m15clean-20260916`. Daily data cutoff is shown per stock (currently September 15, 2026). The app's live market status is separate from this historical snapshot.

The builder reads the run's frozen stock event artifacts and daily histories, validates source checksums, and writes only `kanida-app/var/pattern_history.sqlite3`. It executes no detectors and changes no market/research databases. The cache contains these two patterns' measurements plus compressed daily candles for replay. This separates serving data from the large raw OHLCV and research stores.

Initial build completed for all **495 stocks**, with **2,970 separate stock/variant/state cells**, in **183,386,112 bytes** (about 183 MB). SQLite integrity and universe checks passed. Every horizon's measured + pending + excluded count reconciles to its occurrence total, and higher + lower + unchanged reconciles to measured count. State and variant counts overlap conceptually and must not be summed as unique formations.

Serving is read-only. Missing cached stocks are reported as unavailable, never as zero occurrences. Source manifest/version/definition mismatches and withheld publication status block serving. Unreviewed source history keeps its review label. Linked scanner selections also verify the exact symbol, pattern, variant, side, timeframe, detector identity and setup/confirmed state.

## Build / resume

From the repository root:

```powershell
& market_scanner/.venv/Scripts/python.exe kanida-app/scripts/build_pattern_history_pilot.py --workers 4
```

The build is resumable per verified stock. Optional `--symbols TITAN BHEL` creates a bounded pilot subset; `--cache` selects a separate output file. `PILOT_PATTERN_HISTORY_PATH` configures serving. A changed source or calculation version needs a new cache path; never mix versions. This pilot is a frozen historical snapshot, not an automatic vendor-ingestion pipeline.

## Main files

- Backend: `kanida-app/server/kanida_pilot/pattern_history.py`.
- Member routes and live identity checks: `kanida-app/server/kanida_pilot/app.py`, `/api/pattern-history/{catalogue,stocks,history,replay}`.
- Frontend: `kanida-app/src/patternHistory/` and `kanida-app/app/pattern-history.tsx`.
- Discover integration: `src/discover/EvidenceCard.tsx` and `src/discover/index.tsx`.
- Tests: `server/tests/test_pattern_history.py` and `test_pattern_history_routes.py`.

## Validation

TypeScript checks and Expo web export passed. The backend integration/regression run passed 101 tests across pilot history, member routes, research cards and live detections. Additional builder tests verify selective extraction against the generic decoder, escaped/chunked JSON, and source immutability. Real TITAN movements were independently recomputed from the frozen daily source. Browser checks covered both pattern families, setup/confirmed separation, horizon switching and historical replay.

The final focused builder suite passed 12 tests. The navigation regression check is `node scripts/check-pattern-history.cjs` from `kanida-app`: page links use `phase` because `state` is reserved by Expo navigation; API queries still use `state`.

Keep rollout limited to these two patterns until the user has reviewed the pilot. Expanding the catalogue should reuse this measurement contract with each additional detector's exact event/geometry/state semantics.

## Drawing review alongside the pilot

The authenticated `/pattern-drawing-review` page contains frozen real examples for all 107 pattern IDs and 262 registered direction/variant combinations. One representative per pattern was visually inspected; the separate `docs/pattern_research/DRAWING_VISUAL_REVIEW.md` report records findings and targeted post-fix rechecks. This is not certification of every occurrence, variant or timeframe.

Original CH01–CH10 overlays are preserved. Expanded overlays use the stored pivots and defining candles, separate recognition from confirmation, and preserve the exact occurrence when navigating. CH17 retains an explicit sample-level detector-quality finding. CH19–CH21 use labelled straight structure guides because these frozen events did not save a fitted curve.
