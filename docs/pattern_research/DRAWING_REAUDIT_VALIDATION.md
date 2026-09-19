# Drawing re-audit validation

## Checks completed

- 149 backend tests passed across `test_pattern_drawing_accuracy.py`, `test_candle_drawing_reaudit.py`, `test_harmonic_pa_drawing_reaudit.py`, and `test_pattern_live.py`.
- 14 historical-outcome service and route tests passed. Drawing work did not alter the pilot research specification or cached outcomes.
- Frontend TypeScript validation passed; web export succeeded.
- `check-research-drawing.cjs`: source bounds, preserved cup fit, no invented swings, levels and candle spans passed.
- `check-drawing-labels.cjs`: 262 frozen examples at a narrow 233 px plot; no label required text fallback. This checks label-to-label collisions, not every possible label-to-line intersection.
- `check-detection-links.cjs`: exact occurrence selection and evidence guards passed.
- `check-pattern-history.cjs`: historical navigation and event identity passed.
- `check-chart-window.cjs`: eight window checks passed.
- Final merge completed: 262/262 exact-event reviews, 107/107 pattern IDs, no pending visual rechecks. Browser verification confirmed the live gallery shows these counts and both open detector concerns.
- Published frontend bundle `entry-6de63490455919c92c12ef207e0b9389.js`; drawing adapter SHA `9fac99a612ffa7fb49401136883ee5a64e3ec64467c631b1bac0cfded66d0ac4`. Review records are hidden if the artifact's adapter hash changes.
- Final scanner reload is healthy. Four live chart endpoints returned 192 matches with bounded coordinates; Hikkake inside-bar triggers were verified in live 1H and 1W responses. Frozen research identity remains unchanged.

## Scope and limitations

The four family reports record manual visual inspection separately from these automated checks. Their merge refuses missing, duplicate, changed, unreviewed or pending exact-event records. The gallery does not infer visual approval from test success.

The old `check-shapes.cjs` browser script was attempted but stopped before browser launch because its standalone TypeScript loader does not supply `require` for the current geometry module. It is not counted as passing. Current browser verification used the authenticated drawing gallery through the browser tool; frozen geometry checks use the current regression scripts above.

Two detector-definition concerns (CH17 and CH23) and straight-guide limitations (CH19–CH21) remain explicitly documented in the review. No drawing-only change is represented as proof of statistical reliability, detector validity, or performance across every historical occurrence/timeframe.
