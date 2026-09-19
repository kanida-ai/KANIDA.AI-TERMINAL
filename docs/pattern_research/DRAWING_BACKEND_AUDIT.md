# Drawing backend audit — 17 September 2026

## Scope and evidence

Frozen run `4b33a5249562631524d6`; examples in `drawing_audit_examples.json` cover 107 pattern IDs and 262 directional variants. This is an audit of presentation from stored events, not a new validation of trading performance or detector quality.

All **61 candlestick IDs / 165 saved directional variants** have the exact configured recognition span: `detected_index - formation_start_index + 1`. Every saved candle rectangle matches that span. Confirmation bars are separate. Detector formation widths are explicit in `market_scanner/pattern_research/candlesticks.py:TABLE`; TA-Lib averaging lookback is not used as the rectangle width.

Specific visually ambiguous examples:

| Pattern | Rebased marked candles | Later confirmation | Meaning |
|---|---|---|---|
| CDL3BLACKCROWS | 15–18, four candles | none in selected example | Preceding white candle plus three black crows |
| CDL3LINESTRIKE | 15–18, four candles | 19 | Three directional candles plus strike candle |
| CDLADVANCEBLOCK | 15–17, three candles | 20 | Three advancing candles; intervening confirmation wait excluded |

Three Black Crows explicitly tests the preceding white candle in the [TA-Lib canonical implementation](https://github.com/TA-Lib/ta-lib/blob/main/src/ta_func/ta_CDL3BLACKCROWS.c). Its four-bar span is deliberate. Hammer, Hanging Man, Inverted Hammer and Shooting Star likewise include one defining prior candle in the stored contract. Their drawings now label that prior context. The pattern high/low reference lines may extend toward confirmation; this does not increase the marked candle rectangle.

## Corrected presentation defects

- Named high/low extrema and mirrored chart geometry replace generic close-price connections for the expanded chart patterns. Rounding/cup outlines are explicitly structure guides because frozen events do not publish fitted-curve coefficients.
- Fitted boundaries begin at the first named supporting pivot, not automatically at formation start. CH11's AADHARHFC example previously projected support backward from its first trough at 426.45 to approximately 395.96 before any supporting low. Fits can encode mean-x as their origin, so their origin is not used to guess the first anchor.
- PA01–PA08 mark the actual recognition candles and separate later confirmation. PA05 labels the comparison window and narrowest bar separately. PA07/PA08 show full-range versus opening gaps only when observed candle prices agree with the frozen gap size.
- Cached refresh uses the exact stored source-window offset and the recognition timestamp. It preserves CH01–CH10 overlays and all detection identities, never invokes detectors, and fails closed when required coordinates are unavailable.
- Cached confirmation trigger recovery now uses each family's schema: candle/chart `confirm_level`, price action `confirmation_level`, harmonic `confirmation.level`.

## Limits and unresolved concerns

- Faithfully drawing stored geometry does not certify that a detector's formation is economically meaningful. CH17 AADHARHFC 4H 2025-03-21 has peaks around 440/449/449 but valleys around 424 and 345. The drawing reflects those stored extrema; the unusually unequal valleys warrant a separate detector-definition review. No detector or historical event was changed here.
- The server API test fixtures were updated by the integration owner to use its current boot wrapper. The complete `test_pattern_drawing_accuracy.py` + `test_pattern_live.py` run now passes **89 tests**, with no exclusions.
- Gallery refresh rebuilt 249 expanded overlays directly from saved causal windows and preserved 13 original legacy overlays exactly; all 262 examples remain drawable with no coordinate-validation failures. No detector, legacy replay or event-selection rerun was needed.
- Final scanner reload with `--reuse-snapshot` completed at approximately 19:45:17. `/health` returns success. Live TITAN 1D, ATGL 1W and BPCL 1W responses all serve the updated `geometry-guides-2` overlays with valid indices; ATGL Three Black Crows explicitly labels preceding context, and TITAN/BPCL PA08 label verified opening gaps. State remains `research` / `market15`, with 107 IDs, 262 variants, evidence `identity_match` to run `4b33a5249562631524d6`, and no rescan running. `/api/health` is not a defined endpoint.
