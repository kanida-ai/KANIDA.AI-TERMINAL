# Expanded chart drawing re-audit

All 32 expanded chart directional variants CH11-CH28; one exact frozen occurrence per variant, not every historic occurrence/timeframe.

All 32 examples were visually inspected through actual browser screenshots and checked against their frozen events and OHLCV. First 10 legacy patterns are owned by a separate review. All 32 exact examples were rechecked in final renderer bundle 2c4647 after the combined drawing changes. The CH26 theoretical target caption was also verified on both directional cards in final bundle 5a1b536.

| Pattern / variant / side | Exact example | State | Finding / observation |
|---|---|---|---|
| CH11 / downside_resolution / short | AADHARHFC 1H 2025-06-10 14:15:00 | confirmed | Both directional cases use the same ascending-triangle geometry; supporting line begins at its first real trough. Short outcome is explicitly downside resolution. |
| CH11 / upside / long | AADHARHFC 1H 2025-12-15 09:15:00 | confirmed | Both directional cases use the same ascending-triangle geometry; supporting line begins at its first real trough. Short outcome is explicitly downside resolution. |
| CH12 / support_breakdown / short | AADHARHFC 1H 2024-10-22 15:15:00 | confirmed | Support tests and base high match named lows and the frozen base range. No zigzag fabricated for a horizontal breakdown. |
| CH13 / downside / short | AADHARHFC 1H 2024-12-09 10:15:00 | confirmed | Both rectangles retain actual upper/lower touches; the large daily range in upside case is preserved, not narrowed for appearance. |
| CH13 / upside / long | AADHARHFC 1D 2026-05-05 09:15:00 | confirmed | Both rectangles retain actual upper/lower touches; the large daily range in upside case is preserved, not narrowed for appearance. |
| CH14 / bear / short | ABSLAMC 1H 2024-06-21 14:15:00 | confirmed | Pole and converging pivot boundaries match mirrored OHLC extremes. Bull example is setup only. Pole high/low wording corrected. |
| CH14 / bull / long | ICICIBANK 1H 2018-05-10 15:15:00 | setup | Pole and converging pivot boundaries match mirrored OHLC extremes. Bull example is setup only. Pole high/low wording corrected. |
| CH15 / canonical / short | AADHARHFC 1H 2025-06-25 09:15:00 | confirmed | Double-top peaks and true intervening low are marked. Neckline is the stored low, not a fitted close-price line. |
| CH16 / canonical / long | AADHARHFC 1H 2025-07-15 10:15:00 | confirmed | Double-bottom troughs and true intervening high are marked; neckline and confirmation are consistent with stored event. |
| CH17 / canonical / short | AADHARHFC 4H 2025-03-21 13:15:00 | setup | Setup only: peaks 444.25/449.95/447.95; valleys 417.70/346.05. Frozen mapping is accurate, but unequal valley depths require a separate definition review. No bearish breakdown occurred in this shown event. |
| CH18 / canonical / long | AADHARHFC 1H 2026-06-15 09:15:00 | confirmed | Equal troughs 451.20/451.00/449.80, unequal intervening highs 486.40/476.00; higher high is the specified neckline 486.40, confirmation close 488.60. Definition does not require comparable intervening heights. |
| CH19 / canonical / long | AADHARHFC 4H 2024-09-23 09:15:00 | confirmed | Stored left rim, bowl extreme and actual signal close are connected by a clearly caveated straight structure guide. A fitted bowl curve was not saved. |
| CH20 / canonical / short | AADHARHFC 1H 2026-02-24 12:15:00 | confirmed | Mirrored rim low and dome high are correctly connected; guide is explicitly not the detector quadratic fit. |
| CH21 / canonical / short | ABDL 1H 2024-10-25 13:15:00 | confirmed | Both low rims, high dome, high handle and failure reference are drawn from named candles. Straight guide limitation is explicit. |
| CH22 / downside / short | AADHARHFC 1H 2024-08-26 12:15:00 | setup | Both directional variants show actual alternating pivots and separately fitted diverging boundaries; downside example is setup, upside example confirmed. |
| CH22 / upside / long | AADHARHFC 1H 2024-11-28 09:15:00 | confirmed | Both directional variants show actual alternating pivots and separately fitted diverging boundaries; downside example is setup, upside example confirmed. |
| CH23 / downside / short | AADHARHFC 1H 2024-06-13 12:15:00 | confirmed | Six named anchors map faithfully, but widest high at126 and widest low at92 are separated by 34 bars. This strongly skewed shape is allowed by stored predicate and merits morphology review. Upside is neutral setup; downside confirmed on same date. |
| CH23 / upside / long | AADHARHFC 1H 2024-06-13 12:15:00 | setup | Six named anchors map faithfully, but widest high at126 and widest low at92 are separated by 34 bars. This strongly skewed shape is allowed by stored predicate and merits morphology review. Upside is neutral setup; downside confirmed on same date. |
| CH24 / bottom_multi / long | AADHARHFC 1H 2025-05-12 09:15:00 | confirmed | All four single/multi top/bottom variants show actual isolated candle group, real first and second full-range price gaps, and correct island edge. |
| CH24 / bottom_single / long | HDBFS 1D 2026-04-15 09:15:00 | confirmed | All four single/multi top/bottom variants show actual isolated candle group, real first and second full-range price gaps, and correct island edge. |
| CH24 / top_multi / short | ICICIBANK 1H 2026-04-23 09:15:00 | confirmed | All four single/multi top/bottom variants show actual isolated candle group, real first and second full-range price gaps, and correct island edge. |
| CH24 / top_single / short | IGIL 1D 2025-07-31 09:15:00 | confirmed | All four single/multi top/bottom variants show actual isolated candle group, real first and second full-range price gaps, and correct island edge. |
| CH25 / inverted_v_top / short | AADHARHFC 1H 2026-08-07 15:15:00 | confirmed | Both V orientations use the original causal pivot and actual confirming close. V-bottom subsequent wick 420.45 is below original pivot 425.35, but close-based buffered invalidation was never met. Failure reference added; no future wick substituted. |
| CH25 / v_bottom / long | AADHARHFC 1H 2024-10-21 15:15:00 | confirmed | Both V orientations use the original causal pivot and actual confirming close. V-bottom subsequent wick 420.45 is below original pivot 425.35, but close-based buffered invalidation was never met. Failure reference added; no future wick substituted. |
| CH26 / bear / short | AADHARHFC 1H 2024-08-28 10:15:00 | confirmed | Both mirrored first legs and corrections use actual extrema. Blue projection target is theoretical, not a realized second leg. Published failure reference added. |
| CH26 / bull / long | AADHARHFC 1H 2024-06-14 09:15:00 | confirmed | Both mirrored first legs and corrections use actual extrema. Blue projection target is theoretical, not a realized second leg. Published failure reference added. |
| CH27 / bottom / long | AADHARHFC 1H 2025-01-28 14:15:00 | confirmed | Both lead-in trendlines and bump extrema use correct real-price mirror. Bottom label corrected to Bump trough. Published failure reference added. |
| CH27 / top / short | AADHARHFC 1H 2024-06-19 12:15:00 | confirmed | Both lead-in trendlines and bump extrema use correct real-price mirror. Bottom label corrected to Bump trough. Published failure reference added. |
| CH28 / bearish / short | AADHARHFC 1H 2024-12-09 12:15:00 | setup | All four variants retain actual contracting swings, with short mirror arrays swapped correctly. Volume-contraction variants have recorded declining pullback means. All shown examples are setups, not confirmed breakouts; failure references added. |
| CH28 / bearish_volume_contraction / short | AADHARHFC 1H 2025-10-29 12:15:00 | setup | All four variants retain actual contracting swings, with short mirror arrays swapped correctly. Volume-contraction variants have recorded declining pullback means. All shown examples are setups, not confirmed breakouts; failure references added. |
| CH28 / bullish / long | AADHARHFC 1H 2025-06-06 10:15:00 | setup | All four variants retain actual contracting swings, with short mirror arrays swapped correctly. Volume-contraction variants have recorded declining pullback means. All shown examples are setups, not confirmed breakouts; failure references added. |
| CH28 / bullish_volume_contraction / long | AADHARHFC 1H 2025-08-04 12:15:00 | setup | All four variants retain actual contracting swings, with short mirror arrays swapped correctly. Volume-contraction variants have recorded declining pullback means. All shown examples are setups, not confirmed breakouts; failure references added. |

## Findings

- **EXPANDED_FAILURE_REFERENCE_OMITTED** (fixed_and_visually_rechecked): Published failure_level was omitted. Added Failure reference verbatim; close-failure rules include ATR buffer and are not stop executions.
- **MIRRORED_EXTREME_WORDING** (fixed_and_visually_rechecked): Pole top now Pole high/low; mirrored bump bottom now Bump trough.
- **CH17_UNEQUAL_VALLEYS** (unresolved_detector_definition_concern): Peaks 444.25/449.95/447.95, valleys 417.70/346.05; setup only with frozen neckline 346.05. Present implementation has no valley-comparability rule. Presentation cannot repair classification.
- **CH23_SKEWED_DIAMOND** (unresolved_detector_definition_concern): Widest high index 126 vs widest low 92; separation 34 of formation 66 bars. Predicate accepts opposite middle extrema without a timing-alignment gate. Correct overlay appears strongly skewed.
- **CURVE_NOT_SAVED** (documented_representation_limit): Straight guides reproduce named extrema, not an unavailable fitted curve.
- **SETUP_STATE_VISIBILITY** (fixed_and_visually_rechecked): Gallery did not prominently show frozen setup/confirmed state; drawing complete was only an animation state but could imply completed pattern.
- **CH25_CAUSAL_PIVOT_VS_LATER_WICK** (documented_detector_rule): Long sample named pivot 425.35 is later undercut by wick 420.45; all subsequent closes stay above 425.35-0.12*4.8025 before confirmation. Do not replace the causal pivot with a future extreme.
- **PROJECTION_VS_OBSERVATION** (fixed_and_visually_rechecked): Any forward dashed extrapolation and measured-move target must be explicitly identified as projection rather than observed structure.

Validation: 36 drawing tests pass after the expanded mapping changes. No source/detector edits, training, scan or backtest rerun was performed.

## Final renderer recheck

All 32 expanded variants visually rechecked. Stored failure references, mirrored low labels, setup/confirmed states, straight-guide caveats and dashed-intersection caveats are visible. CH26 now visibly identifies its blue measured-move target as a projection, not an observed outcome; verified on both directional cards. The CH17 and CH23 detector-definition concerns remain unresolved; no aesthetic alteration was made to hide them.

Final combined suite: 149 tests passed (drawing accuracy, candlestick re-audit, harmonic/PA re-audit and live integration).

Final status: all 32 exact expanded chart examples visually rechecked; no drawing fix remains pending. CH17 and CH23 remain explicitly unresolved detector-definition concerns. The source chart detector is byte-identical to frozen run (SHA-256 f77a1fb0b7841fe7760153bb431920423d742f7665b2960163c31ec5039a12e4).

Final live verification: scanner `/health` succeeds after189.9s snapshot reload. Four sampled chart endpoints (192 total matches) serve `geometry-guides-3` with valid point indices. Hikkake trigger verified on BPCL1W long/setup324.05 and short/confirmed312.85, plus TITAN1H long/setup4878.50. Research source remains market15 with run4b33a5249562631524d6 and evidence identity_match; no rescan running.
