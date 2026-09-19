# Complete registered-variant drawing review

**107 pattern types · 262 exact examples visually inspected**, including all 13 directions of the first 10 chart patterns.

One actual frozen occurrence for every registered pattern variant and direction, including CH01–CH10.

Not every historical occurrence or timeframe. A source-faithful drawing does not certify detector validity or trading performance.

## Findings

| Scope | Observation | Status |
|---|---|---|
| CH01 | Removed browser replacement of the saved quadratic cup curve; rendering now uses the detector fit unchanged. | resolved_visual_recheck_complete |
| CH02, CH03 | Removed inferred extra zigzags that were not published detection anchors. | resolved_visual_recheck_complete |
| CH01, CH02, CH03, CH04, CH05, CH06, CH07, CH08, CH09, CH10 | Fit-to-pattern extent and price scale honor saved formation bounds and levels. All 13 legacy directions visually rechecked. | resolved_visual_recheck_complete |
| CH04, CH05, CH06, CH07 | Dashed extensions explicitly describe a mathematical intersection, not a price forecast. | resolved_visual_recheck_complete |
| CH25, CH26, CH27, CH28 | Published failure_level was omitted. Added Failure reference verbatim; close-failure rules include ATR buffer and are not stop executions. | fixed_and_visually_rechecked |
| CH14, CH27 | Pole top now Pole high/low; mirrored bump bottom now Bump trough. | fixed_and_visually_rechecked |
| CH17 | Peaks 444.25/449.95/447.95, valleys 417.70/346.05; setup only with frozen neckline 346.05. Present implementation has no valley-comparability rule. Presentation cannot repair classification. | unresolved_detector_definition_concern |
| CH23 | Widest high index 126 vs widest low 92; separation 34 of formation 66 bars. Predicate accepts opposite middle extrema without a timing-alignment gate. Correct overlay appears strongly skewed. | unresolved_detector_definition_concern |
| CH19, CH20, CH21 | Straight guides reproduce named extrema, not an unavailable fitted curve. | documented_representation_limit |
| CH14, CH17, CH22, CH23, CH28 | Gallery did not prominently show frozen setup/confirmed state; drawing complete was only an animation state but could imply completed pattern. | fixed_and_visually_rechecked |
| CH25 | Long sample named pivot 425.35 is later undercut by wick 420.45; all subsequent closes stay above 425.35-0.12*4.8025 before confirmation. Do not replace the causal pivot with a future extreme. | documented_detector_rule |
| CH11, CH14, CH22, CH23, CH26 | Dashed mathematical-intersection caveat visually verified on CH11/14/23. Final CH26 bearish and bullish cards visibly state: Blue: measured-move target (a projection, not an observed outcome). | fixed_and_visually_rechecked |
| CDL3INSIDE, CDL3OUTSIDE | All eight CDL3INSIDE/CDL3OUTSIDE variant/side examples intrinsically confirm on the same third candle. A single combined recognition/confirmation label preserves that meaning without adding another candle. | fixed_and_all_eight_rechecked |
| CDLHIKKAKE, CDLHIKKAKEMOD | Hikkake confirms across the second candle extreme; modified Hikkake across the third. Both use the bar immediately before recognition, not the entire pattern high/low. Earlier drawings omitted this distinct trigger and could make a valid library confirmation look inconsistent with the range. | fixed_and_all_four_rechecked |
| CDLMORNINGDOJISTAR, CDLMORNINGSTAR | On the two canonical long AADHARHFC 1H examples dated 2025-07-15, the small-grid Recognized text crosses a horizontal high/trigger line. The marker remains interpretable and its price/date mapping is correct. This is a readability limitation, not a geometry defect. | documented_legibility_limit |
| HA01, HA02, HA03, HA04, HA05, HA06, HA07, HA08, HA09, HA10 | Published confirmation.failure_level was omitted. Rendered verbatim as Failure level and checked against each terminal pivot; no detector or event changed. | resolved_all_26_variants_visually_rechecked |
| HA01, HA02, HA03, HA04, HA05, HA06, HA07, HA08, HA09, HA10 | Blue levels now explicitly described as projected potential reversal zone (PRZ), rather than leaving their meaning implicit. | resolved_all_26_variants_visually_rechecked |
| HA01, HA02, HA03, HA04, HA05, HA06, HA07, HA08, HA09, HA10 | Grid headers now distinguish Forming / setup from Confirmed; drawing animation completion does not imply confirmation. | resolved_all_26_variants_visually_rechecked |
| HA10 | AD/XA can be negative because it is a signed displacement. HA10 constraints do not use AD/XA; omitted from HA10 display only. Both final frontend variants visually rechecked, with exact geometry unchanged. | resolved_both_variants_visually_rechecked |

## Exact-event review records

| Pattern | Variant / side | Symbol / timeframe | State | Review |
|---|---|---|---|---|
| CH01 | legacy_1.0.1 / long | AADHARHFC / 1D | confirmed | inspected_source_geometry_consistent |
| CH02 | legacy_1.0.1 / long | AADHARHFC / 1H | confirmed | inspected_source_geometry_consistent |
| CH03 | legacy_1.0.1 / long | AADHARHFC / 1H | confirmed | inspected_source_geometry_consistent |
| CH03 | legacy_1.0.1 / short | ITCHOTELS / 4H | setup | inspected_source_geometry_consistent |
| CH04 | legacy_1.0.1 / long | AADHARHFC / 1W | confirmed | inspected_source_geometry_consistent |
| CH04 | legacy_1.0.1 / short | AADHARHFC / 1H | confirmed | inspected_source_geometry_consistent |
| CH05 | legacy_1.0.1 / long | AADHARHFC / 1H | confirmed | inspected_source_geometry_consistent |
| CH06 | legacy_1.0.1 / short | AADHARHFC / 1H | confirmed | inspected_source_geometry_consistent |
| CH07 | legacy_1.0.1 / long | AADHARHFC / 1H | setup | inspected_source_geometry_consistent |
| CH07 | legacy_1.0.1 / short | AADHARHFC / 1H | setup | inspected_source_geometry_consistent |
| CH08 | legacy_1.0.1 / short | AADHARHFC / 4H | confirmed | inspected_source_geometry_consistent |
| CH09 | legacy_1.0.1 / short | AADHARHFC / 4H | setup | inspected_source_geometry_consistent |
| CH10 | legacy_1.0.1 / long | AADHARHFC / 1H | setup | inspected_source_geometry_consistent |
| CH11 | downside_resolution / short | AADHARHFC / 1H | confirmed | reviewed_no_mapping_discrepancy |
| CH11 | upside / long | AADHARHFC / 1H | confirmed | reviewed_no_mapping_discrepancy |
| CH12 | support_breakdown / short | AADHARHFC / 1H | confirmed | reviewed_no_mapping_discrepancy |
| CH13 | downside / short | AADHARHFC / 1H | confirmed | reviewed_no_mapping_discrepancy |
| CH13 | upside / long | AADHARHFC / 1D | confirmed | reviewed_no_mapping_discrepancy |
| CH14 | bear / short | ABSLAMC / 1H | confirmed | reviewed_mapping_fix_visually_verified |
| CH14 | bull / long | ICICIBANK / 1H | setup | reviewed_mapping_fix_visually_verified |
| CH15 | canonical / short | AADHARHFC / 1H | confirmed | reviewed_no_mapping_discrepancy |
| CH16 | canonical / long | AADHARHFC / 1H | confirmed | reviewed_no_mapping_discrepancy |
| CH17 | canonical / short | AADHARHFC / 4H | setup | reviewed_with_detector_definition_concern |
| CH18 | canonical / long | AADHARHFC / 1H | confirmed | reviewed_no_mapping_discrepancy |
| CH19 | canonical / long | AADHARHFC / 4H | confirmed | reviewed_with_representation_limit |
| CH20 | canonical / short | AADHARHFC / 1H | confirmed | reviewed_with_representation_limit |
| CH21 | canonical / short | ABDL / 1H | confirmed | reviewed_with_representation_limit |
| CH22 | downside / short | AADHARHFC / 1H | setup | reviewed_no_mapping_discrepancy |
| CH22 | upside / long | AADHARHFC / 1H | confirmed | reviewed_no_mapping_discrepancy |
| CH23 | downside / short | AADHARHFC / 1H | confirmed | reviewed_with_detector_definition_concern |
| CH23 | upside / long | AADHARHFC / 1H | setup | reviewed_with_detector_definition_concern |
| CH24 | bottom_multi / long | AADHARHFC / 1H | confirmed | reviewed_no_mapping_discrepancy |
| CH24 | bottom_single / long | HDBFS / 1D | confirmed | reviewed_no_mapping_discrepancy |
| CH24 | top_multi / short | ICICIBANK / 1H | confirmed | reviewed_no_mapping_discrepancy |
| CH24 | top_single / short | IGIL / 1D | confirmed | reviewed_no_mapping_discrepancy |
| CH25 | inverted_v_top / short | AADHARHFC / 1H | confirmed | reviewed_mapping_fix_visually_verified |
| CH25 | v_bottom / long | AADHARHFC / 1H | confirmed | reviewed_mapping_fix_visually_verified |
| CH26 | bear / short | AADHARHFC / 1H | confirmed | reviewed_mapping_fix_visually_verified |
| CH26 | bull / long | AADHARHFC / 1H | confirmed | reviewed_mapping_fix_visually_verified |
| CH27 | bottom / long | AADHARHFC / 1H | confirmed | reviewed_mapping_fix_visually_verified |
| CH27 | top / short | AADHARHFC / 1H | confirmed | reviewed_mapping_fix_visually_verified |
| CH28 | bearish / short | AADHARHFC / 1H | setup | reviewed_mapping_fix_visually_verified |
| CH28 | bearish_volume_contraction / short | AADHARHFC / 1H | setup | reviewed_mapping_fix_visually_verified |
| CH28 | bullish / long | AADHARHFC / 1H | setup | reviewed_mapping_fix_visually_verified |
| CH28 | bullish_volume_contraction / long | AADHARHFC / 1H | setup | reviewed_mapping_fix_visually_verified |
| CDL2CROWS | canonical / short | AADHARHFC / 1D | confirmed | drawing_matches_frozen_definition |
| CDL2CROWS | canonical_context / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDL3BLACKCROWS | canonical / short | ITCHOTELS / 1D | setup | drawing_matches_frozen_definition |
| CDL3BLACKCROWS | canonical_context / short | ITCHOTELS / 1D | setup | drawing_matches_frozen_definition |
| CDL3INSIDE | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDL3INSIDE | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDL3INSIDE | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDL3INSIDE | canonical_context / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDL3LINESTRIKE | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDL3LINESTRIKE | canonical / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDL3LINESTRIKE | canonical_context / long | AADHARHFC / 4H | setup | drawing_matches_frozen_definition |
| CDL3LINESTRIKE | canonical_context / short | ABDL / 1D | setup | drawing_matches_frozen_definition |
| CDL3OUTSIDE | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDL3OUTSIDE | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDL3OUTSIDE | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDL3OUTSIDE | canonical_context / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDL3STARSINSOUTH | canonical / long | ABSLAMC / 1H | setup | drawing_matches_frozen_definition |
| CDL3STARSINSOUTH | canonical_context / long | ABSLAMC / 1H | setup | drawing_matches_frozen_definition |
| CDL3WHITESOLDIERS | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDL3WHITESOLDIERS | canonical_context / long | ABDL / 1W | confirmed | drawing_matches_frozen_definition |
| CDLABANDONEDBABY | canonical / long | GROWW / 1D | setup | drawing_matches_frozen_definition |
| CDLABANDONEDBABY | canonical / short | ICICIBANK / 1D | confirmed | drawing_matches_frozen_definition |
| CDLABANDONEDBABY | canonical_context / long | GROWW / 1D | setup | drawing_matches_frozen_definition |
| CDLABANDONEDBABY | canonical_context / short | MMTC / 1D | confirmed | drawing_matches_frozen_definition |
| CDLADVANCEBLOCK | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLADVANCEBLOCK | canonical_context / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLBELTHOLD | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLBELTHOLD | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLBELTHOLD | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLBELTHOLD | canonical_context / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLBREAKAWAY | canonical / long | RRKABEL / 1D | setup | drawing_matches_frozen_definition |
| CDLBREAKAWAY | canonical / short | ACMESOLAR / 1D | setup | drawing_matches_frozen_definition |
| CDLBREAKAWAY | canonical_context / long | RRKABEL / 1D | setup | drawing_matches_frozen_definition |
| CDLBREAKAWAY | canonical_context / short | PIRAMALFIN / 1D | setup | drawing_matches_frozen_definition |
| CDLCLOSINGMARUBOZU | break_down / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLCLOSINGMARUBOZU | break_up / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLCLOSINGMARUBOZU | color_direction / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLCLOSINGMARUBOZU | color_direction / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLCONCEALBABYSWALL | canonical / long | RRKABEL / 1H | setup | drawing_matches_frozen_definition |
| CDLCONCEALBABYSWALL | canonical_context / long | TATAPOWER / 1D | setup | drawing_matches_frozen_definition |
| CDLCOUNTERATTACK | canonical / long | ABDL / 1H | setup | drawing_matches_frozen_definition |
| CDLCOUNTERATTACK | canonical / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLCOUNTERATTACK | canonical_context / long | ABSLAMC / 1D | setup | drawing_matches_frozen_definition |
| CDLCOUNTERATTACK | canonical_context / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLDARKCLOUDCOVER | canonical / short | AADHARHFC / 1W | confirmed | drawing_matches_frozen_definition |
| CDLDARKCLOUDCOVER | canonical_context / short | AADHARHFC / 1W | confirmed | drawing_matches_frozen_definition |
| CDLDOJI | break_down / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLDOJI | break_up / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLDOJISTAR | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLDOJISTAR | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLDOJISTAR | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLDOJISTAR | canonical_context / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLDRAGONFLYDOJI | break_down / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLDRAGONFLYDOJI | break_up / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLDRAGONFLYDOJI | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLENGULFING | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLENGULFING | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLENGULFING | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLENGULFING | canonical_context / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLEVENINGDOJISTAR | canonical / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLEVENINGDOJISTAR | canonical_context / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLEVENINGSTAR | canonical / short | AADHARHFC / 1D | confirmed | drawing_matches_frozen_definition |
| CDLEVENINGSTAR | canonical_context / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLGAPSIDESIDEWHITE | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLGAPSIDESIDEWHITE | canonical / short | AADHARHFC / 4H | setup | drawing_matches_frozen_definition |
| CDLGAPSIDESIDEWHITE | canonical_context / long | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLGAPSIDESIDEWHITE | canonical_context / short | AADHARHFC / 1D | setup | drawing_matches_frozen_definition |
| CDLGRAVESTONEDOJI | break_down / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLGRAVESTONEDOJI | break_up / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLGRAVESTONEDOJI | canonical_context / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHAMMER | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHAMMER | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHANGINGMAN | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHANGINGMAN | canonical_context / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHARAMI | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHARAMI | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHARAMI | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHARAMI | canonical_context / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHARAMICROSS | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHARAMICROSS | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHARAMICROSS | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHARAMICROSS | canonical_context / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHIGHWAVE | break_down / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHIGHWAVE | break_up / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHIKKAKE | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHIKKAKE | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHIKKAKEMOD | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLHIKKAKEMOD | canonical / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLHOMINGPIGEON | canonical / long | AADHARHFC / 1D | confirmed | drawing_matches_frozen_definition |
| CDLHOMINGPIGEON | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLIDENTICAL3CROWS | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLIDENTICAL3CROWS | canonical_context / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLINNECK | canonical / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLINNECK | canonical_context / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLINVERTEDHAMMER | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLINVERTEDHAMMER | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLKICKING | canonical / long | JYOTICNC / 1H | setup | drawing_matches_frozen_definition |
| CDLKICKING | canonical / short | ICICIBANK / 1H | confirmed | drawing_matches_frozen_definition |
| CDLKICKINGBYLENGTH | canonical / long | JYOTICNC / 1H | setup | drawing_matches_frozen_definition |
| CDLKICKINGBYLENGTH | canonical / short | ICICIBANK / 1H | confirmed | drawing_matches_frozen_definition |
| CDLLADDERBOTTOM | canonical / long | AADHARHFC / 4H | confirmed | drawing_matches_frozen_definition |
| CDLLADDERBOTTOM | canonical_context / long | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLLONGLEGGEDDOJI | break_down / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLLONGLEGGEDDOJI | break_up / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLLONGLINE | break_down / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLLONGLINE | break_up / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLLONGLINE | color_direction / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLLONGLINE | color_direction / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLMARUBOZU | break_down / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLMARUBOZU | break_up / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLMARUBOZU | color_direction / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLMARUBOZU | color_direction / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLMATCHINGLOW | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLMATCHINGLOW | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLMATHOLD | canonical / long | ICICIBANK / 1D | setup | drawing_matches_frozen_definition |
| CDLMATHOLD | canonical_context / long | ICICIBANK / 1D | setup | drawing_matches_frozen_definition |
| CDLMORNINGDOJISTAR | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_with_documented_legibility_limit |
| CDLMORNINGDOJISTAR | canonical_context / long | AADHARHFC / 4H | confirmed | drawing_matches_frozen_definition |
| CDLMORNINGSTAR | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_with_documented_legibility_limit |
| CDLMORNINGSTAR | canonical_context / long | AADHARHFC / 4H | confirmed | drawing_matches_frozen_definition |
| CDLONNECK | canonical / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLONNECK | canonical_context / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLPIERCING | canonical / long | AADHARHFC / 1D | confirmed | drawing_matches_frozen_definition |
| CDLPIERCING | canonical_context / long | AADHARHFC / 1D | confirmed | drawing_matches_frozen_definition |
| CDLRICKSHAWMAN | break_down / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLRICKSHAWMAN | break_up / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLRISEFALL3METHODS | canonical / long | AADHARHFC / 4H | setup | drawing_matches_frozen_definition |
| CDLRISEFALL3METHODS | canonical / short | ITCHOTELS / 4H | setup | drawing_matches_frozen_definition |
| CDLRISEFALL3METHODS | canonical_context / long | ABDL / 1H | setup | drawing_matches_frozen_definition |
| CDLRISEFALL3METHODS | canonical_context / short | ITCHOTELS / 4H | setup | drawing_matches_frozen_definition |
| CDLSEPARATINGLINES | canonical / long | AADHARHFC / 1D | confirmed | drawing_matches_frozen_definition |
| CDLSEPARATINGLINES | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLSEPARATINGLINES | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLSEPARATINGLINES | canonical_context / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLSHOOTINGSTAR | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLSHOOTINGSTAR | canonical_context / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLSHORTLINE | break_down / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLSHORTLINE | break_up / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLSPINNINGTOP | break_down / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLSPINNINGTOP | break_up / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLSTALLEDPATTERN | canonical / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLSTALLEDPATTERN | canonical_context / short | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLSTICKSANDWICH | canonical / long | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLSTICKSANDWICH | canonical_context / long | AADHARHFC / 4H | setup | drawing_matches_frozen_definition |
| CDLTAKURI | break_down / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLTAKURI | break_up / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLTAKURI | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLTASUKIGAP | canonical / long | AADHARHFC / 4H | confirmed | drawing_matches_frozen_definition |
| CDLTASUKIGAP | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLTASUKIGAP | canonical_context / long | ABSLAMC / 4H | confirmed | drawing_matches_frozen_definition |
| CDLTASUKIGAP | canonical_context / short | ABSLAMC / 4H | setup | drawing_matches_frozen_definition |
| CDLTHRUSTING | canonical / short | AADHARHFC / 1D | confirmed | drawing_matches_frozen_definition |
| CDLTHRUSTING | canonical_context / short | AADHARHFC / 1D | confirmed | drawing_matches_frozen_definition |
| CDLTRISTAR | canonical / long | AADHARHFC / 1D | confirmed | drawing_matches_frozen_definition |
| CDLTRISTAR | canonical / short | AADHARHFC / 1D | setup | drawing_matches_frozen_definition |
| CDLTRISTAR | canonical_context / long | AADHARHFC / 1D | confirmed | drawing_matches_frozen_definition |
| CDLTRISTAR | canonical_context / short | AADHARHFC / 1D | setup | drawing_matches_frozen_definition |
| CDLUNIQUE3RIVER | canonical / long | AADHARHFC / 4H | confirmed | drawing_matches_frozen_definition |
| CDLUNIQUE3RIVER | canonical_context / long | AADHARHFC / 1H | setup | drawing_matches_frozen_definition |
| CDLUPSIDEGAP2CROWS | canonical / short | ABSLAMC / 1D | setup | drawing_matches_frozen_definition |
| CDLUPSIDEGAP2CROWS | canonical_context / short | ABSLAMC / 1D | setup | drawing_matches_frozen_definition |
| CDLXSIDEGAP3METHODS | canonical / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLXSIDEGAP3METHODS | canonical / short | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLXSIDEGAP3METHODS | canonical_context / long | AADHARHFC / 1H | confirmed | drawing_matches_frozen_definition |
| CDLXSIDEGAP3METHODS | canonical_context / short | AADHARHFC / 1D | setup | drawing_matches_frozen_definition |
| HA01 | canonical / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| HA01 | canonical / short | AADHARHFC / 1W | confirmed | visually_inspected_semantics_pass |
| HA02 | ext_1_27 / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| HA02 | ext_1_27 / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| HA02 | ext_1_618 / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| HA02 | ext_1_618 / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| HA03 | canonical / long | ABSLAMC / 1H | confirmed | visually_inspected_semantics_pass |
| HA03 | canonical / short | ABSLAMC / 1H | setup | visually_inspected_semantics_pass |
| HA04 | b_0_382 / long | TATAPOWER / 4H | setup | visually_inspected_semantics_pass |
| HA04 | b_0_382 / short | ABSLAMC / 1H | setup | visually_inspected_semantics_pass |
| HA04 | b_0_50 / long | ITCHOTELS / 1W | setup | visually_inspected_semantics_pass |
| HA04 | b_0_50 / short | ABDL / 1H | confirmed | visually_inspected_semantics_pass |
| HA05 | canonical / long | DCMSHRIRAM / 1D | confirmed | visually_inspected_semantics_pass |
| HA05 | canonical / short | BEL / 1D | confirmed | visually_inspected_semantics_pass |
| HA06 | ext_1_27 / long | ABSLAMC / 1H | setup | visually_inspected_semantics_pass |
| HA06 | ext_1_27 / short | MMTC / 1H | confirmed | visually_inspected_semantics_pass |
| HA06 | ext_1_618 / long | ABSLAMC / 1H | setup | visually_inspected_semantics_pass |
| HA06 | ext_1_618 / short | DCMSHRIRAM / 1H | confirmed | visually_inspected_semantics_pass |
| HA07 | canonical / long | DCMSHRIRAM / 1D | confirmed | visually_inspected_semantics_pass |
| HA07 | canonical / short | LTTS / 1D | setup | visually_inspected_semantics_pass |
| HA08 | canonical / long | ICICIBANK / 1H | confirmed | visually_inspected_semantics_pass |
| HA08 | canonical / short | ICICIBANK / 1D | setup | visually_inspected_semantics_pass |
| HA09 | canonical / long | ABSLAMC / 4H | setup | visually_inspected_semantics_pass |
| HA09 | canonical / short | ITCHOTELS / 4H | confirmed | visually_inspected_semantics_pass |
| HA10 | canonical / long | ABSLAMC / 1D | confirmed | visually_inspected_semantics_pass |
| HA10 | canonical / short | AADHARHFC / 1H | setup | visually_inspected_semantics_pass |
| PA01 | bottom / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA01 | top / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA02 | cluster_break_down / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA02 | cluster_break_up / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA02 | first_break_down / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA02 | first_break_up / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA02 | nested_break_down / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA02 | nested_break_up / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA03 | bearish_close / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA03 | bullish_close / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA04 | lower_shadow / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA04 | lower_shadow_context / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA04 | upper_shadow / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA04 | upper_shadow_context / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA05 | inside_nr4_break_down / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA05 | inside_nr4_break_up / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA05 | nr4_break_down / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA05 | nr4_break_up / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA05 | nr7_break_down / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA05 | nr7_break_up / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA06 | bear / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA06 | bull / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA07 | falling / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA07 | rising / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA08 | bearish / short | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |
| PA08 | bullish / long | AADHARHFC / 1H | confirmed | visually_inspected_semantics_pass |

Machine-readable records preserve each frozen event identity, event hash, semantic checks, visual observations and family report provenance.
