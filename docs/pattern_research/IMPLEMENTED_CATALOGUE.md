# Implemented core pattern catalogue

Frozen research run: `8ae6ddc251e80668239e`. **107 catalogue entries; 262 direction/variant combinations**, evaluated on 1H, 4H, 1D and 1W. Implementation is isolated research; it is not activated in the live scanner.

Exact numeric definitions, supported states and source hashes are in `IMPLEMENTED_CATALOGUE.json`. The original proposal remains in `docs/PATTERN_CATALOGUE_PROPOSAL.md`. The index below describes the actual frozen implementation.

## Scope limitations

- Gap-free adaptations of morning/evening stars, inverted hammer and shooting star are deferred; the canonical TA-Lib recognitions are included.
- Adam/Eve double-top/bottom subtypes are tags, without separate backtested variant cells.
- Narrow-range tie variants and window gap-hold/fill/retest strategies are deferred.
- Some rounding/island/resolution entries emit confirmed events only. A setup detection is not a calibrated probability.

## Registered combinations

| ID | Family | Actual variant and direction |
|---|---|---|
| CDL2CROWS | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDL3BLACKCROWS | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDL3INSIDE | candlestick | canonical: long / short (confirmed); canonical_context: long / short (confirmed) |
| CDL3LINESTRIKE | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDL3OUTSIDE | candlestick | canonical: long / short (confirmed); canonical_context: long / short (confirmed) |
| CDL3STARSINSOUTH | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDL3WHITESOLDIERS | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDLABANDONEDBABY | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDLADVANCEBLOCK | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDLBELTHOLD | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDLBREAKAWAY | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDLCLOSINGMARUBOZU | candlestick | color_direction: long / short (setup + confirmed); break_up: long (setup + confirmed); break_down: short (setup + confirmed) |
| CDLCONCEALBABYSWALL | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDLCOUNTERATTACK | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDLDARKCLOUDCOVER | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDLDOJI | candlestick | break_up: long (setup + confirmed); break_down: short (setup + confirmed) |
| CDLDOJISTAR | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDLDRAGONFLYDOJI | candlestick | canonical_context: long (setup + confirmed); break_up: long (setup + confirmed); break_down: short (setup + confirmed) |
| CDLENGULFING | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDLEVENINGDOJISTAR | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDLEVENINGSTAR | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDLGAPSIDESIDEWHITE | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDLGRAVESTONEDOJI | candlestick | canonical_context: short (setup + confirmed); break_up: long (setup + confirmed); break_down: short (setup + confirmed) |
| CDLHAMMER | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDLHANGINGMAN | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDLHARAMI | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDLHARAMICROSS | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDLHIGHWAVE | candlestick | break_up: long (setup + confirmed); break_down: short (setup + confirmed) |
| CDLHIKKAKE | candlestick | canonical: long / short (setup + confirmed) |
| CDLHIKKAKEMOD | candlestick | canonical: long / short (setup + confirmed) |
| CDLHOMINGPIGEON | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDLIDENTICAL3CROWS | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDLINNECK | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDLINVERTEDHAMMER | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDLKICKING | candlestick | canonical: long / short (setup + confirmed) |
| CDLKICKINGBYLENGTH | candlestick | canonical: long / short (setup + confirmed) |
| CDLLADDERBOTTOM | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDLLONGLEGGEDDOJI | candlestick | break_up: long (setup + confirmed); break_down: short (setup + confirmed) |
| CDLLONGLINE | candlestick | color_direction: long / short (setup + confirmed); break_up: long (setup + confirmed); break_down: short (setup + confirmed) |
| CDLMARUBOZU | candlestick | color_direction: long / short (setup + confirmed); break_up: long (setup + confirmed); break_down: short (setup + confirmed) |
| CDLMATCHINGLOW | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDLMATHOLD | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDLMORNINGDOJISTAR | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDLMORNINGSTAR | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDLONNECK | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDLPIERCING | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDLRICKSHAWMAN | candlestick | break_up: long (setup + confirmed); break_down: short (setup + confirmed) |
| CDLRISEFALL3METHODS | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDLSEPARATINGLINES | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDLSHOOTINGSTAR | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDLSHORTLINE | candlestick | break_up: long (setup + confirmed); break_down: short (setup + confirmed) |
| CDLSPINNINGTOP | candlestick | break_up: long (setup + confirmed); break_down: short (setup + confirmed) |
| CDLSTALLEDPATTERN | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDLSTICKSANDWICH | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDLTAKURI | candlestick | canonical_context: long (setup + confirmed); break_up: long (setup + confirmed); break_down: short (setup + confirmed) |
| CDLTASUKIGAP | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDLTHRUSTING | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDLTRISTAR | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CDLUNIQUE3RIVER | candlestick | canonical: long (setup + confirmed); canonical_context: long (setup + confirmed) |
| CDLUPSIDEGAP2CROWS | candlestick | canonical: short (setup + confirmed); canonical_context: short (setup + confirmed) |
| CDLXSIDEGAP3METHODS | candlestick | canonical: long / short (setup + confirmed); canonical_context: long / short (setup + confirmed) |
| CH01 | chart | legacy_1.0.1: long (setup + confirmed) |
| CH02 | chart | legacy_1.0.1: long (confirmed) |
| CH03 | chart | legacy_1.0.1: long / short (setup + confirmed) |
| CH04 | chart | legacy_1.0.1: long / short (setup + confirmed) |
| CH05 | chart | legacy_1.0.1: long (setup + confirmed) |
| CH06 | chart | legacy_1.0.1: short (setup + confirmed) |
| CH07 | chart | legacy_1.0.1: long / short (setup) |
| CH08 | chart | legacy_1.0.1: short (setup + confirmed) |
| CH09 | chart | legacy_1.0.1: short (setup + confirmed) |
| CH10 | chart | legacy_1.0.1: long (setup + confirmed) |
| CH11 | chart | upside: long (setup + confirmed); downside_resolution: short (confirmed) |
| CH12 | chart | support_breakdown: short (setup + confirmed) |
| CH13 | chart | upside: long (setup + confirmed); downside: short (setup + confirmed) |
| CH14 | chart | bull: long (setup + confirmed); bear: short (setup + confirmed) |
| CH15 | chart | canonical: short (setup + confirmed) |
| CH16 | chart | canonical: long (setup + confirmed) |
| CH17 | chart | canonical: short (setup + confirmed) |
| CH18 | chart | canonical: long (setup + confirmed) |
| CH19 | chart | canonical: long (confirmed) |
| CH20 | chart | canonical: short (confirmed) |
| CH21 | chart | canonical: short (setup + confirmed) |
| CH22 | chart | upside: long (setup + confirmed); downside: short (setup + confirmed) |
| CH23 | chart | upside: long (setup + confirmed); downside: short (setup + confirmed) |
| CH24 | chart | top_single: short (confirmed); top_multi: short (confirmed); bottom_single: long (confirmed); bottom_multi: long (confirmed) |
| CH25 | chart | v_bottom: long (setup + confirmed); inverted_v_top: short (setup + confirmed) |
| CH26 | chart | bull: long (setup + confirmed); bear: short (setup + confirmed) |
| CH27 | chart | top: short (setup + confirmed); bottom: long (setup + confirmed) |
| CH28 | chart | bullish: long (setup + confirmed); bearish: short (setup + confirmed); bullish_volume_contraction: long (setup + confirmed); bearish_volume_contraction: short (setup + confirmed) |
| HA01 | harmonic | canonical: long / short (setup + confirmed) |
| HA02 | harmonic | ext_1_27: long / short (setup + confirmed); ext_1_618: long / short (setup + confirmed) |
| HA03 | harmonic | canonical: long / short (setup + confirmed) |
| HA04 | harmonic | b_0_382: long / short (setup + confirmed); b_0_50: long / short (setup + confirmed) |
| HA05 | harmonic | canonical: long / short (setup + confirmed) |
| HA06 | harmonic | ext_1_27: long / short (setup + confirmed); ext_1_618: long / short (setup + confirmed) |
| HA07 | harmonic | canonical: long / short (setup + confirmed) |
| HA08 | harmonic | canonical: long / short (setup + confirmed) |
| HA09 | harmonic | canonical: long / short (setup + confirmed) |
| HA10 | harmonic | canonical: long / short (setup + confirmed) |
| PA01 | price_action | top: short (setup + confirmed); bottom: long (setup + confirmed) |
| PA02 | price_action | first_break_up: long (setup + confirmed); first_break_down: short (setup + confirmed); cluster_break_up: long (setup + confirmed); cluster_break_down: short (setup + confirmed); nested_break_up: long (setup + confirmed); nested_break_down: short (setup + confirmed) |
| PA03 | price_action | bullish_close: long (setup + confirmed); bearish_close: short (setup + confirmed) |
| PA04 | price_action | lower_shadow: long (setup + confirmed); upper_shadow: short (setup + confirmed); lower_shadow_context: long (setup + confirmed); upper_shadow_context: short (setup + confirmed) |
| PA05 | price_action | nr4_break_up: long (setup + confirmed); nr4_break_down: short (setup + confirmed); nr7_break_up: long (setup + confirmed); nr7_break_down: short (setup + confirmed); inside_nr4_break_up: long (setup + confirmed); inside_nr4_break_down: short (setup + confirmed) |
| PA06 | price_action | bull: long (setup + confirmed); bear: short (setup + confirmed) |
| PA07 | price_action | rising: long (setup + confirmed); falling: short (setup + confirmed) |
| PA08 | price_action | bullish: long (setup + confirmed); bearish: short (setup + confirmed) |

All original ten chart IDs use the legacy detector logic; the expanded evaluation model is versioned separately. New chart/harmonic definitions are explicit operational research rules, with assumptions recorded in the detector notes.

For execution, costs, chronological selection and input limitations, see `EVALUATION_NOTES.md`, `DETECTOR_NOTES.md` and the run manifest. No positive result is automatically approved for a predictive claim.
