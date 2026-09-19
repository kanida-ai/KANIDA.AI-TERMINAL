# Complete candlestick drawing re-audit

Status: **complete_with_explicit_limits**. Visual coverage **165/165 variants**; semantic checks pass **165/165**.

All 165 frozen candlestick variant/side examples across 61 pattern IDs, one stored event and one observed timeframe per variant. Every example receives a fresh visual review; the earlier 61-representative review is not treated as coverage.

Drawing accuracy is correspondence to the stored event, its pinned recognition span and OHLC. This review does not certify every detector classification, trend quality, profitability, every historical occurrence, or every timeframe. Canonical TA-Lib recognitions do not require trend context; canonical_context variants do.

## Findings

### INTRINSIC_CONFIRMATION_LABEL

All eight CDL3INSIDE/CDL3OUTSIDE variant/side examples intrinsically confirm on the same third candle. A single combined recognition/confirmation label preserves that meaning without adding another candle. One Recognized + confirmed marker on the third candle; no invented later confirmation bar. Status: fixed_and_all_eight_rechecked.

### HIKKAKE_TRIGGER_EXPLANATION

Hikkake confirms across the second candle extreme; modified Hikkake across the third. Both use the bar immediately before recognition, not the entire pattern high/low. Earlier drawings omitted this distinct trigger and could make a valid library confirmation look inconsistent with the range. Inside bar trigger uses the actual named bar high for long and low for short, beginning at that inside bar and ending at signal. Status: fixed_and_all_four_rechecked.

### SMALL_GRID_LABEL_ON_LEVEL

On the two canonical long AADHARHFC 1H examples dated 2025-07-15, the small-grid Recognized text crosses a horizontal high/trigger line. The marker remains interpretable and its price/date mapping is correct. This is a readability limitation, not a geometry defect. No further noncritical renderer change during this full audit; retained as an explicit limitation. Status: documented_legibility_limit.

Hikkake definitions verified against the installed TA-Lib core 0.6.4: [Hikkake source](https://raw.githubusercontent.com/TA-Lib/ta-lib/v0.6.4/src/ta_func/ta_CDLHIKKAKE.c), [modified Hikkake source](https://raw.githubusercontent.com/TA-Lib/ta-lib/v0.6.4/src/ta_func/ta_CDLHIKKAKEMOD.c).

## Exact event review ledger

| # | Pattern | Variant / side | Event | Visual page / row | Semantic checks |
|---:|---|---|---|---|---|
| 1 | CDL2CROWS | canonical / short | AADHARHFC 1D 2025-11-19 09:15:00 | 1 / 1 | pass |
| 2 | CDL2CROWS | canonical_context / short | AADHARHFC 1H 2025-04-30 11:15:00 | 1 / 1 | pass |
| 3 | CDL3BLACKCROWS | canonical / short | ITCHOTELS 1D 2025-06-19 09:15:00 | 1 / 1 | pass |
| 4 | CDL3BLACKCROWS | canonical_context / short | ITCHOTELS 1D 2025-06-19 09:15:00 | 1 / 2 | pass |
| 5 | CDL3INSIDE | canonical / long | AADHARHFC 1H 2026-03-24 10:15:00 | 1 / 2 | pass |
| 6 | CDL3INSIDE | canonical / short | AADHARHFC 1H 2026-09-15 09:15:00 | 1 / 2 | pass |
| 7 | CDL3INSIDE | canonical_context / long | AADHARHFC 1H 2026-03-24 10:15:00 | 1 / 3 | pass |
| 8 | CDL3INSIDE | canonical_context / short | AADHARHFC 1H 2026-09-02 09:15:00 | 1 / 3 | pass |
| 9 | CDL3LINESTRIKE | canonical / long | AADHARHFC 1H 2025-11-12 09:15:00 | 1 / 3 | pass |
| 10 | CDL3LINESTRIKE | canonical / short | AADHARHFC 1H 2026-06-22 15:15:00 | 2 / 1 | pass |
| 11 | CDL3LINESTRIKE | canonical_context / long | AADHARHFC 4H 2026-04-20 09:15:00 | 2 / 1 | pass |
| 12 | CDL3LINESTRIKE | canonical_context / short | ABDL 1D 2026-08-10 09:15:00 | 2 / 1 | pass |
| 13 | CDL3OUTSIDE | canonical / long | AADHARHFC 1H 2026-08-24 11:15:00 | 2 / 2 | pass |
| 14 | CDL3OUTSIDE | canonical / short | AADHARHFC 1H 2026-08-27 10:15:00 | 2 / 2 | pass |
| 15 | CDL3OUTSIDE | canonical_context / long | AADHARHFC 1H 2026-08-24 11:15:00 | 2 / 2 | pass |
| 16 | CDL3OUTSIDE | canonical_context / short | AADHARHFC 1H 2026-08-27 10:15:00 | 2 / 3 | pass |
| 17 | CDL3STARSINSOUTH | canonical / long | ABSLAMC 1H 2025-09-01 14:15:00 | 2 / 3 | pass |
| 18 | CDL3STARSINSOUTH | canonical_context / long | ABSLAMC 1H 2025-09-01 14:15:00 | 2 / 3 | pass |
| 19 | CDL3WHITESOLDIERS | canonical / long | AADHARHFC 1H 2025-04-02 14:15:00 | 3 / 1 | pass |
| 20 | CDL3WHITESOLDIERS | canonical_context / long | ABDL 1W 2026-05-04 09:15:00 | 3 / 1 | pass |
| 21 | CDLABANDONEDBABY | canonical / long | GROWW 1D 2026-03-10 09:15:00 | 3 / 1 | pass |
| 22 | CDLABANDONEDBABY | canonical / short | ICICIBANK 1D 2013-07-25 09:15:00 | 3 / 2 | pass |
| 23 | CDLABANDONEDBABY | canonical_context / long | GROWW 1D 2026-03-10 09:15:00 | 3 / 2 | pass |
| 24 | CDLABANDONEDBABY | canonical_context / short | MMTC 1D 2023-10-19 09:15:00 | 3 / 2 | pass |
| 25 | CDLADVANCEBLOCK | canonical / short | AADHARHFC 1H 2026-05-11 09:15:00 | 3 / 3 | pass |
| 26 | CDLADVANCEBLOCK | canonical_context / short | AADHARHFC 1H 2026-02-23 09:15:00 | 3 / 3 | pass |
| 27 | CDLBELTHOLD | canonical / long | AADHARHFC 1H 2026-09-01 14:15:00 | 3 / 3 | pass |
| 28 | CDLBELTHOLD | canonical / short | AADHARHFC 1H 2026-06-08 11:15:00 | 4 / 1 | pass |
| 29 | CDLBELTHOLD | canonical_context / long | AADHARHFC 1H 2026-08-28 12:15:00 | 4 / 1 | pass |
| 30 | CDLBELTHOLD | canonical_context / short | AADHARHFC 1H 2026-06-08 11:15:00 | 4 / 1 | pass |
| 31 | CDLBREAKAWAY | canonical / long | RRKABEL 1D 2024-08-16 09:15:00 | 4 / 2 | pass |
| 32 | CDLBREAKAWAY | canonical / short | ACMESOLAR 1D 2025-02-03 09:15:00 | 4 / 2 | pass |
| 33 | CDLBREAKAWAY | canonical_context / long | RRKABEL 1D 2024-08-16 09:15:00 | 4 / 2 | pass |
| 34 | CDLBREAKAWAY | canonical_context / short | PIRAMALFIN 1D 2026-09-01 09:15:00 | 4 / 3 | pass |
| 35 | CDLCLOSINGMARUBOZU | break_down / short | AADHARHFC 1H 2026-08-10 11:15:00 | 4 / 3 | pass |
| 36 | CDLCLOSINGMARUBOZU | break_up / long | AADHARHFC 1H 2026-05-13 12:15:00 | 4 / 3 | pass |
| 37 | CDLCLOSINGMARUBOZU | color_direction / long | AADHARHFC 1H 2026-03-24 10:15:00 | 5 / 1 | pass |
| 38 | CDLCLOSINGMARUBOZU | color_direction / short | AADHARHFC 1H 2026-08-10 11:15:00 | 5 / 1 | pass |
| 39 | CDLCONCEALBABYSWALL | canonical / long | RRKABEL 1H 2026-06-08 14:15:00 | 5 / 1 | pass |
| 40 | CDLCONCEALBABYSWALL | canonical_context / long | TATAPOWER 1D 2016-02-25 09:15:00 | 5 / 2 | pass |
| 41 | CDLCOUNTERATTACK | canonical / long | ABDL 1H 2026-07-23 09:15:00 | 5 / 2 | pass |
| 42 | CDLCOUNTERATTACK | canonical / short | AADHARHFC 1H 2024-05-31 09:15:00 | 5 / 2 | pass |
| 43 | CDLCOUNTERATTACK | canonical_context / long | ABSLAMC 1D 2022-10-17 09:15:00 | 5 / 3 | pass |
| 44 | CDLCOUNTERATTACK | canonical_context / short | AADHARHFC 1H 2024-05-31 09:15:00 | 5 / 3 | pass |
| 45 | CDLDARKCLOUDCOVER | canonical / short | AADHARHFC 1W 2026-07-20 09:15:00 | 5 / 3 | pass |
| 46 | CDLDARKCLOUDCOVER | canonical_context / short | AADHARHFC 1W 2026-07-20 09:15:00 | 6 / 1 | pass |
| 47 | CDLDOJI | break_down / short | AADHARHFC 1H 2026-09-15 09:15:00 | 6 / 1 | pass |
| 48 | CDLDOJI | break_up / long | AADHARHFC 1H 2026-09-04 10:15:00 | 6 / 1 | pass |
| 49 | CDLDOJISTAR | canonical / long | AADHARHFC 1H 2025-09-23 11:15:00 | 6 / 2 | pass |
| 50 | CDLDOJISTAR | canonical / short | AADHARHFC 1H 2025-04-07 09:15:00 | 6 / 2 | pass |
| 51 | CDLDOJISTAR | canonical_context / long | AADHARHFC 1H 2025-09-23 11:15:00 | 6 / 2 | pass |
| 52 | CDLDOJISTAR | canonical_context / short | AADHARHFC 1H 2025-04-07 09:15:00 | 6 / 3 | pass |
| 53 | CDLDRAGONFLYDOJI | break_down / short | AADHARHFC 1H 2026-09-15 09:15:00 | 6 / 3 | pass |
| 54 | CDLDRAGONFLYDOJI | break_up / long | AADHARHFC 1H 2026-07-09 13:15:00 | 6 / 3 | pass |
| 55 | CDLDRAGONFLYDOJI | canonical_context / long | AADHARHFC 1H 2026-07-09 13:15:00 | 7 / 1 | pass |
| 56 | CDLENGULFING | canonical / long | AADHARHFC 1H 2026-08-28 12:15:00 | 7 / 1 | pass |
| 57 | CDLENGULFING | canonical / short | AADHARHFC 1H 2026-08-17 09:15:00 | 7 / 1 | pass |
| 58 | CDLENGULFING | canonical_context / long | AADHARHFC 1H 2026-08-28 12:15:00 | 7 / 2 | pass |
| 59 | CDLENGULFING | canonical_context / short | AADHARHFC 1H 2026-06-08 11:15:00 | 7 / 2 | pass |
| 60 | CDLEVENINGDOJISTAR | canonical / short | AADHARHFC 1H 2026-03-18 09:15:00 | 7 / 2 | pass |
| 61 | CDLEVENINGDOJISTAR | canonical_context / short | AADHARHFC 1H 2026-03-18 09:15:00 | 7 / 3 | pass |
| 62 | CDLEVENINGSTAR | canonical / short | AADHARHFC 1D 2025-11-19 09:15:00 | 7 / 3 | pass |
| 63 | CDLEVENINGSTAR | canonical_context / short | AADHARHFC 1H 2026-06-29 11:15:00 | 7 / 3 | pass |
| 64 | CDLGAPSIDESIDEWHITE | canonical / long | AADHARHFC 1H 2025-03-05 11:15:00 | 8 / 1 | pass |
| 65 | CDLGAPSIDESIDEWHITE | canonical / short | AADHARHFC 4H 2025-09-08 13:15:00 | 8 / 1 | pass |
| 66 | CDLGAPSIDESIDEWHITE | canonical_context / long | AADHARHFC 1H 2026-01-05 10:15:00 | 8 / 1 | pass |
| 67 | CDLGAPSIDESIDEWHITE | canonical_context / short | AADHARHFC 1D 2025-02-04 09:15:00 | 8 / 2 | pass |
| 68 | CDLGRAVESTONEDOJI | break_down / short | AADHARHFC 1H 2026-07-20 09:15:00 | 8 / 2 | pass |
| 69 | CDLGRAVESTONEDOJI | break_up / long | AADHARHFC 1H 2026-07-10 13:15:00 | 8 / 2 | pass |
| 70 | CDLGRAVESTONEDOJI | canonical_context / short | AADHARHFC 1H 2026-07-06 11:15:00 | 8 / 3 | pass |
| 71 | CDLHAMMER | canonical / long | AADHARHFC 1H 2026-04-21 09:15:00 | 8 / 3 | pass |
| 72 | CDLHAMMER | canonical_context / long | AADHARHFC 1H 2025-04-23 13:15:00 | 8 / 3 | pass |
| 73 | CDLHANGINGMAN | canonical / short | AADHARHFC 1H 2026-09-02 09:15:00 | 9 / 1 | pass |
| 74 | CDLHANGINGMAN | canonical_context / short | AADHARHFC 1H 2026-03-10 15:15:00 | 9 / 1 | pass |
| 75 | CDLHARAMI | canonical / long | AADHARHFC 1H 2026-08-24 10:15:00 | 9 / 1 | pass |
| 76 | CDLHARAMI | canonical / short | AADHARHFC 1H 2026-07-28 15:15:00 | 9 / 2 | pass |
| 77 | CDLHARAMI | canonical_context / long | AADHARHFC 1H 2026-08-24 10:15:00 | 9 / 2 | pass |
| 78 | CDLHARAMI | canonical_context / short | AADHARHFC 1H 2026-04-13 09:15:00 | 9 / 2 | pass |
| 79 | CDLHARAMICROSS | canonical / long | AADHARHFC 1H 2024-09-20 14:15:00 | 9 / 3 | pass |
| 80 | CDLHARAMICROSS | canonical / short | AADHARHFC 1H 2026-05-12 09:15:00 | 9 / 3 | pass |
| 81 | CDLHARAMICROSS | canonical_context / long | AADHARHFC 1H 2024-08-06 09:15:00 | 9 / 3 | pass |
| 82 | CDLHARAMICROSS | canonical_context / short | AADHARHFC 1H 2026-01-14 14:15:00 | 10 / 1 | pass |
| 83 | CDLHIGHWAVE | break_down / short | AADHARHFC 1H 2026-09-03 15:15:00 | 10 / 1 | pass |
| 84 | CDLHIGHWAVE | break_up / long | AADHARHFC 1H 2026-09-04 10:15:00 | 10 / 1 | pass |
| 85 | CDLHIKKAKE | canonical / long | AADHARHFC 1H 2026-07-21 10:15:00 | 10 / 2 | pass |
| 86 | CDLHIKKAKE | canonical / short | AADHARHFC 1H 2026-08-27 09:15:00 | 10 / 2 | pass |
| 87 | CDLHIKKAKEMOD | canonical / long | AADHARHFC 1H 2025-09-10 09:15:00 | 10 / 2 | pass |
| 88 | CDLHIKKAKEMOD | canonical / short | AADHARHFC 1H 2026-06-09 09:15:00 | 10 / 3 | pass |
| 89 | CDLHOMINGPIGEON | canonical / long | AADHARHFC 1D 2024-11-06 09:15:00 | 10 / 3 | pass |
| 90 | CDLHOMINGPIGEON | canonical_context / long | AADHARHFC 1H 2026-03-24 10:15:00 | 10 / 3 | pass |
| 91 | CDLIDENTICAL3CROWS | canonical / short | AADHARHFC 1H 2025-04-23 09:15:00 | 11 / 1 | pass |
| 92 | CDLIDENTICAL3CROWS | canonical_context / short | AADHARHFC 1H 2026-06-22 14:15:00 | 11 / 1 | pass |
| 93 | CDLINNECK | canonical / short | AADHARHFC 1H 2025-12-05 09:15:00 | 11 / 1 | pass |
| 94 | CDLINNECK | canonical_context / short | AADHARHFC 1H 2025-12-05 09:15:00 | 11 / 2 | pass |
| 95 | CDLINVERTEDHAMMER | canonical / long | AADHARHFC 1H 2026-02-03 14:15:00 | 11 / 2 | pass |
| 96 | CDLINVERTEDHAMMER | canonical_context / long | AADHARHFC 1H 2026-02-03 14:15:00 | 11 / 2 | pass |
| 97 | CDLKICKING | canonical / long | JYOTICNC 1H 2025-03-18 09:15:00 | 11 / 3 | pass |
| 98 | CDLKICKING | canonical / short | ICICIBANK 1H 2016-10-26 10:15:00 | 11 / 3 | pass |
| 99 | CDLKICKINGBYLENGTH | canonical / long | JYOTICNC 1H 2025-03-18 09:15:00 | 11 / 3 | pass |
| 100 | CDLKICKINGBYLENGTH | canonical / short | ICICIBANK 1H 2016-10-26 10:15:00 | 12 / 1 | pass |
| 101 | CDLLADDERBOTTOM | canonical / long | AADHARHFC 4H 2026-03-25 13:15:00 | 12 / 1 | pass |
| 102 | CDLLADDERBOTTOM | canonical_context / long | AADHARHFC 1H 2026-07-23 15:15:00 | 12 / 1 | pass |
| 103 | CDLLONGLEGGEDDOJI | break_down / short | AADHARHFC 1H 2026-09-15 09:15:00 | 12 / 2 | pass |
| 104 | CDLLONGLEGGEDDOJI | break_up / long | AADHARHFC 1H 2026-09-04 10:15:00 | 12 / 2 | pass |
| 105 | CDLLONGLINE | break_down / short | AADHARHFC 1H 2026-09-11 09:15:00 | 12 / 2 | pass |
| 106 | CDLLONGLINE | break_up / long | AADHARHFC 1H 2026-07-30 15:15:00 | 12 / 3 | pass |
| 107 | CDLLONGLINE | color_direction / long | AADHARHFC 1H 2026-07-30 15:15:00 | 12 / 3 | pass |
| 108 | CDLLONGLINE | color_direction / short | AADHARHFC 1H 2026-09-11 09:15:00 | 12 / 3 | pass |
| 109 | CDLMARUBOZU | break_down / short | AADHARHFC 1H 2026-05-07 14:15:00 | 13 / 1 | pass |
| 110 | CDLMARUBOZU | break_up / long | AADHARHFC 1H 2026-05-13 12:15:00 | 13 / 1 | pass |
| 111 | CDLMARUBOZU | color_direction / long | AADHARHFC 1H 2026-03-16 09:15:00 | 13 / 1 | pass |
| 112 | CDLMARUBOZU | color_direction / short | AADHARHFC 1H 2026-05-07 14:15:00 | 13 / 2 | pass |
| 113 | CDLMATCHINGLOW | canonical / long | AADHARHFC 1H 2026-05-04 15:15:00 | 13 / 2 | pass |
| 114 | CDLMATCHINGLOW | canonical_context / long | AADHARHFC 1H 2025-08-07 09:15:00 | 13 / 2 | pass |
| 115 | CDLMATHOLD | canonical / long | ICICIBANK 1D 2015-10-09 09:15:00 | 13 / 3 | pass |
| 116 | CDLMATHOLD | canonical_context / long | ICICIBANK 1D 2015-10-09 09:15:00 | 13 / 3 | pass |
| 117 | CDLMORNINGDOJISTAR | canonical / long | AADHARHFC 1H 2025-07-15 09:15:00 | 13 / 3 | pass |
| 118 | CDLMORNINGDOJISTAR | canonical_context / long | AADHARHFC 4H 2026-04-06 13:15:00 | 14 / 1 | pass |
| 119 | CDLMORNINGSTAR | canonical / long | AADHARHFC 1H 2025-07-15 09:15:00 | 14 / 1 | pass |
| 120 | CDLMORNINGSTAR | canonical_context / long | AADHARHFC 4H 2026-04-06 13:15:00 | 14 / 1 | pass |
| 121 | CDLONNECK | canonical / short | AADHARHFC 1H 2025-12-05 09:15:00 | 14 / 2 | pass |
| 122 | CDLONNECK | canonical_context / short | AADHARHFC 1H 2025-12-05 09:15:00 | 14 / 2 | pass |
| 123 | CDLPIERCING | canonical / long | AADHARHFC 1D 2026-09-01 09:15:00 | 14 / 2 | pass |
| 124 | CDLPIERCING | canonical_context / long | AADHARHFC 1D 2026-09-01 09:15:00 | 14 / 3 | pass |
| 125 | CDLRICKSHAWMAN | break_down / short | AADHARHFC 1H 2026-09-15 09:15:00 | 14 / 3 | pass |
| 126 | CDLRICKSHAWMAN | break_up / long | AADHARHFC 1H 2026-09-04 10:15:00 | 14 / 3 | pass |
| 127 | CDLRISEFALL3METHODS | canonical / long | AADHARHFC 4H 2026-05-22 09:15:00 | 15 / 1 | pass |
| 128 | CDLRISEFALL3METHODS | canonical / short | ITCHOTELS 4H 2026-01-29 09:15:00 | 15 / 1 | pass |
| 129 | CDLRISEFALL3METHODS | canonical_context / long | ABDL 1H 2025-11-27 09:15:00 | 15 / 1 | pass |
| 130 | CDLRISEFALL3METHODS | canonical_context / short | ITCHOTELS 4H 2026-01-29 09:15:00 | 15 / 2 | pass |
| 131 | CDLSEPARATINGLINES | canonical / long | AADHARHFC 1D 2026-06-12 09:15:00 | 15 / 2 | pass |
| 132 | CDLSEPARATINGLINES | canonical / short | AADHARHFC 1H 2026-04-02 09:15:00 | 15 / 2 | pass |
| 133 | CDLSEPARATINGLINES | canonical_context / long | AADHARHFC 1H 2024-07-04 13:15:00 | 15 / 3 | pass |
| 134 | CDLSEPARATINGLINES | canonical_context / short | AADHARHFC 1H 2026-04-02 09:15:00 | 15 / 3 | pass |
| 135 | CDLSHOOTINGSTAR | canonical / short | AADHARHFC 1H 2025-07-03 09:15:00 | 15 / 3 | pass |
| 136 | CDLSHOOTINGSTAR | canonical_context / short | AADHARHFC 1H 2025-07-03 09:15:00 | 16 / 1 | pass |
| 137 | CDLSHORTLINE | break_down / short | AADHARHFC 1H 2026-09-15 09:15:00 | 16 / 1 | pass |
| 138 | CDLSHORTLINE | break_up / long | AADHARHFC 1H 2026-09-11 14:15:00 | 16 / 1 | pass |
| 139 | CDLSPINNINGTOP | break_down / short | AADHARHFC 1H 2026-09-15 09:15:00 | 16 / 2 | pass |
| 140 | CDLSPINNINGTOP | break_up / long | AADHARHFC 1H 2026-09-11 14:15:00 | 16 / 2 | pass |
| 141 | CDLSTALLEDPATTERN | canonical / short | AADHARHFC 1H 2026-09-08 14:15:00 | 16 / 2 | pass |
| 142 | CDLSTALLEDPATTERN | canonical_context / short | AADHARHFC 1H 2026-02-20 12:15:00 | 16 / 3 | pass |
| 143 | CDLSTICKSANDWICH | canonical / long | AADHARHFC 1H 2025-06-03 10:15:00 | 16 / 3 | pass |
| 144 | CDLSTICKSANDWICH | canonical_context / long | AADHARHFC 4H 2025-10-09 13:15:00 | 16 / 3 | pass |
| 145 | CDLTAKURI | break_down / short | AADHARHFC 1H 2026-09-03 15:15:00 | 17 / 1 | pass |
| 146 | CDLTAKURI | break_up / long | AADHARHFC 1H 2026-07-09 13:15:00 | 17 / 1 | pass |
| 147 | CDLTAKURI | canonical_context / long | AADHARHFC 1H 2026-07-09 13:15:00 | 17 / 1 | pass |
| 148 | CDLTASUKIGAP | canonical / long | AADHARHFC 4H 2025-08-20 09:15:00 | 17 / 2 | pass |
| 149 | CDLTASUKIGAP | canonical / short | AADHARHFC 1H 2025-06-03 12:15:00 | 17 / 2 | pass |
| 150 | CDLTASUKIGAP | canonical_context / long | ABSLAMC 4H 2025-07-21 09:15:00 | 17 / 2 | pass |
| 151 | CDLTASUKIGAP | canonical_context / short | ABSLAMC 4H 2024-03-20 13:15:00 | 17 / 3 | pass |
| 152 | CDLTHRUSTING | canonical / short | AADHARHFC 1D 2024-10-22 09:15:00 | 17 / 3 | pass |
| 153 | CDLTHRUSTING | canonical_context / short | AADHARHFC 1D 2024-10-22 09:15:00 | 17 / 3 | pass |
| 154 | CDLTRISTAR | canonical / long | AADHARHFC 1D 2025-06-09 09:15:00 | 18 / 1 | pass |
| 155 | CDLTRISTAR | canonical / short | AADHARHFC 1D 2024-12-24 09:15:00 | 18 / 1 | pass |
| 156 | CDLTRISTAR | canonical_context / long | AADHARHFC 1D 2025-06-09 09:15:00 | 18 / 1 | pass |
| 157 | CDLTRISTAR | canonical_context / short | AADHARHFC 1D 2024-12-24 09:15:00 | 18 / 2 | pass |
| 158 | CDLUNIQUE3RIVER | canonical / long | AADHARHFC 4H 2025-02-07 09:15:00 | 18 / 2 | pass |
| 159 | CDLUNIQUE3RIVER | canonical_context / long | AADHARHFC 1H 2025-01-29 10:15:00 | 18 / 2 | pass |
| 160 | CDLUPSIDEGAP2CROWS | canonical / short | ABSLAMC 1D 2024-11-07 09:15:00 | 18 / 3 | pass |
| 161 | CDLUPSIDEGAP2CROWS | canonical_context / short | ABSLAMC 1D 2024-11-07 09:15:00 | 18 / 3 | pass |
| 162 | CDLXSIDEGAP3METHODS | canonical / long | AADHARHFC 1H 2024-06-10 09:15:00 | 18 / 3 | pass |
| 163 | CDLXSIDEGAP3METHODS | canonical / short | AADHARHFC 1H 2026-07-29 12:15:00 | 19 / 1 | pass |
| 164 | CDLXSIDEGAP3METHODS | canonical_context / long | AADHARHFC 1H 2024-06-10 09:15:00 | 19 / 1 | pass |
| 165 | CDLXSIDEGAP3METHODS | canonical_context / short | AADHARHFC 1D 2025-01-28 09:15:00 | 19 / 1 | pass |
