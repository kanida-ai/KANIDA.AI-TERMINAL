# Frozen drawing example coverage

Frozen run: `4b33a5249562631524d6`.

Actual examples: **107/107 pattern IDs**, **262/262 directional variants**.

Representative real frozen event per registered variant and side; confirmed then longest useful formation within the first indexed stock containing that variant. Frozen event indices remain absolute; line/bar indices are rebased by window_offset. No new research detection or backtest is run. Legacy replay only recovers its original overlays.

Every displayed candle and every overlay point ends at or before the event signal. Original frozen geometry is preserved unchanged. This checks presentation causality, not a new detector prefix-stability proof.

These are review examples, not an assertion that every drawing is visually correct. Visual review remains explicit.

| Pattern | Variant | Side | Example | State | Lines | Checks |
|---|---|---|---|---|---:|---|
| CDL2CROWS Two crows | canonical | short | AADHARHFC 1D 2025-11-19 09:15:00 | confirmed | 6 | pass |
| CDL2CROWS Two crows | canonical_context | short | AADHARHFC 1H 2025-04-30 11:15:00 | setup | 4 | pass |
| CDL3BLACKCROWS Three black crows | canonical | short | ITCHOTELS 1D 2025-06-19 09:15:00 | setup | 5 | pass |
| CDL3BLACKCROWS Three black crows | canonical_context | short | ITCHOTELS 1D 2025-06-19 09:15:00 | setup | 5 | pass |
| CDL3INSIDE Three inside up/down | canonical | long | AADHARHFC 1H 2026-03-24 10:15:00 | confirmed | 4 | pass |
| CDL3INSIDE Three inside up/down | canonical | short | AADHARHFC 1H 2026-09-15 09:15:00 | confirmed | 4 | pass |
| CDL3INSIDE Three inside up/down | canonical_context | long | AADHARHFC 1H 2026-03-24 10:15:00 | confirmed | 4 | pass |
| CDL3INSIDE Three inside up/down | canonical_context | short | AADHARHFC 1H 2026-09-02 09:15:00 | confirmed | 4 | pass |
| CDL3LINESTRIKE Three-line strike | canonical | long | AADHARHFC 1H 2025-11-12 09:15:00 | confirmed | 6 | pass |
| CDL3LINESTRIKE Three-line strike | canonical | short | AADHARHFC 1H 2026-06-22 15:15:00 | setup | 4 | pass |
| CDL3LINESTRIKE Three-line strike | canonical_context | long | AADHARHFC 4H 2026-04-20 09:15:00 | setup | 4 | pass |
| CDL3LINESTRIKE Three-line strike | canonical_context | short | ABDL 1D 2026-08-10 09:15:00 | setup | 4 | pass |
| CDL3OUTSIDE Three outside up/down | canonical | long | AADHARHFC 1H 2026-08-24 11:15:00 | confirmed | 4 | pass |
| CDL3OUTSIDE Three outside up/down | canonical | short | AADHARHFC 1H 2026-08-27 10:15:00 | confirmed | 4 | pass |
| CDL3OUTSIDE Three outside up/down | canonical_context | long | AADHARHFC 1H 2026-08-24 11:15:00 | confirmed | 4 | pass |
| CDL3OUTSIDE Three outside up/down | canonical_context | short | AADHARHFC 1H 2026-08-27 10:15:00 | confirmed | 4 | pass |
| CDL3STARSINSOUTH Three stars in the south | canonical | long | ABSLAMC 1H 2025-09-01 14:15:00 | setup | 4 | pass |
| CDL3STARSINSOUTH Three stars in the south | canonical_context | long | ABSLAMC 1H 2025-09-01 14:15:00 | setup | 4 | pass |
| CDL3WHITESOLDIERS Three white soldiers | canonical | long | AADHARHFC 1H 2025-04-02 14:15:00 | confirmed | 6 | pass |
| CDL3WHITESOLDIERS Three white soldiers | canonical_context | long | ABDL 1W 2026-05-04 09:15:00 | confirmed | 6 | pass |
| CDLABANDONEDBABY Abandoned baby | canonical | long | GROWW 1D 2026-03-10 09:15:00 | setup | 4 | pass |
| CDLABANDONEDBABY Abandoned baby | canonical | short | ICICIBANK 1D 2013-07-25 09:15:00 | confirmed | 6 | pass |
| CDLABANDONEDBABY Abandoned baby | canonical_context | long | GROWW 1D 2026-03-10 09:15:00 | setup | 4 | pass |
| CDLABANDONEDBABY Abandoned baby | canonical_context | short | MMTC 1D 2023-10-19 09:15:00 | confirmed | 6 | pass |
| CDLADVANCEBLOCK Advance block | canonical | short | AADHARHFC 1H 2026-05-11 09:15:00 | confirmed | 6 | pass |
| CDLADVANCEBLOCK Advance block | canonical_context | short | AADHARHFC 1H 2026-02-23 09:15:00 | confirmed | 6 | pass |
| CDLBELTHOLD Belt hold | canonical | long | AADHARHFC 1H 2026-09-01 14:15:00 | confirmed | 6 | pass |
| CDLBELTHOLD Belt hold | canonical | short | AADHARHFC 1H 2026-06-08 11:15:00 | confirmed | 6 | pass |
| CDLBELTHOLD Belt hold | canonical_context | long | AADHARHFC 1H 2026-08-28 12:15:00 | confirmed | 6 | pass |
| CDLBELTHOLD Belt hold | canonical_context | short | AADHARHFC 1H 2026-06-08 11:15:00 | confirmed | 6 | pass |
| CDLBREAKAWAY Breakaway | canonical | long | RRKABEL 1D 2024-08-16 09:15:00 | setup | 4 | pass |
| CDLBREAKAWAY Breakaway | canonical | short | ACMESOLAR 1D 2025-02-03 09:15:00 | setup | 4 | pass |
| CDLBREAKAWAY Breakaway | canonical_context | long | RRKABEL 1D 2024-08-16 09:15:00 | setup | 4 | pass |
| CDLBREAKAWAY Breakaway | canonical_context | short | PIRAMALFIN 1D 2026-09-01 09:15:00 | setup | 4 | pass |
| CDLCLOSINGMARUBOZU Closing marubozu | break_down | short | AADHARHFC 1H 2026-08-10 11:15:00 | confirmed | 6 | pass |
| CDLCLOSINGMARUBOZU Closing marubozu | break_up | long | AADHARHFC 1H 2026-05-13 12:15:00 | confirmed | 6 | pass |
| CDLCLOSINGMARUBOZU Closing marubozu | color_direction | long | AADHARHFC 1H 2026-03-24 10:15:00 | confirmed | 6 | pass |
| CDLCLOSINGMARUBOZU Closing marubozu | color_direction | short | AADHARHFC 1H 2026-08-10 11:15:00 | confirmed | 6 | pass |
| CDLCONCEALBABYSWALL Concealing baby swallow | canonical | long | RRKABEL 1H 2026-06-08 14:15:00 | setup | 4 | pass |
| CDLCONCEALBABYSWALL Concealing baby swallow | canonical_context | long | TATAPOWER 1D 2016-02-25 09:15:00 | setup | 4 | pass |
| CDLCOUNTERATTACK Counterattack | canonical | long | ABDL 1H 2026-07-23 09:15:00 | setup | 4 | pass |
| CDLCOUNTERATTACK Counterattack | canonical | short | AADHARHFC 1H 2024-05-31 09:15:00 | setup | 4 | pass |
| CDLCOUNTERATTACK Counterattack | canonical_context | long | ABSLAMC 1D 2022-10-17 09:15:00 | setup | 4 | pass |
| CDLCOUNTERATTACK Counterattack | canonical_context | short | AADHARHFC 1H 2024-05-31 09:15:00 | setup | 4 | pass |
| CDLDARKCLOUDCOVER Dark cloud cover | canonical | short | AADHARHFC 1W 2026-07-20 09:15:00 | confirmed | 6 | pass |
| CDLDARKCLOUDCOVER Dark cloud cover | canonical_context | short | AADHARHFC 1W 2026-07-20 09:15:00 | confirmed | 6 | pass |
| CDLDOJI Doji | break_down | short | AADHARHFC 1H 2026-09-15 09:15:00 | confirmed | 6 | pass |
| CDLDOJI Doji | break_up | long | AADHARHFC 1H 2026-09-04 10:15:00 | confirmed | 6 | pass |
| CDLDOJISTAR Doji star | canonical | long | AADHARHFC 1H 2025-09-23 11:15:00 | confirmed | 6 | pass |
| CDLDOJISTAR Doji star | canonical | short | AADHARHFC 1H 2025-04-07 09:15:00 | confirmed | 6 | pass |
| CDLDOJISTAR Doji star | canonical_context | long | AADHARHFC 1H 2025-09-23 11:15:00 | confirmed | 6 | pass |
| CDLDOJISTAR Doji star | canonical_context | short | AADHARHFC 1H 2025-04-07 09:15:00 | confirmed | 6 | pass |
| CDLDRAGONFLYDOJI Dragonfly doji | break_down | short | AADHARHFC 1H 2026-09-15 09:15:00 | confirmed | 6 | pass |
| CDLDRAGONFLYDOJI Dragonfly doji | break_up | long | AADHARHFC 1H 2026-07-09 13:15:00 | confirmed | 6 | pass |
| CDLDRAGONFLYDOJI Dragonfly doji | canonical_context | long | AADHARHFC 1H 2026-07-09 13:15:00 | confirmed | 6 | pass |
| CDLENGULFING Engulfing | canonical | long | AADHARHFC 1H 2026-08-28 12:15:00 | confirmed | 6 | pass |
| CDLENGULFING Engulfing | canonical | short | AADHARHFC 1H 2026-08-17 09:15:00 | confirmed | 6 | pass |
| CDLENGULFING Engulfing | canonical_context | long | AADHARHFC 1H 2026-08-28 12:15:00 | confirmed | 6 | pass |
| CDLENGULFING Engulfing | canonical_context | short | AADHARHFC 1H 2026-06-08 11:15:00 | confirmed | 6 | pass |
| CDLEVENINGDOJISTAR Evening doji star | canonical | short | AADHARHFC 1H 2026-03-18 09:15:00 | setup | 4 | pass |
| CDLEVENINGDOJISTAR Evening doji star | canonical_context | short | AADHARHFC 1H 2026-03-18 09:15:00 | setup | 4 | pass |
| CDLEVENINGSTAR Evening star | canonical | short | AADHARHFC 1D 2025-11-19 09:15:00 | confirmed | 6 | pass |
| CDLEVENINGSTAR Evening star | canonical_context | short | AADHARHFC 1H 2026-06-29 11:15:00 | setup | 4 | pass |
| CDLGAPSIDESIDEWHITE Gap side-by-side white lines | canonical | long | AADHARHFC 1H 2025-03-05 11:15:00 | confirmed | 6 | pass |
| CDLGAPSIDESIDEWHITE Gap side-by-side white lines | canonical | short | AADHARHFC 4H 2025-09-08 13:15:00 | setup | 4 | pass |
| CDLGAPSIDESIDEWHITE Gap side-by-side white lines | canonical_context | long | AADHARHFC 1H 2026-01-05 10:15:00 | setup | 4 | pass |
| CDLGAPSIDESIDEWHITE Gap side-by-side white lines | canonical_context | short | AADHARHFC 1D 2025-02-04 09:15:00 | setup | 4 | pass |
| CDLGRAVESTONEDOJI Gravestone doji | break_down | short | AADHARHFC 1H 2026-07-20 09:15:00 | confirmed | 6 | pass |
| CDLGRAVESTONEDOJI Gravestone doji | break_up | long | AADHARHFC 1H 2026-07-10 13:15:00 | confirmed | 6 | pass |
| CDLGRAVESTONEDOJI Gravestone doji | canonical_context | short | AADHARHFC 1H 2026-07-06 11:15:00 | confirmed | 6 | pass |
| CDLHAMMER Hammer | canonical | long | AADHARHFC 1H 2026-04-21 09:15:00 | confirmed | 7 | pass |
| CDLHAMMER Hammer | canonical_context | long | AADHARHFC 1H 2025-04-23 13:15:00 | confirmed | 7 | pass |
| CDLHANGINGMAN Hanging man | canonical | short | AADHARHFC 1H 2026-09-02 09:15:00 | confirmed | 7 | pass |
| CDLHANGINGMAN Hanging man | canonical_context | short | AADHARHFC 1H 2026-03-10 15:15:00 | confirmed | 7 | pass |
| CDLHARAMI Harami | canonical | long | AADHARHFC 1H 2026-08-24 10:15:00 | confirmed | 6 | pass |
| CDLHARAMI Harami | canonical | short | AADHARHFC 1H 2026-07-28 15:15:00 | confirmed | 6 | pass |
| CDLHARAMI Harami | canonical_context | long | AADHARHFC 1H 2026-08-24 10:15:00 | confirmed | 6 | pass |
| CDLHARAMI Harami | canonical_context | short | AADHARHFC 1H 2026-04-13 09:15:00 | confirmed | 6 | pass |
| CDLHARAMICROSS Harami cross | canonical | long | AADHARHFC 1H 2024-09-20 14:15:00 | confirmed | 6 | pass |
| CDLHARAMICROSS Harami cross | canonical | short | AADHARHFC 1H 2026-05-12 09:15:00 | confirmed | 6 | pass |
| CDLHARAMICROSS Harami cross | canonical_context | long | AADHARHFC 1H 2024-08-06 09:15:00 | confirmed | 6 | pass |
| CDLHARAMICROSS Harami cross | canonical_context | short | AADHARHFC 1H 2026-01-14 14:15:00 | confirmed | 6 | pass |
| CDLHIGHWAVE High-wave candle | break_down | short | AADHARHFC 1H 2026-09-03 15:15:00 | confirmed | 6 | pass |
| CDLHIGHWAVE High-wave candle | break_up | long | AADHARHFC 1H 2026-09-04 10:15:00 | confirmed | 6 | pass |
| CDLHIKKAKE Hikkake | canonical | long | AADHARHFC 1H 2026-07-21 10:15:00 | confirmed | 7 | pass |
| CDLHIKKAKE Hikkake | canonical | short | AADHARHFC 1H 2026-08-27 09:15:00 | confirmed | 7 | pass |
| CDLHIKKAKEMOD Modified hikkake | canonical | long | AADHARHFC 1H 2025-09-10 09:15:00 | confirmed | 7 | pass |
| CDLHIKKAKEMOD Modified hikkake | canonical | short | AADHARHFC 1H 2026-06-09 09:15:00 | setup | 5 | pass |
| CDLHOMINGPIGEON Homing pigeon | canonical | long | AADHARHFC 1D 2024-11-06 09:15:00 | confirmed | 6 | pass |
| CDLHOMINGPIGEON Homing pigeon | canonical_context | long | AADHARHFC 1H 2026-03-24 10:15:00 | confirmed | 6 | pass |
| CDLIDENTICAL3CROWS Identical three crows | canonical | short | AADHARHFC 1H 2025-04-23 09:15:00 | confirmed | 6 | pass |
| CDLIDENTICAL3CROWS Identical three crows | canonical_context | short | AADHARHFC 1H 2026-06-22 14:15:00 | confirmed | 6 | pass |
| CDLINNECK In-neck | canonical | short | AADHARHFC 1H 2025-12-05 09:15:00 | setup | 4 | pass |
| CDLINNECK In-neck | canonical_context | short | AADHARHFC 1H 2025-12-05 09:15:00 | setup | 4 | pass |
| CDLINVERTEDHAMMER Inverted hammer | canonical | long | AADHARHFC 1H 2026-02-03 14:15:00 | confirmed | 7 | pass |
| CDLINVERTEDHAMMER Inverted hammer | canonical_context | long | AADHARHFC 1H 2026-02-03 14:15:00 | confirmed | 7 | pass |
| CDLKICKING Kicking | canonical | long | JYOTICNC 1H 2025-03-18 09:15:00 | setup | 4 | pass |
| CDLKICKING Kicking | canonical | short | ICICIBANK 1H 2016-10-26 10:15:00 | confirmed | 6 | pass |
| CDLKICKINGBYLENGTH Kicking by length | canonical | long | JYOTICNC 1H 2025-03-18 09:15:00 | setup | 4 | pass |
| CDLKICKINGBYLENGTH Kicking by length | canonical | short | ICICIBANK 1H 2016-10-26 10:15:00 | confirmed | 6 | pass |
| CDLLADDERBOTTOM Ladder bottom | canonical | long | AADHARHFC 4H 2026-03-25 13:15:00 | confirmed | 6 | pass |
| CDLLADDERBOTTOM Ladder bottom | canonical_context | long | AADHARHFC 1H 2026-07-23 15:15:00 | setup | 4 | pass |
| CDLLONGLEGGEDDOJI Long-legged doji | break_down | short | AADHARHFC 1H 2026-09-15 09:15:00 | confirmed | 6 | pass |
| CDLLONGLEGGEDDOJI Long-legged doji | break_up | long | AADHARHFC 1H 2026-09-04 10:15:00 | confirmed | 6 | pass |
| CDLLONGLINE Long-line candle | break_down | short | AADHARHFC 1H 2026-09-11 09:15:00 | confirmed | 6 | pass |
| CDLLONGLINE Long-line candle | break_up | long | AADHARHFC 1H 2026-07-30 15:15:00 | confirmed | 6 | pass |
| CDLLONGLINE Long-line candle | color_direction | long | AADHARHFC 1H 2026-07-30 15:15:00 | confirmed | 6 | pass |
| CDLLONGLINE Long-line candle | color_direction | short | AADHARHFC 1H 2026-09-11 09:15:00 | confirmed | 6 | pass |
| CDLMARUBOZU Marubozu | break_down | short | AADHARHFC 1H 2026-05-07 14:15:00 | confirmed | 6 | pass |
| CDLMARUBOZU Marubozu | break_up | long | AADHARHFC 1H 2026-05-13 12:15:00 | confirmed | 6 | pass |
| CDLMARUBOZU Marubozu | color_direction | long | AADHARHFC 1H 2026-03-16 09:15:00 | confirmed | 6 | pass |
| CDLMARUBOZU Marubozu | color_direction | short | AADHARHFC 1H 2026-05-07 14:15:00 | confirmed | 6 | pass |
| CDLMATCHINGLOW Matching low | canonical | long | AADHARHFC 1H 2026-05-04 15:15:00 | confirmed | 6 | pass |
| CDLMATCHINGLOW Matching low | canonical_context | long | AADHARHFC 1H 2025-08-07 09:15:00 | confirmed | 6 | pass |
| CDLMATHOLD Mat hold | canonical | long | ICICIBANK 1D 2015-10-09 09:15:00 | setup | 4 | pass |
| CDLMATHOLD Mat hold | canonical_context | long | ICICIBANK 1D 2015-10-09 09:15:00 | setup | 4 | pass |
| CDLMORNINGDOJISTAR Morning doji star | canonical | long | AADHARHFC 1H 2025-07-15 09:15:00 | confirmed | 6 | pass |
| CDLMORNINGDOJISTAR Morning doji star | canonical_context | long | AADHARHFC 4H 2026-04-06 13:15:00 | confirmed | 6 | pass |
| CDLMORNINGSTAR Morning star | canonical | long | AADHARHFC 1H 2025-07-15 09:15:00 | confirmed | 6 | pass |
| CDLMORNINGSTAR Morning star | canonical_context | long | AADHARHFC 4H 2026-04-06 13:15:00 | confirmed | 6 | pass |
| CDLONNECK On-neck | canonical | short | AADHARHFC 1H 2025-12-05 09:15:00 | setup | 4 | pass |
| CDLONNECK On-neck | canonical_context | short | AADHARHFC 1H 2025-12-05 09:15:00 | setup | 4 | pass |
| CDLPIERCING Piercing | canonical | long | AADHARHFC 1D 2026-09-01 09:15:00 | confirmed | 6 | pass |
| CDLPIERCING Piercing | canonical_context | long | AADHARHFC 1D 2026-09-01 09:15:00 | confirmed | 6 | pass |
| CDLRICKSHAWMAN Rickshaw man | break_down | short | AADHARHFC 1H 2026-09-15 09:15:00 | confirmed | 6 | pass |
| CDLRICKSHAWMAN Rickshaw man | break_up | long | AADHARHFC 1H 2026-09-04 10:15:00 | confirmed | 6 | pass |
| CDLRISEFALL3METHODS Rising/falling three methods | canonical | long | AADHARHFC 4H 2026-05-22 09:15:00 | setup | 4 | pass |
| CDLRISEFALL3METHODS Rising/falling three methods | canonical | short | ITCHOTELS 4H 2026-01-29 09:15:00 | setup | 4 | pass |
| CDLRISEFALL3METHODS Rising/falling three methods | canonical_context | long | ABDL 1H 2025-11-27 09:15:00 | setup | 4 | pass |
| CDLRISEFALL3METHODS Rising/falling three methods | canonical_context | short | ITCHOTELS 4H 2026-01-29 09:15:00 | setup | 4 | pass |
| CDLSEPARATINGLINES Separating lines | canonical | long | AADHARHFC 1D 2026-06-12 09:15:00 | confirmed | 6 | pass |
| CDLSEPARATINGLINES Separating lines | canonical | short | AADHARHFC 1H 2026-04-02 09:15:00 | confirmed | 6 | pass |
| CDLSEPARATINGLINES Separating lines | canonical_context | long | AADHARHFC 1H 2024-07-04 13:15:00 | confirmed | 6 | pass |
| CDLSEPARATINGLINES Separating lines | canonical_context | short | AADHARHFC 1H 2026-04-02 09:15:00 | confirmed | 6 | pass |
| CDLSHOOTINGSTAR Shooting star | canonical | short | AADHARHFC 1H 2025-07-03 09:15:00 | confirmed | 7 | pass |
| CDLSHOOTINGSTAR Shooting star | canonical_context | short | AADHARHFC 1H 2025-07-03 09:15:00 | confirmed | 7 | pass |
| CDLSHORTLINE Short-line candle | break_down | short | AADHARHFC 1H 2026-09-15 09:15:00 | confirmed | 6 | pass |
| CDLSHORTLINE Short-line candle | break_up | long | AADHARHFC 1H 2026-09-11 14:15:00 | confirmed | 6 | pass |
| CDLSPINNINGTOP Spinning top | break_down | short | AADHARHFC 1H 2026-09-15 09:15:00 | confirmed | 6 | pass |
| CDLSPINNINGTOP Spinning top | break_up | long | AADHARHFC 1H 2026-09-11 14:15:00 | confirmed | 6 | pass |
| CDLSTALLEDPATTERN Stalled pattern | canonical | short | AADHARHFC 1H 2026-09-08 14:15:00 | setup | 4 | pass |
| CDLSTALLEDPATTERN Stalled pattern | canonical_context | short | AADHARHFC 1H 2026-02-20 12:15:00 | setup | 4 | pass |
| CDLSTICKSANDWICH Stick sandwich | canonical | long | AADHARHFC 1H 2025-06-03 10:15:00 | setup | 4 | pass |
| CDLSTICKSANDWICH Stick sandwich | canonical_context | long | AADHARHFC 4H 2025-10-09 13:15:00 | setup | 4 | pass |
| CDLTAKURI Takuri | break_down | short | AADHARHFC 1H 2026-09-03 15:15:00 | confirmed | 6 | pass |
| CDLTAKURI Takuri | break_up | long | AADHARHFC 1H 2026-07-09 13:15:00 | confirmed | 6 | pass |
| CDLTAKURI Takuri | canonical_context | long | AADHARHFC 1H 2026-07-09 13:15:00 | confirmed | 6 | pass |
| CDLTASUKIGAP Tasuki gap | canonical | long | AADHARHFC 4H 2025-08-20 09:15:00 | confirmed | 6 | pass |
| CDLTASUKIGAP Tasuki gap | canonical | short | AADHARHFC 1H 2025-06-03 12:15:00 | confirmed | 6 | pass |
| CDLTASUKIGAP Tasuki gap | canonical_context | long | ABSLAMC 4H 2025-07-21 09:15:00 | confirmed | 6 | pass |
| CDLTASUKIGAP Tasuki gap | canonical_context | short | ABSLAMC 4H 2024-03-20 13:15:00 | setup | 4 | pass |
| CDLTHRUSTING Thrusting | canonical | short | AADHARHFC 1D 2024-10-22 09:15:00 | confirmed | 6 | pass |
| CDLTHRUSTING Thrusting | canonical_context | short | AADHARHFC 1D 2024-10-22 09:15:00 | confirmed | 6 | pass |
| CDLTRISTAR Tristar | canonical | long | AADHARHFC 1D 2025-06-09 09:15:00 | confirmed | 6 | pass |
| CDLTRISTAR Tristar | canonical | short | AADHARHFC 1D 2024-12-24 09:15:00 | setup | 4 | pass |
| CDLTRISTAR Tristar | canonical_context | long | AADHARHFC 1D 2025-06-09 09:15:00 | confirmed | 6 | pass |
| CDLTRISTAR Tristar | canonical_context | short | AADHARHFC 1D 2024-12-24 09:15:00 | setup | 4 | pass |
| CDLUNIQUE3RIVER Unique three-river bottom | canonical | long | AADHARHFC 4H 2025-02-07 09:15:00 | confirmed | 6 | pass |
| CDLUNIQUE3RIVER Unique three-river bottom | canonical_context | long | AADHARHFC 1H 2025-01-29 10:15:00 | setup | 4 | pass |
| CDLUPSIDEGAP2CROWS Upside-gap two crows | canonical | short | ABSLAMC 1D 2024-11-07 09:15:00 | setup | 4 | pass |
| CDLUPSIDEGAP2CROWS Upside-gap two crows | canonical_context | short | ABSLAMC 1D 2024-11-07 09:15:00 | setup | 4 | pass |
| CDLXSIDEGAP3METHODS Gap three methods | canonical | long | AADHARHFC 1H 2024-06-10 09:15:00 | confirmed | 6 | pass |
| CDLXSIDEGAP3METHODS Gap three methods | canonical | short | AADHARHFC 1H 2026-07-29 12:15:00 | confirmed | 6 | pass |
| CDLXSIDEGAP3METHODS Gap three methods | canonical_context | long | AADHARHFC 1H 2024-06-10 09:15:00 | confirmed | 6 | pass |
| CDLXSIDEGAP3METHODS Gap three methods | canonical_context | short | AADHARHFC 1D 2025-01-28 09:15:00 | setup | 4 | pass |
| CH01 Cup & Handle | legacy_1.0.1 | long | AADHARHFC 1D 2024-09-23 09:15:00 | confirmed | 3 | pass |
| CH02 Horizontal Breakout | legacy_1.0.1 | long | AADHARHFC 1H 2024-11-07 10:15:00 | confirmed | 2 | pass |
| CH03 Flag & Pole | legacy_1.0.1 | long | AADHARHFC 1H 2025-07-21 09:15:00 | confirmed | 3 | pass |
| CH03 Flag & Pole | legacy_1.0.1 | short | ITCHOTELS 4H 2026-08-10 09:15:00 | setup | 3 | pass |
| CH04 Symmetrical Triangle | legacy_1.0.1 | long | AADHARHFC 1W 2026-06-29 09:15:00 | confirmed | 4 | pass |
| CH04 Symmetrical Triangle | legacy_1.0.1 | short | AADHARHFC 1H 2026-06-01 09:15:00 | confirmed | 4 | pass |
| CH05 Falling Wedge | legacy_1.0.1 | long | AADHARHFC 1H 2026-09-01 12:15:00 | confirmed | 4 | pass |
| CH06 Rising Wedge | legacy_1.0.1 | short | AADHARHFC 1H 2024-08-12 09:15:00 | confirmed | 4 | pass |
| CH07 Channel | legacy_1.0.1 | long | AADHARHFC 1H 2026-04-22 09:15:00 | setup | 4 | pass |
| CH07 Channel | legacy_1.0.1 | short | AADHARHFC 1H 2026-07-30 13:15:00 | setup | 4 | pass |
| CH08 Descending Triangle | legacy_1.0.1 | short | AADHARHFC 4H 2025-12-17 09:15:00 | confirmed | 4 | pass |
| CH09 Head & Shoulders | legacy_1.0.1 | short | AADHARHFC 4H 2025-01-02 13:15:00 | setup | 5 | pass |
| CH10 Inverse Head & Shoulders | legacy_1.0.1 | long | AADHARHFC 1H 2025-02-28 12:15:00 | setup | 5 | pass |
| CH11 Ascending triangle - downside resolution | downside_resolution | short | AADHARHFC 1H 2025-06-10 14:15:00 | confirmed | 6 | pass |
| CH11 Ascending triangle - upside | upside | long | AADHARHFC 1H 2025-12-15 09:15:00 | confirmed | 6 | pass |
| CH12 Horizontal breakdown | support_breakdown | short | AADHARHFC 1H 2024-10-22 15:15:00 | confirmed | 4 | pass |
| CH13 Rectangle - downside break | downside | short | AADHARHFC 1H 2024-12-09 10:15:00 | confirmed | 6 | pass |
| CH13 Rectangle - upside break | upside | long | AADHARHFC 1D 2026-05-05 09:15:00 | confirmed | 6 | pass |
| CH14 Bear pennant | bear | short | ABSLAMC 1H 2024-06-21 14:15:00 | confirmed | 9 | pass |
| CH14 Bull pennant | bull | long | ICICIBANK 1H 2018-05-10 15:15:00 | setup | 8 | pass |
| CH15 Double top | canonical | short | AADHARHFC 1H 2025-06-25 09:15:00 | confirmed | 5 | pass |
| CH16 Double bottom | canonical | long | AADHARHFC 1H 2025-07-15 10:15:00 | confirmed | 5 | pass |
| CH17 Triple top | canonical | short | AADHARHFC 4H 2025-03-21 13:15:00 | setup | 4 | pass |
| CH18 Triple bottom | canonical | long | AADHARHFC 1H 2026-06-15 09:15:00 | confirmed | 5 | pass |
| CH19 Rounding bottom / saucer | canonical | long | AADHARHFC 4H 2024-09-23 09:15:00 | confirmed | 5 | pass |
| CH20 Rounding top / dome | canonical | short | AADHARHFC 1H 2026-02-24 12:15:00 | confirmed | 5 | pass |
| CH21 Inverted cup & handle | canonical | short | ABDL 1H 2024-10-25 13:15:00 | confirmed | 9 | pass |
| CH22 Broadening formation - downside | downside | short | AADHARHFC 1H 2024-08-26 12:15:00 | setup | 5 | pass |
| CH22 Broadening formation - upside | upside | long | AADHARHFC 1H 2024-11-28 09:15:00 | confirmed | 6 | pass |
| CH23 Diamond - downside | downside | short | AADHARHFC 1H 2024-06-13 12:15:00 | confirmed | 7 | pass |
| CH23 Diamond - upside | upside | long | AADHARHFC 1H 2024-06-13 12:15:00 | setup | 6 | pass |
| CH24 Island reversal bottom - multi bar | bottom_multi | long | AADHARHFC 1H 2025-05-12 09:15:00 | confirmed | 6 | pass |
| CH24 Island reversal bottom - single bar | bottom_single | long | HDBFS 1D 2026-04-15 09:15:00 | confirmed | 6 | pass |
| CH24 Island reversal top - multi bar | top_multi | short | ICICIBANK 1H 2026-04-23 09:15:00 | confirmed | 6 | pass |
| CH24 Island reversal top - single bar | top_single | short | IGIL 1D 2025-07-31 09:15:00 | confirmed | 6 | pass |
| CH25 Inverted V top | inverted_v_top | short | AADHARHFC 1H 2026-08-07 15:15:00 | confirmed | 6 | pass |
| CH25 V bottom | v_bottom | long | AADHARHFC 1H 2024-10-21 15:15:00 | confirmed | 6 | pass |
| CH26 Measured move - bearish | bear | short | AADHARHFC 1H 2024-08-28 10:15:00 | confirmed | 8 | pass |
| CH26 Measured move - bullish | bull | long | AADHARHFC 1H 2024-06-14 09:15:00 | confirmed | 8 | pass |
| CH27 Bump-and-run reversal bottom | bottom | long | AADHARHFC 1H 2025-01-28 14:15:00 | confirmed | 6 | pass |
| CH27 Bump-and-run reversal top | top | short | AADHARHFC 1H 2024-06-19 12:15:00 | confirmed | 6 | pass |
| CH28 Volatility-contraction base - bearish | bearish | short | AADHARHFC 1H 2024-12-09 12:15:00 | setup | 5 | pass |
| CH28 Volatility-contraction base - bearish, volume contraction | bearish_volume_contraction | short | AADHARHFC 1H 2025-10-29 12:15:00 | setup | 5 | pass |
| CH28 Volatility-contraction base - bullish | bullish | long | AADHARHFC 1H 2025-06-06 10:15:00 | setup | 5 | pass |
| CH28 Volatility-contraction base - bullish, volume contraction | bullish_volume_contraction | long | AADHARHFC 1H 2025-08-04 12:15:00 | setup | 5 | pass |
| HA01 Bullish AB=CD | canonical | long | AADHARHFC 1H 2025-08-07 14:15:00 | confirmed | 14 | pass |
| HA01 Bearish AB=CD | canonical | short | AADHARHFC 1W 2025-11-03 09:15:00 | confirmed | 14 | pass |
| HA02 Bullish Alternate AB=CD 1.27 | ext_1_27 | long | AADHARHFC 1H 2025-04-28 09:15:00 | confirmed | 14 | pass |
| HA02 Bearish Alternate AB=CD 1.27 | ext_1_27 | short | AADHARHFC 1H 2024-12-31 12:15:00 | confirmed | 14 | pass |
| HA02 Bullish Alternate AB=CD 1.618 | ext_1_618 | long | AADHARHFC 1H 2026-07-27 09:15:00 | confirmed | 14 | pass |
| HA02 Bearish Alternate AB=CD 1.618 | ext_1_618 | short | AADHARHFC 1H 2025-02-10 09:15:00 | confirmed | 14 | pass |
| HA03 Bullish Gartley | canonical | long | ABSLAMC 1H 2023-09-18 14:15:00 | confirmed | 17 | pass |
| HA03 Bearish Gartley | canonical | short | ABSLAMC 1H 2025-09-04 12:15:00 | setup | 17 | pass |
| HA04 Bullish Bat (B 0.382 subtype) | b_0_382 | long | TATAPOWER 4H 2019-06-10 13:15:00 | setup | 17 | pass |
| HA04 Bearish Bat (B 0.382 subtype) | b_0_382 | short | ABSLAMC 1H 2025-10-03 12:15:00 | setup | 17 | pass |
| HA04 Bullish Bat (B 0.50 subtype) | b_0_50 | long | ITCHOTELS 1W 2026-01-05 09:15:00 | setup | 17 | pass |
| HA04 Bearish Bat (B 0.50 subtype) | b_0_50 | short | ABDL 1H 2025-11-14 09:15:00 | confirmed | 17 | pass |
| HA05 Bullish Alternate Bat | canonical | long | DCMSHRIRAM 1D 2013-09-12 09:15:00 | confirmed | 17 | pass |
| HA05 Bearish Alternate Bat | canonical | short | BEL 1D 2025-04-07 09:15:00 | confirmed | 17 | pass |
| HA06 Bullish Butterfly 1.27 | ext_1_27 | long | ABSLAMC 1H 2024-03-19 13:15:00 | setup | 17 | pass |
| HA06 Bearish Butterfly 1.27 | ext_1_27 | short | MMTC 1H 2023-02-01 13:15:00 | confirmed | 17 | pass |
| HA06 Bullish Butterfly 1.618 | ext_1_618 | long | ABSLAMC 1H 2023-01-06 11:15:00 | setup | 17 | pass |
| HA06 Bearish Butterfly 1.618 | ext_1_618 | short | DCMSHRIRAM 1H 2019-01-10 11:15:00 | confirmed | 17 | pass |
| HA07 Bullish Crab | canonical | long | DCMSHRIRAM 1D 2024-06-13 09:15:00 | confirmed | 17 | pass |
| HA07 Bearish Crab | canonical | short | LTTS 1D 2022-11-21 09:15:00 | setup | 17 | pass |
| HA08 Bullish Deep Crab | canonical | long | ICICIBANK 1H 2018-12-07 09:15:00 | confirmed | 17 | pass |
| HA08 Bearish Deep Crab | canonical | short | ICICIBANK 1D 2023-04-20 09:15:00 | setup | 17 | pass |
| HA09 Bullish Shark | canonical | long | ABSLAMC 4H 2022-11-28 13:15:00 | setup | 17 | pass |
| HA09 Bearish Shark | canonical | short | ITCHOTELS 4H 2025-03-10 13:15:00 | confirmed | 17 | pass |
| HA10 Bullish 5-0 | canonical | long | ABSLAMC 1D 2026-03-10 09:15:00 | confirmed | 17 | pass |
| HA10 Bearish 5-0 | canonical | short | AADHARHFC 1H 2024-10-21 12:15:00 | setup | 17 | pass |
| PA01 Tweezer bottom | bottom | long | AADHARHFC 1H 2026-03-23 14:15:00 | confirmed | 6 | pass |
| PA01 Tweezer top | top | short | AADHARHFC 1H 2026-08-04 09:15:00 | confirmed | 6 | pass |
| PA02 Inside bar (cluster), break down | cluster_break_down | short | AADHARHFC 1H 2026-08-21 09:15:00 | confirmed | 7 | pass |
| PA02 Inside bar (cluster), break up | cluster_break_up | long | AADHARHFC 1H 2026-06-02 14:15:00 | confirmed | 7 | pass |
| PA02 Inside bar (first), break down | first_break_down | short | AADHARHFC 1H 2026-09-07 09:15:00 | confirmed | 7 | pass |
| PA02 Inside bar (first), break up | first_break_up | long | AADHARHFC 1H 2026-06-19 14:15:00 | confirmed | 7 | pass |
| PA02 Inside bar (nested), break down | nested_break_down | short | AADHARHFC 1H 2026-09-15 09:15:00 | confirmed | 7 | pass |
| PA02 Inside bar (nested), break up | nested_break_up | long | AADHARHFC 1H 2025-05-05 10:15:00 | confirmed | 7 | pass |
| PA03 Outside bar, bearish close | bearish_close | short | AADHARHFC 1H 2026-05-13 09:15:00 | confirmed | 6 | pass |
| PA03 Outside bar, bullish close | bullish_close | long | AADHARHFC 1H 2026-08-28 12:15:00 | confirmed | 6 | pass |
| PA04 Pin bar, lower shadow | lower_shadow | long | AADHARHFC 1H 2026-04-24 14:15:00 | confirmed | 6 | pass |
| PA04 Pin bar, lower shadow after decline | lower_shadow_context | long | AADHARHFC 1H 2026-04-24 14:15:00 | confirmed | 6 | pass |
| PA04 Pin bar, upper shadow | upper_shadow | short | AADHARHFC 1H 2026-09-07 09:15:00 | confirmed | 6 | pass |
| PA04 Pin bar, upper shadow after advance | upper_shadow_context | short | AADHARHFC 1H 2026-09-07 09:15:00 | confirmed | 6 | pass |
| PA05 Inside NR4, break down | inside_nr4_break_down | short | AADHARHFC 1H 2026-09-03 15:15:00 | confirmed | 7 | pass |
| PA05 Inside NR4, break up | inside_nr4_break_up | long | AADHARHFC 1H 2026-09-03 09:15:00 | confirmed | 7 | pass |
| PA05 NR4 narrow range, break down | nr4_break_down | short | AADHARHFC 1H 2026-09-03 15:15:00 | confirmed | 6 | pass |
| PA05 NR4 narrow range, break up | nr4_break_up | long | AADHARHFC 1H 2026-09-11 14:15:00 | confirmed | 6 | pass |
| PA05 NR7 narrow range, break down | nr7_break_down | short | AADHARHFC 1H 2026-09-03 15:15:00 | confirmed | 6 | pass |
| PA05 NR7 narrow range, break up | nr7_break_up | long | AADHARHFC 1H 2026-09-11 14:15:00 | confirmed | 6 | pass |
| PA06 Bearish fakey | bear | short | AADHARHFC 1H 2024-11-25 12:15:00 | confirmed | 10 | pass |
| PA06 Bullish fakey | bull | long | AADHARHFC 1H 2025-05-30 12:15:00 | confirmed | 10 | pass |
| PA07 Falling window | falling | short | AADHARHFC 1H 2026-03-19 12:15:00 | confirmed | 9 | pass |
| PA07 Rising window | rising | long | AADHARHFC 1H 2026-01-02 11:15:00 | confirmed | 9 | pass |
| PA08 Gap-and-reversal, bearish | bearish | short | AADHARHFC 1H 2026-05-21 12:15:00 | confirmed | 10 | pass |
| PA08 Gap-and-reversal, bullish | bullish | long | AADHARHFC 1H 2025-01-10 12:15:00 | confirmed | 10 | pass |

## No frozen occurrence

| Pattern | Variant | Side |
|---|---|---|
