# KANIDA chart and candlestick pattern catalogue

**Proposal v0.1 — 15 September 2026**

Companion: [structured catalogue index](C:/Users/SPS/Documents/Kanida_Falcon/docs/PATTERN_CATALOGUE_PROPOSAL.json). The JSON is an index for planning and review, not an executable detector registry.

## 1. Scope and starting point

Use the OHLCV already in `db/kanida.db`. The vendor API integration is in progress separately and is not a dependency for this research. Start with the existing universe of 1,431 active NSE equity metadata records and the existing 1H, 4H, 1D and 1W timeframes. Evaluate each stock on the complete history actually available for that timeframe.

This proposal contains **107 catalogue entries**:

| Group | Entries | Scope |
|---|---:|---|
| Chart patterns | 28 | The 10 existing detector IDs plus 18 additions; several entries contain directional variants. |
| Candlestick reference detectors | 61 | Complete coverage of the 61 TA-Lib candlestick function IDs, including overlapping shape classifications. |
| Additional price-action families | 8 | Explicitly defined additions such as tweezers, inside bars and narrow-range patterns. |
| Harmonic patterns | 10 | Advanced research candidates, after the main chart/candlestick expansion. |

**107 entries does not mean 107 independent trading strategies.** A shape, its bullish/bearish variants, a confirmation rule and an exit rule are separate concepts. Some entries intentionally overlap; the catalogue records these relationships rather than claiming independent evidence for each label. “All patterns” means this named, versioned scope; it is not a claim that every named pattern in every trading tradition has been enumerated.

The candlestick coverage reference is the [official TA-Lib function catalogue](https://ta-lib.org/functions/). Chart-family references include [StockCharts ChartSchool](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/chart-patterns). Definitions below describe proposed detection semantics, not expected profitability.

### Verified local baseline

- The active chart research run is `0e15f954dc432754`, with all 1,431 stocks processed and 74,412 stock/pattern/timeframe/direction result records. These include no-occurrence and insufficient-evidence records.
- Saved research spans as early as 1 January 2013 and as late as 31 July 2026, depending on stock and timeframe. The research run date is not the market-data date.
- The current 10-detector batch includes a single 60/20/20 chronological training/validation/test split. It is not a rolling walk-forward batch.
- Four completed rolling walk-forward study files were found, all for TITAN/channel/1H. Full-universe rolling coverage remains work to do.
- OHLCV is already separate from `market_scanner/output/backtests.sqlite3`. Frozen candles are in `output/history/`; custom study jobs/results are in `output/studies/`.
- This document adds a proposal only. It does not register new detectors, migrate databases, or claim new research results.

Local implementation references: [current registry](C:/Users/SPS/Documents/Kanida_Falcon/market_scanner/config.json), [detector rules](C:/Users/SPS/Documents/Kanida_Falcon/market_scanner/RULES.md), [backtesting](C:/Users/SPS/Documents/Kanida_Falcon/market_scanner/BACKTESTING.md), and [rolling studies](C:/Users/SPS/Documents/Kanida_Falcon/market_scanner/STUDIES.md).

## 2. Shared definitions and variants

### Candle vocabulary

- **Up candle:** close above open. **Down candle:** close below open. An equal open and close has no candle direction.
- **Body:** absolute difference between open and close. **Range:** high minus low. **Upper/lower shadow:** the part above/below the body.
- **ATR:** average true range, a measure of recent price movement. Use only observations available at the signal time, with a saved calculation version.
- **Body gap:** the two real-body intervals do not overlap. **Full-range gap/window:** the entire high-low intervals do not overlap. An opening gap from the previous close is a third, different measurement.
- **Prior trend:** measured before the pattern begins. Proposed new-pattern context rule: fit the previous 20 closes; classify up/down only if the fitted change exceeds +1/-1 prior ATR. Otherwise use neutral context. This is a KANIDA research choice, not a universal definition.
- **Long, short, near and equal in TA-Lib rows:** use the chosen TA-Lib release's candle settings. Save the library version, settings and penetration parameters explicitly. Do not substitute hand-picked ratios while presenting the detector as identical to TA-Lib.

TA-Lib exposes configurable candle settings, and individual detectors do not necessarily check the trend their traditional interpretation assumes. Several shape functions emit a positive or negative integer that represents candle color or recognition state, not a trade recommendation. See the [TA-Lib API](https://ta-lib.org/api/), [Doji](https://ta-lib.org/functions/cdldoji.html), and [High-Wave](https://ta-lib.org/functions/cdlhighwave.html) descriptions.

### Three kinds of variants

1. **Structural variants:** bull/bear, top/bottom, ascending/descending, classic/inverse. Preserve these in the pattern identity.
2. **Detection-definition variants:** canonical gap rules versus a specifically named gap-free adaptation; strict versus relaxed equality; ordinary versus extended harmonic ratios. Each needs its own version and results.
3. **Strategy variants:** enter after detection, enter after confirmation, or enter after a retest; different stops and holding periods. These share the detected occurrence but have separate strategy IDs.

Keep a candlestick's `shape_id`, `prior_trend`, `interpretation`, and `trade_side` separate. For example, a lower-shadow shape after a decline can qualify as a hammer; the related shape after an advance can qualify as a hanging man. Their actual canonical geometrical checks also differ.

### Detection states

| State | Meaning | What can be tested |
|---|---|---|
| Developing candidate | A partial structure exists but required components are absent. | Separate future feature; requires a precise partial-pattern rule and timestamps. Do not infer it by looking backward from completed patterns. |
| Setup / detected | All shape requirements are present on closed candles, but a separate price confirmation has not occurred. | Next eligible candle-open entry, if that family has a predeclared direction. |
| Confirmed | The predeclared close-based confirmation has occurred. Some multi-candle detectors already include this step. | Entry after the actual confirmation timestamp. |
| Invalidated / expired | A defined failure boundary was breached, or confirmation did not arrive in time. | Retain the event and its observed outcome. |

The existing scanner's `setup` means complete geometry awaiting breakout. It does not mean an unfinished live candle. A one-candle pattern is detected only when that candle closes; a three-candle pattern cannot be claimed on candle two.

**Proposed common candle confirmation:** within the next three complete candles, close beyond the signal structure's high for a bullish setup or low for a bearish setup by 0.12 prior ATR. Record detection and confirmation separately. For patterns with intrinsic confirmation, use that detector's own confirmation event; do not silently add an extra fourth or fifth candle.

**Neutral shapes:** show the shape without a directional recommendation. Define separate upside-break and downside-break strategies; do not interpret a positive library output as a long entry. A descriptive long/short forward-return study may also be stored, explicitly labeled as such.

## 3. Chart-pattern catalogue — 28 entries

`Existing` refers to the exact current ID, not every variant proposed in its row. Retain current v1.0.1 results and rules; any added direction or changed geometry gets a new definition version.

### Existing families

| ID | Pattern and variants | Definition and confirmation | Coverage |
|---|---|---|---|
| CH01 | Cup & handle; later inverse variant separately | Rounded bottom between comparable rims after an advance, followed by a smaller handle. Confirm above the rim/handle resistance. Keep current bowl-fit, depth and handle gates. | Existing `cup_handle`; bullish. |
| CH02 | Horizontal breakout | Repeated tests of a flat resistance level followed by a fresh qualifying close above it. Unlike a rectangle, this does not require flat support. | Existing `horizontal_breakout`; confirmed bullish only. |
| CH03 | Flag & pole; bull and bear | Efficient directional impulse followed by a smaller counter-trend parallel consolidation with contracting volume. Confirm out of the flag in the pole direction. | Existing `flag_pole`; both directions. |
| CH04 | Symmetrical triangle; upside/downside break | Lower swing highs and higher swing lows form converging boundaries. The completed shape is neutral; the observed breakout supplies direction. | Existing `symmetrical_triangle`; both breakout directions. |
| CH05 | Falling wedge; reversal/continuation context | Both boundaries fall and narrow; upper boundary descends faster. Bullish confirmation crosses the upper line. Label preceding decline versus a pullback within an advance separately. | Existing `falling_wedge`; proposed context tags need new research. |
| CH06 | Rising wedge; reversal/continuation context | Both boundaries rise and narrow; lower boundary rises faster. Bearish confirmation crosses the lower line. Keep context separate from shape. | Existing `rising_wedge`; proposed context tags need new research. |
| CH07 | Price channel; rising, falling, horizontal | Approximately parallel boundaries with repeated alternating touches. Separate trading a contained channel from a breakout/retest of its boundary. | Existing `channel`; current implementation excludes confirmed breaks. |
| CH08 | Descending triangle; downside/upside resolution | Flat support and falling resistance converge. Preserve the current bearish definition; an upside resolution would be a new variant, not an existing bearish success. | Existing `descending_triangle`; downside only. |
| CH09 | Head & shoulders top; flat/sloped neckline | Three peaks, with the middle highest, following an advance. The intervening troughs form a neckline. Confirm with a close below it. | Existing `head_shoulders`. |
| CH10 | Inverse head & shoulders; flat/sloped neckline | Three troughs, with the middle lowest, following a decline. Intervening peaks form the neckline. Confirm with a close above it. | Existing `inverse_head_shoulders`. |

The exact implemented numerical gates are in the [local detector rules](C:/Users/SPS/Documents/Kanida_Falcon/market_scanner/RULES.md). General terminology: [cup with handle](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/chart-patterns/cup-with-handle), [flags and pennants](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/chart-patterns/flag-pennant), and [price channels](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/chart-patterns/price-channel).

### Proposed additions

The definitions in this table are operational proposals. Pattern-specific numerical tolerances still need to be frozen and tested before implementation is considered complete.

| ID | Pattern and variants | Definition and confirmation | Priority |
|---|---|---|---|
| CH11 | Ascending triangle; upside/downside resolution | Flat resistance and rising support converge. Primary variant confirms above resistance; opposite-side failure/resolution is recorded separately. [Reference](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/chart-patterns/ascending-triangle). | 1 |
| CH12 | Horizontal breakdown | Mirror of CH02: repeated flat support tests followed by a fresh qualifying close below support. Retain distinct support/resistance event IDs. | 1 |
| CH13 | Rectangle / trading range; top/bottom, upside/downside | Repeated approximately equal highs and lows form two horizontal boundaries. Neutral while contained; confirm on a close outside. A channel with negligible slope is the same geometric family, so link aliases. [Reference](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/chart-patterns/rectangle). | 1 |
| CH14 | Pennant; bull/bear | A directional pole followed by a short converging consolidation; unlike a flag, boundaries are not parallel. Confirm in the pole direction. [Reference](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/chart-patterns/flag-pennant). | 1 |
| CH15 | Double top; Adam/Adam, Adam/Eve, Eve/Adam, Eve/Eve | Two separated comparable peaks following an advance; require an intervening trough. Confirm below that trough. Adam means a sharp peak, Eve a rounded peak; these subtype classifiers need independent shape rules. [Reference](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/chart-patterns/double-top-reversal). | 1; subtype work 2 |
| CH16 | Double bottom; the same four shape combinations | Two separated comparable troughs following a decline. Confirm above the intervening peak. Reject pairs without sufficient separation or intervening recovery. | 1; subtype work 2 |
| CH17 | Triple top | Three separated comparable peaks after an advance. Confirm below the support defined by intervening troughs. Link the earlier double-top candidate rather than rewriting its history. | 1 |
| CH18 | Triple bottom | Three separated comparable troughs after a decline. Confirm above intervening resistance. Earlier two-trough events retain their original timestamps. [Reference](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/chart-patterns/triple-bottom-reversal). | 1 |
| CH19 | Rounding bottom / saucer | A sustained transition from falling to flat to rising prices, fitted as a bowl, without requiring a handle. Confirm above the prior rim resistance. [Reference](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/chart-patterns/rounding-bottom). | 2 |
| CH20 | Rounding top / dome | Proposed geometric mirror of CH19: rising to flat to falling prices. Confirm below rim support. Keep mirror assumptions explicit. | 2 |
| CH21 | Inverted cup & handle | Proposed bearish mirror of CH01: rounded dome, shallow upward handle after a decline, then confirmation below support. Not a relabeling of any ordinary rounded top. | 2 |
| CH22 | Broadening / megaphone; top, bottom, ascending, descending | Alternating swings diverge so the range expands. Tag slope and prior trend; confirm beyond the relevant outer boundary. A broadening wedge is a sloped subtype, not a converging wedge. | 2 |
| CH23 | Diamond; top, bottom, continuation | Broadening swings transition to contraction, with a distinct widest middle. Confirm through the final converging boundary. Prior trend and resolution determine the context label. | 3 |
| CH24 | Island reversal; top/bottom, single/multi-bar island | A full-range gap isolates one or more bars, followed by a full-range gap in the opposite direction. Confirmation exists only after the second gap; no missing-data gap may stand in for a price gap. | 2 |
| CH25 | V bottom / inverted V top | Proposed short, steep decline followed by rapid recovery, or its mirror; deliberately excludes a rounded bowl. Confirm only after recovery crosses a predeclared level, never at the extreme in retrospect. | 3 |
| CH26 | Measured move; bullish/bearish | Directional leg, corrective consolidation, then a second leg in the original direction. Detect at the second-leg breakout; an equal-leg projection is a reference target, not a prerequisite learned from future price. [Reference](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/chart-patterns/measured-move-bullish). | 2 |
| CH27 | Bump-and-run reversal; top/bottom | A moderate trend accelerates into a steeper extension, then breaks its prior trend support/resistance. Measure acceleration in normalized price per bar, not screen angles. The bottom version is a proposed mirror. [Reference](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/chart-patterns/bump-and-run-reversal). | 3 |
| CH28 | Volatility-contraction base; bullish/bearish research variants | KANIDA proposal: at least three successive smaller pullbacks within a base, decreasing normalized ranges and contracting volume. Confirm beyond the base boundary. This is a geometric contraction model, not a claim to reproduce a complete named discretionary trading system. | 2 |

Diamond, broadening and island family names are also represented in [Fidelity's technical-event inventory](https://www.fidelity.com/quick-content/etf/help/research/learn_er_quotes.shtml). Mirror definitions and priority assignments above are KANIDA proposals.

### Common proposed chart gates

- Reuse confirmed seven-bar pivots initially; a pivot's availability timestamp is three bars after its extreme. Never put the trading signal on the earlier extreme.
- Start new multi-swing searches with 20–150 bars, minimum five bars between separate major turning points, and at least 2 ATR of pattern height. These are starting specifications to review on fixtures, not fitted outcomes.
- Use a starting equality tolerance of 0.5 ATR for repeated levels. For multi-touch boundaries require at least two separated touches per side and retain fit residuals; use stricter existing gates for unchanged detectors.
- Require the configured breakout buffer of 0.12 ATR. Save price confirmation separately from volume confirmation; preserve the existing 1.2-times-volume gate for legacy variants.
- Every new pattern must have an explicit failure boundary and expiry rule before testing. Suggested default expiry for an unconfirmed new chart setup: ten subsequent closed bars or a geometry failure, whichever comes first. Family-specific exceptions must be named.
- Do not relax these gates because a backtest produced few trades. A legitimate result is “no qualifying occurrences.”

## 4. Candlestick catalogue — all 61 reference detectors

All entries below are **new relative to the current scanner**. Links identify each official detector specification. These are short recognition descriptions; the linked implementation and pinned candle settings supply the exact comparisons. The wrapper adds pre-pattern trend context where required. Bodies are distinct from full high-low ranges.

### Single-candle shapes and context-dependent forms — 17

| Function ID | Pattern / variants | Recognition definition and interpretation |
|---|---|---|
| CDLDOJI | [Doji](https://ta-lib.org/functions/cdldoji.html) | Body is at or below the configured doji threshold. Neutral shape; trade direction requires a separate rule. |
| CDLLONGLEGGEDDOJI | [Long-legged doji](https://ta-lib.org/functions/cdllongleggeddoji.html) | Doji with at least one long shadow under the reference implementation. A stricter both-shadows definition would be a separate variant. Neutral. |
| CDLDRAGONFLYDOJI | [Dragonfly doji](https://ta-lib.org/functions/cdldragonflydoji.html) | Doji near the range high, negligible upper shadow and a nontrivial lower shadow. Direction depends on context and confirmation. |
| CDLGRAVESTONEDOJI | [Gravestone doji](https://ta-lib.org/functions/cdlgravestonedoji.html) | Doji near the range low, negligible lower shadow and a nontrivial upper shadow. Context-dependent, not automatically a short entry. |
| CDLRICKSHAWMAN | [Rickshaw man](https://ta-lib.org/functions/cdlrickshawman.html) | Doji near the middle of its range with long shadows on both sides. Neutral; more restrictive than generic doji. |
| CDLTAKURI | [Takuri](https://ta-lib.org/functions/cdltakuri.html) | Dragonfly-like doji with an exceptionally long lower shadow. Proposed bullish interpretation requires a preceding decline. |
| CDLSPINNINGTOP | [Spinning top](https://ta-lib.org/functions/cdlspinningtop.html) | Small body; both shadows exceed body length. Up/down candle colors are variants of a neutral shape. |
| CDLHIGHWAVE | [High-wave candle](https://ta-lib.org/functions/cdlhighwave.html) | Small body and very long shadows on both sides. Neutral; library sign represents color. |
| CDLSHORTLINE | [Short-line candle](https://ta-lib.org/functions/cdlshortline.html) | Short body and short shadows. Up/down colors are descriptive; use as a shape/filter or a separately confirmed strategy. |
| CDLLONGLINE | [Long-line candle](https://ta-lib.org/functions/cdllongline.html) | Long body and short shadows. Up/down momentum form; classify continuation/reversal only from prior context. |
| CDLMARUBOZU | [Marubozu; up/down](https://ta-lib.org/functions/cdlmarubozu.html) | Long body with both shadows negligible. Direction follows body color; the shape alone does not establish a reversal. |
| CDLCLOSINGMARUBOZU | [Closing marubozu; up/down](https://ta-lib.org/functions/cdlclosingmarubozu.html) | Long body ending near the favorable extreme: up candle closes near high, down candle near low. The opening-end shadow may remain. |
| CDLBELTHOLD | [Belt hold; bullish/bearish](https://ta-lib.org/functions/cdlbelthold.html) | Long body opens near its low for the up form or near its high for the down form. Opposite-end shadow is allowed. |
| CDLHAMMER | [Hammer](https://ta-lib.org/functions/cdlhammer.html) | Small upper-positioned body, long lower shadow, minimal upper shadow, near the previous low. Require a preceding decline for the bullish reversal study. |
| CDLHANGINGMAN | [Hanging man](https://ta-lib.org/functions/cdlhangingman.html) | Small body, long lower shadow, minimal upper shadow, near the previous high. Require a preceding advance for the bearish study. |
| CDLINVERTEDHAMMER | [Inverted hammer](https://ta-lib.org/functions/cdlinvertedhammer.html) | Small body with a long upper shadow and little lower shadow; reference definition also requires a downward body gap. Bullish context after a decline. |
| CDLSHOOTINGSTAR | [Shooting star](https://ta-lib.org/functions/cdlshootingstar.html) | Small body, long upper shadow, little lower shadow and an upward body gap in the reference definition. Bearish context after an advance. |

### Two-candle relationships — 15

| Function ID | Pattern / variants | Recognition definition and interpretation |
|---|---|---|
| CDLENGULFING | [Engulfing; bullish/bearish](https://ta-lib.org/functions/cdlengulfing.html) | Second opposite-color body encloses the first body. Full-range engulfing is different. Preserve the library's edge-equality recognition grades as metadata. |
| CDLHARAMI | [Harami; bullish/bearish](https://ta-lib.org/functions/cdlharami.html) | Long body followed by a short body inside it. Direction is opposite the first candle; record second-candle color rather than silently requiring it to differ. |
| CDLHARAMICROSS | [Harami cross; bullish/bearish](https://ta-lib.org/functions/cdlharamicross.html) | Harami whose second body is a doji. Link to the parent harami occurrence when both match. |
| CDLPIERCING | [Piercing](https://ta-lib.org/functions/cdlpiercing.html) | Long down candle, then long up candle opening below the prior low and recovering beyond the first body's midpoint while remaining below its open. Bullish. |
| CDLDARKCLOUDCOVER | [Dark cloud cover](https://ta-lib.org/functions/cdldarkcloudcover.html) | Long up candle, then down candle opening above the prior high and penetrating sufficiently into the first body without fully engulfing it. Bearish; save penetration setting. |
| CDLDOJISTAR | [Doji star; bullish/bearish context](https://ta-lib.org/functions/cdldojistar.html) | Long body followed by a doji whose body gaps away in the first candle's direction. This is a setup, not the completed three-candle morning/evening form. |
| CDLCOUNTERATTACK | [Counterattack; bullish/bearish](https://ta-lib.org/functions/cdlcounterattack.html) | Two long opposite-color bodies finish at approximately the same close. Second candle supplies the conventional direction. |
| CDLHOMINGPIGEON | [Homing pigeon](https://ta-lib.org/functions/cdlhomingpigeon.html) | Long down body followed by a smaller down body contained within it. Bullish-context study after a decline; overlaps same-color harami geometry. |
| CDLMATCHINGLOW | [Matching low](https://ta-lib.org/functions/cdlmatchinglow.html) | Two down candles close at approximately the same level. A close-based support test, unlike tweezers, which compare lows. |
| CDLKICKING | [Kicking; bullish/bearish](https://ta-lib.org/functions/cdlkicking.html) | Opposite-color marubozu candles separated by a full-range gap. Conventional direction follows the second candle. |
| CDLKICKINGBYLENGTH | [Kicking by length](https://ta-lib.org/functions/cdlkickingbylength.html) | Same kicking geometry, but direction follows the longer marubozu. A direction-policy variant, not independent evidence of a second occurrence. |
| CDLSEPARATINGLINES | [Separating lines; bullish/bearish](https://ta-lib.org/functions/cdlseparatinglines.html) | Opposite-color candles open near the same price; second is a long belt-hold candle. Continuation direction follows the second body, with matching prior trend required. |
| CDLONNECK | [On-neck](https://ta-lib.org/functions/cdlonneck.html) | Long down candle; next up candle opens below its low and closes near that low. Bearish continuation context. |
| CDLINNECK | [In-neck](https://ta-lib.org/functions/cdlinneck.html) | Long down candle; next up candle opens below its low and closes just inside the prior body, near the prior close. Bearish continuation context. |
| CDLTHRUSTING | [Thrusting](https://ta-lib.org/functions/cdlthrusting.html) | Long down candle; next up candle opens below its low and recovers into the body, beyond the near-close area but below its midpoint. Bearish continuation context. |

### Three-candle sequences — 20

| Function ID | Pattern / variants | Recognition definition and interpretation |
|---|---|---|
| CDLMORNINGSTAR | [Morning star](https://ta-lib.org/functions/cdlmorningstar.html) | Long down body, small downward body-gapped star, then an up candle penetrating the first body. Bullish; save penetration threshold. |
| CDLEVENINGSTAR | [Evening star](https://ta-lib.org/functions/cdleveningstar.html) | Long up body, small upward body-gapped star, then a down candle penetrating the first body. Bearish. |
| CDLMORNINGDOJISTAR | [Morning doji star](https://ta-lib.org/functions/cdlmorningdojistar.html) | Morning-star structure with a doji as the middle candle. More specific subtype; avoid duplicate recommendations. |
| CDLEVENINGDOJISTAR | [Evening doji star](https://ta-lib.org/functions/cdleveningdojistar.html) | Evening-star structure with a middle doji. Same subtype relationship. |
| CDLABANDONEDBABY | [Abandoned baby; bottom/top](https://ta-lib.org/functions/cdlabandonedbaby.html) | Middle doji isolated by full-range gaps on both sides; third body reverses into the first. Stricter than a doji star. |
| CDL3WHITESOLDIERS | [Three white soldiers](https://ta-lib.org/functions/cdl3whitesoldiers.html) | Three up candles with progressively higher closes, opens within/near prior bodies and short upper shadows, subject to body-size checks. Bullish-context study. |
| CDL3BLACKCROWS | [Three black crows](https://ta-lib.org/functions/cdl3blackcrows.html) | Three descending down candles, opening inside preceding down bodies and closing near lows. The reference detector also checks a preceding up candle, so its input pattern uses four bars. |
| CDLIDENTICAL3CROWS | [Identical three crows](https://ta-lib.org/functions/cdlidentical3crows.html) | Three descending down candles with short lower shadows, each later open near the preceding close. More specific bearish form. |
| CDL3INSIDE | [Three inside up/down](https://ta-lib.org/functions/cdl3inside.html) | Harami pair followed by an opposite-to-first candle closing beyond the first open. The third candle already supplies confirmation. |
| CDL3OUTSIDE | [Three outside up/down](https://ta-lib.org/functions/cdl3outside.html) | Engulfing pair followed by a further close in the engulfing direction. Preserve parent-pair relationship and later confirmation time. |
| CDL2CROWS | [Two crows](https://ta-lib.org/functions/cdl2crows.html) | Up candle, upward body-gapped down candle, then another down candle opening inside the second body and closing within the first up body. Three candles despite the name. |
| CDLUPSIDEGAP2CROWS | [Upside-gap two crows](https://ta-lib.org/functions/cdlupsidegap2crows.html) | Long up body, gapped small down body, then a down body engulfing the second but still closing above the first close. Distinct from two crows. |
| CDLADVANCEBLOCK | [Advance block](https://ta-lib.org/functions/cdladvanceblock.html) | Three higher-closing up candles whose bodies/shadows show a weakening advance. Bearish warning setup; do not assume every occurrence reverses. |
| CDLSTALLEDPATTERN | [Stalled pattern / deliberation](https://ta-lib.org/functions/cdlstalledpattern.html) | Three higher-closing up candles; the last is a small body near the top of the preceding long candle. Bearish-context setup. |
| CDL3STARSINSOUTH | [Three stars in the south](https://ta-lib.org/functions/cdl3starsinsouth.html) | Three down candles with shrinking bodies/ranges: a long lower-shadow first candle, smaller second, then a small contained marubozu. Bullish-context study; use exact reference low relationships. |
| CDLUNIQUE3RIVER | [Unique three-river bottom](https://ta-lib.org/functions/cdlunique3river.html) | Long down candle, down harami body making a lower low, then a small up candle in the lower area. Bullish traditional interpretation; confirmation tested separately. |
| CDLTRISTAR | [Tristar; bullish/bearish](https://ta-lib.org/functions/cdltristar.html) | Three doji; the middle body gaps away and the third retreats toward the opposite side. Direction depends on whether the middle is the upper or lower star. |
| CDLSTICKSANDWICH | [Stick sandwich](https://ta-lib.org/functions/cdlsticksandwich.html) | Down/up/down sequence with approximately equal first and third closes and the middle candle trading above that level. Bullish-context support pattern. |
| CDLGAPSIDESIDEWHITE | [Gap side-by-side white lines; up/down gap](https://ta-lib.org/functions/cdlgapsidesidewhite.html) | Two similar up bodies with nearby opens both stand across a body gap from the first candle. Direction follows gap direction, even when the later candles are up. |
| CDLTASUKIGAP | [Tasuki gap; upside/downside](https://ta-lib.org/functions/cdltasukigap.html) | Body gap in one direction, then an opposite-color candle opening within the preceding body and retreating into—but not filling—the gap. Continuation context. |

### Remaining continuation, reversal and trap sequences — 9

| Function ID | Pattern / variants | Recognition definition and interpretation |
|---|---|---|
| CDLXSIDEGAP3METHODS | [Gap three methods; upside/downside](https://ta-lib.org/functions/cdlxsidegap3methods.html) | Three candles: two same-color bodies separated by a gap, then an opposite candle that fills the gap. Distinct from Tasuki's unfilled gap. |
| CDL3LINESTRIKE | [Three-line strike; bullish/bearish](https://ta-lib.org/functions/cdl3linestrike.html) | Three advancing/declining same-color candles followed by a large opposite candle spanning their move. Reference sign follows the first three candles' continuation direction. Any opposite reversal interpretation must be a separate research variant. |
| CDLRISEFALL3METHODS | [Rising/falling three methods](https://ta-lib.org/functions/cdlrisefall3methods.html) | Five candles: long directional body, three smaller opposing candles constrained by reference range checks, then a long candle resuming the original direction. |
| CDLMATHOLD | [Mat hold; bullish reference](https://ta-lib.org/functions/cdlmathold.html) | Five candles: long up candle, upward-gapped small reaction, two further shallow reaction candles, then an up candle above reaction highs. The library detects bullish only; bearish mirror would be custom. |
| CDLBREAKAWAY | [Breakaway; bullish/bearish](https://ta-lib.org/functions/cdlbreakaway.html) | Five candles: long body, same-direction body gap and extension, then an opposite final candle returning into the original gap. |
| CDLLADDERBOTTOM | [Ladder bottom](https://ta-lib.org/functions/cdlladderbottom.html) | Five candles: three declining down candles, a fourth down candle with an upper shadow, then an up candle opening above the fourth open and closing above its high. |
| CDLCONCEALBABYSWALL | [Concealing baby swallow](https://ta-lib.org/functions/cdlconcealbabyswall.html) | Four down candles: two marubozu, a gapped-down third whose upper shadow reaches the prior body, and a fourth engulfing the third's full range. Bullish traditional interpretation. |
| CDLHIKKAKE | [Hikkake; bullish/bearish](https://ta-lib.org/functions/cdlhikkake.html) | Inside bar followed by a directional range displacement; a reversal through the inside-bar boundary within three bars confirms the trap. Preserve setup versus confirmed library outputs. |
| CDLHIKKAKEMOD | [Modified hikkake; bullish/bearish](https://ta-lib.org/functions/cdlhikkakemod.html) | Adds a preceding nested-range condition and a close near an extreme before the ordinary trap displacement. Four-bar setup, optional subsequent confirmation within three bars. |

### Candlestick implementation boundaries

- **All 61 reference IDs are coverage requirements**, not a promise of one user-facing card per function.
- Preserve library distinctions that disagree with casual chart descriptions: full-range versus body gaps; three crows' preceding bar; neutral doji outputs; kicking-by-length direction; bullish-only Mat Hold; the three-line-strike direction convention.
- For no-gap morning/evening stars, no-gap inverted hammers and no-gap shooting stars, use named `gap_free` variants. Do not switch off canonical gap conditions on intraday data without changing identity.
- Candle functions generally use OHLC geometry. Save volume as context and test volume-filtered variants separately; a lack of breakout-volume expansion must not erase every valid candle shape by default.
- A four-price doji (open = high = low = close) is a zero-range subtype. Keep it as a data-quality/shape tag; exclude it from standalone trade entry by default.
- Context-only shapes still receive occurrence and forward-outcome statistics. They need an explicit directional trigger before strategy performance is reported.

## 5. Additional price-action catalogue — 8 families

These are explicit KANIDA definitions, separate from the 61 reference IDs. Numeric defaults below are proposed starting rules. Use the same saved ATR calculation as other new patterns.

| ID | Family / variants | Proposed measurable definition | Confirmation / overlap |
|---|---|---|---|
| PA01 | Tweezer top / bottom | Two adjacent bars test highs/lows within max(one known tick, 0.1 prior ATR); second closes away from the tested level. Require preceding advance/decline. | Confirm through the pair's opposite extreme. Distinct from matching closes. |
| PA02 | Inside bar; single / nested / mother-bar cluster | Entire second range lies within the first, with at least one strict edge. Nested ranges and multiple inside bars share the original mother-bar identity. | Close beyond mother-bar high/low. Body-only containment is harami, not this pattern. |
| PA03 | Outside bar; bullish/bearish close | Second high exceeds first high and second low falls below first low. Tag close in the top/bottom quarter of the second range. | Study next-open entry or later range break separately; distinct from body engulfing. |
| PA04 | Pin bar / rejection bar; upper/lower shadow | Dominant shadow at least twice the body and 60% of total range; opposite shadow at most 15% of range. Exclude zero range; use prior trend as metadata. | Close beyond the short-shadow end. Overlaps hammer/shooting-star families; link shared event. |
| PA05 | Narrow-range contraction; NR4 / NR7 / inside-NR | Current range is strictly smallest among the most recent four/seven complete candles. Inside-NR also satisfies PA02. Equal minima are a separate optional tie variant. | Break above/below the contraction range; direction is unknown at detection. |
| PA06 | Fakey / failed inside-bar breakout; bull/bear | An inside-bar cluster is followed by a breach of a mother-bar edge, then a close back inside within three bars. | Confirm through the opposite edge later; retain failure and confirmation times. Hikkake overlap is annotated, not assumed equivalent. |
| PA07 | Rising / falling window | Current low exceeds prior high, or current high is below prior low. Require both adjacent intervals to be complete; no fabricated bars. | Gap-hold, gap-fill and retest are separate outcome/strategy variants. |
| PA08 | Gap-and-reversal; bullish/bearish | Open beyond the previous full range, then close back inside that range on the same completed candle. Bullish closes in the upper half, bearish in the lower half. | Entry can only follow that close. Distinguish an opening gap reversal from an island requiring two full gaps. |

Terminology references: [StockCharts candle dictionary](https://chartschool.stockcharts.com/table-of-contents/chart-analysis/candlestick-charts/candlestick-pattern-dictionary) and [Fidelity's inside/outside-bar event categories](https://www.fidelity.com/quick-content/etf/help/research/learn_er_quotes.shtml). Exact thresholds in PA01–PA08 are proposed here rather than attributed to those sources.

## 6. Harmonic research catalogue — 10 entries

These are **priority 3 research candidates**. Each has bullish and bearish mirror geometry. X, A, B, C and D name consecutive alternating swing points; for Shark use its O-X-A-B-C notation. Ratios compare absolute leg lengths; the point ordering determines direction.

The table gives defining distinctions, not a complete execution specification. Before implementation, freeze every leg constraint, ratio tolerance, time symmetry rule and pivot-availability rule. If D/C is a swing requiring later bars for confirmation, detection occurs when that swing is known, not retrospectively at the ideal turning price. A projected reversal zone is a candidate, not a filled trade.

| ID | Family | Defining distinction | Variants |
|---|---|---|---|
| HA01 | [AB=CD](https://harmonictrader.com/harmonic-patterns/abcd-pattern/) | Two similarly sized directional legs AB and CD separated by the BC correction; completion is at D. | Bull/bear; price equality and time symmetry recorded separately. |
| HA02 | [Alternate AB=CD](https://harmonictrader.com/harmonic-patterns/alternate-abcd-pattern/) | CD extends the AB-sized projection rather than matching it; distinguish the 1.27 and 1.618 extensions. | Bull/bear; separate extension IDs. |
| HA03 | [Gartley](https://harmonictrader.com/harmonic-patterns/gartley-pattern/) | B near the 0.618 retracement of XA and D near the 0.786 retracement, with supporting AB=CD/BC alignment. | Bull/bear; retracement structure stays inside X. |
| HA04 | [Bat](https://harmonictrader.com/harmonic-patterns/bat-pattern/) | B shallower than 0.618 of XA; D near 0.886; stronger BC extension distinguishes it from Gartley. | Bull/bear; 0.382/0.50 B-point subtypes. |
| HA05 | [Alternate Bat](https://harmonictrader.com/harmonic-patterns/alternate-bat-pattern/) | Shallower B-point structure with terminal extension near 1.13 of XA rather than the standard Bat retracement. | Bull/bear; distinct from an invalid standard Bat. |
| HA06 | [Butterfly](https://harmonictrader.com/harmonic-patterns/butterfly-pattern/) | B near 0.786 of XA; D extends past X, with the ideal 1.27 XA projection and supporting leg relationships. | Bull/bear; wider extensions need separately specified variants. |
| HA07 | [Crab](https://harmonictrader.com/harmonic-patterns/crab-pattern/) | Terminal 1.618 XA extension, with a shallower B retracement and an extreme BC projection. | Bull/bear; keep distinct from Deep Crab. |
| HA08 | [Deep Crab](https://harmonictrader.com/harmonic-patterns/deep-crab-pattern/) | Shares the 1.618 XA terminal extension but B lies near 0.886 of XA. | Bull/bear; deeper B is the distinguishing constraint. |
| HA09 | [Shark](https://harmonictrader.com/harmonic-patterns/shark-pattern/) | Extended impulse returns toward the initial support/resistance region, combining a 0.886–1.13 OX retracement/extension zone with an extreme impulse relationship. | Bull/bear; use O-X-A-B-C notation rather than silently borrowing Gartley labels. |
| HA10 | [5-0](https://harmonictrader.com/harmonic-patterns/5-0/) | Strong BC reversal leg of 1.618–2.24 times AB, followed by a D retracement near half of BC, with reciprocal AB=CD alignment. | Bull/bear; a first-pullback structure rather than an ordinary M/W. |

These references establish family names and geometric distinctions. No external accuracy percentages or profitability claims are adopted.

## 7. Alias, duplication and combination policy

Examples of intentional overlap:

- Doji → dragonfly/gravestone/long-legged/rickshaw/Takuri subtypes.
- Harami → harami cross or homing pigeon; a later three-inside event links to the earlier pair.
- Engulfing → later three-outside confirmation.
- Morning/evening star → doji-star subtype; abandoned baby adds stricter gaps.
- Kicking and kicking-by-length share geometry but differ in directional interpretation.
- Horizontal channel and rectangle can share geometry. Flag and pennant share a pole but differ in consolidation shape.
- Measured move and AB=CD may share swings but have different detection timing and interpretation.

Store all qualifying labels, but group related matches into one user-facing occurrence with subtype tags where appropriate. Historical results must still use the exact definition, context and strategy variant selected on the card.

**Pattern combinations are a later research layer.** Examples: hammer at double-bottom support; engulfing during a wedge breakout; inside bar within a flag. Do not compute every possible pair immediately. First establish standalone occurrences and results, then predeclare a small combination list. A combination receives its own strategy identity and cannot borrow a parent's win rate.

## 8. Using the existing OHLCV correctly

1. Continue with 1H/4H aggregated from complete 5-minute bars and 1D/1W from daily bars. The presence of 1-minute source data does not automatically expand the requested timeframe scope.
2. Record actual source coverage for every stock/timeframe: first candle, last candle, available bars, rejected bars and gaps. Missing history is different from no pattern.
3. Keep the existing 09:15 session alignment and closing stubs. Save actual candle duration and session slot. A 15-minute closing stub is not a full 1H candle; short-range classifications need duration-aware audits. For canonical TA-Lib parity, retain the pinned ordinary candle-settings behavior and label stub involvement. A slot-normalized or full-duration-only model is a separately tested definition.
4. Distinguish valid overnight price gaps from missing observations and suspected corporate-action discontinuities. The existing `gap` quality flag is not a universal price-gap classifier. Gap-family implementation needs this distinction before its results are valid.
5. Canonical gap patterns can be evaluated on every timeframe; report zero or sparse matches honestly. A separately named gap-free adaptation must never populate the canonical pattern's historical card.
6. Existing 40-bar warm-up and 260-bar detector windows remain the initial chart baseline. Candle functions also need their individual lookback plus any trend-context history; avoid pretending a one-candle shape needs no preceding data for its configured thresholds.
7. Freeze the exact candles, source date, aggregation/calendar version, code and rules for each research run. Later vendor data can use the same normalized candle interface, but vendor/adjustment changes require a new snapshot identity.
8. Data ending in July 2026 must be displayed as historical. A scan run in September does not make its July matches current.

## 9. Proposed backtesting and walk-forward plan

### A. Separate descriptive history from strategy selection

For each occurrence, save forward outcomes at fixed horizons using the existing baseline horizons: 6 candles for 1H/4H, 10 for 1D and 4 for 1W. For neutral shapes, label hypothetical long/short forward returns explicitly. These descriptive measurements are not selected-rule out-of-sample results.

For directional strategies, reuse the existing initial candidate grid: holding periods 3/6/12/24 for 1H/4H, 5/10/20/40 for 1D, and 2/4/8/13 for 1W; time exits alone or 1/2 ATR stops with 1/2/3R targets. Here R is the entry-to-stop risk distance. Only apply trigger choices supported by the pattern's state model. Do not double-count a pattern's built-in confirmation as a new independent setup.

Entries begin at the next eligible completed-data candle open after the signal becomes available. Preserve the legacy next-open research assumption and label it; a later execution-latency model must have a different version. No fill at a pivot's earlier extreme or an unobserved intrabar price.

### B. Full rolling walk-forward coverage

Proposed default: **36 months of prior training, followed by six months of testing**, advancing six months at a time. Select rules independently per stock × pattern × timeframe × direction, using prior training data only. Purge trades whose potential holding periods extend beyond training eligibility and retain the existing maximum-hold embargo. Freeze the chosen rule for its next test window.

Retain a minimum of 20 eligible training trades initially and the existing positive mean-minus-standard-error selection score. No passing rule means that cell skips the next test window. A short-history stock may have descriptive results while having no eligible walk-forward window. Record this as insufficient history rather than moving its training boundary into future data.

These 36/6 settings are a proposed starting protocol, consistent with the study worker's defaults, not a claim they are optimal. Weekly and rare structures may produce too few independent trades. The existing final-test sample threshold of 20 remains the initial minimum for an estimate; show raw sample counts below it with insufficient-evidence status.

Results for earlier chronological splits and new rolling folds must remain separately labeled. Data already inspected in old research cannot be newly described as an untouched release holdout. Future observations from the vendor can support a prospective evaluation after definitions are frozen.

### C. Reporting and cost consistency

- Store gross and net returns, assumed costs, win/loss counts, average return, holding time, MFE/MAE (favorable/adverse excursion), and sample uncertainty. Portfolio drawdown requires a specified shared account; do not sum overlapping per-cell returns.
- Initially preserve the legacy 30 bps round-trip fees plus 10 bps round-trip slippage for like-for-like cell research. Align the configurable study worker's fee/slippage semantics explicitly before comparing results; it has a different cash/fill calculation path.
- Bearish multiday equity results remain hypothetical price studies unless actual borrow/contract data supports execution. Existing F&O membership is an equity filter, not futures OHLCV for these studies.
- Keep “not run”, “missing data”, “no occurrences”, “no passing rule”, “small sample”, “tested negative”, and “tested positive” distinct. A passing sample threshold does not mean a profitable result.
- A larger catalogue means more opportunities to select a lucky result. Save all attempted rules and failed variants, disclose how many were compared, and avoid promoting solely on the best displayed backtest. Publication of calibrated probabilities or claims of significance requires a separate statistical validation step.

### D. Data identity and persistence

Proposed research key:

`instrument + timeframe + pattern_id + structural_variant + definition_version + context + side + trigger_version + rule_id + data_snapshot + cost_model + validation_protocol`

Occurrence records also need structure-start, detected-at, confirmed-at, expiry/failure timestamps, constituent candle/pivot indices, aliases and measured geometry. Walk-forward records need training/test boundaries, selected rule, selection score and actual test trades for every fold—including skipped folds.

Reuse the existing separation of source candles, research results and product data. New research can extend a versioned research store while keeping large frozen histories and ledgers in compressed artifacts with checksums. This catalogue does not require copying the 158 GB source database into the user-facing application database.

## 10. Suggested implementation order

| Stage | Work | Completion evidence |
|---|---|---|
| 0 — Foundation | Snapshot existing OHLCV/coverage; normalize pattern/variant IDs; preserve legacy results; define occurrence/state records and gap semantics. | Reproducible source identity and coverage manifest; existing detectors still reproduce their saved behavior. |
| 1 — Common patterns | Add CH11–CH18; add common candlesticks (doji family, hammer/hanging man, inverted hammer/shooting star, engulfing/harami/cross, piercing/dark cloud, morning/evening stars, soldiers/crows); PA01–PA05. | Positive/negative geometry examples, causal replay checks, saved occurrence counts, baseline backtests and rolling folds for every eligible cell. |
| 2 — Complete main catalogue | Implement all remaining reference candle IDs, remaining PA families, and priority-2 chart patterns. Add named gap-free variants after canonical versions are stable. | All 61 reference IDs accounted for; every configured cell has a terminal result or explicit data limitation. |
| 3 — Advanced shapes | Priority-3 chart patterns and HA01–HA10, with complete numerical contracts and pivot-timing tests before research. | Versioned geometric definitions, reproducible drawings and genuinely forward-only signal availability. |
| 4 — Product and refresh | Attach exact matching precomputed results to cards; update scans from available candles; add explicit research-refresh jobs. | Historical and fresh signals clearly dated; no history lookup substitutes a different stock/variant/side. |

The priority order manages implementation and validation work; it does not rank patterns by expected profitability. Full research expansion can begin with the present database without waiting for the vendor API.

## 11. Explicit boundaries for later catalogue versions

Elliott-wave counts, full Wyckoff phases, Wolfe waves, Cypher and three-drives variants, Point & Figure formations, Renko/Heikin-Ashi patterns, opening-range strategies and indicator-cross strategies are not claimed as implemented or covered by the 107 entries. They require additional named definitions, transformed-chart assumptions or a broader strategy scope. They can be added to a subsequent version; “not in v0.1” must remain visible rather than silently omitted from a claim of universal coverage.

No new vendor credentials, cloud-provider decision or user requirement is needed to use this catalogue as the starting research scope. Remaining numerical details are implementation specifications to freeze before each detector's first research run, not reasons to postpone the catalogue.
