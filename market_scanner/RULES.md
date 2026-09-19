# KANIDA detector rules · version 1.0.1

This is a deterministic first implementation. A match qualifies against the rules below. Detection precision has not yet been measured against a trader-labeled market dataset. Pattern fit is a ranking score, not a confidence percentage or a prediction of profit. A zero-result cell is valid.

All enabled detectors run independently on every supported timeframe. Only one best-fitting instance per stock / pattern / timeframe is returned; a stock may qualify for several different patterns. The enabled list is configurable in config.json.

## Closed candles and data quality

Source timestamps use Asia/Kolkata. Intraday candles aggregate actual 5-minute bars. 1H intervals start at 09:15, ending with a 15:15–15:30 stub. 4H intervals are 09:15–13:15 and 13:15–15:30; the closing interval is shorter than four hours. Intervals never cross sessions. A 2-minute finalization delay applies.

Every expected source bar must exist. Weekly candles require every scheduled daily session. No prices are forward-filled. Invalid OHLC, incomplete buckets, gaps inside a candidate, extreme open-to-previous-close changes above 35%, and structures with over 5% zero-volume candles are excluded. The latest 260 complete bars are retained per timeframe. Global minimum history is 40 bars.

## Setups versus confirmed breakouts

A qualified setup has a complete geometric structure but no confirmed breakout. Confirmation requires a close at least 0.12 ATR beyond the boundary, a fresh crossing within the last three closed candles, and volume at least 1.2 times the median of the prior 20 comparable session-slot candles. Price must remain beyond the boundary and within 2.5 ATR. Neckline and rim setups must be within 2.5 ATR of the boundary. Old or immediately failed breakouts are excluded.

## Pivots and ranking

A pivot must be the unique extreme of a seven-bar window; three later closed bars confirm it. Scores start at geometry-specific baselines and reward containment, low residual errors, symmetry, touches, or shape fit. Scores below 72 are excluded and scores cap at 99. These hand-designed scores have no empirical probability interpretation.

## Cup & Handle

Rims are 24–150 bars apart. The bottom must fall in the central half of the cup. Depth exceeds both 3 ATR and 4% of rim price, and stays below 40% of rim price. Rims align within 22% of depth. A preceding 20-bar advance must exceed half the cup depth.

Quadratic bowl R² must be at least 0.78, with mean-square error at least 10% lower than a sharp V fit. Price spends 18–60% of the cup near its floor. Handle length is under 45% of cup width, handle depth is 7–40% of cup depth, its low precedes the current bar, and mean volume contracts. Rim breaks use the common confirmation rules.

## Horizontal Breakout

Search 24, 36, 48, 64, 90, and 120-bar bases. At least three confirmed swing highs test resistance within 0.55 ATR and span 40% of the base. Support varies by over 1.2 ATR. Only a fresh volume-confirmed close above resistance qualifies. An unbroken range is not a breakout.

## Flag & Pole

The impulse exceeds 5 ATR with directional path efficiency at least 72% and volume at least 1.2 times the prior mean. Search 10, 14, 18, and 24-bar counter-trend flags. Consolidation fits parallel boundaries, retraces 8–50% of the pole, stays narrower than half the pole, and has mean volume no more than 85% of pole volume. Both bullish and bearish flags are detected.

## Symmetrical Triangle

Falling resistance converges with rising support. At least three pivots on each boundary span 48% of the window, with at least five alternating reactions. At least 90% of bars remain within the lines with 0.7 ATR tolerance. Boundary RMS fit error is at most 0.65 ATR. Final width is 20–80% of initial width. The apex must be ahead and no farther than 1.5 window lengths. Setups are neutral; breakout direction is observed.

## Falling Wedge and Rising Wedge

The same repeated-pivot, containment and convergence gates apply. Falling wedges have two negative slopes, with resistance falling faster. Rising wedges have two positive slopes, with support rising faster. Opposite-direction breaks invalidate the directional result.

## Channel

The repeated-pivot and containment gates apply. Boundaries remain parallel: slope divergence over the window is less than both 0.9 ATR and 18% of the initial width. Final width is 80–120% of initial width. Broken channels do not qualify as active channels.

## Descending Triangle

Falling resistance converges on horizontal support. The pivot, fit, containment and convergence gates apply. Support slope is within 0.45 ATR per window. An upside break invalidates the bearish structure.

## Head & Shoulders and Inverse Head & Shoulders

Three consecutive confirmed peaks or troughs define shoulders and head. Two intervening opposite extrema define the neckline. Width is at least 16 bars and depth at least 4 ATR. Head prominence is 18–65% of depth beyond both shoulders. Shoulder levels differ by at most 23% of depth; left/right duration ratio is 0.45–2.2. Neckline levels differ by at most 30% of depth. A preceding advance or decline of at least half the depth is required. Price beyond the right shoulder invalidates the structure. The supplied reference images informed these labels and geometry.

## Scope and limits

The universe includes every active NSE record labeled STOCK or EQ in the database, including stocks with no prices so coverage remains visible. It does not include exchange listings absent from the database. Corporate-action adjustments of the raw OHLC have not been independently verified. Large discontinuities are excluded, but smaller corporate actions can still affect geometry.

Historical results show the actual candle date. Current-only results require each stock's candle end to equal the latest expected exchange close for its timeframe. The supplied database ends in July 2026. There is no live feed, ingestion service, broker connection or order execution in this app.

## Automatic scanning and persistence

The scheduler runs while the local server is running, polling every 60 seconds. Completed candle boundaries, new source commits, delayed data and corrected candles trigger rescanning. Each batch atomically replaces its timeframe snapshot, including removing patterns that no longer qualify. Restart serves the persisted snapshot immediately, then checks the database. A full first scan takes several minutes; reading cached matches takes seconds or less.

## Calendar maintenance

The configured 2026 calendar includes normal NSE holidays and the February 1 budget session. Pre-2026 historical sessions use observed daily dates from RELIANCE and AMRUTANJAN. Extraordinary historical sessions may need explicit timing overrides. The November 8, 2026 Muhurat session is excluded until its actual times are configured. Add published special-session times and future calendar years to config.json and restart.
