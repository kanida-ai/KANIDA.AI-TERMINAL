# Live detection with the 107-pattern research detector set

Owner-facing description of what `market_scanner/pattern_live.py` and
`market_scanner/pattern_warmup.py` do, what was measured, what the switches are,
and what is **not** solved.

Contracts this implements: `docs/DATA_PIPELINE_CONTRACT.md` §2A (CAS session
regimes), §5 (live detection, lifecycle, warm-up/parity gate);
`docs/pattern_research/EVIDENCE_SERVING_CONTRACT.md` §2 (compatibility identity),
§4 (live event identity and lifecycle), §5 (live/research parity gate).

**Status: built and measured, switched OFF.** `SCANNER_PATTERN_SET` defaults to
`legacy`; nothing about the running scanner changes until you set it to
`research`.

**To turn it on** (nothing was restarted for you):

```powershell
# from the repository root, with the scanner on 8765 stopped
$env:SCANNER_CANDLE_SOURCE = 'market15'
$env:SCANNER_PATTERN_SET   = 'research'
market_scanner/.venv/Scripts/python.exe -m market_scanner.server
```

The scanner refuses to start in `research` mode if
`market_scanner/output/live_warmup.json` is missing, did not pass, or was
measured against different detector code — re-run
`python -m market_scanner.pattern_warmup` in that case. The first scan replaces
the snapshot cache with research cells, so set `SCANNER_CACHE_DB` to a scratch
path first if you want the legacy snapshot kept.

---

## 1. What runs

The live scanner imports the research detector modules through the **same call
the research runner makes** — `pattern_research.runner.registry(MODULES)` — and
runs them on completed candles from the live store. No detector logic is forked,
copied or reimplemented; `market_scanner/pattern_live.py` contains no pattern
rule of its own.

| | legacy (default) | research |
|---|---|---|
| detectors | `market_scanner/detectors.py`, 10 chart patterns | `market_scanner/pattern_research/{legacy,candlesticks,chart_patterns,price_action,harmonics}.py` |
| cells | 10 | **107 pattern ids / 262 direction-variant combinations** |
| states | `setup` / `confirmed`, as of the last bar only | `forming` → `confirmed` → `invalidated` → `expired` |
| history per scan | `config.history_bars` = 260 | the measured warm-up (§3) |
| persistence | snapshot cells only | snapshot cells **plus** a `detections` instance ledger |

Both paths read the same candle record from `market_scanner/data.py`, so
`SCANNER_CANDLE_SOURCE=market15` (contract §2/§2A, including the CAS 15:15
session end) applies unchanged to both.

## 2. Lifecycle

Per **episode** — one instance of one `(pattern_id, variant, side)` on one
symbol and timeframe — keyed by timestamps and spec identity, never a bar index:

```
detection_id = sha256(symbol | timeframe | pattern_id | variant | side
                      | detector_spec_hash | formation_start | detected_at_bar_end)[:20]
episode_id   = sha256(symbol | timeframe | pattern_id | variant | side | formation_start)[:20]
```

State as of the newest completed candle:

| state | when |
|---|---|
| `forming` | the detector emitted a `setup`, its tracking window has not elapsed, and nothing has closed through the failure boundary |
| `confirmed` | the detector emitted its own `confirmed` event, inside its tracking window |
| `invalidated` | a completed candle closed through the failure boundary, on or before the end of the tracking window |
| `expired` | the tracking window elapsed with no confirmation, **or** a quality gap broke the segment the setup lives in |

A detection that leaves `forming`/`confirmed` is still reported for
`TERMINAL_GRACE` = 3 closed bars, so the app and the ledger both see the
transition rather than a setup that silently disappears.

**Tracking window (expiry), per family.** Taken from the detector itself
wherever it publishes one, and otherwise from the family's own documented
confirmation window:

| source | value |
|---|---|
| `geometry.expiry_bars` | chart CH26/CH27, which widen their own window up to 40 bars |
| `geometry.confirmation.window` | harmonics, 10 bars |
| family default `chart` | 10 (`common.CHART_SETUP_EXPIRY`) |
| family default `candlestick`, `price_action` | 3 (`common.CANDLE_CONFIRM_WINDOW`) |
| family default `harmonic` | 10 (`harmonics.CONFIRM_WINDOW`) |

**Failure boundary.** The research detectors track a failure boundary internally
but only *publish* one for some families. So:

- `failure_level_source = "detector"` — the boundary the detector itself
  published (`geometry.failure_level` for CH26/CH27,
  `geometry.confirmation.failure_level` for harmonics).
- `failure_level_source = "live_structure"` — everything else. This is a
  **live-layer rule declared here**, not the detector's: a close beyond the
  formation's opposite extreme (`min(low)` over `formation_start..detected` for
  a long, `max(high)` for a short) by 0.12 × ATR — the same buffer every
  close-beyond test in the package uses. It is tagged on every detection that
  uses it, it is versioned with `LIVE_RULES_VERSION`, and **no research result
  depends on it**: the research evaluation measures the detector's own emitted
  events, which this never changes.

## 2A. Chart overlays

`market_scanner/pattern_lines.py` translates each family's `geometry` into the
`lines` shape the app has always drawn — the same one
`market_scanner/detectors.py` emits and `kanida-app/src/patternGeometry.ts` +
`PatternCanvas.tsx` consume:

```json
{"label": "Resistance", "role": "boundary", "points": [{"index": 12, "value": 1477.5}]}
```

Roles: `boundary` (level or trendline; two of them named *Resistance* and
*Support* also produce the canvas's shaded envelope, and every boundary appears
in the legend readout), `curve`, `anchors` (the detector's own pivots, drawn as
circles), `shape` (a polyline through the formation), `label` (a single point
that becomes chart text).

| family | what is drawn |
|---|---|
| chart CH11–CH28 | published levels (`resistance`, `support`, `neckline`, `rim_level`, `base_boundary`, `breakout_level`, `projection_target`, `retracement_level`, `island_extreme_edge`), published fits `[j0, y0, slope]` (`support_line`, `upper_line`, `lower_line`, `upper_right_line`, `lower_right_line`, `trendline`), the pivot lists the detector used (`resistance_touches`, `support_troughs`, `troughs` with their own `trough_levels`, `upper/lower_anchors`, `pullback_highs/lows`, `lead_in_lows`, …), the pole / measured-leg / bowl / dome polylines, named bars as labels, and the trigger level |
| chart CH01–CH10 | **recovered from the original replay** (below) |
| harmonic | the XABCD polyline through the published point prices, a label per point, a ratio label at the midpoint of each numerator leg (`BC/AB 0.55`), the PRZ band as two boundaries, and the trigger level |
| price_action | the published level pair (`pair_high/low`, `mother_high/low`, `nr_high/low`, `prior_high/low`) or, for PA03/PA04/PA07 which publish none, the marked candles' own range; plus the named bars (mother bar, false break, failure close) and the confirmation level |
| candlestick | the pattern's published `high`/`low`, the marked-candle polyline, the trigger level, and (Hikkake) the setup bar |
| every family | the level the **lifecycle** actually tested, labelled `Failure level` when the detector published it and `Failure level (live rule)` when it is the live-layer fallback (§2) |

**CH01–CH10 recovery.** The legacy adapter `pattern_research/legacy.py` builds a
`HistoricalReplay`, keeps the episode events and throws the match — including
the `lines` the app has drawn since day one — away; a CH01–CH10 research event
carries **no geometry at all**. `pattern_lines.LegacyOverlays` rebuilds the same
replay over the same bars with the same mask and takes the match back, so the
overlay is the original detector's, not a reimplementation. The recovered match
is accepted only when its pattern **and score** match the event; a mismatch
returns a note rather than a different pattern's lines. The replay is built
lazily, once per (symbol, timeframe), only when a CH01–CH10 detection is
surfaced.

**Nothing is invented.** Every value drawn is a price the detector published, a
high/low/close of a bar the detector *named* (the fallback is declared per key
in `ANCHORS`/`POINTS` and documented here), or the live layer's tracking level —
which carries its own label. Two drawing choices are recorded rather than
hidden: an anchor index without a published price is drawn at that bar's high or
low according to the key's own meaning, and a level for a single-candle pattern
on the newest bar is anchored one candle back so it has somewhere to be drawn
(the *marked* range always comes from the bars the detector named, never from
that extended span).

**Contract:** `build()` returns `(lines, note)` with `note` non-empty **exactly
when** `lines` is empty, so the UI shows an explicit "marker only" instead of a
silent blank. `geometry_note` and `drawing_plan` ride on every match (including
in `/api/matches`); `lines` is served by `/api/chart`. Every point index is an
integer inside the **served** window — `Scanner.trim_window` widens the kept
candles to cover the oldest overlay point and rebases every index with it.

**Measured coverage** (8 symbols × 4 timeframes × 4 as-of points = 92 cells,
34,027 detections, `pattern_lines` audit): **173 of 173 observed
(family, pattern id, variant) combinations produced overlays**, 0 marker-only,
0 points outside the served window, 0 contract violations. 115,577 boundary,
12,237 shape, 6,510 label, 565 anchor and 1 curve line. The declared plan covers
all **205 registered (pattern id, variant) pairs**, CH01–CH10 among them.
Overlay building costs nothing measurable: per-symbol CPU for a four-timeframe
pass is 1.48 s with overlays against 1.55 s before them (same 60-symbol sample,
contended box — the difference is noise).

## 3. Warm-up / parity gate

`market_scanner/pattern_warmup.py`. Run from the repository root:

```powershell
market_scanner/.venv/Scripts/python.exe -m market_scanner.pattern_warmup --workers 3
```

Method: build the full candle history through the live path
(`load_market15` → `aggregate_market15`), run each research module over it once
(the **reference**), then pick several *as-of* bars and, for each candidate
window `W`, re-run the module on `bars[A-W+1 .. A]` — the exact slice a live
loader would hand it, with the first bar's `gap` flag cleared the way a short
fetch really clears it. Compare the last `TAIL` = 60 bars after normalising
every event to **timestamps**. The required window is the smallest tested `W`
such that it and every larger tested `W` disagree on nothing, for every sampled
symbol and as-of bar; it is then raised to the family's own declared lookback.

### 3.1 The measured table

Measurement of 2026-09-16 (`market_scanner/output/live_warmup.json`), 8 symbols,
5 as-of bars each, 11 candidate windows, **6,005 window comparisons over 53,284
reference events**, 437 s wall clock on 3 workers.

`measured` is what the comparison established; `required` is that raised to the
family's own declared lookback, which is what the live loader honours.

| family | declared lookback | 1H | 4H | 1D | 1W |
|---|---|---|---|---|---|
| chart (CH01–CH28, legacy + research) | 260 | **260** (measured 220) | **260** (measured 220) | **260** (measured 170) | **260** (measured 170) |
| candlestick (61 TA-Lib ids) | 25 | **100** (measured 100) | **100** (measured 100) | **100** (measured 100) | **100** (measured 100) |
| price_action (PA01–PA08) | 30 | **100** (measured 100) | **100** (measured 100) | **100** (measured 100) | **100** (measured 100) |
| harmonic (HA01–HA10) | 163 | **163** (measured 100) | **163** (measured 130) | **163** (measured 100) | **163** (measured 80) |
| **loader fetch depth** | – | **260** | **260** | **260** | **260** |

The chart floor is 260 because `pattern_research/legacy.py` replays the original
scanner through `HistoricalReplay(bars, 260, …)`; its published `lookback` of 40
is a minimum bar count, not that window. The harmonic floor is the 163 the
harmonic specifications declare.

**Disagreement at smaller windows** (worst case over all symbols and as-of bars,
missing + invented events in the compared 60-bar tail):

| family × timeframe | 60 | 80 | 100 | 130 | 170 | 220 | 280…800 | events compared |
|---|---|---|---|---|---|---|---|---|
| chart 1H | 7 | 4 | 6 | 5 | 1 | 0 | 0 | 216 |
| chart 4H | 9 | 11 | 6 | 4 | 2 | 0 | 0 | 192 |
| chart 1D | 7 | 6 | 3 | 1 | 0 | 0 | 0 | 110 |
| chart 1W | 7 | 5 | 2 | 2 | 0 | 0 | – | 44 |
| candlestick 1H | 187 | 10 | 0 | 0 | 0 | 0 | 0 | 14,137 |
| candlestick 4H | 161 | 9 | 0 | 0 | 0 | 0 | 0 | 12,928 |
| candlestick 1D | 181 | 10 | 0 | 0 | 0 | 0 | 0 | 6,161 |
| candlestick 1W | 181 | 4 | 0 | 0 | 0 | 0 | – | 2,122 |
| price_action 1H | 76 | 8 | 0 | 0 | 0 | 0 | 0 | 6,785 |
| price_action 4H | 88 | 5 | 0 | 0 | 0 | 0 | 0 | 6,852 |
| price_action 1D | 76 | 6 | 0 | 0 | 0 | 0 | 0 | 2,705 |
| price_action 1W | 50 | 4 | 0 | 0 | 0 | 0 | – | 990 |
| harmonic 1H | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 10 |
| harmonic 4H | 4 | 1 | 1 | 0 | 0 | 0 | 0 | 24 |
| harmonic 1D | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 6 |
| harmonic 1W | 1 | 0 | 0 | 0 | 0 | 0 | – | 2 |

Harmonic samples are small (2–24 events in the compared tails) — that is why the
declared 163 floor, not the measured 80–130, is what runs. The 1W grid stops at
220 because the deepest weekly history the live store can build is 271 candles.

### 3.2 Coverage the measurement actually exercised

The evidence contract requires specific cases to be present, not assumed.
Observations in the reference runs:

| case | observations |
|---|---|
| harmonic (HA01–HA10) | 1,375 |
| Hikkake confirmation (CDLHIKKAKE / CDLHIKKAKEMOD) | 29,722 |
| mother-bar clusters (PA02 inside bars / PA06) | 146,504 |
| legacy chart CH01–CH10 (pivot replacement, overlapping formations) | 5,347 |
| research chart CH11–CH28 | 11,131 |
| candlestick | 180,334 |
| price action | 367,808 |

Gap and session boundaries: the sample includes FORCEMOT (the genuine
2023-10-26…2024-02-13 suspension), DELHIVERY (77 quality gaps on 1H, 64 on 4H,
and a late usable start), AMRUTANJAN (49 gaps), and 7 of the 8 symbols are on
the CAS regime, so every post-2026-08-03 intraday bucket in the measurement ends
at 15:15 (contract §2A). Zero coverage gaps were reported.

### 3.3 What the measurement could **not** make agree

Four events out of 53,284 compared (**7.5 × 10⁻⁵**) disagree at *every* tested
window, including the largest. They are not a warm-up problem and more history
cannot fix them:

```
candlesticks CDLLONGLINE break_down     short confirmed  DELHIVERY 4H 2025-10-16 15:30
candlesticks CDLLONGLINE color_direction short confirmed DELHIVERY 4H 2025-10-16 15:30
price_action PA01 top                   short confirmed  DELHIVERY 4H 2025-10-16 15:30
price_action PA03 bearish_close         short confirmed  DELHIVERY 4H 2025-10-16 15:30
```

Cause, measured directly: `common.prepare` accumulates ATR through
`np.cumsum` over the whole array, so a differently truncated window reaches the
same ATR by a different summation order. For this bar the confirmation test sits
exactly on its threshold — `close - level` is `-5.7e-14` on the full history and
exactly `0.0` on the truncated window, so `close < level` flips. ATR differs by
`8.749999999999636` vs `8.750000000000090`.

Among events both sides *do* agree on, relative ATR and score drift at or above
the accepted warm-up is **0.0**. The 2.8% figure in the report's
`max_relative_atr_drift` comes from the deliberately-too-short 60-bar windows,
where the first bar's true range has no previous close.

This is a real limit of the live/research join and is listed in §8.

## 4. Switches

| switch | values | default | effect |
|---|---|---|---|
| `SCANNER_PATTERN_SET` | `legacy` \| `research` | **`legacy`** | which detector set the scan loop runs |
| `SCANNER_CANDLE_SOURCE` | `legacy` \| `market15` | `legacy` | which store the candles come from (`data.py`) |
| `SCANNER_RESEARCH_RUN` | run id | `4b33a5249562631524d6` | the research run a live detection's identity is compared against |
| `SCANNER_WARMUP_REPORT` | path | `market_scanner/output/live_warmup.json` | the measurement the gate reads |
| `SCANNER_CACHE_DB` | path | `market_scanner/output/scanner.sqlite3` | snapshot cache; override to trial the research set without overwriting the live snapshot |
| `SCANNER_DETECTION_RETENTION_DAYS` | whole days, `0` disables | `30` | how long a **terminated** detection stays in the ledger |
| `SCANNER_DETECTION_MAX_ROWS` | whole rows, `0` disables | `200000` | hard ceiling on terminated ledger rows, applied after the age cut |

Environment wins over `config.json` for all of them. With the switch off, the
scan loop, the cache signature, the loader depth and the `/api/matches` payload
are exactly what they were.

## 5. API

`/api/state` gains `pattern_set`, plus a `research` block when it is on:
detector spec hash, evidence identity, the warm-up summary, the per-timeframe
history depth, and live counts per lifecycle state. `patterns` becomes the
107-id catalogue instead of the 10 legacy ones.

`/api/matches` in research mode returns an object, not a bare list:

```json
{"matches": [...], "total": 41230, "returned": 500, "limit": 500, "pattern_set": "research",
 "history_screen": {"applied": false, "reason": "research_matches_have_no_legacy_history",
                    "default_min_trades": 5,
                    "skipped": {"mode": "reference", "performance": null, "return_band": null,
                                "min_trades": 5},
                    "evidence": "evidence=true / research_cell_status",
                    "message": "No sample-size, performance or return-range screen is applied …"}}
```

**`history_screen` — the legacy-history screen never runs silently.** `performance`,
`return_band` and `min_trades` screen a match on its **legacy backtest history**.
A research detection carries none by design (joining one by pattern name alone is
the fallback EVIDENCE_SERVING_CONTRACT.md forbids), so that screen is `False` for
every row: running it deletes the whole book rather than filtering it. It used to
do exactly that, and on a *bare* request too — the only screen in play there was
`performance.DEFAULT_MIN_TRADES` (5), which the caller never asked for.

In research mode the screen is therefore **not applied**, and never silently:

* every 200 body carries `history_screen`, with `applied: false`, the machine-readable
  `reason`, the screen it did **not** run (`skipped`, `null` when none was in play) and
  where the real evidence is — each detection's own research cell, via `evidence=true` /
  `research_cell_status`;
* a caller that **explicitly** asks for the screen (`performance=`, `return_band=`, or
  `min_trades` greater than 0) gets **HTTP 400** with that reason, rather than an
  unscreened set that reads as if it had been screened. `min_trades=0` is the explicit
  "any sample size" — the absence of a screen — and is served normally.

Legacy mode is untouched: still a bare JSON list, still screened, no `history_screen`
key. (`market_scanner/tests/test_matches_api.py` pins both sides.)

Filters: `pattern_id`, `variant`, `side`, `family`, `state`
(`forming|confirmed|invalidated|expired`), `detector_state` (`setup|confirmed`),
`evidence_status`, `live=true`, `timeframe`, `symbol`, `current=true`,
`limit` (default 500, max 5000). `evidence=true` additionally looks each match's
cell up in the research store.

Each match carries `pattern_id`, `variant`, `side`, `family`, `state`,
`state_reason`, `detector_state`, `live`, `bars_since_state`,
`formation_start`, `detected_at_bar_end`, `signal_at_bar_end`,
`confirmed_at_bar_end`, `fit_score` (0–1, the detector's own geometric fit) and
`score` (0–100, for the existing UI's fit dial). The research families score in
[0, 1] and are scaled up; the CH01–CH10 adapter carries the original scanner's
0–100 quality and is scaled down — neither is silently rescaled into the other's
meaning, and **neither is a probability**. `/api/chart` additionally returns
`geometry` and `tracking`.

## 5A. The detection ledger and its retention

Every surfaced detection is upserted into `detections` in the scan cache, keyed
by `detection_id`, preserving `first_seen` so the moment a setup first appeared
is never lost — that is what makes per-instance outcome tracking possible later.
The payload stores the record without `geometry` or `lines`; both are rebuilt
from the candles on demand.

At ~85 live detections per symbol per pass over ~500 symbols the ledger gains
tens of thousands of rows a day, so `pattern_live.prune` runs at the end of each
committed scan:

* only `invalidated` and `expired` rows are eligible — a `forming` or
  `confirmed` detection is the live book and is **never** pruned by age, however
  long it has been standing;
* terminated rows whose `last_seen` is older than `SCANNER_DETECTION_RETENTION_DAYS`
  (default 30) are dropped;
* if terminated rows still exceed `SCANNER_DETECTION_MAX_ROWS` (default 200,000)
  the oldest-by-`last_seen` are dropped down to the cap, so the newest terminated
  instances — the ones an outcome study still needs — survive;
* either cut is disabled with `0`, and the counts removed are reported in the
  snapshot metadata as `detection_retention`.

## 6. Evidence identity

`detector_spec_hash` = sha256 over the canonical specification JSON, the sha256
of every file whose bytes decide what a detector emits
(`pattern_live.DETECTOR_FILES`) and the research runner's own dependency record.
The frozen research run's manifest is hashed the **same way**, so the two are
comparable by construction rather than by coincidence.

| `evidence.status` | meaning |
|---|---|
| `identity_match` | live detector code, specifications and dependencies are byte-identical to the research run — the identity *permits* a research card |
| `detector_mismatch` | the live detector differs; **no compatible historical evidence** |
| `unavailable` | the research run manifest is not readable here; **no compatible historical evidence** |

Measured on this tree, 2026-09-16: `identity_match`, both sides
`e88131bc87d3d5a9c0945a6669c78e8ddec50d122733655e92d62a5eb92414b5`, against run
`4b33a5249562631524d6` (source run `m15clean-20260916`, snapshot
`snap_20260916T055228_f4bab0e9`).

`identity_match` never means a card exists. The per-cell lookup is
`/api/matches?evidence=true`, and a `(symbol, timeframe, pattern_id, variant,
side)` the research run never studied comes back as
`{"status": "no_compatible_evidence"}`. The legacy backtest store is **never**
joined to a research detection: that would be exactly the pattern-name-only
fallback §2 of the evidence contract forbids.

## 7. Scan cost, and what was done about it

### 7.1 Measured end to end

Taken 2026-09-16 04:15–04:35 IST on an idle box (the concurrent evidence job had
finished; load was 3 busy processes at 27% when the run started),
`workers: 4`, `SCANNER_CANDLE_SOURCE=market15`, full scanner universe of 1,431
symbols of which 513 have 15-minute data. Scanner startup — which scans
`candles_15m` for per-symbol coverage — took **75.1 s** before the first pass.

| stage | timeframes | wall | CPU | cells scanned | detections | live | with overlay | marker-only | errors |
|---|---|---|---|---|---|---|---|---|---|
| **A** intraday, every hour | 1H | **2 m 26 s** (145.9 s) | 170.8 s | 512 | 26,297 | 15,782 | 26,297 | 0 | 0 |
| **B** 13:15 and the close | 4H | **4 m 07 s** (247.1 s) | 271.2 s | 512 | 22,718 | 13,600 | 22,718 | 0 | 0 |
| **C** after the close | 1D + 1W | **2 m 57 s** (176.5 s) | 190.2 s | 949 | 40,198 | 20,646 | 40,198 | 0 | 0 |
| **D** forced full pass | all four | **9 m 49 s** (588.5 s) | 681.7 s | 1,974 | 89,246 | 50,043 | 89,246 | 0 | 0 |

Per symbol: 102 ms (1H), 173 ms (4H), 123 ms (1D+1W), 411 ms (full pass).

Two things this settles:

1. **Every staged pass is inside the 5-minute budget** — the hourly 1H pass at
   2 m 26 s, the 4H pass at 4 m 07 s, the after-close 1D+1W pass at 2 m 57 s.
   The **full** four-timeframe pass is 9 m 49 s, nearly double the budget, which
   is exactly why the source-advance branch no longer forces it (§7.3). `POST
   /api/scan` still can, and takes that long.
2. **Threads buy almost nothing.** Wall 588.5 s against 681.7 s of CPU across
   four workers is a speed-up of about 1.16×: the detectors are pure
   Python/NumPy and the GIL dominates. Raising `workers` will not help much;
   breaking the ceiling needs a process pool, which is not built.

Detections by family across the full pass (all four timeframes):

| family | 1H | 4H | 1D | 1W |
|---|---|---|---|---|
| candlestick | 17,059 | 14,862 | 13,638 | 13,495 |
| price_action | 8,590 | 7,403 | 5,965 | 6,152 |
| chart | 637 | 422 | 480 | 415 |
| harmonic | 44 | 31 | 31 | 22 |

**All 89,246 detections carried a drawable overlay; none was marker-only.**
Ledger after the pass: 89,246 rows (31,233 forming · 18,810 confirmed ·
16,378 invalidated · 22,825 expired), ~1.3 KB a row. The scan cache reached
693 MB — 313 MB of snapshot cells, the rest ledger and indexes.

### 7.2 Per-symbol cost, and the legacy comparison

Same store and cutoff, 60 randomly-sampled symbols the store holds, single
worker. Taken while the box was still shared with the research evidence job, so
the wall column here is contended and the CPU column is not; the end-to-end
numbers above supersede it for planning.

| | CPU s / symbol | wall s / symbol (contended) |
|---|---|---|
| research, all four timeframes | **1.554** | 1.922 |
| research, 1H only | 0.430 | 0.517 |
| research, 4H only | 0.537 | 0.736 |
| research, 1D only | 0.291 | 0.395 |
| research, 1W only | 0.305 | 0.366 |
| **legacy, all four timeframes** (same 20 symbols) | **0.373** | 0.529 |
| a symbol absent from the 15-minute store | 0.0008 | – |

The research set costs **4.2× the legacy set** per symbol. Projected CPU for a
full four-timeframe pass:

* 495 symbols (the research universe): **769 CPU-seconds ≈ 12.8 CPU-minutes**
* 1,431 symbols (the scanner's full `kanida.db` universe, 918 of which have no
  15-minute data and cost ~0): **798 CPU-seconds**

### 7.3 What was done about it

**Staging by timeframe.** The candle-close scheduler already
rescans only the timeframes whose close advanced. What did not stage was the
*source-advance* branch: `market15` publishes a new 15-minute bar every cycle,
and that forced a full four-timeframe rescan each time. On the research set the
source advance now rescans only the timeframes that are actually behind their
latest expected close (`Scanner.behind`), so:

* 1H runs once an hour — the measured **2 m 26 s** pass;
* 4H runs at 13:15 and at the close — **4 m 07 s**;
* 1D and 1W only after the session closes — **2 m 57 s** together;
* `POST /api/scan` still forces the full four-timeframe pass — **9 m 49 s**.

The legacy path is untouched: a source commit there can change anything, so it
still rescans everything.

Two knobs remain if that is not enough: `workers` in `config.json` (threads), and
`SCANNER_CACHE_DB` to run a research scan beside the live one rather than
instead of it. Moving detection to a process pool would break the GIL ceiling but
is not built — the scan loop would have to hand each worker a picklable context.

### 7.4 What the research set finds on today's data

Same 60-symbol sample, cutoff 2026-09-15 15:30, 60-bar live tail, terminal grace
3 bars. **168.9 detections per symbol** across the four timeframes (85.4 of them
still `forming` or `confirmed`), from **83 distinct pattern ids**.

| family | 1H | 4H | 1D | 1W | total | of which live |
|---|---|---|---|---|---|---|
| candlestick | 1,852 | 1,765 | 1,588 | 1,694 | 6,899 | 3,510 |
| price_action | 713 | 806 | 759 | 737 | 3,015 | 1,436 |
| chart | 52 | 57 | 54 | 48 | 211 | 164 |
| harmonic | 4 | 2 | 4 | 2 | 12 | 11 |
| **total** | 2,621 | 2,630 | 2,405 | 2,481 | **10,137** | **5,121** |

Lifecycle split: `forming` 2,661 · `confirmed` 2,460 · `invalidated` 2,125 ·
`expired` 2,891. Most frequent ids: PA05 (narrow range) 1,702, CDLLONGLINE
1,051, CDLSPINNINGTOP 746, PA02 (inside bar) 656, CDLSHORTLINE 596.

Scaled to the 495 symbols the store holds for the research universe that is on
the order of **84,000 detections per full pass, ~42,000 of them live** — which is
why `/api/matches` is paginated in research mode and why the app must filter by
pattern id, variant, side and state rather than render the list.

Cell status in the sample: 1H and 4H scanned for all 60; 1D scanned for 58
(2 symbols have no daily bars in the store); 1W scanned for 54, 4
`insufficient_history` (fewer than 100 weekly candles) and 2 with none. Mean
window actually detected on: 1H 260.0, 4H 258.2, 1D 251.3, 1W 230.0 candles.


## 8. Known limits

1. **Four events in 53,284 flip on a floating-point tie** (§3.3). Where a
   confirmation test sits within ~1e-13 of its threshold, the ATR that
   `np.cumsum` reaches by a different summation order decides it. More history
   does not fix it. Rate measured: 7.5 × 10⁻⁵. Removing it would mean changing
   `common.prepare` — i.e. changing the detectors and invalidating the frozen
   research run — so it is reported rather than patched.
2. **No per-cell evidence card is served yet.** `evidence.status =
   identity_match` says the identity permits a research card; `?evidence=true`
   reads the research cell's coverage status, occurrence count and walk-forward
   mean, but the full card contract (`EVIDENCE_SERVING_CONTRACT.md` §6:
   population/release ids, confidence intervals, labels) is not implemented.
   Nothing in the live path presents a research number as a forecast.
3. **The invalidation boundary is a live-layer rule for most families** (§2).
   Only CH26/CH27 and the harmonics publish their own. The fallback is declared,
   versioned and tagged, but it is an addition to the research definition, not
   part of it.
4. **Overlay coverage is measured on what fired, not on all 205 pairs.** 173
   (family, pattern id, variant) combinations were observed and every one drew;
   the remaining 32 registered pairs (CH21, CH24 `top_single`, a few rare
   candlesticks) did not fire in the audit window, so their overlays are
   *declared* by `plan_for` and covered by the registry test but have not been
   seen against real geometry. Three drawing choices are conventions, not
   detector output, and are labelled as such in §2A: anchor y-values for indices
   without a published price, the one-bar-back anchor for a level on the newest
   candle, and the `Failure level (live rule)` line.
5. **1D/1W depth is bounded by the live store.** `market15.daily_bars` starts
   2021-07-05, so the deepest weekly series the live path can build is ~271
   candles; a symbol with a later listing (DELHIVERY: 224) cannot meet the
   chart family's 260-bar warm-up on 1W and that family simply does not run
   there. The research export reached further back only by using the verified
   `raw_archive` daily payloads, which the live loader does not read.
6. **Not every scanner symbol is in the 15-minute store.** The scanner universe
   from `db/kanida.db` is 1,431 active NSE equities; `db/market15.db` holds 513.
   The rest report `no_complete_data`, and some symbols that do have intraday
   bars (AMRUTANJAN) have no daily bars at all, so their 1D/1W cells are empty.
7. **Warm-up was measured on 8 symbols.** Coverage of the required cases is
   demonstrated (§3.2) and the chart/harmonic floors come from the detectors'
   own declared lookbacks rather than the thinner measured numbers, but the
   harmonic tails held only 2–24 events. Widening the sample is cheap
   (`--symbols …`) and should be redone whenever a detector changes — the gate
   refuses to run against a report whose `detector_spec_hash` does not match.
8. **The scan cache is large.** One measured full pass left 693 MB: 313 MB of
   snapshot cells (1,974 cells, each with its 260-candle window and every match
   with its overlay) and the rest ledger plus indexes. The ledger is bounded
   (§5A) — its payload is ~1.3 KB a row after the omissions in `LEDGER_OMIT`,
   so the 200,000-row cap holds it near 250 MB — but the cell snapshot is
   replaced wholesale each pass, not pruned, and scales with the warm-up depth.
   Point `SCANNER_CACHE_DB` at a disk that can take it.
9. **The wall-clock budget is projected, not observed** (§7). Per-symbol CPU is
   measured; the end-to-end wall clock needs an idle box.
10. **Adding these two modules changes a future research run id.**
   `runner.code_files()` hashes every `market_scanner/*.py`, so a new
   `runner.prepare` will mint a different run id than `4b33a5249562631524d6`
   even though no detector changed. `detector_spec_hash` — the identity that
   governs evidence — is computed from an explicit file list and is unaffected.

## 9. Tests

```powershell
market_scanner/.venv/Scripts/python.exe -m pytest market_scanner/tests -q
market_scanner/.venv/Scripts/python.exe -m pytest market_data/tests -q
```

`market_scanner/tests/test_pattern_live.py` covers live-vs-research parity on
synthetic and real candles, detection-id stability across fetch depths, every
lifecycle transition, the warm-up gate's refusals, per-family warm-up
enforcement, CAS session correctness end to end, the persistence ledger, and
that the legacy switch reproduces the legacy matches exactly.

For the overlays it asserts the lines/note exclusivity on every detection, that
the declared plan covers all 205 registered pairs, that each family draws from
the values its detector published (harmonic legs and ratio labels, chart levels
and fits, the mother-bar pair, the candle range and trigger level), that the
live fallback level is labelled as the live rule and the detector's is not, that
CH01–CH10 overlays come back from the replay on real candles, and that every
point still indexes a served candle after `trim_window`. For the ledger it
asserts that age and row-cap pruning drop terminated rows oldest-first, that a
live detection is never pruned, that both cuts are configurable and can be
disabled, and that bad configuration is rejected.
