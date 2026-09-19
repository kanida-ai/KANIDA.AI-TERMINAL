# Detector notes (Claude Code)

Scope: CH11–CH28, 61 CDL, PA01–PA08, HA01–HA10 under `market_scanner/pattern_research/`. Interface per `CLAUDE_HANDOFF.md`. Status lives in `DETECTOR_STATUS.json`.

## Shared conventions (`common.py`, definition_version 1.0.0)

- **Bars:** `gap` on bar *i* = data-quality discontinuity between *i−1* and *i*. Invalid OHLC bars are isolated. Nothing spans a segment break. No filling.
- **ATR:** mean true range of the 20 bars **before** *i*, all in one segment, floored at 0.1% of close[i]. True range never uses a close across a break. An event's `atr` is ATR at its `signal_index`. Thresholds use ATR at the setup bar.
- **Pivots:** radius 3, strict unique extreme in one clean segment. Available at *p+3*.
- **Prior trend:** least-squares fit of closes *s−20..s−1*. `up`/`down` if the fitted change is beyond ±1 ATR[s], otherwise `neutral`. `None` if the window is not clean.
- **States:** `setup` is emitted once, at the first closed bar where the shape is known. `confirmed` is emitted once, at the confirming close, with the same `episode`, `detected_index` = setup bar, and `confirmed_index` = signal bar.
- **Episode:** an int identity within a key (default = formation start; PA02/PA06 = mother bar). Distinct adjacent candle formations stay distinct. No three-absent-bar rearm.
- **Session stubs:** `quality_tags` gets `session_stub` when a formation includes an intraday bar shorter than 60/240 minutes.
- **Default candle confirmation:** within 3 bars, close beyond the structure high/low by 0.12 × ATR(setup). Expires at a segment break.

## Tests

From the repo root:

```
market_scanner/.venv/Scripts/python.exe -m unittest discover -s market_scanner/tests -p "test_detectors_*.py"
```

`test_detectors_support.py` provides `assert_contract` (spec keys, event schema, sort order, JSON safety, no repeats) and `assert_causal` (prefix truncation plus future perturbation).

## Module notes

### candlesticks.py — ready

- **Scope:** all 61 CDL IDs on TA-Lib 0.6.8, version asserted in tests. All candle settings are restored to defaults before every run; the defaults are recorded in `CANDLE_SETTINGS`. Penetration is pinned: 0.3 for star/abandoned-baby patterns, 0.5 for dark cloud cover and mat hold. The library runs separately on each gap-free segment. 165 study cells.
- **Variants:**
  - `canonical`: library sign.
  - `canonical_context`: prior-trend gate. Reversal long needs a down trend; continuation long needs an up trend.
  - `color_direction`: long-line, marubozu and closing marubozu only. Explicitly descriptive.
  - `break_up` / `break_down`: neutral shapes. The setup direction is `neutral`, and four-price doji are excluded.
  - Dragonfly, gravestone and takuri emit a constant +100 recognition code. They get `canonical_context` on their conventional side only, plus break variants. They never get a library-sign long.
  - Kicking, kicking-by-length and both hikkakes are direction-only, so they have no context variant.
- **Intrinsic confirmation:** CDL3INSIDE and CDL3OUTSIDE emit `confirmed` only, at the third candle. CDLHIKKAKE and CDLHIKKAKEMOD use the library's ±200 confirmation, linked to the most recent ±100 setup within 3 bars.
- **Tags:** `edge_equality_grade` for ±80 outputs, `four_price_doji`, `session_stub`. Geometry holds the high/low, colour, prior trend and volume ratio (volume is context only, never a gate).
- **Verified:**
  - Every raw recognition matches the library exactly, on a 3,000-bar set, whenever prior ATR exists.
  - Hikkake confirmations match the library exactly.
  - Prefix and future-perturbation checks pass.
  - Gap-flag fixtures behave: a gap inside the formation, a gap in the confirmation window, and a gap too close for ATR all correctly suppress events.
  - Tampering with the candle settings leaves outputs unchanged, because defaults are restored.
  - Codex finding 2 is fixed: defaults are restored before lookbacks are cached at import. An isolated-import subprocess test covers it, and a negative control without the fix reproduces the failure.
  - 20k bars take about 1.9 s.
- **Limitations:**
  - The named `gap_free` morning/evening star, inverted hammer and shooting star adaptations are **not implemented**. These are custom definitions and not part of the 61 canonical cells.
  - Some functions are rare on synthetic data (CDLCONCEALBABYSWALL, CDLMATHOLD, CDLKICKING). Their library parity rests on the shared generic wrapper, not on per-function positive fixtures.
  - No slot-normalised or full-duration-only stub model exists. Stubs are only tagged.

### chart_patterns.py — ready (32 cells)

- **Common gates:** 20–150 bar window, ≥5 bars between turns, height ≥2 ATR, 0.5 ATR equality, 0.12 ATR breakout. Expiry is 10 bars or geometry failure. Buffers use ATR frozen at the setup. A new formation must start after the previous emitted formation's last anchor. Volume is recorded, and gated only in CH28 `_volume_contraction`.
- **Named exceptions:**
  - CH12 needs ≥3 support touches.
  - CH14 pennant: pole ≥5 ATR with efficiency ≥0.72, pennant 6–25 bars, window 15–50.
  - CH21 drops the legacy handle-volume gate and caps the window at 150 bars.
  - CH24 island has no window, height or expiry gates.
  - CH26 and CH27 expiry is min(40, max(10, leg bars)).
- **Confirmed-only cells:** CH11 `downside_resolution`, CH19/CH20 rounding, and all four CH24 island cells.
- **Adam/Eve:** only geometry/tags. "adam" means ≤2 of bars t−3..t+3 lie within 0.25 ATR of the extreme.
- **Codex findings:** 4 (empty `_flat_cluster` slice) and 5 (CH28 duplicate claim) are fixed and locked by regression tests.
- **Late fix:** `detect()` no longer returns nothing under 40 bars, which broke short-prefix causality. It now skips only when no ATR is possible (≤20 bars).
- **Synthetic coverage:** CH14, CH21, CH23 and CH24 never fired on random walks. Fixtures cover them, and Codex reports real occurrences.

## Final handoff (2026-09-15)

- **Ready:** all five modules. `DETECTOR_STATUS.json` has `overall: ready` and the SHA-256 of every source/test file.
- **Tests:** 135 pass. The registry holds 107 IDs as 262 combinations: 165 candle, 32 chart, 26 price-action, 26 harmonic, and Codex's legacy CH01–CH10.
- **Lead checks:** I re-verified every module's worker results myself on extra gap-flagged random walks: contract, `validate_events`, and prefix/future invariance, including 21–60 bar prefixes.
- **Not implemented:** gap-free candle adaptations, PA05 tie variants, PA07 gap-hold/fill/retest strategies, Adam/Eve variants. All are listed in the status limitations.
- **For Codex:** the chart short-history change landed after your 4-stock pilot. Re-run real-data validation to record the final hashes before the freeze. No further source edits are planned from my side.

### price_action.py — ready (26 cells)

- **Default confirmation:** close beyond the level by 0.12 × ATR(setup) within 3 bars, in one clean segment.
- **PA01 tweezer:** tolerance = max(0.01, 0.1 ATR); bar 2 closes in the far half and ≥0.25 ATR from the level; prior trend required.
- **PA02 inside bar:** variants `first`, `cluster`, `nested`, each with break_up/break_down; episode = mother bar. Setup is neutral. It expires at the first breach of the mother range without a qualifying close.
- **PA03 outside bar:** close in the top/bottom quarter only.
- **PA04 pin bar:** dominant shadow ≥2× body and ≥60% of range; opposite shadow ≤15%. Plus `_context` variants.
- **PA05 NR bars:** `nr4`, `nr7`, `inside_nr4`; strictly smallest range, no ties; two-sided break.
- **PA06 fakey:** a mother-edge breach, then a close back inside within 3 bars (the setup). Confirmed through the opposite edge within 3 bars; invalidated by a close back beyond the failed edge.
- **PA07 windows:** OHLC-only price windows; gap-flagged bars are never eligible.
- **PA08 gap-and-reversal:** opens beyond the prior range, closes inside it in the correct half.
- **Omitted:** NR ties; middle-close outside bars; gap-hold/fill/retest strategy variants; an exactly-one inside bar (not causal).

### harmonics.py — ready (26 cells)

- **Swings:** alternating radius-3 pivots. A same-type pivot replaces the last swing only if strictly more extreme, and only at its own time. Bars that are both a high and a low pivot are skipped. The swing list resets at gaps.
- **Setup:** fires at the last pivot +3.
- **Confirmation:** within 10 bars, a close beyond the D..setup extreme by 0.12 ATR. A close beyond D by 0.12 ATR first blocks it.
- **Gates:** each leg ≥1 ATR and XA ≥2 ATR; span 20–150 bars; clean window.
- **Ratio bands:** frozen in each spec definition. HA02 has `ext_1_27` / `ext_1_618`; HA04 has `b_0_382` / `b_0_50`; HA06 has `ext_1_27` / `ext_1_618`. HA10 5-0 uses X-A-B-C-D. Codex finding 3 is fixed: HA03 Gartley and HA10 5-0 canonical now gate CD/AB in [0.90, 1.10], with rejection tests at 0.80 and 1.20. Before the gate, the other 5-0 bands allowed CD/AB of about 0.73–1.23.
- **Codex Q1 is applied:** HA04 Bat, HA06 Butterfly and HA07 Crab canonical require a lower-bound-only CD/AB ≥ 0.90, with no upper limit and no new variants. Typical 1.27 and 1.618 extensions are annotated in `geometry.abcd_extension` but never gated. CD/AB is recorded for all five-point templates.
- **When the minimum actually binds:** only for Butterfly `ext_1_27`. The other bands already imply CD/AB of at least about 1.08 for Bat, 1.30 for Butterfly 1.618 and 1.89 for Crab. A bar-level rejection fixture exists for Butterfly `ext_1_27`; the other four cells are rejection-tested on their ratios.
- **Limitation:** HA04–HA08 never fire on random walks, so their correctness evidence is synthetic fixtures only.
