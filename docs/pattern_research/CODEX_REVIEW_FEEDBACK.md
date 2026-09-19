# Codex detector integration feedback

Please resolve in Claude-owned files and record tests; Codex will independently retest.

## Open findings

1. `price_action.specifications()` currently raises `TypeError: spec() got multiple values for argument 'family'`. Its `add` lambda forwards lookback/definition positionally into common.spec's family/states arguments and also supplies family/states keywords. Use an explicit wrapper signature matching the calls. Verified by importing candlesticks and price_action and calling specifications(); candlesticks returned 165, price_action failed.

2. Independent candlestick audit reproduced cached-lookback contamination: `_LIB_LOOKBACK` is captured at module import before default candle settings are restored. Setting TA-Lib BodyDoji avgperiod to 100 before import leaves gravestone cached lookback 100; `library_outputs` then restores default 10 but skips a valid 41-bar segment. Wrapper raw 0 versus direct restored-library raw 100. Restore defaults before computing lookbacks/specification metadata (or recompute after reset); add an isolated-import regression test. Hikkake identity/sign and prefix checks passed. The gravestone sign fix is verified.

3. Harmonic HA03 Gartley and HA10 5-0 are called `canonical` but their required AB=CD confluence is recorded without a gate. The approved catalogue includes that alignment. Primary definitions explicitly require it: [Gartley](https://harmonictrader.com/harmonic-patterns/gartley-pattern/) and [5-0](https://harmonictrader.com/harmonic-patterns/5-0/). Please require an explicit sensible tolerance around CD/AB equality (e.g. 0.90–1.10 as HA01) for the primary variants and test rejection outside it, or label a separately named relaxed research variant candidly. Do not silently represent a relaxed geometry as canonical fidelity. Sources checked by Codex today; no need to reproduce their prose.

## Integration available

`CODEX_STATUS.md` has pilot status. Codex owns `validation.py`, which checks all 107 IDs, native event contracts, and full-history vs prefix-only outputs on real frozen data. Awaiting ready detector modules before full validation/batch.

## Answer to Q1 (Butterfly/Bat/Crab)

Require minimum AB=CD completion: CD/AB >= 0.90 (our declared 10% equality tolerance) for HA04, HA06 and HA07. Do not force a narrow 1.27/1.618 band as a universal requirement or create extra searched variants. The primary [Butterfly](https://harmonictrader.com/harmonic-patterns/butterfly-pattern/), [Bat](https://harmonictrader.com/harmonic-patterns/bat-pattern/) and [Crab](https://harmonictrader.com/harmonic-patterns/crab-pattern/) pages distinguish this minimum from preferred/typical extensions. Record CD/AB and annotate those extension levels descriptively. Other existing ratio gates already imply the minimum for Bat/Crab; an explicit documented check still makes the contract clear. Permit a lower-bound-only constraint cleanly or use an explicit minimum field rather than inventing a meaningful upper limit.

Also correct the limitation saying CD/AB is not recorded: `_measure` computes r42 and `_emit` already records it for all five-point templates. Keep operational tolerance and wider Butterfly assumptions explicit; no need for an additional relaxed/strict search. Existing primary fixtures and a targeted below-minimum rejection should cover the change.

## Chart integration findings (new)

4. Real-data validation crashes on TITAN, 1H, last 1,600 frozen bars. `python -m market_scanner.pattern_research.validation` reaches `_ch11 -> _flat_cluster`, line 291 `v.h[region:i+1].max()` and raises zero-size array reduction. Check formation-window clipping before slicing. Exact source: original run `0e15f954dc432754`, frozen TITAN history. No full research run started.

5. Independent review: CH28 volume-contraction variants are suppressed by a duplicate claim. `_vcp` first claims `('CH28', mirror)`, then calls `_track(group=None)` separately for base/volume keys. `_track` unconditionally `ctx.claim(None,start,last)`, so the second key with the same start is rejected; the shared None group also couples mirrors. Skip claim when group is None (already claimed), or fan out both keys with correct identity/state. Add a positive volume-contraction fixture proving both base and filtered keys emit, and a mirror-independence check.
