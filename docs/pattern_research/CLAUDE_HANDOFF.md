# Claude Code + Codex pattern research work order

User authorization: implement the approved catalogue together, use the existing local OHLCV, backtest and walk-forward test with multiple workers, and verify accuracy. Vendor integration is separate. Catalogue: `docs/PATTERN_CATALOGUE_PROPOSAL.md` and `.json`.

## Ownership

- **Claude Code:** new detector modules and their tests. You may delegate to your own workers with non-overlapping files. Own `market_scanner/pattern_research/common.py`, `candlesticks.py`, `chart_patterns.py`, `price_action.py`, `harmonics.py`, plus `tests/test_detectors_*.py`. Own `docs/pattern_research/DETECTOR_STATUS.json` and `DETECTOR_NOTES.md`.
- **Codex:** package initialization, runner, result storage, legacy adapter, evaluation engine, batch runs and integration/audit. Do not edit these or existing production files.
- A Codex audit worker owns `docs/pattern_research/AUDIT_FINDINGS.md`.

All changes are additive. Do not edit the source database, existing results, existing registry/config or existing production scanner. No deployments, trades, credential access or vendor calls. Read only project files necessary for this research. The shared directory is `C:/Users/SPS/Documents/Kanida_Falcon`.

## Detector interface (freeze this contract)

Each of the four detector modules exports:

```python
def specifications() -> list[dict]: ...
def detect(bars: list[dict], timeframe: str) -> dict[tuple[str,str,str], list[dict]]: ...
```

`specifications()` includes every supported study cell, even if no occurrence is found:

```python
{"pattern_id":"CDLENGULFING", "variant":"canonical_context", "side":"long",
 "name":"Bullish engulfing", "family":"candlestick", "definition_version":"1.0.0",
 "states":["setup","confirmed"], "lookback":40, "definition": "... exact numeric/context rules ..."}
```

`pattern_id` must match catalogue IDs: CH11–CH28 (Codex owns CH01–CH10 legacy adapter), all 61 CDL IDs, PA01–PA08, HA01–HA10. `variant` separates structural/definition variants. `side` is `long` or `short`. Family is `chart`, `candlestick`, `price_action`, or `harmonic`.

`detect()` returns grouped events keyed by `(pattern_id, variant, side)`, sorted by signal_index. Every event includes:

```python
{"signal_index":100, "episode":97, "state":"setup", "atr":2.0,
 "score":1.0, "direction":"bullish", "pattern_start":"2020-01-01 09:15:00",
 "formation_start_index":97, "detected_index":100}
```

Optional fields: `geometry`, `alias_group`, `library_value`, `quality_tags`, `confirmed_index`. State is `setup` or `confirmed`. signal_index is the actual availability bar (not an earlier pivot). episode links confirmation to its setup. Distinct adjacent candle patterns must remain distinct episodes; never apply the legacy three-absent-bar rule to all candles. Avoid repeated identical events. All outputs finite/JSON-safe and causal under prefix truncation and future perturbation.

Bars match the existing frozen histories: time/end/open/high/low/close/volume/gap (gap is an existing data-quality flag, NOT every valid overnight price gap). Use only available closed data. A flagged quality gap disqualifies affected shapes; no gap filling. Trend context is measured before the formation, as specified in catalogue. Neutral shape codes cannot become longs merely because library output is positive: provide independently confirmed direction variants or explicitly labeled descriptive directional studies.

## Implementation requirements

Use canonical TA-Lib for all 61 candle detectors, pin installed version and penetration parameters, do not handwave unsupported functions. Codex will install TA-Lib into `market_scanner/.venv`; Python is `C:/Users/SPS/Documents/Kanida_Falcon/market_scanner/.venv/Scripts/python.exe`. NumPy/Numba/pandas are already installed. 12 logical CPUs, ~32 GB RAM. Do not launch full-universe backtests yourself; Codex will run them after integration gates.

Implement all 18 additional chart, eight price-action, and ten harmonic catalogue families with explicit numeric contracts. Record any unimplemented variant candidly; never return a placeholder detector and call coverage complete. Preserve causal confirmed pivots (radius 3) and timestamp late pivots when known. Optimize full-history detection: vectorize candles; evaluate geometric candidates at meaningful pivot changes instead of expensive unconstrained all-window/all-bar searches. Correctness takes precedence over speed.

For every family write meaningful positive/negative fixtures, prefix/future-data invariance, identity/state tests, and checks preventing missing-data gaps from creating patterns. Cross-check the 61 raw candle recognitions against the library. The shared interface above is mandatory so Codex can independently evaluate.

## Coordination

First create `DETECTOR_STATUS.json` with overall `working`, owner, modules, and status. Update when a module is ready with files, test commands/results and known limitations. Keep `DETECTOR_NOTES.md` concise. Read `AUDIT_FINDINGS.md` when available. Do not modify Codex-owned files. Ask interface questions by writing `docs/pattern_research/QUESTIONS_FOR_CODEX.md`.

Save code in the actual shared workspace, not an isolated unsynchronized checkout. Do not change the application's security/permission settings. Leave running user work alone. Return a concise final handoff when all modules are ready; Codex owns independent review and the final research verdict.
