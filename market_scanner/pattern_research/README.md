# Isolated expanded pattern research

Uses verified frozen OHLCV from the active original backtest run. It creates a separate SQLite summary database and compressed detailed artifacts under `market_scanner/output/expanded_research`. It does not activate the production scanner registry or write to `db/kanida.db`.

From the repository directory in PowerShell:

```powershell
# Unit fixtures and research execution checks
& ./market_scanner/.venv/Scripts/python.exe -m unittest discover -s market_scanner/tests -p 'test_detectors_*.py' -v
& ./market_scanner/.venv/Scripts/python.exe -m unittest discover -s market_scanner/pattern_research/tests -v

# Independent full-registry and real-data causal-prefix gate
& ./market_scanner/.venv/Scripts/python.exe -m market_scanner.pattern_research.validation

# Small full-registry pilot, all four intervals
& ./market_scanner/.venv/Scripts/python.exe -m market_scanner.pattern_research.runner --symbols TITAN LTTS --workers 2

# Full existing source universe; requires a current successful validation record
& ./market_scanner/.venv/Scripts/python.exe -m market_scanner.pattern_research.runner --workers 4

# Resume using the exact manifest path printed by the runner
& ./market_scanner/.venv/Scripts/python.exe -m market_scanner.pattern_research.runner --execute-manifest C:/absolute/path/to/manifest.json

# Build the selected run's coverage and result report
& ./market_scanner/.venv/Scripts/python.exe -m market_scanner.pattern_research.report RUN_ID
```

`--modules legacy` limits a pilot to the original ten pattern IDs. Omitting `--symbols` selects all complete stocks in the source run, not every symbol ever present in the source database. Timeframes are 1H, 4H, 1D and 1W. Variant and side create separate study cells.

Preparation records code, dependency and history hashes, definitions, selected universe and protocol. Execution runs from the copied code directory. Resume verifies saved artifacts before skipping completed stocks. Code or dependency mismatches fail explicitly. Historical run identity never silently acquires new definitions.

The evaluator produces a whole-history fixed-horizon descriptive baseline and a separate 36-month-training/6-month-test rolling walk-forward study. Training requires 20 nonoverlapping trades and a positive mean-minus-standard-error score. Entry is next eligible open; costs total 40 bps per round trip. See the recorded assumptions and `docs/pattern_research/EVALUATION_NOTES.md` for gaps, stops, embargo, final-sample censoring, cohort accounting and short-side limitations.

Coverage completion means the selected jobs finished and their artifacts verified. No-occurrence, insufficient-history, insufficient-training and small-test-sample outcomes are legitimate completed studies. They do not establish that a pattern predicts profitable trades. Full-catalogue/full-universe coverage is reported separately from pilot coverage.
