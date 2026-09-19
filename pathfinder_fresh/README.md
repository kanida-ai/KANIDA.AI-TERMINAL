# Pathfinder Fresh Engine

This is a clean-room Pathfinder implementation. It imports nothing from the legacy
`pathfinder` package and reads OHLCV only from `db/kanida.db` in this workspace.

The execution boundary is deliberately narrow:

1. observe the latest complete market session;
2. select only from the reviewed question library in `questions.py`;
3. run the matching deterministic computation;
4. attach provenance, sample size, period, regime, comparison group, costs, and a null-calibrated result;
5. rank the candidate and publish it only if it clears the usefulness threshold;
6. freeze the future grading rule before publication;
7. launch eligible virtual-capital experiments;
8. grade due findings and experiment runs as Right, Wrong, or Inconclusive;
9. version failed hypotheses, disclose the change, and retire after the configured limit;
10. evaluate graduation only through the champion/challenger promotion gate.

Run from the workspace root with the bundled Python runtime:

```powershell
& 'C:\Users\SPS\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' `
  -m pathfinder_fresh.cli --replay-from 2026-05-01 --as-of latest
```

The engine writes three artifacts under `pathfinder_fresh/output/`:

- `pathfinder_edition.txt`: clarity-first customer narrative;
- `pathfinder_public.json`: public payload with unreviewed constituents withheld;
- `pathfinder_audit.json`: internal evidence and experiment audit trail.

The public experiment label is intentionally conservative: research experiment,
not a recommendation. There are no user entry prices, targets, stops, or execution
instructions. Counsel and RA review remain required before exposing stock-level
constituents or public virtual-basket performance.
