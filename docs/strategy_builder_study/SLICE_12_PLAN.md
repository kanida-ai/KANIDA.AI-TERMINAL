# Slice 12 — trustworthy evidence, faster building, closing the partials, multi-expiry

Owner decisions (26 Sep 2026):
- All four themes are in scope.
- The NSE F&O bhavcopy download is approved.

This replaces the parked BANKNIFTY/FINNIFTY calendar decision: the calendar comes from the verified bhavcopy archives.

Execution boundary is unchanged: research and paper only; live stays gated (Shyam).

## A. Trustworthy evidence (the open P0s)

| # | Item | Acceptance |
|---|---|---|
| A1 | **NSE F&O bhavcopy archive** (worker, new files only). Download the daily F&O bhavcopies (old `fo*bhav.csv.zip` format, and UDiFF from Jul 2024) at ≤1 request/s into `db/fo_bhavcopy.db`, one row per contract per day: symbol, instrument, expiry, strike, type, OHLC, settle, OI, contracts. Resumable, idempotent, with a coverage report. | Every trading day in range is present or reported missing with its reason. No fabricated rows. |
| A2 | **Verified expiry calendar** per underlying from A1 (replaces the derived rules). BANKNIFTY/FINNIFTY join the Lab. | The Lab's expiry dates equal the bhavcopy's; the unverified label is removed where verified. |
| A3 | **Point-in-time F&O membership** from A1: a stock is tradable on a date only if it had listed options that day. | A stock that entered F&O in 2023 produces no 2019 trades. The badge text states that membership is point-in-time. |
| A4 | **Realistic stock slippage.** Half-spread by premium bucket, measured from captured bid/ask snapshots, with at least 1 tick. | Stock fills cost ≥ the measured median half-spread; the provenance names the table. |
| A5 | **Bad-price guard.** Overnight jumps over 40% that match no corporate action, and gaps over 10 sessions, block that stock's window. History is checked by continuity, not by the first date. | NMDC 2022-10-27 and the DELHIVERY/360ONE stray rows are excluded, with reasons. |
| A6 | **Evidence versioning.** `EVIDENCE_VERSION` is stored in every entry and recomputed on mismatch. | Changing a statistic setting invalidates every old entry. |
| A7 | Stock width as a % of spot (no look-ahead through the price level); skip an entry with fewer than `exit_dte+1` sessions to expiry; survivors grouped by structure and direction, with a shuffled-weekday placebo. | Tests for each. |
| A8 | **Evidence attached to a strategy:** the Lab evidence behind a strategy's structure is shown on the builder, with its version and date, and goes stale when the rule changes. | The builder shows the deciding run, its version and its age. |
| A9 | **Point-in-time leakage test.** Perturbing any data after a decision date never changes that decision's trade. | Property test over random rules. |
| A10 | Re-run the NIFTY and stock grids on the corrected Lab; BANKNIFTY/FINNIFTY grids. | Honest results reported, whatever they are. |

## B. Faster building
- Linked legs (strike stepping moves the linked group).
- POP on both sides (profit and loss probabilities).
- A chain filter (strike range, liquid only).
- Stock options in the builder (physically settled: the expiry-day warning and exit rule shown).
- Paper exits: rule-based (target/stop) and expiry settlement at intrinsic value.
- Warnings with a one-tap fix (e.g. "add a hedge").

## C. Close the partials
- **Engine:** exact lookup-by-key (worker, engine branch); the pilot's bridge uses it, so an unknown hand-off resolves automatically when the key is authoritatively absent.
- **Phone check:** in the iOS Simulator's Safari.
- **Accessibility:** a keyboard-only and assistive-tech pass.
- **Two-user isolation tests** across every `/api/sb/*` route.
- **Backup/restore drill** of the strategy-builder DB.

## D. Multi-expiry (calendars and diagonals)
- **Analytics:** value at the near expiry, with the far legs repriced by BSM at their own IV; the payoff labelled as a model.
- **Builder and templates:** calendars, diagonals, roll-out.
- **Order review:** per-leg expiry.
- **Adjust:** roll to the next expiry.
- **Lab:** calendar rules come only after A2's verified calendar.
