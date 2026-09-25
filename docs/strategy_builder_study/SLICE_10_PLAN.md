# Slice 10 — GTM audit reconciliation and integrity slice (plan, not built)

Date: 2026-09-26 (IST). Inputs:
- **GTM audit:** `research/gtm-audit-2026-09-25/` (01 product audit S01–S19 / W01–W16, 02 pending PRD P01–P24, 03 verification E01–E05 / C01–C29, and `test_audit_regressions.py`).
- **My audit:** `docs/KANIDA_Requirements_Master_2026-09-25.xlsx` (942 rows; top-10 gaps; "complete before new features").

## 1. How the two audits fit together

The two audits look at different altitudes, so they mostly **complement** each other.

- **My workbook** is platform-wide: data capture, hosting, paywall, backups, legal, evidence honesty and the live path. Only ~157 rows touch the strategy product.
- **The GTM audit** goes deep on the strategy product alone: state integrity across transitions, quote validity, the valuation contract, and mobile.

Re-checked on 2026-09-26: all 3 audit regressions still fail on the current build.
- Crossed quote accepted.
- 2-hour-old sample available to an alert.
- Delta changes 33.75 → 33.43 when only the entry price changes.

### Merged (same requirement, now one row with both sources)

| GTM item | My workbook row | Merged position |
|---|---|---|
| P06 / S18 — 9,856 failed stock runs, raw `'NoneType'…lock` dump | "Fix 4 HIGH stock-Lab findings before re-running the stock batch" | **Root cause already fixed after their audit** (`Daily` works with `store=None`). The UI dump (`Lab.tsx:212`) and the failed batch are still shown. The 4 HIGH findings still block a re-run. |
| P09 — weekday-only session gate, no exchange calendar | watchdog "holidays not known"; Kite token date uses local clock; BANKNIFTY/FINNIFTY calendar parked | One shared calendar service. **Historical** sessions are derivable today (`market_data/calendar.py`). **Future** holidays need a published NSE list, which needs an owner decision (see §4). |
| P15 / P16 — bridge opens only; timeout becomes "refused" | "Live-trading path not connected: intake branch unmerged, options off" | Same gate. Live stays off. Nothing from P15/P16 goes into slice 10. |
| P06 "never rank on failed evidence" | "0 of 480 NIFTY rules pass; evidence release not signed off" | Same honesty rule. Discover already ranks only evidence-board entries. Failed or incomplete batches must never feed the board. |
| P24 — observability, backups, two-user isolation | "No safety net: no backups, no alert channel" | Same platform item. It sits outside the strategy slice. |

### Conflicts / stale statements (flagged, not silently resolved)

1. **"98 tests"** (GTM P24, coverage matrix). That count is the strategy-builder file only. The full pilot suite is ~644. This is not a real conflict, but the workbook will quote both counts with their scope.
2. **Rename "Discover Strategies"** (GTM P11). This conflicts with the owner's Falcon home spec (`docs/FALCON_DISCOVER_SPEC.md`), which uses that nav name for the stock scanner. **Owner decision.**
3. **"Hide Experiments for beta"** (GTM P06 scope option) conflicts with slice 9, where Discover badges come from the evidence board. Proposal: keep the NIFTY board, which is healthy and honestly shows zero passes. Quarantine failed batches.
4. **My top gap #1, "capture stopped since 23 Sep"**, is now resolved: Mac services are live, and the F&O backfill is running (5,200 of 15,832 contracts at 01:07 IST). The workbook row will be updated.
5. **GTM P02 "funds must block when margin unknown"** vs today's paper review, which warns and doesn't block. Proposal: name today's mode "educational paper (funds not enforced)" and block only in a funds-enforced mode.

### Only in the GTM audit (new to my workbook)

Their items P01–P05, P07, P08, P10–P14 and P17–P23 (the S01–S19 detail) are new rows. They go under the theme "Strategy Builder", sourced as "GTM audit 2026-09-25 §P##".

### Only in my workbook (not in GTM scope)

These stay on the platform track, not in slice 10:
- the portal paywall and open endpoints;
- legal and SEBI;
- the two options look-ahead leaks;
- q-value wording;
- hosting and the api.kanida.ai 503;
- the Falcon home placeholder.

## 2. Slice 10 scope: "Integrity" (GTM gates G1 + G2, plus containment of G3–G6)

The principle is the GTM sequence step 1: no new catalog breadth. Each item needs:
- a regression test;
- a desktop check and a 390 px mobile check;
- loading, error and stale states;
- corrected copy.

| # | Item | What changes | Acceptance |
|---|---|---|---|
| 10.1 | **P01 save barrier** | 1. Every edit bumps a local revision. 2. Snapshot, Duplicate, Lab, Review, Adjust and navigation all **flush and await** the save. 3. The snapshot carries `expectedDraftVersion` and `inputHash` (server rejects a mismatch). 4. A 409 keeps the local copy (offer "keep as copy / reload"). 5. The Saved badge means the displayed hash equals the server hash. 6. A Retry now button. | W04 replay: edit, then snapshot within 0–800 ms, and the snapshot has the new value and hash. |
| 10.2 | **P02 quote validity (core)** | 1. One `quote_validity()` function covers crossed, zero, missing, stale option (per-leg event time, not spot time) and future timestamps. 2. The live chain keeps option timestamps. 3. Preview, paper fills and alerts all use it. 4. Analysis results carry `inputHash`, and the client drops stale results; a failed recalculation shows the old values marked stale. | GTM test 1 passes, plus cases for an old option with a fresh spot, and a mixed batch. |
| 10.3 | **P04 cost vs market** | 1. `entryPrice` is kept separate from `markPrice` / `marketIV`. 2. Greeks, POP and SD use market IV from the current quote. 3. Entry changes only P&L and breakevens. 4. "ATM IV" becomes the real chain ATM, or is labelled as a proxy. 5. Deployment Greeks use marks. | GTM test 3 passes, plus golden cases: debit/credit, ratio, zero bid, and entry outside the solvable IV range. |
| 10.4 | **P10 alert freshness** | 1. Each rule has its own sample-freshness requirement (spot for crossing; all held legs for P&L/delta). 2. A stale sample never triggers or rearms. 3. Suppressed evaluations are recorded with a reason. 4. The UI shows the last valid value and time. | GTM test 2 passes, plus stale→fresh with the condition held and no storm. |
| 10.5 | **P09 minimal calendar** | 1. Exchange-local time everywhere (no host `now()`). 2. Session gate and scenario dates read one calendar service. 3. Past sessions come from the derived calendar; future holidays come from a versioned file. **Needs an owner decision on the source (§4).** 4. Without that file, the gate says "holiday list not loaded" and blocks the executable preview. | Holiday, host timezone UTC vs IST, and expiry-day cases. |
| 10.6 | **P03 Discover integrity** | 1. Any input change invalidates results and disables Use/Compare. 2. A superseded request is ignored. 3. Create-from-candidate uses a server `candidateId` holding its own terms (no current form + old legs). 4. The executable price basis is kept. 5. The budget is relabelled "max-loss budget", with margin shown separately as known/unknown. | W06 replay (Rise→Find→Fall): the old Use disappears. There are no mixed terms. |
| 10.7 | **P06 containment** | 1. A failed batch shows counts plus one concise reason; diagnostics stay server-side. 2. Failed or incomplete batches are excluded from the board. 3. Builder → Lab passes the underlying, width and lots, and shows the mapping. 4. "To" becomes "Last decision date", with the later exit disclosed. | No exception flood. A width 2 / size 2 draft does not test width 4 / size 1. |
| 10.8 | **P12 critical mobile + P13 targets** | 1. Order review becomes one card per order at ≤600 px. 2. Chain B/S and leg steppers are at least 44 px. 3. Mobile chain tabs show their data (contract detail row). 4. Add from chain is not clipped. | No overlap at 360, 390 and 430 px (Playwright screenshots). |
| 10.9 | **P07 critical labels** | Fix the misleading strings: chain "no bid/ask in this store", P&L "entry=LTP", paper "LTP + slippage", and the "liquidation bid/ask" label when the mark actually fell back to LTP. The labels are driven by per-leg basis fields. | The same contract has the same label in chain, leg, analysis, preview and monitor. |

**Deferred to slice 11+:**
- P05 safe transforms and undo/redo;
- P08 full state sweep;
- P11 navigation (after the owner's rename decision);
- P14 hierarchy;
- P17–P23;
- the rest of P12/P13.

**Live (P15/P16/P20-live) stays gated, as agreed.** Their 3 regression tests will be copied into `server/tests/test_audit_regressions.py`, with their origin credited.

**Order of build:** 10.2 and 10.3 first (foundational), then 10.1, then 10.4 and 10.5, then 10.6 and 10.7, then 10.8 and 10.9. After that comes an independent audit pass, and then the workbook update:
- new "GTM audit" source rows;
- a `Slice` column tagged "10" / "11+";
- the conflicts above added to the Conflicts tab.

## 3. What slice 10 will not claim

It will not claim:
- live readiness;
- native mobile;
- screen-reader certification;
- a market-open browser rehearsal. The market opens Monday 28 Sep, and a supervised paper rehearsal can happen then.

## 4. Owner decisions needed

1. **Future NSE holiday list for 10.5.** Options:
   - (a) download NSE's published 2026 trading-holiday circular. This is a download, so it needs your yes.
   - (b) you paste the list.
   - (c) ship the gate as "list not loaded, blocking executable preview".
2. **"Discover Strategies" rename (P11):** keep it for the scanner, per your Falcon spec, or rename it as GTM suggests.
3. **Parked reminder: BANKNIFTY/FINNIFTY calendar** (bhavcopy download vs derived calendar). This is due before slices 10–11 and ties into 10.5.

## 5. Owner decisions (2026-09-26)

- Holidays: download NSE's published 2026 trading-holiday list into a versioned file (approved).
- "Discover Strategies" rename: decide later (slice 11 navigation).
- BANKNIFTY/FINNIFTY Lab calendar: parked until slice 11.
- Build all 9 items as scoped.

## 6. Result (26 Sep 2026)

All 9 items are built and independently audited; the fixes are recorded in `BUILD_RESULT_SLICES_1-3.md` under "Slice 10, batch 2".
- The GTM audit's 3 regressions pass.
- Full suite: 673 passed (the 6 derivatives failures predate this slice and are flagged separately).
- The phone layouts were verified at 390 px on a harness with fake live quotes.
- Numbering note: slice 10 batch 1 was the competitor-gap builder features (commit 73d1fe5). This plan is batch 2.
