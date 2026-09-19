# KANIDA backlog

One list. Top item is worked first. Nothing new is built unless it is on this list.

How we work:
- One item at a time. Before/after screenshot for every screen change.
- The owner approves each item before the next one starts.
- Before starting: rough time and cost. During: an update every 30 minutes.

Status: ⬜ not started · 🔄 in progress · 👀 waiting for owner review · ✅ done

---

## 1. Today's prices show in the app — ✅ done (17 Sep 2026)
**Done means:** during market hours the app shows today's prices.
**Result:** prices arrive every 15 minutes (verified 09:15, 09:30, 09:45 bars). Patterns refresh when each 1-hour candle completes (first one of the day at 10:15).
**Follow-up moved to item 2:** the top bar shows today while the chart still shows yesterday until 10:15 — confusing.

## 2. Make the Discover page simple — 👀 waiting for owner review (17 Sep, screenshots in `qa/item2/`)
**Done means:**
1. No card opens empty. The default strategies are ones that have setups right now.
2. One count per card. Two tabs only: **Now (N)** and **History (N)**. No "Spotted today" chip, no extra "3 today" line. Setups found in the last session get a small "new" marker instead.
3. Each row reads cleanly: stock, company, Forming/Confirmed, "since 16 Sep". Nothing cut off.
4. No research words on the page: no "walk-forward", "very_small", "requires review", "tier", "ledger".
5. The page and the top bar never disagree about dates. One short line explains it: "Prices 09:45 · patterns updated 10:15".
6. The old 10-pattern block is hidden, not shown with an explanation.
**Not in this item:** what the evidence card says (item 3).

**Signing this off — or rejecting it — has a consequence in the test suite.** Five checks in
`scripts/check-discover.e2e.cjs` assert the behaviour this item removed. They are **parked**, not deleted:
they report as `PARK`, carry their reason in the output, and do not fail the run. They exist so that a
rejection cannot quietly lose the coverage.

| If the owner… | Then |
| --- | --- |
| **signs it off** | retire **15.1**, **15.2** (stored block collapsed-and-expandable) and **15.5** ("Spotted today" chip); retarget **13.5**, **13.6** at a *research* strategy, since a stored one has no block to appear in. Then remove them from `PARKED` in the script. |
| **rejects it** (wants the stored block or the chip back) | un-park them — they are already written and are exactly what catches the old behaviour not coming back properly. |

If any parked check starts **passing**, the suite says so loudly (`ATTENTION: parked check(s) … now PASS`):
the behaviour has returned and the park is stale either way.

## 2a. The app always shows the last scan — 👀 waiting for owner review (owner request, 18 Sep)
**Done means:** when the scanner is restarting, loading or unreachable, the app still shows the last scan it had, with a line saying when it was taken ("last scan 17 Sep 15:30 · reconnecting"). No "Strategies unavailable" or "Research is unavailable" error for a temporary outage. A genuine, lasting failure is still reported clearly, but only after the app has shown what it has.
**Result:** the pilot keeps the last good copy of every read the app draws with (`var/last_good.sqlite3`, capped at 200 entries / 24 MB, evicted oldest first, survives a pilot restart). While the scanner is quiet the app shows that scan and says when it was taken; after 5 minutes the same data is shown with the connection stated plainly. The scanner now opens its port before loading the snapshot, so a caller during startup gets "starting, N% loaded" instead of a refused connection. Nothing that decides a trade reads the cache: plan, exit-plan, simulate and live submission still fail closed on an unreachable scanner, and position sizing is never replayed.

## 2b. Derivative tab (F&O) — 🔄 in progress (owner priority, 18 Sep)
**Done means:** a new **Derivative** tab next to Discover Strategies, built on 15-minute Kite data, showing: open-interest build-up per strike, volume versus a contract's own average, volume-to-OI spikes, premium traded in ₹, put-call ratio, max pain, and futures OI build-up. Cards over a linked chart, like the TrendSpider dashboard, with filters. Display only — it says what is happening, never what will happen. Spec: `docs/DERIVATIVES_SPEC.md`.
**Note:** options data cannot be recovered once contracts expire, so capture runs from 18 Sep onward.

## 3. Rewrite the evidence card in trader language — ⬜
**Done means:** 3–4 plain lines. How many past cases, what usually happened next, the typical best and worst move, and how that compares to just holding the stock. The number of past cases is shown first; small samples are clearly marked and never shown as big headline numbers. Short setups read correctly (a fall is good for a short).

## 4. Fix "add strategy" on the admin page — 👀 waiting for owner review (18 Sep)
**Done means:** the owner can add, preview and switch on a new strategy from the admin page without an error.
**What was actually wrong:** the form built its pattern buttons from `/api/state` — which is whichever detector
set the scanner happens to be running — and posted every one of them as a "stored pattern". Since the scanner
moved to the researched 107, every button was a researched pattern posted in the stored shape, and the server
refused all of them. The registry itself was fine: it has accepted both kinds all along.
**Result:** the form now asks the server what it will accept (`/api/admin/strategy-sources`) and offers exactly
that — the 10 stored scan patterns and the 107 researched ones, whatever the scanner is running — and posts each
in its own shape. Picking a pattern narrows the variant, side and timeframe to what was actually researched, so
the form can no longer offer a combination the server refuses. Verified on the QA pilot (8083): the old request
is still refused, and now says which source would work; a stored pattern and a researched one both save, preview
(495 researched stocks) and switch on.
**Unchanged on purpose:** a new strategy is created switched off, and its evidence card still says "Historical
data requires review" until the owner signs the data off — adding a strategy from this page never publishes
anything.
**Note:** the owner's pilot is running the exported web build from 07:41 today, so the new form appears there
only after the next web export.

## 5. Decide the product direction on evidence — ⬜
**Done means:** the owner decides how the app presents the finding that patterns alone do not beat holding the stock, and what we test next (pattern + market conditions, or pattern + existing quant work).

## 6. Switch from Kite to the data vendor — ⬜ (waiting on vendor details)
**Done means:** prices come from the vendor by changing settings only; the vendor passes the checks in `market_data/tests/test_conformance.py`; the app shows "15 min delayed" correctly.
**Storage side of the same decision:** `docs/STORAGE_PLAN.md` — measured plan to stop storing price history (166 GB → 3.2 GB) and keep only a 140-session rolling window; awaiting owner approval before anything is deleted.

---

## 7. The expanded chart legend sits on top of the chart — ⬜ (found 18 Sep, measured)

**What happens.** The legend floats over the chart's top-left corner. Collapsed it is a thin 32px strip and
costs nothing. Expanded it becomes a full panel sitting directly on the candles, and there is no way to get
rid of it without collapsing the whole legend.

**Where, measured rather than guessed.** It expands itself whenever the chart pane is taller than 360px:

| Screen | Panel size | How much of the chart it covers | Candles hidden |
| --- | --- | --- | --- |
| Desktop | 534 × 386 | about 65% of the height | 26 |
| Tablet | 420 × 356 | about 66% of the height | 32 |
| Narrow, phone | 32px strip | none — 6px of clear space | none |

**How much of this is new.** 18px of the depth came from the 18 Sep fix to the chart height: the chart grew
upward while the legend stayed where it was. The other ~340px has been there far longer and is simply what
happens when a panel that size floats over a chart. Recorded so it is not re-argued later.

**Also the same problem.** When there is no setup to show, the *collapsed* legend carries a list of stocks
that fall outside your filters — up to about 160px over a 108px chart. Too big to clear by any means that
keeps the chart readable.

**Done means:** the reader can see the full legend and the full chart at the same time. The recommendation is
to give the expanded panel its own column beside the chart on desktop and tablet, where there is width to
spare. Lowering the chart's price scale to make room is NOT the answer at this size — 386px of headroom would
squash the candles into the bottom quarter, which satisfies the measurement and gives the reader less chart.

---

## Known smaller issues (not scheduled yet)
- At full desktop width the first card's labels can wrap onto three lines.
- The full-screen chart shows 500 of ~50,000 live detections.
- Record the suite script's hash next to the bundle's timestamp in every QA run. On 18 Sep two workers
  changed the bundle and the check script at the same time; only the bundle was being tracked, so a
  change in the script looked like a regression in the app and cost real time to disprove.
- Evidence cards say "data requires review" until the owner signs off the data (part of item 5).
- The Discover dock fits two 46px rows in about 102px — ten pixels of slack. Nothing is wrong today, but a
  default height that barely clears its own stated minimum will keep producing near-misses. The dock height
  is owned by the workspace layout, not by `DiscoverPanel`.

---

## QA baseline — what a clean run looks like

Without this, nobody can tell a regression from a failure that was already there. Update the numbers **and
the named failures** whenever a run legitimately changes them, and say why in the same commit.

**Taken:** 18 Sep 2026, against `dist-pilot` exported **10:52:13** (`2026-09-18T17:52:12.947Z`), suite script
`check-discover.e2e.cjs` **sha256 `8bb5c28db3db`**, the UI-QA server on 8083, and the scanner on 8765 in the
**research** pattern set (run `4b33a5249562631524d6`).

**Always record both.** The suite now prints `Running against: script <sha> · bundle <mtime>` at the start and
end of every run, and stores them in `results.json`. We lost real time to a script change that read as a
bundle regression because neither was written down; a result quoted without both is not reproducible.

| Suite | Command (from `kanida-app`) | Clean result |
| --- | --- | --- |
| Discover acceptance | `node scripts/check-discover.e2e.cjs` | **107 checks · 100 passed · 0 failed · 5 parked** (the five below) |
| Workspace, short viewport | `node scripts/check-phase0.cjs --only=short` | **13 checks · 0 failed** (10 consecutive runs, all green) |
| Pilot server | `PYTHONPATH=server .pilot-venv/Scripts/python.exe -m pytest server/tests -q` | **270 passed** |
| Scanner | `market_scanner/.venv/Scripts/python.exe -m pytest market_scanner/tests -q` | **432 passed, 186 subtests** |
| Types | `npx tsc --noEmit` | clean |

**The five are PARKED, not failing.** Each asserts behaviour item 2 removed, and item 2 is still awaiting the
owner's review, so they are waiting on a decision rather than reporting a defect. They do not fail the run;
the consequence of either decision is written under item 2 above. A parked check that starts passing is
announced loudly, so neither outcome loses the coverage silently.

| Check | Asserts | Item 2 says |
| --- | --- | --- |
| 15.1, 15.2 | the stored 10-pattern block is collapsed behind a line and expands with a reason | "The old 10-pattern block is **hidden**, not shown with an explanation" — the server drops it (`strategies.py catalog()`) |
| 15.5 | the "Spotted today" chip narrows the list | "**No** 'Spotted today' chip" — it no longer exists anywhere in `src/discover/` |
| 13.5, 13.6 | an admin-enabled **stored** strategy appears in / leaves the Discover picker | same as 15.1: on the research set there is no stored block to appear in. Needs a research strategy instead, or to be skipped on this pattern set |

**Reading the numbers safely**
- The row counts move on their own (a scanner restart took the workspace list from 396 to 403 setups in one
  afternoon). No check should hard-code a count, and none does now.
- The block's default A/B strategies are registry **data** and move too. The suite reads them from
  `/api/strategies/catalog` at startup and prints them: `PASS pattern set: research (defaults A="…" B="…")`.
  A check that hard-codes two strategy names will fail on data rather than on behaviour.
- The QA sign-in throttle is 12 attempts per 900 s (`server/kanida_pilot/auth.py`). Each suite run costs
  several. Past the cap, runs fail at `FAIL login` with **1 check · 1 failed** — that is the throttle, not the
  app. Wait the window out; do not clear the counter.
- Readiness in the Discover suite is "the card's own tab count is ≥ 1 **and** at least one row is rendered",
  never the status line: the status line is legitimately empty once a list is complete.
