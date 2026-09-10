# Pathfinder — Build Session S2: THE EXPERIMENT LOOP (virtual capital, versions, graduation)

> Fresh session in this repo. Read `CLAUDE.md`, **`docs/sessions/PATHFINDER.md`** (LOCKED spec — governing
> sentence is law), **`docs/handbacks/PF-S1.md`** (what S1 built — align to its engine modules and feed
> contract), and `docs/DOC_MAP.md`. Then this brief.

## Goal
Turn S1's research findings into **autonomous, forward-tested, versioned, publicly-graded experiments** —
the living research-scientist loop:
**Observe → Ask → Research → Backtest → Decide if worth testing → Deploy virtual capital → Track forward →
Compare expected vs actual → Learn → Change the experiment if required → Run the next experiment.**

## Scope — IN
1. **Experiment registry** (spec addendum 3): every hypothesis has an ID, **versions** (v1, v2…), the
   **change made between versions and why**, the **number of variants tried** (shown with results — no
   silent p-hacking), and a **retirement rule** (max N revisions before the idea is buried).
2. **Promotion from finding → experiment**: an S1 finding that clears the "worth testing" gate (evidence
   strength + novelty + cost hurdle) opens an experiment v1 with a **defined virtual basket / hypothesis**, a
   **stated period/horizon**, and the **historical expectation recorded up front**.
3. **Virtual capital + forward tracking**: deploy virtual capital to the defined basket for the stated period;
   **mark deterministically** (next-open entry, costs + slippage, point-in-time); track the actual forward
   result. Reuse the virtual-book machinery (S1/P1 `book.py`, `Kanida_Falcon/scripts/confirm_and_trade.py`).
4. **Expected vs actual + learn**: at horizon end compare historical expectation vs forward result and produce
   the honest statement (*"Historical expectation +1.2%; forward virtual +0.8% — edge present but weaker"* /
   *"failed; most failures when breadth was weak; added a breadth condition; testing v2"*). The **learning
   changes the experiment** → v2 (recorded: what changed, why, trial count).
5. **Grading + scoreboard**: experiments graded Right / Wrong / Inconclusive by **pre-frozen** rules
   (addendum 4); the S1 scoreboard extends to experiments; **failures are public**.
6. **Graduation gate** (addenda 5/9): promotion to Trader/Investor **only** through the existing
   champion/challenger gate — positive OOS edge, beats incumbent on net return, lower drawdown — using the
   **arena promotion gate + NDP null-calibration** from `Kanida_Falcon`. Never by narrative. Emit a
   *proposal* (human-gated), never an automatic switch.
7. **Experiment cards in the feed**: extend S1's `/api/pathfinder/feed` + add `/api/pathfinder/experiments`
   and `/experiment/{id}` (versions, trial counts, expected-vs-actual, learning, next) — the 7-line customer
   narrative: *I noticed → I tested → history showed → I decided to test with virtual money → what happened →
   what I learned and changed → what I'm testing next.*
8. **Compliance default** (addendum 6): public experiment cards show **sector/theme + evidence**; constituent
   lists only inside the app under RA review; **no user-facing entry/target/stop**.
9. **Governance**: honor the L1–L4 hierarchy in `config/pathfinder_constitution.yaml` (agent tunes within
   approved ranges; Constitution human-only).

## Scope — OUT
The swipeable frontend feed (S3). Real-money anything. Trader/Investor agent internals.

## Reuse
S1's engine + feed contract · P1's `backend/pathfinder/engine/{book,repository,governance}.py` (append-only,
hash-chained persistence; virtual book; L1–L4 types) · `Kanida_Falcon` arena promotion gate + NDP
null-calibration + `confirm_and_trade.py` · `config/pathfinder_constitution.yaml`.

## Done when
- A real S1 finding opens an experiment v1 with virtual capital and a recorded expectation; it is
  forward-tracked deterministically; expected-vs-actual is computed and stated honestly; a learning creates v2
  with what-changed/why/trial-count; retirement and the graduation gate both work (a retired idea and a
  proposal are demonstrable).
- Scoreboard covers experiments; failures visible; trial counts shown.
- Feed + experiment endpoints serve it; narrative digit-free; provenance everywhere.
- `dev-quant-auditor` confirms: no LLM numbers, real null-calibration/OOS/costs, no silent optimization.
- Committed locally on `feat/product-build` (never push/main). Hand-back `docs/handbacks/PF-S2.md`.
