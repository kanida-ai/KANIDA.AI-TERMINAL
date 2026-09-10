# PATHFINDER — LOCKED ENGINE SPEC (founder-authored, source of truth)

> **Pathfinder is not a content generator that happens to use market data; it is a governed research
> engine that computes market evidence first and uses GenAI only to decide what is worth investigating
> and to explain verified results clearly.**
> *(Keep this sentence at the top. It prevents almost every future design mistake.)*

This supersedes all earlier Pathfinder framing. The prior P0/P1/P2 build (a fixed 288-rule "sober lab")
is **not** this product — rebuild the engine to this spec. Reuse where noted (§Reuse), don't re-derive.

---

## What Pathfinder is
Pathfinder is an autonomous, **clarity-first** AI market-research and experimentation engine. After each
market close it scans the market to identify what is **unusual, important, changing, or worth testing**
across **market, sector, stock, relationship, anomaly, and experiment** themes. It generates the questions
a trader would naturally ask, researches them on historical data, **ranks findings by usefulness, novelty,
evidence strength, and trader relevance,** and publishes only the research that deserves attention. It then
forward-tests promising hypotheses with **virtual capital**, **grades itself publicly**, learns from
outcomes, and **promotes only validated strategies** into the Trader or Investor agents. It is also KANIDA's
autonomous R&D engine.

## The feed (clarity-first, endless, never padded)
The **first 2–3 stories give immediate clarity on "what matters now" in under 30 seconds.** After that the
feed continues with more high-quality discoveries — sector shifts, stock behaviours, anomalies,
relationships, debunks, experiments, watchlist themes. **Swipe vertically** to keep discovering; **swipe
deeper** into a finding for its evidence, related stocks, experiment history, or Pathfinder-generated
follow-up questions. Principle: **front-load clarity, preserve depth, keep discovery alive.** The goal is
**not** feed length — it is trader **understanding, productivity, curiosity, and trust**, with enough fresh
discovery daily that users return to see what Pathfinder discovered, tested, got right, got wrong, and
learned next. *Enter → clarity → leave.*

## Evidence & provenance (computed before it is written)
Every finding is **computed before it is written** and carries its evidence: **sample size, historical
period, market regime, comparison group, and the transaction-cost hurdle** used to judge meaningfulness.
Pathfinder states clearly whether evidence is from the **same stock / peer group / sector / whole market.**
Published findings are later **revisited and graded — Right / Wrong / Inconclusive — with a visible running
scoreboard (with n)**, so the system builds accountability, not just attractive narratives.

## The experiment loop (a living research scientist, not a dashboard)
When Pathfinder finds a promising hypothesis it moves into autonomous experimentation:
**Observe → Ask a question → Research → Backtest → Decide if worth testing → Deploy virtual capital →
Track the forward outcome → Compare expected vs actual → Learn → Change the experiment if required →
Run the next experiment.**
Virtual capital converts historical research into forward-tested evidence. Pathfinder should be able to say:
*"Historical expectation was +1.2%. The forward virtual experiment returned +0.8%. The edge is still
present, but weaker than expected,"* or *"The experiment failed. Most failures occurred when breadth was
weak. I added a breadth condition and will test Version 2 next."* That visible learning loop is central.

## Boundaries (research vs execution vs real money)
- **Pathfinder virtual capital = autonomous research experiment.**
- **Trader Agent virtual capital = execution of a declared strategy** (mandate, disciplines, risk rules).
- **Real-money AutoTrade = a separate execution layer.**
- Stock-specific Pathfinder findings are **research items, not trade instructions** — **no user-facing
  entry / target / stop / execution** on the research surface; those pass RA/compliance review first.
- A successful Pathfinder experiment can **graduate** — a short-horizon edge → Trader Agent; a durable
  long-term relationship → Investor Agent — **only through the existing champion/challenger promotion
  gate**, never by narrative.

## Customer narrative (always simple)
*I noticed this. I researched it because of this evidence. Here is what history showed. I decided to test it
with virtual money. Here is what happened. Here is what I learned and changed. Here is what I'm doing next.*
Win rate, return, sample size, drawdown, virtual capital, performance **support** that story, never dominate.

---

## The 12 build principles (preserve exactly)
1. **No forced content volume.** Publish only when the usefulness threshold is met. Four strong findings beat twenty weak ones.
2. **AI does not invent statistics.** The AI identifies, selects, prioritizes, explains, narrates. All returns, win rates, sample sizes, regimes, comparisons, evidence come from **deterministic computation**.
3. **Research questions come from an expandable library.** Pathfinder decides which *approved* questions are relevant today. New templates are added deliberately; the model never fabricates arbitrary tests.
4. **Every finding has provenance** — same stock / peer group / sector / whole market + sample size, period, regime, comparison group, cost hurdle.
5. **Every published claim has a pre-defined grading rule.** Horizon and Right/Wrong/Inconclusive criteria are **frozen before publication** — never decided after the event.
6. **Every experiment has memory and versioning** — `#8421 → v1 → v2 → v3`, with what changed, why, number of trials, forward results, and retirement criteria. Prevents silent optimization / p-hacking.
7. **Virtual money is part of research validation.** History generates a hypothesis; **forward virtual performance decides whether it survives** outside the backtest.
8. **Pathfinder can fail publicly.** The scoreboard shows `Right 61 · Wrong 24 · Inconclusive 15 · n=100`, not just wins.
9. **Successful research does not auto-become a product strategy.** Promotion only through the champion/challenger gate (OOS performance, net returns, drawdown, approved criteria).
10. **Pathfinder ≠ Trader.** Pathfinder discovers/validates with research capital; Trader operates a declared mandate; AutoTrade executes with real money.
11. **Compliance enforced in architecture.** Stock-level research publishes only through the approved RA workflow. Until counsel resolves public virtual baskets/performance, the customer-facing experiment must not look like a trade recommendation.
12. **The customer experience stays simple** despite the complexity underneath (the 7-line narrative above). Null calibration, OOS gates, versioning, costs, regimes, trial counts, evidence lineage make the story *trustworthy* without turning the UI into a quant terminal.

## Spec addenda (make explicit in the build)
1. **No minimum card count** — gated by a usefulness threshold, not a quota. "Pathfinder found four things today" is a valid edition. Never pad.
2. **Question library, not free generation** — `question → parameters → computation → evidence card`. New question types are templates, **founder-reviewed**. The AI narrates computed results; it never invents them.
3. **Experiment registry with versions and trial counts** — ID, versions, the change + why between versions, number of variants tried, a retirement rule (max N revisions before the idea is buried). Trial count shown with results so the loop can't become silent p-hacking. **Reuse NDP's null-calibration and the arena's promotion gate.**
4. **Grading rules per finding type**, defined before publication (e.g. `NO TRADE` is Right if the trade would have lost after costs; a **theme call** is Right if the sector outperformed Nifty over the stated horizon; **Inconclusive** if within the cost hurdle). Running scoreboard with n always visible.
5. **Graduation via the existing promotion gate** (champion/challenger: positive OOS edge, beats incumbent on net return, lower drawdown) — never by narrative.
6. **Compliance of virtual-capital experiments** — a published virtual basket with tracked returns may be read by SEBI as a recommendation. **Counsel question:** how to label experiments as research not advice ("research experiment — not a recommendation; no entry/exit for users"), and whether to enrol with PaRRVA. Until answered, experiment baskets show **sector/theme + evidence**; constituent lists appear only inside the app under RA review.
7. **Evidence provenance on every card** — same stock / similar stocks / sector / whole market, with n, period, regime, comparison group, cost hurdle.

---

## Reuse (validated — do not re-derive)
- **The deterministic computations are already prototyped and proven on real data** in `Documents/Kanida_Falcon`: `pathfinder_theme.py` (theme/cycle detection + proof + watchlist + verdict) and `pathfinder_demo.py` (group base rates across market/dip/surge/anomaly/relationship, plain-English horizons, cost-aware decisions, the daily verdict). **These are the reference implementation for the engine's math.**
- **R&D engine pieces to port:** `engine/state_engine/baseline.py` (base rates), `scripts/regime.py` (regime), the **arena** promotion gate + **NDP** null-calibration (addendum 3/5), `confirm_and_trade.py` (virtual book), `db/kanida.db` (data).
- **From the prior build, reuse the scaffolding, not the wrong engine:** the P0 API-contract pattern + `pathfinder_llm` gateway (AI narrates, engine computes), and the P2 Expo app shell — re-skinned to the **swipeable clarity-first feed** and the founder's mockups.

## Build instruction
One fresh session rebuilds the Pathfinder engine to THIS spec, reusing the reference computations and R&D
pieces above. The AI (Claude, via the gateway) does **only** selection/prioritization/narration; every
number is deterministic. Honor the 12 principles and 7 addenda as acceptance criteria; `dev-quant-auditor`
verifies no number originates in the LLM and that null-calibration/OOS/cost-hurdle/grading are real.
