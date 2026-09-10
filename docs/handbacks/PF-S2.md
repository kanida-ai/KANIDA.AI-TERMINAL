# Hand-back — Pathfinder Session S2: THE EXPERIMENT LOOP (post-audit, post-re-audit)

**Date:** 2026-09-10 (IST) · **Branch:** `feat/product-build` · **Brief:** `docs/sessions/PATHFINDER_S2_EXPERIMENTS.md`
under the LOCKED spec `docs/sessions/PATHFINDER.md`, on top of S1 (`docs/handbacks/PF-S1.md`).
**Status:** built to the brief's "done when", audited by `dev-quant-auditor` (§4: sixteen findings, every
CONFIRMED one fixed and pinned) and then **re-audited independently** (§4.1: nine findings N1–N9, every one
fixed and pinned in `backend/tests/test_pathfinder_s2_audit2.py`, each pin verified to FAIL on the pre-re-audit
commit `187c488`); every S1 suite passes unchanged (134) plus P0 (37), P1 (60), the S2 suite (44) and the
re-audit suite (19) — **294 passed**; the real-warehouse registry was rebuilt twice from scratch on the
committed code and is identical, chain hashes included (§3.4); committed locally, not pushed.

> **Governing sentence, honoured in code:** the engine computes every number; the model never
> produces one. In S2 the model's only permitted act is to re-write the BODY of one of the seven story
> beats under the engine's headline through the S1 strict contract plus the addendum-6 lint; no key is
> configured, so every narrative in the store says `produced_by = engine`. Every number on every surface
> is a `Fact` minted by `FactSet.add()` with provenance pinned to `pathfinder_experiments@1.0.0+code.<hash>`.

> **The one thing to read first — and it changed twice, once per audit.** On the real warehouse, with the
> six S1 templates and the S1 hurdle (0.50% round trip), **no unconditioned S1 hypothesis clears costs on
> the whole sealed history at any edition since 2025**. The loop therefore takes the S1 card's own
> follow-up question, evaluates a CLOSED set of 18 conditioned variants with every trial counted, and
> applies the Constitution draft's gauntlet plus the NDP promote-only-if-it-holds check. **Before the first
> audit** one variant cleared — dips on a session the whole market fell more than a percent, over five
> sessions — and a real experiment ran forward for twelve weeks (§3.3, the auditor reproduced it). The first
> audit's finding 10 conditioned the placebo pool and the variant scored p = 0.076 against 0.05. **The
> independent re-audit then showed that the number the loop had frozen was not the expectation of the
> strategy the book trades at all (N1):** the +0.25% per trade was measured over every firing equal-weighted,
> while the book can take at most five names per session by liquidity, ten at once, one per symbol —
> and the population it can actually own returns **−0.29% per trade** (n 1,682 on 395 days; −0.37% on the
> twelve discovery years; +0.43% on the trailing window, of which three days carry 160% and which is
> +0.13% without 2025-04-04). The re-audit also replaced the forward null (N2): under a fixed-day
> stock-permutation null the archived forward record (+1.05% on 23 trades, five days) scores **p = 0.34**,
> not 0.16. The served registry holds **zero experiments, 131 declined findings and 72 counted trials**,
> and now declines the dip family on **expectancy, twice the slippage, edge and placebo at once** — not on a
> single gate. **The negative result is the finding** (§8.1): the S1 dip / surge families carry no
> tradeable edge on this data. Nothing was softened to keep an experiment. **Everything in every registry
> is a simulated backfill (`backfilled = 1`, forward n = 0).**

---

## 1. What was built

A new package, `backend/pathfinder/experiments/`, on top of S1's `research/` (untouched: `schemas.py`
gained the S2 contract, `router.py` a third source). Reuse, not re-derivation: P1's `engine/book.py`
conventions ported onto S1's sealed long frame; P1's `engine/replay.py` day-blocked placebo, context-
conditioned pool and cluster-robust t; P1's `engine/governance.py` `GateSet` / `Constitution` /
`check_implementable`; P1's `engine/repository.py` hash-chain; the arena's `constitutional_score`
verbatim (`Kanida_Falcon/arena/arena.py`); NDP's train→validate→promote-only-if-it-holds
(`scripts/mine_phase1.py`, `TRAIN_MAX = 2024`).

| Module | What it owns |
|---|---|
| `config.py` | Every knob, env-overridable; `ENGINE_VERSION = pathfinder_experiments@1.0.0+code.<sha256[:12] of experiments/*.py>` on every edition and every fact. Founder stubs marked (§6). Numbers the Constitution draft already states (capital, position limits, gauntlet bars, LLM budget) are read from it and nowhere else. |
| `hypotheses.py` | **The closed hypothesis library**: `finding -> family -> variants`. Three families (`dip_bounce` from a `dip` card; `surge_fade` / `surge_chase` from a `surge` card); nine CONDITIONS (any session · market fell · market rose · market fell > 1% · RISK_ON · NEUTRAL · RISK_OFF · breadth ≥ 50% · breadth < 50%; all causal at the signal close) × two HORIZONS (1, 5 — S1's, inside the draft's `_exits` range) = **18 variants in a fixed order, every one a trial**. `IMPLIES` / `EXCLUSIVE` make a no-op or empty revision unrepresentable. `replay()` measures the strategy the book trades — `f{h}` (next open → horizon close) minus the hurdle — **over the population the book itself would have taken** (`book_select()`: liquidity rank, the per-session cap, the concurrency cap, one per symbol — re-audit N1; the equal-weighted figure over every firing is minted next to it as context) on three windows (whole sealed history; the window ending `discovery_end`; the trailing window after it), against **the pool of every resolved stock-session on the rule's own kind of day** (audit A10), with a **day-blocked placebo on winsorised means that re-draws to 5,000 near the bar** (N8), a **CR3 cluster-robust t** (N5) and **concentration facts** — the three best days' share of the window's P&L and the expectancy without the best day (N3). Templates S2 cannot derive a rule from (`volume_anomaly`: no side; `relationship`: a multi-session short leg; `theme_cycle`, `market_regime`: no family yet) are declined with the reason on the record. |
| `book.py` | **The virtual book** on the sealed frame: entry at the NEXT OPEN (never a synthetic open — S1 A7), exit at the horizon CLOSE, costs + slippage both ways on every closed trade, the draft's limits (₹10 lakh, 10% per position, 10 concurrent, 5 new per session), liquidity-descending selection, marked to close every session; **never a bar past the seal** — a position whose exit lies past it stays open; a signal on the seal day is seen, not taken (A14); a trade through a glitch / corporate-action bar is closed **unresolved** — listed, never graded, as the evidence convention's NaN (A7). A pure function of (rule, period, seal), pinned by S2-15. No price stop (§5.2). `passive_incumbent()`: the same capital, equal weight in the whole universe over the same window, costs charged. |
| `grading.py` | **Frozen, symmetric grading of a period** (`experiment_grading@1.1.0+code.<hash of grading.py>`): the metric is the period's mean net P&L per closed resolved trade; Right above one standard error, Wrong below, Inconclusive inside or under the frozen minimum of trades, **void** when nothing closed. The band is the LARGER of the plain and CR3 cluster-robust standard errors (§5.3). **Frozen cumulative rules** in every version's spec (A4): the version's whole forward record buried when its CR3 cluster-robust t falls below minus Student's t critical value with G−1 degrees of freedom (N5: 2.78 on five days, 2.09 on twenty) on ≥ 10 resolved trades over ≥ 5 signal days; four consecutive void periods bury (`rule_stopped_firing`). The spec also names the expected population (`expected_population`, N1) and the drawdown convention (N9). `compare()` → stronger / weaker / failed / inconclusive / void. |
| `gate.py` | **Worth-testing gate** = the draft's discovery gauntlet (net > 0, at 2× slippage > 0, edge vs the same-day pool > 0, day-blocked placebo ≤ 0.05) on the whole sealed history **and** the trailing window on its own (NDP), sample floors, implementability, novelty, the S1 card's evidence strength — **every value on the book-selected population** (N1); four ADVISORY gates (family-wise bar α / the family's ALL-TIME trial count — N4; the pre-`discovery_end` window at 2× slippage; CR3 cluster-t against Student's t(G−1) — N5; the trailing window without its best day — N3) recorded, never fatal here — significance gates promotion, not survival (P1's argument, kept). **Graduation gate** on the version's FORWARD record only: OOS n ≥ `min_n_for_promotion`, expectancy > 0 and at 2× slippage, a **fixed-day stock-permutation placebo** over the forward record (N2: the version's own signal days held fixed, random resolved names from each day's pool), CR3 cluster-t against t(G−1), both `insufficient` — never passed — below `min_forward_signal_days` (5), arena roster **Keep**, beats the incumbent on net return with no worse drawdown (one convention — N9), implementable, `constitution_signed` (a real signature — N7). A PROPOSAL exists only when every gate but the signature passes; on the unsigned draft its status is `blocked_unsigned_constitution`. **There is no "promoted" anywhere in the registry.** |
| `learning.py` | **What a graded period changes, computed**: the share of the period's losers under each candidate condition (facts); for a Wrong period the gate re-run for every candidate revision (the rule + one more condition — never an implied or exclusive one), **every candidate a counted trial**, the family-wise bar over the idea's whole trial count (A12); the one that would have excluded the most losers AND clears becomes the next version (L3: backtested on the seal, `improved` computed from its own forward record once graded — A11); none clears → buried; a version beyond `max_versions` → buried. |
| `store.py` | **The registry** (`var/pathfinder_experiments.db`, schema 2, WAL): append-only by trigger, hash-chained on the tables that are the record with **content-only hashes** — timestamp columns and the timestamp keys inside JSON columns are outside the hash (A2) — so two builds chain identically; refused on a superseded schema; `pfx_trials` is the p-hacking ledger; `pfx_trades` the only table naming a constituent. `scoreboard()`: Right · Wrong · Inconclusive · n over graded periods, void apart, forward / backfilled split, by family, trials. See `docs/DATA_MODEL.md`. |
| `narrate.py` | **The seven beats** as digit-free `ExperimentStoryLine`s; the engine template writes all seven; a model may re-write a body under the engine's headline through `enforce_narrate(strict=True)` AND the addendum-6 lint at the source (A8), and is replaced visibly when it cannot. The `history_showed` beat says the trailing window was seen for every variant — a persistence check, not a holdout (A5). |
| `loop.py` | **The daily step, sealed at D**: TRACK every open period (re-walk, append marks and closed trades no earlier than their session) → GRADE what completed under the frozen rule — **refusing if the running grader's version differs from the frozen one** (A6) → COMPARE → LEARN → CONTINUE / REVISE / BURY (period rule, then the cumulative rule) → graduation gate → OPEN from D's new ROOT S1 findings (research → gate → v1 with a frozen expectation and rule, or a reasoned decline naming the variant that came closest and the gates it failed with their values) → NARRATE. `is_backfilled` is S1's. A declined family is not re-researched for 60 sessions. |
| `views.py` | Rows → contract. `card()` — public, **as of the edition it is served on** (S2-25b): state, score, trials as of then; `backfilled` per news edition with `opened_backfilled` alongside (A1); a fact whose value names a constituent of the book is withheld (A9). `record()` — in-app: versions, trials, periods with `grader_version`, expected-vs-actual, learning, change-log with computed `improved`, post-mortem, proposal; constituents withheld unless `KANIDA_PF_RA_REVIEWED=1`. |

**Contract (`backend/pathfinder/schemas.py`, `docs/openapi.yaml` regenerated):** `ExperimentState`
(testing / buried / proposed — nothing else), `ComparisonCategory`, `ExperimentBeat`, `ExperimentGradingRule`,
`Expectation` (frozen; `computed_by` refuses a model; period ≤ seal), `ForwardResult` (return with drawdown,
`n_unresolved`), `ExpectedVsActual`, `LearningView` (never L4), `PeriodView` (`grader_version`), `VersionView`
(L3 needs `validation`), `ExperimentStoryLine`, `ExperimentCard` (**addendum 6 in the schema**: no field for a
constituent; `PUBLIC_CARD_BANNED_RE` on the theme and every beat; seven beats in order; per-edition
`backfilled` + `opened_backfilled`), `BasketView`, `ProposalView` (`decided_by = engine`, human-gated),
`ExperimentRecord` (buried ⇔ post-mortem; `trials_total` = trials on record), `RejectedCandidate` (the closest
variant and its failed gates with values), `ExperimentScoreboard`, `ExperimentsResponse` (losers first),
`FeedResponse.experiment_cards` / `experiments_scoreboard` (cards must be the edition's own),
`DeathCause.revisions_exhausted` / `rule_stopped_firing`. `GET /api/pathfinder/experiments` and `/experiment/{id}`
serve the registry when `KANIDA_PATHFINDER_SOURCE=research` (404 with no registry — never fixtures); the P0/P1
shapes are served by every other source, unchanged (S2-33). `GET /feed?date=` carries the experiment cards
written on that edition and the experiment scoreboard as of it, under the research source only.

## 2. How to run

```bash
# 1. the S1 editions the loop steps over (S2 rebuilt the S1 store from 2026-05-04; §3.2)
python scripts/run_pathfinder_scan.py --archive-store --i-understand-this-archives-the-store --date 2026-07-29 --backfill 60
# 2. the loop, sealed at each edition (append-only; a stepped edition is skipped; the registry is never deleted)
python scripts/run_pathfinder_experiments.py                      # ~8 min for 61 editions
python scripts/run_pathfinder_experiments.py --archive-store --i-understand-this-archives-the-store
#    env: KANIDA_PATHFINDER_EXPERIMENTS_DB, KANIDA_PATHFINDER_CONSTITUTION, KANIDA_PFX_* (§6), --llm none|auto|recorded|live
# 3. serve it
cd backend && KANIDA_PATHFINDER_SOURCE=research uvicorn pathfinder.mock_app:app --port 8010
curl http://127.0.0.1:8010/api/pathfinder/experiments                 # cards, not_opened (with trial counts), scoreboard
curl http://127.0.0.1:8010/api/pathfinder/experiment/exp_dip_bounce_20260504   # 404 on the served registry (§3.2)
curl "http://127.0.0.1:8010/api/pathfinder/feed?date=2026-07-29"
#    KANIDA_PF_RA_REVIEWED=1 lets the RECORD (never the card) list constituents
# 4. tests
python -m pytest backend/tests/test_pathfinder_s2.py -q            # 44 rows, ~70 s (synthetic, engineered conditional edge)
python -m pytest backend/tests/test_pathfinder_s2_audit2.py -q     # 19 re-audit pins, ~30 s (3 rows read the warehouse + the archived registry; skip if absent)
python -m pytest backend/tests/test_pathfinder_s1*.py backend/tests/test_pathfinder_p0.py backend/tests/test_pathfinder_p1.py -q
python scripts/gen_openapi.py --check
```

**Signing the Constitution (N7).** `is_signed` is true only when the file carries a `signature:` block with
`signed_by`, `signed_at` and `document_sha256` equal to `governance.constitution_content_sha256(document)` —
the sha256 of every key but the signature block in canonical JSON — so a governed number changed after
signing un-signs the document. `approved_by` is a label; it signs nothing.

## 3. Results / evidence

### 3.1 Tests

`backend/tests/test_pathfinder_s2.py`: **44 passed** (S2-01…S2-33, S2-25b, and the ten audit pins
`test_a*` — `docs/TEST_PLAN.md`). `backend/tests/test_pathfinder_s2_audit2.py`: **19 passed** (the re-audit
pins N1–N9, §4.1; sixteen synthetic, three on the warehouse + the archived registry; **all 19 fail on
`187c488`**, verified in a detached worktree). The four S1 suites: **134 passed, unchanged**. P0 **37**, P1 **60**.
**294 passed**. `gen_openapi.py --check` in sync (the regeneration was +116 lines, −0: additive only). The S2 rows run on a synthetic universe with
an engineered CONDITIONAL edge so that every branch — open, track, Right / Inconclusive / void, Wrong →
learn → v2 → v3 → buried, `rule_stopped_firing`, the cumulative kill, the graduation proposal (blocked on
the unsigned draft; awaiting a human on a signed copy), the unresolved trade, the forward-edition card —
is exercised on data the test controls and recomputed independently in the row.

### 3.2 The real run — a SIMULATED BACKFILL over 61 S1 editions, 2026-05-04 → 07-29: nothing opened

**Read this first.** Every row was generated on 2026-09-10 for sessions in May–July; each step saw bars
≤ its date only (pinned by S2-02/04/15 and verified by the auditor on the raw warehouse), but the outcomes
were knowable in the world when the rows were written. `backfilled = 1` on every row.

**The S1 store was archived and rebuilt** (`var/pathfinder_research.db.archived-20260910T110627` is
S1's 13-edition store; the new one holds 61 editions from 2026-05-04, 127 independent grades: Right 58 ·
Wrong 30 · Inconclusive 39) because an experiment needs sessions to run in. The S1 code is untouched; the
S1 hand-back's §3.2 describes the archived store and remains true of it.

**What the gate did with 131 S1 root findings (every one a `pfx_candidates` row, served in `not_opened`):**

| Outcome | Findings | Reason on the record |
|---|---|---|
| no family in this build | 27 `market_regime` · 23 `theme_cycle` | "no closed hypothesis family for template … (founder input)" |
| not derivable | 8 `volume_anomaly` · 6 `relationship` | no side to trade / a multi-session short leg |
| family already researched and declined | 48 `surge` · 15 `dip` | "re-testing one hypothesis daily is p-hacking by repetition — next evaluation after 60 sessions" |
| **researched: 18 counted trials each, none cleared** | 4 (`dip` 05-04 and 07-29, `surge` 05-04 and 07-29) | below |

Every number below is on the **book-selected population** (re-audit N1: the trades the virtual book could
have taken under the draft's limits), with the equal-weighted figure the pre-re-audit registry had frozen
shown for context. Trailing = 2025-01-01 → the edition; "loo" = the window's expectancy without its
single best signal day (N3); the family-wise bar divides by the family's all-time trials (N4).

| Researched finding | closest variant (fewest failed gates) | book-selected whole history: net / 2× / edge / placebo p (n, sessions) | equal-weighted whole (context) | trailing: net (n) · loo · top-3 share | the fatal gates it failed |
|---|---|---|---|---|---|
| `fnd_20260504_dip_zentec` (a real `no_trade` dip card) | dips on a session the whole market fell > 1%, five-session window | **−0.285% / −0.485% / −0.12% / 0.63** (n 1,682 of 8,337 fired, 395 sessions) | +0.246% (n 8,337) | +0.43% (186) · **+0.13%** · 160% | **`history_expectancy_net`, `history_expectancy_2x_slippage`, `history_edge_vs_baseline`, `history_placebo`** — 0 of the 18 dip variants has a positive book-tradeable expectancy |
| `fnd_20260729_dip_j_kbank` (the 60-session retry; family-wise bar 0.05/36) | the same | −0.262% / −0.462% / −0.12% / 0.58 (n 1,710 of 8,383, 402 sessions) | +0.242% (n 8,383) | +0.55% (214) · +0.31% · 113% | the same four; 0 of 18 positive |
| `fnd_20260504_surge_cempro` | fade the day's biggest jump on a > 1% market fall, five-session window | +0.415% / +0.215% / +1.25% / 0.001 (n 681 of 753, 313 sessions) | +0.331% (n 753) | +0.34% (77) · +0.08% · 160% | **`implementable_under_cost_convention`** (a five-session short in the cash segment); the family-wise bar 0.0028 is not cleared either (advisory) |
| `fnd_20260729_surge_pcbl` | the same | +0.417% / +0.217% / +1.27% / 0.001 (n 693 of 765, 319 sessions) | +0.335% (n 765) | +0.36% (89) · +0.11% · 151% | the same |

**Served scoreboard as of 07-29:** Right 0 · Wrong 0 · Inconclusive 0 · n = 0 · void 0 · pending 0 ·
forward 0 · backfilled 0 · testing 0 · buried 0 · proposed 0 · **candidates not opened 131 · trials 72**.
This is the honest reading of the S1 library against the Constitution draft's bar on this data: **the dip
family has no book-tradeable edge** — the strategy the book would actually run loses after costs on the
whole history and on the discovery years, and even the equal-weighted count is flat (−0.008%) without one
election day (2024-06-04) — and the
surge fade's positive book number (+0.42%, p 0.001) belongs to a five-session short that no retail customer
can hold in the cash segment, which is why it is declined on implementability (a one-session short family is
item 5 of §8). That is a finding, and it is what the gate exists to say.

### 3.3 What the loop did BEFORE the audit — the superseded registry (kept, not deleted)

`var/pathfinder_experiments.db.archived-20260910T115630` (engine `+code.74493d7c3b0e`) was built with the
pre-audit, unconditioned placebo pool (p = 0.017). It is the registry the auditor examined, and **every
number below was reproduced by the auditor from the raw warehouse** (§4). It is superseded — the null was
wrong by P1's own standard — and it is kept because the spec's whole loop ran on real data inside it:

**`exp_dip_bounce_20260504`** ← `fnd_20260504_dip_zentec`; 1 of 18 variants cleared; expectation frozen at
seal 05-04: **+0.2456% per trade** (hit 53.6%, median +0.67%, n 8,337 on 441 sessions; 2× slippage +0.046%;
trailing +1.93% on 768; the pre-2025 window at 2× slippage **−0.13%** and cluster-t **0.38** printed on the card
as failed advisories). ₹10 lakh, 10% per position, periods of 10 signal sessions, graded at one standard error:

| Period | graded | closed | mean net / trade | expected | book · worst DD | verdict → category |
|---|---|---|---|---|---|---|
| 1 · 05-05 → 05-18 | 05-25 | 14 | **−2.23%** | +0.25% | −3.13% · 4.44% | Inconclusive (KAYNES −24.3%, NLCINDIA +15.8%; inside one SE) |
| 2 · 05-19 → 06-02 | 06-09 | 0 | — | — | — | void — the rule never fired |
| 3 · 06-03 → 06-16 | 06-23 | 9 | **+6.16%** | +0.25% | +5.54% · 3.56% | **Right → stronger** |
| 4 · 06-17 → 07-01 | 07-08 | 0 | — | — | — | void |
| 5 · 07-02 → 07-15 | 07-22 | 2 | +2.86% | +0.25% | +0.57% · 1.38% | Inconclusive (below the frozen minimum of 5) |
| 6 · 07-16 → 07-29 | — | 2 (07-22 → 07-29) | — | — | — | open at the data edge |

27 closed trades (`ABB 05-11 → 05-12 open 6360.0 → 05-18 close 6413.5 = +0.84% gross, +0.34% net`), no Wrong
period, so no v2 on real data and none should be; the graduation gate ran after every graded period and
failed on `oos_sample_size` (14 / 14 / 23 / 23 / 25 vs 50) among others — no proposal. Under the audit's
cumulative rule (§1, A4) that record would also have been judged on 25 trades over 6 signal days and left
open (t not below −1.96).

**The same record re-run under the re-audited engine (§4.1), from the raw warehouse — worse, which is
correct.** The expectation the loop would freeze at seal 05-04 for that variant, over the population the
book can take (`book_select`, the draft's limits):

| population (seal 2026-05-04) | n (fired / skipped) | signal days | expectancy net | 2× slippage | hit | edge vs pool | placebo p | CR3 t | top-3 days' share of P&L | without the best day |
|---|---|---|---|---|---|---|---|---|---|---|
| **book-selected, whole history** (the frozen number) | **1,682** (8,337 / 6,655) | 395 | **−0.285%** | −0.485% | 48.3% | −0.12% | 0.63 | −0.96 | undefined (total ≤ 0) | −0.34% |
| book-selected, discovery (→ 2024-12-31) | 1,496 (7,569 / 6,073) | 349 | −0.373% | −0.573% | 47.6% | −0.10% | — | −1.15 | undefined | −0.43% |
| book-selected, trailing (2025-01 → 05-04) | 186 (768 / 582) | 46 | +0.429% | +0.229% | 54.3% | −0.01% | — | 0.56 | **160%** (best 2025-04-04) | **+0.13%** |
| equal-weighted, whole history (the number the card had frozen — context only now) | 8,337 | 441 | +0.246% | +0.046% | 53.6% | +0.41% | — | 0.38 | 187% (best 2024-06-04) | **−0.008%** |

The worth-testing gate now fails that variant on **four fatal gates at once** — expectancy (−0.285 vs 0),
twice the slippage (−0.485), edge (−0.12), placebo (0.63 vs 0.05) — plus every advisory. The forward record
under the fixed-day stock-permutation null (N2): the 23 trades of 05-05 → 06-23 on five signal days,
mean net **+1.05%** against a null mean of **+0.67%**, score **p = 0.34** (1,000 draws, SE 0.015; 0.34 again at
5,000 — the archived day-blocked value was 0.162); the 25 trades to 07-22 on six days, +1.20%, **p = 0.32**;
CR3 t 0.43 against t(4) = 2.78 and 0.54 against t(5) = 2.57; the 05-25 check (14 trades on three days) is
below the five-day floor and is `insufficient`, not passed. Random names on the same five days made two
thirds of that book's return. **Where v2, retirement and the proposal are demonstrated:** the synthetic universe,
pinned by S2-17/18 (Wrong → 5 counted revisions → v2 (L3) → v3 → buried, post-mortem public, losers-first
listing), `test_a4b` (`rule_stopped_firing` after four void periods), S2-20/21 (a proposal only when every
measurable gate passes; `blocked_unsigned_constitution` on the draft, `proposed_awaiting_human` on a signed
copy; the incumbent's return is the passive equal-weight book's; edge reversed → no proposal).

### 3.4 Determinism and end-to-end verification

**Determinism.** The served registry was rebuilt twice from scratch on the re-audited code into two files
(`var/pathfinder_experiments.db` — the previous live registry archived as `…archived-20260910T130933` —
and `var/pathfinder_experiments.build3b.db`), 61 steps each, in two concurrent processes (logs
`var/s2_build3a_log.txt`, `s2_build3b_log.txt`; engine `+code.4a261e9a236e`, schema 3). Compared table by
table with timestamps (columns and the keys inside JSON) removed: **13/13 tables identical (61 editions,
131 candidates, 72 trials), 6/6 chains identical.** The same held for the pre-re-audit builds
(`build2f`, engine `+code.cfae44c0f6e9`).
S2-25 pins the same property on the synthetic loop with builds an hour apart (versions, outcomes,
post-mortems: identical rows AND identical row hashes while every `created_at` / `frozen_at` differs).
Every earlier registry is `var/pathfinder_experiments.db.archived-<stamp>` — archived, never deleted.

**Over HTTP** (`KANIDA_PATHFINDER_SOURCE=research uvicorn pathfinder.mock_app:app --port 8011`, real calls
against the served registry, `scratchpad/s2_http_e2e.py`):

```
GET /api/pathfinder/experiments -> 200 | count 0 | not_opened 131 | engine pathfinder_experiments@1.0.0+code.4a261e9a236e | llm none
  SCOREBOARD Right 0 · Wrong 0 · Inconclusive 0 · n=0 · void 0 · pending 0 · forward 0 · backfilled 0 · testing 0 · buried 0
             · proposed 0 · not opened 131 · trials 72 · record_label: simulated backfill — generated after the fact; not a forward track record
  researched-and-declined (best_expectancy_net_pct is the BOOK-selected number; family_trials_all_time never restarts):
    fnd_20260504_surge_cempro  trials=18 family_trials_all_time=18 etv=+0.415 failed=['implementable_under_cost_convention']
    fnd_20260504_dip_zentec    trials=18 family_trials_all_time=18 etv=-0.285 failed=['history_expectancy_net=-0.285 vs 0', 'history_expectancy_2x_slippage=-0.485 vs 0', 'history_edge_vs_baseline=-0.118 vs 0', 'history_placebo=0.634 vs 0.05']
    fnd_20260729_surge_pcbl    trials=18 family_trials_all_time=36 etv=+0.417 failed=['implementable_under_cost_convention']
    fnd_20260729_dip_j_kbank   trials=18 family_trials_all_time=36 etv=-0.262 failed=['history_expectancy_net=-0.262 vs 0', 'history_expectancy_2x_slippage=-0.462 vs 0', 'history_edge_vs_baseline=-0.118 vs 0', 'history_placebo=0.58 vs 0.05']
    (every best_rule_text is a measurement definition — no "position", no instruction)
GET /feed?date=2026-07-29 -> 200 | backfilled=True | experiment_cards [] | experiments_scoreboard: 131 not opened / 72 trials
GET /experiment/DROP -> 400 · GET /experiment/exp_nope -> 404
```
(The pre-re-audit served lines — `etv=0.246 failed=['history_placebo=0.076 vs 0.05']` at engine `+code.cfae44c0f6e9` — are
what the archived registry `…T130933` still serves; they were the equal-weighted numbers.)

The synthetic-registry surfaces (a card with seven digit-free beats, a record with versions / trials /
periods / expected-vs-actual / learning / proposal, constituents withheld, a buried record with its
post-mortem listed first, a forward card on a backfilled experiment) are pinned over HTTP by S2-32 and
`test_a1`; against the pre-audit real registry the same calls served `exp_dip_bounce_20260504` with the
§3.3 numbers, 18 trials, the three failed advisories and `basket.constituents = []`.

## 4. The quant audit

`dev-quant-auditor` audited the loop adversarially against the pre-audit registry (§3.3). It confirmed:
point-in-time on marks and trades (0 rows written before their session; re-walks identical on a later
seal; a seal on a signal day opens nothing), the frozen expectation reproduced number for number from
the raw warehouse (n 8,337 / 441 days / +0.2456% / trailing +1.9272% / discovery 2× −0.1255% / cluster-t
0.382), periods 1 / 3 / 5 reproduced (−2.2346% / +6.1598% / +2.8625%), trials 54 = 3 × 18 numbered 1…18,
the family-wise bar 0.05/18 recorded and failed, no "promoted" state, the forward placebo's hurdle
handled correctly, every narrative digit-free and engine-authored, the calendar arithmetic, and 167 tests
passing. It **did not approve the hand-back as first drafted**. Every finding, what changed, and the pin:

| # | Finding (auditor) | What changed | Pinned by |
|---|---|---|---|
| **1 HIGH** | The feed would 500 on the first FORWARD edition of a backfilled experiment: the card's `backfilled` was the opening edition's forever, and `FeedResponse` requires the edition's flag. Also mislabelled a forward card as a backfill. | `ExperimentCard.backfilled` is the NEWS edition's (`pfx_editions.backfilled`), `opened_backfilled` alongside; the feed validator checks `news_edition == edition_date`. | `test_a1` (opens backfilled, steps same-day, the forward feed validates; the opening card still says backfilled) |
| **2 HIGH** | "Identical row hashes" was false on disk: `frozen_at`, facts' `computed_at` and story `at` sat inside hashed JSON; S2-25 passed only because both synthetic builds shared one `computed_at`. | Content-only hashes: `content_payload()` strips timestamp columns AND timestamp keys inside JSON before hashing; `verify_chain` likewise. | S2-25 rewritten: builds an hour apart, identical hashes; §3.4 on the real registry |
| **3 HIGH** | Hand-back numbers not on the registry (27 vs 25 trades at 07-22; "29" nowhere; surge trailing +0.29 vs 0.23/0.26; "104" vs 128); "committed locally" false at the time; §3.5/§4 placeholders. | §3 regenerated from the final registries; committed (§7). | — |
| **4 MEDIUM** | The larger-of band could shelter a losing version forever (period 1: plain SE 2.65 → Inconclusive while the cluster-t was −2.6) and nothing killed on the cumulative record or on a rule that stopped firing. | Frozen CUMULATIVE rules in every version's spec: cluster-robust t < −1.96 on ≥ 10 resolved trades over ≥ 5 signal days buries (`edge_did_not_persist_oos`); 4 consecutive void periods bury (`rule_stopped_firing`, new `DeathCause`). Read from the frozen spec only. | `test_a4`, `test_a4b` |
| **5 MEDIUM** | The trailing window was a selection criterion (the chosen variant was the ONLY one of 18 whose trailing window was positive) and the card presented it as independent confirmation. | A `trailing_looks` fact and a sentence on every card: "a persistence check, not an independent holdout: that window was seen for all N variants". | S2-12 (fact), narrative |
| **6 MEDIUM** | Outcomes copied the frozen `rule_version`; a changed `grading.py` could judge an old version silently. | `grader_version` stamped at grade time; a mismatch **refuses to grade** (archive and rebuild). | `test_a6` |
| **7 MEDIUM** | The book could close a trade through a glitch / corporate-action bar at a phantom price while the evidence convention treats it as NaN (0 of 27 stored trades affected; prospective). | `VTrade.resolved` / `unresolved_reason`; `judge()`, the forward record and the scoreboard use resolved trades only; `n_unresolved` on every `ForwardResult`. | `test_a7` |
| **8 MEDIUM** | A model body with a banned word passed the narrate contract and bricked the card at read time. | `PUBLIC_CARD_BANNED_RE` applied in `ExperimentNarrator.lines`; engine fallback, visibly. | `test_a8` |
| **9 LOW** | The S1 subject fact could carry a stock name onto the public card ("enforced by luck"). | A fact whose value names a constituent of the book is withheld on the card (same id, value replaced). | `test_a9` |
| **10 LOW** | The placebo / baseline pool was not context-conditioned (incomplete P1 port); the edge mixed raw with winsorised means. | The pool is every resolved stock-session on the rule's own kind of day; edge on one convention. **This is what closed the real experiment (p 0.017 → 0.076).** | `test_a10`, S2-03 |
| **11 LOW** | `improved` never computed. | A successor's forward mean vs its predecessor's once it has a graded period; null until then. | S2-28 |
| **12 LOW** | The family-wise n in learning was the running trial index. | The idea's whole trial count after the round. | `test_a12` |
| **13 LOW** | Literal numerals in `why` / `what_we_kept`. | Digit-free; the counts are facts. | S2-12/17 |
| **14 LOW** | `signals_seen` missed a signal on the seal day. | Seen, not taken. | `test_a14` |
| **15 LOW** | "No unconditioned hypothesis clears costs on any date since 2025" overstated (the unconditioned dip's trailing window alone is +0.61%). | Reworded: on the whole sealed history at each edition. | §preface |
| 16 | NOT FOUND (checked): `open_periods` SQL, `pending`, ordering, RNG seeding, Union response models, retry bypass, `compare()`, short-family signs. | — | — |

Found while fixing: (a) the feed handler opened the real registry for S1's own tests (a "database is
locked" during a concurrent build) — the feed attaches cards only under the research source, and the
registry opens in WAL with a 30 s timeout; (b) the declined-candidate row named the highest-expectancy
variant rather than the one that came closest — it now names the closest and its failed gates with values.

### 4.1 The independent re-audit (pass #4)

An independent auditor recomputed everything in §3.3 from the raw warehouse (its numbers were exact) and
found nine items. Every one is fixed; every fix is pinned in `backend/tests/test_pathfinder_s2_audit2.py`,
and every pin was run against a detached worktree at `187c488` and **fails there** (18 failed + the
archived-registry row, which fails there too when pointed at the archive). The contract stayed
backward-compatible: additive fields only; the registry schema moved to 3 (a version-2 file is refused on
open and archived, never migrated — the pre-re-audit live registry is `…archived-20260910T130933`).

| # | Finding (auditor) | Fix | Pinned by |
|---|---|---|---|
| **N1 HIGH** | The frozen expectation was not the expectation of the strategy the book trades: `replay()` weighted every signal equally while `run_book` takes at most 5 per session by liquidity, 10 concurrent, one per symbol. On the same rule the book-takeable population is −0.27% (auditor) / **−0.285%** (this engine's exact walk, n 1,682) against the card's +0.25%; trailing +0.52% / +0.43% vs +1.93%; 19 of 46 forward signals were skipped by the cap. | `hypotheses.book_select()` replays the book's own selection over history (liquidity rank, per-session cap, concurrency cap, one per symbol — the same walk as `run_book`, cash check aside); `replay(limits=…)` measures that population; `measure()` mints the three windows on it and the equal-weighted whole window as `equal_weighted` context. The frozen expectation, every worth-testing and revision gate value, and the grading rule's `expected_net_pct` are the book-selected numbers; `Expectation.population / signals_fired / signals_skipped / equal_weighted_*` and the facts `population`, `signals_fired`, `signals_skipped`, `equal_weighted_expectancy` say so on the card; beat 3 states both. | `test_n1_book_select_*`, `test_n1_replay_*`, `test_n1_the_loop_freezes_*` (frozen = independent book-replay, ≠ equal-weighted, both on the card), `test_n1_real_*` (−0.285% / n 1,682 vs +0.2456% / 8,337) |
| **N2 HIGH** | The graduation placebo was the unconditioned day-blocked null on a forward window; conditioning it is degenerate there (every > 1%-down day IS a signal day, p 0.53). The fixed-day stock-permutation null gives p 0.35 on the 23 forward trades. | `loop.fixed_day_permutation_means()`: the version's signal days held fixed, as many random resolved names drawn (without replacement) from each day's pool as the book took that day, plain-mean convention (the one the record is graded on); `min_forward_signal_days` (5, founder input) below which `oos_placebo` and `oos_cluster_significance` are **`insufficient`** — a new `Gate.insufficient` / `GateView.insufficient` flag, never passed, distinct from a measured failure; `ForwardRecord.placebo_kind` names it. `oos_cluster_significance` kept (CR3, N5). | `test_n2_the_forward_null_*`, `test_n2_the_graduation_gate_is_insufficient_*`, `test_n2_the_loop_records_*`, `test_n2_real_*` (**p ≈ 0.34** on the archived 23 trades / 5 days at 5,000 draws) |
| **N3 MEDIUM** | Three days (2025-04-04, 04-07, 01-13) contribute 106% of the trailing window's net P&L; without 04-04 alone trailing etv is 1.15 not 1.93; discovery is +0.075% and fails 2× slippage; the trailing check was the sole discriminator among the 18 variants. | `hypotheses.concentration()`: on every window of every trial and card — `top3_days_share_pct` (defined when the total is positive), `best_day`, `expectancy_without_best_day_net`; facts `top3_days_share`, `expectancy_without_best_day`, `trailing_top3_days_share`, `trailing_expectancy_without_best_day`; the advisory gate **`trailing_expectancy_without_best_day` > 0**; beat 3 reads the persistence check with its concentration. | `test_n3_concentration_*`, `test_n3_the_card_*`, `test_n3_real_*` (best day 2025-04-04, share 105.7%, without it 1.15) |
| **N4 MEDIUM** | The family-wise trial count restarted on retry (`n_trials=len(variants)`; `trials_for_experiment` counted one finding + one experiment): the 07-29 retry's bar was 0.05/18 with 36 family trials on record. | `store.family_trials(family_id, as_of)` counts `pfx_trials` all time by `variant_json.family_id`; the opening gate's bar is α / (family all-time + this round), learning's likewise; `pfx_candidates.family_trials_all_time`, `RejectedCandidate.family_trials_all_time`, `ExperimentCard.family_trials_all_time` (as of the edition), a fact and a sentence in beat 2. | `test_n4_*` (18 → 36; bars 0.05/18 → 0.05/36) |
| **N5 LOW** | CR0 cluster statistics on as few as five days (the cumulative kill and `oos_cluster_significance`). | `cluster_robust_se()` is **CR3** (each cluster's residual sum scaled by 1/(1 − n_g/n)); the critical value is **Student's t with G−1 degrees of freedom** (`t_critical`, an incomplete-beta implementation — no SciPy dependency; matches `scipy.stats.t.ppf` to 5 decimals), frozen in the rule spec (`cluster_t_kind`, `cumulative_bury_alpha`); grading rule **1.1.0**; the band uses CR3 too; the graduation and worth-testing cluster gates use t(G−1). | `test_n5_*` |
| **N6 LOW** | "go long the dip", "a virtual long position from the next session's open", "hold for five sessions then exit at the close", "position size at most a tenth", "short it at the open" passed `PUBLIC_CARD_BANNED_RE`; beat 3 stated the holding horizon, beat 4 the position fraction. | Regex widened (a side, a holding / exit / entry instruction, sizing, "at the open"); beats 2/3/4 rewritten as a study — theme, condition, study window, the book's own population, the equal-weighted context, the concentration — with no fraction and no holding instruction; `Variant.rule_text()` is a measurement definition ("the return of a long leg measured from the following session's open …") on the RA-reviewed record. | `test_n6_the_public_card_rejects_*` (all five probes), `test_n6_the_engines_public_beats_*` |
| **N7 LOW** | `is_signed` was a string-prefix check on `approved_by`. | `Constitution.signed_by / signed_at / document_sha256` from a `signature:` block; `is_signed` only when all three are present and the hash equals `constitution_content_sha256(document)`; `signature_status` on every gate statement; the S2 test's signed copy now carries a real signature. | `test_n7_*` |
| **N8 LOW** | 1,000 draws give ±0.009 (2 SE) near the bar (the pre-audit 0.017 was seed-lucky: 0.025 at 10k); the p compared the raw signal mean while the expectation is winsorised. | `placebo_p_value()`: re-draws to `placebo_draws_near_bar` (5,000) when \|p − bar\| < 2 SE (at p̂ or at the bar); `block_placebo_means(winsor_pct=…)` winsorises every draw's mean and the statistic is the winsorised gross mean; `placebo_se` and `placebo_convention` on the stats, the expectation and the gate statement. | `test_n8_*` |
| **N9 LOW** | `_version_forward` drawdown was relative-to-peak; `BookRun.drawdowns()` additive points of capital; the incumbent gate compared the two. | One convention — `book.DRAWDOWN_CONVENTION`, percent decline from the running peak — via `drawdowns_relative_to_peak()` on both sides; `ForwardResult.drawdown_convention`; the gate statement names it; the period fact label says it. | `test_n9_*` |

Found while fixing: three `pathfinder.mock_app` uvicorn servers left running by the previous pass (ports
8010–8012, the HTTP e2e check) held the live registry open and blocked the archive rename; they were stopped
(they are test scaffolding — the port-8001 backend was not touched).

## 5. Risks

### 5.1 There is no forward record, and no experiment on the served registry
Every row is a backfill (§3.2). The forward record starts with the first step run on a session's own
date; `is_backfilled` labels it and `test_a1` pins that a forward card on a backfilled experiment
validates. The first forward step will open nothing from the S1 dip / surge families: their book-tradeable
expectancy is negative on this data (§3.3, §8.1) — no bar setting changes that.

### 5.2 No price stop — a conflict with the Constitution draft's `hard_stop_required`
The S1 expectation was measured on a time-boxed exit with no price stop; the forward test replays the same
strategy (the evidence must be the strategy that is traded). The draft's `risk.hard_stop_required: true`
was written for P1's rulebooks. **Founder decision:** keep S2's convention, or add a stop to BOTH the
expectation and the book as an L2 parameter inside the draft's `_exits.stop_pct` range.

### 5.3 The verdict band and the cumulative rule
A period is judged against one standard error of its own mean — the larger of the plain and cluster-robust
estimates (S2-06 found the cluster estimate collapsing to 0.04 on three clusters). The cost, shown on the
real period 1 (−2.23% with KAYNES −24%, Inconclusive), is that a wild period is not called; the auditor's
finding 4 is answered by the frozen cumulative rule, not by narrowing the band. A founder may prefer a fixed
band — one line in `grading.py`, and the rule version changes.

### 5.4 Selection on the whole history; the trailing window is not a holdout
The expectation is the S1 evidence convention over the book's own population (N1); the trailing-window
check is a persistence check inside the selection history and every card says so (A5), now read with its
concentration — the three best days' share and the value without the best day (N3) — because on the real
warehouse three days were the whole "persistence". The pre-`discovery_end` numbers are on every trial and
an advisory gate. The clean OOS is the forward virtual record, which the graduation gate reads under a
fixed-day stock-permutation null (N2) and only once it holds `min_forward_signal_days` signal days.

### 5.5 Multiple comparisons
18 variants at opening, 5 + 3 + … per revision; the count is on every card and every decline; the
family-wise bar (α / the family's ALL-TIME trials, across every finding and retry — N4) is advisory at
opening, required at graduation; a declined family waits 60 sessions. The gate does not enforce the
family-wise bar at discovery (P1's argument). The best-of-18 variant of §3.3 carries no multiplicity
adjustment in its headline numbers; the family-wise bar it would have had to clear (0.05/18 = 0.0028, or
0.05/36 on the retry) is on its record.

### 5.9 The book-replay expectation models four of the book's five constraints
`book_select` walks liquidity rank, the per-session cap, the concurrency cap and one-per-symbol exactly as
`run_book` does; it does not model the cash check (a fresh book of the same capital opens every period, so
the check binds only inside a losing period), and each window is its own fresh book. A forward period's
selection can therefore differ from the historical replay at the margin of a losing period. The
selection rule and the counts (`signals_fired`, `signals_skipped`) are on every card.

### 5.6 The version's compounded book return
For the incumbent comparison the periods are compounded as sequential books of the same capital although
a period's horizon tail overlaps the next period's first sessions; the gate statement says so; the primary
OOS gate (per-trade expectancy) is unaffected.

### 5.7 Families the loop cannot open
`volume_anomaly` (no side), `relationship` (a multi-session short leg), `theme_cycle` / `market_regime`
(no family in this build) — each declined with the reason. Founder input: a sector-basket family.

### 5.8 Survivorship and data quality
Inherited from S1 (`PF-S1.md` §5.3/§5.5). The book inherits every exclusion (no entry at a synthetic open;
a trade through a hole is unresolved).

## 6. Founder inputs — stubbed, flagged

| Input | Stub | Where |
|---|---|---|
| **Worth-testing gate thresholds** | the draft's gauntlet (net > 0, 2× slippage > 0, edge > 0, placebo ≤ 0.05) + trailing > 0 (NDP) + sample floors 100 / 30 + S1 evidence strength ≥ 0.5 — all on the book-selected population. **No bar decides today: the closest variant fails expectancy, 2× slippage, edge and placebo at once (§3.3).** The placebo bar stays 0.05. | `gate.worth_testing_gates`, `config.min_*`, `KANIDA_PFX_*`, the Constitution |
| **Forward signal-day floor** | 5 independent signal days before the forward placebo and cluster t are read (`insufficient` below) | `config.min_forward_signal_days`, `KANIDA_PFX_MIN_FORWARD_SIGNAL_DAYS` |
| **Placebo resolution** | 1,000 draws; 5,000 when the p sits within two standard errors of the bar | `config.placebo_draws`, `placebo_draws_near_bar` |
| **Constitution signature** | none — the draft carries no `signature:` block and `is_signed` is false; signing = `signed_by`, `signed_at`, `document_sha256` of the content | `governance.constitution_content_sha256`, §2 |
| **Discovery end (NDP TRAIN_MAX)** | 2024-12-31 | `config.discovery_end` |
| **Virtual capital per experiment** | ₹10,00,000 · 10% per position · 10 concurrent · 5 new per session | Constitution draft `risk` |
| **Forward period** | 10 signal sessions; minimum 5 closed trades to grade | `config.period_sessions`, `min_trades_to_grade` |
| **Retirement rules** | 3 versions then buried; cumulative t < −1.96 on ≥ 10 trades / ≥ 5 days; 4 consecutive void periods | `config.max_versions`, `grading.CUMULATIVE_*`, `MAX_CONSECUTIVE_VOID` |
| **Approved parameter ranges** | the draft's; only `_exits.horizon_sessions` covers S1's rules — S2 tunes nothing numeric (revisions add a condition, L3) | `config/pathfinder_constitution.yaml` (UNSIGNED — nothing promotes) |
| **The first 5 hypotheses** | none authored; the engine researched four real S1 findings and declined each on the record | §3.2 |
| **Retry after decline** | 60 sessions | `config.retry_after_sessions` |
| **Daily LLM budget / key** | draft `llm.daily_budget_usd = 5.00`; no `ANTHROPIC_API_KEY` → engine narration, stated | `narrate.ExperimentNarrator`, `--llm` |
| **RA review of constituents** | withheld unless `KANIDA_PF_RA_REVIEWED=1` on the serving process (a stand-in for the RA workflow) | `views.ra_reviewed` |
| **Hurdle / slippage** | S1's (0.30% + 0.10% each way) | `research/config.py` |
| **Graduation target agent** | horizon ≤ 10 sessions → Trader, else Investor | `loop.track_period` |

## 7. Docs written / changed

`docs/openapi.yaml` regenerated (the S2 contract; the re-audit added fields only — `Expectation.population`,
`signals_fired`, `signals_skipped`, `equal_weighted_expectancy_net_pct`, `equal_weighted_n`, the four
concentration fields, `placebo_convention`, `placebo_se`, `cluster_t_kind`; `GateView.insufficient`;
`ForwardResult.drawdown_convention`; `ExperimentCard.family_trials_all_time`; `RejectedCandidate.family_trials_all_time`);
`backend/pathfinder/fixtures/*.json` unchanged by content; `docs/TEST_PLAN.md` (S2 section: 34 rows + the audit
table + the re-audit table); `docs/DATA_MODEL.md` describes the registry at schema 2 and was **not** touched in the
re-audit pass (outside its file scope) — the schema-3 delta for its owner is one column,
`pfx_candidates.family_trials_all_time INTEGER NOT NULL DEFAULT 0` (N4), and `pfx_meta.schema_version = 3`; this
hand-back. New: `backend/pathfinder/experiments/*` (10 modules),
`backend/tests/test_pathfinder_s2.py`, `backend/tests/test_pathfinder_s2_audit2.py`, `scripts/run_pathfinder_experiments.py`.
Changed: `backend/pathfinder/schemas.py`, `backend/pathfinder/router.py`, `backend/pathfinder/engine/governance.py`
(N7 signature; `Gate.insufficient`).
Untracked by design (`var/` is git-ignored): `var/pathfinder_research.db` (rebuilt, 61 editions),
`var/pathfinder_experiments.db` (the served registry), the `.build2*.db` comparison files, and every
`.archived-<stamp>` registry, including the pre-audit one in §3.3.

## 8. Next step

1. **Founder — the result to rely on is the NEGATIVE one; do not lower the bar.** The variant that came
   closest on the real warehouse — dips on a session the whole market fell more than a percent, over five
   sessions — is the best of 18 with **no multiplicity adjustment** in its headline numbers; its expectancy
   over the twelve discovery years is **+0.075%** per trade (t 0.13) and **fails twice the slippage** there;
   its trailing "persistence" is **three days** (160% of the window's P&L on the book's population; +0.13%
   without 2025-04-04); and over the population the virtual book can actually take — the strategy that
   would be traded — its expectancy is **NEGATIVE: −0.285% per trade on 1,682 trades over 395 days**, failing
   expectancy, twice the slippage, edge and placebo at once. Its forward record, under the fixed-day null,
   is p = 0.34. There is no bar setting at which this becomes an edge, and the placebo bar stays at 0.05.
   **The founder may rely on the negative result: the S1 dip and surge families carry no tradeable edge on
   this data.** What remains for the founder: sign the Constitution when it is right (`signature:` block —
   §2; nothing promotes until then); decide §5.2 (stop) and §5.3 (band); extend
   `approved_parameter_ranges` to the S1 template parameters if the agent is to tune them (L2); and
   author families beyond dip / surge (item 5), since the loop's machinery is now shown to say "no"
   correctly on real data and has not yet had a hypothesis worth a "yes".
2. **Ops:** schedule `run_pathfinder_scan.py` then `run_pathfinder_experiments.py` after each close;
   the first same-day step is the first forward row.
3. **S3:** the swipeable feed against `/feed` (experiment cards ride on it under the research source)
   and `/experiment/{id}`.
4. **Counsel (addendum 6):** the RA-review flag is a stand-in; the constituent list is withheld from
   every public surface until the workflow exists.
5. **S2.1:** a sector-basket family (`theme_cycle` / rotation) graded on sector-minus-market, and a
   one-session short family for the surge fade (implementable intraday) — both founder-reviewed templates.
