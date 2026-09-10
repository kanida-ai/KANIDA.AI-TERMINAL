# Hand-back — Pathfinder Session S2: THE EXPERIMENT LOOP (post-audit)

**Date:** 2026-09-10 (IST) · **Branch:** `feat/product-build` · **Brief:** `docs/sessions/PATHFINDER_S2_EXPERIMENTS.md`
under the LOCKED spec `docs/sessions/PATHFINDER.md`, on top of S1 (`docs/handbacks/PF-S1.md`).
**Status:** built to the brief's "done when" and audited by `dev-quant-auditor` (§4: sixteen findings,
every CONFIRMED one fixed and pinned); every S1 suite passes unchanged (134) plus P0 (37), P1 (60) and the
new S2 suite (44) — **275 passed**; the real-warehouse registry was built twice from scratch on the
committed code and is identical, chain hashes included (§3.4); committed locally, not pushed.

> **Governing sentence, honoured in code:** the engine computes every number; the model never
> produces one. In S2 the model's only permitted act is to re-write the BODY of one of the seven story
> beats under the engine's headline through the S1 strict contract plus the addendum-6 lint; no key is
> configured, so every narrative in the store says `produced_by = engine`. Every number on every surface
> is a `Fact` minted by `FactSet.add()` with provenance pinned to `pathfinder_experiments@1.0.0+code.<hash>`.

> **The one thing to read first — and it changed during the audit.** On the real warehouse, with the six
> S1 templates and the S1 hurdle (0.50% round trip), **no unconditioned S1 hypothesis clears costs on the
> whole sealed history at any edition since 2025** (surge fade −0.27% net, dip −0.20% / −0.06%, every
> pair negative at two legs, zero `virtual_long` market days). The loop therefore does what the spec's
> research scientist does: it takes the S1 card's own follow-up question, evaluates a CLOSED set of 18
> conditioned variants with every trial counted, and applies the Constitution draft's own gauntlet plus
> the NDP promote-only-if-it-holds check. **Before the audit** one variant cleared — dips on a session the
> whole market fell more than a percent, held five sessions — and a real experiment ran forward for
> twelve weeks (§3.3: its numbers are real and the auditor reproduced them from the raw warehouse).
> **The audit's finding 10** (the placebo's pool was not conditioned on the rule's own kind of day — an
> incomplete port of P1's `eligible_mask`) was fixed, and under the correct null the same variant scores
> **p = 0.076 against the Constitution's 0.05**: one gate short, every other gate passed. So the served
> registry holds **zero experiments, 131 declined findings and 72 counted trials**, and says exactly
> why. I did not soften the null to keep the experiment. **Everything in every registry is a simulated
> backfill (`backfilled = 1`, forward n = 0).**

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
| `hypotheses.py` | **The closed hypothesis library**: `finding -> family -> variants`. Three families (`dip_bounce` from a `dip` card; `surge_fade` / `surge_chase` from a `surge` card); nine CONDITIONS (any session · market fell · market rose · market fell > 1% · RISK_ON · NEUTRAL · RISK_OFF · breadth ≥ 50% · breadth < 50%; all causal at the signal close) × two HORIZONS (1, 5 — S1's, inside the draft's `_exits` range) = **18 variants in a fixed order, every one a trial**. `IMPLIES` / `EXCLUSIVE` make a no-op or empty revision unrepresentable. `replay()` measures what the S1 card measured and the book trades — `f{h}` (next open → horizon close) minus the hurdle — on three windows (whole sealed history; the window ending `discovery_end`; the trailing window after it), against **the pool of every resolved stock-session on the rule's own kind of day** (audit A10), with a **day-blocked placebo** and a **cluster-robust t**. Templates S2 cannot derive a rule from (`volume_anomaly`: no side; `relationship`: a multi-session short leg; `theme_cycle`, `market_regime`: no family yet) are declined with the reason on the record. |
| `book.py` | **The virtual book** on the sealed frame: entry at the NEXT OPEN (never a synthetic open — S1 A7), exit at the horizon CLOSE, costs + slippage both ways on every closed trade, the draft's limits (₹10 lakh, 10% per position, 10 concurrent, 5 new per session), liquidity-descending selection, marked to close every session; **never a bar past the seal** — a position whose exit lies past it stays open; a signal on the seal day is seen, not taken (A14); a trade through a glitch / corporate-action bar is closed **unresolved** — listed, never graded, as the evidence convention's NaN (A7). A pure function of (rule, period, seal), pinned by S2-15. No price stop (§5.2). `passive_incumbent()`: the same capital, equal weight in the whole universe over the same window, costs charged. |
| `grading.py` | **Frozen, symmetric grading of a period** (`experiment_grading@1.0.0+code.<hash of grading.py>`): the metric is the period's mean net P&L per closed resolved trade; Right above one standard error, Wrong below, Inconclusive inside or under the frozen minimum of trades, **void** when nothing closed. The band is the LARGER of the plain and cluster-robust standard errors (§5.3). **Frozen cumulative rules** in every version's spec (A4): the version's whole forward record buried at cluster-robust t < −1.96 on ≥ 10 resolved trades over ≥ 5 signal days; four consecutive void periods bury (`rule_stopped_firing`). `compare()` → stronger / weaker / failed / inconclusive / void. |
| `gate.py` | **Worth-testing gate** = the draft's discovery gauntlet (net > 0, at 2× slippage > 0, edge vs the same-day pool > 0, day-blocked placebo ≤ 0.05) on the whole sealed history **and** the trailing window on its own (NDP), sample floors, implementability, novelty, the S1 card's evidence strength; three ADVISORY gates (family-wise bar α/trials; the pre-`discovery_end` window at 2× slippage; cluster-t) recorded, never fatal here — significance gates promotion, not survival (P1's argument, kept). **Graduation gate** on the version's FORWARD record only: OOS n ≥ `min_n_for_promotion`, expectancy > 0 and at 2× slippage, day-blocked placebo over the forward window, cluster-t, arena roster **Keep**, beats the incumbent on net return with no worse drawdown, implementable, `constitution_signed`. A PROPOSAL exists only when every gate but the signature passes; on the unsigned draft its status is `blocked_unsigned_constitution`. **There is no "promoted" anywhere in the registry.** |
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
python -m pytest backend/tests/test_pathfinder_s2.py -q            # 44 rows, ~80 s (synthetic, engineered conditional edge)
python -m pytest backend/tests/test_pathfinder_s1*.py backend/tests/test_pathfinder_p0.py backend/tests/test_pathfinder_p1.py -q
python scripts/gen_openapi.py --check
```

## 3. Results / evidence

### 3.1 Tests

`backend/tests/test_pathfinder_s2.py`: **44 passed** (S2-01…S2-33, S2-25b, and the ten audit pins
`test_a*` — `docs/TEST_PLAN.md`). The four S1 suites: **134 passed, unchanged**. P0 **37**, P1 **60**.
**275 passed** in 4 m 16 s. `gen_openapi.py --check` in sync. The S2 rows run on a synthetic universe with
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

| Researched finding | closest variant (fewest failed gates) | whole-history net / 2× / edge | trailing net | the ONE gate it failed |
|---|---|---|---|---|
| `fnd_20260504_dip_zentec` (a real `no_trade` dip card) | dips on a session the whole market fell > 1%, held 5 | +0.2456% / +0.0456% / +0.41% (n 8,337 on 441 sessions) | +1.93% (n 768) | **day-blocked placebo p = 0.076 vs 0.05** (random stocks on the same kind of day) |
| `fnd_20260729_dip_j_kbank` (the 60-session retry) | the same | +0.2422% | +1.93% | **placebo p = 0.093 vs 0.05** |
| `fnd_20260504_surge_cempro` | fade the day's biggest jump on a > 1% market fall, held 5 | +0.331% | +0.23% | **implementable_under_cost_convention** (a five-session short) |
| `fnd_20260729_surge_pcbl` | the same | +0.335% | +0.26% | the same |

**Served scoreboard as of 07-29:** Right 0 · Wrong 0 · Inconclusive 0 · n = 0 · void 0 · pending 0 ·
forward 0 · backfilled 0 · testing 0 · buried 0 · proposed 0 · **candidates not opened 131 · trials 72**.
This is the honest reading of the S1 library against the Constitution draft's bar on this data: the one
conditioned dip edge is real in expectancy on every window but not distinguishable, at 5%, from picking
random names on the same crash days. That is a finding, and it is what the gate exists to say.

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
open (t not below −1.96). **Where v2, retirement and the proposal are demonstrated:** the synthetic universe,
pinned by S2-17/18 (Wrong → 5 counted revisions → v2 (L3) → v3 → buried, post-mortem public, losers-first
listing), `test_a4b` (`rule_stopped_firing` after four void periods), S2-20/21 (a proposal only when every
measurable gate passes; `blocked_unsigned_constitution` on the draft, `proposed_awaiting_human` on a signed
copy; the incumbent's return is the passive equal-weight book's; edge reversed → no proposal).

### 3.4 Determinism and end-to-end verification

**Determinism.** The served registry was built twice from scratch on the committed code into two files
(`var/pathfinder_experiments.db`, `var/pathfinder_experiments.build2f.db`), 61 steps each, in two
processes minutes apart. Compared table by table with timestamps (columns and the keys inside JSON)
removed: **13/13 tables identical (61 editions, 131 candidates, 72 trials), 6/6 chains identical.**
S2-25 pins the same property on the synthetic loop with builds an hour apart (versions, outcomes,
post-mortems: identical rows AND identical row hashes while every `created_at` / `frozen_at` differs).
Every earlier registry is `var/pathfinder_experiments.db.archived-<stamp>` — archived, never deleted.

**Over HTTP** (`KANIDA_PATHFINDER_SOURCE=research uvicorn pathfinder.mock_app:app --port 8011`, real calls
against the served registry, `scratchpad/s2_http_e2e.py`):

```
GET /api/pathfinder/experiments -> 200 | count 0 | not_opened 131 | engine pathfinder_experiments@1.0.0+code.cfae44c0f6e9 | llm none
  SCOREBOARD Right 0 · Wrong 0 · Inconclusive 0 · n=0 · void 0 · pending 0 · forward 0 · backfilled 0 · testing 0 · buried 0
             · proposed 0 · not opened 131 · trials 72 · record_label: simulated backfill — generated after the fact; not a forward track record
  researched-and-declined:
    fnd_20260504_surge_cempro  trials=18 closest="…on a session the whole market fell by more than a percent: a virtual short…" etv=0.331 failed=['implementable_under_cost_convention']
    fnd_20260504_dip_zentec    trials=18 closest="…on a session the whole market fell by more than a percent: a virtual long…"  etv=0.246 failed=['history_placebo=0.076 vs 0.05']
    fnd_20260729_surge_pcbl    trials=18 … failed=['implementable_under_cost_convention']
    fnd_20260729_dip_j_kbank   trials=18 … etv=0.242 failed=['history_placebo=0.093 vs 0.05']
GET /feed?date=2026-05-04 -> 200 | backfilled=True | S1 published 4 | experiment_cards [] | experiments_scoreboard: 4 not opened / 36 trials
GET /feed?date=2026-07-29 -> 200 | backfilled=True | S1 published 4 | experiment_cards [] | experiments_scoreboard: 131 not opened / 72 trials
GET /experiment/DROP -> 400 · GET /experiment/exp_nope -> 404
```

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

## 5. Risks

### 5.1 There is no forward record, and no experiment on the served registry
Every row is a backfill (§3.2). The forward record starts with the first step run on a session's own
date; `is_backfilled` labels it and `test_a1` pins that a forward card on a backfilled experiment
validates. Whether the first forward step opens anything depends on the founder's bar (§6): at the
draft's placebo bar of 0.05 the one real conditioned edge is one gate short.

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
The expectation is the S1 evidence convention; the trailing-window check is a persistence check inside the
selection history and every card says so (A5). The pre-`discovery_end` numbers are on every trial and an
advisory gate. The clean OOS is the forward virtual record, which the graduation gate reads.

### 5.5 Multiple comparisons
18 variants at opening, 5 + 3 + … per revision; the count is on every card and every decline; the
family-wise bar (α / trials) is advisory at opening, required at graduation; a declined family waits 60
sessions. The gate does not enforce the family-wise bar at discovery (P1's argument).

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
| **Worth-testing gate thresholds** | the draft's gauntlet (net > 0, 2× slippage > 0, edge > 0, placebo ≤ 0.05) + trailing > 0 (NDP) + sample floors 100 / 30 + S1 evidence strength ≥ 0.5. **The placebo bar is the one that decides today (0.076 / 0.093 vs 0.05).** | `gate.worth_testing_gates`, `config.min_*`, `KANIDA_PFX_*`, the Constitution |
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

`docs/openapi.yaml` regenerated (the S2 contract); `backend/pathfinder/fixtures/*.json` unchanged by content;
`docs/TEST_PLAN.md` (S2 section: 34 rows + the audit table); `docs/DATA_MODEL.md` (the registry's tables,
schema 2); this hand-back. New: `backend/pathfinder/experiments/*` (10 modules), `backend/tests/test_pathfinder_s2.py`,
`scripts/run_pathfinder_experiments.py`. Changed: `backend/pathfinder/schemas.py`, `backend/pathfinder/router.py`.
Untracked by design (`var/` is git-ignored): `var/pathfinder_research.db` (rebuilt, 61 editions),
`var/pathfinder_experiments.db` (the served registry), the `.build2*.db` comparison files, and every
`.archived-<stamp>` registry, including the pre-audit one in §3.3.

## 8. Next step

1. **Founder:** the placebo bar — the one real conditioned edge sits at p = 0.076 / 0.093 against 0.05 with
   every other gate passed; at 0.10 it would open, at 0.05 it stays a finding. Sign or amend the
   Constitution (nothing promotes until then); decide §5.2 (stop) and §5.3 (band); extend
   `approved_parameter_ranges` to the S1 template parameters if the agent is to tune them (L2).
2. **Ops:** schedule `run_pathfinder_scan.py` then `run_pathfinder_experiments.py` after each close;
   the first same-day step is the first forward row.
3. **S3:** the swipeable feed against `/feed` (experiment cards ride on it under the research source)
   and `/experiment/{id}`.
4. **Counsel (addendum 6):** the RA-review flag is a stand-in; the constituent list is withheld from
   every public surface until the workflow exists.
5. **S2.1:** a sector-basket family (`theme_cycle` / rotation) graded on sector-minus-market, and a
   one-session short family for the surge fade (implementable intraday) — both founder-reviewed templates.
