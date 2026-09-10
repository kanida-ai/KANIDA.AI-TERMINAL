# Hand-back — Pathfinder Session S1: ENGINE CORE (post-audit)

**Date:** 2026-09-10 (IST) · **Branch:** `feat/product-build` · **Brief:** `docs/sessions/PATHFINDER_S1_ENGINE.md`
under the LOCKED spec `docs/sessions/PATHFINDER.md`.
**Status:** built to the brief's "done when"; **REJECTED by the adversarial quant audit as first handed
over** (§4); every CONFIRMED finding and the mandatory PLAUSIBLE ones fixed, each pinned by a
regression test that encodes the auditor's recomputed number; store rebuilt from scratch, verified
deterministic across two independent builds and end to end over HTTP (§3); committed locally, not pushed.

> **Governing sentence, honoured in code:** Pathfinder computes market evidence first and uses GenAI
> only to decide what is worth investigating and to explain verified results. **No number in this
> build originates in a model** — every number is a `Fact` minted by a template with provenance,
> the model can only *veto* a card that already cleared the threshold and *narrate* the body with
> `{{fact:…}}` tokens under the engine's own headline; a completion carrying a digit, a number word
> or a quantifier without a fact reference, or no reference at all, is rejected.

> **What the audit taught, in one line:** the first hand-over described a stale store written by
> superseded code as a "non-reproducible regime glitch" (§5.1), graded a rejection by a rule it
> satisfied by construction, scored a null result as six-sigma evidence, and invented sample sizes to
> satisfy a schema. All of that is now unrepresentable in code, not merely fixed in data.

---

## 1. What was built

A new package, `backend/pathfinder/research/`, alongside the P1 `engine/` (kept — S2 will reuse
its point-in-time seal, gauntlet and promotion gate). The P0/P1 scaffolding is extended, not
discarded: the same `pathfinder_llm` gateway and output contract, the same `{{fact:…}}` pattern,
the same guarded router, `docs/openapi.yaml` regenerated from the Pydantic contract.

| Module | What it owns |
|---|---|
| `data.py` | **The seal.** A frame built for one `as_of` holds no later bar — and, since the audit, **no later price either**: the helper columns forward outcomes are minted from (`next_open`, `_d{h}`, `_c{h}`, `_badfwd{h}`) exist only on the private unsealed history and are dropped from every sealed frame (P7). Forward outcomes (`f1/f3/f5` = close[t+h] / **open[t+1]** − 1; `r5cc` = the week-later move) are NaN until resolved inside the seal. Zero prices are holes; so are the warehouse's six listing-day glitch bars (a > 4x or < 0.25x close-to-close ratio — DELHIVERY +9,200% on listing day): their return is NaN and every forward window straddling them is NaN (D1). |
| `regime.py` | Port of `Kanida_Falcon/scripts/regime.py` (breadth 200/50, NIFTY trend, VIX percentile, A/D → RISK_ON/NEUTRAL/RISK_OFF). Provenance only, never a forecast. Independent long-frame breadth cross-check on the last session. |
| `facts.py` | The only way a number is minted: `FactSet.add()` → a `Fact` with data source, period, `as_of`, cost convention, `computed_by` **pinned to the code hash**, universe and evidence level. Three kinds of number: a **statistic** (n required for pct/ratio/x), a **parameter**, a **single observation** (no n, `sample_flag = not_applicable`) — an invented n is refused (C8). A share of exactly 0% or 100% over ≥ 100 cases is refused at mint time (C1). |
| `library.py` | **The question library** — `question → parameters → computation → evidence card`. Six seeded templates porting the two prototypes' math: `market_regime`, `theme_cycle` (+ the laggard→leader rotation flip), `dip`, `surge`, `volume_anomaly`, `relationship`. Every decision-bearing base rate is now the **graded metric** (next open → horizon close), judged against a **named control**, with **expectancy** (winsorised mean net of hurdle) minted and gating next to the median (C4–C7, P5). The decision rules are pure functions (`dip_decision`, `surge_decision`, `carry_decision`, `call_on_excess`) pinned by tests. |
| `grading.py` | Per-type Right / Wrong / Inconclusive rules (`directional_call`, `no_trade_call`, `theme_call`, `theme_watch`, **`rotation_reject`**, `anomaly_move`, `pair_convergence`) built **at publication** and stored with the finding. Every verdict is **symmetric in the graded metric** (C2/C3); rank at the horizon is recorded as a fact, never judged. `rule_version` = `grading_rules@1.1.0+code.<sha256 of grading.py>`; a spec must be complete when frozen and the evaluator never reads live config (P2). The theme kinds define "the market" exactly as the templates do (sectors with ≥ min_names names). |
| `ranking.py` | Usefulness = 0.30 evidence strength + 0.20 novelty + 0.30 trader relevance + 0.20 magnitude; publication threshold 0.65. No quota, no padding. Evidence strength takes the template's `evidence_z`, which is now the z of the base rate against its control (the lift for the anomaly, the coin flip for hit rates, on independent episodes for the pair). |
| `narrate.py` | The model's two permitted jobs: `classify` publish/hold on a card that already cleared the threshold; `narrate` the **body** from the card's fact table under the **engine's headline** (the model's headline is discarded — the decision is not the model's to restate). The strict feed contract (`enforce_narrate(strict=True)`) is re-applied here so no provider can skip it (P1). Without a provider the engine's own digit-free template narrates and the card says `produced_by = "engine"`. |
| `store.py` | Append-only SQLite (`pf_editions`, `pf_findings`, `pf_candidates`, `pf_grades`, `pf_scoreboard`); triggers reject UPDATE/DELETE; one grade per finding, ever; rejected candidates kept with their scores. `pf_editions.engine_version` carries the research code hash (C1); `scoreboard(X).pending` is "pending as of X" (P4). A store written by superseded code is deleted and rebuilt — it cannot be corrected. |
| `scan.py` | seal → regime → every library question → score → threshold → (model veto) → narrate → publish → **grade whatever completed its horizon** → snapshot the scoreboard. An edition is never recomputed. |
| `llm/contracts.py` | The gateway contract, plus the strict feed mode: number words and frequency quantifiers ("nine in ten", "half", "doubled", "most of the time", "usually") must sit in a sentence that cites a fact; at least one fact reference; no number word in a headline. |

**Contract (`backend/pathfinder/schemas.py`, `docs/openapi.yaml`):** `EvidenceLevel`,
`Provenance.level`, `FindingProvenance`, `GradingRule`, `GradingState`, `Narrative` (digit-free for
*both* authors), `UsefulnessScore`, `Finding`, `Scoreboard`, `FeedResponse`,
`GET /api/pathfinder/feed?date=` — clarity-first: `what_matters_now` (first ≤3) then `discoveries`.
Post-audit additions: `SampleFlag.not_applicable`, `GradingKind.rotation_reject`.

**Deviations from the prototypes (all justified in code comments and pinned by S1-34 / the audit rows):**
1. **Entry = next open, and every decision-bearing statistic is measured that way.** The prototypes
   measured close-to-close. Under the traded convention the market card's "60% carry" is 51%, buying a
   6%+ surge at the next open loses (42% / −0.53% / expectancy −0.53%), the rotation flip's base rate
   is 771 flips / 41.8% / −0.42% (not 761 / 44.4% / −0.33% close-to-close), and the pair's headline
   statistic is the spread trade's P&L from the next open (52% cleared costs on 121 episodes), not the
   prototype's "89% of 412 sessions narrowed" — which is kept, labelled a width statistic, as context.
2. **Volume anomaly is judged by its lift over the unconditional rate.** The prototype's 47% "resolved"
   is the universe's 48% — a null result. The card now says so and is a `no_trade` debunk with evidence
   strength 0.5, not a `new_experiment` at strength 1.0.
3. **Theme persistence is conditioned on the card's own criteria and measured on the graded metric.**
   "Leaders like this" (beat the market on ≥ 9 of 15 sessions with a higher cumulative return) went on
   to beat the market by more than costs 46% of the time with negative expectancy (n = 2,655), so the
   theme card is a `watch`. The prototype's "70% still top-three" (any leader) is kept as context only.
4. **The rotation flip follows its base rate** (the prototype opened an experiment unconditionally) and
   its rejection is graded on sector-minus-market only.
5. **Expectancy gates every call** next to the median and hit rate (quant rule: ETV decides).
6. **Data holes.** MAZDOCK's eight 0.0 bars and six listing-day glitch bars are holes, not prices. The
   exact deltas to the prototype counts are asserted (persistence n 3,316 vs 3,296; surge 24,727 vs
   24,734; dip 12,632 vs 12,633).

## 2. How to run

```bash
# the after-close scan (latest close; --date for a specific close; --backfill N for prior sessions)
python scripts/run_pathfinder_scan.py --fresh --backfill 12
# env: KANIDA_DB (price warehouse), KANIDA_PATHFINDER_RESEARCH_DB (the store),
#      KANIDA_PF_COST_HURDLE_PCT, KANIDA_PF_USEFULNESS_THRESHOLD, KANIDA_PF_PAIRS ("A,B;C,D"),
#      KANIDA_PF_ANOMALY_MIN_LIFT_PP
#      --llm none|auto|recorded|live  (default none: engine narration, stated on every card)

# serve it
cd backend && uvicorn pathfinder.mock_app:app --port 8010
curl http://127.0.0.1:8010/api/pathfinder/feed            # latest edition
curl http://127.0.0.1:8010/api/pathfinder/feed?date=2026-07-22

# tests
python -m pytest backend/tests/test_pathfinder_s1.py -q          # 34 rows
python -m pytest backend/tests/test_pathfinder_s1_audit.py -q    # 39 audit regressions
python -m pytest backend/tests -q                                # the whole backend suite
python scripts/gen_openapi.py --check                            # contract in sync
```

## 3. Results / evidence

### 3.1 Tests

`backend/tests/test_pathfinder_s1.py`: **34 passed** (S1-01…S1-34). `backend/tests/test_pathfinder_s1_audit.py`:
**39 passed** (A-C1…A-D1 + the real-warehouse rows, `docs/TEST_PLAN.md`). P0 and P1 unchanged and
passing (`gen_openapi.py --check` in sync after the two enum additions).
Whole backend suite: <<SUITE>>

### 3.2 The real run — thirteen editions, 2026-07-13 → 2026-07-29, rebuilt from scratch after the fixes

**Provenance of this table:** `var/pathfinder_research.db`, built by `run_pathfinder_scan.py --fresh
--backfill 12` on the post-audit code (`engine_version` = `pathfinder_research@1.1.0+code.896c99ef76c0`). The store this
section described in the first hand-over was written by **superseded code** (`var/pathfinder_research_smoke.db`,
now deleted — §5.1); the numbers below supersede it and are not comparable to it. Each edition is
computed from bars ≤ its date only. Threshold 0.65. **Never padded:**

| Edition | Candidates | Published | Graded that day | Scoreboard after (pending = as of that date) |
|---|---|---|---|---|
| 07-13 | 4 | 4 | — | pending 4 |
| 07-14 | 5 | 3 | 2 | R1 W0 I1 n=2 · pending 5 |
| 07-15 | 5 | 3 | 2 | R1 W1 I2 n=4 · pending 6 |
| 07-16 | 5 | 4 | 2 | R3 W1 I2 n=6 · pending 8 |
| 07-17 | 5 | 3 | 2 | R4 W1 I3 n=8 · pending 9 |
| 07-20 | 6 | 4 | 4 | R7 W2 I3 n=12 · pending 9 |
| 07-21 | 6 | 5 | 2 | R8 W3 I3 n=14 · pending 12 |
| 07-22 | 6 | 5 | 4 | R11 W3 I4 n=18 · pending 13 |
| 07-23 | 6 | 4 | 5 | R14 W4 I5 n=23 · pending 12 |
| 07-24 | 6 | 3 | 4 | R17 W5 I5 n=27 · pending 11 |
| 07-27 | 5 | 2 | 5 | R20 W7 I5 n=32 · pending 8 |
| 07-28 | 6 | 4 | 3 | R21 W7 I7 n=35 · pending 9 |
| 07-29 | 7 | 5 | 4 | **Right 22 · Wrong 9 · Inconclusive 8 · n=39** · pending 10 |

49 findings published out of 72 candidates; 23 stayed below the threshold and are in `pf_candidates`
with their scores. What changed against the superseded run, and why:
- **No theme call fired in thirteen editions.** The old engine called Realty a `virtual_long` on 07-13
  (usefulness 0.97) through a persistence gate that never binds; graded on "leaders like this" it is a
  `watch` every day, and the seven theme watches / rotation rejects graded so far are Right 6 · Wrong 2
  · Inconclusive 1 — under the symmetric rule, where Right means the sector lagged by more than costs.
- **The volume anomaly is a debunk, not the lead.** Seven anomaly cards published as `no_trade` (evidence
  strength 0.5, ranked fourth or lower, or below the threshold — ALKEM 07-14 at 0.55); graded as the
  NO TRADE they are: Right 1 · Wrong 3. The old run published them as `new_experiment` at strength 1.0,
  twice at #1.
- **The pair is the traded metric.** HDFCBANK/ICICIBANK published twice on the spread-trade base rate
  (121 episodes, 52% cleared costs, expectancy +0.22%); the 07-20 experiment was graded **WRONG**
  (the spread widened by more than two legs' costs); five other days it sat at 0.58–0.64, below the bar.
- **Surge rejections and dip no-trades** carry the scoreboard: surge (7,2,2), dip (8,0,2). The market card
  is (0,1,3) — its median follow-through never cleared costs, and once it was wrong to say so.

### 3.3 Port fidelity (S1-34, real warehouse, close 2026-07-29)

Where the statistic is identical the numbers reproduce the prototypes exactly: IT beat the market on
**8 of 15** sessions, **+11.7% vs +2.6%**, breadth **93%**; any-leader persistence **70% of 3,316**;
HDFCBANK/ICICIBANK's gap narrowed **89% of 412** times (kept as context, not as the claim). Where they
differ the delta is a data hole and is asserted: dip n 12,632 (12,633), surge 24,727 (24,734),
persistence n 3,316 (3,296).

### 3.4 Determinism and end-to-end verification

**Determinism (C1).** The store was built twice from scratch (`--fresh --backfill 12`) into two files
by two independent processes. Compared table by table with only timestamps (`computed_at`, `at`,
`frozen_at`, `graded_at`, `created_at`, `generated_at`) removed: **49/49 findings identical, 39/39
grades identical (verdicts and realised facts), 13/13 editions identical (`engine_version` =
`pathfinder_research@1.1.0+code.896c99ef76c0`), 72/72 candidates identical.** Every grade carries
`rule_version = grading_rules@1.1.0+code.67a3f35e5965`. The same property is pinned by A-C1 on the
synthetic universe and on the real 07-29 seal.

**Over HTTP** (`uvicorn pathfinder.mock_app:app --port 8010`, real calls):

```
GET /api/pathfinder/feed -> 200 | edition 2026-07-29 | regime NEUTRAL (risk score 46/100, breadth>200DMA 56%) | llm none
universe 497 · candidates 7 · published 5 · threshold 0.65
  #1 what_matters_now theme_cycle     Telecommunication   reject    u=0.86 ev=1.00 sector       n=771    rotation_reject/5 pending engine
  #2 what_matters_now surge           PCBL                reject    u=0.86 ev=1.00 whole_market n=24727  no_trade_call/1   pending engine
  #3 what_matters_now dip             J&KBANK             no_trade  u=0.77 ev=0.62 whole_market n=12632  no_trade_call/1   pending engine
  #4 discovery        volume_anomaly  DCMSHRIRAM          no_trade  u=0.69 ev=0.50 whole_market n=13154  no_trade_call/5   pending engine
       headline: Huge volume, flat close: a striking day that history says means nothing
       group: control = every Nifty-five-hundred stock-session with a resolved week-later move since 2013-01-01 (the unconditional rate); ...
  #5 discovery        theme_cycle     Information Technology  watch  u=0.68 ev=0.81 sector   n=2655   theme_watch/5     pending engine
digit-free narratives: True | all fact refs resolve and >=1: True | provenance complete: True | no fabricated n: True
SCOREBOARD Right 22 · Wrong 9 · Inconclusive 8 · n=39 · pending 10
  by template: dip (8,0,2) · market_regime (0,1,3) · relationship (0,1,0) · surge (7,2,2) · theme_cycle (6,2,1) · volume_anomaly (1,3,0)
GET /feed?date=2026-07-20 -> 200 · 4 findings: Power rotation_reject RIGHT · HDFCBANK/ICICIBANK pair_convergence WRONG · Realty theme_watch RIGHT · MAPMYINDIA no_trade_call RIGHT
  Power rotation_reject realised: excess -1.12% rank_at_horizon 14 (recorded, not judged) -> right
GET /feed?date=DROP -> 400 · GET /feed?date=1999-01-01 -> 404 · GET /api/pathfinder/loop -> 200 (P0 surface intact)
```

What the feed now shows against the audit: the zero-lift anomaly is a `no_trade` debunk at evidence
strength 0.5, ranked #4 (it was #1 as evidence at 1.0); the pair card, when it publishes, leads with the
spread trade on 121 episodes (on 07-29 it is below the threshold at 0.58); every `comparison_group`
names a real control; no fact carries an invented n; the rotation reject is graded on the excess alone
with the rank recorded as a fact. Two of the 07-29 cards were computed and **not** published (the market
card at 0.64 and the pair at 0.58) — in `pf_candidates` with their scores.

## 4. The quant audit

The `dev-quant-auditor` **rejected** the engine as first handed over. Every finding below was
reproduced, fixed, and pinned by a regression test in `backend/tests/test_pathfinder_s1_audit.py`
that fails on the old code and encodes the auditor's recomputed expectation.

| # | Finding (auditor) | What changed | Pinned by |
|---|---|---|---|
| **C1** | `var/pathfinder_research_smoke.db` (superseded code) carried `big_move = 0.0%` over 13,119 cases and a 07-29 regime of `RISK_OFF 34/100, breadth 10%` (independent recompute: 55.79%, NEUTRAL, 46); the hand-back called the two stores "edition-for-edition identical" and the regime "unexplained". Both false — the code changed between runs. | Smoke store deleted (append-only stores are rebuilt, never corrected). `pf_editions.engine_version` and every fact's `computed_by` carry `+code.<sha256[:12] of research/*.py>`. `FactSet.add` refuses a share of exactly 0%/100% over n ≥ 100 (medians exempt; a documented `degenerate_ok` for a structurally bounded share). §3.2/§3.4/§5.1 rewritten. | A-C1 (×4, incl. real: the same seal scanned twice into two stores is byte-identical) |
| **C2** | The rotation flip's REJECT was graded with `theme_watch`, whose Right condition included "dropped out of the top three" — a bottom-four sector satisfies that 96.1% of the time (3.9% of 761 flips are top-3 at the horizon) regardless of outcome, though it beat the market by > hurdle in 36.9% of flips. | New `GradingKind.rotation_reject`: Right if sector − market f5 < −H, Wrong if > H, else Inconclusive. Rank at horizon is minted as a fact, not judged. | A-C2 (×3), real-C6 |
| **C3** | `theme_watch`: Wrong required `excess > H AND still_top3`, Right required `excess < −H OR not still_top3` — asymmetric in Pathfinder's favour. | Symmetric: Wrong if excess > H, Right if excess < −H, else Inconclusive. Rule text rewritten. | A-C2/A-C3 |
| **C4** | Volume anomaly: 47.35% (n 13,154) "moved 3%+" vs an unconditional 48.44% (n 1.26M) — zero lift — yet z was computed against 50% (≈ 6σ), evidence strength 1.0, ranked #1 on 07-23/24; `comparison_group` named the selection, not a control. | `base_big_move` (n = every resolved stock-session), `lift`, `base_up_share` minted; z = lift beyond `anomaly_min_lift_pp` (5 pp, founder input) over the control; `comparison_group` names the control. Zero-lift → `no_trade` debunk graded `no_trade_call`/5; a real lift → `new_experiment` graded `anomaly_move`. | A-C4 (×2), real-C4 (z = 0, strength 0.5, no_trade) |
| **C5** | Pair card evidence (89.1% of 412 sessions "narrowed") ≠ graded metric (next-open spread P&L vs ±2H: Right 51.0%, Wrong 31.3%, median +0.69% on the same 412); 412 overlapping event-days treated as independent (121 episodes). | Primary facts = spread-trade P&L from the next open on **episodes** (first day of each run): Right 52.1%, Wrong 28.9%, up 62.0%, median +0.71%, expectancy +0.22% net of both legs, n = 121; event-day versions labelled overlapping; `snapped_back` labelled a width statistic. Decision needs median > 2H, up ≥ 58%, expectancy > 0. | A-C5 (×2), real-C5 |
| **C6** | Rotation base rate summed close-to-close returns (761 / 44.4% / −0.33%); the rule grades f5 (recompute: 770 / 41.8% / −0.42%); docstring and label claimed next-open. | Computed from sector and market means of f5 on the same universe the grader uses: **771 / 41.8% / −0.42%** (771 once the six glitch bars are holes; 770 reproduces exactly with the guard off). Labels and docstring say next open. | real-C6, real-D1 |
| **C7** | Persistence gate never binds (69–74% every year since 2013; `persist ≥ 55` constant) and "leaders like this" was every leader-session. | `like_this_*` facts on the card's own criteria (sec_cum > mkt_cum AND days_out ≥ 9/15) measuring the graded metric: n 2,655, beat-by->H 45.95%, lagged 41.7%, median +0.10%, expectancy −0.17%. Strong test = those + expectancy; `persistence` relabelled "any leader … context only". IT on 07-29 is a `watch`. | A-C7, real-C7 |
| **C8** | Fabricated n: `n=dO if dO else 1`, n=n1/nn on thresholds, n=z_window on today's z, n=20 on today's volume multiple, n=1 on single observations rendered as "greyed"; the schema forced n on every pct/ratio/x. | `FactSet.add(sample="parameter"|"observation")` → `sample_flag = not_applicable`, no n; passing n on those is refused; a statistic still requires n. Applied to `move_today`, `volume_x`, `sigma`, `threshold`, `move_threshold`, `band`, `min_lift`, `window`, `watch_*_move`, `other_*_return`, and the grader's single realised outcomes. | A-C8 (×2), real-C4 |
| **P1** | Prose could carry numbers as words ("nine in ten", "half", "doubled"); the model's headline replaced the engine's; zero fact refs accepted; nothing checked body vs decision. | Strict feed contract: number-word / quantifier sentences must cite a fact; ≥ 1 ref and ≥ 1 token used; no number word in a headline; **the engine's headline is kept** (the decision is the engine's); re-enforced in `narrate.py` regardless of provider. | A-P1 (×8) |
| **P2** | `GRADING_RULES_VERSION` a constant; grader fell back to live config for missing spec keys. | `rule_version` = hash of `grading.py`; `REQUIRED_SPEC` per kind asserted in `build_rule`; `evaluate` raises on a missing key. | A-P2 (×2) |
| **P7** | `sealed()` kept `next_open`, `_d{h}`, `_c{h}` — post-seal prices physically present on a resealed frame. | Dropped in `sealed()` (`POST_SEAL_COLUMNS`), together with the new `_badfwd{h}`. | A-P7, real-P7 |
| **P4** | Scoreboard `pending` counted only never-graded findings. | `pending` = edition ≤ X and (no grade or `data_as_of` > X). | A-P4 |
| **P5** | Decisions used median + hit rate; the quant rule says expectancy decides. | Expectancy = winsorised (1%) mean net of the hurdle, minted on every group base rate and gating every call (`dip_decision`, `surge_decision`, `carry_decision`, `call_on_excess`). Winsorised because the raw dip five-day mean was **+14.3%** off one 831x glitch "return" (+0.08% winsorised). | A-P5 (×2), real-C8/P5 |
| **D1** (found while fixing) | Six listing-day / corporate-action bars with close-to-close ratios of 4.6x to 1,122x were counted as +6% "surges" and poisoned means and sector-day returns. | A >4x / <0.25x bar is a hole: NaN return, NaN for every forward window straddling it, NaN `r15` across it. | A-D1, real-D1 |
| P3 (documented) | Survivorship disclosure partial. | Not fixable without point-in-time membership: today's Nifty-500 members **and today's sector labels** are applied to 2013→2026 history. Stated on every provenance (`universe`); see §5.3. | — |
| P6 (documented) | The model veto can change the top-3 by promoting rank 4. | True by design: ranking is deterministic and the model may only remove; a hold moves the next card up. Every hold is on the record in `pf_candidates` with the model's rationale. Not changed. | S1-30 |
| P8 (documented) | Deviations-from-prototype list. | §1, six items, each asserted. | S1-34 |

**Not done:** the P1 engine's own narrate path (`engine/narrator.py`, the recorded cassette) keeps the
non-strict contract — six of its fourteen recorded narrations cite no fact and one says "twice"; making
it strict would invalidate the P1 cassette, which is out of S1's scope. Flagged for S2.

## 5. Risks

### 5.1 The "unexplained regime reading" was a stale run — corrected account
The first hand-over said one of three "otherwise byte-identical" backfills produced a 07-29 regime of
`RISK_OFF (34/100, breadth 10%)`, that "every other fact in that edition was identical", and that the
rebuild was "edition-for-edition identical to the previous run". **That was wrong.** The odd store was
`var/pathfinder_research_smoke.db`, written **before** `scan.py`, `data.py` and `regime.py` were patched
during the session; it also carried `big_move = 0.0%` over 13,119 cases on the 07-14 ALKEM card, and its
07-27 edition never computed the market card. It was a run of superseded code, misdescribed as a glitch.
What now prevents a repeat: every edition and every fact records the code hash; a determinism test
(synthetic and real) pins two builds of the same seal to identical cards; a 0%/100% share over ≥ 100
cases cannot be minted; the smoke store is gone. The `build_regime` long-frame cross-check stays.

### 5.2 The model is not in the loop yet
No `ANTHROPIC_API_KEY`; the recorded provider has no cassettes for feed keys. Every real edition is
engine-narrated and says so on every card (`produced_by: engine`). The gateway path is exercised by
S1-29/30 and A-P1 with a fake provider through the *same* contracts: a numeral, a number word without
a reference, a zero-reference narration and a replaced headline are each rejected or overridden.

### 5.3 Survivorship (audit P3)
The universe is today's Nifty-500 membership **and today's sector labels**, applied to the whole
2013→2026 history and stated on every provenance. Group base rates on survivors flatter absolute
numbers; the cards are cost-hurdle decisions and hit rates rather than absolute returns, which limits
but does not remove the bias. A point-in-time membership history is the real fix and the warehouse does
not have one.

### 5.4 Grading a probabilistic claim on one outcome
A card that says "46% of leaders like this beat the market" is graded on the single subject's outcome.
The per-card verdict is noisy by construction; the per-template split in the scoreboard is the number
to read. A zero-lift anomaly is graded as the NO TRADE it now is (would a long from the next open have
lost after costs), which is symmetric and approximately a coin flip — not flattering.

### 5.5 Data quality
The warehouse is labelled corporate-action back-adjusted but carries at least six listing-day glitch bars
(D1) and MAZDOCK's zero bars. The guards catch ratios outside [0.25, 4]; a smaller unadjusted split (2x,
3x) would pass them. Means are winsorised at 1% for that reason; medians and hit rates are robust.

### 5.6 Not built (S2/S3 by design)
No virtual capital, no experiment registry/versions, no promotion gate, no swipeable UI.

## 6. Founder inputs — stubbed with the prototype defaults, flagged

| Input | Stub | Where |
|---|---|---|
| Question-template set | the six prototype-proven templates | `research/library.py` `LIBRARY` |
| Cost hurdle | **0.30% round trip** (prototype) | `config.cost_hurdle_pct` / `KANIDA_PF_COST_HURDLE_PCT` |
| Usefulness threshold + weights | 0.65; weights 0.30/0.20/0.30/0.20; template relevance 1.0/1.0/0.7/0.6/0.5/0.5; decision weights | `config.usefulness_threshold`, `ranking.py` |
| **Anomaly minimum lift** (new, C4) | **5 percentage points** over the unconditional rate | `config.anomaly_min_lift_pp` / `KANIDA_PF_ANOMALY_MIN_LIFT_PP` |
| Expectancy winsorisation (new, P5) | 1% each tail | `config.expectancy_winsor_pct` |
| Per-type grading rules | the seven kinds above, ±hurdle band, pair = 2× | `grading.build_rule` |
| Pair list | (TMPV, M&M) (HDFCBANK, ICICIBANK) (INFY, TCS) (SBIN, BANKBARODA) | `config.pairs` / `KANIDA_PF_PAIRS` |
| Template parameters | dip −6%, surge +6%, hit-rate bar 58%, fade bar 45%, volume 3× / ±1.5% / 3% move, regime band ±0.2%, theme window 15 / min 5 names / horizon 5 / "like this" ≥ 9 of 15, pair z-window 60 / 2σ | `config.py` |
| "What matters now" size | 3 | `config.what_matters_now` |

## 7. Docs written / changed

`docs/openapi.yaml` (regenerated: `SampleFlag.not_applicable`, `GradingKind.rotation_reject`) ·
`docs/TEST_PLAN.md` (S1 section: S1-34 deltas, the 39 audit rows, known gaps rewritten) ·
`docs/DATA_MODEL.md` (engine/rule code hashes, pending-as-of, `not_applicable`) ·
`backend/pathfinder/fixtures/*.json` (regenerated by `gen_openapi.py`).
New: `backend/pathfinder/research/*` (10 modules), `backend/tests/test_pathfinder_s1.py`,
`backend/tests/test_pathfinder_s1_audit.py`, `scripts/run_pathfinder_scan.py`, this hand-back.

## 8. Next step

1. **Founder:** confirm or change the stubs in §6 — the threshold, the hurdle and the new anomaly
   minimum lift are the three that change what gets published.
2. **S2:** the experiment loop on top of this store — a `new_experiment` card becomes an experiment
   with versions and trial counts; virtual capital; expected-vs-actual; promotion only through the
   arena gate. Apply the strict narrate contract to the P1 engine path (re-record its cassette).
3. **S3:** the swipeable feed against `GET /api/pathfinder/feed` — the contract is generated and mocked.
4. **Ops:** schedule `run_pathfinder_scan.py` after each close once the EOD pipeline lands the bar;
   the scan grades the prior editions in the same pass. Never correct a store — rebuild it.
