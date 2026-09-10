# Hand-back — Pathfinder Session S1: ENGINE CORE (post-audit)

**Date:** 2026-09-10 (IST) · **Branch:** `feat/product-build` · **Brief:** `docs/sessions/PATHFINDER_S1_ENGINE.md`
under the LOCKED spec `docs/sessions/PATHFINDER.md`.
**Status:** built to the brief's "done when"; **REJECTED by the adversarial quant audit as first handed
over** (§4); every CONFIRMED finding and the mandatory PLAUSIBLE ones fixed, each pinned by a
regression test that encodes the auditor's recomputed number; store rebuilt from scratch, verified
deterministic across two independent builds and end to end over HTTP (§3); committed locally, not pushed.
**Second pass (2026-09-10):** a SECOND independent audit of that commit found eight further issues
(A1–A8, §4 "Second audit") — the most important being that **the whole scoreboard is a simulated
backfill and was presented as a track record**. All eight fixed and pinned
(`backend/tests/test_pathfinder_s1_audit2.py`, 28 rows); the store archived (never deleted) and
rebuilt under the new conventions; §3.2/§3.3 rewritten as what they are: **a simulated backfill, not
a forward track record. Forward n = 0 today.**

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

> **What the second audit taught, in one line:** a store that is generated after the fact is a
> backfill however honest its seal, and a feed that republishes one claim daily and grades every
> repeat is counting one coin flip many times. Both are now labelled in the schema and unrepresentable
> as anything else: `backfilled` is on every edition, card and grade, and a repeated open claim is a
> `continue` that is graded once.

---

## 1. What was built

A new package, `backend/pathfinder/research/`, alongside the P1 `engine/` (kept — S2 will reuse
its point-in-time seal, gauntlet and promotion gate). The P0/P1 scaffolding is extended, not
discarded: the same `pathfinder_llm` gateway and output contract, the same `{{fact:…}}` pattern,
the same guarded router, `docs/openapi.yaml` regenerated from the Pydantic contract.

| Module | What it owns |
|---|---|
| `data.py` | **The seal.** *Second audit:* corporate-action days (split / bonus / demerger / rights ex-dates from the warehouse's `corp_actions` table, plus any single-day \|move\| > 30% as a suspected one) and the six glitch bars are holes — NaN return, NaN for every forward window across them — and are counted per edition (A4); a bar whose open equals its close is a **synthetic open** and no outcome is measured from an entry at it (A7); `r{h}cc` is minted per horizon so the anomaly grader reads the rule's own horizon (A8).  A frame built for one `as_of` holds no later bar — and, since the audit, **no later price either**: the helper columns forward outcomes are minted from (`next_open`, `_d{h}`, `_c{h}`, `_badfwd{h}`) exist only on the private unsealed history and are dropped from every sealed frame (P7). Forward outcomes (`f1/f3/f5` = close[t+h] / **open[t+1]** − 1; `r5cc` = the week-later move) are NaN until resolved inside the seal. Zero prices are holes; so are the warehouse's six listing-day glitch bars (a > 4x or < 0.25x close-to-close ratio — DELHIVERY +9,200% on listing day): their return is NaN and every forward window straddling them is NaN (D1). |
| `regime.py` | Port of `Kanida_Falcon/scripts/regime.py` (breadth 200/50, NIFTY trend, VIX percentile, A/D → RISK_ON/NEUTRAL/RISK_OFF). Provenance only, never a forecast. Independent long-frame breadth cross-check on the last session. |
| `facts.py` | *Second audit A6:* a statistic over n = 0 cannot be minted — "0.0% of 0 times" is unrepresentable. The only way a number is minted: `FactSet.add()` → a `Fact` with data source, period, `as_of`, cost convention, `computed_by` **pinned to the code hash**, universe and evidence level. Three kinds of number: a **statistic** (n required for pct/ratio/x), a **parameter**, a **single observation** (no n, `sample_flag = not_applicable`) — an invented n is refused (C8). A share of exactly 0% or 100% over ≥ 100 cases is refused at mint time (C1). |
| `library.py` | *Second audit:* every decision and every frozen rule is judged against `cfg.hurdle_pct` = costs + slippage both ways (A5); each card carries an **evidence signature** — the statistic and comparison group for a group card, the sector / pair and decision otherwise (A2); the theme card is judged on the graded metric's conditional base rate with an **effective n** (one case per sector per non-overlapping horizon), states persistence as the non-overlapping mechanical number, and is withheld when no leader like it has resolved (A3/A6). **The question library** — `question → parameters → computation → evidence card`. Six seeded templates porting the two prototypes' math: `market_regime`, `theme_cycle` (+ the laggard→leader rotation flip), `dip`, `surge`, `volume_anomaly`, `relationship`. Every decision-bearing base rate is now the **graded metric** (next open → horizon close), judged against a **named control**, with **expectancy** (winsorised mean net of hurdle) minted and gating next to the median (C4–C7, P5). The decision rules are pure functions (`dip_decision`, `surge_decision`, `carry_decision`, `call_on_excess`) pinned by tests. |
| `grading.py` | *Second audit:* rank at the horizon is withheld, never zero (A6); the anomaly kind reads the rule's horizon (A8); the hurdle includes slippage (A5). Per-type Right / Wrong / Inconclusive rules (`directional_call`, `no_trade_call`, `theme_call`, `theme_watch`, **`rotation_reject`**, `anomaly_move`, `pair_convergence`) built **at publication** and stored with the finding. Every verdict is **symmetric in the graded metric** (C2/C3); rank at the horizon is recorded as a fact, never judged. `rule_version` = `grading_rules@1.1.0+code.<sha256 of grading.py>`; a spec must be complete when frozen and the evaluator never reads live config (P2). The theme kinds define "the market" exactly as the templates do (sectors with ≥ min_names names). |
| `ranking.py` | Usefulness = 0.30 evidence strength + 0.20 novelty + 0.30 trader relevance + 0.20 magnitude; publication threshold 0.65. No quota, no padding. *Second audit A2:* novelty is keyed on the **evidence signature**, not the day's subject — the same group statistic under a new stock is the same claim (0.25). Evidence strength takes the template's `evidence_z`, which is now the z of the base rate against its control (the lift for the anomaly, the coin flip for hit rates, on independent episodes for the pair). |
| `narrate.py` | The model's two permitted jobs: `classify` publish/hold on a card that already cleared the threshold; `narrate` the **body** from the card's fact table under the **engine's headline** (the model's headline is discarded — the decision is not the model's to restate). The strict feed contract (`enforce_narrate(strict=True)`) is re-applied here so no provider can skip it (P1). Without a provider the engine's own digit-free template narrates and the card says `produced_by = "engine"`. |
| `store.py` | *Second audit:* `backfilled` on every edition and finding, the scoreboard split `forward` / `backfilled` with `n` = independent grades, `n_total`, `continued` (A1/A2); `continues` on a finding that repeats an open claim — served, not counted, never graded on its own (A2); a store under a superseded schema is refused on open, and the script archives rather than deletes (A8). Append-only SQLite (`pf_editions`, `pf_findings`, `pf_candidates`, `pf_grades`, `pf_scoreboard`); triggers reject UPDATE/DELETE; one grade per finding, ever; rejected candidates kept with their scores. `pf_editions.engine_version` carries the research code hash (C1); `scoreboard(X).pending` is "pending as of X" (P4). A store written by superseded code is deleted and rebuilt — it cannot be corrected. |
| `scan.py` | seal → regime → every library question → score → threshold → (model veto) → narrate → publish → **grade whatever completed its horizon** → snapshot the scoreboard. An edition is never recomputed. *Second audit:* `is_backfilled(edition, computed_at)` — generated after the session date in IST is a backfill (A1); a draft whose signature matches an OPEN root finding becomes a `continue` after the new findings, outside "what matters now" (A2); every provenance carries `cfg.data_disclosures` (A4/A7/A8). |
| `llm/contracts.py` | The gateway contract, plus the strict feed mode: number words and frequency quantifiers ("nine in ten", "half", "doubled", "most of the time", "usually") must sit in a sentence that cites a fact; at least one fact reference; no number word in a headline. |

**Contract (`backend/pathfinder/schemas.py`, `docs/openapi.yaml`):** `EvidenceLevel`,
`Provenance.level`, `FindingProvenance`, `GradingRule`, `GradingState`, `Narrative` (digit-free for
*both* authors), `UsefulnessScore`, `Finding`, `Scoreboard`, `FeedResponse`,
`GET /api/pathfinder/feed?date=` — clarity-first: `what_matters_now` (first ≤3) then `discoveries`.
Post-audit additions: `SampleFlag.not_applicable`, `GradingKind.rotation_reject`. **Second audit:**
`FeedResponse.backfilled` / `record_label` / `continued_count`; `Finding.backfilled` / `continues`;
`GradingState.backfilled` / `record` / `continues` and `GradingStatus.continued`;
`FindingProvenance.disclosures`; `Scoreboard.forward` / `backfilled` / `n_total` / `n_independent` /
`continued` / `record_label`; the labels `BACKFILL_LABEL` ("simulated backfill — generated after the
fact; not a forward track record") and `FORWARD_LABEL`.

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
python scripts/run_pathfinder_scan.py --backfill 12
# rebuild: the store is append-only and is NEVER deleted — it is moved aside, and only with the flag
python scripts/run_pathfinder_scan.py --archive-store --i-understand-this-archives-the-store --backfill 12
# env: KANIDA_DB (price warehouse), KANIDA_PATHFINDER_RESEARCH_DB (the store),
#      KANIDA_PF_COST_HURDLE_PCT, KANIDA_PF_SLIPPAGE_PCT (each way; second audit A5),
#      KANIDA_PF_USEFULNESS_THRESHOLD, KANIDA_PF_PAIRS ("A,B;C,D"), KANIDA_PF_ANOMALY_MIN_LIFT_PP
#      --llm none|auto|recorded|live  (default none: engine narration, stated on every card)

# serve it
cd backend && uvicorn pathfinder.mock_app:app --port 8010
curl http://127.0.0.1:8010/api/pathfinder/feed            # latest edition
curl http://127.0.0.1:8010/api/pathfinder/feed?date=2026-07-22

# tests
python -m pytest backend/tests/test_pathfinder_s1.py -q          # 34 rows
python -m pytest backend/tests/test_pathfinder_s1_audit.py -q    # 39 audit regressions (pass-1 conventions, explicit)
python -m pytest backend/tests/test_pathfinder_s1_audit2.py -q   # 28 second-audit regressions (pass-2 conventions)
python -m pytest backend/tests -q                                # the whole backend suite
python scripts/gen_openapi.py --check                            # contract in sync
```

## 3. Results / evidence

### 3.1 Tests

`backend/tests/test_pathfinder_s1.py`: **34 passed** (S1-01…S1-34). `backend/tests/test_pathfinder_s1_audit.py`:
**39 passed** (A-C1…A-D1 + the real-warehouse rows, `docs/TEST_PLAN.md`). `backend/tests/test_pathfinder_s1_audit2.py`:
**28 passed** (the second audit, A2-A1…A2-A8 + HTTP; the file does not collect against the pre-fix
engine). P0 and P1 unchanged and passing (`gen_openapi.py --check` in sync after the contract
additions). The pass-1 suites pin the pass-1 conventions explicitly (`PASS1` / `PASS1_DATA`); the
pass-2 suite pins the defaults (hurdle 0.50% incl. slippage, corporate-action days and synthetic
opens excluded).

### 3.2 The real run — thirteen editions, 2026-07-13 → 2026-07-29: a SIMULATED BACKFILL, not a forward track record

**Read this first.** Every edition below was generated on **2026-09-10** for sessions in July. Each
one is computed from bars ≤ its date only (the seal holds), but the outcomes were knowable in the
world when the cards were written. That makes the whole table a **simulated backfill**; the feed,
every card and the scoreboard say so (`record_label`, `backfilled = 1`, `forward = 0`). Nothing here
is a track record. The forward record starts at zero and begins with the first scan run on a
session's own date after the EOD bar lands (§8).

**Provenance of this table:** `var/pathfinder_research.db`, built by `run_pathfinder_scan.py
--archive-store --i-understand-this-archives-the-store --backfill 12` on the second-pass code
(`engine_version` = `pathfinder_research@1.2.0+code.84efb0cfb4dd`, `rule_version` =
`grading_rules@1.2.0+code.d5f3e3fce7ac`). The previous store (pass-1 code, 49 findings, n = 39) is
`var/pathfinder_research.db.archived-20260910T091954` — archived, not deleted (A8); its numbers are
superseded and not comparable. Data rules on this build: 179 corporate-action bars + 45 suspected +
6 glitch bars excluded; 19,083 synthetic opens carry no outcome. Threshold 0.65, hurdle 0.50%.
**Never padded; continuations are not publications; n is independent:**

| Edition | Candidates | Published (new) | Continued | Graded that day | Scoreboard after (independent n; pending = as of that date) |
|---|---|---|---|---|---|
| 07-13 | 4 | 4 | 0 | — | pending 4 |
| 07-14 | 5 | 2 | 2 | 2 | R1 W0 I1 n=2 · pending 4 |
| 07-15 | 5 | 2 | 2 | 2 | R1 W0 I3 n=4 · pending 4 |
| 07-16 | 5 | 2 | 1 | 2 | R3 W0 I3 n=6 · pending 4 |
| 07-17 | 5 | 1 | 2 | 1 | R4 W0 I3 n=7 · pending 4 |
| 07-20 | 6 | 3 | 0 | 3 | R6 W1 I3 n=10 · pending 4 |
| 07-21 | 6 | 3 | 1 | 1 | R6 W1 I4 n=11 · pending 6 |
| 07-22 | 6 | 4 | 2 | 2 | R8 W1 I4 n=13 · pending 8 |
| 07-23 | 6 | 1 | 3 | 4 | R10 W2 I5 n=17 · pending 5 |
| 07-24 | 6 | 1 | 3 | 1 | R10 W2 I6 n=18 · pending 5 |
| 07-27 | 5 | 1 | 2 | 3 | R11 W3 I7 n=21 · pending 3 |
| 07-28 | 6 | 1 | 1 | 2 | R12 W3 I8 n=23 · pending 2 |
| 07-29 | 7 | 3 | 0 | 2 | **Right 12 · Wrong 4 · Inconclusive 9 · n=25 (independent) · forward 0 · backfilled 25** · 19 continuations folded · pending 3 |

**28 new findings** published out of 72 candidates (the pass-1 store published 49 of 72 — the
second auditor's "the threshold does not gate"); **19 continuations** served but not counted; 25
independent grades from 25 grade rows (`n_total = n`: nothing was graded twice). What the
continue semantics did to the repeats the auditor counted: Realty was published **once** (07-13,
`watch`, graded Right on 07-20: excess −1.13%) and continued four times; IT once (07-22, `watch`)
and continued four times; HDFCBANK/ICICIBANK once (07-20, `watch` at the 0.50% hurdle — the spread
trade's expectancy is −0.20% net of both legs, so it is no longer an experiment) and continued
four times; the volume debunk chains (ANURAS → ALKEM, UNIONBANK; SOBHA → TTML, PVRINOX, …) are one
claim each. Dip cards: four published in thirteen editions (the identical group statistic is
novelty 0.25 and mostly falls below the bar), graded 3 Right · 1 Inconclusive.

By template (independent grades): dip (3, 0, 1) · market_regime (0, 0, 6) · relationship (0, 1, 0)
· surge (6, 0, 2) · theme_cycle (2, 1, 0) · volume_anomaly (1, 2, 0). The market card's no-trade is
Inconclusive six times out of six — at a 0.50% hurdle the equal-weight market's next-session move
never left the band; that is the honest reading of a null result, not a win.

### 3.3 Port fidelity (S1-34, real warehouse, close 2026-07-29) — under the pass-1 data rules, with the pass-2 deltas

Where the statistic is identical the numbers reproduce the prototypes exactly **under the pass-1
conventions** (S1-34 runs with `PASS1_DATA`): IT beat the market on **8 of 15** sessions, **+11.7% vs
+2.6%**, breadth **93%**; any-leader persistence **70% of 3,316**; HDFCBANK/ICICIBANK's gap narrowed
**89% of 412** times (context, not the claim); dip n 12,632 (12,633), surge n 24,727 (24,734),
persistence n 3,316 (3,296) — each delta a data hole, asserted.

Under the **second audit's data rules** (the defaults, pinned in `test_pathfinder_s1_audit2.py`) the
same close reads: dip n **12,368**, hit **50.6%**, median **+0.10%** (was 0.0% — the zero pile),
expectancy −0.20% → no_trade; surge n **24,302**, expectancy −0.73% → reject; IT "leaders like this"
**2,649 sessions = 879 independent cases**, beat-by-hurdle 42.3%, lagged 38.3%, median +0.07%,
expectancy **−0.37%** → watch; mechanical non-overlapping persistence **25.5% of 220** (the 70% is
an overlap artefact and is labelled so); Telecommunication flip **776** / 34.4% beat-by-hurdle /
expectancy −0.72% → reject; anomaly 13,127 cases, lift −1.1 pp → no_trade; pair 120 episodes,
expectancy −0.20% → watch. This is a simulated backfill of base rates, not a traded record.

### 3.4 Determinism and end-to-end verification

**Determinism (C1).** The store was built twice from scratch (`--backfill 12`) into two files by two
independent processes on the second-pass code. Compared table by table with only timestamps
(`computed_at`, `at`, `frozen_at`, `graded_at`, `created_at`, `generated_at`) removed: **47/47
findings identical (28 new + 19 continuations), 25/25 grades identical (verdicts and realised
facts), 13/13 editions identical, 72/72 candidates identical.** Every grade carries
`rule_version = grading_rules@1.2.0+code.d5f3e3fce7ac`. The same property is pinned by A-C1 on the
synthetic universe and on the real 07-29 seal. A second run of the scan for the latest date
(2026-07-29) is refused as already published (append-only), grades nothing new, and the edition
stays `backfilled = 1` — the warehouse's last bar is six weeks old, so no edition can be forward
until the EOD pipeline lands a bar on its own day; the same-day case is pinned by A2-A1.

**Over HTTP** (`uvicorn pathfinder.mock_app:app --port 8010`, real calls):

```
GET /api/pathfinder/feed -> 200 | edition 2026-07-29 | regime NEUTRAL (risk score 46/100, breadth>200DMA 56%) | llm none
backfilled=True | record_label: simulated backfill — generated after the fact; not a forward track record
universe 497 · candidates 7 · published 3 · continued 0 · threshold 0.65
  #1 what_matters_now theme_cycle  Telecommunication  reject    u=0.86 n=776    rotation_reject/5 H=0.5 pending backfilled=True
  #2 what_matters_now surge        PCBL               reject    u=0.71 n=24302  no_trade_call/1   H=0.5 pending backfilled=True
  #3 what_matters_now dip          J&KBANK            no_trade  u=0.66 n=12368  no_trade_call/1   H=0.5 pending backfilled=True
       record (every card): simulated backfill — generated after the fact; not a forward track record
data_source: kanida_falcon.ohlc_daily (NSE EOD; split/bonus-adjusted; demergers and some other corporate actions UNADJUSTED — corporate-action days excluded, see provenance)
disclosures: Survivorship (no delisted name; today's members and sector labels on history) · split/bonus-adjusted only ·
             corporate-action days excluded (table ex-dates + |move| > 30%) · synthetic opens carry no outcome
cost_convention: pf_cost_hurdle_v2: 0.50% round-trip hurdle = 0.30% costs + 0.10% slippage each way (slippage stub; founder inputs pending) ...
facts with n=0: [] | bodies with 'of 0 times': []
SCOREBOARD Right 12 · Wrong 4 · Inconclusive 9 · n=25 (independent) · n_total 25 · continued 19 · forward 0 · backfilled 25 · pending 3
  record_label: simulated backfill — generated after the fact; not a forward track record
  by template: dip (3,0,1) · market_regime (0,0,6) · relationship (0,1,0) · surge (6,0,2) · theme_cycle (2,1,0) · volume_anomaly (1,2,0)
GET /feed?date=2026-07-23 -> 200 · published 1 · continued 3 · backfilled True
  NIFTY 50 (equal weight)  no_trade  graded     inconclusive
  HDFCBANK / ICICIBANK     continue  continued  continues fnd_20260720_pair_hdfcbank_icicibank
  Information Technology   continue  continued  continues fnd_20260722_theme_information_technology
  PVRINOX                  continue  continued  continues fnd_20260721_volume_sobha
GET /feed?date=2026-07-13 theme Realty watch -> RIGHT · realised excess -1.13% · rank_at_horizon 1 (recorded, not judged) · hurdle 50 bps
GET /feed?date=DROP -> 400 · GET /feed?date=1999-01-01 -> 404 · GET /api/pathfinder/loop -> 200 (P0 surface intact)
```

What the feed now shows against the second audit: the backfill label on the edition, every card and
the scoreboard, with **forward 0**; repeated open claims as `continue` → `continued` pointing at
their root, not as new publications and not in `published_count`; **independent n** next to
`n_total` and the folded count; corporate-action days excluded and disclosed, the data source
labelled honestly; the theme decisions judged against the slippage-inclusive hurdle on the graded
metric's conditional base rate; no zero-count statistic anywhere. Four of the 07-29 cards were
computed and **not** published (below 0.65) — in `pf_candidates` with their scores.

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

### Second audit (of commit 69c1f95) — A1–A8, all fixed and pinned in `backend/tests/test_pathfinder_s1_audit2.py`

A second `dev-quant-auditor` pass on the post-fix engine. Every row below fails on 69c1f95 (the
suite does not even collect against it — verified by stashing the engine and running it).

| # | Finding (second auditor) | What changed | Pinned by |
|---|---|---|---|
| **A1** (critical) | **The entire scoreboard is a backfill presented as a track record.** Every `pf_editions.generated_at` is 2026-09-10 for July sessions; `frozen_at <= edition_date` holds for 0 findings; no `backfilled` flag anywhere; the feed and §3.2/§3.3 presented "Right 20 · Wrong 16 · Inconclusive 9 · n=45" as a public record. | `backfilled` on `pf_editions` and `pf_findings` (`is_backfilled`: generated after the session date **in IST** — one day late is a backfill, whatever the seal says); `Scoreboard.forward` / `backfilled` split, `record_label`; `FeedResponse.backfilled` / `record_label`; `GradingState.backfilled` / `record` on every card; the store refuses a superseded schema rather than relabelling. **Forward = 0 today, visibly**; a forward record can only start when the scan runs on the session's own date. §3.2/§3.3 rewritten as a simulated backfill. | A2-A1 (×3), A2-HTTP |
| **A2** | The threshold did not gate (65 of 73 candidates published); dip/surge cards carried the identical group statistic every edition (23 cards) because novelty was keyed on the subject STOCK; Realty ×7, IT ×6, HDFCBANK/ICICIBANK ×3 — ~16 of n=45 grades were re-grades of three calls. | Novelty keyed on the **evidence signature** (statistic + comparison group at display precision for group cards; sector / pair + decision otherwise). A draft whose signature matches an **open** root finding is a `continue`: served after the new findings in `discovery`, `published_count` excludes it, `pf_candidates` records it, it is **never graded** — one grade per claim per non-overlapping horizon. Scoreboard `n` = independent, `n_total`, `continued`. On the rebuilt store: 13 editions, 28 publications and 19 continuations from 72 candidates (was 49 publications); 25 independent grades (§3.2). | A2-A2 (×4), A2-HTTP |
| **A3** | The theme card's traded claim had no evidence: the graded metric's base rate on past leaders (Right 45.2 / Wrong 42.4 / Inconclusive 12.4, median +0.05% at H = 0.30) was never computed or shown; persistence "69.9%" at h=5 is a window-overlap artefact (25.1% at non-overlapping h=15). | Pass-1 C7 verified in place, then extended: `like_this_*` (the graded metric on leaders meeting the card's criteria) gates the call on **expectancy > 0 at the slippage-inclusive hurdle**, median and hit rate, on an **effective n** (`_effective_n`: one case per sector per non-overlapping horizon — 2,649 sessions = 879 cases on 07-29); `persistence` relabelled OVERLAPPING context; `persistence_mechanical` (stride = window, h = 15: **25.5% of 220**) is what the body states. The card is withheld when nothing like it has resolved. | A2-A3 (×4) |
| **A4** | `data_source` claimed "corporate-action back-adjusted"; demergers are not adjusted (CGPOWER −71.7% 2016-03-15, TATACHEM −56.2% 2020-03-04, ABFRL −55.9% 2025-05-22, ADANIENT −41.9% 2015-06-03), nor some split-like prints (JBCHEPHARM −49% 2023-09-18, SPLPETRO −50% 2022-06-07); 23 of 12,633 dip cases were ≤ −30%. | Label: "split/bonus-adjusted; demergers and some other corporate actions UNADJUSTED". Exclusion: split / bonus / demerger / rights ex-dates from `corp_actions` (184 rows, 179 bars hit) plus any \|move\| > 30% as a suspected corporate action (45 bars) are holes like the six glitch bars — not a dip / surge case, not the day's subject, no forward window across them. Disclosed on every provenance (`disclosures`) and counted on every edition. Dip n 12,632 → **12,368** (with A7). | A2-A4 (×4), A2-HTTP |
| **A5** | No slippage anywhere though `Provenance.cost_convention` promised it; the theme `strong` gate was `sec_cum > mkt_cum` with no hurdle while the grade is judged at ±H; the hand-back claimed "the cost hurdle is applied". | `slippage_pct` = **0.10% each way** (FOUNDER INPUT, `KANIDA_PF_SLIPPAGE_PCT`); `cfg.hurdle_pct` = 0.30 + 2 × 0.10 = **0.50%** on every decision, every expectancy, every frozen rule, every provenance; the theme call needs expected excess above it (A3). The rotation flip at 50 bps: 776 / 34.4% / expectancy −0.72% → reject. | A2-A5 (×3) |
| **A6** | Zero sentinels as values: `rank_at_horizon = 0` when unrankable; `c6 = 0.0 if nc else 0.0`, `l_beat = … if nL else 0.0`, `beat = … if nr else 0.0` → "0.0% of 0 times". | `FactSet.add` refuses a statistic over n = 0; the rank fact, the pair width statistic and the persistence facts are withheld when unresolved; the theme / rotation cards are withheld when no base rate exists. | A2-A6 (×4) |
| **A7** | "Typical next-session move 0.0%" on the dip card was the median sitting on a pile of exactly-zero outcomes: 1.5% of bars (4.2% in 2013) have open == close; `data.py` treated only non-positive prices as holes. | A bar whose open equals its close is a **synthetic open** (`_synth_open`, 19,083 bars): no `f{h}` enters at it; `r{h}cc` untouched. Dip: hit 49.7% → **50.6%**, median 0.00% → **+0.10%** (the auditor's recompute), expectancy still < 0 → no_trade. Disclosed on provenance. | A2-A7 (×2) |
| **A8** | Survivorship undisclosed to the user; `--fresh` DELETED the "append-only" store; the anomaly grader ignored the rule horizon (fixed `r5cc`); theme grader vs template market definition. | `FindingProvenance.disclosures` carries the survivorship sentence (no delisted name; today's members and sector labels on history); `--fresh` removed, `--archive-store` renames to `.archived-<timestamp>` and requires `--i-understand-this-archives-the-store`; `r{h}cc` per horizon and the grader reads `rule.horizon_sessions`; the grader's market and the template's `fmkt` are asserted equal on the same date and `market_return.n` counts that market's names (the whole-universe n was the mislabel). | A2-A8 (×3) |

**Pinning discipline.** The pass-1 suites keep the first auditor's numbers reproducible by
running under the pass-1 conventions **explicitly** (`PASS1` = no slippage, synthetic opens on;
`PASS1_DATA` also keeps corporate-action days) — the same device pass 1 used for the 770-flip
reproduction with the glitch guard off. The pass-2 suite pins the defaults.

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

### 5.5 Data quality (rewritten after the second audit)
The warehouse is **split/bonus-adjusted only**. Demergers and some split-like events print as −40% to
−72% one-day moves; the six listing-day glitch bars (D1) and MAZDOCK's zero bars are holes. The engine
now excludes the ex-date bar of every split / bonus / demerger / rights action in the `corp_actions`
table (2020 →) and flags any single-day |move| > 30% as a suspected corporate action (45 bars, which
catches the pre-2020 demergers); an unadjusted 2x or 3x split between 2013 and 2019 with a move inside
30% would still pass. 1.5% of bars carry a synthetic open (open == close) and no outcome enters at
them. Means are winsorised at 1%; medians and hit rates are robust. All of it is on every card's
`provenance.disclosures`.

### 5.7 There is no forward record (second audit A1)
Every edition in the store was generated on 2026-09-10 for July sessions. The scoreboard is a
**simulated backfill** — computed point-in-time, but after the outcomes were knowable in the world —
and every surface says so: `record_label`, `backfilled` on each card and grade, `forward = 0`. It
becomes a forward record only from the first scan run on a session's own date after the EOD bar lands
(§8). Until then nothing here is a track record, and the hand-back does not call it one.

### 5.6 Not built (S2/S3 by design)
No virtual capital, no experiment registry/versions, no promotion gate, no swipeable UI.

## 6. Founder inputs — stubbed with the prototype defaults, flagged

| Input | Stub | Where |
|---|---|---|
| Question-template set | the six prototype-proven templates | `research/library.py` `LIBRARY` |
| Cost hurdle | **0.30% round trip** (prototype) | `config.cost_hurdle_pct` / `KANIDA_PF_COST_HURDLE_PCT` |
| **Slippage** (new, second audit A5) | **0.10% each way — a STUB, flagged**; the hurdle every decision and grade uses is costs + 2 × slippage = **0.50%** | `config.slippage_pct` / `KANIDA_PF_SLIPPAGE_PCT`, `config.hurdle_pct` |
| Corporate-action guard (new, A4) | table types split / bonus / demerger / rights; suspected if \|move\| > **30%** | `config.exclude_corp_actions`, `config.corp_action_ret_guard_pct` |
| Synthetic-open rule (new, A7) | open == close to the tick | `config.exclude_synthetic_opens` |
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
Second pass: `backend/tests/test_pathfinder_s1_audit2.py` (new), `docs/openapi.yaml` regenerated
(`backfilled`, `record_label`, `continued_count`, `continues`, `disclosures`, the scoreboard split,
`GradingStatus.continued`), `docs/TEST_PLAN.md` (the 28 second-audit rows, known gaps 5–6),
`docs/DATA_MODEL.md` (the new columns, archive-not-delete), `var/pathfinder_research.db` rebuilt
(the previous store is `var/pathfinder_research.db.archived-<timestamp>`, not deleted).

## 8. Next step

1. **Founder:** confirm or change the stubs in §6 — the threshold, the hurdle and the new anomaly
   minimum lift are the three that change what gets published.
2. **S2:** the experiment loop on top of this store — a `new_experiment` card becomes an experiment
   with versions and trial counts; virtual capital; expected-vs-actual; promotion only through the
   arena gate. Apply the strict narrate contract to the P1 engine path (re-record its cassette).
3. **S3:** the swipeable feed against `GET /api/pathfinder/feed` — the contract is generated and mocked.
4. **Ops:** schedule `run_pathfinder_scan.py` after each close once the EOD pipeline lands the bar;
   the scan grades the prior editions in the same pass. **That first same-day run is the first forward
   edition** (`backfilled = 0`); everything before it stays labelled a backfill. Never correct a store —
   archive and rebuild it.
5. **Founder:** the slippage stub (0.10% each way) is a guess; confirm it — it moves every hurdle.
