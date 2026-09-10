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
**Third pass (2026-09-10):** a THIRD re-audit of commit f34ecea verified all sixteen earlier fixes
and found six more edge-leaks (N1–N6, §4 "Third audit"): continuations bypassed the threshold and
were served; rounded signatures re-opened one claim and graded it three times as independent; the
pair *watch* was graded as a convergence *call*; `like_this_*` facts carried the overlapping n;
unmeasurable outcomes stayed pending forever; the quantifier ban missed "a handful", "many", "few".
All six fixed and pinned (`backend/tests/test_pathfinder_s1_audit3.py`, 33 rows, 29 of which fail
on f34ecea); the store archived and rebuilt again; §3.2's two false claims ("never padded", "nothing
was graded twice") corrected below.

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

> **What the third audit taught, in one line:** a rule that is honest in the general case can still
> leak at its edges — a continuation exempted from the threshold is padding, a claim keyed on a rounded
> string is several claims, a grading kind chosen by template rather than by decision judges a claim
> the card never made, and a `None` that means "not yet" and a `None` that means "never" must not be
> the same `None`. Each edge is now a contract (`Finding`, `FeedResponse`, `Scoreboard`) rather than a
> convention, and the independent n is recomputed from the grade rows themselves rather than assumed
> from how they were published.

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
| `library.py` | *Third audit:* each template declares `claim_parts` — how many leading parts of its evidence signature are the CLAIM (template, decision, comparison group); the next part is the primary statistic, and `same_claim()` treats a repeat inside `cfg.claim_tolerance_pp` (1 pp) as the same claim (N2); the anomaly signature leads with the lift; the pair card freezes `pair_watch` for a watch and `pair_convergence` for an experiment — the kind follows the decision (N3); `like_this_*` statistics are minted on the effective n, the overlapping count stays a labelled count fact (N4). *Second audit:* every decision and every frozen rule is judged against `cfg.hurdle_pct` = costs + slippage both ways (A5); each card carries an **evidence signature** — the statistic and comparison group for a group card, the sector / pair and decision otherwise (A2); the theme card is judged on the graded metric's conditional base rate with an **effective n** (one case per sector per non-overlapping horizon), states persistence as the non-overlapping mechanical number, and is withheld when no leader like it has resolved (A3/A6). **The question library** — `question → parameters → computation → evidence card`. Six seeded templates porting the two prototypes' math: `market_regime`, `theme_cycle` (+ the laggard→leader rotation flip), `dip`, `surge`, `volume_anomaly`, `relationship`. Every decision-bearing base rate is now the **graded metric** (next open → horizon close), judged against a **named control**, with **expectancy** (winsorised mean net of hurdle) minted and gating next to the median (C4–C7, P5). The decision rules are pure functions (`dip_decision`, `surge_decision`, `carry_decision`, `call_on_excess`) pinned by tests. |
| `grading.py` | *Third audit:* `pair_watch` — Right if the declined spread trade lost more than twice the hurdle, Wrong if it paid more than twice, Inconclusive inside (symmetric, like `theme_watch`) (N3); a rule whose horizon completed inside the seal but whose outcome is a data hole (synthetic open at entry, corporate-action / glitch bar inside the window, more than a fifth of a sector unresolved) returns a **`void`** grade carrying the reason — not Right / Wrong / Inconclusive, never counted, never pending forever; `None` now means only "not yet due" (N5). *Second audit:* rank at the horizon is withheld, never zero (A6); the anomaly kind reads the rule's horizon (A8); the hurdle includes slippage (A5). Per-type Right / Wrong / Inconclusive rules (`directional_call`, `no_trade_call`, `theme_call`, `theme_watch`, **`rotation_reject`**, `anomaly_move`, `pair_convergence`) built **at publication** and stored with the finding. Every verdict is **symmetric in the graded metric** (C2/C3); rank at the horizon is recorded as a fact, never judged. `rule_version` = `grading_rules@1.1.0+code.<sha256 of grading.py>`; a spec must be complete when frozen and the evaluator never reads live config (P2). The theme kinds define "the market" exactly as the templates do (sectors with ≥ min_names names). |
| `ranking.py` | Usefulness = 0.30 evidence strength + 0.20 novelty + 0.30 trader relevance + 0.20 magnitude; publication threshold 0.65. No quota, no padding. *Second audit A2:* novelty is keyed on the **evidence signature**, not the day's subject — the same group statistic under a new stock is the same claim (0.25). Evidence strength takes the template's `evidence_z`, which is now the z of the base rate against its control (the lift for the anomaly, the coin flip for hit rates, on independent episodes for the pair). |
| `narrate.py` | The model's two permitted jobs: `classify` publish/hold on a card that already cleared the threshold; `narrate` the **body** from the card's fact table under the **engine's headline** (the model's headline is discarded — the decision is not the model's to restate). The strict feed contract (`enforce_narrate(strict=True)`) is re-applied here so no provider can skip it (P1). Without a provider the engine's own digit-free template narrates and the card says `produced_by = "engine"`. |
| `store.py` | *Third audit:* schema 3 (a v2 store is refused on open, archived, rebuilt); `novelty()` / `open_root()` match on the claim within the tolerance (N2); `scoreboard()` derives `n` from the grade rows as one grade per claim per non-overlapping horizon — a grade of a claim whose earlier grade (independent or itself folded) was still measuring at this edition date is folded, `regraded = n_total − n` (N2); `void` grade rows are counted apart (`void`) and never in n (N5). *Second audit:* `backfilled` on every edition and finding, the scoreboard split `forward` / `backfilled` with `n` = independent grades, `n_total`, `continued` (A1/A2); `continues` on a finding that repeats an open claim — served, not counted, never graded on its own (A2); a store under a superseded schema is refused on open, and the script archives rather than deletes (A8). Append-only SQLite (`pf_editions`, `pf_findings`, `pf_candidates`, `pf_grades`, `pf_scoreboard`); triggers reject UPDATE/DELETE; one grade per finding, ever; rejected candidates kept with their scores. `pf_editions.engine_version` carries the research code hash (C1); `scoreboard(X).pending` is "pending as of X" (P4). A store written by superseded code is deleted and rebuilt — it cannot be corrected. |
| `scan.py` | seal → regime → every library question → score → **threshold FIRST** → continuation-or-new → (model veto) → narrate → publish → **grade whatever completed its horizon** (void when unmeasurable) → snapshot the scoreboard. An edition is never recomputed. *Third audit N1:* a sub-threshold draft that repeats an open claim is a `pf_candidates` row ("below usefulness threshold (continues <root>)") and is NOT served — before this, `root is not None` was tested before the threshold and fifteen of nineteen continuations in the store were served below 0.65. *Second audit:* `is_backfilled(edition, computed_at)` — generated after the session date in IST is a backfill (A1); a draft whose signature matches an OPEN root finding becomes a `continue` after the new findings, outside "what matters now" (A2); every provenance carries `cfg.data_disclosures` (A4/A7/A8). |
| `llm/contracts.py` | The gateway contract, plus the strict feed mode: number words and frequency quantifiers ("nine in ten", "half", "doubled", "most of the time", "usually") must sit in a sentence that cites a fact; at least one fact reference; no number word in a headline. *Third audit N6:* the quantifier list now also bans "a handful", "few", "many", "several", "a couple", "the bulk", "nearly every", "most", "some", "plenty", "a majority", "almost all", "hardly any" (and siblings) — "A handful of cases bounced, and many did not." was accepted with no fact. |

**Contract (`backend/pathfinder/schemas.py`, `docs/openapi.yaml`):** `EvidenceLevel`,
`Provenance.level`, `FindingProvenance`, `GradingRule`, `GradingState`, `Narrative` (digit-free for
*both* authors), `UsefulnessScore`, `Finding`, `Scoreboard`, `FeedResponse`,
`GET /api/pathfinder/feed?date=` — clarity-first: `what_matters_now` (first ≤3) then `discoveries`.
Post-audit additions: `SampleFlag.not_applicable`, `GradingKind.rotation_reject`. **Second audit:**
`FeedResponse.backfilled` / `record_label` / `continued_count`; `Finding.backfilled` / `continues`;
`GradingState.backfilled` / `record` / `continues` and `GradingStatus.continued`;
`FindingProvenance.disclosures`; `Scoreboard.forward` / `backfilled` / `n_total` / `n_independent` /
`continued` / `record_label`; the labels `BACKFILL_LABEL` ("simulated backfill — generated after the
fact; not a forward track record") and `FORWARD_LABEL`. **Third audit:** `GradingKind.pair_watch`;
`Verdict.void`, `GradingStatus.void`, `GradingState.void_reason`; `Scoreboard.regraded` / `void`;
`KINDS_FOR_DECISION` — the `Finding` contract refuses a decision frozen under a kind that judges a
different claim (a watch under a call kind); the `Finding` threshold check no longer exempts
continuations and `FeedResponse` refuses any served card below the edition's threshold.

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
python -m pytest backend/tests/test_pathfinder_s1_audit3.py -q   # 33 third-audit regressions (N1–N6 + D2 + HTTP)
python -m pytest backend/tests -q                                # the whole backend suite
python scripts/gen_openapi.py --check                            # contract in sync
```

## 3. Results / evidence

### 3.1 Tests

`backend/tests/test_pathfinder_s1.py`: **34 passed** (S1-01…S1-34). `backend/tests/test_pathfinder_s1_audit.py`:
**39 passed** (A-C1…A-D1 + the real-warehouse rows, `docs/TEST_PLAN.md`). `backend/tests/test_pathfinder_s1_audit2.py`:
**28 passed** (the second audit, A2-A1…A2-A8 + HTTP; the file does not collect against the pre-fix
engine). `backend/tests/test_pathfinder_s1_audit3.py`: **33 passed** (the third audit, N1–N6 + D2 + HTTP;
the file collects on f34ecea and **29 of 33 rows fail there** — verified in a `git worktree` at that
commit; the four that pass are three quantifier phrases the old list already caught and a
plain-prose sanity row). **134 passed** across the four S1 files (~85 s with the warehouse). P0 and
P1 unchanged and passing (`gen_openapi.py --check` in sync after the contract additions). The
pass-1 suites pin the pass-1 conventions explicitly (`PASS1` / `PASS1_DATA`); the pass-2 suite
pins the defaults (hurdle 0.50% incl. slippage, corporate-action days and synthetic opens
excluded). Three earlier pins were superseded by N4 and updated in place: `like_this_*`
statistics now carry `n` = the effective count (A-C7 synthetic; real-C7 879, not 2,655; A2-A3
879, not 2,649).

### 3.2 The real run — thirteen editions, 2026-07-13 → 2026-07-29: a SIMULATED BACKFILL, not a forward track record

**Read this first.** Every edition below was generated on **2026-09-10** for sessions in July. Each
one is computed from bars ≤ its date only (the seal holds), but the outcomes were knowable in the
world when the cards were written. That makes the whole table a **simulated backfill**; the feed,
every card and the scoreboard say so (`record_label`, `backfilled = 1`, `forward = 0`). Nothing here
is a track record. The forward record starts at zero and begins with the first scan run on a
session's own date after the EOD bar lands (§8).

**Provenance of this table:** `var/pathfinder_research.db`, built by `run_pathfinder_scan.py
--archive-store --i-understand-this-archives-the-store --backfill 12` on the third-pass code
(`engine_version` = `pathfinder_research@1.3.0+code.9dbfc17c712d`, `rule_version` =
`grading_rules@1.3.0+code.fd36d023094d`, store schema 3, `claim_tolerance_pp = 1.0` in every
edition's params). The pass-2 store is `var/pathfinder_research.db.archived-20260910T100104` (pass-1:
`…T091954`; two refused partial builds from the D2 fault, §5.8: `…T100402`, `…T101535`) — archived,
not deleted (A8); their numbers are superseded and not comparable. Data rules on this build: 179
corporate-action bars + 45 suspected + 6 glitch bars excluded; 19,083 synthetic opens carry no
outcome. Threshold 0.65, hurdle 0.50%.

**What the second-pass version of this section claimed, and what was true.** It said "never padded;
continuations are not publications; n is independent" and "`n_total = n`: nothing was graded
twice". Neither held. Fifteen of the nineteen continuations it served were **below the 0.65
threshold** (min 0.3952) — the 07-23 feed served one publishable card and three sub-threshold ones
(0.649, 0.558, 0.535) — because a repeat of an open claim was recognised *before* the threshold was
applied and the `Finding` contract exempted it. And the volume-anomaly debunk was **graded three
times as three independent claims** (ANURAS 07-13 → TORNTPHARM 07-16 → SOBHA 07-21, each inside
the previous horizon, signatures `…|-1.20` / `…|-1.10` / `…|-1.10`) because the claim was keyed on a
rounded string; the scoreboard showed `volume_anomaly (1, 2, 0)` with `n_total == n`. What is now
enforced: the threshold gates every served card, continuation or not (`Finding`, `FeedResponse`,
the scan order); the claim is (template, decision, comparison group) with the primary statistic
inside 1 pp; and `n` is recomputed from the grade rows as one grade per claim per non-overlapping
horizon (`regraded = n_total − n` on the served scoreboard) rather than assumed from publication.

| Edition | Candidates | Published (new) | Continued (served) | Below threshold (of which repeats of an open claim) | Graded that day | Scoreboard after (independent n; pending = as of that date) |
|---|---|---|---|---|---|---|
| 07-13 | 4 | 4 | 0 | 0 | — | pending 4 |
| 07-14 | 5 | 2 | 1 | 2 (1) | 2 | R1 W0 I1 n=2 · pending 4 |
| 07-15 | 5 | 2 | 1 | 2 (1) | 2 | R1 W0 I3 n=4 · pending 4 |
| 07-16 | 5 | 1 | 0 | 4 (2) | 2 | R3 W0 I3 n=6 · pending 3 |
| 07-17 | 5 | 1 | 0 | 4 (2) | 1 | R4 W0 I3 n=7 · pending 3 |
| 07-20 | 6 | 2 | 0 | 4 (0) | 3 | R6 W1 I3 n=10 · pending 2 |
| 07-21 | 6 | 2 | 1 | 3 (0) | 0 | R6 W1 I3 n=10 · pending 4 |
| 07-22 | 6 | 4 | 1 | 1 (1) | 1 | R7 W1 I3 n=11 · pending 7 |
| 07-23 | 6 | 1 | 0 | 5 (3) | 3 | R9 W1 I4 n=14 · pending 5 |
| 07-24 | 6 | **0** | 0 | 6 (3) | 1 | R9 W1 I5 n=15 · pending 4 |
| 07-27 | 5 | 1 | 0 | 4 (2) | 2 | R11 W1 I5 n=17 · pending 3 |
| 07-28 | 6 | 1 | 0 | 5 (1) | 2 | R12 W1 I6 n=19 · pending 2 |
| 07-29 | 7 | 4 | 0 | 3 (0) | 2 | **Right 12 · Wrong 2 · Inconclusive 7 · n=21 (independent) · n_total 21 · regraded 0 · void 0 · forward 0 · backfilled 21** · 4 continuations served · pending 4 |

**25 new findings** published out of 72 candidates (pass 2: 28 new + 19 continuations served; pass
1: 49), **4 continuations served** (every one ≥ 0.65 on its own novelty-discounted score: Realty
07-14/07-15, HDFCBANK/ICICIBANK 07-21/07-22), **16 repeats of an open claim withheld as
sub-threshold candidates** (on the record in `pf_candidates` with their scores and the root they
would have continued). **07-24 published nothing** — a valid edition under addendum 1. 21 independent
grades from 21 grade rows: with continuation detection on the claim rather than the string,
TORNTPHARM 07-16 is a (sub-threshold) continuation of ANURAS and SOBHA 07-21 is a new root only
because ANURAS's horizon completed on 07-20 — no re-grade could form, so `regraded = 0` here; the
fold is exercised by A3-N2 on a store where detection is disabled. The HDFCBANK/ICICIBANK watch of
07-20 is graded **Right** by `pair_watch` (spread trade −2.26%: not worth calling) — it was Wrong
under `pair_convergence`, a claim the card did not make. Dip cards: three published, graded 2 Right ·
1 Inconclusive.

By template (independent grades): dip (2, 0, 1) · market_regime (0, 0, 4) · relationship (1, 0, 0)
· surge (6, 0, 2) · theme_cycle (2, 1, 0) · volume_anomaly (1, 1, 0). The market card's no-trade is
Inconclusive four times out of four — at a 0.50% hurdle the equal-weight market's next-session move
never left the band; that is the honest reading of a null result, not a win. The volume debunk is
now graded twice on two non-overlapping horizons (ANURAS 07-13 Wrong, SOBHA 07-21 Right) instead of
three times on overlapping ones.

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
independent processes on the third-pass code, **sequentially** (see §5.8 for why the first attempt,
run in parallel and then alone, was refused by the C1 guard — the numexpr fault, now fixed).
Compared table by table with only timestamps (`computed_at`, `at`, `frozen_at`, `graded_at`,
`created_at`, `generated_at`) removed: **13/13 editions, 29/29 findings (25 new + 4 continuations),
21/21 grades (verdicts and realised facts), 72/72 candidates identical.** Every grade carries
`rule_version = grading_rules@1.3.0+code.fd36d023094d`. The same property is pinned by A-C1 on the
synthetic universe and on the real 07-29 seal; the numexpr/bottleneck backends are pinned off by
A3-D2. Build time 1 m 45 s per store.

**Over HTTP** (`uvicorn pathfinder.mock_app:app --port 8010`, real calls against the rebuilt store):

```
GET /api/pathfinder/feed?date=2026-07-23 -> 200 | backfilled=True | threshold 0.65 | published 1 · continued 0 · served 1
  #1 what_matters_now market_regime  NIFTY 50 (equal weight)  no_trade  u=0.650  no_trade_call/1  graded inconclusive
     (pass 2 served this card plus three continuations at 0.649 / 0.558 / 0.535 — now pf_candidates rows
      "below usefulness threshold (continues fnd_20260720_pair_hdfcbank_icicibank / …theme_information_technology / …volume_sobha)")
  SCOREBOARD R9 W1 I4 n=14 · n_independent 14 · n_total 14 · regraded 0 · void 0 · continued 4 · pending 5 · forward 0 · backfilled 14

GET /api/pathfinder/feed -> 200 | edition 2026-07-29 | regime NEUTRAL | backfilled=True | published 4 · continued 0 · served 4
  #1 what_matters_now theme_cycle     Telecommunication  reject    u=0.86  rotation_reject/5  pending
  #2 what_matters_now surge           PCBL               reject    u=0.71  no_trade_call/1    pending
  #3 what_matters_now volume_anomaly  DCMSHRIRAM         no_trade  u=0.69  no_trade_call/5    pending
  #4 discovery        dip             J&KBANK            no_trade  u=0.66  no_trade_call/1    pending
  record_label (edition, every card, scoreboard): simulated backfill — generated after the fact; not a forward track record
  SCOREBOARD Right 12 · Wrong 2 · Inconclusive 7 · n=21 (independent) · n_total 21 · regraded 0 · void 0 · continued 4 · forward 0 · backfilled 21 · pending 4
  by template: dip (2,0,1) · market_regime (0,0,4) · relationship (1,0,0) · surge (6,0,2) · theme_cycle (2,1,0) · volume_anomaly (1,1,0)

GET /feed?date=2026-07-20 HDFCBANK / ICICIBANK watch -> kind pair_watch · verdict RIGHT · spread_trade -2.2575%
  rule.right: "Right if it was not worth calling: the spread trade lost more than twice the cost hurdle (two legs)"
GET /feed?date=DROP -> 400 · GET /feed?date=1999-01-01 -> 404
```

No served card is below the threshold on any edition (asserted over every `pf_findings` row and by
the `FeedResponse` contract); the scoreboard carries `regraded` and `void` next to `n_total`; the
backfill labels are intact on the edition, every card and the scoreboard; the pair watch is judged
on the claim it made. The 07-29 edition computed seven cards and published four; the three below
0.65 (the market card at 0.64, IT at 0.63, the pair at 0.59) are in `pf_candidates` with their
scores.

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

### Third audit (re-audit of commit f34ecea) — N1–N6, all fixed and pinned in `backend/tests/test_pathfinder_s1_audit3.py`

A third `dev-quant-auditor` pass verified all sixteen earlier fixes in place and found three
CONFIRMED edge-leaks and three PLAUSIBLE ones. All six fixed (plus D2, found while rebuilding); 29 of the suite's 33 rows fail on
f34ecea (verified in a `git worktree` at that commit — the file collects there because the new
symbols are imported inside the rows).

| # | Finding (third auditor) | What changed | Pinned by |
|---|---|---|---|
| **N1** (confirmed) | Continuations bypassed the usefulness threshold: `scan.py` tested `root is not None` before `s.total < s.threshold`, and `Finding` exempted continuations from the threshold invariant. In the store 15 of 19 continuations were below 0.65 (min 0.3952); the 07-23 feed served one publishable card and three sub-threshold ones (0.649 / 0.558 / 0.535). §3.2 said "never padded". | The threshold is applied **first**, to every draft. A sub-threshold repeat of an open claim goes to `pf_candidates` (`decision = continue`, "below usefulness threshold (continues <root>)") and is not served. `Finding` refuses any card below its threshold, continuation or not; `FeedResponse` refuses a served card below the edition's threshold. | A3-N1 (×2), A3-HTTP |
| **N2** (confirmed) | Rounded signatures re-opened the same claim: ANURAS 07-13 `…\|-1.20` (h = 5, due 07-20) → TORNTPHARM 07-16 `…\|-1.10` published NEW and graded while ANURAS was open → SOBHA 07-21 `…\|-1.10` NEW while TORNTPHARM was open. One null claim, three overlapping grades (W, W, R), reported as `volume_anomaly (1,2,0)` with `n_total == n`. §3.2 said "nothing was graded twice". | Each template declares `claim_parts`: the claim is (template, decision, comparison group) and the next signature part is the primary statistic (hit rate; the anomaly's lift, now leading its signature); `same_claim()` folds a repeat inside `cfg.claim_tolerance_pp` = **1 pp**. `novelty()` / `open_root()` match on the claim. `scoreboard()` computes `n` from the grade rows: one grade per claim per non-overlapping horizon — a grade whose claim was still being measured by an earlier grade at its edition date is folded (`regraded = n_total − n`), whatever the store holds. The auditor's chain yields ONE grade. | A3-N2 (×4), A3-HTTP |
| **N3** (confirmed) | The pair `watch` was graded by `pair_convergence` — a claim the card did not make. `fnd_20260720_pair_hdfcbank_icicibank` said "a watch, not a call" and was graded WRONG because the spread lost 2.26% (had it converged the non-call would have been RIGHT — flattering). | The kind follows the **decision**: `GradingKind.pair_watch` — Right if the declined spread trade lost more than twice the hurdle, Wrong if it paid more than twice, Inconclusive inside — listed in `RELATIONSHIP.grading_kinds`; `KINDS_FOR_DECISION` in the `Finding` contract makes a watch under a call kind unrepresentable. On the rebuilt store the 07-20 watch is graded **Right** by `pair_watch`. | A3-N3 (×2), real-N3 (HDFCBANK/ICICIBANK sealed 07-20: −2.26% → Right) |
| **N4** (plausible) | `like_this_*` statistics were minted with `n` = the overlapping session count (2,645) while the decision, the z and the provenance used the effective 878; the fact-level `sample_flag` derived from the inflated n. | Minted on `nL_eff`; `like_this_cases` stays a count fact labelled overlapping. Three earlier pins updated (§3.1). | A3-N4 |
| **N5** (plausible) | A permanently unresolvable outcome (synthetic next open, corporate action inside the window, > 20% of a sector's members NaN) returned `None` from `evaluate` forever → the finding stayed `pending` indefinitely. | When the horizon has completed inside the seal and the outcome is a hole, `evaluate` returns a **`void`** grade with the reason (`Verdict.void`, `GradingStatus.void`, `void_reason`); stored in `pf_grades` (CHECK extended), counted in `Scoreboard.void`, excluded from `n` / `n_total` / `pending`. `None` now means only "not yet due". | A3-N5 (×2) |
| **N6** (plausible) | `QUANTIFIER_RE` missed "a handful", "few", "many", "several", "a couple", "the bulk", "nearly every"; the probe "A handful of cases bounced, and many did not." was accepted without a fact ref. | The list extended with those and "most", "some", "plenty", "a majority", "almost all", "hardly any", "numerous", "virtually every", "the lion's share" … — each needs a fact reference in its sentence. | A3-N6 (×19) |

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
**Third-pass correction:** the `big_move = 0.0%` in that store was very probably not superseded
code at all but the intermittent numexpr fault found and fixed in the third pass (§5.8, D2) — the
signature is identical. The regime reading may still have been stale code; the zero share was
the library fault.

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

### 5.8 D2 — the engine was intermittently NON-deterministic: pandas' numexpr backend zeroed a forward column (third pass; found, fixed, pinned)
While rebuilding for the third pass, three of four full rebuilds were refused by the C1
degenerate-share guard — on different seals and different facts each time
(`volume_unionbank_big_move = 0.0% over 13,100` at 07-15; `theme_realty_like_this_beat = 0.0%
over 877` at 07-15 and again at 07-17), while the fourth passed all thirteen editions, and every
refused seal recomputed correctly on its own. A probe that recomputed the theme and anomaly cards
for every seal four times in one process found it: **with numexpr enabled, one pass produced an
`f5` column with `abs().mean() == 0.0` and seven more non-NaN rows than every other pass** — the
1.25M-row `_c5 / next_open - 1` arithmetic in `data.sealed()` is routed by pandas through numexpr
(2.14.1, 12 threads, under pandas 2.3.3 / numpy 2.3.5), which intermittently returned garbage on
this host. **With numexpr disabled the same probe had zero mismatches** and the two rebuilds in
§3.4 are byte-identical. Fix: `research/config.py` turns `compute.use_numexpr` and
`compute.use_bottleneck` OFF for any process that imports the engine; pinned by A3-D2.
Two consequences: (1) the guard did exactly what it exists for — a zero share over thousands of
cases was refused at mint time and no such edition ever reached a store; (2) **§5.1 is corrected**
— the first pass's smoke store (`big_move = 0.0%` over 13,119 cases) has exactly this signature
and was almost certainly this bug, not superseded code; the code-hash stamping, the determinism
rows and the guard remain the right defences, and they are what caught it this time. The
partial stores from the refused builds were archived, not deleted (§7).

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
| **Claim tolerance** (new, third audit N2) | **1 percentage point** on the primary statistic (hit rate / lift): a repeat inside it is the same claim | `config.claim_tolerance_pp` (recorded in every edition's `params_json`) |
| Per-type grading rules | the eight kinds above (incl. `pair_watch`), ±hurdle band, pair = 2× | `grading.build_rule` |
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
Third pass: `backend/tests/test_pathfinder_s1_audit3.py` (new, 33 rows), `docs/openapi.yaml`
regenerated (`pair_watch`, `void`, `void_reason`, `regraded`, `Scoreboard.void`),
`backend/pathfinder/fixtures/*.json` refreshed, `docs/TEST_PLAN.md` (the third-audit rows, the
three superseded pins), `docs/DATA_MODEL.md` (void verdicts, the independence fold, schema 3),
`var/pathfinder_research.db` archived and rebuilt again (schema 3; the pass-2 store is
`var/pathfinder_research.db.archived-20260910T100104`).

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
