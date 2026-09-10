# Hand-back — Pathfinder Session S3: THE SWIPEABLE FEED

**Date:** 2026-09-10 (IST) · **Branch:** `feat/product-build` (on `187c488`) · **Brief:** `docs/sessions/PATHFINDER_S3_FEED.md`
under the LOCKED spec `docs/sessions/PATHFINDER.md`, consuming the S1 feed (`docs/handbacks/PF-S1.md`) and the S2
experiment loop (`docs/handbacks/PF-S2.md`).
**Status:** built to the brief's "done when"; every gate green (§3); driven by hand in a browser at 375 px dark
and at desktop width (§3.3); committed locally on `feat/product-build`, not pushed. **Nothing in `backend/` was
touched** — the S2 re-audit was rebuilding the registry concurrently (§5.1) and every contract problem found is
documented in §4 rather than fixed.

> **The one thing to read first.** The Pathfinder surface is no longer a scrolling list of cards. It is a
> full-screen, vertically swipeable **story feed** on the real S1/S2 contract: *what matters now* in the first
> two or three screens, then discoveries, then the experiments — and on the real data that means the honest
> story **"Nothing cleared the gate"**, with the counted trials, the declined count and the closest variant
> and the one gate it failed — then the scoreboard with **forward n = 0 · backfilled n = 127** and the backfill
> label three ways, then what is being tested next. A four-finding day is four stories. The 24 Jul 2026
> edition, which published nothing, renders as one honest story and the scoreboard. Nothing is padded,
> nothing is a raw token, nothing is a synthesised number, nothing is an entry, target, stop or execution.

> **The mockup PNGs were still absent.** `docs/design/mockups/` holds only `PLACE_MOCKUPS_HERE.md`. The feed
> was built from the written Design DNA (`docs/FRONTEND_SPEC.md`) and the elevation direction ("the mockup
> is the baseline, Instagram is the bar"). **Pixel-matching awaits the founder's images** (§6).

---

## 1. What was built

All in `kanida-app/` (Expo SDK 57, expo-router, React Native + react-native-web — one codebase, no platform fork).
The agent-agnostic shell is kept (tabs, `AgentHeader`, tokens, `facts.tsx`, `honesty.ts`, the single base-URL
seam); the P0-shaped Pathfinder screens and components are removed (§1.4).

### 1.1 The feed — `/pathfinder` (`?date=`, `?story=`)

`src/components/feed/StoryPager.tsx` + `StorySlide.tsx`, composed by `src/lib/stories.ts` (pure, unit-tested):

| Story | Rendered from | What the screen shows |
|---|---|---|
| **Finding** (theme in play · stock behaviour · volume anomaly · relationship · market; `· Debunk` when the decision is `no_trade` / `reject`) | `what_matters_now[]` then `discoveries[]` | kicker (*What matters now · 1 of 3* / *Discovery n*) · subject + kind · the engine's digit-free headline in hero type · 2–4 **key-fact tiles** (`key_fact_refs`, numeric only, each tappable → provenance sheet) · the lede (first sentences of the narrative, every figure a tappable fact) · the decision pill + its reason · the grading line (pending with the frozen horizon, or the verdict) with the record chip · provenance chips (level · n · period · regime · hurdle) · *Evidence & provenance →* |
| **Experiment** (the 7 beats) | `experiment_cards[]` on the edition | kicker + state pill (TESTING / BURIED / PROPOSED) · theme · the seven beat headlines in order (*I noticed → I tested → history showed → I decided to test it with virtual money → what happened → what I learned → what I'm testing next*) · Right · Wrong · Inconclusive · n · void · the comparison (stronger / weaker / failed …) · versions · trials counted · periods graded · the record label verbatim · *Versions, trials, expected vs actual →* |
| **Nothing cleared the gate** | `/experiments` when the edition has no card | the counted trials, findings researched, candidates declined (each an `EngineFigure`, §1.3) · **the closest variant** (rule text labelled *a research hypothesis as the engine defines it — not an instruction*), its expectancy, and the gate it failed verbatim (`history_placebo=0.093 vs 0.05`) · the record label · *Why nothing opened →*. If the registry cannot be reached it says so, names the base URL, and shows nothing in its place. |
| **The record** (verdict / scoreboard) | `scoreboard` + `experiments_scoreboard` | Right · Wrong · Inconclusive as three tallies · n (independent) · pending · void · **the record label three ways** (chrome chip, story chip, the engine's sentence) · **Forward n = 0 / Backfilled n = 127** tiles · by question type with the n-flag · the experiment line · *Full scoreboard →* |
| **What I'm testing next** | the cards' `follow_up_questions[]` + pending horizons | the follow-ups (each tap opens its card's depth) · verdicts still pending with the frozen horizon |
| **Nothing published** | an edition with zero findings | "Nothing cleared the usefulness threshold on 24 Jul 2026", the candidate count and the universe size, the regime — *that is a valid edition; Pathfinder never pads a quiet day* |

Interaction: swipe **up** for the next story (plain `ScrollView` with `pagingEnabled` → CSS scroll-snap on web,
native paging on iOS/Android); **horizontal swipe, ArrowRight / Enter, or the story's own button** for depth;
**Back returns to the same story** (the pager remembers its position per edition; depth links carry `?date=`).
Progress rail on top: one segment per story, filled to the current one, each segment tappable; `n / N`;
edition date · regime · record chip. On a wide screen the same pager sits in a phone-width column with an
index rail on the left, arrow buttons on the right and keyboard paging (↑ ↓ j k Space; → for depth). Only the
current story and its neighbours are mounted. Each story fades and settles in as it becomes active (calm,
360 ms). Loading skeletons; an error state that names the base URL.

### 1.2 Depth

| Route | What it shows |
|---|---|
| `/pathfinder/story/[id]?date=` | the full narrative with attribution (engine-narrated / model) · the decision and its meaning · **the evidence**: level (in words), comparison group, period, regime, cost hurdle + convention, data source, universe, `computed_by`, point-in-time as-of, every disclosure · **every fact** as a tappable row with n / flag / level · **the rule frozen at publication** (kind, horizon, hurdle, what is measured, Right / Wrong / Inconclusive legs, `frozen_at`, `rule_version`) · **the grade** (status, verdict, graded-at, data-as-of, void reason, the realised facts, the record label) · related stocks (labelled research items) · follow-up questions · the usefulness score against the threshold · the disclosure |
| `/pathfinder/experiment/[id]` | state, score, comparison, record label, opened / news editions · the question and the rule under test (labelled a research definition) · evidence chips · the seven beats in full · **post-mortem first when buried** (cause, summary, what we kept) · **the graduation proposal** when one exists (status, every gate with value vs bar, the human-gate sentence) · **versions** `#id → v1 → v2 → v3` (rule, conditions, horizon, change, why, level, validation, trials for this version, **the frozen expectation** — expectancy, 2× slippage, median, hit rate *supporting*, signal days, trailing window *labelled a persistence check*, edge vs baseline, placebo p and draws, cluster t, discovery window at 2× — with its own provenance strip; **every period** with mean net / trade vs expected, book return with its drawdowns, closed · open · unresolved, signals taken / seen, **expected vs actual** (digit-free statement with facts), **the learning** (level, candidate revisions evaluated / cleared, adopted rule, next action), the grader version, the record label) · **every counted trial** (the p-hacking ledger) · the worth-testing gates · **the basket** (constituents withheld pending RA review unless the API marks them reviewed) · the change-log · disclosure |
| `/pathfinder/experiments` | the registry losers first (buried → testing → proposed), then **researched-and-declined** with the closest variant and the failed gate, then the declined-before-research grouped by the engine's reason with counts |
| `/pathfinder/scoreboard?date=` | the findings board (Right · Wrong · Inconclusive · n independent; forward vs backfilled; pending / void / regraded / continued / all grade rows; by question type) and the experiments board (graded periods, void, pending, forward/backfilled, testing / buried / proposed / not opened / trials, by family) |
| `/` (Home) | the Pathfinder card now reads the feed: edition, record chip, the first headline, stories today, Right · Wrong · Inconclusive, n and forward n, the record label verbatim |

### 1.3 Honesty, enforced in the UI

- **Every number is a computed fact with tappable provenance.** `facts.tsx` resolves `{{fact:…}}` into a
  tappable figure; the sheet shows the value, label, unit, n and flag, evidence level, window, point-in-time
  as-of, data source, universe, cost convention, `computed_by`, computed-at, note. An unresolvable id renders
  a visible `[missing figure]`, never the raw token. A parameter / single observation (`not_applicable`) says
  it is not a statistic. A greyed sample stays grey wherever it appears (tile, row, inline).
- **`EngineFigure`** — a figure the engine reports *without* a fact card (a scoreboard tally, the registry's
  counts, a declined variant's expectancy): tappable, and its sheet says which engine reported it, as of
  when, and that **no n / window card was served** for it. It never pretends to be a Fact and the app never
  invents a provenance (§4.2 asks the contract to serve Facts here).
- **The record label** (`BACKFILL_LABEL` / `FORWARD_LABEL`) is rendered verbatim on the edition chrome, the
  scoreboard story, the full scoreboard, every graded card's grading line (`grading.record`), every experiment
  card, every period. Amber for a backfill so it can never pass for a track record at a glance.
- **No `+` on a drawdown** (`pctAbs`); a loss always shows a true minus; small samples labelled from the
  server's flag; **no sort control anywhere**; died / buried / Wrong / void rendered as fully as Right.
- **No entry / target / stop / execution.** Two client-side lints in `honesty.ts`: experiment surfaces use
  the server's `PUBLIC_CARD_BANNED_RE` word list verbatim; S1 research cards use the narrower *shape-of-an-
  order* list (entry · exit price · target · stop-loss · execute · place an order · take/book profit),
  because the S1 engine's own debunks legitimately say "history says do not buy the dip" (§4.3). A line that
  fails is replaced by a visible `[withheld — this line would read like a trade instruction]`. Rule texts
  are labelled *a research hypothesis as the engine defines it — not an instruction*. Constituents appear
  only when `basket.constituents_visibility = in_app_ra_reviewed`.
- **Never padded.** `buildStories` renders exactly what the engine published; the only stories the client
  adds are the honest ones (nothing published / nothing cleared the gate / the record / what's next), each
  built from engine counts and engine text, and the "next" story is omitted when there is nothing to say.

### 1.4 Removed / reshaped

`src/app/pathfinder/(agent)/{overview,experiments,learnings,lab}.tsx`, `ExperimentCard`, `Pipeline`, `Ledger`,
`Performance`, `Evidence`, `Story`, `Sparkline` — all P0/P1-shaped; the research source serves neither `/loop`
nor `/learnings` (§4.1). `src/api/types.ts` is now the S1/S2 mirror; `client.ts` exposes `feed(date)`,
`experiments()`, `experiment(id)` with a per-session memo (an edition is append-only, so the same edition is
the same bytes; `invalidate()` on manual refresh). `tests/contract.test.ts` is the S3 suite against the live
research API; the P0 rows moved to `tests/contract.p0.test.ts` (`npm run test:contract:p0`, mock source only).
Design tokens gained `hero`, `lede`, `metricLg`.

## 2. How to run

```bash
# 1. the research API (S1 feed + S2 experiments) — the app's default base URL is http://127.0.0.1:8010
cd backend && KANIDA_PATHFINDER_SOURCE=research uvicorn pathfinder.mock_app:app --port 8010
#    PowerShell:  $env:KANIDA_PATHFINDER_SOURCE="research"; python -m uvicorn pathfinder.mock_app:app --port 8010
#    (uvicorn / fastapi / pandas: C:\Users\SPS\anaconda3\python.exe on this machine)

# 2. the app — one codebase
cd kanida-app && npm install && npm start
#    w = web · i = iOS · a = Android · or scan the QR in Expo Go (the client infers the Metro host:8010)
#    EXPO_PUBLIC_API_BASE_URL=<engine> npm start     # the only seam

# 3. gates
npm run typecheck && npx expo lint && npm test          # 33 unit rows, no server
npm run test:contract                                   # 16 rows against the LIVE research API
npx expo export --platform all                          # iOS + Android + web
```

Deep links: `/pathfinder?date=2026-07-24` (the empty edition), `/pathfinder?story=<id>` (land on a story),
`/pathfinder/story/fnd_20260722_theme_information_technology?date=2026-07-22` (a Wrong grade),
`/pathfinder/scoreboard?date=2026-07-29`, `/pathfinder/experiments`.

## 3. Results / evidence

### 3.1 Gates (all run in this session, in this order, on the final tree)

| Gate | Result |
|---|---|
| `npm run typecheck` (`tsc --noEmit`, app + tests, typed routes regenerated) | clean |
| `npx expo lint` | **0 errors, 0 warnings** |
| `npm test` (`tests/lib.test.ts` 17 + `tests/feed.test.ts` 16) | **33 passed, 0 failed** |
| `npm run test:contract` against the LIVE research API on `:8010` (`KANIDA_PATHFINDER_SOURCE=research`) | **16 passed, 0 failed** — run twice: against the S2 registry as committed at `187c488` (131 declined, 72 trials) and again against the re-audit's registry while it was being rebuilt (as of 2026-05-27, 42 declined, 36 trials) — same 16/16 |
| `npx expo export --platform all` | **exit 0** — android, ios and web bundles; 17 static web routes including `/pathfinder`, `/pathfinder/story/[id]`, `/pathfinder/experiment/[id]`, `/pathfinder/experiments`, `/pathfinder/scoreboard` |

The rows are in `docs/TEST_PLAN.md` § Pathfinder S3 (S3-01 … S3-55). The contract suite probes the latest
edition plus the six prior editions it can find and asserts: at most three in *what matters now* and strictly
increasing ranks; **nothing served below the threshold**; digit-free narratives whose every token resolves;
provenance on every fact with `date_range.end ≤ as_of` and no model in `computed_by`; addendum-7 provenance
on every card; the rule frozen at publication and a consistent grading state; **the backfill label on the
edition, every card and the scoreboard, with forward + backfilled = n and `regraded = n_total − n`**; no
order-shaped text or field; a zero-finding edition still valid; guarded 400/404s; the registry losers-first
with every decline reasoned and every researched decline naming its closest variant and failed gates; seven
digit-free beats and no constituent field on any public card; the record's versions / trials / periods /
basket rules; the disclosure everywhere.

### 3.2 UX verification — what I actually saw

Driven in two browsers against the live research API (real July 2026 editions, simulated backfill):

**Phone, 375 × 812, dark** (the Browser pane with device emulation):

1. **Home** — the Pathfinder card: *Edition 29 Jul 2026 · SIMULATED BACKFILL* chip, the first headline ("A
   laggard sector flipped to the top today, but history says it is usually noise"), *4 stories today*,
   *58 · 30 · 39 Right · Wrong · Incl.*, *n = 127 · forward 0*, the record sentence verbatim in amber, the
   as-of stamp. Trader and Investor still *Not built*.
2. **Feed, story 1 / 7** — progress rail with the first segment lit; chrome *Pathfinder · 29 Jul 2026 ·
   NEUTRAL · 1 / 7 · SIMULATED BACKFILL*; kicker *WHAT MATTERS NOW · 1 OF 3 / Theme in play · Debunk*;
   *Telecommunication · SECTOR*; the hero headline; four key-fact tiles (17 rank over the window · 1 rank
   today · 41.2% beat-the-market share n = 776 · −0.37% typical sector-minus-market n = 776); a five-line
   lede with tappable figures; *REJECTED · USUALLY NOISE* + "History says a one-day flip like this is usually
   noise; not chasing it."; *Verdict pending — the horizon has not completed · due 5 sessions after 29 Jul
   2026 · SIMULATED BACKFILL*; chips *SECTOR · N = 776 · JAN 2013 – JUL 2026 · NEUTRAL · HURDLE 0.50%*; the
   *Evidence & provenance →* button pinned at the bottom. Everything fits; nothing clipped.
3. **Story 3 / 7** — *Volume anomaly · Debunk*, DCMSHRIRAM, "Huge volume, flat close: a striking day that
   history says means nothing"; tiles 32.96× (a parameter — no n shown) and 47.3% (n = 13,127); *NO TRADE*;
   rail segments 1–2 filled, 3 active.
4. **Story 5 / 7 — "Nothing cleared the gate"** — "Pathfinder evaluated **72** counted variants across **4**
   findings and declined **131** candidates on the record. Declining is the result, not a gap."; *THE
   CLOSEST, AND WHY IT DID NOT OPEN*: the surge rule text, *expectancy net +0.33%*, "A research hypothesis as
   the engine defines it — not an instruction.", `failed: implementable_under_cost_convention`; the record
   sentence. (After the tie-break fix the dip near-miss — `history_placebo=0.093 vs 0.05` — ranks first; the
   registry page lists both.)
5. **Story 6 / 7 — the record** — *THE RECORD · SIMULATED BACKFILL*; **58 / 30 / 39** in green / red / amber;
   *N = 127 · INDEPENDENT GRADES · PENDING 4*; the record sentence; **FORWARD n = 0 "nothing yet"** beside
   **BACKFILLED n = 127 R 58 · W 30 · I 39**; by question type (dip R 8 W 2 I 6 n 16 *too few to read* …);
   the experiments line (n 0 · not opened 131 · 72 trials counted).
6. **Story 7 / 7 — what I'm testing next** — three follow-up cards (Telecommunication: "Did the flip come on
   broad participation or one large name?", PCBL ×2), "+ 4 more on the cards themselves", *VERDICTS STILL
   PENDING* for the four subjects, "End of this edition · swipe ↓ to go back up".
7. **The empty edition, `?date=2026-07-24`** — 1 / 3: "Nothing cleared the usefulness threshold on 24 Jul
   2026 — **6** candidate questions were computed across **497** stocks, and none scored above the threshold.
   That is a valid edition." with *RISK_OFF · DATA TO 24 JUL 2026*; then the no-experiment story; then the
   record.
8. **Depth for PCBL** (`/pathfinder/story/fnd_20260729_surge_pcbl?date=2026-07-29`) — header with the
   violet flask; *WHAT MATTERS NOW · STOCK BEHAVIOUR · DEBUNK · rank 2 · 29 Jul 2026*; the full narrative with
   every figure dotted-underlined and tappable; *ENGINE-NARRATED · Computed, then templated. No model in the
   loop.*; *THE DECISION — Rejected · usually noise*; the evidence, numbers, rule and grade sections below.
9. **The honest error state** — while the re-audit was rebuilding the registry (`/experiments` → 404
   `no_experiments`), story 5 / 7 rendered "The experiment registry could not be reached — The experiment
   loop has not run yet. `http://127.0.0.1:8010/api/pathfinder/experiments` — Nothing is shown in its
   place — an experiment story you cannot source is not a story."; the rest of the feed unaffected.

**Desktop, 1366 px wide** (the user's Chrome, which is in light mode — so the light theme was exercised
too; it is legible, the dark theme remains the designed one):

10. The feed as a phone-width column: index rail on the left (*THIS EDITION 1 Telecommunication · 2 PCBL ·
    3 DCMSHRIRAM · 4 J&KBANK · 5 Nothing cleared the gate · 6 The record · 7 Testing next*), the pager in the
    middle, ↑ ↓ → buttons on the right, "↑ ↓ to browse · → for the evidence".
11. **The flow**: ArrowDown → story 2 (PCBL, "The day's biggest jump: history rejects chasing it", 14.9% and
    42.8% n = 24,302); ArrowDown → 3 / 7 DCMSHRIRAM; **ArrowRight → the depth page** for DCMSHRIRAM;
    **browser Back → 3 / 7 DCMSHRIRAM again**, rail highlighting 3 — back returns to the same story.
12. **The full scoreboard** (`/pathfinder/scoreboard?date=2026-07-29`): the label chip and sentence, 58 / 30 /
    39, *N = 127 · INDEPENDENT · one grade per claim per non-overlapping horizon*, Forward n = 0 vs
    Backfilled n = 127, Pending 4 · Void 0 · Regraded 0 · Continued 10 · All grade rows 127, six question
    types with n-flags, the noisy-per-card caveat.
13. **The registry** (`/pathfinder/experiments`): "No experiment is open", the zero scoreboard, *4 findings
    researched, none cleared*, each card with rule text, expectancy, and `failed: …` in mono.
14. **A Wrong grade** (`fnd_20260722_theme_information_technology`, edition 2026-07-22): *THEME WATCH ·
    HORIZON 5 SESSIONS · HURDLE 0.50%*, the Right / Wrong / Inconclusive legs, `frozen … grading_rules@1.3.0+
    code.fd36d023094d`; *THE GRADE — WRONG · Graded under the rule frozen at publication · on data to 29 Jul
    2026 · SIMULATED BACKFILL*; *WHAT ACTUALLY HAPPENED*: sector move 7.3%, market 1.2%, sector-minus-market
    6.1%, rank at horizon 1 (recorded, not judged), hurdle 50 bps; the record sentence.

**Experiment-story layouts — SYNTHETIC / ARCHIVED, NOT REAL RESULTS.** The served registry holds zero
experiments (the real S2 result), so the experiment slide and record were exercised against the S2 test
suite's synthetic-universe registries (`test_pathfinder_s2.py`'s `keep` / `lose` fixtures, built into the
scratchpad and served on a side port; the archived pre-audit real registry is schema-1 and the store refuses
it by design). Seen on the synthetic `keep` registry: the feed at 20 May 2026 with the experiment story
*the bounce after a hard one-day fall, across the whole market* in position 5; the record page: *EXPERIMENT ·
DIP BOUNCE · PROPOSED*, *Right 8 Wrong 0 Inconclusive 1 · n = 9 · too few to read · RIGHT — BUT WEAKER THAN
HISTORY*, the record label, the question and the rule under test, the seven beats with "a constituent —
withheld pending RA review" in place of the name, *A graduation proposal awaits a human · Graduation proposal
· v1 → trader*, the frozen expectation block ("frozen 11 Sept 2026, sealed at 26 Feb 2026"). The `lose`
(buried, v1 → v2 → v3, post-mortem) registry was built but could not be served: mid-session the concurrent
re-audit bumped the experiments store to schema 3 and the store refuses a schema-2 file (§5.1). The buried
layout is covered by unit rows and by the record page's post-mortem-first branch; it has not been seen
rendered. None of the synthetic material is in the product or its tests as data.

### 3.3 Things the verification caught and I fixed

1. The key-fact tiles overflowed the phone width (the sizing style sat on the inner view, not on the
   pressable) — fixed; two tiles on a short phone, four on a tall one.
2. The story content overflowed 812 px and hid the *Evidence* button — the slide now has a fixed header, a
   shrinking middle (tiles + lede, clipped) and a pinned footer, with line budgets by height.
3. The chrome overlapped the kicker — chrome height raised.
4. Pending cards printed "due date not set" because the engine leaves `due_session` null (§4.4) — the line
   now states the frozen rule's own horizon ("due 5 sessions after 29 Jul 2026").
5. The strict S2 word list withheld honest S1 debunk copy ("Buying big surges lost money after costs") —
   split into the two lints (§4.3).
6. "Closest" candidates tied on gate count showed an unimplementable short before the statistical
   near-miss — a statistical near-miss now ranks first.

## 4. Contract findings for the backend audit (documented, not changed)

1. **`/api/pathfinder/loop` and `/api/pathfinder/learnings` return a guarded 500 under the research
   source**: `get_store()` raises `unknown KANIDA_PATHFINDER_SOURCE: 'research'`. The app no longer calls
   either (Home reads the feed). Either serve them from the research store or 404 them cleanly.
2. **`RejectedCandidate` and the scoreboards carry bare numbers, not Facts** (`best_expectancy_net_pct`,
   `trials_evaluated`, the tallies). The app renders them as *engine-reported figures* with a sheet that says
   no fact card was served. Serving `Fact`s here (with n, window, cost convention) would let every number on
   the feed be a Fact.
3. **S1 `decision_reason` / headlines contain "buy" / "buying"** ("history says do not buy the dip", "Buying
   big surges lost money after costs") — legitimate debunks that the S2 `PUBLIC_CARD_BANNED_RE` would refuse.
   S1 text is not linted server-side. The client applies the narrower order lint to S1 cards; a server-side
   lint of the same shape (entry / exit price / target / stop / execute / order) on `Finding` text would make
   the boundary a contract.
4. **`GradingState.due_session` is null on every pending finding** (it is set only when graded). The client
   states the frozen horizon instead. Stamping the due session at publication (the rule has the horizon) is
   a one-liner and would let the feed say a date.
5. **`FeedResponse` carries no engine version** (`ExperimentsResponse` does). The empty-edition story's
   figures name the edition header as their source; an `engine_version` on the feed would be better.
6. **The re-audit's schema additions are additive** (`Expectation.population / signals_fired /
   signals_skipped / equal_weighted_* / top3_days_share_pct / *_without_best_day / placebo_convention /
   placebo_se / cluster_t_kind`, `ForwardResult.drawdown_convention`, `LearningView.family_trials_all_time /
   insufficient`, `RejectedCandidate.family_trials_all_time`) — the client ignores them today; `types.ts`
   should be extended once the audit lands (or generated from `openapi.yaml`, P2's Risk 3).
7. **`ScoreCounts.sample_flag` on a scoreboard of n = 0 is `greyed`**, so the registry's zero board reads
   "too few to read" — technically true, but "no grades yet" would be the honest copy at the source.

## 5. Risks

### 5.1 The backend was moving under this session
The S2 re-audit modified `backend/pathfinder/{schemas,experiments/*}.py`, `docs/openapi.yaml`,
`docs/TEST_PLAN.md`, `docs/handbacks/PF-S2.md` and rebuilt `var/pathfinder_experiments.db` (schema 3) while
S3 was in flight — for a spell `/experiments` 404'd, which the feed rendered honestly (§3.2 item 9). The
contract suite passed against both the committed registry and the rebuilt one; only S3's own hunks of
`docs/TEST_PLAN.md` were staged (the audit's are left in the working tree for that session to commit). The
"closest variant" story will read differently once the rebuilt registry lands (under the re-audit's pool
the dip near-miss fails four gates, not one) — the app renders whatever the record says.

### 5.2 No device run (inherited)
`expo export --platform all` proves the three targets bundle; the web target was driven by hand. No macOS
or Android SDK here. First action for whoever picks it up: `npm run ios` / `npm run android`, and check the
paging feel and the safe-area insets on a notched phone.

### 5.3 The web pager's edge cases
Programmatic scrolling and keyboard paging were verified; touch-swipe on mobile web relies on CSS
scroll-snap and was not driven with a real finger. The horizontal "swipe deeper" is a simple touch-delta
heuristic (56 px, < 40 px vertical) — tune on a device.

### 5.4 The mockups are still not in the repo
§6. Everything visual is an interpretation of the written DNA and the Instagram direction.

### 5.5 Hand-maintained types
`src/api/types.ts` mirrors the contract by hand; the contract suite catches semantic drift, not a rename.

## 6. What could not be pixel-matched (mockup PNGs absent)

`docs/design/mockups/` contains only `PLACE_MOCKUPS_HERE.md`. Built from the written DNA — near-black cool
ground, cards a touch lighter with hairline borders and 18 px radius, violet Pathfinder identity, mint /
red / amber semantics, tabular numerals on every figure, small uppercase letter-spaced labels, the bottom tab
bar — and the elevation direction. What awaits the images: the exact type sizes and weights, the tile and
chip proportions, the progress-rail geometry, the header treatment, iconography (the flask is a drawn glyph),
and whether the feed should be edge-to-edge or carded on phone. Drop the PNGs in and the next session
reconciles; the structure will not need to change.

## 7. Docs written / changed

`kanida-app/**` (the feed; see §1.4 for removals) · `kanida-app/README.md` · `docs/FRONTEND_SPEC.md`
(S3 build record: the surface, the story anatomy, 13 design decisions) · `docs/TEST_PLAN.md` (S3 section,
rows S3-01…S3-55, known gaps; header row) · this hand-back. Nothing in `backend/`, `scripts/`,
`docs/openapi.yaml` or `var/` was changed by S3.

## 8. Next step

1. **Founder:** drop the mockup PNGs into `docs/design/mockups/` — one short session reconciles.
2. **Backend audit:** §4.1 (`/loop` / `/learnings` under the research source), §4.4 (`due_session` at
   publication), §4.3 (an S1-side order lint), §4.2 (Facts for the registry's numbers). Then extend
   `types.ts` for §4.6.
3. **Device run** (§5.2), then tune the swipe-deeper threshold and the story line budgets on real phones.
4. **Ops:** once the first same-day scan runs, the forward tiles stop saying "nothing yet" on their own —
   no app change.
