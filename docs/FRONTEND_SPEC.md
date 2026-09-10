# FRONTEND SPEC — KANIDA.AI (design DNA + Pathfinder screens)

## THE RULE (read first)
The founder's mockups in `docs/design/mockups/` are the reference. **Match this visual system exactly. You may
ELEVATE the craft (spacing, motion, polish, clarity) — you must NEVER regress below the mockups' bar.** Render
the *honest engine data* in this language (WATCH/died experiments shown, n<20 greyed, returns paired with
drawdown, no promises). If the real data is thinner than the mockups' sample numbers, show it honestly in the
same beautiful frame — the design must make honest, WATCH-heavy data look premium.

## Design DNA (from the mockups)
- **Ground:** near-black, slightly cool. **Cards:** a touch lighter, **rounded ~16–20px**, hairline borders,
  soft depth. Generous padding, calm spacing.
- **Accents (semantic):** **mint/green** = active · positive · primary CTA; **red** = negative/short;
  **amber** = watch/caution; each agent has an **identity colour**: Trader = blue, Investor = green,
  **Pathfinder = violet/purple**. Semantic colour is separate from the agent accent.
- **Type:** clean geometric sans (SF/Inter feel), **tabular numerals** for all figures, small UPPERCASE
  labels with letter-spacing, balanced headings.
- **Chrome:** bottom tab bar **Home · Agents · AutoTrade · Insights · Profile** (active tab tinted). Screen
  header = back arrow · agent avatar · name · green **Active** badge · one-line **mandate** (target · holding ·
  strategy). Sub-tabs sit under the header.
- **Data viz:** small area sparklines with an emphasised endpoint; faint grid; green/red directional. Charts
  support the story, never dominate.

## Shared agent-screen pattern (Trader / Investor / Pathfinder all follow this)
`Header (avatar · name · Active · mandate)` → `Sub-tabs` → content. Every agent shows: mandate (target ·
holding · universe · approach), disciplines/rules, today's activity, an honest track record, and insights.
Trader/Investor use this exact frame; **Pathfinder is the first build** and is specified below.

## PATHFINDER screens (FIRST MISSION — build these)
Identity: **violet flask** avatar · "Pathfinder Agent" · green Active · tagline **"Discover repeatable market
edges · Test → Validate → Evolve."** Sub-tabs: **Overview · Experiments · Learnings · Strategy Lab.**
Core UX law: **the research STORY dominates; metrics support.** Each experiment reads as *I noticed → I'm
testing because → result → learned → next.*

- **Home card (on the Home screen):** violet flask · "Pathfinder Agent" · Active · "Discover repeatable market
  edges" · 3 stats (experiments · promising · experimental return) · chevron.
- **Overview tab:**
  - **Today's Activity:** Experiments running · Promising · Avg virtual return (honest, with n).
  - **Edge Discovery Pipeline:** New Ideas → Backtesting → Virtual Trading → Validating → Graduated (counts).
  - **Today's Experiments** (preview list, "View All") → experiment cards (below).
  - **What I'm Testing Tomorrow:** a numbered list of new hypotheses.
  - **What I Learned Today:** a lightbulb card, one plain-English learning
    (e.g. *"#8421 works strongly in bull markets but loses its edge below Nifty 50-DMA"*).
- **Experiment card (the atom):** `#id` · **status badge** (VALIDATING=blue · PROMISING=green ·
  EXPERIMENTING=violet · REJECTED/DIED=red) · **the hypothesis in quotes**
  (*"Buy stocks falling >4% after 3 positive days"*) · **Historical return** · **Virtual return** (green/red) ·
  **Occurrences / n** · small sparkline · chevron. Honest: a died experiment is shown with its post-mortem, not
  hidden; n<20 greyed.
- **Experiment detail (the full story — the heart):** the whole loop for one hypothesis, top to bottom:
  *I noticed (the trigger/observation)* → *I'm testing this because (the computed evidence)* → *the virtual
  experiment running (capital, positions, days)* → *what happened (real tracked result)* → *what I learned and
  changed* (the **change-log**: what changed → why → evidence → previous version → new version → did it
  improve) → *what I'll test next.* Numbers come from the deterministic engine; the narrative from the LLM.
- **Experiments tab:** the full filterable list of experiment cards.
- **Learnings tab:** **Key Learnings** (numbered insights) + **What I'm Testing Tomorrow** (numbered).
- **Strategy Lab tab:** graduated strategies + versions (L3 learning) — later; stub for P2.

## Platforms
**ONE Expo codebase → iOS app + Android app + responsive web (desktop web + mobile web).** Build once; the
same components serve all three. Match the mockups on phone; the web/tablet layout scales the same system up.

## States (enforce)
Loading skeletons · honest empty states ("no experiment reached a decision today") · died/rejected shown ·
n-flags · offline/error states · accessible focus + contrast in both the dark theme (primary) and light.

---

## P2 build record — what was built, and the decisions that extend this spec

Built in `kanida-app/` (Expo SDK 57, expo-router, React Native + `react-native-web`). See
`docs/handbacks/P2.md` for how to run it and `docs/adr/0005-one-expo-codebase-base-url-is-the-only-seam.md`
for the platform decision.

**⚠️ The mockups were never delivered.** `docs/design/mockups/` still contains only
`PLACE_MOCKUPS_HERE.md`. The design DNA above was the entire reference. Everything below is
therefore an *interpretation* of this document, built to be corrected against the images when they
land. Founder: drop the two PNGs in and the next session reconciles.

### Design decisions taken in P2 (each one extends or interprets the spec)

| # | Decision | Why |
|---|---|---|
| 1 | **The experiment card's two figures are EXPECTANCY, not "return".** Cumulative return and its drawdown sit on the supporting line beneath, at caption weight. | This spec says "Historical return · Virtual return"; CLAUDE.md says expectancy is the hero and no return is shown without its drawdown. The card's *layout, weight and rhythm are unchanged* — only what the big number means. |
| 2 | **No "average virtual return" anywhere.** The Overview's third counter is *Rejected · published*, and a note says why the blend is absent. | This spec asks for "Avg virtual return (honest, with n)". The contract serves no aggregate, and averaging books with different rules, horizons and sample sizes **in the client** would be a number originating outside the deterministic engine. Requested as a contract addition — see the P2 hand-back. |
| 3 | **Inline `{{fact:…}}` figures render in neutral emphasis, not green/red.** Colour is reserved for the metric components. | A fact has no semantic direction: `12.7%` is good as a win rate and bad as a drawdown. The sentence around it already says which; colouring it would be a one-number dishonest chart. |
| 4 | **A `+` sign appears only where it carries information** — in a labelled metric. Inline in prose a positive number is unsigned; a negative always shows a true minus (`−`). | "a worst drawdown of +12.7%" reads as a gain. Caught in review of the live render. |
| 5 | **Sub-tabs are real routes** (`/pathfinder/overview`, `/experiments`, `/learnings`, `/lab`), not local state. Experiment detail sits outside the sub-tab group. | Every tab is deep-linkable and shareable on web, and the browser back button behaves. |
| 6 | **The rejected/died column is rendered outside the pipeline funnel**, on its own full-width row. | It is not a stage things pass *through* — it is where most ideas end. Putting it in the funnel implies the opposite. |
| 7 | **Every story beat states who authored it** — "Claude Sonnet 5 · interpreted the engine's numbers" or "Deterministic engine · computed, not written". | The core principle (the LLM never calculates) is only credible if the reader can see the boundary. |
| 8 | **There is no sort control on the ledger or the experiments list.** Filtering by stage is allowed; re-ranking by result is not. | A "sort by best" button is how a losers-first track record quietly becomes a winners-first one. |
| 9 | **Strategy Lab renders graduated experiments and their versions rather than a mocked strategies view.** | The contract has no strategy-versions resource. Mocking one in the client would invent product. |
| 10 | **Unbuilt tabs (Trader, Investor, AutoTrade, Insights, Profile) say "Not built" and show nothing.** | A placeholder that looks like data is a lie with good typography. |

### What P2 did NOT build

- Trader and Investor agent screens (same frame, later slice).
- Strategy Lab's version-to-version diff — blocked on a contract resource.
- Light theme was implemented and is legible, but the dark theme is the designed one.
