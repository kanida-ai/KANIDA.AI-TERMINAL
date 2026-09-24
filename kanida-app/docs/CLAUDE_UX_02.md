# KANIDA market story visual approval prototype

## Owner update requiring a live walkthrough

The existing Chrome tab at `http://127.0.0.1:8082/derivative` is signed in and populated; Codex verified it directly. Before this task passes acceptance, follow `CLAUDE_UX_LIVE_REVIEW.md` and produce `CLAUDE_UX_LIVE_RESULT.md` with the actual end-to-end walkthrough evidence. The separate built-in browser's unauthenticated session is not the user's Chrome session. Review the real app's complete page and interactions and apply those findings to this prototype.

## Assignment and ownership

Codex is directing product design and will review the prototype against the owner's vision. Claude Code builds this bounded prototype. The owner explicitly wants a premium visual experience, agent-like text presentation, hand-drawn chart explanations, cinematic transitions, and desktop/mobile views for approval BEFORE production implementation. Build now; do not stop at another proposal.

Use the existing local `C:\Users\SPS\Documents\Kanida_Falcon` working directory. Read AGENTS.md, CLAUDE_UX_01_RESULT.md, and this brief. Consult the owner's requirements document only for unresolved product details. Preserve every existing change. No backend work, package installation, credentials, database changes, production app edits, branch switching, commits, pushes, or deployment.

## Review corrections

The first review was source-based, not a signed-in visual walkthrough. Do not imply otherwise. Its claim that all needed data is already captured is too strong: the current snapshot schema retires bid/ask fields, and historical baselines, freshness, Greeks, and coverage still need tracing. The preliminary market_intelligence.py is unfinished and untested; its output is not an approved production contract. Design to the owner's experience requirements, with illustrative data clearly separated from actual API data.

## Deliverable and scope

Create a polished, clickable, responsive standalone approval prototype at `kanida-app/docs/prototypes/market-story/index.html`, with local CSS/JS assets if useful. No external dependencies, downloaded fonts, remote imagery, or API calls. It must work independently of pilot authentication, using explicitly illustrative scenarios. This is a high-fidelity wireframe and motion study, not a live market feature.

Write `kanida-app/docs/CLAUDE_UX_02_RESULT.md` with the entry path, implemented interactions, verification, known limitations, and files changed. Keep the report under 600 words and the final chat handoff under 150 words. Do not spend tokens rereading unrelated modules. One coherent direction is sufficient; do not build three competing designs.

## Visual direction

Create the feel of a calm, precise derivatives desk with a distinctive editorial hierarchy. Retain KANIDA's dark ink and restrained mint identity, with warm white narrative text, muted slate context, and amber for uncertainty. Use a comfortable system sans-serif, tabular numerals for data, and fine intentional rules. Premium comes from proportion, legibility, meaningful negative space, and coordinated interactions. Avoid neon overload, glass panels everywhere, oversized empty rectangles, spinning globes, generic AI sparkle icons, or a grid of unrelated KPI cards.

Desktop: compact brand/navigation rail; compact instrument/expiry/session context across the top; a clear primary story beside an annotated chart; call and put summaries; timeline and expandable evidence below. Make the primary story and both side summaries visible near the first viewport at 1440x900. Keep all dates and market times visibly IST. Display an unobtrusive but persistent `Illustrative design preview — not live market data` label.

Mobile at 390px: compact context, primary story, accessible Calls/Puts comparison or tabs, chart, latest timeline events with full-day expansion, and evidence. Avoid horizontal page scrolling, nested scrollers, and huge fixed-height desktop panels stacked on top of one another. Show the same content faithfully in both layouts.

## Content hierarchy

1. **Context:** NIFTY, illustrative expiry, selected session reading, and freshness/replay status. Controls must change an actual state or be explicitly disabled with an explanation. Do not add fake functioning instrument selectors.
2. **What matters now:** a specific headline; two short plain-English sentences of evidence and context. Example: `Call buying is spreading above 23,350`. Explain that this is an inference consistent with the observations, not known participant intent or a trade instruction.
3. **Calls and puts:** independent behavior, location, leading strike, breadth, first detection, and duration. Combined context comes after these, with conflicts preserved.
4. **Where it happened:** an illustrative spot-price chart, clear price/time axes, and annotated strike levels. A second compact strike-participation graphic can show which strikes joined. Do not confuse spot points, option premium, OI, and IV on one axis.
5. **How the day evolved:** selectable events from initial observation through appearance, persistence, spread, weakening, and reversal. Selection synchronizes the narrative, chart annotations, side summaries, and evidence to the same reading. No future evidence appears in an earlier reading.
6. **Why KANIDA said this:** expandable facts, inference, comparison baseline, uncertainty, and supporting numbers. Basic traders see plain language first; experienced traders can inspect the evidence.
7. **Inspect tools:** a compact entry to Option chain, OI, IV, and futures context. In this prototype implement at least one meaningful evidence drawer; label the remaining unimplemented destinations clearly rather than making dead buttons look functional.

## Motion and human touch

- Provide a working `Replay explanation` control. On deliberate replay, progressively reveal the short explanation in readable word groups, then trace the relevant historical chart segment and draw a clean hand-drawn-style underline/bracket around the strike cluster. Total sequence roughly 2–4 seconds. The user can skip it immediately.
- Use precise SVG strokes for chart tracing and annotations. The hand-drawn quality belongs to explanatory marks, not distorted market data or wobbly axes. Do not draw forecast paths.
- Coordinate narrative emphasis with chart/strike emphasis so the user sees the evidence being described. Use a subtle traveling point or short highlight rather than a decorative robot/avatar.
- Reading changes use short fades or geometry transitions around 160–240 ms. Do not erase and retype the entire page every interval. Do not simulate engine thinking, invent confidence scores, or use endless typing indicators.
- Default content is immediately readable. Replay animates only when requested; animation must never delay access to data or evidence.
- Include a quiet-motion toggle and respect prefers-reduced-motion. Cancel pending animation on scenario/read changes. Keyboard users can activate all controls, and screen readers receive the completed explanation once, not every typed word.

## Required illustrative scenarios

Use a compact scenario selector with at least these states, fully affecting the view:

1. Confirmed-looking call activity builds across three readings and broadens from one strike to neighboring strikes; the narrative still identifies behavior as inferred.
2. Earlier activity weakens or narrows; the wording references its earlier strength and does not repeat the initial headline.
3. OI rises while premium/IV conflict; explicitly show unconfirmed activity without forcing bullish/bearish direction.
4. Missing or partial capture; clearly distinguish unavailable observation from quiet market activity. Preserve earlier story as historical, without extending persistence across the gap.

Use internally coherent synthetic numbers. Three readings at 09:30, 09:45, and 10:00 span 30 minutes from first detection, not 45. If no 09:15 observation exists, do not invent it as the market-open baseline. A broadening cluster must have actual additional participating strikes in the fixture. Evidence must reconcile with the displayed narrative at every selectable time.

## Verification and acceptance

- Open the standalone prototype if browser access is available; verify at 1440x900 and 390x844. State clearly if visual testing is unavailable.
- Check all scenario controls, timeline selections, replay, skip, evidence expansion, and reduced motion.
- Check no clipped text, overlapping controls, horizontal page overflow, or missing assets.
- Verify synthetic-evidence consistency and common as-of time across every visible section.
- The owner should be able to approve layout, visual language, narrative style, and motion from this prototype without logging in.
- Finish the prototype and result file, then stop. Codex will perform acceptance review and prepare focused corrections.
