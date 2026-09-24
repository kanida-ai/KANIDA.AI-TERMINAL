# CLAUDE_UX_01 — derivatives UX review

Bounded review. No application code changed. Branch `codex/market-intelligence-engine` left as found;
all uncommitted and untracked work preserved.

## 1. What I inspected

**Source, read in full:** `src/derivative/index.tsx`, `frame.tsx`, `ScreenerSection.tsx`, `SignalTable.tsx`,
and the components they drive (`ChainWidget`, `OiByStrikeSection`, `OiGridSection`, `SessionBlocks`,
`SessionPanel`, `IvGridSection`, `UnusualWidget`, `Table`, `ChartTile`, `FuturesChartPanel`). Server side:
`app.py` route table, `derivatives.py` (`oi_grid`, `_grid_points`, `iv_series`, capture envelope),
`market_intelligence.py`, and the store schema in `market_data/tests/test_metrics.py:624`.

**Owner requirements:** `Derivative detail questions and answers.docx`, extracted locally.

**Prior findings, read as context not as proof:** `DERIVATIVE_TAB_AUDIT.md`,
`derivative-audit-2026-09-19/README.md`.

**Live screen: not inspected.** `http://127.0.0.1:8082/derivative` answers 200 but
`/api/derivatives/status` returns `SIGN_IN_REQUIRED`, and the built-in browser has no pilot session. I did
not sign in or create an account. **Unverified visually:** actual pixel heights and above-the-fold cut at
1440×900 and 390×844, the wheel-trap reproduction, real text density, and whether any prior finding has
since been fixed in a way source does not reveal. Every geometry figure below is computed from source
constants (`BLOCK_H=530`, `RAIL_H=320`, `IDLE_H=190`, `SIGNAL_W=556`, breakpoints 1200 / 760), not measured.

Two prior findings **are** closed in current source: the unusual-cell text bomb is now a summarised count
(`logic.ts:unusualText`, `UNUSUAL_COUNTS_TEXT`), and the chain/OI-by-strike/ΔOI blocks now honour a shared
reading boundary.

## 2. Issues, worst first

| # | Issue | Location | Impact on a trader |
|---|---|---|---|
| 1 | **The context bar states one Reading that is false for six of nine blocks.** Only chain, OI-by-strike and ΔOI receive `at`. PCR, max pain, IV, futures build-up, the per-strike IV series, the index dashboard and the futures list each resolve their own newest session. | `index.tsx:104` (ContextBar), `:229`; `ChainWidget.tsx:55`, `OiByStrikeSection.tsx:36` (pass `at`); `SessionBlocks.tsx:91,182,294,395`, `IvGridSection.tsx:117` (do not) | The one line that claims to fix the page's clock is contradicted by most of the page. On 18 Sep that gap was 4h15m. This is the trust defect; everything else is comfort. |
| 2 | **No "what matters now" layer exists.** Nine blocks of figure + chart + table. The only interpretation on the page is SignalTable's per-row `interpretation` cell at 9px in a flexed column. | `index.tsx:257–330`; `SignalTable.tsx:113` | The trader does all the interpretation, which is the product's stated job. Headline → explanation → persistence has no home. |
| 3 | **One 530px geometry for nine unequal blocks.** `BLOCK_H` is sized by the tallest content panel (the 2×5 ΔOI grid). PCR, max pain and futures build-up put 5–7 readings rows in the same box. `IDLE_H` collapses only when no symbol is chosen. | `frame.tsx:378` (`BLOCK_H`), `:36` (`IDLE_H`), `Block` | ~6,000px of desktop scroll; ~1,600px of it is three ratios. Nothing that matters is ever on one screen. |
| 4 | **Explanation is a glossary, filed per block.** 28 `_TEXT`/`_WHAT` constants; InfoDisclosure groups: Screener 8, SessionBlocks 15, SignalTable 4, ΔOI 2. | `logic.ts`; `ScreenerSection.tsx:100–118`, `SessionBlocks.tsx:164,263,366,472` | Honest and well written, but it is reference material the reader must open block by block. The owner asked for plain English *in* the sentence with terminology underneath. |
| 5 | **Fixed-height panes each wrap their own scroller, inside the page scroller.** | `Table.tsx:59,86`; `ChainWidget.tsx:106`; `SignalTable.tsx:87`; `OiGridSection.tsx:185`; `SessionPanel.tsx:225`; page scroller `index.tsx:355` | Six nested scroll regions. The prior audit reproduced a wheel trap twice; the structure that caused it is unchanged. |
| 6 | **Calls and puts are never read independently before being combined.** SignalTable gives two side cells then one combined `market_signal` per row; the ΔOI grid gives ten tiles and no per-side conclusion. | `SignalTable.tsx:44–53,95–120`; `OiGridSection.tsx:94–140` | No answer to "what are calls doing" as a statement with its own persistence and leading strike. Owner: independently first, correlate second. |
| 7 | **No session timeline.** The nearest thing is SignalTable's rows, newest first, in a 556×320 panel at 9–10px, beside the screener. | `SignalTable.tsx`; `ScreenerSection.tsx:167–170` (`RAIL_H`, `SIGNAL_W`) | The mandatory readable 09:15-onward narrative cannot be read there. |
| 8 | **Mobile is the desktop page stacked.** One breakpoint; below 1200 every block becomes two 530px panes stacked, and wide tables keep a horizontal scroller. | `frame.tsx:381,383`; `index.tsx:143`; `Table.tsx:59` | ~9,500px on a phone, with sideways scrolling inside it. No overview/detail split. |

## 3. Proposed content order

**Desktop**

```
CONTEXT   NIFTY ▾ · 25 Sep expiry (5d) ▾ · 18 Sep session · 11:30 reading
          live edge 15:45 ▸ · Partial capture ▸                      [⟳]

WHAT MATTERS NOW
  Fresh call writing building near 23,350 CE
  New positions are entering 23,350–23,400 CE while premium and IV ease.
  Continuing 45 min · 3 consecutive readings · 3 of 10 strikes · 23,350 leads
  ▸ Evidence — strikes · baseline used · observed Δ OI/price/IV · uncertainty

CALLS                              PUTS
  writing · broadening · 3 strikes   buying · appearing · 1 strike
  23,350 leads                       23,200 leads
  ── combined ──  OI is building, but premiums are not confirming yet.

SESSION TIMELINE   09:15 ─────────────────── 11:30          [see the day]
  11:30  Call buildup broadening — now three strikes
  11:15  Fresh call interest appears at 23,350 CE
  10:45  No material change; earlier buildup remains intact

WHERE IT IS HAPPENING   [ spot/futures chart ][ OI by strike ]   one row

INSPECT  ▸ Option chain  ▸ ΔOI grid  ▸ PCR  ▸ Max pain  ▸ IV
         ▸ Futures build-up  ▸ Screener     (accordion; today's blocks, intact)
```

**Mobile** — context (one line, tap to expand) → headline card → Calls / Puts as two tabs → latest three
timeline entries + "see the day" → chart → Inspect list, each opening full-screen. No block renders two
530px panes stacked.

## 4. First implementation slice

**"One clock, and one thing that matters."**

*Changed:* `index.tsx` (thread `at` to every block; reorder), `SessionBlocks.tsx`, `IvGridSection.tsx`,
`IndexWidget.tsx`, `FuturesWidget.tsx` (accept and send `at`). *New:* `NowCard.tsx`, `SidePair.tsx`,
`SessionTimeline.tsx`. *Not touched:* the nine blocks' internals — they move under `INSPECT`, unchanged.

*Preserved interactions:* screener row → tab-wide symbol; tab-wide strike hover highlight; any row/tile →
the one linked chart; expand / close / restore-all; filter builder; reading selector; every existing empty,
stale, floors, no-baseline and capture-health string.

*Acceptance, observable:*
1. Every block's as-of equals the context bar's Reading, or the block states which reading it is on —
   checked on a session where the screener falls back (18 Sep 2026: 11:30 vs 15:45).
2. At 1440×900 with a symbol chosen, headline + explanation + persistence are visible without scrolling.
3. Calls and puts each carry behaviour, persistence and leading strike before any combined line.
4. Timeline has one entry per captured reading from the session's first to the reading on screen; a reading
   never taken is absent; a reading taken with no value says so and carries no behaviour.
5. A no-material-change reading renders one line, not an empty card.
6. The wheel over any inner table continues scrolling the page at that table's ends.
7. At 390×844: no horizontal page scroll; no block renders as two stacked 530px panes.
8. Every current loading / not-captured / partial / stale / filtered-out wording is unchanged.

## 5. Real data vs illustrative fixtures

**Available from current APIs today**

- Context bar and freshness/replay status — `/api/derivatives/screener` → `context` + `capture`
  (`is_newest`, `newest_at`, `mode`, floors, coverage). Already rendered; needs `at` honoured downstream.
- Per-strike ΔOI, price and direction through the session, ATM basis, marks, flow vocabulary —
  `/api/derivatives/oi-grid` (`derivatives.py:2074`).
- Per-reading call/put words and the combined read — `logic.ts:signalRows` over that same grid read.
- ATM and per-strike implied volatility, marked computed — `/api/derivatives/iv-series`.
- Spot / futures context — `/api/derivatives/futures-buildup`, `/futures-chart`, `underlying_snapshots`.
- All loading, missing, stale, partial-capture wording — capture envelope, unchanged.

**Requires backend work; render as clearly labelled illustrative fixtures until it resumes**

- The event objects themselves: appearing / building / broadening / concentrating / shifting / slowing /
  reversing, `consecutive_scans`, `duration_minutes`, breadth (isolated / clustered / dispersed),
  `uncertainty[]`, and the headline/explanation/context triple. **`market_intelligence.py` already computes
  exactly this** (`replay()`, `explain()`) — but grep confirms it has no caller and no route. Fixture from
  its own output shape, so the UI is built against the real contract.
- Its inputs are already captured: `snapshots(last_price, average_price, volume, oi, bid, ask)` and
  `candles_15m(volume)`; IV is computable via `implied_vol.py`. No new capture is needed for a route.
- **Not captured, and must never be fixtured as a number on screen:** depth beyond best bid/ask, and
  historical normal ("unusual for 10:15"). `historical_significance` is `None` in the engine — render it as
  "not measured".
- "Since your last visit", saved views and alerts: nothing exists.

## Handoff

Report: `kanida-app/docs/CLAUDE_UX_01_RESULT.md`. No code changed.

**Blockers for the next round:** (a) no authenticated pilot session, so nothing above is visually verified —
if Codex or the owner can supply a signed-in session, issues 3, 5 and 8 should be confirmed on screen before
geometry is changed; (b) issue 1 is a correctness fix, not a layout choice, and should be settled before the
new hierarchy is built on top of it; (c) `market_intelligence.py` needs a route before section 2 of the
wireframe carries anything but fixtures.
