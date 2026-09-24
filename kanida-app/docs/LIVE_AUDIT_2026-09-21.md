# Live-market audit — Monday 21 Sep 2026

Operator: Shyam (Seattle). Audit: Claude, acting as CTO. All times IST unless stated.

## ☀ Morning summary — read this first

**Session 21 Sep: every one of the 26 readings (09:30 → 15:45) captured, computed and served correctly.** 27,317–27,323
contracts and spot for 216/216 at every reading; **0 ΔOI and 0 Δprice mismatches** against raw snapshots at every
reading (≈1,300 contracts recomputed each time); PCR and max pain exact per expiry; IV independently re-solved to
0.001 points. Capture 36–65 s (09:30 first-run 119 s), metrics 1½–2 min after each mark, all within the 240 s grace.

**What was wrong on screen, and is fixed (all deployed, tested, verified live):**
1. Open tabs never updated — now follow the newest reading (10 s check; hidden tabs too).
2. "F&O not captured" was a false alarm on every healthy day (API bug) — fixed.
3. History rewrote itself when ATM moved — **fixed at the data layer**: immutable per-reading snapshots
   (`intelligence.db`), each on its own ATM/contracts, chained from the stored previous one, versioned, insert-only.
4. Left list and middle pane disagreed — both now read the same snapshots.
5. Middle pane rebuilt to your brief (no system language, opening read, change-first headlines, arrowed deltas,
   persistence only when established); engine now at **3.3** after four live-found wording/colour defects
   (MIXED meaning, behaviour-word vs evidence, behaviour change as a new run, reductions always red REVERSING).

**Signal-to-noise, end of day (`var/sn_reports/SN_2026-09-21.md`) — one session, the eve of expiry: evidence, not a verdict.**
- 4,624 insights: **signal 1,014 (22%) · partial 1,112 · noise 1,761 (38%) · unresolved 737.** S/N ratio **0.58**.
- 15-min confirmation **32% vs a 26% baseline** — little more than chance.
- Top noise reasons: small OI change (3,057), transient within 15 min (1,979), single strike (988), flip-flop (766).
- **Counter-intuitive and important:** the "strong evidence" markers did *not* predict persistence today —
  OI ≥ 1% of the side: 21% signal vs 28% below it; breadth ≥ 3: 24% vs 28%; a named behaviour (writing/buying):
  23% vs 35% for plain "activity". Naming the behaviour is not adding reliability. One day (expiry eve) is not
  enough to change rules — but it is enough to stop assuming that bigger/broader = more meaningful.
- Commentary noise: **892 side flip-flops**, 209 repeated headlines.
- Caveat on the audit itself: "fading" confirms 88% — staying quiet is easy to confirm; it needs its own baseline.

**Decisions waiting for you:**
- The materiality floor (199 of 216 "reading a behaviour" mid-session). Today's data says a simple 1% floor will
  not fix it — suggest two or three sessions of the report before choosing a rule.
- `market_scanner/config.json` → `"candle_source": "market15"` so the scanner never starts on the stale store.
- Right pane still carries two sentences I added about the software (you asked me not to redesign it).
- **Cloud move: read [CLOUD_MOVE_CHECKLIST.md](CLOUD_MOVE_CHECKLIST.md)** — the image needs Node at runtime,
  `intelligence.db` needs a durable volume and a single writer, and where `derivatives.db` lives is undecided.
- **Nothing is committed.** Commit before any image build.
  After the close I restored CRLF line endings on five files whose working copies had become LF/mixed
  (`derivatives.py`, `check-derivative.cjs`, `ScreenerSection.tsx`, `types.ts`, `test_derivative_context.py`), so each
  diff now shows only real changes (`derivatives.py`: ~3,900 changed lines → 194). Content unchanged.
- **Final state:** 456 backend tests pass, 1 skipped (`test_the_last_live_reading_of_18_sep…` reads the live store; Friday
  11:30 has aged out of the 40-reading screener window, so it now skips instead of failing), 453 frontend checks,
  typecheck clean.

**Open defects, root-caused, not fixed:** IDEA & YESBANK grids are empty all day (no previous-session close OI for
their contracts; should self-heal tomorrow; the message wrongly says "not enough readings"). `exchange_time` stopped
being stored on quotes today (lineage only). Restarting the app parks open tabs on "Reconnect".

**For tomorrow 22 Sep (expiry day):** capture is armed for 09:30:20; the token worker refreshes from 06:00 IST; the
pilot, snapshot worker and audit keep running. The pattern scanner was started by hand with the live store — if the
machine restarts it must be started again with `SCANNER_CANDLE_SOURCE=market15`.

---
Chain audited at every reading: **market → capture → calculation → conclusion → screen.**

Tooling: `kanida-app/scripts/live_audit_mark.py "<mark>" NIFTY BANKNIFTY …` recomputes ΔOI, Δprice, per-expiry
PCR and max pain from raw `snapshots` and diffs them against `metrics`; flags carry-over quotes, frozen OI,
absent-stored-as-zero, and missing spot/PCR/max pain. Read-only (sqlite `mode=ro`). The API → screen side is
checked in the authenticated Chrome tab against the network payloads.

Deep set each reading: NIFTY, BANKNIFTY, RELIANCE, + one active stock. Breadth checks across all 216.

---

## Pre-market — 08:15–09:10

| Link | Status | Evidence |
|---|---|---|
| Machine awake | PASS | `STANDBYIDLE` AC = 0 (never sleep); on mains |
| Kite token | PASS | auth worker 07:00: *"token already valid today AND live check passed"*; capture reads newest token row, not by date |
| F&O capture | PASS (after restart) | Killed by the 07:16 restart; relaunched. `capture loop alive, waking 09:30:20` |
| F&O metrics | PASS (after restart) | Relaunched; `nothing pending; the metrics loop is alive` |
| Equity prices | PASS (after restart) | Relaunched under the scheduler |
| Pattern scanner (8765) | **FIXED** | Down after restart → app showed *"last scan 07:19, not reconnected"*. First relaunch came up on `legacy` candles → badge *"Data 31 Jul · STALE 52d"*. Relaunched with `SCANNER_CANDLE_SOURCE=market15` → *"Live 15-minute store, latest 18 Sep 15:15, stale=False"*. **Not persisted — recurs on every restart.** |
| App (8082) | PASS | 54/54 page requests 200; signed in |
| Capture source | PASS | `build(provider_id="kite")`; `fake_nfo` only referenced by tests; single writer (`store.writer_lock()`) |
| Clock | PASS | Capture: `now_ist() = UTC + 5:30` — independent of the machine's Pacific zone. UI: no `Date` construction in `src/derivative`; stored IST strings printed verbatim, so a Seattle browser cannot shift them |
| Hardcoding | PASS | No literal dates, symbols, strikes or demo strings in `src/derivative/*` |
| Calculation (Fri 15:45 replay) | PASS | 761 contracts: 0 ΔOI mismatches, 0 Δprice mismatches. NIFTY/BANKNIFTY/RELIANCE per-expiry PCR and max pain recompute exactly (NIFTY 22-Sep: PCR 1.1269, MP 23,350) |

**Known defects carried in (not new):** PCR / max-pain / IV / futures / index blocks ignore the selected reading;
Friday afternoon spot missing for all 216 (null in both `underlying_snapshots` and `metrics`); ₹2 cr premium floor
not captured; CE colour differs between two widgets. `underlying_snapshots.pcr_oi / max_pain_strike` are null on
Friday's rows — the screen reads the per-expiry `metrics` rows instead; to be re-checked on live rows.

**Operational risks for today:** a restart kills the jobs if their console windows are closed (exit `0xC000013A`);
the scanner is not an automatic job and defaults to the stale store; a token-refresh failure is silent.

---

## Live log

| Time | Status | Capture | Backend | UI | AI read | Issue / action |
|---|---|---|---|---|---|---|
| 09:30 | **FAIL → FIXED** | 27,317 contracts + 216 underlyings, all 27,379 quoted, spot for 216/216 (`kite.quote`). **Took 119 s vs Friday's 33–40 s** — half the 240 s grace (WARNING, tracking) | ΔOI/Δprice correctly NULL at the day's first reading (not measured against Friday); per-expiry PCR & max pain recompute exactly for NIFTY/BANKNIFTY/RELIANCE/HDFCBANK (NIFTY 22-Sep: spot 23,402.45, PCR 1.1795, MP 23,400) | **(1) Page never advanced**: at 09:33:48 an open tab still showed Fri 15:45 header / Fri 11:30 story while the API served 09:30. **(2) "F&O not captured" chip** during a complete capture | Correctly silent: "first line appears after two readings" | (1) Root cause: frontend — no tab read ever re-ran except on Refresh. Fix: `useFollowLatest` polls `/api/derivatives/status.as_of` each 60 s (visible tab) and re-reads the tab when the reading advances; never moves a pinned past reading. Deployed 09:36. (2) Root cause: API — `capture_health` with no `at` fell straight to `missing_capture`; the chip always calls it with no `at`, so it has been a **false outage warning on every healthy day**. Fix: unnamed = newest attempted reading; regression test added (121 pass). Deployed 09:38. Verified: chip gone, header 09:30, chain spot/ATM match store. Auto-advance to be verified at 09:45 **without reload** |
| 09:45 | **WARNING → FIXED ×3** | 27,319 rows, 45.8 s (normal; 09:30's 119 s was the day's first-run instrument load). Spot 216/216 | **First real 15-min changes: 0 ΔOI and 0 Δprice mismatches over 1,322 contracts** (NIFTY/BANKNIFTY/RELIANCE/HDFCBANK); per-expiry PCR/MP exact. SAIL & NIFTYFPI: every option OI/LTP/volume identical to 09:30 — **genuinely untraded** (no option trade today; SAIL fut last trade 09:24), not stale capture. `exchange_time` null on 27,319/27,319 today vs 27,238/27,238 Fri — lineage-only (app does not read it; weakens the carry-over check) — WARNING, capture service, after close | Middle card verified number-by-number: +93.6L CE / +81L PE ΔOI, 23,400 CE −₹4.95 (−6.7%), ΔPCR −0.02 → 1.16, MP 23,400 unchanged, strikes ranked by ΔOI — all match store | MIXED correct (1.16× inside 1.25 band) but **named only CE strikes and CE evidence** while puts did the opposite premium move → FIXED: both locations + both sides' evidence. **Right pane "LATEST CHANGE 09:30→09:45 / not enough readings" contradicted the middle** → FIXED: right pane shows *where positions stand* (heaviest 23,400 CE / 23,300 PE, spot between, PCR 1.16, MP 23,400, day adds +1.29 Cr at 23,400 CE & PE — all verified vs store). **My own caveat said "a second reading is needed" beside a pane showing that change** → FIXED | Owner ask: populate at one reading → *Where positions stand* (middle: numbers; right: sentences; levels only, prior-close comparison labelled). **Watcher skipped hidden tabs** → now polls always. **Each app restart parks open tabs on "Reconnect to KANIDA"** (5 restarts this morning, caused by the audit) → restarts now batched; staged fix (10:30 wording) held back. Left pane "0 of 216" until 10:30 — same first-hour cause, server-side; after 10:30 |
| 10:00 | **PASS data · FAIL→FIXED ×4 UI** | 27,319 rows, 49.5 s, spot 216/216 | 0 ΔOI / 0 Δprice mismatches (1,322 contracts); per-expiry PCR/MP exact. **IV independently re-solved** (own Black-Scholes, bisection, r 6.5%, time-to-expiry to the hour): 23,400 CE 12.6026 vs served 12.6034; 23,400 PE 14.1115 vs 14.1124; 23,500 CE 12.3691 vs 12.3689 | **Tab advanced 09:45→10:00 with no reload** (stale-UI fix verified). Middle numbers verified: Put ΔOI +56.6L (5,657,860), Call +40.1L (4,006,015), ΔIV ↓0.1, ΔPCR ↑0.02, MP → 23,400 | **(1) "Put writing" headlined over "prices little changed"** — regime word outran evidence → regime now requires the side's premium to move materially (≥1% of level) in the matching direction. **(2) Left pane said calls, middle said puts** for one interval (different side-choice bases) → first-hour list now chooses on total OI change, the pane's basis | Owner brief (middle pane): removed all system language ("too early", "hour-wide", "last fifteen minutes only"); OPENING READ at first reading on the grid's own near-the-money set; MIXED only when sides contradict (both adding ≠ mixed); breadth never compared against nothing; persistence shown only when established ("Since 10:15"); arrowed deltas by default, levels on press; one evidence sentence. Left pane first hour: opening read + latest-15-min side. Tests: 448 frontend (incl. a system-language sweep over every reading), 133 backend. Deployed 10:12 and 10:14 |
| — | **WARNING (owner decision)** | | Book-wide 15-min \|ΔOI\| is a median **0.82%** of a side's OI; 101 of 432 sides moved <0.5%. The left list now reads **210 of 216 "reading a behaviour"** — nearly every instrument, because any OI change counts in the first hour and the existing flat rule is relative to each series' own young peak | | | Needs a materiality floor (e.g. ≥1% of side OI, or a share of the book) before "reading a behaviour" means something. Not changed live: it is an analytics rule |
| 10:15 | **PASS data · FAIL→FIXED (point-in-time)** | 27,319 rows, 49.3 s, spot 216/216. SAIL resumed trading (only NIFTYFPI frozen) | 0 ΔOI / 0 Δprice mismatches; PCR/MP exact. Spot 23,372.35 → **ATM moved 23,400 → 23,350** | Tab advanced to 10:15 on its own, **while hidden** (≤60 s poll lag) | 10:15 read: "Call writing building above ATM — Call OI rose 42L across 23,350–23,450 CE while prices declined; 23,400 CE leads. Put activity changed only marginally." Regime justified: calls dominant, premium ↓₹9.20 at 23,400 CE | **History repainted when ATM moved**: at 10:00 the pane said "Put activity building below ATM"; at 10:15 the same reading read "Call activity building", and the opening read's strikes changed. Root cause: frontend walked the whole session on the CURRENT ATM ten. Fix: each earlier reading described from the grid `at=` that reading (its own ATM ten), computed once and kept; the first reading read from the next grid. Verified: 10:00 "Put activity…", 09:45 "Call activity…", 09:30 "23,400–23,600 CE / 23,300–23,400 PE" — exactly as shown at the time. **Open:** the right pane's session history walks the same current-ATM grid and can repaint the same way |
| 10:30 | **PASS data · FIXED at the data layer (Ask 1)** | 27,319 rows, 38 s; metrics 10:31:47 | 0 ΔOI / 0 Δprice mismatches | Before the fix the pane said "APPEARED · Call writing appears" and the list "begin building · since 10:30", while the pane's own history showed calls building since 10:15; the list jumped 210 → 12 "reading a behaviour" as it switched rule sets | Now: both panes read the same immutable snapshots — "BROADENED · Since 10:15 · Call writing broadens into higher strikes" (left and middle identical) | **Immutable reading snapshots shipped** (`server/kanida_pilot/snapshots.py`, `server/engine/run_engine.cjs`, `intelligence.db`): each reading written once per instrument from `oi_grid(at=T)` (its own spot/ATM/ten contracts, exact tokens kept) by the browser's own TypeScript run server-side; session context chained from the STORED previous snapshot, strikes matched by strike; INSERT OR IGNORE, no update path; engine+rules version on every row. Backfilled today: 1,080 snapshots (5 readings × 216). **Unreconstructable: IDEA & YESBANK every reading — grid builder returns no contracts for these sub-₹25 underlyings despite spot/ATM (pre-existing grid gap, not missing data; to investigate).** Engine 3.1 (key-strike "new activity" judged against the stored previous snapshot) written as its own version: 3.0 rows untouched — version separation proven on live data. Page poll cut 60 s → 10 s (a new reading reaches the screen ~2 min after its mark: capture ~40–50 s + metrics ~60–90 s + ≤10 s) |
| 10:45 | **PASS · Ask 2 live** | 27,319 rows, capture done 10:46:15 | Worker snapshotted 10:45 for 216 instruments at 10:47:06 IST (~1 min after metrics), then ran the audit | Panes read the stored snapshots | — | **Signal-to-noise audit shipped** (`server/kanida_pilot/signal_noise.py`, rules `sn/1`): claims recorded once per snapshot (INSERT OR IGNORE), outcomes APPENDED at 15m/30m/60m/EOD from later snapshots only, verdicts with computed reasons, a permutation baseline beside every confirmation rate, EOD report written to `var/sn_reports/SN_<date>.md` and served at `/api/derivatives/signal-noise`. First read (1,028 insights, 210 verdicts — the 09:45 cohort only, **small sample**): 15-min confirmation 30% vs 23% baseline; signal 56 / partial 58 / noise 71 / unresolved 25; top noise reasons small OI change 94, transient 79, flip-flop 39, reversed 36, ATM-shift artefact 22; **breadth ≥3, OI ≥1% and a named behaviour barely separate signal from noise (31–32% vs 25–28%)**; 121 side flip-flops |
| 11:00 | **PASS · engine fix 3.2** | 27,319 rows, 52.4 s; metrics 11:01:29 | 0 ΔOI / 0 Δprice mismatches; PCR/MP exact. **Worker snapshotted 216 instruments at 11:01:36 — 7 s after metrics, unattended** | Screen = snapshot; left and middle agree ("Put writing appears below ATM") | History showed "10:30 Call writing broadens → 10:45 Call **buying** narrows": premium had flipped, the chain treated it as one run → **engine 3.2**: a named behaviour changing starts a new run ("Call buying appears above ATM"). Rebuilt as its own version; 3.0/3.1 rows untouched. S/N: 1,226 records, 421 verdicts (signal 107 · partial 132 · noise 140 · unresolved 42) |
| — | **FINDING (root-caused, fix after close)** | IDEA, YESBANK | Grid empty every reading. **Not** the price floor and **not** missing data: live OI is captured for 10/10 contracts at every reading, but **0/10 have a previous-session close OI** (Friday's recovered afternoon), and the grid's ΔOI is measured against that close — so the whole day's ΔOI is null | Screen reads "Not enough readings captured yet" for them — **the wrong cause** | — | Should self-heal tomorrow (today's close will exist). Proper fix after close: the 15-min interval changes do not need the previous close (OI differences between readings are exact), and the empty message must name the real cause ("no previous-session close for these contracts") |
| 11:15 | PASS | 38.1 s; metrics 11:16:51 | 0 mismatches; no frozen instruments | snapshots 11:16:55 (4 s after metrics) | NIFTY "Put writing broadens into lower strikes · since 11:00" — first chained persistence from stored snapshots; evidence names calls covered (OI −19.9L, premium ↑) | S/N 1,049 verdicts across versions |
| 11:30 | PASS | 40.6 s; metrics 11:32:14 | 0 mismatches. Frozen OI: MANAPPURAM, NIFTYNXT50, SAIL, NIFTYFPI — verified **genuinely thin trading**, not stale capture (MANAPPURAM/NIFTYNXT50 had fresh option trades 11:24–11:25 inside the interval; OI simply did not move) | snapshots 11:32:21 | NIFTY "Put writing losing momentum below ATM · since 11:00" (OI +38L vs +51.6L prior) — correct SLOWED |  |
| 11:45 | PASS | 43.1 s; metrics 11:46:36 | 0 mismatches; thin: MANAPPURAM, NIFTYFPI, SAIL | snapshots 11:46:37 | NIFTY "Put buying appears below ATM" — premium turned up after put writing; the 3.2 rule correctly starts a new run | S/N 1,449 verdicts |
| 12:00 | PASS (+screen) | 40.9 s; metrics 12:01:59 | 0 mismatches | snapshots 12:02:06; **tab auto-advanced to 12:00 with no reload** (~1 h since last load); left = middle; history = stored states | NIFTY "Put writing appears below ATM" after "Put buying appears" at 11:45 — **behaviour-word flip-flop on small premium moves** (regime test ≥1% of premium level is noisy) → for EOD review, not changed live |
| 12:15 | PASS | 43.9 s; metrics 12:16:23 | 0 mismatches | snapshots 12:16:24 | NIFTY "Put buying appears below ATM" — third alternation of put buying/writing (11:45 → 12:00 → 12:15) |
| 12:30 | PASS | 36.8 s; metrics 12:31:46 | 0 mismatches | snapshots 12:31:48 | NIFTY "Call buying appears above ATM". Machine's Pacific date rolled over at this reading — no effect (capture, metrics, worker all key on IST from the data) |
| 12:45 | PASS | 40.6 s; metrics 12:47:12 | 0 mismatches; thin: MANAPPURAM, NIFTYFPI, SAIL | snapshots 12:47:12 | NIFTY "Earlier call buying is fading" with no evidence line — quiet after activity, stated sparsely (quiet-market behaviour as briefed) |
| 13:00 | PASS (+screen) | 38.9 s; metrics 13:01:37 | 0 mismatches | snapshots 13:01:38; screen auto-advanced to 13:00, left = middle | NIFTY "Call writing appears above ATM" (Call OI +37.4L, premium ↓; 23,500 CE leads) |
| 13:15 | PASS | 45.5 s; metrics 13:16:02 | 0 mismatches | snapshots 13:16:07 | NIFTY first **genuine MIXED**: "Call positions building while put positions reduce" (Put OI −17.4L vs Call +15.8L, 1.10×) — the corrected definition (sides contradict) in action |
| 13:30 | PASS | 37.6 s; metrics 13:31:27 | 0 mismatches; thin: MANAPPURAM, NIFTYFPI, NIFTYNXT50, SAIL | snapshots 13:31:36 | NIFTY "Put writing appears below ATM" (Put OI +36.2L, premium ↓) |
| 13:45 | PASS | 36.6 s; metrics 13:46:53 | 0 mismatches | snapshots 13:46:56 | NIFTY "Leading put activity shifts from 23,400 to 23,450 PE · since 13:30" — correct SHIFTED within a stored run |
| 14:00 | PASS data · **FIX engine 3.3** | 42.3 s; metrics 14:01:21 | 0 mismatches; thin (lunch): BANDHANBNK, INOXWIND, MANAPPURAM, NIFTYFPI, NIFTYNXT50, SAIL | screen on 14:00, left = middle | **"APPEARED · Call short covering appears" over "Call OI fell 81.6L" — a reduction painted green as a build.** Root cause: the chain only called a reduction REVERSING when the same side had been building before. Fix 3.3: a closing behaviour is always REVERSING (red); a continuing reduction keeps its persistence ("still being reduced"). Rebuilt 19 readings × 216 as its own version; now "REVERSING · Call positions being reduced above ATM". 453 frontend checks, 13 backend |
| 14:15 | PASS | 37.2 s; metrics 14:16:50 | 0 mismatches | snapshots 14:16:53 (engine 3.3, written live) | NIFTY "Call writing appears above ATM" (Call OI +64L, premium ↓; put OI +41.4L, premium ↑) |
| 14:30 | PASS | 39.8 s; metrics 14:32:17 | 0 mismatches; no frozen instruments | snapshots 14:32:20 | NIFTY "REVERSING · Call positions being reduced above ATM" — both sides trimmed ~6–7L (vs a 64L add at 14:15): a small move now headlined, a materiality-review case |
| 14:45 | PASS | 46.0 s; metrics 14:46:45 | 0 mismatches | snapshots 14:46:46 | NIFTY MIXED "Call positions building while put positions reduce" (Call +30.6L vs Put −27.4L) — genuine contradiction |
| 15:00 | PASS (+screen) · **FIX audit sn/1.1** | 42.2 s; metrics 15:02:15 | 0 mismatches | snapshots 15:02:15; screen on 15:00, left = middle | NIFTY "REVERSING · Put positions being reduced below ATM" — both sides closing (Put −34.9L, Call −29.2L) on the eve of the 22 Sep expiry | **Caught before it bit:** the audit closed the session at the first reading ≥ 15:30, but capture keeps a 15:45 mark — the EOD outcome would have been written one reading early and, being insert-only, never corrected. Fixed: close at 15:45, audit version sn/1.1 (earlier records untouched). Deployed 15:03:30 |
| 15:15 | PASS | 40.7 s; metrics 15:16:44 | 0 mismatches | snapshots 15:16:52 | NIFTY "REVERSING · Put positions still being reduced below ATM · since 15:00" (Put OI −1.2 Cr) — 3.3 continuing-reduction path verified live |
| 15:30 | PASS | **65.2 s** (closing volume; within 240 s grace); metrics 15:32:14 | 0 mismatches | snapshots 15:32:22 | NIFTY "REVERSING · Put positions being reduced below ATM" — both sides unwinding into the 22 Sep expiry (Put −1.7 Cr, Call −1.2 Cr); a new run, not "still", because put premium turned from falling to rising (3.2 rule) |
| 15:30–15:45 | PASS data · **FIX snapshots + audit sn/1.2** | 15:30 65.2 s; 15:45 (post-close) 59.1 s | 0 mismatches both | 15:45 is a **post_close** settlement capture, not a trading reading: the worker snapshotted it (0/216 usable) and the audit used it as the session's last reading, recording every EOD outcome unresolved. Fixed: worker skips post_close; EOD judged against the last valid snapshot; close tied to the 15:30 reading + 15:50 IST wall clock; audit sn/1.2 (sn/1.1 rows kept). Test added. EOD report regenerated: 4,624 insights, 0 pending |
