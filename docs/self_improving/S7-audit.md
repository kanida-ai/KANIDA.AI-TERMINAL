# S7 audit — build_autotrade_readiness.py (Auto-Trade Readiness, spec Table 14)

**Auditor:** AuditAgent-S7 (read-only). **Date (IST):** 2026-06-16.
**Target:** `universe_engine/self_improving/build_autotrade_readiness.py` (1419 lines) + `S7-build-log.md`.
**Method:** full read of target + build log; cross-checked confirmed live-DB data facts and `schema_self_improving.sql`.

---

## VERDICT: RED

One safety-critical defect on the decisive criterion (item #1): at the PICK level, NULL on
the REQUIRED-POSITIVE AUTO_TRADE criteria `repeater_type` and `signal_still_valid_at_open`
is treated as **"satisfied / pass"**, not as a blocker. Because both columns are 0%
populated in the live RND DB (confirmed fact), picks can be — and by construction WILL be —
labeled `AUTO_TRADE` despite *unconfirmed* repeater health and *unconfirmed* next-open
validity. For a real-money auto-trade gate this over-states readiness. This is the exact
RED trigger named in item #1 ("NULL treated as satisfied/pass").

Everything else (thresholds, precedence, no-fabrication, validation-gate honesty,
idempotency/safety, Excel structure) is correct. Fixing the NULL semantics flips this to GREEN.

---

## MUST-FIX (numbered)

1. **[RED — safety-critical] NULL must DOWNGRADE, not pass, on required AUTO_TRADE criteria at pick level.**
   `classify_pick`, line **687**:
   ```python
   rep_ok = repeater in ("FRESH", "HEALTHY_REPEATER") or repeater is None
   ```
   and line **617** / **653**:
   ```python
   sig_valid_open = pick.get("signal_still_valid_at_open")   # NULL today
   ...
   if sig_valid_open == 0:                                   # only 0 downgrades; None passes
   ```
   With both columns NULL (confirmed 0% populated), neither the WATCHLIST clause
   (`repeater == "PERSISTENCE_TRAP"`, `sig_valid_open == 0`) nor the MANUAL clause
   (`repeater == "STALE"`) fires, so a pick with otherwise-clean inputs falls through to
   `else: decision = "AUTO_TRADE"` (line **686**). The reason string then literally prints
   `repeater=NULL(assumed-ok)` (line 690) and `sig_valid_at_open=None` (line 693).
   **Fix:** an unknown REQUIRED-positive criterion must force *at least* `MANUAL_REVIEW`
   (i.e. a pick can only be AUTO_TRADE when repeater ∈ {FRESH, HEALTHY_REPEATER} **and**
   signal_valid is known-true). Add to the MANUAL (or WATCHLIST) tier:
   `if repeater is None: manual.append("repeater_type unknown -> cannot AUTO_TRADE")` and
   `if sig_valid_open is None: manual.append("signal_valid_at_open unknown -> cannot AUTO_TRADE")`.
   The `pick_time_unknowns` note (lines 671-677) already *records* the NULLs but does not
   *act* on them — recording is not gating.

2. **[Recommended, ties to #1] Source pick-level signal_valid from `falcon_signal_validity`.**
   `falcon_signal_validity` has `(signal_date, symbol, persona, valid_at_next_open)`
   (`schema_self_improving.sql:227-234`) and is POPULATED (10,079 rows, confirmed). The code
   only reads the always-NULL `baseline_trades.signal_still_valid_at_open` (line 617). Join
   `falcon_signal_validity.valid_at_next_open` on `(signal_date, symbol, persona)` so the
   criterion becomes LIVE instead of inert. Once #1 is fixed (NULL blocks), wiring this real
   source is what lets legitimately-valid picks reach AUTO_TRADE rather than all being
   downgraded. (Not RED on its own, but it is the natural other half of fixing #1.)

---

## Item-by-item findings

### #1 — NULL semantics on AUTO_TRADE required-positive criteria — **UNSAFE (RED)**
- Pick level (decisive): see MUST-FIX #1. `repeater is None` and `sig_valid_open is None`
  both pass through to AUTO_TRADE. Code cites: lines **615, 617, 645-654, 661-662, 685-694**.
- Pattern level: `signal_validity_next_day_pct` is taxonomy-sourced and POPULATED (confirmed),
  so the parallel `if sig_valid is not None and sig_valid < SIGNAL_VALID_MIN` (line **394**)
  is effectively live there; repeater/sector/capitulation are correctly deferred to pick level
  (lines 47-55, 408-409). The pattern level is NOT the failure — the pick level is.

### #2 — Pick-level signal_valid source — **inert; should join falcon_signal_validity** (see MUST-FIX #2)
Confirmed the populated alternative exists and is keyed identically. Recommended enrichment;
becomes load-bearing once #1 gates NULL.

### #3 — Table 14 thresholds + precedence — **CORRECT**
- Precedence most-restrictive-wins implemented as AVOID → WATCHLIST → MANUAL → AUTO at both
  levels (lines 410-420 pattern, 679-686 pick) and report order `DECISIONS` (line 133).
- Bands exact vs the brief: `MULT_AUTO_MIN=1.0`, `MULT_MANUAL_LO=0.8` → `[0.8,1.0)` MANUAL,
  `<0.8` WATCHLIST (lines 120-122, 383/398/643/657); `SIGNAL_VALID_MIN=60` (123),
  `INTRADAY_FP_MAX=30` with `>30` AVOID (124, 379/639); INIT_STOP `2+`→AVOID / `1`→MANUAL
  (127-128, 373/400, 635/659); constitutional 2+ INIT_STOP in 60d → AVOID (129, 284-309).
- HFCL: `maturity=insufficient_data OR n_resolved<18 → WATCHLIST` (lines 119, 385-388 pattern;
  647-650 pick, using min fired-pattern n_resolved). Matches the HFCL rule.

### #4 — No fabrication — **CORRECT**
Every unavailable criterion is named, never invented: pattern-level pick-time note (408-409,
428); pick-level `pick_time_unknowns` lists `repeater_type NULL` / sector-all-NULL /
`signal_still_valid_at_open NULL` (671-697). capitulation% fully deferred — `CAPITULATION_MAX`
is defined (125) but never used in any branch (verified: no reference in classify_pattern or
classify_pick), consistent with "DEFERRED, never fabricated." This is honest — but note it is
the *recording* of NULLs that is correct; the *gating* on them (item #1) is what fails.

### #5 — Pick-validation gate honesty — **HONEST**
`build_pick_validation` (768-834): groups CLOSED picks (net_ret_pct or net_pnl NOT NULL) by
decision → n/WR/avg_net_ret/sum_pnl/%winners, plus ALL_TOP10 row (796-799). "Concentrates
winners" claimed only if `wr_lift > 0 AND ret_lift >= 0` (line **817**); otherwise prints the
honest "RISK filter, not a return amplifier" verdict (860-863). Labeled in-sample in the
workbook note and build log §5. No inflated claim.
- Caveat (not a defect, but flows from #1): if #1 is left unfixed, the AUTO_TRADE group is
  populated by NULL-pass picks, so the gate would be validating a wrongly-defined group.
  Fixing #1 first makes this gate meaningful.

### #6 — Idempotent + safe — **CORRECT**
ALTER PRAGMA-guarded, added only if absent (176-192); UPDATE-in-place by PK, re-runnable
(1041-1068); single `BEGIN…commit` with rollback on error (1357-1365); RND-only — PROD never
opened (16, 1289); `--dry-run` computes/prints everything then `con.rollback()` and writes no
DB and no Excel (1343-1354, returns before the write block). No PROD/shared-code mutation.

### #7 — Excel correctness — **CORRECT**
3 workbooks: readiness {by_pattern, pick_validation, by_pick} (1132-1164); early_exit
{per-pattern D1..D60 + optimal_hold + ALL_PATTERNS summary} (1201-1238); weekly_review
{latest_review + changed_patterns} (1241-1280). early_exit joins signal_day_study ↔
contributions on `(signal_date, symbol)` across the two personas (884-904); `optimal_hold_day`
= argmax avg cumulative close_ret (line **946**); HFCL n_occ≥18 gate on the recommendation
(945, 961). openpyxl with CSV fallback throughout.

---

## Explicit answers (for the orchestrator)

**(a) What NULL does to AUTO_TRADE eligibility — UNSAFE PASS, not safe downgrade.**
At pick level, `repeater is None` is explicitly OR'd into `rep_ok` (line 687) and
`signal_still_valid_at_open` NULL only downgrades on `== 0` (line 653) — NULL slips through to
the `else: AUTO_TRADE` branch (line 686), tagging the pick `repeater=NULL(assumed-ok)`. Since
both columns are 0% populated, this is not hypothetical: every otherwise-clean pick is
over-granted AUTO_TRADE. This is the RED driver.

**(b) Should signal_valid be sourced from falcon_signal_validity? — YES.**
`baseline_trades.signal_still_valid_at_open` is 0% populated (inert); `falcon_signal_validity
.valid_at_next_open` is populated and keyed on `(signal_date, symbol, persona)`. Join it.

**(c) Live vs inert criteria.**
- PATTERN level LIVE: multiplier (weekly_state populated), quality_flag, pattern_maturity,
  big_loser_risk, signal_validity_next_day_pct, intraday_false_positive_rate (all taxonomy,
  populated), INIT_STOP recency/constitutional (from contributions⋈baseline). INERT/deferred:
  repeater, sector regime, capitulation (correctly pick-time-only).
- PICK level LIVE: multiplier (mean of fired weekly_state — populated), worst-of-fired
  taxonomy attrs, sector-headwind via `move_type`/`sector_tailwind` fallback (populated),
  INIT_STOP history. INERT (always-NULL): repeater_type, signal_still_valid_at_open,
  pattern_weight_multiplier, sector_regime_on_signal_date. capitulation% fully deferred.
  → The two inert REQUIRED-positive criteria (repeater, signal_valid) are exactly the ones
  mishandled in item #1.

**(d) Is the validation gate honest? — YES.**
WR-and-return double condition, in-sample labeled, honest "risk filter" fallback. Its only
weakness is downstream of #1: it scores a wrongly-defined AUTO_TRADE group until #1 is fixed.

---

## Note (limitation, NOT the RED trigger)
The data gap itself (repeater/capitulation unavailable upstream) is a noted, documented
limitation, not RED. The RED is purely that the gap is handled by *over-granting* (NULL=pass)
instead of *downgrading* (NULL=cannot-AUTO_TRADE) on a real-money gate. Two-line fix in
`classify_pick`; optional follow-up to wire `falcon_signal_validity`.

---

## Re-audit (fixes)

**Auditor:** AuditAgent-S7b (read-only on code; this file is the only write). **Date (IST):** 2026-06-16.
**Target:** `universe_engine/self_improving/build_autotrade_readiness.py` (1616 lines, grew from 1419).
**Method:** full re-read of the target (both pages), cross-checked `schema_self_improving.sql`,
`build_baseline.py`, and the live column names the new code depends on. No code run.

### VERDICT: GREEN

The RED defect is fixed. NULL on a required-positive AUTO_TRADE criterion can no longer
reach AUTO_TRADE at either the pick or the pattern level. Dominant-pattern aggregation is a
defensible, bounded design choice that does not disable the safety gates. The validation
cohorts and verdicts are honest, n-labeled, and in-sample disclosed. Idempotency / RND-only /
dry-run-writes-nothing all still hold. No new bug, crash, fabrication, or PROD write found.

### Item-by-item verification (cite file:line)

**#1 — Pick-level NULL semantics (the RED fix) — FIXED.**
`classify_pick` now splits the required positives into `auto_failed` (definitive) and
`auto_unconfirmed` (unknown):
- repeater (lines **755-759**): not in {FRESH,HEALTHY_REPEATER} → if None → `auto_unconfirmed`,
  else value → `auto_failed`.
- sector (lines **761-765**): `not (has_alpha or is_headwind is False)` → True → `auto_failed`;
  unknown (None) → `auto_unconfirmed`.
- signal_valid (lines **766-769**): `!= 1` and None → `auto_unconfirmed`; `== 0` already routed
  to WATCHLIST at **721**.
The `else: AUTO_TRADE` branch (line **787**) fires **only** when `avoid`, `watch`, `manual`,
`auto_failed` AND `auto_unconfirmed` are all empty (precedence chain **775-787**). Therefore any
NULL on the three required positives lands in `auto_unconfirmed`, forcing MANUAL_REVIEW
(**781-786**). **Confirmed: NULL cannot reach AUTO_TRADE at pick level.** Matches the dry-run
(per-pick AUTO_TRADE = 0, driven by 0%-populated repeater).

**#2 — signal_valid sourced from the populated table — CORRECT.**
`_load_signal_validity_map` (lines **564-579**) reads `falcon_signal_validity.valid_at_next_open`
filtered by persona `falcon_top10_signal_validity`, keyed `(signal_date, symbol)`. `classify_pick`
falls back to it (lines **681-685**) when `baseline_trades.signal_still_valid_at_open` is NULL.
Semantics correct: `0 → WATCHLIST` (721), `1 → pass`, `None → auto_unconfirmed` (768-769).
Schema cross-check: `falcon_signal_validity (signal_date, symbol, persona, …, valid_at_next_open INTEGER)`
at `schema_self_improving.sql:227-234` — key + column confirmed.

**#3 — Pattern-level NULL fix — FIXED.**
`classify_pattern` builds `auto_unconfirmed` for `sig_valid is None` (line **413**) and
`intraday_fp is None` (line **416**); precedence routes a non-empty `auto_unconfirmed` to
MANUAL_REVIEW (lines **430-433**), never the AUTO_TRADE else (434-442). **Confirmed: the old
"NA>=60 → AUTO_TRADE" path is closed; NULL cannot reach AUTO_TRADE at pattern level.**

**#4 — Dominant-pattern aggregation — SAFE; risk gates intact.**
`_dom(attr, default)` (lines **660-664**) selects, among fired patterns that actually HAVE the
attr (non-None), the value belonging to the pattern with the highest `oos_lift`; returns
`default` when none qualify. Empty / all-None fired lists are handled (`cands` empty → default).
`oos_lift` None is guarded by `or 0.0` so `max(...)` never compares None. `oos_lift` is loaded
from `falcon_promoted_patterns.avg_oos_year_lift_pp` (`_load_oos_lift`, **547-561**) — column
verified real (same column read in `build_baseline.py:355`), and the loader is wrapped in
`try/except sqlite3.OperationalError` returning `{}` if the table/column is absent (tolerant).
Used only for `quality_flag`, `pattern_maturity`, `intraday_fp`, `n_resolved`. Genuine risk
gates are NOT subject to dominant-selection:
- `big_loser_risk` stays **any-of** (lines **670-671**) — one flagged fired pattern raises it.
- recent INIT_STOP and constitutional 60d are computed **across the full stock+pattern
  history of every fired pattern** (lines **687-696**), worst-wins, not dominant.
The one place dominant-selection touches a hard AVOID gate is `intraday_fp` (AVOID at **707** if
`>30`): a thin co-pattern with FP>30 will not force AVOID if the dominant pattern's FP is clean.
This is the documented, intentional trade-off (a pick's edge is its dominant pattern, not its
noisiest co-pattern; min-across-fired WATCHLISTed ~74% of picks — code comment **651-659**).
Judgement: this does not mask the *constitutional / INIT_STOP / big_loser* safety gates (those
remain any-of / worst-of), and intraday_fp is a quality threshold rather than a hard-stop
constitutional rule, so dominant-selection on it is defensible and disclosed. **Not a defect.**

**#5 — Validation cohorts — HONEST.**
`TRADEABLE_ex_AVOID` (lines **918-919**, **946**) = closed picks with `decision != AVOID`.
`AUTO_TRADE_PENDING_REPEATER` (line **913-914**, def at **771-773**) = passes all recorded
gates with the *sole* unconfirmed item being the never-recorded repeater — flag definition
matches its use exactly. `gate_removes_losers` (lines **953-954**) requires `avoid_wr < all_wr`
AND `trad_wr_lift > 0` — a real two-part risk-filter test, not a return-amplifier overclaim.
Small-n cohorts are printed with their n (PENDING `pend_n` shown, **1046-1052**; the print does
not suppress n=3) and the headline `concentrates_winners` is gated on WR-lift>0 AND
ret-lift>=0 (**936-938**). The AUTO_TRADE=0 case prints an explicit data-gap NOTE (**1033-1037**),
not a fabricated verdict. All verdicts labeled in-sample. **Honest.**

**#6 — Format-crash fix — CONFIRMED.**
`print_pick_validation` uses `'%.0f'` for `sum_net_pnl` (line **1010**). Repo-wide search for the
`%,`-comma flag returned no matches in the file. No remaining comma-flagged `%`-format.

### No new bug / safety regressions
- `_dom` empty & all-None handled; `oos_lift` None-safe (`or 0.0`); loader table-tolerant. ✓
- `else: AUTO_TRADE` reaches only when all five lists empty (pick **775-787**, pattern **421-434**). ✓
- `pending_repeater` (771-773) is exactly "only-repeater-unconfirmed" — matches its definition. ✓
- Idempotent ALTER PRAGMA-guarded, add-only (**187-193**); UPDATE-in-place by PK (**1243-1265**);
  single `BEGIN…commit` with rollback-on-error (**1554-1562**); `--dry-run` rolls back and
  returns before any write (**1540-1551**); RND-only — only `sqlite3.connect(rnd_db)` opened
  (**1491**), PROD never opened. ✓
- Excel writers: new extras keys (`sig_valid_src`, `auto_pending_repeater`, `oos_lift`) are
  ignored by the fixed header lists; new validation rows (`TRADEABLE_ex_AVOID`,
  `AUTO_TRADE_PENDING_REPEATER`, `ALL_TOP10`) read only generic keys present on every row via
  `_pick_val_row` (**1311-1313**). No KeyError surface. ✓

### Non-blocking note (NOT a defect, outside this file's write scope)
`S7-build-log.md` still documents the pick-level pattern attributes as "WORST of fired
patterns" (line 40) and the NULL-as-not-triggered handling (§4 / §Limitations) — both now stale
versus the shipped code (dominant-pattern selection + NULL-blocks-AUTO_TRADE). Recommend the
orchestrator refresh the build log to match; it does not affect runtime correctness or safety.

### Answers for the orchestrator
- **(a)** NULL can no longer reach AUTO_TRADE at the pick level (required positives route NULL
  to `auto_unconfirmed` → MANUAL_REVIEW; AUTO else fires only when all five gate-lists are
  empty) OR at the pattern level (sig_valid/intraday_fp None → `auto_unconfirmed` → MANUAL).
- **(b)** Dominant-pattern is safe: constitutional / INIT_STOP / big_loser_risk gates remain
  any-of / worst-of-all-fired; dominant-selection only governs quality/maturity/intraday_fp/
  n_resolved, and `_dom` is empty/None-safe.
- **(c)** Validation is honest: cohorts are correctly defined and n-labeled, verdicts are
  two-part and in-sample-disclosed, and the AUTO_TRADE=0 / pending=3 outcomes are surfaced as
  data gaps, not overclaimed.
