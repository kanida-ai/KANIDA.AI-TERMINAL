# TEST_PLAN — KANIDA.AI

Owner session: `qa` (see `docs/DOC_MAP.md`). Continuous — every build session appends the rows for
what it built. **Rule: no requirement without a test row.**

| Slice | Rows | Suite | Status |
|---|---|---|---|
| **Pathfinder P0** (contract + mock) | below | `backend/tests/test_pathfinder_p0.py` | **37 passing** |
| **Pathfinder P1** (engine) | below | `backend/tests/test_pathfinder_p1.py` | **60 rows** |
| **Pathfinder P2** (frontend, mock source) | below | `kanida-app/tests/{lib,contract.p0}.test.ts` | superseded by S3 for the Pathfinder surface; P0 contract rows kept for the mock source |
| **Pathfinder S3** (the swipeable feed) | below | `kanida-app/tests/{lib,feed,contract}.test.ts` | **33 unit + 16 contract passing** |
| **Pathfinder S1** (research engine + feed) | below | `backend/tests/test_pathfinder_s1.py` + `test_pathfinder_s1_audit.py` + `test_pathfinder_s1_audit2.py` + `test_pathfinder_s1_audit3.py` | **34 + 39 + 28 + 33 passing** |
| **Pathfinder S2** (experiment loop) | below | `backend/tests/test_pathfinder_s2.py`, `backend/tests/test_pathfinder_s2_audit2.py` | **63 passing** (34 + 10 audit pins + 19 re-audit pins) |
| Trader slice | *to be appended by session 01/S1.1* | — | not started |

Run:

```bash
python -m pytest backend/tests/test_pathfinder_p0.py -q          # P0
cd kanida-app && npm run test:all                                # P2 (needs the mock on :8010)
```

---

## Pathfinder P0 — contract + honest mock

Requirements traced: PRD **R5** (Pathfinder runner: hypothesis-as-book, status lifecycle, mandatory
post-mortem), **R17** (digest + graveyard), **R4** (book conventions, expectancy + drawdown, 2×
slippage), **R3** (append-only, reproducible), plus the CLAUDE.md non-negotiables, which are tested
as first-class rows because they are what makes this product honest.

Every row runs against the **served payload** (through the router), not the fixture object — the
contract is tested the way a client experiences it.

| # | Requirement / law | Test | Assertion |
|---|---|---|---|
| P0-01 | R5 — the four endpoints exist and serve | `test_all_endpoints_serve` | All 9 URLs (4 endpoints + 6 experiment ids) return 200. |
| P0-02 | R5 — status lifecycle is complete | `test_every_lifecycle_status_has_a_fixture` | Each of queued/testing/validating/promising/promoted/died has ≥1 fixture. |
| P0-03 | R5 — the status filter works | `test_status_filter_narrows` | `?status=died` returns only died and fewer than the unfiltered list. |
| P0-04 | Guarded errors | `test_unknown_experiment_is_a_clean_404` | Unknown id → 404 `{"error":{code,message,request_id}}`, no other keys. |
| P0-05 | Guarded errors | `test_malformed_experiment_id_is_a_clean_400` | `DROP-TABLE` as an id → 400 `invalid_experiment_id`. |
| P0-06 | Guarded errors leak nothing | `test_error_bodies_leak_nothing` | No traceback, SQL, driver name, module path or filesystem path in any error body. |
| P0-07 | **Expectancy is the hero / return paired with drawdown** | `test_every_performance_block_pairs_return_with_drawdown` | Every performance block carries expectancy, the 2× slippage expectancy, max drawdown and current drawdown. |
| P0-08 | Win rate is never the headline | `test_win_rate_is_never_the_only_metric` | Wherever `win_rate_pct` appears, expectancy appears too. |
| P0-09 | **No return promises** | `test_no_promise_phrases_anywhere` | 18 banned phrases absent from every payload (explicit negations — "there is no price target" — are allowed and tested for). |
| P0-10 | **No target prices** | `test_no_target_or_projection_fields_anywhere` | No field named target / target_price / expected_return / projection / forecast / upside anywhere. |
| P0-11 | Invalidation replaces the target | `test_every_rulebook_has_an_invalidation_not_a_target` | Every rulebook has a non-empty `invalidation`. |
| P0-12 | **n + date range + source + cost convention on every number** | `test_every_number_carries_source_range_asof_and_costs` | ≥20 provenance blocks, each complete. |
| P0-13 | **Point-in-time — no look-ahead** | `test_no_provenance_looks_ahead` | `date_range.end <= as_of` on every provenance; windows are ordered. |
| P0-14 | **The LLM never calculates** (provenance leg) | `test_nothing_is_computed_by_a_model` | No `computed_by` contains claude/gpt/gemini/sonnet/haiku/opus/llm. |
| P0-15 | **Entry = next open after the signal bar** | `test_entries_are_the_next_session_after_the_signal` | ≥25 trades, each with `entry_date > signal_date` and a non-null `slippage_bps`. |
| P0-16 | **n<20 greyed, n<50 flagged** | `test_sample_flags_match_the_rule` | Every `sample_flag` recomputed from `n` and compared. |
| P0-17 | Fixtures are honest about small samples | `test_fixtures_are_honest_about_small_samples` | At least one greyed and one flagged case exist. |
| P0-18 | **Losers first** | `test_every_ledger_is_losers_first` | Every ledger is ascending by net P&L and leads with a loser. |
| P0-19 | Losers first, at list level | `test_experiment_list_leads_with_the_struggling_ones` | The died experiment leads the list; the promoted one is last. |
| P0-20 | **The LLM never calculates** (prose leg) | `test_llm_authored_prose_contains_no_numerals` | ≥25 LLM-authored lines; none contains a digit once `{{…}}` tokens are stripped. |
| P0-21 | Fact references resolve | `test_every_fact_reference_resolves` | Every `{{fact:…}}` used resolves against `facts[]` and is declared in `fact_refs`. |
| P0-22 | Model attribution | `test_llm_lines_name_their_model_and_engine_lines_do_not` | LLM lines name a model from the locked routing; engine lines name none. |
| P0-23 | **Backtests are labelled** | `test_every_performance_block_carries_its_honesty_label` | `historical_replay` → "Simulated · Not traded · Not PaRRVA-verified"; `virtual_book` → the virtual-money label. |
| P0-24 | Disclosure on every response | `test_every_response_carries_the_research_disclosure` | Every payload carries the research disclosure. |
| P0-25 | R5 — **post-mortem required on death** | `test_died_experiment_publishes_a_post_mortem` | `exp_0007` has a cause, evidence and a non-empty "what we kept". |
| P0-26 | Post-mortems only on death | `test_only_died_experiments_carry_a_post_mortem` | No live experiment carries one. |
| P0-27 | Change-log format | `test_change_log_records_what_why_evidence_and_versions` | Every entry has what/why/evidence/new_version/level/improved; **L3 states its validation; L4 is human-decided and human-approved**. |
| P0-28 | R3 — append-only | `test_change_log_is_append_only` | `seq` strictly increasing and unique per experiment. |
| P0-29 | A queued experiment invents nothing | `test_queued_experiment_shows_no_results` | `historical_return`, `virtual_return`, `n`, `spark`, `virtual_book` all null. |
| P0-30 | Autonomy = event-driven | `test_triggers_are_fired_by_the_engine_not_a_clock` | Every trigger's `fired_by` is `engine`. |
| P0-31 | The loop is a story | `test_loop_tells_the_six_beats_in_order` | Beats are exactly noticed → hypothesis → experiment → outcome → learning → next. |
| P0-32 | Token instrumentation from day 1 | `test_loop_reports_real_token_usage` | `llm_usage` has per-model token counts, a cost, a daily budget, and only routed model ids. |
| P0-33 | An honest research agenda | `test_learnings_show_what_is_blocked_too` | At least one "testing next" item names what blocks it. |
| P0-34 | **Fixtures are not flattering** | `test_some_experiments_fail_the_2x_slippage_gate` | At least one performance block has non-positive expectancy at 2× slippage. |
| P0-35 | **Fixtures are not flattering** | `test_forward_is_not_uniformly_better_than_history` | Forward expectancy is worse than historical in ≥3 experiments. |
| P0-36 | Contract ↔ code cannot drift | `test_openapi_yaml_is_in_sync_with_the_models` | `scripts/gen_openapi.py --check` exits 0. |
| P0-37 | The contract is valid | `test_openapi_yaml_validates` | `scripts/validate_openapi.py` exits 0 (OpenAPI 3.1, all `$ref`s resolve, no orphan schemas, every operation has 200/400/404/500, no banned fields, `PerformanceBlock` requires expectancy + both drawdowns and does **not** require win rate). |

### Known gap for P1

The Postgres DDL (`migrations/0001_pathfinder.sql`) has **no test row** — no database was reachable
in P0. P1 must add: *"the migration applies cleanly to an empty database, and each product-law CHECK
rejects the row it is meant to reject"* (append-only, look-ahead, entry-after-signal, L4-human-only,
L3-validation, LLM-prose-has-no-numerals, parameter-within-range).

---

## Pathfinder P1 — the engine (`backend/tests/test_pathfinder_p1.py`)

```bash
python scripts/run_pathfinder_loop.py --fresh --llm recorded   # produces var/pathfinder.db
python -m pytest backend/tests/test_pathfinder_p1.py -q        # 60 rows
```

P0's rows tested the **contract** against fixtures. These test the **engine**. Rows P1-46…P1-58 run
against the real run database, so the honesty rules are asserted on engine output rather than on
anything hand-written; they skip loudly if the loop has not been run.

### Point-in-time — the seal is mechanical, not a convention

| # | Guards | Test | Passes when |
|---|---|---|---|
| P1-01 | **No look-ahead** | `frame_cannot_hold_a_bar_after_its_as_of` | Constructing a `PriceFrames` whose data extends past its `as_of` raises. |
| P1-02 | **No look-ahead** | `reseal_can_only_narrow` | `sealed_at` narrows; re-sealing forward raises. |
| P1-03 | **No look-ahead** | `window_end_after_as_of_is_refused` | Asking for a window past the seal raises. |
| P1-04 | **No look-ahead (the silent one)** | `expanding_percentile_never_looks_forward` | A reading is ranked only against sessions strictly before it; the full-sample rank of the same series differs. |
| P1-05 | **No look-ahead** | `replay_window_after_the_seal_is_refused` | A replay window ending past the frame's seal raises. |

### Marking conventions — the evidence is the strategy that is traded

| # | Guards | Test | Passes when |
|---|---|---|---|
| P1-06 | **Entry = next open** | `entry_is_the_next_open_never_the_signal_bar` | Every trade's `entry_date > signal_date` and `entry_price` equals the next session's open. |
| P1-07 | **Costs on every trade** | `costs_are_charged_to_every_closed_trade_winners_included` | `net == gross − round-trip cost` on every trade, winners included. |
| P1-08 | Backtests do not flatter | `same_session_stop_and_target_resolves_as_the_stop` | A bar spanning both levels exits at the **stop**. |
| P1-09 | Backtests do not flatter | `a_gap_through_the_stop_fills_at_the_open` | A gap-down exit fills at the open, worse than the stop level. |
| P1-10 | **`entry_idx + horizon ≤ today_idx`** | `a_signal_too_close_to_the_seal_is_not_evidence` | Signals within `horizon` of the frame end are unresolved and dropped. |
| P1-11 | **2× slippage gate** | `two_times_slippage_is_exactly_two_extra_slippage_sides` | The 1×/2× gap equals exactly two extra slippage sides, and 2× is always lower. |
| P1-12 | Drawdown is real | `drawdown_is_reported_positive_and_from_the_running_peak` | Max and current drawdown from the running peak, both positive. |

### Selection discipline — the discovery window is the only one that may choose

| # | Guards | Test | Passes when |
|---|---|---|---|
| P1-13 | **Discovery-only selection** | `discovery_scan_refuses_a_frame_that_can_see_past_the_window` | Handing the scan an unsealed frame raises before anything is ranked. |
| P1-14 | **L2 ranges** | `out_of_range_parameters_are_rejected_never_clamped` | An out-of-range parameter raises; it is never silently clamped into range. |
| P1-15 | **Closed library** | `unknown_primitives_and_contexts_are_rejected` | A model cannot invent a feature, a context, or a direction. |
| P1-16 | Closed library | `a_valid_proposal_parses_and_keeps_integer_parameters_integral` | A lookback stays an integer; the rulebook carries an invalidation. |

### Governance — L1–L4

| # | Guards | Test | Passes when |
|---|---|---|---|
| P1-17 | Implementability | `multi_session_shorts_are_not_implementable` | A multi-session cash short fails the gate; long and single-session pass. |
| P1-18 | **Promotion is human** | `promotion_is_blocked_while_the_constitution_is_unsigned` | The promotion gate fails while `approved_by` reads UNSIGNED. |
| P1-19 | **L4 is human-only** | `l4_is_human_only` | An engine-decided L4 change raises. |
| P1-20 | **L3 must validate** | `l3_must_state_its_validation` | An L3 change without stated validation raises. |
| P1-21 | Every change cites evidence | `every_change_must_cite_evidence` | A change-log entry with no evidence raises. |
| P1-22 | **L2 ranges** | `l2_parameter_check_raises_rather_than_clamping` | In-range passes, out-of-range raises. |
| P1-23 | **The LLM never calculates** | `provenance_refuses_to_name_a_model_or_look_ahead` | `computed_by` naming a model raises; `range_end > as_of` raises. |

### The gateway's output contract

| # | Guards | Test | Passes when |
|---|---|---|---|
| P1-24 | **The LLM never calculates** | `llm_prose_may_not_contain_a_bare_numeral` | A digit outside a `{{fact:…}}` token raises; the token form passes. |
| P1-25 | The LLM never calculates | `a_narrate_headline_may_not_contain_a_digit_at_all` | Any digit in a headline raises. |
| P1-26 | No invention | `a_model_may_not_reference_a_fact_it_was_not_given` | An unsupplied fact id raises. |
| P1-27 | Resolvable narrative | `fact_refs_used_in_prose_must_be_declared` | A token used but undeclared raises. |
| P1-28 | Closed sets | `classify_may_not_invent_a_category` | A label outside the closed set raises. |
| P1-29 | **L4 unrepresentable** | `reason_cannot_express_a_constitution_change` | `L4` is absent from the schema enum and rejected at enforcement. |
| P1-30 | Cache correctness | `the_fact_table_handed_to_a_model_is_deterministically_ordered` | Two orderings of the same facts render identically. |
| P1-31 | **Hard daily cap** | `the_daily_budget_is_checked_before_the_call` | A spent ledger raises `BudgetExceeded` *before* any call. |
| P1-32 | Real metering | `cost_comes_from_token_counts_at_the_published_rates` | Cost matches the published per-MTok rates; an unknown model raises. |
| P1-33 | **Locked routing** | `the_job_not_the_caller_picks_the_model` | The routing table matches the Constitution and only names the three permitted models. |
| P1-34 | **Autonomy, not a clock** | `the_loop_refuses_to_wake_a_model_without_a_trigger` | `_wake` without a `trigger_id` raises. |

### Laws the database enforces on its own

Closes the gap P0 flagged: *each product-law CHECK rejects the row it exists to reject.*

| # | Guards | Test | Passes when |
|---|---|---|---|
| P1-35 | **Append-only** | `history_tables_reject_update` | `UPDATE` raises. |
| P1-36 | **Append-only** | `history_tables_reject_delete` | `DELETE` raises. |
| P1-37 | **The LLM never calculates** | `a_fact_may_not_claim_a_model_computed_it` | `computed_by = 'claude-sonnet-5'` is rejected by the DB. |
| P1-38 | **Point-in-time** | `a_fact_may_not_be_computed_from_data_after_its_as_of` | `range_end > as_of` is rejected. |
| P1-39 | **The LLM never calculates** | `llm_prose_with_a_numeral_is_rejected_by_the_database` | The token form inserts; a bare numeral is rejected. |
| P1-40 | **Entry = next open** | `a_trade_must_enter_after_its_signal_bar` | `entry_date <= signal_date` is rejected. |
| P1-41 | **Costs** | `a_closed_trade_without_costs_is_rejected` | A closed trade with no `costs_pct`/`pnl_pct_net` is rejected. |
| P1-42 | **No target prices** | `a_rulebook_may_not_contain_a_target_price` | A rulebook carrying `target_price` is rejected. |
| P1-43 | **L3 / L4** | `l4_and_l3_are_enforced_by_the_database_too` | Engine-authored L4 and unvalidated L3 are both rejected. |
| P1-44 | **L2 ranges** | `a_parameter_outside_its_approved_range_is_rejected` | Out-of-range parameter rejected by CHECK. |
| P1-45 | **Hash chain** | `the_hash_chain_detects_a_rewritten_row` | Tampering via a connection with the trigger dropped is still detected. |

### The served payload, from the REAL run

| # | Guards | Test | Passes when |
|---|---|---|---|
| P1-46 | The contract holds for engine data | `every_endpoint_serialises_under_the_p0_contract` | All four P0 responses serialise from engine rows, unchanged. |
| P1-47 | **The LLM never calculates** | `no_llm_authored_line_in_the_real_run_contains_a_numeral` | Every `produced_by='llm'` line is digit-free once tokens are stripped. |
| P1-48 | **The LLM never calculates** | `no_number_in_the_real_run_claims_a_model_computed_it` | 20+ provenance blocks, none naming a model. |
| P1-49 | **Point-in-time** | `no_number_in_the_real_run_was_computed_past_its_as_of` | Every `date_range.end <= as_of`. |
| P1-50 | **Entry + costs** | `every_real_trade_enters_after_its_signal_and_is_costed` | Every book trade enters later than its signal, with costs and slippage. |
| P1-51 | **Losers first** | `the_ledger_is_losers_first` | Every ledger ascending by net P&L. |
| P1-52 | **Losers first** | `the_experiment_list_leads_with_the_graveyard` | The list leads with `died`. |
| P1-53 | Resolvable narrative | `every_fact_reference_in_the_real_run_resolves` | No dangling `{{fact:…}}` anywhere. |
| P1-54 | Death is published | `a_dead_experiment_publishes_a_post_mortem_that_kept_something` | Cause, evidence and a substantive "what we kept". |
| P1-55 | **Promotion is human** | `nothing_is_promoted_while_the_constitution_is_unsigned` | No experiment is `promoted`. |
| P1-56 | **Expectancy is the hero** | `every_performance_block_leads_with_expectancy_and_carries_a_drawdown` | Expectancy + both drawdowns present; 2× slippage always lower. |
| P1-57 | **Locked routing** | `a_live_llm_call_used_the_model_its_job_routes_to` | Every `provider='live:anthropic'` row matches the routing table; every row names a permitted model. |
| P1-58 | **Append-only, provable** | `every_hash_chain_in_the_real_run_is_intact` | All six chained tables verify. |
| P1-59 | Engine data serves | `the_endpoints_serve_engine_data_over_http_from_any_thread` | Several endpoints in sequence over HTTP — a thread-bound connection served only the first. |
| P1-60 | Guarded errors | `an_unknown_experiment_returns_a_guarded_error` | 404 with `{error:{code,message}}`; no stack trace, SQL or path in the message. |

### What the quant audit changed

`dev-quant-auditor` was run adversarially against the engine, the run database and the price
warehouse before this session was declared done. It confirmed the point-in-time seal, the exit
arithmetic (hand-checked on real trades including the gap-through-stop case), the availability
rule, the window separation, the cost arithmetic, the book's cash accounting, and the exact
reproducibility of every published number. It also found four things that were wrong, and the
rows above now cover all four:

| Finding | What was wrong | Now |
|---|---|---|
| Placebo null mis-specified | i.i.d. draws against day-clustered signals; null spread understated ~4× | `block_placebo` + `cluster_robust_t`; P1-11a/P1-04 unchanged, the gate rows rewritten |
| Attribution overclaimed | the capacity gap was blamed on the tie-break; the refuting numbers were computed and discarded | decomposition measured and published; the claim is conditional |
| Universe note wrong | described 1,562 symbols with 1 delisting; the engine queries 498 with 0 | `SURVIVORSHIP_NOTE` rewritten to the filter that actually runs |
| `total_return_pct` on a replay | a cumulative unit-stake sum published as a return (3,866%) | `None` for `historical_replay`; asserted by P1-56 |

### Known gaps for P1

1. **The Postgres DDL is still unexecuted.** P1-35…P1-45 prove the laws against the **SQLite mirror**
   (`migrations/0002_pathfinder_sqlite.sql`), because no Postgres was reachable (no driver installed,
   and the local instance needs a password this session will not supply). `0001_pathfinder.sql`
   remains untested. Whoever gets a scratch database should re-run these rows against it — they are
   written to port.
2. **No live-model row.** P1-57 is written to catch a routing violation but there were no
   `provider='live:anthropic'` rows to catch it on: this run used the recorded provider. The row will
   start doing work the moment a key is configured.
3. **One unexplained observation, now guarded.** Twice during the build, a `run_book` / `replay` call
   returned zero signals where an identical call moments later returned the correct number — both
   times on the first run after a source file had just been patched in place. Three consecutive clean
   runs did not reproduce it, and the root cause was not established. `run_book` now raises rather
   than returning a silent empty book, so the failure mode is a crash instead of an optimistic zero.


---

## Pathfinder S1 — the research engine + clarity-first feed (`backend/tests/test_pathfinder_s1.py`)

Requirements traced: the LOCKED spec `docs/sessions/PATHFINDER.md` (12 principles, 7 addenda) and
the S1 brief's "done when". Rows S1-01…S1-33 run on a **synthetic fat-tailed universe** where every
bar is under the test's control; S1-34 runs on the **real warehouse** and skips when it is absent.

```bash
python -m pytest backend/tests/test_pathfinder_s1.py -q        # 34 rows, ~20s with the warehouse
python -m pytest backend/tests/test_pathfinder_s1_audit.py -q  # 39 audit regressions, ~50s with the warehouse
python scripts/run_pathfinder_scan.py --backfill 12            # the real store the feed serves (append-only;
#   to rebuild: --archive-store --i-understand-this-archives-the-store — the old store is renamed, never deleted)
```

| Row | Asserts | Law |
|---|---|---|
| S1-01 | forward outcomes `f1/f3/f5/r5cc` are NaN in the last *h* rows and present in the row before (no off-by-one either way) | point-in-time |
| S1-02 | `f{h}` = close[t+h] / **open[t+1]** − 1 — entry is the next open, not the signal close | entry = next open |
| S1-03 | **seal invariance**: the frame sealed at D equals the frame built from history truncated at D, column for column | point-in-time |
| S1-04 | a sealed frame refuses to widen | point-in-time |
| S1-05 | a 0.0 price is a hole: NaN close, NaN return, NaN forward outcome; no ±inf anywhere | never fabricate |
| S1-06 | the library holds exactly the six seeded templates, each with a computation, question, source and grading kinds | addendum 2 |
| S1-07 | the scan runs **only** what the library holds (patching the library to one template yields one template's cards) | principle 3 |
| S1-08 | every template fires on an engineered close (dip / surge / anomaly / pair / theme / regime) | — |
| S1-09 | a calm close yields fewer cards — nothing is padded | addendum 1 |
| S1-10 | every card carries level · n · period · regime · comparison group · cost hurdle · universe · source · as_of | addendum 7 |
| S1-11 | every fact carries provenance with a level, no period past `as_of`, no model in `computed_by` | principle 4 |
| S1-12 | narratives are digit-free (headline: no digit at all; body: only `{{fact}}` tokens), every ref resolves, every narrative cites a fact | principle 2 |
| S1-13 | small samples are labelled (`greyed` < 20), never hidden | CLAUDE.md |
| S1-14 | the hurdle is on every card and the decision is the one the facts + hurdle imply (the prototype's rules) | cost-aware |
| S1-15 | a `Narrative` with a bare numeral is unrepresentable | principle 2 |
| S1-16 | directional / no-trade verdicts: Right beyond +hurdle, Wrong beyond −hurdle, **Inconclusive inside** (long, short, no-trade) | addendum 4 |
| S1-17 | no grade before the horizon completes | principle 5 |
| S1-18 | theme call vs theme watch: the "not yet a cycle" call is Wrong when it clearly was one | addendum 4 |
| S1-19 | anomaly-move and pair-convergence verdicts (pair uses 2× hurdle) | addendum 4 |
| S1-20 | **the rule is frozen**: the hurdle is changed in config after publication; the grade uses the frozen one | principle 5 |
| S1-21 | findings grade exactly when their horizon closes; `due_session` and `data_as_of` are recorded | principle 5 |
| S1-22 | scoreboard counts sum to n; per-template split sums; snapshots are appended | principle 8 |
| S1-23 | findings, grades, editions and snapshots reject UPDATE/DELETE; one grade per finding, ever | append-only |
| S1-24 | an edition is never recomputed | append-only |
| S1-25 | the threshold gates publication; **zero published is a valid edition**; rejected candidates are on the record with scores | addendum 1 |
| S1-26 | ranks strictly increase, usefulness strictly decreases, the first ≤3 are `what_matters_now` | clarity-first |
| S1-27 | novelty decays for a repeated finding | ranking |
| S1-28 | evidence strength rewards sample and clarity | ranking |
| S1-29 | a model that writes a numeral is rejected by the contract and the engine narrates, visibly | principle 2 |
| S1-30 | the model may only veto a card that already cleared the threshold; it is never asked about the rest; a good narration is stamped `llm` + model | principle 2 |
| S1-31 | `GET /feed` over HTTP: ranked, tiered, digit-free, provenance, frozen rule, grading state, scoreboard; an earlier edition shows its grades | done-when |
| S1-32 | feed errors are guarded (400 bad date, 404 no edition, nothing internal leaks) | guarded errors |
| S1-33 | no research store → 404, never fixtures | honesty |
| S1-34 | **port fidelity on the real warehouse**, close 2026-07-29: IT led 8/15, +11.7% vs +2.6%, persistence 70% (n=3,316 — the prototype's 3,296 dropped 20 sessions to a data hole, verified by re-injecting it), pair 89% of 412, dip n=12,632, surge n=24,727 (the prototype's 24,734 included a +inf off a 0.0 bar and six listing-day glitch bars — see A-D1) | reuse, don't re-derive |

### S1 quant-audit regressions (`backend/tests/test_pathfinder_s1_audit.py`, 39 rows)

Each row encodes the auditor's recomputed expectation and **failed on the engine as handed over**
(`docs/handbacks/PF-S1.md` §4). Rows prefixed `real` run on the warehouse at close 2026-07-29.

| Row | Asserts | Finding |
|---|---|---|
| A-C1 | `pf_editions.engine_version` and every `computed_by` carry the content hash of `research/*.py`; the hash changes when a file changes; **the same seal scanned twice into two stores yields byte-identical `card_json`** (synthetic and real); a share of exactly 0% / 100% over n≥100 is refused at mint time (the smoke run's `big_move=0.0%` over 13,119) | C1 |
| A-C2 | `rotation_reject` (and `theme_watch`) are **Wrong** when the sector beat the market by more than the hurdle even though it ranks fourth at the horizon (the old rule graded that Right); rank at horizon is a fact, not a verdict input; the rule text names no top-three condition | C2 |
| A-C3 | the "not a cycle" verdicts are symmetric: lagged by > hurdle → Right, inside → Inconclusive | C3 |
| A-C4 | z is measured on the **lift beyond a minimum** over the unconditional control (47.35% vs 48.44% on 13,154 → z = 0; the old z vs 50% was > 6); the card mints `base_big_move` (n = every resolved stock-session), `lift`, names the control in `comparison_group`; a zero-lift card is `no_trade` (`no_trade_call`/5), evidence strength 0.5 | C4 |
| A-C5 | episodes = first day of each run of consecutive extreme sessions; `n` = episodes; the key facts are the spread trade's hit rate and median; event-day facts are labelled overlapping; `snapped_back` is labelled a width statistic, not a trade result | C5 |
| A-C7 | the strong gate is `leaders like this` (sector beat the market on ≥ 9/15 sessions with a higher cumulative return) on the graded metric (sector − market f5) **plus expectancy**; `persistence` is labelled "any leader … context only" | C7 |
| A-C8 | a pct/ratio/x statistic without n is refused; a parameter / single observation is minted with `sample_flag = not_applicable` and **no n**; no card carries an n on `move_today`, `volume_x`, `sigma`, `threshold`, `watch_*_move`, `other_*_return`, `window` | C8 |
| A-P1 | the strict feed contract rejects "nine in ten", "half", "doubled", "most of the time", "usually", "one in four" without a fact ref **in the same sentence**, rejects zero refs, rejects number words in the headline; **the model's headline is discarded and the engine's kept** | P1 |
| A-P2 | `rule_version` ends with the sha256 of `grading.py`; `build_rule` refuses an incomplete spec; `evaluate` raises rather than reading live config for a missing key | P2 |
| A-P7 | a sealed / resealed frame has none of `next_open`, `_d{h}`, `_c{h}`, `_badfwd{h}`; the last five rows of every symbol carry no week-later outcome (synthetic + real 497-name frame) | P7 |
| A-P4 | `scoreboard(X).pending` counts findings graded on a seal later than X | P4 |
| A-P5 | `dip_decision` / `surge_decision` / `carry_decision` / `call_on_excess` refuse a call whose median clears costs with negative expectancy; every group card mints an `expectancy` fact | P5 |
| A-D1 | a >4x / <0.25x close-to-close bar is a hole: its return is NaN and every forward window straddling it is NaN (synthetic); with the guard off the auditor's exact rotation numbers (770 / 41.8% / −0.42%) and the old surge n (24,733) reproduce | found while fixing |
| real-C4 | DCMSHRIRAM 07-29: big 47.35% (n 13,154), base 48.44% (n > 1.25M), lift −1.09 pp, z = 0, `no_trade`, evidence 0.5, usefulness < 0.70; `volume_x`/`move_today`/`move_threshold` have no n | C4 |
| real-C5 | HDFCBANK/ICICIBANK: 412 event-days = 121 episodes; on the 412: Right 51.0% / Wrong 31.3% / median +0.69%; on the 121: 52.1% / 28.9% / +0.71% / up 62.0%; snapped-back 89.1% of 412 kept as context | C5 |
| real-C6 | Telecommunication flip: 771 flips / 41.8% / −0.42% on sector − market **f5**; 36.9% beat by > hurdle; label and rule metric say "next open"; `rotation_reject` | C6 |
| real-C7 | IT leader: like-this n = 2,655, beat-by->hurdle 45.95% (< 58), expectancy < 0 → `watch`; any-leader persistence 70.1% of 3,316 is context | C7 |
| real-C8/P5 | dip n = 12,632, median 0.00%, expectancy₁ ≈ 0, expectancy₅ ≈ +0.08% (the raw mean was +14.3% off one 831x bar) → no_trade; surge n = 24,727, expectancy < 0 → reject | C8/P5/D1 |

### S1 SECOND quant-audit regressions (`backend/tests/test_pathfinder_s1_audit2.py`, 28 rows)

A second, independent audit of the post-fix engine (commit 69c1f95) found eight further issues
(`docs/handbacks/PF-S1.md` §4, "Second audit"). Each row below **fails on that commit** (the file
does not even collect against it) and encodes the second auditor's recomputed number. The
`real` rows run under the SECOND audit's conventions — hurdle 0.30% + 2 × 0.10% slippage = 0.50%,
corporate-action days and synthetic opens excluded; the pass-1 suites pin the pass-1 conventions
explicitly (`PASS1` / `PASS1_DATA` in `test_pathfinder_s1.py`) so the first auditor's numbers still
reproduce.

| Row | Asserts | Finding |
|---|---|---|
| A2-A1 | an edition generated after its session date (IST) is `backfilled = 1` on the edition, on every finding and on every grade; the feed and every card carry the label "simulated backfill — not a forward track record"; the scoreboard splits `forward` / `backfilled` and **forward is 0** on a backfilled store, visibly; a scan run on the session's own date is `backfilled = 0` and its grades land in `forward`; one day late is a backfill; a store under the superseded schema is refused (`StoreSchemaError`), never relabelled | A1 |
| A2-A2 | `novelty_key` = `template\|subject\|evidence signature`; the same group statistic under a different subject scores 0.25; a group card's signature carries the statistic, not the stock; a repeat of an OPEN claim on the next sessions is a `continue` pointing at the ROOT finding, served after the new findings in `discovery`, excluded from `published_count`, recorded in `pf_candidates` as a continuation, **never graded**; once the root's horizon completes the same claim is a new publication; the scoreboard's `n` = `n_independent`, `n_total` counts grade rows, `continued` counts folded cards | A2 |
| A2-A3 | `_effective_n` folds the same sector inside one horizon window; `_persistence(stride=window)` samples non-overlapping windows and returns `(None, 0)` rather than a zero; `call_on_excess` refuses a call whose expectancy is negative at the slippage-inclusive hurdle or whose effective n is small; **real**: IT leader like-this beat 42.3% / lagged 38.3% / median +0.07% / expectancy −0.37% on 2,649 sessions = **879 independent cases** (= the card's n), overlapping persistence 70.0% labelled OVERLAPPING context, mechanical h=15 persistence **25.5% of 220** in the body instead → `watch` | A3 |
| A2-A4 | `data_source` no longer says "back-adjusted" and says UNADJUSTED; a −40% print with no table entry is a suspected corporate action and a table split ex-date is one, both NaN returns, neither the day's subject nor a dip case (with the rule off the −40% print IS the subject); a dividend is not excluded; every finding's provenance carries the exclusion and survivorship disclosures and the edition records the exclusion counts; **real**: CGPOWER 2016-03-15, TATACHEM 2020-03-04, ABFRL 2025-05-22, ADANIENT 2015-06-03, JBCHEPHARM 2023-09-18, SPLPETRO 2022-06-07 are holes; no dip case ≤ −30% remains (the auditor's 23); 179 table + 45 suspected + 6 glitch bars excluded; dip n 12,368 | A4 |
| A2-A5 | `hurdle_pct` = 0.30 + 2 × 0.10 = 0.50 on every decision (`ScanContext.hurdle`, `_rate`), every frozen rule and every provenance; the hurdle fact says slippage; `KANIDA_PF_SLIPPAGE_PCT` is a founder input; **real**: the rotation flip is 776 / 34.4% beat-by-hurdle / expectancy −0.72% → reject, IT expectancy < 0 → watch, both at 50 bps | A5 |
| A2-A6 | a statistic over n = 0 cannot be minted (`FactSet.add` refuses "0.0% of 0 times"); the grader withholds `rank_at_horizon` when the sector cannot be ranked (never 0); the theme card is withheld when no leader like this has resolved; persistence facts are withheld when nothing resolved; no pair fact carries n = 0 | A6 |
| A2-A7 | a bar whose open equals its close is `_synth_open`; `f{h}` of the row entering at it is NaN while `r{h}cc` is untouched; with the rule off the outcome is exactly 0.0 (the artefact); **real**: 19,083 synthetic-open bars; dip hit 50.6% and median **+0.10%** (was 0.0%) → still no_trade | A7 |
| A2-A8 | `archive_store` refuses without `--i-understand-this-archives-the-store`, renames to `.archived-<stamp>`, never deletes; `--fresh` exits; the anomaly grader reads `r{h}cc` for the RULE's horizon (a 3-session rule is Right where a 5-session rule is Wrong); the theme grader's realised market move equals the template's `fmkt` on the same date and `market_return.n` counts that market's names | A8 |
| A2-HTTP | `/api/pathfinder/feed` serves `backfilled`, `record_label`, `continued_count`, per-card `grading.record` / `continues` / status `continued`, provenance `disclosures`, the honest `data_source`, no fact with n = 0, and a scoreboard with `forward.n == 0`, `backfilled.n == n == n_independent`, `continued ≥ 1` | A1/A2/A4/A6 |

Three pins in the pass-1/pass-2 suites were superseded by the third audit's N4 and updated in
place: `like_this_*` statistics now carry `n` = the effective (independent) count, not the
overlapping session count (A-C7 synthetic: `n == like_this_independent`; real-C7: 879, not 2,655;
A2-A3 real: 879, not 2,649).

### S1 THIRD quant-audit regressions (`backend/tests/test_pathfinder_s1_audit3.py`, 33 rows)

A third, independent re-audit of commit f34ecea verified all sixteen earlier fixes and found six
further leaks (`docs/handbacks/PF-S1.md` §4, "Third audit"). The file **collects** on f34ecea
(new symbols are imported inside the rows) and **29 of its 33 rows fail there** — verified by
running it in a `git worktree` at that commit; the four that pass are three quantifier phrases the
old list already caught (`a majority`, `almost all`, `the vast majority`) and a plain-prose sanity
row.

| Row | Asserts | Finding |
|---|---|---|
| A3-N1 | the usefulness threshold is applied FIRST: a sub-threshold repeat of an open claim is a `pf_candidates` row (`decision = continue`, reason `below usefulness threshold (continues fnd_…)`, `published = 0`, no finding id) and is NOT served; `rep.continued` is empty; `Finding` refuses a continuation below its threshold (the exemption is gone); `FeedResponse` refuses any served card below the edition's threshold | N1 |
| A3-N2 | `independent_grades`: the ANURAS 07-13 → TORNTPHARM 07-16 → SOBHA 07-21 chain (lift −1.20 / −1.10 / −1.10, each inside the previous horizon) is ONE independent grade; a repeat on/after the previous due session is independent; a different decision or a statistic beyond the tolerance is a new claim; `claim_of` / `same_claim` read each template's `claim_parts`; `open_root` and `novelty` treat a hit rate re-read 0.9 pp away as the same open claim (continue, 0.25) and 1.5 pp away as new; with publication-time detection disabled the scoreboard still reports `n_total > n == n_independent`, `regraded = n_total − n` from the grade rows | N2 |
| A3-N3 | `GradingKind.pair_watch` exists and is in `RELATIONSHIP.grading_kinds`; a losing spread trade is **Right**, a paying one Wrong, inside twice the hurdle Inconclusive (the call kind reads the other way); the template freezes `pair_watch` for a watch and `pair_convergence` for an experiment; the `Finding` contract refuses a `watch` under `pair_convergence` (`KINDS_FOR_DECISION`); **real**: HDFCBANK/ICICIBANK sealed 2026-07-20 is a watch frozen under `pair_watch`, spread trade −2.26% by 07-27 → **Right** (was Wrong) | N3 |
| A3-N4 | every `like_this_*` statistic carries `n` = `like_this_independent` = the card's n; the overlapping count is its own labelled count fact; the fact-level `sample_flag` derives from the effective n | N4 |
| A3-N5 | a directional rule whose entry bar is a synthetic open returns a `void` `GradeResult` with the reason once the horizon completed, `None` while it has not; pair and theme kinds close the same way; through the store a void finding is status `void` / verdict `void` with `void_reason`, is not in `pending()`, `scoreboard.void == 1`, `n` and `n_total` exclude it | N5 |
| A3-N6 | "A handful of cases bounced, and many did not." is rejected; each of "a handful", "few", "many", "several", "a couple", "the bulk", "nearly every", "most", "some", "plenty", "a majority", "almost all", "hardly any" (and siblings) needs a fact reference in its sentence; cited in the same sentence it passes; plain prose passes | N6 |
| A3-HTTP | `/api/pathfinder/feed` serves nothing below `usefulness_threshold`, `continued_count == 0` where the repeat was sub-threshold, and a scoreboard carrying `regraded == n_total − n` and `void` | N1/N2/N5 |
| A3-D2 | importing the research engine turns pandas' `compute.use_numexpr` and `compute.use_bottleneck` OFF — on this host numexpr 2.14.1 intermittently returned an all-zero `f5` column for the 1.25M-row forward-return arithmetic (three of four rebuilds refused by the C1 guard; a 4-pass probe: 1 mismatch with numexpr on, 0 off) | D2 (found while rebuilding; `PF-S1.md` §5.8) |

### Known gaps for S1

1. **The "unexplained regime reading" was not unexplained.** The `RISK_OFF … breadth 10%` edition
   sat in `var/pathfinder_research_smoke.db`, a store written by the code **before**
   `scan.py`/`data.py`/`regime.py` were patched; the same store carried `big_move = 0.0%` over
   13,119 cases. The hand-over described it as a non-reproducible glitch — it was a stale run of
   superseded code. The smoke store is deleted; every edition now records the code hash (A-C1), a
   determinism test pins two builds to identical cards, and the 0%/100%-share guard would have
   refused the bad fact. The `build_regime` cross-check stays.
2. **No live-model row.** S1-29/30 and A-P1 exercise the contract with a fake provider; the recorded
   provider has no cassettes for feed keys, so every real edition is engine-narrated and says so.
   The P1 engine's own narrate path (`engine/narrator.py`) still uses the non-strict contract.
3. **The Postgres target** is untouched by S1 — the research store is SQLite only.
4. **Survivorship** (audit P3 / second audit A8): today's Nifty-500 membership and today's sector
   labels are applied to 2013→2026 history and the universe holds no delisted name; base rates are
   cost-hurdle decisions rather than absolute returns, which limits but does not remove the bias.
   No point-in-time membership in the warehouse. Now a user-facing sentence on every provenance.
5. **No forward record exists yet** (second audit A1). Every edition in the store was generated on
   2026-09-10 for sessions in July: the scoreboard is a **simulated backfill** and says so on the
   feed, on every card and in `forward = 0`. A forward record starts only when the scan runs on the
   session's own date after the EOD bar lands; no test can create one against a stale warehouse.
6. **Demergers are not adjusted** (A4). The engine excludes the ex-date bar and every forward window
   across it; it does not correct the price basis. A point-in-time adjusted feed is the real fix.

---

## Pathfinder S2 — the experiment loop (`backend/tests/test_pathfinder_s2.py`)

Session S2 (`docs/sessions/PATHFINDER_S2_EXPERIMENTS.md`, hand-back `docs/handbacks/PF-S2.md`).
Rows S2-01…S2-33 run on a **synthetic universe with an engineered CONDITIONAL edge** (hard one-day
falls that came on a market-wide fall bounce over the next week; falls on ordinary days do not) so
every branch of the loop is exercised on data the test controls, and the numbers the loop reports are
recomputed independently in the row. The real-warehouse run is documented in the hand-back.

```bash
python -m pytest backend/tests/test_pathfinder_s2.py -q                 # 44 rows, ~80 s
python scripts/run_pathfinder_scan.py --date 2026-07-29 --backfill 60   # the S1 editions the loop steps over
python scripts/run_pathfinder_experiments.py                            # the loop, sealed at each edition
```

| Row | Asserts | Principle |
|---|---|---|
| S2-01 | the variant set is CLOSED (9 conditions × 2 horizons = 18), ordered, serialisable; every horizon inside the Constitution's `_exits` range | addendum 2/3 |
| S2-02 | replay measures the S1 convention (`f{h}` − hurdle), recomputed independently; a signal inside the horizon of the seal is never a trade; a window past the seal raises | point-in-time |
| S2-03 | the engineered edge is conditional: the conditioned variant clears expectancy on all three windows and the placebo; the unconditioned rule does not; windows split at `discovery_end` | NDP |
| S2-04 | the book enters at the NEXT OPEN, exits at the horizon CLOSE, charges costs + slippage; on an earlier seal the positions are OPEN and nothing past the seal is read | entry = next open; seal |
| S2-05 | the Constitution's limits bind (max new per session) and the selection rule is liquidity, descending | reuse book.py |
| S2-06 | verdicts are symmetric in one standard error — the LARGER of the plain and cluster-robust estimates (three clusters collapsed CR0 to 0.04 and would have called noise Right); fewer trades than the frozen minimum = Inconclusive; comparison categories | addendum 4 |
| S2-07 | a period with no closed trade is `void` with its reason, never counted | N5 |
| S2-08 | the grading rule is frozen with the version (hash-versioned by `grading.py`); every outcome was judged under it; the expected value in the spec equals the frozen expectation | principle 5 |
| S2-09 | the worth-testing gate = the Constitution's discovery gauntlet + the trailing-window check + implementability + novelty + evidence strength; each fails on the numbers | addendum 3/5 |
| S2-10 | advisory gates (family-wise bar over the counted trials, pre-`discovery_end` window at 2× slippage, cluster-t) are recorded, never kill, and the family-wise bar divides α by the trial count | no silent p-hacking |
| S2-11 | the 2× slippage gate is a different number from the hurdle gate | cost sensitivity |
| S2-12 | a real S1 `dip` **no_trade** finding opens v1 with a FROZEN expectation (seal = the opening edition), 18 trials on the record numbered 1…18, one adopted, the candidate row points at the experiment, the S1 card's own facts travel with the version | done-when |
| S2-13 | non-derivable findings and repeats are declined ON THE RECORD; a repeat is not re-researched; one experiment per family | novelty |
| S2-14 | a declined family is not re-researched daily (`retry_after_sessions`); the decline records the best variant and its failed gates | no p-hacking by repetition |
| S2-15 | forward marks and trades are written no earlier than their session; a re-walk of the period from the store's own rule on a LATER seal reproduces the trades and marks exactly | point-in-time; determinism |
| S2-16 | expected-vs-actual is computed (mean of the stored trades), the gap is arithmetic, the statement is digit-free and honest ("weaker" / "at least as strong") | done-when |
| S2-17 | a Wrong period runs the failure analysis, evaluates 7 counted revisions, adopts one → v2 (L3, validated on the seal, expectation frozen on the grading seal); v3; then BURIED with a post-mortem; nothing tracks a buried idea | addendum 3/6 |
| S2-18 | learning never creates a version beyond `max_versions` | retirement rule |
| S2-19 | the arena's constitutional score is the ported one (Test / Watch / Keep / Retire) | reuse arena.py |
| S2-20 | the graduation gate runs after every graded period; a PROPOSAL exists only when every gate but the signature passes; on the unsigned draft it is `blocked_unsigned_constitution`; the registry never holds "promoted" | addendum 5/9 |
| S2-21 | on a signed Constitution the proposal `awaits a human`; the incumbent is the same capital in the whole universe over the same window; with the edge reversed no proposal exists | champion/challenger |
| S2-22 | the scoreboard counts each graded period once, excludes void, splits forward/backfilled, counts trials; nothing is graded as of the opening date | principle 8 |
| S2-23 | a step run on its own session date is FORWARD; a later one is a BACKFILL; the card says which | A1 |
| S2-24 | every table rejects UPDATE/DELETE; the chains verify; a rewritten row breaks its chain | append-only |
| S2-25 | two builds of the same seals are identical but for timestamps, hashes included | determinism |
| S2-26 | the public card tells seven digit-free beats from facts, names no stock, has no constituent field | addendum 6 |
| S2-27 | the card contract refuses a trade instruction ("entry", "stop-loss", "buy", "target") and a constituent field | addendum 6 |
| S2-28 | the record withholds constituents until RA review (`KANIDA_PF_RA_REVIEWED`), carries versions, trials (= `trials_total`), the change-log (`improved` null) and the proposal | addendum 6 |
| S2-29 | the buried record publishes its post-mortem; `/experiments` lists losers first | principle 8 |
| S2-30 | a model that writes a numeral is rejected on every beat and the engine narrates, visibly; a good completion is stamped `llm` + model under the ENGINE's headline | principle 2 |
| S2-31 | no fact on any surface names a model in `computed_by`; no period ends after its `as_of` | principle 2 |
| S2-32 | `/experiments`, `/experiment/{id}`, `/feed` (cards with the edition's backfilled flag, the scoreboard as of the edition) over HTTP; 404 with no registry — never fixtures; guarded 400/404 | done-when |
| S2-25b | a card served for a PAST edition shows only what was known then (state, score, trial count as of that edition) | point-in-time on the served surface |
| S2-33 | the P0 path is unchanged when the source is `mock` | no regression |

### S2 quant-audit regressions (same file, rows `test_a*`)

A `dev-quant-auditor` pass on the S2 loop (`docs/handbacks/PF-S2.md` §4) confirmed the numbers reproduce from
the raw warehouse and found sixteen items; every CONFIRMED one is fixed and pinned:

| Row | Finding | Pinned |
|---|---|---|
| A1 | the feed would have refused (500) the first FORWARD edition of an experiment opened in a backfill — the card's `backfilled` was the opening edition's forever. Now per news edition, with `opened_backfilled` alongside | `test_a1_*` |
| A2 | chain hashes embedded timestamps inside JSON (`frozen_at`, facts' `computed_at`, story `at`), so two builds did not chain identically. Now content-only | S2-25 (rebuilt: two builds an hour apart, identical hashes) |
| A4 | a wide per-period band could shelter a losing version forever; a rule that stops firing was tracked forever. Frozen CUMULATIVE rules in every version's `spec`: cluster-robust t below −z on ≥10 resolved trades over ≥5 signal days buries; 4 consecutive void periods bury (`rule_stopped_firing`) | `test_a4_*`, `test_a4b_*` |
| A5 | the trailing window is seen by every variant (selection), and the card presented it as independent confirmation. Now a `trailing_looks` fact and a sentence on the card say it is a persistence check | S2-12 (fact present), narrative text |
| A6 | outcomes copied the frozen `rule_version`; a changed grader could judge an old version silently. Now the grader's own version is stamped (`grader_version`) and a mismatch REFUSES to grade | `test_a6_*` |
| A7 | the book could close a trade through a glitch / corporate-action bar (a fake −50%) that the evidence convention treats as NaN. Now such a trade is `unresolved`: listed, never graded | `test_a7_*` |
| A8 | a model body with a banned word passed the narrate contract and broke the card at read time (500). Now lint at the source; engine fallback | `test_a8_*` |
| A9 | the S1 subject fact could carry a stock name onto the public card. Now a fact whose value names a constituent of the book is withheld on the card | `test_a9_*` |
| A10 | the placebo / baseline pool was not context-conditioned (incomplete P1 port) and the edge mixed raw with winsorised means. Now the pool is the rule's own kind of day; one convention | `test_a10_*` |
| A11 | `improved` was never computed. Now: a successor version's forward mean vs its predecessor's, once graded; null until then | S2-28 |
| A12 | the family-wise n during learning was the running trial index. Now the idea's whole trial count after the round | `test_a12_*` |
| A13 | literal numerals in `why` / `what_we_kept` (engine text fields). Now digit-free; the counts are facts | S2-12/S2-17 |
| A14 | `signals_seen` missed a signal on the seal day. Now seen, not taken | `test_a14_*` |

### S2 independent re-audit (`backend/tests/test_pathfinder_s2_audit2.py`, rows `test_n*`)

An independent re-audit recomputed every S2 number from the raw warehouse (`docs/handbacks/PF-S2.md` §4.1) and
found nine items; every one is fixed and pinned. **Every row below fails on `187c488`** (verified in a detached
worktree: 18 failed + the archived-registry row, which fails there too once `KANIDA_PFX_ARCHIVED_REGISTRY` points at
the archived file). The three `real_*` rows read the price warehouse and the archived pre-audit registry
(`var/pathfinder_experiments.db.archived-20260910T115630`) and skip where either is absent.

```bash
python -m pytest backend/tests/test_pathfinder_s2_audit2.py -q          # 19 rows, ~30 s (16 synthetic + 3 real-data)
```

| Row | Finding | Pinned |
|---|---|---|
| N1 | the frozen expectation was measured over EVERY signal equal-weighted while the book takes at most 5 per session by liquidity, 10 concurrent, one per symbol — a different strategy (on the real warehouse +0.25% vs the book's own −0.29%). Now `hypotheses.book_select` replays the book's own selection over history; the expectation and every gate value are on that population; the equal-weighted figure is a labelled context fact (`Expectation.population`, `signals_fired/skipped`, `equal_weighted_expectancy_net_pct`) | `test_n1_book_select_*` (the walk: rank, caps, one per symbol), `test_n1_replay_*` (selected = top-k liquidity per day; skipped counted), `test_n1_the_loop_freezes_*` (frozen value = independent book-replay; ≠ equal-weighted; both on the card), `test_n1_real_*` (−0.285% on n 1,682 vs +0.2456% on 8,337; the gate closes on expectancy) |
| N2 | the graduation placebo re-drew DAYS on a forward window where every qualifying day is a signal day (degenerate; p 0.53 conditioned). Now `loop.fixed_day_permutation_means`: the version's own signal days held fixed, random resolved names drawn from each day's pool; `insufficient` below `min_forward_signal_days` (5), never passed | `test_n2_the_forward_null_*` (bounded by the fixed days' pools; deterministic), `test_n2_the_graduation_gate_is_insufficient_*`, `test_n2_the_loop_records_*` (the run crosses the floor), `test_n2_real_*` (the archived 23 trades / 5 days: p ≈ 0.35 at 5,000 draws) |
| N3 | three days carried the trailing "persistence" (106% of its net P&L; without 2025-04-04 alone 1.15 not 1.93). Now concentration facts on every window and trial (`top3_days_share_pct`, `expectancy_without_best_day`) and an advisory gate `trailing_expectancy_without_best_day` | `test_n3_concentration_*`, `test_n3_the_card_*`, `test_n3_real_*` (best day 2025-04-04, share 105.7%, without it 1.15) |
| N4 | the family-wise trial count restarted on every retry (0.05/18 on the 60-session retry although the family had 36). Now `store.family_trials` counts the family's trials ALL TIME across findings, retries and revisions; the bar divides by it; `pfx_candidates.family_trials_all_time`, `ExperimentCard.family_trials_all_time`, `RejectedCandidate.family_trials_all_time` | `test_n4_*` (18 then 36; bars 0.05/18 then 0.05/36) |
| N5 | CR0 cluster statistics on as few as five signal days (cumulative kill, `oos_cluster_significance`). Now CR3 (`cluster_robust_se`) against Student's t with G−1 degrees of freedom (`t_critical`, no SciPy), frozen in the rule spec; grading rule 1.1.0 | `test_n5_*` (2.776 on five days; a record CR0/z would bury survives CR3/t(4); a truly negative one still buries) |
| N6 | "go long the dip", "a virtual long position …", "hold for … exit at …", "position size …", "short it at the open" passed the public-card lint; beat 3 stated the holding horizon and beat 4 the position fraction. Regex widened; beats 3/4 rewritten as a study (theme, condition, study window, the book's own population); `rule_text` is a measurement definition | `test_n6_the_public_card_rejects_*` (all five probes), `test_n6_the_engines_public_beats_*` |
| N7 | `is_signed` was a string-prefix check on `approved_by`. Now explicit `signature: {signed_by, signed_at, document_sha256}`; signed only when all three are present AND the hash equals `constitution_content_sha256(document)` (every key but the signature block, canonical JSON) | `test_n7_*` (free-text approver ≠ signed; missing field ≠ signed; a changed gauntlet number un-signs) |
| N8 | 1,000 placebo draws give ±0.009–0.014 (2 SE) near the bar and the p compared the RAW signal mean while the expectation is winsorised. Now `placebo_p_value` re-draws to 5,000 when |p − bar| < 2 SE (at p̂ or at the bar); draw means and the statistic are both winsorised | `test_n8_the_placebo_redraws_*`, `test_n8_the_replay_records_*` |
| N9 | `_version_forward` measured drawdown relative to peak; `BookRun.drawdowns()` in additive points of capital; the incumbent gate compared the two. Now ONE convention, `book.DRAWDOWN_CONVENTION` (percent decline from the running peak), on both sides and on `ForwardResult.drawdown_convention` | `test_n9_*` (25% not 30 points; gate bar 25.0; statement names the convention) |

## Pathfinder P2 — the frontend (`kanida-app`)

Two suites, both run by Node's built-in test runner (no test framework dependency added):

```bash
cd backend && uvicorn pathfinder.mock_app:app --port 8010   # the server under test
cd kanida-app && npm run test:all                           # 31 rows
```

`tests/lib.test.ts` (15 rows) is pure and needs nothing running. `tests/contract.test.ts`
(16 rows) runs against a **live server** on `EXPO_PUBLIC_API_BASE_URL` — the P0 mock today, the
P1 engine tomorrow. **It is the acceptance suite for the swap:** if the real engine passes it
unchanged, every screen renders honestly against real data with no code change.

### Unit rows — `tests/lib.test.ts`

| ID | Requirement | Test | Passes when |
|---|---|---|---|
| P2-01 | `{{fact:…}}` tokens are resolved, never printed | `parseSegments splits prose around every reference token, in order` | Prose splits into text/fact/exp runs in source order with the right ids. |
| P2-02 | Prose with no tokens still renders | `parseSegments handles prose with no tokens and empty prose` | Plain and empty strings return a single text run. |
| P2-03 | Edge positions | `a token at the very start or end is not swallowed` | Leading and trailing tokens survive parsing. |
| P2-04 | Fact extraction | `factIdsIn returns every fact referenced, and only facts` | Only `fact:` ids returned, in order. |
| P2-05 | A raw token reaching a customer is detectable | `hasUnrenderedTokens catches a raw token reaching the screen` | Detects `{{…}}` in a rendered string. |
| P2-06 | The mockup's `#id` chip | `experiment ids render as the mockup chip` | `exp_0007` → `#0007`. |
| P2-07 | Versions shorten without losing the number | `versions shorten without losing the number` | `strategy@v2.1` → `v2.1`. |
| P2-08 | **A loss never renders as a gain** | `a signed percentage never loses its sign` | `−0.03%` uses a true minus; null → `—`, never `0`. |
| P2-09 | Drawdowns are magnitudes | `drawdowns render as unsigned magnitudes` | `±12.7` → `12.7%`. |
| P2-10 | **The unit decides the rendering, never the caller** | `a fact renders in its declared unit, never a guessed one` | Inline facts are unsigned unless negative (a drawdown is never `+12.7%`); metrics ask for the explicit `+`. |
| P2-11 | Indian digit grouping | `large counts use Indian digit grouping` | `1482000` → `14,82,000`. |
| P2-12 | Windows are stated | `a date range renders as month-year to month-year` | `Jan 2018 – Dec 2025`. |
| P2-13 | **The 2× slippage gate** | `the 2x-slippage gate is strictly positive` | Zero expectancy at 2× slippage does **not** pass. |
| P2-14 | **`improved: null` is not a pass** | `improved:null reads as "too early to say", never as a pass` | Renders neutral copy, not a success. |
| P2-15 | The graveyard is a feature | `a died experiment is labelled REJECTED, not hidden or softened` | `died` → `REJECTED`, and it is rendered outside the funnel rather than dropped from it. |

### Contract rows — `tests/contract.test.ts` (live server)

| ID | Requirement | Test | Passes when |
|---|---|---|---|
| P2-16 | Every figure is traceable | `every fact token in every payload resolves to a served fact` | No token would render as `[missing figure]`. |
| P2-17 | **The LLM never calculates** | `LLM-authored prose contains no literal numeral` | ≥20 LLM lines checked; none contains a digit once tokens are stripped. |
| P2-18 | Locked job→model routing | `the models used are only the ones the gateway routes to` | Only `claude-sonnet-5` / `claude-haiku-4-5` / `claude-opus-5`. |
| P2-19 | A model never computes | `no provenance names a model as the thing that computed a number` | `computed_by` never matches a model name. |
| P2-20 | **Expectancy + both drawdowns + provenance** | `every performance block carries expectancy AND both drawdowns AND full provenance` | ≥15 blocks; all five provenance fields present; **no `date_range.end` post-dates its `as_of`**. |
| P2-21 | **No promises** | `no payload contains a target price, projection or promised return` | No promise-shaped field reaches the client. |
| P2-22 | **Losers first** | `the experiments list leads with the dead ones` | The list leads with `died` and ends with `promoted`. |
| P2-23 | **Losers first (ledger)** | `every virtual ledger is ascending by net P&L, worst trade first` | Every book's ledger is ascending; the app never re-sorts it. |
| P2-24 | **Point-in-time + costs** | `every simulated trade enters on a LATER session than its signal, with costs charged` | ≥20 trades; entry after signal; costs and slippage ≥ 0 on every closed trade. |
| P2-25 | One n-flag rule, one owner | `sample_flag matches the product rule the UI renders` | Server flag equals n<20 greyed / n<50 flagged / else ok, everywhere. |
| P2-26 | Failure is published | `a died experiment publishes a post-mortem with what was kept` | Cause, evidence and a substantive "what we kept". |
| P2-27 | **Governance** | `every change-log entry answers what/why/evidence/versions, and L4 is human-only` | Append-only `seq`; L4 human-decided **and** human-approved; L3 states its validation. |
| P2-28 | Autonomy is event-driven | `every trigger was fired by the deterministic engine, never a clock or the model` | Every trigger's `fired_by` is `engine`. |
| P2-29 | Filters cannot mislead | `the status filter is honoured` | Each `?status=` returns only that status and echoes the filter. |
| P2-30 | Guarded errors | `an unknown experiment id returns a guarded error the app can render` | 404 with `{error:{code,message}}`; no stack trace or SQL in the message. |
| P2-31 | Disclosure everywhere | `every payload carries its research disclosure` | Present on all four endpoints. |

### Known gaps for P2

(Superseded by S3 below for the Pathfinder surface; the P2 contract rows now live in
`tests/contract.p0.test.ts` and run only against the mock source.)

1. **No on-device render test.** `npx expo export --platform all` proves iOS, Android and web all
   *bundle* from this one source (1238 / 1387 / 832 modules, exit 0), and the web target was driven
   by hand at 375 px and 1440 px in both themes. The iOS and Android bundles were **not run on a
   simulator or device** in this session — no macOS, no Android SDK here. First action for whoever
   picks up the app: `npm run ios` / `npm run android`.
2. **No visual-regression or a11y automation.** Contrast, focus rings and touch targets were
   designed for and inspected by hand, not asserted.

---

## Pathfinder S3 — the swipeable feed (`kanida-app`)

Session S3 (`docs/sessions/PATHFINDER_S3_FEED.md`, hand-back `docs/handbacks/PF-S3.md`). Three suites,
all on Node's built-in runner:

```bash
cd backend && KANIDA_PATHFINDER_SOURCE=research uvicorn pathfinder.mock_app:app --port 8010   # the server under test
cd kanida-app && npm test               # lib + feed rows, no server
cd kanida-app && npm run test:contract  # rows against the LIVE research API
cd kanida-app && npm run test:all
```

### Unit rows — `tests/lib.test.ts` (17 rows) and `tests/feed.test.ts` (16 rows)

| ID | Requirement | Test | Passes when |
|---|---|---|---|
| S3-01…05 | `{{fact:…}}` tokens resolved, never printed (P2-01…05 kept) | `parseSegments …`, `factIdsIn …`, `hasUnrenderedTokens …` | as P2. |
| S3-06 | Experiment ids read as a chip | `experiment ids render as the mockup chip` | `exp_dip_bounce_20260504` → `#dip_bounce_20260504`. |
| S3-08 | **A loss never renders as a gain** | `a signed percentage never loses its sign` | true minus; null → `—`. |
| S3-09 | **No `+` on a drawdown** | `drawdowns render as unsigned magnitudes — never a plus on a drawdown` | `pctAbs` never emits `+`. |
| S3-10 | The unit decides | `a fact renders in its declared unit, never a guessed one` | as P2-10. |
| S3-14 | Every sample flag has honest copy | `every sample flag has copy, and the small ones say so` | flagged = "small sample", greyed = "too few to read", `not_applicable` says "not a statistic". |
| S3-15 | A backfill is named | `a backfill is named a backfill, a forward record a forward record` | `recordShort`. |
| S3-16 | Decision / verdict copy is not a promise | `every decision and every verdict has customer copy that is not a promise` | no "guarantee / will return / target price"; Wrong and Void are labelled as such. |
| S3-17 | Story types come from the engine's template | `a no_trade or reject is a debunk; the story type follows the engine template` | theme / stock / volume / pair / market; `· Debunk` on no_trade / reject. |
| S3-18 | **Compliance lint (addendum 6)** | `the compliance lint withholds a trade instruction and passes research prose` | "Buy the dip at the open", "stop-loss", "target" withheld; the engine's research rule text passes; the S1 debunk "history says do not buy the dip" passes the order lint but "entry … stop-loss" does not. |
| S3-19 | **Clarity first, in the engine's order** | `the feed order is clarity first: what matters now, discoveries, experiments, scoreboard, next` | finding ×4 → no_experiment → scoreboard → next; tier positions 1 of 3. |
| S3-20 | **No padding** | `a four-finding edition renders four finding stories — no padding` | exactly four. |
| S3-21 | **A zero-finding edition is a story, not a blank** | `an edition that published nothing renders ONE honest story, then the scoreboard — never a blank` | `empty_edition` → `no_experiment` → `scoreboard`, with the candidate count. |
| S3-22 | **Nothing cleared the gate is a story** | `when no experiment opened, the story says so and names the closest variant and its failed gate` | only researched candidates are "closest"; a statistical near-miss ranks before an unimplementable one; trials and declined counts carried. |
| S3-23 | The registry may be absent | `the no-experiment story survives a registry that has not loaded, without inventing anything` | `registry: null`, `closest: []`. |
| S3-24 | An experiment card is a story | `an experiment card on the edition becomes an experiment story in the feed, after the findings` | seven beats; title = theme. |
| S3-25 | **Backfill label verbatim on the scoreboard** | `the scoreboard story carries the record label and the forward/backfilled split verbatim` | `BACKFILL_LABEL`, forward n 0, backfilled n 21. |
| S3-26 | Forward is forward | `a forward edition is labelled forward, not backfilled` | `FORWARD_LABEL`. |
| S3-27 | What's next is real | `the next story collects every follow-up question and every pending horizon …` | four follow-ups, four pending; no next story when nothing is pending or asked. |
| S3-28 | **Paging + depth navigation** | `the index is clamped inside the feed and a story can be found by id for back-navigation` | `clampIndex`, `indexOfStory` (used by `?story=` and the position memory). |
| S3-29 | Key facts are numeric and greyed stays greyed | `key facts are numeric, in the engine's order, and greyed samples stay flagged` | text subject skipped; flag preserved. |
| S3-30 | The lede never splits a token | `the lede keeps whole sentences and never splits a fact token` | sentence split; no `{{` in text runs. |
| S3-31 | **No raw token can reach the screen** | `every fact token in every story narrative resolves against the story's own facts` | every ref resolvable; headline never carries a token. |
| S3-32 | **No trade instruction on the surface** | `no composed copy on the research surface reads like a trade instruction` | decision reasons, headlines, follow-ups, themes, beat headlines, rule texts, titles. |
| S3-33 | **Backfill label on every graded card** | `the backfill label is present on every graded card and on the scoreboard, verbatim` | `grading.record` = `BACKFILL_LABEL` on every card of a backfilled edition. |

### Contract rows — `tests/contract.test.ts` (16 rows, live research API)

| ID | Requirement | Test | Passes when |
|---|---|---|---|
| S3-40 | Clarity first | `the latest edition serves clarity first …` | ≤ 3 in `what_matters_now`, tiers right, ranks strictly increasing, served = published + continued. |
| S3-41 | **Never padded** | `nothing served is below the usefulness threshold` | every served card ≥ the edition's threshold, across the latest + 6 prior editions. |
| S3-42 | **Digit-free narrative, resolvable facts** | `every narrative is digit-free outside its fact tokens, and every token resolves …` | no digit in headline / bare body / reason; every `{{fact}}` and `key_fact_ref` served; model named iff llm-authored. |
| S3-43 | **Provenance on every fact, no look-ahead, no model** | `every fact carries provenance with a window that ends no later than its as_of, and no model computed it` | six provenance fields; `date_range.end ≤ as_of`; `computed_by` names no model; statistics carry n; `not_applicable` carries none. |
| S3-44 | **Addendum-7 provenance on every card** | `every card carries the addendum-7 provenance …` | level ∈ 4; n; period ≤ as_of; regime; comparison group; hurdle > 0; disclosures ≥ 1; flag matches n. |
| S3-45 | **Rule frozen at publication; grading state consistent** | `the grading rule was frozen at publication …` | right / wrong / inconclusive legs; graded ⇒ verdict; void ⇒ reason; continued ⇒ `continues`; card and grade agree on `backfilled`; `record` matches. |
| S3-46 | **Backfill label everywhere; the split sums** | `the backfill label is on the edition, on every card and on the scoreboard — and the split sums` | `record_label` ↔ `backfilled`; R+W+I = n; forward + backfilled = n; `n_independent = n`; `regraded = n_total − n`; a scoreboard with forward n 0 says backfill. |
| S3-47 | **No order on the research surface** | `no card, question, reason or follow-up … reads like an order` | headline / body / reason / question / follow-ups pass the order lint; no field named target / stop / entry_price / projection. |
| S3-48 | A zero-finding edition is valid | `an edition that published nothing is still a valid, honest edition` | `published_count 0`, scoreboard present, label present (the 24 Jul 2026 edition). |
| S3-49 | Guarded errors (feed) | `the feed guards its errors …` | `?date=DROP` → 400, `?date=1999-01-01` → 404, no internals. |
| S3-50 | Registry counts, losers first, label | `the registry counts agree with its items, lists losers first, and carries the record label` | count = items; buried → testing → proposed; sums; engine version; disclosure. |
| S3-51 | **The declined are on the record** | `every declined candidate is on the record with a reason; the researched ones name the closest variant and its failed gates` | reason ≥ 10 chars; researched ⇒ rule text + failed gate; rule text passes the instruction lint; `trials_total` ≥ declined trials. |
| S3-52 | **Seven digit-free beats, no constituent field** | `every public experiment card tells the seven beats in order …` | beats in order; digit-free; facts resolve; instruction lint; `record_label` ↔ `backfilled`; no `constituents` / `basket` on a card; a feed card's `news_edition` is the edition's. |
| S3-53 | The record | `an experiment record, when one exists, carries versions, trials, periods and a withheld or RA-reviewed basket` | versions ≥ 1; trials = `trials_total`; expectation period ≤ seal; graded periods carry a verdict and the record label; constituents only when RA-reviewed; buried ⇔ post-mortem. |
| S3-54 | Guarded errors (experiment) | `the experiment endpoint guards its errors …` | `DROP` → 400, `exp_nope` → 404. |
| S3-55 | Disclosure everywhere | `every payload carries its research disclosure` | feed, every finding, the registry. |

### Known gaps for S3

1. **No on-device render test** (inherited): `npx expo export --platform all` exits 0 for iOS, Android
   and web; the web target was driven by hand at 375 px (dark) and at desktop width (the user's
   Chrome, light). No macOS / Android SDK on this machine.
2. **No visual-regression or a11y automation.** The pager, the progress rail, the depth flow and the
   honest states were verified by eye (see the hand-back for exactly what was seen).
3. **The contract mirror (`src/api/types.ts`) is hand-maintained.** The concurrent S2 re-audit added
   optional fields to `Expectation`, `ForwardResult`, `LearningView`, `RejectedCandidate` while S3 was
   in flight; they are additive and ignored by the client until mirrored. *(Closed by the integration
   polish below: every re-audit field is mirrored and rendered.)*

## Pathfinder integration polish — the S3 §4 seams (`backend/tests/test_pathfinder_polish.py`, `kanida-app`)

Hand-back: `docs/handbacks/PF-POLISH.md`. Backend rows run in the S1/S2 command (300 pass = 294 + these 6).

| Row | Requirement | Test | What it pins |
|---|---|---|---|
| PL-01 | **Never a 500.** `/loop` and `/learnings` under `KANIDA_PATHFINDER_SOURCE=research` | `test_pl01_loop_and_learnings_are_guarded_under_the_research_source_never_500` | 404 `not_served_by_source`, `error.use` names `/feed` and `/experiments`; no traceback / SQL in the body; `/feed` and `/experiments` with no store still answer `no_edition` / `no_experiments`. |
| PL-01b | the mock source is unchanged | `test_pl01b_the_mock_source_still_serves_loop_and_learnings` | both 200 under `mock`. |
| PL-02 | the mock app's `/` and `/healthz` under the research source | `test_pl02_root_and_healthz_never_raise_under_the_research_source` | 200; `data_source` names the research source, never "mock"; the endpoints listed are the served ones. |
| PL-03 | the exchange-calendar projection | `test_pl03_the_projection_skips_weekends_and_nse_closures` | Fri → Mon; Republic Day and Gandhi Jayanti skipped; n = 0 refused. |
| PL-04 | `engine_version` / `schema_version` on the feed; `due_session` on every pending card, LABELLED | `test_pl04_the_served_feed_stamps_versions_and_a_labelled_due_session_on_pending_cards` | the edition row's research engine hash (+ the S2 hash when cards ride on it); an engine-stamped date keeps `session_calendar`; a null one is projected and says `projected`; a graded card's verdict and realised facts are untouched; the whole payload re-validates. |
| PL-04b | additive contract | `test_pl04b_the_contract_carries_the_new_fields_as_optional_additive` | the new fields default to null. |
| PL-10 | app: `GateView.insufficient` never renders as FAIL | `tests/lib.test.ts` · `an insufficient gate reads "not enough data" …` | glyph `·`, label *not enough data*, neutral tone; passed / failed / advisory keep theirs. |
| PL-11 | app: a projected due session is labelled | `tests/lib.test.ts` · `a projected due session is labelled; the engine's own is not` | `dueBasisShort` → `projected` only for a `projected: …` basis. |
| PL-12 | app: additive fields propagate, never invented | `tests/feed.test.ts` · `the next story carries each pending card's due-session basis verbatim …` | `dueBasis` verbatim on the next story (null when not served); the empty-edition story names `engine_version` only when served. |
| PL-13 | contract (live): the feed names its engine and shape | `contract.test.ts` · `the feed names the engine that computed it …` | `pathfinder_research@x.y.z+code.<12 hex>`; `pathfinder_experiments@` when cards ride; `pathfinder_feed@…+research_store.n+experiments_store.n`. |
| PL-14 | contract (live): every pending card has a due session with a basis | `contract.test.ts` · `every pending card has a due session, and a projected one says so …` | due after the edition; basis `session_calendar` or `projected`; a grade never rests on a projection; a continuation is never projected from its own edition. |
| PL-15 | contract (live): legacy endpoints never 500 | `contract.test.ts` · `the P0/P1 endpoints the research source does not serve …` | 404 `not_served_by_source` with `use[]`, no internals. |

Gates on the final tree: backend 300 passed · `gen_openapi.py --check` in sync (35 additive lines) ·
`npm run typecheck` clean · `npx expo lint` 0/0 · `npm test` 36 passed · `npm run test:contract` 19 passed
against the live research API on `:8010` · `npx expo export --platform all` exit 0 (17 static routes).
