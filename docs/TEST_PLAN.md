# TEST_PLAN — KANIDA.AI

Owner session: `qa` (see `docs/DOC_MAP.md`). Continuous — every build session appends the rows for
what it built. **Rule: no requirement without a test row.**

| Slice | Rows | Suite | Status |
|---|---|---|---|
| **Pathfinder P0** (contract + mock) | below | `backend/tests/test_pathfinder_p0.py` | **37 passing** |
| **Pathfinder P1** (engine) | below | `backend/tests/test_pathfinder_p1.py` | **60 rows** |
| **Pathfinder P2** (frontend) | below | `kanida-app/tests/{lib,contract}.test.ts` | **31 passing** |
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

1. **No on-device render test.** `npx expo export --platform all` proves iOS, Android and web all
   *bundle* from this one source (1238 / 1387 / 832 modules, exit 0), and the web target was driven
   by hand at 375 px and 1440 px in both themes. The iOS and Android bundles were **not run on a
   simulator or device** in this session — no macOS, no Android SDK here. First action for whoever
   picks up the app: `npm run ios` / `npm run android`.
2. **No visual-regression or a11y automation.** Contrast, focus rings and touch targets were
   designed for and inspected by hand, not asserted.
