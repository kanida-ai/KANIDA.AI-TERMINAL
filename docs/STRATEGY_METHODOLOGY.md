# STRATEGY_METHODOLOGY — KANIDA.AI

Owner session: `quant` (see `docs/DOC_MAP.md`). Read by every agent/strategy session and by
`dev-quant-auditor`.

| Section | Status |
|---|---|
| §1 Shared quant law (book conventions, point-in-time, the go/no-go gauntlet) | **written, P0** — applies to every manager |
| §2 Pathfinder methodology (the research loop) | **written, P0** |
| §3 The `pathfinder_llm` gateway contract | **written, P0** |
| §4 Trader rulebook | *stub — founder input; session S1.1* |
| §5 Investor rulebook | *stub — founder input; session S2.1* |
| §6 Base-rate / observation rules | *stub — founder input; session S2.3* |

---

# §1 Shared quant law

These apply to **every** number this product renders, from any manager. They are the same laws the
global quant rules state, written once here so no session re-derives them.

**Point-in-time is law.** A decision at time *t* uses only data available at *t*. Every price read
takes an `as_of`. A historical occurrence is usable only when
`available_from_idx = entry_idx + MAX_HORIZON - 1 <= today_idx`. No look-ahead, no survivorship;
the universe is point-in-time; prices are corporate-action-adjusted.

**Marking convention.** Entry is the **next open after the signal bar** — never the signal bar
itself. Exits are rule-driven (stop / trail / horizon / invalidation), never discretionary. Intraday
books mark to the stated close time.

**Costs.** Every simulated trade is charged the versioned cost convention (brokerage + STT +
exchange + GST + stamp) **plus** slippage. Every result is also re-run at **2× slippage**; an edge
that only exists at the friendly assumption is a cost artefact, not an edge.

**Expectancy decides — never win rate.** Expectancy per trade, net of costs, is the hero metric.
Win rate, average win and average loss are supporting detail and are never rendered as the headline.

**Evidence must be the strategy that is traded.** A historical claim is a **strategy replay** under
the exact stop / target / trail / horizon that the virtual book uses — never raw hold-to-close.

**Every number carries n + date range + data source + cost convention.** n < 50 is flagged, n < 20
is greyed. A statistic with no n does not ship.

**Every return is shown with its drawdown**, both worst and current. There are no target prices and
no return promises anywhere in the product.

**Discovery vs out-of-sample.** Any rule calibration uses a discovery window; the forward window is
never touched during calibration. Pathfinder's forward virtual book *is* the out-of-sample.

**The go/no-go gauntlet.** Before any hypothesis is allowed to hold virtual capital:

1. Deterministic strategy replay over the discovery window only.
2. Positive expectancy **net of costs**.
3. Positive expectancy at **2× slippage**.
4. Sample big enough to speak: n ≥ 50, else it is labelled and not promoted.
5. Not a duplicate of existing work (novelty check against recall).
6. The rule has an **invalidation**, not a target.

Failing any gate is a publishable outcome, not a hidden one.

**Six things P1 added to the gauntlet. Four were designed in; two were forced by an
adversarial audit that found the first version of them wrong.**

1. **A baseline, and the edge measured against it.** The universe the engine queries is
   `in_nifty500 = 1 AND is_active = 1`: **498 names, of which 0 stop trading before the
   last session and 182 were not listed when the discovery window opened.** That is two
   biases, not one — survivorship (caused by `is_active`, not by an accident of backfill)
   and index-membership look-ahead. Both inflate ABSOLUTE returns. So every result is
   reported alongside `baseline` — what the *same* universe did over the *same* window
   under the *same* exits — and candidates are ranked on `edge = signal − baseline`, where
   the bias is common to both terms. Measured residual: splitting discovery by each
   symbol's eventual appreciation (unknowable at *t*) moves the edge between 1.15 and
   2.24 while the absolute expectancy moves much further. **The mitigation rescues the
   edge; it does not rescue the absolute number, which is why the absolute number is
   never a headline.**

2. **A placebo drawn BY DAY, not by trade.** This is the correction that mattered most.
   These screens do not produce *n* independent bets — a gap-up screen fires on a handful
   of market-wide sessions, so 1,840 discovery "trades" arrive on 195 days. An i.i.d.
   permutation null understates its own standard deviation by roughly the square root of
   the average cluster size: **measured at 0.20 against the correct 0.84, a factor of
   four**, which turns a p-value of a few percent into a p-value of zero. The null is now
   built by resampling **days** and taking from each drawn day as many outcomes as the
   signal took on one of its days, so the comparison is like-for-like in shape as well as
   in size. `replay.block_placebo`.

3. **A cluster-robust t, on signal days.** For the same reason. On one real experiment
   the naive t was **7.25** and the clustered t was **1.63**. The clustered one is the
   number that is published and the number that gates. `replay.cluster_robust_t`.

4. **Significance gates PROMOTION, not survival.** Discovery is a *screen* at the naked
   bar; the family-wise bar (`alpha / n_candidates`) is recorded next to it as an
   **advisory** so the selection inflation is visible rather than forgiven. Validation
   kills on the economics — expectancy, 2× slippage, edge, sample size — and records
   significance as advisory too. Both advisories become **requirements for promotion**.
   The reason is that an idea which is positive out of sample but not yet distinguishable
   from chance is exactly what a virtual book exists to accumulate evidence on: killing it
   treats absence of evidence as evidence of absence, and promoting it does the opposite.
   An empirical p-value is reported at its resolution floor (`1/draws`), never as zero.

5. **An implementability gate, before ranking.** The single strongest candidate in the P1
   scan was a multi-session **short** — not executable in the NSE cash segment under the
   delivery cost convention; it would need stock futures, which Pathfinder does not model.
   It was killed on that basis alone, with a published post-mortem. An edge nobody can
   trade is not a finding; it is a distraction that costs a slot.

6. **The book gate, and a decomposition rather than a story.** A rule's expectancy is
   measured over every firing; a book takes only what it has capital and slots for. When
   they differ, the difference must be **measured, not narrated**. The first version of
   this engine asserted that the difference was the book's tie-break, computed the
   alternatives, and discarded the comparison; the audit ran the discarded numbers and the
   tie-break explained **4%** of the gap. The engine now publishes the spread across every
   approved selection rule, the share of the gap it explains, and the competing structural
   explanation — that **47%** of the rule's trades arrived on its three busiest sessions,
   which a ten-slot book is structurally unable to be in — and only claims the tie-break
   when the share supports it.

---

# §2 Pathfinder methodology

Pathfinder is an **LLM research brain wrapped around a deterministic quant engine.**

```
Market data
  → DETERMINISTIC scanners / statistical engine      (Python/SQL — computes EVERYTHING)
  → LLM generates a hypothesis                        (proposes)
  → DETERMINISTIC backtest / replay                   (point-in-time, after costs)
  → LLM interprets the computed evidence              (interprets)
  → virtual trade, opened BY RULE
  → DETERMINISTIC tracking / marks / P&L              (computes)
  → LLM reviews the outcome                           (reflects)
  → learning recorded, inside the Constitution
  → LLM proposes the next experiment                  (decides what next)
```

## §2.1 The one rule everything else serves

**The LLM never calculates.** It does not compute win rate, expectancy, drawdown or P&L from
candles — Python/SQL does, point-in-time, after costs. The LLM proposes, interprets, reflects,
decides-what-next, and narrates.

This is enforced in three independent places, so a bug in one does not produce a dishonest number:

1. **Database** — `pathfinder.story_lines` has a CHECK that LLM-authored `headline`/`body`, with
   `{{fact:…}}` tokens stripped, contains no digit; `facts`/`outcomes`/`evidence` have a CHECK that
   `computed_by` does not name a model.
2. **API contract** — `StoryLine` in `docs/openapi.yaml` carries the same rule as a Pydantic
   validator; a payload that breaks it cannot be serialised.
3. **Gateway** — a provider implementation must raise `OutputContractViolation` rather than return
   prose containing a numeral, and must verify every `{{fact:…}}` it returns was actually supplied
   in the call's `facts`.

Numbers reach the customer as **facts**: `{{fact:fct_e3_virt_exp}}` in the narrative, resolved
client-side against `facts[]`, where each fact carries its own n, window, source, cost convention
and the deterministic component that computed it.

> The one thing that is *not* a "number originating in the LLM": a **rule parameter**. "A 3-session
> pullback below 0.75× median volume" is a rule *definition*, stored in `pathfinder.parameters`
> inside a Constitution-approved range — not a computed result. Rule text may carry those; narrative
> claims may not.

## §2.2 Autonomy = continuous observation, intelligent activation

The deterministic observer runs continuously and cheaply. **The LLM is woken by a trigger, never by
a clock.** The trigger types (`pathfinder.triggers`):

| Trigger | Fires when |
|---|---|
| `unusual_market_condition` | The observer rates a condition unusual against its own history → consider novelty → search recall → decide whether a new hypothesis is justified. |
| `observation_threshold` | A virtual experiment reaches **30 new observations** → review the evidence. |
| `performance_deviation` | Live performance deviates materially from expectation → investigate why. |
| `scheduled_review` | The evening research window (the only time-based one; it schedules deterministic work, and only wakes the LLM if that work produced something). |
| `human_request` | The founder queued a question directly. |

`triggers.fired_by` may only be `engine` or `human`. A model can never wake itself.

## §2.3 Governance — the versioned Constitution and the 4-level hierarchy

| Level | What the agent may do | Gate |
|---|---|---|
| **L1 Evidence** | Append new observations to the evidence base. | Automatic. |
| **L2 Parameter** | Propose/tune a threshold. | Only **inside the Constitution-approved range** — a database CHECK, not a convention. |
| **L3 Strategy** | Create a **new strategy version**. | It may not **replace** an incumbent until it has been **backtested AND forward-validated**; the validation text is a NOT-NULL requirement on the change-log row. |
| **L4 Constitution** | **Nothing.** | Risk limits, honesty rules, compliance boundaries, permitted actions are **human-controlled only**. The agent learns *inside* the Constitution; it cannot rewrite it. |

**Every change records:** what changed → why → evidence → previous version → new version → whether
performance improved. `improved` may be `null` — "not yet enough forward evidence to say" is an
honest answer and is never guessed. This change-log is customer-facing copy, not an internal log.

## §2.4 Lifecycle and the graveyard

`queued → testing → validating → promising → promoted` — or `died`, at any point.

Death is a **published product feature**. A died experiment must carry a post-mortem with a cause,
the evidence, the retired version, and — the important field — **what we kept**: the learning that
survives the death. `exp_0007` in the P0 fixtures is the worked example: the gap-continuation
pattern is real, the tradeable edge is not, and the surviving learning ("a gap is a liquidity event,
not a direction event") became a gate every later intraday hypothesis has to pass.

---

# §3 The `pathfinder_llm` gateway contract

**Product code never calls a model SDK.** It calls one of three verbs on the gateway. Typed shape:
`backend/pathfinder/llm/gateway.py` (contract only — P0 ships **no provider implementation**).
If the code and this document disagree, this document wins.

## §3.1 Job → model routing (locked)

| Pathfinder job | `Job` | Model id |
|---|---|---|
| Generate a genuinely new hypothesis | `hypothesis` | `claude-sonnet-5` |
| Critique / reflection after an experiment | `critique` | `claude-sonnet-5` |
| Decide the next research direction | `decide_next` | `claude-sonnet-5` |
| Convert deterministic results into storyline | `narrate` | `claude-haiku-4-5` |
| Classify / tag / summarize | `classify` | `claude-haiku-4-5` |
| Exceptional hard research problem (rare) | `hard_research` | `claude-opus-5` |

The **job**, not the caller, picks the model. Changing a row is an **L4 (Constitution) decision** —
`pathfinder.llm_calls` has a CHECK pinning the permitted model set.

## §3.2 The three verbs

```python
reason(*, job, question, facts, context=(), schema_id="pathfinder.reason.v1",
       constitution_version, prompt_version, budget, batch=False) -> ReasonResult
narrate(*, beat, facts, context=(), schema_id="pathfinder.narrate.v1",
        constitution_version, prompt_version, budget, batch=True) -> NarrateResult
classify(*, text, labels, facts=(), schema_id="pathfinder.classify.v1",
         constitution_version, prompt_version, budget, batch=True) -> ClassifyResult
```

Common to all three:

- **`facts`** — already-computed deterministic numbers, each with its id, label, value, unit, n and
  provenance. This is the *only* numeric input a model ever sees. It never receives raw candles.
- **`budget`** — the hard daily cap, checked **before** the call. No headroom → `BudgetExceeded`, and
  the loop pauses. It does not improvise, degrade to a cheaper model silently, or write text without
  a model.
- **`constitution_version` / `prompt_version`** — recorded on every `llm_calls` row so any output can
  be reproduced and attributed.
- **Return value carries `usage`** — the real token counts from the provider response. An unmetered
  call is a bug.
- **Failure is loud.** `GatewayError` / `BudgetExceeded` / `OutputContractViolation`. A caller must
  degrade gracefully (skip the beat, keep the deterministic numbers) — never fabricate the text.

## §3.3 Structured output schemas

Every call uses **structured outputs**: `output_config: {"format": {...}}` on `messages.create`
(**not** the deprecated `output_format` parameter), and `strict: true` with
`additionalProperties: false` on any tool schema. Schemas are registered by `schema_id` and
versioned; changing one is a new id, never an edit.

**`pathfinder.reason.v1`**

```json
{
  "type": "object", "additionalProperties": false,
  "required": ["claim", "body", "fact_refs", "abstained"],
  "properties": {
    "claim":        {"type": "string", "maxLength": 160},
    "body":         {"type": "string", "maxLength": 1200},
    "fact_refs":    {"type": "array", "items": {"type": "string", "pattern": "^fct_[a-z0-9_]+$"}},
    "proposed_action": {"type": ["string", "null"],
                        "enum": ["open_experiment","tune_parameter","cut_version",
                                 "promote","kill","queue_next","no_action", null]},
    "learning_level":  {"type": ["string", "null"], "enum": ["L1","L2","L3", null]},
    "confidence":      {"type": ["string", "null"],
                        "enum": ["provisional","supported","strong", null]},
    "abstained":       {"type": "boolean"},
    "abstain_reason":  {"type": ["string", "null"], "maxLength": 300}
  }
}
```

`learning_level` cannot be `"L4"` — the enum makes a Constitution change unrepresentable.
`abstained: true` is a **first-class success**: "the sample is too small to say anything" is the
answer we want when it is the true one.

**`pathfinder.narrate.v1`**

```json
{
  "type": "object", "additionalProperties": false,
  "required": ["beat", "headline", "body", "fact_refs"],
  "properties": {
    "beat":      {"type": "string",
                  "enum": ["noticed","hypothesis","experiment","outcome","learning","next"]},
    "headline":  {"type": "string", "maxLength": 120, "pattern": "^[^0-9]*$"},
    "body":      {"type": "string", "maxLength": 900},
    "fact_refs": {"type": "array", "items": {"type": "string", "pattern": "^fct_[a-z0-9_]+$"}}
  }
}
```

The `headline` pattern makes a bare numeral **structurally impossible**. `body` may contain
`{{fact:…}}` tokens, so it cannot use the same pattern — the gateway checks it after the fact
(strip the tokens, assert no digit remains) and raises `OutputContractViolation` if it fails.

**`pathfinder.classify.v1`**

```json
{
  "type": "object", "additionalProperties": false,
  "required": ["label", "rationale"],
  "properties": {
    "label":     {"type": "string"},
    "labels":    {"type": "array", "items": {"type": "string"}},
    "rationale": {"type": "string", "maxLength": 400},
    "score":     {"type": ["number", "null"], "minimum": 0, "maximum": 1}
  }
}
```

`labels` is a **closed set** supplied by the caller; the gateway rejects a returned label that is not
in it. The model may not invent a category.

## §3.4 Prompt caching — the stable prefix

Caching is a **prefix match**: any byte change anywhere in the prefix invalidates everything after
it. Render order is `tools` → `system` → `messages`. Pathfinder's layout:

| Position | Content | Cached |
|---|---|---|
| `system` block 1 | The **Constitution** (frozen text for a given `constitution_version`) | ✅ `cache_control: {"type": "ephemeral"}` |
| `system` block 2 | Methodology + the honesty rules + the never-calculate instruction | ✅ same breakpoint |
| `messages[0]` | The experiment's rulebook + the **fact table**, rendered deterministically (sorted keys, stable formatting) | ✅ second breakpoint |
| `messages[-1]` | The volatile question / the new evidence | ❌ after the last breakpoint |

Rules that keep the hit rate real:

- **No timestamps, UUIDs or `datetime.now()` in the prefix.** The `as_of` belongs in the volatile
  tail, not the system prompt.
- **Serialise facts deterministically** (sorted keys) — an unsorted `json.dumps` is a silent
  invalidator.
- **Keep the tool list fixed** for a given job.
- **Verify, don't assume:** log `usage.cache_read_input_tokens` on every call. Zero across repeated
  same-prefix calls means something is invalidating the cache; that is an alert, not a mystery.
- Max 4 breakpoints per request; the minimum cacheable prefix is model-dependent, so a short prefix
  silently will not cache.

## §3.5 Batching

Non-urgent work goes through the **Message Batches** API at ~50% cost: `narrate` and `classify`
default to `batch=True`. `reason` defaults to `batch=False` — a hypothesis or a kill decision is
part of a loop turn and waits on nothing.

Batch results arrive in **any order** — key them by `custom_id`, never by position. `custom_id` is
the `llm_call_id` so the metering row can be closed when the result lands.

## §3.6 Budget and metering

- Every call writes a `pathfinder.llm_calls` row: job, model, prompt version, schema id, batched,
  the four token counters, `cost_usd`, latency, ok/error.
- The cap is enforced against `budget_day_ist` — **IST**, not the host's timezone.
- The check is **before** the call. `BudgetExceeded` pauses the loop; the deterministic observer
  keeps running and the queue keeps filling, so nothing is lost — the narrative just stops until
  tomorrow.
- **No fixed monthly cost assumption.** The daily cap is the control; measured spend is the input to
  choosing it. `GET /api/pathfinder/loop` surfaces `llm_usage` (window `today_ist`) with
  `daily_budget_usd` and `budget_used_pct` so the burn is visible from day one.
- Reference rates (input/output per MTok, first-party API): `claude-sonnet-5` $2/$10 ·
  `claude-haiku-4-5` $1/$5 · `claude-opus-5` $5/$25. Cache reads are far cheaper than fresh input,
  which is why §3.4 matters more than model choice.

## §3.7 Provider independence

Claude → GPT → Gemini must be a **config swap**. Therefore:

- Nothing outside a provider module may import a vendor SDK, name a vendor type, or read a
  vendor-shaped response. `backend/pathfinder/llm/gateway.py` contains no SDK import.
- `Usage` is the gateway's own shape; a provider maps its response onto it.
- Provider-specific request details (below) live **inside** the provider module.

Claude-specific details the P1 provider must get right:

- **Thinking:** `claude-sonnet-5` and `claude-opus-5` take `thinking: {"type": "adaptive"}`;
  `budget_tokens` is removed on those and returns 400. `claude-haiku-4-5` still uses
  `{"type": "enabled", "budget_tokens": N}` (N < `max_tokens`, minimum 1024) and **rejects**
  `output_config.effort` — so effort tuning applies to the Sonnet/Opus jobs only.
- **No assistant prefill** on any of the three models — use structured outputs to shape the response.
- **Mid-conversation `role: "system"` messages** are *not* supported on `claude-sonnet-5`; the
  gateway must not rely on them.
- Parse tool/structured inputs with a JSON parser, never string matching — escaping varies.
- Always check `stop_reason` before reading content.

## §3.8 What the gateway is NOT allowed to do

- Place, propose or route an order. Pathfinder emits **research**; execution is a Kite Publisher
  hand-off on the customer path and is out of Pathfinder's reach entirely.
- Write to `constitution_versions` (L4 is human-only).
- Return a number that did not arrive in its `facts`.
- Silently fall back to a different model, a cached answer, or a template when a call fails.
