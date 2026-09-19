# V3 — Autonomous Quant Research Agent
## Specification for review. Nothing gets built until this is agreed.

---

## 0. The one thing I need to argue with first

Your brief says: *"saying no, not possible is not an option."*

I agree with that completely for **search** and I have to reject it for **evaluation**,
because those are different acts and conflating them is how this project dies.

**Search must never stop.** If a hypothesis fails, the agent proposes the next one.
It never reports "there is no edge here" and halts. It never asks permission to
continue. It never declines a direction because it looks hard. On that, your
instruction stands exactly as written and is encoded as a hard rule below.

**Evaluation must be free to say no.** The single most valuable thing this engine
has produced in weeks of work is a set of correct rejections:

- the monotone column looked like a discovery; a permutation test showed the best
  value was no better than the best of random noise
- the volume hypothesis had six correlated features agreeing; it failed at 9/18 folds
- fifteen exit policies were compared and the winner was selection, not skill
- a scorecard graded states "A" for losing less money than a losing baseline

Every one of those would have been shipped by an agent that cannot say no. An
agent forbidden from rejecting will always find something — that is guaranteed by
multiple testing, not a matter of tuning. With enough candidates, noise produces
whatever you asked for.

So the rule is:

> **The agent may never refuse to search. The agent must always be willing to
> reject a candidate. Persistence in looking is not the same as credulity in
> believing.**

If you disagree with this framing, say so now, because everything below assumes it.

---

## 1. Mission

Two independent agents, one per stock:

- **AGENT-ADANI** → ADANIENT
- **AGENT-CARTRADE** → CARTRADE

Each runs continuously and autonomously to find, validate and promote trading
playbooks for its own stock.

**Goal:** average **+1% per day** on a fixed **₹30,000** allocation, no compounding.
Any holding period qualifies — intraday, overnight, 2-day, 7-day, X-day — provided
the *average per calendar day held* reaches 1%.

**What that means numerically, stated plainly:** ₹300/day, ~₹75,000/year on ₹30,000
of capital. That is **~250% annually, unlevered.** I am not going to tell you it is
impossible, and I am not going to pretend it is a routine target. It is roughly an
order of magnitude above what the unconditional baselines for these two stocks
support (ADANIENT short: +0.051%/day; CARTRADE: negative both sides). The gap
between 0.05% and 1.00% is what the agent exists to close, and the honest position
is that we do not yet know if it can be closed. The agent's job is to keep
attacking it and to report progress against it truthfully.

---

## 2. The locked holdout — agreed: 2026 H1

**2026-01-01 → 2026-06-30 is sealed.** Neither agent may read it, load it, sample
it, or compute a single statistic from it, at any point, for any reason.

Enforcement, not convention:
- research code loads through a wrapper that hard-filters `date < 2026-01-01`
- the wrapper raises if any query touches the sealed range
- the sealed slice lives in a separate SQLite file the research process cannot open
- an audit hash of the sealed data is recorded at seal time and re-checked at deploy

**It is opened exactly once, at deploy, per playbook.** After a playbook is
evaluated on the holdout the result is permanent — pass or fail — and that
playbook may not be revised and re-tested against it. Iterating against a holdout
is what turns it into a training set.

Research data: 2022-01-01 → 2025-12-31 (992 sessions). Within that, the agent runs
walk-forward internally; the holdout is a second, harder gate on top.

---

## 3. Architecture — atoms

Each agent is a supervisor over specialised atoms. Atoms are independent, testable,
and communicate only through a shared registry.

| atom | responsibility |
|---|---|
| **MICROSTRUCTURE** | mines the 1-minute tape for facts, not averages (§4) |
| **HYPOTHESIS** | proposes falsifiable candidates, maintains the open queue |
| **LABEL** | constructs targets across sides, horizons and transition points |
| **VALIDATION** | walk-forward, permutation, shuffle control, stability |
| **EXECUTION** | entry timing, pyramiding, exits, costs, slippage, fills |
| **CAPITAL** | position sizing, margin, exposure, drawdown control |
| **ADVERSARY** | actively tries to break every surviving candidate (§6) |
| **CURATOR** | registry, lifecycle, rediscovery, retirement |
| **REPORTER** | daily digest, honest scorecard, progress against +1% |

The **ADVERSARY** atom is not decoration. Its sole purpose is to destroy the
agent's own findings, and it is scored on how many it kills.

---

## 4. The microstructure atom — where your 1-minute thesis lives

You are right that averages destroy the information. A stock's day is a sequence of
regimes — accumulation, markup, distribution, markdown — and a daily bar collapses
all of it into four numbers.

This atom does **not** compute rolling means. It extracts **events** from the
1-minute tape, timestamped, with the state around them.

Your examples, made concrete and measurable:

**Price pushed down on high volume, then dry-up** — falling ATP (turnover ÷ volume)
with volume in a high percentile, followed by a volume collapse below a low
percentile while price stops making new lows. Measured: bars since the last new low,
volume decay rate, ATP slope, whether the bid side thins.

**Volatility contraction before expansion** — realised volatility in rolling
windows compressing to a low percentile of its own recent history, range narrowing
bar over bar, then the breakout direction and its follow-through.

**Repeated downward pressure** — consecutive bars closing in the lower part of
their own range on above-median volume, and how many such bars precede exhaustion.

Beyond those, the atom is instructed to discover its own event types. The
vocabulary it starts with is: time-of-day, volume percentile, ATP direction,
range percentile, close location within bar, consecutive-bar patterns, VWAP
distance and crossings, volume-at-price concentration, gap behaviour, opening
range structure, first/last hour asymmetry, and any composition of these.

**No boundary is placed on what it may hypothesise.** The only constraint is that
every event must be computable from bars strictly before the decision point, and
every event must be falsifiable.

---

## 5. Run protocol — parallel bursts, agreed

Continuous operation, structured as bursts so results are checkpointed and the
machine is not left in an unknown state.

- **burst = 30–60 min**, 3 parallel workers (~3 GB each, sized to your RAM)
- each burst: pull the top-priority hypotheses, test, write to registry, checkpoint
- between bursts: CURATOR consolidates, ADVERSARY attacks survivors, HYPOTHESIS
  re-prioritises from what was learned
- crash-safe: every burst resumable, registry is append-only with schema versioning
- a burst that finds nothing is a normal outcome and is logged as evidence, not
  failure

**Self-improvement is concrete, not aspirational:** the agent tracks which
hypothesis *families* produce survivors and reallocates search budget toward them,
and it records dead ends permanently so no cycle is ever spent re-testing a
rejected idea.

---

## 6. Validation — the part that cannot be negotiated

Every candidate passes all of these or it does not exist:

1. **Causality** — truncation test: rebuild features on truncated data, demand
   bit-identical values. A deliberately planted leak must be caught every run.
2. **Walk-forward** — expanding monthly, train strictly before test.
3. **Search correction** — permutation null over the *maximum* statistic across all
   candidates tried. Reported alongside every headline number, always.
4. **Shuffle control** — the same pipeline on permuted labels must promote <5%.
5. **Absolute profitability** — expectancy > 0 net of 16 bps, not merely better
   than a losing baseline.
6. **Stability** — consistent across years, robust to ±20% parameter changes and
   ±5 min timing shifts.
7. **Rediscovery** — survives re-derivation under a different universe slice,
   feature set or seed.
8. **Capacity** — realistic fills at the traded size on that stock's actual volume.

**ADVERSARY** then attacks: shifts the entry by a few minutes, re-tests on a
different sub-period, removes the best 5% of trades, adds pessimistic slippage,
and checks whether the result is one market regime in disguise.

Only what survives all of it reaches the holdout.

---

## 7. Capital and measurement — agreed

- **₹30,000 fixed per stock, no compounding.** Every result reported in rupees and
  in per-day percent on that fixed base.
- Costs: 11 bps round trip, plus 5 bps slippage, plus a per-stock liquidity
  adjustment the EXECUTION atom estimates from the tape.
- Leverage reported as a separate view, never assumed.
- Multi-day performance normalised to **per calendar day held**, so a 7-day trade
  returning 7% counts as 1%/day and competes fairly with intraday.
- Positional shorts modelled through futures only — contract size, margin,
  rollover, expiry — or not modelled at all. Never as inverted cash equity.

---

## 8. Deployment gate

A playbook goes live only when:

1. all of §6 passed, and
2. the sealed 2026 H1 slice is opened once and the playbook holds there, and
3. paper-trading forward for 20 sessions matches the expected distribution.

If the holdout fails, the playbook is dead. Not revised — dead. It goes to the
registry as a permanent record so no future cycle re-proposes it.

---

## 9. What I need from you before building

1. **Do you accept §0** — search never stops, evaluation may reject?
2. **2026 H1 sealed** — confirmed, or a different window?
3. **RAM** — decides worker count for the parallel bursts.
4. **Runtime** — hours per day the machine can be occupied?
5. **What ends this?** If after N bursts the best validated playbook is +0.15%/day
   rather than +1%, is that a success worth trading, or a failure? I would rather
   agree the answer now than discover we disagree later.
