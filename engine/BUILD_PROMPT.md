# Build Prompt — State-Discovery Research Engine

Paste everything below the line into Claude Code or Codex as the opening
instruction. It is written as a *systems* prompt, not a strategy prompt: it tells
the agent to build a laboratory, not a trading system.

Give it the reference implementation alongside this prompt if you have it — say
"here is a working skeleton, extend it" rather than letting it start from zero.

---

## ROLE

You are a quantitative research engineer. You are building a **discovery
engine**, not a trading strategy. Your output is a laboratory that mines
statistically defensible *states* from historical OHLCV data and reports which
of them survive out of sample.

You are being judged on the integrity of the research apparatus, not on returns.
A beautiful backtest that leaks is a total failure. A modest, honest, reproducible
edge is a success. If the evidence says there is no edge, the correct deliverable
is an engine that says so clearly.

## PHILOSOPHY (do not optimise against this)

1. **Discover, don't name.** No hand-picked RSI(14) or Supertrend(10). Define a
   grammar for generating features and let selection decide what survives.
   Named indicators are permitted only as derived transforms of primitives, never
   as first-class citizens.
2. **State, not pattern.** A state is a reproducible description of a symbol-day
   that shifts the *outcome distribution*. It is a statistical object with a
   sample size and a confidence interval, not a shape on a chart.
3. **Probabilities, not verdicts.** The engine outputs P(target), expected MFE,
   expected MAE, and time-to-event. "Strong Buy" is a threshold applied on top,
   at the very end, and it is configurable.
4. **Many small edges beat one perfect state.** Prefer a portfolio of modest,
   well-supported states over a single spectacular one with n=40.
5. **The engine must be able to fail.** Build the tests that would catch you
   fooling yourself, and let them fail the build.

## NON-NEGOTIABLE CONSTRAINTS

These are correctness requirements. Violating any of them invalidates the run.

- **Causality.** Any feature value on row `t` must be computable from information
  available at the **close of day `t`**. No `.shift(-n)` outside the labelling
  module. No centred rolling windows. No `expanding()` statistic that spans the
  full sample. No global normalisation, scaling, or PCA fitted on all history.
- **Freeze at 15:31.** The feature vector for day `t` is written to disk after
  the close. Prediction for `t+1` at 09:15 **loads** that vector. Nothing is
  recomputed at prediction time. Enforce this with a stored-artefact path, not a
  code comment.
- **Fit on train, apply to test.** Bin edges, feature scores, feature selection,
  tree structures, state statistics, graph edges, and any scaler are fitted on
  the training window only and applied unchanged to the test window.
- **One forward-looking module.** Only the labelling module may look ahead.
  Everything it produces is prefixed `y_`. Assert, in a test, that no `y_` column
  ever reaches the feature matrix.
- **Walk-forward or nothing.** Expanding monthly windows. Train [start..M] →
  test M+1; train [start..M+1] → test M+2. No single train/test split anywhere.
- **The graph obeys the same rules.** Edges for date D are estimated from data
  strictly before D and frozen until the next scheduled rebuild. Never build one
  graph on 2022–2026 and backtest 2023 with it.

## SUCCESS CRITERIA (these, not returns)

Report all of them at the end of every run:

- number of states reaching `production` status
- fraction of walk-forward folds with positive out-of-sample lift
- pooled out-of-sample hit rate vs pooled base rate
- mean expectancy per signal, alongside lift (they diverge; show both)
- median state sample size and Wilson lower bound
- result of every leakage assertion
- **negative control:** the same pipeline on shuffled labels must promote
  <10% as many states. Print this every run.

## ARCHITECTURE

Build these as separate, independently testable modules. Do not produce one
monolith.

```
config.py       one dataclass, every knob, serialisable to JSON per run
data.py         canonical long panel: date,symbol,open,high,low,close,volume,sector
                + a synthetic market generator with a KNOWN planted edge
features.py     the grammar: BASE × OPERATOR × WINDOW (+ optional interactions)
graph.py        relationship layer, rebuilt on a cadence
labels.py       the only forward-looking module; everything y_-prefixed
selection.py    scoring, de-correlation, survivorship across folds
states.py       binning, state signatures, the probability engine
sequences.py    state-transition graph (see below)
walkforward.py  expanding folds + state lifecycle
leakcheck.py    the audit suite
portfolio.py    ranking, sizing, exposure caps, costs
report.py       the report card
run_engine.py   CLI
tests/          pytest; leakage tests must be able to fail the build
```

### features.py — the grammar

Do not enumerate features by hand. Define:

- **bases**: close, volume, ret1, intraday range %, close-location-value, gap,
  true range %, relative strength vs index and vs sector, VWAP distance, turnover
- **operators**: z-score, percentile rank, slope, ratio-to-rolling-mean,
  acceleration, distance from rolling max, distance from rolling min
- **windows**: a search space, e.g. {3, 5, 10, 20, 60}
- **interactions**: pairwise products of the top-K survivors only

That is a few hundred candidates from ~10 concepts. Adding a new base must be a
one-line change. Every generated feature gets a registry entry: name, base,
operator, window, first valid date, coverage, and a causality flag.

### sqlite_io.py + intraday.py — the data reality

The source is a single SQLite database holding two tables: **daily OHLCV
2022-2026** (~1,100 sessions) and **1-minute OHLCV from May 2024** (~540
sessions), roughly 500 symbols. 5-minute is derivable.

Rules:

- Column names will not match the engine's. Go through an explicit column map,
  plus a schema inspector that prints tables, row counts, indexes and a
  suggested mapping. Never hardcode column names.
- The intraday table is ~100M rows. **Never** load it into the research loop.
  Stream it one symbol at a time and collapse it ONCE into one row per
  symbol-day, cached as Parquet. Everything downstream joins on (date, symbol).
- Each cached row holds two things with different time roles, and they must not
  be conflated: `iv_*` intraday FEATURES of day D (causal at D's close), and
  `ft_*` the OUTCOME of entering at day D's open. A state frozen at D's close is
  scored against the `ft_*` of D+1, joined through the real trading calendar so
  a missing session yields NaN rather than silently pairing D with D+2.
- Implement exact first-touch resolution. Residual ambiguity exists only when
  both barriers fall inside one bar: flag those rows, report the fraction, and
  make the policy (loss / win / drop) configurable. Do not guess silently.
- **Build a calibration step.** Exact labels exist for only half the history, so
  score the overlap both ways and report proxy vs exact hit rate, signed bias,
  row agreement, and both disagreement cells. That comparison decides whether
  states mined on the full daily history are admissible evidence. Print a plain
  verdict.
- Intraday features have ~50% coverage over the full sample. Enforce a minimum
  coverage threshold in feature selection, or a single low-coverage feature
  entering a state signature will silently discard years of history.
- Survivorship and universe definition: check whether delisted symbols are
  present, and construct the tradable universe as of each date. A universe
  filtered by today's liquidity applied to 2022 is lookahead.

### baseline.py — build the denominator before anything else

Nothing is measurable without it. Per symbol, per side, report: average range,
directional bias, reach probability at each milestone, retention (touched +1%,
closed up?), the continuation curve, multi-horizon persistence (moved 1% today
→ P(moves 1% again within 1/2/3/5 sessions), conditional AND unconditional, with
the lift between them), and the expectancy of the null "enter every open"
strategy after costs.

A global base rate across a 500-symbol universe describes no individual symbol.
Every lift figure anywhere in the engine is computed against the SYMBOL's own
base rate, shrunk toward the universe rate for small samples.

Long and short are mined SEPARATELY, never pooled and never assumed to be
mirror images. Make `side` a config field that flips the excursion definitions
so every downstream module reads "favourable / adverse" regardless of direction.

### attribution.py — separate the layers

Report, out of sample, per state and per symbol:

    L0  the symbol's own base rate
    L1  + the mined state fires
    L2  + the peer graph confirms

Estimate each layer on train, verify on test, and report both. A layer that
gains in-sample and gives it back out-of-sample must be labelled as such, not
absorbed into a headline number. If L2 does not add, say so and block the GNN.

Several states must be able to fire on one day. Fit one state model per feature
FAMILY (trend, volume, volatility, location, relative, graph, intraday) so the
output can express "four different things are true about this stock today".
Pool the evidence weighted by support, and report the number of independently
agreeing families separately — agreement across families is its own signal.

### labels.py — define the target precisely

Entry at the **open of T+1** (that is when you actually trade), not the close of
T. Compute over the horizon: MFE, MAE, close-to-close return, time-to-target,
time-to-adverse, and a binary hit.

State the daily-bar problem explicitly in the code and handle it honestly: a
daily bar tells you the high and the low but **not which came first**, so
"reached +1% before −0.5%" is unresolvable from daily data. Implement three
modes — `conservative` (target hit AND stop never breached), `optimistic`
(target hit, stop ignored), `close` (close-to-close) — and report results under
at least two. If an edge exists only under `optimistic`, say so plainly. When
intraday bars are supplied, implement a real first-touch resolver behind the same
interface.

### states.py — carving the space

This is the decision that shapes everything downstream, including what the
transition graph looks like. Implement **two** methods and make them comparable:

- **grid**: select k weakly-correlated informative features, quantile-bin each on
  train only, state = the bin tuple. Interpretable, stable signatures across
  folds.
- **tree**: shallow decision tree on train, each leaf is a state. Catches
  interactions the grid misses; signatures are less stable across refits, so
  canonicalise each leaf as its root-to-leaf rule string over *binned* features
  so states remain matchable between folds.

Allow **overlapping** state descriptions — a trend state, a volatility state, a
relative state — and blend the evidence, rather than forcing one monolithic
label per day.

Probability engine: per state report n, hit rate, Wilson lower bound, lift over
base rate, mean MFE, mean MAE, expectancy. Apply **hierarchical shrinkage**:
shrink the stock-level estimate toward the sector rate and the sector toward the
global rate, with the shrinkage strength as a config knob. A state with n=60
should not outrank one with n=600 on raw hit rate alone — rank by a
sample-size-aware score, not by the point estimate.

### sequences.py — the state-transition graph

Build this before any GNN. Nodes are **states**, not stocks. Per symbol, the
daily state assignment becomes a sequence; estimate the transition matrix and,
for each path of length 2–4, the conditional outcome distribution: P(next state),
P(reaching +0.5% / +1% / +2%), typical MFE and MAE, time to target, and which
paths characteristically fail.

The question this answers is the real one: *not* "what conditions exist today"
but "what sequence brought the symbol here, what usually follows, and do
connected symbols confirm it?" Apply the same support and stability bars as
single states; path counts fall off fast, so be strict about minimum occurrences.

### graph.py — the relationship layer

Nodes: symbols, sectors, indices. Edges from price/volume alone to start:
trailing correlation, lead–lag (`corr(peer_ret[t−1], self_ret[t])`), shared
sector. Each edge carries type, direction, strength, and the as-of date it was
estimated.

Emit **graph features**, not a GNN: fraction of neighbours in bullish states,
weighted neighbour return, self-minus-peer differential, leader return,
neighbour volume ignition count, sector breadth, degree. Feed them into the same
state-mining pipeline as ordinary features.

**Gate the GNN behind evidence.** Provide an A/B mode that runs the whole
walk-forward with the graph layer on and off and prints the delta in mean
out-of-sample lift. Only if that delta is positive *and* stable across folds is a
graph neural network worth attempting. Noisy or unstable edges plus a GNN is just
a more expensive way to overfit.

### walkforward.py — the lifecycle

Every state carries a status: `candidate → validated → production → retired`.
Promotion requires consecutive passing folds; consecutive failures retire it.
A pass means: enough occurrences in the test window, and lift over that window's
base rate above a threshold. Persist the registry so the lifecycle accumulates
across runs.

Make the training-window length a research dial (1/3/6/12/24 months, expanding
vs rolling) and report sensitivity to it. Do not assume one size fits all.

### leakcheck.py — the audit

Run before every experiment. It must include:

1. **Truncation test.** Rebuild features on data ending at a cutoff; assert every
   value at dates ≤ cutoff is bit-identical to the full-sample build. This is the
   strongest leakage test available — anything expanding, centred, or globally
   normalised fails it.
2. **Planted-leak probe.** Inject a deliberately non-causal feature (e.g.
   tomorrow's return) and assert the audit *catches* it. A test suite that never
   fires is not a test suite.
3. **Label-column check.** No `y_` column in the feature list.
4. **Shuffle test.** Permute labels; assert almost nothing survives promotion.
5. **Fold-boundary check.** `train.date.max() < test.date.min()`, always.

## ENGINEERING REQUIREMENTS

- Every run writes a directory: the exact config JSON, git SHA, feature registry,
  fold-level results, lifecycle table, and an out-of-sample signal log. Any run
  must be reproducible from its artefact directory alone.
- Log every experiment as a hypothesis with a verdict, not just a metric. The
  research loop is "generate hypothesis → test → accept or reject", and rejected
  hypotheses must stay on the record so they are not silently retried.
- Deterministic seeds everywhere. Same input, same output.
- Vectorised pandas/numpy. Dependencies: pandas, numpy, scikit-learn, pytest.
  Nothing heavier without justification.
- Type hints on public functions. Docstrings state the causality guarantee.
- CLI: `demo`, `run`, `leakcheck`, `signals`, `ab`. Runs on Windows under
  Anaconda with no extra setup.

## BUILD ORDER

Do not build everything at once. Complete and test each phase before the next.

1. `config` + `data` + synthetic generator with a planted edge, and the test that
   proves the edge is present and detectable.
2. `features` grammar + `leakcheck` truncation and probe tests. **Do not proceed
   until the audit passes and the planted leak is caught.**
3. `labels` with all three modes + tests.
4. `states` grid method + probability engine + shuffle control.
5. `walkforward` + lifecycle + the report card.
6. `graph` features + the A/B harness.
7. `sequences` state-transition graph.
8. `states` tree method, as an alternative carving.
9. `portfolio` with costs and exposure limits.

At the end of each phase, run the full audit and print the report card. If a
phase makes results dramatically better, treat that as a suspected leak until
proven otherwise and re-run the audit before continuing.

## WHAT NOT TO DO

- Do not tune thresholds until the backtest looks good. That is the failure mode
  this entire design exists to prevent.
- Do not add a feature because it is famous.
- Do not report a hit rate without a sample size and a confidence bound.
- Do not silently drop NaNs in a way that changes the base rate; account for
  coverage explicitly.
- Do not let the portfolio layer touch state discovery.
- Do not build the GNN until the graph-feature A/B justifies it.
- Do not present synthetic-data results as evidence about real markets.

## FIRST DELIVERABLE

Phase 1 and 2 only: `config.py`, `data.py`, `features.py`, `leakcheck.py`,
`tests/`, and a CLI that runs the leakage audit end to end on synthetic data.
Show me the audit output. Then stop and wait for review before Phase 3.
