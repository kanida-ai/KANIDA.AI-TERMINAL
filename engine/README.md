# State-Discovery Research Engine

A leak-proof lab for discovering *states* — data-mined combinations of conditions
that measurably shift the next-day outcome distribution — from daily OHLCV panels.

This is **not** a strategy and **not** a backtester. It is judged by one number:
how many discovered states survive walk-forward validation. A pretty equity curve
here means nothing; a state that keeps clearing its bar on unseen months means
something.

---

## Install

Anaconda Prompt or PowerShell:

```powershell
conda create -n stateengine python=3.11 -y
conda activate stateengine
cd path\to\state_engine
pip install -r requirements.txt
```

Verify:

```powershell
python -m pytest tests -q
```

All 14 tests should pass. Three of them are designed to fail loudly if you ever
introduce lookahead.

---

## Run it

Everything goes through `run_engine.py`. With no `--csv`, it generates a
synthetic market that contains a **deliberately planted edge**, so you can
confirm the machinery works before pointing it at real money data.

```powershell
# 1. leakage audit only  (~30s)
python run_engine.py leakcheck

# 2. audit + full walk-forward on synthetic data  (~4 min)
python run_engine.py demo

# 3. your own data
python run_engine.py run --csv data\panel.csv

# 4. what would I trade at tomorrow's open?
python run_engine.py signals --csv data\panel.csv

# 5. does the graph layer actually add anything?
python run_engine.py ab --csv data\panel.csv
```

Useful switches:

| flag | meaning |
|---|---|
| `--target 0.01 --stop 0.005` | the +1% / −0.5% barriers |
| `--horizon 1` | T+1 only; `3` = next three sessions |
| `--label-mode conservative\|optimistic\|close` | see *Honest limitation* below |
| `--state-method grid\|tree` | how the feature space gets carved |
| `--state-features 3` | conditions per state signature |
| `--no-graph` | kill the relationship layer |
| `--no-interactions` | kill pairwise products (strictest setting) |
| `--train-months 12` | history before the first test month |

Output lands in `outputs\`: `*_folds.csv` (per-month report card),
`*_lifecycle.csv` (every state and its status), `*_trades.csv` (every
out-of-sample signal), `*_config.json` (exact settings, so the run reproduces).

---

## Your SQLite database (daily 2022-2026 + intraday May 2024-2026)

Five commands, in order. Run each once.

```powershell
# 1. see what's actually in there, and get a starter column mapping
python run_engine.py inspect-db --db market.db

# 2. one-off: index the intraday table. Without this, 500 symbols means
#    500 full table scans. Costs disk, saves hours.
python run_engine.py index-db --db market.db

# 3. collapse ~100M intraday rows into one row per symbol-day.
#    Run once per (target, stop, resolution). Takes a while; it's the only
#    time anything reads the raw bars.
python run_engine.py build-intraday --db market.db --bar-minutes 5

# 4. THE IMPORTANT ONE: is the daily proxy label trustworthy?
python run_engine.py calibrate --db market.db --bar-minutes 5

# 5. research
python run_engine.py run --db market.db --symbols-limit 100      # fast iteration
python run_engine.py run --db market.db --exact-labels           # intraday era only
```

Step 1 writes `outputs/db_config.json` with a guessed mapping of your column
names. **Check it**, fix anything wrong, and pass it back with `--db-config`.

### Why step 4 decides your whole design

Your two tables cover different periods: daily gives ~1,100 sessions, intraday
~540. Exact first-touch labels only exist for the shorter window. So the
question is whether you may trust states mined on the full daily history.

`calibrate` answers it by scoring the same days both ways and comparing. On the
bundled test database the conservative daily proxy calls 11.9% winners where the
exact label says 17.4% — a 5.5-point understatement, 94.5% row agreement, and it
never claims a win that didn't happen (it can't; conservative is a strict subset).
Your real data will give different numbers. If the bias is small, mine on all
1,100 days. If it's large, only the 540 intraday days count as evidence.

### The coverage guard

Intraday features (`iv_*`) only exist from May 2024. If one got selected into a
state signature, every pre-2024 row would silently drop out of the sample. So
`min_feature_coverage` (default 0.95) excludes any feature that isn't populated
across the training window. Train on 2022-2026 and the `iv_*` features are
automatically ignored; restrict to 2024+ and they automatically come back. Two
tiers, one knob.

### Scale notes for 500 symbols

- 500 x 1,100 daily rows x ~400 features is roughly 2-3 GB peak in pandas.
  Develop with `--symbols-limit 100`, then run the full universe once settled.
- Use 5-minute bars for the main loop. The engine prints the same-bar ambiguous
  fraction; if it's under ~1%, 5-minute is enough and it's 5x faster than
  1-minute. Keep 1-minute for spot checks and for estimating open slippage.
- **Survivorship.** If your DB only holds symbols that are listed *today*, every
  backtest is biased. Check for delisted names. And define the universe as of
  each date — "top 500 by turnover" applied backwards to 2022 is lookahead.

---

## Your data (CSV route)

One long CSV. One row per symbol-day.

```csv
date,symbol,open,high,low,close,volume,sector
2022-01-03,RELIANCE,2380.0,2402.5,2371.1,2395.2,4821000,ENERGY
2022-01-03,ONGC,148.2,150.9,147.6,150.1,18220000,ENERGY
```

`sector` is optional but the graph layer is much better with it. Prices should be
split/bonus adjusted. There is also `data.load_yfinance()` if you want a quick
start (`pip install yfinance`, NSE symbols take a `.NS` suffix).

---

## Baseline first, always

Before mining anything, profile what each stock does on its own. A state that
hits 70% is remarkable on a stock whose base rate is 34% and worthless on one
whose base rate is 68%.

```powershell
python run_engine.py baseline --db market.db                    # whole universe
python run_engine.py baseline --db market.db --symbol ICICIBANK # one stock
```

The single-symbol report gives: average range, directional bias, reach
probability at ±0.5/0.7/1.0/1.5/2.0%, **retention** (touched +1%, did it close
up? — this is what tells you momentum vs mean-reverting), the **continuation
curve** (reached +0.5%, how often does it get to +0.7%, +1.0%?), **multi-horizon
persistence** (it moved 1% today — does it move 1% again within 1/2/3/5
sessions, and what is the lift over the unconditional rate?), and the **null
strategy** expectancy that any mined state has to beat after costs.

Both sides, always. Long and short are profiled separately because they are
different trades with different base rates.

## Layered attribution — base → state → graph

```powershell
python run_engine.py attribute  --db market.db --side long
python run_engine.py both-sides --db market.db          # long and short in one run
python run_engine.py evidence   --db market.db          # the 15:31 output
```

`attribute` reports, out of sample:

```
  L0  base rate, no state        : 0.161
  L1  + state fires              : 0.177   (+0.016)
  L2  + graph confirms           : 0.200   (+0.023 on top)
```

and a per-state table where the denominator is that **symbol's own** base rate,
not the universe average. If L2 fails to add out of sample the report says so
explicitly and tells you not to ship the graph layer or attempt a GNN.

`evidence` is the end-of-day run. One row per symbol, showing every state firing
from every feature family, how many independently agreed, whether the peer graph
confirmed, and the resulting probability against that symbol's base rate:

```
symbol   symbol_base  p_state  n_states  n_confirming  graph_confirms  p_final  signal
SYM025         0.129    0.163         6             3           False    0.163    WEAK
```

Several states firing at once is the normal case, not an edge case: one model is
fitted per feature family (trend, volume, volatility, location, relative, graph,
intraday), so agreement across independent families is itself evidence.

---

## What the modules do

```
raw OHLCV
  → data.build_index          equal-weight market + sector returns (same-day, causal)
  → features.build_features    the grammar: BASE × OPERATOR × WINDOW → ~320 candidates
  → graph.build_graph_features peer/lead-lag edges rebuilt on a cadence, never globally
  → labels.add_labels          THE ONLY forward-looking module; everything it makes is y_*
  → states.fit_state_model     bin edges + feature selection + state signatures (TRAIN only)
  → states.state_stats         the probability engine: hit rate, MFE, MAE, Wilson bound
  → walkforward.run_walkforward expanding monthly folds + candidate→validated→production→retired

  baseline.py     per-symbol denominators: reach, retention, continuation, persistence
  attribution.py  L0→L1→L2 decomposition, per-symbol base rates, multi-family states
```

The design rule that everything else serves: **a feature on row `t` must be
computable from information available at the close of day `t`.** The state vector
is frozen at 15:31. At 09:15 you *load* it; you never recompute.

---

## Honest limitations — read these

**1. Daily bars cannot order intraday touches.** "Reached +1% before −0.5%" is not
resolvable from a daily bar: you know the high and the low, not which came first.
So there are three label modes. `conservative` requires the target hit *and* the
stop never breached (understates the truth). `optimistic` ignores the stop
(overstates it). The truth is in between — **if your edge only exists under
`optimistic`, you do not have an edge.** When you add 1-minute data, replace
`labels.add_labels` with a real first-touch resolver; nothing else changes.

**2. Hit-rate lift is not profit.** In the synthetic demo the engine recovers the
planted edge — mean out-of-sample lift about +2 percentage points, ~34 of 52
folds positive — and mean expectancy per trade is still slightly *negative*.
That is not a bug, it is the lesson: at a 2:1 target-to-stop ratio you need a far
bigger hit-rate lift than +2pp to pay for the asymmetry, before costs. The report
card prints expectancy next to lift for exactly this reason.

**3. Interactions are seeded on the first half of the sample.** A mild
compromise for speed. For a maximally strict run use `--no-interactions`.

**4. The synthetic demo numbers are synthetic.** They prove the plumbing works.
They say nothing about any real market.

**5. No costs, no slippage, no impact, no borrow.** Add them in the portfolio
layer before believing anything.

---

## The four questions

Every feature and every state should have to answer these, every fold:
recurrence (does it happen often enough?), relevance (does it move the
distribution?), stability (does it keep working on unseen months?), and
incremental value (does it add anything over what you already have?). The
lifecycle in `walkforward.py` is that discipline made mechanical: two consecutive
passes promote to *validated*, four to *production*, three consecutive failures
*retire* it. Overfitting stops being a worry and becomes a test that fails.
