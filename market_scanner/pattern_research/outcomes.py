"""Forward-outcome evidence: "what happened next" after every pattern occurrence.

This module answers, for every historical occurrence of a
(symbol, timeframe, pattern_id, variant, side, state) cell:

* the **net return path** over h = 1..H bars of holding, in the pattern's own
  timeframe (not forced into calendar days),
* the **maximum favourable / adverse excursion** (MFE / MAE) up to each h, taken
  from bar highs and lows, and
* the **holding period that historically worked**, chosen on training folds only
  and reported out-of-sample on the test folds.

Conventions are *imported* from :mod:`evaluation`, never re-invented:

===========================  ===========================================================
Convention                   Source
===========================  ===========================================================
Entry                        next bar's open after the signal bar (``entry = signal+1``)
Horizon ``h``                identical to ``evaluation``'s ``hold``: the position is held
                             ``h`` bars and exits at the **close of bar ``entry+h-1``**,
                             so ``h = 1`` means open-to-close of the entry bar.
Costs                        ``evaluation.COST_PCT`` = 0.40% of entry notional, charged
                             once, at the measured horizon. Fills are unslipped.
Signal / entry gap           an occurrence whose signal or entry bar carries the
                             data-quality ``gap`` flag is excluded (``evaluation`` code -3).
Gap during the hold          the position exits at the **open of the first gap bar** and
                             keeps its gain/loss (``evaluation`` code 6); excursions past
                             that point are unknown, so they are flagged.
Availability (point-in-time) horizon ``h`` counts only when ``entry + h <= len(bars)``;
                             the full grid is available from
                             ``available_from_idx = entry + H - 1``.
Folds                        rolling 36 calendar-month train / 6-month test, first test
                             36 months after the first bar (``evaluation._months``).
Purge                        candidate horizon + ``embargo`` bars must END strictly before
                             test start (``embargo`` = the largest candidate horizon, H).
Minimum training trades      ``evaluation.MIN_TRAIN`` (20), score = mean - 1 standard
                             error, required strictly positive.
Non-overlap                  an entry must be after the previously accepted exit; one
                             admission stream across all test folds.
===========================  ===========================================================

Excursions are **price** excursions, before transaction costs, exactly as in
``evaluation._excursions``. They are floored at zero.

On top of the fixed-horizon event study, three reporting layers make it stand on
its own without relying on the walk-forward selection:

1. **Unconditional baseline** (Lo/Mamaysky/Wang style).  For every cell the
   conditional distribution is compared against the *same stock, same timeframe,
   same side, same calendar window and same horizon* distribution obtained by
   entering at **every eligible bar** (a bar whose own and whose preceding bar
   carry no data-quality gap).  Point estimates use every eligible entry; the
   confidence interval for the difference uses a **stationary block bootstrap**
   (Politis-Romano, geometric block lengths, mean block = H bars) on both
   samples, resampled **independently**.  Independent resampling ignores the
   positive correlation between the conditional subset and the baseline that
   contains it, which *widens* the interval: the conservative direction.
2. **Year-by-year stability**: n, mean net and win rate per calendar year, plus a
   pre/post median-date split, at the cell's stability horizon, so decay is
   visible instead of averaged away.
3. **Multiple-testing control**: a per-cell two-sided p-value for the
   out-of-sample mean, taken as ``max`` of a Student-t test and a stationary
   bootstrap test (again the conservative choice), then Benjamini-Hochberg FDR
   across every testable cell of a run, stored with the number of trials.

**Overlap, stated once.**  Descriptive per-horizon aggregates deliberately
include *overlapping* occurrences - that is what "what happened next for every
occurrence" means - so their ``n`` is a count of occurrences, not of independent
observations, and no p-value is ever computed from them.  Every inferential
number (fold selection, out-of-sample statistics, p-values) is computed only on
the **non-overlapping** admission stream (an entry must follow the previous
accepted exit), with training pools purged by an H-bar embargo, and the baseline
comparison uses block bootstraps sized to the overlap horizon.

Nothing here touches a broker, the app database, or the 158 GB source database.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import sqlite3
from datetime import date
from pathlib import Path

import numpy as np
from numba import njit

from .evaluation import BASELINE, COST_PCT, MIN_TRAIN, _months, _prepare, _stats

OUTCOME_ENGINE_VERSION = '1.2.0'

#: Default maximum holding horizon (bars) per timeframe.  Contract v1 §4.
#: Confirm/adjust from the data with :func:`flatten_horizon` before freezing.
HORIZONS = {'1H': 30, '4H': 20, '1D': 10, '1W': 8}

#: Sample-size bands.  Anything below ``adequate`` must be displayed as labelled.
SAMPLE_BANDS = ((100, 'adequate'), (30, 'moderate'), (10, 'small'), (1, 'very_small'))
PERCENTILES = (10, 25, 50, 75, 90)
EXCLUSIONS = ('no_entry_bar', 'signal_or_entry_gap')

#: n at/above which an out-of-sample result stops being called a small sample.
OOS_MIN_SAMPLE = 20

#: Stationary-bootstrap settings for the conditional-vs-unconditional difference.
BOOTSTRAP_REPLICATES = 400
BOOTSTRAP_SEED = 20260915
BOOTSTRAP_MIN_SAMPLE = 20          # conditional occurrences needed before a CI is quoted
BASELINE_BOOTSTRAP_MAX = 1500      # systematic thinning cap on the baseline sequence
BASELINE_COVERAGE_MIN = 0.80       # below this the baseline window is flagged as mismatched
WINDOW_QUANTUM_DAYS = 30           # window-cache granularity so cells can share a baseline

#: Block length for the out-of-sample bootstrap p-value.  Those trades are already
#: non-overlapping, so the block only allows for residual clustering.
OOS_BOOTSTRAP_BLOCK = 4
OOS_MIN_TESTABLE = 5               # fewer trades than this gets no p-value at all
FDR_LEVELS = (0.10, 0.05)

#: A cell whose whole edge disappears when its best two calendar years are removed.
CONCENTRATION_YEARS = 2

#: Percentage-point floor below which a bootstrap difference is not a difference.
ZERO_TOLERANCE = 1e-9

#: First-touch barrier grid, in percent of entry and in multiples of the signal ATR.
#: Declared here, never tuned per cell.
BARRIER_GRID = (
    dict(id='pct:1.0:1.0', unit='pct', target=1.0, stop=1.0),
    dict(id='pct:2.0:1.0', unit='pct', target=2.0, stop=1.0),
    dict(id='pct:3.0:1.5', unit='pct', target=3.0, stop=1.5),
    dict(id='atr:1.0:1.0', unit='atr', target=1.0, stop=1.0),
    dict(id='atr:2.0:1.0', unit='atr', target=2.0, stop=1.0),
)
#: If a bar's range touches both barriers, the STOP is counted first.  Intrabar order
#: is unknowable from OHLC, so the pessimistic branch is the only honest one.
BARRIER_TIE_RULE = 'stop_first_when_both_touched_in_the_same_bar'
BARRIER_OUTCOMES = {0: 'neither', 1: 'target', 2: 'stop', 3: 'gap_exit',
                    -1: 'undetermined_insufficient_history', -2: 'invalid_atr_or_level'}
#: The pair whose label the "last N setups" table shows.
DISPLAY_BARRIER_ID = 'pct:2.0:1.0'
LAST_N_OCCURRENCES = 5

#: Horizons the UI shows.  The engine still computes every horizon 1..H; this is
#: presentation only, and values beyond H are dropped (and reported as dropped).
DISPLAY_GRID = {'1H': (1, 2, 4, 8, 12, 24), '4H': (1, 2, 3, 5, 8, 10),
                '1D': (1, 2, 3, 5, 10), '1W': (1, 2, 4, 8, 12)}

#: Condition buckets.  Pre-declared constants: no bucket boundary is ever chosen
#: after looking at returns.
VOLUME_LOOKBACK = 20
VOLUME_HIGH = 1.5                  # signal-bar volume / prior 20-bar median
VOLUME_LOW = 0.7
REGIME_LOOKBACK = 200
REGIME_BAND = 0.05                 # +/-5% over the lookback splits up / flat / down
QUALITY_TERTILES = 3
BUCKET_MIN_SAMPLE = 30             # below this a bucket reports 'insufficient'
#: Index membership is a market-cap PROXY and a CURRENT snapshot, not point-in-time.
CAP_TIERS = (('in_nifty100', 'large'), ('in_nifty200', 'mid'), ('in_nifty500', 'small'))


# --------------------------------------------------------------------- helpers
def horizon_grid(timeframe, override=None):
    """Maximum holding horizon in bars for ``timeframe`` (configurable)."""
    if override is not None:
        value = override.get(timeframe) if isinstance(override, dict) else override
        if value is not None:
            value = int(value)
            if value < 1:
                raise ValueError('Horizon grid must hold at least one bar')
            return value
    if timeframe not in HORIZONS:
        raise ValueError('Unsupported timeframe: ' + str(timeframe))
    return HORIZONS[timeframe]


def display_grid(timeframe, horizon=None):
    """Which horizons the UI should show, clipped to what the engine computed."""
    declared = list(DISPLAY_GRID.get(timeframe, ()))
    horizon = horizon if horizon is not None else horizon_grid(timeframe)
    shown = [h for h in declared if h <= horizon]
    return dict(declared=declared, shown=shown,
                dropped_beyond_horizon=[h for h in declared if h > horizon],
                max_horizon=horizon)


def sample_label(n):
    for threshold, label in SAMPLE_BANDS:
        if n >= threshold:
            return label
    return 'none'


def flatten_horizon(curve, fraction=0.90):
    """First horizon (1-based) at which ``curve`` reaches ``fraction`` of its end value.

    ``curve`` is a per-horizon median MFE list, ``None`` where the sample is empty.
    Returns ``None`` when the curve never rises (no terminal value to normalise by).
    """
    finite = [(i, v) for i, v in enumerate(curve, 1) if v is not None and math.isfinite(v)]
    if not finite:
        return None
    terminal = finite[-1][1]
    if terminal <= 0:
        return None
    for h, value in finite:
        if value >= fraction * terminal:
            return h
    return finite[-1][0]


def _round(value, digits=6):
    if value is None:
        return None
    value = float(value)
    return None if not math.isfinite(value) else round(value, digits)


def _nan_to_none(row, digits=3):
    """Percent values at 0.1 basis-point resolution; ``None`` where unavailable."""
    return [None if not math.isfinite(v) else round(float(v), digits) for v in row]


# ------------------------------------------------------------------ statistics
def _betacf(a, b, x, iterations=300, tiny=1e-30, epsilon=3e-16):
    """Continued fraction for the incomplete beta function (Lentz's method)."""
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c, d = 1.0, 1.0 - qab * x / qap
    d = tiny if abs(d) < tiny else d
    d = 1.0 / d
    h = d
    for m in range(1, iterations + 1):
        m2 = 2 * m
        numerator = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + numerator * d
        d = tiny if abs(d) < tiny else d
        c = 1.0 + numerator / c
        c = tiny if abs(c) < tiny else c
        d = 1.0 / d
        h *= d * c
        numerator = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + numerator * d
        d = tiny if abs(d) < tiny else d
        c = 1.0 + numerator / c
        c = tiny if abs(c) < tiny else c
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < epsilon:
            break
    return h


def _betainc(a, b, x):
    """Regularised incomplete beta I_x(a, b); the venv has no scipy."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    front = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
                     + a * math.log(x) + b * math.log1p(-x))
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _betacf(a, b, x) / a
    return 1.0 - front * _betacf(b, a, 1.0 - x) / b


def t_test_p_value(values):
    """Two-sided one-sample Student-t p-value for H0: mean = 0."""
    values = np.asarray(values, dtype=np.float64)
    n = values.size
    if n < 2:
        return None
    sd = float(values.std(ddof=1))
    mean = float(values.mean())
    if sd <= 0:
        return 0.0 if mean != 0 else 1.0
    t = abs(mean) / (sd / math.sqrt(n))
    df = n - 1
    return float(min(1.0, _betainc(df / 2.0, 0.5, df / (df + t * t))))


def _stationary_indices(n, replicates, mean_block, seed):
    """Politis-Romano stationary bootstrap index matrix, shaped ``(replicates, n)``.

    Blocks have geometric lengths with mean ``mean_block`` and wrap circularly, so
    the resample keeps the serial dependence that overlapping horizons create.
    """
    rng = np.random.default_rng(seed)
    p = 1.0 / max(float(mean_block), 1.0)
    positions = np.arange(n, dtype=np.int64)
    new_block = rng.random((replicates, n)) < p
    new_block[:, 0] = True
    starts = rng.integers(0, n, size=(replicates, n), dtype=np.int64)
    block_start = np.maximum.accumulate(np.where(new_block, positions, -1), axis=1)
    offset = positions[None, :] - block_start
    base = np.take_along_axis(starts, block_start, axis=1)
    return ((base + offset) % n).astype(np.int64)


def _bootstrap_means(matrix, indices):
    """Replicate means of every column of ``matrix`` under one index matrix."""
    replicates = indices.shape[0]
    out = np.full((replicates, matrix.shape[1]), np.nan)
    for column in range(matrix.shape[1]):
        out[:, column] = matrix[:, column][indices].mean(axis=1)
    return out


def bootstrap_p_value(values, block, replicates=BOOTSTRAP_REPLICATES, seed=BOOTSTRAP_SEED):
    """Two-sided stationary-bootstrap p-value for H0: mean = 0 (sample re-centred)."""
    values = np.asarray(values, dtype=np.float64)
    n = values.size
    if n < 2:
        return None
    observed = abs(float(values.mean()))
    centred = values - values.mean()
    indices = _stationary_indices(n, replicates, block, seed)
    replicate_means = np.abs(centred[indices].mean(axis=1))
    # Add-one correction: a bootstrap p-value is never reported as exactly zero.
    return float((np.count_nonzero(replicate_means >= observed) + 1) / (replicates + 1))


def bootstrap_difference_p_value(conditional, baseline, conditional_block, baseline_block,
                                 replicates=BOOTSTRAP_REPLICATES, seed=BOOTSTRAP_SEED,
                                 baseline_replicates=None):
    """Two-sided p-value for H0: mean(conditional) == mean(baseline).

    The right null for a condition bucket.  Testing a bucket's mean against ZERO
    would reward any long position in a stock that rose, which is exactly the
    artefact the baseline exists to remove.  Both samples are resampled with a
    stationary block bootstrap and resampled independently, which ignores their
    positive correlation and therefore widens the difference: conservative.
    """
    conditional = np.asarray(conditional, dtype=np.float64)
    baseline = np.asarray(baseline, dtype=np.float64)
    if conditional.size < 2 or baseline.size < 2:
        return None, None
    if baseline_replicates is None:
        baseline_replicates = _bootstrap_means(
            baseline[:, None], _stationary_indices(baseline.size, replicates,
                                                   baseline_block, seed))[:, 0]
    draws = _bootstrap_means(conditional[:, None],
                             _stationary_indices(conditional.size, replicates,
                                                 conditional_block, seed + 3))[:, 0]
    difference = draws - baseline_replicates
    # A difference below ZERO_TOLERANCE is float noise, not a difference: without this
    # two identical samples would land on the lower p clip instead of on p = 1.
    below = float(np.count_nonzero(difference <= ZERO_TOLERANCE))
    above = float(np.count_nonzero(difference >= -ZERO_TOLERANCE))
    p = 2.0 * min(below, above) / replicates
    return float(min(1.0, max(p, 1.0 / (replicates + 1)))), baseline_replicates


def benjamini_hochberg(p_values):
    """BH-adjusted q-values, preserving input order.  ``None`` entries stay ``None``."""
    indexed = [(i, p) for i, p in enumerate(p_values) if p is not None]
    q_values = [None] * len(p_values)
    m = len(indexed)
    if not m:
        return q_values
    indexed.sort(key=lambda pair: pair[1])
    running = 1.0
    for rank in range(m, 0, -1):
        i, p = indexed[rank - 1]
        running = min(running, p * m / rank)
        q_values[i] = min(1.0, running)
    return q_values


# ----------------------------------------------------------------- numeric core
def _forward_paths(prices, gaps, entries, horizon, sign):
    """Per-occurrence forward path arrays, shaped ``(len(entries), horizon)``.

    Returns ``(net, mfe, mae, exit_idx, gap_h, avail)``.  ``net``/``mfe``/``mae``
    are NaN where the horizon does not exist in history yet (point-in-time law).
    """
    n = len(prices)
    count = len(entries)
    shape = (count, horizon)
    if not count:
        empty = np.zeros(shape)
        return empty, empty.copy(), empty.copy(), np.zeros(shape, np.int64), \
            np.zeros(count, np.int64), np.zeros(shape, bool)
    pad = np.full(horizon, np.nan)
    o = np.concatenate([prices[:, 0], pad])
    hi = np.concatenate([prices[:, 1], pad])
    lo = np.concatenate([prices[:, 2], pad])
    cl = np.concatenate([prices[:, 3], pad])
    gp = np.concatenate([gaps, np.zeros(horizon, bool)])

    def window(a):
        return np.lib.stride_tricks.sliding_window_view(a, horizon)[entries]

    w_open, w_high, w_low, w_close, w_gap = window(o), window(hi), window(lo), window(cl), window(gp)
    entry_price = prices[entries, 0][:, None]
    offsets = np.arange(horizon, dtype=np.int64)[None, :]
    avail = (entries[:, None] + offsets + 1) <= n

    with np.errstate(invalid='ignore'):
        move_high = sign * (w_high - entry_price) / entry_price * 100.0
        move_low = sign * (w_low - entry_price) / entry_price * 100.0
        net = sign * (w_close - entry_price) / entry_price * 100.0 - COST_PCT
    mfe = np.fmax(np.fmax.accumulate(np.fmax(move_high, move_low), axis=1), 0.0)
    mae = np.fmax(-np.fmin.accumulate(np.fmin(move_high, move_low), axis=1), 0.0)
    exit_off = np.broadcast_to(offsets, shape).copy()

    # A data-quality gap inside the hold exits at that bar's open (evaluation code 6).
    after = w_gap.copy()
    after[:, 0] = False                      # the entry bar's own flag is handled upstream
    has_gap = after.any(axis=1)
    first = np.argmax(after, axis=1)
    gap_h = np.where(has_gap, first + 1, 0).astype(np.int64)
    for r in np.flatnonzero(has_gap):
        k = int(first[r])
        price = float(entry_price[r, 0])
        move = sign * (float(w_open[r, k]) - price) / price * 100.0
        net[r, k:] = move - COST_PCT
        mfe[r, k:] = max(float(mfe[r, k - 1]), move, 0.0)
        mae[r, k:] = max(float(mae[r, k - 1]), -move, 0.0)
        exit_off[r, k:] = k

    blank = ~avail
    net[blank] = np.nan
    mfe[blank] = np.nan
    mae[blank] = np.nan
    return net, mfe, mae, entries[:, None] + exit_off, gap_h, avail


@njit(cache=True)
def _train_select(entries, net, exit_idx, avail, starts, ends, embargo,
                  train_starts, test_starts, minimum):
    """Pick the best horizon per fold using TRAINING data only.

    Mirrors ``evaluation._choose``: purged, non-overlapping, at least ``minimum``
    training trades, score = mean net return minus one standard error, strictly
    positive, lowest horizon wins ties.
    """
    folds = len(test_starts)
    count, horizon = net.shape
    n = len(starts)
    chosen = np.full(folds, -1, np.int64)
    train_n = np.zeros(folds, np.int64)
    train_mean = np.zeros(folds, np.float64)
    train_score = np.zeros(folds, np.float64)
    for f in range(folds):
        best = -1
        best_score = 0.0
        best_n = 0
        best_mean = 0.0
        for h in range(horizon):
            last = -1
            cnt = 0
            mean = 0.0
            m2 = 0.0
            for e in range(count):
                entry = entries[e]
                begin = starts[entry]
                if begin < train_starts[f]:
                    continue
                if begin >= test_starts[f]:
                    break
                purge_end = entry + h + embargo
                if purge_end >= n or ends[purge_end] >= test_starts[f]:
                    continue
                if not avail[e, h]:
                    continue
                if entry <= last:
                    continue
                last = exit_idx[e, h]
                value = net[e, h]
                cnt += 1
                delta = value - mean
                mean += delta / cnt
                m2 += delta * (value - mean)
            if cnt >= minimum:
                score = mean - math.sqrt(max(0.0, m2) / (cnt - 1) / cnt)
                if score > best_score:
                    best = h
                    best_score = score
                    best_n = cnt
                    best_mean = mean
        chosen[f] = best
        train_n[f] = best_n
        train_mean[f] = best_mean
        train_score[f] = best_score
    return chosen, train_n, train_mean, train_score


@njit(cache=True)
def _first_touch(prices, gaps, entries, atr, horizon, sign, target_mult, stop_mult, is_atr):
    """First-touch outcome per occurrence under one target/stop pair.

    Mirrors ``evaluation._matrix`` exactly: an opening price already beyond a
    barrier fills at that open, the STOP is tested before the target both at the
    open and inside the bar, and a data-quality gap inside the hold ends the
    observation at that bar.  Codes are ``BARRIER_OUTCOMES``.
    """
    n = len(prices)
    count = len(entries)
    outcome = np.zeros(count, np.int8)
    bars = np.zeros(count, np.int64)
    ambiguous = np.zeros(count, np.bool_)
    for e in range(count):
        entry = entries[e]
        price = prices[entry, 0]
        if is_atr:
            width = atr[e]
            if not np.isfinite(width) or width <= 0:
                outcome[e] = -2
                continue
            target = price + sign * target_mult * width
            stop = price - sign * stop_mult * width
        else:
            target = price * (1.0 + sign * target_mult / 100.0)
            stop = price * (1.0 - sign * stop_mult / 100.0)
        if min(target, stop) <= 0.0:
            outcome[e] = -2
            continue
        last = min(entry + horizon, n)
        resolved = False
        for j in range(entry, last):
            if j > entry and gaps[j]:
                outcome[e] = 3
                bars[e] = j - entry
                resolved = True
                break
            open_price = prices[j, 0]
            if sign * (open_price - stop) <= 0.0:
                outcome[e] = 2
                bars[e] = j - entry + 1
                resolved = True
                break
            if sign * (open_price - target) >= 0.0:
                outcome[e] = 1
                bars[e] = j - entry + 1
                resolved = True
                break
            high = prices[j, 1]
            low = prices[j, 2]
            hit_stop = low <= stop if sign == 1 else high >= stop
            hit_target = high >= target if sign == 1 else low <= target
            if hit_stop:
                outcome[e] = 2
                bars[e] = j - entry + 1
                ambiguous[e] = hit_target
                resolved = True
                break
            if hit_target:
                outcome[e] = 1
                bars[e] = j - entry + 1
                resolved = True
                break
        if not resolved:
            if entry + horizon <= n:
                outcome[e] = 0
                bars[e] = horizon
            else:
                outcome[e] = -1
    return outcome, bars, ambiguous


def _quartiles(values):
    if not len(values):
        return dict(n=0, p25=None, median=None, p75=None)
    values = np.asarray(values, dtype=np.float64)
    return dict(n=int(values.size), p25=_round(np.percentile(values, 25), 2),
                median=_round(np.median(values), 2), p75=_round(np.percentile(values, 75), 2))


def barrier_table(prices, gaps, starts, ends, entries, atr, horizon, sign, grid=BARRIER_GRID):
    """P(target first) / P(stop first) / P(neither) plus time-to-touch, per pair."""
    n = len(prices)
    rows = []
    touches = {}
    for pair in grid:
        outcome, bars, ambiguous = _first_touch(
            np.ascontiguousarray(prices), np.ascontiguousarray(gaps),
            np.ascontiguousarray(entries), np.ascontiguousarray(atr), int(horizon), int(sign),
            float(pair['target']), float(pair['stop']), pair['unit'] == 'atr')
        touches[pair['id']] = (outcome, bars)
        decided = np.isin(outcome, (0, 1, 2, 3))
        total = int(decided.sum())
        row = dict(pair, tie_rule=BARRIER_TIE_RULE, n=total,
                   undetermined_n=int((outcome == -1).sum()),
                   invalid_n=int((outcome == -2).sum()),
                   both_touched_same_bar_n=int(ambiguous.sum()),
                   sample_label=sample_label(total))
        if total:
            for code, name in ((1, 'target'), (2, 'stop'), (0, 'neither'), (3, 'gap_exit')):
                hits = int((outcome == code).sum())
                row['n_' + name] = hits
                row['p_' + name] = _round(hits / total * 100, 3)
            for code, name in ((1, 'target'), (2, 'stop')):
                picked = outcome == code
                row['bars_to_' + name] = _quartiles(bars[picked])
                if picked.any():
                    landed = np.clip(entries[picked] + bars[picked] - 1, 0, n - 1)
                    span = ends[landed] - starts[entries[picked]]
                    row['calendar_days_to_' + name] = _round(np.median(span), 2)
                else:
                    row['calendar_days_to_' + name] = None
        else:
            for name in ('target', 'stop', 'neither', 'gap_exit'):
                row['n_' + name] = 0
                row['p_' + name] = None
            for name in ('target', 'stop'):
                row['bars_to_' + name] = _quartiles([])
                row['calendar_days_to_' + name] = None
        rows.append(row)
    return rows, touches


def return_peak(horizons):
    """The horizon where the median (and mean) forward return stops improving."""
    def peak(field):
        best, best_h = None, None
        for row in horizons:
            value = row.get(field)
            if row['n'] and value is not None and (best is None or value > best):
                best, best_h = value, row['h']
        return best_h, _round(best)
    median_h, median_value = peak('median_net_return_pct')
    mean_h, mean_value = peak('mean_net_return_pct')
    return dict(median_peak_h=median_h, median_peak_net_return_pct=median_value,
                mean_peak_h=mean_h, mean_peak_net_return_pct=mean_value)


def _fold_windows(starts, ends):
    """Rolling 36-month train / 6-month test windows, identical to ``evaluate_cell``."""
    if not len(starts):
        return []
    cursor = _months(date.fromordinal(int(starts[0])), 36)
    final_day = date.fromordinal(int(ends[-1]))
    windows = []
    while cursor <= final_day:
        stop = _months(cursor, 6)
        windows.append(dict(index=len(windows), train_start=_months(cursor, -36),
                            test_start=cursor, test_stop=stop,
                            test_end=min(date.fromordinal(stop.toordinal() - 1), final_day)))
        cursor = stop
    return windows


def _scan(entries, net, exit_idx, avail, starts, ends, h, lo, hi, last=-1,
          purge_before=None, embargo=0):
    """Admit non-overlapping occurrences at horizon ``h`` whose entry starts in [lo, hi)."""
    n = len(starts)
    picked, indexes = [], []
    for e, entry in enumerate(entries):
        begin = starts[entry]
        if begin < lo:
            continue
        if begin >= hi:
            break
        if purge_before is not None:
            purge_end = entry + h + embargo
            if purge_end >= n or ends[purge_end] >= purge_before:
                continue
        if not avail[e, h]:
            continue
        if entry <= last:
            continue
        last = int(exit_idx[e, h])
        picked.append(float(net[e, h]))
        indexes.append(e)
    return picked, indexes, last


def select_holding_period(entries, net, exit_idx, avail, starts, ends, embargo,
                          minimum=MIN_TRAIN):
    """Fold-based horizon selection: in-sample pick, out-of-sample report.

    Never returns the in-sample best as the expected return: ``in_sample`` and
    ``out_of_sample`` are separate blocks and the caller must show both.
    """
    horizon = net.shape[1]
    windows = _fold_windows(starts, ends)
    if not windows or not len(entries):
        return dict(status='insufficient_history' if not windows else 'insufficient',
                    horizon=None, horizon_grid_bars=horizon, folds=[],
                    in_sample=dict(horizon=None, stats=_stats([]), per_fold=[],
                                   scope='training_folds_only_never_an_expected_return'),
                    out_of_sample=dict(horizon_by_fold=[], stats=_stats([]),
                                       sample_label=sample_label(0),
                                       scope='test_folds_at_the_training_selected_horizon'))
    chosen, train_n, train_mean, train_score = _train_select(
        np.ascontiguousarray(entries), np.ascontiguousarray(net),
        np.ascontiguousarray(exit_idx), np.ascontiguousarray(avail),
        np.ascontiguousarray(starts), np.ascontiguousarray(ends),
        int(embargo), np.array([w['train_start'].toordinal() for w in windows], np.int64),
        np.array([w['test_start'].toordinal() for w in windows], np.int64), int(minimum))

    folds, forward, forward_idx, trained, horizon_by_fold, last = [], [], [], [], [], -1
    for w, h in zip(windows, chosen):
        record = dict(index=w['index'], train_start=w['train_start'].isoformat(),
                      test_start=w['test_start'].isoformat(), test_end=w['test_end'].isoformat(),
                      horizon=None, training_stats=_stats([]), test_stats=_stats([]),
                      status='insufficient_positive_training_evidence')
        h = int(h)
        if h >= 0:
            training, _, _ = _scan(entries, net, exit_idx, avail, starts, ends, h,
                                   w['train_start'].toordinal(), w['test_start'].toordinal(),
                                   purge_before=w['test_start'].toordinal(), embargo=embargo)
            tested, picked_idx, last = _scan(entries, net, exit_idx, avail, starts, ends, h,
                                             w['test_start'].toordinal(),
                                             w['test_stop'].toordinal(), last)
            record.update(horizon=h + 1, status='frozen_for_test',
                          training_stats=_stats(training), test_stats=_stats(tested))
            forward.extend(tested)
            forward_idx.extend(picked_idx)
            trained.extend(training)
            horizon_by_fold.append(h + 1)
        folds.append(record)

    picked = [f['horizon'] for f in folds if f['horizon']]
    stats = _stats(forward)
    if not picked or not stats['n']:
        status = 'insufficient'
    elif stats['n'] < OOS_MIN_SAMPLE:
        status = 'small_out_of_sample_sample'
    else:
        status = 'tested'
    modal = max(set(picked), key=lambda v: (picked.count(v), -v)) if picked else None
    return dict(status=status, horizon=modal if status != 'insufficient' else None,
                horizon_grid_bars=horizon, folds=folds,
                in_sample=dict(horizon=modal, stats=_stats(trained),
                               per_fold=[dict(fold=f['index'], horizon=f['horizon'],
                                              n=f['training_stats']['n'],
                                              expectancy_pct=f['training_stats']['expectancy_pct'])
                                         for f in folds],
                               scope='training_folds_only_never_an_expected_return'),
                out_of_sample=dict(horizon_by_fold=horizon_by_fold, stats=stats,
                                   sample_label=sample_label(stats['n']),
                                   returns=[round(v, 6) for v in forward],
                                   entry_indices=[int(entries[i]) for i in forward_idx],
                                   overlap='non_overlapping_single_admission_stream',
                                   scope='test_folds_at_the_training_selected_horizon'))


# ------------------------------------------------------- unconditional baseline
def eligible_entries(gaps):
    """Every bar that could have been entered: its own and its signal bar are clean.

    This is the ``evaluation`` entry gate (code -3) applied to the whole series, so
    the unconditional sample is drawn under exactly the conditional sample's rules.
    """
    n = len(gaps)
    if n < 2:
        return np.zeros(0, dtype=np.int64)
    ok = ~gaps[1:] & ~gaps[:-1]
    return (np.flatnonzero(ok) + 1).astype(np.int64)


def _baseline_paths(prices, gaps, horizon, sign, side, cache):
    key = ('baseline_paths', side, horizon)
    if cache is not None and key in cache:
        return cache[key]
    entries = eligible_entries(gaps)
    net, mfe, mae, exit_idx, gap_h, avail = _forward_paths(prices, gaps, entries, horizon, sign)
    value = dict(entries=entries, net=net, mfe=mfe, mae=mae, avail=avail)
    if cache is not None:
        cache[key] = value
    return value


def _thin(count, cap):
    """Deterministic, evenly spaced subsample - a matched sample, never a lucky draw."""
    if count <= cap:
        return np.arange(count, dtype=np.int64), 1.0
    picked = np.unique(np.linspace(0, count - 1, cap).astype(np.int64))
    return picked, count / len(picked)


def unconditional_baseline(prices, gaps, starts, entries, net, avail, horizon, sign, side,
                           cache=None, replicates=BOOTSTRAP_REPLICATES, seed=BOOTSTRAP_SEED):
    """Conditional minus unconditional, same stock / window / horizon / cost model.

    Point estimates use every eligible entry inside the cell's occurrence window.
    The difference CI is a stationary block bootstrap (mean block = ``horizon``) of
    the two samples resampled independently, restricted to entries whose full
    horizon grid exists.  Returns ``None``-valued rows where the sample is empty.
    """
    blank = dict(status='no_occurrences', horizons=[], occurrence_window=None,
                 baseline_window=None, window_coverage=None, window_mismatch=None,
                 replicates=0, baseline_thinning=None, method='none', block_bars=horizon,
                 window_quantum_days=WINDOW_QUANTUM_DAYS)
    if not len(entries):
        return blank
    base = _baseline_paths(prices, gaps, horizon, sign, side, cache)
    if not len(base['entries']):
        return dict(blank, status='no_eligible_bars')
    low, high = int(starts[entries].min()), int(starts[entries].max())
    # Quantise the window so neighbouring cells share one baseline slice and one
    # bootstrap; the exact occurrence span is reported alongside it.
    quantum = WINDOW_QUANTUM_DAYS
    q_low, q_high = low // quantum * quantum, (high // quantum + 1) * quantum - 1
    window_key = ('baseline_window', side, horizon, q_low, q_high)
    if cache is not None and window_key in cache:
        window_net, window_avail, window_entries = cache[window_key]
    else:
        inside = (starts[base['entries']] >= q_low) & (starts[base['entries']] <= q_high)
        window_net, window_avail = base['net'][inside], base['avail'][inside]
        window_entries = base['entries'][inside]
        if cache is not None:
            cache[window_key] = (window_net, window_avail, window_entries)
    span = max(int(starts[-1]) - int(starts[0]) + 1, 1)
    coverage = (high - low + 1) / span

    rows = []
    for h in range(horizon):
        conditional = net[:, h][np.isfinite(net[:, h])]
        unconditional = window_net[:, h][np.isfinite(window_net[:, h])]
        row = dict(h=h + 1, baseline_n=int(unconditional.size), conditional_n=int(conditional.size))
        if unconditional.size and conditional.size:
            row.update(baseline_mean_net_return_pct=_round(unconditional.mean()),
                       baseline_median_net_return_pct=_round(np.median(unconditional)),
                       baseline_win_rate_pct=_round(np.count_nonzero(unconditional > 0)
                                                    / unconditional.size * 100),
                       diff_mean_net_return_pct=_round(conditional.mean() - unconditional.mean()),
                       diff_median_net_return_pct=_round(np.median(conditional)
                                                         - np.median(unconditional)))
        else:
            row.update(baseline_mean_net_return_pct=None, baseline_median_net_return_pct=None,
                       baseline_win_rate_pct=None, diff_mean_net_return_pct=None,
                       diff_median_net_return_pct=None)
        row.update(diff_ci95_low=None, diff_ci95_high=None, diff_excludes_zero=None)
        rows.append(row)

    result = dict(status='point_estimates_only', horizons=rows,
                  window_net=window_net, window_entries=window_entries,
                  window_bounds=(q_low, q_high),
                  occurrence_window=[date.fromordinal(low).isoformat(),
                                     date.fromordinal(high).isoformat()],
                  baseline_window=[date.fromordinal(q_low).isoformat(),
                                   date.fromordinal(q_high).isoformat()],
                  window_coverage=_round(coverage, 4),
                  window_mismatch=bool(coverage < BASELINE_COVERAGE_MIN),
                  window_quantum_days=quantum,
                  replicates=0, baseline_thinning=None, block_bars=horizon,
                  method='eligible_bar_entries_same_window_same_cost')

    # The CI only means something with a real conditional sample and a full grid.
    full = avail[:, -1] if avail.size else np.zeros(0, bool)
    conditional_full = net[full][:, :] if full.any() else np.zeros((0, horizon))
    if conditional_full.shape[0] < BOOTSTRAP_MIN_SAMPLE:
        return dict(result, status='insufficient_sample_for_interval')
    window_full = window_net[window_avail[:, -1]] if window_avail.size else np.zeros((0, horizon))
    if window_full.shape[0] < BOOTSTRAP_MIN_SAMPLE:
        return dict(result, status='insufficient_baseline_for_interval')

    picked, thinning = _thin(window_full.shape[0], BASELINE_BOOTSTRAP_MAX)
    key = ('baseline_boot', side, horizon, q_low, q_high, replicates)
    if cache is not None and key in cache:
        base_boot = cache[key]
    else:
        base_boot = _bootstrap_means(window_full[picked],
                                     _stationary_indices(len(picked), replicates, horizon, seed))
        if cache is not None:
            cache[key] = base_boot
    cond_boot = _bootstrap_means(
        conditional_full, _stationary_indices(conditional_full.shape[0], replicates, horizon,
                                              seed + 1))
    difference = cond_boot - base_boot
    low_ci = np.percentile(difference, 2.5, axis=0)
    high_ci = np.percentile(difference, 97.5, axis=0)
    for h, row in enumerate(rows):
        low_value, high_value = float(low_ci[h]), float(high_ci[h])
        row.update(diff_ci95_low=_round(low_value), diff_ci95_high=_round(high_value),
                   diff_excludes_zero=bool(low_value > ZERO_TOLERANCE
                                           or high_value < -ZERO_TOLERANCE))
    return dict(result, status='tested', replicates=replicates,
                baseline_thinning=_round(thinning, 3),
                bootstrap_n=dict(conditional=int(conditional_full.shape[0]),
                                 baseline=int(len(picked))),
                method='eligible_bar_entries_same_window_same_cost_stationary_block_bootstrap')


# ------------------------------------------------------------- year-by-year decay
def _year_table(returns, years):
    table = {}
    for value, year in zip(returns, years):
        bucket = table.setdefault(year, [])
        bucket.append(value)
    rows = []
    for year in sorted(table):
        values = np.asarray(table[year], dtype=np.float64)
        rows.append(dict(year=int(year), n=int(values.size),
                         mean_net_return_pct=_round(values.mean()),
                         median_net_return_pct=_round(np.median(values)),
                         win_rate_pct=_round(np.count_nonzero(values > 0) / values.size * 100),
                         sum_net_return_pct=_round(values.sum())))
    return rows


def _split_stats(returns, ordinals):
    if not returns:
        return None
    pivot = float(np.median(ordinals))
    early = [v for v, o in zip(returns, ordinals) if o <= pivot]
    late = [v for v, o in zip(returns, ordinals) if o > pivot]
    return dict(pivot_date=date.fromordinal(int(pivot)).isoformat(),
                first_half=_stats(early), second_half=_stats(late))


def stability(entries, net, exit_idx, avail, starts, ends, horizon_index, oos_returns,
              oos_entries):
    """Calendar-year decay at one horizon, on the non-overlapping admission stream."""
    picked, indexes, _ = _scan(entries, net, exit_idx, avail, starts, ends, horizon_index,
                               0, date.max.toordinal())
    ordinals = [int(starts[entries[i]]) for i in indexes]
    years = [date.fromordinal(o).year for o in ordinals]
    rows = _year_table(picked, years)
    oos_ordinals = [int(starts[e]) for e in oos_entries]
    oos_rows = _year_table(oos_returns, [date.fromordinal(o).year for o in oos_ordinals])
    positive = sum(1 for r in rows if r['mean_net_return_pct'] and r['mean_net_return_pct'] > 0)
    concentrated = None
    if picked:
        total = float(np.sum(picked))
        best = sorted((r['sum_net_return_pct'] or 0.0) for r in rows)[-CONCENTRATION_YEARS:]
        concentrated = bool(total > 0 and total - sum(best) <= 0)
    return dict(horizon=horizon_index + 1, all_history=dict(
                    stats=_stats(picked), by_year=rows,
                    split=_split_stats(picked, ordinals),
                    overlap='non_overlapping_single_admission_stream'),
                out_of_sample=dict(stats=_stats(oos_returns), by_year=oos_rows,
                                   split=_split_stats(oos_returns, oos_ordinals)),
                years=len(rows), positive_years=positive,
                positive_year_fraction=_round(positive / len(rows), 4) if rows else None,
                edge_concentrated_in_best_years=concentrated,
                concentration_years=CONCENTRATION_YEARS)


# ----------------------------------------------------------- condition buckets
def _rolling_clean(gaps, window):
    """True at i when bars i-window+1..i contain no data-quality gap."""
    n = len(gaps)
    out = np.zeros(n, dtype=bool)
    if n < window:
        return out
    flagged = np.lib.stride_tricks.sliding_window_view(gaps[1:], window - 1).any(axis=1) \
        if window > 1 else np.zeros(n - 1, dtype=bool)
    out[window - 1:] = ~flagged[:n - window + 1]
    return out


def bar_buckets(bars, prices, gaps, index_bars=None):
    """Point-in-time bucket label for every bar, from information known at its close.

    ``volume``  signal-bar volume against the median of the previous
                ``VOLUME_LOOKBACK`` bars: ``high`` >= 1.5x, ``low`` <= 0.7x, else
                ``normal``.
    ``regime``  return over ``REGIME_LOOKBACK`` bars of the index series when one is
                supplied, otherwise of the stock itself: ``up`` above +5%, ``down``
                below -5%, else ``flat``.
    Windows that span a data-quality gap, or that do not exist yet, are ``None``.
    """
    n = len(bars)
    volume = np.array([float(b.get('volume') or 0.0) for b in bars]) if n else np.zeros(0)
    labels = {}

    volume_label = np.full(n, None, dtype=object)
    if n > VOLUME_LOOKBACK:
        windows = np.lib.stride_tricks.sliding_window_view(volume, VOLUME_LOOKBACK)[:-1]
        median = np.median(windows, axis=1)                       # bars i-20..i-1
        index = np.arange(VOLUME_LOOKBACK, n)
        clean = _rolling_clean(gaps, VOLUME_LOOKBACK + 1)[index]
        with np.errstate(divide='ignore', invalid='ignore'):
            ratio = np.where(median > 0, volume[index] / median, np.nan)
        for position, value, ok in zip(index, ratio, clean):
            if not ok or not math.isfinite(value):
                continue
            volume_label[position] = ('high' if value >= VOLUME_HIGH
                                      else 'low' if value <= VOLUME_LOW else 'normal')
    labels['volume'] = volume_label

    source = prices[:, 3]
    aligned = True
    if index_bars:
        lookup = {b['time']: float(b['close']) for b in index_bars}
        source = np.array([lookup.get(b['time'], np.nan) for b in bars])
        aligned = bool(np.isfinite(source).sum() > n * 0.9)
    regime_label = np.full(n, None, dtype=object)
    if n > REGIME_LOOKBACK and aligned:
        index = np.arange(REGIME_LOOKBACK, n)
        clean = _rolling_clean(gaps, REGIME_LOOKBACK + 1)[index]
        with np.errstate(divide='ignore', invalid='ignore'):
            change = source[index] / source[index - REGIME_LOOKBACK] - 1.0
        for position, value, ok in zip(index, change, clean):
            if not ok or not math.isfinite(value):
                continue
            regime_label[position] = ('up' if value > REGIME_BAND
                                      else 'down' if value < -REGIME_BAND else 'flat')
    labels['regime'] = regime_label
    labels['_regime_source'] = 'index_series' if (index_bars and aligned) else \
        'stock_own_%d_bar_trend' % REGIME_LOOKBACK
    return labels


def _tertile_labels(scores):
    """In-sample score tertiles.  A stratification for reading, not a tradable rule."""
    scores = np.asarray(scores, dtype=np.float64)
    if scores.size < QUALITY_TERTILES or not np.isfinite(scores).all():
        return np.full(scores.size, None, dtype=object), None
    cuts = [float(np.percentile(scores, 100.0 * k / QUALITY_TERTILES))
            for k in range(1, QUALITY_TERTILES)]
    if len(set(cuts)) != len(cuts):
        return np.full(scores.size, None, dtype=object), None
    labels = np.full(scores.size, 'q1_low', dtype=object)
    labels[scores > cuts[0]] = 'q2_mid'
    labels[scores > cuts[1]] = 'q3_high'
    return labels, [_round(c) for c in cuts]


def _bucket_row(dimension, bucket, returns, mfe, mae, baseline, baseline_scope,
                replicates, seed, minimum=BUCKET_MIN_SAMPLE, block=1, cache_key=None,
                cache=None):
    row = dict(dimension=dimension, bucket=bucket, n=len(returns),
               sample_label=sample_label(len(returns)),
               baseline_scope=baseline_scope, baseline_n=len(baseline),
               overlap='non_overlapping_single_admission_stream',
               scope='all_history_non_overlapping_not_out_of_sample',
               null_hypothesis='bucket mean == mean of unconditional entries in the same '
                               'bucket (NOT zero: a long position in a stock that rose beats '
                               'zero without any edge)',
               p_value_method='stationary_bootstrap_of_the_difference',
               status='insufficient', p_value=None, p_value_vs_zero=None,
               q_value=None, fdr_trials=None, discovery_q10=None, discovery_q05=None,
               fdr='assigned by the run-level Benjamini-Hochberg post-pass over the bucket '
                   'family; read bucket_outcomes.q_value')
    if returns:
        values = np.asarray(returns, dtype=np.float64)
        row.update(mean_net_return_pct=_round(values.mean()),
                   median_net_return_pct=_round(np.median(values)),
                   win_rate_pct=_round(np.count_nonzero(values > 0) / values.size * 100),
                   median_mfe_pct=_round(np.median(mfe)) if len(mfe) else None,
                   median_mae_pct=_round(np.median(mae)) if len(mae) else None)
    else:
        row.update(mean_net_return_pct=None, median_net_return_pct=None, win_rate_pct=None,
                   median_mfe_pct=None, median_mae_pct=None)
    if len(baseline):
        base = np.asarray(baseline, dtype=np.float64)
        row['baseline_mean_net_return_pct'] = _round(base.mean())
        row['baseline_median_net_return_pct'] = _round(np.median(base))
        if returns:
            row['diff_mean_net_return_pct'] = _round(values.mean() - base.mean())
            row['diff_median_net_return_pct'] = _round(np.median(values) - np.median(base))
    else:
        row['baseline_mean_net_return_pct'] = None
        row['baseline_median_net_return_pct'] = None
        row['diff_mean_net_return_pct'] = None
        row['diff_median_net_return_pct'] = None
    if len(returns) >= minimum and len(baseline) >= minimum:
        thinned, _ = _thin(len(baseline), BASELINE_BOOTSTRAP_MAX)
        sample = np.asarray(baseline, dtype=np.float64)[thinned]
        cached = cache.get(cache_key) if (cache is not None and cache_key) else None
        p, replicate_means = bootstrap_difference_p_value(
            returns, sample, OOS_BOOTSTRAP_BLOCK, block, replicates, seed,
            baseline_replicates=cached)
        if cache is not None and cache_key and replicate_means is not None:
            cache[cache_key] = replicate_means
        row.update(status='tested', p_value=_round(p),
                   p_value_vs_zero=_round(t_test_p_value(returns)),
                   baseline_bootstrap_n=int(len(thinned)))
    elif len(returns) >= minimum:
        row['status'] = 'insufficient_baseline'
    return row


def condition_buckets(bars, prices, gaps, starts, ends, entries, events, net, mfe, mae,
                      exit_idx, avail, horizon_index, window_net, window_entries,
                      window_bounds=None, cache=None, index_bars=None,
                      replicates=BOOTSTRAP_REPLICATES, seed=BOOTSTRAP_SEED, side='long'):
    """Pre-declared condition breakdowns, each against a baseline in the same bucket.

    Buckets are read at the SIGNAL bar (``entry - 1``) for occurrences and at the
    same offset for unconditional entries, so the two are measured identically.
    """
    key = ('bar_buckets',)
    if cache is not None and key in cache:
        assigned = cache[key]
    else:
        assigned = bar_buckets(bars, prices, gaps, index_bars)
        if cache is not None:
            cache[key] = assigned
    picked, indexes, _ = _scan(entries, net, exit_idx, avail, starts, ends, horizon_index,
                               0, date.max.toordinal())
    signals = np.array([entries[i] - 1 for i in indexes], dtype=np.int64)
    base_signals = window_entries - 1
    base_column = window_net[:, horizon_index]
    base_ok = np.isfinite(base_column)

    rows = []
    for dimension in ('volume', 'regime'):
        cell_labels = assigned[dimension]
        conditional = [cell_labels[i] for i in signals] if len(signals) else []
        baseline = cell_labels[base_signals] if len(base_signals) else np.zeros(0, dtype=object)
        for bucket in sorted({b for b in conditional if b} | {b for b in baseline if b}):
            keep = [k for k, label in enumerate(conditional) if label == bucket]
            mask = np.array([label == bucket for label in baseline], dtype=bool) & base_ok
            rows.append(_bucket_row(
                dimension, bucket, [picked[k] for k in keep],
                [mfe[indexes[k], horizon_index] for k in keep],
                [mae[indexes[k], horizon_index] for k in keep],
                base_column[mask], 'unconditional_entries_in_the_same_bucket',
                replicates, seed, block=horizon_index + 1,
                cache_key=('bucket_base_boot', side, horizon_index, window_bounds,
                           dimension, bucket, replicates), cache=cache))

    scores = np.array([float(events[i].get('score', 0.0)) for i in indexes]) if indexes else         np.zeros(0)
    tertiles, cuts = _tertile_labels(scores)
    for bucket in sorted({b for b in tertiles if b}):
        keep = [k for k, label in enumerate(tertiles) if label == bucket]
        row = _bucket_row('quality_tertile', bucket, [picked[k] for k in keep],
                          [mfe[indexes[k], horizon_index] for k in keep],
                          [mae[indexes[k], horizon_index] for k in keep],
                          base_column[base_ok],
                          'cell_unconditional_entries_no_score_exists_for_a_non_pattern_bar',
                          replicates, seed, block=horizon_index + 1,
                          cache_key=('bucket_base_boot', side, horizon_index, window_bounds,
                                     'cell', 'all', replicates), cache=cache)
        row['tertile_cuts'] = cuts
        row['stratification'] = 'in_sample_score_tertiles_not_a_point_in_time_rule'
        rows.append(row)
    return dict(horizon=horizon_index + 1, minimum_sample=BUCKET_MIN_SAMPLE,
                regime_source=assigned['_regime_source'], rows=rows,
                cap_tier_and_sector='cell-level columns; bucket across symbols at query time',
                definitions=dict(volume='signal-bar volume / median of the prior %d bars; '
                                        'high >= %.2fx, low <= %.2fx' % (VOLUME_LOOKBACK,
                                                                         VOLUME_HIGH, VOLUME_LOW),
                                 regime='%d-bar return; up > +%.0f%%, down < -%.0f%%'
                                        % (REGIME_LOOKBACK, REGIME_BAND * 100, REGIME_BAND * 100),
                                 quality='score tertiles within the cell'))


# ----------------------------------------------------- last occurrences (display)
def last_occurrences(bars, entries, net, mfe, mae, avail, horizon_index, touches,
                     barrier_id=DISPLAY_BARRIER_ID, last_n=LAST_N_OCCURRENCES):
    """The most recent occurrences, labelled by the declared display barrier."""
    outcome, held = touches.get(barrier_id, (None, None))
    rows = []
    for e in range(len(entries) - 1, -1, -1):
        if len(rows) >= last_n:
            break
        if not avail[e, horizon_index]:
            continue
        entry = int(entries[e])
        code = int(outcome[e]) if outcome is not None else -1
        label = BARRIER_OUTCOMES.get(code, 'undetermined_insufficient_history')
        bars_held = int(held[e]) if outcome is not None and code in (1, 2, 3)             else horizon_index + 1
        rows.append(dict(entry_date=bars[entry]['time'][:10], entry_time=bars[entry]['time'],
                         entry_index=entry, entry_price=_round(bars[entry]['open']),
                         horizon=horizon_index + 1,
                         net_return_pct=_round(net[e, horizon_index]),
                         mfe_pct=_round(mfe[e, horizon_index]),
                         mae_pct=_round(mae[e, horizon_index]),
                         bars_held=bars_held,
                         outcome='target_hit' if code == 1 else 'stop_hit' if code == 2
                         else 'gap_exit' if code == 3 else 'time_exit' if code == 0 else label))
    return dict(barrier_id=barrier_id, tie_rule=BARRIER_TIE_RULE,
                note='bars_held is the first touch of the display barrier; net/MFE/MAE are '
                     'measured at the cell horizon, which is a separate exit',
                rows=rows)


# --------------------------------------------------------------- per-cell engine
def outcome_cell(bars, events, timeframe, side, state, max_horizon=None,
                 keep_occurrences=True, cache=None, baseline=True,
                 replicates=BOOTSTRAP_REPLICATES, seed=BOOTSTRAP_SEED,
                 index_bars=None, last_n=LAST_N_OCCURRENCES):
    """Forward outcomes for one (symbol, timeframe, pattern, variant, side, state) cell.

    ``events`` must already be filtered to ``state`` and satisfy the detector
    contract validated by ``runner.validate_events``.
    """
    if side not in ('long', 'short'):
        raise ValueError('Unsupported side: ' + str(side))
    horizon = horizon_grid(timeframe, max_horizon)
    sign = 1 if side == 'long' else -1
    prices, gaps, starts, ends = _prepare(bars)
    n = len(bars)
    events = sorted(events, key=lambda e: (e['signal_index'], str(e.get('episode', ''))))

    excluded = {}
    kept = []
    for event in events:
        signal = int(event['signal_index'])
        if signal + 1 >= n:
            excluded['no_entry_bar'] = excluded.get('no_entry_bar', 0) + 1
            continue
        if bool(gaps[signal]) or bool(gaps[signal + 1]):
            excluded['signal_or_entry_gap'] = excluded.get('signal_or_entry_gap', 0) + 1
            continue
        kept.append(event)

    entries = np.array([int(e['signal_index']) + 1 for e in kept], dtype=np.int64)
    net, mfe, mae, exit_idx, gap_h, avail = _forward_paths(prices, gaps, entries, horizon, sign)

    records = []
    if keep_occurrences:
        for i, event in enumerate(kept):
            entry = int(entries[i])
            full = entry + horizon - 1
            available = int(avail[i].sum())
            records.append(dict(
                episode=int(event.get('episode', event['signal_index'])),
                state=state, signal_index=int(event['signal_index']), entry_index=entry,
                signal_time=bars[event['signal_index']]['end'], entry_time=bars[entry]['time'],
                entry_price=_round(prices[entry, 0], 6),
                direction=event.get('direction'), score=_round(event.get('score')),
                atr_at_signal=_round(event.get('atr')),
                pattern_start=event.get('pattern_start'),
                max_horizon=horizon, available_horizons=available,
                available_full_grid=bool(available == horizon),
                available_from_index=full,
                available_from_time=bars[full]['end'] if full < n else None,
                gap_exit_h=int(gap_h[i]) or None,
                excursion_bounded=not bool(gap_h[i]),
                cost_pct=COST_PCT,
                net_return_pct=_nan_to_none(net[i]),
                mfe_pct=_nan_to_none(mfe[i]),
                mae_pct=_nan_to_none(mae[i]),
                exit_index=[int(j) if ok else None
                            for j, ok in zip(exit_idx[i], avail[i])]))

    per_session = float(np.median(np.unique(starts, return_counts=True)[1])) if n else None
    # Calendar span of each horizon, measured from the data (for display only).
    span_days = None
    if len(entries):
        landed = ends[np.clip(exit_idx, 0, n - 1)] - starts[entries][:, None]
        span_days = np.where(avail, landed.astype(np.float64), np.nan)

    horizons = []
    for h in range(horizon):
        column = net[:, h]
        ok = np.isfinite(column)
        values = column[ok]
        count = int(values.size)
        row = dict(h=h + 1, n=count, sample_label=sample_label(count),
                   gap_exited_n=int(np.count_nonzero((gap_h > 0) & (gap_h <= h + 1) & ok)))
        if count:
            fav = mfe[:, h][ok]
            adv = mae[:, h][ok]
            med_fav, med_adv = float(np.median(fav)), float(np.median(adv))
            row.update(mean_net_return_pct=_round(values.mean()),
                       median_net_return_pct=_round(np.median(values)),
                       win_rate_pct=_round(float(np.count_nonzero(values > 0)) / count * 100),
                       loss_rate_pct=_round(float(np.count_nonzero(values < 0)) / count * 100),
                       percentiles={f'p{p}': _round(np.percentile(values, p)) for p in PERCENTILES},
                       median_mfe_pct=_round(med_fav), median_mae_pct=_round(med_adv),
                       mean_mfe_pct=_round(fav.mean()), mean_mae_pct=_round(adv.mean()),
                       mfe_mae_ratio=_round(med_fav / med_adv) if med_adv > 0 else None,
                       median_span_calendar_days=_round(np.nanmedian(span_days[:, h]), 2)
                       if span_days is not None and np.isfinite(span_days[:, h]).any() else None)
        else:
            row.update(mean_net_return_pct=None, median_net_return_pct=None, win_rate_pct=None,
                       loss_rate_pct=None, percentiles=None, median_mfe_pct=None,
                       median_mae_pct=None, mean_mfe_pct=None, mean_mae_pct=None,
                       mfe_mae_ratio=None, median_span_calendar_days=None)
        horizons.append(row)

    curve = [row['median_mfe_pct'] for row in horizons]
    selection = select_holding_period(entries, net, exit_idx, avail, starts, ends, horizon)

    unconditional = unconditional_baseline(prices, gaps, starts, entries, net, avail, horizon,
                                           sign, side, cache, replicates, seed) if baseline else \
        dict(status='skipped', horizons=[], occurrence_window=None, baseline_window=None,
             window_coverage=None, window_mismatch=None, replicates=0, baseline_thinning=None,
             method='skipped', block_bars=horizon, window_quantum_days=WINDOW_QUANTUM_DAYS)
    window_net = unconditional.pop('window_net', None)
    window_entries = unconditional.pop('window_entries', None)
    window_bounds = unconditional.pop('window_bounds', None)

    # Stability at the selected horizon; without one, at the pre-declared baseline hold.
    selected = selection['horizon']
    stability_source = 'selected' if selected else 'declared_baseline'
    stability_h = min(selected or BASELINE[timeframe], horizon)
    decay = stability(entries, net, exit_idx, avail, starts, ends, stability_h - 1,
                      selection['out_of_sample'].get('returns', []),
                      selection['out_of_sample'].get('entry_indices', [])) if len(entries) else None
    if decay is not None:
        decay['horizon_source'] = stability_source

    # First-touch barriers, the time it takes to reach them, and where the median
    # forward return stops improving.
    atr_values = np.array([float(e['atr']) for e in kept], dtype=np.float64)
    barriers, touches = barrier_table(prices, gaps, starts, ends, entries, atr_values,
                                      horizon, sign) if len(entries) else ([], {})
    buckets = None
    if len(entries) and window_net is not None and window_entries is not None:
        buckets = condition_buckets(bars, prices, gaps, starts, ends, entries, kept, net, mfe,
                                    mae, exit_idx, avail, stability_h - 1, window_net,
                                    window_entries, window_bounds, cache, index_bars,
                                    replicates, seed, side)
    recent = last_occurrences(bars, entries, net, mfe, mae, avail, stability_h - 1, touches,
                              last_n=last_n) if len(entries) else None

    # Significance of the OUT-OF-SAMPLE mean only; never of the overlapping aggregates.
    oos_returns = selection['out_of_sample'].get('returns', [])
    inference = dict(scope='out_of_sample_non_overlapping_trades', n=len(oos_returns),
                     p_value=None, p_value_t=None, p_value_bootstrap=None,
                     method='max(student_t, stationary_bootstrap)',
                     bootstrap_block_bars=OOS_BOOTSTRAP_BLOCK, replicates=replicates,
                     # Multiplicity is a property of the run, so q-values are assigned by
                     # apply_fdr() once every symbol is committed.  These stay null here;
                     # cell_outcomes.oos_q_value is the authoritative value.
                     q_value=None, fdr_trials=None, discovery_q10=None, discovery_q05=None,
                     fdr='assigned by the run-level Benjamini-Hochberg post-pass; read '
                         'cell_outcomes.oos_q_value / fdr_trials / discovery_q10',
                     status='insufficient_out_of_sample_trades')
    if len(oos_returns) >= OOS_MIN_TESTABLE:
        p_t = t_test_p_value(oos_returns)
        p_b = bootstrap_p_value(oos_returns, OOS_BOOTSTRAP_BLOCK, replicates, seed + 2)
        inference.update(p_value_t=_round(p_t), p_value_bootstrap=_round(p_b),
                         p_value=_round(max(p_t, p_b)), status='tested')

    occurrences = len({r['episode'] for r in records}) if keep_occurrences else len(kept)
    return dict(
        baseline=unconditional, stability=decay, inference=inference,
        barriers=dict(tie_rule=BARRIER_TIE_RULE, grid=[p['id'] for p in BARRIER_GRID],
                      display_barrier_id=DISPLAY_BARRIER_ID, rows=barriers),
        return_peak=return_peak(horizons), buckets=buckets, last_occurrences=recent,
        display_grid=display_grid(timeframe, horizon),
        outcome_engine_version=OUTCOME_ENGINE_VERSION, timeframe=timeframe, side=side, state=state,
        max_horizon=horizon, bars=n, events=len(events), admitted=len(kept),
        occurrences=occurrences, exclusions=excluded,
        bars_per_session=_round(per_session, 3),
        horizons=horizons, occurrence_records=records,
        mfe_flatten_h_90pct=flatten_horizon(curve, 0.90),
        mfe_flatten_h_95pct=flatten_horizon(curve, 0.95),
        mfe_terminal_median_pct=curve[-1] if curve else None,
        selection=selection,
        sample_label=sample_label(len(kept)),
        assumptions=dict(
            entry='Next observed open after the signal bar; signal or entry gap disqualifies',
            horizon='h bars held, exit at the close of bar entry+h-1 (identical to evaluation.hold)',
            costs=f'Fixed {COST_PCT}% of entry notional charged once at the measured horizon',
            excursions='Price excursions from entry using bar highs/lows, before costs, floored at 0',
            gap_exit='A data-quality gap inside the hold exits at that bar open; later excursions unknown',
            availability='Horizon h counts only when entry+h <= len(bars); full grid from entry+H-1',
            selection='Horizon chosen on training folds only (36m train / 6m test, purged, '
                      'non-overlapping, >=20 trades, mean minus one standard error > 0)',
            reporting='in_sample and out_of_sample are separate; the in-sample best is never '
                      'an expected return',
            overlap='Descriptive per-horizon aggregates include overlapping occurrences, so '
                    'their n counts occurrences, not independent observations, and no p-value '
                    'is derived from them. Fold selection, out-of-sample statistics and every '
                    'p-value use only the non-overlapping admission stream (entry after the '
                    'previous accepted exit) with an H-bar purge/embargo in training; the '
                    'baseline difference uses stationary block bootstraps of mean block H',
            unconditional_baseline='Entries at every eligible bar of the same stock, timeframe, '
                                   'side, window and horizon under the same 0.40% cost and the '
                                   'same availability rule; the difference CI resamples the two '
                                   'samples independently, which widens it (conservative)',
            barriers='First touch of a target/stop pair from the entry open using intrabar '
                     'high/low; if a bar touches both, the STOP is counted first because OHLC '
                     'cannot reveal intrabar order. An occurrence counts only once decided or '
                     'once the full horizon exists',
            buckets='Condition buckets are pre-declared constants, read at the signal bar, and '
                    'each is compared with unconditional entries in the SAME bucket so a bucket '
                    'cannot look good merely because the market rose. Score tertiles are an '
                    'in-sample stratification, not a point-in-time rule. Market-cap tier and '
                    'sector are current index-membership/label snapshots, not point-in-time',
            multiplicity='Per-cell two-sided p-value = max(Student-t, stationary bootstrap) on '
                         'out-of-sample trade returns; Benjamini-Hochberg q-values are computed '
                         'across every testable cell of the run, not per cell',
            shorts='Hypothetical price study; borrow, funding and eligibility unmodeled',
            source='Frozen OHLCV as supplied; corporate actions require independent verification'))


# ---------------------------------------------------------------------- storage
ROOT = Path(__file__).resolve().parents[1] / 'output' / 'expanded_research'
DB_NAME = 'outcomes.sqlite3'

SCHEMA = '''
CREATE TABLE IF NOT EXISTS outcome_runs(
  id TEXT PRIMARY KEY, research_run TEXT NOT NULL, source_run TEXT, snapshot_id TEXT,
  engine_version TEXT NOT NULL, created_at TEXT NOT NULL, status TEXT NOT NULL,
  metadata TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS outcome_symbols(
  run TEXT NOT NULL, symbol TEXT NOT NULL, status TEXT NOT NULL, seconds REAL,
  cells INTEGER, occurrences INTEGER, history_sha256 TEXT, metadata TEXT NOT NULL,
  PRIMARY KEY(run,symbol));
CREATE TABLE IF NOT EXISTS occurrence_outcomes(
  run TEXT NOT NULL, research_run TEXT NOT NULL, snapshot_id TEXT, history_sha256 TEXT,
  symbol TEXT NOT NULL, timeframe TEXT NOT NULL, pattern_id TEXT NOT NULL,
  variant TEXT NOT NULL, side TEXT NOT NULL, state TEXT NOT NULL,
  episode INTEGER NOT NULL, signal_index INTEGER NOT NULL, entry_index INTEGER NOT NULL,
  signal_time TEXT, entry_time TEXT, entry_price REAL, direction TEXT, score REAL,
  atr_at_signal REAL, pattern_start TEXT, max_horizon INTEGER, available_horizons INTEGER,
  available_full_grid INTEGER, available_from_index INTEGER, available_from_time TEXT,
  gap_exit_h INTEGER, excursion_bounded INTEGER, cost_pct REAL,
  net_return_pct TEXT, mfe_pct TEXT, mae_pct TEXT, exit_index TEXT,
  PRIMARY KEY(run,symbol,timeframe,pattern_id,variant,side,state,signal_index,episode));
CREATE TABLE IF NOT EXISTS cell_outcomes(
  run TEXT NOT NULL, research_run TEXT NOT NULL, snapshot_id TEXT, history_sha256 TEXT,
  symbol TEXT NOT NULL, timeframe TEXT NOT NULL, pattern_id TEXT NOT NULL,
  variant TEXT NOT NULL, side TEXT NOT NULL, state TEXT NOT NULL, family TEXT,
  max_horizon INTEGER, events INTEGER, admitted INTEGER, occurrences INTEGER,
  sample_label TEXT, mfe_flatten_h_90pct INTEGER, mfe_flatten_h_95pct INTEGER,
  mfe_terminal_median_pct REAL, selection_status TEXT, selected_horizon INTEGER,
  in_sample_median_training_expectancy_pct REAL, oos_n INTEGER, oos_expectancy_pct REAL,
  oos_win_rate_pct REAL, oos_sample_label TEXT,
  -- Unconditional (every eligible bar) baseline, same window / horizon / cost model.
  baseline_status TEXT, baseline_window_coverage REAL, baseline_window_mismatch INTEGER,
  baseline_mean_net_return_pct REAL, baseline_diff_mean_net_return_pct REAL,
  baseline_diff_ci95_low REAL, baseline_diff_ci95_high REAL, baseline_beats_unconditional INTEGER,
  -- Calendar-year decay at the stability horizon (non-overlapping admission stream).
  stability_horizon INTEGER, stability_horizon_source TEXT, stability_years INTEGER,
  stability_positive_years INTEGER, stability_edge_concentrated INTEGER,
  -- Significance of the OUT-OF-SAMPLE mean only, then BH-FDR across the run's cells.
  oos_p_value REAL, oos_p_value_t REAL, oos_p_value_bootstrap REAL, oos_q_value REAL,
  fdr_trials INTEGER, discovery_q10 INTEGER, discovery_q05 INTEGER,
  -- First touch of the declared display barrier. Ties inside one bar count as the STOP.
  display_barrier_id TEXT, barrier_n INTEGER, p_target_first REAL, p_stop_first REAL,
  p_neither REAL, median_bars_to_target REAL, median_bars_to_stop REAL,
  return_peak_h INTEGER, return_peak_net_return_pct REAL,
  -- Current label snapshots, NOT point-in-time: index membership is a cap proxy.
  market_cap_tier TEXT, sector TEXT, is_fno INTEGER,
  display_grid TEXT, last_occurrences TEXT,
  summary TEXT NOT NULL,
  PRIMARY KEY(run,symbol,timeframe,pattern_id,variant,side,state));
CREATE TABLE IF NOT EXISTS bucket_outcomes(
  run TEXT NOT NULL, research_run TEXT NOT NULL, symbol TEXT NOT NULL, timeframe TEXT NOT NULL,
  pattern_id TEXT NOT NULL, variant TEXT NOT NULL, side TEXT NOT NULL, state TEXT NOT NULL,
  horizon INTEGER, dimension TEXT NOT NULL, bucket TEXT NOT NULL, n INTEGER,
  sample_label TEXT, status TEXT, win_rate_pct REAL, mean_net_return_pct REAL,
  median_net_return_pct REAL, median_mfe_pct REAL, median_mae_pct REAL,
  -- Every bucket is compared with unconditional entries in the SAME bucket, so a
  -- bucket cannot look good merely because the market rose in it.
  baseline_scope TEXT, baseline_n INTEGER, baseline_mean_net_return_pct REAL,
  diff_mean_net_return_pct REAL,
  -- Descriptive, non-overlapping, whole history: NOT an out-of-sample statement.
  -- H0 is "this bucket equals unconditional entries in the SAME bucket", never
  -- "this bucket beats zero" - the latter is passed by any long in a rising stock.
  p_value REAL, p_value_method TEXT, p_value_vs_zero REAL, q_value REAL, fdr_trials INTEGER,
  discovery_q10 INTEGER, discovery_q05 INTEGER, detail TEXT NOT NULL,
  PRIMARY KEY(run,symbol,timeframe,pattern_id,variant,side,state,dimension,bucket));
CREATE INDEX IF NOT EXISTS outcome_bucket_lookup
  ON bucket_outcomes(run,dimension,bucket,q_value);
CREATE INDEX IF NOT EXISTS outcome_cell_lookup
  ON cell_outcomes(run,pattern_id,timeframe,side,selection_status);
CREATE INDEX IF NOT EXISTS outcome_cell_discovery ON cell_outcomes(run,oos_q_value);
CREATE INDEX IF NOT EXISTS outcome_occurrence_lookup
  ON occurrence_outcomes(run,symbol,timeframe,pattern_id,variant,side,state);
'''

_OCCURRENCE_COLUMNS = (
    'run', 'research_run', 'snapshot_id', 'history_sha256', 'symbol', 'timeframe', 'pattern_id',
    'variant', 'side', 'state', 'episode', 'signal_index', 'entry_index', 'signal_time',
    'entry_time', 'entry_price', 'direction', 'score', 'atr_at_signal', 'pattern_start',
    'max_horizon', 'available_horizons', 'available_full_grid', 'available_from_index',
    'available_from_time', 'gap_exit_h', 'excursion_bounded', 'cost_pct', 'net_return_pct',
    'mfe_pct', 'mae_pct', 'exit_index')

_CELL_COLUMNS = (
    'run', 'research_run', 'snapshot_id', 'history_sha256', 'symbol', 'timeframe', 'pattern_id',
    'variant', 'side', 'state', 'family', 'max_horizon', 'events', 'admitted', 'occurrences',
    'sample_label', 'mfe_flatten_h_90pct', 'mfe_flatten_h_95pct', 'mfe_terminal_median_pct',
    'selection_status', 'selected_horizon', 'in_sample_median_training_expectancy_pct',
    'oos_n', 'oos_expectancy_pct', 'oos_win_rate_pct', 'oos_sample_label',
    'baseline_status', 'baseline_window_coverage', 'baseline_window_mismatch',
    'baseline_mean_net_return_pct', 'baseline_diff_mean_net_return_pct',
    'baseline_diff_ci95_low', 'baseline_diff_ci95_high', 'baseline_beats_unconditional',
    'stability_horizon', 'stability_horizon_source', 'stability_years',
    'stability_positive_years', 'stability_edge_concentrated',
    'oos_p_value', 'oos_p_value_t', 'oos_p_value_bootstrap', 'oos_q_value',
    'fdr_trials', 'discovery_q10', 'discovery_q05',
    'display_barrier_id', 'barrier_n', 'p_target_first', 'p_stop_first', 'p_neither',
    'median_bars_to_target', 'median_bars_to_stop', 'return_peak_h',
    'return_peak_net_return_pct', 'market_cap_tier', 'sector', 'is_fno',
    'display_grid', 'last_occurrences', 'summary')

_BUCKET_COLUMNS = (
    'run', 'research_run', 'symbol', 'timeframe', 'pattern_id', 'variant', 'side', 'state',
    'horizon', 'dimension', 'bucket', 'n', 'sample_label', 'status', 'win_rate_pct',
    'mean_net_return_pct', 'median_net_return_pct', 'median_mfe_pct', 'median_mae_pct',
    'baseline_scope', 'baseline_n', 'baseline_mean_net_return_pct', 'diff_mean_net_return_pct',
    'p_value', 'p_value_method', 'p_value_vs_zero', 'q_value', 'fdr_trials', 'discovery_q10',
    'discovery_q05', 'detail')


def dumps(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':'))


def connect(root=ROOT):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(root / DB_NAME, timeout=60)
    con.execute('PRAGMA journal_mode=WAL')
    con.executescript(SCHEMA)
    return con


def save_outcome_run(con, metadata):
    con.execute('INSERT OR REPLACE INTO outcome_runs VALUES(?,?,?,?,?,?,?,?)',
                (metadata['id'], metadata['research_run'], metadata.get('source_run'),
                 metadata.get('snapshot_id'), OUTCOME_ENGINE_VERSION, metadata['created_at'],
                 metadata['status'], dumps(metadata)))
    con.commit()


def symbol_artifact(run, symbol, root=ROOT):
    return Path(root) / run / 'outcomes' / (hashlib.sha256(symbol.encode()).hexdigest() + '.json.gz')


def write_symbol_artifact(run, symbol, result, root=ROOT):
    path = symbol_artifact(run, symbol, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = dumps(result).encode('utf-8')
    temporary = path.with_suffix('.tmp')
    temporary.write_bytes(gzip.compress(raw, compresslevel=3))
    temporary.replace(path)
    return str(path), hashlib.sha256(raw).hexdigest()


def _cell_rows(run, result):
    provenance = (run, result['research_run'], result.get('snapshot_id'),
                  result.get('history_sha256'), result['symbol'])
    labels = result.get('labels') or {}
    cells, occurrences, buckets = [], [], []
    for cell in result['cells']:
        key = (cell['timeframe'], cell['pattern_id'], cell['variant'], cell['side'], cell['state'])
        selection = cell['selection']
        oos = selection['out_of_sample']['stats']
        training = [f['training_stats']['expectancy_pct'] for f in selection['folds']
                    if f['training_stats']['expectancy_pct'] is not None]
        summary = {k: v for k, v in cell.items() if k != 'occurrence_records'}
        base = cell.get('baseline') or {}
        decay = cell.get('stability') or {}
        inference = cell.get('inference') or {}
        at_stability = None
        if base.get('horizons') and decay.get('horizon'):
            index = min(decay['horizon'], len(base['horizons'])) - 1
            at_stability = base['horizons'][index]
        at_stability = at_stability or {}
        difference = at_stability.get('diff_mean_net_return_pct')
        low, high = at_stability.get('diff_ci95_low'), at_stability.get('diff_ci95_high')
        peak = cell.get('return_peak') or {}
        shown = next((r for r in (cell.get('barriers') or {}).get('rows', ())
                      if r['id'] == DISPLAY_BARRIER_ID), {})
        display = dict(id=shown.get('id'), n=shown.get('n'), p_target=shown.get('p_target'),
                       p_stop=shown.get('p_stop'), p_neither=shown.get('p_neither'),
                       bars_to_target=(shown.get('bars_to_target') or {}).get('median'),
                       bars_to_stop=(shown.get('bars_to_stop') or {}).get('median'))
        cells.append(provenance + key + (
            cell.get('family'), cell['max_horizon'], cell['events'], cell['admitted'],
            cell['occurrences'], cell['sample_label'], cell['mfe_flatten_h_90pct'],
            cell['mfe_flatten_h_95pct'], cell['mfe_terminal_median_pct'],
            selection['status'], selection['horizon'],
            float(np.median(training)) if training else None,
            oos['n'], oos['expectancy_pct'], oos['win_rate_pct'],
            selection['out_of_sample']['sample_label'],
            base.get('status'), base.get('window_coverage'),
            None if base.get('window_mismatch') is None else int(base['window_mismatch']),
            at_stability.get('baseline_mean_net_return_pct'), difference, low, high,
            None if low is None else int(low > 0),
            decay.get('horizon'), decay.get('horizon_source'), decay.get('years'),
            decay.get('positive_years'),
            None if decay.get('edge_concentrated_in_best_years') is None
            else int(decay['edge_concentrated_in_best_years']),
            inference.get('p_value'), inference.get('p_value_t'),
            inference.get('p_value_bootstrap'), inference.get('q_value'),
            inference.get('fdr_trials'), inference.get('discovery_q10'),
            inference.get('discovery_q05'),
            display['id'], display['n'], display['p_target'], display['p_stop'],
            display['p_neither'], display['bars_to_target'], display['bars_to_stop'],
            peak.get('median_peak_h'), peak.get('median_peak_net_return_pct'),
            labels.get('market_cap_tier'), labels.get('sector'), labels.get('is_fno'),
            dumps(cell.get('display_grid')), dumps((cell.get('last_occurrences') or {}).get('rows', [])),
            dumps(summary)))
        for row in ((cell.get('buckets') or {}).get('rows') or ()):
            buckets.append((run, result['research_run'], result['symbol']) + key + (
                cell['buckets']['horizon'], row['dimension'], row['bucket'], row['n'],
                row['sample_label'], row['status'], row.get('win_rate_pct'),
                row.get('mean_net_return_pct'), row.get('median_net_return_pct'),
                row.get('median_mfe_pct'), row.get('median_mae_pct'), row['baseline_scope'],
                row['baseline_n'], row.get('baseline_mean_net_return_pct'),
                row.get('diff_mean_net_return_pct'), row.get('p_value'),
                row.get('p_value_method'), row.get('p_value_vs_zero'),
                None, None, None, None, dumps(row)))
        for record in cell.get('occurrence_records', ()):
            occurrences.append(provenance + key + (
                record['episode'], record['signal_index'], record['entry_index'],
                record['signal_time'], record['entry_time'], record['entry_price'],
                record['direction'], record['score'], record['atr_at_signal'],
                record['pattern_start'], record['max_horizon'], record['available_horizons'],
                int(record['available_full_grid']), record['available_from_index'],
                record['available_from_time'], record['gap_exit_h'],
                int(record['excursion_bounded']), record['cost_pct'],
                dumps(record['net_return_pct']), dumps(record['mfe_pct']),
                dumps(record['mae_pct']), dumps(record['exit_index'])))
    return cells, occurrences, buckets


def commit_symbol(con, run, symbol, path, digest, root=ROOT):
    raw = gzip.decompress(Path(path).read_bytes())
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError('Outcome artifact digest mismatch: ' + symbol)
    result = json.loads(raw)
    if result.get('run') != run or result.get('symbol') != symbol:
        raise ValueError('Outcome artifact identity mismatch: ' + symbol)
    cells, occurrences, buckets = _cell_rows(run, result)
    metadata = {k: v for k, v in result.items() if k != 'cells'}
    metadata.update(artifact=Path(path).name, artifact_sha256=digest, cells=len(cells),
                    occurrence_rows=len(occurrences), bucket_rows=len(buckets))
    with con:
        con.execute('DELETE FROM cell_outcomes WHERE run=? AND symbol=?', (run, symbol))
        con.execute('DELETE FROM occurrence_outcomes WHERE run=? AND symbol=?', (run, symbol))
        con.execute('DELETE FROM bucket_outcomes WHERE run=? AND symbol=?', (run, symbol))
        con.executemany('INSERT INTO cell_outcomes VALUES(' + ','.join('?' * len(_CELL_COLUMNS)) + ')', cells)
        con.executemany('INSERT INTO occurrence_outcomes VALUES(' + ','.join('?' * len(_OCCURRENCE_COLUMNS)) + ')',
                        occurrences)
        con.executemany('INSERT INTO bucket_outcomes VALUES(' + ','.join('?' * len(_BUCKET_COLUMNS)) + ')',
                        buckets)
        con.execute('INSERT OR REPLACE INTO outcome_symbols VALUES(?,?,?,?,?,?,?,?)',
                    (run, symbol, 'complete', result.get('seconds'), len(cells), len(occurrences),
                     result.get('history_sha256'), dumps(metadata)))
    return len(cells), len(occurrences)


def save_symbol_error(con, run, symbol, error):
    with con:
        con.execute('INSERT OR REPLACE INTO outcome_symbols VALUES(?,?,?,?,?,?,?,?)',
                    (run, symbol, 'error', None, 0, 0, None, dumps(error)))


#: The two independent testing families of a run.  Cells and condition buckets are
#: corrected separately, as the brief requires.
FDR_FAMILIES = (dict(name='cells', table='cell_outcomes', p='oos_p_value', q='oos_q_value'),
                dict(name='buckets', table='bucket_outcomes', p='p_value', q='q_value'))


def apply_fdr(con, run, levels=FDR_LEVELS, family=None):
    """Benjamini-Hochberg across one whole testing family of ``run``.

    Multiplicity is a property of the run, not of one row, so this is a post-pass
    over every committed symbol.  It is idempotent: rerun it after a resume.
    """
    family = family or FDR_FAMILIES[0]
    table, p_column, q_column = family['table'], family['p'], family['q']
    clear = ('UPDATE %s SET %s=NULL,fdr_trials=NULL,discovery_q10=NULL,discovery_q05=NULL '
             'WHERE run=?' % (table, q_column))
    rows = list(con.execute('SELECT rowid,%s FROM %s WHERE run=? AND %s IS NOT NULL '
                            'ORDER BY rowid' % (p_column, table, p_column), (run,)))
    trials = len(rows)
    if not trials:
        with con:
            con.execute(clear, (run,))
        return dict(run=run, family=family['name'], trials=0,
                    discoveries={f'q<{level}': 0 for level in levels})
    q_values = benjamini_hochberg([p for _, p in rows])
    high, low = max(levels), min(levels)
    updates = [(q, trials, int(q < high), int(q < low), rowid)
               for (rowid, _), q in zip(rows, q_values)]
    with con:
        con.execute(clear, (run,))
        con.executemany('UPDATE %s SET %s=?,fdr_trials=?,discovery_q10=?,discovery_q05=? '
                        'WHERE rowid=?' % (table, q_column), updates)
    return dict(run=run, family=family['name'], trials=trials,
                discoveries={f'q<{level}': sum(1 for q in q_values if q < level)
                             for level in levels},
                method='benjamini_hochberg_over_the_whole_%s_family_of_the_run' % family['name'])


def apply_all_fdr(con, run, levels=FDR_LEVELS):
    """Correct both families; each is its own family, never pooled."""
    return {family['name']: apply_fdr(con, run, levels, family) for family in FDR_FAMILIES}


def completed_symbols(con, run):
    return {row[0] for row in con.execute(
        "SELECT symbol FROM outcome_symbols WHERE run=? AND status='complete'", (run,))}
