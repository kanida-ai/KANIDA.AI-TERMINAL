"""Causal, per-cell research: fixed baseline and rolling unseen rule selection.

No capital allocation, broker execution, database access, or cross-cell pooling.
"""
from __future__ import annotations

from calendar import monthrange
from datetime import date
import math

import numpy as np
from numba import njit

ENGINE_VERSION = '1.0.1'
HOLDS = {'1H': (3, 6, 12, 24), '4H': (3, 6, 12, 24),
         '1D': (5, 10, 20, 40), '1W': (2, 4, 8, 13)}
BASELINE = {'1H': 6, '4H': 6, '1D': 10, '1W': 4}
COST_PCT = .4
MIN_TRAIN = 20
REASONS = {1: 'time', 2: 'stop', 3: 'target', 4: 'stop_gap',
           5: 'target_gap', 6: 'data_gap_first_available_open'}
EXCLUDED = {-1: 'unsupported_state', -2: 'final_horizon', -3: 'signal_or_entry_gap',
            -4: 'invalid_price'}
_BAR_CACHE = (None, None)


def _months(value, months):
    y, m = divmod(value.year * 12 + value.month - 1 + months, 12)
    return date(y, m + 1, min(value.day, monthrange(y, m + 1)[1]))


def _prepare(bars):
    global _BAR_CACHE
    if _BAR_CACHE[0] is bars:
        return _BAR_CACHE[1]
    prices = np.array([[b[k] for k in ('open', 'high', 'low', 'close')] for b in bars], dtype=np.float64).reshape(-1, 4)
    gaps = np.array([bool(b.get('gap', False)) for b in bars], dtype=np.bool_)
    starts = np.array([date.fromisoformat(b['time'][:10]).toordinal() for b in bars], dtype=np.int64)
    ends = np.array([date.fromisoformat(b['end'][:10]).toordinal() for b in bars], dtype=np.int64)
    # Bad candles cannot manufacture a valid excursion or unobserved fill.
    if len(prices) and (not np.all(np.isfinite(prices)) or np.any(prices <= 0) or
                        np.any(prices[:, 1] < prices[:, 2]) or
                        np.any(prices[:, 1] < np.maximum(prices[:, 0], prices[:, 3])) or
                        np.any(prices[:, 2] > np.minimum(prices[:, 0], prices[:, 3]))):
        raise ValueError('Research requires finite positive and coherent OHLC candles')
    if any(b['end'] <= b['time'] for b in bars):
        raise ValueError('Research candles must end after their start')
    if any(bars[i]['time'] <= bars[i-1]['time'] or bars[i]['time'] < bars[i-1]['end'] for i in range(1, len(bars))):
        raise ValueError('Research candles must be chronological and non-overlapping')
    result = prices, gaps, starts, ends
    _BAR_CACHE = (bars, result)
    return result


def _rules(timeframe, states):
    rows = []
    for trigger in sorted(set(states)):
        for hold in HOLDS[timeframe]:
            for stop, target in [(0., 0.)] + [(s, r) for s in (1., 2.) for r in (1., 2., 3.)]:
                rows.append(dict(trigger=trigger, hold=hold, stop_atr=stop, target_r=target,
                                 id=f'{trigger}:{hold}:{stop:g}:{target:g}'))
    return sorted(rows, key=lambda r: r['id'])


@njit(cache=True)
def _matrix(prices, gaps, signals, atr, event_state, rule_state, holds, stops, targets, sign):
    """Compute each possible fill once; future fill values never rank admission."""
    nr, ne = len(holds), len(signals)
    exits = np.full((nr, ne), -1, dtype=np.int64)
    fills = np.zeros((nr, ne), dtype=np.float64)
    codes = np.full((nr, ne), -1, dtype=np.int8)
    ambiguous = np.zeros((nr, ne), dtype=np.bool_)
    returns = np.zeros((nr, ne), dtype=np.float64)
    for r in range(nr):
        for e in range(ne):
            if event_state[e] != rule_state[r]:
                continue
            signal = signals[e]
            entry = signal + 1
            if entry + holds[r] > len(prices):
                codes[r, e] = -2
                continue
            if gaps[signal] or gaps[entry]:
                codes[r, e] = -3
                continue
            price = prices[entry, 0]
            stop = price - sign * stops[r] * atr[e]
            target = price + sign * stops[r] * targets[r] * atr[e]
            if stops[r] and (not np.isfinite(atr[e]) or atr[e] <= 0 or min(stop, target) <= 0):
                codes[r, e] = -4
                continue
            for j in range(entry, entry + holds[r]):
                o, h, l, c = prices[j]
                code = 0
                fill = c
                if j > entry and gaps[j]:
                    code, fill = 6, o
                elif stops[r]:
                    if sign * (o - stop) <= 0:
                        code, fill = 4, o
                    elif sign * (o - target) >= 0:
                        code, fill = 5, o
                    else:
                        hit_stop = l <= stop if sign == 1 else h >= stop
                        hit_target = h >= target if sign == 1 else l <= target
                        if hit_stop:
                            code, fill = 2, stop
                            ambiguous[r, e] = hit_target
                        elif hit_target:
                            code, fill = 3, target
                if not code and j == entry + holds[r] - 1:
                    code = 1
                if code:
                    exits[r, e], fills[r, e], codes[r, e] = j, fill, code
                    returns[r, e] = sign * (fill - price) / price * 100. - COST_PCT
                    break
    return exits, fills, codes, ambiguous, returns


@njit(cache=True)
def _choose(exits, returns, signals, starts, ends, holds, train_start, test_start, embargo, minimum):
    best, best_score = -1, 0.
    for r in range(len(holds)):
        last, n, mean, m2 = -1, 0, 0., 0.
        for e in range(len(signals)):
            entry = signals[e] + 1
            if entry >= len(starts) or starts[entry] < train_start:
                continue
            if starts[entry] >= test_start:
                break
            purge_end = entry + holds[r] - 1 + embargo
            if purge_end >= len(ends) or ends[purge_end] >= test_start:
                continue
            if exits[r, e] < 0 or entry <= last:
                continue
            last = exits[r, e]
            n += 1
            delta = returns[r, e] - mean
            mean += delta / n
            m2 += delta * (returns[r, e] - mean)
        if n >= minimum:
            score = mean - math.sqrt(max(0., m2) / (n - 1) / n)
            if score > best_score:
                best, best_score = r, score
    return best


def _stats(values):
    values = np.asarray(values, dtype=np.float64)
    if not np.all(np.isfinite(values)):
        raise ValueError('Nonfinite trade returns cannot be reported as research results')
    n = len(values)
    if not n:
        return dict(n=0, wins=0, losses=0, breakeven=0, win_rate_pct=None, win_rate_ci95=None, expectancy_pct=None,
                    standard_error_pct=None, expectancy_ci95=None, selection_score=None,
                    profit_factor=None, sum_net_return_pct=0.)
    mean = float(values.mean())
    se = float(values.std(ddof=1) / math.sqrt(n)) if n > 1 else None
    negative, positive = values[values < 0], values[values > 0]
    z = 1.959963984540054
    win_fraction = len(positive) / n
    divisor = 1 + z*z/n
    mid = (win_fraction + z*z/(2*n)) / divisor
    half = z * math.sqrt(win_fraction*(1-win_fraction)/n + z*z/(4*n*n)) / divisor
    return dict(n=n, wins=int(len(positive)), losses=int(len(negative)),
                breakeven=int(np.count_nonzero(values == 0)),
                win_rate_pct=float(win_fraction * 100), win_rate_ci95=[(mid-half)*100, (mid+half)*100], expectancy_pct=mean,
                standard_error_pct=se, expectancy_ci95=[mean-1.96*se, mean+1.96*se] if se is not None else None,
                selection_score=mean-se if se is not None else None,
                profit_factor=float(positive.sum() / -negative.sum()) if len(negative) else None,
                sum_net_return_pct=float(values.sum()))


def _excursions(bars, entry, end, price, fill, sign, code, stop, target):
    """Price excursion bounds only over the held position, never after exit.

    Full candles before the exit are known. Barrier exits do not reveal the
    order of the exit candle's extrema. Missing-data intervals have no finite
    defensible upper bound, so their upper bounds are null.
    """
    favorable = adverse = 0.
    def observe(values, mfe, mae):
        for value in values:
            move = sign * (float(value)-price) / price * 100
            mfe, mae = max(mfe, move), max(mae, -move)
        return mfe, mae
    for j in range(entry, end):
        favorable, adverse = observe((bars[j]['high'], bars[j]['low']), favorable, adverse)
    final = bars[end]
    if code == 1:
        favorable, adverse = observe((final['high'], final['low']), favorable, adverse)
        upper_favorable, upper_adverse = favorable, adverse
    elif code in (4, 5, 6):
        favorable, adverse = observe((final['open'],), favorable, adverse)
        upper_favorable = None if code == 6 else favorable
        upper_adverse = None if code == 6 else adverse
    else:
        favorable, adverse = observe((final['open'], fill), favorable, adverse)
        possible_favorable, possible_adverse = observe((final['high'], final['low']), 0., 0.)
        upper_favorable = max(favorable, min(possible_favorable, max(0., sign*(target-price)/price*100)))
        upper_adverse = max(adverse, min(possible_adverse, max(0., sign*(price-stop)/price*100)))
    return dict(mfe_pct=favorable, mae_pct=adverse, mfe_lower_pct=favorable,
                mfe_upper_pct=upper_favorable, mae_lower_pct=adverse,
                mae_upper_pct=upper_adverse, excursion_uncertain=code in (2, 3, 6),
                excursion_unbounded_due_to_missing_data=code == 6)


def _trade(bars, event, rule, r, e, matrix, side, fold=None):
    exits, fills, codes, ambiguous, returns = matrix
    entry, end = int(event['signal_index']) + 1, int(exits[r, e])
    price, fill, code = float(bars[entry]['open']), float(fills[r, e]), int(codes[r, e])
    sign = 1 if side == 'long' else -1
    opening = code in (4, 5, 6)
    stop = price-sign*rule['stop_atr']*float(event['atr']) if rule['stop_atr'] else None
    target = price+sign*rule['stop_atr']*rule['target_r']*float(event['atr']) if rule['stop_atr'] else None
    return dict(signal_index=int(event['signal_index']), entry_index=entry, exit_index=end,
                signal_time=bars[event['signal_index']]['end'], entry_time=bars[entry]['time'],
                exit_time=bars[end]['time'] if opening else bars[end]['end'],
                exit_candle_start=bars[end]['time'], exit_candle_end=bars[end]['end'],
                exit_timing='open' if opening else 'close' if code == 1 else 'intrabar_settled_at_close',
                episode=int(event.get('episode', event['signal_index'])), state=event['state'], side=side,
                entry=price, exit=fill, stop=stop, target=target,
                rule=dict(rule), fold=fold, gross_return_pct=float(returns[r, e] + COST_PCT),
                cost_pct=COST_PCT, net_return_pct=float(returns[r, e]), exit_reason=REASONS[code],
                same_bar_stop_target=bool(ambiguous[r, e]), holding_bars=end-entry+1,
                formation_start=event.get('pattern_start'), atr_at_signal=float(event['atr']),
                **_excursions(bars, entry, end, price, fill, sign, code, stop, target))


def evaluate_cell(bars, events, timeframe, side, states):
    """Evaluate one stock/pattern/variant/timeframe/side over immutable candles.

    `states` declares supported triggers. Input events must be causally generated;
    the evaluator checks indexes/state and preserves their separate identities.
    Do not mutate `bars` in-place between calls (prepared arrays are cached).
    """
    if timeframe not in HOLDS or side not in ('long', 'short'):
        raise ValueError('Unsupported timeframe or side')
    states = sorted(set(states))
    if not states or not set(states) <= {'setup', 'confirmed'}:
        raise ValueError('Declare supported setup/confirmed states')
    events = sorted(events, key=lambda e: (e['signal_index'], e['state'], str(e.get('episode', ''))))
    for e in events:
        if not isinstance(e['signal_index'], (int, np.integer)) or not 0 <= e['signal_index'] < len(bars):
            raise ValueError('Event signal index must address an available candle')
        if e['state'] not in states or not math.isfinite(float(e['atr'])) or float(e['atr']) <= 0:
            raise ValueError('Event state/ATR must satisfy the detector contract')
    prices, gaps, starts, ends = _prepare(bars)
    rules = _rules(timeframe, states)
    # The baseline is declared from detector capabilities, not picked from future events.
    reference_trigger = 'setup' if 'setup' in states else 'confirmed'
    reference = dict(trigger=reference_trigger, hold=BASELINE[timeframe], stop_atr=0., target_r=0.,
                     id=f'{reference_trigger}:{BASELINE[timeframe]}:0:0')
    baseline_index = next(i for i, r in enumerate(rules) if r['id'] == reference['id'])
    signals = np.array([e['signal_index'] for e in events], dtype=np.int64)
    atr = np.array([e['atr'] for e in events], dtype=np.float64)
    event_state = np.array([e['state'] == 'confirmed' for e in events], dtype=np.int8)
    rule_state = np.array([r['trigger'] == 'confirmed' for r in rules], dtype=np.int8)
    holds = np.array([r['hold'] for r in rules], dtype=np.int64)
    matrix = _matrix(prices, gaps, signals, atr, event_state, rule_state, holds,
                     np.array([r['stop_atr'] for r in rules]), np.array([r['target_r'] for r in rules]),
                     1 if side == 'long' else -1)
    exits, fills, codes, ambiguous, returns = matrix

    def admit(r, lo, hi, last=-1, fold=None, training_before=None):
        ledger, exclusions = [], {}
        for e, event in enumerate(events):
            entry = event['signal_index'] + 1
            # End-of-data events have no entry timestamp; still count explicitly.
            if entry >= len(bars):
                if training_before is None and (not len(bars) or hi > ends[-1]) and event['state'] == rules[r]['trigger']:
                    exclusions['final_horizon'] = exclusions.get('final_horizon', 0) + 1
                continue
            if starts[entry] < lo or starts[entry] >= hi or event['state'] != rules[r]['trigger']:
                continue
            if training_before is not None:
                purge_end = entry + rules[r]['hold'] - 1 + max(HOLDS[timeframe])
                if purge_end >= len(bars) or ends[purge_end] >= training_before:
                    exclusions['purged_horizon'] = exclusions.get('purged_horizon', 0) + 1
                    continue
            code = int(codes[r, e])
            if code < 0:
                reason = EXCLUDED[code]
                exclusions[reason] = exclusions.get(reason, 0) + 1
                continue
            if entry <= last:
                exclusions['overlap'] = exclusions.get('overlap', 0) + 1
                continue
            last = int(exits[r, e])
            ledger.append(_trade(bars, event, rules[r], r, e, matrix, side, fold))
        return ledger, exclusions, last

    reference_trades, reference_exclusions, _ = admit(baseline_index, 0, date.max.toordinal())
    folds, forward, forward_exclusions = [], [], {}
    last_exit = -1
    if bars:
        cursor = _months(date.fromordinal(int(starts[0])), 36)
        final_day = date.fromordinal(int(ends[-1]))
        while cursor <= final_day:
            end = _months(cursor, 6)
            train_start = _months(cursor, -36)
            r = int(_choose(exits, returns, signals, starts, ends, holds, train_start.toordinal(),
                            cursor.toordinal(), max(HOLDS[timeframe]), MIN_TRAIN))
            fold = dict(index=len(folds), train_start=train_start.isoformat(), test_start=cursor.isoformat(),
                        test_end=min(date.fromordinal(end.toordinal()-1), final_day).isoformat(),
                        rule=None, training_stats=_stats([]), status='insufficient_positive_training_evidence',
                        stats=_stats([]), exclusions={}, stats_scope='entry_cohort_with_full_trade_outcome')
            if r >= 0:
                training, _, _ = admit(r, train_start.toordinal(), cursor.toordinal(), training_before=cursor.toordinal())
                ledger, excluded, last_exit = admit(r, cursor.toordinal(), end.toordinal(), last_exit, len(folds))
                fold.update(rule=rules[r], training_stats=_stats([t['net_return_pct'] for t in training]),
                            status='frozen_for_test', stats=_stats([t['net_return_pct'] for t in ledger]), exclusions=excluded)
                forward.extend(ledger)
                for reason, n in excluded.items():
                    forward_exclusions[reason] = forward_exclusions.get(reason, 0) + n
            folds.append(fold)
            cursor = end
    stats = _stats([t['net_return_pct'] for t in forward])
    status = 'no_occurrences' if not events else 'insufficient_history' if not folds else 'tested' if stats['n'] >= 20 else 'small_test_sample' if stats['n'] else 'no_selected_test_trades'
    return dict(engine_version=ENGINE_VERSION, status=status, bars=len(bars), events=len(events),
                reference=dict(rule=reference, stats=_stats([t['net_return_pct'] for t in reference_trades]),
                               trades=reference_trades, exclusions=reference_exclusions,
                               scope='whole_history_descriptive_baseline'),
                walkforward=dict(stats=stats, trades=forward, status=status), folds=folds,
                candidate_count=len(rules), exclusions=dict(reference=reference_exclusions, walkforward=forward_exclusions),
                assumptions=dict(training_months=36, test_months=6, minimum_training_trades=MIN_TRAIN,
                                 minimum_test_trades_for_sample_status=20, embargo_bars=max(HOLDS[timeframe]),
                                 fee_bps_roundtrip=30, slippage_bps_roundtrip=10,
                                 costs='Fixed 0.40% of entry notional; price fills are unslipped',
                                 selection='Maximum mean net return minus one standard error, strictly positive; lexicographic rule-id tie break',
                                 entry='Next observed open only when signal and entry candles are not quality-gap flagged',
                                 final_sample='Full selected-rule potential holding horizon must exist before admitting final-sample trades',
                                 training_purge='Full candidate holding horizon plus maximum candidate hold must end strictly before test start',
                                 overlap='Entry index must be after prior accepted exit index; one stream across all folds',
                                 fold_accounting='Entry-cohort trade outcomes, including exits after the fold; no calendar P&L or portfolio equity computed',
                                 gap_exit='Held positions exit at first available quality-gap open; no future-gap censoring',
                                 shorts='Hypothetical price study; borrow, funding, eligibility and derivatives basis unmodeled',
                                 inference='Historical research with candidate screening; no guarantee, multiplicity-adjusted significance, or portfolio claim',
                                 source='Stored OHLCV as supplied; corporate actions, dividend adjustment and point-in-time universe require independent verification'))
