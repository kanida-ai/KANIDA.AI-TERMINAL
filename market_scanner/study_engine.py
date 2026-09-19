"""Chronological shared-capital research. No broker calls or source DB writes."""
from __future__ import annotations
from dataclasses import asdict
from datetime import date, datetime, timedelta
import heapq
import math
import statistics

VERSION = '2.0.0'


def add_months(value, months):
    value = date.fromisoformat(value[:10])
    month = value.year * 12 + value.month - 1 + months
    return date(month // 12, month % 12 + 1, 1).isoformat()


def outcome(event, bars, side, rule, settings):
    """Create a fill path. Future outcomes never decide admission/allocation."""
    if event['state'] != rule.trigger:
        return None
    i = event['signal_index'] + 1
    if i >= len(bars) or bars[i]['time'][:10] > settings['end']:
        return None
    if settings['product'] == 'CNC' and side == 'short':
        return {'skipped_reason': 'Overnight cash-equity short is ineligible', 'signal_index': event['signal_index']}
    if settings['product'] == 'MIS' and settings['timeframe'] not in ('1H', '4H'):
        return {'skipped_reason': 'Intraday execution requires intraday candles', 'signal_index': event['signal_index']}
    if settings['product'] == 'MIS' and bars[i]['time'][11:16] >= '15:15':
        return {'skipped_reason': 'No completed intraday exit candle before 15:15', 'signal_index': event['signal_index']}
    sign = 1 if side == 'long' else -1
    slip = settings['slippage_bps'] / 10000
    entry = float(bars[i]['open']) * (1 + sign * slip)
    if not math.isfinite(entry) or entry <= 0:
        return None
    stop = entry - sign * rule.stop_atr * event['atr'] if rule.stop_atr else None
    target = entry + sign * rule.stop_atr * rule.target_r * event['atr'] if rule.stop_atr else None
    if stop is not None and min(stop, target) <= 0:
        return None
    last = min(len(bars) - 1, i + rule.hold - 1)
    while last >= i and (bars[last]['end'][:10] > settings['end'] or settings['product'] == 'MIS' and
                        (bars[last]['time'][:10] != bars[i]['time'][:10] or bars[last]['end'][11:16] > '15:15')):
        last -= 1
    if last < i:
        return {'skipped_reason': 'No eligible exit candle in the chosen period', 'signal_index': event['signal_index']}
    mfe = mae = 0.0
    uncertain = False
    for j in range(i, last + 1):
        b = bars[j]
        o, h, l, c = (float(b[k]) for k in ('open', 'high', 'low', 'close'))
        reason, raw, timing = None, c, 'close'
        if j > i and b.get('gap'):
            reason, raw, timing = 'data_gap_first_available_open', o, 'open'
        elif stop is not None:
            if sign * (o - stop) <= 0:
                reason, raw, timing = 'stop_gap', o, 'open'
            elif sign * (o - target) >= 0:
                reason, raw, timing = 'target_gap', o, 'open'
            else:
                hit_stop = l <= stop if sign == 1 else h >= stop
                hit_target = h >= target if sign == 1 else l <= target
                if hit_stop:
                    reason, raw, timing = 'stop', stop, 'intrabar'
                    uncertain = bool(hit_target)
                elif hit_target:
                    reason, raw, timing = 'target', target, 'intrabar'
        # Intrabar highs/lows cannot establish the excursion before an exit.
        observed = [o, raw] if reason else [h, l, c]
        if reason and timing != 'open':
            uncertain = True
        for v in observed:
            move = sign * (v - entry) / entry * 100
            mfe, mae = max(mfe, move), max(mae, -move)
        if reason is None and j == last:
            reason = 'session_exit' if settings['product'] == 'MIS' else 'time_exit' if j == i + rule.hold - 1 else 'study_end'
        if reason:
            exit_price = raw * (1 - sign * slip)
            gross = sign * (exit_price - entry) / entry * 100
            cost = settings['fee_bps'] / 100  # declared round-trip fee on entry notional
            return dict(signal_index=event['signal_index'], entry_index=i, exit_index=j,
                        signal_time=bars[event['signal_index']]['end'], entry_time=bars[i]['time'],
                        exit_time=b['time'] if timing == 'open' else b['end'], exit_candle_end=b['end'],
                        exit_timing=timing, entry=entry, exit=exit_price, raw_entry=float(bars[i]['open']),
                        raw_exit=raw, stop=stop, target=target, side=side, product=settings['product'],
                        net_return_pct=gross - cost, gross_return_pct=gross, cost_pct=cost,
                        mfe_pct=mfe, mae_pct=mae, excursion_uncertain=uncertain, exit_reason=reason,
                        holding_bars=j - i + 1, holding_days=(date.fromisoformat(b['time'][:10]) - date.fromisoformat(bars[i]['time'][:10])).days,
                        overnight=bars[i]['time'][:10] != b['time'][:10], rule=asdict(rule),
                        score=event['score'], formation_start=event.get('pattern_start'), episode=event['episode'])
    return None


def independent(trades):
    last = -1
    for trade in trades:
        if not trade or trade.get('skipped_reason') or trade['entry_index'] <= last:
            continue
        yield trade
        last = trade['exit_index']


def learn_rule(events, bars, side, rules, settings, before, start, embargo, minimum):
    best = None
    for rule in rules:
        ledger = []
        for event in events:
            i = event['signal_index'] + 1
            # Entire potential horizon is purged, independent of realized exit.
            if i >= len(bars) or bars[i]['time'][:10] < start or i + rule.hold + embargo >= len(bars):
                continue
            if bars[i + rule.hold + embargo]['end'][:10] >= before:
                continue
            trade = outcome(event, bars, side, rule, settings)
            if trade and not trade.get('skipped_reason'):
                ledger.append(trade)
        ledger = list(independent(ledger))
        if len(ledger) < minimum:
            continue
        values = [t['net_return_pct'] for t in ledger]
        score = statistics.mean(values) - statistics.stdev(values) / math.sqrt(len(values)) if len(values) > 1 else -math.inf
        if score > 0 and (best is None or (score, rule.id) > (best['score'], best['rule'].id)):
            best = dict(rule=rule, score=score, samples=len(values), average=statistics.mean(values))
    return best


def portfolio(candidates, histories, settings, cancelled=lambda: False):
    """Merge observed marks and orders by time. One pool of cash, whole shares."""
    candidates = sorted(candidates, key=lambda t: (t['entry_time'], -t['score'], t['symbol'], t['timeframe'], t['pattern'], t['side']))
    for i, t in enumerate(candidates):
        t['id'] = str(i + 1)
    def marks(key, bars):
        for b in bars:
            if b['time'][:10] < settings['start'] or b['time'][:10] > settings['end']:
                continue
            yield (b['time'], 2, key, 'mark', float(b['open']))
            if b['end'][:10] <= settings['end']:
                yield (b['end'], 0, key, 'mark', float(b['close']))
    orders = []
    for t in candidates:
        orders.append((t['entry_time'], 4, t['id'].zfill(10), 'entry', t))
        orders.append((t['exit_time'], 3 if t['exit_timing'] == 'open' else 1, t['id'].zfill(10), 'exit', t))
    orders.sort(key=lambda x: x[:3])
    streams = [marks(key, bars) for key, bars in histories.items()] + [iter(orders)]
    cash = float(settings['capital'])
    positions, ledger, skipped, curve = {}, [], [], []
    fees = realized = 0.0
    peak = cash
    max_dd = 0.0
    prices = {}
    last_time = None
    def equity_point(timestamp):
        nonlocal peak, max_dd
        unrealized = sum(p['quantity'] * (prices.get(p['symbol'], p['raw_entry']) - p['entry']) * (1 if p['side'] == 'long' else -1) for p in positions.values())
        collateral = sum(p['notional'] for p in positions.values())
        equity = cash + collateral + unrealized
        peak = max(peak, equity)
        dd = 100 * (peak - equity) / peak if peak > 0 else 0
        max_dd = max(max_dd, dd)
        curve.append(dict(time=timestamp, equity=round(equity, 2), cash=round(cash, 2),
                          realized=round(realized, 2), unrealized=round(unrealized, 2), drawdown_pct=round(dd, 4),
                          positions=len(positions), trade_ids=list(positions)))
    curve.append(dict(time=settings['start'] + ' 00:00:00', equity=cash, cash=cash, realized=0, unrealized=0, drawdown_pct=0, positions=0, trade_ids=[]))
    count = 0
    for timestamp, priority, key, kind, value in heapq.merge(*streams, key=lambda x: x[:3]):
        count += 1
        if count % 2048 == 0 and cancelled():
            raise InterruptedError('Study cancelled')
        if last_time is not None and timestamp != last_time:
            equity_point(last_time)
        last_time = timestamp
        if kind == 'mark':
            prices[key.split('|')[0]] = value
        elif kind == 'exit':
            t = value
            p = positions.pop(t['id'], None)
            if p:
                gross = p['quantity'] * (t['exit'] - t['entry']) * (1 if t['side'] == 'long' else -1)
                cash += p['notional'] + gross
                net = gross - p['costs']
                realized += net
                ledger.append(dict(p, gross_pnl=round(gross, 2), net_pnl=round(net, 2), cash_after=round(cash, 2)))
        else:
            t = value
            reason = None
            if any(p['symbol'] == t['symbol'] for p in positions.values()):
                reason = 'An existing position already uses this stock'
            elif len(positions) >= settings['max_positions']:
                reason = 'Maximum simultaneous positions reached'
            # Allocation is fixed from initial capital unless reinvestment is selected.
            capital = cash + sum(p['notional'] + p['quantity'] * (prices.get(p['symbol'], p['raw_entry']) - p['entry']) * (1 if p['side'] == 'long' else -1) for p in positions.values()) if settings['reinvest'] else settings['capital']
            budget = min(max(0, cash), max(0, capital) * settings['allocation_pct'] / 100)
            rate = settings['fee_bps'] / 10000
            quantity = int(budget / (t['entry'] * (1 + rate)))
            if t['stop'] is not None:
                risk_per_share = abs(t['entry'] - t['stop']) + t['entry'] * (rate + settings['slippage_bps'] / 10000)
                quantity = min(quantity, int(max(0, capital) * settings['risk_pct'] / 100 / max(.0001, risk_per_share)))
            if quantity < 1:
                reason = reason or 'Insufficient allocated cash for one share and costs'
            if reason:
                skipped.append(dict(t, skipped_reason=reason))
                continue
            notional = quantity * t['entry']
            cost = notional * rate
            cash -= notional + cost
            fees += cost
            p = dict(t, quantity=quantity, notional=notional, costs=cost)
            positions[t['id']] = p
            prices[t['symbol']] = t['raw_entry']
    if last_time:
        equity_point(last_time)
    final = curve[-1]['equity']
    returns = [t['net_return_pct'] for t in ledger]
    gains = [t['net_pnl'] for t in ledger if t['net_pnl'] > 0]
    losses = [t['net_pnl'] for t in ledger if t['net_pnl'] < 0]
    summary = dict(starting_capital=settings['capital'], ending_equity=final, net_profit=round(final - settings['capital'], 2),
                   return_pct=round(100 * (final / settings['capital'] - 1), 4), total_costs=round(fees, 2),
                   trades=len(ledger), wins=len(gains), losses=len(losses), skipped=len(skipped),
                   win_rate=100 * len(gains) / len(ledger) if ledger else None,
                   average_return_pct=statistics.mean(returns) if returns else None,
                   best_trade_pct=max(returns) if returns else None, worst_trade_pct=min(returns) if returns else None,
                   median_holding_days=statistics.median([t['holding_days'] for t in ledger]) if ledger else None,
                   median_holding_bars=statistics.median([t['holding_bars'] for t in ledger]) if ledger else None,
                   max_favorable_pct=max([t['mfe_pct'] for t in ledger], default=None),
                   max_adverse_pct=max([t['mae_pct'] for t in ledger], default=None),
                   max_drawdown_pct=round(max_dd, 4), open_positions=len(positions))
    # Preserve every execution timestamp plus evenly sampled marks for transport.
    times = {t[k] for t in ledger for k in ('entry_time', 'exit_time')}
    stride = max(1, math.ceil(len(curve) / 1800))
    shown = [p for i, p in enumerate(curve) if i % stride == 0 or p['time'] in times or i == len(curve) - 1]
    daily = {}
    for point in curve:daily[point['time'][:10]] = point
    return dict(summary=summary, trades=sorted(ledger, key=lambda t: (t['entry_time'], t['id'])), skipped=skipped, curve=shown, daily_curve=list(daily.values()),
                equity_observations=len(curve), curve_sampled=stride > 1,
                open_positions=list(positions.values()))
