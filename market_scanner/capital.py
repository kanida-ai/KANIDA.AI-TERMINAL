"""Cash accounting over frozen trade ledgers, with no new rule selection or fills."""
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP, ROUND_CEILING

D = lambda value: Decimal(str(value))
CENT = Decimal('0.01')
def money(value): return value.quantize(CENT, rounding=ROUND_HALF_UP)


def simulate_account(trades, side, initial='auto'):
    if side not in ('long', 'short'):
        raise ValueError('Unknown trade side')
    trades = sorted(trades, key=lambda t: (t['entry_index'], t['exit_index']))
    previous_exit = -1
    for trade in trades:
        entry, exit_price, cost = D(trade['entry']), D(trade['exit']), D(trade['cost_pct'])
        if not all(v.is_finite() for v in (entry, exit_price, cost)) or min(entry, exit_price) <= 0 or cost < 0:
            raise ValueError('Invalid frozen fill or cost')
        if trade['entry_index'] <= previous_exit or trade['exit_index'] < trade['entry_index']:
            raise ValueError('Cash accounts require a non-overlapping trade ledger')
        previous_exit = trade['exit_index']
    first_need = D(trades[0]['entry']) * (1 + D(trades[0]['cost_pct']) / 100) if trades else D(0)
    if initial == 'auto':
        initial = next((D(n) for n in (10000, 30000, 50000) if D(n) >= first_need),
                       (first_need / 1000).to_integral_value(rounding=ROUND_CEILING) * 1000)
    try:
        start = money(D(initial))
    except Exception:
        raise ValueError('Starting capital must be a finite rupee amount') from None
    if not start.is_finite() or not D(1) <= start <= D(1000000000):
        raise ValueError('Starting capital must be between ₹1 and ₹1,000,000,000')
    balance, fees, peak, drawdown = start, D(0), start, D(0)
    ledger, curve = [], [{'trade': 0, 'time': trades[0]['entry_time'] if trades else None, 'balance': float(start)}]
    wins = executed = 0
    for i, t in enumerate(trades):
        entry, exit_price, rate = D(t['entry']), D(t['exit']), D(t['cost_pct']) / 100
        quantity = max(0, int((balance / (entry * (1 + rate))).to_integral_value(rounding=ROUND_FLOOR)))
        notional, fee = money(entry * quantity), money(entry * quantity * rate)
        if notional + fee > balance and quantity > 0:
            quantity -= 1
            notional, fee = money(entry * quantity), money(entry * quantity * rate)
        gross = money(quantity * (exit_price - entry) * (1 if side == 'long' else -1))
        net, before = gross - fee, balance
        balance += net
        fees += fee
        executed += quantity > 0
        wins += net > 0
        peak = max(peak, balance)
        drawdown = max(drawdown, (peak - balance) / peak * 100)
        row = {key: t.get(key) for key in ('signal_index', 'entry_index', 'exit_index', 'entry_time',
               'exit_candle_start', 'exit_candle_end', 'exit_timing', 'entry', 'exit', 'exit_reason')}
        row.update(trade=i+1, shares=quantity, balance_before=float(before), notional=float(notional),
                   costs=float(fee), reserved_cash=float(before - notional - fee), gross_pnl=float(gross),
                   net_pnl=float(net), balance_after=float(balance),
                   skipped_reason=('Account depleted' if before <= 0 else 'Cannot afford one share plus costs') if not quantity else None)
        ledger.append(row)
        curve.append({'trade': i+1, 'time': t['exit_candle_end'], 'balance': float(balance)})
    return {'starting_capital': float(start), 'ending_capital': float(balance), 'net_profit': float(balance-start),
            'growth_pct': float((balance/start - 1) * 100), 'total_costs': float(fees), 'eligible_trades': len(trades),
            'executed_trades': executed, 'skipped_trades': len(trades)-executed,
            'cash_trade_win_rate': 100*wins/executed if executed else None,
            'closed_trade_drawdown_pct': float(drawdown), 'depleted': balance <= 0,
            'first_share_cost': float(money(first_need)) if trades else None, 'ledger': ledger, 'curve': curve}


def study_account(study, segment='reference', initial='auto'):
    if segment not in ('reference', 'train', 'validation', 'test'):
        raise ValueError('Unknown capital study segment')
    if segment != 'reference' and not study.get('rule'):
        raise ValueError('No validated rule is available for this segment')
    trades = study['reference_trades'] if segment == 'reference' else [t for t in study['trades'] if t['split'] == segment]
    result = simulate_account(trades, study['side'], initial)
    result.update({key: study[key] for key in ('run_id', 'symbol', 'timeframe', 'pattern', 'side')})
    result.update(accounting_version='1.0.0', segment=segment, requested_capital=str(initial), assumptions=study.get('assumptions', {}),
        rule=study['reference'].get('rule') if segment == 'reference' else study['rule'],
        notes=[
            'Each stock × pattern × timeframe × direction is a separate account. Each segment resets the starting cash. These accounts cannot be added into a portfolio.',
            'Auto chooses ₹10,000 / ₹30,000 / ₹50,000, or rounds up to ₹1,000 above that, using only the first eligible entry price plus costs. Future prices and returns do not set starting capital.',
            'Reinvest available cash in whole shares. Reserve the frozen round-trip fee and slippage assumptions on entry notional; deduct them once. No deposits, leverage, or interest. Unaffordable trades are skipped.',
            'Curve and drawdown use settled cash after trades. They omit intra-trade fluctuations and are not mark-to-market drawdown.',
            'The original price fills and eligibility are unchanged. Capital, liquidity, taxes, dividends and corporate-action assumptions limit interpretation; these are hypothetical results.',
            'Shorts use 100% entry-notional collateral; sale proceeds are locked, and losses can exceed collateral. Borrowing, funding, real margin requirements and derivative lot sizes are not modeled.'
            if study['side'] == 'short' else 'Long trades use cash equities with a minimum of one share.'
        ])
    return result
