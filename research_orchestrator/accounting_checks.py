"""Required numerical checks that remain active under python -O."""
import math


def verify_account_strict(account, spec):
    summary = account['summary'];trades = account['trades']
    def require(condition, message):
        if not condition:
            raise ValueError('Accounting check failed: ' + message)
    require(summary['open_positions'] == 0, 'positions remain open after the test boundary')
    for point in account['daily_curve']:
        require(math.isfinite(point['cash']) and point['cash'] >= -.01, 'invalid cash or borrowing')
        require(math.isfinite(point['equity']), 'non-finite equity')
        require(0 <= point['positions'] <= spec['max_positions'], 'position limit')
    gross = sum(t['quantity'] * (t['exit'] - t['entry']) for t in trades)
    fees = sum(t['costs'] for t in trades)
    require(abs(summary['ending_equity'] - (spec['capital'] + gross - fees)) <= .011, 'equity does not reconcile')
    require(abs(summary['total_costs'] - fees) <= .011, 'fees do not reconcile')
    for trade in trades:
        require(trade['quantity'] >= 1 and int(trade['quantity']) == trade['quantity'], 'non-whole share quantity')
        require(trade['signal_time'] < trade['entry_time'] <= trade['exit_time'], 'signal/entry/exit ordering')
        require(spec['start'] <= trade['entry_time'][:10] <= trade['exit_time'][:10] <= spec['end'], 'out-of-period trade')
