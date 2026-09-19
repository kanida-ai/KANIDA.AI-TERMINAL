from datetime import datetime, timedelta
import unittest
from market_scanner.backtest import Rule
from market_scanner.study_engine import outcome, portfolio, learn_rule


def bars(values, start='2024-01-02 09:15:00'):
    begin = datetime.fromisoformat(start)
    return [dict(time=(begin+timedelta(hours=i)).isoformat(sep=' '),end=(begin+timedelta(hours=i+1)).isoformat(sep=' '),
                 open=v,high=v+1,low=v-1,close=v,volume=100,gap=False) for i,v in enumerate(values)]


def settings(**kwargs):
    return dict(dict(start='2024-01-01',end='2024-12-31',capital=10000,max_positions=1,allocation_pct=100,
                     reinvest=True,risk_pct=100,fee_bps=0,slippage_bps=0,product='CNC',timeframe='1H'),**kwargs)


def event(i=0):
    return dict(signal_index=i,state='setup',atr=2,score=90,episode=i,pattern_start='2024-01-02 09:15:00')


def trade(symbol, bs, cfg=None):
    return dict(outcome(event(),bs,'long',Rule('setup',len(bs)-1),cfg or settings()),symbol=symbol,timeframe='1H',pattern='cup_handle')


def test_shared_cash_rejects_simultaneous_signals_and_ignores_future_return_for_ranking():
    a=bars([100,100,105]);b=bars([100,100,200])
    result=portfolio([trade('A',a),trade('B',b)],{'A|1H':a,'B|1H':b},settings())
    assert [t['symbol'] for t in result['trades']]==['A']
    assert result['summary']['ending_equity']==10500
    assert result['skipped'][0]['symbol']=='B'
    assert all(p['cash']>=0 for p in result['curve'])


def test_open_position_drawdown_is_marked_before_recovery():
    bs=bars([100,100,80,110]);result=portfolio([trade('A',bs)],{'A|1H':bs},settings())
    assert result['summary']['max_drawdown_pct']==20
    assert result['summary']['ending_equity']==11000
    assert any(p['unrealized']==-2000 for p in result['curve'])


def test_whole_shares_and_fees_reconcile():
    cfg=settings(fee_bps=40);bs=bars([3000,3000,3300])
    r=portfolio([trade('A',bs,cfg)],{'A|1H':bs},cfg)
    assert r['trades'][0]['quantity']==3
    assert r['summary']['total_costs']==36
    assert r['summary']['ending_equity']==10864
    assert r['summary']['net_profit']==r['trades'][0]['net_pnl']


def test_unaffordable_trade_is_skipped():
    bs=bars([20000,20000,21000]);r=portfolio([trade('A',bs)],{'A|1H':bs},settings())
    assert not r['trades'] and r['summary']['ending_equity']==10000


def test_delivery_cash_short_is_ineligible():
    assert outcome(event(),bars([100,100,90]),'short',Rule('setup',2),settings())['skipped_reason']


def test_intraday_closes_on_last_available_candle_before_cutoff():
    bs=bars([100]*10);t=outcome(event(),bs,'short',Rule('setup',8),settings(product='MIS'))
    assert t['exit_time']=='2024-01-02 15:15:00'
    assert t['exit_reason']=='session_exit'
    assert not t['overnight']


def test_intraday_rejects_daily_candles():
    assert outcome(event(),bars([100]*4),'short',Rule('setup',2),settings(product='MIS',timeframe='1D'))['skipped_reason']


def test_gap_is_handled_after_entry_not_used_to_skip_it():
    bs=bars([100,100,80,110]);bs[2]['gap']=True
    t=outcome(event(),bs,'long',Rule('setup',3),settings())
    assert t['entry_index']==1 and t['exit_index']==2
    assert t['exit_reason']=='data_gap_first_available_open'


def test_same_bar_stop_target_uses_stop_and_flags_uncertainty():
    bs=bars([100,100,100]);bs[1].update(high=120,low=80)
    t=outcome(event(),bs,'long',Rule('setup',2,1,2),settings())
    assert t['exit_reason']=='stop' and t['exit']==98 and t['excursion_uncertain']


def test_forward_outcome_does_not_change_prior_rule_selection():
    bs=[]
    for i in range(120):
        day=datetime(2024,1,1)+timedelta(days=i)
        bs.append(dict(time=day.strftime('%Y-%m-%d 09:15:00'),end=day.strftime('%Y-%m-%d 15:30:00'),
                       open=100+i,high=102+i,low=99+i,close=101+i,gap=False))
    es=[event(i) for i in range(0,100,5)];rules=[Rule('setup',2),Rule('setup',3)]
    cfg=settings(timeframe='1D')
    first=learn_rule(es,bs,'long',rules,cfg,'2024-03-01','2024-01-01',3,5)
    changed=[dict(b) for b in bs]
    for b in changed:
        if b['time'][:10]>='2024-03-01':b.update(open=1,high=500,low=.1,close=1)
    second=learn_rule(es,changed,'long',rules,cfg,'2024-03-01','2024-01-01',3,5)
    assert first and first==second


def test_small_learning_sample_does_not_invent_a_rule():
    assert learn_rule([event()],bars([100,101,102,103]),'long',[Rule('setup',1)],settings(),'2024-12-01','2024-01-01',1,5) is None


def test_study_end_closes_position_without_resetting_account():
    bs=bars([100,100,105]);t=outcome(event(),bs,'long',Rule('setup',20),settings())
    assert t['exit_reason']=='study_end'
    r=portfolio([dict(t,symbol='A',timeframe='1H',pattern='cup_handle')],{'A|1H':bs},settings())
    assert r['summary']['open_positions']==0 and r['summary']['ending_equity']==10500


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(unittest.FunctionTestCase(fn) for name, fn in globals().items() if name.startswith('test_') and callable(fn))
