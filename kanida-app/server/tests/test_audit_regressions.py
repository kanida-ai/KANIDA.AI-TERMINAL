"""Desired GTM invariants, intentionally failing on the audited build.

Uses the existing isolated temporary-DB / fake-market fixtures. No live API or
real account mutations. Run from kanida-app with PYTHONPATH=server:server/tests.
These tests belong to the audit, not the application's implementation suite.
"""
from datetime import timedelta
from test_strategy_builder import live, strategy, EXP
from kanida_pilot.strategy_builder import execution as EX
from kanida_pilot.strategy_builder import alerts as AL
from kanida_pilot.strategy_builder import service as S


def test_crossed_quote_blocks_preview(live):
    app, owner, other, fake = live
    s = strategy(owner)
    fake.books['NIFTY26SEP23000CE'] = (160.0, 150.0)
    p = owner.post(f"/api/sb/strategies/{s['id']}/preview", json={}).json()
    assert not p['can_submit'], 'A crossed bid 160 > ask 150 was accepted'


def test_stale_live_source_suppresses_alert(live, monkeypatch):
    app, owner, other, fake = live
    al = app.state.strategy_builder_alerts
    al.stop()
    monkeypatch.setattr(AL, 'market_open', lambda at=None: True)
    monkeypatch.setattr(fake, '_now', lambda: (EX.now_ist()-timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S'))
    s = strategy(owner)
    r = owner.post(f"/api/sb/strategies/{s['id']}/alerts", json={
        'type': 'price_cross', 'params': {'level': 22990, 'direction': 'above'}
    }).json()
    assert not r['now'].get('available'), 'Two-hour-old data was available to an alert'


def test_entry_cost_does_not_change_current_market_delta(live):
    app, owner, other, fake = live
    s = strategy(owner, 'long_call')
    body = s['draft']['body']
    market = app.state.strategy_builder_market
    a = S.analysis(market, body)
    changed = {**body, 'legs': [{**l, 'price_basis': 'manual', 'price': 300.0} for l in body['legs']]}
    b = S.analysis(market, changed)
    assert a['greeks']['delta'] == b['greeks']['delta'], (
        f"Same contracts and market, different cost basis: delta {a['greeks']['delta']} -> {b['greeks']['delta']}"
    )
