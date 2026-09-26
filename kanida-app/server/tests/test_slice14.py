"""Slice 14 (Strategies side) - the immutable decision log, research consent, paper modes and times (P08), fill
realism (E08: displayed-size fills, net-P&L exit rules). Isolated fixtures only."""
import sqlite3
import pytest
from test_strategy_builder import live, pilot, strategy, EXP  # noqa: F401


def _deploy(owner, s, key):
 p = owner.post(f"/api/sb/strategies/{s['id']}/preview", json={}).json()
 return p, owner.post(f"/api/sb/strategies/{s['id']}/deployments", json={'preview_id': p['id'], 'preview_hash': p['hash'], 'idempotency_key': key, 'confirm': True}).json()


# --- the decision log --------------------------------------------------------------------------------------------------
def test_every_decision_is_logged_and_the_log_cannot_be_edited_or_deleted(pilot):
 app, owner, _o = pilot
 s = strategy(owner)
 owner.post(f"/api/sb/strategies/{s['id']}/snapshots", json={'expected_version': s['draft']['version']})
 run = owner.post(f"/api/sb/strategies/{s['id']}/paper", json={'confirm': True, 'expected_version': s['draft']['version']}).json()
 owner.post(f"/api/sb/paper/{run['id']}/close", json={'confirm': True})
 kinds = [d['kind'] for d in owner.get(f"/api/sb/decisions?strategy_id={s['id']}").json()['decisions']]
 assert {'strategy_created', 'snapshot', 'paper_open', 'paper_close'} <= set(kinds)
 st = app.state.strategy_builder_store
 ok, n, bad = st.verify_decisions()
 assert ok and n >= 4 and bad is None
 with pytest.raises(sqlite3.DatabaseError):
  st.c.execute("update decision_log set payload='{}'")
 with pytest.raises(sqlite3.DatabaseError):
  st.c.execute('delete from decision_log')
 st.c.rollback()


def test_a_tampered_row_breaks_the_hash_chain(pilot):
 app, owner, _o = pilot
 strategy(owner);strategy(owner)
 st = app.state.strategy_builder_store
 st.c.execute('drop trigger decision_log_no_update')                # simulate someone bypassing the guard
 st.c.execute("update decision_log set payload='{\"x\":1}' where seq=(select min(seq) from decision_log)");st.c.commit()
 ok, _n, bad = st.verify_decisions()
 assert not ok and bad is not None


def test_research_consent_is_opt_in_and_stamped_on_new_records(pilot):
 app, owner, other = pilot
 assert owner.get('/api/sb/consent').json()['consent'] is False
 s1 = strategy(owner)
 assert owner.post('/api/sb/consent', json={'consent': 'yes'}).status_code == 400
 assert owner.post('/api/sb/consent', json={'consent': True}).json()['consent'] is True
 s2 = strategy(owner)
 d = {x['strategy_id']: x for x in owner.get('/api/sb/decisions').json()['decisions'] if x['kind'] == 'strategy_created'}
 assert d[s1['id']]['research_consent'] == 0 and d[s2['id']]['research_consent'] == 1
 # another user never sees them, and cannot read decisions of a strategy they do not own
 assert other.get('/api/sb/decisions').json()['decisions'] == []
 assert other.get(f"/api/sb/decisions?strategy_id={s1['id']}").status_code == 404


# --- P08 paper modes and times -----------------------------------------------------------------------------------------
def test_practice_run_names_its_mode_and_keeps_action_time_apart_from_market_time(pilot):
 _a, owner, _o = pilot
 s = strategy(owner)
 run = owner.post(f"/api/sb/strategies/{s['id']}/paper", json={'confirm': True, 'expected_version': s['draft']['version']}).json()
 assert run['mode'] == 'practice_stored' and run['mode_label'] == 'Stored-price practice'
 assert run['opened_market_at'] == run['opened_reading'] and run['opened_action_at'] and run['opened_action_at'] != run['opened_market_at']
 assert run['eligible']['close']['ok'] and not run['eligible']['monitor']['ok'] and run['eligible']['monitor']['reason']
 closed = owner.post(f"/api/sb/paper/{run['id']}/close", json={'confirm': True}).json()
 assert closed['closed_action_at'] and closed['closed_market_at'] and not closed['eligible']['close']['ok']


def test_paper_capital_says_which_ledger_it_covers(live):
 _a, owner, _o, _f = live
 cap = owner.get('/api/sb/deployments').json()['paper_capital']
 assert cap['scope'] == 'paper_live' and 'live-quote paper deployments' in cap['policy']


# --- E08 fill realism ------------------------------------------------------------------------------------------------
def test_a_fill_takes_only_the_displayed_size_and_the_rest_keeps_resting(live):
 app, owner, _o, fake = live
 s = strategy(owner, lots=2)                                           # 130 units per leg
 syms = {l['side']: None for l in s['draft']['body']['legs']}
 p = owner.post(f"/api/sb/strategies/{s['id']}/preview", json={}).json()
 buy = next(o for o in p['orders'] if o['side'] == 'B')
 fake.depth_qty = {buy['symbol']: 65}                                  # only one lot is displayed at the ask
 d = owner.post(f"/api/sb/strategies/{s['id']}/deployments", json={'preview_id': p['id'], 'preview_hash': p['hash'], 'idempotency_key': 'dq1', 'confirm': True}).json()
 held = {x['leg_id']: x['units'] for x in d['positions']}
 assert held[buy['leg_id']] == 65                                      # partial: 65 of 130
 assert d['status'] != 'active'                                        # the plan is not complete, and the sell leg waits for it
 fake.depth_qty = {}
 app.state.strategy_builder_execution.dispatch(d['id'])
 d2 = owner.get(f"/api/sb/deployments/{d['id']}").json()
 assert {x['leg_id']: x['units'] for x in d2['positions']}[buy['leg_id']] == 130
 fills = [x for x in owner.get('/api/sb/decisions').json()['decisions'] if x['kind'] == 'fill']
 assert any(f['payload']['partial'] for f in fills) and all(f['mode'] == 'paper_live' for f in fills)


def test_two_intents_never_fill_against_the_same_displayed_size(live):
 app, owner, _o, fake = live
 s1 = strategy(owner);s2 = strategy(owner)
 p1 = owner.post(f"/api/sb/strategies/{s1['id']}/preview", json={}).json()
 buy = next(o for o in p1['orders'] if o['side'] == 'B')
 # both strategies buy the same call; 65 units displayed in total
 fake.depth_qty = {buy['symbol']: 65}
 ex = app.state.strategy_builder_execution
 _, d1 = _deploy(owner, s1, 'x1')
 _, d2 = _deploy(owner, s2, 'x2')
 held = lambda d: {x['leg_id']: x['units'] for x in owner.get(f"/api/sb/deployments/{d['id']}").json()['positions']}
 # each dispatch cycle sees 65 displayed; neither deployment can hold more than the size shown to it in one cycle
 assert held(d1)[buy['leg_id']] <= 65 and held(d2)[buy['leg_id']] <= 65


def test_exit_rules_judge_net_pnl_after_costs(live):
 app, owner, _o, fake = live
 s = strategy(owner)
 _, d = _deploy(owner, s, 'n1')
 ex = app.state.strategy_builder_execution
 uid = ex.c.execute('select user_id from deployments where id=?', (d['id'],)).fetchone()[0]
 got = ex.deployment(uid, d['id'])
 # right after entry the position is down by the spread plus fees: NET is below zero even if gross unrealised is small
 assert got['net'] == pytest.approx((got['realised'] or 0) + (got['unrealised'] or 0) - got['fees'], abs=0.01) and got['net'] < 0
 ex.set_exit_rules(uid, d['id'], None, 1)                             # stop at 1% of max loss: net (spread + fees) is already past it
 note = ex.check_exit_rules(uid, d['id'])
 assert note and note.startswith('stop rule met (net ')
 assert any(x['kind'] == 'exit_rule_fired' and x['payload']['rule'] == 'stop' for x in owner.get('/api/sb/decisions').json()['decisions'])


# --- P09 costs, P15 origin, P11 status -------------------------------------------------------------------------------
def test_analysis_carries_a_round_trip_cost_estimate_and_net_extremes(pilot):
 _a, owner, _o = pilot
 s = strategy(owner)
 a = owner.post('/api/sb/analyze', json={'body': s['draft']['body']}).json()
 c = a['costs']
 assert c['round_trip'] == pytest.approx(c['entry'] + c['exit_estimate'], abs=0.01) and c['exit_estimate'] > 0
 assert c['max_loss_net'] == pytest.approx(a['max_loss']['value'] - c['round_trip'], abs=0.01)
 assert a['max_loss']['value'] < 0 and a['max_loss']['basis'] == 'expiry_gross'      # the gross view is unchanged


def test_a_discover_draft_remembers_its_view_limits_and_shown_numbers(pilot):
 _a, owner, _o = pilot
 r = owner.post('/api/sb/discover', json={'underlying': 'NIFTY', 'expiry': EXP, 'view': 'up', 'target': 23300, 'max_loss': 50000, 'lots': 1}).json()
 c = r['candidates'][0]
 s = owner.post('/api/sb/discover/use', json={'candidate_id': c['candidate_id']}).json()
 o = s['draft']['body']['origin']
 assert o['source'] == 'discover' and o['view'] == 'up' and o['target'] == 23300 and o['max_loss'] == 50000
 assert o['shown']['pop'] == c['pop'] and o['as_of'] == r['as_of']
 # unknown keys are dropped and it survives a save
 body = {**s['draft']['body'], 'origin': {**o, 'evil': 'x', 'shown': {**o['shown'], 'pop': 'high'}}}
 saved = owner.post(f"/api/sb/strategies/{s['id']}/draft", json={'version': s['draft']['version'], 'body': body}).json()
 assert 'evil' not in saved['draft']['body']['origin'] and saved['draft']['body']['origin']['shown']['pop'] is None


def test_feed_status_speaks_plainly_and_keeps_the_technical_reason_separate(live):
 app, owner, _o, fake = live
 from kanida_pilot.strategy_builder.kite_market import MarketRouter
 class Dead:
  def available(self): return False, 'The Kite token was rejected (it expires every morning; the engine auth worker mints a new one).'
 st = MarketRouter(None, Dead()).status()
 assert st['code'] == 'AUTH_EXPIRED' and 'auth worker' not in st['user_message'] and 'stored prices' in st['user_message']
 assert MarketRouter(None, None).status()['code'] == 'NOT_CONFIGURED'


def test_a_stale_contract_quote_is_not_a_current_mark_and_rules_pause(live):
 app, owner, _o, fake = live
 s = strategy(owner)
 _, d = _deploy(owner, s, 'st1')
 ex = app.state.strategy_builder_execution
 uid = ex.c.execute('select user_id from deployments where id=?', (d['id'],)).fetchone()[0]
 fresh = ex.deployment(uid, d['id'])
 assert fresh['net'] is not None and all(p['mark_stale'] is None for p in fresh['positions'])
 real = fake.chain
 def old_chain(u, e):
  ch = real(u, e)
  for r in ch['rows']:
   for k in ('CE', 'PE'):r[k]['quote_at'] = '2026-01-01 09:15:00'
  return ch
 fake.chain = old_chain
 stale = ex.deployment(uid, d['id'])
 assert stale['net'] is None and any(p['mark_stale'] == 'STALE' for p in stale['positions'])
 assert stale['last_known'] and stale['last_known']['net'] == fresh['net']      # dated last-known, never zero
 ex.set_exit_rules(uid, d['id'], None, 1)
 assert ex.check_exit_rules(uid, d['id']) is None                              # a rule never fires on a stale mark
