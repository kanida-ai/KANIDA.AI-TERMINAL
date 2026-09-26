"""Slice 14 - derivatives-engine integrity gates (research/derivatives-intelligence-2026-09-26/01-engine-audit.md).

One block per gate: E03 fresh/legacy schema reader, E07 effective-dated STT, E05 pricing boundaries and IV validity,
E04 bar-end time alignment and matched-cohort OI, E06 PCR/max-pain completeness, E10 control and episodes, interpretation
snapshot scope (two expiries, additive migration) and E01 vendor-token reuse. Temp fixtures only - no real store.
"""
import json
import math
import random
import sqlite3
from datetime import date, datetime
from types import SimpleNamespace

import pytest

from market_data.derivatives import metrics as M
from market_data.derivatives.instruments import Contract
from market_data.derivatives.store import DerivativesStore
from kanida_pilot import implied_vol as IV
from kanida_pilot.snapshots import SnapshotStore
from kanida_pilot.signal_noise import SignalNoise
from kanida_pilot.strategy_builder import analytics as A, charges as CH, evidence as EV, lab as LB
from kanida_pilot.strategy_builder.market import Market


# ======================================================================================================== E03 schema
def _fresh_store(tmp_path):
    path = tmp_path / 'fresh.db'
    with DerivativesStore(path) as d:
        d.sync_contracts([Contract(11, 'NIFTY26SEP23000CE', 'NIFTY', 'CE', 23000, date(2026, 9, 29), 65),
                          Contract(12, 'NIFTY26SEP23000PE', 'NIFTY', 'PE', 23000, date(2026, 9, 29), 65)],
                         in_scope_tokens=[11, 12])
        d.con.execute("insert into underlying_snapshots(underlying,captured_at,spot,vendor_id,fetched_at,snapshot_id) "
                      "values('NIFTY','2026-09-25 15:30:00',23010,'fixture','2026-09-25','fx')")
        for tok, px in ((11, 120.0), (12, 105.0)):
            d.con.execute("insert into snapshots(instrument_token,captured_at,last_price,oi,volume,vendor_id) "
                          "values(?,?,?,?,?,'fixture')", (tok, '2026-09-25 15:30:00', px, 1000, 50))
    return path


def test_e03_fresh_capture_schema_reads_with_bid_ask_unavailable(tmp_path):
    path = _fresh_store(tmp_path)
    cols = {r[1] for r in sqlite3.connect(path).execute('pragma table_info(snapshots)')}
    assert 'bid' not in cols and 'ask' not in cols          # the fresh schema really has no book columns
    m = Market(str(path))
    try:
        ch = m.chain('NIFTY', '2026-09-29')
    finally:
        m.close()
    row = ch['rows'][0]
    for side in ('CE', 'PE'):
        assert row[side]['bid'] is None and row[side]['ask'] is None       # unavailable, never fabricated
        assert 'no_bid_ask' in row[side]['flags']
        assert row[side]['ltp'] is not None
    assert ch['tick_size'] is not None


def test_e03_legacy_schema_still_serves_its_book(tmp_path):
    path = tmp_path / 'legacy.db'
    c = sqlite3.connect(path)
    c.executescript('''
     create table contracts(instrument_token integer primary key, tradingsymbol text, underlying text, instrument_type text,
      strike real, expiry text, lot_size integer, tick_size real);
     create table snapshots(instrument_token integer, captured_at text, last_price real, bid real, ask real, oi integer,
      volume integer, last_trade_time text);
     create table underlying_snapshots(underlying text, captured_at text, spot real);''')
    c.execute("insert into contracts values(1,'NIFTY26SEP23000CE','NIFTY','CE',23000,'2026-09-29',65,0.05)")
    c.execute("insert into snapshots values(1,'2026-09-25 15:30:00',120,119.5,120.5,1000,50,'2026-09-25 15:29:50')")
    c.execute("insert into underlying_snapshots values('NIFTY','2026-09-25 15:30:00',23010)")
    c.commit(); c.close()
    m = Market(str(path))
    try:
        side = m.chain('NIFTY', '2026-09-29')['rows'][0]['CE']
    finally:
        m.close()
    assert side['bid'] == 119.5 and side['ask'] == 120.5 and 'no_bid_ask' not in side['flags']


# ======================================================================================================== E07 costs
def test_e07_stt_is_effective_dated_from_primary_sources():
    s = CH._SCHEDULE
    for kind in ('option_sale', 'option_exercise', 'futures_sale'):
        for e in s[kind]['history']:
            assert e['source'] in s['sources'] and s['sources'][e['source']]['url'].startswith('https://')
            assert e['primary'] is True
    assert CH.option_sale_stt_rate('2026-03-31') == pytest.approx(0.001)
    assert CH.option_sale_stt_rate('2026-04-01') == pytest.approx(0.0015)
    assert CH.option_sale_stt_rate('2024-09-30') == pytest.approx(0.000625)
    assert CH.option_sale_stt_rate('2024-10-01') == pytest.approx(0.001)
    assert CH.option_sale_stt_rate('2023-03-31') == pytest.approx(0.0005)
    assert CH.option_sale_stt_rate('2016-05-31') == pytest.approx(0.00017)
    assert CH.option_sale_stt_rate('2016-06-01') == pytest.approx(0.0005)
    r = CH.stt_rates('2025-06-02')
    assert r['futures_sale']['rate'] == pytest.approx(0.0002) and r['option_exercise']['rate'] == pytest.approx(0.00125)
    assert CH.futures_stt(1_000_000, '2026-04-01') == pytest.approx(500.0)
    assert CH.futures_stt(1_000_000, '2023-03-31') == pytest.approx(100.0)


def test_e07_leg_charges_use_the_trade_date_and_default_to_today():
    assert CH.leg_charges('S', 100, 1000, '2026-03-31')['stt'] == pytest.approx(100.0)
    assert CH.leg_charges('S', 100, 1000, date(2026, 4, 1))['stt'] == pytest.approx(150.0)
    assert CH.leg_charges('S', 100, 1000, datetime(2026, 4, 1, 9, 15))['stt'] == pytest.approx(150.0)
    assert CH.leg_charges('B', 100, 1000, '2026-04-01')['stt'] == 0.0
    assert CH.leg_charges('S', 100, 1000)['stt'] == pytest.approx(150.0)             # today (Sep 2026)
    # the old positional `rates` slot still works
    assert CH.leg_charges('S', 100, 1000, {**CH.RATES, 'stt_sell': 0.002})['stt'] == pytest.approx(200.0)
    est = CH.estimate([{'side': 'S', 'price': 100, 'lots': 1, 'lot_size': 1000}], trade_date='2025-01-02')
    assert est['stt'] == pytest.approx(100.0) and est['version'] == CH.VERSION and est['stt_as_of'] == '2025-01-02'


def test_e07_exercise_stt_uses_the_rate_and_base_in_force():
    assert CH.exercise_stt(10_000, '2026-03-26') == pytest.approx(12.5)
    assert CH.exercise_stt(10_000, '2026-04-30') == pytest.approx(15.0)
    assert CH.exercise_stt(0, '2026-04-30') == 0.0
    # before 1 Sep 2019 exercise STT was on the settlement price (notional), not intrinsic: never substituted
    with pytest.raises(CH.STTScheduleGap):
        CH.exercise_stt(10_000, '2018-06-28')
    assert CH.exercise_stt(10_000, '2018-06-28', notional_value=1_000_000) == pytest.approx(1250.0)
    with pytest.raises(CH.STTScheduleGap):
        CH.option_sale_stt_rate('2015-12-31')                                      # outside verified coverage


def test_e07_lab_refuses_a_period_before_the_verified_schedule_and_supersedes_old_fee_runs():
    lab = LB.Lab.__new__(LB.Lab)
    lab.cal = SimpleNamespace(available=lambda: False, listed=lambda u: False)
    lab.stocks = lambda: set()
    with pytest.raises(LB.LabError) as err:
        LB.Lab.validate(lab, {'template': 'bull_call_spread', 'param': 4, 'from': '2015-06-01', 'to': '2020-01-01'})
    assert err.value.code == 'COST_SCHEDULE_GAP'
    assert LB.fees_superseded('fo-options-2026-09-estimate') and LB.fees_superseded(None)
    assert not LB.fees_superseded(CH.VERSION)
    assert EV.EVIDENCE_VERSION.endswith(':fees')
    spec = {'split': '2024-01-01', 'template': 'bull_call_spread', 'param': 4, 'weekday': 2, 'dte_min': 1, 'dte_max': 7,
            'from': '2019-01-01', 'to': '2026-01-01'}
    e = EV.entry('s14-old', spec, {'trades': [], 'manifest': {'fees': 'fo-options-2026-09-estimate'}}, 0.0)
    assert e['fees'] == 'fo-options-2026-09-estimate' and LB._superseded(e)
    e2 = EV.entry('s14-new', spec, {'trades': [], 'manifest': {'fees': CH.VERSION}}, 0.0)
    assert not LB._superseded(e2)


def test_e07_lab_trade_costs_follow_the_trade_date():
    """The same short leg sold before and after 1 Apr 2026 pays the rate of ITS day, not today's constant."""
    before = CH.leg_charges('S', 50.0, 75, '2026-03-30')
    after = CH.leg_charges('S', 50.0, 75, '2026-04-02')
    assert after['stt'] / before['stt'] == pytest.approx(1.5)
    assert 'CH.exercise_stt(' in open(LB.__file__).read() and '0.00125' not in open(LB.__file__).read()


# ======================================================================================================== E05 pricing
def test_e05_zero_vol_positive_time_is_the_discounted_strike_value():
    r = A.RATE
    assert A.bs_price(100, 100, 1, 0, 'CE') == pytest.approx(100 - 100 * math.exp(-r))
    assert A.bs_price(100, 100, 1, 0, 'PE') == 0.0
    assert A.bs_price(90, 100, 1, 0, 'PE') == pytest.approx(100 * math.exp(-r) - 90)
    assert A.bs_price(100, 100, 0, 0.2, 'CE') == 0.0                 # settlement stays intrinsic against spot
    assert A.bs_price(110, 100, 0, 0.2, 'CE') == 10.0
    g = A.bs_greeks(100, 100, 1, 0, 'CE')
    assert g['delta'] == 1.0 and g['gamma'] == 0.0 and g['vega'] == 0.0
    # theta = value lost per calendar day as time passes, against a finite difference of the deterministic value
    fd = A.bs_price(100, 100, 1 - 1 / 365, 0, 'CE') - A.bs_price(100, 100, 1, 0, 'CE')
    assert g['theta'] == pytest.approx(fd, rel=1e-3)
    assert A.bs_greeks(100, 100, 1, 0, 'PE')['delta'] == 0.0
    assert A.bs_greeks(90, 100, 1, 0, 'PE')['delta'] == -1.0


@pytest.mark.parametrize('field,args', [
    ('price', (float('nan'), 100, 100, .2)), ('price', (float('inf'), 100, 100, .2)),
    ('spot', (5.0, float('nan'), 100, .2)), ('strike', (5.0, 100, float('inf'), .2)),
    ('years', (5.0, 100, 100, float('nan')))])
def test_e05_non_finite_inputs_are_rejected_with_a_reason(field, args):
    out = IV.solve(*args, 'CE', sensitivity=False)
    assert out['iv'] is None and out['reason'] == 'non_finite_input' and out['invalid_input'] == field
    assert IV.REASONS['non_finite_input']


def test_e05_expiry_morning_solves_on_actual_time_and_after_settlement_does_not():
    t = 6 / (365 * 24)
    px = IV.price_bs(100, 100, t, .065, .2, 'CE')
    out = IV.solve(px, 100, 100, t, 'CE', days_to_expiry=0, sensitivity=False)
    assert out['iv'] == pytest.approx(.2, abs=1e-6) and out['expiry_day'] is True
    assert IV.solve(px, 100, 100, 0.0, 'CE', days_to_expiry=0)['reason'] == 'expiry_today'
    assert IV.solve(px, 100, 100, -0.001, 'CE', days_to_expiry=0)['reason'] == 'expiry_today'


def test_e05_trade_timestamp_quality_is_explicit():
    px = IV.price_bs(100, 100, .1, .065, .2, 'CE')
    assert IV.solve(px, 100, 100, .1, 'CE', sensitivity=False)['trade_time_quality'] == 'unknown'
    assert IV.solve(px, 100, 100, .1, 'CE', seconds_since_last_trade=60, sensitivity=False)['trade_time_quality'] == 'current'
    assert IV.solve(px, 100, 100, .1, 'CE', seconds_since_last_trade=-60, sensitivity=False)['iv'] is not None  # capture lag
    fut = IV.solve(px, 100, 100, .1, 'CE', seconds_since_last_trade=-3600, sensitivity=False)
    assert fut['iv'] is None and fut['reason'] == 'future_last_trade' and fut['trade_time_quality'] == 'future'
    nan_age = IV.solve(px, 100, 100, .1, 'CE', seconds_since_last_trade=float('nan'), sensitivity=False)
    assert nan_age['trade_time_quality'] == 'unknown'


def test_e05_dividend_explanation_has_the_right_sign():
    text = IV.ASSUMPTIONS['dividend']
    assert 'LOWER on calls' in text and 'HIGHER on puts' in text
    assert 'slightly high on calls' not in text


# ======================================================================================================== E04 time/OI
def test_e04_volume_baseline_counts_only_bars_that_have_ended():
    bars = [{'bar_start': '2026-09-24 09:15:00', 'volume': 100}, {'bar_start': '2026-09-24 09:30:00', 'volume': 200}]
    assert M.cumulative_by_time_of_day(bars, cutoff=datetime(2026, 9, 25, 9, 30)) == [100]
    assert M.cumulative_by_time_of_day(bars, cutoff=datetime(2026, 9, 25, 9, 45)) == [300]
    # 5-minute bars: the interval is read from the data, so the 09:25 bar (ends 09:30) counts at 09:30
    five = [{'bar_start': f'2026-09-24 09:{m:02d}:00', 'volume': 10} for m in (15, 20, 25, 30)]
    assert M.cumulative_by_time_of_day(five, cutoff=datetime(2026, 9, 25, 9, 30)) == [30]
    # sparse bars never stretch a bar beyond 15 minutes
    sparse = [{'bar_start': '2026-09-24 09:15:00', 'volume': 1}, {'bar_start': '2026-09-24 11:00:00', 'volume': 2}]
    assert M.cumulative_by_time_of_day(sparse, cutoff=datetime(2026, 9, 25, 11, 15)) == [3]


def test_e04_sql_baseline_uses_bar_end(tmp_path):
    c = sqlite3.connect(tmp_path / 'b.db'); c.row_factory = sqlite3.Row
    c.execute('create table candles_15m(instrument_token integer, bar_start text, volume integer)')
    c.executemany('insert into candles_15m values(1,?,?)', [('2026-09-24 09:15:00', 100), ('2026-09-24 09:30:00', 200)])
    assert M.load_tod_baselines(c, datetime(2026, 9, 25, 9, 30))[1] == [100.0]


def _cm(kind, strike, oi, oi_change, prev_oi=None):
    return SimpleNamespace(contract=SimpleNamespace(is_option=True, instrument_type=kind, strike=strike),
                           oi=oi, volume=10, buildup_day=SimpleNamespace(oi_change=oi_change),
                           volume_to_oi=SimpleNamespace(prev_day_oi=prev_oi), premium=M.PremiumTraded(status=M.STATUS_NO_DATA),
                           unusual=False, spot=100.0, captured_at=datetime(2026, 9, 25, 10, 0))


def test_e04_underlying_oi_change_is_a_matched_cohort_with_births_and_deaths_apart():
    legs = [_cm('CE', 100, 1100, 100, 1000),           # matched: 1000 -> 1100
            _cm('PE', 100, 900, -100, 1000),           # matched: 1000 -> 900
            _cm('CE', 110, 5000, None),                # birth: no prior close
            _cm('PE', 90, None, None, 700)]            # death: prior close, no current OI
    r = M.roll_up_underlying('NIFTY', legs, captured_at=datetime(2026, 9, 25, 10, 0), spot=100.0)
    assert r.oi_change_pct_day == pytest.approx(0.0)   # was +150% when the birth was summed into "now"
    assert (r.oi_matched_now, r.oi_matched_before, r.oi_matched_legs) == (2000, 2000, 2)
    assert (r.oi_births, r.oi_births_oi) == (1, 5000)
    assert (r.oi_deaths, r.oi_deaths_prior_oi) == (1, 700)


# ======================================================================================================== E06 metrics
def test_e06_missing_side_is_never_a_zero_pcr():
    r = M.put_call_ratio([{'instrument_type': 'CE', 'strike': 100, 'oi': 10, 'volume': 5},
                          {'instrument_type': 'PE', 'strike': 100, 'oi': None, 'volume': None}])
    assert r.pcr_oi is None and r.pcr_volume is None and r.status == M.STATUS_INCOMPLETE
    assert r.pe_oi_missing == 1 and r.oi_complete is False and r.pcr_oi_status == M.STATUS_INCOMPLETE
    r = M.put_call_ratio([{'instrument_type': 'CE', 'strike': 100, 'oi': 10, 'volume': 5}])
    assert r.pcr_oi is None and r.status != M.STATUS_OK                      # no put leg at all
    ok = M.put_call_ratio([{'instrument_type': 'CE', 'strike': 100, 'oi': 10, 'volume': 5},
                           {'instrument_type': 'PE', 'strike': 100, 'oi': 0, 'volume': 5}])
    assert ok.status == M.STATUS_OK and ok.pcr_oi == 0.0 and ok.oi_complete   # a REAL zero put side is a zero


def test_e06_max_pain_reports_ties_coverage_and_matches_brute_force():
    flat = {100.0: {'ce_oi': 0, 'pe_oi': 0}, 110.0: {'ce_oi': 10, 'pe_oi': 10}, 120.0: {'ce_oi': 0, 'pe_oi': 0}}
    mp = M.max_pain(flat, spot=118)
    assert mp.tied_strikes == (110.0,) and mp.tie_low == mp.tie_high == 110.0
    tie = M.max_pain({100.0: {'ce_oi': 0, 'pe_oi': 5}, 110.0: {'ce_oi': 0, 'pe_oi': 0}, 120.0: {'ce_oi': 5, 'pe_oi': 0}}, spot=119)
    assert tie.tied_strikes == (100.0, 110.0, 120.0) and (tie.tie_low, tie.tie_high) == (100.0, 120.0) and tie.strike == 120.0
    part = M.max_pain([{'strike': 100, 'instrument_type': 'CE', 'oi': 5}, {'strike': 100, 'instrument_type': 'PE', 'oi': None}])
    assert part.status == M.STATUS_INCOMPLETE and part.oi_missing == 1 and part.legs == 2
    g = random.Random(14)
    for _ in range(25):
        book = {float(k): {'ce_oi': g.randint(0, 9) * 75, 'pe_oi': g.randint(0, 9) * 75} for k in range(20000, 20000 + 50 * g.randint(2, 40), 50)}
        mp = M.max_pain(book)
        if mp.status != M.STATUS_OK:
            continue
        for s, v in mp.payout_by_strike.items():
            brute = sum(r['ce_oi'] * max(0, s - k) + r['pe_oi'] * max(0, k - s) for k, r in book.items())
            assert v == pytest.approx(brute, rel=1e-12, abs=1e-6)


def test_e06_flow_rows_state_the_observation_and_that_the_trader_is_unknown():
    from kanida_pilot import derivatives as D
    for axes in ('down|building', 'up|building', 'up|unwinding', 'down|unwinding'):
        assert D.FLOW_ATTRIBUTIONS[axes].startswith('Consistent with') and 'who traded is not known' in D.FLOW_ATTRIBUTIONS[axes]
    assert set(D.FLOW_OBSERVATIONS) == {f'{p}|{o}' for p in ('up', 'down', 'flat') for o in ('building', 'unwinding', 'flat')}


# ======================================================================================================== E10 controls
class _DB:
    def __init__(self, rows): self.rows = rows
    def execute(self, *a): return self
    def fetchall(self): return self.rows


def _rec(u, at, state='building', row='calls', beh='writing'):
    return {'underlying': u, 'reading_at': at, 'original_state': state, 'row': row, 'behaviour': beh}


def test_e10_control_is_never_the_instrument_itself_and_n_counts_episodes():
    follow = {'_status': 'ok', 'state': 'building', 'row': 'calls', 'behaviour': 'writing'}

    class One(SignalNoise):
        def _db(self): return _DB([_rec('NIFTY', '2026-09-25 10:00:00')])
        def _snaps(self, *a): return {'NIFTY': []}
        def _follow(self, *a): return follow
    assert One('x').baseline('2026-09-25', 'e') == (None, 0)

    seen = []

    class Two(SignalNoise):
        def _db(self):
            return _DB([_rec('NIFTY', f'2026-09-25 10:{m:02d}:00') for m in (0, 15, 30)]     # one episode, 3 rows
                       + [_rec('NIFTY', '2026-09-25 10:45:00', row='puts')]                 # a new episode
                       + [_rec('BANKNIFTY', '2026-09-25 10:00:00')])
        def _snaps(self, *a): return {'NIFTY': ['n'], 'BANKNIFTY': ['b']}
        def _follow(self, series, *a):
            seen.append(series); return follow
    d = Two('x').baseline_detail('2026-09-25', 'e')
    assert d['episodes'] == 3 and d['rows'] == 5 and d['unit'] == 'episodes' and d['sample'].startswith('exploratory')
    # BANKNIFTY's control is NIFTY's series and every NIFTY record's control is BANKNIFTY's - never its own
    assert seen == [['n'], ['b'], ['b']]


# ======================================================================================================== snapshot scope
def _snap(**kw):
    base = {'session': '2026-09-25', 'reading_at': '2026-09-25 10:00:00', 'underlying': 'NIFTY', 'status': 'ok',
            'engine_version': 'e1', 'rules_version': '1', 'created_at': 'now', 'expiry': '2026-09-29'}
    return {**base, **kw}


def test_snapshots_keep_two_expiries_and_two_rule_versions(tmp_path):
    st = SnapshotStore(tmp_path / 's.db')
    st.insert(_snap()); st.insert(_snap(expiry='2026-10-06')); st.insert(_snap(rules_version='2'))
    st.insert(_snap())                                            # same scope again: ignored, never updated
    assert st._db().execute('select count(*) from reading_snapshots').fetchone()[0] == 3
    st.insert(_snap(reading_at='2026-09-25 10:15:00', expiry='2026-10-06'))
    prev = st.previous('NIFTY', '2026-09-25', '2026-09-25 10:30:00', 'e1', expiry='2026-09-29')
    assert prev['expiry'] == '2026-09-29'                          # a chain never crosses expiries


def test_snapshot_scope_migration_is_additive_and_verbatim(tmp_path):
    path = tmp_path / 'old.db'
    c = sqlite3.connect(path)
    c.executescript('''create table reading_snapshots(id integer primary key, session text not null, reading_at text not null,
      underlying text not null, expiry text, spot real, atm_strike real, status text not null, reason text, contracts text,
      reading text, chained text, previous_snapshot_id integer, engine_version text not null, rules_version text not null,
      created_at text not null, unique(underlying, reading_at, engine_version));
      create index reading_snapshots_session on reading_snapshots(session, underlying, reading_at);''')
    rows = [(7, '2026-09-24', '2026-09-24 10:00:00', 'NIFTY', '2026-09-29', 23000.0, 23000.0, 'ok', None,
             json.dumps([{'k': 1}]), json.dumps({'headline': 'x'}), json.dumps({'h': 1}), None, 'e1', '1', 't0'),
            (9, '2026-09-24', '2026-09-24 10:15:00', 'NIFTY', '2026-09-29', 23010.0, 23000.0, 'ok', None,
             '[]', '{}', '{}', 7, 'e1', '1', 't1')]
    c.executemany('insert into reading_snapshots values(' + ','.join('?' * 16) + ')', rows)
    c.commit(); c.close()
    st = SnapshotStore(path)
    db = st._db()
    got = [tuple(r) for r in db.execute('select * from reading_snapshots order by id')]
    assert got == rows                                             # same ids, same bytes
    assert [tuple(r) for r in db.execute('select * from reading_snapshots_v1 order by id')] == rows   # old table kept
    st.insert(_snap(session='2026-09-24', reading_at='2026-09-24 10:00:00', expiry='2026-10-06'))
    assert db.execute('select count(*) from reading_snapshots').fetchone()[0] == 3
    st2 = SnapshotStore(path)                                      # idempotent: a second open migrates nothing
    assert st2._db().execute('select count(*) from reading_snapshots').fetchone()[0] == 3


# ======================================================================================================== E01 identity
def test_e01_token_reuse_keeps_the_old_identity_and_resolves_point_in_time(tmp_path):
    with DerivativesStore(tmp_path / 'd.db') as d:
        old = Contract(1, 'NIFTY26SEP100CE', 'NIFTY', 'CE', 100, date(2026, 9, 29), 65)
        new = Contract(1, 'NIFTY26OCT200PE', 'NIFTY', 'PE', 200, date(2026, 10, 27), 65)
        d.sync_contracts([old], in_scope_tokens=[1], seen_at='2026-09-01T03:00:00')
        d.con.execute("insert into snapshots(instrument_token,captured_at,last_price,vendor_id) values(1,'2026-09-25 10:00:00',5,'k')")
        d.sync_contracts([old], in_scope_tokens=[1], seen_at='2026-09-02T03:00:00')
        assert d.con.execute('select count(*) from contract_token_history').fetchone()[0] == 0   # a re-sync is not a reuse
        d.sync_contracts([new], in_scope_tokens=[1], seen_at='2026-09-30T03:00:00')
        kept = d.con.execute('select * from contracts where tradingsymbol=?', (old.tradingsymbol,)).fetchall()
        assert len(kept) == 1 and kept[0]['instrument_token'] < 0 and kept[0]['in_scope'] == 0
        assert kept[0]['expiry'] == '2026-09-29' and kept[0]['strike'] == 100
        cur = d.contract(1)
        assert cur['tradingsymbol'] == new.tradingsymbol and cur['first_seen'] == '2026-09-30T03:00:00'
        # raw rows are not rewritten; the old bar resolves to the old identity, a new one to the new
        assert d.con.execute('select instrument_token from snapshots').fetchone()[0] == 1
        assert d.contract_at(1, '2026-09-25 10:00:00')['tradingsymbol'] == old.tradingsymbol
        assert d.contract_at(1, '2026-10-01')['tradingsymbol'] == new.tradingsymbol
        d.sync_contracts([new], in_scope_tokens=[1], seen_at='2026-10-01T03:00:00')
        assert d.con.execute("select count(*) from contracts where instrument_token<0").fetchone()[0] == 1


def test_e01_lot_size_correction_is_versioned_not_a_new_contract(tmp_path):
    with DerivativesStore(tmp_path / 'd.db') as d:
        d.sync_contracts([Contract(5, 'NIFTY26SEP100CE', 'NIFTY', 'CE', 100, date(2026, 9, 29), 75)], seen_at='2026-09-01T03:00:00')
        d.sync_contracts([Contract(5, 'NIFTY26SEP100CE', 'NIFTY', 'CE', 100, date(2026, 9, 29), 65)], seen_at='2026-09-10T03:00:00')
        h = d.con.execute('select * from contract_token_history').fetchall()
        assert len(h) == 1 and h[0]['reason'] == 'metadata_change' and h[0]['lot_size'] == 75
        assert d.contract(5)['lot_size'] == 65
        assert d.con.execute('select count(*) from contracts').fetchone()[0] == 1
