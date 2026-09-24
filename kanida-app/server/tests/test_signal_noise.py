"""THE SIGNAL-TO-NOISE AUDIT — claim-specific outcomes, appended, never rewriting what KANIDA said."""
import json
import sqlite3

from kanida_pilot.signal_noise import SignalNoise, judge, verdict, SN_VERSION
from kanida_pilot.snapshots import SCHEMA as SNAP_SCHEMA

OK = {'_status': 'ok'}


def claim(state='building', row='calls', behaviour='writing', strikes=(23400, 23500), lead=23400):
    return {'state': state, 'row': row, 'behaviour': behaviour, 'strike_range': list(strikes), 'leading_strike': lead}


def later(state='held', row='calls', behaviour='writing', strikes=(23400, 23500), lead=23400):
    return {**OK, 'state': state, 'row': row, 'behaviour': behaviour, 'strike_range': list(strikes),
            'leading_strike': lead}


def test_each_claim_is_judged_on_its_own_terms():
    assert judge(claim(), later())[0] == 'confirmed'
    assert judge(claim(), later('balanced'))[0] == 'expired', 'activity that disappears is transient'
    assert judge(claim(), later(row='puts'))[0] == 'expired'
    assert judge(claim(), later('reversing', behaviour='buyers_exiting'))[0] == 'invalidated'
    assert judge(claim(), later(strikes=(23700,)))[0] == 'partially_confirmed', 'same behaviour, different strikes'
    assert judge(claim('broadened', strikes=(23400, 23500, 23600)), later(strikes=(23400,)))[0] == 'partially_confirmed'
    assert judge(claim('fading'), later('balanced'))[0] == 'confirmed', 'a fading claim holds when it stays quiet'
    assert judge(claim('fading'), later('building'))[0] == 'invalidated'
    assert judge(claim(), None)[0] == 'unresolved'
    assert judge(claim(), {'_status': 'unreconstructable'})[0] == 'unresolved'


def test_a_verdict_carries_its_reasons():
    rec = {'oi_change': 1000, 'oi_level': 1_000_000, 'price_change_pct': 0.2, 'breadth': 1, 'headline': 'X',
           'previous_headline': 'X', 'regime': None}
    final, noise, _ = verdict({'15m': 'expired', '30m': 'expired', '60m': 'expired'}, rec)
    assert final == 'noise'
    for reason in ('small_oi_change', 'price_move_small', 'single_strike', 'transient', 'duplicate_commentary'):
        assert reason in noise
    final, _, signal = verdict({'15m': 'confirmed', '30m': 'confirmed', '60m': 'confirmed'},
                               {'oi_change': 50000, 'oi_level': 1_000_000, 'breadth': 3, 'regime': 'Call writing'})
    assert final == 'signal' and 'persisted_60m' in signal and 'aligned_evidence' in signal
    assert verdict({'15m': 'confirmed'}, {}) is None, 'no verdict before the 60-minute outcome exists'


def _store(tmp_path, readings):
    """readings: list of (at, chained-state dict or None for unreconstructable)."""
    p = tmp_path / 'intel.db'
    db = sqlite3.connect(p)
    db.executescript(SNAP_SCHEMA)
    for i, (at, st) in enumerate(readings, start=1):
        db.execute("""insert into reading_snapshots(id,session,reading_at,underlying,expiry,spot,atm_strike,status,reason,
                      contracts,reading,chained,engine_version,rules_version,created_at)
                      values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                   (i, '2026-09-21', at, 'NIFTY', '2026-09-22', 23400, 23400, 'ok' if st else 'unreconstructable', None,
                    '[]', json.dumps(st) if st else None, json.dumps(st) if st else None, 'pane/3.1', 'signal/2', 'x'))
    db.commit()
    return p


def st(state, row='calls', strikes=(23400, 23500), read='Call writing building above ATM'):
    return {'state': state, 'row': row, 'behaviour': 'writing', 'strike_range': list(strikes), 'leading_strike': 23400,
            'plain_language_read': read, 'call_oi_change': 50000, 'call_oi_level': 1_000_000, 'breadth': len(strikes)}


def test_records_are_written_once_and_outcomes_appended_without_look_ahead(tmp_path):
    marks = [f'2026-09-21 {t}:00' for t in ('09:45', '10:00', '10:15', '10:30', '10:45')]
    p = _store(tmp_path, [(marks[0], st('building')), (marks[1], st('held'))])
    sn = SignalNoise(p)
    assert sn.record('2026-09-21', 'pane/3.1', 'signal/2') == 2
    assert sn.record('2026-09-21', 'pane/3.1', 'signal/2') == 0, 'a claim is recorded once'
    sn.evaluate('2026-09-21', 'pane/3.1')
    db = sqlite3.connect(p)
    got = db.execute('select horizon,outcome from insight_outcomes where record_id=1').fetchall()
    assert got == [('15m', 'confirmed')], 'only the horizon whose reading exists — the future is pending, not guessed'
    # later readings arrive: the 30m / 60m outcomes are APPENDED, the 15m one and the record are untouched
    original = db.execute('select * from insight_records where id=1').fetchone()
    first15 = db.execute("select evaluated_at,outcome from insight_outcomes where record_id=1 and horizon='15m'").fetchone()
    db.executemany("""insert into reading_snapshots(session,reading_at,underlying,expiry,spot,atm_strike,status,
                      contracts,reading,chained,engine_version,rules_version,created_at) values(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                   [('2026-09-21', m, 'NIFTY', '2026-09-22', 23400, 23400, 'ok', '[]', json.dumps(s), json.dumps(s),
                     'pane/3.1', 'signal/2', 'x') for m, s in ((marks[2], st('balanced')), (marks[3], st('balanced')),
                                                              (marks[4], st('balanced')))])
    db.commit()
    sn.evaluate('2026-09-21', 'pane/3.1')
    outs = dict(db.execute('select horizon,outcome from insight_outcomes where record_id=1').fetchall())
    assert outs == {'15m': 'confirmed', '30m': 'expired', '60m': 'expired'}
    assert db.execute('select * from insight_records where id=1').fetchone() == original, 'the claim is never rewritten'
    assert db.execute("select evaluated_at,outcome from insight_outcomes where record_id=1 and horizon='15m'").fetchone() == first15
    v = db.execute('select final,noise_reasons from insight_verdicts where record_id=1').fetchone()
    assert v[0] == 'partial_signal', 'confirmed at 15m, gone by 30m'
    rep = sn.report('2026-09-21', 'pane/3.1')
    assert rep['sn_version'] == SN_VERSION and rep['insights'] == 2
    assert rep['baseline_15m_rate'] is None or 0 <= rep['baseline_15m_rate'] <= 1


def test_quiet_readings_are_not_insights(tmp_path):
    p = _store(tmp_path, [('2026-09-21 09:45:00', st('balanced')), ('2026-09-21 10:00:00', st('opening'))])
    assert SignalNoise(p).record('2026-09-21', 'pane/3.1', 'signal/2') == 0
