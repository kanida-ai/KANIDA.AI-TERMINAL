"""SIGNAL-TO-NOISE AUDIT — does what KANIDA says turn out to matter?

Built on the immutable reading snapshots (snapshots.py). The chain is:

    immutable 15-min snapshot -> generated insight -> later snapshots -> outcome -> signal / noise

  * INSIGHT RECORDS: every generated claim (a snapshot whose state is a market behaviour) is recorded ONCE, exactly as
    KANIDA stated it, with the engine + rules + audit versions. INSERT OR IGNORE; never updated.
  * OUTCOMES are APPENDED, per horizon (15m / 30m / 60m / eod), from the snapshot at that later reading only - no
    look-ahead, and never by editing the original. Each test is specific to the claim (a "broadened" claim is judged
    on breadth, a "fading" claim on the activity staying gone), not on whether the index went up or down.
  * VERDICTS (signal / partial_signal / noise / unresolved) are appended once the 60-minute outcome exists, with the
    REASONS - computed ones (small OI change, single strike, transient, flip-flop, duplicate commentary, ATM-shift,
    illiquid) - so the report says WHY, not just how often.
  * A BASELINE is always reported beside a confirmation rate: the same test applied to a different instrument's later
    snapshot. If claims "confirm" no more often than unrelated pairs, the confirmation rate is not skill.

Nothing here changes a production threshold. The report is evidence for review:
observe -> measure -> diagnose -> test -> validate -> deploy.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

SN_VERSION = 'sn/1.2 2026-09-21'
# Every number the audit judges with, in one place and versioned with it. Configurable, never silently changed.
RULES = {
    'horizons': {'15m': 15, '30m': 30, '60m': 60},
    'tolerance_minutes': 5,          # a follow reading may be up to this late (a missed mark), never early
    'material_oi_share': 0.01,       # |side ΔOI| below 1% of that side's OI is a small change
    'material_price_pct': 1.0,       # a premium move under 1% is a small one
    'illiquid_price': 1.0,           # a leading contract under ₹1
    # the session's LAST TRADING reading, and the IST wall-clock time after which the day is treated as closed. The
    # 15:45 post-close capture is not a trading reading (21 Sep): the close is the 15:30 reading, judged only once
    # the clock is past 15:50 so nothing is decided while a reading could still land.
    'session_close': '15:30',
    'closed_after_ist': '15:50',
}
RUN = {'building', 'appeared', 'held', 'strengthened', 'broadened', 'concentrated', 'shifted', 'slowed', 'reversing'}
INSIGHTS = RUN | {'fading', 'mixed'}
EXITS = {'short_covering', 'buyers_exiting'}

SCHEMA = """
create table if not exists insight_records(
  id integer primary key, session text not null, reading_at text not null, underlying text not null,
  instrument_type text, expiry text, snapshot_id integer not null, atm_strike real,
  engine_version text, rules_version text, sn_version text not null,
  original_state text, state_change text, side text, row text, behaviour text, regime text,
  strike_range text, leading_strike real, breadth integer,
  price_change real, price_change_pct real, oi_change real, oi_level real, iv_change real, pcr_change real,
  max_pain_change real, supporting text, conflicting text, headline text, interpretation text,
  previous_headline text, previous_row text, created_at text not null,
  unique(snapshot_id, sn_version));
create table if not exists insight_outcomes(
  id integer primary key, record_id integer not null, horizon text not null, outcome text not null, detail text,
  follow_snapshot_id integer, follow_reading_at text, sn_version text not null, evaluated_at text not null,
  unique(record_id, horizon, sn_version));
create table if not exists insight_verdicts(
  id integer primary key, record_id integer not null, final text not null, noise_reasons text, signal_reasons text,
  sn_version text not null, decided_at text not null, unique(record_id, sn_version));
"""


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def _t(s):
    return datetime.strptime(str(s)[:19], '%Y-%m-%d %H:%M:%S')


def _dir(state, behaviour):
    return 'reducing' if state == 'reversing' or (behaviour in EXITS) else 'building'


def judge(claim, follow):
    """ONE CLAIM against ONE later snapshot. Returns (outcome, detail). The test is the claim's own."""
    if follow is None:
        return 'unresolved', 'no later snapshot at this horizon'
    if follow.get('_status') != 'ok':
        return 'unresolved', 'the later reading could not be anchored'
    cs, fs = claim['state'], follow['state']
    if fs == 'not_observed':
        return 'unresolved', 'the later reading carried no comparable value'
    if cs == 'fading':
        if fs in ('balanced', 'fading', 'opening') or follow.get('row') != claim.get('row'):
            return 'confirmed', f'the {claim.get("row")} activity stayed quiet'
        return 'invalidated', 'the activity came back on the same side'
    if cs == 'mixed':
        if fs == 'mixed':
            return 'confirmed', 'the sides still contradict'
        if fs in RUN:
            return 'partially_confirmed', 'one side continued, the contradiction resolved'
        return 'expired', 'both sides went quiet'
    # a behaviour claim: building / reducing on one side, at named strikes
    if fs in ('balanced', 'fading'):
        return 'expired', 'the activity did not persist'
    if follow.get('row') != claim.get('row'):
        return 'expired', f'the {follow.get("row")} side took over'
    if _dir(fs, follow.get('behaviour')) != _dir(cs, claim.get('behaviour')):
        return 'invalidated', 'positions on the same side moved the other way'
    have = set(claim.get('strike_range') or [])
    later = set(follow.get('strike_range') or [])
    if have and not (have & later):
        return 'partially_confirmed', 'the same behaviour, at different strikes'
    if cs == 'broadened' and len(later) < len(have):
        return 'partially_confirmed', 'the behaviour continued but narrowed back'
    if cs == 'concentrated' and len(later) > len(have):
        return 'partially_confirmed', 'the behaviour continued but spread again'
    if cs == 'strengthened' and fs == 'slowed':
        return 'partially_confirmed', 'the behaviour continued at a slower pace'
    if cs == 'shifted' and follow.get('leading_strike') != claim.get('leading_strike'):
        return 'partially_confirmed', 'the lead moved again'
    return 'confirmed', f'the same behaviour continued ({fs})'


def verdict(outcomes, rec):
    """Signal / partial / noise / unresolved from the appended outcomes, with the reasons."""
    o15, o30, o60 = (outcomes.get(h) for h in ('15m', '30m', '60m'))
    if o60 is None:
        return None
    noise, signal = [], []
    share = abs(rec['oi_change'] or 0) / rec['oi_level'] if rec.get('oi_level') else None
    if share is not None and share < RULES['material_oi_share']:
        noise.append('small_oi_change')
    if rec.get('price_change_pct') is not None and abs(rec['price_change_pct']) < RULES['material_price_pct']:
        noise.append('price_move_small')
    if (rec.get('breadth') or 0) == 1:
        noise.append('single_strike')
    if o15 == 'expired':
        noise.append('transient')
    if o15 == 'invalidated':
        noise.append('reversed')
    if rec.get('previous_headline') and rec.get('previous_headline') == rec.get('headline'):
        noise.append('duplicate_commentary')
    if rec.get('_flip'):
        noise.append('flip_flop')
    if rec.get('_atm_moved') and o15 in ('expired', 'invalidated'):
        noise.append('atm_shift_artifact')
    if rec.get('_lead_price') is not None and rec['_lead_price'] < RULES['illiquid_price']:
        noise.append('illiquid')
    if o60 == 'confirmed':
        signal.append('persisted_60m')
    if rec.get('_broadened_after'):
        signal.append('broadened_after')
    if rec.get('regime') and (rec.get('breadth') or 0) >= 3 and share is not None and share >= RULES['material_oi_share']:
        signal.append('aligned_evidence')
    if o60 == 'unresolved' and o15 == 'unresolved':
        final = 'unresolved'
    elif o60 == 'confirmed' or (o30 == 'confirmed' and o60 == 'partially_confirmed'):
        final = 'signal'
    elif o60 == 'partially_confirmed' or o15 == 'confirmed':
        final = 'partial_signal'
    elif o15 in ('expired', 'invalidated') and o30 != 'confirmed':
        final = 'noise'
    else:
        final = 'unresolved'
    return final, noise, signal


class SignalNoise:
    def __init__(self, path, index_names=None):
        self.path = Path(path)
        self._local = threading.local()
        self.index_names = set(index_names or [])

    def _db(self):
        db = getattr(self._local, 'db', None)
        if db is None:
            db = sqlite3.connect(str(self.path), timeout=10, check_same_thread=False)
            db.row_factory = sqlite3.Row
            db.executescript(SCHEMA)
            self._local.db = db
        return db

    # --- snapshots, as the audit reads them -------------------------------------------------------------------
    def _snaps(self, session, engine_version):
        rows = self._db().execute(
            'select * from reading_snapshots where session=? and engine_version=? order by underlying, reading_at',
            (session, engine_version)).fetchall()
        by = defaultdict(list)
        for r in rows:
            s = json.loads(r['chained']) if r['status'] == 'ok' and r['chained'] else {}
            s['_status'], s['_id'], s['_at'], s['_atm'] = r['status'], r['id'], r['reading_at'], r['atm_strike']
            s['_expiry'] = r['expiry']
            by[r['underlying']].append(s)
        return by

    @staticmethod
    def _follow(series, at, minutes):
        target = _t(at) + timedelta(minutes=minutes)
        late = target + timedelta(minutes=RULES['tolerance_minutes'])
        for s in series:
            when = _t(s['_at'])
            if target <= when <= late:
                return s
        return None

    # --- 1. record the claims made at each reading (once) ----------------------------------------------------------
    def record(self, session, engine_version, rules_version):
        db = self._db()
        wrote = 0
        for name, series in self._snaps(session, engine_version).items():
            prev = None
            for s in series:
                if s['_status'] == 'ok' and s.get('state') in INSIGHTS:
                    row = s.get('row')
                    oi = s.get('put_oi_change') if row == 'puts' else s.get('call_oi_change')
                    level = s.get('put_oi_level') if row == 'puts' else s.get('call_oi_level')
                    cur = db.execute(
                        """insert or ignore into insight_records(session,reading_at,underlying,instrument_type,expiry,
                           snapshot_id,atm_strike,engine_version,rules_version,sn_version,original_state,state_change,
                           side,row,behaviour,regime,strike_range,leading_strike,breadth,price_change,price_change_pct,
                           oi_change,oi_level,iv_change,pcr_change,max_pain_change,supporting,conflicting,headline,
                           interpretation,previous_headline,previous_row,created_at)
                           values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (session, s['_at'], name, 'index' if name in self.index_names else 'stock', s['_expiry'],
                         s['_id'], s['_atm'], engine_version, rules_version, SN_VERSION, s.get('state'),
                         s.get('state_change'), s.get('side'), row, s.get('behaviour'), s.get('regime'),
                         json.dumps(s.get('strike_range') or []), s.get('leading_strike'),
                         len(s.get('strike_range') or []), s.get('price_change'), s.get('price_change_pct'), oi, level,
                         s.get('iv_change'), s.get('pcr_change'), s.get('max_pain_change'),
                         json.dumps([e.get('text') for e in s.get('supporting_evidence') or []]),
                         json.dumps([e.get('text') for e in s.get('conflicting_evidence') or []]),
                         s.get('plain_language_read'), s.get('plain_language_evidence'),
                         (prev or {}).get('plain_language_read'), (prev or {}).get('row'), _now()))
                    wrote += cur.rowcount
                if s['_status'] == 'ok':
                    prev = s
        db.commit()
        return wrote

    # --- 2. append outcomes as later readings arrive; 3. decide verdicts ----------------------------------------------
    def evaluate(self, session, engine_version):
        db = self._db()
        snaps = self._snaps(session, engine_version)
        ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        past_close = ist_now.strftime('%Y-%m-%d') > session or (
            ist_now.strftime('%Y-%m-%d') == session and ist_now.strftime('%H:%M') >= RULES['closed_after_ist'])
        closed = past_close and any(str(s['_at'])[11:16] >= RULES['session_close'] and s['_status'] == 'ok'
                                    for ss in snaps.values() for s in ss)
        recs = db.execute('select * from insight_records where session=? and engine_version=? and sn_version=?',
                          (session, engine_version, SN_VERSION)).fetchall()
        added = 0
        for rec in recs:
            series = snaps.get(rec['underlying'], [])
            claim = {'state': rec['original_state'], 'row': rec['row'], 'behaviour': rec['behaviour'],
                     'strike_range': json.loads(rec['strike_range'] or '[]'), 'leading_strike': rec['leading_strike']}
            horizons = dict(RULES['horizons'])
            for h, minutes in horizons.items():
                follow = self._follow(series, rec['reading_at'], minutes)
                if follow is None and not closed:
                    continue                    # not yet known - pending, not unresolved
                outcome, detail = judge(claim, follow)
                cur = db.execute("""insert or ignore into insight_outcomes(record_id,horizon,outcome,detail,
                                    follow_snapshot_id,follow_reading_at,sn_version,evaluated_at)
                                    values(?,?,?,?,?,?,?,?)""",
                                 (rec['id'], h, outcome, detail, follow and follow['_id'], follow and follow['_at'],
                                  SN_VERSION, _now()))
                added += cur.rowcount
            if closed:
                # the session's last VALID snapshot. sn/1.1 took the 15:45 post-close row, which is not a trading
                # reading, and recorded every EOD outcome as unresolved (21 Sep 15:47) - kept, superseded by sn/1.2.
                last = next((s for s in reversed(series) if s['_at'] > rec['reading_at'] and s['_status'] == 'ok'), None)
                outcome, detail = judge(claim, last)
                cur = db.execute("""insert or ignore into insight_outcomes(record_id,horizon,outcome,detail,
                                    follow_snapshot_id,follow_reading_at,sn_version,evaluated_at)
                                    values(?,?,?,?,?,?,?,?)""",
                                 (rec['id'], 'eod', outcome, detail, last and last['_id'], last and last['_at'],
                                  SN_VERSION, _now()))
                added += cur.rowcount
            # the verdict, once the 60-minute outcome exists
            got = {r['horizon']: r['outcome'] for r in db.execute(
                'select horizon,outcome from insight_outcomes where record_id=? and sn_version=?', (rec['id'], SN_VERSION))}
            if '60m' in got:
                f15 = self._follow(series, rec['reading_at'], 15)
                detail = dict(rec)
                detail['_flip'] = bool(f15 and f15.get('_status') == 'ok' and rec['previous_row']
                                       and rec['previous_row'] != rec['row'] and f15.get('row') == rec['previous_row'])
                detail['_atm_moved'] = bool(f15 and f15.get('_atm') is not None and f15['_atm'] != rec['atm_strike'])
                detail['_broadened_after'] = any(
                    s.get('state') == 'broadened' and s.get('row') == rec['row']
                    for s in series if rec['reading_at'] < s['_at'] <= (_t(rec['reading_at']) + timedelta(minutes=60)).strftime('%Y-%m-%d %H:%M:%S'))
                snap = next((s for s in series if s['_id'] == rec['snapshot_id']), {})
                detail['_lead_price'] = snap.get('price_level')
                v = verdict(got, detail)
                if v:
                    final, noise, signal = v
                    cur = db.execute("""insert or ignore into insight_verdicts(record_id,final,noise_reasons,signal_reasons,
                                        sn_version,decided_at) values(?,?,?,?,?,?)""",
                                     (rec['id'], final, json.dumps(noise), json.dumps(signal), SN_VERSION, _now()))
                    added += cur.rowcount
        db.commit()
        return added

    # --- baseline: the same test against an UNRELATED instrument's later snapshot --------------------------------------
    def baseline(self, session, engine_version, horizon='15m'):
        snaps = self._snaps(session, engine_version)
        names = sorted(snaps)
        recs = self._db().execute('select * from insight_records where session=? and engine_version=? and sn_version=?',
                                  (session, engine_version, SN_VERSION)).fetchall()
        hits = n = 0
        for rec in recs:
            other = names[(names.index(rec['underlying']) + 1) % len(names)] if rec['underlying'] in names else None
            if not other:
                continue
            follow = self._follow(snaps[other], rec['reading_at'], RULES['horizons'][horizon])
            if follow is None:
                continue
            claim = {'state': rec['original_state'], 'row': rec['row'], 'behaviour': rec['behaviour'],
                     'strike_range': [], 'leading_strike': None}
            outcome, _ = judge(claim, follow)
            if outcome == 'unresolved':
                continue
            n += 1
            hits += outcome == 'confirmed'
        return (hits / n) if n else None, n

    # --- the report ------------------------------------------------------------------------------------------------
    def report(self, session, engine_version):
        db = self._db()
        recs = db.execute('select * from insight_records where session=? and engine_version=? and sn_version=?',
                          (session, engine_version, SN_VERSION)).fetchall()
        outs = defaultdict(dict)
        for r in db.execute("""select o.record_id,o.horizon,o.outcome from insight_outcomes o join insight_records r
                               on r.id=o.record_id where r.session=? and o.sn_version=?""", (session, SN_VERSION)):
            outs[r['record_id']][r['horizon']] = r['outcome']
        verd = {r['record_id']: r for r in db.execute(
            """select v.* from insight_verdicts v join insight_records r on r.id=v.record_id
               where r.session=? and v.sn_version=?""", (session, SN_VERSION))}
        total = len(recs)
        finals = Counter(verd[r['id']]['final'] if r['id'] in verd else 'pending' for r in recs)
        by_h = {h: Counter(outs[r['id']].get(h, 'pending') for r in recs) for h in ('15m', '30m', '60m', 'eod')}
        by_type = defaultdict(Counter)
        by_name = defaultdict(Counter)
        reasons = Counter()
        signal_reasons = Counter()
        for r in recs:
            kind = r['regime'] or r['original_state']
            f = verd[r['id']]['final'] if r['id'] in verd else 'pending'
            by_type[kind][f] += 1
            by_name[r['underlying']][f] += 1
            if r['id'] in verd:
                reasons.update(json.loads(verd[r['id']]['noise_reasons'] or '[]'))
                signal_reasons.update(json.loads(verd[r['id']]['signal_reasons'] or '[]'))
        # product noise: saying the same thing again, and flip-flopping, independent of outcome
        dup = sum(1 for r in recs if r['previous_headline'] and r['previous_headline'] == r['headline'])
        flips = 0
        by_series = defaultdict(list)
        for r in sorted(recs, key=lambda x: (x['underlying'], x['reading_at'])):
            by_series[r['underlying']].append(r['row'])
        for rows in by_series.values():
            flips += sum(1 for a, b, c in zip(rows, rows[1:], rows[2:]) if a == c and a != b)
        base, base_n = self.baseline(session, engine_version, '15m')
        decided = sum(finals[k] for k in ('signal', 'partial_signal', 'noise'))
        conf15 = by_h['15m']['confirmed']
        judged15 = sum(v for k, v in by_h['15m'].items() if k not in ('pending', 'unresolved'))
        # evidence-quality splits: does breadth / materiality separate signal from noise?
        split = defaultdict(Counter)
        for r in recs:
            if r['id'] not in verd:
                continue
            f = verd[r['id']]['final']
            share = abs(r['oi_change'] or 0) / r['oi_level'] if r['oi_level'] else None
            split['breadth>=3' if (r['breadth'] or 0) >= 3 else 'breadth<3'][f] += 1
            split['oi>=1%' if share is not None and share >= RULES['material_oi_share'] else 'oi<1%'][f] += 1
            split['behaviour named' if r['regime'] else 'activity only'][f] += 1
        return {
            'session': session, 'engine_version': engine_version, 'sn_version': SN_VERSION, 'rules': RULES,
            'generated_at': _now(), 'insights': total, 'final': dict(finals), 'by_horizon': {h: dict(c) for h, c in by_h.items()},
            'signal_to_noise': (finals['signal'] / finals['noise']) if finals['noise'] else None,
            'confirmed_15m_rate': (conf15 / judged15) if judged15 else None,
            'baseline_15m_rate': base, 'baseline_pairs': base_n,
            'by_type': {k: dict(v) for k, v in by_type.items()},
            'by_instrument': {k: dict(v) for k, v in sorted(by_name.items(), key=lambda kv: -sum(kv[1].values()))},
            'noise_reasons': dict(reasons.most_common()), 'signal_reasons': dict(signal_reasons.most_common()),
            'duplicate_commentary': dup, 'flip_flops': flips, 'evidence_splits': {k: dict(v) for k, v in split.items()},
            'decided': decided,
        }


def _pct(n, d):
    return '—' if not d else f'{100 * n / d:.0f}%'


def markdown(rep):
    """The report as engineering and quant review it."""
    f = rep['final']
    lines = [f"# Signal-to-noise — {rep['session']}", '',
             f"Engine `{rep['engine_version']}` · audit `{rep['sn_version']}` · generated {rep['generated_at']} UTC", '',
             '## Overall', '',
             f"| Insights | Signal | Partial | Noise | Unresolved | Pending |", '|---|---|---|---|---|---|',
             f"| {rep['insights']} | {f.get('signal', 0)} | {f.get('partial_signal', 0)} | {f.get('noise', 0)} | "
             f"{f.get('unresolved', 0)} | {f.get('pending', 0)} |", '',
             f"Signal-to-noise ratio: **{'—' if rep['signal_to_noise'] is None else f'{rep['signal_to_noise']:.2f}'}**", '',
             f"15-minute confirmation rate **{'—' if rep['confirmed_15m_rate'] is None else f'{100*rep['confirmed_15m_rate']:.0f}%'}** "
             f"against a baseline of **{'—' if rep['baseline_15m_rate'] is None else f'{100*rep['baseline_15m_rate']:.0f}%'}** "
             f"(the same test on an unrelated instrument's next reading, {rep['baseline_pairs']} pairs). "
             'A confirmation rate no higher than the baseline is not skill.', '',
             '## Outcomes by horizon', '', '| Horizon | Confirmed | Partial | Expired | Invalidated | Unresolved | Pending |',
             '|---|---|---|---|---|---|---|']
    for h, c in rep['by_horizon'].items():
        lines.append(f"| {h} | {c.get('confirmed', 0)} | {c.get('partially_confirmed', 0)} | {c.get('expired', 0)} | "
                     f"{c.get('invalidated', 0)} | {c.get('unresolved', 0)} | {c.get('pending', 0)} |")
    lines += ['', '## By insight type', '', '| Type | Insights | Signal | Partial | Noise | Signal rate (decided) |', '|---|---|---|---|---|---|']
    for k, c in sorted(rep['by_type'].items(), key=lambda kv: -sum(kv[1].values())):
        dec = c.get('signal', 0) + c.get('partial_signal', 0) + c.get('noise', 0)
        lines.append(f"| {k} | {sum(c.values())} | {c.get('signal', 0)} | {c.get('partial_signal', 0)} | {c.get('noise', 0)} | "
                     f"{_pct(c.get('signal', 0), dec)} |")
    lines += ['', '## Why insights were noise', '', '| Reason | Count |', '|---|---|']
    lines += [f'| {k} | {v} |' for k, v in rep['noise_reasons'].items()] or ['| — | — |']
    lines += ['', '## What the signals had', '', '| Reason | Count |', '|---|---|']
    lines += [f'| {k} | {v} |' for k, v in rep['signal_reasons'].items()] or ['| — | — |']
    lines += ['', '## Evidence quality — does it separate signal from noise?', '', '| Split | Signal | Partial | Noise | Signal rate |', '|---|---|---|---|---|']
    for k, c in rep['evidence_splits'].items():
        dec = c.get('signal', 0) + c.get('partial_signal', 0) + c.get('noise', 0)
        lines.append(f"| {k} | {c.get('signal', 0)} | {c.get('partial_signal', 0)} | {c.get('noise', 0)} | {_pct(c.get('signal', 0), dec)} |")
    lines += ['', '## Unnecessary commentary', '',
              f"- Repeated headline, unchanged from the reading before: **{rep['duplicate_commentary']}**",
              f"- Side flip-flops (A → B → A within three readings): **{rep['flip_flops']}**", '',
              '## By instrument (most insights first)', '', '| Instrument | Insights | Signal | Partial | Noise | Pending |', '|---|---|---|---|---|---|']
    for k, c in list(rep['by_instrument'].items())[:25]:
        lines.append(f"| {k} | {sum(c.values())} | {c.get('signal', 0)} | {c.get('partial_signal', 0)} | {c.get('noise', 0)} | {c.get('pending', 0)} |")
    lines += ['', '_Evidence for review, not an instruction: no production threshold is changed by this report._', '']
    return '\n'.join(lines)
