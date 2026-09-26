"""IMMUTABLE 15-MINUTE READING SNAPSHOTS — what KANIDA saw, as it saw it, kept.

Found live 21 Sep 2026: when the at-the-money strike moved (23,400 -> 23,350 at 10:15), earlier readings were
re-described on the NEW contract set and history changed what it said. The raw F&O store was never wrong - every
reading already holds every in-scope contract and the spot - but every view re-derived the past from the grid
anchored at the LATEST spot.

This module fixes that at the right layer:

  1. Each reading T, for each underlying, is described ONCE from the grid anchored AT T (`oi_grid(at=T)`: T's own
     spot, T's own at-the-money ten), by the SAME engine the browser runs (server/engine/run_engine.cjs loading
     src/derivative/signal.ts) in snapshot mode: T's own fifteen minutes only.
  2. Session context (held / broadened / shifted, persistence) is added by the engine's `chainStep` from the
     PREVIOUS STORED SNAPSHOT - never by re-reading earlier readings on T's contracts.
  3. The exact contracts, the spot, the ATM, the reading, the chained reading and the engine + rules versions are
     written to `intelligence.db` with INSERT OR IGNORE. There is no UPDATE path: a stored reading never changes.
     A later engine writes its own rows under its own version; it never overwrites an older one.
  4. A reading that cannot be anchored (no spot captured at T) is recorded as `unreconstructable` with the reason,
     not guessed.

READ-ONLY on db/derivatives.db. The only writes are to the intelligence store.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

LOG = logging.getLogger('pilot.snapshots')
ROOT = Path(__file__).resolve().parents[1]           # kanida-app/server
ENGINE = ROOT / 'engine' / 'run_engine.cjs'

SCHEMA = """
create table if not exists reading_snapshots(
  id integer primary key,
  session text not null,
  reading_at text not null,
  underlying text not null,
  expiry text,
  spot real,
  atm_strike real,
  status text not null,              -- ok | unreconstructable
  reason text,
  contracts text,                    -- JSON: the exact contracts behind the reading, with their values AT the reading
  reading text,                      -- JSON: the engine's reading of T's own fifteen minutes
  chained text,                      -- JSON: the reading with session context from the stored previous snapshot
  previous_snapshot_id integer,
  engine_version text not null,
  rules_version text not null,
  created_at text not null,
  unique(underlying, reading_at, engine_version, expiry, rules_version)
);
create index if not exists reading_snapshots_session_v2 on reading_snapshots(session, underlying, reading_at);
"""

#: Slice 14 (audit E03/E41 "interpretation snapshots"): the scope of a stored reading is underlying + reading +
#: engine + EXPIRY + RULES version, so two expiries (or two rule versions) of one reading can coexist. A store
#: created under the old key (underlying, reading_at, engine_version) is migrated by `_migrate_scope`: the old
#: table is RENAMED and KEPT as `reading_snapshots_v1` (nothing is deleted), and every row is copied verbatim - same
#: id, same bytes - into the new table. No stored claim is rewritten; the copy is verified by count and content
#: checksum before it commits, and a failed check rolls the whole migration back.
OLD_SCOPE = ('underlying', 'reading_at', 'engine_version')
NEW_SCOPE = ('underlying', 'reading_at', 'engine_version', 'expiry', 'rules_version')


def _unique_scopes(db, table='reading_snapshots'):
    out = []
    for idx in db.execute(f'pragma index_list({table})').fetchall():
        if idx[2]:   # unique
            out.append(tuple(r[2] for r in db.execute(f'pragma index_info("{idx[1]}")').fetchall()))
    return out


def _migrate_scope(db):
    """Idempotent. Returns True when a migration ran."""
    exists = db.execute("select 1 from sqlite_master where type='table' and name='reading_snapshots'").fetchone()
    if not exists or OLD_SCOPE not in _unique_scopes(db):
        return False
    db.execute('begin immediate')
    try:
        if OLD_SCOPE not in _unique_scopes(db):      # another connection migrated while we waited for the lock
            db.execute('rollback')
            return False
        if db.execute("select 1 from sqlite_master where name='reading_snapshots_v1'").fetchone():
            raise RuntimeError('reading_snapshots_v1 already exists; refusing to overwrite a kept table')
        cols = [r[1] for r in db.execute('pragma table_info(reading_snapshots)').fetchall()]
        db.execute('alter table reading_snapshots rename to reading_snapshots_v1')
        for stmt in [x.strip() for x in SCHEMA.split(';') if x.strip()]:
            db.execute(stmt)
        new_cols = {r[1] for r in db.execute('pragma table_info(reading_snapshots)').fetchall()}
        keep = [c for c in cols if c in new_cols]
        cl = ','.join(keep)
        db.execute(f'insert into reading_snapshots({cl}) select {cl} from reading_snapshots_v1 order by id')
        check = f"select count(*), total(length(coalesce(reading,''))+length(coalesce(chained,''))+length(coalesce(contracts,''))), total(id) from "
        if db.execute(check + 'reading_snapshots').fetchone() != db.execute(check + 'reading_snapshots_v1').fetchone():
            raise RuntimeError('snapshot scope migration copy did not verify')
        db.execute('commit')
        LOG.info('reading_snapshots: scope migrated to %s; old table kept as reading_snapshots_v1', NEW_SCOPE)
        return True
    except Exception:
        db.execute('rollback')
        raise


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


class Engine:
    """The long-lived Node engine. One request per line, in order; a dead engine is restarted on next use."""

    def __init__(self, command=None):
        self.command = command or ['node', str(ENGINE)]
        self._proc = None
        self._lock = threading.Lock()
        self._id = 0

    def _ensure(self):
        if self._proc is None or self._proc.poll() is not None:
            self._proc = subprocess.Popen(self.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                          stderr=subprocess.DEVNULL, text=True, encoding='utf-8', bufsize=1)

    def call(self, op, **payload):
        with self._lock:
            self._ensure()
            self._id += 1
            self._proc.stdin.write(json.dumps({'id': self._id, 'op': op, **payload}) + '\n')
            self._proc.stdin.flush()
            line = self._proc.stdout.readline()
            if not line:
                self._proc = None
                raise RuntimeError('intelligence engine exited')
            answer = json.loads(line)
            if answer.get('error'):
                raise RuntimeError(answer['error'])
            return answer

    def close(self):
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()


class SnapshotStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()

    def _db(self):
        db = getattr(self._local, 'db', None)
        if db is None:
            db = sqlite3.connect(str(self.path), timeout=10, check_same_thread=False)
            db.row_factory = sqlite3.Row
            db.execute('pragma journal_mode=wal')
            db.isolation_level = None          # explicit transactions: the scope migration controls its own
            _migrate_scope(db)
            db.executescript(SCHEMA)
            db.isolation_level = ''
            self._local.db = db
        return db

    def insert(self, row):
        """INSERT OR IGNORE - the ONLY write. A snapshot that exists is never touched again."""
        db = self._db()
        cols = ','.join(row)
        db.execute(f'insert or ignore into reading_snapshots({cols}) values({",".join("?" * len(row))})',
                   tuple(row.values()))
        db.commit()

    def previous(self, underlying, session, before, engine_version, expiry=None):
        """The stored snapshot before `before`. With `expiry`, only the SAME expiry chains (two expiries of one
        reading can coexist since slice 14; a context chain never crosses from one to the other)."""
        sql = """select * from reading_snapshots where underlying=? and session=? and reading_at<? and engine_version=?
               and status='ok'"""
        args = [underlying, session, before, engine_version]
        if expiry:
            sql += ' and expiry=?'
            args.append(expiry)
        return self._db().execute(sql + ' order by reading_at desc limit 1', args).fetchone()

    def done(self, reading_at, engine_version):
        return self._db().execute('select count(*) from reading_snapshots where reading_at=? and engine_version=?',
                                  (reading_at, engine_version)).fetchone()[0]

    def session_rows(self, underlying, session, engine_version=None, upto=None):
        sql = 'select * from reading_snapshots where underlying=? and session=?'
        args = [underlying, session]
        if engine_version:
            sql += ' and engine_version=?'
            args.append(engine_version)
        if upto:
            sql += ' and reading_at<=?'
            args.append(upto)
        return self._db().execute(sql + ' order by reading_at', args).fetchall()

    def latest_per_underlying(self, session, reading_at, engine_version):
        return self._db().execute(
            """select * from reading_snapshots where session=? and reading_at=? and engine_version=?""",
            (session, reading_at, engine_version)).fetchall()


def _upto(series, at):
    """A session series cut at T: a snapshot of T is built from nothing after T."""
    if not isinstance(series, dict):
        return series
    out = dict(series)
    out['points'] = [p for p in (series.get('points') or []) if str(p.get('at') or '') <= at]
    return out


class SnapshotWorker:
    """Writes one snapshot per underlying per reading, once, in reading order. Runs in the pilot as a thread."""

    def __init__(self, derivatives, store, engine=None):
        self.d = derivatives
        self.store = store
        self.engine = engine or Engine()
        self._version = None

    def version(self):
        if self._version is None:
            v = self.engine.call('version')
            self._version = (v['engine_version'], v['rules_version'])
        return self._version

    # --- the store's readings ------------------------------------------------------------------------------
    def _conn(self):
        return self.d._connect()

    def readings(self, session):
        c = self._conn()
        if c is None:
            return []
        return [r[0] for r in c.execute(
            # TRADING readings only. The 15:45 `post_close` capture is a settlement read after the session: the grid
            # ends at 15:30 by design, so there is no fifteen-minute interval to snapshot (found 21 Sep 15:46).
            "select distinct captured_at from underlying_snapshots where substr(captured_at,1,10)=? "
            "and coalesce(mark_kind,'') != 'post_close' order by 1",
            (session,))]

    def metrics_ready(self, at):
        c = self._conn()
        return bool(c and c.execute("select 1 from metrics where captured_at=? and scope='underlying' limit 1",
                                    (at,)).fetchone())

    def underlyings(self, at):
        c = self._conn()
        return [r[0] for r in c.execute('select underlying from underlying_snapshots where captured_at=? order by 1',
                                        (at,))] if c else []

    def newest_session(self):
        c = self._conn()
        r = c.execute('select max(substr(captured_at,1,10)) from underlying_snapshots').fetchone() if c else None
        return r[0] if r else None

    # --- one underlying at one reading -------------------------------------------------------------------------
    def snapshot(self, underlying, at, session, first):
        engine_version, rules_version = self.version()
        base = {'session': session, 'reading_at': at, 'underlying': underlying, 'engine_version': engine_version,
                'rules_version': rules_version, 'created_at': _now()}
        if first:
            body = self.d.oi_by_strike(underlying, '', at)
            spot = body.get('spot')
            reading = self.engine.call('opening', at=at, body=body).get('reading') if body.get('rows') else None
            if not reading or spot is None:
                return {**base, 'status': 'unreconstructable', 'expiry': body.get('expiry'), 'spot': spot,
                        'reason': 'no spot or no open interest captured at this reading'}
            atm = min((r.get('strike') for r in body['rows'] if r.get('strike') is not None),
                      key=lambda k: (abs(k - spot), -k))
            ladder = sorted(r['strike'] for r in body['rows'] if r.get('strike') is not None)
            i = ladder.index(atm)
            keep = set(ladder[i:i + 5]) | set(ladder[max(0, i - 4):i + 1])
            contracts = [{'strike': r['strike'], 'ce_oi': r.get('ce_oi'), 'pe_oi': r.get('pe_oi')}
                         for r in body['rows'] if r.get('strike') in keep]
            chained = self.engine.call('chain', prev=None, cur=reading)['reading']
            return {**base, 'status': 'ok', 'expiry': body.get('expiry'), 'spot': spot, 'atm_strike': atm,
                    'contracts': json.dumps(contracts), 'reading': json.dumps(reading),
                    'chained': json.dumps(chained), 'reason': 'opening read from oi-by-strike at this reading'}
        grid = self.d.oi_grid(underlying, '', at)
        rows = grid.get('rows') or []
        if not rows:
            return {**base, 'status': 'unreconstructable', 'expiry': grid.get('expiry'), 'spot': grid.get('spot'),
                    'reason': grid.get('note') or grid.get('empty_reason') or 'no grid at this reading'}
        expiry = grid.get('expiry') or ''
        reading = self.engine.call('reading', at=at, grid=grid,
                                   pcr=_upto(self.d.pcr_series(underlying, expiry), at),
                                   maxPain=_upto(self.d.maxpain_series(underlying, expiry), at),
                                   iv=_upto(self.d.iv_series(underlying, expiry), at)).get('reading')
        if not reading:
            return {**base, 'status': 'unreconstructable', 'expiry': expiry, 'spot': grid.get('spot'),
                    'reason': 'the grid anchored at this reading does not end at it'}
        prev = self.store.previous(underlying, session, at, engine_version, expiry=expiry or None)
        chained = self.engine.call('chain', prev=json.loads(prev['chained']) if prev else None,
                                   cur=reading)['reading']
        contracts = []
        for r in rows:
            point = next((p for p in (r.get('points') or []) if str(p.get('at')) == at), {}) or {}
            contracts.append({'slot': r.get('slot'), 'option_type': r.get('option_type'), 'strike': r.get('strike'),
                              'instrument_token': r.get('instrument_token'), 'tradingsymbol': r.get('tradingsymbol'),
                              'oi': point.get('oi'), 'delta_oi': point.get('delta_oi'), 'price': point.get('price')})
        return {**base, 'status': 'ok', 'expiry': expiry, 'spot': grid.get('spot'), 'atm_strike': grid.get('atm_strike'),
                'contracts': json.dumps(contracts), 'reading': json.dumps(reading), 'chained': json.dumps(chained),
                'previous_snapshot_id': prev['id'] if prev else None, 'reason': None}

    # --- a whole reading, then a whole session -----------------------------------------------------------------
    def process_reading(self, session, at, first, pause=0.0):
        engine_version, _ = self.version()
        names = self.underlyings(at)
        if self.store.done(at, engine_version) >= len(names):
            return 0
        wrote = 0
        for name in names:
            try:
                self.store.insert(self.snapshot(name, at, session, first))
                wrote += 1
            except Exception as error:  # noqa: BLE001 - one instrument never stops the reading
                LOG.warning('snapshot %s @ %s failed: %s', name, at, error)
            if pause:
                time.sleep(pause)
        return wrote

    def process_session(self, session, pause=0.0):
        """Every reading of the session, OLDEST FIRST - the chain needs each previous snapshot to exist."""
        readings = self.readings(session)
        total = 0
        for i, at in enumerate(readings):
            if not self.metrics_ready(at):
                break                   # never snapshot a reading whose metrics have not landed: it is immutable
            total += self.process_reading(session, at, first=(i == 0), pause=pause)
        return total

    def run(self, stop, interval=10.0, pause=0.002, after=None):
        """`after(session)` runs once new snapshots land, and at least once a minute - the signal-to-noise audit."""
        last = 0.0
        while not stop.wait(interval):
            try:
                session = self.newest_session()
                if session:
                    n = self.process_session(session, pause=pause)
                    if n:
                        LOG.info('snapshots: wrote %d for %s', n, session)
                    if after and (n or time.monotonic() - last > 60):
                        after(session)
                        last = time.monotonic()
            except Exception as error:  # noqa: BLE001
                LOG.warning('snapshot worker cycle failed: %s', error)


def rows_for_api(rows):
    """Stored snapshots as the API serves them: the chained reading, plus where it came from."""
    out = []
    for r in rows:
        item = {'reading_at': r['reading_at'], 'status': r['status'], 'reason': r['reason'],
                'underlying': r['underlying'], 'expiry': r['expiry'], 'spot': r['spot'],
                'atm_strike': r['atm_strike'], 'engine_version': r['engine_version'],
                'rules_version': r['rules_version'], 'snapshot_id': r['id'],
                'previous_snapshot_id': r['previous_snapshot_id']}
        if r['status'] == 'ok':
            item['state'] = json.loads(r['chained'])
            item['contracts'] = json.loads(r['contracts'] or '[]')
        out.append(item)
    return out


RUN_STATES = {'building', 'appeared', 'held', 'strengthened', 'broadened', 'concentrated', 'shifted', 'slowed',
              'reversing', 'mixed'}


def events_from_snapshots(store, session, at, engine_version, expected):
    """THE MARKET LIST FROM THE SAME SNAPSHOTS THE PANE READS. Found live 21 Sep 10:30: the list said "Call positions
    begin building · since 10:30" while the pane's history showed calls building since 10:15 — two engines, two
    answers. Built only when EVERY instrument's snapshot for the reading exists; otherwise None, and the caller
    keeps its own pass."""
    rows = store.latest_per_underlying(session, at, engine_version)
    if not rows or len(rows) < expected:
        return None
    out = []
    for r in rows:
        if r['status'] != 'ok':
            out.append({'underlying': r['underlying'], 'at': at, 'headline': 'No comparable values captured at this reading',
                        'state': 'not_observed', 'live': False, 'weight': 0, 'measured': 0, 'lead': None,
                        'side': None, 'row': None, 'first_at': None, 'expiry': r['expiry'], 'spot': r['spot']})
            continue
        s = json.loads(r['chained'])
        out.append({'underlying': r['underlying'], 'at': at, 'previous_at': s.get('previous_timestamp'),
                    'headline': s.get('plain_language_read') or '', 'state': s.get('state'),
                    'behaviour': s.get('behaviour'), 'side': s.get('side'), 'row': s.get('row'),
                    'strikes': s.get('strike_range') or [], 'lead': s.get('leading_strike'),
                    'first_at': s.get('persistence_since'), 'elapsed_minutes': s.get('persistence_minutes'),
                    'scans': s.get('consecutive_readings') or 0, 'live': s.get('state') in RUN_STATES,
                    'weight': s.get('weight') or 0, 'measured': 1, 'expiry': r['expiry'], 'spot': r['spot'],
                    'engine_version': r['engine_version'], 'snapshot_id': r['id']})
    out.sort(key=lambda x: (0 if x.get('live') else 1, -(x.get('weight') or 0), x['underlying']))
    return out
