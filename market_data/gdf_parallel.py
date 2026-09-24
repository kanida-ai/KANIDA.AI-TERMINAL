"""GDF TRIAL — parallel capture. Production stays on Kite; this process keeps a SEPARATE comparison store.

    python -m market_data.gdf_parallel run     --out kanida-app/docs/gdf_trial     # 09:10 -> 16:00 IST, one vendor session
    python -m market_data.gdf_parallel compare --out kanida-app/docs/gdf_trial     # reads the store only; safe while live

One process, one vendor session (the key allows exactly one), three jobs interleaved on it:

1. DELAY PROOF - the probe set is polled every 60 s (15-min bars) and the 1-minute set every 60 s; each poll row goes to
   gdf_polls_<day>.csv in the same format as gdf_live_trial, so its report still derives delay and candle availability.
2. VENDOR SWEEP - at every quarter hour + 1 min, all 200 trial symbols (100 NFO, 95 NSE, 5 NSE_IDX) are fetched and
   every bar is upserted into vendor_shadow.db with the time we FIRST saw it, the time we last saw it, its first
   values and how many times its values were revised.
3. KITE COPY - each production Kite reading of the same instruments is copied (read-only) from db/derivatives.db
   into the same store. Kite is NOT called again; the production capture is not touched.

The instrument set is fixed for the whole day at start (the symbol limit is per key), centred on Kite's previous-session
close - chosen before today's first bar exists, so nothing in it looks ahead.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import statistics
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .gdf_catalogue import catalogue
from .gdf_experiments import round2, round3, round4, round5, round6, round7, round8, round9, round10, run_all as experiments
from .gdf_live_trial import FIELDS, report as delay_report
from .gdf_provider import GdfProvider, INDEX_NAMES, INDEX_UNDERLYINGS, gdf_identifier
from .types import IST

ROOT = Path(__file__).resolve().parents[1]
KITE_STORE = ROOT / 'db' / 'derivatives.db'

SCHEMA = """
create table if not exists universe(ident text primary key, exchange text, underlying text, kind text, strike real,
    expiry text, kite_token integer, role text, chosen_at text, centre real);
create table if not exists vendor_bars(ident text, bar_start text, open real, high real, low real, close real,
    volume integer, oi integer, first_seen text, last_seen text, first_close real, first_oi integer,
    first_volume integer, revisions integer default 0, primary key(ident, bar_start));
create table if not exists kite_rows(ident text, reading text, price real, oi integer, volume integer,
    copied_at text, primary key(ident, reading));
create table if not exists sweeps(started text primary key, finished text, symbols integer, calls integer,
    errors integer, note text);
create table if not exists errors(at text, ident text, fn text, error text);
create table if not exists snap_bars(ident text, exchange text, period integer, bar_start text, open real, high real,
    low real, close real, volume integer, oi integer, first_seen text, last_seen text, revisions integer default 0,
    primary key(ident, period, bar_start));
create table if not exists fast_polls(started text, finished text, period integer, calls integer, rows integer,
    errors integer, newest_ist text, oldest_ist text, wall_ms integer, max_call_ms integer);
create table if not exists timings(at text, fn text, symbol text, periodicity text, ttfb_ms integer, transfer_ms integer, bytes integer);
create table if not exists es_bars(ident text, exchange text, period integer, bar_start text, open real, high real,
    low real, close real, volume integer, oi integer, first_seen text, last_seen text, revisions integer default 0,
    primary key(ident, period, bar_start));
create table if not exists es_polls(poll_ist text, exchange text, period integer, groups integer, rows integer,
    newest_group_ist text, reply_ms integer, reply_bytes integer);
create table if not exists bar_checks(ident text, bar_start text, checked_at text, minutes integer,
    m_open real, m_high real, m_low real, m_close real, m_volume integer, m_oi integer,
    b_first_seen text, b_open real, b_high real, b_low real, b_close real, b_volume integer, b_oi integer,
    b_first_close real, b_first_volume integer, primary key(ident, bar_start));
"""


def ist_now():
    return datetime.now(timezone.utc).astimezone(IST)


def _stamp(dt=None):
    return (dt or ist_now()).strftime('%Y-%m-%d %H:%M:%S')


# --- the instrument set ---------------------------------------------------------------------------------------------
def choose_universe(kite: sqlite3.Connection, today: date):
    """100 NFO + 95 NSE + 5 NSE_IDX, from Kite's own contract table and its previous-session close."""
    prev = kite.execute("""select max(captured_at) from underlying_snapshots where mark_kind='bar_close'
                           and substr(captured_at,1,10) < ?""", (today.isoformat(),)).fetchone()[0]
    spot = dict(kite.execute('select underlying, spot from underlying_snapshots where captured_at=?', (prev,)))
    rows, chosen = [], _stamp()

    def ladder(u, side_n):
        exp = kite.execute("""select min(expiry) from contracts where underlying=? and instrument_type='CE'
                              and expiry>=?""", (u, today.isoformat())).fetchone()[0]
        strikes = sorted({r[0] for r in kite.execute("""select strike from contracts where underlying=? and expiry=?
                          and instrument_type='CE'""", (u, exp))})
        atm = min(strikes, key=lambda s: (abs(s - spot[u]), -s))
        i = strikes.index(atm)
        for k in strikes[max(0, i - side_n): i + side_n + 1]:
            for kind in ('CE', 'PE'):
                tok = kite.execute("""select instrument_token from contracts where underlying=? and expiry=? and strike=?
                                      and instrument_type=?""", (u, exp, k, kind)).fetchone()
                role = 'probe' if k == atm and u in ('NIFTY', 'BANKNIFTY') else 'sweep'
                rows.append((gdf_identifier(u, kind, date.fromisoformat(exp), k), 'NFO', u, kind, k, exp,
                             tok[0] if tok else None, role, chosen, atm))

    ladder('NIFTY', 8)
    ladder('BANKNIFTY', 8)
    for u in ('NIFTY', 'BANKNIFTY', 'RELIANCE', 'HDFCBANK'):
        tok, exp = kite.execute("""select instrument_token, expiry from contracts where underlying=? and instrument_type='FUT'
                                   and expiry>=? order by expiry limit 1""", (u, today.isoformat())).fetchone()
        rows.append((gdf_identifier(u, 'FUT', date.fromisoformat(exp)), 'NFO', u, 'FUT', None, exp, tok,
                     'probe' if u != 'HDFCBANK' else 'sweep', chosen, spot[u]))
    ladder('RELIANCE', 3)
    ladder('HDFCBANK', 3)
    # 95 equities: RELIANCE and HDFCBANK first (their F&O is in the set), then by previous-session F&O volume
    active = [u for u, in kite.execute("""select underlying from underlying_snapshots where captured_at=?
                    order by coalesce(total_ce_volume,0)+coalesce(total_pe_volume,0) desc""", (prev,))
              if u not in INDEX_UNDERLYINGS]
    stocks = ['RELIANCE', 'HDFCBANK'] + [u for u in active if u not in ('RELIANCE', 'HDFCBANK')]
    for u in stocks[:95]:
        rows.append((u, 'NSE', u, 'EQ', None, None, None, 'probe' if u in ('RELIANCE', 'HDFCBANK') else 'sweep',
                     chosen, spot.get(u)))
    for u, name in INDEX_NAMES.items():
        rows.append((name, 'NSE_IDX', u, 'IDX', None, None, None,
                     'probe' if u in ('NIFTY', 'BANKNIFTY') else 'sweep', chosen, spot.get(u)))
    counts = {e: sum(1 for r in rows if r[1] == e) for e in ('NFO', 'NSE', 'NSE_IDX')}
    assert counts['NFO'] <= 100 and counts['NSE'] <= 95 and counts['NSE_IDX'] <= 5, counts
    return rows, prev


# --- store writes ---------------------------------------------------------------------------------------------------
def in_session(bar_start: datetime) -> bool:
    return '09:15' <= bar_start.strftime('%H:%M') < '15:30'


def upsert_bars(db, ident, rows, seen):
    for r in rows:
        if not in_session(r['bar_start']):
            continue
        key = (ident, _stamp(r['bar_start']))
        vals = (r.get('Open'), r.get('High'), r.get('Low'), r.get('Close'), r.get('TradedQty'), r.get('OpenInterest'))
        old = db.execute('select open,high,low,close,volume,oi from vendor_bars where ident=? and bar_start=?', key).fetchone()
        if old is None:
            db.execute("""insert into vendor_bars values(?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
                       key + vals + (seen, seen, vals[3], vals[5], vals[4]))
        elif tuple(old) != vals:
            db.execute("""update vendor_bars set open=?,high=?,low=?,close=?,volume=?,oi=?,last_seen=?,
                          revisions=revisions+1 where ident=? and bar_start=?""", vals + (seen,) + key)
        else:
            db.execute('update vendor_bars set last_seen=? where ident=? and bar_start=?', (seen,) + key)


def copy_kite(db, kite, today: str):
    """Every production Kite reading of today not yet copied - read-only against the production store."""
    have = {r[0] for r in db.execute('select distinct reading from kite_rows')}
    readings = [r[0] for r in kite.execute("""select distinct captured_at from underlying_snapshots
                    where substr(captured_at,1,10)=? and mark_kind='bar_close' order by 1""", (today,))
                if r[0] not in have]
    uni = db.execute('select ident, exchange, underlying, kite_token from universe').fetchall()
    now, n = _stamp(), 0
    for t in readings:
        spot = dict(kite.execute('select underlying, spot from underlying_snapshots where captured_at=?', (t,)))
        snap = {r[0]: r[1:] for r in kite.execute(
            'select instrument_token, last_price, oi, volume from snapshots where captured_at=?', (t,))}
        for ident, exch, u, tok in uni:
            if exch == 'NFO':
                if tok in snap:
                    db.execute('insert or ignore into kite_rows values(?,?,?,?,?,?)', (ident, t) + tuple(snap[tok]) + (now,))
                    n += 1
            elif u in spot:
                db.execute('insert or ignore into kite_rows values(?,?,?,?,?,?)', (ident, t, spot[u], None, None, now))
                n += 1
    db.commit()
    return readings, n


# --- is the 15-minute bar the WHOLE quarter? rebuild it from the (delayed) 1-minute bars ---------------------------
def quarter_bars(today, now):
    """15-minute bar starts whose every 1-minute piece the delayed feed has released (bar end + 15 min + 1)."""
    out, t = [], datetime(today.year, today.month, today.day, 9, 15, tzinfo=IST)
    while t + timedelta(minutes=31) <= now and t.strftime('%H:%M') < '15:30':
        out.append(_stamp(t))
        t += timedelta(minutes=15)
    return out


def check_bar(p, db, checks, bar):
    b0 = datetime.strptime(bar, '%Y-%m-%d %H:%M:%S').replace(tzinfo=IST)
    for ident, exch in checks:
        try:
            m = [r for r in _history(p, ident, exch, 1, b0) if b0 <= r['bar_start'] < b0 + timedelta(minutes=15)]
        except Exception as error:  # noqa: BLE001
            db.execute('insert into errors values(?,?,?,?)', (_stamp(), ident, 'bar_check', str(error)[:160]))
            continue
        agg = ((m[0]['Open'], max(r['High'] for r in m), min(r['Low'] for r in m), m[-1]['Close'],
                sum(r.get('TradedQty') or 0 for r in m), m[-1].get('OpenInterest')) if m else (None,) * 6)
        b = db.execute("""select first_seen, open, high, low, close, volume, oi, first_close, first_volume from vendor_bars
                          where ident=? and bar_start=?""", (ident, bar)).fetchone() or (None,) * 9
        db.execute('insert or replace into bar_checks values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (ident, bar, _stamp(), len(m)) + agg + tuple(b))
    db.commit()


# --- the fast lane: named-symbol snapshots, 25 per call ---------------------------------------------------------------
def fast_lane(p, db, batches, period):
    """One refresh of every trial symbol. 15-minute rows also feed vendor_bars (same bar as GetHistory - verified 22 Sep)."""
    t0, started, rows, errors, stamps, slowest = time.monotonic(), _stamp(), 0, 0, [], 0
    for exch, ids in batches:
        c0 = time.monotonic()
        req = {'MessageType': 'GetSnapshot', 'Exchange': exch, 'Periodicity': 'MINUTE', 'Period': period,
               'isShortIdentifiers': 'false', 'InstrumentIdentifiers': [{'Value': i} for i in ids]}
        try:
            res = p.call(req, lambda j: j.get('MessageType') == 'SnapshotResult'
                         or ('Request' in j and (j['Request'] or {}).get('MessageType') == 'GetSnapshot'))
        except Exception as error:  # noqa: BLE001
            errors += 1
            db.execute('insert into errors values(?,?,?,?)', (_stamp(), exch, f'fast{period}', str(error)[:160]))
            continue
        slowest = max(slowest, int((time.monotonic() - c0) * 1000))
        seen, got = _stamp(), [r for r in (res.get('Result') or []) if isinstance(r, dict) and r.get('LastTradeTime')]
        for r in got:
            r['bar_start'] = datetime.fromtimestamp(int(r['LastTradeTime']), timezone.utc).astimezone(IST)
            stamps.append(r['bar_start'])
        db.executemany("""insert into snap_bars values(?,?,?,?,?,?,?,?,?,?,?,?,0)
            on conflict(ident, period, bar_start) do update set last_seen=excluded.last_seen,
            revisions=revisions + (close is not excluded.close or volume is not excluded.volume or oi is not excluded.oi),
            open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close,
            volume=excluded.volume, oi=excluded.oi""",
            [(r.get('InstrumentIdentifier'), exch, period, _stamp(r['bar_start']), r.get('Open'), r.get('High'),
              r.get('Low'), r.get('Close'), r.get('TradedQty'), r.get('OpenInterest'), seen, seen) for r in got])
        if period == 15:
            for r in got:
                upsert_bars(db, r.get('InstrumentIdentifier'), [r], seen)
        rows += len(got)
    db.execute('insert into fast_polls values(?,?,?,?,?,?,?,?,?,?)',
               (started, _stamp(), period, len(batches), rows, errors, _stamp(max(stamps)) if stamps else '',
                _stamp(min(stamps)) if stamps else '', int((time.monotonic() - t0) * 1000), slowest))
    db.commit()


# --- the live loop --------------------------------------------------------------------------------------------------
def _history(p, ident, exch, period, since):
    req = {'MessageType': 'GetHistory', 'Exchange': exch, 'InstrumentIdentifier': ident, 'Periodicity': 'MINUTE',
           'Period': period, 'From': int(since.timestamp()), 'To': int(ist_now().timestamp()) + 60}
    rows = p.call(req, lambda j: 'Result' in j and 'Request' in j
                  and (j['Request'] or {}).get('InstrumentIdentifier') == ident).get('Result') or []
    for r in rows:
        r['bar_start'] = datetime.fromtimestamp(int(r['LastTradeTime']), timezone.utc).astimezone(IST)
    rows.sort(key=lambda r: r['bar_start'])
    return rows


def run(out: Path, start='09:10', stop='16:00'):
    out.mkdir(parents=True, exist_ok=True)
    today = ist_now().date()
    db = sqlite3.connect(out / 'vendor_shadow.db')
    db.executescript(SCHEMA)
    kite = sqlite3.connect(f'file:{KITE_STORE.as_posix()}?mode=ro', uri=True)
    if not db.execute('select count(*) from universe').fetchone()[0]:
        rows, prev = choose_universe(kite, today)
        db.executemany('insert into universe values(?,?,?,?,?,?,?,?,?,?)', rows)
        db.commit()
        print(f'universe: {len(rows)} symbols centred on Kite close {prev}', flush=True)
    uni = db.execute('select ident, exchange, kind, role from universe order by exchange, ident').fetchall()
    probes = [(i, e) for i, e, k, r in uni if r == 'probe']
    want = ('NIFTY 50', 'RELIANCE', 'FUTIDX_NIFTY_', 'OPTIDX_NIFTY_')     # one of each kind, as on 21 Sep
    minute = [next((i, e) for i, e in probes if i.startswith(w)) for w in want if any(i.startswith(w) for i, _ in probes)]
    (out / 'probe_symbols.txt').write_text('\n'.join(f'{s}\t{e}' for s, e in probes), encoding='utf-8')
    path = out / f'gdf_polls_{today:%Y-%m-%d}.csv'
    new = not path.exists()
    fh = open(path, 'a', newline='', encoding='utf-8')
    w = csv.DictWriter(fh, fieldnames=FIELDS)
    if new:
        w.writeheader()
    while ist_now().strftime('%H:%M') < start:
        time.sleep(20)
    day_open = datetime(today.year, today.month, today.day, 9, 0, tzinfo=IST)
    p = GdfProvider(reply_timeout=60)   # whole-exchange replies take 20-29 s; 20 s timed them out (22 Sep)
    catalogued = (out / f'GDF_FIELDS_{today:%Y-%m-%d}.json').exists()
    pick = lambda pre: next(i for i, _ in probes if i.startswith(pre))
    checks = probes + [tuple(r) for r in db.execute("""select ident, exchange from universe where underlying='RELIANCE'
                                                  and kind in ('CE','PE') and strike=centre""")]
    checked = {r[0] for r in db.execute('select distinct bar_start from bar_checks')}
    queue, last_probe, last_exch, last_min, swept, sweep = [], 0.0, 0.0, 0.0, set(), None
    last_fast15, last_nse, t_start = 0.0, -1, time.monotonic()
    batches = [(e, [i for i, x, k, r in uni if x == e][n:n + 25]) for e in ('NFO', 'NSE', 'NSE_IDX')
               for n in range(0, sum(1 for _, x, _, _ in uni if x == e), 25)]
    try:
        while ist_now().strftime('%H:%M') < stop:
            loop = time.monotonic()
            now = ist_now()
            # FAST LANE (replaces the one-symbol-at-a-time sweep, 22 Sep 10:5x): every trial symbol via GetSnapshot in
            # batches of 25 (the measured per-call cap) - 1-minute bars every loop; the completed 15-minute bar every loop
            # in the 3 minutes after each quarter close, else every 5 min
            try:
                fast_lane(p, db, batches, 1)
                if now.minute % 15 <= 2 or loop - last_fast15 >= 300:
                    fast_lane(p, db, batches, 15)
                    last_fast15 = loop
            except Exception as error:  # noqa: BLE001
                db.execute('insert into errors values(?,?,?,?)', (_stamp(), 'fast_lane', 'loop', str(error)[:160]))
            jobs = []
            # 15-min probes every minute around each quarter close (when a new bar lands), else every 5 min
            if loop - last_probe >= 300:
                jobs += [('GetHistory', s, e, '15minute') for s, e in probes]
                last_probe = loop
            if loop - last_min >= 120:
                jobs += [('GetHistory', s, e, '1minute') for s, e in minute]
                last_min = loop
            # the 30-second design under test: whole-exchange 1-minute snapshots - NFO every 30 s, NSE / NSE_IDX every 60 s
            # full-chain lane: whole-exchange snapshots (~28 s each) every 2 min for NFO, 4 min for NSE, so the fast lane
            # keeps its 30 s cadence. NSE_IDX is not served by the exchange snapshot (0 rows, 22 Sep 09:56).
            if loop - last_exch >= 120:
                jobs.append(('GetExchangeSnapshot', 'NFO', 'NFO', '1minute'))
                if int(loop - t_start) // 240 != last_nse:
                    jobs.append(('GetExchangeSnapshot', 'NSE', 'NSE', '1minute'))
                    last_nse = int(loop - t_start) // 240
                last_exch = loop
            for fn, sym, exch, per in jobs:
                row = {'fn': fn, 'symbol': sym, 'exchange': exch, 'periodicity': per, 'error': ''}
                t0 = time.monotonic()
                try:
                    if fn == 'GetHistory':
                        res = _history(p, sym, exch, 15 if per == '15minute' else 1,
                                       now - timedelta(days=4) if per == '15minute' else day_open)
                        top = res[-1] if res else {}
                        row.update(bars=len(res), close=top.get('Close'), oi=top.get('OpenInterest'),
                                   volume=top.get('TradedQty'), newest_bar_ist=_stamp(top['bar_start']) if top else '')
                        if per == '15minute':
                            upsert_bars(db, sym, res, _stamp())
                    else:
                        per_n = 15 if per == '15minute' else 1
                        req = {'MessageType': 'GetExchangeSnapshot', 'Exchange': exch, 'Periodicity': 'MINUTE', 'Period': per_n}
                        res = p.call(req, lambda j, e=exch: 'Result' in j and (j.get('Request') or {}).get('MessageType')
                                     == 'GetExchangeSnapshot' and (j.get('Request') or {}).get('Exchange') == e)
                        groups = res.get('Result') or []
                        rows = [dict(r, _g=g.get('LastTradeTime')) for g in groups for r in (g.get('Result') or [])]
                        newest = max((g.get('LastTradeTime') or 0) for g in groups) if groups else 0
                        seen = _stamp()
                        db.executemany("""insert into es_bars values(?,?,?,?,?,?,?,?,?,?,?,?,0)
                            on conflict(ident, period, bar_start) do update set last_seen=excluded.last_seen,
                            revisions=revisions + (close is not excluded.close or volume is not excluded.volume
                                                   or oi is not excluded.oi),
                            open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close,
                            volume=excluded.volume, oi=excluded.oi""",
                            [(r.get('InstrumentIdentifier'), exch, per_n, _stamp(datetime.fromtimestamp(r.get('LastTradeTime') or r['_g'], timezone.utc).astimezone(IST)),
                              r.get('Open'), r.get('High'), r.get('Low'), r.get('Close'), r.get('TradedQty'),
                              r.get('OpenInterest'), seen, seen) for r in rows])
                        size = len(json.dumps(res, separators=(',', ':')))
                        newest_s = (datetime.fromtimestamp(newest, timezone.utc).astimezone(IST).strftime('%Y-%m-%d %H:%M:%S')
                                    if newest else '')
                        db.execute('insert into es_polls values(?,?,?,?,?,?,?,?)',
                                   (seen, exch, per_n, len(groups), len(rows), newest_s,
                                    int((time.monotonic() - t0) * 1000), size))
                        row.update(bars=len(rows), volume=len({r.get('InstrumentIdentifier') for r in rows}),
                                   newest_bar_ist=newest_s, oi=size,
                                   close=sum(1 for r in rows if str(r.get('InstrumentIdentifier', '')).startswith('OPT')))
                except Exception as error:  # noqa: BLE001 - a failed poll is a data point, not a crash
                    row['error'] = str(error)[:160]
                    db.execute('insert into errors values(?,?,?,?)', (_stamp(), sym, fn, row['error']))
                row['reply_ms'] = int((time.monotonic() - t0) * 1000)
                row['poll_ist'] = _stamp()
                if not row['error'] and p.last_timing:
                    db.execute('insert into timings values(?,?,?,?,?,?,?)', (row['poll_ist'], fn, sym, per,
                               p.last_timing['ttfb_ms'], p.last_timing['transfer_ms'], p.last_timing['bytes']))
                w.writerow(row)
                fh.flush()
            db.commit()
            if 3 <= now.minute % 15 <= 12:
                if not catalogued:           # every field the trial returns, once a day
                    catalogued = True
                    try:
                        catalogue(p, out, pick('OPTIDX_NIFTY_'), pick('OPTIDX_BANKNIFTY_'), pick('FUTIDX_NIFTY_'), p._api_key)
                        print(f'{_stamp()} field catalogue written', flush=True)
                    except Exception as error:  # noqa: BLE001 - the catalogue never stops the capture
                        print(f'{_stamp()} field catalogue failed: {error}', flush=True)
                if catalogued and not (out / f'GDF_EXPERIMENTS_{today:%Y-%m-%d}.json').exists()                         and 3 <= ist_now().minute % 15 <= 9:      # 150 s push test + plain ws, done before the next close
                    try:
                        experiments(p, out)
                        print(f'{_stamp()} experiments written', flush=True)
                    except Exception as error:  # noqa: BLE001
                        print(f'{_stamp()} experiments failed: {error}', flush=True)
                        (out / f'GDF_EXPERIMENTS_{today:%Y-%m-%d}.json').write_text('{"error": "%s"}' % str(error)[:100])
                if (out / f'GDF_EXPERIMENTS_{today:%Y-%m-%d}.json').exists() and 3 <= ist_now().minute % 15 <= 5                         and ist_now().strftime('%H:%M') >= '10:18'                         and not (out / f'GDF_EXPERIMENTS2_{today:%Y-%m-%d}.json').exists():   # ~6 min, done before 10:30
                    try:
                        round2(p, out)
                        print(f'{_stamp()} experiments round 2 written', flush=True)
                    except Exception as error:  # noqa: BLE001
                        print(f'{_stamp()} experiments round 2 failed: {error}', flush=True)
                        (out / f'GDF_EXPERIMENTS2_{today:%Y-%m-%d}.json').write_text('{"error": "%s"}' % str(error)[:100])
                if 3 <= ist_now().minute % 15 <= 9 and ist_now().strftime('%H:%M') >= '10:33'                         and not (out / f'GDF_EXPERIMENTS3_{today:%Y-%m-%d}.json').exists():   # 100-symbol GetSnapshot timing
                    try:
                        round3(p, out, [(i, e) for i, e, k, r in uni])
                        print(f'{_stamp()} experiments round 3 written', flush=True)
                    except Exception as error:  # noqa: BLE001
                        print(f'{_stamp()} experiments round 3 failed: {error}', flush=True)
                        (out / f'GDF_EXPERIMENTS3_{today:%Y-%m-%d}.json').write_text('{"error": "%s"}' % str(error)[:100])
                if 3 <= ist_now().minute % 15 <= 9 and (out / f'GDF_EXPERIMENTS3_{today:%Y-%m-%d}.json').exists()                         and not (out / f'GDF_EXPERIMENTS4_{today:%Y-%m-%d}.json').exists():   # GetSnapshot cap + batching
                    try:
                        round4(p, out, [(i, e) for i, e, k, r in uni])
                        print(f'{_stamp()} experiments round 4 written', flush=True)
                    except Exception as error:  # noqa: BLE001
                        print(f'{_stamp()} experiments round 4 failed: {error}', flush=True)
                        (out / f'GDF_EXPERIMENTS4_{today:%Y-%m-%d}.json').write_text('{"error": "%s"}' % str(error)[:100])
                if 3 <= ist_now().minute % 15 <= 9 and not (out / f'GDF_EXPERIMENTS5_{today:%Y-%m-%d}.json').exists():
                    try:                                   # push subscription, with InstrumentIdentifier as the vendor said
                        round5(p, out, [i for i, e in probes if e == 'NFO'])
                        print(f'{_stamp()} experiments round 5 written', flush=True)
                    except Exception as error:  # noqa: BLE001
                        print(f'{_stamp()} experiments round 5 failed: {error}', flush=True)
                        (out / f'GDF_EXPERIMENTS5_{today:%Y-%m-%d}.json').write_text('{"error": "%s"}' % str(error)[:100])
                if 3 <= ist_now().minute % 15 <= 9 and (out / f'GDF_EXPERIMENTS5_{today:%Y-%m-%d}.json').exists()                         and not (out / f'GDF_EXPERIMENTS6b_{today:%Y-%m-%d}.json').exists():
                    try:
                        round6(p, out)
                        print(f'{_stamp()} experiments round 6 written', flush=True)
                    except Exception as error:  # noqa: BLE001
                        print(f'{_stamp()} experiments round 6 failed: {error}', flush=True)
                        (out / f'GDF_EXPERIMENTS6b_{today:%Y-%m-%d}.json').write_text('{"error": "%s"}' % str(error)[:100])
                if ist_now().strftime('%H:%M') >= '09:33' and ist_now().minute % 15 in (3, 4)                         and not (out / f'GDF_PUSH_TEST_{today:%Y-%m-%d}.json').exists():
                    subs = [(e, i) for i, e in probes if i.startswith(('FUTIDX_NIFTY_', 'FUTIDX_BANKNIFTY_', 'OPTIDX_NIFTY_'))]
                    subs += [('NSE', 'RELIANCE')]           # 5-6 subscriptions: futures, ATM options, a stock
                    try:                                   # ~9 min: the fast lane pauses while it runs
                        d = round7(p, out, subs)
                        print(f'{_stamp()} push test written: {d["pushes"]} pushes', flush=True)
                    except Exception as error:  # noqa: BLE001
                        print(f'{_stamp()} push test failed: {error}', flush=True)
                        (out / f'GDF_PUSH_TEST_{today:%Y-%m-%d}.json').write_text('{"error": "%s"}' % str(error)[:100])
                if not (out / f'GDF_BULK_{today:%Y-%m-%d}.json').exists() and 3 <= ist_now().minute % 15 <= 9:
                    try:                                   # the bulk functions from the vendor's docs, ~6 min
                        atm = db.execute("select centre from universe where underlying='NIFTY' and kind='CE'").fetchone()[0]
                        round8(p, out, int(atm))
                        print(f'{_stamp()} bulk probe written', flush=True)
                    except Exception as error:  # noqa: BLE001
                        print(f'{_stamp()} bulk probe failed: {error}', flush=True)
                        (out / f'GDF_BULK_{today:%Y-%m-%d}.json').write_text('{"error": "%s"}' % str(error)[:100])
                if (out / f'GDF_BULK_{today:%Y-%m-%d}.json').exists() and 3 <= ist_now().minute % 15 <= 9                         and not (out / f'GDF_BULK2_{today:%Y-%m-%d}.json').exists():
                    try:
                        round9(p, out, [i for i, e in probes if e == 'NFO'])
                        print(f'{_stamp()} bulk probe 2 written', flush=True)
                    except Exception as error:  # noqa: BLE001
                        print(f'{_stamp()} bulk probe 2 failed: {error}', flush=True)
                        (out / f'GDF_BULK2_{today:%Y-%m-%d}.json').write_text('{"error": "%s"}' % str(error)[:100])
                if not (out / f'GDF_FILTERS_{today:%Y-%m-%d}.json').exists() and 3 <= ist_now().minute % 15 <= 9:
                    try:                                   # does a Product filter make the whole-exchange call faster?
                        round10(p, out)
                        print(f'{_stamp()} filter test written', flush=True)
                    except Exception as error:  # noqa: BLE001
                        print(f'{_stamp()} filter test failed: {error}', flush=True)
                        (out / f'GDF_FILTERS_{today:%Y-%m-%d}.json').write_text('{"error": "%s"}' % str(error)[:100])
                due = [b for b in quarter_bars(today, now) if b not in checked]
                if due:
                    checked.add(due[0])
                    check_bar(p, db, checks, due[0])
                    print(f'{_stamp()} bar check {due[0]} done', flush=True)
            # the sweep takes whatever of this 30 s the probes left
            while queue and time.monotonic() - loop < 28:
                ident, exch = queue.pop(0)
                sweep['calls'] += 1
                try:
                    upsert_bars(db, ident, _history(p, ident, exch, 15, day_open), _stamp())
                except Exception as error:  # noqa: BLE001
                    sweep['errors'] += 1
                    db.execute('insert into errors values(?,?,?,?)', (_stamp(), ident, 'sweep', str(error)[:160]))
            if sweep and not queue:
                db.execute('insert or replace into sweeps values(?,?,?,?,?,?)',
                           (sweep['started'], _stamp(), sweep['n'], sweep['calls'], sweep['errors'], ''))
                sweep = None
            db.commit()
            try:
                readings, n = copy_kite(db, kite, today.isoformat())
                if readings:
                    print(f'{_stamp()} kite copied {readings} ({n} rows)', flush=True)
            except sqlite3.Error as error:  # production store busy: try again next loop
                print(f'{_stamp()} kite copy deferred: {error}', flush=True)
            time.sleep(max(0.0, 30 - (time.monotonic() - loop)))
    finally:
        p.close()
        fh.close()
        db.commit()
        db.close()


# --- the comparison -------------------------------------------------------------------------------------------------
GROUPS = [('NIFTY options', "u.underlying='NIFTY' and u.kind in ('CE','PE')"),
          ('BANKNIFTY options', "u.underlying='BANKNIFTY' and u.kind in ('CE','PE')"),
          ('Stock options', "u.underlying in ('RELIANCE','HDFCBANK') and u.kind in ('CE','PE')"),
          ('Futures', "u.kind='FUT'"), ('Equities (spot)', "u.kind='EQ'"), ('Indices (spot)', "u.kind='IDX'")]


def compare(out: Path, day: str | None = None):
    day = day or ist_now().strftime('%Y-%m-%d')
    db = sqlite3.connect(f'file:{(out / "vendor_shadow.db").as_posix()}?mode=ro', uri=True)
    lines = [f'# Vendor vs Kite, same instruments, same readings — {day}', '',
             'Pairing: the vendor bar STARTING at T−15m (the vendor stamps bar starts) against the Kite reading at T. '
             'Kite\'s value is a last trade taken seconds after the mark; the vendor\'s is the bar close. '
             '"Seen" is when this process first received that vendor bar, in IST.', '',
             '| Reading T | Group | Pairs | Vendor bars missing | Price median abs diff | Within 0.5% | OI identical | Vendor first seen (median, min after T) |',
             '|---|---|---|---|---|---|---|---|']
    readings = [r[0] for r in db.execute('select distinct reading from kite_rows where substr(reading,1,10)=? order by 1', (day,))]
    for t in readings:
        bar = _stamp(datetime.strptime(t, '%Y-%m-%d %H:%M:%S') - timedelta(minutes=15))
        for name, where in GROUPS:
            q = f"""select k.price, k.oi, v.close, v.oi, v.first_seen from universe u
                    join kite_rows k on k.ident=u.ident and k.reading=?
                    left join vendor_bars v on v.ident=u.ident and v.bar_start=? where {where}"""
            rows = db.execute(q, (t, bar)).fetchall()
            if not rows:
                continue
            paired = [r for r in rows if r[2] is not None]
            px = [abs(r[2] - r[0]) / r[0] * 100 for r in paired if r[0]]
            oi = [r for r in paired if r[1] is not None and r[3] is not None]
            seen = [(datetime.strptime(r[4], '%Y-%m-%d %H:%M:%S') - datetime.strptime(t, '%Y-%m-%d %H:%M:%S'))
                    .total_seconds() / 60 for r in paired]
            lines.append(f"| {t[11:16]} | {name} | {len(paired)}/{len(rows)} | {len(rows) - len(paired)} | "
                         f"{statistics.median(px):.3f}% | {sum(1 for x in px if x <= 0.5)}/{len(px)} | "
                         + (f"{sum(1 for r in oi if r[1] == r[3])}/{len(oi)}" if oi else '—') + ' | '
                         + (f"{statistics.median(seen):+.1f}" if seen else '—') + ' |'
                         if px else f"| {t[11:16]} | {name} | 0/{len(rows)} | {len(rows)} | — | — | — | — |")
    sw = db.execute('select started, finished, symbols, calls, errors from sweeps order by 1').fetchall()
    lines += ['', '## Vendor sweeps', '', '| Started | Finished | Symbols | Errors |', '|---|---|---|---|']
    lines += [f'| {a[11:]} | {b[11:]} | {n} | {e} |' for a, b, n, c, e in sw]
    errs = db.execute('select ident, fn, error, count(*) from errors group by 1,2,3 order by 4 desc limit 20').fetchall()
    if errs:
        lines += ['', '## Vendor errors (top 20 distinct)', ''] + [f'- `{i}` {f}: {e} ×{n}' for i, f, e, n in errs]
    text = '\n'.join(lines) + '\n'
    (out / f'VENDOR_VS_KITE_{day}.md').write_text(text, encoding='utf-8')
    return text


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('mode', choices=('run', 'compare', 'universe'))
    ap.add_argument('--out', required=True)
    ap.add_argument('--day', default=None)
    a = ap.parse_args(argv)
    out = Path(a.out)
    if a.mode == 'universe':            # dry run: prints the set, touches no vendor and writes nothing
        kite = sqlite3.connect(f'file:{KITE_STORE.as_posix()}?mode=ro', uri=True)
        rows, prev = choose_universe(kite, date.fromisoformat(a.day) if a.day else ist_now().date())
        for e in ('NFO', 'NSE', 'NSE_IDX'):
            sel = [r for r in rows if r[1] == e]
            print(e, len(sel), [r[0] for r in sel[:6]], '...')
        print('probes', [r[0] for r in rows if r[7] == 'probe'], 'centred on', prev)
        return
    if a.mode == 'run':
        run(out)
        print(delay_report(out, a.day))
    print(compare(out, a.day))


if __name__ == '__main__':
    main()
