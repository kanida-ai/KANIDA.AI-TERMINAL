"""GDF TRIAL — can we get each new delayed minute in under 30 s? Two alternatives to request/reply polling, measured.

1. PUSH: ``SubscribeSnapshot`` (enabled on the trial for 1/2/5/10/15 minutes). The vendor sends each new snapshot
   when it is ready; we record when every pushed message arrives, how big it is and which minute it carries.
2. PLAIN ws:// instead of wss:// - the same whole-exchange request, to see whether TLS is where the time goes.

Both run inside the one allowed session, once a day, mid-bar, and write GDF_EXPERIMENTS_<day>.json.
"""
from __future__ import annotations

import json
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

from .gdf_provider import _read_env
from .types import IST


def _ist(ts=None):
    return (datetime.fromtimestamp(ts, timezone.utc) if ts else datetime.now(timezone.utc)).astimezone(IST)


def _newest(j):
    best, rows = 0, 0
    for g in j.get('Result') or []:
        if not isinstance(g, dict):
            continue
        best = max(best, g.get('LastTradeTime') or 0)
        inner = g.get('Result')
        if isinstance(inner, list):
            rows += len(inner)
            best = max([best] + [r.get('LastTradeTime') or 0 for r in inner if isinstance(r, dict)])
        else:
            rows += 1
    return best, rows


def subscribe_test(p, exchange='NFO', period=1, seconds=150):
    events = []
    with p._lock:
        ws = p._ws or p._connect()
        p.limiter.wait()
        sent = time.monotonic()
        ws.send(json.dumps({'MessageType': 'SubscribeSnapshot', 'Exchange': exchange, 'Periodicity': 'MINUTE',
                            'Period': period, 'Unsubscribe': 'false'}))
        end = sent + seconds
        while time.monotonic() < end:
            try:
                raw = ws.recv()
            except socket.timeout:
                continue
            at = _ist()
            try:
                j = json.loads(raw)
            except ValueError:
                events.append({'at': at.strftime('%H:%M:%S'), 'text': raw[:160]})
                continue
            if j.get('MessageType') == 'Echo':
                continue
            newest, rows = _newest(j)
            events.append({'at': at.strftime('%H:%M:%S.%f')[:12], 'after_s': round(time.monotonic() - sent, 1),
                           'type': j.get('MessageType'), 'bytes': len(raw), 'rows': rows,
                           'newest_bar_ist': _ist(newest).strftime('%H:%M') if newest else None,
                           'lag_s': round((at - _ist(newest)).total_seconds(), 1) if newest else None,
                           'keys': sorted(j)[:12]})
        p.limiter.wait()
        ws.send(json.dumps({'MessageType': 'SubscribeSnapshot', 'Exchange': exchange, 'Periodicity': 'MINUTE',
                            'Period': period, 'Unsubscribe': 'true'}))
        drain = time.monotonic() + 8
        while time.monotonic() < drain:          # let any in-flight push arrive before normal calls resume
            try:
                ws.recv()
            except socket.timeout:
                continue
    return events


def plain_ws_test(p, calls=2):
    out, plain = [], _read_env().get('GDF_WS_URL_PLAIN', '')
    if not plain:
        return [{'error': 'GDF_WS_URL_PLAIN not configured'}]
    secure = p._url
    try:
        p.close()
        p._url = plain
        for _ in range(calls):
            t0 = time.monotonic()
            res = p.call({'MessageType': 'GetExchangeSnapshot', 'Exchange': 'NFO', 'Periodicity': 'MINUTE', 'Period': 1},
                         lambda j: 'Result' in j and (j.get('Request') or {}).get('MessageType') == 'GetExchangeSnapshot')
            newest, rows = _newest(res)
            out.append(dict(p.last_timing, total_ms=int((time.monotonic() - t0) * 1000), rows=rows,
                            newest_bar_ist=_ist(newest).strftime('%H:%M') if newest else None, at=_ist().strftime('%H:%M:%S')))
    except Exception as error:  # noqa: BLE001 - the finding is the failure
        out.append({'error': str(error)[:160]})
    finally:
        p.close()
        p._url = secure
    return out


def run_all(p, out: Path):
    day = _ist().strftime('%Y-%m-%d')
    doc = {'started_ist': _ist().strftime('%H:%M:%S')}
    try:
        doc['subscribe_NFO_1m'] = subscribe_test(p)
    except Exception as error:  # noqa: BLE001
        doc['subscribe_NFO_1m'] = [{'error': str(error)[:160]}]
    doc['plain_ws'] = plain_ws_test(p)
    (out / f'GDF_EXPERIMENTS_{day}.json').write_text(json.dumps(doc, indent=1), encoding='utf-8')
    return doc


# --- round 2: pipelining, a smaller (filtered) reply, a bigger TCP window, and the push retried for longer -----------
def _es(exchange='NFO', period=1, **extra):
    return dict({'MessageType': 'GetExchangeSnapshot', 'Exchange': exchange, 'Periodicity': 'MINUTE', 'Period': period}, **extra)


def pipeline_test(p, n=3, gap=5.0):
    """Send n requests `gap` s apart WITHOUT waiting, then time each reply: does the server work on them in parallel?"""
    out = []
    with p._lock:
        ws = p._ws or p._connect()
        t0 = time.monotonic()
        for i in range(n):
            p.limiter.wait()
            ws.send(json.dumps(_es(UserTag=f'pipe{i}')))
            out.append({'i': i, 'sent_s': round(time.monotonic() - t0, 1)})
            if i < n - 1:
                time.sleep(gap)
        got, end = 0, time.monotonic() + 120
        while got < n and time.monotonic() < end:
            try:
                raw = ws.recv()
            except socket.timeout:
                continue
            j = json.loads(raw)
            if j.get('MessageType') == 'Echo' or 'Result' not in j:
                continue
            tag = (j.get('Request') or {}).get('UserTag', '?')
            newest, rows = _newest(j)
            out.append({'reply': tag, 'at_s': round(time.monotonic() - t0, 1), 'rows': rows, 'bytes': len(raw),
                        'newest_bar_ist': _ist(newest).strftime('%H:%M') if newest else None})
            got += 1
    return out


def timed(p, req, label):
    t0 = time.monotonic()
    try:
        res = p.call(req, lambda j: 'Result' in j and (j.get('Request') or {}).get('MessageType') == req['MessageType'])
        newest, rows = _newest(res)
        return dict(p.last_timing, label=label, total_ms=int((time.monotonic() - t0) * 1000), rows=rows,
                    newest_bar_ist=_ist(newest).strftime('%H:%M') if newest else None)
    except Exception as error:  # noqa: BLE001
        return {'label': label, 'error': str(error)[:160], 'total_ms': int((time.monotonic() - t0) * 1000)}


def round2(p, out: Path):
    from .wsclient import WebSocket
    day = _ist().strftime('%Y-%m-%d')
    doc = {'started_ist': _ist().strftime('%H:%M:%S')}
    doc['filtered'] = [timed(p, _es(InstrumentType='FUTIDX'), 'NFO futures only'),
                       timed(p, _es(InstrumentType='OPTIDX'), 'NFO index options only'),
                       timed(p, _es(), 'NFO all (control)')]
    doc['pipeline'] = pipeline_test(p)
    factory = p._socket_factory
    try:
        p.close()
        p._socket_factory = lambda u: WebSocket(u, timeout=5.0, rcvbuf=8 << 20)
        doc['rcvbuf_8MB'] = [timed(p, _es(), 'NFO all, 8 MB receive buffer') for _ in range(2)]
    finally:
        p.close()
        p._socket_factory = factory
    try:
        doc['subscribe_NFO_1m_200s'] = subscribe_test(p, seconds=200)
    except Exception as error:  # noqa: BLE001
        doc['subscribe_NFO_1m_200s'] = [{'error': str(error)[:160]}]
    (out / f'GDF_EXPERIMENTS2_{day}.json').write_text(json.dumps(doc, indent=1), encoding='utf-8')
    return doc


# --- round 3: GetSnapshot for up to 100 named symbols in one call - fast enough for the headline chain? -------------
def _snap(exchange, idents, period=1):
    return {'MessageType': 'GetSnapshot', 'Exchange': exchange, 'Periodicity': 'MINUTE', 'Period': period,
            'isShortIdentifiers': 'false', 'InstrumentIdentifiers': [{'Value': i} for i in idents]}


def _snap_rows(j):
    res = j.get('Result') or []
    return [r for r in res if isinstance(r, dict)]


def timed_snap(p, exchange, idents, period, label):
    t0 = time.monotonic()
    try:
        res = p.call(_snap(exchange, idents, period), lambda j: j.get('MessageType') == 'SnapshotResult'
                     or ('Request' in j and (j['Request'] or {}).get('MessageType') == 'GetSnapshot'))
        rows = _snap_rows(res)
        newest = max((r.get('LastTradeTime') or 0) for r in rows) if rows else 0
        return dict(p.last_timing, label=label, asked=len(idents), total_ms=int((time.monotonic() - t0) * 1000),
                    rows=len(rows), newest_bar_ist=_ist(newest).strftime('%H:%M') if newest else None,
                    at=_ist().strftime('%H:%M:%S'))
    except Exception as error:  # noqa: BLE001
        return {'label': label, 'asked': len(idents), 'error': str(error)[:160], 'total_ms': int((time.monotonic() - t0) * 1000)}


def round3(p, out: Path, universe):
    """universe: (ident, exchange) for all 200 trial symbols."""
    day = _ist().strftime('%Y-%m-%d')
    nfo = [i for i, e in universe if e == 'NFO']
    nse = [i for i, e in universe if e == 'NSE']
    idx = [i for i, e in universe if e == 'NSE_IDX']
    doc = {'started_ist': _ist().strftime('%H:%M:%S'), 'runs': []}
    plan = [('NFO', nfo[:10], 1, '10 NFO, 1-min'), ('NFO', nfo[:50], 1, '50 NFO, 1-min'),
            ('NFO', nfo, 1, '100 NFO, 1-min'), ('NFO', nfo, 1, '100 NFO, 1-min (repeat)'),
            ('NFO', nfo, 15, '100 NFO, 15-min'), ('NSE', nse, 1, '95 NSE, 1-min'), ('NSE_IDX', idx, 1, '5 NSE_IDX, 1-min'),
            ('NFO', nfo, 1, '100 NFO, 1-min (repeat 2)')]
    for exch, ids, per, label in plan:
        doc['runs'].append(timed_snap(p, exch, ids, per, label))
    (out / f'GDF_EXPERIMENTS3_{day}.json').write_text(json.dumps(doc, indent=1), encoding='utf-8')
    return doc


# --- round 4: where is GetSnapshot's per-call cap, and how fast are 100 symbols fetched in batches under it? ----------
def round4(p, out: Path, universe):
    day = _ist().strftime('%Y-%m-%d')
    nfo = [i for i, e in universe if e == 'NFO']
    nse = [i for i, e in universe if e == 'NSE']
    doc = {'started_ist': _ist().strftime('%H:%M:%S'), 'cap': [], 'batched': []}
    saved, p.reply_timeout = p.reply_timeout, 15      # 10 symbols answered in 0.5 s; a silent refusal should not cost 60 s
    try:
        for n in (15, 20, 25, 30, 40):
            doc['cap'].append(timed_snap(p, 'NFO', nfo[:n], 1, f'{n} NFO, 1-min'))
        ok = max([r['asked'] for r in doc['cap'] if 'error' not in r] + [10])
        for exch, ids in (('NFO', nfo), ('NSE', nse)):
            t0 = time.monotonic()
            runs = [timed_snap(p, exch, ids[k:k + ok], 1, f'{exch} batch {k // ok + 1}') for k in range(0, len(ids), ok)]
            doc['batched'].append({'exchange': exch, 'symbols': len(ids), 'batch_size': ok, 'calls': len(runs),
                                   'rows': sum(r.get('rows', 0) for r in runs), 'errors': sum('error' in r for r in runs),
                                   'wall_ms': int((time.monotonic() - t0) * 1000),
                                   'newest': sorted({r.get('newest_bar_ist') for r in runs if r.get('newest_bar_ist')}),
                                   'runs': runs})
    finally:
        p.reply_timeout = saved
    (out / f'GDF_EXPERIMENTS4_{day}.json').write_text(json.dumps(doc, indent=1), encoding='utf-8')
    return doc


# --- round 5: SubscribeSnapshot with InstrumentIdentifier, as the vendor specified (22 Sep 10:5x) -------------------
def round5(p, out: Path, idents, seconds=150, tag='5', raw_ident=None):
    """Subscribe the NFO probe contracts (comma-separated, vendor's example 'NIFTY-I,'), record every push, unsubscribe."""
    day = _ist().strftime('%Y-%m-%d')
    events, ident = [], raw_ident if raw_ident is not None else ','.join(idents) + ','
    base = {'MessageType': 'SubscribeSnapshot', 'Exchange': 'NFO', 'InstrumentIdentifier': ident,
            'Periodicity': 'MINUTE', 'Period': 1}
    with p._lock:
        ws = p._ws or p._connect()
        p.limiter.wait()
        sent = time.monotonic()
        ws.send(json.dumps(base))
        while time.monotonic() < sent + seconds:
            try:
                raw = ws.recv()
            except socket.timeout:
                continue
            at = _ist()
            try:
                j = json.loads(raw)
            except ValueError:
                events.append({'at': at.strftime('%H:%M:%S'), 'text': raw[:160]})
                continue
            if j.get('MessageType') == 'Echo':
                continue
            res = j.get('Result')
            rows = res if isinstance(res, list) else ([res] if isinstance(res, dict) else [j])
            ltt = [r.get('LastTradeTime') for r in rows if isinstance(r, dict) and r.get('LastTradeTime')]
            events.append({'at': at.strftime('%H:%M:%S.%f')[:12], 'after_s': round(time.monotonic() - sent, 1),
                           'type': j.get('MessageType'), 'bytes': len(raw), 'rows': len(rows),
                           'idents': sorted({str(r.get('InstrumentIdentifier')) for r in rows if isinstance(r, dict)})[:4],
                           'bar_ist': sorted({_ist(t).strftime('%H:%M') for t in ltt}),
                           'lag_s_vs_bar_start': round((at - _ist(max(ltt))).total_seconds(), 1) if ltt else None,
                           'keys': sorted(j)[:10], 'sample': raw[:300] if len(events) < 2 else None})
        p.limiter.wait()
        ws.send(json.dumps(dict(base, Unsubscribe='true')))
        drain = time.monotonic() + 8
        while time.monotonic() < drain:
            try:
                ws.recv()
            except socket.timeout:
                continue
    doc = {'request': dict(base), 'events': events}
    (out / f'GDF_EXPERIMENTS{tag}_{day}.json').write_text(json.dumps(doc, indent=1), encoding='utf-8')
    return doc


def round6(p, out: Path):
    """Round 5 pushed nothing for 7 contracts. Now the vendor's example verbatim ('NIFTY-I,'), then one long identifier."""
    a = round5(p, out, [], seconds=90, tag='6a', raw_ident='NIFTY-I,')
    b = round5(p, out, [], seconds=90, tag='6b', raw_ident='FUTIDX_NIFTY_29SEP2026_XX_0')
    return a, b


# --- round 7: the longer push test - several single-identifier subscriptions on one session, each push checked -------
def subscribe_array(p, subs, seconds=120):
    """Vendor (23 Sep): 'pass the required InstrumentIdentifier values as an array/list'. Tried literally, one request."""
    got = []
    with p._lock:
        ws = p._ws or p._connect()
        p.limiter.wait()
        req = {'MessageType': 'SubscribeSnapshot', 'Exchange': subs[0][0],
               'InstrumentIdentifier': [i for e, i in subs if e == subs[0][0]], 'Periodicity': 'MINUTE', 'Period': 1}
        ws.send(json.dumps(req))
        t0 = time.monotonic()
        while time.monotonic() < t0 + seconds:
            try:
                raw = ws.recv()
            except socket.timeout:
                continue
            j = json.loads(raw) if raw.startswith('{') else {'MessageType': 'text', 'text': raw[:160]}
            if j.get('MessageType') == 'Echo':
                continue
            got.append({'at': _ist().strftime('%H:%M:%S'), 'type': j.get('MessageType'),
                        'ident': j.get('InstrumentIdentifier'),
                        'bar': _ist(j['LastTradeTime']).strftime('%H:%M') if j.get('LastTradeTime') else None,
                        'text': j.get('text')})
        p.limiter.wait()
        ws.send(json.dumps(dict(req, Unsubscribe='true')))
        drain = time.monotonic() + 8
        while time.monotonic() < drain:
            try:
                ws.recv()
            except socket.timeout:
                continue
    return {'request': req, 'messages': got}


def round7(p, out: Path, subs, seconds=540):
    """subs: [(exchange, identifier)]. One SubscribeSnapshot per identifier (the only form that pushed on 22 Sep).
    Every push is recorded; afterwards each pushed minute is compared with the FINAL 1-minute history for that minute,
    answering: does a push carry the completed minute, or the minute as it stood when pushed?"""
    day = _ist().strftime('%Y-%m-%d')
    pushes, other = [], []
    try:
        array_form = subscribe_array(p, subs)
    except Exception as error:  # noqa: BLE001
        array_form = {'error': str(error)[:160]}
    with p._lock:
        ws = p._ws or p._connect()
        for exch, ident in subs:
            p.limiter.wait()
            ws.send(json.dumps({'MessageType': 'SubscribeSnapshot', 'Exchange': exch, 'InstrumentIdentifier': ident,
                                'Periodicity': 'MINUTE', 'Period': 1}))
        t0 = time.monotonic()
        while time.monotonic() < t0 + seconds:
            try:
                raw = ws.recv()
            except socket.timeout:
                continue
            at = _ist()
            try:
                j = json.loads(raw)
            except ValueError:
                other.append({'at': at.strftime('%H:%M:%S'), 'text': raw[:160]})
                continue
            if j.get('MessageType') == 'Echo':
                continue
            if j.get('MessageType') == 'RealtimeSnapshotResult' and j.get('LastTradeTime'):
                pushes.append({'at': at.strftime('%Y-%m-%d %H:%M:%S.%f')[:23], 'ident': j.get('InstrumentIdentifier'),
                               'exchange': j.get('Exchange'), 'bar_start': _ist(j['LastTradeTime']).strftime('%Y-%m-%d %H:%M:%S'),
                               'open': j.get('Open'), 'high': j.get('High'), 'low': j.get('Low'), 'close': j.get('Close'),
                               'volume': j.get('TradedQty'), 'oi': j.get('OpenInterest')})
            else:
                other.append({'at': at.strftime('%H:%M:%S'), 'type': j.get('MessageType'), 'text': raw[:200]})
        for exch, ident in subs:
            p.limiter.wait()
            ws.send(json.dumps({'MessageType': 'SubscribeSnapshot', 'Exchange': exch, 'InstrumentIdentifier': ident,
                                'Periodicity': 'MINUTE', 'Period': 1, 'Unsubscribe': 'true'}))
        drain = time.monotonic() + 8
        while time.monotonic() < drain:
            try:
                ws.recv()
            except socket.timeout:
                continue
    # the final 1-minute history for every pushed minute
    checks = []
    for exch, ident in subs:
        mine = [x for x in pushes if x['ident'] == ident]
        if not mine:
            checks.append({'ident': ident, 'pushes': 0})
            continue
        start = datetime.strptime(min(x['bar_start'] for x in mine), '%Y-%m-%d %H:%M:%S').replace(tzinfo=IST)
        try:
            res = p.call({'MessageType': 'GetHistory', 'Exchange': exch, 'InstrumentIdentifier': ident, 'Periodicity': 'MINUTE',
                          'Period': 1, 'From': int(start.timestamp()), 'To': int(time.time()) + 60},
                         lambda j, i=ident: 'Result' in j and (j.get('Request') or {}).get('InstrumentIdentifier') == i)
        except Exception as error:  # noqa: BLE001
            checks.append({'ident': ident, 'pushes': len(mine), 'error': str(error)[:120]})
            continue
        final = {_ist(r['LastTradeTime']).strftime('%Y-%m-%d %H:%M:%S'): r for r in res.get('Result') or []}
        rows = []
        for x in mine:
            f = final.get(x['bar_start'])
            rows.append({'bar': x['bar_start'][11:16], 'pushed_at': x['at'][11:19],
                         'push': [x['close'], x['volume'], x['oi']],
                         'final': [f.get('Close'), f.get('TradedQty'), f.get('OpenInterest')] if f else None,
                         'same': bool(f) and (x['open'], x['high'], x['low'], x['close'], x['volume']) ==
                                 (f.get('Open'), f.get('High'), f.get('Low'), f.get('Close'), f.get('TradedQty'))})
        checks.append({'ident': ident, 'pushes': len(mine), 'complete': sum(r['same'] for r in rows),
                       'rows': rows})
    doc = {'subs': subs, 'seconds': seconds, 'array_form': array_form, 'pushes': len(pushes), 'other': other[:30],
           'checks': checks, 'push_log': pushes}
    (out / f'GDF_PUSH_TEST_{day}.json').write_text(json.dumps(doc, indent=1), encoding='utf-8')
    return doc


# --- round 8: the BULK functions in the vendor's own docs that our key does not list (23 Sep) -------------------------
# StreamAllSnapshots pushes a RealtimeSnapshotCollection of every symbol enabled for the key ("no additional function is
# required to be enabled"); SubscribeOptionChain / GetLastQuoteOptionChain cover a whole chain in one request. Our
# GetLimitation lists only GetExchangeSnapshot, GetHistory, GetSnapshot, SubscribeSnapshot - so each is asked once here.
def probe(p, req, listen=0.0, label=''):
    out = {'label': label or req['MessageType'], 'request': req, 'messages': []}
    with p._lock:
        ws = p._ws or p._connect()
        p.limiter.wait()
        t0 = time.monotonic()
        ws.send(json.dumps(req))
        end = t0 + (listen or 25)
        while time.monotonic() < end:
            try:
                raw = ws.recv()
            except socket.timeout:
                continue
            try:
                j = json.loads(raw)
            except ValueError:
                out['messages'].append({'after_s': round(time.monotonic() - t0, 1), 'text': raw[:200]})
                continue
            if j.get('MessageType') == 'Echo':
                continue
            res = j.get('Result')
            rows = res if isinstance(res, list) else ([res] if isinstance(res, dict) else [])
            ltt = [r.get('LastTradeTime') for r in rows if isinstance(r, dict) and r.get('LastTradeTime')] or \
                  ([j['LastTradeTime']] if j.get('LastTradeTime') else [])
            out['messages'].append({'after_s': round(time.monotonic() - t0, 1), 'type': j.get('MessageType'),
                                    'message': j.get('Message'), 'bytes': len(raw), 'rows': len(rows),
                                    'idents': sorted({str(r.get('InstrumentIdentifier')) for r in rows if isinstance(r, dict)})[:3],
                                    'bars': sorted({_ist(t).strftime('%H:%M') for t in ltt})[-3:],
                                    'fields': sorted(rows[0])[:24] if rows and isinstance(rows[0], dict) else sorted(j)[:20]})
            if not listen and (j.get('MessageType') or '').endswith(('Result', 'Collection')):
                break
    return out


def round8(p, out: Path, atm, expiry='29SEP2026'):
    day = _ist().strftime('%Y-%m-%d')
    doc = {'started_ist': _ist().strftime('%H:%M:%S'), 'probes': []}
    chain = {'Exchange': 'NFO', 'Product': 'NIFTY', 'Expiry': expiry, 'StrikePrice': atm, 'Depth': 10}
    try:
        doc['probes'].append(probe(p, {'MessageType': 'GetLastQuoteOptionChain', **chain}, label='option chain, one request'))
        doc['probes'].append(probe(p, {'MessageType': 'SubscribeOptionChain', **chain, 'Unsubscribe': False},
                                   listen=130, label='option chain, pushed'))
        doc['probes'].append(probe(p, {'MessageType': 'SubscribeOptionChain', **chain, 'Unsubscribe': True}, label='chain unsubscribe'))
        doc['probes'].append(probe(p, {'MessageType': 'StreamAllSnapshots', 'Exchange': 'NFO', 'Periodicity': 'MINUTE',
                                       'Period': 1}, listen=150, label='ALL symbols, pushed (NFO)'))
    finally:
        p.close()      # a stream has no unsubscribe: drop the socket so normal polling resumes clean
    (out / f'GDF_BULK_{day}.json').write_text(json.dumps(doc, indent=1), encoding='utf-8')
    return doc


def round9(p, out: Path, idents):
    """Round 8: the chain functions are 'Function not enabled'; StreamAllSnapshots (Exchange=NFO) returned NOTHING at
    all - no error either. Here: the stream without an Exchange, StreamAllSymbols, and the array form of
    SubscribeSnapshot the vendor described, this time capturing the server's own error text."""
    day = _ist().strftime('%Y-%m-%d')
    doc = {'started_ist': _ist().strftime('%H:%M:%S'), 'probes': []}
    try:
        doc['probes'].append(probe(p, {'MessageType': 'SubscribeSnapshot', 'Exchange': 'NFO',
                                       'InstrumentIdentifier': idents[:3], 'Periodicity': 'MINUTE', 'Period': 1},
                                   listen=20, label='SubscribeSnapshot, identifiers as an array'))
        doc['probes'].append(probe(p, {'MessageType': 'StreamAllSnapshots', 'Periodicity': 'MINUTE', 'Period': 1},
                                   listen=90, label='StreamAllSnapshots, no Exchange'))
        doc['probes'].append(probe(p, {'MessageType': 'StreamAllSymbols', 'Exchange': 'NFO'}, listen=90,
                                   label='StreamAllSymbols (NFO)'))
    finally:
        p.close()
    (out / f'GDF_BULK2_{day}.json').write_text(json.dumps(doc, indent=1), encoding='utf-8')
    return doc


def round10(p, out: Path):
    """The vendor's 'it can be configured' = filtered exchange snapshots (one call per index / product / segment).
    InstrumentType was measured 22 Sep (20.5 s even for 5 rows); here the Product filter, which we had not tried."""
    day = _ist().strftime('%Y-%m-%d')
    doc = {'started_ist': _ist().strftime('%H:%M:%S'), 'runs': [
        timed(p, _es(Product='NIFTY'), 'NFO, Product=NIFTY'),
        timed(p, _es(Product='RELIANCE'), 'NFO, Product=RELIANCE'),
        timed(p, _es(InstrumentType='OPTSTK'), 'NFO, stock options only'),
        timed(p, _es('NSE'), 'NSE, all stocks (control)'),
        timed(p, _es(), 'NFO, everything (control)'),
    ]}
    (out / f'GDF_FILTERS_{day}.json').write_text(json.dumps(doc, indent=1), encoding='utf-8')
    return doc
