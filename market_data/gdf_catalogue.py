"""GDF TRIAL — field catalogue: EVERY field every enabled function returns, not only the ones KANIDA uses today.

Run inside the one vendor session (gdf_parallel calls ``catalogue(p, out)`` once per day at start). For each enabled
function and each instrument kind (index, equity, future, option) it records the request sent, every field name in the
reply, its type, a sample value, and how many rows had it empty / zero. Replies are scrubbed of the key before writing.
Functions the trial refuses are recorded as refused, with the vendor's own message.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .types import IST


def _now():
    return datetime.now(timezone.utc).astimezone(IST)


def _rows(reply):
    """The data rows of a reply: Result may be a list of rows, a list of groups holding rows, or a dict."""
    res = reply.get('Result')
    if isinstance(res, dict):
        return [res]
    if not isinstance(res, list):
        return [reply]
    out = []
    for r in res:
        if isinstance(r, dict) and isinstance(r.get('Result'), list):
            out += [dict(x, _group=r.get('LastTradeTime')) for x in r['Result'] if isinstance(x, dict)]
        elif isinstance(r, dict):
            out.append(r)
    return out


def _profile(rows):
    fields = defaultdict(lambda: {'type': set(), 'sample': None, 'empty': 0, 'zero': 0})
    for r in rows:
        for k, v in r.items():
            f = fields[k]
            f['type'].add(type(v).__name__)
            if v in (None, '', [], {}):
                f['empty'] += 1
            elif v == 0:
                f['zero'] += 1
            elif f['sample'] is None:
                f['sample'] = v if not isinstance(v, (list, dict)) else json.dumps(v)[:80]
    return {k: {'type': '/'.join(sorted(v['type'])), 'sample': v['sample'], 'empty': v['empty'], 'zero': v['zero']}
            for k, v in fields.items()}


def probes(opt_n, opt_b, fut, eq='RELIANCE', idx='NIFTY 50'):
    frm = int((_now() - timedelta(hours=2)).timestamp())
    to = int(_now().timestamp()) + 60
    hist = lambda ex, ident, per: {'MessageType': 'GetHistory', 'Exchange': ex, 'InstrumentIdentifier': ident,
                                   'Periodicity': 'MINUTE', 'Period': per, 'From': frm, 'To': to}
    snap = lambda ex, idents, per: {'MessageType': 'GetSnapshot', 'Exchange': ex, 'Periodicity': 'MINUTE', 'Period': per,
                                    'isShortIdentifiers': 'false', 'InstrumentIdentifiers': [{'Value': i} for i in idents]}
    return [
        ('GetLimitation', {'MessageType': 'GetLimitation'}),
        ('GetExchanges', {'MessageType': 'GetExchanges'}),
        ('GetInstruments NSE_IDX', {'MessageType': 'GetInstruments', 'Exchange': 'NSE_IDX', 'DetailedInfo': 'true'}),
        ('GetInstruments NFO option', {'MessageType': 'GetInstruments', 'Exchange': 'NFO', 'InstrumentType': 'OPTIDX',
                                       'Product': 'NIFTY', 'OptionType': 'CE', 'DetailedInfo': 'true'}),
        ('GetInstruments NSE equity', {'MessageType': 'GetInstruments', 'Exchange': 'NSE', 'Product': eq, 'DetailedInfo': 'true'}),
        ('GetHistory 1m index', hist('NSE_IDX', idx, 1)), ('GetHistory 1m equity', hist('NSE', eq, 1)),
        ('GetHistory 1m future', hist('NFO', fut, 1)), ('GetHistory 1m option', hist('NFO', opt_n, 1)),
        ('GetHistory 15m option', hist('NFO', opt_n, 15)),
        ('GetSnapshot 1m NFO', snap('NFO', [fut, opt_n, opt_b], 1)), ('GetSnapshot 15m NFO', snap('NFO', [fut, opt_n], 15)),
        ('GetSnapshot 1m NSE', snap('NSE', [eq], 1)), ('GetSnapshot 1m NSE_IDX', snap('NSE_IDX', [idx], 1)),
        ('GetExchangeSnapshot NFO 1m', {'MessageType': 'GetExchangeSnapshot', 'Exchange': 'NFO', 'Periodicity': 'MINUTE', 'Period': 1}),
        # asked once each so the answer is on record today (known refused on 21 Sep)
        ('GetLastQuote NFO', {'MessageType': 'GetLastQuote', 'Exchange': 'NFO', 'isShortIdentifier': 'false', 'InstrumentIdentifier': fut}),
        ('GetExpiryDates NFO', {'MessageType': 'GetExpiryDates', 'Exchange': 'NFO', 'Product': 'NIFTY'}),
        ('GetHistory DAY', dict(hist('NSE', eq, 1), Periodicity='DAY')),
        ('GetHistory TICK', dict(hist('NFO', fut, 1), Periodicity='TICK')),
    ]


def catalogue(p, out: Path, opt_n, opt_b, fut, secret: str = ''):
    day = _now().strftime('%Y-%m-%d')
    doc = {'taken_ist': _now().strftime('%Y-%m-%d %H:%M:%S'), 'functions': {}}
    for name, req in probes(opt_n, opt_b, fut):
        entry = {'request': req}
        try:
            mt = req['MessageType']
            reply = p.call(req, lambda j, mt=mt: j.get('MessageType') == mt[3:] + 'Result'
                           or ('Request' in j and (j['Request'] or {}).get('MessageType') == mt))
            rows = _rows(reply)
            entry.update(rows=len(rows), fields=_profile(rows), top_level=sorted(k for k in reply if k != 'Result'),
                         first_row=rows[0] if rows else None)
        except Exception as error:  # noqa: BLE001 - a refusal is the finding
            entry['refused'] = str(error)[:200]
        doc['functions'][name] = entry
    text = json.dumps(doc, indent=1, default=str)
    if secret:
        text = text.replace(secret, '<redacted>')
    (out / f'GDF_FIELDS_{day}.json').write_text(text, encoding='utf-8')
    doc = json.loads(text)
    lines = [f'# GDF — every field the trial returns ({doc["taken_ist"]} IST)', '']
    for name, e in doc['functions'].items():
        if 'refused' in e:
            lines += [f'## {name} — refused', '', f'`{e["refused"]}`', '']
            continue
        lines += [f'## {name} — {e["rows"]} rows', '', '| Field | Type | Sample | Empty | Zero |', '|---|---|---|---|---|']
        lines += [f"| `{k}` | {v['type']} | {str(v['sample'])[:60]} | {v['empty']} | {v['zero']} |" for k, v in e['fields'].items()]
        lines.append('')
    (out / f'GDF_FIELDS_{day}.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return doc
