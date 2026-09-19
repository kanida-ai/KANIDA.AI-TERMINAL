"""Read-only instrument labels. Membership is the supplied snapshot, not historical membership."""
from collections import Counter
from functools import lru_cache
from time import time
from .data import ROOT, connect_source, load_config

UNIVERSES = {'nifty50': ('Nifty 50', 'in_nifty50'), 'nifty100': ('Nifty 100', 'in_nifty100'),
             'nifty200': ('Nifty 200', 'in_nifty200'), 'nifty500': ('Nifty 500', 'in_nifty500'),
             'fno': ('F&O eligible equities', 'is_fno')}


def labels():
    return _labels(int(time() // 60))


def sector_name(value):
    value = (value or '').strip()
    return 'Information Technology' if value.upper() in ('IT', 'INFORMATION TECHNOLOGY') else value or 'Unclassified'


@lru_cache(maxsize=2)
def _labels(refresh_bucket):
    with connect_source(ROOT / load_config()['database']) as con:
        con.row_factory = __import__('sqlite3').Row
        rows = con.execute("""SELECT symbol, sector, updated_at, in_nifty50, in_nifty100,
            in_nifty200, in_nifty500, is_fno FROM instrument_labels
            WHERE exchange='NSE' AND instrument_type IN ('STOCK','EQ') AND is_active=1""")
        return {r['symbol']: {
            'sector': sector_name(r['sector']),
            'universes': [key for key, (_, column) in UNIVERSES.items() if r[column] == 1],
            'labels_as_of': r['updated_at'], 'market_cap': None,
            # Zero flags on an undated, unclassified row do not establish non-membership.
            'membership_unknown': not r['updated_at'] and not any(r[c] == 1 for _, c in UNIVERSES.values()),
        } for r in rows}


def options():
    data = labels()
    sectors = Counter(r['sector'] for r in data.values())
    dates = sorted({r['labels_as_of'] for r in data.values() if r['labels_as_of']})
    return {'total': len(data), 'sectors': [{'value': k, 'count': n} for k, n in sorted(sectors.items())],
            'universes': [{'value': k, 'label': label, 'count': sum(k in r['universes'] for r in data.values())}
                          for k, (label, _) in UNIVERSES.items()],
            'unclassified': sectors['Unclassified'],
            'membership_unknown': sum(r['membership_unknown'] for r in data.values()),
            'labels_as_of': dates[-1] if dates else None, 'oldest_labels_as_of': dates[0] if dates else None,
            'market_cap_available': False,
            'note': 'Counts are stocks carrying each database label, not complete live index lists. '
                    'Labels filter the available stocks today; historical trades are not filtered by membership at their entry date. '
                    'F&O selects labeled cash equities; no futures lots or derivative contracts are simulated. '
                    'Large/mid/small cap classification is unavailable in the supplied metadata.'}


def selected_symbols(filters):
    sector, universe = filters.get('sector', ''), filters.get('universe', '')
    if sector:sector = sector_name(sector)
    if universe and universe not in UNIVERSES and universe != 'unknown':
        raise ValueError('Unknown stock universe')
    if filters.get('market_cap'):
        raise ValueError('Verified market-cap classifications are not available')
    if not sector and not universe:
        return None
    return [symbol for symbol, row in labels().items()
            if (not sector or row['sector'] == sector) and
            (not universe or (row['membership_unknown'] if universe == 'unknown' else universe in row['universes']))]


def annotate(rows):
    data = labels()
    return [{**row, **data.get(row['symbol'], {'sector': 'Unclassified', 'universes': [],
             'labels_as_of': None, 'membership_unknown': True, 'market_cap': None})} for row in rows]
