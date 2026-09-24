"""GDF SHADOW CHECK — does the vendor agree with what Kite captured, reading by reading?

    python -m market_data.gdf_shadow --session 2026-09-21 --underlyings NIFTY BANKNIFTY RELIANCE

For each underlying: its near-expiry at-the-money option ladder (ATM and four strikes either side, per side, chosen
at the session's FIRST reading from Kite's own contract table), its near future, and its spot. For every KANIDA
reading T, the vendor's bar STARTING T-15m (GDF stamps bar starts - measured) is paired with Kite's snapshot at T,
and price and open interest are compared. Nothing is written to any store: this is evidence about a vendor, kept in
a report. It stays inside the trial's 100-instrument limit (about 20 NFO symbols per underlying).
"""
from __future__ import annotations

import argparse
import sqlite3
import statistics
from datetime import date, datetime, timedelta
from pathlib import Path

from .gdf_provider import GdfProvider, gdf_identifier, INDEX_NAMES
from .types import IST

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / 'db' / 'derivatives.db'


def _contracts(db, underlying, session):
    first = db.execute("select min(captured_at) from underlying_snapshots where underlying=? and substr(captured_at,1,10)=?",
                       (underlying, session)).fetchone()[0]
    spot = db.execute('select spot from underlying_snapshots where underlying=? and captured_at=?', (underlying, first)).fetchone()[0]
    expiry = db.execute("""select min(expiry) from contracts where underlying=? and instrument_type in ('CE','PE')
                           and expiry>=?""", (underlying, session)).fetchone()[0]
    strikes = sorted({r[0] for r in db.execute("""select strike from contracts where underlying=? and expiry=?
                       and instrument_type='CE'""", (underlying, expiry))})
    atm = min(strikes, key=lambda s: (abs(s - spot), -s))
    i = strikes.index(atm)
    rows = db.execute("""select instrument_token,instrument_type,strike,expiry from contracts where underlying=? and expiry=?
                         and ((instrument_type='CE' and strike in (%s)) or (instrument_type='PE' and strike in (%s)))"""
                      % (','.join(map(str, strikes[i:i + 5])), ','.join(map(str, strikes[max(0, i - 4):i + 1]))),
                      (underlying, expiry)).fetchall()
    fut = db.execute("""select instrument_token,'FUT',null,expiry from contracts where underlying=? and instrument_type='FUT'
                        and expiry>=? order by expiry limit 1""", (underlying, session)).fetchone()
    return first, spot, atm, expiry, list(rows) + ([fut] if fut else [])


def compare(session: str, underlyings, provider: GdfProvider):
    db = sqlite3.connect(f'file:{STORE.as_posix()}?mode=ro', uri=True)
    day = date.fromisoformat(session)
    start, end = datetime(day.year, day.month, day.day, 9, 0, tzinfo=IST), datetime(day.year, day.month, day.day, 15, 45, tzinfo=IST)
    results, lines = [], []
    for u in underlyings:
        first, spot, atm, expiry, contracts = _contracts(db, u, session)
        lines.append(f'### {u} — ATM {atm:g} at {first[11:16]} (spot {spot}), options {expiry}')
        items = [(tok, kind, strike, exp) for tok, kind, strike, exp in contracts]
        # spot: the index / equity itself against Kite's underlying spot
        items.append((None, 'SPOT', None, None))
        for tok, kind, strike, exp in items:
            if kind == 'SPOT':
                ident = INDEX_NAMES.get(u, u)
                kite = {r[0]: (r[1], None) for r in db.execute(
                    "select captured_at,spot from underlying_snapshots where underlying=? and substr(captured_at,1,10)=?", (u, session))}
            else:
                ident = gdf_identifier(u, kind, date.fromisoformat(exp), strike)
                kite = {r[0]: (r[1], r[2]) for r in db.execute(
                    "select captured_at,last_price,oi from snapshots where instrument_token=? and substr(captured_at,1,10)=?", (tok, session))}
            try:
                bars = provider.history(ident, '15minute', start, end)
            except Exception as error:  # noqa: BLE001 - one symbol refused is a finding, not a crash
                results.append({'u': u, 'ident': ident, 'error': str(error)[:120]})
                lines.append(f'- `{ident}`: **refused** — {str(error)[:120]}')
                continue
            px, oi, n, miss = [], [], 0, 0
            for b in bars:
                reading = (b['bar_start'] + timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
                if reading not in kite:
                    continue
                kp, koi = kite[reading]
                n += 1
                if kp and b.get('Close'):
                    px.append(abs(b['Close'] - kp) / kp * 100)
                if koi is not None and b.get('OpenInterest') is not None:
                    oi.append(abs(b['OpenInterest'] - koi) / koi * 100 if koi else 0.0)
            miss = len(kite) - n
            res = {'u': u, 'ident': ident, 'kind': kind, 'paired': n, 'kite_readings': len(kite), 'vendor_bars': len(bars),
                   'px_median': statistics.median(px) if px else None, 'px_max': max(px) if px else None,
                   'px_within_0_5': sum(1 for x in px if x <= 0.5), 'oi_exact': sum(1 for x in oi if x == 0),
                   'oi_median': statistics.median(oi) if oi else None, 'oi_n': len(oi), 'px_n': len(px)}
            results.append(res)
            lines.append(f"- `{ident}`: paired {n}/{len(kite)} readings · price |Δ| median "
                         f"{res['px_median'] if res['px_median'] is None else round(res['px_median'], 3)}% "
                         f"(max {res['px_max'] if res['px_max'] is None else round(res['px_max'], 2)}%, "
                         f"{res['px_within_0_5']}/{res['px_n']} within 0.5%)"
                         + (f" · OI exact {res['oi_exact']}/{res['oi_n']}, median |Δ| {round(res['oi_median'], 3)}%"
                            if res['oi_n'] else ''))
    return results, lines


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('--session', required=True)
    ap.add_argument('--underlyings', nargs='+', default=['NIFTY', 'BANKNIFTY', 'RELIANCE'])
    ap.add_argument('--out', default=None)
    a = ap.parse_args(argv)
    p = GdfProvider()
    try:
        results, lines = compare(a.session, a.underlyings, p)
    finally:
        p.close()
    ok = [r for r in results if 'error' not in r]
    px = [r['px_median'] for r in ok if r.get('px_median') is not None]
    oi_exact = sum(r['oi_exact'] for r in ok)
    oi_n = sum(r['oi_n'] for r in ok)
    head = [f'# GDF shadow check vs Kite — {a.session}', '',
            f'{len(ok)} instruments compared, {len(results) - len(ok)} refused · {p.requests_made} vendor calls.',
            f'Price: median of per-instrument median |Δ| **{round(statistics.median(px), 3) if px else "—"}%**. '
            f'Open interest: **{oi_exact}/{oi_n}** paired readings identical.', '',
            'Pairing: the vendor bar STARTING at T−15m against Kite\'s snapshot at T. Kite\'s snapshot is a last trade '
            'taken ~20–60 s after the mark; the vendor\'s is the bar close — small price gaps are expected, OI should '
            'match closely.', '']
    text = '\n'.join(head + lines) + '\n'
    if a.out:
        Path(a.out).write_text(text, encoding='utf-8')
    print(text)


if __name__ == '__main__':
    main()
