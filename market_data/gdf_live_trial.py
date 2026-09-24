"""GDF TRIAL — live-session harness. One process, one vendor session (the key allows exactly one), all day.

    python -m market_data.gdf_live_trial run    --out <dir>      # 09:14 -> 15:50 IST, polls and logs to CSV
    python -m market_data.gdf_live_trial report --out <dir>      # reads the CSV only; safe while `run` is live

Every poll row records: our IST clock when the reply arrived, the function, symbol, exchange, periodicity, the vendor's
NEWEST bar (its epoch LastTradeTime rendered in IST), that bar's close / OI / volume, how many bars came back, and the
reply latency. Nothing is inferred at poll time; the report derives delay and candle availability from these rows.
"""
from __future__ import annotations

import argparse
import csv
import statistics
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .gdf_provider import GdfProvider
from .types import IST

FIELDS = ['poll_ist', 'fn', 'symbol', 'exchange', 'periodicity', 'bars', 'newest_bar_ist', 'close', 'oi', 'volume',
          'reply_ms', 'error']


def ist_now():
    return datetime.now(timezone.utc).astimezone(IST)


def probe_symbols(p: GdfProvider):
    """The probe set, chosen from the VENDOR's own data at the start: ATM from the vendor's latest NIFTY 50 / BANK bar."""
    def atm(index_name, step, fallback):
        try:
            rows = p.history(index_name, '15minute', ist_now() - timedelta(days=5), ist_now(), max_rows=1)
            return int(round(rows[-1]['Close'] / step) * step) if rows else fallback
        except Exception:  # noqa: BLE001
            return fallback
    n, b = atm('NIFTY 50', 50, 23400), atm('NIFTY BANK', 100, 56500)
    return [('NIFTY 50', 'NSE_IDX'), ('NIFTY BANK', 'NSE_IDX'), ('RELIANCE', 'NSE'), ('HDFCBANK', 'NSE'),
            ('FUTIDX_NIFTY_29SEP2026_XX_0', 'NFO'), ('FUTIDX_BANKNIFTY_29SEP2026_XX_0', 'NFO'),
            ('FUTSTK_RELIANCE_29SEP2026_XX_0', 'NFO'),
            (f'OPTIDX_NIFTY_22SEP2026_CE_{n}', 'NFO'), (f'OPTIDX_NIFTY_22SEP2026_PE_{n}', 'NFO'),
            (f'OPTIDX_BANKNIFTY_29SEP2026_CE_{b}', 'NFO'), (f'OPTIDX_BANKNIFTY_29SEP2026_PE_{b}', 'NFO')]


def run(out: Path, start='09:14', stop='15:50'):
    out.mkdir(parents=True, exist_ok=True)
    path = out / f'gdf_polls_{ist_now():%Y-%m-%d}.csv'
    new = not path.exists()
    fh = open(path, 'a', newline='', encoding='utf-8')
    w = csv.DictWriter(fh, fieldnames=FIELDS)
    if new:
        w.writeheader()
    while ist_now().strftime('%H:%M') < start:
        time.sleep(20)
    p = GdfProvider()
    syms = probe_symbols(p)
    minute_syms = [syms[0], syms[2], syms[4], syms[7]]
    (out / 'probe_symbols.txt').write_text('\n'.join(f'{s}\t{e}' for s, e in syms), encoding='utf-8')
    last_min = last_exch = 0.0
    try:
        while ist_now().strftime('%H:%M') < stop:
            loop = time.monotonic()
            jobs = [('GetHistory', s, e, '15minute') for s, e in syms]
            if loop - last_min >= 60:
                jobs += [('GetHistory', s, e, '1minute') for s, e in minute_syms]
                last_min = loop
            if loop - last_exch >= 300:
                jobs.append(('GetExchangeSnapshot', 'NFO', 'NFO', '15minute'))
                last_exch = loop
            for fn, sym, exch, per in jobs:
                row = {'fn': fn, 'symbol': sym, 'exchange': exch, 'periodicity': per, 'error': ''}
                t0 = time.monotonic()
                try:
                    if fn == 'GetHistory':
                        req = {'MessageType': 'GetHistory', 'Exchange': exch, 'InstrumentIdentifier': sym,
                               'Periodicity': 'MINUTE', 'Period': 15 if per == '15minute' else 1, 'Max': 3}
                        res = p.call(req, lambda j, s=sym: 'Result' in j and 'Request' in j
                                     and (j['Request'] or {}).get('InstrumentIdentifier') == s)['Result'] or []
                        res.sort(key=lambda r: r['LastTradeTime'])
                        top = res[-1] if res else {}
                        row.update(bars=len(res), close=top.get('Close'), oi=top.get('OpenInterest'),
                                   volume=top.get('TradedQty'),
                                   newest_bar_ist=(datetime.fromtimestamp(top['LastTradeTime'], timezone.utc)
                                                   .astimezone(IST).strftime('%Y-%m-%d %H:%M:%S') if top else ''))
                    else:
                        req = {'MessageType': 'GetExchangeSnapshot', 'Exchange': 'NFO', 'Periodicity': 'MINUTE', 'Period': 15}
                        res = p.call(req, lambda j: 'Result' in j and (j.get('Request') or {}).get('MessageType') == 'GetExchangeSnapshot')
                        groups = res.get('Result') or []
                        rows = [r for g in groups for r in (g.get('Result') or [])]
                        newest = max((g.get('LastTradeTime') or 0) for g in groups) if groups else 0
                        row.update(bars=len(rows), volume=len({r.get('InstrumentIdentifier') for r in rows}),
                                   newest_bar_ist=(datetime.fromtimestamp(newest, timezone.utc).astimezone(IST)
                                                   .strftime('%Y-%m-%d %H:%M:%S') if newest else ''),
                                   close=sum(1 for r in rows if str(r.get('InstrumentIdentifier', '')).startswith('OPT')))
                except Exception as error:  # noqa: BLE001 - a failed poll is a data point, not a crash
                    row['error'] = str(error)[:160]
                row['reply_ms'] = int((time.monotonic() - t0) * 1000)
                row['poll_ist'] = ist_now().strftime('%Y-%m-%d %H:%M:%S')
                w.writerow(row)
                fh.flush()
            time.sleep(max(0.0, 30 - (time.monotonic() - loop)))
    finally:
        p.close()
        fh.close()


def _t(s):
    return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')


def report(out: Path, day: str | None = None):
    day = day or ist_now().strftime('%Y-%m-%d')
    fh = open(out / f'gdf_polls_{day}.csv', encoding='utf-8')
    headed = fh.readline().startswith('poll_ist')
    fh.seek(0)
    # a file created empty before the run starts gets no header row (22 Sep) - read it with the known field order
    rows = list(csv.DictReader(fh) if headed else csv.DictReader(fh, fieldnames=FIELDS))
    ok = [r for r in rows if not r.get('error') and r.get('newest_bar_ist') and r.get('poll_ist')]
    errs = [r for r in rows if r.get('error')]
    lines = [f'# GDF live trial — {day}', '',
             f'{len(rows)} polls · {len(errs)} errors · first {rows[0]["poll_ist"][11:] if rows else "—"} '
             f'· last {rows[-1]["poll_ist"][11:] if rows else "—"} IST', '']
    # --- Task 2/3: newest vendor market time vs our IST clock, sampled at each quarter hour -----------------------
    lines += ['## Newest vendor data vs IST clock (first poll after each quarter hour)', '',
              '| IST now | Symbol | Exch | TF | Newest vendor bar starts | Bar ends | Close | Delay to bar END | Result |',
              '|---|---|---|---|---|---|---|---|---|']
    seen = set()
    for r in ok:
        now = _t(r['poll_ist'])
        slot = (now.strftime('%H'), now.minute // 15, r['symbol'], r['periodicity'])
        if r['fn'] != 'GetHistory' or slot in seen or now.minute % 15 > 3:
            continue
        seen.add(slot)
        start = _t(r['newest_bar_ist'])
        end = start + (timedelta(minutes=15) if r['periodicity'] == '15minute' else timedelta(minutes=1))
        lag = (now - end).total_seconds() / 60
        verdict = 'PASS' if 13 <= lag <= 20 or (r['periodicity'] == '15minute' and 0 <= lag <= 35) else 'CHECK'
        lines.append(f"| {now:%H:%M:%S} | {r['symbol']} | {r['exchange']} | {r['periodicity']} | {start:%H:%M} | "
                     f"{end:%H:%M} | {r['close']} | {lag:.1f} min | {verdict} |")
    # --- Task 4: when does a 15-minute bar first appear, and when do its values stop changing? --------------------
    lines += ['', '## 15-minute candle availability (per bar: first seen, last changed)', '',
              '| Symbol | Bar | First seen (IST) | Values final by (IST) | First seen − bar end | Final − bar end |',
              '|---|---|---|---|---|---|']
    hist = defaultdict(list)
    for r in ok:
        if r['fn'] == 'GetHistory' and r['periodicity'] == '15minute':
            hist[(r['symbol'], r['newest_bar_ist'])].append(r)
    avail = []
    for (sym, bar), polls in sorted(hist.items()):
        if sym not in ('FUTIDX_NIFTY_29SEP2026_XX_0', 'NIFTY 50', 'RELIANCE'):
            continue
        first = _t(polls[0]['poll_ist'])
        final = first
        for a, b in zip(polls, polls[1:]):
            if (a['close'], a['oi'], a['volume']) != (b['close'], b['oi'], b['volume']):
                final = _t(b['poll_ist'])
        end = _t(bar) + timedelta(minutes=15)
        avail.append(((first - end).total_seconds() / 60, (final - end).total_seconds() / 60))
        lines.append(f"| {sym} | {_t(bar):%H:%M}–{end:%H:%M} | {first:%H:%M:%S} | {final:%H:%M:%S} | "
                     f"{(first - end).total_seconds() / 60:+.1f} min | {(final - end).total_seconds() / 60:+.1f} min |")
    if avail:
        lines += ['', f"Median: a 15-min bar first appears **{statistics.median(a for a, _ in avail):+.1f} min** after it "
                      f"ends; its values are final **{statistics.median(b for _, b in avail):+.1f} min** after it ends."]
    # --- whole-exchange snapshot -----------------------------------------------------------------------------------
    ex = [r for r in ok if r['fn'] == 'GetExchangeSnapshot']
    if ex:
        lines += ['', '## GetExchangeSnapshot (NFO)', '', '| IST now | Newest group | Instruments | Options among them |',
                  '|---|---|---|---|']
        lines += [f"| {r['poll_ist'][11:]} | {r['newest_bar_ist'][11:16]} | {r['volume']} | {r['close']} |" for r in ex]
    if errs:
        lines += ['', '## Errors', ''] + [f"- {r['poll_ist'][11:]} {r['fn']} {r['symbol']}: {r['error']}" for r in errs[:30]]
    text = '\n'.join(lines) + '\n'
    (out / f'GDF_LIVE_{day}.md').write_text(text, encoding='utf-8')
    return text


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('mode', choices=('run', 'report'))
    ap.add_argument('--out', required=True)
    ap.add_argument('--day', default=None)
    a = ap.parse_args(argv)
    out = Path(a.out)
    if a.mode == 'run':
        run(out)
    print(report(out, a.day))


if __name__ == '__main__':
    main()
