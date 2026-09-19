"""Warm-up / parity gate for the research detector set in the live scanner.

Contract: ``docs/DATA_PIPELINE_CONTRACT.md`` section 5 and
``docs/pattern_research/EVIDENCE_SERVING_CONTRACT.md`` section 5.

The question this answers is narrow and mechanical: **how much history must the
live loader fetch before a detector family, on a given timeframe, produces
exactly the events that the same detector produces when it can see the whole
history?**  A blanket "last 260 candles" is not evidence.

Method
------
1. For one symbol and timeframe, build the full candle history through the live
   aggregation path (``market_scanner.data.aggregate_market15``) and run every
   research detector module over it once.  That is the **reference**.
2. Pick several *as-of* bars (simulated live scans at different moments).  For
   an as-of bar ``A`` the reference is the full-history event list restricted to
   ``signal_index <= A`` -- legitimate because prefix causality (later candles
   never rewrite earlier events) is an independently validated property of these
   detectors (``market_scanner.pattern_research.validation``), and every
   ``W = max(grid)`` comparison below re-checks it on this data.
3. For each candidate window ``W``, re-run the same detector on
   ``bars[A-W+1 .. A]`` -- the exact slice a live loader would hand it, with the
   first bar's ``gap`` flag cleared the way a short fetch really does clear it.
4. Compare the **tail** region (the last ``TAIL`` bars before ``A``, which is
   everything the live layer can still call forming/confirmed) after normalising
   every event to **timestamps**, never bar indices.
5. The required warm-up for (family, timeframe) is the smallest ``W`` in the grid
   such that it and every larger tested ``W`` disagree on nothing, for every
   sampled symbol and as-of bar.  It is then raised to the family's own declared
   lookback (``declared_floor``): a sampled tail that happened to contain no
   150-bar chart formation is not evidence that a shorter window suffices.

Window-independent disagreements
--------------------------------
``common.prepare`` accumulates ATR through ``np.cumsum`` over the whole array, so
a differently truncated window can produce an ATR that differs in the last ulp.
Where a confirmation test sits exactly on its threshold, that last ulp decides
the event.  Measured case (DELHIVERY 4H, 2025-10-16): ``close - level`` is
``-5.7e-14`` on the full history and exactly ``0.0`` on the truncated window, so
``close < level`` flips and two CDLLONGLINE / two price-action confirmations
appear on one side only.

More history cannot fix that, so it is **not** counted as a warm-up failure: a
disagreement that survives the largest tested window is separated out, counted,
listed by identity in ``window_independent_disagreements`` and reported as a
rate.  The pass/fail criterion is event identity and timing; ATR/score drift
among matched events is measured separately, both overall and at the accepted
warm-up.

Run (repository root)::

    market_scanner/.venv/Scripts/python.exe -m market_scanner.pattern_warmup --workers 6

Writes ``market_scanner/output/live_warmup.json``; ``market_scanner.pattern_live``
refuses to run the research set without it.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / 'output' / 'live_warmup.json'

#: Candidate warm-up windows, smallest first.
GRID = (60, 80, 100, 130, 170, 220, 280, 360, 460, 600, 800)
#: Bars before the as-of bar that the live layer can still surface.  Must exceed
#: the longest tracked setup expiry (chart CH26/CH27 cap at 40 closed bars).
TAIL = 60
#: Simulated live scans, as offsets back from the newest completed bar.
ASOF_OFFSETS = (0, 200, 450, 900, 1700)

#: Sampled symbols.  Chosen before any result was looked at, to cover the cases
#: section 5 of the evidence contract names: ordinary large caps with long clean
#: history, a CAS (F&O) symbol and a non-CAS symbol (contract 2A), a symbol with
#: a genuine multi-month suspension gap (FORCEMOT, 2023-10-26..2024-02-13), and a
#: symbol whose usable history starts late (DELHIVERY).
DEFAULT_SYMBOLS = ('RELIANCE', 'TITAN', 'INFY', 'FORCEMOT', 'DELHIVERY',
                   'AMRUTANJAN', 'BEL', 'CIPLA')

TIMEFRAMES = ('1H', '4H', '1D', '1W')

#: Families whose coverage the gate must positively demonstrate, and the pattern
#: ids that evidence each one.  An empty observation count is reported as a gap,
#: never silently passed.
COVERAGE_PROBES = {
    'harmonic': ('HA01', 'HA02', 'HA03', 'HA04', 'HA05', 'HA06', 'HA07', 'HA08', 'HA09', 'HA10'),
    'hikkake_confirmation': ('CDLHIKKAKE', 'CDLHIKKAKEMOD'),
    'mother_bar_cluster': ('PA02', 'PA06'),
    'legacy_chart': ('CH01', 'CH02', 'CH03', 'CH04', 'CH05', 'CH06', 'CH07', 'CH08', 'CH09', 'CH10'),
    'research_chart': tuple(f'CH{n:02d}' for n in range(11, 29)),
    'candlestick': ('CDLENGULFING', 'CDLDOJI', 'CDLHARAMI'),
    'price_action': ('PA01', 'PA03', 'PA04', 'PA05', 'PA07', 'PA08'),
}


# --------------------------------------------------------------------------- data
def _cutoff(value=None):
    from .data import now_ist
    if value:
        return datetime.fromisoformat(value)
    return now_ist()


def load_history(symbol, cutoff, sessions=4000):
    """Full live-path candle history for one symbol, per timeframe.

    Uses exactly the loader, calendar and aggregation the live scanner uses, so
    a window measured here is a window the scanner can actually ask for.
    """
    from market_data.calendar import REGIME_CAS, RegimeBook, SessionRegime
    from market_data.live.calendar_ext import live_calendar
    from .data import (aggregate_market15, load_market15, market15_path, market15_regimes,
                       open_market15)

    with open_market15(market15_path()) as store:
        regimes = market15_regimes(store)
        intraday = load_market15(store, symbol, True, cutoff, sessions=sessions)
        daily = load_market15(store, symbol, False, cutoff)
    first = regimes.get(symbol)
    book = RegimeBook([SessionRegime(symbol, first, REGIME_CAS, 'stored')]) if first else RegimeBook()
    calendar = live_calendar().for_symbol(symbol, book)
    frames = {}
    for tf in TIMEFRAMES:
        rows = intraday if tf in ('1H', '4H') else daily
        bars, quality = aggregate_market15(rows, tf, calendar, None, cutoff, 1_000_000)
        frames[tf] = (bars, quality)
    return frames, bool(first)


def window(bars, start, stop):
    """``bars[start:stop]`` as a live fetch really delivers it.

    A short fetch cannot know whether its **first** candle followed the previous
    one, so ``finish_bars`` leaves that bar's ``gap`` flag False.  Reproducing
    that here is what makes the measurement honest: it is the one way a
    truncated window is not a plain slice of the full one.
    """
    out = [dict(b) for b in bars[start:stop]]
    if out:
        out[0]['gap'] = False
    return out


# ---------------------------------------------------------------- normalisation
def normalise(events, bars, module_name):
    """Events -> timestamp-keyed identities (never bar indices)."""
    out = {}
    for key, rows in events.items():
        for event in rows:
            i = int(event['signal_index'])
            d = int(event.get('detected_index', i))
            start = event.get('pattern_start')
            if start is None:
                start = bars[int(event['formation_start_index'])]['time']
            identity = (module_name, key[0], key[1], key[2], event['state'], event['direction'],
                        str(start), bars[d]['end'], bars[i]['end'])
            out[identity] = (float(event['atr']), float(event['score']), bars[i]['end'])
    return out


def tail_subset(normalised, tail_start_end):
    return {k: v for k, v in normalised.items() if k[-1] >= tail_start_end}


def drift(reference, candidate):
    """Largest relative ATR/score difference among events present in both."""
    atr = score = 0.0
    for key, (a, s, _) in candidate.items():
        other = reference.get(key)
        if other is None:
            continue
        if other[0]:
            atr = max(atr, abs(a - other[0]) / abs(other[0]))
        if other[1]:
            score = max(score, abs(s - other[1]) / abs(other[1]))
    return atr, score


# ------------------------------------------------------------------ measurement
def measure_symbol(symbol, cutoff_text, grid=GRID, tail=TAIL, offsets=ASOF_OFFSETS):
    from .pattern_research.runner import MODULES, registry

    started = time.monotonic()
    cutoff = _cutoff(cutoff_text)
    modules, _specs = registry(list(MODULES))
    frames, is_cas = load_history(symbol, cutoff)
    rows = []
    coverage = {name: 0 for name in COVERAGE_PROBES}
    notes = []
    persistent_identities = set()

    for tf in TIMEFRAMES:
        bars, quality = frames[tf]
        n = len(bars)
        if n < min(grid) + tail:
            notes.append(f'{symbol}/{tf}: only {n} candles; skipped')
            continue
        reference = {}
        for module in modules:
            name = module.__name__.rsplit('.', 1)[-1]
            reference[name] = normalise(module.detect(bars, tf), bars, name)
            for probe, ids in COVERAGE_PROBES.items():
                coverage[probe] += sum(1 for k in reference[name] if k[1] in ids)
        for offset in offsets:
            asof = n - 1 - offset
            if asof < min(grid) + tail:
                continue
            asof_end = bars[asof]['end']
            tail_start_end = bars[max(asof - tail + 1, 0)]['end']
            for name, full in reference.items():
                want = tail_subset({k: v for k, v in full.items() if k[-1] <= asof_end}, tail_start_end)
                module = next(m for m in modules if m.__name__.endswith('.' + name))
                block, widest = [], None
                for w in grid:
                    begin = asof - w + 1
                    if begin < 0:
                        continue
                    truncated = window(bars, begin, asof + 1)
                    got = tail_subset(normalise(module.detect(truncated, tf), truncated, name),
                                      tail_start_end)
                    missing = sorted(set(want) - set(got))
                    extra = sorted(set(got) - set(want))
                    atr_drift, score_drift = drift(want, got)
                    widest = set(missing) | set(extra)
                    block.append({'symbol': symbol, 'timeframe': tf, 'module': name, 'window': w,
                                  'asof': asof_end, 'reference_events': len(want),
                                  'missing': len(missing), 'extra': len(extra),
                                  'disagreeing': set(missing) | set(extra),
                                  'atr_drift': atr_drift, 'score_drift': score_drift,
                                  'missing_sample': [list(k[1:]) for k in missing[:3]],
                                  'extra_sample': [list(k[1:]) for k in extra[:3]]})
                # A disagreement that survives the **largest** window this series can
                # supply is not a warm-up problem: no live loader could ever fetch more.
                # It is separated out, counted and reported, never silently absorbed.
                persistent = widest or set()
                for row in block:
                    stuck = row.pop('disagreeing') & persistent
                    row['persistent'] = len(stuck)
                    row['warmup_disagreement'] = row['missing'] + row['extra'] - len(stuck)
                    if stuck:
                        persistent_identities.update(stuck)
                rows.extend(block)
        notes.append(f'{symbol}/{tf}: {n} candles, {quality["gaps"]} quality gaps, '
                     f'{quality["incomplete_buckets"]} incomplete buckets dropped')
    return {'symbol': symbol, 'cas': is_cas, 'rows': rows, 'coverage': coverage,
            'persistent_identities': sorted(list(k) for k in persistent_identities),
            'notes': notes, 'seconds': round(time.monotonic() - started, 1)}


# ------------------------------------------------------------------- aggregation
MODULE_FAMILY = {'legacy': 'chart', 'chart_patterns': 'chart', 'candlesticks': 'candlestick',
                 'price_action': 'price_action', 'harmonics': 'harmonic'}
#: The CH01-CH10 adapter replays the original scanner, which rolls a 260-bar
#: window (``legacy.detect`` -> ``HistoricalReplay(bars, 260, ...)``).  Its
#: published ``lookback`` of 40 is the minimum bar count, not that window.
LEGACY_REPLAY_LOOKBACK = 260


def declared_floor():
    """``family -> largest lookback the detectors themselves declare``.

    The measurement can only ever *raise* the warm-up above what the detector
    code says it needs; it must never lower it below.  A sampled tail that
    happened to contain no 150-bar chart formation is not evidence that 130
    candles are enough for one.
    """
    from .pattern_research.runner import MODULES, registry
    _modules, specs = registry(list(MODULES))
    floor = {}
    for spec in specs:
        lookback = int(spec.get('minimum_history') or spec.get('lookback') or 0)
        if spec['variant'].startswith('legacy_'):
            lookback = max(lookback, LEGACY_REPLAY_LOOKBACK)
        floor[spec['family']] = max(floor.get(spec['family'], 0), lookback)
    return floor


def required_windows(rows, grid=GRID, floor=None):
    """(family, timeframe) -> the warm-up the live loader must honour.

    ``measured_bars`` is the smallest tested window that -- and every larger
    tested window -- reproduced the full-history tail exactly.  ``required_bars``
    is that, raised to the family's own declared lookback.
    """
    floor = declared_floor() if floor is None else floor
    worst, seen, stuck = {}, {}, {}
    for row in rows:
        key = (MODULE_FAMILY[row['module']], row['timeframe'], row['window'])
        worst[key] = max(worst.get(key, 0), row['warmup_disagreement'])
        stuck[key] = max(stuck.get(key, 0), row['persistent'])
        seen[key] = seen.get(key, 0) + row['reference_events']
    table = {}
    families = sorted({k[0] for k in worst})
    timeframes = [tf for tf in TIMEFRAMES if any(k[1] == tf for k in worst)]
    for family in families:
        for tf in timeframes:
            available = [w for w in grid if (family, tf, w) in worst]
            if not available:
                continue
            chosen = None
            for w in available:
                if all(worst[(family, tf, bigger)] == 0 for bigger in available if bigger >= w):
                    chosen = w
                    break
            table[f'{family}|{tf}'] = {
                'family': family, 'timeframe': tf,
                'measured_bars': chosen,
                'declared_lookback': floor.get(family),
                'required_bars': None if chosen is None else max(chosen, floor.get(family, 0)),
                'disagreement_by_window': {str(w): worst[(family, tf, w)] for w in available},
                'window_independent_disagreement': stuck[(family, tf, max(available))],
                'reference_events_compared': seen[(family, tf, max(available))],
                'largest_tested': max(available),
                'status': 'pass' if chosen is not None else 'no_window_passed',
            }
    return table


def build(results, grid=GRID, tail=TAIL, offsets=ASOF_OFFSETS):
    from .pattern_live import LIVE_RULES_VERSION, detector_spec_hash, research_identity

    rows = [r for result in results for r in result['rows']]
    table = required_windows(rows, grid)
    coverage = {name: sum(r['coverage'].get(name, 0) for r in results) for name in COVERAGE_PROBES}
    gaps = sorted(name for name, count in coverage.items() if count == 0)
    per_timeframe = {}
    for tf in TIMEFRAMES:
        entries = [v for v in table.values() if v['timeframe'] == tf]
        if entries:
            values = [e['required_bars'] for e in entries]
            per_timeframe[tf] = None if any(v is None for v in values) else max(values)
    accepted = []
    for row in rows:
        entry = table.get(f'{MODULE_FAMILY[row["module"]]}|{row["timeframe"]}')
        if entry and entry['required_bars'] is not None and row['window'] >= entry['measured_bars']:
            accepted.append(row)
    failed = sorted(k for k, v in table.items() if v['required_bars'] is None)
    persistent = sorted({tuple(k) for result in results for k in result['persistent_identities']})
    persistent = [list(k) for k in persistent]
    compared = sum(v['reference_events_compared'] for v in table.values())
    return {
        'version': 1,
        'live_rules_version': LIVE_RULES_VERSION,
        'detector_spec_hash': detector_spec_hash(),
        'research': research_identity(),
        'generated_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'method': ('Full-history detection vs start-truncated windows on real market15 candles, '
                   'normalised to timestamps; the truncated window has its first bar gap flag '
                   'cleared exactly as a short live fetch does.'),
        'grid': list(grid), 'tail_bars': tail, 'asof_offsets': list(offsets),
        'symbols': [r['symbol'] for r in results],
        'cas_symbols': [r['symbol'] for r in results if r['cas']],
        'comparisons': len(rows),
        'table': table,
        'required_bars_by_timeframe': per_timeframe,
        'coverage_observations': coverage,
        'coverage_gaps': gaps,
        # Disagreements that no amount of extra history removes. Measured, named and
        # carried into docs/LIVE_DETECTION.md rather than rounded away.
        'window_independent_disagreements': persistent,
        'window_independent_rate': (len(persistent) / compared) if compared else 0.0,
        'reference_events_compared': compared,
        # Drift across every tested window includes the deliberately-too-short
        # ones, where the first bar's true range has no previous close; the
        # number that matters is the one at or above the accepted warm-up.
        'max_relative_atr_drift': max((r['atr_drift'] for r in rows), default=0.0),
        'max_relative_score_drift': max((r['score_drift'] for r in rows), default=0.0),
        'max_relative_atr_drift_at_required': max((r['atr_drift'] for r in accepted), default=0.0),
        'max_relative_score_drift_at_required': max((r['score_drift'] for r in accepted), default=0.0),
        'status': 'pass' if not failed and not gaps else 'incomplete',
        'failed_cells': failed,
        'notes': [n for r in results for n in r['notes']],
    }


def markdown(report):
    order = ['chart', 'candlestick', 'price_action', 'harmonic']
    families = [f for f in order if any(v['family'] == f for v in report['table'].values())]
    timeframes = [tf for tf in TIMEFRAMES if tf in report['required_bars_by_timeframe']]
    lines = ['| family | declared lookback | ' + ' | '.join(timeframes) + ' |',
             '|---|---|' + '---|' * len(timeframes)]
    for family in families:
        cells, declared = [], None
        for tf in timeframes:
            entry = report['table'].get(f'{family}|{tf}')
            if entry is None:
                cells.append('n/a')
                continue
            declared = entry.get('declared_lookback')
            cells.append(f'FAIL (>{entry["largest_tested"]})' if entry['required_bars'] is None
                         else f'{entry["required_bars"]} (measured {entry["measured_bars"]})')
        lines.append(f'| {family} | {declared} | ' + ' | '.join(cells) + ' |')
    lines.append('| **loader minimum** | - | ' +
                 ' | '.join(str(report['required_bars_by_timeframe'][tf]) for tf in timeframes) + ' |')
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbols', nargs='+', default=list(DEFAULT_SYMBOLS))
    parser.add_argument('--workers', type=int, default=max(1, (os.cpu_count() or 4) // 2))
    parser.add_argument('--cutoff', help='ISO timestamp; defaults to now (IST)')
    parser.add_argument('--out', default=str(OUTPUT))
    args = parser.parse_args(argv)
    began = time.monotonic()
    results = []
    if args.workers > 1:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(measure_symbol, s, args.cutoff): s for s in args.symbols}
            for future in as_completed(futures):
                results.append(future.result())
                print(f'{futures[future]}: {results[-1]["seconds"]}s', flush=True)
    else:
        for symbol in args.symbols:
            results.append(measure_symbol(symbol, args.cutoff))
            print(f'{symbol}: {results[-1]["seconds"]}s', flush=True)
    results.sort(key=lambda r: r['symbol'])
    report = build(results)
    report['wall_clock_seconds'] = round(time.monotonic() - began, 1)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=1, allow_nan=False), encoding='utf-8')
    print(markdown(report), flush=True)
    print(json.dumps({k: report[k] for k in (
        'status', 'comparisons', 'coverage_gaps', 'required_bars_by_timeframe', 'failed_cells',
        'max_relative_atr_drift', 'max_relative_atr_drift_at_required', 'coverage_observations',
        'reference_events_compared', 'window_independent_rate', 'wall_clock_seconds')},
        allow_nan=False), flush=True)
    for identity in report['window_independent_disagreements']:
        print('window-independent disagreement: ' + ' '.join(identity), flush=True)
    return 0 if report['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
