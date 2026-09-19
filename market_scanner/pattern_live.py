"""Live adapter for the 107-pattern research detector set.

Contract: ``docs/DATA_PIPELINE_CONTRACT.md`` section 5,
``docs/pattern_research/EVIDENCE_SERVING_CONTRACT.md`` sections 2, 4 and 5.
Operator documentation: ``docs/LIVE_DETECTION.md``.

What this module is
-------------------
The scanner has always run the ten legacy chart detectors in
``market_scanner/detectors.py``.  The research batch runs 107 pattern ids /
262 direction-variant cells from ``market_scanner/pattern_research/``.  This
module lets the **same versioned research code** run on live completed candles,
without forking or reimplementing one line of detector logic: it imports the
research modules through ``pattern_research.runner.registry`` -- the identical
call the research runner makes -- and only adds what a historical batch never
needed:

* a **lifecycle** (``forming`` -> ``confirmed`` -> ``invalidated`` -> ``expired``)
  keyed by timestamps and spec identity, never by bar index;
* a **detector spec identity** that can be compared against the frozen research
  run, so a live detection can never silently claim evidence that was produced
  by different code;
* a **warm-up gate**: the research set does not run at all until
  ``market_scanner/pattern_warmup.py`` has measured, on real data, how much
  history each family and timeframe needs.

Switches (environment wins over ``config.json``)
------------------------------------------------
``SCANNER_PATTERN_SET``   ``legacy`` (default) | ``research``
``SCANNER_CANDLE_SOURCE`` ``legacy`` (default) | ``market15``   (see ``data.py``)
``SCANNER_RESEARCH_RUN``  research run id used for evidence identity

Honesty rules this module keeps
-------------------------------
* An incomplete candle never reaches a detector: the aggregation layer drops it
  and this module only ever reads the bars it is handed.
* Lifecycle state is derived, and the derivation is versioned
  (``LIVE_RULES_VERSION``).  Where a detector publishes its own expiry or
  failure boundary in ``geometry`` that is used; where it does not, the fallback
  rule is declared here, tagged ``live_structure`` on every detection that uses
  it, and is **not** presented as the detector's own rule.
* Evidence identity is compared, never assumed.  ``evidence.status`` is
  ``identity_match`` only when the live detector spec hash equals the research
  run's; otherwise the app is told which component differs.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent

LEGACY = 'legacy'
RESEARCH = 'research'

#: Version of the *live lifecycle rules in this file* (not of any detector).
#: Bump it whenever expiry/invalidation/identity derivation changes; it is part
#: of the scan cache signature and of the warm-up report.
LIVE_RULES_VERSION = '1.0.0'

#: Default research run whose identity a live detection is compared against.
DEFAULT_RESEARCH_RUN = '4b33a5249562631524d6'
RESEARCH_OUTPUT = ROOT / 'output' / 'expanded_research'
WARMUP_REPORT = ROOT / 'output' / 'live_warmup.json'

#: Bars before the newest completed candle in which a detection is still
#: surfaced.  Must exceed the longest tracked setup expiry (40) so nothing that
#: can still confirm is dropped.
LIVE_TAIL = 60

#: The files whose bytes decide what a detector emits.  Everything else in the
#: research run manifest (reports, audits, rule documents) can change without
#: changing a single detection, so it is deliberately not part of this identity.
#: Order is irrelevant; the digest sorts.
DETECTOR_FILES = (
    'detectors.py', 'historical.py', 'replay_filter.py', 'backtest.py', 'backtest_worker.py',
    'pattern_research/__init__.py', 'pattern_research/common.py', 'pattern_research/legacy.py',
    'pattern_research/candlesticks.py', 'pattern_research/chart_patterns.py',
    'pattern_research/price_action.py', 'pattern_research/harmonics.py',
)

MODULE_FAMILY = {'legacy': 'chart', 'chart_patterns': 'chart', 'candlesticks': 'candlestick',
                 'price_action': 'price_action', 'harmonics': 'harmonic'}

#: Fallback tracking window per family, in **closed bars after the detection
#: bar**, used only when the detector did not publish its own in ``geometry``.
#: Each number is the detector's own documented confirmation/tracking window:
#: chart ``common.CHART_SETUP_EXPIRY`` = 10, candlestick and price action
#: ``common.CANDLE_CONFIRM_WINDOW`` = 3, harmonic ``harmonics.CONFIRM_WINDOW`` = 10.
FAMILY_EXPIRY = {'chart': 10, 'candlestick': 3, 'price_action': 3, 'harmonic': 10}

STATE_ORDER = {'setup': 0, 'confirmed': 1}
# Reentrant: detector_spec_hash() holds it while calling research_registry().
_LOCK = threading.RLock()
_REGISTRY = None
_SPEC_HASH = None
_WARMUP = None
_RESEARCH = {}


# --------------------------------------------------------------------- switches
def pattern_set(config=None):
    """``legacy`` (default) or ``research``.  Environment wins over config."""
    value = (os.environ.get('SCANNER_PATTERN_SET')
             or (config or {}).get('pattern_set') or LEGACY)
    value = str(value).strip().lower()
    if value not in (LEGACY, RESEARCH):
        raise ValueError(f"SCANNER_PATTERN_SET must be {LEGACY!r} or {RESEARCH!r}, not {value!r}")
    return value


def research_run_id(config=None):
    return (os.environ.get('SCANNER_RESEARCH_RUN')
            or (config or {}).get('research_run') or DEFAULT_RESEARCH_RUN)


# --------------------------------------------------------------------- registry
def research_registry():
    """``(modules, specs, spec_by_key)`` from the research runner itself.

    Imported, never copied: a change to a detector changes the live scanner and
    the spec hash in the same commit, which is the only way the two can be kept
    honest about being the same code.
    """
    global _REGISTRY
    with _LOCK:
        if _REGISTRY is None:
            from .pattern_research.runner import MODULES, registry
            modules, specs = registry(list(MODULES))
            by_key = {(s['pattern_id'], s['variant'], s['side']): s for s in specs}
            family = {m.__name__.rsplit('.', 1)[-1]: MODULE_FAMILY[m.__name__.rsplit('.', 1)[-1]]
                      for m in modules}
            _REGISTRY = (modules, specs, by_key, family)
    return _REGISTRY


def catalogue():
    """``[{id, name, description}]`` for ``/api/state`` -- one row per pattern id."""
    _modules, specs, _by_key, _family = research_registry()
    seen = {}
    for spec in specs:
        entry = seen.setdefault(spec['pattern_id'], {
            'id': spec['pattern_id'], 'name': spec['name'], 'family': spec['family'],
            'variants': [], 'sides': [], 'description': spec['definition'][:240]})
        if spec['variant'] not in entry['variants']:
            entry['variants'].append(spec['variant'])
        if spec['side'] not in entry['sides']:
            entry['sides'].append(spec['side'])
    return [seen[k] for k in sorted(seen)]


# --------------------------------------------------------------------- identity
def _dependency_versions():
    """The research runner's own dependency record, not a restatement of it.

    Using ``runner.dependencies()`` is what makes the live and research digests
    comparable by construction rather than by coincidence."""
    from .pattern_research.runner import dependencies
    return dependencies()


def _digest(files, specs, dependencies):
    payload = json.dumps({'files': dict(sorted(files.items())), 'specifications': specs,
                          'dependencies': dict(sorted(dependencies.items()))},
                         sort_keys=True, separators=(',', ':'), ensure_ascii=False)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def detector_spec_hash():
    """Identity of the detector code + specifications running right now."""
    global _SPEC_HASH
    with _LOCK:
        if _SPEC_HASH is None:
            _modules, specs, _by_key, _family = research_registry()
            files = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                     for name in DETECTOR_FILES}
            _SPEC_HASH = _digest(files, specs, _dependency_versions())
    return _SPEC_HASH


def research_identity(run_id=None, config=None):
    """The frozen research run's detector identity, recomputed the same way.

    Returns a dict that always includes ``run`` and ``status``.  ``status`` is
    ``missing`` when the run manifest is not on this machine -- in that case the
    app must say *no compatible evidence*, not fall back to a name match.
    """
    run_id = run_id or research_run_id(config)
    with _LOCK:
        cached = _RESEARCH.get(run_id)
    if cached is not None:
        return cached
    manifest = RESEARCH_OUTPUT / run_id / 'manifest.json'
    if not manifest.exists():
        result = {'run': run_id, 'status': 'missing',
                  'note': f'No research manifest at {manifest}'}
    else:
        data = json.loads(manifest.read_text(encoding='utf-8'))
        # The runner records relative paths with the producing OS's separator
        # (`pattern_research\common.py` on Windows); normalise before comparing.
        hashes = {str(k).replace('\\', '/'): v for k, v in data.get('code_hashes', {}).items()}
        absent = [name for name in DETECTOR_FILES if name not in hashes]
        if absent:
            result = {'run': run_id, 'status': 'incomparable',
                      'note': 'Research manifest does not record: ' + ', '.join(absent)}
        else:
            result = {
                'run': run_id, 'status': 'known',
                'source_run': data.get('source_run'),
                'protocol': data.get('protocol'),
                'timeframes': data.get('timeframes'),
                'symbols': len(data.get('symbols', [])),
                'detector_spec_hash': _digest({n: hashes[n] for n in DETECTOR_FILES},
                                              data['specifications'], data['dependencies']),
            }
    with _LOCK:
        _RESEARCH[run_id] = result
    return result


def evidence_identity(config=None):
    """Whether a live detection may be joined to this research run at all."""
    live = detector_spec_hash()
    research = research_identity(config=config)
    if research['status'] != 'known':
        status, note = 'unavailable', research.get('note', 'Research run is not readable here')
    elif research['detector_spec_hash'] == live:
        status, note = 'identity_match', ('Live detector code, specifications and dependencies are '
                                          'byte-identical to the research run.')
    else:
        status, note = 'detector_mismatch', ('Live detector identity differs from the research run; '
                                             'no compatible historical evidence.')
    return {'status': status, 'note': note, 'detector_spec_hash': live,
            'research_run': research['run'], 'research_status': research['status'],
            'research_detector_spec_hash': research.get('detector_spec_hash'),
            'research_source_run': research.get('source_run'),
            'live_rules_version': LIVE_RULES_VERSION}


def research_cell_status(rows, run_id=None, config=None):
    """Look up the research cell behind each ``(symbol, timeframe, id, variant, side)``.

    Returns ``{key: status}``; a key the research run never studied is absent,
    which the caller must render as *no compatible evidence*, never as zero.
    """
    run_id = run_id or research_run_id(config)
    database = RESEARCH_OUTPUT / 'research.sqlite3'
    out = {}
    if not database.exists() or not rows:
        return out
    con = sqlite3.connect(f'file:{database.as_posix()}?mode=ro', uri=True, timeout=30)
    try:
        con.execute('PRAGMA query_only=ON')
        sql = ('SELECT status, occurrences, wf_n, wf_mean FROM cells WHERE run=? AND symbol=? '
               'AND timeframe=? AND pattern_id=? AND variant=? AND side=?')
        for key in rows:
            row = con.execute(sql, (run_id,) + tuple(key)).fetchone()
            if row:
                out[tuple(key)] = {'status': row[0], 'occurrences': row[1],
                                   'walkforward_trades': row[2], 'walkforward_mean_pct': row[3]}
    finally:
        con.close()
    return out


# ----------------------------------------------------------------- warm-up gate
class WarmupError(RuntimeError):
    """The research set was asked for without a passing warm-up measurement."""


def warmup(path=None, required=True):
    """The measured warm-up report, or raise.  Cached per process."""
    global _WARMUP
    path = Path(path or os.environ.get('SCANNER_WARMUP_REPORT') or WARMUP_REPORT)
    with _LOCK:
        if _WARMUP is not None and _WARMUP.get('_path') == str(path):
            report = _WARMUP
        else:
            report = None
    if report is None:
        if not path.exists():
            if not required:
                return None
            raise WarmupError(
                f'No warm-up measurement at {path}. The research pattern set stays off until '
                f'`python -m market_scanner.pattern_warmup` has measured, on real candles, how '
                f'much history each family and timeframe needs.')
        report = json.loads(path.read_text(encoding='utf-8'))
        report['_path'] = str(path)
        with _LOCK:
            _WARMUP = report
    if required:
        if report.get('status') != 'pass':
            raise WarmupError('Warm-up gate did not pass: ' + json.dumps(
                {'status': report.get('status'), 'failed_cells': report.get('failed_cells'),
                 'coverage_gaps': report.get('coverage_gaps')}))
        if report.get('detector_spec_hash') != detector_spec_hash():
            raise WarmupError(
                'Warm-up was measured against a different detector spec hash '
                f'({report.get("detector_spec_hash")}); re-run market_scanner.pattern_warmup.')
        if report.get('live_rules_version') != LIVE_RULES_VERSION:
            raise WarmupError(
                f'Warm-up was measured under live rules {report.get("live_rules_version")}, '
                f'this build is {LIVE_RULES_VERSION}; re-run market_scanner.pattern_warmup.')
    return report


def required_bars(timeframe, path=None):
    """Minimum candles the live loader must fetch for ``timeframe``.

    The measured maximum across families.  The measurement window already
    contains the ``LIVE_TAIL`` bars the scanner surfaces (the gate compares the
    tail *inside* the truncated window), so nothing is added here and nothing is
    guessed: an unmeasured timeframe is an error, not a default.
    """
    report = warmup(path)
    table = report.get('required_bars_by_timeframe') or {}
    value = table.get(timeframe)
    if not value:
        raise WarmupError(f'No measured warm-up for timeframe {timeframe}')
    return int(value)


def family_bars(timeframe, path=None):
    """``{family: measured warm-up bars}`` for ``timeframe``.

    Per family, because they differ by a factor of three: refusing to run the
    candlestick family on a symbol with 150 weekly candles because the chart
    family needs 260 would throw away detections we *can* stand behind."""
    report = warmup(path)
    out = {}
    for entry in (report.get('table') or {}).values():
        if entry['timeframe'] == timeframe and entry.get('required_bars'):
            out[entry['family']] = int(entry['required_bars'])
    if not out:
        raise WarmupError(f'No measured warm-up for timeframe {timeframe}')
    return out


def families_for(timeframe, available, path=None):
    """``(runnable, short)`` families given ``available`` candles.

    A family whose measured warm-up is not met does not run at all. It is never
    run on a shorter window and labelled as if it had: that is precisely the
    claim the gate exists to prevent."""
    need = family_bars(timeframe, path)
    runnable = sorted(f for f, bars in need.items() if available >= bars)
    short = sorted(f for f, bars in need.items() if available < bars)
    return runnable, short


def minimum_bars(timeframe, path=None):
    """Below this no family can run, so the timeframe is ``insufficient_history``."""
    return min(family_bars(timeframe, path).values())


def warmup_summary(path=None):
    """Small, safe block for ``/api/state``; never raises."""
    try:
        report = warmup(path, required=False)
    except Exception as exc:  # noqa: BLE001
        return {'status': 'error', 'error': str(exc)}
    if report is None:
        return {'status': 'missing', 'path': str(path or WARMUP_REPORT)}
    return {k: report.get(k) for k in
            ('status', 'generated_at', 'grid', 'tail_bars', 'symbols', 'comparisons',
             'required_bars_by_timeframe', 'coverage_gaps', 'failed_cells',
             'max_relative_atr_drift', 'detector_spec_hash', 'live_rules_version')}


# -------------------------------------------------------------------- lifecycle
def _geometry(event):
    geometry = event.get('geometry')
    return geometry if isinstance(geometry, dict) else {}


def expiry_for(event, family):
    """Closed bars after the detection bar for which the setup is tracked.

    Prefers the detector's own published window (``geometry.expiry_bars`` for the
    chart families that widen it, ``geometry.confirmation.window`` for
    harmonics); otherwise the family default, which restates the detector's
    documented confirmation window.
    """
    geometry = _geometry(event)
    value = geometry.get('expiry_bars')
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    confirmation = geometry.get('confirmation')
    if isinstance(confirmation, dict) and isinstance(confirmation.get('window'), (int, float)):
        return int(confirmation['window'])
    return FAMILY_EXPIRY[family]


def failure_for(event, bars, formation_start, detected, atr, bullish):
    """``(level, source)`` -- the close-through level that kills this setup.

    ``source='detector'`` when the detector published the boundary it actually
    tracks; ``source='live_structure'`` for the fallback declared here: a close
    beyond the formation's opposite extreme by 0.12 x ATR (the same buffer every
    detector in the package uses for a close-beyond test).  The fallback is a
    **live-layer rule**, tagged on every detection that uses it; it is not the
    research detector's own failure boundary and no research result depends on
    it.
    """
    from .pattern_research.common import BREAKOUT_BUFFER_ATR as BUF
    geometry = _geometry(event)
    level = geometry.get('failure_level')
    if level is None:
        confirmation = geometry.get('confirmation')
        if isinstance(confirmation, dict):
            level = confirmation.get('failure_level')
    if isinstance(level, (int, float)):
        return float(level), 'detector'
    window = bars[formation_start:detected + 1]
    if not window:
        return None, 'none'
    if bullish:
        return min(b['low'] for b in window) - BUF * atr, 'live_structure'
    return max(b['high'] for b in window) + BUF * atr, 'live_structure'


def _terminate(bars, begin, end, level, bullish):
    """First bar in ``(begin, end]`` that kills the setup, and why."""
    for j in range(begin + 1, end + 1):
        if bars[j].get('gap'):
            return j, 'segment_break'
        if level is not None:
            close = bars[j]['close']
            if (close < level) if bullish else (close > level):
                return j, 'invalidated'
    return None, None


def resolve_state(bars, setup, confirm, formation_start, detected, family, bullish):
    """The lifecycle state of one episode as of the newest closed bar.

    ``forming`` -> ``confirmed`` -> ``invalidated`` -> ``expired``.  Keys are
    timestamps; bar indices exist only inside this call.
    """
    last = len(bars) - 1
    anchor = setup or confirm
    atr = float(anchor['atr'])
    expiry = expiry_for(anchor, family)
    level, level_source = failure_for(anchor, bars, formation_start, detected, atr, bullish)
    reference = int(confirm['signal_index']) if confirm is not None else detected
    stop = min(last, reference + expiry)
    j, why = _terminate(bars, reference, stop, level, bullish)
    detail = {'expiry_bars': expiry, 'failure_level': level, 'failure_level_source': level_source,
              'tracked_from': bars[reference]['end'],
              'tracked_to': bars[min(reference + expiry, last)]['end']}
    if why == 'invalidated':
        return 'invalidated', 'closed_through_failure_level', bars[j]['end'], detail
    if why == 'segment_break':
        return 'expired', 'segment_break', bars[j]['end'], detail
    if last > reference + expiry:
        return 'expired', 'tracking_window_elapsed', bars[reference + expiry]['end'], detail
    if confirm is not None:
        return 'confirmed', 'detector_confirmation', bars[reference]['end'], detail
    return 'forming', 'awaiting_confirmation', bars[detected]['end'], detail


# -------------------------------------------------------------------- detection
def _identity(*parts):
    return hashlib.sha256('|'.join(str(p) for p in parts).encode('utf-8')).hexdigest()[:20]


def _score(module_name, raw):
    """``(fit_score 0..1, display score 0..100)``.

    The research families score geometric fit in [0, 1]; the legacy CH01-CH10
    adapter carries the original scanner's 0-100 quality.  Neither is a
    probability, and neither is rescaled into the other's meaning silently.
    """
    raw = float(raw)
    if module_name == 'legacy':
        return round(min(1.0, max(0.0, raw / 100.0)), 6), round(min(100.0, max(0.0, raw)), 1)
    return round(raw, 6), round(min(100.0, max(0.0, raw * 100.0)), 1)


#: Bars after a detection leaves ``forming``/``confirmed`` for which it is still
#: surfaced, so the app (and the persisted ledger) sees the terminal transition
#: instead of a setup that silently disappears.
TERMINAL_GRACE = 3


def detect_live(bars, timeframe, *, symbol, company=None, sector=None, tail=LIVE_TAIL,
                evidence=None, input_snapshot=None, spec_hash=None, grace=TERMINAL_GRACE,
                families=None):
    """Run the research detector set on one symbol/timeframe of closed candles.

    ``bars`` must already be completed candles in ascending order (the scanner's
    aggregation drops anything incomplete).  Returns one record per **episode**
    whose detection or confirmation falls inside the live tail and which is
    either still live or terminated within the last ``grace`` closed bars.
    Pass ``grace=None`` to get every episode in the tail (what the warm-up gate
    and the lifecycle tests need).  ``families`` restricts the run to the
    families whose measured warm-up this window satisfies; ``None`` runs all.
    """
    from . import pattern_lines
    modules, _specs, by_key, family_of = research_registry()
    if not bars:
        return []
    # Lazy: only built if a CH01-CH10 detection is actually surfaced.
    legacy_overlays = pattern_lines.LegacyOverlays(bars)
    spec_hash = spec_hash or detector_spec_hash()
    evidence = evidence if evidence is not None else evidence_identity()
    n = len(bars)
    first_live = max(0, n - int(tail))
    time_index = {b['time']: i for i, b in enumerate(bars)}
    end_index = {b['end']: i for i, b in enumerate(bars)}
    as_of = bars[-1]['end']
    out = []
    for module in modules:
        name = module.__name__.rsplit('.', 1)[-1]
        family = family_of[name]
        if families is not None and family not in families:
            continue
        for key, events in module.detect(bars, timeframe).items():
            if not events:
                continue
            spec = by_key.get(key)
            if spec is None:                       # unregistered output is a bug, not a signal
                raise ValueError(f'Unregistered detector output {key} from {name}')
            episodes = defaultdict(list)
            for event in events:
                episodes[event['episode']].append(event)
            for rows in episodes.values():
                rows.sort(key=lambda e: (e['signal_index'], STATE_ORDER[e['state']]))
                setup = next((e for e in rows if e['state'] == 'setup'), None)
                confirm = next((e for e in rows if e['state'] == 'confirmed'), None)
                anchor = setup or confirm
                detected = int(anchor.get('detected_index', anchor['signal_index']))
                newest = max(detected, int(confirm['signal_index']) if confirm is not None else -1)
                if newest < first_live:
                    continue
                start = anchor.get('formation_start_index')
                if start is None:
                    start = time_index.get(anchor.get('pattern_start'), detected)
                start = int(start)
                bullish = spec['side'] == 'long'
                state, reason, state_at, detail = resolve_state(
                    bars, setup, confirm, start, detected, family, bullish)
                signal = int((confirm or setup)['signal_index'])
                live = state in ('forming', 'confirmed')
                since_state = n - 1 - end_index.get(state_at, n - 1)
                if grace is not None and not live and since_state > grace:
                    continue
                fit, display = _score(name, anchor['score'])
                lines, geometry_note = pattern_lines.build(
                    spec, confirm or setup, bars, formation_start=start, signal_index=signal,
                    tracking=detail, legacy=legacy_overlays)
                record = {
                    # ---- identity (timestamps + spec, never bar index)
                    'detection_id': _identity(symbol, timeframe, key[0], key[1], key[2],
                                              spec_hash, bars[start]['time'], bars[detected]['end']),
                    'episode_id': _identity(symbol, timeframe, key[0], key[1], key[2],
                                            bars[start]['time']),
                    'symbol': symbol, 'company': company, 'sector': sector,
                    'timeframe': timeframe,
                    'pattern_id': key[0], 'variant': key[1], 'side': key[2],
                    'pattern': key[0], 'pattern_name': spec['name'], 'family': family,
                    'module': name, 'definition_version': spec['definition_version'],
                    'detector_spec_hash': spec_hash, 'live_rules_version': LIVE_RULES_VERSION,
                    # ---- lifecycle
                    'state': state, 'state_reason': reason, 'state_at': state_at,
                    'detector_state': (confirm or setup)['state'],
                    'live': live, 'bars_since_state': since_state,
                    # ---- times
                    'formation_start': bars[start]['time'],
                    'detected_at_bar_end': bars[detected]['end'],
                    'signal_at_bar_end': bars[signal]['end'],
                    'confirmed_at_bar_end': (bars[int(confirm['signal_index'])]['end']
                                             if confirm is not None else None),
                    'candle_end': as_of,
                    'bars_since_signal': n - 1 - signal,
                    # ---- measures (never a probability)
                    'direction': (confirm or setup)['direction'],
                    'fit_score': fit, 'score': display,
                    'atr': float(anchor['atr']), 'price': float(bars[-1]['close']),
                    'quality_tags': sorted(set((setup or {}).get('quality_tags', []))
                                           | set((confirm or {}).get('quality_tags', []))),
                    'alias_group': anchor.get('alias_group'),
                    # ---- drawing + provenance
                    'start_index': start, 'end_index': signal,
                    'bars_in_pattern': signal - start + 1,
                    'geometry': anchor.get('geometry'),
                    'tracking': detail,
                    # The app draws from `lines`; `geometry_note` is non-empty exactly
                    # when there is nothing drawable, so it can say "marker only"
                    # instead of rendering a silent blank (pattern_lines.build).
                    'lines': lines, 'geometry_note': geometry_note,
                    'drawing_plan': pattern_lines.plan_for(spec),
                    # The aggregation layer (data.aggregate / aggregate_market15) drops any
                    # bucket that is not complete before this function ever sees it, so every
                    # bar behind this detection is a closed candle. This field restates that
                    # guarantee for the app; it is not an independent claim about the feed.
                    'candle_complete': True,
                    'input': input_snapshot,
                    # Compact enough to survive into /api/matches. 'identity_match' means the
                    # identity *permits* a research card, never that one exists: the per-cell
                    # lookup is /api/matches?evidence=true.
                    'evidence_status': evidence['status'],
                    'evidence': evidence,
                    'id': f'{symbol}:{timeframe}:{key[0]}:{key[1]}:{key[2]}',
                }
                out.append(record)
    out.sort(key=lambda r: (-r['score'], r['symbol'], r['pattern_id'], r['variant'], r['side']))
    return out


# ------------------------------------------------------------------ persistence
SCHEMA = '''
CREATE TABLE IF NOT EXISTS detections(
  detection_id TEXT PRIMARY KEY,
  episode_id TEXT NOT NULL, symbol TEXT NOT NULL, timeframe TEXT NOT NULL,
  pattern_id TEXT NOT NULL, variant TEXT NOT NULL, side TEXT NOT NULL, family TEXT NOT NULL,
  detector_spec_hash TEXT NOT NULL, live_rules_version TEXT NOT NULL,
  state TEXT NOT NULL, state_reason TEXT, state_at TEXT,
  formation_start TEXT NOT NULL, detected_at TEXT NOT NULL, signal_at TEXT NOT NULL,
  confirmed_at TEXT, direction TEXT, fit_score REAL, atr REAL,
  as_of TEXT NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL, payload TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS detections_live ON detections(state, timeframe, pattern_id);
CREATE INDEX IF NOT EXISTS detections_symbol ON detections(symbol, timeframe);
CREATE INDEX IF NOT EXISTS detections_episode ON detections(episode_id);
'''


#: Kept out of the ledger payload. ``geometry``/``lines`` are rebuilt from the
#: candles on demand; ``input``, ``evidence`` and ``drawing_plan`` are the same
#: text on every row of a scan and live in the snapshot metadata and the spec;
#: ``company``/``sector`` belong to the instrument, not the detection. Together
#: they were ~60% of a 2.1 KB row over an 89k-row pass.
LEDGER_OMIT = ('geometry', 'lines', 'input', 'evidence', 'drawing_plan', 'company', 'sector')


def ensure_schema(con):
    con.executescript(SCHEMA)


def persist(con, records, seen_at):
    """Upsert detections, preserving each instance's ``first_seen``.

    Keeping the history of an instance is what later makes per-instance outcome
    tracking possible; a scan must never lose the moment a setup first appeared.
    """
    rows = [(r['detection_id'], r['episode_id'], r['symbol'], r['timeframe'], r['pattern_id'],
             r['variant'], r['side'], r['family'], r['detector_spec_hash'], r['live_rules_version'],
             r['state'], r['state_reason'], r['state_at'], r['formation_start'],
             r['detected_at_bar_end'], r['signal_at_bar_end'], r['confirmed_at_bar_end'],
             r['direction'], r['fit_score'], r['atr'], r['candle_end'], seen_at, seen_at,
             json.dumps({k: v for k, v in r.items() if k not in LEDGER_OMIT},
                        separators=(',', ':'), allow_nan=False))
            for r in records]
    con.executemany('''INSERT INTO detections VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(detection_id) DO UPDATE SET
          state=excluded.state, state_reason=excluded.state_reason, state_at=excluded.state_at,
          confirmed_at=excluded.confirmed_at, as_of=excluded.as_of,
          last_seen=excluded.last_seen, payload=excluded.payload''', rows)
    return len(rows)


def live_counts(con):
    """``{state: n}`` over the persisted detection ledger."""
    try:
        return {state: n for state, n in
                con.execute('SELECT state, COUNT(*) FROM detections GROUP BY state')}
    except sqlite3.Error:
        return {}


#: Days of terminated detections the ledger keeps. One measured full pass over
#: the universe writes 89,246 rows at ~1.3 KB each, and every new closed bar
#: starts fresh episodes, so the ledger cannot be unbounded. Live detections are
#: never pruned by age.
RETENTION_DAYS = 30
#: Hard ceiling on terminated rows, applied after the age cut. 0 disables it.
#: At the measured row size this bounds the ledger's payload at roughly 250 MB.
#: It is the binding constraint in practice: the daily inflow reaches the cap
#: well inside 30 days. A durable per-instance outcome ledger belongs in the
#: research store, not in this scan cache.
RETENTION_MAX_ROWS = 200_000


def retention(config=None):
    """``(days, max_rows)``. Environment wins over config; 0 disables either cut."""
    def _read(name, key, default):
        raw = os.environ.get(name) or (config or {}).get(key)
        if raw in (None, ''):
            return default
        try:
            value = int(raw)
        except (TypeError, ValueError):
            raise ValueError(f'{name} must be a whole number of {"days" if "DAYS" in name else "rows"}')
        if value < 0:
            raise ValueError(f'{name} cannot be negative')
        return value
    return (_read('SCANNER_DETECTION_RETENTION_DAYS', 'detection_retention_days', RETENTION_DAYS),
            _read('SCANNER_DETECTION_MAX_ROWS', 'detection_max_rows', RETENTION_MAX_ROWS))


def prune(con, now, config=None):
    """Drop terminated detections older than the retention window.

    Only ``invalidated``/``expired`` rows are eligible: a detection that is still
    ``forming`` or ``confirmed`` is the live book and is never pruned by age,
    however long it has been standing. Returns ``{by_age, by_cap, remaining}``.
    """
    days, cap = retention(config)
    removed_age = removed_cap = 0
    if days:
        cutoff = (now - timedelta(days=days)).isoformat(sep=' ')
        removed_age = con.execute(
            "DELETE FROM detections WHERE state IN ('invalidated','expired') AND last_seen < ?",
            (cutoff,)).rowcount or 0
    if cap:
        terminated = con.execute(
            "SELECT COUNT(*) FROM detections WHERE state IN ('invalidated','expired')").fetchone()[0]
        if terminated > cap:
            # Oldest-first by the moment the row last changed, so the newest
            # terminated instances (the ones an outcome study still needs) stay.
            removed_cap = con.execute(
                "DELETE FROM detections WHERE detection_id IN ("
                " SELECT detection_id FROM detections WHERE state IN ('invalidated','expired')"
                " ORDER BY last_seen ASC, detection_id ASC LIMIT ?)",
                (terminated - cap,)).rowcount or 0
    remaining = con.execute('SELECT COUNT(*) FROM detections').fetchone()[0]
    return {'by_age': removed_age, 'by_cap': removed_cap, 'remaining': remaining,
            'retention_days': days, 'max_rows': cap}
