"""Transparent per-study screens, shared by historical listings and scanner cards."""
import math
import re
from decimal import Decimal, ROUND_HALF_UP

PROFILES = ('positive', 'high_wr', 'strong')
DEFAULT_MIN_TRADES = 5
RETURN_BANDS = {
    '0_0.5': (0, .5, False), '0.5_1': (.5, 1, False), '1_2': (1, 2, False),
    '2_5': (2, 5, False), '5_10': (5, 10, True), 'over_10': (10, None, False),
}


def displayed_return(value):
    if value is None or not math.isfinite(value):
        return None
    return float(Decimal(str(value)).quantize(Decimal('.01'), rounding=ROUND_HALF_UP))


def return_band(value):
    value = displayed_return(value)
    if value is None or value < 0:
        return None
    for key, (low, high, inclusive) in RETURN_BANDS.items():
        if high is None and value > low:
            return key
        if high is not None and value >= low and (value <= high if inclusive else value < high):
            return key
    return None


def holding_details(timeframe, hold, maximum=False):
    if not isinstance(hold, int) or hold < 1:
        return None
    if timeframe in ('1H', '4H'):
        per_day = 7 if timeframe == '1H' else 2
        low, high = math.ceil(hold / per_day), math.ceil((hold + per_day - 1) / per_day)
        duration = '1 trading day' if high == 1 else f'{low}–{high} trading days' if low != high else f'{high} trading days'
        overnight = high > 1
    elif timeframe == '1D':
        duration, overnight = f'{hold} trading day'+('s' if hold != 1 else ''), hold > 1
    elif timeframe == '1W':
        duration, overnight = f'About {hold} week'+('s' if hold != 1 else ''), True
    else:
        return None
    return {'bars': hold, 'maximum': maximum, 'duration': duration, 'can_carry_overnight': overnight,
            'label': ('Up to ' if maximum else '') + duration + (' · may carry overnight' if overnight else ' · within one session'),
            'detail': f'{hold} {timeframe} chart candles'+(' maximum; stops/targets can exit earlier.' if maximum else '; exit at the final candle close.')+
                      ' Only market candles count. Nights, weekends and holidays add elapsed time. Durations assume normal market sessions.'}


def decorate(summary):
    result = dict(summary)
    for mode in ('reference', 'test'):
        stats = dict(summary.get(mode) or {'n': 0})
        hold = (stats.get('rule') or {}).get('hold') if mode == 'reference' else (summary.get('rule') or {}).get('hold')
        # Older frozen summaries retain the selected rule's description, not its object.
        if mode == 'test' and hold is None:
            match = re.search(r'(?:^|; )max (\d+) candles$', summary.get('rule_description') or '')
            hold = int(match[1]) if match else None
        stats.update(display_return_pct=displayed_return(stats.get('expectancy_pct')),
                     return_band=return_band(stats.get('expectancy_pct')),
                     holding=holding_details(summary.get('timeframe'), hold, maximum=mode == 'test'))
        result[mode] = stats
    return result


def screen_args(filters):
    mode = filters.get('mode') or 'reference'
    profile = filters.get('performance') or ''
    if mode not in ('reference', 'test') or profile not in ('', *PROFILES):
        raise ValueError('Unknown historical basis or performance filter')
    if filters.get('return_band') and filters['return_band'] not in RETURN_BANDS:
        raise ValueError('Unknown return range')
    try:
        minimum = int(filters.get('min_trades') if filters.get('min_trades') not in (None, '') else DEFAULT_MIN_TRADES)
    except (TypeError, ValueError):
        raise ValueError('Minimum trades must be a whole number') from None
    if not 0 <= minimum <= 100000:
        raise ValueError('Minimum trades must be between 0 and 100,000; 0 shows any sample size')
    if profile == 'strong':
        mode, minimum = 'test', max(20, minimum)
    return mode, profile, minimum


# --- the legacy-history screen against matches that have no legacy history -------------------------------
# `screen_stats` reads a match's LEGACY backtest history. A research detection deliberately carries none
# (joining one by pattern name alone is the fallback EVIDENCE_SERVING_CONTRACT.md forbids), so `any(...)`
# over that empty history is False for every row and the screen would silently delete the whole book.
# It is therefore not run on research matches - and not quietly ignored either: `history_screen_report` is
# served in the body, and a caller that explicitly ASKED for the screen is refused with REFUSAL below.
NO_LEGACY_HISTORY = 'research_matches_have_no_legacy_history'
NO_LEGACY_HISTORY_MESSAGE = (
    'No sample-size, performance or return-range screen is applied to these matches. That screen reads the '
    'legacy backtest history, and a research detection carries none by design. Each detection\'s evidence is '
    'its own research cell: request it with evidence=true and read research_evidence / research_cell_status.')
NO_LEGACY_HISTORY_REFUSAL = (
    'This screen (performance, return_band or min_trades) needs the legacy backtest history, and research '
    'detections carry none, so it cannot be applied and will not be faked. Drop it, or send min_trades=0, and '
    'read each detection\'s own evidence with evidence=true.')


def history_screen_requested(filters):
    """True when the caller ASKED for the legacy-history screen, rather than inheriting the server default.

    A bare request asked for nothing: `DEFAULT_MIN_TRADES` is this module's default, not the caller's
    intent. `min_trades=0` is the explicit "any sample size", which is the absence of a screen.
    Call `screen_args` first; the values here are assumed already validated.
    """
    if filters.get('performance') or filters.get('return_band'):
        return True
    raw = filters.get('min_trades')
    return raw not in (None, '') and int(raw) > 0


def history_screen_report(filters):
    """The machine-readable statement served beside research matches: the screen did NOT run, and why.

    `skipped` is the screen that would have run on a legacy match (so a bare request shows the
    `DEFAULT_MIN_TRADES` one it no longer silently applies), or None when no screen was in play at all.
    """
    mode, profile, minimum = screen_args(filters)
    band = filters.get('return_band') or None
    return {'applied': False, 'reason': NO_LEGACY_HISTORY, 'default_min_trades': DEFAULT_MIN_TRADES,
            'skipped': {'mode': mode, 'performance': profile or None, 'return_band': band,
                        'min_trades': minimum} if (profile or band or minimum) else None,
            'evidence': 'evidence=true / research_cell_status', 'message': NO_LEGACY_HISTORY_MESSAGE}


def profiles(stats, mode):
    expectancy, wr = stats.get('expectancy_pct'), stats.get('win_rate')
    if not stats.get('n') or expectancy is None or not math.isfinite(expectancy) or expectancy <= 0:
        return []
    result = ['positive']
    if wr is not None and wr >= 60:
        result.append('high_wr')
    ci = stats.get('expectancy_ci95')
    if mode == 'test' and stats['n'] >= 20 and ci and ci[0] is not None and ci[0] > 0:
        result.append('strong')
    return result


def sql_screen(filters):
    mode, profile, minimum = screen_args(filters)
    metric = lambda key: f"json_extract(summary,'$.{mode}.{key}')"
    conditions, args = [], []
    if minimum:
        conditions.append(metric('n') + '>=?')
        args.append(minimum)
    if profile:
        conditions.append(metric('expectancy_pct') + '>0')
        if profile == 'high_wr':
            conditions.append(metric('win_rate') + '>=60')
        if profile == 'strong':
            conditions.append(metric('expectancy_ci95[0]') + '>0')
    band = filters.get('return_band')
    if band:
        low, high, inclusive = RETURN_BANDS[band]
        value = 'kanida_display_return(' + metric('expectancy_pct') + ')'
        conditions.append(metric('n') + '>0')
        conditions.append(value + ('>?' if high is None else '>=?'));args.append(low)
        if high is not None:
            conditions.append(value + ('<=?' if inclusive else '<?'));args.append(high)
    return mode, conditions, args


def screen_stats(stats, filters):
    mode, profile, minimum = screen_args(filters)
    if (stats.get('n') or 0) < minimum:
        return False
    if profile and profile not in profiles(stats, mode):
        return False
    band = filters.get('return_band')
    return not band or bool(stats.get('n')) and return_band(stats.get('expectancy_pct')) == band


def compact(summary, run):
    summary = decorate(summary)
    result = {'run': run, 'side': summary['side'], 'status': summary['status']}
    for mode in ('reference', 'test'):
        stats = summary.get(mode) or {'n': 0}
        result[mode] = {key: stats.get(key) for key in
                       ('n', 'win_rate', 'expectancy_pct', 'expectancy_ci95', 'max_adverse_pct',
                        'return_band', 'display_return_pct', 'holding')}
        result[mode]['profiles'] = profiles(stats, mode)
    return result
