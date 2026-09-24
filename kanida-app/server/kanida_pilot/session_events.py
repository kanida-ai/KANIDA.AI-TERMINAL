"""The session walk, server side — one pass per reading, shared by every reader.

WHY THIS EXISTS AS A SECOND IMPLEMENTATION, AND WHAT KEEPS IT HONEST.

The same walk lives in `src/derivative/summary.ts`, where it describes ONE instrument out of a payload the
page has already fetched. That is the right place for it when a reader has chosen a name. It is the wrong
place for "what is happening across the market": answering that in the browser means one request per
instrument, two hundred of them, repeated in every reader's browser, and no way to alert on the result.

So the walk is here too, and the drift that a second implementation invites is held by
`server/tests/test_session_events.py`, which runs BOTH over one shared fixture
(`server/tests/fixtures/session_walk.json`) and fails if they disagree on a single field. The fixture is
generated from the TypeScript side; this module is judged against it, never the other way round.

NOTHING HERE IS A NEW ANALYTIC. The behaviour table, the flat bands, the hour-wide window and the strength
ladder are the tab's own (`FLOW_LABELS`, rule `signal/2`). This file adds no threshold.
"""
from __future__ import annotations

#: The tab's own window: each reading is measured against the latest one at least an hour earlier, and no
#: more than two hours earlier. Mirrored from logic.ts — check-derivative.cjs fails if the two drift.
WINDOW_MINUTES = 60
WINDOW_MAX_MINUTES = 120
#: A change under this fraction of the side's own largest change SO FAR is flat, not a direction.
FLAT_FRACTION = 0.05
PRICE_FLAT_FRACTION = 0.05
RULE_VERSION = 'signal/2'

#: The four behaviours that carry a story, and the two of them that are an exit rather than an entry.
DIRECTIONAL = ('buying', 'writing', 'short_covering', 'buyers_exiting')
EXITS = ('short_covering', 'buyers_exiting')
#: More than a quarter larger than the reading before is strengthening; more than a quarter smaller is
#: slowing. The block's own ratio, not a new constant.
PACE_UP, PACE_DOWN = 1.25, 0.75


def number(value):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out and out not in (float('inf'), float('-inf')) else None


def reading_minutes(at):
    """Minutes since the epoch for a `YYYY-MM-DD HH:MM` stamp, or None when it cannot be read."""
    text = str(at or '').strip()
    if len(text) < 16:
        return None
    try:
        y, m, d = int(text[0:4]), int(text[5:7]), int(text[8:10])
        hh, mm = int(text[11:13]), int(text[14:16])
    except ValueError:
        return None
    # the same arithmetic logic.ts uses: a UTC day number plus the clock, never a local timezone
    from datetime import datetime, timezone
    try:
        day = datetime(y, m, d, tzinfo=timezone.utc).timestamp() / 60
    except ValueError:
        return None
    return day + hh * 60 + mm


def baseline_index(times, i, window=WINDOW_MINUTES, limit=WINDOW_MAX_MINUTES):
    """The reading THIS one is compared against: the latest at least `window` minutes earlier, and no more
    than `limit` earlier. None when the session holds none — an early reading, or a hole wider than the
    window. Never widened to find one."""
    now = reading_minutes(times[i] if i < len(times) else None)
    if now is None:
        return None
    for j in range(i - 1, -1, -1):
        then = reading_minutes(times[j])
        if then is None:
            continue
        gap = now - then
        if gap < window:
            continue
        return j if gap <= limit else None
    return None


def side_change(slots, i, back, key):
    """The change one side made between two readings, over the contracts that carried a value at BOTH — a
    like-for-like basket, and the count of what was in it."""
    now = then = 0.0
    n = 0
    for slot in slots:
        points = slot.get('points') or []
        a = number((points[i] or {}).get(key)) if i < len(points) else None
        b = number((points[back] or {}).get(key)) if back < len(points) else None
        if a is None or b is None:
            continue
        now += a
        then += b
        n += 1
    return (now - then, n) if n else (None, 0)


def side_reading(slots, i, back, kind, peak, price_peak):
    """One side (or one contract, handed a one-slot basket) read at reading `i` against reading `back`."""
    if back is None:
        return None
    change, contracts = side_change(slots, i, back, 'delta_oi')
    if change is None:
        return None
    price_change, _ = side_change(slots, i, back, 'price')
    flat = peak > 0 and abs(change) < FLAT_FRACTION * peak
    oi_direction = 'flat' if (peak <= 0 or flat) else ('building' if change > 0 else 'unwinding')
    if price_change is None:
        price_direction = 'no baseline'
    elif price_peak <= 0 or abs(price_change) < PRICE_FLAT_FRACTION * price_peak:
        price_direction = 'flat'
    else:
        price_direction = 'up' if price_change > 0 else 'down'
    return {'change': change, 'price_change': price_change, 'contracts': contracts,
            'oi_direction': oi_direction, 'price_direction': price_direction}


def behaviour_of(kind, price_direction, oi_direction):
    """The tab's own table, grouped: four that name a participant, two that name a position change without
    one, three that name no change at all."""
    p, o = str(price_direction or ''), str(oi_direction or '')
    if not p or not o or p == 'no baseline' or o == 'no baseline':
        return 'none'
    if o == 'building':
        return 'buying' if p == 'up' else ('writing' if p == 'down' else 'positions_added')
    if o == 'unwinding':
        return 'short_covering' if p == 'up' else ('buyers_exiting' if p == 'down' else 'positions_closing')
    return 'quiet' if p == 'flat' else 'price_only'


def side_behaviour(rows):
    """The side's behaviour: the one the most strikes read, and — when two tie — the one carrying the larger
    total position change. A tie that survives both tests is NOT resolved."""
    bag = {}
    for row in rows:
        if row['behaviour'] not in DIRECTIONAL:
            continue
        cur = bag.setdefault(row['behaviour'], {'n': 0, 'weight': 0.0, 'strikes': []})
        cur['n'] += 1
        cur['weight'] += abs(number(row['oi_change']) or 0)
        cur['strikes'].append(row['strike'])
    if not bag:
        return 'none', []
    ranked = sorted(bag.items(), key=lambda kv: (-kv[1]['n'], -kv[1]['weight']))
    if len(ranked) > 1 and ranked[0][1]['n'] == ranked[1][1]['n'] \
            and ranked[0][1]['weight'] == ranked[1][1]['weight']:
        return 'none', []
    return ranked[0][0], sorted({s for s in ranked[0][1]['strikes'] if s is not None})


HEADLINES = {
    'appeared': lambda s, n, x: f"{s} positions begin {'reducing' if x else 'building'}",
    'continued': lambda s, n, x: f"{s} positions keep {'reducing' if x else 'building'} at the same strikes",
    'strengthened': lambda s, n, x: f"{s} position {'reduction' if x else 'building'} picks up",
    'slowed': lambda s, n, x: f"{s} position {'reduction' if x else 'building'} slows",
    'broadened': lambda s, n, x: (f"{s} positions {'reduce' if x else 'build'} at "
                                  + ('another strike' if n == 1 else f'{n} more strikes')),
    'narrowed': lambda s, n, x: (f"{s} position {'reduction' if x else 'building'} narrows to "
                                 + ('one strike' if n == 1 else f'{n} strikes')),
    'shifted': lambda s, n, x: f"The largest {s.lower()} change moves to another strike",
    'unwound': lambda s, n, x: f"Earlier {s.lower()} positions are being reduced",
    'faded': lambda s, n, x: f"No further {s.lower()} position change this interval",
    'quiet': lambda s, n, x: 'No material change at this reading',
    'not_observed': lambda s, n, x: 'This reading carried no comparable value',
    'no_baseline': lambda s, n, x: 'Not enough readings yet to compare',
}


def observe(slots, window=WINDOW_MINUTES, limit=WINDOW_MAX_MINUTES):
    """THE FORWARD WALK. One entry per reading, built from that reading and the ones before it — never from
    one after it, which is what stops history repainting when the session grows."""
    slots = [s for s in (slots or []) if s and s.get('present')]
    if not slots:
        return []
    calls = [s for s in slots if s.get('row') != 'puts']
    puts = [s for s in slots if s.get('row') == 'puts']
    length = max((len(s.get('points') or []) for s in slots), default=0)
    if not length:
        return []
    times = []
    for i in range(length):
        stamp = None
        for s in slots:
            points = s.get('points') or []
            if i < len(points) and (points[i] or {}).get('at'):
                stamp = points[i]['at']
                break
        times.append(stamp)

    state = {'calls': {'peak': 0.0, 'price_peak': 0.0}, 'puts': {'peak': 0.0, 'price_peak': 0.0}}
    runs = {'calls': None, 'puts': None}
    before = {'calls': None, 'puts': None}
    out = []

    for i in range(length):
        at = times[i]
        back = baseline_index(times, i, window, limit)
        span = None if back is None else (reading_minutes(at) or 0) - (reading_minutes(times[back]) or 0)

        def read_side(items, kind, row):
            keep = state[row]
            first = side_reading(items, i, back, kind, keep['peak'], keep['price_peak'])
            oi = first['change'] if first else None
            pr = first['price_change'] if first else None
            peak = keep['peak'] if oi is None else max(keep['peak'], abs(oi))
            price_peak = keep['price_peak'] if pr is None else max(keep['price_peak'], abs(pr))
            keep['peak'], keep['price_peak'] = peak, price_peak

            measured = 0
            rows = []
            for slot in items:
                one = side_reading([slot], i, back, kind, peak, price_peak)
                if one:
                    measured += 1
                behaviour = behaviour_of(kind, one['price_direction'], one['oi_direction']) if one else 'none'
                rows.append({'strike': number(slot.get('strike')), 'side': kind,
                             'oi_change': one['change'] if one else None,
                             'price_change': one['price_change'] if one else None,
                             'behaviour': behaviour})

            behaviour, strikes = side_behaviour(rows)
            active = [r for r in rows if r['behaviour'] == behaviour and r['strike'] is not None
                      and behaviour in DIRECTIONAL]
            lead = None
            for r in active:
                if lead is None or abs(number(r['oi_change']) or 0) > abs(number(lead['oi_change']) or 0):
                    lead = r
            weight = sum(abs(number(r['oi_change']) or 0) for r in active)

            prev = before[row]
            joined = [s for s in strikes if not prev or s not in prev['strikes']] if prev else []
            left = [s for s in prev['strikes'] if s not in strikes] if prev else []

            covered = measured > 0
            run = runs[row]
            if back is None:
                word = 'no_baseline'
            elif not covered:
                word = 'not_observed'
            elif behaviour == 'none':
                word = 'faded' if run else 'quiet'
                run = None
            elif not run or run['behaviour'] != behaviour:
                word = ('unwound' if run and behaviour in EXITS and run['behaviour'] in DIRECTIONAL
                        and run['behaviour'] not in EXITS else 'appeared')
                run = {'behaviour': behaviour, 'first': at, 'last': at, 'scans': 1, 'strikes': strikes,
                       'lead': lead['strike'] if lead else None, 'weight': weight, 'lead_stable': True}
            else:
                grew = bool(joined) and not left
                shrank = bool(left) and not joined
                moved = bool(lead) and run['lead'] is not None and lead['strike'] is not None \
                    and lead['strike'] != run['lead']
                if grew:
                    word = 'broadened'
                elif shrank:
                    word = 'narrowed'
                elif moved:
                    word = 'shifted'
                elif run['weight'] > 0 and weight > run['weight'] * PACE_UP:
                    word = 'strengthened'
                elif run['weight'] > 0 and weight < run['weight'] * PACE_DOWN:
                    word = 'slowed'
                else:
                    word = 'continued'
                run = {'behaviour': run['behaviour'], 'first': run['first'], 'last': at,
                       'scans': run['scans'] + 1, 'strikes': strikes,
                       'lead': lead['strike'] if lead else run['lead'], 'weight': weight,
                       # the claim "X has led throughout" is only available while this holds
                       'lead_stable': run['lead_stable'] and (
                           not lead or lead['strike'] is None or lead['strike'] == run['lead'])}
            runs[row] = run

            first_at = run['first'] if run else None
            started = reading_minutes(first_at)
            # elapsed is measured to the last reading that CARRIED a value, never to the one on screen: an
            # interval nobody observed is not counted as time the behaviour continued
            here = reading_minutes(run['last'] if run else at)
            elapsed = (here - started) if (run and started is not None and here is not None) else None

            built = {'side': kind, 'row': row, 'behaviour': behaviour, 'strikes': strikes,
                     'lead': lead['strike'] if lead else None,
                     'lead_oi_change': lead['oi_change'] if lead else None,
                     'joined': joined, 'left': left, 'weight': weight,
                     'lead_stable': bool(run and run['lead_stable']), 'state': word,
                     'first_at': first_at, 'last_confirmed_at': run['last'] if run else None,
                     'elapsed_minutes': None if elapsed is None else int(elapsed),
                     'scans': run['scans'] if run else 0,
                     'measured': measured, 'slots': len(items)}
            before[row] = built
            return built

        c = read_side(calls, 'CE', 'calls')
        p = read_side(puts, 'PE', 'puts')
        out.append({'at': at, 'from': None if back is None else times[back],
                    'window_minutes': None if span is None else int(span),
                    'previous_at': times[i - 1] if i > 0 else None,
                    'covered': c['measured'] > 0 or p['measured'] > 0,
                    'calls': c, 'puts': p, 'rule_version': RULE_VERSION})
    return out


def headline(observation):
    """The Tier-1 line for one reading: what the measured numbers did, naming no participant."""
    if not observation:
        return ''
    calls, puts = observation['calls'], observation['puts']
    lead = calls if calls['weight'] >= puts['weight'] else puts
    side = 'Call' if lead['row'] == 'calls' else 'Put'
    state = lead['state']
    moved = len(lead['joined']) if state == 'broadened' else (
        len(lead['strikes']) if state == 'narrowed' else 0)
    exiting = lead['behaviour'] in EXITS
    return (HEADLINES.get(state) or HEADLINES['continued'])(side, moved, exiting)


INTERVAL_MINUTES = 15


def _span(strikes, code):
    """'23,400 CE' or '23,400–23,600 CE' — the strikes a side's reading covers, as a range."""
    got = sorted(s for s in (strikes or []) if s is not None)
    if not got:
        return ''
    text = lambda v: f"{int(v):,}" if float(v).is_integer() else f"{v:,}"
    return f"{text(got[0])} {code}" if len(got) == 1 else f"{text(got[0])}–{text(got[-1])} {code}"


def first_hour(slots, observations):
    """THE FIRST HOUR, read for what it CAN say. Found live 21 Sep 2026: until 10:30 every row of the market list
    read "Not enough readings yet to compare" and the list reported 0 of 216, while every instrument's last fifteen
    minutes were fully measured. Two cases, and neither claims a persistence it has not observed:

      * one reading only  -> the OPENING READ: where open interest is concentrated near the money, as levels.
      * two or more       -> the latest fifteen minutes, from the SAME walk run over a one-step window, so the
                             words are the tab's own behaviour rules and nothing new. No start time, no elapsed
                             minutes: a fifteen-minute move describes itself and claims no more.
    """
    slots = [s for s in (slots or []) if s and s.get('present')]
    if not slots or not observations:
        return None
    if len(observations) == 1:
        def heavy(row):
            ranked = []
            for s in slots:
                if (s.get('row') == 'puts') != (row == 'puts'):
                    continue
                points = s.get('points') or []
                oi = number((points[-1] or {}).get('oi')) if points else None
                if oi is not None and oi > 0 and number(s.get('strike')) is not None:
                    ranked.append((oi, number(s.get('strike'))))
            ranked.sort(reverse=True)
            return [k for _, k in ranked[:3]], sum(1 for _ in ranked)
        calls, nc = heavy('calls')
        puts, npt = heavy('puts')
        if not calls and not puts:
            return None
        parts = []
        if calls:
            parts.append(f"Calls concentrated around {_span(calls, 'CE')}")
        if puts:
            parts.append(f"{'puts' if calls else 'Puts'} around {_span(puts, 'PE')}")
        return {'headline': '; '.join(parts), 'state': 'opening', 'behaviour': 'none', 'live': False,
                'strikes': calls or puts, 'lead': None, 'side': 'CE' if calls else 'PE',
                'row': 'calls' if calls else 'puts', 'first_at': None, 'elapsed_minutes': None, 'scans': 0,
                'lead_stable': False, 'measured': nc + npt, 'weight': 0}
    # THE LATEST FIFTEEN MINUTES, side by side: which side added (or removed) more open interest, where, and
    # which strike carried most of it. The side is chosen on the TOTAL change — the same basis the middle pane
    # uses in the first hour, so the list and the pane can never name different sides for one interval (found
    # live 21 Sep 10:00: the list said calls, the pane said puts). No behaviour word here: whether premium moved
    # enough to call it writing or buying is the pane's judgement, made with the premium in front of it.
    def side_moves(row):
        moves = []
        for s in slots:
            if (s.get('row') == 'puts') != (row == 'puts'):
                continue
            points = s.get('points') or []
            if len(points) < 2:
                continue
            a_, b_ = number((points[-1] or {}).get('delta_oi')), number((points[-2] or {}).get('delta_oi'))
            k = number(s.get('strike'))
            if a_ is None or b_ is None or k is None or a_ == b_:
                continue
            moves.append((k, a_ - b_))
        return moves
    calls, puts = side_moves('calls'), side_moves('puts')
    ct, pt = sum(c for _, c in calls), sum(c for _, c in puts)
    if not calls and not puts:
        return None
    row = 'puts' if abs(pt) > abs(ct) else 'calls'
    moves, total = (puts, pt) if row == 'puts' else (calls, ct)
    if not moves or total == 0:
        return None
    code, side = ('PE', 'Put') if row == 'puts' else ('CE', 'Call')
    same = [(k, c) for k, c in moves if (c > 0) == (total > 0)]
    strikes = sorted(k for k, _ in same)
    lead = max(same, key=lambda m: abs(m[1]))[0] if same else None
    where = _span(strikes, code)
    verb = 'building' if total > 0 else 'reducing'
    return {'headline': f"{side} positions {verb} {'at' if len(strikes) == 1 else 'across'} {where}".strip(),
            'state': 'building' if total > 0 else 'reversing', 'behaviour': 'positions_added' if total > 0 else 'reduced',
            'live': True, 'strikes': strikes, 'lead': lead, 'side': code, 'row': row,
            'first_at': None, 'elapsed_minutes': None, 'scans': 0, 'lead_stable': False,
            'measured': len(calls) + len(puts), 'weight': abs(total)}


def summarise(underlying, observations, slots=None):
    """One instrument, as the market list needs it: the latest reading, in one line, with the facts that
    make it worth looking at."""
    if not observations:
        return None
    now = observations[-1]
    calls, puts = now['calls'], now['puts']
    lead = calls if calls['weight'] >= puts['weight'] else puts
    live = lead['behaviour'] in DIRECTIONAL
    # the hour-wide window is not full yet: say what the first hour CAN say (first_hour), never "not enough"
    early = first_hour(slots, observations) if (slots and lead['state'] == 'no_baseline') else None
    row = {
        'underlying': underlying,
        'at': now['at'], 'previous_at': now['previous_at'],
        'headline': headline(now),
        'side': lead['side'], 'row': lead['row'],
        'state': lead['state'], 'behaviour': lead['behaviour'],
        'strikes': lead['strikes'], 'lead': lead['lead'],
        'first_at': lead['first_at'], 'elapsed_minutes': lead['elapsed_minutes'],
        'scans': lead['scans'], 'lead_stable': lead['lead_stable'],
        'measured': lead['measured'], 'slots': lead['slots'],
        # the size behind it, used only to order the list — never shown as a score
        'weight': lead['weight'],
        'live': live,
        'rule_version': RULE_VERSION,
    }
    if early:
        row.update(early)
    return row
