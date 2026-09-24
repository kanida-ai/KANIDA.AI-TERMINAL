"""The two implementations of one walk must not drift.

`src/derivative/summary.ts` describes the instrument a reader has selected, out of a payload the page has
already fetched. `kanida_pilot/session_events.py` describes every instrument at a reading, once, for
everybody. They are the same rule, written twice, and that is exactly the arrangement this repository
already guards elsewhere (check-derivative.cjs reads metrics, derivatives.py and logic.ts and fails if their
ids, comparators or thresholds ever separate).

`server/tests/fixtures/session_walk.json` is generated from the TYPESCRIPT side by
`scripts/emit-session-walk-fixture.cjs`. This file is judged against it, never the other way round: if the
two disagree on a single field at a single reading, this test names the reading, the side and the field.
"""
import json
import pathlib

import pytest

from kanida_pilot import session_events

FIXTURE = pathlib.Path(__file__).parent / 'fixtures' / 'session_walk.json'

#: Only what both sides produce. A field one has and the other does not is a shape difference, not drift,
#: and arguing about it in a test teaches nobody anything.
SIDE_FIELDS = ('behaviour', 'state', 'strikes', 'lead', 'joined', 'left', 'lead_stable',
               'first_at', 'last_confirmed_at', 'elapsed_minutes', 'scans', 'measured', 'slots')
READING_FIELDS = ('at', 'from', 'window_minutes', 'previous_at', 'covered')


@pytest.fixture(scope='module')
def shared():
    assert FIXTURE.exists(), (
        'run `node scripts/emit-session-walk-fixture.cjs` to regenerate the shared walk fixture')
    return json.loads(FIXTURE.read_text(encoding='utf-8'))


def test_the_two_walks_agree_reading_by_reading(shared):
    walked = session_events.observe(shared['slots'])
    expected = shared['expected']
    assert len(walked) == len(expected), 'the two walks disagree on how many readings the session has'

    for mine, theirs in zip(walked, expected):
        where = str(theirs['at'])[11:16]
        for field in READING_FIELDS:
            assert mine[field] == theirs[field], f'{where}: reading field {field!r} drifted'
        for row in ('calls', 'puts'):
            for field in SIDE_FIELDS:
                assert mine[row][field] == theirs[row][field], \
                    (f'{where}: {row}.{field} drifted — server {mine[row][field]!r} '
                     f'vs browser {theirs[row][field]!r}')


def test_the_headline_is_word_for_word_the_same(shared):
    walked = session_events.observe(shared['slots'])
    for mine, theirs in zip(walked, shared['expected']):
        assert session_events.headline(mine) == theirs['headline'], \
            f"{str(theirs['at'])[11:16]}: the headline drifted"


def test_the_constants_are_the_tab_s_own():
    """A threshold that differs between the two walks would drift them silently at some future reading."""
    logic = (pathlib.Path(__file__).parents[2] / 'src' / 'derivative' / 'logic.ts').read_text(encoding='utf-8')
    assert f'SIGNAL_WINDOW_MINUTES={session_events.WINDOW_MINUTES}' in logic
    assert f'SIGNAL_WINDOW_MAX_MINUTES={session_events.WINDOW_MAX_MINUTES}' in logic
    assert f'GRID_FLAT_FRACTION={session_events.FLAT_FRACTION}' in logic
    assert f'GRID_PRICE_FLAT_FRACTION={session_events.PRICE_FLAT_FRACTION}' in logic
    assert f"SIGNAL_RULE_VERSION='{session_events.RULE_VERSION}'" in logic


def test_nothing_after_the_selected_reading_is_read(shared):
    """The walk is forward: cut the session short and every earlier reading must be byte-identical."""
    full = session_events.observe(shared['slots'])
    short = [dict(s, points=(s['points'] or [])[:6]) for s in shared['slots']]
    cut = session_events.observe(short)
    assert len(cut) == 6
    for i, entry in enumerate(cut):
        assert entry == full[i], f'reading {i} repainted when the session grew past it'


def test_an_unobserved_interval_is_not_measured_time(shared):
    """A reading taken that carried nothing freezes the elapsed time; it never extends the run."""
    holed = [dict(s, points=[dict(p, delta_oi=None, price=None) if i == 7 else p
                             for i, p in enumerate(s['points'])]) for s in shared['slots']]
    walked = session_events.observe(holed)
    last = walked[-1]
    assert last['calls']['state'] == 'not_observed'
    assert last['covered'] is False
    assert last['calls']['elapsed_minutes'] == 30, 'elapsed ran across an interval nobody observed'
    assert last['calls']['scans'] == 3, 'an unobserved reading was counted as a scan of the run'


def test_a_headline_never_calls_a_reduction_a_build(shared):
    """The same check the browser suite holds: the verb follows the open interest."""
    shrunk = []
    for slot in shared['slots']:
        if slot['strike'] == 23350:
            deltas = [0, 0, 0, 0, -20000, -40000, -60000, -80000]
            prices = [80, 80, 80, 80, 70, 60, 50, 40]
            slot = dict(slot, points=[dict(p, delta_oi=deltas[i], price=prices[i])
                                      for i, p in enumerate(slot['points'])])
        shrunk.append(slot)
    walked = session_events.observe(shrunk)
    last = walked[-1]
    lead = last['calls'] if last['calls']['weight'] >= last['puts']['weight'] else last['puts']
    if lead['behaviour'] in session_events.EXITS:
        said = session_events.headline(last)
        assert 'build' not in said.lower(), f'a reduction headlined as a build: {said!r}'


def test_summarise_names_the_instrument_and_claims_no_participant(shared):
    row = session_events.summarise('NIFTY', session_events.observe(shared['slots']))
    assert row['underlying'] == 'NIFTY'
    assert row['rule_version'] == session_events.RULE_VERSION
    banned = ('buying', 'writing', 'covering', 'unwinding', 'bullish', 'bearish', 'will', 'expect')
    for word in banned:
        assert word not in row['headline'].lower(), f'a market-list headline may not say {word!r}'


def test_an_empty_payload_writes_nothing():
    for bad in (None, [], [{'present': False}], [{'present': True, 'points': []}]):
        assert session_events.observe(bad) == []
    assert session_events.summarise('X', []) is None


# --- THE FIRST HOUR (found live 21 Sep 2026: "0 of 216", every row "Not enough readings yet to compare") ------------
def _slots(marks, ce, pe):
    """Ten ATM slots. `ce`/`pe`: {strike: [(oi, delta_oi, price) per mark]}."""
    out = []
    for row, kind, table in (('calls', 'CE', ce), ('puts', 'PE', pe)):
        for strike, series in table.items():
            out.append({'present': True, 'row': row, 'option_type': kind, 'strike': strike,
                        'points': [{'at': m, 'oi': o, 'delta_oi': d, 'price': p}
                                   for m, (o, d, p) in zip(marks, series)]})
    return out


MARKS = ['2026-09-21 09:30:00', '2026-09-21 09:45:00', '2026-09-21 10:00:00']
CE = {23400: [(2000, 0, 80), (2600, 600, 72), (3100, 1100, 66)],
      23500: [(3000, 0, 50), (3400, 400, 45), (3800, 800, 40)],
      23600: [(1500, 0, 30), (1500, 0, 30), (1500, 0, 30)]}
PE = {23300: [(2500, 0, 60), (2520, 20, 60), (2530, 30, 60)],
      23200: [(1800, 0, 40), (1800, 0, 40), (1800, 0, 40)]}


def test_the_opening_reading_is_read_for_where_positions_sit():
    slots = _slots(MARKS[:1], {k: v[:1] for k, v in CE.items()}, {k: v[:1] for k, v in PE.items()})
    row = session_events.summarise('NIFTY', session_events.observe(slots), slots)
    assert row['state'] == 'opening'
    assert row['headline'].startswith('Calls concentrated around 23,400–23,600 CE')
    assert 'puts around 23,200–23,300 PE' in row['headline']
    assert row['live'] is False, 'where positions sit is not a behaviour, and is not counted as one'
    assert row['first_at'] is None and row['elapsed_minutes'] is None
    assert 'not enough' not in row['headline'].lower()


def test_inside_the_first_hour_the_latest_fifteen_minutes_are_read():
    slots = _slots(MARKS, CE, PE)
    walk = session_events.observe(slots)
    assert walk[-1]['calls']['state'] == 'no_baseline', 'the hour-wide window is genuinely not full'
    row = session_events.summarise('NIFTY', walk, slots)
    assert row['live'] is True
    assert row['state'] == 'building'
    assert row['headline'].startswith('Call positions building')
    assert '23,400' in row['headline']
    # PERSISTENCE IS WITHHELD, NOT BORROWED
    assert row['first_at'] is None and row['elapsed_minutes'] is None and row['scans'] == 0
    assert 'not enough' not in row['headline'].lower()


def test_once_the_hour_is_full_the_episode_speaks_again():
    """The fallback only fills the gap: a walk whose latest reading HAS its hour-wide baseline is summarised
    exactly as it was before the fallback existed."""
    marks = [f'2026-09-21 {h}:{m}:00' for h, m in (('09', '30'), ('09', '45'), ('10', '00'), ('10', '15'), ('10', '30'))]
    ce = {23400: [(2000 + 600 * i, 600 * i, 80 - 6 * i) for i in range(5)],
          23500: [(3000 + 400 * i, 400 * i, 50 - 5 * i) for i in range(5)]}
    pe = {23300: [(2500, 0, 60)] * 5}
    slots = _slots(marks, ce, pe)
    walk = session_events.observe(slots)
    assert walk[-1]['calls']['state'] != 'no_baseline', 'the fixture must reach a full hour'
    assert session_events.summarise('X', walk, slots) == session_events.summarise('X', walk)


def test_the_first_hour_list_picks_the_side_that_moved_more_open_interest():
    """Found live 21 Sep 10:00: the list said calls while the pane said puts for one interval. In the first hour
    the list chooses the side on TOTAL OI change — the pane's basis — whatever the premium did."""
    marks = MARKS[:2]
    ce = {23400: [(2000, 0, 80), (2400, 400, 76)]}
    pe = {23300: [(2500, 0, 60), (3500, 1000, 60)], 23200: [(1800, 0, 40), (2300, 500, 40)]}
    slots = _slots(marks, ce, pe)
    row = session_events.summarise('NIFTY', session_events.observe(slots), slots)
    assert row['row'] == 'puts' and row['headline'].startswith('Put positions building')
    assert row['lead'] == 23300
    assert 'writing' not in row['headline'] and 'buying' not in row['headline']
