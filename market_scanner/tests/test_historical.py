import unittest
import numpy as np
from market_scanner.detectors import detect, pivots
from market_scanner.historical import HistoricalReplay, confirmed_pivot_positions
from test_scanner import candles,boundary_fixture


def strip_positions(matches):
    return [{k:v for k,v in m.items() if k not in ('signal_index','history_start_index','window_offset')} for m in matches]


class HistoricalTests(unittest.TestCase):
    def test_vectorized_pivots_match_original(self):
        rng=np.random.default_rng(234)
        values=rng.normal(size=700)
        values[100:110]=3
        for high in (True,False):
            np.testing.assert_array_equal(confirmed_pivot_positions(values,high),pivots(values,high))

    def test_replay_equals_scanner_on_each_signal_date(self):
        rng=np.random.default_rng(91)
        bars=candles(100+np.cumsum(rng.normal(0,.8,340)))
        replay=HistoricalReplay(bars)
        for i in range(39,len(bars),11):
            self.assertEqual(strip_positions(replay.at(i)),detect(bars[max(0,i-259):i+1]))

    def test_future_prices_cannot_change_an_earlier_detection(self):
        bars=boundary_fixture('channel')
        as_of=139
        frozen=HistoricalReplay(bars[:as_of+1]).at(as_of)
        changed=bars[:as_of+1]+candles(np.linspace(20,1000,90))
        self.assertEqual(HistoricalReplay(changed).at(as_of),frozen)
        self.assertTrue(frozen)

    def test_timestamps_and_overlay_offsets_match_the_replayed_date(self):
        bars=candles(np.r_[np.full(180,70.),[b['close'] for b in boundary_fixture('channel')]])
        replay=HistoricalReplay(bars)
        i=len(bars)-1
        matches=replay.at(i)
        self.assertTrue(matches)
        for m in matches:
            self.assertEqual(m['signal_index'],i)
            self.assertEqual(m['pattern_start'],bars[m['history_start_index']]['time'])
            self.assertEqual(m['window_offset']+m['end_index'],i)


if __name__=='__main__':unittest.main()
