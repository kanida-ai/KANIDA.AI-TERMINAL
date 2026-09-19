"""Explicit CH01--CH10 adapter over the existing causal chart replay."""
from __future__ import annotations

from ..backtest import episodes
from ..backtest_worker import SIDES
from ..detectors import NAMES
from ..historical import HistoricalReplay
from ..replay_filter import masks

IDS = dict(zip(('cup_handle','horizontal_breakout','flag_pole','symmetrical_triangle',
    'falling_wedge','rising_wedge','channel','descending_triangle','head_shoulders',
    'inverse_head_shoulders'), ('CH01','CH02','CH03','CH04','CH05','CH06','CH07','CH08','CH09','CH10')))


def specifications():
    return [dict(pattern_id=identifier,variant='legacy_1.0.1',side=side,name=NAMES[name],
                 family='chart',definition_version='1.0.1',
                 states=['confirmed'] if name=='horizontal_breakout' else ['setup'] if name=='channel' else ['setup','confirmed'],
                 lookback=40,definition='Unchanged legacy rules; see frozen RULES.md')
            for name,identifier in IDS.items() for side in SIDES[name]]


def detect(bars, timeframe):
    result={(s['pattern_id'],s['variant'],s['side']):[] for s in specifications()}
    if len(bars)<40:
        return result
    replay = HistoricalReplay(bars,260,list(IDS))
    mask = masks(replay.h,replay.l,replay.c,replay.v,replay.tr,replay.tops,replay.bottoms,260)
    groups,_ = episodes(replay,mask,3)
    result.update({(IDS[name],'legacy_1.0.1',side):events for (name,side),events in groups.items()})
    return result
