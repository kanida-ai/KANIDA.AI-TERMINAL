"""Chronological replay of the SAME detector rules; no trade assumptions.

This module prepares stock-specific signal histories. It deliberately does not
turn detections into trades: entry/exit/holding/cost rules must be specified.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import datetime
from pathlib import Path
import hashlib
import json
import time

import numpy as np

from .data import Calendar, ROOT, aggregate, connect_source, load_config, now_ist, observed_sessions
from .detectors import Detector, NAMES


def load_full_history(con, symbol, intraday, through):
    """Read all available indexed rows for one stock, not the scanner's tail."""
    table='ohlc_5min' if intraday else 'ohlc_daily'
    return con.execute(f'''SELECT bar_time,open,high,low,close,volume FROM {table}
        WHERE symbol=? AND bar_time<=? ORDER BY bar_time''',
        (symbol,through.isoformat(sep=' '))).fetchall()


def confirmed_pivot_positions(values, high=True):
    """All candidate pivots; callers MUST enforce confirmation at signal time."""
    if len(values)<7:
        return np.empty(0,dtype=int)
    windows=np.lib.stride_tricks.sliding_window_view(values,7)
    center=windows[:,3]
    extreme=windows.max(axis=1) if high else windows.min(axis=1)
    return np.flatnonzero((center==extreme)&((windows==center[:,None]).sum(axis=1)==1))+3


class HistoricalReplay:
    """Reuses Detector methods with inputs sliced strictly at each signal close.

    OHLC and pivots are precomputed for speed. No pivot to the right of t-3 is
    visible at t, and no data older than the scanner's 260-bar window is exposed.
    This is verified against the ordinary scanner and truncated-history replay.
    """
    def __init__(self,bars,lookback=260,enabled=None):
        self.bars=bars
        self.lookback=lookback
        self.enabled=set(NAMES if enabled is None else enabled)
        self.o,self.h,self.l,self.c,self.v=(np.array([b[k] for b in bars],dtype=float) for k in ('open','high','low','close','volume'))
        self.tops=confirmed_pivot_positions(self.h)
        self.bottoms=confirmed_pivot_positions(self.l,False)
        self.tr=np.maximum(self.h-self.l,np.maximum(abs(self.h-np.roll(self.c,1)),abs(self.l-np.roll(self.c,1))))
        self.slots={}
        for i,b in enumerate(bars):
            self.slots.setdefault(b['time'][11:16],[]).append(i)

    def at(self,signal_index,mask=63):
        if not mask or signal_index<39 or signal_index>=len(self.bars):
            return []
        begin=max(0,signal_index-self.lookback+1)
        stop=signal_index+1
        d=Detector.__new__(Detector)
        d.bars=self.bars[begin:stop]
        d.n=stop-begin
        d.o,d.h,d.l,d.c,d.v=(a[begin:stop] for a in (self.o,self.h,self.l,self.c,self.v))
        d.atr=max(float(np.mean(self.tr[signal_index-20:signal_index])),float(self.c[signal_index])*.001)
        # Strict seven-bar pivot confirmation: pivot+3 <= signal_index.
        d.tops=self.tops[(self.tops>=begin+3)&(self.tops<=signal_index-3)]-begin
        d.bottoms=self.bottoms[(self.bottoms>=begin+3)&(self.bottoms<=signal_index-3)]-begin
        comparable=self.slots[self.bars[signal_index]['time'][11:16]]
        left=bisect_left(comparable,begin)
        right=bisect_left(comparable,signal_index)
        previous=comparable[max(left,right-20):right]
        base=float(np.median(self.v[previous])) if previous else 0
        d.volume_ratio=float(self.v[signal_index]/base) if base>0 else 0
        candidates=[]
        if mask&32 and 'horizontal_breakout' in self.enabled:candidates.extend(d.horizontal())
        if mask&1 and self.enabled&{'symmetrical_triangle','falling_wedge','rising_wedge','channel','descending_triangle'}:candidates.extend(d.boundaries())
        if mask&2 and 'cup_handle' in self.enabled:candidates.extend(d.cups())
        if mask&4 and 'head_shoulders' in self.enabled:candidates.extend(d.shoulders())
        if mask&8 and 'inverse_head_shoulders' in self.enabled:candidates.extend(d.shoulders(True))
        if mask&16 and 'flag_pole' in self.enabled:candidates.extend(d.flags())
        best={}
        for m in candidates:
            if m and m['pattern'] in self.enabled and (m['pattern'] not in best or m['score']>best[m['pattern']]['score']):
                best[m['pattern']]=m
        matches=sorted(best.values(),key=lambda m:-m['score'])
        # Preserve window-local overlay indexes for exact reproduction; add absolute indexes.
        for m in matches:
            m['signal_index']=signal_index
            m['history_start_index']=begin+m['start_index']
            m['window_offset']=begin
        return matches

    def events(self,begin=39,end=None):
        """Every qualified observation; no hidden deduplication/trade choice."""
        for index in range(max(39,begin),min(len(self.bars),end if end is not None else len(self.bars))):
            yield index,self.at(index)


def prepare_stock(symbol,timeframes=('1H','4H','1D','1W'),through=None):
    config=load_config()
    through=through or now_ist()
    with connect_source(ROOT/config['database']) as con:
        # One read transaction gives this stock's daily/intraday data a consistent snapshot.
        con.execute('BEGIN')
        calendar=Calendar(config,observed_sessions(con))
        daily=load_full_history(con,symbol,False,through) if set(timeframes)&{'1D','1W'} else []
        intraday=load_full_history(con,symbol,True,through) if set(timeframes)&{'1H','4H'} else []
    frames={}
    for tf in timeframes:
        rows=intraday if tf in ('1H','4H') else daily
        # aggregate uses a bounded tail for the scanner; here retain all source history.
        frames[tf]=aggregate(rows,tf,calendar,through,limit=max(1,len(rows)))
    return frames


def pilot(symbol='TITAN',observations=300):
    """A replay-equivalence pilot, NOT a backtest or probability estimate."""
    from .detectors import detect
    started=time.monotonic()
    config=load_config()
    frames=prepare_stock(symbol)
    report={'symbol':symbol,'detector_version':config['pattern_version'],'purpose':'Historical replay validation only; trading rules pending','timeframes':{}}
    for tf,(bars,quality) in frames.items():
        replay=HistoricalReplay(bars,config['history_bars'],config['enabled_patterns'])
        sample_start=max(39,len(bars)-observations)
        counts={name:0 for name in config['enabled_patterns']}
        began=time.monotonic()
        for i,matches in replay.events(sample_start):
            for m in matches:counts[m['pattern']]+=1
            if (i-sample_start)%37==0 or i==len(bars)-1:
                reference=detect(bars[max(0,i-259):i+1],config['enabled_patterns'])
                plain=[{k:v for k,v in m.items() if k not in ('signal_index','history_start_index','window_offset')} for m in matches]
                assert plain==reference,(symbol,tf,i,'replay differs from scanner')
        duration=time.monotonic()-began
        report['timeframes'][tf]={'bars':len(bars),'first_candle':bars[0]['time'] if bars else None,'last_candle':bars[-1]['end'] if bars else None,
            'quality':quality,'pilot_closes':len(bars)-sample_start,'qualified_observations':counts,
            'seconds':round(duration,3),'seconds_per_close':round(duration/max(1,len(bars)-sample_start),6)}
    report['total_seconds']=round(time.monotonic()-started,3)
    path=ROOT/'output'/f'historical_replay_pilot_{symbol}.json'
    path.write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report,indent=2),flush=True)


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description='Prepare full history and validate a chronological replay pilot. No trades or probabilities.')
    parser.add_argument('--symbol',default='TITAN')
    parser.add_argument('--observations',type=int,default=300)
    args=parser.parse_args()
    pilot(args.symbol,args.observations)
