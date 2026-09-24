"""The match lifecycle: New match → Still matching → Strengthening / Weakening → Condition ended.

A pure walk over one instrument's per-reading results, oldest first. Nothing here reads the clock or the
store, so a replay of the session is exactly what a live run at each reading said.

  matched  True   the scanner's conditions held at this reading
           False  they did not (or the instrument left the strike range)
           None   the reading carried no value for it: NOT OBSERVED. It neither extends nor ends a match
                  (summary.ts RULE 2), and it is drawn as a gap, never as a zero.

  pace     'strengthening' | 'weakening' | None — the lead condition's latest move against the one before it,
           on the block's own bands (PACE_UP 1.25x / PACE_DOWN 0.75x). Only a match that is ALREADY running
           can strengthen or weaken; its first reading is always "New match".

A match that ends and later holds again is a new EPISODE on the same instrument — one card, with every
episode listed — never a second "new" result for the same instrument.
"""
from __future__ import annotations

#: The transitions a notification may be raised for. "Still matching" is never one: nothing changed.
ALERT_KINDS=('new','ended','changed')


def walk(matched,pace):
 """(timeline, episodes, events) for one instrument.

 timeline  one status per reading: 'none' | 'new' | 'still' | 'strengthening' | 'weakening' | 'ended' |
           'gap' (not observed while a match was running) | 'unseen' (not observed, nothing running)
 episodes  [{'start': i, 'end': i|None, 'last': i, 'readings': n}] — `end` is the reading at which the
           condition was first seen NOT to hold; None while the episode is still running.
 events    [(i, kind)] with kind in ALERT_KINDS — the transitions, and only the transitions.
 """
 timeline,episodes,events=[],[],[]
 active=None
 last_pace=None
 for i,m in enumerate(matched):
  if m is None:
   timeline.append('gap' if active else 'unseen');continue
  if m and active is None:
   active={'start':i,'end':None,'last':i,'readings':1};episodes.append(active)
   timeline.append('new');events.append((i,'new'));last_pace=None
  elif m:
   active['last']=i;active['readings']+=1
   word=pace[i] if i<len(pace) else None
   timeline.append(word or 'still')
   if word and word!=last_pace:events.append((i,'changed'));last_pace=word
  elif active is not None:
   active['end']=i;active=None
   timeline.append('ended');events.append((i,'ended'))
  else:
   timeline.append('none')
 return timeline,episodes,events


def current(timeline):
 """The status an instrument's card carries now, and the reading it was set at. A card whose match ended
 keeps saying "Condition ended" until it matches again."""
 for i in range(len(timeline)-1,-1,-1):
  t=timeline[i]
  if t in ('new','still','strengthening','weakening','ended'):return t,i
 return None,None
