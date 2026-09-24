"""THE SCANNER AND THE SIGNAL, SIDE BY SIDE (owner, 22 Sep 2026): "the AI summary and signal summary should be in
line with the screener the user selected, and specific to his profile."

When the workspace was pointed at an instrument FROM a scanner match, both the AI summary and the 15-min signal
widget open with this block, built once here so the two can never disagree:

  · which scanner — checked against the caller's OWN scanners and the KANIDA defaults. A scanner id that is not
    theirs is ignored, never read;
  · the match's live lifecycle from the screener itself (still matching since 10:15, strengthening, ended at
    11:00 and why) — the same evaluation the Screener results widget shows;
  · the screener's own "Matched because" lines;
  · condition by condition, whether the 15-min signal engine's latest reading points the same way. The two read
    different windows (a scanner may read 45 minutes; the engine reads the last 15), so a difference is stated as
    exactly that, not as an error in either.

Only conditions the engine has a counterpart for are compared (OI and build-up by side, PCR, max pain, IV); the
rest are listed as not compared rather than guessed at.
"""
from __future__ import annotations
from ..screener import vocab as V

SIDE_KEY={'CE':'call_state','PE':'put_state'}
SIDE_WORD={'CE':'call','PE':'put'}
ENGINE_WORD={'building':'rose','reducing':'fell','quiet':'barely moved','opening':'was the day\'s first reading',
 'not_measured':'was not measured'}


def _want(state):
 if state.startswith('up') or state in ('rev_up','writing','buying'):return 'up'
 if state.startswith('down') or state in ('rev_down','short_covering','buyers_exiting'):return 'down'
 if state=='stable':return 'flat'
 return None


def _compare(c,match,cur,when):
 metric,state=c['metric'],c['state']
 cond=V.METRICS[metric]['label']+' · '+V.state_word(metric,state)
 want=_want(state)
 if metric in ('oi','flow'):
  side=c.get('side') if c.get('side') in ('CE','PE') else match.get('type')
  if side not in SIDE_KEY:return None
  if metric=='flow':want='up' if state in ('writing','buying') else 'down'
  got=cur.get(SIDE_KEY[side])
  seen={'building':'up','reducing':'down','quiet':'flat'}.get(got)
  name=f"{SIDE_WORD[side]} OI"
  if seen is None:return {'condition':cond,'agrees':None,'text':f"{cond}: the 15-min signal has no {name} direction at {when}."}
  ok=seen==want
  return {'condition':cond,'agrees':ok,'text':(f"{cond}: the 15-min signal {'agrees' if ok else 'differs'} — {name} "
   f"{ENGINE_WORD.get(got,got)} over the 15 minutes to {when}"
   +('' if ok else ", while the scanner reads its own window")+'.')}
 if metric in ('pcr','maxpain','iv'):
  key={'pcr':'pcr_change','maxpain':'max_pain_change','iv':'iv_change'}[metric]
  v=cur.get(key)
  if v is None:return {'condition':cond,'agrees':None,'text':f"{cond}: not measured by the 15-min signal at {when}."}
  seen='up' if v>0 else 'down' if v<0 else 'flat'
  if metric=='iv' and any('IV was unchanged' in (e.get('text') if isinstance(e,dict) else str(e))
    for e in cur.get('conflicting_evidence') or []):seen='flat'
  ok=seen==want
  noun={'pcr':'PCR','maxpain':'max pain','iv':'ATM IV'}[metric]
  moved={'up':'rose','down':'fell','flat':'was unchanged'}[seen]
  return {'condition':cond,'agrees':ok,'text':f"{cond}: the 15-min signal {'agrees' if ok else 'differs'} — {noun} {moved} "
   f"over the 15 minutes to {when}."}
 return None


def context(screener,user,focus,underlying,snapshot=None):
 """The scanner block for one selection, or None when the selection did not come from a scanner the user can see."""
 if not screener or not focus or not focus.get('scanner_id'):return None
 sc=screener.db.scanner(focus['scanner_id'])
 if not sc or not (sc['is_default'] or sc['owner_id']==user['id']):return None
 payload=screener.results(sc['definition'])
 matches=payload.get('matches') or []
 key=focus.get('match_key')
 match=next((m for m in matches if m['key']==key),None) or next((m for m in matches if m.get('underlying')==underlying),None)
 from ..screener.definition import reads_as
 out={'scanner_id':sc['id'],'scanner':sc['name'],'mine':not sc['is_default'],'reads_as':reads_as(sc['definition']),
  'as_of':payload.get('as_of'),'match':None,'status':None,'status_text':'','because':[],'agreement':[],'not_compared':[]}
 if not match:
  out['status_text']=f"{underlying} is not in this scanner's matches today."
  return out
 out['match']={k:match.get(k) for k in ('key','title','underlying','expiry','strike','type','status','first_matched',
  'episode_started','last_matched','ended_at','episode_readings','readings_matched')}
 out['status']=match['status']
 if match['active']:
  word={'new':'New match','still':'Still matching','strengthening':'Strengthening','weakening':'Weakening'}[match['status']]
  out['status_text']=(f"{word} — matching since {match['episode_started']} "
   f"({match['episode_readings']} reading{'s' if match['episode_readings']!=1 else ''}).")
 else:
  out['status_text']=f"Condition ended at {match['ended_at']}. {match.get('ended_because') or ''}".strip()
 out['because']=list(match.get('because') or [])
 if snapshot:
  when=str(snapshot.get('timestamp') or snapshot.get('current_timestamp') or '')[11:16] or 'the latest reading'
  for c in sc['definition']['conditions']:
   got=_compare(c,match,snapshot,when)
   if got:out['agreement'].append(got)
   else:out['not_compared'].append(V.METRICS[c['metric']]['label']+' · '+V.state_word(c['metric'],c['state']))
 return out
