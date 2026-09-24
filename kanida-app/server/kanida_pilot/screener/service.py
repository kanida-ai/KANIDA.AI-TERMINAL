"""The screener, assembled: sessions (read-only F&O store), the evaluator, the screener's own store, alerts.

Evaluation is cached per (definition digest, session) and recomputed only when a new 15-minute reading has
landed, so a page refresh never re-runs a session and two users on the same default share one run.
Alerts are raised from the evaluation itself — whoever triggers it (the 15-minute job, or a page read) — and the
store's UNIQUE key guarantees a transition notifies once.
"""
from __future__ import annotations
import logging,os,threading,time
from pathlib import Path
from . import defaults,definition as D,nl,vocab as V
from .data import Sessions,Store
from .evaluate import Evaluator
from .greeks import Greeks
from .store import ScreenerStore

ROOT=Path(__file__).resolve().parents[3]
DEFAULT_DB=str(ROOT/'var'/'screener.db')
log=logging.getLogger('screener')


def screener_database():
 return os.getenv('PILOT_SCREENER_DATABASE') or DEFAULT_DB


class Screener:
 def __init__(self,derivatives_path,screener_path=None):
  self.source=Store(derivatives_path)
  self.sessions=Sessions(self.source)
  self.db=ScreenerStore(screener_path or screener_database())
  defaults.seed(self.db)
  self.greeks=Greeks(self.source,self.db)
  self.evaluator=Evaluator(self.sessions,self.greeks)
  self.lock=threading.RLock()

 # --- status --------------------------------------------------------------------------------------------
 def status(self):
  if not self.source.available():
   return {'available':False,'text':'No F&O store is attached to this server.'}
  s=self.sessions.get()
  if s is None or not s.n:return {'available':False,'text':'No F&O readings are stored yet.'}
  return {'available':True,'session':s.session,'as_of':s.readings[-1],'readings':len(s.readings),
   'first':s.readings[0],'underlyings':len(s.underlyings()),'reading_minutes':V.READING_MINUTES}

 def underlyings(self):
  s=self.sessions.get()
  return s.underlyings() if s else []

 # --- evaluation ----------------------------------------------------------------------------------------
 def results(self,definition,session=None,cache=True):
  d=D.normalize(definition)
  digest=D.digest(d)
  with self.lock:
   s=self.sessions.get(session)
   if s is None or not s.n:return self.evaluator.run(d,session)
   through=s.readings[-1]
   if cache:
    hit=self.db.result(digest,s.session)
    if hit and hit['through']==through:return hit['payload']
   started=time.time()
   payload=self.evaluator.run(d,s.session)
   payload['elapsed_ms']=int((time.time()-started)*1000)
   payload['reads_as']=D.reads_as(d)
   self.db.put_result(digest,s.session,through,payload)
  self.raise_alerts(digest,payload)
  return payload

 def evaluate_all(self):
  """The 15-minute job: every saved scanner's definition, once per distinct digest, then alerts."""
  seen,count=set(),0
  for sc in self.db.all_scanners():
   if sc['digest'] in seen:continue
   seen.add(sc['digest'])
   try:self.results(sc['definition']);count+=1
   except Exception:log.exception('Screener evaluation failed for %s',sc['id'])
  return count

 # --- alerts --------------------------------------------------------------------------------------------
 def raise_alerts(self,digest,payload):
  if not payload.get('available'):return 0
  session=payload['session']
  scanners={sc['id']:sc for sc in self.db.all_scanners() if sc['digest']==digest}
  if not scanners:return 0
  rows=[]
  for sub in self.db.all_subscriptions():
   sc=scanners.get(sub['scanner_id'])
   if not sc:continue
   want={'new':sub['notify_new'],'ended':sub['notify_ended'],'changed':sub['notify_changed']}
   since=sub['alerts_from'] or ''
   for m in payload['matches']:
    for e in m.get('events') or []:
     at=f"{session} {e['at']}:00"
     if not want.get(e['kind']) or at<=since:continue
     rows.append((sub['user_id'],sc['id'],session,at,m['key'],e['kind'],m['title'],self._alert_text(sc,m,e)))
  return self.db.add_alerts(rows)

 @staticmethod
 def _alert_text(sc,m,e):
  if e['kind']=='new':
   why=(m.get('because') or [''])[0]
   return f"New match in {sc['name']} at {e['at']}. {why}".strip()
  if e['kind']=='ended':
   return f"{sc['name']}: the match ended at {e['at']}. {m.get('ended_because') or ''}".strip()
  now=V.LIFECYCLE.get(e.get('status') or '','changed').lower()
  return f"{sc['name']}: the match changed at {e['at']} — now {now}."

 # --- natural language ----------------------------------------------------------------------------------
 def parse(self,text):
  return nl.parse(text,self.underlyings())
