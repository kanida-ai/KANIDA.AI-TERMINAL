from __future__ import annotations
import json,hashlib
from sqlalchemy import select
from .db import plans,watches,audit,record,ident,now,row
from .errors import PilotError
def body_hash(data):return hashlib.sha256(json.dumps(data,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def valid_request(data):
 key=data.get('request_id')
 if not isinstance(key,str) or not 8<=len(key)<=100:raise PilotError(400,'REQUEST_ID','A valid request identifier is required.')
 return key
def legacy(plan):
 """Plans saved before the exact-rule gate: annotate on read, never rewrite the stored payload.
 Their evidence_applies came from the scanner's 'supported' status, which already required the
 later-test sample minimum, so it is the honest equivalent of tradable_evidence.
 A non-tradable legacy draft is presented as 'illustrative' (its stored row is untouched): it stays
 readable, pausable and archivable, but simulation and live submission refuse it (EXIT_EVIDENCE) until it
 is prepared again with passing evidence. Tradable legacy plans are still re-verified at submission."""
 if 'tradable_evidence' in plan:return plan
 tradable=bool(plan.get('exit_mode')=='suggested' and plan.get('evidence_applies') is True)
 value=dict(plan,tradable_evidence=tradable,evidence_check='legacy',
  tradable_reason='Saved before exact-rule evidence checks. '+('Its rule was the frozen tested rule.' if tradable else 'Its exits have no exact-rule evidence; prepare it again before relying on it.'))
 if not tradable and value.get('status')=='draft':
  value.update(status='illustrative',illustrative=True,blockers=list(value.get('blockers') or [])+['Illustrative plan: no exact-rule evidence, so it cannot be simulated or executed'])
 return value
def resumable(payload):
 """Resume may only restore 'draft' when the plan's own evidence is tradable (same normalisation as reads)."""
 return legacy(dict(payload)).get('tradable_evidence') is True and payload.get('illustrative') is not True

class Product:
 def __init__(self,db,evidence):self.db=db;self.evidence=evidence
 def state(self,user):
  with self.db.tx() as c:
   saved=[legacy(dict(r['payload'],status=r['status'])) for r in c.execute(select(plans).where(plans.c.user_id==user['id']).order_by(plans.c.created.desc())).mappings()]
   watch=[r['payload'] for r in c.execute(select(watches).where(watches.c.user_id==user['id']).order_by(watches.c.created.desc())).mappings()]
   events=[dict(r) for r in c.execute(select(audit).where(audit.c.user_id==user['id']).order_by(audit.c.created.desc()).limit(100)).mappings()]
  return dict(plans=saved,watchlist=watch,events=events,mode='private_pilot',execution_connected=False)
 def save(self,user,data):
  request=valid_request(data);hashed=body_hash(data)
  with self.db.tx() as c:
   old=row(c,select(plans).where(plans.c.user_id==user['id'],plans.c.request_id==request))
   if old:
    if old['payload_hash']!=hashed:raise PilotError(409,'REQUEST_REUSED','This request identifier was used for different plan settings.')
    return old['payload']
  context=None
  if data.get('study_context'):
   source=data['study_context']
   if not isinstance(source,dict) or not all(isinstance(source.get(k),str) and 1<=len(source[k])<=80 for k in ('id','trade_id')):
    raise PilotError(400,'STUDY_CONTEXT','Choose a saved historical trade.')
   study=self.evidence.study('/api/studies/job',{'owner':user['id'],'id':source['id'],'result':'true'})
   if study.get('status')!='complete':raise PilotError(409,'STUDY_CONTEXT','The historical study is not complete.')
   result=study.get('result',{});trade=next((t for t in result.get('trades',[]) if t['id']==source['trade_id']),None)
   match=self.evidence.match(data.get('match_id'))
   if not trade or any(trade[k]!=match[k] for k in ('symbol','pattern','timeframe')) or trade['side']!=data.get('side'):
    raise PilotError(409,'STUDY_CONTEXT','Choose a study trade matching this stock, pattern, timeframe and direction.')
   context=dict(id=source['id'],trade_id=trade['id'],run=result.get('run'),engine_version=result.get('engine_version'),
                validation=result.get('validation'),settings=study['settings'],rule=trade.get('rule'),
                note='Historical research context. This plan uses its own explicitly reviewed exit rules.')
  value=self.evidence.prepare(data);value['id']=ident();value['request_id']=request
  if context:value['research_context']=context
  # Exact-rule evidence gate. The suggestion inside the plan is re-fetched by the server, so a
  # client cannot forge it. A plan without it is refused unless the caller explicitly saves it as
  # illustrative; illustrative plans get status 'illustrative', which simulation and live
  # submission both refuse (they require 'draft'), and resume never promotes them to 'draft'.
  suggested=value.get('exit_mode')=='suggested';source=value.get('exit_evidence') or {}
  tradable=bool(suggested and source.get('tradable_evidence') is True)
  reason=source.get('tradable_reason') if suggested else 'Custom exits are untested: no historical replay of this exact rule exists.'
  reason=reason or 'No historical replay of this exact exit rule meets the sample requirement.'
  if not tradable and data.get('illustrative') is not True:raise PilotError(409,'EXIT_EVIDENCE',reason)
  value.update(tradable_evidence=tradable,tradable_reason=reason,illustrative=not tradable)
  if not tradable:
   value['status']='illustrative';value['blockers']=list(value.get('blockers') or [])+['Illustrative plan: no exact-rule evidence, so it cannot be simulated or executed']
  with self.db.tx() as c:
   old=row(c,select(plans).where(plans.c.user_id==user['id'],plans.c.request_id==request))
   if old:
    if old['payload_hash']!=hashed:raise PilotError(409,'REQUEST_REUSED','This request identifier was used for different plan settings.')
    return old['payload']
   c.execute(plans.insert().values(id=value['id'],user_id=user['id'],request_id=request,payload_hash=hashed,payload=value,status='draft' if tradable else 'illustrative',created=now(),updated=now()))
   record(c,user['id'],'plan','Plan saved' if tradable else 'Illustrative plan saved',f"{value['symbol']} · {value['pattern_name']} · {value['timeframe']}. No order submitted."+('' if tradable else ' Cannot be simulated.'),value['id'])
  return value
 def watch(self,user,data):
  identity=data.get('match_id');action=data.get('action')
  if not isinstance(identity,str) or len(identity)>150 or action not in ('add','remove'):raise PilotError(400,'WATCH_ACTION','Invalid watchlist action.')
  # Watching is a display/tracking action, not a trade, so it resolves against the DISPLAY set and works
  # on either pattern set. The trade gates keep resolving against `Evidence.match`, which is legacy-only.
  match=self.evidence.display_match(identity) if action=='add' else None
  with self.db.tx() as c:
   condition=(watches.c.user_id==user['id'])&(watches.c.id==identity)
   old=row(c,select(watches).where(condition))
   if action=='add' and not old:
    value={k:match[k] for k in ('id','symbol','pattern','pattern_name','timeframe','direction','candle_end')}
    c.execute(watches.insert().values(user_id=user['id'],id=identity,payload=value,created=now()))
    record(c,user['id'],'watch','Setup saved to watch',match['symbol']+' · '+match['timeframe'],identity)
   elif action=='remove' and old:c.execute(watches.delete().where(condition))
  return {'ok':True}
 def change(self,user,data):
  action=data.get('action')
  if action not in ('pause','resume','archive'):raise PilotError(400,'PLAN_ACTION','Invalid plan action.')
  with self.db.tx() as c:
   condition=(plans.c.id==data.get('id'))&(plans.c.user_id==user['id'])
   value=row(c,select(plans).where(condition).with_for_update())
   if not value:raise PilotError(404,'PLAN_NOT_FOUND','Plan not found.')
   if value['status']=='archived':raise PilotError(409,'PLAN_ARCHIVED','This plan is archived.')
   status={'pause':'paused','resume':'draft' if resumable(value['payload']) else 'illustrative','archive':'archived'}[action]
   c.execute(plans.update().where(condition).values(status=status,updated=now()))
   record(c,user['id'],'plan','Plan '+status,value['payload']['symbol']+' · existing orders are managed separately.',value['id'])
  return legacy(dict(value['payload'],status=status))
