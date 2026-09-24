"""THE AI SUMMARY — deterministic (owner decision, 22 Sep 2026). No language model, no key, no cost.

It reads the SAME stored evidence the connected widgets draw, at the same reading, and says what matters across
it in six short sections. It adds no analytic of its own and no threshold of its own: every sentence is either
the 15-min signal engine's own words (the immutable snapshots in var/intelligence.db, rule signal/2) or a stored
figure from db/derivatives.db, and every sentence names the widget it comes from.

  What changed   this reading against the one before
  Where          which strikes, against ATM, and how wide
  Persistent     what has held across consecutive readings (each side's OI direction, reading by reading)
  Conflicting    the engine's own conflicting evidence and caveat tags
  Key strikes    the engine's key strikes
  Unusual        the store's own unusual-activity flags at this reading

SCOPE. By default it reads only the sources whose widgets are on the workspace for this instrument ("connected");
the 15-min signal engine is always read, because it is what the summary is built on. Nothing here is a forecast
and nothing here is a recommendation: it never says buy, sell, bullish or bearish.
"""
from __future__ import annotations
import json,sqlite3
from pathlib import Path

WIDGET_SOURCE={'pcr':'pcr','max_pain':'max_pain','iv':'iv','greeks':'iv','option_chain':'unusual','volume':'unusual',
 'oi_by_strike':'unusual','market_scan':'unusual'}
SOURCE_LABEL={'signal':'15-min signal','oi':'OI','pcr':'PCR','max_pain':'Max pain','iv':'IV','unusual':'Option chain',
 'screener':'Screener'}
SIDE={'CE':'call','PE':'put'}
STATE_WORD={'building':'rose','reducing':'fell','quiet':'barely moved'}
CAVEAT=('Describes what was captured at the 15-min readings — not a forecast and not a recommendation.')


def ro(path):
 return sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=10)


def hhmm(at):return str(at or '')[11:16]


def units(v):
 if v is None:return '—'
 a=abs(v);sign='+' if v>0 else '−' if v<0 else ''
 def fit(x):
  t=f'{x:.0f}' if x>=100 else f'{x:.1f}'
  return t[:-2] if t.endswith('.0') else t
 if a>=1e7:return f'{sign}{fit(a/1e7)}Cr'
 if a>=1e5:return f'{sign}{fit(a/1e5)}L'
 if a>=1e3:return f'{sign}{fit(a/1e3)}K'
 return f'{sign}{round(a)}'


def strike(v):return '—' if v is None else (f'{v:,.0f}' if float(v).is_integer() else f'{v:,.1f}')


def session_snapshots(intelligence_path,underlying,session=None):
 """The newest engine's snapshots of one underlying's session, oldest first: [(reading_at, expiry, object)]."""
 if not Path(intelligence_path).is_file():return []
 c=ro(intelligence_path)
 try:
  head=c.execute("select session,engine_version from reading_snapshots where underlying=? and status='ok'"
   +(" and session=?" if session else '')+" order by reading_at desc, id desc limit 1",
   (underlying,session) if session else (underlying,)).fetchone()
  if not head:return []
  rows=c.execute("select reading_at,expiry,chained,reading from reading_snapshots where underlying=? and session=?"
   " and engine_version=? and status='ok' order by reading_at",(underlying,head[0],head[1])).fetchall()
 finally:c.close()
 out=[]
 for at,expiry,chained,reading in rows:
  try:obj=json.loads(chained or reading or '{}')
  except ValueError:continue
  out.append((at,expiry,obj))
 return out


def unusual_contracts(derivatives_path,underlying,at,expiry=None,top=3):
 if not Path(derivatives_path).is_file():return []
 c=ro(derivatives_path)
 try:
  sql=("select instrument_type,strike,vol_tod_ratio,unusual_reasons from metrics where scope='contract' and captured_at=?"
   " and underlying=? and unusual=1 and instrument_type in ('CE','PE')")
  args=[at,underlying]
  if expiry:sql+=' and expiry=?';args.append(expiry)
  rows=c.execute(sql+' order by coalesce(vol_tod_ratio,0) desc',args).fetchall()
 finally:c.close()
 return rows


def _run(snaps,key):
 """How many consecutive readings, ending at the newest, a side's OI moved the same way — and since when."""
 last=snaps[-1][2].get(key)
 if last not in ('building','reducing'):return 0,None,last
 n=0;since=None
 for at,_e,obj in reversed(snaps):
  if obj.get(key)!=last:break
  n+=1;since=obj.get('previous_timestamp') or at
 return n,since,last


def latest_snapshot(intelligence_path,underlying):
 snaps=session_snapshots(intelligence_path,underlying)
 return snaps[-1][2] if snaps else None


def build(intelligence_path,derivatives_path,underlying,sources=(),scope='connected',focus=None,session=None,scanner=None):
 """`scanner` is alignment.context(...) — the user's scanner, its match and the engine's agreement — when the
 workspace was pointed here from a scanner match. It leads the summary, so the summary is about THAT match."""
 snaps=session_snapshots(intelligence_path,underlying,session)
 if not snaps:
  return {'available':False,'underlying':underlying,
   'text':f'No 15-min reading of {underlying} has been captured yet — the summary appears after the first one.'}
 at,expiry,cur=snaps[-1]
 prev=snaps[-2][2] if len(snaps)>1 else None
 allowed={'signal','oi','screener'}
 if scope=='all':allowed|={'pcr','max_pain','iv','unusual'}
 else:allowed|={WIDGET_SOURCE[s] for s in sources if s in WIDGET_SOURCE}
 read=set()
 sections={k:[] for k in ('scanner','context','changed','where','persistent','conflicting','key_strikes','unusual')}
 def add(section,text,source):
  if source in allowed and text:
   sections[section].append({'text':text,'source':SOURCE_LABEL[source]});read.add(source)

 # --- the user's scanner, its match, and whether the 15-min signal agrees -----------------------------------
 if scanner:
  add('scanner',f"{scanner['scanner']}{' (your scanner)' if scanner.get('mine') else ''}: {scanner['reads_as']}",'screener')
  add('scanner',scanner.get('status_text'),'screener')
  for b in scanner.get('because') or []:add('scanner',b,'screener')
  for a in scanner.get('agreement') or []:add('scanner',a['text'],'screener')
 # --- or just where the selection came from, when it was not a scanner match --------------------------------
 elif focus and focus.get('source'):
  when=f" — first matched {focus['from']}" if focus.get('from') else ''
  add('context',f"Opened from the scanner “{focus['source']}”{when}.",'screener')
  for b in (focus.get('because') or [])[:1]:add('context',b,'screener')

 # --- what changed --------------------------------------------------------------------------------------------
 headline=cur.get('plain_language_headline') or cur.get('plain_language_read')
 change=cur.get('state_change')
 if headline:add('changed',f"{change+': ' if change and change.lower() not in headline.lower() else ''}{headline} "
  f"(the {hhmm(at)} reading).",'signal')
 co,po=cur.get('call_oi_change'),cur.get('put_oi_change')
 if co is not None or po is not None:
  add('changed',f"Over the last 15 minutes call OI moved {units(co)} and put OI {units(po)}.",'oi')
 pc,pp=cur.get('pcr_current'),cur.get('pcr_previous')
 if pc is not None and pp is not None and round(pc,2)!=round(pp,2):
  add('changed',f"PCR {pp:.2f} → {pc:.2f}.",'pcr')
 mc,mp=cur.get('max_pain_current'),cur.get('max_pain_previous')
 if mc is not None and mp is not None and mc!=mp:
  add('changed',f"Max pain moved from {strike(mp)} to {strike(mc)}.",'max_pain')
 ivc=cur.get('iv_change')
 # the engine's own reading of IV wins: when it says IV was unchanged, a +0.1 pt figure is not "a change"
 iv_flat=any('IV was unchanged' in (e.get('text') if isinstance(e,dict) else str(e))
  for e in cur.get('conflicting_evidence') or [])
 if ivc not in (None,0,0.0) and not iv_flat:
  add('changed',f"ATM IV {'+' if ivc>0 else '−'}{abs(ivc):.1f} pt against the reading before (COMPUTED).",'iv')
 elif cur.get('iv_reason'):add('changed',f"ATM IV: {cur['iv_reason']}",'iv')

 # --- where ----------------------------------------------------------------------------------------------------
 rng=cur.get('strike_range') or []
 side=SIDE.get(cur.get('side') or '')
 if rng:
  span=strike(rng[0]) if len(rng)==1 else f"{strike(min(rng))}–{strike(max(rng))}"
  lead=f"; {strike(cur['leading_strike'])} {cur.get('side')} carries the most" if cur.get('leading_strike') else ''
  where=cur.get('location') or ''
  breadth=cur.get('breadth')
  width={'isolated':'at one strike','clustered':'across neighbouring strikes','dispersed':'across scattered strikes'}.get(breadth,'')
  add('where',f"{(side or 'The').capitalize()} activity {width+' ' if width else ''}{span}{' '+where if where else ''}{lead}.",'signal')
 bd=cur.get('breadth_direction')
 if bd in ('wider','narrower') and cur.get('breadth_previous') is not None:
  add('where',f"The set of active strikes got {bd}: {cur['breadth_previous']} → {cur.get('breadth_current')}.",'signal')

 # --- persistent -----------------------------------------------------------------------------------------------
 for key,name in (('call_state','Call'),('put_state','Put')):
  n,since,state=_run(snaps,key)
  if n>=2:add('persistent',f"{name} OI {STATE_WORD[state]} at each of the last {n} readings (since {hhmm(since)}).",'signal')
 if cur.get('persistence_known') and cur.get('persistence_since'):
  add('persistent',f"The engine has held “{cur.get('regime') or headline}” since {hhmm(cur['persistence_since'])}"
   f"{' ('+str(cur['persistence_minutes'])+' min)' if cur.get('persistence_minutes') else ''}.",'signal')
 if prev and pc is not None:
  pcs=[o.get('pcr_current') for _a,_e,o in snaps if o.get('pcr_current') is not None]
  if len(pcs)>=4 and (all(b<a for a,b in zip(pcs[-4:],pcs[-3:])) or all(b>a for a,b in zip(pcs[-4:],pcs[-3:]))):
   add('persistent',f"PCR has {'fallen' if pcs[-1]<pcs[-4] else 'risen'} at each of the last 3 readings.",'pcr')
 if not sections['persistent']:add('persistent','Nothing has held in the same direction for more than one reading.','signal')

 # --- conflicting ----------------------------------------------------------------------------------------------
 for e in cur.get('conflicting_evidence') or []:
  text=e.get('text') if isinstance(e,dict) else str(e)
  add('conflicting',text,'iv' if 'IV' in (text or '') else 'signal')
 for t in cur.get('tags') or []:
  if 'not confirming' in t.lower() or 'mixed' in t.lower():add('conflicting',f"Caveat: {t}.",'iv' if 'IV' in t else 'signal')
 if (cur.get('call_state')=='building' and cur.get('put_state')=='building'):
  add('conflicting','Both sides added open interest at this reading — two behaviours, not one direction.','signal')

 # --- key strikes ----------------------------------------------------------------------------------------------
 for k in (cur.get('key_strikes') or [])[:4]:
  add('key_strikes',f"{strike(k.get('strike'))} {k.get('side')} — {k.get('note') or ''}".rstrip(' —'),'signal')

 # --- unusual --------------------------------------------------------------------------------------------------
 if 'unusual' in allowed:
  rows=unusual_contracts(derivatives_path,underlying,at,expiry)
  if rows:
   ce=sum(1 for r in rows if r[0]=='CE');pe=len(rows)-ce
   add('unusual',f"{len(rows)} contract{'s' if len(rows)!=1 else ''} flagged unusually active at this reading "
    f"({ce} call, {pe} put).",'unusual')
   for kind,s,vtr,reasons in rows[:3]:
    add('unusual',f"{strike(s)} {kind}: {(reasons or '').split(',')[0] or (f'volume {vtr:.1f}× its time-of-day median' if vtr else 'unusual')}.",'unusual')
  else:add('unusual','No contract was flagged unusually active at this reading.','unusual')

 missing=sorted({'pcr','max_pain','iv','unusual'}-allowed)
 return {'available':True,'underlying':underlying,'expiry':expiry,'as_of':at,'session':at[:10],'scanner':scanner,
  'engine_version':cur.get('engine_version'),'sections':sections,
  'read_from':[SOURCE_LABEL[s] for s in ('signal','oi','pcr','max_pain','iv','unusual','screener') if s in read],
  'not_read':[SOURCE_LABEL[s] for s in missing],'caveat':CAVEAT,'readings':len(snaps)}
