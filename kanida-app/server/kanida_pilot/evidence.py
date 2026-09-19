from __future__ import annotations
import hashlib,json,re,time,sys
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
from urllib.parse import urlencode
from .errors import PilotError
from .lastgood import LastGood
from .product import legacy

# Reuse the approved research rule and sizing code, without opening its databases.
SOURCE=Path(__file__).resolve().parents[3]
if str(SOURCE) not in sys.path:sys.path.insert(0,str(SOURCE))
from market_scanner.exit_plan import describe
from market_scanner.product import build_plan
from market_scanner.capital import study_account

PATHS={'/api/state','/api/filter-options','/api/matches','/api/stocks','/api/stock','/api/chart','/api/coverage',
 '/api/backtests/state','/api/backtests','/api/backtests/cell','/api/backtests/chart','/api/backtests/capital'}
def cache_key(path,params):return hashlib.sha256((path+'?'+urlencode(sorted(params.items()))).encode()).hexdigest()

# --- last-good display cache (BACKLOG item 2a) ---------------------------------------------------------------
#: The only paths whose last successful body may be replayed while the scanner is unreachable. This is the
#: RENDER set: what the workspace, the chart, the stock search and the Discover page need to draw the last scan.
#: `/api/backtests/capital` is deliberately NOT here - it is position sizing, and a size computed on a cached
#: snapshot must never be shown as if it were current. Serving a cached copy still requires the caller to pass
#: `cache=True`; every trade-deciding call uses the default (`cache=False`) and therefore still fails closed.
CACHEABLE={'/api/state','/api/filter-options','/api/matches','/api/stocks','/api/stock','/api/chart','/api/coverage',
 '/api/backtests/state','/api/backtests','/api/backtests/cell','/api/backtests/chart'}
#: How long the app keeps calling the outage "reconnecting" before it is reported as a lasting failure.
#: Mirrored by RECONNECT_GRACE_SECONDS in src/layout/dataStatus.ts.
RECONNECT_GRACE_SECONDS=300
#: A served-from-cache answer is held this long before the upstream is tried again. Without it a scanner that
#: times out (rather than refusing the connection) would make every single request wait the full HTTP timeout.
FAILOVER_SECONDS=10
LIVE_SECONDS=30
#: What the app is allowed to say about the outage: plain words, no stack traces, no host names.
STARTING_TEXT='The scanner is still starting up.'
UNREACHABLE_TEXT='The scanner is not answering.'
def upstream_message(error):
 """One short, non-technical sentence for the app. Never the exception text."""
 response=getattr(error,'response',None)
 if response is not None:
  try:body=response.json()
  except Exception:body={}
  if isinstance(body,dict) and (body.get('starting') is True or body.get('loading') is True):return STARTING_TEXT
  if getattr(response,'status_code',0)==503:return STARTING_TEXT
 return UNREACHABLE_TEXT
def stamp_ist(epoch):
 try:return datetime.fromtimestamp(float(epoch),IST).strftime('%Y-%m-%d %H:%M:%S')
 except (TypeError,ValueError,OSError):return None

# Headline Discover stats come from the cell's `reference`: a whole-history fixed-hold time exit.
# That is never the stop/target/hold rule the exit plan sizes, so it must be labelled as such.
HISTORY_LABEL='Pattern hold-period history, not this exit rule'
PATTERN_DIRECTION={'long':'bullish','short':'bearish'}
def tradability(plan,direction=None):
 """Is there evidence for the EXACT exit rule that would be traded?

 No point-in-time engine can replay the structural 1:2 benchmark (its stop comes from the
 current geometry, not an ATR multiple), so only the frozen selected rule can qualify: it must be
 the rule actually used, pass the scanner's later-test support checks, and meet the sample minimum.
 `direction` is the scanner match's pattern direction. The trade side must be that direction and the
 scanner must rate the setup 'review'. Missing fields fail closed.
 """
 ev=plan.get('evidence') or {};metrics=plan.get('rule_metrics') or {};selected=ev.get('selected_rule') or {};rule=plan.get('rule') or {}
 minimum=int(ev.get('minimum_later_trades') or 20);n=int(metrics.get('n') or 0);mean=metrics.get('expectancy_pct')
 # A8: `same` compares plan['rule'] with the selected rule inside the SAME describe() output. describe() only
 # produces kind 'atr' by copying that selected rule, so whenever evidence_applies is True this holds by
 # construction; it guards the invariant, not the user's plan. The meaningful comparison, the SAVED plan's
 # rule against a fresh server re-fetch, is done in require_current_evidence() at simulate/live submission.
 same=rule.get('kind')=='atr' and all(rule.get(k)==selected.get(k) for k in ('trigger','hold','stop_atr','target_r'))
 if plan.get('status')=='failed':return False,'This exit rule lost money on later data after costs.'
 if plan.get('evidence_applies') is not True or not same:
  return False,'Illustrative benchmark only: no historical replay of this exact stop, target and hold exists.'
 if n<minimum:return False,f'Only {n} later-test trades for this exact rule; {minimum} are required.'
 if mean is None or mean<=0:return False,'This exact rule has no positive later-test average after costs.'
 if not plan.get('usable'):return False,'The tested stop is too wide or the target is invalid for this planner.'
 # A6: the research tests a neutral pattern as separate long and HYPOTHETICAL short studies
 # (market_scanner/backtest.py episodes); neither is a directional signal, so neither is tradable.
 if direction=='neutral':
  return False,'Neutral pattern: its breakout direction is unresolved, so a long or hypothetical short on it is research only, not tradable evidence.'
 if direction is None or PATTERN_DIRECTION.get(plan.get('side'))!=direction:
  return False,'The trade direction does not match a known pattern direction, so this rule is not tradable evidence.'
 if plan.get('screen')!='review':
  return False,'The scanner does not rate this setup For review, so its exit rule is not tradable evidence.'
 return True,f'Exact tested rule: {n} later-test trades after assumed costs (minimum {minimum}).'

IST=timezone(timedelta(hours=5,minutes=30))
RESEARCH_SET='research'
#: How many research detections the DISPLAY surfaces hold. One pass produces tens of thousands; the app's
#: workspace list, legend and watchlist are built for hundreds, and the scanner orders by fit score, so this
#: is the best-fitting slice of the live book. `/api/state` carries the real totals beside it.
RESEARCH_DISPLAY_LIMIT=500
def asks_for_legacy_history_screen(params):
 """True when a `/api/matches` query asks to screen on the LEGACY backtest history.

 Mirrors market_scanner.performance.history_screen_requested: the scanner's own DEFAULT_MIN_TRADES is not a
 request, and `min_trades=0` is the explicit "any sample size", i.e. the absence of a screen. A value that is
 not a whole number counts as asked, so the caller hears about it here rather than as an upstream outage.
 """
 if params.get('performance') or params.get('return_band'):return True
 raw=str(params.get('min_trades') if params.get('min_trades') is not None else '').strip()
 if not raw:return False
 try:return int(raw)>0
 except ValueError:return True

def display_match(m):
  """One research detection in the shape the legacy display surfaces read.

  Nothing is invented. `state` becomes the detector's OWN event (`setup`/`confirmed`), which is the same
  vocabulary the legacy matches use, and the research lifecycle is kept beside it under `lifecycle` rather
  than overwritten. `history` stays empty: a research detection has no legacy backtest cell and borrowing
  one is precisely the fallback the evidence contract forbids. The research identity rides along so the
  chart can draw THIS episode and the evidence card can be gated on it."""
  return dict(m,state=m.get('detector_state') or m.get('state'),lifecycle=m.get('state'),history=[],
   universes=list(m.get('universes') or []),pattern_set=RESEARCH_SET)
DEFAULT_MAX_DATA_AGE_DAYS=3
RULE_FIELDS=('kind','trigger','hold','stop_atr','target_r')
def market_today():return datetime.now(IST).date()
def max_data_age(settings):
 value=getattr(settings,'max_data_age_days',DEFAULT_MAX_DATA_AGE_DAYS)
 return DEFAULT_MAX_DATA_AGE_DAYS if value is None else int(value)
def rule_key(rule):return tuple((rule or {}).get(k) for k in RULE_FIELDS)
def rule_text(rule):
 r=rule or {}
 return f"{r.get('trigger','?')} trigger, {r.get('stop_atr','?')} ATR stop, {r.get('target_r','?')}R target, {r.get('hold','?')}-candle hold"
def data_age(state,today=None):
 found=re.match(r'^(\d{4})-(\d{2})-(\d{2})',str((state or {}).get('source_latest') or ''))
 if not found:return None,None
 try:end=date(int(found[1]),int(found[2]),int(found[3]))
 except ValueError:return None,None
 return end,max(0,((today or market_today())-end).days)
def day_text(value):return value.strftime('%d %b %Y').lstrip('0')

def require_current_evidence(evidence,payload,max_age_days):
 """Refuse a simulated or live submission unless the SAVED plan still has current exact-rule evidence.

 1. EXIT_EVIDENCE: the saved plan itself must be tradable, using the same legacy normalisation as reads.
 2. DATA_STALE: stored market data must be no older than max_age_days (PILOT_MAX_DATA_AGE_DAYS) and not
    flagged stale by the scanner. This applies to every simulation, including chosen synthetic scenarios:
    the scenario prices are artificial, but the plan's size, stop and entry come from the stored snapshot,
    so a plan built on stale evidence must not enter even the order workflow as if it were current.
 3. EXIT_EVIDENCE_CHANGED: a fresh server re-fetch (same path as save) must still be tradable, on the same
    candle snapshot, with the same rule the plan saved. A new research run is accepted only if that exact
    rule still passes. The saved payload is never trusted for the evidence verdict itself.
 """
 payload=payload or {};plan=legacy(payload)
 if plan.get('tradable_evidence') is not True or payload.get('illustrative') is True or payload.get('exit_mode')!='suggested':
  reason=str(plan.get('tradable_reason') or 'This plan has no exact-rule evidence.')
  raise PilotError(409,'EXIT_EVIDENCE','This plan cannot be simulated or sent live without exact-rule evidence. '+reason+' Prepare the plan again with passing evidence.')
 state=evidence.get('/api/state',{});end,age=data_age(state)
 if end is None:
  raise PilotError(409,'DATA_STALE','The stored market data date is unknown, so this plan cannot be simulated or sent live. Refresh market data, then prepare the plan again.')
 if age>max_age_days:
  raise PilotError(409,'DATA_STALE',f"Stored market data ends {day_text(end)}, {age} {'day' if age==1 else 'days'} ago; plans need data no older than {max_age_days} {'day' if max_age_days==1 else 'days'}. This plan cannot be simulated or sent live until market data is refreshed and the plan is prepared again.")
 if state.get('source_stale') is True:
  raise PilotError(409,'DATA_STALE',f"Stored market data ends {day_text(end)} and is behind the latest completed trading session. This plan cannot be simulated or sent live until market data is refreshed.")
 saved=payload.get('exit_evidence') or {};saved_rule=payload.get('exit_rule') or saved.get('rule')
 match_id=payload.get('match_id');side=payload.get('side')
 if not isinstance(match_id,str) or side not in PATTERN_DIRECTION or not saved_rule or not saved.get('snapshot'):
  raise PilotError(409,'EXIT_EVIDENCE_CHANGED','This plan does not record the exact rule and candle it was built on, so its evidence cannot be re-verified. Prepare the plan again.')
 try:current=evidence.exit_plan(match_id,side)
 except PilotError as error:
  if error.code in ('SETUP_CHANGED','STALE_EVIDENCE','DIRECTION'):
   raise PilotError(409,'EXIT_EVIDENCE_CHANGED','The research behind this plan changed since it was saved. '+error.message) from None
  raise
 except (ValueError,KeyError,TypeError):
  raise PilotError(409,'EXIT_EVIDENCE_CHANGED','The chart snapshot behind this plan changed. Reopen the setup and prepare the plan again.') from None
 if current.get('tradable_evidence') is not True:
  raise PilotError(409,'EXIT_EVIDENCE_CHANGED','The exact-rule evidence for this plan no longer passes: '+str(current.get('tradable_reason') or 'reason unavailable.')+' Prepare the plan again.')
 if current.get('snapshot')!=saved.get('snapshot'):
  raise PilotError(409,'EXIT_EVIDENCE_CHANGED',f"A newer candle ({current.get('snapshot')}) replaced the one this plan was built on ({saved.get('snapshot')}). Its next-open entry has passed; prepare the plan again.")
 current_rule=current.get('rule') or {}
 if len({rule_key(current_rule),rule_key(saved_rule),rule_key(saved.get('rule') or saved_rule)})!=1 or payload.get('hold')!=current_rule.get('hold') or payload.get('reward')!=current_rule.get('target_r'):
  raise PilotError(409,'EXIT_EVIDENCE_CHANGED',f"The tested exit rule changed since this plan was saved (saved: {rule_text(saved_rule)}; now: {rule_text(current_rule)}). Prepare the plan again.")
 return dict(run=current.get('run'),saved_run=saved.get('run'),run_changed=current.get('run')!=saved.get('run'),snapshot=current.get('snapshot'),
  rule=current_rule,data_end=end.isoformat(),data_age_days=age,max_data_age_days=max_age_days,basis='exact_rule_later_test',
  reason=current.get('tradable_reason'))

class Evidence:
 def __init__(self,settings,http,last_good=None):
  self.settings=settings;self.http=http;self.cache={}
  # Last-good DISPLAY cache. A Settings without `last_good_path` (the tests' SimpleNamespace) gets an
  # in-memory store, so nothing here depends on a file existing.
  self.last_good=last_good if last_good is not None else LastGood(getattr(settings,'last_good_path','') or '')
  #: Wall-clock second the CURRENT outage began, or None while the scanner is answering. Any successful
  #: upstream call - display or trade path - clears it, so "how long has it been down" is one honest number.
  self.outage_since=None
  self.upstream_error=''
 def study(self,path,params=None,body=None):
  # Intentional, not inverted: research_directory selects the offline snapshot mode (see get()),
  # which serves only precomputed JSON and has no research worker. Studies, replays and lab jobs
  # need live computation over the scanner API, so they are refused rather than faked.
  if self.settings.research_directory:raise PilotError(503,'STUDY_WORKER','Connect the historical research worker to run or replay a study.')
  if path not in ('/api/studies','/api/studies/job','/api/studies/cancel','/api/studies/chart','/api/replay','/api/replay/chart',
   '/api/lab/capabilities','/api/lab/jobs','/api/lab/job','/api/lab/chart','/api/lab/strategies','/api/lab/portfolios',
   '/api/lab/interpret','/api/lab/research','/api/lab/cancel','/api/lab/save','/api/lab/analyse','/api/lab/prepare','/api/lab/portfolio-action'):raise PilotError(404,'NOT_FOUND','Study endpoint not found.')
  try:
   response=self.http.post(self.settings.research_url+path,json=body) if body is not None else self.http.get(self.settings.research_url+path,params=params or {})
   value=response.json()
  except Exception:raise PilotError(503,'STUDY_WORKER','The historical research worker is unavailable. Retry shortly.') from None
  if response.status_code>=400:raise PilotError(response.status_code,'STUDY_INVALID',value.get('error','Study unavailable'))
  return value
 # --- outage bookkeeping ------------------------------------------------------------------------------------
 def mark_down(self,message):
  if self.outage_since is None:self.outage_since=time.time()
  self.upstream_error=message
 def mark_up(self):
  self.outage_since=None;self.upstream_error=''
 def down_seconds(self):
  return 0 if self.outage_since is None else int(max(0,time.time()-self.outage_since))
 def provenance(self,value,stored,message):
  """What the app is told about a replayed body, and the body with that provenance attached.

  Only these provenance keys are added - no live field is ever merged into a cached body, and no number
  inside it is recomputed. A list body (the legacy `/api/matches` shape) is returned untouched; the app
  learns the outage from `/api/state` and the strategy catalog, which are objects.
  """
  down=self.down_seconds()
  as_of=None
  if isinstance(value,dict):
   status=value.get('data_status') if isinstance(value.get('data_status'),dict) else {}
   as_of=value.get('server_time') or status.get('as_of') or value.get('source_latest') or value.get('data_end')
  prov=dict(served_from_cache=True,cached_at=stamp_ist(stored),cached_as_of=as_of,upstream_error=message,
   upstream_down_seconds=down,reconnecting=down<=RECONNECT_GRACE_SECONDS)
  return ({**value,**prov} if isinstance(value,dict) else value),prov

 def fetch(self,path,params=None,cache=False):
  """`(value, provenance)`; provenance is None when the value came from the scanner just now.

  A thin wrapper over `get()` so that a subclass which overrides `get` (the test doubles do) is still the
  one answering. For a dict body the same provenance is also stamped into the body itself, so a caller that
  only has the value can still tell.
  """
  value=self.get(path,params,cache)
  entry=self.cache.get(cache_key(path,params or {})) if isinstance(getattr(self,'cache',None),dict) else None
  return value,(entry[2] if entry and len(entry)>2 else None)

 def get(self,path,params=None,cache=False):
  """One research read.

  `cache=False` (the default) is the fail-closed path every trade decision uses: an unreachable scanner is
  a 503, exactly as before this cache existed. `cache=True` is the DISPLAY path: when the scanner refuses
  the connection, times out or answers 5xx (including "still starting up"), the last successful body for
  this exact path+query is replayed, stamped with `served_from_cache`, `cached_at`, `cached_as_of`,
  `upstream_error` and `upstream_down_seconds`. No live field is ever merged into a replayed body.
  """
  params=params or {}
  if path not in PATHS:raise PilotError(404,'NOT_FOUND','Research endpoint not found.')
  key=cache_key(path,params)
  cached=self.cache.get(key)
  if cached and cached[0]>time.monotonic():
   # A short-lived replay must not leak into a fail-closed read: only a cache=True caller may see it.
   if cached[2] is None or cache:return cached[1]
  if self.settings.research_directory:
   root=Path(self.settings.research_directory)
   file=root/(key+'.json')
   if file.is_file():value=json.loads(file.read_text(encoding='utf-8'))
   elif path=='/api/stocks':
    index=json.loads((root/'stocks.json').read_text(encoding='utf-8'));q=params.get('q','').upper()
    value=[s for s in index if q in (s['symbol']+' '+s.get('company','')).upper()][:50]
   elif path=='/api/backtests/capital':
    cell=self.get('/api/backtests/cell',{k:params[k] for k in ('symbol','timeframe','pattern','side')})
    if params.get('run') and cell['run_id']!=params['run']:raise PilotError(409,'STALE_EVIDENCE','Reopen this study to refresh its evidence.')
    value=study_account(cell,params.get('segment','reference'),params.get('capital','auto'))
   else:raise PilotError(404,'EVIDENCE_UNAVAILABLE','This research object is not in the pilot snapshot.')
  else:
   try:
    response=self.http.get(self.settings.research_url+path,params=params);response.raise_for_status();value=response.json()
   except Exception as error:
    message=upstream_message(error)
    self.mark_down(message)
    row=self.last_good.get(key) if cache and path in CACHEABLE else None
    if row is None:
     raise PilotError(503,'RESEARCH_UNAVAILABLE','Research is temporarily unavailable. Your account and saved plans are safe.') from None
    served,prov=self.provenance(row[0],row[1],message)
    if len(self.cache)>160:self.cache.clear()
    self.cache[key]=(time.monotonic()+FAILOVER_SECONDS,served,prov)
    return served
   self.mark_up()
  if isinstance(value,dict):value.pop('database',None)
  # Only what the app renders is remembered, and only after a clean upstream answer.
  if cache and path in CACHEABLE and not self.settings.research_directory:self.last_good.put(path,key,value)
  if len(self.cache)>160:self.cache.clear()
  self.cache[key]=(time.monotonic()+LIVE_SECONDS,value,None)
  return value
 # --- the two match sets -----------------------------------------------------------------------------------
 # DISPLAY and TRADE are deliberately different sets, and the difference is the whole point:
 #   display_matches() - what the chart workspace, the legend and the watchlist draw. On the research pattern
 #                       set these are real detections from the live scanner, carrying NO history, so nothing
 #                       downstream can read an evidence number off them.
 #   matches()         - what the exit plan, the plan builder and the trade gates resolve against. That path
 #                       joins the LEGACY backtest store by (symbol, timeframe, pattern), so a research
 #                       detection must never enter it: EVIDENCE_SERVING_CONTRACT.md §2 forbids exactly that
 #                       pattern-name-only fallback. It therefore fails CLOSED with an empty list.
 def pattern_set(self):
  """Which detector set the scanner is running: 'legacy' or 'research'. Cheap - /api/state is cached."""
  try:state=self.get('/api/state',{},cache=True) or {}
  except PilotError:return 'legacy'
  return 'research' if state.get('pattern_set')==RESEARCH_SET else 'legacy'

 def display_matches(self,params=None):
  """Matches for the DISPLAY surfaces, always a list.

  On the legacy set this is byte-identical to the call the app has always made. On the research set the
  scanner answers with a paginated object over tens of thousands of detections, so the request is narrowed to
  the live book on the newest completed candle and capped; `/api/state` carries the real totals, so nothing
  is hidden. Each row is adapted by `display_match` - and keeps its empty `history`.
  """
  params=dict(params or {})
  if self.pattern_set()!='research':
   return self.get('/api/matches',params,cache=True)
  # `performance`/`return_band`/`min_trades` screen a match on its LEGACY backtest history, which a research
  # detection does not have: the screen is False for every row, so it empties the book instead of filtering
  # it. The scanner refuses it with a 400 (market_scanner/performance.NO_LEGACY_HISTORY_REFUSAL); refusing it
  # HERE gives the caller that same honest answer instead of an upstream 4xx, which `get()` would otherwise
  # read as an outage and answer with a stale body or RESEARCH_UNAVAILABLE. `min_trades=0` is the explicit
  # "any sample size" - the absence of a screen - and is passed through.
  if asks_for_legacy_history_screen(params):
   raise PilotError(400,'NO_LEGACY_HISTORY','These are live detections, not stored backtests, so there are no '
    'past trades to count. Drop the performance, return-range or minimum-trades filter. Each setup’s evidence '
    'is on its own card.')
  query={**params,'live':'true','current':'true','limit':str(RESEARCH_DISPLAY_LIMIT)}
  value=self.get('/api/matches',query,cache=True)
  rows=value.get('matches') if isinstance(value,dict) else value
  out={}
  for m in rows or []:
   # A scanner match id identifies a CELL, not an episode, so two standing episodes of the same cell share
   # one id. The list is score-ordered, so the first is the best-fitting; keeping it leaves the served ids
   # unique, which the workspace's own id lookups and the chart legend both rely on.
   if isinstance(m,dict) and m.get('id') not in out:out[m.get('id')]=display_match(m)
  return list(out.values())

 def display_match(self,identity):
  match=next((m for m in self.display_matches({'min_trades':'0'}) if m.get('id')==identity),None)
  if not match:raise PilotError(409,'SETUP_CHANGED','This setup is no longer in the current research snapshot.')
  return match

 def matches(self):
  value=self.get('/api/matches',{'min_trades':'0'})
  return value if isinstance(value,list) else []
 def match(self,identity):
  match=next((m for m in self.matches() if m['id']==identity),None)
  if not match:raise PilotError(409,'SETUP_CHANGED','This setup is no longer in the current research snapshot.')
  return match
 def exit_plan(self,identity,side,run=None,snapshot=None):
  match=self.match(identity);h=next((h for h in match['history'] if h['side']==side),None)
  if not h:raise PilotError(400,'DIRECTION','Choose a researched direction.')
  if run and h['run']!=run or snapshot and match['candle_end']!=snapshot:raise PilotError(409,'STALE_EVIDENCE','Evidence changed. Reopen this setup.')
  cell=self.get('/api/backtests/cell',dict(symbol=match['symbol'],timeframe=match['timeframe'],pattern=match['pattern'],side=side))
  if cell['run_id']!=h['run']:raise PilotError(409,'STALE_EVIDENCE','Evidence changed. Reopen this setup.')
  chart=self.get('/api/chart',dict(symbol=match['symbol'],timeframe=match['timeframe']))
  plan=describe(match,side,chart,cell);tradable,reason=tradability(plan,match.get('direction'))
  # Added after the scanner's content id is computed; both fields derive from that content.
  plan.update(tradable_evidence=tradable,tradable_reason=reason,history_label=HISTORY_LABEL,
   evidence_basis='exact_rule_later_test' if tradable else 'illustrative_benchmark')
  return plan
 def prepare(self,data):
  match=self.match(data.get('match_id'))
  suggestion=self.exit_plan(match['id'],data.get('side')) if data.get('suggestion_id') or data.get('exit_mode')=='suggested' else None
  try:return build_plan(data,[match],suggestion)
  except (ValueError,TypeError,KeyError) as e:raise PilotError(400,'PLAN_INVALID',str(e)) from None
