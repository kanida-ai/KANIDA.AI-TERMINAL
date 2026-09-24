"""The workspace definition, validated. The page saves this whole object; the server checks every widget against
the registry and stores a canonical copy, so what comes back is always something the page can draw.

    {
      "name": "My Intraday Options",
      "selected": {"underlying": "NIFTY", "expiry": null,
                   "focus": {"side": "CE", "strikes": [24400, 24500], "from": "10:15", "to": "11:15",
                             "source": "Call OI building", "because": ["..."]}},
      "widgets": [
        {"widget_id": "w1", "widget_type": "option_chain", "position": 0, "size": "M", "height": "tall",
         "follow_workspace": true, "instrument": null, "expiry": null, "strike_range": null, "timeframe": null,
         "scanner_id": null, "settings": {}}
      ]
    }

`position` is the reading order: the grid packs widgets left to right in that order, and the phone feed shows them
top to bottom in it. Size is one of S / M / L / F(ull) — 3 / 4 / 6 / 12 of 12 columns.
"""
from __future__ import annotations
import re
import uuid
from .registry import HEIGHTS,SIZES,WIDGETS

MAX_WIDGETS=24
COLUMNS=('auto',2,3,4,5)
NAME_MAX=60
SYMBOL=re.compile(r'^[A-Z0-9&-]{1,20}$')
DATE=re.compile(r'^\d{4}-\d{2}-\d{2}$')
TIME=re.compile(r'^\d{2}:\d{2}$')


class WorkspaceError(ValueError):
 pass


def _symbol(v):
 if v in (None,''):return None
 s=str(v).strip().upper()
 if not SYMBOL.match(s):raise WorkspaceError(f'"{v}" is not an instrument symbol.')
 return s


def _date(v):
 if v in (None,'','follow'):return None
 s=str(v)
 if not DATE.match(s):raise WorkspaceError(f'"{v}" is not an expiry date.')
 return s


def _widget(raw,index):
 if not isinstance(raw,dict):raise WorkspaceError('Each widget must be an object.')
 kind=raw.get('widget_type')
 spec=WIDGETS.get(kind)
 if not spec:raise WorkspaceError(f'There is no widget called "{kind}".')
 size=raw.get('size') or spec['size']
 if size not in SIZES:raise WorkspaceError(f'Widget size must be one of {", ".join(SIZES)}.')
 height=raw.get('height') or spec['height']
 if height not in HEIGHTS:raise WorkspaceError('Widget height is short or tall.')
 follow=bool(raw.get('follow_workspace',True)) if spec['instrument'] else False
 instrument=_symbol(raw.get('instrument')) if spec['instrument'] else None
 if spec['instrument'] and not follow and not instrument:
  raise WorkspaceError(f'{spec["label"]}: a pinned widget needs its own instrument.')
 settings={}
 allowed=spec['settings']
 for key,value in (raw.get('settings') or {}).items():
  if key not in allowed:continue  # a setting this widget does not have is dropped, never stored
  rule=allowed[key]
  if rule=='expiry':continue
  if rule=='scanner':settings[key]=str(value)[:32] if value else None;continue
  if value not in rule:raise WorkspaceError(f'{spec["label"]}: "{value}" is not a choice for {key}.')
  settings[key]=value
 expiry=_date(raw.get('expiry')) if 'expiry' in allowed else None
 scanner=str(raw.get('scanner_id') or settings.pop('scanner_id',None) or '')[:32] or None
 if 'scanner_id' not in allowed:scanner=None
 wid=re.sub(r'[^a-z0-9]','',str(raw.get('widget_id') or ''))[:16] or uuid.uuid4().hex[:10]
 return {'widget_id':wid,'widget_type':kind,'position':index,'size':size,'height':height,
  'follow_workspace':follow,'instrument':instrument,'expiry':expiry,'strike_range':None,'timeframe':None,
  'scanner_id':scanner,'settings':settings}


def _focus(raw):
 if not isinstance(raw,dict):return None
 side=raw.get('side') if raw.get('side') in ('CE','PE') else None
 strikes=[]
 for s in (raw.get('strikes') or [])[:12]:
  try:strikes.append(float(s))
  except (TypeError,ValueError):continue
 a,b=str(raw.get('from') or ''),str(raw.get('to') or '')
 because=[str(x)[:300] for x in (raw.get('because') or [])[:4]]
 # the scanner the match came from and the match's own key, so the summary and the signal can speak about THAT
 # scanner's match (checked against the user's own scanners when it is read, never trusted from here)
 sid=re.sub(r'[^A-Za-z0-9_-]','',str(raw.get('scanner_id') or ''))[:32] or None
 key=str(raw.get('match_key') or '')[:80] or None
 return {'side':side,'strikes':strikes,'from':a if TIME.match(a) else None,'to':b if TIME.match(b) else None,
  'source':str(raw.get('source') or '')[:80] or None,'because':because,'scanner_id':sid,'match_key':key}


def normalize(raw):
 if not isinstance(raw,dict):raise WorkspaceError('A workspace must be an object.')
 name=' '.join(str(raw.get('name') or '').split())[:NAME_MAX] or 'My workspace'
 widgets=raw.get('widgets') or []
 if not isinstance(widgets,list):raise WorkspaceError('widgets must be a list.')
 if len(widgets)>MAX_WIDGETS:raise WorkspaceError(f'A workspace holds at most {MAX_WIDGETS} widgets.')
 ordered=sorted(enumerate(widgets),key=lambda p:(p[1].get('position',p[0]) if isinstance(p[1],dict) else p[0],p[0]))
 out=[_widget(w,i) for i,(_j,w) in enumerate(ordered)]
 ids=[w['widget_id'] for w in out]
 if len(set(ids))!=len(ids):
  seen=set()
  for w in out:
   if w['widget_id'] in seen:w['widget_id']=uuid.uuid4().hex[:10]
   seen.add(w['widget_id'])
 sel=raw.get('selected') or {}
 selected={'underlying':_symbol(sel.get('underlying')),'expiry':_date(sel.get('expiry')),'focus':_focus(sel.get('focus'))}
 # how many widgets a row may hold: 'auto' (as many as fit at their minimum widths) or a cap the user chose
 cols=(raw.get('layout') or {}).get('columns','auto')
 if cols not in COLUMNS:raise WorkspaceError('Widgets per row is Auto or 2-5.')
 return {'name':name,'selected':selected,'widgets':out,'layout':{'columns':cols}}


def widget(kind,size=None,height=None,**extra):
 """A template's widget, before normalisation."""
 return {'widget_type':kind,**({'size':size} if size else {}),**({'height':height} if height else {}),**extra}
