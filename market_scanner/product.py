"""LAN product preview, with read-only research access and isolated paper plans.

This service never submits orders. Plans are durable review artifacts; a broker or
forward paper execution engine is deliberately not represented as connected.
"""
from __future__ import annotations
import argparse
from contextlib import closing
from datetime import datetime, timezone
import json
import math
import mimetypes
from pathlib import Path
import sqlite3
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, unquote, parse_qs, urlencode
from functools import lru_cache
from .exit_plan import describe
from urllib.request import urlopen
from urllib.error import HTTPError, URLError

from .data import ROOT

DB = ROOT / 'output' / 'product.sqlite3'
WEB = ROOT.parent / 'kanida-app' / 'dist'
UPSTREAM = 'http://127.0.0.1:8765'
READ_PATHS = {'/api/state', '/api/filter-options', '/api/matches', '/api/stocks',
              '/api/stock', '/api/chart', '/api/coverage', '/api/backtests/state',
              '/api/backtests', '/api/backtests/cell', '/api/backtests/chart', '/api/backtests/capital'}


def now(): return datetime.now(timezone.utc).isoformat()


def read_engine(path):
    with urlopen(UPSTREAM + path, timeout=30) as response:
        data = json.load(response)
    if urlparse(path).path == '/api/state':
        data.pop('database', None)
    return data


@lru_cache(maxsize=128)
def exit_study(run,symbol,timeframe,pattern,side):
    value=read_engine('/api/backtests/cell?'+urlencode(dict(symbol=symbol,timeframe=timeframe,pattern=pattern,side=side))) or {}
    if run and value.get('run_id')!=run:raise ValueError('Research changed. Reopen this setup.')
    return value


def get_exit_plan(identity,side,run=None,snapshot=None,matches=None):
    rows=matches if matches is not None else read_engine('/api/matches?min_trades=0')
    match=next((m for m in rows if m['id']==identity),None)
    if not match:raise ValueError('This setup is no longer in the scan.')
    history=next((h for h in match['history'] if h['side']==side),None)
    if not history:raise ValueError('Choose a researched direction')
    actual_run=history.get('run')
    if (run and run!=actual_run) or (snapshot and snapshot!=match['candle_end']):raise ValueError('Evidence or candle changed. Reopen this setup.')
    study=exit_study(actual_run,match['symbol'],match['timeframe'],match['pattern'],side)
    chart=read_engine('/api/chart?'+urlencode(dict(symbol=match['symbol'],timeframe=match['timeframe'])))
    return describe(match,side,chart,study)


def initialize():
    DB.parent.mkdir(exist_ok=True)
    with closing(sqlite3.connect(DB)) as con, con:
        con.executescript('''CREATE TABLE IF NOT EXISTS plans (
          id TEXT PRIMARY KEY, payload TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT, created TEXT NOT NULL,
          title TEXT NOT NULL, detail TEXT NOT NULL, plan_id TEXT);
          CREATE TABLE IF NOT EXISTS watchlist (id TEXT PRIMARY KEY, payload TEXT NOT NULL);
        ''')
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS plan_request ON plans(json_extract(payload,'$.request_id'))")


def finite(value, name, low, high):
    try: value = float(value)
    except (ValueError, TypeError): raise ValueError(name + ' must be a number') from None
    if not math.isfinite(value) or not low <= value <= high:
        raise ValueError(f'{name} must be between {low} and {high}')
    return value


def build_plan(data, matches, suggestion=None):
    match = next((m for m in matches if m['id'] == data.get('match_id')), None)
    if not match: raise ValueError('This setup is no longer in the scan snapshot. Refresh Discover.')
    side = data.get('side')
    if side not in ('long', 'short') or not any(h['side'] == side for h in match.get('history', [])):
        raise ValueError('Choose a researched direction for this setup')
    account = finite(data.get('account', 100000), 'Paper account', 1000, 10000000)
    allocation = finite(data.get('allocation', 10000), 'Allocation', 100, account)
    risk_pct = finite(data.get('risk_pct', .5), 'Account risk', .1, 2)
    stop_pct = finite(data.get('stop_pct', 2.5), 'Stop distance', .001 if data.get('exit_mode')=='suggested' else .25, 20)
    reward = finite(data.get('reward', 2), 'Target multiple', .5, 5)
    history = next(h for h in match['history'] if h['side'] == side)
    hold = {'1H': 6, '4H': 6, '1D': 10, '1W': 4}[match['timeframe']]
    mode=data.get('exit_mode','custom')
    if mode not in ('suggested','custom'):raise ValueError('Unknown exit mode')
    if suggestion and data.get('suggestion_id')!=suggestion['id']:raise ValueError('Exit plan changed. Review the latest suggestion.')
    if mode=='suggested':
        if not suggestion or not suggestion['usable']:raise ValueError('A current usable exit suggestion is required')
        stop_pct=suggestion['stop_pct']; reward=suggestion['reward']; hold=suggestion['rule']['hold']
    else:
        raw_hold=finite(data.get('hold',hold),'Holding candles',1,260)
        if raw_hold!=int(raw_hold):raise ValueError('Holding candles must be a whole number')
        hold=int(raw_hold)
    trigger=suggestion['rule']['trigger'] if mode=='suggested' else data.get('trigger','setup')
    if trigger not in ('setup','confirmed'):raise ValueError('Invalid entry trigger')
    if side=='short' and stop_pct*reward>=100:raise ValueError('Short target must remain above zero')
    cost_pct=suggestion['cost_pct'] if suggestion else .4
    price = finite(match.get('price'), 'Snapshot price', .01, 10000000)
    risk_budget = account * risk_pct / 100
    # Reserve the same 0.40% round-trip cost assumption as the original research.
    quantity = min(math.floor(allocation / (price * (1+cost_pct/100))),
                   math.floor(risk_budget / (price * (stop_pct + cost_pct) / 100)))
    if quantity < 1: raise ValueError('Increase allocation or risk budget to cover one share and assumed costs')
    sign = 1 if side == 'long' else -1
    return dict(id=str(uuid.uuid4()), created=now(), updated=now(), status='draft',
        match_id=match['id'], symbol=match['symbol'], pattern=match['pattern'],
        pattern_name=match['pattern_name'], timeframe=match['timeframe'], side=side,
        snapshot_end=match['candle_end'], snapshot_price=price, source_current=match['current'],
        account=account, allocation=allocation, risk_pct=risk_pct, stop_pct=stop_pct,
        reward=reward, quantity=quantity, notional=round(quantity*price, 2),
        reserved_cost=round(quantity*price*cost_pct/100, 2),
        planned_risk=round(quantity*price*(stop_pct+cost_pct)/100, 2),
        stop_preview=round(price*(1-sign*stop_pct/100), 2),
        target_preview=round(price*(1+sign*stop_pct*reward/100), 2),
        hold=hold, entry=f'After a new {trigger} candle, enter at the next candle open',
        exit_mode=mode, exit_rule=suggestion['rule'] if mode=='suggested' else dict(kind='percent',trigger=trigger,hold=hold,target_r=reward),
        exit_evidence=suggestion if mode=='suggested' else None, evidence_applies=bool(mode=='suggested' and suggestion['evidence_applies']),
        cost_pct=cost_pct, suggestion_id=suggestion['id'] if suggestion else None,
        exit='Stop, target, or holding limit; first reached. Stop first if both occur in one candle.',
        execution='not_connected', rule_basis=(suggestion['label']+' · '+suggestion['stop_basis']) if mode=='suggested' else 'Custom exits; historical rule evidence does not apply.',
        history=history, minimum_history=int(finite(data.get('minimum_history',5), 'Minimum history', 0, 100000)),
        blockers=['Forward paper execution is not connected'] + ([] if mode=='suggested' and suggestion['evidence_applies'] else ['Exit rules lack independent support']) + ([] if match['current'] else ['Fresh completed-candle prices are required']) +
                 (['Short borrow and instrument eligibility require verification'] if side=='short' else []))


def list_product():
    with closing(sqlite3.connect(DB)) as con:
        plans=[json.loads(r[0]) for r in con.execute('SELECT payload FROM plans ORDER BY rowid DESC')]
        events=[dict(id=r[0],created=r[1],title=r[2],detail=r[3],plan_id=r[4])
                for r in con.execute('SELECT * FROM events ORDER BY id DESC LIMIT 80')]
        watchlist=[json.loads(r[0]) for r in con.execute('SELECT payload FROM watchlist ORDER BY rowid DESC')]
    return {'plans': plans, 'events': events, 'watchlist':watchlist,'mode': 'paper_planning', 'execution_connected': False}


def watch_action(data):
    identity=data.get('match_id')
    if not isinstance(identity,str) or len(identity)>150: raise ValueError('A setup id is required')
    action=data.get('action')
    if action not in ('add','remove'): raise ValueError('Unknown watch action')
    match=None
    if action=='add':
        match=next((m for m in read_engine('/api/matches?min_trades=0') if m['id']==identity),None)
        if not match: raise ValueError('This setup is not in the scan snapshot. Refresh your agent feed.')
    with closing(sqlite3.connect(DB,timeout=30)) as con, con:
        con.execute('BEGIN IMMEDIATE')
        exists=con.execute('SELECT payload FROM watchlist WHERE id=?',(identity,)).fetchone()
        if action=='add' and not exists:
            payload={k:match[k] for k in ('id','symbol','pattern','pattern_name','timeframe','direction','candle_end')}
            payload['created']=now()
            con.execute('INSERT INTO watchlist VALUES(?,?)',(identity,json.dumps(payload)))
            con.execute('INSERT INTO events(created,title,detail,plan_id) VALUES(?,?,?,?)',
                (now(),'Added to your watchlist',f"{match['symbol']} · {match['pattern_name']} · {match['timeframe']}. Compared with each refreshed scan while the app is open.",None))
        if action=='remove' and exists:
            con.execute('DELETE FROM watchlist WHERE id=?',(identity,))
    return {'ok':True,'action':action,'match_id':identity}


def save_plan(data):
    request_id=data.get('request_id')
    if not isinstance(request_id,str) or not request_id or len(request_id)>100:raise ValueError('A request id is required')
    with closing(sqlite3.connect(DB,timeout=30)) as con:
        existing=con.execute("SELECT payload FROM plans WHERE json_extract(payload,'$.request_id')=?",(request_id,)).fetchone()
        if existing:return json.loads(existing[0])
    matches=read_engine('/api/matches?min_trades=0')
    suggestion=get_exit_plan(data.get('match_id'),data.get('side'),matches=matches) if data.get('suggestion_id') or data.get('exit_mode')=='suggested' else None
    plan=build_plan(data,matches,suggestion)
    with closing(sqlite3.connect(DB,timeout=30)) as con, con:
        con.execute('BEGIN IMMEDIATE')
        # Double clicks and network retries use a stable client request id.
        request_id=str(data.get('request_id',''))
        if not request_id or len(request_id)>100: raise ValueError('A request id is required')
        for row in con.execute('SELECT payload FROM plans'):
            existing=json.loads(row[0])
            if existing.get('request_id')==request_id: return existing
        plan['request_id']=request_id
        con.execute('INSERT INTO plans VALUES(?,?)',(plan['id'],json.dumps(plan)))
        con.execute('INSERT INTO events(created,title,detail,plan_id) VALUES(?,?,?,?)',
            (now(),'Paper plan saved',f"{plan['symbol']} · {plan['pattern_name']} · {plan['timeframe']}. No order placed.",plan['id']))
    return plan


def change_plan(data):
    action=data.get('action')
    if action not in ('pause','resume','archive'): raise ValueError('Unknown plan action')
    with closing(sqlite3.connect(DB)) as con, con:
        row=con.execute('SELECT payload FROM plans WHERE id=?',(data.get('id'),)).fetchone()
        if not row: raise ValueError('Plan not found')
        plan=json.loads(row[0])
        if plan['status']=='archived': raise ValueError('This plan is archived')
        plan.update(status={'pause':'paused','resume':'draft','archive':'archived'}[action],updated=now())
        con.execute('UPDATE plans SET payload=? WHERE id=?',(json.dumps(plan),plan['id']))
        con.execute('INSERT INTO events(created,title,detail,plan_id) VALUES(?,?,?,?)',
                    (now(),f"Paper plan {plan['status']}",f"{plan['symbol']} · no execution enabled",plan['id']))
    return plan


def make_handler(port):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_): pass
        def send(self, status, data, content_type='application/json; charset=utf-8'):
            if not isinstance(data, bytes): data=json.dumps(data,allow_nan=False).encode()
            self.send_response(status)
            self.send_header('Content-Type',content_type)
            self.send_header('Content-Length',str(len(data)))
            self.send_header('Cache-Control','no-store')
            self.send_header('X-Content-Type-Options','nosniff')
            self.end_headers()
            self.wfile.write(data)
        def do_GET(self):
            try:
                path=urlparse(self.path).path
                if path=='/health': return self.send(200,{'ok':True,'app':'KANIDA Product'})
                if path=='/api/product': return self.send(200,list_product())
                if path=='/api/product/exit-plan':
                    q={k:v[0] for k,v in parse_qs(urlparse(self.path).query).items()}
                    return self.send(200,get_exit_plan(q.get('match_id'),q.get('side'),q.get('run'),q.get('snapshot')))
                if path in READ_PATHS: return self.send(200,read_engine(self.path))
                if path.startswith('/api/'): return self.send(404,{'error':'Not found'})
                file=(WEB/unquote(path).lstrip('/')).resolve()
                if not file.is_relative_to(WEB.resolve()): return self.send(404,{'error':'Not found'})
                if not file.is_file(): file=WEB/'index.html'
                if not file.exists(): return self.send(503,{'error':'Build the product web preview first'})
                return self.send(200,file.read_bytes(),mimetypes.guess_type(file)[0] or 'application/octet-stream')
            except ValueError as error: self.send(409,{'error':str(error)})
            except HTTPError as error: self.send(error.code, json.load(error))
            except (URLError, TimeoutError): self.send(503,{'error':'The research engine is unavailable. Start market_scanner first.'})
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError): pass
            except Exception as error: self.send(500,{'error':str(error)})
        def do_POST(self):
            try:
                # Browser writes must be same-origin JSON. Native clients have no Origin.
                origin=self.headers.get('Origin')
                if origin and origin!=f'http://{self.headers.get("Host")}':
                    return self.send(403,{'error':'Same-origin request required'})
                if self.headers.get('Content-Type','').split(';')[0]!='application/json':
                    return self.send(415,{'error':'JSON required'})
                size=int(self.headers.get('Content-Length','0'))
                if not 0<size<=8192: return self.send(413,{'error':'Invalid request size'})
                data=json.loads(self.rfile.read(size))
                if not isinstance(data,dict): raise ValueError('A JSON object is required')
                path=urlparse(self.path).path
                if path=='/api/product/plans': return self.send(201,save_plan(data))
                if path=='/api/product/watch': return self.send(200,watch_action(data))
                if path=='/api/product/plan-action': return self.send(200,change_plan(data))
                return self.send(404,{'error':'Not found'})
            except (ValueError,TypeError) as error: self.send(400,{'error':str(error)})
            except (URLError, TimeoutError): self.send(503,{'error':'The research engine is unavailable'})
            except (BrokenPipeError,ConnectionResetError,ConnectionAbortedError): pass
            except Exception as error: self.send(500,{'error':str(error)})
    return Handler


def main():
    global DB
    parser=argparse.ArgumentParser()
    parser.add_argument('--port',type=int,default=8082)
    parser.add_argument('--state-db',type=Path,help='Optional isolated paper-plan database for local testing')
    args=parser.parse_args()
    if args.state_db: DB=args.state_db.resolve()
    initialize()
    server=ThreadingHTTPServer(('0.0.0.0',args.port),make_handler(args.port))
    print(f'KANIDA product ready on port {args.port}',flush=True)
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()


if __name__=='__main__': main()
