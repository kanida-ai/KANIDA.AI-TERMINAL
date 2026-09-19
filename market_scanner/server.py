from __future__ import annotations
import argparse
import json
import logging
import mimetypes
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .data import ROOT, load_config
from .engine import Scanner
from . import backtest_store, catalog, capital, pattern_live, performance, studies, strategy_lab, strategy_portfolio


#: How long the previous successful startup took, so "starting" can carry a progress estimate.
BOOT_TIME_FILE=ROOT/'output'/'boot_time.json'


class Booting:
    """The scanner while it is still being built, so the port is open before it is ready.

    Loading the snapshot (~90,000 cells) takes minutes. Building the Scanner BEFORE binding the socket meant
    every caller got a refused connection for that whole time and could not tell "starting" from "crashed".
    The port is now opened first and this object answers until the Scanner exists: HTTP 503 carrying
    `starting: true`, which the pilot reads as "still starting up" rather than a failure
    (kanida_pilot/evidence.upstream_message).

    `loaded_pct` is honest about what it is. The engine does not report progress while it reads the snapshot,
    and instrumenting it would mean changing engine.py, so the percentage is elapsed time against how long the
    LAST successful startup took - and it says so with `loaded_pct_estimated`. It is never allowed to reach 100.
    """
    def __init__(self):
        self.scanner=None
        self.error=None
        self.started=time.time()
        self.expected=self.last_boot_seconds()

    @staticmethod
    def last_boot_seconds():
        try: return float(json.loads(BOOT_TIME_FILE.read_text(encoding='utf-8'))['seconds'])
        except Exception: return None

    def record_boot_seconds(self,seconds):
        try:
            BOOT_TIME_FILE.parent.mkdir(parents=True,exist_ok=True)
            BOOT_TIME_FILE.write_text(json.dumps({'seconds':round(seconds,1)}),encoding='utf-8')
        except Exception: pass

    def status(self):
        """The 503 body while the snapshot is loading, or the failure if the build did not survive."""
        elapsed=round(time.time()-self.started,1)
        if self.error:
            return {'error':f'The scanner failed to start: {self.error}','starting':False,'phase':'failed',
                    'elapsed_seconds':elapsed}
        body={'error':'The scanner is still starting up. Retry shortly.','starting':True,
              'phase':'loading snapshot','elapsed_seconds':elapsed}
        if self.expected and self.expected>0:
            body.update(loaded_pct=min(99,round(100*elapsed/self.expected)),loaded_pct_estimated=True,
                        expected_seconds=round(self.expected,1))
        return body

    def build(self,config,reuse_snapshot):
        """Construct the real Scanner off the accept loop. A failure is reported, never a silent exit."""
        try:
            scanner=Scanner(config)
            scanner.watch(initial_scan=not reuse_snapshot or not scanner.metadata.get('finished_at'))
            self.scanner=scanner        # published last: nothing serves a half-built scanner
            seconds=time.time()-self.started
            self.record_boot_seconds(seconds)
            print(f'KANIDA snapshot loaded in {round(seconds,1)}s',flush=True)
        except Exception as exc:
            self.error=f'{type(exc).__name__}: {exc}'
            logging.exception('Scanner failed to start')


def make_handler(boot):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,format,*args):
            if args and ('/api/state' in str(args[0]) or '/api/matches' in str(args[0])): return
            logging.getLogger('http').info(format,*args)

        def send(self,status,body,content_type='application/json; charset=utf-8'):
            if not isinstance(body,bytes): body=json.dumps(body,allow_nan=False).encode()
            self.send_response(status)
            self.send_header('Content-Type',content_type)
            self.send_header('Content-Length',str(len(body)))
            self.send_header('Cache-Control','no-store')
            self.send_header('X-Content-Type-Options','nosniff')
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            url=urlparse(self.path)
            query=parse_qs(url.query)
            get=lambda k,default='':query.get(k,[default])[0]
            scanner=boot.scanner
            if scanner is None:
                # /health stays answerable while starting, so a watchdog can tell "booting" from "dead".
                if url.path=='/health':return self.send(200,dict(boot.status(),ok=False,app='KANIDA'))
                return self.send(503,boot.status())
            try:
                if url.path=='/api/lab/capabilities':return self.send(200,strategy_lab.capabilities())
                if url.path=='/api/lab/jobs':return self.send(200,strategy_lab.jobs(get('owner')))
                if url.path=='/api/lab/job':return self.send(200,strategy_lab.get(get('owner'),get('id'),True))
                if url.path=='/api/lab/strategies':return self.send(200,strategy_lab.saved(get('owner')))
                if url.path=='/api/lab/chart':return self.send(200,strategy_lab.chart(get('owner'),get('id'),get('variant'),get('trade')))
                if url.path=='/api/lab/portfolios':return self.send(200,strategy_portfolio.papers(get('owner')))
                if url.path=='/api/studies':return self.send(200,studies.jobs(get('owner')))
                if url.path=='/api/studies/job':return self.send(200,studies.get_job(get('owner'),get('id'),get('result')=='true'))
                if url.path=='/api/studies/chart':return self.send(200,studies.trade_chart(get('owner'),get('id'),get('trade'),get('occurrence')))
                if url.path=='/api/replay':return self.send(200,studies.legacy_replay({k:v[0] for k,v in query.items()}))
                if url.path=='/api/replay/chart':
                    cell=backtest_store.cell(get('symbol'),get('timeframe'),get('pattern'),get('side'))
                    if not cell or cell['run_id']!=get('run'):raise ValueError('Historical snapshot changed; reopen the evidence')
                    trade=next((t for t in cell['reference_trades'] if t['signal_index']==int(get('signal','-1'))),None)
                    if not trade:raise ValueError('Historical trade not found')
                    return self.send(200,studies.chart_bundle(cell['run_id'],get('symbol'),get('timeframe'),trade['signal_index'],get('pattern'),trade))
                if url.path=='/api/state': return self.send(200,scanner.state())
                if url.path=='/api/filter-options':return self.send(200,catalog.options())
                if url.path=='/api/backtests/state':return self.send(200,backtest_store.state())
                if url.path=='/api/backtests':return self.send(200,backtest_store.listing({k:v[0] for k,v in query.items()}))
                if url.path=='/api/backtests/cell':
                    data=backtest_store.cell(get('symbol').upper(),get('timeframe'),get('pattern'),get('side'))
                    if data:data=catalog.annotate([data])[0]
                    return self.send(200 if data else 404,data or {'error':'This stock study is not available yet'})
                if url.path=='/api/backtests/capital':
                    data=backtest_store.cell(get('symbol').upper(),get('timeframe'),get('pattern'),get('side'))
                    if data and get('run') and get('run')!=data['run_id']:
                        return self.send(409,{'error':'The active backtest changed. Reopen the study before calculating capital.'})
                    return self.send(200,capital.study_account(data,get('segment','reference'),get('capital','auto'))) if data else self.send(404,{'error':'This stock study is not available yet'})
                if url.path=='/api/backtests/chart':
                    data=backtest_store.chart(get('symbol').upper(),get('timeframe'),int(get('signal','-1')),get('pattern'),get('side'),get('segment','reference'))
                    return self.send(200 if data else 404,data or {'error':'Historical chart not available'})
                if url.path=='/api/matches':
                    data=scanner.summaries()
                    for key in ('pattern','timeframe','symbol','state'):
                        if get(key): data=[m for m in data if m[key]==get(key)]
                    # Research pattern set only: these keys do not exist on a legacy match,
                    # so an unused filter leaves the legacy response untouched.
                    for key in ('pattern_id','variant','side','family','detector_state','evidence_status'):
                        if get(key): data=[m for m in data if m.get(key)==get(key)]
                    if get('live')=='true': data=[m for m in data if m.get('live')]
                    if get('current')=='true': data=[m for m in data if m['current']]
                    filters={k:v[0] for k,v in query.items()}
                    symbols=catalog.selected_symbols(filters)
                    if symbols is not None:
                        selected=set(symbols);data=[m for m in data if m['symbol'] in selected]
                    research=scanner.pattern_set==pattern_live.RESEARCH
                    if research:
                        # The legacy backtest store holds CH01-CH10 under the original
                        # scanner's identity. Joining it to a research detection would be
                        # exactly the pattern-name-only fallback the evidence contract
                        # forbids, so research matches carry no legacy history at all.
                        data=catalog.annotate(data)
                        for m in data: m['history']=[]
                    else:
                        data=backtest_store.match_history(data)
                    mode,profile,minimum=performance.screen_args(filters)
                    history_screen=None
                    if research:
                        # A screen over an EMPTY history is False for every row, so running it here deleted
                        # 100% of the detections - including on a bare request, where the only reason it ran
                        # at all was performance.DEFAULT_MIN_TRADES. It is not run on research matches, and
                        # it is not dropped on the floor either: `history_screen` in the body says it did not
                        # apply and why, and a caller that explicitly ASKED for it is refused (400) rather
                        # than handed a set that merely looks screened.
                        if performance.history_screen_requested(filters):
                            raise ValueError(performance.NO_LEGACY_HISTORY_REFUSAL)
                        history_screen=performance.history_screen_report(filters)
                    elif profile or get('return_band') or minimum:
                        data=[m for m in data if any(performance.screen_stats(h[mode],filters) for h in m['history'])]
                    if research:
                        # 262 cells x 495 symbols produces tens of thousands of live
                        # detections; an unbounded response is not servable. The count
                        # before the cut is always returned so nothing is hidden.
                        total=len(data)
                        limit=max(1,min(int(get('limit','500') or 500),5000))
                        data=data[:limit]
                        if get('evidence')=='true':
                            found=pattern_live.research_cell_status(
                                [(m['symbol'],m['timeframe'],m['pattern_id'],m['variant'],m['side']) for m in data])
                            for m in data:
                                key=(m['symbol'],m['timeframe'],m['pattern_id'],m['variant'],m['side'])
                                cell=found.get(key)
                                m['research_evidence']=(dict(cell,status_source='research_cell') if cell and
                                                        m.get('evidence_status')=='identity_match'
                                                        else {'status':'no_compatible_evidence'})
                        return self.send(200,{'matches':data,'total':total,'returned':len(data),
                                              'limit':limit,'pattern_set':scanner.pattern_set,
                                              'history_screen':history_screen})
                    return self.send(200,data)
                if url.path=='/api/stocks':
                    term=get('q').upper()
                    symbols=catalog.selected_symbols({k:v[0] for k,v in query.items()})
                    selected=set(symbols) if symbols is not None else None
                    return self.send(200,catalog.annotate([s for s in scanner.stocks if (selected is None or s['symbol'] in selected) and (term in s['symbol'].upper() or term in s['company'].upper())][:50]))
                if url.path=='/api/stock':
                    data=scanner.stock(get('symbol').upper())
                    return self.send(200 if data else 404,data or {'error':'Unknown stock'})
                if url.path=='/api/chart':
                    data=scanner.chart(get('symbol').upper(),get('timeframe'))
                    return self.send(200 if data else 404,data or {'error':'No scan snapshot available yet'})
                if url.path=='/api/coverage': return self.send(200,scanner.coverage())
                if url.path=='/api/rules':
                    return self.send(200,{'text':(ROOT/'RULES.md').read_text(encoding='utf-8')})
                if url.path=='/health': return self.send(200,{'ok':True,'app':'KANIDA'})
                if url.path in ('/research-readout','/research-readout.json'):
                    extension='json' if url.path.endswith('.json') else 'html'
                    file=ROOT/'output'/('research_readout.'+extension)
                    if not file.exists():return self.send(404,{'error':'Research readout has not been generated yet'})
                    return self.send(200,file.read_bytes(),('application/json' if extension=='json' else 'text/html')+'; charset=utf-8')
                files={'/':'index.html','/index.html':'index.html','/style.css':'style.css','/app.js':'app.js','/chart.js':'chart.js','/backtest.js':'backtest.js','/filters.js':'filters.js','/capital.js':'capital.js','/favicon.svg':'favicon.svg'}
                if url.path not in files: return self.send(404,{'error':'Not found'})
                file=ROOT/'static'/files[url.path]
                return self.send(200,file.read_bytes(),(mimetypes.guess_type(file)[0] or 'application/octet-stream')+'; charset=utf-8')
            except (BrokenPipeError,ConnectionResetError,ConnectionAbortedError): pass
            except ValueError as error:
                self.send(400,{'error':str(error)})
            except Exception:
                logging.exception('Request failed')
                self.send(500,{'error':'The request could not be completed. See the server log.'})

        def do_POST(self):
            scanner=boot.scanner
            if scanner is None:return self.send(503,boot.status())
            # Local-only API: cross-origin pages cannot trigger a market scan.
            origin=self.headers.get('Origin')
            allowed={f'http://127.0.0.1:{scanner.config["port"]}',f'http://localhost:{scanner.config["port"]}'}
            if origin and origin not in allowed: return self.send(403,{'error':'Local origin required'})
            path=urlparse(self.path).path
            if path.startswith('/api/lab/'):
                try:
                    length=int(self.headers.get('Content-Length','0'))
                    if not 0<length<=65536:raise ValueError('Request is too large')
                    data=json.loads(self.rfile.read(length));owner=studies.identity(data.get('owner'))
                    actions={
                        'interpret':lambda:strategy_lab.interpret(data.get('text'),data.get('previous')),
                        'research':lambda:strategy_lab.start(owner,data),
                        'cancel':lambda:strategy_lab.cancel(owner,data.get('id')),
                        'save':lambda:strategy_lab.save_strategy(owner,data),
                        'analyse':lambda:strategy_portfolio.analyse(data),
                        'prepare':lambda:strategy_portfolio.prepare(owner,data),
                        'portfolio-action':lambda:strategy_portfolio.action(owner,data.get('id'),data),
                    }
                    fn=actions.get(path.rsplit('/',1)[-1])
                    if not fn:raise ValueError('Unknown strategy action')
                    return self.send(200,fn())
                except (ValueError,TypeError,KeyError) as e:return self.send(400,{'error':str(e)})
            if path in ('/api/studies','/api/studies/cancel'):
                try:
                    length=int(self.headers.get('Content-Length','0'))
                    if not 0<length<=65536:raise ValueError('Study request is too large')
                    data=json.loads(self.rfile.read(length))
                    result=studies.start(data.get('owner'),data.get('settings')) if path=='/api/studies' else studies.cancel(data.get('owner'),data.get('id'))
                    return self.send(200,result)
                except (ValueError,TypeError,KeyError) as e:return self.send(400,{'error':str(e)})
            if urlparse(self.path).path!='/api/scan': return self.send(404,{'error':'Not found'})
            accepted=scanner.request_scan(force=True)
            return self.send(202 if accepted else 409,{'accepted':accepted,'message':'Scan started' if accepted else 'A scan is already running'})
    return Handler


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--port',type=int)
    parser.add_argument('--reuse-snapshot',action='store_true',help='For a server-code reload: keep the complete cached scan; still watch future source commits and candle closes')
    args=parser.parse_args()
    config=load_config()
    if args.port: config['port']=args.port
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    # The port is bound FIRST and the snapshot is loaded behind it, so a caller during the minutes-long
    # startup gets a clear "starting, N% loaded" instead of a refused connection it cannot interpret.
    boot=Booting()
    server=ThreadingHTTPServer((config['host'],config['port']),make_handler(boot))
    threading.Thread(target=boot.build,args=(config,args.reuse_snapshot),daemon=True,name='scanner-boot').start()
    print(f'KANIDA ready at http://{config["host"]}:{config["port"]} (loading the scan snapshot)',flush=True)
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally:
        if boot.scanner is not None: boot.scanner.stop.set()
        server.server_close()


if __name__=='__main__': main()
